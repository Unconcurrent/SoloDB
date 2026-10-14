using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Operations;

namespace SoloDatabase.Metadata.CSharp;

// Compiler-established use sites, rather than products of model root lists.
internal static class QueryRegistrations
{
    internal sealed class Code
    {
        internal Code(string helpers, string descriptors) { Helpers = helpers; Descriptors = descriptors; }
        internal string Helpers { get; }
        internal string Descriptors { get; }
    }
    private static readonly SymbolDisplayFormat Display = SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
        SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions | SymbolDisplayMiscellaneousOptions.ExpandValueTuple);

    internal static IEnumerable<ITypeSymbol> InferredRoots(Compilation compilation, IEnumerable<Site> sites, IEnumerable<ITypeSymbol> selected)
    {
        var known = new HashSet<string>(selected.Select(Name), StringComparer.Ordinal);
        foreach (var site in sites)
        {
            if (site.Method.ContainingType.ToDisplayString() == "System.Linq.Queryable") continue;
            foreach (var type in site.Method.TypeArguments.Concat(site.Method.ContainingType.TypeArguments))
                if (Closed(type) && !HasAnonymous(type)) { known.Add(Name(type)); yield return type; }
            if (site.Method.Name != "Compile" || site.Invocation.ArgumentList.Arguments.Count == 0 ||
                site.Invocation.ArgumentList.Arguments[0].Expression is not LambdaExpressionSyntax lambda) continue;
            var semantic = compilation.GetSemanticModel(site.Invocation.SyntaxTree);
            foreach (var node in lambda.DescendantNodes().OfType<ExpressionSyntax>())
            {
                var original = semantic.GetTypeInfo(node).Type;
                if (original == null) continue;
                var type = site.Symbols.Type(original);
                if (SequenceElement(type) != null || EnumerableElement(type) == null || !Closed(type) || HasAnonymous(type)) continue;
                yield return type;
            }
        }
        foreach (var operation in Providers(sites,known))
            if (!HasAnonymous(operation.Result)) yield return operation.Result;
    }

    private static List<(bool Sequence,ITypeSymbol Source,ITypeSymbol Result)> Providers(IEnumerable<Site> sites, HashSet<string> roots)
    {
        var direct = new List<(bool Sequence,ITypeSymbol Source,ITypeSymbol Result)>();
        foreach (var site in sites)
        {
            var method = site.Method;
            if (method.ContainingType.ToDisplayString() != "System.Linq.Queryable" || method.Parameters.Length == 0 ||
                SequenceElement(method.Parameters[0].Type) is not ITypeSymbol source || !Closed(source) || !Closed(method.ReturnType)) continue;
            var element = SequenceElement(method.ReturnType);
            direct.Add((element != null,source,element ?? method.ReturnType));
        }
        var result = new List<(bool Sequence,ITypeSymbol Source,ITypeSymbol Result)>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var operation in direct) if (roots.Contains(Name(operation.Source))) Add(operation);
        for (var index=0; index < result.Count; index++)
            if (result[index].Sequence)
                foreach (var next in direct)
                    if (SymbolEqualityComparer.Default.Equals(result[index].Result,next.Source))
                        Add((next.Sequence,result[index].Source,next.Result));
        return result;
        void Add((bool Sequence,ITypeSymbol Source,ITypeSymbol Result) operation)
        {
            if (seen.Add(operation.Sequence + "|" + Name(operation.Source) + "|" + Name(operation.Result))) result.Add(operation);
        }
    }

    internal static Code Collect(Compilation compilation, IEnumerable<string> models, IEnumerable<Site> sites, string registrationType)
    {
        var roots = new HashSet<string>(models, StringComparer.Ordinal);
        var carriers = new HashSet<string>(StringComparer.Ordinal);
        var helpers = new StringBuilder();
        var descriptors = new StringBuilder();
        var queryOperations = new HashSet<string>(StringComparer.Ordinal);
        string SourceTypes(IEnumerable<ITypeSymbol> types) => "new string[]{" + string.Join(",",types.Select(type => Quote(SourceTypeIdentity.Encode(type)))) + "}";
        bool Accessible(ITypeSymbol type) => SourceTypeIdentity.Accessible(compilation,type);
        void TypedRegistration(string method, params ITypeSymbol[] types)
        {
            descriptors.Append("[assembly: global::SoloDatabase.Generated.SourceQueryRegistration(").Append(Quote(method)).Append(',').Append(SourceTypes(types)).Append(")]\n");
        }
        INamedTypeSymbol? Callable(ITypeSymbol? original, Symbols symbols)
        {
            if (original == null || symbols.Type(original) is not INamedTypeSymbol type) return null;
            if (type.OriginalDefinition.ToDisplayString() == "System.Linq.Expressions.Expression<TDelegate>")
                type = (INamedTypeSymbol)type.TypeArguments[0];
            return type.DelegateInvokeMethod == null ? null : type;
        }
        void ParameterFacts(INamedTypeSymbol? callable)
        {
            if (callable?.DelegateInvokeMethod is not IMethodSymbol signature) return;
            var parameters = signature.Parameters.Where(parameter => parameter.RefKind == RefKind.None &&
                !parameter.Type.IsRefLikeType && parameter.Type.TypeKind is not (TypeKind.Pointer or TypeKind.FunctionPointer) && Closed(parameter.Type))
                .Select(parameter => parameter.Type).ToArray();
            var encoded = SourceTypes(parameters);
            if (queryOperations.Add("parameters|" + encoded))
                descriptors.Append("[assembly: global::SoloDatabase.Generated.SourceQueryRegistration(\"RegisterParameterFacts\",")
                    .Append(encoded).Append(")]\n");
        }
        void Operation(int kind, ITypeSymbol owner, string name, IEnumerable<ITypeSymbol> genericTypes,
            IEnumerable<ITypeSymbol> argumentTypes)
        {
            var key = kind + "|" + Name(owner) + "|" + name + "|" + Types(genericTypes) + "|" + Types(argumentTypes);
            if (!queryOperations.Add(key)) return;
            descriptors.Append("[assembly: global::SoloDatabase.Generated.SourceQueryOperationRegistration(").Append(kind).Append(',')
                .Append(Quote(SourceTypeIdentity.Encode(owner))).Append(',').Append(Quote(name)).Append(',')
                .Append(SourceTypes(genericTypes)).Append(',').Append(SourceTypes(argumentTypes)).Append(")]\n");
        }
        void Producer(int kind, string node, ITypeSymbol[] types, string body)
        {
            var function = kind == 6 ? "global::System.Func<int,int,global::System.Func<object[],object>,object>"
                : "global::System.Func<" + string.Join(",",types.Select(Name)) + ">";
            var key = kind + "|" + node + "|" + string.Join(",",types.Select(Name));
            if (!queryOperations.Add(key)) return;
            var entry = "QueryValueOperation" + queryOperations.Count;
            descriptors.Append("[assembly: global::SoloDatabase.Generated.QueryOperationRegistration(").Append(kind)
                .Append(",typeof(").Append(Name(types[0])).Append("),").Append(Quote(node)).Append(',')
                .Append(Types(types)).Append(',').Append(Types(kind == 6 ? ((INamedTypeSymbol)types[0]).DelegateInvokeMethod!.Parameters.Select(parameter => parameter.Type) : Array.Empty<ITypeSymbol>())).Append(',')
                .Append(Quote("SoloDatabase.Generated." + registrationType)).Append(',').Append(Quote(entry)).Append(")]\n");
            helpers.Append("internal static ").Append(function).Append(' ').Append(entry).Append("() => ").Append(body).Append(";\n");
        }

        foreach (var site in sites)
        {
            var semantic = compilation.GetSemanticModel(site.Invocation.SyntaxTree);
            var invocation = site.Invocation;
            var method = site.Method;
            if (method.ContainingType.ToDisplayString() == "System.Linq.Queryable")
            {
                foreach (var parameter in method.Parameters) ParameterFacts(Callable(parameter.Type,site.Symbols));
                foreach (var enclosing in invocation.Ancestors().OfType<LambdaExpressionSyntax>())
                {
                    var converted = semantic.GetTypeInfo(enclosing).ConvertedType;
                    if (converted is INamedTypeSymbol expression &&
                        expression.OriginalDefinition.ToDisplayString() == "System.Linq.Expressions.Expression<TDelegate>")
                        ParameterFacts(Callable(expression,site.Symbols));
                }
            }
            if (method.ContainingNamespace.ToDisplayString() != "SoloDatabase") continue;
            var compiled = method.Name == "Compile";
            if (compiled && (method.ContainingType.TypeArguments.Length == 0 ||
                !roots.Contains(Name(method.ContainingType.TypeArguments[0])))) continue;
            if (semantic.GetOperation(invocation) is not IInvocationOperation checkedCall) continue;
            foreach (var expressionInput in QueryExpressionInputs.Arguments(compilation,checkedCall,site.Symbols))
            {
                var callable = Callable(QueryExpressionInputs.Contract(site.Symbols.Type(expressionInput.Parameter.Type)),site.Symbols);
                if (callable?.DelegateInvokeMethod is not IMethodSymbol signature) continue;
                ParameterFacts(callable);
                foreach (var origin in expressionInput.Origins)
                {
                    var lambda = origin.Lambda;
                    semantic = compilation.GetSemanticModel(lambda.SyntaxTree);
                    var symbols = origin.Symbols;
                    string Name(ITypeSymbol type) => QueryRegistrations.Name(symbols.Type(type));
                    var sourceType = symbols.Type(compiled ? method.ContainingType.TypeArguments[0] : signature.Parameters[0].Type);
                    var source = Name(sourceType);
                    var argumentSymbols = signature.Parameters.Skip(1).Select(parameter => symbols.Type(parameter.Type)).ToArray();
                    var argumentType = argumentSymbols.Length == 0 ? compilation.GetTypeByMetadataName("Microsoft.FSharp.Core.Unit")! :
                        argumentSymbols.Length == 1 ? argumentSymbols[0] : compilation.GetTypeByMetadataName("System.ValueTuple`" + argumentSymbols.Length)!.Construct(argumentSymbols);
                    var arguments = signature.Parameters.Skip(1).Select(parameter => Name(parameter.Type)).ToArray();
                    var args = arguments.Length switch
                    {
                        0 => "global::Microsoft.FSharp.Core.Unit", 1 => arguments[0],
                        _ => "global::System.ValueTuple<" + string.Join(",", arguments) + ">"
                    };
                    foreach (var node in lambda.DescendantNodes().OfType<ExpressionSyntax>())
                    {
                        var info = semantic.GetTypeInfo(node);
                        if (info.Type != null && symbols.Type(info.Type) is IArrayTypeSymbol { Rank: > 1 } addressedArray && queryOperations.Add("arrayaddress|" + Name(addressedArray)))
                            descriptors.Append("[assembly: global::SoloDatabase.Generated.SourceQueryRegistration(\"RegisterArrayAddress\",").Append(SourceTypes(new[] { addressedArray })).Append(")]\n");
                        if (info.Type != null && symbols.Type(info.Type) is ITypeSymbol type && EnumerableElement(type) is ITypeSymbol element)
                        {
                            if (element is INamedTypeSymbol { IsAnonymousType: true } anonymous)
                                Carrier(anonymous, sourceType, argumentType);
                            else if (Writable(element))
                            {
                                if (compiled) {
                                    TypedRegistration("RegisterSequence",sourceType,element);
                                    TypedRegistration("RegisterCompiledSequence",sourceType,argumentType,element);
                                }
                            }
                        }
                        if (node is InvocationExpressionSyntax call && semantic.GetSymbolInfo(call).Symbol is IMethodSymbol originalOperation)
                        {
                            var operation = symbols.Method(originalOperation);
                            if (operation.ContainingType.ToDisplayString() == "System.Linq.Queryable" &&
                                SequenceElement(operation.ReturnType) == null && Writable(operation.ReturnType))
                            {
                                if (compiled) TypedRegistration("RegisterResult",sourceType,operation.ReturnType);
                                if (compiled) TypedRegistration("RegisterCompiledResult",sourceType,argumentType,operation.ReturnType);
                            }
                            else if (operation.ContainingType.ToDisplayString() != "System.Linq.Queryable") RegisterCall(operation);
                        }
                        if ((node is BinaryExpressionSyntax || node is PrefixUnaryExpressionSyntax || node is PostfixUnaryExpressionSyntax || node is CastExpressionSyntax) &&
                            semantic.GetSymbolInfo(node).Symbol is IMethodSymbol operatorMethod &&
                            operatorMethod.MethodKind is MethodKind.UserDefinedOperator or MethodKind.Conversion)
                            RegisterCall(symbols.Method(operatorMethod));
                        if (semantic.GetConversion(node).MethodSymbol is IMethodSymbol conversionMethod)
                            RegisterCall(symbols.Method(conversionMethod));
                        if (node is ElementAccessExpressionSyntax && semantic.GetSymbolInfo(node).Symbol is IPropertySymbol indexer && indexer.GetMethod != null)
                            RegisterCall(symbols.Method(indexer.GetMethod));
                        if (node is MemberAccessExpressionSyntax access && semantic.GetSymbolInfo(access).Symbol is ISymbol member)
                        {
                            var valueType = member is IPropertySymbol property ? symbols.Type(property.Type) : member is IFieldSymbol field ? symbols.Type(field.Type) : null;
                            var memberOwner = symbols.Type(member.ContainingType);
                            if (valueType != null && Writable(valueType) && Writable(memberOwner))
                            {
                                Operation(member is IFieldSymbol ? 0 : 1, memberOwner, member.Name, Array.Empty<ITypeSymbol>(), Array.Empty<ITypeSymbol>());
                            }
                        }
                        if (node is ObjectCreationExpressionSyntax created && semantic.GetSymbolInfo(created).Symbol is IMethodSymbol originalConstructor &&
                            symbols.Method(originalConstructor) is IMethodSymbol constructor &&
                            Writable(constructor.ContainingType) && constructor.Parameters.All(parameter => Writable(parameter.Type)))
                        {
                            if (constructor.Parameters.Any(parameter => parameter.RefKind != RefKind.None))
                            {
                                var key = "refconstructor|" + constructor.ToDisplayString(Display);
                                if (queryOperations.Add(key)) descriptors.Append("[assembly: global::SoloDatabase.Generated.SourceQueryOperationRegistration(2,")
                                    .Append(Quote(SourceTypeIdentity.Encode(constructor.ContainingType))).Append(",\".ctor\",new string[0],new string[]{")
                                    .Append(string.Join(",",constructor.Parameters.Select(parameter => Quote((parameter.RefKind == RefKind.None ? "" : "R") + SourceTypeIdentity.Encode(parameter.Type)))))
                                    .Append("})]\n");
                            }
                            else Operation(2, constructor.ContainingType, ".ctor", Array.Empty<ITypeSymbol>(), constructor.Parameters.Select(parameter => parameter.Type));
                        }
                        if (node is CastExpressionSyntax cast)
                            Conversion(semantic.GetTypeInfo(cast.Expression).Type, semantic.GetTypeInfo(cast).Type);
                        if (node is PrefixUnaryExpressionSyntax unary && semantic.GetTypeInfo(unary.Operand).ConvertedType is ITypeSymbol originalInput && info.Type != null)
                        {
                            var input = symbols.Type(originalInput);
                            var output = symbols.Type(info.Type);
                            var unaryKind = unary.Kind() switch
                            {
                                SyntaxKind.UnaryMinusExpression => "Negate", SyntaxKind.UnaryPlusExpression => "UnaryPlus",
                                SyntaxKind.LogicalNotExpression or SyntaxKind.BitwiseNotExpression => "Not", _ => null
                            };
                            var token = unary.OperatorToken.Text;
                            if (unaryKind != null && Writable(input) && Writable(output) && Accessible(input) && Accessible(output))
                            {
                                Producer(4, unaryKind, new[] { input,output }, "value => unchecked((" + Name(output) + ")(" + token + "value))");
                                if (unaryKind == "Negate")
                                    Producer(4, "NegateChecked", new[] { input,output }, "value => checked((" + Name(output) + ")(-value))");
                            }
                        }
                        if (node is LambdaExpressionSyntax && Callable(info.ConvertedType,symbols) is INamedTypeSymbol delegateType)
                        {
                            if (delegateType.DelegateInvokeMethod is IMethodSymbol invoke && !invoke.ReturnsVoid && Writable(invoke.ReturnType) &&
                                invoke.Parameters.All(parameter => parameter.RefKind == RefKind.None && Writable(parameter.Type)))
                            {
                                if (!Accessible(delegateType)) TypedRegistration("RegisterLambda",new[] { (ITypeSymbol)delegateType }.Concat(invoke.Parameters.Select(parameter => parameter.Type)).ToArray());
                                else
                                {
                                    var names = invoke.Parameters.Select((_, index) => "arg" + index).ToArray();
                                    var assignments = string.Join("",names.Select((name,index) => "values[offset+" + index + "]=" + name + ";"));
                                    Producer(6, "RegisterLambda", new[] { delegateType },
                                        "(offset,size,run) => new " + Name(delegateType) + "((" + string.Join(",",names) +
                                        ") => { var values = new object[size];" + assignments + "return (" + Name(invoke.ReturnType) + ")run.Invoke(values); })");
                                }
                            }
                        }
                        if (node is BinaryExpressionSyntax binary && BinaryKind(binary.Kind()) is string kind)
                        {
                            var originalLeft = semantic.GetTypeInfo(binary.Left).ConvertedType;
                            var originalRight = semantic.GetTypeInfo(binary.Right).ConvertedType;
                            var left = originalLeft == null ? null : symbols.Type(originalLeft);
                            var right = originalRight == null ? null : symbols.Type(originalRight);
                            var result = info.Type == null ? null : symbols.Type(info.Type);
                            if (left != null && right != null && result != null && Writable(left) && Writable(right) && Writable(result) && Accessible(left) && Accessible(right) && Accessible(result))
                            {
                                Producer(5, kind, new[] { left,right,result }, "(left,right) => unchecked(left " + binary.OperatorToken.Text + " right)");
                                if (kind is "Add" or "Subtract" or "Multiply")
                                    Producer(5, kind + "Checked", new[] { left,right,result }, "(left,right) => checked(left " + binary.OperatorToken.Text + " right)");
                            }
                        }
                        Conversion(info.Type, info.ConvertedType);
                    }

                    void Conversion(ITypeSymbol? input, ITypeSymbol? output)
                    {
                        if (input == null || output == null) return;
                        input = symbols.Type(input); output = symbols.Type(output);
                        if (SymbolEqualityComparer.Default.Equals(input, output) || !Writable(input) || !Writable(output) || !Accessible(input) || !Accessible(output) ||
                            input.TypeKind == TypeKind.Interface || output.TypeKind == TypeKind.Interface || SequenceElement(input) != null) return;
                        Producer(4, "Convert", new[] { input,output }, "value => unchecked((" + Name(output) + ")value)");
                        Producer(4, "ConvertChecked", new[] { input,output }, "value => checked((" + Name(output) + ")value)");
                    }
                    void RegisterCall(IMethodSymbol operation)
                    {
                        operation = operation.ReducedFrom == null ? operation : operation.IsGenericMethod ?
                            operation.ReducedFrom.Construct(operation.TypeArguments.ToArray()) : operation.ReducedFrom;
                        if (operation.ReturnsByRef || operation.ReturnsByRefReadonly || operation.ReturnType.IsRefLikeType || operation.Parameters.Any(parameter => parameter.Type.IsRefLikeType)) return;
                        if (operation.ReturnsVoid || operation.Parameters.Any(parameter => parameter.RefKind != RefKind.None))
                        {
                            var key = "refcall|" + operation.ToDisplayString(Display);
                            if (!queryOperations.Add(key)) return;
                            descriptors.Append("[assembly: global::SoloDatabase.Generated.SourceQueryOperationRegistration(3,")
                                .Append(Quote(SourceTypeIdentity.Encode(operation.ContainingType))).Append(',').Append(Quote(operation.MetadataName)).Append(',')
                                .Append(SourceTypes(operation.TypeArguments)).Append(",new string[]{")
                                .Append(string.Join(",",operation.Parameters.Select(parameter => Quote((parameter.RefKind == RefKind.None ? "" : "R") + SourceTypeIdentity.Encode(parameter.Type)))))
                                .Append("})]\n");
                            return;
                        }
                        Operation(3, operation.ContainingType, operation.MetadataName, operation.TypeArguments, operation.Parameters.Select(parameter => parameter.Type));
                    }
                }
            }
        }
        foreach (var operation in Providers(sites,roots))
        {
            if (operation.Result is INamedTypeSymbol { IsAnonymousType: true } anonymous)
                Carrier(anonymous,operation.Source,null,operation.Sequence);
            else if (Writable(operation.Source) && Writable(operation.Result))
                TypedRegistration(operation.Sequence ? "RegisterSequence" : "RegisterResult",operation.Source,operation.Result);
        }
        return new Code(helpers.ToString(), descriptors.ToString());

        void Carrier(INamedTypeSymbol type, ITypeSymbol sourceType, ITypeSymbol? argumentType, bool sequence = true)
        {
            var source = Name(sourceType); var args = argumentType == null ? null : Name(argumentType);
            var key = source + "|" + args + "|" + sequence + "|" + type.ToDisplayString();
            if (!carriers.Add(key)) return;
            foreach (var property in type.GetMembers().OfType<IPropertySymbol>())
                if (property.Type is INamedTypeSymbol { IsAnonymousType: true } nested) Carrier(nested, sourceType, argumentType);
            TypedRegistration(argumentType == null ? (sequence ? "RegisterSequence" : "RegisterResult") : "RegisterCompiledSequence",
                argumentType == null ? new ITypeSymbol[] { sourceType,type } : new ITypeSymbol[] { sourceType,argumentType,type });
            descriptors.Append("[assembly: global::SoloDatabase.Generated.SourceQueryRegistration(\"RegisterAnonymous\",").Append(SourceTypes(new[] { type })).Append(")]\n");
        }

    }

    internal sealed class Site
    {
        internal Site(InvocationExpressionSyntax invocation, IMethodSymbol method, Symbols symbols)
        { Invocation = invocation; Method = method; Symbols = symbols; }
        internal InvocationExpressionSyntax Invocation { get; }
        internal IMethodSymbol Method { get; }
        internal Symbols Symbols { get; }
    }

    // Substitutions come from checked use sites, including forwarding helpers.
    internal sealed class Symbols
    {
        private readonly Compilation compilation;
        private readonly Dictionary<IParameterSymbol,QueryExpressionInputs.Origin[]> inputs = new(SymbolEqualityComparer.Default);
        private readonly Dictionary<ITypeParameterSymbol,ITypeSymbol> parameters = new(SymbolEqualityComparer.Default);
        internal Symbols(Compilation compilation, IMethodSymbol? method = null, Symbols? enclosing = null)
        {
            this.compilation = compilation;
            if (enclosing != null)
            {
                foreach (var pair in enclosing.parameters) parameters.Add(pair.Key,pair.Value);
                foreach (var pair in enclosing.inputs) inputs.Add(pair.Key,pair.Value);
            }
            if (method == null) return;
            for (var index = 0; index < method.TypeArguments.Length; index++)
                parameters[method.OriginalDefinition.TypeParameters[index]] = method.TypeArguments[index];
            for (var owner = method.ContainingType; owner != null; owner = owner.ContainingType)
                for (var index = 0; index < owner.TypeArguments.Length; index++)
                    parameters[owner.OriginalDefinition.TypeParameters[index]] = owner.TypeArguments[index];
        }
        internal ITypeSymbol Type(ITypeSymbol type)
        {
            if (type is ITypeParameterSymbol parameter)
                return parameters.TryGetValue(parameter, out var actual) ? actual : type;
            if (type is IArrayTypeSymbol array)
                return compilation.CreateArrayTypeSymbol(Type(array.ElementType), array.Rank);
            if (type is not INamedTypeSymbol named) return type;
            if (named.IsAnonymousType)
            {
                var properties = named.GetMembers().OfType<IPropertySymbol>().ToArray();
                return compilation.CreateAnonymousTypeSymbol(properties.Select(property => Type(property.Type)).ToImmutableArray(),
                    properties.Select(property => property.Name).ToImmutableArray());
            }
            var root = named.OriginalDefinition;
            if (named.ContainingType != null)
            {
                var owner = (INamedTypeSymbol)Type(named.ContainingType);
                root = owner.GetTypeMembers(named.Name, named.Arity).Single(candidate =>
                    SymbolEqualityComparer.Default.Equals(candidate.OriginalDefinition, named.OriginalDefinition));
            }
            return named.TypeArguments.Length == 0 ? root : root.Construct(named.TypeArguments.Select(Type).ToArray());
        }
        internal QueryExpressionInputs.Origin[] Input(IParameterSymbol parameter) =>
            inputs.TryGetValue(parameter.OriginalDefinition,out var origins) ? origins : Array.Empty<QueryExpressionInputs.Origin>();
        internal void BindInput(IParameterSymbol parameter, QueryExpressionInputs.Origin[] origins) =>
            inputs[parameter.OriginalDefinition] = origins;
        internal bool Same(Symbols other) => SameTypes(other) && inputs.Count == other.inputs.Count && inputs.All(pair =>
            other.inputs.TryGetValue(pair.Key,out var values) && pair.Value.Length == values.Length &&
            pair.Value.Zip(values,(left,right) => left.Lambda.SyntaxTree == right.Lambda.SyntaxTree && left.Lambda.Span == right.Lambda.Span &&
                left.Symbols.SameTypes(right.Symbols)).All(equal => equal));
        private bool SameTypes(Symbols other) => parameters.Count == other.parameters.Count && parameters.All(pair =>
            other.parameters.TryGetValue(pair.Key,out var actual) && SymbolEqualityComparer.Default.Equals(pair.Value,actual));
        internal IMethodSymbol Method(IMethodSymbol method)
        {
            if (method.MethodKind == MethodKind.LocalFunction)
                return method.TypeArguments.Length == 0 ? method : method.ConstructedFrom.Construct(method.TypeArguments.Select(Type).ToArray());
            if (method.ReducedFrom != null)
                method = method.IsGenericMethod ? method.ReducedFrom.Construct(method.TypeArguments.ToArray()) : method.ReducedFrom;
            var owner = (INamedTypeSymbol)Type(method.ContainingType);
            var candidates = method.AssociatedSymbol is IPropertySymbol property
                ? owner.GetMembers(property.Name).OfType<IPropertySymbol>().SelectMany(candidate => new[] { candidate.GetMethod,candidate.SetMethod }).OfType<IMethodSymbol>()
                : owner.GetMembers(method.Name).OfType<IMethodSymbol>();
            var rebound = candidates.Single(candidate =>
                SymbolEqualityComparer.Default.Equals(candidate.OriginalDefinition, method.OriginalDefinition));
            return method.TypeArguments.Length == 0 ? rebound : rebound.Construct(method.TypeArguments.Select(Type).ToArray());
        }
    }

    internal static IReadOnlyList<Site> Analyze(Compilation compilation, Action<Location,string> missing)
    {
        var sites = new List<Site>();
        var unresolved = new Dictionary<InvocationExpressionSyntax,IMethodSymbol>();
        var relevant = new HashSet<IMethodSymbol>(SymbolEqualityComparer.Default);
        var callers = new Dictionary<IMethodSymbol,HashSet<IMethodSymbol>>(SymbolEqualityComparer.Default);
        foreach (var tree in compilation.SyntaxTrees)
        {
            var semantic = compilation.GetSemanticModel(tree);
            foreach (var call in tree.GetRoot().DescendantNodes().OfType<InvocationExpressionSyntax>())
            {
                if (semantic.GetSymbolInfo(call).Symbol is not IMethodSymbol target) continue;
                var declaration = call.Ancestors().FirstOrDefault(node => node is BaseMethodDeclarationSyntax or LocalFunctionStatementSyntax);
                if (declaration == null || semantic.GetDeclaredSymbol(declaration) is not IMethodSymbol parent) continue;
                parent = parent.OriginalDefinition;
                if (IsSoloDBUse(target)) relevant.Add(parent);
                else
                {
                    target = (target.ReducedFrom ?? target).OriginalDefinition;
                    if (target.DeclaringSyntaxReferences.Length == 0) continue;
                    if (!callers.TryGetValue(target,out var values)) callers.Add(target,values = new HashSet<IMethodSymbol>(SymbolEqualityComparer.Default));
                    values.Add(parent);
                }
            }
        }
        var pending = new Queue<IMethodSymbol>(relevant);
        while (pending.Count != 0)
            if (callers.TryGetValue(pending.Dequeue(),out var values))
                foreach (var caller in values) if (relevant.Add(caller)) pending.Enqueue(caller);
        var visited = new Dictionary<IMethodSymbol,List<Symbols>>(SymbolEqualityComparer.Default);
        var empty = new Symbols(compilation);
        foreach (var tree in compilation.SyntaxTrees)
            foreach (var invocation in tree.GetRoot().DescendantNodes().OfType<InvocationExpressionSyntax>()) Inspect(invocation, empty);
        var resolved = new HashSet<InvocationExpressionSyntax>(sites.Select(site => site.Invocation));
        foreach (var item in unresolved)
            if (!resolved.Contains(item.Key)) missing(item.Key.GetLocation(),item.Value.ToDisplayString());
        return sites;

        void Inspect(InvocationExpressionSyntax invocation, Symbols symbols)
        {
            var semantic = compilation.GetSemanticModel(invocation.SyntaxTree);
            if (semantic.GetSymbolInfo(invocation).Symbol is not IMethodSymbol original ||
                !IsSoloDBUse(original) && !relevant.Contains((original.ReducedFrom ?? original).OriginalDefinition)) return;
            var method = symbols.Method(original);
            if (IsSoloDBUse(method))
            {
                if (method.ContainingType.TypeArguments.All(Closed) && method.TypeArguments.All(Closed))
                    sites.Add(new Site(invocation,method,symbols));
                else unresolved[invocation] = method;
                return;
            }
            if (method.DeclaringSyntaxReferences.Length == 0 ||
                !method.TypeArguments.All(Closed) || !method.ContainingType.TypeArguments.All(Closed)) return;
            var boundInputs = semantic.GetOperation(invocation) is IInvocationOperation checkedCall
                ? QueryExpressionInputs.Arguments(compilation,checkedCall,symbols).ToArray()
                : Array.Empty<(IParameterSymbol Parameter, QueryExpressionInputs.Origin[] Origins)>();
            if (method.MethodKind != MethodKind.LocalFunction && !method.IsGenericMethod && !method.ContainingType.IsGenericType &&
                boundInputs.All(input => input.Origins.Length == 0)) return;
            var actual = new Symbols(compilation,method,symbols);
            foreach (var input in boundInputs) actual.BindInput(input.Parameter,input.Origins);
            if (!visited.TryGetValue(method,out var contexts)) visited.Add(method,contexts = new List<Symbols>());
            if (contexts.Any(previous => previous.Same(actual))) return;
            contexts.Add(actual);
            foreach (var declaration in method.DeclaringSyntaxReferences)
            {
                var root = declaration.GetSyntax();
                foreach (var call in root.DescendantNodes(node => ReferenceEquals(node,root) || node is not LocalFunctionStatementSyntax)
                    .OfType<InvocationExpressionSyntax>()) Inspect(call,actual);
            }
        }
        static bool IsSoloDBUse(IMethodSymbol method) =>
            (method.ReducedFrom ?? method).ContainingType.ToDisplayString() == "System.Linq.Queryable" ||
            (method.ReducedFrom ?? method).ContainingAssembly.Name == "SoloDB" &&
            method.ContainingNamespace.ToDisplayString().StartsWith("SoloDatabase",StringComparison.Ordinal) &&
            !method.ContainingType.Name.Contains("Metadata");
    }

    private static bool HasAnonymous(ITypeSymbol type) =>
        type is INamedTypeSymbol named && (named.IsAnonymousType || named.TypeArguments.Any(HasAnonymous)) ||
        type is IArrayTypeSymbol array && HasAnonymous(array.ElementType);

    private static bool Closed(ITypeSymbol type) => type is not ITypeParameterSymbol &&
            (type is not INamedTypeSymbol named || named.TypeArguments.All(Closed) && (named.ContainingType == null || Closed(named.ContainingType))) &&
            (type is not IArrayTypeSymbol array || Closed(array.ElementType));

    private static ITypeSymbol? SequenceElement(ITypeSymbol type) =>
        type is INamedTypeSymbol named && named.ContainingNamespace.ToDisplayString() == "System.Linq" &&
        named.Name is "IQueryable" or "IOrderedQueryable" && named.TypeArguments.Length == 1 ? named.TypeArguments[0] : null;
    private static ITypeSymbol? EnumerableElement(ITypeSymbol type) =>
        type is IArrayTypeSymbol array ? array.ElementType :
        type is INamedTypeSymbol named && named.SpecialType != SpecialType.System_String ?
            new[] { named }.Concat(named.AllInterfaces).FirstOrDefault(contract =>
                contract.OriginalDefinition.SpecialType == SpecialType.System_Collections_Generic_IEnumerable_T)?.TypeArguments[0] : null;
    private static string? BinaryKind(SyntaxKind kind) => kind switch
    {
        SyntaxKind.AddExpression => "Add", SyntaxKind.SubtractExpression => "Subtract", SyntaxKind.MultiplyExpression => "Multiply",
        SyntaxKind.DivideExpression => "Divide", SyntaxKind.ModuloExpression => "Modulo", SyntaxKind.EqualsExpression => "Equal",
        SyntaxKind.NotEqualsExpression => "NotEqual", SyntaxKind.LessThanExpression => "LessThan", SyntaxKind.LessThanOrEqualExpression => "LessThanOrEqual",
        SyntaxKind.GreaterThanExpression => "GreaterThan", SyntaxKind.GreaterThanOrEqualExpression => "GreaterThanOrEqual",
        SyntaxKind.BitwiseAndExpression => "And", SyntaxKind.BitwiseOrExpression => "Or", SyntaxKind.ExclusiveOrExpression => "ExclusiveOr",
        SyntaxKind.LeftShiftExpression => "LeftShift", SyntaxKind.RightShiftExpression => "RightShift", _ => null
    };
    private static void CollectAnonymous(ITypeSymbol type, List<INamedTypeSymbol> values)
    {
        if (type is IArrayTypeSymbol array) CollectAnonymous(array.ElementType, values);
        else if (type is INamedTypeSymbol named)
        {
            if (named.IsAnonymousType)
            {
                if (values.Any(value => SymbolEqualityComparer.Default.Equals(value, named))) return;
                values.Add(named);
                foreach (var property in named.GetMembers().OfType<IPropertySymbol>()) CollectAnonymous(property.Type, values);
            }
            foreach (var argument in named.TypeArguments) CollectAnonymous(argument, values);
        }
    }
    private static bool Writable(ITypeSymbol type)
    {
        var anonymous = new List<INamedTypeSymbol>();
        CollectAnonymous(type, anonymous);
        return anonymous.Count == 0;
    }
    private static string Name(ITypeSymbol type) => type.ToDisplayString(Display);
    private static string Quote(string value) => Microsoft.CodeAnalysis.CSharp.SymbolDisplay.FormatLiteral(value, true);
    private static string Types(IEnumerable<ITypeSymbol> types) => "new global::System.Type[] {" + string.Join(",", types.Select(type => "typeof(" + Name(type) + ")")) + "}";
    private static string Arguments(IEnumerable<IParameterSymbol> parameters) =>
        string.Join(",", parameters.Select((parameter, index) => "(" + Name(parameter.Type) + ")values[" + index + "]"));
}
