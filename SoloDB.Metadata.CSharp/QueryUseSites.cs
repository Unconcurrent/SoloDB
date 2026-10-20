using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Operations;
using SoloDatabase.Metadata.Generation;

namespace SoloDatabase.Metadata.CSharp;

// Checked use sites and their original source substitutions.
internal static partial class QueryRegistrations
{
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
            BindOwner(method.ContainingType);
        }
        private void BindOwner(INamedTypeSymbol owner)
        {
            for (INamedTypeSymbol? current = owner; current != null; current = current.ContainingType)
                for (var index = 0; index < current.TypeArguments.Length; index++)
                    parameters[current.OriginalDefinition.TypeParameters[index]] = Type(current.TypeArguments[index]);
        }
        internal Symbols WithOwner(INamedTypeSymbol owner)
        {
            var actual = new Symbols(compilation,null,this);
            actual.BindOwner((INamedTypeSymbol)Type(owner));
            return actual;
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
            pair.Value.Zip(values,(left,right) => left.Syntax.SyntaxTree == right.Syntax.SyntaxTree && left.Syntax.Span == right.Syntax.Span &&
                SymbolEqualityComparer.Default.Equals(left.Type,right.Type) && left.Symbols.SameTypes(right.Symbols)).All(equal => equal));
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

    private static bool RawParameter(IMethodSymbol method,IParameterSymbol parameter)
    {
        method = method.ReducedFrom ?? method;
        return TypeSupport.SqlArgumentParameter(method.ContainingAssembly.Name,method.ContainingType.ToDisplayString(),parameter.Name,
            parameter.Type.SpecialType == SpecialType.System_Object ? "System.Object" : parameter.Type.ToDisplayString());
    }

    internal static IReadOnlyList<Site> Analyze(Compilation compilation, Action<Location,string> missing)
    {
        var sites = new List<Site>();
        var unresolved = new Dictionary<InvocationExpressionSyntax,IMethodSymbol>();
        var relevant = new HashSet<ISymbol>(SymbolEqualityComparer.Default);
        var rawInputs = new HashSet<ISymbol>(SymbolEqualityComparer.Default);
        var callers = new Dictionary<ISymbol,HashSet<ISymbol>>(SymbolEqualityComparer.Default);
        var ownerCalls = new Dictionary<INamedTypeSymbol,List<SyntaxNode>>(SymbolEqualityComparer.Default);
        foreach (var tree in compilation.SyntaxTrees)
        {
            var semantic = compilation.GetSemanticModel(tree);
            foreach (var call in Calls(tree.GetRoot()))
            {
                if (semantic.GetSymbolInfo(call).Symbol is not IMethodSymbol target) continue;
                var owner = semantic.GetEnclosingSymbol(call.SpanStart)?.ContainingType?.OriginalDefinition;
                if (owner != null)
                {
                    if (!ownerCalls.TryGetValue(owner,out var owned)) ownerCalls.Add(owner,owned = new List<SyntaxNode>());
                    owned.Add(call);
                }
                if (SourceConstructor(target))
                {
                    for (var containing = target.ContainingType; containing != null; containing = containing.ContainingType)
                        for (var current = containing; current != null; current = current.BaseType)
                            AddCaller(current.OriginalDefinition,target.OriginalDefinition);
                }
                var declaration = call.Ancestors().FirstOrDefault(node => node is BaseMethodDeclarationSyntax or LocalFunctionStatementSyntax);
                ISymbol? parent = declaration != null && semantic.GetDeclaredSymbol(declaration) is IMethodSymbol method
                    ? method.OriginalDefinition : owner;
                if (parent == null) continue;
                if (parent is IMethodSymbol && owner != null) AddCaller(parent,owner);
                if (call is InvocationExpressionSyntax && IsSoloDBUse(target))
                {
                    relevant.Add(parent);
                    if ((target.ReducedFrom ?? target).Parameters.Any(parameter => RawParameter(target,parameter))) rawInputs.Add(parent);
                }
                else
                {
                    target = (target.ReducedFrom ?? target).OriginalDefinition;
                    if (target.DeclaringSyntaxReferences.Length == 0 && !SourceConstructor(target)) continue;
                    AddCaller(target,parent);
                }
            }
        }
        foreach (var selected in new[] { relevant,rawInputs })
        {
            var pending = new Queue<ISymbol>(selected);
            while (pending.Count != 0)
                if (callers.TryGetValue(pending.Dequeue(),out var values))
                    foreach (var caller in values) if (selected.Add(caller)) pending.Enqueue(caller);
        }
        var visited = new Dictionary<ISymbol,List<Symbols>>(SymbolEqualityComparer.Default);
        var empty = new Symbols(compilation);
        foreach (var tree in compilation.SyntaxTrees)
            foreach (var invocation in Calls(tree.GetRoot())) Inspect(invocation, empty);
        var resolved = new HashSet<InvocationExpressionSyntax>(sites.Select(site => site.Invocation));
        foreach (var item in unresolved)
            if (!resolved.Contains(item.Key)) missing(item.Key.GetLocation(),item.Value.ToDisplayString());
        return sites;

        void AddCaller(ISymbol target, ISymbol caller)
        {
            if (!callers.TryGetValue(target,out var values)) callers.Add(target,values = new HashSet<ISymbol>(SymbolEqualityComparer.Default));
            values.Add(caller);
        }

        void Inspect(SyntaxNode invocation, Symbols symbols)
        {
            var semantic = compilation.GetSemanticModel(invocation.SyntaxTree);
            if (semantic.GetSymbolInfo(invocation).Symbol is not IMethodSymbol original ||
                !(invocation is InvocationExpressionSyntax && IsSoloDBUse(original)) &&
                !relevant.Contains((original.ReducedFrom ?? original).OriginalDefinition)) return;
            var method = symbols.Method(original);
            if (invocation is InvocationExpressionSyntax use && IsSoloDBUse(method))
            {
                if (method.ContainingType.TypeArguments.All(Closed) && method.TypeArguments.All(Closed))
                    sites.Add(new Site(use,method,symbols));
                else unresolved[use] = method;
                return;
            }
            var construction = SourceConstructor(method);
            if (!construction && method.DeclaringSyntaxReferences.Length == 0 ||
                !method.TypeArguments.All(Closed) || !Closed(method.ContainingType)) return;
            var arguments = semantic.GetOperation(invocation) switch
            {
                IInvocationOperation checkedCall => checkedCall.Arguments,
                IObjectCreationOperation checkedCreation => checkedCreation.Arguments,
                _ => ImmutableArray<IArgumentOperation>.Empty
            };
            var boundInputs = QueryExpressionInputs.Arguments(compilation,arguments,symbols,
                rawInputs.Contains(method.OriginalDefinition)).ToArray();
            if (method.MethodKind != MethodKind.LocalFunction && !method.IsGenericMethod && !method.ContainingType.IsGenericType &&
                boundInputs.All(input => input.Origins.Length == 0)) return;
            var actual = new Symbols(compilation,method,symbols);
            foreach (var input in boundInputs) actual.BindInput(input.Parameter,input.Origins);
            if (!visited.TryGetValue(method,out var contexts)) visited.Add(method,contexts = new List<Symbols>());
            if (contexts.Any(previous => previous.Same(actual))) return;
            contexts.Add(actual);
            if (construction)
            {
                InspectOwner(method.ContainingType,actual);
                return;
            }
            foreach (var declaration in method.DeclaringSyntaxReferences)
            {
                var root = declaration.GetSyntax();
                foreach (var call in Calls(root,node => ReferenceEquals(node,root) || node is not LocalFunctionStatementSyntax)) Inspect(call,actual);
            }
        }

        void InspectOwner(INamedTypeSymbol owner, Symbols symbols)
        {
            for (var containing = owner; containing != null; containing = containing.ContainingType)
                for (var current = containing; current != null; current = current.BaseType)
                {
                    if (!ownerCalls.TryGetValue(current.OriginalDefinition,out var calls)) continue;
                    var actual = SymbolEqualityComparer.Default.Equals(current,owner)
                        ? symbols.WithOwner(current)
                        : new Symbols(compilation).WithOwner((INamedTypeSymbol)symbols.Type(current));
                    if (!visited.TryGetValue(current,out var contexts)) visited.Add(current,contexts = new List<Symbols>());
                    if (contexts.Any(previous => previous.Same(actual))) continue;
                    contexts.Add(actual);
                    foreach (var call in calls) Inspect(call,actual);
                }
        }
        static bool SourceConstructor(IMethodSymbol method) => method.MethodKind == MethodKind.Constructor &&
            method.ContainingType.DeclaringSyntaxReferences.Length != 0;
        static IEnumerable<SyntaxNode> Calls(SyntaxNode root, Func<SyntaxNode,bool>? descend = null) =>
            root.DescendantNodes(descend).Where(node => node is InvocationExpressionSyntax or BaseObjectCreationExpressionSyntax or ConstructorInitializerSyntax or PrimaryConstructorBaseTypeSyntax);
        static bool IsSoloDBUse(IMethodSymbol method) =>
            (method.ReducedFrom ?? method).ContainingType.ToDisplayString() == "System.Linq.Queryable" ||
            (method.ReducedFrom ?? method).ContainingAssembly.Name == "SoloDB" &&
            method.ContainingNamespace.ToDisplayString().StartsWith("SoloDatabase",StringComparison.Ordinal) &&
            !method.ContainingType.Name.Contains("Metadata");
    }

}
