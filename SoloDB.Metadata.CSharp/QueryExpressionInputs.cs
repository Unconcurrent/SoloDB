using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Operations;

namespace SoloDatabase.Metadata.CSharp;

// Compiler argument bindings delimit the authored expression inputs. No input is evaluated.
internal static class QueryExpressionInputs
{
    internal sealed class Origin
    {
        internal Origin(LambdaExpressionSyntax lambda, QueryRegistrations.Symbols symbols)
        { Syntax = lambda; Symbols = symbols; }
        internal Origin(SyntaxNode syntax, QueryRegistrations.Symbols symbols, ITypeSymbol type)
        { Syntax = syntax; Symbols = symbols; Type = type; }
        internal SyntaxNode Syntax { get; }
        internal LambdaExpressionSyntax Lambda => (LambdaExpressionSyntax)Syntax;
        internal ITypeSymbol? Type { get; }
        internal QueryRegistrations.Symbols Symbols { get; }
    }

    internal static INamedTypeSymbol? Contract(ITypeSymbol type)
    {
        if (type is IArrayTypeSymbol { Rank: 1 } array) type = array.ElementType;
        return type is INamedTypeSymbol named &&
            named.OriginalDefinition.ToDisplayString() == "System.Linq.Expressions.Expression<TDelegate>" ? named : null;
    }

    private static bool SourceInput(ITypeSymbol type) => type.SpecialType == SpecialType.System_Object ||
        type.TypeKind == TypeKind.Interface || type is INamedTypeSymbol { IsReferenceType: true, IsSealed: false } ||
        type is IArrayTypeSymbol { ElementType.IsReferenceType: true };

    private static Origin? KnownSource(ITypeSymbol? type, SyntaxNode syntax, QueryRegistrations.Symbols symbols) =>
        type != null && symbols.Type(type) is ITypeSymbol actual && actual.SpecialType != SpecialType.System_Object &&
        actual.TypeKind != TypeKind.Dynamic && actual is not INamedTypeSymbol { IsAbstract: true }
            ? new Origin(syntax,symbols,actual) : null;

    private sealed class Visits : HashSet<ISymbol>
    {
        internal Visits() : base(SymbolEqualityComparer.Default) { }
        internal readonly Dictionary<ISymbol, Origin[]> Bindings = new(SymbolEqualityComparer.Default);
    }

    private static IEnumerable<(IParameterSymbol Parameter, IOperation Value)> SelectedInputs(IEnumerable<IArgumentOperation> arguments) =>
        arguments.Where(argument => argument.Parameter != null).Select(argument => (argument.Parameter!,argument.Value));

    internal static IEnumerable<(IParameterSymbol Parameter, Origin[] Origins)> Arguments(
        Compilation compilation, IInvocationOperation invocation, QueryRegistrations.Symbols symbols, bool rawSources = false)
    {
        foreach (var argument in invocation.Arguments)
            if (argument.Parameter is IParameterSymbol parameter)
            {
                if (Contract(symbols.Type(parameter.Type)) != null)
                    yield return (parameter, Read(compilation, argument.Value, symbols,
                        new Visits()).ToArray());
                else if (rawSources && SourceInput(symbols.Type(parameter.Type)))
                    yield return (parameter, Sources(compilation,argument.Value,symbols));
            }
    }

    internal static Origin[] Sources(Compilation compilation, IOperation value, QueryRegistrations.Symbols symbols) =>
        Read(compilation,value,symbols,new Visits(),true).ToArray();

    private static IEnumerable<Origin> Read(Compilation compilation, IOperation? value,
        QueryRegistrations.Symbols symbols, Visits visited, bool source = false, bool stored = false)
    {
        var known = source && value != null && value is not IParenthesizedOperation &&
            (value is not IConversionOperation conversionValue || conversionValue.OperatorMethod != null)
                ? KnownSource(value.Type,value.Syntax,symbols) : null;
        if (known != null && value is IArrayCreationOperation or ICollectionExpressionOperation) return new[] { known };
        var origins = ReadOrigins(compilation,value,symbols,visited,source,stored).ToArray();
        return origins.Length != 0 || known == null ? origins : new[] { known };
    }

    private static IEnumerable<Origin> ReadOrigins(Compilation compilation, IOperation? value,
        QueryRegistrations.Symbols symbols, Visits visited, bool source, bool stored)
    {
        switch (value)
        {
            case IConversionOperation conversion when conversion.OperatorMethod == null:
                return Read(compilation, conversion.Operand, symbols, visited,source);
            case IConversionOperation { OperatorMethod: IMethodSymbol operation } conversion when source:
                return Produced(operation, Operands(operation,conversion.Operand),conversion.Syntax);
            case IUnaryOperation { OperatorMethod: IMethodSymbol operation } unary when source:
                return Produced(operation, Operands(operation,unary.Operand),unary.Syntax);
            case IBinaryOperation { OperatorMethod: IMethodSymbol operation } binary when source:
                return Produced(operation, Operands(operation,binary.LeftOperand,binary.RightOperand),binary.Syntax);
            case IAssignmentOperation assignment when source:
                return WriteResult(assignment,stored);
            case IIncrementOrDecrementOperation increment when source:
                return WriteResult(increment,stored);
            case IParenthesizedOperation parenthesized:
                return Read(compilation, parenthesized.Operand, symbols, visited,source);
            case IDelegateCreationOperation creation:
                return Read(compilation, creation.Target, symbols, visited,source);
            case IAnonymousFunctionOperation { Syntax: LambdaExpressionSyntax lambda }:
                return source ? Array.Empty<Origin>() : new[] { new Origin(lambda, symbols) };
            case IArrayCreationOperation array:
                return Read(compilation, array.Initializer, symbols, visited,source);
            case IArrayInitializerOperation initializer:
                return initializer.ElementValues.SelectMany(element => Read(compilation, element, symbols, visited,source)).ToArray();
            case ICollectionExpressionOperation collection:
                return collection.Elements.SelectMany(element => Read(compilation, element, symbols, visited,source)).ToArray();
            case ISpreadOperation spread:
                return Read(compilation, spread.Operand, symbols, visited,source);
            case IConditionalOperation conditional:
                return Read(compilation, conditional.WhenTrue, symbols, visited,source)
                    .Concat(Read(compilation, conditional.WhenFalse, symbols, visited,source)).ToArray();
            case ICoalesceOperation coalesce:
                return Read(compilation, coalesce.Value, symbols, visited,source)
                    .Concat(Read(compilation, coalesce.WhenNull, symbols, visited,source)).ToArray();
            case IParameterReferenceOperation parameter:
                return symbols.Input(parameter.Parameter).Where(origin => source ? origin.Type != null : origin.Type == null)
                    .Concat(source ? Binding(parameter.Parameter,symbols) : Array.Empty<Origin>()).ToArray();
            case ILocalReferenceOperation local:
                return Binding(local.Local, symbols);
            case IFieldReferenceOperation field:
                return Binding(field.Field, symbols.WithOwner(field.Field.ContainingType));
            case IPropertyReferenceOperation property when property.Property.GetMethod is IMethodSymbol getter:
                return Returns(getter, Inputs(SelectedInputs(property.Arguments)));
            case IInvocationOperation call when source || Contract(symbols.Type(call.Type!)) != null:
                return Returns(call.TargetMethod, Inputs(SelectedInputs(call.Arguments)));
            default:
                return Array.Empty<Origin>();
        }

        ISymbol? TargetSymbol(IOperation target) => target switch
        {
            ILocalReferenceOperation local => local.Local,
            IParameterReferenceOperation parameter => parameter.Parameter,
            IFieldReferenceOperation field => field.Field,
            IPropertyReferenceOperation property => property.Property,
            _ => null
        };

        IEnumerable<(IParameterSymbol Parameter, Origin[] Origins)> Inputs(IEnumerable<(IParameterSymbol Parameter, IOperation Value)> values) =>
            values.Where(input => Contract(symbols.Type(input.Parameter.Type)) != null || source && SourceInput(symbols.Type(input.Parameter.Type)))
                .Select(input => (input.Parameter,Read(compilation,input.Value,symbols,visited,source).ToArray()));

        IEnumerable<(IParameterSymbol Parameter, Origin[] Origins)> Operands(IMethodSymbol method, params IOperation[] operands) =>
            Inputs(method.Parameters.Zip(operands,(parameter,operand) => (parameter,operand)));

        Origin[] Known(ITypeSymbol? type, SyntaxNode syntax) => KnownSource(type,syntax,symbols) is Origin fact
            ? new[] { fact } : Array.Empty<Origin>();

        Origin[] Produced(IMethodSymbol method, IEnumerable<(IParameterSymbol Parameter, Origin[] Origins)> inputs, SyntaxNode syntax)
        {
            var result = Returns(method,inputs).ToArray();
            return result.Length != 0 ? result : Known(method.ReturnType,syntax);
        }

        Origin[] Converted(CommonConversion conversion, Origin[] input, SyntaxNode syntax) => conversion.MethodSymbol is IMethodSymbol method
            ? Produced(method,new[] { (method.Parameters[0],input) },syntax) : input;

        Origin[] WriteResult(IOperation write, bool stored)
        {
            switch (write)
            {
                case ICompoundAssignmentOperation compound:
                    var left = Converted(compound.InConversion,Read(compilation,compound.Target,symbols,visited,true).ToArray(),compound.Syntax);
                    var right = Read(compilation,compound.Value,symbols,visited,true).ToArray();
                    var computed = compound.OperatorMethod is IMethodSymbol operation
                        ? Produced(operation,new[] { (operation.Parameters[0],left),(operation.Parameters[1],right) },compound.Syntax)
                        : Known(compound.OutConversion.MethodSymbol?.Parameters[0].Type ?? compound.Type,compound.Syntax);
                    return Converted(compound.OutConversion,computed,compound.Syntax);
                case ICoalesceAssignmentOperation coalesce:
                    return Read(compilation,coalesce.Target,symbols,visited,true)
                        .Concat(Read(compilation,coalesce.Value,symbols,visited,true)).ToArray();
                case IAssignmentOperation assignment:
                    return Read(compilation,assignment.Value,symbols,visited,true).ToArray();
                case IIncrementOrDecrementOperation increment:
                    var prior = Read(compilation,increment.Target,symbols,visited,true).ToArray();
                    if (increment.IsPostfix && !stored) return prior;
                    return increment.OperatorMethod is IMethodSymbol incrementMethod
                        ? Produced(incrementMethod,new[] { (incrementMethod.Parameters[0],prior) },increment.Syntax)
                        : Known(increment.Type,increment.Syntax);
                default:
                    return Array.Empty<Origin>();
            }
        }

        IEnumerable<Origin> Binding(ISymbol symbol, QueryRegistrations.Symbols context)
        {
            if (!visited.Add(symbol)) return visited.Bindings.TryGetValue(symbol,out var acquired) ? acquired : Array.Empty<Origin>();
            try
            {
                var declarations = symbol.DeclaringSyntaxReferences.Select(reference => reference.GetSyntax()).ToArray();
                var initial = declarations.OfType<VariableDeclaratorSyntax>().Where(variable => variable.Initializer != null)
                    .SelectMany(variable => Read(compilation,compilation.GetSemanticModel(variable.SyntaxTree)
                        .GetOperation(variable.Initializer!.Value),context,visited,source)).ToArray();
                if (!source) return initial;
                var incoming = symbol is IParameterSymbol parameter ? context.Input(parameter).Where(origin => origin.Type != null) : Array.Empty<Origin>();
                visited.Bindings[symbol] = initial.Concat(incoming).ToArray();
                var carrier = symbol.ContainingType;
                var scopes = symbol is IFieldSymbol or IPropertySymbol
                    ? carrier!.DeclaringSyntaxReferences.Select(reference => reference.GetSyntax())
                    : declarations.Select(declaration => declaration.Ancestors().FirstOrDefault(node =>
                        node is BaseMethodDeclarationSyntax or LocalFunctionStatementSyntax or LambdaExpressionSyntax) ?? declaration);
                var assigned = scopes.SelectMany(scope => scope.DescendantNodes().Where(node => node is AssignmentExpressionSyntax or
                        PrefixUnaryExpressionSyntax or PostfixUnaryExpressionSyntax))
                    .Select(syntax => compilation.GetSemanticModel(syntax.SyntaxTree).GetOperation(syntax))
                    .Where(write => write is IAssignmentOperation or IIncrementOrDecrementOperation)
                    .Where(write =>
                    {
                        var target = TargetSymbol(write is IAssignmentOperation assignment ? assignment.Target : ((IIncrementOrDecrementOperation)write!).Target);
                        return target != null && SymbolEqualityComparer.Default.Equals(target.OriginalDefinition,symbol.OriginalDefinition);
                    }).SelectMany(write => Read(compilation,write,context,visited,true,true));
                return initial.Concat(assigned).ToArray();
            }
            finally { visited.Bindings.Remove(symbol); visited.Remove(symbol); }
        }

        IEnumerable<Origin> Returns(IMethodSymbol method, IEnumerable<(IParameterSymbol Parameter, Origin[] Origins)> arguments)
        {
            var actual = new QueryRegistrations.Symbols(compilation, symbols.Method(method), symbols);
            foreach (var argument in arguments) actual.BindInput(argument.Parameter,argument.Origins);
            if (!visited.Add(method.OriginalDefinition)) return Array.Empty<Origin>();
            try
            {
                var declarations = method.AssociatedSymbol is IPropertySymbol associatedProperty
                    ? associatedProperty.DeclaringSyntaxReferences : method.DeclaringSyntaxReferences;
                return declarations.SelectMany(reference =>
                {
                    var declaration = reference.GetSyntax();
                    var bodies = declaration.DescendantNodes(node => ReferenceEquals(node, declaration) ||
                        node is not (LambdaExpressionSyntax or LocalFunctionStatementSyntax))
                        .OfType<ReturnStatementSyntax>().Select(statement => statement.Expression).OfType<ExpressionSyntax>();
                    ExpressionSyntax? expression = declaration switch
                    {
                        MethodDeclarationSyntax function => function.ExpressionBody?.Expression,
                        ConversionOperatorDeclarationSyntax conversion => conversion.ExpressionBody?.Expression,
                        OperatorDeclarationSyntax operation => operation.ExpressionBody?.Expression,
                        LocalFunctionStatementSyntax function => function.ExpressionBody?.Expression,
                        AccessorDeclarationSyntax accessor => accessor.ExpressionBody?.Expression,
                        PropertyDeclarationSyntax property => property.ExpressionBody?.Expression ?? property.Initializer?.Value,
                        ArrowExpressionClauseSyntax body => body.Expression,
                        _ => null
                    };
                    if (expression != null) bodies = bodies.Append(expression);
                    if (declaration is PropertyDeclarationSyntax { AccessorList: not null } declaredProperty)
                        bodies = bodies.Concat(declaredProperty.AccessorList.Accessors
                            .Where(accessor => accessor.Keyword.ValueText == "get" && accessor.ExpressionBody != null)
                            .Select(accessor => accessor.ExpressionBody!.Expression));
                    return bodies.SelectMany(body => Read(compilation,
                        compilation.GetSemanticModel(body.SyntaxTree).GetOperation(body), actual, visited,source));
                }).Concat(source && method.AssociatedSymbol is IPropertySymbol writtenProperty
                    ? Binding(writtenProperty,actual) : Array.Empty<Origin>()).ToArray();
            }
            finally { visited.Remove(method.OriginalDefinition); }
        }
    }
}
