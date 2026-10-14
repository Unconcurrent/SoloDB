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
        { Lambda = lambda; Symbols = symbols; }
        internal LambdaExpressionSyntax Lambda { get; }
        internal QueryRegistrations.Symbols Symbols { get; }
    }

    internal static INamedTypeSymbol? Contract(ITypeSymbol type)
    {
        if (type is IArrayTypeSymbol { Rank: 1 } array) type = array.ElementType;
        return type is INamedTypeSymbol named &&
            named.OriginalDefinition.ToDisplayString() == "System.Linq.Expressions.Expression<TDelegate>" ? named : null;
    }

    internal static IEnumerable<(IParameterSymbol Parameter, Origin[] Origins)> Arguments(
        Compilation compilation, IInvocationOperation invocation, QueryRegistrations.Symbols symbols)
    {
        foreach (var argument in invocation.Arguments)
            if (argument.Parameter is IParameterSymbol parameter && Contract(symbols.Type(parameter.Type)) != null)
                yield return (parameter, Read(compilation, argument.Value, symbols,
                    new HashSet<ISymbol>(SymbolEqualityComparer.Default)).ToArray());
    }

    private static IEnumerable<Origin> Read(Compilation compilation, IOperation? value,
        QueryRegistrations.Symbols symbols, HashSet<ISymbol> visited)
    {
        switch (value)
        {
            case IConversionOperation conversion:
                return Read(compilation, conversion.Operand, symbols, visited);
            case IParenthesizedOperation parenthesized:
                return Read(compilation, parenthesized.Operand, symbols, visited);
            case IDelegateCreationOperation creation:
                return Read(compilation, creation.Target, symbols, visited);
            case IAnonymousFunctionOperation { Syntax: LambdaExpressionSyntax lambda }:
                return new[] { new Origin(lambda, symbols) };
            case IArrayCreationOperation array:
                return Read(compilation, array.Initializer, symbols, visited);
            case IArrayInitializerOperation initializer:
                return initializer.ElementValues.SelectMany(element => Read(compilation, element, symbols, visited)).ToArray();
            case ICollectionExpressionOperation collection:
                return collection.Elements.SelectMany(element => Read(compilation, element, symbols, visited)).ToArray();
            case ISpreadOperation spread:
                return Read(compilation, spread.Operand, symbols, visited);
            case IConditionalOperation conditional:
                return Read(compilation, conditional.WhenTrue, symbols, visited)
                    .Concat(Read(compilation, conditional.WhenFalse, symbols, visited)).ToArray();
            case ICoalesceOperation coalesce:
                return Read(compilation, coalesce.Value, symbols, visited)
                    .Concat(Read(compilation, coalesce.WhenNull, symbols, visited)).ToArray();
            case IParameterReferenceOperation parameter:
                return symbols.Input(parameter.Parameter);
            case ILocalReferenceOperation local:
                return Binding(local.Local, symbols);
            case IFieldReferenceOperation field:
                return Binding(field.Field, symbols.WithOwner(field.Field.ContainingType));
            case IPropertyReferenceOperation property when property.Property.GetMethod is IMethodSymbol getter:
                return Returns(getter, property.Arguments);
            case IInvocationOperation call when Contract(symbols.Type(call.Type!)) != null:
                return Returns(call.TargetMethod, call.Arguments);
            default:
                return Array.Empty<Origin>();
        }

        IEnumerable<Origin> Binding(ISymbol symbol, QueryRegistrations.Symbols context)
        {
            if (!visited.Add(symbol)) return Array.Empty<Origin>();
            try
            {
                return symbol.DeclaringSyntaxReferences.SelectMany(reference =>
                {
                    var declaration = reference.GetSyntax();
                    if (declaration is not VariableDeclaratorSyntax { Initializer: not null } variable)
                        return Array.Empty<Origin>();
                    return Read(compilation, compilation.GetSemanticModel(variable.SyntaxTree)
                        .GetOperation(variable.Initializer.Value), context, visited);
                }).ToArray();
            }
            finally { visited.Remove(symbol); }
        }

        IEnumerable<Origin> Returns(IMethodSymbol method, IEnumerable<IArgumentOperation> arguments)
        {
            var actual = new QueryRegistrations.Symbols(compilation, symbols.Method(method), symbols);
            foreach (var argument in arguments)
                if (argument.Parameter is IParameterSymbol parameter && Contract(symbols.Type(parameter.Type)) != null)
                    actual.BindInput(parameter, Read(compilation, argument.Value, symbols, visited).ToArray());
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
                        compilation.GetSemanticModel(body.SyntaxTree).GetOperation(body), actual, visited));
                }).ToArray();
            }
            finally { visited.Remove(method.OriginalDefinition); }
        }
    }
}
