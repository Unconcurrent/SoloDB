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
            case IConditionalOperation conditional:
                return Read(compilation, conditional.WhenTrue, symbols, visited)
                    .Concat(Read(compilation, conditional.WhenFalse, symbols, visited)).ToArray();
            case ICoalesceOperation coalesce:
                return Read(compilation, coalesce.Value, symbols, visited)
                    .Concat(Read(compilation, coalesce.WhenNull, symbols, visited)).ToArray();
            case IParameterReferenceOperation parameter:
                return symbols.Input(parameter.Parameter);
            case ILocalReferenceOperation local:
                return Binding(local.Local);
            case IFieldReferenceOperation field:
                return Binding(field.Field);
            case IInvocationOperation call when Contract(symbols.Type(call.Type!)) != null:
            {
                var method = call.TargetMethod;
                if (!visited.Add(method.OriginalDefinition)) return Array.Empty<Origin>();
                var actual = new QueryRegistrations.Symbols(compilation, symbols.Method(method), symbols);
                foreach (var argument in Arguments(compilation, call, symbols)) actual.BindInput(argument.Parameter, argument.Origins);
                return method.DeclaringSyntaxReferences.SelectMany(reference =>
                {
                    var declaration = reference.GetSyntax();
                    var bodies = declaration.DescendantNodes(node => ReferenceEquals(node, declaration) ||
                        node is not (LambdaExpressionSyntax or LocalFunctionStatementSyntax))
                        .OfType<ReturnStatementSyntax>().Select(statement => statement.Expression).OfType<ExpressionSyntax>();
                    if (declaration is MethodDeclarationSyntax { ExpressionBody: not null } function)
                        bodies = bodies.Append(function.ExpressionBody.Expression);
                    if (declaration is LocalFunctionStatementSyntax { ExpressionBody: not null } localFunction)
                        bodies = bodies.Append(localFunction.ExpressionBody.Expression);
                    return bodies.SelectMany(body => Read(compilation,
                        compilation.GetSemanticModel(body.SyntaxTree).GetOperation(body), actual, visited));
                }).ToArray();
            }
            default:
                return Array.Empty<Origin>();
        }

        IEnumerable<Origin> Binding(ISymbol symbol)
        {
            if (!visited.Add(symbol)) return Array.Empty<Origin>();
            return symbol.DeclaringSyntaxReferences.SelectMany(reference =>
            {
                var declaration = reference.GetSyntax();
                if (declaration is not VariableDeclaratorSyntax { Initializer: not null } variable)
                    return Array.Empty<Origin>();
                return Read(compilation, compilation.GetSemanticModel(variable.SyntaxTree)
                    .GetOperation(variable.Initializer.Value), symbols, visited);
            }).ToArray();
        }
    }
}
