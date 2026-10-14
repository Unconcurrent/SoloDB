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

    internal static IEnumerable<(IParameterSymbol Parameter, Origin[] Origins)> Arguments(
        Compilation compilation, IInvocationOperation invocation, QueryRegistrations.Symbols symbols, bool objects = false)
    {
        foreach (var argument in invocation.Arguments)
            if (argument.Parameter is IParameterSymbol parameter)
            {
                if (Contract(symbols.Type(parameter.Type)) != null)
                    yield return (parameter, Read(compilation, argument.Value, symbols,
                        new HashSet<ISymbol>(SymbolEqualityComparer.Default)).ToArray());
                else if (objects && symbols.Type(parameter.Type).SpecialType == SpecialType.System_Object)
                    yield return (parameter, Sources(compilation,argument.Value,symbols));
            }
    }

    internal static Origin[] Sources(Compilation compilation, IOperation value, QueryRegistrations.Symbols symbols) =>
        Read(compilation,value,symbols,new HashSet<ISymbol>(SymbolEqualityComparer.Default),true).ToArray();

    private static IEnumerable<Origin> Read(Compilation compilation, IOperation? value,
        QueryRegistrations.Symbols symbols, HashSet<ISymbol> visited, bool source = false)
    {
        var known = source && value is not IParenthesizedOperation && (value is not IConversionOperation conversionValue || conversionValue.OperatorMethod != null) && value?.Type is ITypeSymbol raw &&
            symbols.Type(raw) is ITypeSymbol actual && actual.SpecialType != SpecialType.System_Object && actual.TypeKind != TypeKind.Dynamic
            ? new Origin(value.Syntax,symbols,actual) : null;
        if (known != null && value is IArrayCreationOperation or ICollectionExpressionOperation) return new[] { known };
        var origins = ReadOrigins(compilation,value,symbols,visited,source).ToArray();
        return origins.Length != 0 || known == null ? origins : new[] { known };
    }

    private static IEnumerable<Origin> ReadOrigins(Compilation compilation, IOperation? value,
        QueryRegistrations.Symbols symbols, HashSet<ISymbol> visited, bool source)
    {
        switch (value)
        {
            case IConversionOperation conversion when conversion.OperatorMethod == null:
                return Read(compilation, conversion.Operand, symbols, visited,source);
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
                return symbols.Input(parameter.Parameter).Where(origin => source ? origin.Type != null : origin.Type == null).ToArray();
            case ILocalReferenceOperation local:
                return Binding(local.Local, symbols);
            case IFieldReferenceOperation field:
                return Binding(field.Field, symbols.WithOwner(field.Field.ContainingType));
            case IPropertyReferenceOperation property when property.Property.GetMethod is IMethodSymbol getter:
                return Returns(getter, property.Arguments);
            case IInvocationOperation call when source || Contract(symbols.Type(call.Type!)) != null:
                return Returns(call.TargetMethod, call.Arguments);
            default:
                return Array.Empty<Origin>();
        }

        IEnumerable<Origin> Binding(ISymbol symbol, QueryRegistrations.Symbols context)
        {
            if (!visited.Add(symbol)) return Array.Empty<Origin>();
            try
            {
                var declarations = symbol.DeclaringSyntaxReferences.Select(reference => reference.GetSyntax()).ToArray();
                var initial = declarations.OfType<VariableDeclaratorSyntax>().Where(variable => variable.Initializer != null)
                    .SelectMany(variable => Read(compilation,compilation.GetSemanticModel(variable.SyntaxTree)
                        .GetOperation(variable.Initializer!.Value),context,visited,source));
                if (!source) return initial.ToArray();
                var scopes = symbol is IFieldSymbol field
                    ? field.ContainingType.DeclaringSyntaxReferences.Select(reference => reference.GetSyntax())
                    : declarations.Select(declaration => declaration.Ancestors().FirstOrDefault(node =>
                        node is BaseMethodDeclarationSyntax or LocalFunctionStatementSyntax or LambdaExpressionSyntax) ?? declaration);
                var assigned = scopes.SelectMany(scope => scope.DescendantNodes().OfType<AssignmentExpressionSyntax>())
                    .Select(syntax => compilation.GetSemanticModel(syntax.SyntaxTree).GetOperation(syntax))
                    .OfType<IAssignmentOperation>().Where(assignment =>
                    {
                        var target = assignment.Target switch
                        {
                            ILocalReferenceOperation local => (ISymbol)local.Local,
                            IFieldReferenceOperation field => field.Field,
                            _ => null
                        };
                        return target != null && SymbolEqualityComparer.Default.Equals(target.OriginalDefinition,symbol.OriginalDefinition);
                    }).SelectMany(assignment => Read(compilation,assignment.Value,context,visited,source));
                return initial.Concat(assigned).ToArray();
            }
            finally { visited.Remove(symbol); }
        }

        IEnumerable<Origin> Returns(IMethodSymbol method, IEnumerable<IArgumentOperation> arguments)
        {
            var actual = new QueryRegistrations.Symbols(compilation, symbols.Method(method), symbols);
            foreach (var argument in arguments)
                if (argument.Parameter is IParameterSymbol parameter &&
                    (Contract(symbols.Type(parameter.Type)) != null || source && symbols.Type(parameter.Type).SpecialType == SpecialType.System_Object))
                    actual.BindInput(parameter, Read(compilation, argument.Value, symbols, visited,source).ToArray());
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
                        compilation.GetSemanticModel(body.SyntaxTree).GetOperation(body), actual, visited,source));
                }).ToArray();
            }
            finally { visited.Remove(method.OriginalDefinition); }
        }
    }
}
