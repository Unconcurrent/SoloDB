namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

/// Navigation over a LINQ method chain: reaching a call's source and argument, recognising a
/// rooted chain, and the identity lambda used where a sequence carries no explicit selector.
/// Expression shape and shared overload validation, with no accumulated state; it sits below the descriptor layers
/// that both need it.
module internal ChainExpr =
    /// Grouping must use the same supported comparer contract at every chain depth.
    let validateGroupingComparer (operation: string) (keyType: Type) (expression: Expression) =
        let unsupported () =
            NotSupportedException(
                "Error: " + operation + " comparer overload is not supported.\n" +
                "Reason: SoloDB can translate " + operation + " only with the default equality comparer in SQL.\n" +
                "Fix: Remove the comparer or normalize the key inside the selector.")
        let value =
            try evaluateExpr<obj> expression
            with _ -> raise (unsupported ())
        if not (isNull value) then
            let comparerType = typedefof<System.Collections.Generic.EqualityComparer<_>>.MakeGenericType([| keyType |])
            let defaultProperty = comparerType.GetProperty("Default", System.Reflection.BindingFlags.Public ||| System.Reflection.BindingFlags.Static)
            let defaultComparer = if isNull defaultProperty then null else defaultProperty.GetValue(null)
            if isNull defaultComparer || not (comparerType.IsAssignableFrom(value.GetType()))
               || not (obj.ReferenceEquals(value, defaultComparer) || value.GetType() = defaultComparer.GetType()) then
                raise (unsupported ())

    let sequenceElementType (sequenceType: Type) =
        let isEnumerable (t: Type) =
            t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<System.Collections.Generic.IEnumerable<_>>
        if sequenceType.IsArray then sequenceType.GetElementType()
        elif isEnumerable sequenceType then sequenceType.GetGenericArguments().[0]
        else
            match sequenceType.GetInterfaces() |> Array.tryFind isEnumerable with
            | Some enumerable -> enumerable.GetGenericArguments().[0]
            | None -> raise (NotSupportedException("Could not resolve sequence element type."))

    let mkIdentityLambdaForSequence (expr: Expression) =
        // IGrouping<TKey,TElement> has its key first; generic argument position
        // is not the sequence's element contract.
        let p = Expression.Parameter(sequenceElementType expr.Type)
        Expression.Lambda(p, [| p |])

    let getSource (mce: MethodCallExpression) =
        if not (isNull mce.Object) then mce.Object
        elif mce.Arguments.Count > 0 then mce.Arguments.[0]
        else null

    let getArg (mce: MethodCallExpression) =
        if not (isNull mce.Object) then
            if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
        elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
        else None
