namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Collections.ObjectModel
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

/// Navigation over a LINQ method chain: reaching a call's source and argument, recognising a
/// rooted chain, and the identity selector used where a sequence carries no explicit selector.
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
            try ExpressionEvaluation.evaluateExpr<obj> expression
            with _ -> raise (unsupported ())
        if not (isNull value) then
            let defaultComparer = ModelMetadata.DefaultComparer keyType
            if isNull defaultComparer || not (obj.ReferenceEquals(value, defaultComparer) || value.GetType().Equals(defaultComparer.GetType())) then
                raise (unsupported ())

    let sequenceElementType (sequenceType: Type) = ModelMetadata.SequenceElement sequenceType

    type Selector =
        { Body: Expression
          Parameters: ReadOnlyCollection<ParameterExpression> }
        member this.ReturnType = this.Body.Type

    let selector (lambda: LambdaExpression) =
        { Body = lambda.Body; Parameters = lambda.Parameters }

    let identitySelectorForSequence (expr: Expression) =
        // IGrouping<TKey,TElement> has its key first; generic argument position
        // is not the sequence's element contract.
        let p = Expression.Parameter(sequenceElementType expr.Type)
        { Body = p; Parameters = Array.AsReadOnly [| p |] }

    let getSource (mce: MethodCallExpression) =
        if not (isNull mce.Object) then mce.Object
        elif mce.Arguments.Count > 0 then mce.Arguments.[0]
        else null

    let getArg (mce: MethodCallExpression) =
        if not (isNull mce.Object) then
            if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
        elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
        else None
