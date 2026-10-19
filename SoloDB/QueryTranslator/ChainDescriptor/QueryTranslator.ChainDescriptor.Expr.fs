namespace SoloDatabase

open System
open System.Linq
open System.Linq.Expressions
open System.Reflection
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
    /// The framework default is immutable structural comparer material.
    let rec isDefaultEqualityComparer (expression: Expression) =
        match expression with
        | :? UnaryExpression as conversion when isReferenceUpcast conversion -> isDefaultEqualityComparer conversion.Operand
        | :? MemberExpression as access when isNull access.Expression ->
            match access.Member with
            | :? PropertyInfo as property ->
                let owner = property.DeclaringType
                not(isNull owner) && owner.IsGenericType
                && typedefof<System.Collections.Generic.EqualityComparer<_>>.Equals(owner.GetGenericTypeDefinition())
                && property.Name = "Default" && property.PropertyType.Equals owner
                && not(isNull property.GetMethod) && property.GetMethod.IsStatic
            | _ -> false
        | _ -> false

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

    let identitySelector (elementType: Type) =
        let parameter = Expression.Parameter(elementType)
        { Body = parameter; Parameters = Array.AsReadOnly [| parameter |] }

    let identitySelectorForSequence (expr: Expression) =
        // IGrouping<TKey,TElement> carries the element in its sequence contract.
        identitySelector (sequenceElementType expr.Type)

    let getSource (mce: MethodCallExpression) =
        if not (isNull mce.Object) then mce.Object
        elif mce.Arguments.Count > 0 then mce.Arguments.[0]
        else null

    let getArg (mce: MethodCallExpression) =
        if not (isNull mce.Object) then
            if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
        elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
        else None

    /// Closed overload roles are shared by root and nested grouping adapters.
    type GroupBySelectors =
        { Key: Expression
          Element: Expression option
          Result: LambdaExpression option
          GroupType: Type }

    let groupBySelectors (method: MethodInfo) (arguments: Expression array) =
        let parameters = method.GetParameters()
        let generic = method.GetGenericArguments()
        let mutable element = None
        let mutable result = None
        let mutable elementType = generic[0]
        for index = 2 to parameters.Length - 1 do
            let declared = parameters[index].ParameterType
            if declared.IsGenericType && typedefof<System.Collections.Generic.IEqualityComparer<_>>.Equals(declared.GetGenericTypeDefinition()) then
                validateGroupingComparer "GroupBy" generic[1] arguments[index - 1]
            else
                let delegateType =
                    if declared.IsGenericType && typedefof<Expression<_>>.Equals(declared.GetGenericTypeDefinition()) then declared.GetGenericArguments()[0]
                    else declared
                let arity = delegateType.GetGenericArguments().Length - 1
                match QueryTranslatorVisitPost.tryExtractLambdaExpression arguments[index - 1] with
                | ValueSome lambda when lambda.Parameters.Count = arity && arity = 1 ->
                    element <- Some arguments[index - 1]
                    elementType <- lambda.ReturnType
                | ValueSome lambda when lambda.Parameters.Count = arity && arity = 2 -> result <- Some lambda
                | _ -> raise (NotSupportedException("GroupBy requires inline key, element and result selector expressions."))
        { Key = arguments[0]; Element = element; Result = result
          GroupType = if result.IsNone then sequenceElementType method.ReturnType else typedefof<IGrouping<_,_>>.MakeGenericType(generic[1], elementType) }

    let groupingResult (groupType: Type) (result: LambdaExpression) =
        let group = Expression.Parameter(groupType, "group")
        let items = Expression.Convert(group, result.Parameters[1].Type) :> Expression
        let body = inlineLambdaInvocation result [|Expression.Property(group, "Key") :> Expression; items|]
        Expression.Lambda(body, group)
