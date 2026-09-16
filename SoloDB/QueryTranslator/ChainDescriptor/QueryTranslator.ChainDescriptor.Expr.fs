namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

/// Navigation over a LINQ method chain: reaching a call's source and argument, recognising a
/// rooted chain, and the identity lambda used where a sequence carries no explicit selector.
/// Pure expression shape, no accumulated state, so it sits below the state and descriptor layers
/// that both need it.
module internal ChainExpr =
    let mkIdentityLambdaForSequence (expr: Expression) =
        let targetType =
            match expr.Type.GetGenericArguments() |> Array.tryHead with
            | Some t -> t
            | None -> raise (InvalidOperationException("Could not resolve sequence target type for identity materialization."))
        let p = Expression.Parameter(targetType)
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

    let rec isRootedChain (unwrapConvert: Expression -> Expression) (isChainRoot: Expression -> bool) (expr: Expression) : bool =
        let e = unwrapConvert expr
        if isChainRoot e then true
        else
            match e with
            | :? MethodCallExpression as mc ->
                let src = getSource mc
                not (isNull src) && isRootedChain unwrapConvert isChainRoot src
            | _ -> false

    let preprocessRoot (expr: Expression) : Expression * bool * bool =
        let mutable outerDistinct = false
        let mutable outerMaterialize = false
        let rec loop (e: Expression) =
            match e with
            | :? MethodCallExpression as mc when mc.Method.Name = "Distinct" ->
                outerDistinct <- true
                let src = getSource mc
                if isNull src then e else loop src
            | :? MethodCallExpression as mc when mc.Method.Name = "ToList" || mc.Method.Name = "ToArray" ->
                outerMaterialize <- true
                let src = getSource mc
                if isNull src then e else loop src
            | _ -> e
        loop expr, outerDistinct, outerMaterialize

