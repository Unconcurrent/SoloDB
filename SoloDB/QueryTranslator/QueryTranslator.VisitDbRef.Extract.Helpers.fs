namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open DBRefTypeHelpers
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers

module internal DBRefManyExtractorHelpers =
    let mkIdentityLambdaForDbRefMany (expr: Expression) =
        let targetType =
            match expr.Type.GetGenericArguments() |> Array.tryHead with
            | Some t -> t
            | None -> raise (InvalidOperationException("Could not resolve DBRefMany target type for identity materialization."))
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

    let rec isDBRefManyChain (unwrapConvert: Expression -> Expression) (isDBRefManyType: Type -> bool) (expr: Expression) : bool =
        let e = unwrapConvert expr
        if isDBRefManyType e.Type then true
        else
            match e with
            | :? MethodCallExpression as mc ->
                let src = getSource mc
                not (isNull src) && isDBRefManyChain unwrapConvert isDBRefManyType src
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

    let buildDescriptor
        innerSource
        ofTypeName
        wheres
        sortKeys
        limit
        offset
        postBoundWheres
        postBoundSortKeys
        postBoundLimit
        postBoundOffset
        takeWhileInfo
        groupByKey
        distinct
        selectProjection
        setOp
        terminal
        groupByHaving
        defaultIfEmpty
        postSelectDefaultIfEmpty =
        {
            Source = innerSource
            OfTypeName = ofTypeName
            WherePredicates = wheres |> Seq.toList
            SortKeys = sortKeys |> Seq.toList
            Limit = limit
            Offset = offset
            PostBoundWherePredicates = postBoundWheres |> Seq.toList
            PostBoundSortKeys = postBoundSortKeys |> Seq.toList
            PostBoundLimit = postBoundLimit
            PostBoundOffset = postBoundOffset
            TakeWhileInfo = takeWhileInfo
            GroupByKey = groupByKey
            Distinct = distinct
            SelectProjection = selectProjection
            SetOp = setOp
            Terminal = terminal
            GroupByHavingPredicate = groupByHaving
            DefaultIfEmpty = defaultIfEmpty
            PostSelectDefaultIfEmpty = postSelectDefaultIfEmpty
        }

    let tryBuildCountPropertyDescriptor (unwrapConvert: Expression -> Expression) (isDBRefManyType: Type -> bool) (me: MemberExpression) : QueryDescriptor voption =
        let inner = unwrapConvert me.Expression
        if isDBRefManyType inner.Type then
            ValueSome(
                buildDescriptor
                    inner
                    None
                    []
                    []
                    None
                    None
                    []
                    []
                    None
                    None
                    None
                    None
                    false
                    None
                    None
                    Terminal.Count
                    None
                    None
                    None)
        else
            ValueNone

    let tryBuildMaterializeDescriptor (isDBRefManyType: Type -> bool) (memberExpr: Expression) : QueryDescriptor voption =
        if isDBRefManyType memberExpr.Type then
            let identity = mkIdentityLambdaForDbRefMany memberExpr
            ValueSome(
                buildDescriptor
                    memberExpr
                    None
                    []
                    []
                    None
                    None
                    []
                    []
                    None
                    None
                    None
                    None
                    false
                    (Some identity)
                    None
                    (Terminal.Select(identity :> Expression))
                    None
                    None
                    None)
        else
            ValueNone
