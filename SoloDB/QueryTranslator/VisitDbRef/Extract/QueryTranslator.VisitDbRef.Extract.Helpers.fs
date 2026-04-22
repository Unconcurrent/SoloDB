namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open DBRefTypeHelpers
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.SharedDescriptorExtract

module internal DBRefManyExtractorHelpers =
    let mkIdentityLambdaForDbRefMany (expr: Expression) =
        let targetType =
            match expr.Type.GetGenericArguments() |> Array.tryHead with
            | Some t -> t
            | None -> raise (InvalidOperationException("Could not resolve DBRefMany target type for identity materialization."))
        let p = Expression.Parameter(targetType)
        Expression.Lambda(p, [| p |])

    let rec isDBRefManyChain (unwrapConvert: Expression -> Expression) (isDBRefManyType: Type -> bool) (expr: Expression) : bool =
        let e = unwrapConvert expr
        if isDBRefManyType e.Type then true
        else
            match e with
            | :? MethodCallExpression as mc ->
                let src = getSource mc
                not (isNull src) && isDBRefManyChain unwrapConvert isDBRefManyType src
            | :? MemberExpression as me when not (isNull me.Expression) ->
                // Walk through member access (e.g., property on parameter) to find DBRefMany root.
                isDBRefManyChain unwrapConvert isDBRefManyType me.Expression
            | _ -> false

    let buildDescriptor
        innerSource
        ofTypeName
        castTypeName
        wheres
        sortKeys
        limit
        offset
        postBoundWheres
        postBoundSortKeys
        postBoundLimit
        postBoundOffset
        takeWhileInfo
        postBoundTakeWhileInfo
        groupByKey
        distinct
        selectProjection
        setOps
        terminal
        groupByHaving
        defaultIfEmpty
        postSelectDefaultIfEmpty
        selectManyLambda =
        {
            Source = innerSource
            OfTypeName = ofTypeName
            CastTypeName = castTypeName
            WherePredicates = wheres |> Seq.toList
            SortKeys = sortKeys |> Seq.toList
            Limit = limit
            Offset = offset
            PostBoundWherePredicates = postBoundWheres |> Seq.toList
            PostBoundSortKeys = postBoundSortKeys |> Seq.toList
            PostBoundLimit = postBoundLimit
            PostBoundOffset = postBoundOffset
            TakeWhileInfo = takeWhileInfo
            PostBoundTakeWhileInfo = postBoundTakeWhileInfo
            GroupByKey = groupByKey
            Distinct = distinct
            SelectProjection = selectProjection
            SetOps = setOps
            Terminal = terminal
            GroupByHavingPredicate = groupByHaving
            DefaultIfEmpty = defaultIfEmpty
            PostSelectDefaultIfEmpty = postSelectDefaultIfEmpty
            SelectManyInnerLambda = selectManyLambda
        }

    let tryBuildCountPropertyDescriptor (unwrapConvert: Expression -> Expression) (isDBRefManyType: Type -> bool) (me: MemberExpression) : QueryDescriptor voption =
        let inner = unwrapConvert me.Expression
        if isDBRefManyType inner.Type then
            ValueSome(
                buildDescriptor
                    inner
                    None
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
                    None
                    false
                    None
                    []
                    Terminal.Count
                    None
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
                    None
                    false
                    (Some identity)
                    []
                    (Terminal.Select(identity :> Expression))
                    None
                    None
                    None
                    None)
        else
            ValueNone
