namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.ChainExpr
open SoloDatabase.ChainPolicy


/// The accumulator a chain walk fills in, and its lifecycle: creation, boundary flushing,
/// identity ordering, and finalisation.
module internal ChainState =
    type ExtractionState =
        {
            mutable SeenBoundary: bool
            Wheres: ResizeArray<Expression>
            SortKeys: ResizeArray<Expression * SortDirection>
            PostBoundWheres: ResizeArray<Expression>
            PostBoundSortKeys: ResizeArray<Expression * SortDirection>
            mutable PostBoundLimit: Expression option
            mutable PostBoundOffset: Expression option
            mutable SeenOrderBy: bool
            mutable Limit: Expression option
            mutable Offset: Expression option
            mutable TakeWhileInfo: (LambdaExpression * bool) option
            mutable PostBoundTakeWhileInfo: (LambdaExpression * bool) option
            mutable GroupByKey: LambdaExpression option
            mutable Distinct: bool
            mutable SelectProjection: LambdaExpression option
            SetOps: ResizeArray<SetOperation>
            mutable OfTypeName: string option
            mutable CastTypeName: string option
            mutable GroupByHaving: Expression option
            mutable DefaultIfEmpty: Expression option option
            mutable PostSelectDefaultIfEmpty: Expression option option
            mutable SelectManyLambda: LambdaExpression option
        }

    let createState () =
        {
            SeenBoundary = false
            Wheres = ResizeArray()
            SortKeys = ResizeArray()
            PostBoundWheres = ResizeArray()
            PostBoundSortKeys = ResizeArray()
            PostBoundLimit = None
            PostBoundOffset = None
            SeenOrderBy = false
            Limit = None
            Offset = None
            TakeWhileInfo = None
            PostBoundTakeWhileInfo = None
            GroupByKey = None
            Distinct = false
            SelectProjection = None
            SetOps = ResizeArray()
            OfTypeName = None
            CastTypeName = None
            GroupByHaving = None
            DefaultIfEmpty = None
            PostSelectDefaultIfEmpty = None
            SelectManyLambda = None
        }

    let flushBoundary (state: ExtractionState) =
        state.SeenBoundary <- true
        state.PostBoundWheres.AddRange(state.Wheres)
        state.Wheres.Clear()
        state.PostBoundSortKeys.AddRange(state.SortKeys)
        state.SortKeys.Clear()
        if state.Limit.IsSome then state.PostBoundLimit <- state.Limit
        if state.Offset.IsSome then state.PostBoundOffset <- state.Offset

    let getSourceElementType (src: Expression) =
        if isNull src || not src.Type.IsGenericType then null
        else src.Type.GetGenericArguments() |> Array.tryHead |> Option.defaultValue null

    let addIdentityOrder (state: ExtractionState) (src: Expression) (dir: SortDirection) =
        if not state.SeenOrderBy then
            state.SeenOrderBy <- true
            let key =
                match src with
                | :? MethodCallExpression as srcMc when srcMc.Method.Name = "Select" ->
                    match getArg srcMc with
                    | Some selectorExpr -> selectorExpr
                    | None -> mkIdentityLambdaForSequence src :> Expression
                | _ -> mkIdentityLambdaForSequence src :> Expression
            if state.SeenBoundary then state.SortKeys.Insert(0, (key, dir))
            else state.PostBoundSortKeys.Insert(0, (key, dir))

    let captureTypeFilter (config: ExtractorConfig) (state: ExtractionState) (src: Expression) (targetType: Type) (isCast: bool) =
        let sourceElemType = getSourceElementType src
        if not (isNull sourceElemType) then
            if not isCast || sourceElemType <> targetType then
                config.EnsureOfTypeSupported sourceElemType
        match Utils.typeToName targetType with
        | Some tn when isCast && sourceElemType <> targetType -> state.CastTypeName <- Some tn
        | Some tn when not isCast -> state.OfTypeName <- Some tn
        | _ -> ()

    /// Pure logic for CountBy normalization — returns (normalizedSource, groupByKeyOpt).
    /// Callers apply groupByKeyOpt to their own state type.
    let finalizeState (state: ExtractionState) =
        if not state.SeenBoundary then
            state.Wheres.AddRange(state.PostBoundWheres)
            state.PostBoundWheres.Clear()
            state.SortKeys.AddRange(state.PostBoundSortKeys)
            state.PostBoundSortKeys.Clear()

        if state.SelectProjection.IsNone && state.PostSelectDefaultIfEmpty.IsSome && state.DefaultIfEmpty.IsNone then
            state.DefaultIfEmpty <- state.PostSelectDefaultIfEmpty
            state.PostSelectDefaultIfEmpty <- None

