namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open DBRefTypeHelpers
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.DBRefManyExtractorHelpers
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost
open Utils

module internal DBRefManyExtractChain =
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
            mutable SetOp: SetOperation option
            mutable OfTypeName: string option
            mutable CastTypeName: string option
            mutable GroupByHaving: Expression option
            mutable DefaultIfEmpty: Expression option option
            mutable PostSelectDefaultIfEmpty: Expression option option
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
            SetOp = None
            OfTypeName = None
            CastTypeName = None
            GroupByHaving = None
            DefaultIfEmpty = None
            PostSelectDefaultIfEmpty = None
        }

    let private flushBoundary (state: ExtractionState) =
        state.SeenBoundary <- true
        state.PostBoundWheres.AddRange(state.Wheres)
        state.Wheres.Clear()
        state.PostBoundSortKeys.AddRange(state.SortKeys)
        state.SortKeys.Clear()
        if state.Limit.IsSome then state.PostBoundLimit <- state.Limit
        if state.Offset.IsSome then state.PostBoundOffset <- state.Offset

    let private getSourceElementType (src: Expression) =
        if isNull src || not src.Type.IsGenericType then null
        else src.Type.GetGenericArguments() |> Array.tryHead |> Option.defaultValue null

    let private addIdentityOrder (state: ExtractionState) (src: Expression) (dir: SortDirection) =
        if not state.SeenOrderBy then
            state.SeenOrderBy <- true
            let key =
                match src with
                | :? MethodCallExpression as srcMc when srcMc.Method.Name = "Select" ->
                    match getArg srcMc with
                    | Some selectorExpr -> selectorExpr
                    | None -> mkIdentityLambdaForDbRefMany src :> Expression
                | _ -> mkIdentityLambdaForDbRefMany src :> Expression
            if state.SeenBoundary then state.SortKeys.Insert(0, (key, dir))
            else state.PostBoundSortKeys.Insert(0, (key, dir))

    let private captureTypeFilter (state: ExtractionState) (src: Expression) (targetType: Type) (isCast: bool) =
        let sourceElemType = getSourceElementType src
        if not (isNull sourceElemType) then
            if not isCast || sourceElemType <> targetType then
                DBRefManyHelpers.ensureOfTypeSupported sourceElemType
        match Utils.typeToName targetType with
        | Some tn when isCast && sourceElemType <> targetType -> state.CastTypeName <- Some tn
        | Some tn when not isCast -> state.OfTypeName <- Some tn
        | _ -> ()

    let normalizeCountBySource (terminal: Terminal) (source: Expression) (state: ExtractionState) : Expression =
        match terminal, source with
        | (Terminal.Count | Terminal.LongCount), (:? MethodCallExpression as srcMc) when srcMc.Method.Name = "CountBy" ->
            match getArg srcMc, getSource srcMc with
            | Some keyExpr, src when not (isNull src) ->
                match tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda ->
                    state.GroupByKey <- Some keyLambda
                    src
                | ValueNone -> source
            | _ -> source
        | _ -> source

    let placeCountPredicate (state: ExtractionState) (countPredicate: Expression option) =
        match countPredicate with
        | Some pred when state.SeenBoundary -> state.PostBoundWheres.Add(pred)
        | Some pred -> state.Wheres.Add(pred)
        | None -> ()

    let finalizeState (state: ExtractionState) =
        if not state.SeenBoundary then
            state.Wheres.AddRange(state.PostBoundWheres)
            state.PostBoundWheres.Clear()
            state.SortKeys.AddRange(state.PostBoundSortKeys)
            state.PostBoundSortKeys.Clear()

        if state.SelectProjection.IsNone && state.PostSelectDefaultIfEmpty.IsSome && state.DefaultIfEmpty.IsNone then
            state.DefaultIfEmpty <- state.PostSelectDefaultIfEmpty
            state.PostSelectDefaultIfEmpty <- None

    let rec walkChain (state: ExtractionState) (e: Expression) : Expression =
        let e = unwrapConvert e
        match e with
        | :? MethodCallExpression as mc ->
            let src = getSource mc
            let arg = getArg mc

            match mc.Method.Name with
            | "Where" ->
                match arg with
                | Some pred ->
                    if state.SeenBoundary then state.Wheres.Add(pred)
                    else state.PostBoundWheres.Add(pred)
                | None -> ()
                walkChain state src

            | "OrderBy" | "OrderByDescending" ->
                if not state.SeenOrderBy then
                    state.SeenOrderBy <- true
                    let dir = if mc.Method.Name = "OrderBy" then SortDirection.Asc else SortDirection.Desc
                    match arg with
                    | Some key ->
                        if state.SeenBoundary then state.SortKeys.Insert(0, (key, dir))
                        else state.PostBoundSortKeys.Insert(0, (key, dir))
                    | None -> ()
                walkChain state src

            | "ThenBy" | "ThenByDescending" ->
                if not state.SeenOrderBy then
                    let dir = if mc.Method.Name = "ThenBy" then SortDirection.Asc else SortDirection.Desc
                    match arg with
                    | Some key ->
                        if state.SeenBoundary then state.SortKeys.Add(key, dir)
                        else state.PostBoundSortKeys.Add(key, dir)
                    | None -> ()
                walkChain state src

            | "Take" ->
                if state.SeenBoundary then
                    raise (NotSupportedException(DBRefManyHelpers.multipleTakeSkipBoundariesMessage))
                flushBoundary state
                state.Limit <- arg
                state.Offset <- None
                walkChain state src

            | "Skip" ->
                if state.SeenBoundary
                   && not (state.Limit.IsSome && state.Offset.IsNone && state.PostBoundLimit.IsNone && state.PostBoundOffset.IsNone) then
                    raise (NotSupportedException(DBRefManyHelpers.multipleTakeSkipBoundariesMessage))
                flushBoundary state
                state.Offset <- arg
                state.Limit <- None
                walkChain state src

            | "TakeWhile" | "SkipWhile" ->
                match arg with
                | Some pred ->
                    match tryExtractLambdaExpression pred with
                    | ValueSome lambda ->
                        match state.TakeWhileInfo, state.PostBoundTakeWhileInfo with
                        | None, _ ->
                            state.TakeWhileInfo <- Some (lambda, mc.Method.Name = "TakeWhile")
                        | Some inner, None ->
                            state.PostBoundTakeWhileInfo <- Some inner
                            state.TakeWhileInfo <- Some (lambda, mc.Method.Name = "TakeWhile")
                        | Some _, Some _ ->
                            raise (NotSupportedException(
                                "Error: More than two TakeWhile/SkipWhile boundaries are not supported on DBRefMany.\n" +
                                "Reason: The relation translator currently supports one inner and one outer window boundary.\n" +
                                "Fix: Simplify the query or move additional windowing after AsEnumerable()."))
                    | ValueNone -> ()
                | None -> ()
                walkChain state src

            | "Select" ->
                match arg with
                | Some proj ->
                    match tryExtractLambdaExpression proj with
                    | ValueSome lambda -> state.SelectProjection <- Some lambda
                    | ValueNone -> ()
                | None -> ()
                walkChain state src

            | "Distinct" ->
                state.Distinct <- true
                walkChain state src

            | "GroupBy" ->
                match arg with
                | Some key ->
                    match tryExtractLambdaExpression key with
                    | ValueSome lambda -> state.GroupByKey <- Some lambda
                    | ValueNone -> ()
                | None -> ()
                walkChain state src

            | "OfType" ->
                let genericArgs = mc.Method.GetGenericArguments()
                if genericArgs.Length = 1 then
                    captureTypeFilter state src genericArgs.[0] false
                walkChain state src

            | "Cast" ->
                let genericArgs = mc.Method.GetGenericArguments()
                if genericArgs.Length = 1 then
                    captureTypeFilter state src genericArgs.[0] true
                walkChain state src

            | "Intersect" ->
                match arg with
                | Some rightSrc -> state.SetOp <- Some (SetOperation.Intersect rightSrc)
                | None -> ()
                walkChain state src

            | "Except" ->
                match arg with
                | Some rightSrc -> state.SetOp <- Some (SetOperation.Except rightSrc)
                | None -> ()
                walkChain state src

            | "Union" ->
                match arg with
                | Some rightSrc -> state.SetOp <- Some (SetOperation.Union rightSrc)
                | None -> ()
                walkChain state src

            | "Concat" ->
                match arg with
                | Some rightSrc -> state.SetOp <- Some (SetOperation.Concat rightSrc)
                | None -> ()
                walkChain state src

            | "DistinctBy" ->
                match arg with
                | Some keySel -> state.SetOp <- Some (SetOperation.DistinctBy keySel)
                | None -> ()
                walkChain state src

            | "Order" ->
                addIdentityOrder state src SortDirection.Asc
                walkChain state src

            | "OrderDescending" ->
                addIdentityOrder state src SortDirection.Desc
                walkChain state src

            | "IntersectBy" ->
                if mc.Arguments.Count >= 3 then
                    state.SetOp <- Some (SetOperation.IntersectBy(mc.Arguments.[1], mc.Arguments.[2]))
                walkChain state src

            | "ExceptBy" ->
                if mc.Arguments.Count >= 3 then
                    state.SetOp <- Some (SetOperation.ExceptBy(mc.Arguments.[1], mc.Arguments.[2]))
                walkChain state src

            | "UnionBy" ->
                if mc.Arguments.Count >= 3 then
                    state.SetOp <- Some (SetOperation.UnionBy(mc.Arguments.[1], mc.Arguments.[2]))
                walkChain state src

            | "ToList" | "ToArray" ->
                walkChain state src

            | "DefaultIfEmpty" ->
                if state.SelectProjection.IsNone then
                    state.PostSelectDefaultIfEmpty <- Some arg
                else
                    state.DefaultIfEmpty <- Some arg
                walkChain state src

            | "Zip" ->
                raise (NotSupportedException(
                    "Error: Zip is not supported on DBRefMany.\n" +
                    "Reason: Zip pairs two sequences positionally, which has no direct SQL translation for relation-backed collections.\n" +
                    "Fix: Call .AsEnumerable() before .Zip(), or restructure the query."))
            | "Reverse" ->
                raise (NotSupportedException(
                    "Error: Reverse is not supported on DBRefMany.\n" +
                    "Reason: Reverse requires materializing the full sequence to invert ordering, which cannot be expressed as a single SQL query.\n" +
                    "Fix: Use .OrderByDescending() for reverse ordering, or call .AsEnumerable() before .Reverse()."))
            | "Prepend" ->
                raise (NotSupportedException(
                    "Error: Prepend is not supported on DBRefMany.\n" +
                    "Reason: Prepend inserts an element at a specific position, which has no direct SQL translation for relation-backed collections.\n" +
                    "Fix: Call .AsEnumerable() before .Prepend(), or use .Concat() instead."))
            | "SequenceEqual" ->
                raise (NotSupportedException(
                    "Error: SequenceEqual is not supported on DBRefMany.\n" +
                    "Reason: SequenceEqual compares two sequences element-by-element in order, which has no direct SQL translation.\n" +
                    "Fix: Call .AsEnumerable() before .SequenceEqual(), or compare counts and individual elements."))
            | "Aggregate" ->
                raise (NotSupportedException(
                    "Error: Aggregate is not supported on DBRefMany.\n" +
                    "Reason: LINQ Aggregate (seed/accumulator fold) has no direct SQL translation. Use specific aggregates (Sum, Min, Max, Average, Count) natively.\n" +
                    "Fix: Use .Sum(), .Min(), .Max(), .Average(), or .Count() instead, or call .AsEnumerable() before .Aggregate()."))
            | "Append" ->
                raise (NotSupportedException(
                    "Error: Append is not supported on DBRefMany.\n" +
                    "Reason: Append adds a single element to a relation-backed collection, which is not yet implemented at the DBRefMany translation level.\n" +
                    "Fix: Call .AsEnumerable() before .Append(), or use the mutation API to add elements to the relation."))

            | _ ->
                e
        | _ ->
            e
