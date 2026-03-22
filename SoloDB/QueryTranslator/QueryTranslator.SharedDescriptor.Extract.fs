namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

module internal SharedDescriptorExtract =
    type RecognizedTerminal =
        {
            Terminal: Terminal
            Source: Expression
            CountPredicate: Expression option
        }

    type ExtractorConfig =
        {
            EnsureOfTypeSupported: Type -> unit
            MultipleTakeSkipBoundariesMessage: string
            TooManyTakeWhileBoundariesMessage: string
        }

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
                    | None -> mkIdentityLambdaForSequence src :> Expression
                | _ -> mkIdentityLambdaForSequence src :> Expression
            if state.SeenBoundary then state.SortKeys.Insert(0, (key, dir))
            else state.PostBoundSortKeys.Insert(0, (key, dir))

    let private captureTypeFilter (config: ExtractorConfig) (state: ExtractionState) (src: Expression) (targetType: Type) (isCast: bool) =
        let sourceElemType = getSourceElementType src
        if not (isNull sourceElemType) then
            if not isCast || sourceElemType <> targetType then
                config.EnsureOfTypeSupported sourceElemType
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
        // All non-Count/LongCount terminals pass through unchanged.
        | (Terminal.Any _, _) | (Terminal.All _, _) | (Terminal.Sum _, _) | (Terminal.SumProjected, _)
        | (Terminal.Min _, _) | (Terminal.MinProjected, _) | (Terminal.Max _, _) | (Terminal.MaxProjected, _)
        | (Terminal.Average _, _) | (Terminal.AverageProjected, _) | (Terminal.Select _, _) | (Terminal.Contains _, _)
        | (Terminal.Exists, _) | (Terminal.First _, _) | (Terminal.FirstOrDefault _, _)
        | (Terminal.Last _, _) | (Terminal.LastOrDefault _, _) | (Terminal.Single _, _) | (Terminal.SingleOrDefault _, _)
        | (Terminal.MinBy _, _) | (Terminal.MaxBy _, _) | (Terminal.DistinctBy _, _)
        | (Terminal.ElementAt _, _) | (Terminal.ElementAtOrDefault _, _) | (Terminal.CountBy _, _) -> source
        // Count/LongCount without CountBy source also passes through.
        | (Terminal.Count, _) | (Terminal.LongCount, _) -> source

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

    let tryRecognizeTerminal (mce: MethodCallExpression) : RecognizedTerminal option =
        let source = getSource mce
        if isNull source then
            None
        else
            let mutable countPredicate: Expression option = None
            let terminalOpt =
                match mce.Method.Name with
                | "Any" ->
                    match getArg mce with
                    | Some pred -> Some (Terminal.Any(Some pred))
                    | None -> Some Terminal.Exists
                | "All" ->
                    match getArg mce with
                    | Some pred -> Some (Terminal.All pred)
                    | None -> Some Terminal.Exists
                | "Count" | "LongCount" ->
                    let t = if mce.Method.Name = "Count" then Terminal.Count else Terminal.LongCount
                    countPredicate <- getArg mce
                    Some t
                | "Sum" ->
                    match getArg mce with
                    | Some sel -> Some (Terminal.Sum sel)
                    | None -> Some Terminal.SumProjected
                | "Min" ->
                    match getArg mce with
                    | Some sel -> Some (Terminal.Min sel)
                    | None -> Some Terminal.MinProjected
                | "Max" ->
                    match getArg mce with
                    | Some sel -> Some (Terminal.Max sel)
                    | None -> Some Terminal.MaxProjected
                | "Average" ->
                    match getArg mce with
                    | Some sel -> Some (Terminal.Average sel)
                    | None -> Some Terminal.AverageProjected
                | "Contains" -> getArg mce |> Option.map Terminal.Contains
                | "Select" -> getArg mce |> Option.map Terminal.Select
                | "First" -> Some (Terminal.First(getArg mce))
                | "FirstOrDefault" -> Some (Terminal.FirstOrDefault(getArg mce))
                | "Last" -> Some (Terminal.Last(getArg mce))
                | "LastOrDefault" -> Some (Terminal.LastOrDefault(getArg mce))
                | "Single" -> Some (Terminal.Single(getArg mce))
                | "SingleOrDefault" -> Some (Terminal.SingleOrDefault(getArg mce))
                | "ElementAt" -> getArg mce |> Option.map Terminal.ElementAt
                | "ElementAtOrDefault" -> getArg mce |> Option.map Terminal.ElementAtOrDefault
                | "MinBy" -> getArg mce |> Option.map Terminal.MinBy
                | "MaxBy" -> getArg mce |> Option.map Terminal.MaxBy
                | "DistinctBy" -> getArg mce |> Option.map Terminal.DistinctBy
                | "Order" | "OrderDescending" | "UnionBy" | "IntersectBy" | "ExceptBy" | "DefaultIfEmpty" | "Cast" ->
                    let identityLambda = mkIdentityLambdaForSequence mce
                    Some (Terminal.Select(identityLambda :> Expression))
                | "CountBy" -> getArg mce |> Option.map Terminal.CountBy
                | _ -> None

            terminalOpt
            |> Option.map (fun terminal ->
                let source =
                    match mce.Method.Name with
                    | "DistinctBy" | "Order" | "OrderDescending" | "UnionBy" | "IntersectBy" | "ExceptBy" | "DefaultIfEmpty" | "Cast" ->
                        mce :> Expression
                    | _ -> source
                {
                    Terminal = terminal
                    Source = source
                    CountPredicate = countPredicate
                })

    let rec walkChain (config: ExtractorConfig) (state: ExtractionState) (e: Expression) : Expression =
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
                walkChain config state src

            | "OrderBy" | "OrderByDescending" ->
                if not state.SeenOrderBy then
                    state.SeenOrderBy <- true
                    let dir = if mc.Method.Name = "OrderBy" then SortDirection.Asc else SortDirection.Desc
                    match arg with
                    | Some key ->
                        if state.SeenBoundary then state.SortKeys.Insert(0, (key, dir))
                        else state.PostBoundSortKeys.Insert(0, (key, dir))
                    | None -> ()
                walkChain config state src

            | "ThenBy" | "ThenByDescending" ->
                if not state.SeenOrderBy then
                    let dir = if mc.Method.Name = "ThenBy" then SortDirection.Asc else SortDirection.Desc
                    match arg with
                    | Some key ->
                        if state.SeenBoundary then state.SortKeys.Add(key, dir)
                        else state.PostBoundSortKeys.Add(key, dir)
                    | None -> ()
                walkChain config state src

            | "Take" ->
                if state.SeenBoundary then
                    raise (NotSupportedException(config.MultipleTakeSkipBoundariesMessage))
                flushBoundary state
                state.Limit <- arg
                state.Offset <- None
                walkChain config state src

            | "Skip" ->
                if state.SeenBoundary
                   && not (state.Limit.IsSome && state.Offset.IsNone && state.PostBoundLimit.IsNone && state.PostBoundOffset.IsNone) then
                    raise (NotSupportedException(config.MultipleTakeSkipBoundariesMessage))
                flushBoundary state
                state.Offset <- arg
                state.Limit <- None
                walkChain config state src

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
                            raise (NotSupportedException(config.TooManyTakeWhileBoundariesMessage))
                    | ValueNone -> ()
                | None -> ()
                walkChain config state src

            | "Select" ->
                match arg with
                | Some proj ->
                    match tryExtractLambdaExpression proj with
                    | ValueSome lambda -> state.SelectProjection <- Some lambda
                    | ValueNone -> ()
                | None -> ()
                walkChain config state src

            | "Distinct" ->
                state.Distinct <- true
                walkChain config state src

            | "GroupBy" ->
                match arg with
                | Some key ->
                    match tryExtractLambdaExpression key with
                    | ValueSome lambda -> state.GroupByKey <- Some lambda
                    | ValueNone -> ()
                | None -> ()
                walkChain config state src

            | "OfType" ->
                let genericArgs = mc.Method.GetGenericArguments()
                if genericArgs.Length = 1 then
                    captureTypeFilter config state src genericArgs.[0] false
                walkChain config state src

            | "Cast" ->
                let genericArgs = mc.Method.GetGenericArguments()
                if genericArgs.Length = 1 then
                    captureTypeFilter config state src genericArgs.[0] true
                walkChain config state src

            | "Intersect" ->
                match arg with
                | Some rightSrc -> state.SetOps.Insert(0, SetOperation.Intersect rightSrc)
                | None -> ()
                walkChain config state src

            | "Except" ->
                match arg with
                | Some rightSrc -> state.SetOps.Insert(0, SetOperation.Except rightSrc)
                | None -> ()
                walkChain config state src

            | "Union" ->
                match arg with
                | Some rightSrc -> state.SetOps.Insert(0, SetOperation.Union rightSrc)
                | None -> ()
                walkChain config state src

            | "Concat" ->
                match arg with
                | Some rightSrc -> state.SetOps.Insert(0, SetOperation.Concat rightSrc)
                | None -> ()
                walkChain config state src

            | "DistinctBy" ->
                match arg with
                | Some keySel -> state.SetOps.Insert(0, SetOperation.DistinctBy keySel)
                | None -> ()
                walkChain config state src

            | "Order" ->
                addIdentityOrder state src SortDirection.Asc
                walkChain config state src

            | "OrderDescending" ->
                addIdentityOrder state src SortDirection.Desc
                walkChain config state src

            | "IntersectBy" ->
                if mc.Arguments.Count >= 3 then
                    state.SetOps.Insert(0, SetOperation.IntersectBy(mc.Arguments.[1], mc.Arguments.[2]))
                walkChain config state src

            | "ExceptBy" ->
                if mc.Arguments.Count >= 3 then
                    state.SetOps.Insert(0, SetOperation.ExceptBy(mc.Arguments.[1], mc.Arguments.[2]))
                walkChain config state src

            | "UnionBy" ->
                if mc.Arguments.Count >= 3 then
                    state.SetOps.Insert(0, SetOperation.UnionBy(mc.Arguments.[1], mc.Arguments.[2]))
                walkChain config state src

            | "ToList" | "ToArray" ->
                walkChain config state src

            | "SelectMany" ->
                // Extract the inner lambda for multi-hop DBRefMany flattening.
                // Only handle the simple case: one collection selector, no result selector.
                match arg with
                | Some selectorExpr ->
                    match tryExtractLambdaExpression selectorExpr with
                    | ValueSome lambda when state.SelectManyLambda.IsNone ->
                        state.SelectManyLambda <- Some lambda
                        // Extract inner Select projection so set-op/terminal builders project the right value.
                        if state.SelectProjection.IsNone then
                            let rec findInnerSelect (e: Expression) =
                                let e = unwrapConvert e
                                match e with
                                | :? MethodCallExpression as imc when imc.Method.Name = "Select" ->
                                    let iarg =
                                        if not (isNull imc.Object) then
                                            if imc.Arguments.Count >= 1 then Some imc.Arguments.[0] else None
                                        elif imc.Arguments.Count >= 2 then Some imc.Arguments.[1]
                                        else None
                                    match iarg with
                                    | Some proj -> tryExtractLambdaExpression proj
                                    | None -> ValueNone
                                | :? MethodCallExpression as imc ->
                                    let isrc =
                                        if not (isNull imc.Object) then imc.Object
                                        elif imc.Arguments.Count > 0 then imc.Arguments.[0]
                                        else null
                                    if isNull isrc then ValueNone else findInnerSelect isrc
                                | _ -> ValueNone
                            match findInnerSelect lambda.Body with
                            | ValueSome sel -> state.SelectProjection <- Some sel
                            | ValueNone -> ()
                    | _ -> ()
                | None -> ()
                walkChain config state src

            | "DefaultIfEmpty" ->
                if state.SelectProjection.IsNone then
                    state.PostSelectDefaultIfEmpty <- Some arg
                else
                    state.DefaultIfEmpty <- Some arg
                walkChain config state src

            | "Zip" ->
                raise (NotSupportedException(
                    "Error: Zip is not supported on relation-backed sequences.\n" +
                    "Reason: Zip pairs two sequences positionally, which has no direct SQL translation.\n" +
                    "Fix: Call .AsEnumerable() before .Zip(), or restructure the query."))
            | "Reverse" ->
                raise (NotSupportedException(
                    "Error: Reverse is not supported on relation-backed sequences.\n" +
                    "Reason: Reverse requires materializing the full sequence to invert ordering, which cannot be expressed as a single SQL query.\n" +
                    "Fix: Use .OrderByDescending() for reverse ordering, or call .AsEnumerable() before .Reverse()."))
            | "Prepend" ->
                raise (NotSupportedException(
                    "Error: Prepend is not supported on relation-backed sequences.\n" +
                    "Reason: Prepend inserts an element at a specific position, which has no direct SQL translation.\n" +
                    "Fix: Call .AsEnumerable() before .Prepend(), or use .Concat() instead."))
            | "SequenceEqual" ->
                raise (NotSupportedException(
                    "Error: SequenceEqual is not supported on relation-backed sequences.\n" +
                    "Reason: SequenceEqual compares two sequences element-by-element in order, which has no direct SQL translation.\n" +
                    "Fix: Call .AsEnumerable() before .SequenceEqual(), or compare counts and individual elements."))
            | "Aggregate" ->
                raise (NotSupportedException(
                    "Error: Aggregate is not supported on relation-backed sequences.\n" +
                    "Reason: LINQ Aggregate (seed/accumulator fold) has no direct SQL translation.\n" +
                    "Fix: Use specific aggregates, or call .AsEnumerable() before .Aggregate()."))
            | "Append" ->
                raise (NotSupportedException(
                    "Error: Append is not supported on relation-backed sequences.\n" +
                    "Reason: Append adds a single element to a relation-backed sequence, which is not yet implemented in SQL translation.\n" +
                    "Fix: Call .AsEnumerable() before .Append()."))

            | _ ->
                e
        | _ ->
            e
