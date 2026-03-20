namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open DBRefTypeHelpers
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

/// Unified extraction: walks a DBRefMany expression tree from terminal inward,
/// collecting ALL operators into a QueryDescriptor regardless of order.
module internal DBRefManyExtractor =

    let private isDecimalLikeType (t: Type) =
        let t =
            match Nullable.GetUnderlyingType t with
            | null -> t
            | underlying -> underlying
        t = typeof<decimal>

    let private mkIdentityLambdaForDbRefMany (expr: Expression) =
        let targetType =
            match expr.Type.GetGenericArguments() |> Array.tryHead with
            | Some t -> t
            | None -> raise (InvalidOperationException("Could not resolve DBRefMany target type for identity materialization."))
        let p = Expression.Parameter(targetType)
        Expression.Lambda(p, [| p |])

    /// Extract the source expression from a MethodCallExpression.
    let private getSource (mce: MethodCallExpression) =
        if not (isNull mce.Object) then mce.Object
        elif mce.Arguments.Count > 0 then mce.Arguments.[0]
        else null

    /// Extract the first argument (predicate/selector/value) from a MethodCallExpression.
    let private getArg (mce: MethodCallExpression) =
        if not (isNull mce.Object) then
            if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
        elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
        else None

    /// Check if an expression is a DBRefMany type or wraps one via OfType/OrderBy/Where etc.
    let rec private isDBRefManyChain (expr: Expression) : bool =
        let e = unwrapConvert expr
        if isDBRefManyType e.Type then true
        else
            match e with
            | :? MethodCallExpression as mc ->
                let src = getSource mc
                not (isNull src) && isDBRefManyChain src
            | _ -> false

    /// Try to extract a terminal + operator chain from an expression.
    /// Returns None if the expression is not a DBRefMany chain.
    let tryExtract (expr: Expression) : QueryDescriptor voption =
        // First, identify the terminal and peel it.
        // Pre-peel Distinct, ToList, ToArray from the outermost expression.
        let mutable outerDistinct = false
        let mutable outerMaterialize = false
        let rec preProcess (e: Expression) =
            match e with
            | :? MethodCallExpression as mc when mc.Method.Name = "Distinct" ->
                outerDistinct <- true
                let src = getSource mc
                if isNull src then e else preProcess src
            | :? MethodCallExpression as mc when mc.Method.Name = "ToList" || mc.Method.Name = "ToArray" ->
                outerMaterialize <- true
                let src = getSource mc
                if isNull src then e else preProcess src
            | _ -> e
        let expr = preProcess expr

        match expr with
        | :? MethodCallExpression as mce ->
            let source = getSource mce
            if isNull source then ValueNone
            else
            let mutable countPredicate: Expression option = None

            // Determine terminal type.
            let terminalOpt =
                match mce.Method.Name with
                | "Any" ->
                    match getArg mce with
                    | Some pred -> Some (Terminal.Any(Some pred))
                    | None -> Some Terminal.Exists
                | "All" ->
                    match getArg mce with
                    | Some pred -> Some (Terminal.All pred)
                    | None -> Some Terminal.Exists // All() without pred = trivially true
                | "Count" | "LongCount" ->
                    let t = if mce.Method.Name = "Count" then Terminal.Count else Terminal.LongCount
                    // When Count has a predicate, capture it — it will be injected as WHERE
                    // on the correlated subquery so COUNT applies only to matching rows.
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
                | "Select" ->
                    // Select as terminal: materializes DBRefMany to json_group_array.
                    getArg mce |> Option.map Terminal.Select
                // L4a element-access terminals.
                | "First" -> Some (Terminal.First(getArg mce))
                | "FirstOrDefault" -> Some (Terminal.FirstOrDefault(getArg mce))
                | "Last" -> Some (Terminal.Last(getArg mce))
                | "LastOrDefault" -> Some (Terminal.LastOrDefault(getArg mce))
                | "Single" -> Some (Terminal.Single(getArg mce))
                | "SingleOrDefault" -> Some (Terminal.SingleOrDefault(getArg mce))
                | "ElementAt" -> getArg mce |> Option.map Terminal.ElementAt
                | "ElementAtOrDefault" -> getArg mce |> Option.map Terminal.ElementAtOrDefault
                // R55: MinBy/MaxBy — return the element with min/max key.
                | "MinBy" -> getArg mce |> Option.map Terminal.MinBy
                | "MaxBy" -> getArg mce |> Option.map Terminal.MaxBy
                | "DistinctBy" -> getArg mce |> Option.map Terminal.DistinctBy
                // R55: Intermediate operators as outermost — use identity Select as terminal.
                // The source is set to mce itself (not mce's source) so walkChain processes the operator.
                | "Order" | "OrderDescending" | "UnionBy" | "IntersectBy" | "ExceptBy" ->
                    let identityLambda = mkIdentityLambdaForDbRefMany mce
                    Some (Terminal.Select(identityLambda :> Expression))
                | "CountBy" ->
                    raise (NotSupportedException(
                        "Error: CountBy is not supported in DBRefMany queries.\n" +
                        "Reason: CountBy requires GroupBy + KeyValuePair projection which is not available in correlated subqueries.\n" +
                        "Fix: Call AsEnumerable() before CountBy, or use GroupBy(key).Select(g => new { g.Key, Count = g.Count() })."))
                | _ -> None

            match terminalOpt with
            | None -> ValueNone
            | Some terminal ->

            // For R55 intermediate-as-terminal operators, the source must include the operator itself
            // so walkChain processes it. For normal terminals, source = getSource(mce) which skips the terminal.
            let source =
                match mce.Method.Name with
                | "DistinctBy" | "Order" | "OrderDescending" | "UnionBy" | "IntersectBy" | "ExceptBy" ->
                    mce :> Expression  // Include the operator in the chain
                | _ -> source

            // Check if the source chain involves DBRefMany.
            if not (isDBRefManyChain source) then ValueNone
            else

            // Walk the source chain inward, collecting operators.
            // seenBoundary: once Take/Skip is encountered, all subsequent operators are inner (pre-bound).
            // Operators encountered BEFORE Take/Skip (outermost-first) are post-bound (outer scope).
            let mutable seenBoundary = false
            let mutable wheres = ResizeArray<Expression>()
            let mutable sortKeys = ResizeArray<Expression * SortDirection>()
            let mutable postBoundWheres = ResizeArray<Expression>()
            let mutable postBoundSortKeys = ResizeArray<Expression * SortDirection>()
            let mutable postBoundLimit: Expression option = None
            let mutable postBoundOffset: Expression option = None
            // Track whether we've seen an OrderBy during outermost-first walk.
            // Once seen, any inner OrderBy/ThenBy belongs to a replaced sort scope and must be skipped.
            let mutable seenOrderBy = false
            let mutable limit: Expression option = None
            let mutable offset: Expression option = None
            let mutable takeWhileInfo: (LambdaExpression * bool) option = None
            let mutable groupByKey: LambdaExpression option = None
            let mutable distinct = false
            let mutable selectProj: LambdaExpression option = None
            let mutable setOp: SetOperation option = None
            let mutable ofTypeName: string option = None
            let mutable groupByHaving: Expression option = None

            let rec walkChain (e: Expression) : Expression =
                let e = unwrapConvert e
                match e with
                | :? MethodCallExpression as mc ->
                    let src = getSource mc
                    let arg = getArg mc

                    match mc.Method.Name with
                    | "Where" ->
                        match arg with
                        | Some pred ->
                            if seenBoundary then wheres.Add(pred)
                            else postBoundWheres.Add(pred)
                        | None -> ()
                        walkChain src

                    | "OrderBy" | "OrderByDescending" ->
                        if seenOrderBy then
                            // Inner/replaced OrderBy — skip (LINQ: second OrderBy replaces first).
                            ()
                        else
                            seenOrderBy <- true
                            let dir = if mc.Method.Name = "OrderBy" then SortDirection.Asc else SortDirection.Desc
                            match arg with
                            | Some key ->
                                if seenBoundary then sortKeys.Insert(0, (key, dir))
                                else postBoundSortKeys.Insert(0, (key, dir))
                            | None -> ()
                        walkChain src

                    | "ThenBy" | "ThenByDescending" ->
                        if seenOrderBy then
                            // ThenBy from an inner/replaced sort scope — skip.
                            ()
                        else
                            let dir = if mc.Method.Name = "ThenBy" then SortDirection.Asc else SortDirection.Desc
                            match arg with
                            | Some key ->
                                if seenBoundary then sortKeys.Add(key, dir)
                                else postBoundSortKeys.Add(key, dir)
                            | None -> ()
                        walkChain src

                    | "Take" ->
                        if seenBoundary then
                            raise (NotSupportedException(
                                "Error: Multiple Take/Skip boundaries in DBRefMany query are not supported.\nReason: The descriptor model admits only one semantic pagination boundary.\nFix: Keep at most one Take or Skip in the DBRefMany chain, or move additional pagination after AsEnumerable()."))
                        // Flush all previously collected operators to postBound.
                        // This ensures that operators OUTER to this Take are applied AFTER bounding.
                        seenBoundary <- true
                        postBoundWheres.AddRange(wheres); wheres.Clear()
                        postBoundSortKeys.AddRange(sortKeys); sortKeys.Clear()
                        if limit.IsSome then postBoundLimit <- limit
                        if offset.IsSome then postBoundOffset <- offset
                        limit <- arg
                        offset <- None
                        walkChain src

                    | "Skip" ->
                        if seenBoundary
                           && not (limit.IsSome && offset.IsNone && postBoundLimit.IsNone && postBoundOffset.IsNone) then
                            raise (NotSupportedException(
                                "Error: Multiple Take/Skip boundaries in DBRefMany query are not supported.\nReason: The descriptor model admits only one semantic pagination boundary.\nFix: Keep at most one Take or Skip in the DBRefMany chain, or move additional pagination after AsEnumerable()."))
                        // Flush all previously collected operators to postBound.
                        seenBoundary <- true
                        postBoundWheres.AddRange(wheres); wheres.Clear()
                        postBoundSortKeys.AddRange(sortKeys); sortKeys.Clear()
                        if limit.IsSome then postBoundLimit <- limit
                        if offset.IsSome then postBoundOffset <- offset
                        offset <- arg
                        limit <- None
                        walkChain src

                    | "TakeWhile" | "SkipWhile" ->
                        match arg with
                        | Some pred ->
                            match tryExtractLambdaExpression pred with
                            | ValueSome lambda ->
                                takeWhileInfo <- Some (lambda, mc.Method.Name = "TakeWhile")
                            | ValueNone -> ()
                        | None -> ()
                        walkChain src

                    | "Select" ->
                        match arg with
                        | Some proj ->
                            match tryExtractLambdaExpression proj with
                            | ValueSome lambda -> selectProj <- Some lambda
                            | ValueNone -> ()
                        | None -> ()
                        walkChain src

                    | "Distinct" ->
                        distinct <- true
                        walkChain src

                    | "GroupBy" ->
                        match arg with
                        | Some key ->
                            match tryExtractLambdaExpression key with
                            | ValueSome lambda -> groupByKey <- Some lambda
                            | ValueNone -> ()
                        | None -> ()
                        walkChain src

                    | "OfType" ->
                        let genericArgs = mc.Method.GetGenericArguments()
                        if genericArgs.Length = 1 then
                            let sourceElemType =
                                if isNull src || not src.Type.IsGenericType then null
                                else src.Type.GetGenericArguments() |> Array.tryHead |> Option.defaultValue null
                            if not (isNull sourceElemType) then
                                DBRefManyHelpers.ensureOfTypeSupported sourceElemType
                            match Utils.typeToName genericArgs.[0] with
                            | Some tn -> ofTypeName <- Some tn
                            | None -> ()
                        walkChain src

                    | "Intersect" ->
                        match arg with
                        | Some rightSrc -> setOp <- Some (SetOperation.Intersect rightSrc)
                        | None -> ()
                        walkChain src

                    | "Except" ->
                        match arg with
                        | Some rightSrc -> setOp <- Some (SetOperation.Except rightSrc)
                        | None -> ()
                        walkChain src

                    | "Union" ->
                        match arg with
                        | Some rightSrc -> setOp <- Some (SetOperation.Union rightSrc)
                        | None -> ()
                        walkChain src

                    | "Concat" ->
                        match arg with
                        | Some rightSrc -> setOp <- Some (SetOperation.Concat rightSrc)
                        | None -> ()
                        walkChain src

                    // Newer .NET LINQ operators — R55 DBRefMany parity.
                    | "DistinctBy" ->
                        // Terminal.DistinctBy already carries the key selector.
                        // Do not set groupByKey here, or the descriptor is misrouted into the GroupBy builder.
                        walkChain src

                    | "Order" ->
                        // Parameterless Order — sort by identity (IComparable).
                        if not seenOrderBy then
                            seenOrderBy <- true
                            let key =
                                match src with
                                | :? MethodCallExpression as srcMc when srcMc.Method.Name = "Select" ->
                                    match getArg srcMc with
                                    | Some selectorExpr -> selectorExpr
                                    | None -> mkIdentityLambdaForDbRefMany src :> Expression
                                | _ -> mkIdentityLambdaForDbRefMany src :> Expression
                            if seenBoundary then sortKeys.Insert(0, (key, SortDirection.Asc))
                            else postBoundSortKeys.Insert(0, (key, SortDirection.Asc))
                        walkChain src

                    | "OrderDescending" ->
                        if not seenOrderBy then
                            seenOrderBy <- true
                            let key =
                                match src with
                                | :? MethodCallExpression as srcMc when srcMc.Method.Name = "Select" ->
                                    match getArg srcMc with
                                    | Some selectorExpr -> selectorExpr
                                    | None -> mkIdentityLambdaForDbRefMany src :> Expression
                                | _ -> mkIdentityLambdaForDbRefMany src :> Expression
                            if seenBoundary then sortKeys.Insert(0, (key, SortDirection.Desc))
                            else postBoundSortKeys.Insert(0, (key, SortDirection.Desc))
                        walkChain src

                    | "IntersectBy" ->
                        // IntersectBy(source, rightKeys, keySelector) — 3 args
                        if mc.Arguments.Count >= 3 then
                            setOp <- Some (SetOperation.IntersectBy(mc.Arguments.[1], mc.Arguments.[2]))
                        walkChain src

                    | "ExceptBy" ->
                        if mc.Arguments.Count >= 3 then
                            setOp <- Some (SetOperation.ExceptBy(mc.Arguments.[1], mc.Arguments.[2]))
                        walkChain src

                    | "UnionBy" ->
                        if mc.Arguments.Count >= 3 then
                            setOp <- Some (SetOperation.UnionBy(mc.Arguments.[1], mc.Arguments.[2]))
                        walkChain src

                    | "ToList" | "ToArray" ->
                        // Identity passthrough (L11).
                        walkChain src

                    | _ ->
                        // Unknown operator — stop walking, return current expression as source.
                        e
                | _ ->
                    // Not a method call — this should be the DBRefMany source (member access).
                    e

            let innerSource = walkChain source

            // If no Take/Skip boundary was encountered, all operators are in one scope.
            // Merge postBound buffers back to inner fields.
            if not seenBoundary then
                wheres.AddRange(postBoundWheres); postBoundWheres.Clear()
                sortKeys.AddRange(postBoundSortKeys); postBoundSortKeys.Clear()

            match countPredicate with
            | Some pred when seenBoundary -> postBoundWheres.Add(pred)
            | Some pred -> wheres.Add(pred)
            | None -> ()

            // Handle GroupBy terminal: extract the HAVING predicate from Any/All.
            let finalTerminal, finalGroupByHaving =
                match groupByKey with
                | Some _ ->
                    match terminal with
                    | Terminal.Any(Some pred) -> Terminal.Exists, Some pred
                    | Terminal.All pred -> Terminal.All pred, Some pred  // Keep All for negation in builder
                    | _ -> terminal, None
                | None -> terminal, None

            // If Select is the terminal, capture its projection.
            let finalSelectProj =
                match finalTerminal with
                | Terminal.Select projExpr when selectProj.IsNone ->
                    match tryExtractLambdaExpression projExpr with
                    | ValueSome lambda -> Some lambda
                    | ValueNone -> selectProj
                | _ -> selectProj

            match finalTerminal with
            | Terminal.Average selectorExpr ->
                match tryExtractLambdaExpression selectorExpr with
                | ValueSome selectorLambda when isDecimalLikeType selectorLambda.Body.Type ->
                    raise (NotSupportedException(
                        "Error: Decimal Average over DBRefMany is not supported.\nReason: SQLite AVG is not exact for decimal semantics on this route.\nFix: Use Sum/Count in-memory after AsEnumerable(), or project to a supported numeric type."))
                | _ -> ()
            | Terminal.AverageProjected ->
                match finalSelectProj with
                | Some proj when isDecimalLikeType proj.Body.Type ->
                    raise (NotSupportedException(
                        "Error: Decimal Average over DBRefMany is not supported.\nReason: SQLite AVG is not exact for decimal semantics on this route.\nFix: Use Sum/Count in-memory after AsEnumerable(), or project to a supported numeric type."))
                | _ -> ()
            | _ -> ()

            // Inject Count(pred) predicate as a WHERE clause so the correlated COUNT is owner-scoped.
            match countPredicate with
            | Some pred -> wheres.Add(pred)
            | None -> ()

            ValueSome {
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
                Distinct = distinct || outerDistinct
                SelectProjection = finalSelectProj
                SetOp = setOp
                Terminal = finalTerminal
                GroupByHavingPredicate = finalGroupByHaving
            }

        // Non-MethodCall expressions (MemberExpression for .Count property, etc.)
        | :? MemberExpression as me when me.Member.Name = "Count" && not (isNull me.Expression) ->
            let inner = unwrapConvert me.Expression
            if isDBRefManyType inner.Type then
                ValueSome {
                    Source = inner
                    OfTypeName = None; WherePredicates = []; SortKeys = []
                    Limit = None; Offset = None
                    PostBoundWherePredicates = []; PostBoundSortKeys = []
                    PostBoundLimit = None; PostBoundOffset = None
                    TakeWhileInfo = None
                    GroupByKey = None; Distinct = false; SelectProjection = None
                    SetOp = None; Terminal = Terminal.Count
                    GroupByHavingPredicate = None
                }
            else ValueNone

        | :? MemberExpression as me when outerMaterialize ->
            let memberExpr = unwrapConvert (me :> Expression)
            if isDBRefManyType memberExpr.Type then
                let identity = mkIdentityLambdaForDbRefMany memberExpr
                ValueSome {
                    Source = memberExpr
                    OfTypeName = None; WherePredicates = []; SortKeys = []
                    Limit = None; Offset = None
                    PostBoundWherePredicates = []; PostBoundSortKeys = []
                    PostBoundLimit = None; PostBoundOffset = None
                    TakeWhileInfo = None
                    GroupByKey = None; Distinct = false; SelectProjection = Some identity
                    SetOp = None; Terminal = Terminal.Select(identity :> Expression)
                    GroupByHavingPredicate = None
                }
            else ValueNone

        | _ -> ValueNone
