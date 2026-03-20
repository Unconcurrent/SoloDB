namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open DBRefTypeHelpers
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.DBRefManyExtractorHelpers
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost

module internal DBRefManyExtractor =
    let tryExtract (expr: Expression) : QueryDescriptor voption =
        let expr, outerDistinct, outerMaterialize = preprocessRoot expr

        match expr with
        | :? MethodCallExpression as mce ->
            let source = getSource mce
            if isNull source then ValueNone
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
                    getArg mce |> Option.map Terminal.Select
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
                | "Order" | "OrderDescending" | "UnionBy" | "IntersectBy" | "ExceptBy" | "DefaultIfEmpty" | "Cast" ->
                    let identityLambda = mkIdentityLambdaForDbRefMany mce
                    Some (Terminal.Select(identityLambda :> Expression))
                | "CountBy" -> getArg mce |> Option.map Terminal.CountBy
                | _ -> None

            match terminalOpt with
            | None -> ValueNone
            | Some terminal ->
            let source =
                match mce.Method.Name with
                | "DistinctBy" | "Order" | "OrderDescending" | "UnionBy" | "IntersectBy" | "ExceptBy" | "DefaultIfEmpty" | "Cast" ->
                    mce :> Expression  // Include the operator in the chain
                | _ -> source

            if not (isDBRefManyChain unwrapConvert isDBRefManyType source) then ValueNone
            else
            let mutable seenBoundary = false
            let mutable wheres = ResizeArray<Expression>()
            let mutable sortKeys = ResizeArray<Expression * SortDirection>()
            let mutable postBoundWheres = ResizeArray<Expression>()
            let mutable postBoundSortKeys = ResizeArray<Expression * SortDirection>()
            let mutable postBoundLimit: Expression option = None
            let mutable postBoundOffset: Expression option = None
            let mutable seenOrderBy = false
            let mutable limit: Expression option = None
            let mutable offset: Expression option = None
            let mutable takeWhileInfo: (LambdaExpression * bool) option = None
            let mutable groupByKey: LambdaExpression option = None
            let mutable distinct = false
            let mutable selectProj: LambdaExpression option = None
            let mutable setOp: SetOperation option = None
            let mutable ofTypeName: string option = None
            let mutable castTypeName: string option = None
            let mutable groupByHaving: Expression option = None
            let mutable defaultIfEmpty: Expression option option = None
            let mutable postSelectDefaultIfEmpty: Expression option option = None

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

                    | "Cast" ->
                        let genericArgs = mc.Method.GetGenericArguments()
                        if genericArgs.Length = 1 then
                            let sourceElemType =
                                if isNull src || not src.Type.IsGenericType then null
                                else src.Type.GetGenericArguments() |> Array.tryHead |> Option.defaultValue null
                            if not (isNull sourceElemType) && sourceElemType <> genericArgs.[0] then
                                DBRefManyHelpers.ensureOfTypeSupported sourceElemType
                            match Utils.typeToName genericArgs.[0] with
                            | Some tn when sourceElemType <> genericArgs.[0] -> castTypeName <- Some tn
                            | None -> ()
                            | _ -> ()
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
                        match arg with
                        | Some keySel -> setOp <- Some (SetOperation.DistinctBy keySel)
                        | None -> ()
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

                    | "DefaultIfEmpty" ->
                        // R61: DefaultIfEmpty pipeline modifier.
                        // walkChain walks outer-to-inner: if selectProj is not yet set,
                        // DefaultIfEmpty is OUTER (post-Select); if set, INNER (pre-Select).
                        if selectProj.IsNone then
                            postSelectDefaultIfEmpty <- Some arg
                        else
                            defaultIfEmpty <- Some arg
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

            // R61: If no Select in chain, DefaultIfEmpty captured as postSelect is actually pre-Select.
            if selectProj.IsNone && postSelectDefaultIfEmpty.IsSome && defaultIfEmpty.IsNone then
                defaultIfEmpty <- postSelectDefaultIfEmpty
                postSelectDefaultIfEmpty <- None

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

            // Inject Count(pred) predicate as a WHERE clause so the correlated COUNT is owner-scoped.
            match countPredicate with
            | Some pred -> wheres.Add(pred)
            | None -> ()

            ValueSome(
                buildDescriptor
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
                    groupByKey
                    (distinct || outerDistinct)
                    finalSelectProj
                    setOp
                    finalTerminal
                    finalGroupByHaving
                    defaultIfEmpty
                    postSelectDefaultIfEmpty)

        // Non-MethodCall expressions (MemberExpression for .Count property, etc.)
        | :? MemberExpression as me when me.Member.Name = "Count" && not (isNull me.Expression) ->
            tryBuildCountPropertyDescriptor unwrapConvert isDBRefManyType me

        | :? MemberExpression as me when outerMaterialize ->
            let memberExpr = unwrapConvert (me :> Expression)
            tryBuildMaterializeDescriptor isDBRefManyType memberExpr

        | _ -> ValueNone
