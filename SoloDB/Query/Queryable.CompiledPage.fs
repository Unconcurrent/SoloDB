namespace SoloDatabase

open SoloDatabase.SqlModel

/// A bounded order-index attempt and its exact fallback share one retained statement.
module internal CompiledPage =
    let private integer value = Literal(SqlLiteral.Integer value)
    let private column source name = Column(Some source, name)
    let private binary op left right = Binary(left, op, right)
    let private project name expr = { Alias = Some name; Expr = expr }
    let private select core = { Ctes = []; Body = SingleSelect core }
    let private materialized name core = { Name = name; Materialized = true; Query = select core }

    let private qualify source =
        SqlExpr.map (function
            | Column(None, name) -> column source name
            | JsonExtractExpr(None, name, path) -> JsonExtractExpr(Some source, name, path)
            | JsonRootExtract(None, name) -> JsonRootExtract(Some source, name)
            | node -> node)

    let tryPlan model estimates table source (core: SelectCore) =
        // Caller admits a plain limited collection rowset. Collection DDL makes
        // Id an INTEGER PRIMARY KEY, hence each index's implicit rowid key is Id.
        let isId expression =
            match FlattenTransform.normalizeExprQuoting expression with
            | Column(qualifier, "Id") -> qualifier.IsNone || qualifier = Some source
            | _ -> false
        let normalize expression =
            SqlExpr.map (function
                | Column(Some alias, name) when alias = source -> column table name
                | JsonExtractExpr(Some alias, name, path) when alias = source -> JsonExtractExpr(Some table, name, path)
                | node -> node) (FlattenTransform.normalizeExprQuoting expression)
        let indexed expression = ExpressionMatcher.hasMatchingIndex model table (normalize expression)
        let ordering = core.OrderBy |> List.map (fun order -> { order with Expr = normalize order.Expr })
        let predicate = core.Where |> Option.map normalize
        let orderIndex = ExpressionMatcher.findOrderingIndex model table None ordering
        let filterIndex = predicate |> Option.bind (ExpressionMatcher.findCoveringFilterIndex model table)
        let orderedIndex =
            match core.OrderBy with
            | [order] -> isId order.Expr
            | _ -> orderIndex.IsSome
        // One equality fixes the index's leading key; its implicit rowid then
        // supplies Id order for every argument, without an unfiltered window.
        let equalityOrdered =
            let direct = function Parameter _ | Literal _ -> true | _ -> false
            let ordered key value =
                direct value && (isId key || indexed key)
                && (core.OrderBy |> List.forall (fun order ->
                    isId order.Expr || FlattenTransform.normalizeExprQuoting order.Expr = FlattenTransform.normalizeExprQuoting key))
            match core.Where with
            | Some(Binary(left, (BinaryOperator.Eq | BinaryOperator.Is), right)) ->
                ordered left right || ordered right left
            | _ -> false
        let localFilter =
            core.Where |> Option.exists (SqlExpr.fold (fun valid node ->
                valid && match node with
                         | Exists _ | InSubquery _ | ScalarSubquery _ | AggregateCall _ | WindowCall _ -> false
                         | _ -> true) true)
        let filterProvidesOrder =
            match filterIndex, ExpressionMatcher.findOrderingIndex model table predicate ordering with
            | Some filter, Some ordered -> filter.IndexName = ordered.IndexName
            | _ -> false
        if not orderedIndex || not localFilter || equalityOrdered || filterProvidesOrder then select core else
        let windowName = table + "_page_window"
        let hitsName = table + "_page_hits"
        let stateName = table + "_page_state"
        let fallbackName = table + "_page_fallback"
        let limit = core.Limit.Value
        let offset = defaultArg core.Offset (integer 0L)
        let need = binary BinaryOperator.Add offset limit
        let validBounds = binary BinaryOperator.And
                              (binary BinaryOperator.Gt limit (integer 0L))
                              (binary BinaryOperator.Ge offset (integer 0L))
        // A fixed cap bounds extra work even for a filter clustered at the far end.
        let allowed = binary BinaryOperator.And validBounds (binary BinaryOperator.Le need (integer 4096L))
        let empty =
            { core with Source = None; Joins = []; Where = None; OrderBy = []; Limit = None; Offset = None }
        let keys = core.OrderBy |> List.mapi (fun i order -> project ("ord" + string i) (qualify source order.Expr))
        let pageProjection = project "Id" (column source "Id") :: keys
        let projected alias = pageProjection |> List.map (fun p -> project p.Alias.Value (column alias p.Alias.Value)) |> ProjectionSetOps.ofList
        let ordering alias = core.OrderBy |> List.mapi (fun i order -> { order with Expr = column alias ("ord" + string i) })
        let limitHits = filterIndex.IsSome && orderIndex.IsSome
        let window =
            { core with
                Projections = if limitHits then ProjectionSetOps.ofList pageProjection else core.Projections
                Where = None; Offset = None
                Limit = Some(CaseExpr((allowed, integer 4096L), [], Some(integer 0L))) }
        let hits =
            if limitHits then
                let matches =
                    { empty with Source = core.Source
                                 Projections = ProjectionSetOps.ofList [project "present" (integer 1L)]
                                 Where = Some(binary BinaryOperator.And
                                     (binary BinaryOperator.Eq (column source "Id") (column windowName "Id"))
                                     (qualify source core.Where.Value)) }
                // The first need ordered hits prove the same page as all hits,
                // without fetching filter payloads after sufficiency is established.
                { empty with Source = Some(BaseTable(windowName, None))
                             Projections = projected windowName; Where = Some(Exists(select matches))
                             OrderBy = ordering windowName
                             Limit = Some(CaseExpr((allowed, need), [], Some(integer 0L))) }
            else
            { empty with
                Source = Some(BaseTable(windowName, None))
                Joins = [CrossJoin core.Source.Value]
                Projections = ProjectionSetOps.ofList pageProjection
                Where = Some(binary BinaryOperator.And
                    (binary BinaryOperator.Eq (column windowName "Id") (column source "Id"))
                    (qualify source core.Where.Value)) }
        let count =
            { empty with Source = Some(BaseTable(hitsName, None))
                         Projections = ProjectionSetOps.ofList [project "count" (AggregateCall(AggregateKind.Count, None, false, None))] }
        let enough = binary BinaryOperator.And allowed (binary BinaryOperator.Ge (ScalarSubquery(select count)) need)
        let state = { empty with Projections = ProjectionSetOps.ofList [project "enough" enough] }
        let fallbackGate =
            { empty with Source = Some(BaseTable(stateName, None))
                         Projections = ProjectionSetOps.ofList [project "enabled" (integer 1L)]
                         Where = Some(Unary(UnaryOperator.Not, column stateName "enough")) }
        // Covering reads are cheaper than fetching and sorting document keys.
        // Statistics set a crossover estimate only; both execution arms are exact.
        let threshold =
            match orderIndex, filterIndex, core.OrderBy with
            | Some _, Some _, first :: _ when not (isId first.Expr) ->
                estimates |> Map.tryFind table |> Option.map (fun rows -> max 4096L (rows / 16L))
            | _ -> None
        let retryName = table + "_page_sort"
        let costCtes, membership, retrySource =
            match threshold with
            | None -> [], [], fallbackName
            | Some threshold ->
                let sizeName = table + "_page_size"
                let countRows =
                    { core with Source = Some(BaseTable(fallbackName, None)); Joins = [CrossJoin core.Source.Value]
                                Projections = ProjectionSetOps.ofList [project "present" (integer 1L)]
                                Where = core.Where |> Option.map (qualify source)
                                OrderBy = []; Offset = None
                                Limit = Some(CaseExpr((validBounds, integer threshold), [], Some(integer 0L))) }
                let size =
                    { empty with Source = Some(DerivedTable(select countRows, table + "_page_count_rows"))
                                 Projections = ProjectionSetOps.ofList [project "count" (AggregateCall(AggregateKind.Count, None, false, None))] }
                let countValue =
                    ScalarSubquery(select {
                        empty with
                            Source = Some(BaseTable(sizeName, None))
                            Projections = ProjectionSetOps.ofList [project "count" (column sizeName "count")] })
                let broad = binary BinaryOperator.And validBounds (binary BinaryOperator.Ge countValue (integer threshold))
                let retry = { fallbackGate with Where = Some(binary BinaryOperator.And
                                                    fallbackGate.Where.Value (Unary(UnaryOperator.Not, broad))) }
                let orderedAlias = table + "_page_ordered"
                let orderedRows =
                    { core with Projections = ProjectionSetOps.ofList pageProjection
                                Where = None; Offset = None; Limit = Some(integer -1L) }
                let matchingIds =
                    { core with Projections = ProjectionSetOps.ofList [project "Id" (column source "Id")]
                                OrderBy = []; Limit = None; Offset = None }
                let complementCtes, membershipPredicate =
                    let ordinary = InSubquery(column orderedAlias "Id", select matchingIds)
                    match ExpressionMatcher.tryComparisonComplement core.Where.Value with
                    | Some(key, excluded) when indexed key ->
                        let excludedArms = excluded |> List.map (fun predicate -> { matchingIds with Where = Some predicate })
                        let excludedIds = { Ctes = []; Body = UnionAllSelect(excludedArms.Head, excludedArms.Tail) }
                        let excludedName = table + "_page_excluded_count"
                        let bounded =
                            { empty with Source = Some(DerivedTable(excludedIds, table + "_excluded_ids"))
                                         Projections = ProjectionSetOps.ofList [project "present" (integer 1L)]
                                         Limit = Some(CaseExpr((broad, integer threshold), [], Some(integer 0L))) }
                        let excludedCount =
                            { empty with Source = Some(DerivedTable(select bounded, table + "_excluded_rows"))
                                         Projections = ProjectionSetOps.ofList [project "count" (AggregateCall(AggregateKind.Count, None, false, None))] }
                        let excludedValue = ScalarSubquery(select {
                            empty with Source = Some(BaseTable(excludedName, None))
                                       Projections = ProjectionSetOps.ofList [project "count" (column excludedName "count")] })
                        let smaller = binary BinaryOperator.And broad (binary BinaryOperator.Lt excludedValue (integer threshold))
                        // Broad predicates can have a much smaller excluded set.
                        // Id is non-null, so NOT IN over these disjoint ranges
                        // preserves membership while building a smaller lookup.
                        let negative = Unary(UnaryOperator.Not, InSubquery(column orderedAlias "Id", excludedIds))
                        [materialized excludedName excludedCount], CaseExpr((smaller, negative), [], Some ordinary)
                    | _ -> [], ordinary
                let orderedPage =
                    { empty with Source = Some(DerivedTable(select orderedRows, orderedAlias))
                                 Projections = projected orderedAlias
                                 Where = Some membershipPredicate
                                 OrderBy = ordering orderedAlias; Offset = core.Offset
                                 // LIMIT zero prevents entering the ordered coroutine.
                                 Limit = Some(CaseExpr((broad, limit), [], Some(integer 0L))) }
                [materialized sizeName size; materialized retryName retry] @ complementCtes, [orderedPage], retryName
        let success =
            { core with Source = Some(BaseTable(hitsName, None)); Joins = [CrossJoin(BaseTable(stateName, None))]
                        Projections = projected hitsName; Where = Some(column stateName "enough"); OrderBy = ordering hitsName }
        let fallback =
            { core with Source = Some(BaseTable(retrySource, None)); Joins = [CrossJoin core.Source.Value]
                        Projections = ProjectionSetOps.ofList [project "Id" (column source "Id")]
                        Where = core.Where |> Option.map (qualify source)
                        OrderBy = core.OrderBy |> List.map (fun order -> { order with Expr = qualify source order.Expr }) }
        // Recover ordering values for the selected fallback IDs, rather than
        // carrying duplicate key projections through the full fallback sorter.
        // Admission requires Id or an indexed deterministic expression, so the
        // recovered values equal those used for selection in this statement.
        let retryIds = table + "_page_retry_ids"
        let fallbackRows =
            { empty with Source = Some(DerivedTable(select fallback, retryIds)); Joins = [CrossJoin core.Source.Value]
                         Projections = ProjectionSetOps.ofList pageProjection
                         Where = Some(binary BinaryOperator.Eq (column retryIds "Id") (column source "Id")) }
        let arm name page =
            { empty with Source = Some(DerivedTable(select page, name)); Projections = projected name }
        let membershipArms = membership |> List.map (arm (table + "_page_membership"))
        let union = { Ctes = []; Body = UnionAllSelect(arm (table + "_page_success") success, membershipArms @ [arm (table + "_page_retry") fallbackRows]) }
        let pageAlias = table + "_page_result"
        { Ctes = [materialized windowName window; materialized hitsName hits; materialized stateName state; materialized fallbackName fallbackGate] @ costCtes
          Body = SingleSelect { empty with Source = Some(DerivedTable(union, pageAlias))
                                           Projections = ProjectionSetOps.ofList [project "Id" (column pageAlias "Id")]
                                           OrderBy = ordering pageAlias } }
