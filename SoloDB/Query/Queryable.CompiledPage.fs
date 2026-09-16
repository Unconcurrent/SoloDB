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

    let tryPlan model table source (core: SelectCore) =
        // Caller admits a plain limited collection rowset. Collection DDL makes
        // Id an INTEGER PRIMARY KEY, hence each index's implicit rowid key is Id.
        let isId expression =
            match FlattenTransform.normalizeExprQuoting expression with
            | Column(qualifier, "Id") -> qualifier.IsNone || qualifier = Some source
            | _ -> false
        let indexed expression =
            let normalized = SqlExpr.map (function
                | Column(Some alias, name) when alias = source -> column table name
                | JsonExtractExpr(Some alias, name, path) when alias = source -> JsonExtractExpr(Some table, name, path)
                | node -> node) (FlattenTransform.normalizeExprQuoting expression)
            ExpressionMatcher.hasMatchingIndex model table normalized
        let orderedIndex =
            match core.OrderBy with
            | [order] -> isId order.Expr
            | [key; id] -> key.Direction = id.Direction && isId id.Expr && indexed key.Expr
            | _ -> false
        let localFilter =
            core.Where |> Option.exists (SqlExpr.fold (fun valid node ->
                valid && match node with
                         | Exists _ | InSubquery _ | ScalarSubquery _ | AggregateCall _ | WindowCall _ -> false
                         | _ -> true) true)
        if not orderedIndex || not localFilter then select core else
        let windowName = table + "_page_window"
        let hitsName = table + "_page_hits"
        let stateName = table + "_page_state"
        let fallbackName = table + "_page_fallback"
        let limit = core.Limit.Value
        let offset = defaultArg core.Offset (integer 0L)
        let need = binary BinaryOperator.Add offset limit
        // A fixed cap bounds extra work even for a filter clustered at the far end.
        let allowed = binary BinaryOperator.And
                          (binary BinaryOperator.Gt limit (integer 0L))
                          (binary BinaryOperator.And (binary BinaryOperator.Ge offset (integer 0L))
                              (binary BinaryOperator.Le need (integer 4096L)))
        let empty =
            { core with Source = None; Joins = []; Where = None; OrderBy = []; Limit = None; Offset = None }
        let window =
            { core with
                Where = None; Offset = None
                Limit = Some(CaseExpr((allowed, integer 4096L), [], Some(integer 0L))) }
        let keys = core.OrderBy |> List.mapi (fun i order -> project ("ord" + string i) (qualify source order.Expr))
        let pageProjection = project "Id" (column source "Id") :: keys
        let hits =
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
        let projected alias = pageProjection |> List.map (fun p -> project p.Alias.Value (column alias p.Alias.Value)) |> ProjectionSetOps.ofList
        let ordering alias = core.OrderBy |> List.mapi (fun i order -> { order with Expr = column alias ("ord" + string i) })
        let success =
            { core with Source = Some(BaseTable(hitsName, None)); Joins = [CrossJoin(BaseTable(stateName, None))]
                        Projections = projected hitsName; Where = Some(column stateName "enough"); OrderBy = ordering hitsName }
        let fallback =
            { core with Source = Some(BaseTable(fallbackName, None)); Joins = [CrossJoin core.Source.Value]
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
        let union = { Ctes = []; Body = UnionAllSelect(arm (table + "_page_success") success, [arm (table + "_page_retry") fallbackRows]) }
        let pageAlias = table + "_page_result"
        { Ctes = [materialized windowName window; materialized hitsName hits; materialized stateName state; materialized fallbackName fallbackGate]
          Body = SingleSelect { empty with Source = Some(DerivedTable(union, pageAlias))
                                           Projections = ProjectionSetOps.ofList [project "Id" (column pageAlias "Id")]
                                           OrderBy = ordering pageAlias } }
