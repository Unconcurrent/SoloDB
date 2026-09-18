namespace SoloDatabase

open System
open SoloDatabase.SqlModel

/// Shared page planning; retained queries pay translation only at compilation.
module internal QueryReadPlanning =
    let private rowExpression expression =
        not (SqlExpr.exists (function
            | AggregateCall _ | WindowCall _ | ScalarSubquery _ | Exists _ | InSubquery _ -> true
            | _ -> false) expression)

    let private plainRows (core: SelectCore) =
        not core.Distinct && core.Joins.IsEmpty && core.GroupBy.IsEmpty && core.Having.IsNone

    let private projectionOnly (core: SelectCore) =
        plainRows core && core.Where.IsNone && core.OrderBy.IsEmpty && core.Limit.IsNone && core.Offset.IsNone

    let private baseColumn source name expression =
        match FlattenTransform.normalizeExprQuoting expression with
        | Column(qualifier, column) -> column = name && (qualifier.IsNone || qualifier = Some source)
        | _ -> false

    let private fromSource source (qualifier: string option) = qualifier.IsNone || qualifier = Some source

    let private simplePath path =
        JsonPathOps.toList path |> List.forall (fun segment ->
            not (String.IsNullOrEmpty segment)
            && (segment |> Seq.forall (fun c -> Char.IsLetterOrDigit c || c = '_' || c = '$')))

    let private readsDocument source expression =
        match FlattenTransform.normalizeExprQuoting expression with
        | JsonExtractExpr(qualifier, "Value", path) -> fromSource source qualifier && simplePath path
        | JsonRootExtract(qualifier, "Value") -> fromSource source qualifier
        | _ -> false

    let rec private movableValue (model: IndexModel.IndexModel) table source (core: SelectCore) expression =
        let indexed expression =
            let normalized = expression |> SqlExpr.map (function
                | JsonExtractExpr(Some alias, name, path) when alias = source -> JsonExtractExpr(Some table, name, path)
                | Column(Some alias, name) when alias = source -> Column(Some table, name)
                | node -> node)
            model.Indexes |> List.exists (fun entry ->
                entry.TableName = table && (entry.Terms |> List.exists (fun term ->
                    String.Equals(term.Collation, "BINARY", StringComparison.OrdinalIgnoreCase)
                    && ExpressionMatcher.expressionMatchesIndex table normalized term.Expression)))
        match FlattenTransform.normalizeExprQuoting expression with
        | Literal _ | Parameter _ -> true
        | Column(qualifier, ("Id" | "Value")) -> fromSource source qualifier
        | expression when readsDocument source expression ->
            // A sorter may evaluate projections on discarded rows. Preserve parse
            // errors through the same extraction in the order key: JSONB can
            // validate one path without validating another in the same document.
            let sameRead order =
                readsDocument source order.Expr &&
                match expression, FlattenTransform.normalizeExprQuoting order.Expr with
                | JsonExtractExpr(_, _, path), JsonExtractExpr(_, _, orderPath) -> path = orderPath
                | JsonRootExtract _, JsonRootExtract _ -> true
                | _ -> false
            // Maintained non-partial index expressions are already readable on
            // every stored row, including partially valid JSONB documents.
            indexed expression || (core.OrderBy |> List.exists sameRead)
            // An unfiltered primary-key scan evaluates only emitted rows.
            || (core.Where.IsNone &&
                match core.OrderBy with
                | [order] -> baseColumn source "Id" order.Expr
                | _ -> false)
        | JsonObjectExpr properties ->
            properties |> List.forall (fun (_, value) -> movableValue model table source core value)
        | _ -> false

    let private mapProjectionExpression rewrite (projection: Projection) =
        let alias =
            match projection.Alias, projection.Expr with
            | None, Column(_, name) -> Some name
            | alias, _ -> alias
        { projection with Alias = alias; Expr = rewrite projection.Expr }

    /// Compute grouped keys from covered index values instead of repeatedly
    /// reading documents. The input boundary prevents SQLite flattening those
    /// values back into payload extraction while preserving one read snapshot.
    let private coverGroups (model: IndexModel.IndexModel) (core: SelectCore) =
        let local expression =
            not (SqlExpr.exists (function
                | ScalarSubquery _ | Exists _ | InSubquery _ | WindowCall _ -> true
                | AggregateCall((AggregateKind.Count | AggregateKind.Min | AggregateKind.Max), _, _, _) -> false
                | AggregateCall _ -> true // Floating sums and ordered aggregates depend on input order.
                | FunctionCall(name, _) ->
                    match name.ToUpperInvariant() with
                    | "SUBSTR" | "SUBSTRING" | "LOWER" | "UPPER" | "LENGTH" | "JSON" | "JSON_OBJECT" | "JSONB_OBJECT" -> false
                    | _ -> true
                | _ -> false) expression)
        let expressions (row: SelectCore) =
            (ProjectionSetOps.toList row.Projections |> List.map _.Expr)
            @ row.GroupBy @ (row.OrderBy |> List.map _.Expr)
            @ Option.toList row.Where @ Option.toList row.Having
        if core.GroupBy.IsEmpty || not core.Joins.IsEmpty || core.Distinct
           || ((core.Limit.IsSome || core.Offset.IsSome) && core.OrderBy.IsEmpty) then core else
        let expanded =
            match core.Source with
            | Some(DerivedTable({ Ctes = []; Body = SingleSelect inner }, alias))
                when plainRows inner && inner.Limit.IsNone && inner.Offset.IsNone && inner.OrderBy.IsEmpty ->
                let projections = ProjectionSetOps.toList inner.Projections
                let replace = SqlExpr.map (function
                    | Column(qualifier, name) as original when qualifier.IsNone || qualifier = Some alias ->
                        projections |> List.tryFind (fun p -> p.Alias = Some name || p.Alias.IsNone && p.Expr = Column(None, name))
                        |> Option.map _.Expr |> Option.defaultValue original
                    | node -> node)
                let where =
                    match core.Where |> Option.map replace, inner.Where with
                    | Some l, Some r -> Some(Binary(l, BinaryOperator.And, r))
                    | Some p, None | None, Some p -> Some p
                    | _ -> None
                { core with Source = inner.Source; Where = where
                            Projections = ProjectionSetOps.map (mapProjectionExpression replace) core.Projections
                            GroupBy = core.GroupBy |> List.map replace
                            Having = core.Having |> Option.map replace
                            OrderBy = core.OrderBy |> List.map (fun o -> { o with Expr = replace o.Expr }) }
            | _ -> core
        match expanded.Source with
        | Some(BaseTable(table, sourceAlias)) when expressions expanded |> List.forall local ->
            let source = defaultArg sourceAlias table
            let normalize = SqlExpr.map (function
                | Column(Some alias, name) when alias = source -> Column(Some table, name)
                | JsonExtractExpr(Some alias, name, path) when alias = source -> JsonExtractExpr(Some table, name, path)
                | node -> node) << FlattenTransform.normalizeExprQuoting
            model.Indexes |> List.tryPick (fun index ->
                if index.TableName <> table || index.Terms.IsEmpty
                   || index.Terms |> List.exists (fun term -> not (String.Equals(term.Collation, "BINARY", StringComparison.OrdinalIgnoreCase))) then None else
                let alias = table + "_group_input"
                let rewrite = normalize >> SqlExpr.map (fun node ->
                    index.Terms |> List.tryFindIndex (fun term -> ExpressionMatcher.expressionMatchesIndex table node term.Expression)
                    |> Option.map (fun i -> Column(Some alias, "key" + string i))
                    |> Option.defaultValue node)
                let covered expr =
                    not (SqlExpr.exists (function
                        | Column(Some q, _) -> q <> alias
                        | Column _ | JsonExtractExpr _ | JsonRootExtract _ -> true
                        | _ -> false) (rewrite expr))
                if not (expressions expanded |> List.forall covered) then None else
                let input =
                    { expanded with Source = Some(BaseTable(table, None))
                                    Projections = index.Terms |> List.mapi (fun i term ->
                                        { Alias = Some("key" + string i); Expr = term.Expression }) |> ProjectionSetOps.ofList
                                    Where = expanded.Where |> Option.map normalize; GroupBy = []; Having = None
                                    OrderBy = index.Terms |> List.map (fun term -> { Expr = term.Expression; Direction = term.Direction })
                                    Limit = Some(Literal(SqlLiteral.Integer -1L)); Offset = None }
                Some { expanded with Source = Some(DerivedTable({ Ctes = []; Body = SingleSelect input }, alias))
                                     Projections = ProjectionSetOps.map (mapProjectionExpression rewrite) expanded.Projections
                                     GroupBy = expanded.GroupBy |> List.map rewrite
                                     Where = None
                                     Having = expanded.Having |> Option.map rewrite
                                     OrderBy = expanded.OrderBy |> List.map (fun o -> { o with Expr = rewrite o.Expr }) })
            |> Option.defaultValue core
        | _ -> core

    let rec private planGroups model query =
        match query.Body with
        | SingleSelect core ->
            let planned = if core.GroupBy.IsEmpty then core else coverGroups model core
            if not (obj.ReferenceEquals(planned, core)) && planned <> core then
                { query with Body = SingleSelect planned }
            else
                match core.Source with
                | Some(DerivedTable(inner, alias)) ->
                    let plannedInner = planGroups model inner
                    if obj.ReferenceEquals(plannedInner, inner) then query
                    else
                        { query with Body = SingleSelect { core with Source = Some(DerivedTable(plannedInner, alias)) } }
                | _ -> query
        | _ -> query

    /// Select identifiers first, then recover the projected values for emitted rows.
    let planPage model (estimates: Lazy<Map<string, IndexModel.TableEstimate>>) (query: SqlSelect) =
        match query with
        | { Ctes = []; Body = SingleSelect outer } when projectionOnly outer ->
            match outer.Source with
            | Some(DerivedTable({ Ctes = []; Body = SingleSelect inner }, alias))
                when plainRows inner && inner.Limit.IsSome && not inner.OrderBy.IsEmpty
                     && (inner.OrderBy |> List.forall (fun order -> rowExpression order.Expr)) ->
                match inner.Source, ProjectionSetOps.toList inner.Projections with
                | Some(BaseTable(table, tableAlias)), projections when projections.Length = 2 ->
                    let source = defaultArg tableAlias table
                    let normalize = FlattenTransform.normalizeExprQuoting >> SqlExpr.map (function
                        | Column(Some alias, name) when alias = source -> Column(Some table, name)
                        | JsonExtractExpr(Some alias, name, path) when alias = source -> JsonExtractExpr(Some table, name, path)
                        | node -> node)
                    let predicate = inner.Where |> Option.map normalize
                    let normalizedOrders = inner.OrderBy |> List.map (fun order -> { order with Expr = normalize order.Expr })
                    let alreadyCovered =
                        ExpressionMatcher.hasCoveringOrderingIndex model table predicate normalizedOrders
                            (projections |> List.map (fun projection -> normalize projection.Expr))
                    if alreadyCovered then
                        let ordering = ExpressionMatcher.removeFixedOrdering table predicate normalizedOrders
                        if ordering.Length = normalizedOrders.Length then query else
                        // Retain original aliases; only discard equality-fixed keys.
                        let retained =
                            List.map2 (fun original normalized ->
                                if ordering |> List.exists (fun kept -> kept.Expr = normalized.Expr) then Some original
                                else None) inner.OrderBy normalizedOrders
                            |> List.choose id
                        let input = { inner with OrderBy = retained }
                        { query with Body = SingleSelect { outer with Source = Some(DerivedTable({ Ctes = []; Body = SingleSelect input }, alias)) } }
                    else
                    let exposes name projection =
                        (projection.Alias.IsNone || projection.Alias = Some name) && baseColumn source name projection.Expr
                    let value = projections |> List.tryFind (fun projection ->
                        projection.Alias = Some "Value" || exposes "Value" projection)
                    let usable =
                        (projections |> List.exists (exposes "Id"))
                        && (value |> Option.exists (fun projection -> movableValue model table source inner projection.Expr))
                    let wrapper = ProjectionSetOps.toList outer.Projections
                    let valid expression =
                        rowExpression expression && not (SqlExpr.exists (function
                            | Column(qualifier, name) ->
                                (qualifier.IsSome && qualifier <> Some alias) || (name <> "Id" && name <> "Value")
                            | JsonExtractExpr _ | JsonRootExtract _ -> true
                            | _ -> false) expression)
                    if not usable || wrapper.IsEmpty || not (wrapper |> List.forall (fun p -> valid p.Expr)) then query else
                    let payloadAlias = alias + "_value"
                    let projectedValue = FlattenTransform.normalizeExprQuoting value.Value.Expr
                    let fetchedValue = projectedValue |> SqlExpr.map (function
                        | Column(qualifier, name) when fromSource source qualifier -> Column(Some payloadAlias, name)
                        | JsonExtractExpr(qualifier, name, path) when fromSource source qualifier -> JsonExtractExpr(Some payloadAlias, name, path)
                        | JsonRootExtract(qualifier, name) when fromSource source qualifier -> JsonRootExtract(Some payloadAlias, name)
                        | node -> node)
                    let fetch = ScalarSubquery {
                        Ctes = []
                        Body = SingleSelect {
                            inner with
                                Source = Some(BaseTable(table, Some payloadAlias))
                                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = fetchedValue }]
                                Where = Some(Binary(Column(Some payloadAlias, "Id"), BinaryOperator.Eq, Column(Some alias, "Id")))
                                OrderBy = []; Limit = None; Offset = None } }
                    let replacement =
                        match projectedValue with
                        | Literal _ | Parameter _ -> projectedValue
                        | value when baseColumn source "Id" value -> Column(Some alias, "Id")
                        | _ -> fetch
                    let rewrite = SqlExpr.map (function Column(_, "Value") -> replacement | node -> node)
                    // Carry the caller's actual order through the page boundary. Id is
                    // identity only; no additional ordering term is introduced.
                    let orderName i order = if baseColumn source "Id" order.Expr then "Id" else "ord" + string i
                    let keys = inner.OrderBy |> List.mapi (fun i order ->
                        if baseColumn source "Id" order.Expr then None
                        else Some { Alias = Some(orderName i order); Expr = order.Expr }) |> List.choose id
                    let ids = { inner with Projections = ProjectionSetOps.ofList ((projections |> List.filter (exposes "Id")) @ keys) }
                    let result =
                        { outer with
                            Source = Some(DerivedTable(CompiledPage.tryPlan model estimates table source ids, alias))
                            Projections = ProjectionSetOps.map (fun p -> { p with Expr = rewrite p.Expr }) outer.Projections
                            OrderBy = inner.OrderBy |> List.mapi (fun i order ->
                                { order with Expr = Column(Some alias, orderName i order) }) }
                    { query with Body = SingleSelect result }
                | _ -> query
            | _ -> query
        | _ -> query

    let plan model estimates query =
        planGroups model query |> planPage model estimates
