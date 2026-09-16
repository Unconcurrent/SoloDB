namespace SoloDatabase

open System
open SoloDatabase.SqlModel

/// Exact aggregate strategies over collection row identifiers.
module internal QueryCountPlanning =
    let private integer n = Literal(SqlLiteral.Integer n)
    let private binary op l r = Binary(l, op, r)
    let private select core = { Ctes = []; Body = SingleSelect core }
    let private projection expression = ProjectionSetOps.ofList [{ Alias = Some "Value"; Expr = expression }]
    let private star = AggregateCall(AggregateKind.Count, None, false, None)
    let private plain (core: SelectCore) =
        not core.Distinct && core.Joins.IsEmpty && core.GroupBy.IsEmpty && core.Having.IsNone
        && core.Limit.IsNone && core.Offset.IsNone
    let private from alias (qualifier: string option) = qualifier.IsNone || qualifier = Some alias
    let private isId alias expression =
        match FlattenTransform.normalizeExprQuoting expression with
        | Column(qualifier, "Id") -> from alias qualifier
        | _ -> false

    let private rewrite root model (estimates: Lazy<Map<string, IndexModel.TableEstimate>>) (query: SqlSelect) (core: SelectCore) =
        match core.Source, ProjectionSetOps.toList core.Projections with
        | Some(DerivedTable({ Ctes = []; Body = SingleSelect rows }, alias)), [result]
            when query.Ctes.IsEmpty && plain core && core.Where.IsNone && core.OrderBy.IsEmpty && plain rows && rows.OrderBy.IsEmpty ->
            match rows.Source, result.Expr with
            | Some(BaseTable(table, tableAlias)), FunctionCall(name, [counted])
                when table = root && String.Equals(name, "COUNT", StringComparison.OrdinalIgnoreCase) && isId alias counted ->
                let source = defaultArg tableAlias table
                let exposesId = rows.Projections |> ProjectionSetOps.toList |> List.exists (fun p ->
                    (p.Alias.IsNone || p.Alias = Some "Id") && isId source p.Expr)
                if not exposesId then query else
                // Id is the non-null INTEGER PRIMARY KEY of a collection. This
                // proof does not cross joins, padded rows, groups or set operations.
                let baseRows = { rows with Projections = projection star; OrderBy = [] }
                let normalized expression =
                    SqlExpr.map (function
                    | JsonExtractExpr(Some a, col, path) when a = source -> JsonExtractExpr(Some table, col, path)
                    | Column(Some a, col) when a = source -> Column(Some table, col)
                    | node -> node) (FlattenTransform.normalizeExprQuoting expression)
                let estimate = if rows.Where.IsSome then Map.tryFind table estimates.Value |> Option.map (fun estimate -> estimate.Rows) else None
                let admission =
                    match rows.Where, estimate with
                    | Some predicate, Some size ->
                        ExpressionMatcher.tryComparisonComplement predicate |> Option.bind (fun (key, excluded) ->
                            if ExpressionMatcher.hasMatchingIndex model table (normalized key) then
                                Some(max 4096L (size / 16L), excluded)
                            else None)
                    | _ -> None
                match IndexedFilterPlanning.tryIds model table source rows with
                | Some intersection ->
                    let ids = intersection.Query
                    let counted = { baseRows with Source = Some(DerivedTable(ids, "count_indexed_ids")); Where = None
                                                  Projections = ProjectionSetOps.ofList [{ result with Expr = star }] }
                    let partitions =
                        IndexedFilterPlanning.groups model table source rows
                        |> Option.bind (fun groups ->
                            let parts = groups |> List.map ExpressionMatcher.tryComparisonComplement
                            if parts |> List.forall Option.isSome then Some(groups, parts |> List.collect (fun p -> snd p.Value))
                            else None)
                    match partitions, estimate with
                    | Some(groups, excluded), Some size ->
                        let scan predicate =
                            { rows with Source = Some(BaseTable(table, None)); Where = Some predicate
                                        Projections = ProjectionSetOps.ofList [{ Alias = Some "Id"; Expr = Column(Some table, "Id") }] }
                        let nonempty = intersection.Nonempty
                        let arms = excluded |> List.map scan
                        let union = { Ctes = []; Body = UnionAllSelect(arms.Head, arms.Tail) }
                        let cap = max 4096L (size / 2L)
                        let distinct =
                            { counted with Source = Some(DerivedTable(union, "count_excluded_ids"))
                                           Projections = ProjectionSetOps.ofList [{ Alias = Some "Id"; Expr = Column(None, "Id") }]
                                           Distinct = true }
                        let excludedCount = { counted with Source = Some(DerivedTable(select distinct, "count_excluded_unique")) }
                        let parts = groups |> List.map (fun group -> ExpressionMatcher.tryComparisonComplement group |> Option.get |> snd)
                        let probes, sum = parts |> List.indexed |> List.mapFold (fun used (i, predicates) ->
                            let remaining = binary BinaryOperator.Sub (integer cap) used
                            let scans = predicates |> List.map scan
                            let union = { Ctes = []; Body = UnionAllSelect(scans.Head, scans.Tail) }
                            let bounded = { counted with Source = Some(DerivedTable(union, "count_group_ids"))
                                                         Projections = projection (integer 1L); Limit = Some remaining }
                            let count = { counted with Source = Some(DerivedTable(select bounded, "count_group_rows")) }
                            let name = table + "_count_group_" + string i
                            let readCount =
                                { counted with
                                    Source = Some(BaseTable(name, None))
                                    Projections = projection (Column(None, result.Alias |> Option.defaultValue "Value")) }
                            let value = ScalarSubquery(select readCount)
                            { Name = name; Materialized = true; Query = select count }, binary BinaryOperator.Add used value) (integer 0L)
                        // Each probe spends only the remaining shared budget.
                        // A sum below the cap proves every group was fully read;
                        // saturation leaves zero budget and selects intersection.
                        let affordable = binary BinaryOperator.Lt sum (integer cap)
                        let total = ScalarSubquery(select { baseRows with Where = None })
                        let value = CaseExpr(
                            (Unary(UnaryOperator.Not, nonempty), integer 0L),
                            [affordable, binary BinaryOperator.Sub total (ScalarSubquery(select excludedCount))],
                            Some(ScalarSubquery(select counted)))
                        { Ctes = probes
                          Body = SingleSelect { core with Source = None; Projections = ProjectionSetOps.ofList [{ result with Expr = value }] } }
                    | _ -> { query with Body = SingleSelect counted }
                | None ->
                    match admission with
                    | None -> { query with Body = SingleSelect { baseRows with Projections = ProjectionSetOps.ofList [{result with Expr = star}] } }
                    | Some(cap, excluded) ->
                        let scalar q = ScalarSubquery q
                        let countRows q =
                            select { baseRows with Source = Some(DerivedTable(q, "count_rows")); Where = None; Projections = projection star }
                        let limited predicate =
                            { rows with Projections = projection (integer 1L); Where = predicate; OrderBy = []; Limit = Some(integer cap) }
                        let directCap = 4096L
                        let directCount = countRows (select { limited rows.Where with Limit = Some(integer directCap) })
                        let arms = excluded |> List.map (fun p -> { limited (Some p) with Limit = None })
                        let excludedRows = { Ctes = []; Body = UnionAllSelect(arms.Head, arms.Tail) }
                        let boundedExcluded =
                            select { baseRows with Source = Some(DerivedTable(excludedRows, "excluded_rows")); Where = None
                                                   Projections = projection (integer 1L); Limit = Some(integer cap) }
                        let excludedCount = countRows boundedExcluded
                        let read name = scalar (select { baseRows with Source = Some(BaseTable(name, None)); Where = None; Projections = projection (Column(None, "Value")) })
                        let directName = table + "_count_matches"
                        let excludedName = table + "_count_excluded"
                        let directValue = read directName
                        let excludedValue = read excludedName
                        let total = scalar (select { baseRows with Where = None })
                        let full = scalar (select baseRows)
                        // CASE evaluates the complementary probe only when the direct
                        // probe saturates. An unsaturated probe is an exact answer.
                        // Disjoint ranges plus NULL partition the excluded rows; an
                        // empty/contradictory or NULL-bound range returns before them.
                        let value = CaseExpr(
                            (binary BinaryOperator.Lt directValue (integer directCap), directValue),
                            [binary BinaryOperator.Lt excludedValue (integer cap), binary BinaryOperator.Sub total excludedValue],
                            Some full)
                        let resultCore = { core with Source = None; Projections = ProjectionSetOps.ofList [{ result with Expr = value }] }
                        { Ctes = query.Ctes @ [
                            {Name=directName;Materialized=true;Query=directCount}
                            {Name=excludedName;Materialized=true;Query=excludedCount}]
                          Body = SingleSelect resultCore }
            | _ -> query
        | _ -> query

    let rec plan root model estimates (query: SqlSelect) =
        let source = function
            | DerivedTable(inner, alias) -> DerivedTable(plan root model estimates inner, alias)
            | other -> other
        match query.Body with
        | SingleSelect core ->
            let nested = { core with Source = core.Source |> Option.map source }
            rewrite root model estimates {query with Body = SingleSelect nested} nested
        | _ -> query
