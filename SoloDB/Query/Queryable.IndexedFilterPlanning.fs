namespace SoloDatabase

open SoloDatabase.SqlModel

/// Identifier intersection over independently maintained index expressions.
module internal IndexedFilterPlanning =
    type Intersection = { Query: SqlSelect; Nonempty: SqlExpr; RowIdPredicate: SqlExpr option }
    let private grouped model table source (core: SelectCore) =
        let normalized expression =
            SqlExpr.map (function
                | Column(Some alias, name) when alias = source -> Column(Some table, name)
                | JsonExtractExpr(Some alias, name, path) when alias = source -> JsonExtractExpr(Some table, name, path)
                | node -> node) (FlattenTransform.normalizeExprQuoting expression)
        let isId = function
            | Column(alias, "Id") -> alias.IsNone || alias = Some table
            | _ -> false
        let rec rowIdPredicate = function
            | Binary(left, BinaryOperator.And, right) -> rowIdPredicate left && rowIdPredicate right
            | Binary(left, _, right) -> isId left || isId right
            | _ -> false
        core.Where |> Option.bind (fun predicate ->
            ExpressionMatcher.independentFilterGroups model table (normalized predicate))
        |> Option.map (fun groups ->
            let rowIds, others = groups |> List.partition rowIdPredicate
            let rowId =
                if rowIds.IsEmpty then None
                else Some(rowIds |> List.reduce (fun left right -> Binary(left, BinaryOperator.And, right)))
            // Every admitted collection index carries Id. Apply its bounds in
            // each covered scan instead of materializing a separate rowid set.
            let groups =
                match rowId, others with
                | Some bounds, _ :: _ -> others |> List.map (fun predicate -> Binary(predicate, BinaryOperator.And, bounds))
                | _ -> groups
            groups, rowId)

    let groups model table source core = grouped model table source core |> Option.map fst

    let tryIds model table source (core: SelectCore) =
        grouped model table source core |> Option.map (fun (groups, rowId) ->
            let select core = { Ctes = []; Body = SingleSelect core }
            let id = Column(Some table, "Id")
            let rows predicate =
                { core with Source = Some(BaseTable(table, None)); Joins = []
                            Projections = ProjectionSetOps.ofList [{ Alias = Some "Id"; Expr = id }]
                            Where = Some predicate; OrderBy = []; Limit = None; Offset = None
                            Distinct = false; GroupBy = []; Having = None }
            let alias = table + "_indexed_ids"
            let outerId = Column(Some alias, "Id")
            // An unlimited derived driver prevents flattening into rowid lookups
            // that would evaluate the other indexed field from document payloads.
            let driver = { rows groups.Head with Limit = Some(Literal(SqlLiteral.Integer -1L)) }
            let membership = groups.Tail |> List.map (fun group -> InSubquery(outerId, select (rows group)))
            let query = select { driver with Source = Some(DerivedTable(select driver, alias))
                                             Projections = ProjectionSetOps.ofList [{ Alias = Some "Id"; Expr = outerId }]
                                             Where = if membership.IsEmpty then None else Some(List.reduce (fun left right -> Binary(left, BinaryOperator.And, right)) membership)
                                             Limit = None }
            let nonempty = groups |> List.map (fun group -> Exists(select (rows group)))
                                  |> List.reduce (fun left right -> Binary(left, BinaryOperator.And, right))
            { Query = query; Nonempty = nonempty; RowIdPredicate = rowId })
