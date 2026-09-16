namespace SoloDatabase

open SoloDatabase.SqlModel

/// Additional planning paid once by retained queries.
module internal CompiledQueryPlanning =
    let private rowExpression expression =
        SqlExpr.fold (fun valid node ->
            valid && match node with
                     | AggregateCall _ | WindowCall _ | ScalarSubquery _ | Exists _ | InSubquery _ -> false
                     | _ -> true) true expression

    let private plainRows (core: SelectCore) =
        not core.Distinct && core.Joins.IsEmpty && core.GroupBy.IsEmpty && core.Having.IsNone

    let private projectionOnly (core: SelectCore) =
        plainRows core && core.Where.IsNone && core.OrderBy.IsEmpty && core.Limit.IsNone && core.Offset.IsNone

    let private baseColumn source name expression =
        match FlattenTransform.normalizeExprQuoting expression with
        | Column(qualifier, column) -> column = name && (qualifier.IsNone || qualifier = Some source)
        | _ -> false

    /// Leave the ordered limited rowset intact, but fetch its payload after selection.
    let latePayload model (query: SqlSelect) =
        match query with
        | { Ctes = []; Body = SingleSelect outer } when projectionOnly outer ->
            match outer.Source with
            | Some(DerivedTable({ Ctes = []; Body = SingleSelect inner }, alias))
                when plainRows inner && inner.Limit.IsSome && not inner.OrderBy.IsEmpty
                     && (inner.OrderBy |> List.forall (fun order -> rowExpression order.Expr)) ->
                match inner.Source, ProjectionSetOps.toList inner.Projections with
                | Some(BaseTable(table, tableAlias)), projections when projections.Length = 2 ->
                    let source = defaultArg tableAlias table
                    let exposes name projection =
                        (projection.Alias.IsNone || projection.Alias = Some name) && baseColumn source name projection.Expr
                    let usable =
                        (projections |> List.exists (exposes "Id")) && (projections |> List.exists (exposes "Value"))
                        && (inner.OrderBy |> List.exists (fun order -> baseColumn source "Id" order.Expr))
                    let wrapper = ProjectionSetOps.toList outer.Projections
                    let valid expression =
                        rowExpression expression && SqlExpr.fold (fun valid node ->
                            valid && match node with
                                     | Column(qualifier, name) ->
                                         (qualifier.IsNone || qualifier = Some alias) && (name = "Id" || name = "Value")
                                     | JsonExtractExpr _ | JsonRootExtract _ -> false
                                     | _ -> true) true expression
                    if not usable || wrapper.IsEmpty || not (wrapper |> List.forall (fun p -> valid p.Expr)) then query else
                    let payloadAlias = alias + "_value"
                    let fetch = ScalarSubquery {
                        Ctes = []
                        Body = SingleSelect {
                            inner with
                                Source = Some(BaseTable(table, Some payloadAlias))
                                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = Column(Some payloadAlias, "Value") }]
                                Where = Some(Binary(Column(Some payloadAlias, "Id"), BinaryOperator.Eq, Column(Some alias, "Id")))
                                OrderBy = []; Limit = None; Offset = None } }
                    let rewrite = SqlExpr.map (function Column(_, "Value") -> fetch | node -> node)
                    let ids = { inner with Projections = ProjectionSetOps.ofList (projections |> List.filter (exposes "Id")) }
                    let result =
                        { outer with
                            Source = Some(DerivedTable(CompiledPage.tryPlan model table source ids, alias))
                            Projections = ProjectionSetOps.map (fun p -> { p with Expr = rewrite p.Expr }) outer.Projections }
                    { query with Body = SingleSelect result }
                | _ -> query
            | _ -> query
        | _ -> query
