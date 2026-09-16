namespace SoloDatabase

open System
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

    let private movableValue source (core: SelectCore) expression =
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
            (core.OrderBy |> List.exists sameRead)
            // An unfiltered primary-key scan evaluates only emitted rows.
            || (core.Where.IsNone &&
                match core.OrderBy with
                | [order] -> baseColumn source "Id" order.Expr
                | _ -> false)
        | _ -> false

    /// Select identifiers first, then recover the projected values for emitted rows.
    let planPage model estimates (query: SqlSelect) =
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
                    let value = projections |> List.tryFind (fun projection ->
                        projection.Alias = Some "Value" || exposes "Value" projection)
                    let usable =
                        (projections |> List.exists (exposes "Id"))
                        && (value |> Option.exists (fun projection -> movableValue source inner projection.Expr))
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
                    let projectedValue = FlattenTransform.normalizeExprQuoting value.Value.Expr
                    let fetchedValue =
                        match projectedValue with
                        | Column(_, name) -> Column(Some payloadAlias, name)
                        | JsonExtractExpr(_, name, path) -> JsonExtractExpr(Some payloadAlias, name, path)
                        | JsonRootExtract(_, name) -> JsonRootExtract(Some payloadAlias, name)
                        | value -> value
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
                    let ids = { inner with Projections = ProjectionSetOps.ofList (projections |> List.filter (exposes "Id")) }
                    let result =
                        { outer with
                            Source = Some(DerivedTable(CompiledPage.tryPlan model estimates table source ids, alias))
                            Projections = ProjectionSetOps.map (fun p -> { p with Expr = rewrite p.Expr }) outer.Projections }
                    { query with Body = SingleSelect result }
                | _ -> query
            | _ -> query
        | _ -> query
