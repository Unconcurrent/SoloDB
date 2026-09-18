module internal SoloDatabase.EmitSelect

open System.Text
open SoloDatabase.SqlModel

/// Append expression SQL and parameters in the same order they are emitted.
let rec private appendExpr (ctx: EmitContext) (sql: StringBuilder) (parameters: ResizeArray<string * obj>) expr =
    let emitted = EmitExpr.emitExprWith emitSelect ctx expr
    sql.Append(emitted.Sql) |> ignore
    parameters.AddRange(emitted.Parameters)

and private appendSource (ctx: EmitContext) (sql: StringBuilder) parameters source =
    match source with
    | BaseTable(table, alias) ->
        sql.Append(ctx.QuoteIdent(table)) |> ignore
        match alias with
        | Some alias -> sql.Append(" AS ").Append(EmitJson.quoteIdentifier ctx alias) |> ignore
        | None -> ()
    | DerivedTable(query, alias) ->
        sql.Append('(') |> ignore
        appendSelect ctx sql parameters query
        sql.Append(") ").Append(EmitJson.quoteIdentifier ctx alias) |> ignore
    | FromJsonEach(value, alias) ->
        sql.Append("json_each(") |> ignore
        appendExpr ctx sql parameters value
        sql.Append(')') |> ignore
        match alias with
        | Some alias -> sql.Append(" AS ").Append(EmitJson.quoteIdentifier ctx alias) |> ignore
        | None -> ()

and private appendCore ctx (sql: StringBuilder) parameters (core: SelectCore) =
    sql.Append(if core.Distinct then "SELECT DISTINCT " else "SELECT ") |> ignore
    match core.Projections with
    | AllColumns -> sql.Append('*') |> ignore
    | Explicit _ ->
        let mutable first = true
        for projection in ProjectionSetOps.toList core.Projections do
            if not first then sql.Append(", ") |> ignore
            first <- false
            appendExpr ctx sql parameters projection.Expr
            match projection.Alias with
            | Some alias -> sql.Append(" AS ").Append(EmitJson.quoteIdentifier ctx alias) |> ignore
            | None -> ()

    match core.Source with
    | Some source ->
        sql.Append(" FROM ") |> ignore
        appendSource ctx sql parameters source
    | None -> ()

    for join in core.Joins do
        match join with
        | CrossJoin source ->
            sql.Append(" CROSS JOIN ") |> ignore
            appendSource ctx sql parameters source
        | ConditionedJoin(kind, source, condition) ->
            sql.Append(if kind = Inner then " INNER JOIN " else " LEFT JOIN ") |> ignore
            appendSource ctx sql parameters source
            sql.Append(" ON ") |> ignore
            appendExpr ctx sql parameters condition

    let clause keyword expression =
        match expression with
        | Some expression ->
            sql.Append(keyword: string) |> ignore
            appendExpr ctx sql parameters expression
        | None -> ()
    clause " WHERE " core.Where
    if not core.GroupBy.IsEmpty then
        sql.Append(" GROUP BY ") |> ignore
        let mutable first = true
        for expression in core.GroupBy do
            if not first then sql.Append(", ") |> ignore
            first <- false
            appendExpr ctx sql parameters expression
    clause " HAVING " core.Having
    if not core.OrderBy.IsEmpty then
        sql.Append(" ORDER BY ") |> ignore
        let mutable first = true
        for ordering in core.OrderBy do
            if not first then sql.Append(", ") |> ignore
            first <- false
            appendExpr ctx sql parameters ordering.Expr
            sql.Append(if ordering.Direction = Asc then " ASC" else " DESC") |> ignore
    clause " LIMIT " core.Limit
    clause " OFFSET " core.Offset

/// A select, its derived sources, CTEs and union arms share one output buffer.
and private appendSelect ctx (sql: StringBuilder) parameters (query: SqlSelect) =
    if not query.Ctes.IsEmpty then
        sql.Append("WITH ") |> ignore
        let mutable first = true
        for cte in query.Ctes do
            if not first then sql.Append(", ") |> ignore
            first <- false
            sql.Append(EmitJson.quoteIdentifier ctx cte.Name).Append(" AS ") |> ignore
            if cte.Materialized then sql.Append("MATERIALIZED ") |> ignore
            sql.Append('(') |> ignore
            appendSelect ctx sql parameters cte.Query
            sql.Append(')') |> ignore
        sql.Append(' ') |> ignore
    match query.Body with
    | SingleSelect core -> appendCore ctx sql parameters core
    | UnionAllSelect(head, tail) ->
        appendCore ctx sql parameters head
        for core in tail do
            sql.Append(" UNION ALL ") |> ignore
            appendCore ctx sql parameters core

and emitSelect (ctx: EmitContext) (query: SqlSelect) : Emitted =
    let sql = StringBuilder()
    let parameters = ResizeArray<string * obj>()
    appendSelect ctx sql parameters query
    { Sql = sql.ToString(); Parameters = parameters }

/// Expression emission resolves scalar and membership subqueries through this owner.
let emitExpr (ctx: EmitContext) (expr: SqlExpr) : Emitted =
    EmitExpr.emitExprWith emitSelect ctx expr
