module SoloDatabase.EmitSelect

open SqlDu.Engine.C1.Spec

/// Internal expression emitter bound to emitSelect for subquery resolution.
let rec private emitE (ctx: EmitContext) (expr: SqlExpr) : Emitted =
    EmitExpr.emitExprWith emitSelect ctx expr

/// Emit a TableSource to SQL.
and private emitTableSource (ctx: EmitContext) (source: TableSource) : Emitted =
    match source with
    | BaseTable(table, alias) ->
        let tableSql = ctx.QuoteIdent(table)
        match alias with
        | Some a -> { Sql = sprintf "%s AS %s" tableSql (EmitJson.quoteIdentifier ctx a); Parameters = [] }
        | None -> { Sql = tableSql; Parameters = [] }
    | DerivedTable(query, alias) ->
        let queryEmitted = emitSelect ctx query
        { Sql = sprintf "(%s) %s" queryEmitted.Sql (EmitJson.quoteIdentifier ctx alias)
          Parameters = queryEmitted.Parameters }
    | FromJsonEach(valueExpr, alias) ->
        let exprEmitted = emitE ctx valueExpr
        match alias with
        | Some a ->
            { Sql = sprintf "json_each(%s) AS %s" exprEmitted.Sql (EmitJson.quoteIdentifier ctx a)
              Parameters = exprEmitted.Parameters }
        | None ->
            { Sql = sprintf "json_each(%s)" exprEmitted.Sql
              Parameters = exprEmitted.Parameters }

/// Emit a JoinShape to SQL.
and private emitJoin (ctx: EmitContext) (join: JoinShape) : Emitted =
    let kindStr =
        match join.Kind with
        | Inner -> "INNER JOIN"
        | Left -> "LEFT JOIN"
        | Cross -> "CROSS JOIN"
    let sourceEmitted = emitTableSource ctx join.Source
    match join.On with
    | Some onExpr ->
        let onEmitted = emitE ctx onExpr
        { Sql = sprintf "%s %s ON %s" kindStr sourceEmitted.Sql onEmitted.Sql
          Parameters = sourceEmitted.Parameters @ onEmitted.Parameters }
    | None ->
        { Sql = sprintf "%s %s" kindStr sourceEmitted.Sql
          Parameters = sourceEmitted.Parameters }

/// Emit a Projection to SQL.
and private emitProjection (ctx: EmitContext) (proj: Projection) : Emitted =
    let exprEmitted = emitE ctx proj.Expr
    match proj.Alias with
    | Some alias ->
        { Sql = sprintf "%s AS %s" exprEmitted.Sql (EmitJson.quoteIdentifier ctx alias)
          Parameters = exprEmitted.Parameters }
    | None -> exprEmitted

/// Emit a single SelectCore to SQL.
and private emitSelectCore (ctx: EmitContext) (core: SelectCore) : Emitted =
    let mutable allParams : (string * obj) list = []

    let distinctStr = if core.Distinct then "SELECT DISTINCT" else "SELECT"

    let projParts = core.Projections |> List.map (emitProjection ctx)
    let projSql = projParts |> List.map (fun e -> e.Sql) |> String.concat ", "
    let projParams = projParts |> List.collect (fun e -> e.Parameters)
    allParams <- allParams @ projParams

    let mutable sql = sprintf "%s %s" distinctStr projSql

    match core.Source with
    | Some source ->
        let sourceEmitted = emitTableSource ctx source
        sql <- sprintf "%s FROM %s" sql sourceEmitted.Sql
        allParams <- allParams @ sourceEmitted.Parameters
    | None -> ()

    for join in core.Joins do
        let joinEmitted = emitJoin ctx join
        sql <- sprintf "%s %s" sql joinEmitted.Sql
        allParams <- allParams @ joinEmitted.Parameters

    match core.Where with
    | Some whereExpr ->
        let whereEmitted = emitE ctx whereExpr
        sql <- sprintf "%s WHERE %s" sql whereEmitted.Sql
        allParams <- allParams @ whereEmitted.Parameters
    | None -> ()

    match core.GroupBy with
    | [] -> ()
    | groupExprs ->
        let groupParts = groupExprs |> List.map (emitE ctx)
        let groupSql = groupParts |> List.map (fun e -> e.Sql) |> String.concat ", "
        let groupParams = groupParts |> List.collect (fun e -> e.Parameters)
        sql <- sprintf "%s GROUP BY %s" sql groupSql
        allParams <- allParams @ groupParams

    match core.Having with
    | Some havingExpr ->
        let havingEmitted = emitE ctx havingExpr
        sql <- sprintf "%s HAVING %s" sql havingEmitted.Sql
        allParams <- allParams @ havingEmitted.Parameters
    | None -> ()

    match core.OrderBy with
    | [] -> ()
    | orderClauses ->
        let orderParts =
            orderClauses
            |> List.map (fun ob ->
                let exprEmitted = emitE ctx ob.Expr
                let dirStr = match ob.Direction with Asc -> "ASC" | Desc -> "DESC"
                { Sql = sprintf "%s %s" exprEmitted.Sql dirStr
                  Parameters = exprEmitted.Parameters })
        let orderSql = orderParts |> List.map (fun e -> e.Sql) |> String.concat ", "
        let orderParams = orderParts |> List.collect (fun e -> e.Parameters)
        sql <- sprintf "%s ORDER BY %s" sql orderSql
        allParams <- allParams @ orderParams

    match core.Limit with
    | Some limitExpr ->
        let limitEmitted = emitE ctx limitExpr
        sql <- sprintf "%s LIMIT %s" sql limitEmitted.Sql
        allParams <- allParams @ limitEmitted.Parameters
    | None -> ()

    match core.Offset with
    | Some offsetExpr ->
        let offsetEmitted = emitE ctx offsetExpr
        sql <- sprintf "%s OFFSET %s" sql offsetEmitted.Sql
        allParams <- allParams @ offsetEmitted.Parameters
    | None -> ()

    { Sql = sql; Parameters = allParams }

/// Emit a CTE binding: name AS (SELECT ...)
and private emitCteBinding (ctx: EmitContext) (cte: CteBinding) : Emitted =
    let queryEmitted = emitSelect ctx cte.Query
    { Sql = sprintf "%s AS (%s)" (EmitJson.quoteIdentifier ctx cte.Name) queryEmitted.Sql
      Parameters = queryEmitted.Parameters }

/// Emit a full SqlSelect (CTEs + body).
and emitSelect (ctx: EmitContext) (select: SqlSelect) : Emitted =
    let mutable sql = ""
    let mutable allParams : (string * obj) list = []

    match select.Ctes with
    | [] -> ()
    | ctes ->
        let cteParts = ctes |> List.map (emitCteBinding ctx)
        let cteSql = cteParts |> List.map (fun e -> e.Sql) |> String.concat ", "
        let cteParams = cteParts |> List.collect (fun e -> e.Parameters)
        sql <- sprintf "WITH %s " cteSql
        allParams <- allParams @ cteParams

    match select.Body with
    | SingleSelect core ->
        let coreEmitted = emitSelectCore ctx core
        sql <- sql + coreEmitted.Sql
        allParams <- allParams @ coreEmitted.Parameters

    | UnionAllSelect(head, tail) ->
        let headEmitted = emitSelectCore ctx head
        sql <- sql + headEmitted.Sql
        allParams <- allParams @ headEmitted.Parameters
        for tailCore in tail do
            let tailEmitted = emitSelectCore ctx tailCore
            sql <- sprintf "%s UNION ALL %s" sql tailEmitted.Sql
            allParams <- allParams @ tailEmitted.Parameters

    { Sql = sql; Parameters = allParams }

/// Public expression emitter — resolves subqueries via emitSelect.
/// Use this from adapters and external callers instead of EmitExpr.emitExprWith directly.
let emitExpr (ctx: EmitContext) (expr: SqlExpr) : Emitted =
    emitE ctx expr
