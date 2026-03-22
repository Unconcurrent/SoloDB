module internal SoloDatabase.EmitStatement

open SqlDu.Engine.C1.Spec

/// Internal expression emitter — delegates to EmitSelect.emitExpr which has
/// the subquery resolution wired in.
let private emitE ctx expr = EmitSelect.emitExpr ctx expr

/// Emit an InsertStatement to SQL.
let emitInsert (ctx: EmitContext) (ins: InsertStatement) : Emitted =
    let allParams = ResizeArray<string * obj>()

    let conflictStr =
        match ins.ConflictResolution with
        | NoConflictResolution -> "INSERT"
        | OrIgnore -> "INSERT OR IGNORE"
        | OrReplace -> "INSERT OR REPLACE"

    let tableSql = ctx.QuoteIdent(ins.TableName)
    let colsSql = ins.Columns |> List.map (EmitJson.quoteIdentifier ctx) |> String.concat ", "

    let rowsSql =
        ins.Values
        |> List.map (fun row ->
            let rowParts = row |> List.map (emitE ctx)
            let rowSql = rowParts |> List.map (fun e -> e.Sql) |> String.concat ", "
            let rowParams = Emitted.collectParameters rowParts
            allParams.AddRange(rowParams)
            sprintf "(%s)" rowSql)
        |> String.concat ", "

    let mutable sql = sprintf "%s INTO %s (%s) VALUES %s" conflictStr tableSql colsSql rowsSql

    match ins.Returning with
    | Some returningExprs ->
        let retParts = returningExprs |> List.map (emitE ctx)
        let retSql = retParts |> List.map (fun e -> e.Sql) |> String.concat ", "
        let retParams = Emitted.collectParameters retParts
        sql <- sprintf "%s RETURNING %s" sql retSql
        allParams.AddRange(retParams)
    | None -> ()

    { Sql = sql; Parameters = allParams }

/// Emit an UpdateStatement to SQL.
let emitUpdate (ctx: EmitContext) (upd: UpdateStatement) : Emitted =
    let allParams = ResizeArray<string * obj>()

    let tableSql = ctx.QuoteIdent(upd.TableName)

    let setClauses =
        upd.SetClauses
        |> List.map (fun (col, expr) ->
            let exprEmitted = emitE ctx expr
            allParams.AddRange(exprEmitted.Parameters)
            sprintf "%s = %s" (EmitJson.quoteIdentifier ctx col) exprEmitted.Sql)
        |> String.concat ", "

    let mutable sql = sprintf "UPDATE %s SET %s" tableSql setClauses

    match upd.Where with
    | Some whereExpr ->
        let whereEmitted = emitE ctx whereExpr
        sql <- sprintf "%s WHERE %s" sql whereEmitted.Sql
        allParams.AddRange(whereEmitted.Parameters)
    | None -> ()

    { Sql = sql; Parameters = allParams }

/// Emit a DeleteStatement to SQL.
let emitDelete (ctx: EmitContext) (del: DeleteStatement) : Emitted =
    let allParams = ResizeArray<string * obj>()

    let tableSql = ctx.QuoteIdent(del.TableName)
    let mutable sql = sprintf "DELETE FROM %s" tableSql

    match del.Where with
    | Some whereExpr ->
        let whereEmitted = emitE ctx whereExpr
        sql <- sprintf "%s WHERE %s" sql whereEmitted.Sql
        allParams.AddRange(whereEmitted.Parameters)
    | None -> ()

    { Sql = sql; Parameters = allParams }

/// Emit a DdlStatement to SQL.
let emitDdl (_ctx: EmitContext) (ddl: DdlStatement) : Emitted =
    { Sql = ddl.Sql; Parameters = Emitted.emptyParameters () }

/// Emit any SqlStatement to SQL + parameters.
let emitStatement (ctx: EmitContext) (stmt: SqlStatement) : Emitted =
    match stmt with
    | SelectStmt select -> EmitSelect.emitSelect ctx select
    | InsertStmt insert -> emitInsert ctx insert
    | UpdateStmt update -> emitUpdate ctx update
    | DeleteStmt delete -> emitDelete ctx delete
    | DdlStmt ddl -> emitDdl ctx ddl
