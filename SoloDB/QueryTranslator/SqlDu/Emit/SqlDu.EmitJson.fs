module internal SoloDatabase.EmitJson

open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryableGroupByAliases

/// Escape single quotes and null characters for inline SQLite string literals.
let escapeSQLiteStringLiteral (input: string) : string =
    input.Replace("'", "''").Replace("\0", "")

/// Quote an emitted SQL identifier unless it is already double-quoted.
let quoteIdentifier (ctx: EmitContext) (name: string) : string =
    if name.Length >= 2 && name.[0] = '"' && name.[name.Length - 1] = '"' then name
    else ctx.QuoteIdent(name)

/// Emit a JSON path expression: jsonb_extract(source, '$.path').
/// The caller decides which function name to use.
let emitJsonExtract (ctx: EmitContext) (funcName: string) (sourceAlias: string option) (column: string) (jsonPath: JsonPath) : Emitted =
    let src =
        match sourceAlias with
        | Some alias -> sprintf "%s.%s" (quoteIdentifier ctx alias) (quoteIdentifier ctx column)
        | None -> quoteIdentifier ctx column
    let path = ctx.FormatJsonPath(JsonPathOps.toList jsonPath)
    { Sql = sprintf "%s(%s, %s)" funcName src path
      Parameters = Emitted.emptyParameters () }

/// Emit a jsonb_set expression: jsonb_set(target, path, value[, path2, value2, ...])
/// Each assignment is (JsonPath, SqlExpr). The target and value expressions
/// are emitted by the caller-provided emitExprFn to avoid circular dependencies.
let emitJsonSet (ctx: EmitContext) (emitExprFn: EmitContext -> SqlExpr -> Emitted) (target: SqlExpr) (assignments: (JsonPath * SqlExpr) list) : Emitted =
    let mutable result = emitExprFn ctx target
    for (jsonPath, valueExpr) in assignments do
        let path = ctx.FormatJsonPath(JsonPathOps.toList jsonPath)
        let valueEmitted = emitExprFn ctx valueExpr
        result <-
            { Sql = sprintf "jsonb_set(%s, %s, %s)" result.Sql path valueEmitted.Sql
              Parameters = Emitted.concatParameterSets [ result.Parameters; valueEmitted.Parameters ] }
    result

/// Emit a JSON array expression: jsonb_array(e1, e2, ...)
/// Uses jsonb_array for JSONB storage format (product requirement).
let emitJsonArray (ctx: EmitContext) (emitExprFn: EmitContext -> SqlExpr -> Emitted) (elements: SqlExpr list) : Emitted =
    let parts = elements |> List.map (emitExprFn ctx)
    let sql = parts |> List.map (fun p -> p.Sql) |> String.concat ", "
    let parms = Emitted.collectParameters parts
    { Sql = sprintf "jsonb_array(%s)" sql; Parameters = parms }

/// Emit a JSON object expression: json_object('key1', val1, 'key2', val2, ...)
/// Uses json_object (TEXT JSON) so that downstream jsonb_extract returns typed SQL values
/// (integer, real) instead of binary JSON blobs. This ensures correct numeric ordering
/// and comparison in OrderBy, Where, TakeWhile, and GroupJoin key extraction.
let emitJsonObject (ctx: EmitContext) (emitExprFn: EmitContext -> SqlExpr -> Emitted) (properties: (string * SqlExpr) list) : Emitted =
    let parts =
        properties
        |> List.collect (fun (key, valueExpr) ->
            let valueEmitted = emitExprFn ctx valueExpr
            [ { Sql = sprintf "'%s'" (escapeSQLiteStringLiteral key); Parameters = Emitted.emptyParameters () }
              valueEmitted ])
    let sql = parts |> List.map (fun p -> p.Sql) |> String.concat ", "
    let parms = Emitted.collectParameters parts
    { Sql = sprintf "%s(%s)" jsonObjectFn sql; Parameters = parms }
