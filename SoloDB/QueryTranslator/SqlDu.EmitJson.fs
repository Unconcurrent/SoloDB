module SoloDatabase.EmitJson

open SqlDu.Engine.C1.Spec

/// Emit a JSON path expression: jsonb_extract(source, '$.path').
/// The caller decides which function name to use.
let emitJsonExtract (ctx: EmitContext) (funcName: string) (sourceAlias: string option) (column: string) (JsonPath segments: JsonPath) : Emitted =
    let src =
        match sourceAlias with
        | Some alias -> sprintf "%s.%s" alias column
        | None -> column
    let path = ctx.FormatJsonPath(segments)
    { Sql = sprintf "%s(%s, %s)" funcName src path
      Parameters = [] }

/// Emit a jsonb_set expression: jsonb_set(target, path, value[, path2, value2, ...])
/// Each assignment is (JsonPath, SqlExpr). The target and value expressions
/// are emitted by the caller-provided emitExprFn to avoid circular dependencies.
let emitJsonSet (ctx: EmitContext) (emitExprFn: EmitContext -> SqlExpr -> Emitted) (target: SqlExpr) (assignments: (JsonPath * SqlExpr) list) : Emitted =
    let mutable result = emitExprFn ctx target
    for (JsonPath segments, valueExpr) in assignments do
        let path = ctx.FormatJsonPath(segments)
        let valueEmitted = emitExprFn ctx valueExpr
        result <-
            { Sql = sprintf "jsonb_set(%s, %s, %s)" result.Sql path valueEmitted.Sql
              Parameters = result.Parameters @ valueEmitted.Parameters }
    result

/// Emit a JSON array expression: json_array(e1, e2, ...)
let emitJsonArray (ctx: EmitContext) (emitExprFn: EmitContext -> SqlExpr -> Emitted) (elements: SqlExpr list) : Emitted =
    let parts = elements |> List.map (emitExprFn ctx)
    let sql = parts |> List.map (fun p -> p.Sql) |> String.concat ", "
    let parms = parts |> List.collect (fun p -> p.Parameters)
    { Sql = sprintf "json_array(%s)" sql; Parameters = parms }

/// Emit a JSON object expression: jsonb_object('key1', val1, 'key2', val2, ...)
/// Uses jsonb_object for JSONB storage format (product requirement).
let emitJsonObject (ctx: EmitContext) (emitExprFn: EmitContext -> SqlExpr -> Emitted) (properties: (string * SqlExpr) list) : Emitted =
    let parts =
        properties
        |> List.collect (fun (key, valueExpr) ->
            let valueEmitted = emitExprFn ctx valueExpr
            [ { Sql = sprintf "'%s'" key; Parameters = [] }; valueEmitted ])
    let sql = parts |> List.map (fun p -> p.Sql) |> String.concat ", "
    let parms = parts |> List.collect (fun p -> p.Parameters)
    { Sql = sprintf "jsonb_object(%s)" sql; Parameters = parms }
