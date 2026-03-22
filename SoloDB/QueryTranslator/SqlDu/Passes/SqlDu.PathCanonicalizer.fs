module SoloDatabase.PathCanonicalizer

open SqlDu.Engine.C1.Spec

// ══════════════════════════════════════════════════════════════
// Path canonicalization engine.
//
// Normalize equivalent JSON path forms to canonical DU form.
//   - Nested jsonb_extract(jsonb_extract(V, '$.A'), '$.B') -> jsonb_extract(V, '$.A.B')
//   - Path quoting normalization (if applicable)
//
// Simplify equivalent extract wrapper composition.
//   - json_extract(jsonb_extract(V, '$.X'), '$') -> jsonb_extract(V, '$.X')
//   - Identity extractions on base column
//
// No json_*/jsonb_* variant interchange.
// ══════════════════════════════════════════════════════════════

/// Attempt to merge nested jsonb_extract expressions into a single extraction.
/// jsonb_extract(jsonb_extract(source, '$.A'), '$.B') -> jsonb_extract(source, '$.A.B')
/// Returns Some(merged) if mergeable, None otherwise.
let mergeNestedExtract (expr: SqlExpr) : SqlExpr option =
    match expr with
    | JsonExtractExpr(None, "Value", _) ->
        // jsonb_extract(Value, '$.outerSegs') where Value is itself a column
        // This is already flat — no nesting to merge.
        None
    | FunctionCall("jsonb_extract", [JsonExtractExpr(alias, col, innerPath); Literal(String pathStr)]) ->
        // FunctionCall("jsonb_extract", [inner_extract, path_literal])
        // Merge: jsonb_extract(jsonb_extract(alias.col, '$.inner'), '$.outer')
        // -> jsonb_extract(alias.col, '$.inner.outer')
        if pathStr.StartsWith("$.") then
            let outerSegs = pathStr.Substring(2).Split('.') |> Array.toList
            Some(JsonExtractExpr(alias, col, JsonPathOps.ofList (JsonPathOps.toList innerPath @ outerSegs)))
        else
            None
    | _ -> None

/// Attempt to simplify redundant json_extract wrapper around jsonb_extract.
/// json_extract(jsonb_extract(V, '$.X'), '$') -> jsonb_extract(V, '$.X')
/// NOTE: This ONLY removes json_extract wrappers with identity path '$'.
/// It does NOT interchange jsonb_extract <-> json_extract.
let simplifyIdentityWrapper (expr: SqlExpr) : SqlExpr option =
    match expr with
    | FunctionCall("json_extract", [inner; Literal(String "$")]) ->
        match inner with
        | JsonExtractExpr _ ->
            // json_extract(jsonb_extract(...), '$') — the outer json_extract is
            // an identity extraction on the result of jsonb_extract.
            // HOWEVER: json_extract returns TEXT, jsonb_extract returns BLOB.
            // Removing the wrapper changes the return type.
            // The wrapper changes the return type and must be preserved.
            None
        | _ -> None
    | _ -> None

/// Apply all canonicalization rules to a single expression.
/// Returns Some(canonical) if any rule fired, None if no change.
let canonicalizeJsonbExpr (expr: SqlExpr) : SqlExpr option =
    SqlExpr.tryMap
        (fun node ->
            match mergeNestedExtract node with
            | Some merged -> Some merged
            | None -> simplifyIdentityWrapper node)
        expr
