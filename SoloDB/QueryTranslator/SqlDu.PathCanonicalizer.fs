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
    | JsonExtractExpr(None, "Value", JsonPath outerSegs) ->
        // jsonb_extract(Value, '$.outerSegs') where Value is itself a column
        // This is already flat — no nesting to merge.
        None
    | FunctionCall("jsonb_extract", [JsonExtractExpr(alias, col, JsonPath innerSegs); Literal(String pathStr)]) ->
        // FunctionCall("jsonb_extract", [inner_extract, path_literal])
        // Merge: jsonb_extract(jsonb_extract(alias.col, '$.inner'), '$.outer')
        // -> jsonb_extract(alias.col, '$.inner.outer')
        if pathStr.StartsWith("$.") then
            let outerSegs = pathStr.Substring(2).Split('.') |> Array.toList
            Some(JsonExtractExpr(alias, col, JsonPath (innerSegs @ outerSegs)))
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
let rec canonicalizeJsonbExpr (expr: SqlExpr) : SqlExpr option =
    // Try nested extract merge
    match mergeNestedExtract expr with
    | Some merged -> Some merged
    | None ->
        // Wrapper simplification is effectively disabled due to the return-type change
        // (json_extract wrapper removal would change return type).
        // Recurse into sub-expressions to find nested candidates.
        match expr with
        | Binary(l, op, r) ->
            let cl = canonicalizeJsonbExpr l
            let cr = canonicalizeJsonbExpr r
            match cl, cr with
            | None, None -> None
            | _ ->
                Some(Binary(cl |> Option.defaultValue l, op, cr |> Option.defaultValue r))
        | Cast(inner, sqlType) ->
            match canonicalizeJsonbExpr inner with
            | Some c -> Some(Cast(c, sqlType))
            | None -> None
        | Unary(op, inner) ->
            match canonicalizeJsonbExpr inner with
            | Some c -> Some(Unary(op, c))
            | None -> None
        | FunctionCall(name, args) ->
            let canonArgs = args |> List.map (fun a ->
                match canonicalizeJsonbExpr a with
                | Some c -> c
                | None -> a)
            if canonArgs = args then None
            else
                // Re-check mergeNestedExtract on updated FunctionCall.
                // Handles multi-level nesting: after inner merge, outer
                // may now match the merge pattern.
                let updated = FunctionCall(name, canonArgs)
                match mergeNestedExtract updated with
                | Some merged -> Some merged
                | None -> Some updated
        | Coalesce(exprs) ->
            let canonExprs = exprs |> List.map (fun e ->
                match canonicalizeJsonbExpr e with
                | Some c -> c
                | None -> e)
            if canonExprs = exprs then None
            else Some(Coalesce(canonExprs))
        | CaseExpr(branches, elseE) ->
            let canonBranches = branches |> List.map (fun (c, r) ->
                let cc = canonicalizeJsonbExpr c |> Option.defaultValue c
                let cr = canonicalizeJsonbExpr r |> Option.defaultValue r
                (cc, cr))
            let canonElse = elseE |> Option.map (fun e ->
                canonicalizeJsonbExpr e |> Option.defaultValue e)
            if canonBranches = branches && canonElse = elseE then None
            else Some(CaseExpr(canonBranches, canonElse))
        | JsonSetExpr(target, assignments) ->
            let ct = canonicalizeJsonbExpr target |> Option.defaultValue target
            let cas = assignments |> List.map (fun (p, e) ->
                (p, canonicalizeJsonbExpr e |> Option.defaultValue e))
            if ct = target && cas = assignments then None
            else Some(JsonSetExpr(ct, cas))
        | JsonArrayExpr(elements) ->
            let ce = elements |> List.map (fun e ->
                canonicalizeJsonbExpr e |> Option.defaultValue e)
            if ce = elements then None
            else Some(JsonArrayExpr(ce))
        | JsonObjectExpr(properties) ->
            let cp = properties |> List.map (fun (k, e) ->
                (k, canonicalizeJsonbExpr e |> Option.defaultValue e))
            if cp = properties then None
            else Some(JsonObjectExpr(cp))
        | _ -> None
