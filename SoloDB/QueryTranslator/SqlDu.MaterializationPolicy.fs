module SoloDatabase.MaterializationPolicy

open SqlDu.Engine.C1.Spec

// ══════════════════════════════════════════════════════════════
// C9d: Materialization Policy Engine
//
// JR-4: Flatten-or-preserve decision for materialization wrappers.
//
// Policy:
//   - DBRef/relation materialization patterns (CASE WHEN + jsonb_set +
//     jsonb_array) are PRESERVE_REQUIRED — they encode the observable
//     contract for relation loading.
//   - Group aggregation patterns (json_object + json_group_array +
//     jsonb_set) are PRESERVE_REQUIRED — rewriting changes behavior.
//   - COALESCE + jsonb_set initialization patterns are PRESERVE_REQUIRED
//     — they provide atomicity guarantees.
//   - Transport-only wrappers with no observable contract can be
//     FLATTEN_ALLOWED (synthetic shapes only; none in live corpus).
//
// Contract rule: if a JsonSetExpr or JsonArrayExpr appears in a
// LEFT JOIN projection with a CASE WHEN guard, it is a relation
// materialization pattern and MUST be preserved.
// ══════════════════════════════════════════════════════════════

/// Check if an expression is a relation materialization pattern.
/// Pattern: jsonb_set(target, path, CASE WHEN ... THEN jsonb_array(...) ELSE jsonb_extract(...) END)
let rec isRelationMaterializationExpr (expr: SqlExpr) : bool =
    match expr with
    | JsonSetExpr(_, assignments) ->
        assignments |> List.exists (fun (_, v) ->
            match v with
            | CaseExpr(branches, _) ->
                branches |> List.exists (fun (_, result) ->
                    match result with
                    | JsonArrayExpr _ -> true
                    | _ -> false)
            | _ -> false)
    | _ -> false

/// Check if an expression is a group aggregation materialization pattern.
/// Pattern: jsonb_set(target, path, json_group_array(...))
let rec isGroupMaterializationExpr (expr: SqlExpr) : bool =
    match expr with
    | JsonSetExpr(_, assignments) ->
        assignments |> List.exists (fun (_, v) ->
            match v with
            | AggregateCall(JsonGroupArray, _, _, _) -> true
            | _ -> false)
    | _ -> false

/// Check if an expression is a COALESCE + jsonb_set initialization pattern.
/// Pattern: jsonb_set(COALESCE(source, jsonb('{}')), path, value)
let isCoalesceInitializationExpr (expr: SqlExpr) : bool =
    match expr with
    | JsonSetExpr(Coalesce _, _) -> true
    | _ -> false

/// Check if a SelectCore contains a LEFT JOIN with materialization in projections.
let coreHasRelationMaterialization (core: SelectCore) : bool =
    let hasLeftJoin = core.Joins |> List.exists (fun j -> j.Kind = Left)
    if not hasLeftJoin then false
    else
        core.Projections |> List.exists (fun p -> isRelationMaterializationExpr p.Expr)

/// Check if a SelectCore contains group aggregation materialization.
let coreHasGroupMaterialization (core: SelectCore) : bool =
    core.Projections |> List.exists (fun p -> isGroupMaterializationExpr p.Expr)

/// Determine if a JsonSetExpr in a core is a transport-only wrapper
/// that can be safely flattened. In practice, all live corpus JsonSetExpr
/// usages are either relation materialization or group aggregation,
/// both of which are PRESERVE_REQUIRED.
let isTransportOnlyWrapper (core: SelectCore) (expr: SqlExpr) : bool =
    match expr with
    | JsonSetExpr _ ->
        not (isRelationMaterializationExpr expr)
        && not (isGroupMaterializationExpr expr)
        && not (isCoalesceInitializationExpr expr)
    | _ -> false

/// Check if a JsonSetExpr assignment is an identity write:
/// the value is jsonb_extract(target, same_path) — writing a path back to itself.
/// Comparing source alias/column of the target with the extraction source.
let private isIdentityAssignment (target: SqlExpr) (path: JsonPath) (value: SqlExpr) : bool =
    match target, value with
    | Column(targetAlias, targetCol), JsonExtractExpr(valAlias, valCol, valPath) ->
        targetAlias = valAlias && targetCol = valCol && path = valPath
    | _ -> false

/// Attempt to flatten a JsonSetExpr by removing identity assignments.
/// Returns Some(flattened) if any identity assignments were removed.
/// If ALL assignments are identity → returns just the target.
/// If SOME are identity → returns JsonSetExpr with remaining assignments.
/// If NONE are identity → returns None (no change).
let flattenIdentityAssignments (expr: SqlExpr) : SqlExpr option =
    match expr with
    | JsonSetExpr(target, assignments) when not (isRelationMaterializationExpr expr)
                                         && not (isGroupMaterializationExpr expr)
                                         && not (isCoalesceInitializationExpr expr) ->
        let nonIdentity =
            assignments |> List.filter (fun (path, value) ->
                not (isIdentityAssignment target path value))
        if nonIdentity.Length = assignments.Length then
            None // No identity assignments found
        elif nonIdentity.IsEmpty then
            Some target // All assignments were identity — flatten to just target
        else
            Some(JsonSetExpr(target, nonIdentity)) // Remove identity, keep rest
    | _ -> None
