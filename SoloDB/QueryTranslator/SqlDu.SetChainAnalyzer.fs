module SoloDatabase.SetChainAnalyzer

open SqlDu.Engine.C1.Spec

// ══════════════════════════════════════════════════════════════
// Set-chain analyzer.
//
// Flatten nested jsonb_set chains when writes are non-conflicting
// and order-safe.
//
// Legal flatten: jsonb_set(jsonb_set(V, '$.A', x), '$.B', y) where
//   paths '$.A' and '$.B' are disjoint (no prefix relationship).
//   -> JsonSetExpr(V, [('$.A', x); ('$.B', y)])
//
// Must preserve:
//   - Overlapping paths: '$.A' and '$.A.B' (prefix conflict)
//   - Same path: '$.A' and '$.A' (write-write conflict)
//   - Data dependency: value expression references a path being written
// ══════════════════════════════════════════════════════════════

/// Check if two JSON paths conflict (one is a prefix of the other, or identical).
let pathsConflict (JsonPath segsA: JsonPath) (JsonPath segsB: JsonPath) : bool =
    // Identical paths conflict
    if segsA = segsB then true
    else
        // Check prefix relationship
        let shorter, longer =
            if segsA.Length <= segsB.Length then segsA, segsB
            else segsB, segsA
        // If shorter is a prefix of longer, they conflict
        longer |> List.take (min shorter.Length longer.Length) = shorter

/// Check if any pair of paths in a list conflicts.
let hasConflictingPaths (paths: JsonPath list) : bool =
    let rec check (remaining: JsonPath list) =
        match remaining with
        | [] -> false
        | p :: rest ->
            if rest |> List.exists (fun other -> pathsConflict p other) then true
            else check rest
    check paths

/// Check if any assignment value expression references a path being written.
/// This detects data dependencies like: jsonb_set(V, '$.B', jsonb_extract(V, '$.A'))
/// when '$.A' is also being written. Conservative: any reference to any written path
/// in any value expression means preserve.
let rec private exprReferencesAnyPath (writtenPaths: JsonPath list) (expr: SqlExpr) : bool =
    match expr with
    | JsonExtractExpr(_, _, path) ->
        writtenPaths |> List.exists (fun wp -> pathsConflict wp path)
    | Binary(l, _, r) ->
        exprReferencesAnyPath writtenPaths l || exprReferencesAnyPath writtenPaths r
    | Unary(_, e) -> exprReferencesAnyPath writtenPaths e
    | Cast(e, _) -> exprReferencesAnyPath writtenPaths e
    | FunctionCall(_, args) -> args |> List.exists (exprReferencesAnyPath writtenPaths)
    | Coalesce(exprs) -> exprs |> List.exists (exprReferencesAnyPath writtenPaths)
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) ->
            exprReferencesAnyPath writtenPaths c || exprReferencesAnyPath writtenPaths r)
        || (elseE |> Option.map (exprReferencesAnyPath writtenPaths) |> Option.defaultValue false)
    | JsonSetExpr(t, assigns) ->
        exprReferencesAnyPath writtenPaths t
        || assigns |> List.exists (fun (_, e) -> exprReferencesAnyPath writtenPaths e)
    | JsonArrayExpr(elems) -> elems |> List.exists (exprReferencesAnyPath writtenPaths)
    | JsonObjectExpr(props) -> props |> List.exists (fun (_, e) -> exprReferencesAnyPath writtenPaths e)
    | _ -> false

/// Attempt to flatten a nested FunctionCall chain of jsonb_set calls into a
/// single JsonSetExpr with multiple assignments.
/// Returns Some(flattened) if safe, None if must preserve.
let flattenSetChain (expr: SqlExpr) : SqlExpr option =
    // Collect nested jsonb_set calls: jsonb_set(jsonb_set(target, p1, v1), p2, v2)
    let rec collect (e: SqlExpr) (acc: (JsonPath * SqlExpr) list) : (SqlExpr * (JsonPath * SqlExpr) list) option =
        match e with
        | FunctionCall("jsonb_set", [inner; Literal(String pathStr); value]) when pathStr.StartsWith("$.") ->
            let segs = pathStr.Substring(2).Split('.') |> Array.toList
            let path = JsonPath segs
            collect inner ((path, value) :: acc)
        | JsonSetExpr(target, existingAssigns) ->
            // Already a JsonSetExpr — extend with accumulated assignments
            Some(target, existingAssigns @ acc)
        | _ ->
            // Base case — this is the target
            if acc.IsEmpty then None // Nothing to flatten
            else Some(e, acc)

    match collect expr [] with
    | None -> None
    | Some(_, assignments) when assignments.Length < 2 -> None // Single assignment, no flattening benefit
    | Some(target, assignments) ->
        let paths = assignments |> List.map fst
        // Check for conflicting paths
        if hasConflictingPaths paths then None
        // Check for data dependencies
        elif assignments |> List.exists (fun (_, v) -> exprReferencesAnyPath paths v) then None
        else
            Some(JsonSetExpr(target, assignments))
