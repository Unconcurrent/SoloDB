module SoloDatabase.FlattenSafety

open SqlDu.Engine.C1.Spec

// ══════════════════════════════════════════════════════════════
// Flatten-Safety Predicate
//
// Determines whether a DerivedTable in a FROM clause can be safely
// flattened (merged into the outer query).
//
// A DerivedTable is safe to flatten when:
//   F1: Inner body is SingleSelect (not UnionAll)
//   F2: Inner has no GROUP BY
//   F3: Inner has no HAVING
//   F4: Inner has no LIMIT/OFFSET UNLESS outer is a pure projection
//       wrapper (no WHERE, GROUP BY, HAVING, DISTINCT)
//   F5: Inner has no DISTINCT
//   F6: Inner has no window functions in projections
//   F7: Inner has no aggregate calls in projections
//   F8: Outer has no conflicting GROUP BY that would change merge semantics
// ══════════════════════════════════════════════════════════════

/// Check if an expression contains a window function call.
let rec private hasWindowFunction (expr: SqlExpr) : bool =
    match expr with
    | WindowCall _ -> true
    | Binary(l, _, r) -> hasWindowFunction l || hasWindowFunction r
    | Unary(_, e) -> hasWindowFunction e
    | FunctionCall(_, args) -> args |> List.exists hasWindowFunction
    | Coalesce(exprs) -> exprs |> List.exists hasWindowFunction
    | Cast(e, _) -> hasWindowFunction e
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasWindowFunction c || hasWindowFunction r)
        || (elseE |> Option.map hasWindowFunction |> Option.defaultValue false)
    | JsonSetExpr(t, assignments) ->
        hasWindowFunction t || assignments |> List.exists (fun (_, e) -> hasWindowFunction e)
    | JsonArrayExpr(elems) -> elems |> List.exists hasWindowFunction
    | JsonObjectExpr(props) -> props |> List.exists (fun (_, v) -> hasWindowFunction v)
    | AggregateCall _ -> false // aggregate, not window
    | _ -> false

/// Check if an expression contains an aggregate call.
let rec private hasAggregateCall (expr: SqlExpr) : bool =
    match expr with
    | AggregateCall _ -> true
    | Binary(l, _, r) -> hasAggregateCall l || hasAggregateCall r
    | Unary(_, e) -> hasAggregateCall e
    | FunctionCall(_, args) -> args |> List.exists hasAggregateCall
    | Coalesce(exprs) -> exprs |> List.exists hasAggregateCall
    | Cast(e, _) -> hasAggregateCall e
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasAggregateCall c || hasAggregateCall r)
        || (elseE |> Option.map hasAggregateCall |> Option.defaultValue false)
    | JsonSetExpr(t, assignments) ->
        hasAggregateCall t || assignments |> List.exists (fun (_, e) -> hasAggregateCall e)
    | JsonArrayExpr(elems) -> elems |> List.exists hasAggregateCall
    | JsonObjectExpr(props) -> props |> List.exists (fun (_, v) -> hasAggregateCall v)
    | WindowCall _ -> false
    | _ -> false

/// Check if the outer SelectCore is a pure projection wrapper:
/// no WHERE, no GROUP BY, no HAVING, no DISTINCT, no LIMIT/OFFSET, no ORDER BY.
let private isPureProjectionWrapper (outer: SelectCore) : bool =
    outer.Where.IsNone
    && outer.GroupBy.IsEmpty
    && outer.Having.IsNone
    && not outer.Distinct
    && outer.Limit.IsNone
    && outer.Offset.IsNone
    && outer.OrderBy.IsEmpty
    && outer.Joins.IsEmpty

/// Determine if a DerivedTable's inner SelectCore can be safely flattened
/// into the given outer SelectCore.
let isFlattenSafe (outer: SelectCore) (innerCore: SelectCore) : bool =
    // F2: No GROUP BY
    innerCore.GroupBy.IsEmpty
    // F3: No HAVING
    && innerCore.Having.IsNone
    // F4: No LIMIT/OFFSET unless outer is pure projection wrapper
    && (innerCore.Limit.IsNone && innerCore.Offset.IsNone || isPureProjectionWrapper outer)
    // F5: No DISTINCT
    && not innerCore.Distinct
    // F6: No window functions in projections
    && not (innerCore.Projections |> List.exists (fun p -> hasWindowFunction p.Expr))
    // F7: No aggregate calls in projections
    && not (innerCore.Projections |> List.exists (fun p -> hasAggregateCall p.Expr))
    // F8: Outer has no conflicting GROUP BY
    && outer.GroupBy.IsEmpty

/// Check if a SqlSelect's body is a SingleSelect with a DerivedTable source,
/// and if so, whether it's flatten-safe.
let canFlatten (sel: SqlSelect) : bool =
    match sel.Body with
    | SingleSelect outer ->
        match outer.Source with
        | Some(DerivedTable(innerSel, _)) ->
            match innerSel.Body with
            | SingleSelect innerCore ->
                innerSel.Ctes.IsEmpty && isFlattenSafe outer innerCore
            | UnionAllSelect _ -> false // F1: must be SingleSelect
        | _ -> false
    | UnionAllSelect _ -> false
