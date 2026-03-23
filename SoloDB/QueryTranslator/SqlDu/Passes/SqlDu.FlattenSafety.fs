module internal SoloDatabase.FlattenSafety

open SqlDu.Engine.C1.Spec
open SoloDatabase.ExpressionPredicates

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

let private isSimpleProjectionExpr (expr: SqlExpr) : bool =
    match expr with
    | Column(Some _, _)
    | JsonExtractExpr(Some _, _, _) -> true
    | _ -> false

let private hasSimpleOuterProjections (outer: SelectCore) : bool =
    outer.Projections |> ProjectionSetOps.toList |> List.forall (fun p -> isSimpleProjectionExpr p.Expr)

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
    && not (innerCore.Projections |> ProjectionSetOps.toList |> List.exists (fun p -> hasWindowFunction p.Expr))
    // F7: No aggregate calls in projections
    && not (innerCore.Projections |> ProjectionSetOps.toList |> List.exists (fun p -> hasAggregateCall p.Expr))
    // F8: Outer has no conflicting GROUP BY
    && outer.GroupBy.IsEmpty
    // F9: Outer joins remain fail-closed (join merge deferred)
    && outer.Joins.IsEmpty
    // Expression-complexity guard: outer projections must be simple (Column/JsonExtractExpr).
    // hasBaseTableInnerSource removed — provenance-backed resolution handles DerivedTable sources.
    && hasSimpleOuterProjections outer

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
