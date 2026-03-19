module SoloDatabase.PushdownSafety

open SqlDu.Engine.C1.Spec
open SoloDatabase.ExpressionPredicates
open SoloDatabase.Provenance

// ══════════════════════════════════════════════════════════════
// Pushdown-Safety Predicate
//
// Determines whether a predicate (or individual conjunct) can be
// safely pushed from an outer WHERE through a DerivedTable boundary
// into the inner query's WHERE.
//
// Safety conditions (all must hold):
//   P-S1: Predicate references only columns available in inner query
//   P-S2: Inner has no GROUP BY
//   P-S3: Inner has no window functions in projections
//   P-S4: Inner has no LIMIT/OFFSET
//   P-S5: Inner has no DISTINCT
//   P-S6: Predicate contains no correlated references to outer query levels
//   P-S7: Predicate contains no aggregate calls
//   P-S8: Inner body is SingleSelect (not UnionAll)
//   P-S9: Inner has no HAVING
//
// Conservative cut (deferred from C6):
//   - UNION ALL arm distribution
//   - GROUP BY key-only pushdown
//   - WHERE-to-HAVING reclassification
// ══════════════════════════════════════════════════════════════

/// Collect all column references from a predicate expression.
/// Returns (sourceAlias option * columnName) pairs.
let collectColumnRefs (expr: SqlExpr) : (string option * string) list =
    let marker = (Some "__correlated__", "__subquery__")
    let maskedSubqueryExpr =
        SqlExpr.map
            (fun node ->
                match node with
                | InSubquery(_, sel) -> InSubquery(Literal Null, sel)
                | _ -> node)
            expr
    SqlExpr.fold
        (fun acc node ->
            match node with
            | Column(src, col) -> (src, col) :: acc
            | JsonExtractExpr(src, col, _) -> (src, col) :: acc
            | InSubquery _ | Exists _ | ScalarSubquery _ -> marker :: acc
            | _ -> acc)
        []
        maskedSubqueryExpr
    |> List.rev

/// Check if an expression contains an aggregate call (P-S7).
let hasAggregateCall (expr: SqlExpr) : bool =
    ExpressionPredicates.hasAggregateCall expr

/// Check if an expression contains correlated references (subqueries) (P-S6).
let hasCorrelatedRef (expr: SqlExpr) : bool =
    ExpressionPredicates.hasCorrelatedRef expr

/// Build the set of column names available in the inner query's projections.
/// Maps alias (or raw column name if unaliased) to the projection expression.
let buildInnerColumnSet (innerCore: SelectCore) : Set<string> =
    innerCore.Projections
    |> ProjectionSetOps.toList
    |> List.choose (fun p ->
        match p.Alias with
        | Some alias -> Some alias
        | None ->
            match p.Expr with
            | Column(_, colName) -> Some colName
            | _ -> None
    )
    |> Set.ofList

/// P-S1: Check that all column references in a predicate are available
/// in the inner query's projection set. The predicate references the
/// DerivedTable alias, which we strip to check against inner column names.
let predicateRefsAvailable (predicate: SqlExpr) (innerColumns: Set<string>) (derivedAlias: string) : bool =
    let refs = collectColumnRefs predicate
    refs |> List.forall (fun (src, col) ->
        match src with
        | Some s when s = derivedAlias || s = "" ->
            // Reference to derived table alias — must exist in inner projections
            Set.contains col innerColumns
        | None ->
            // Unqualified — must exist in inner projections
            Set.contains col innerColumns
        | Some "__correlated__" -> false // Correlated subquery marker
        | Some _ -> false // Reference to some other alias — not pushable
    )

/// Check if a DerivedTable's inner SelectCore allows predicate pushdown.
/// This checks the structural properties of the inner query (P-S2 through P-S5, P-S8, P-S9).
let isInnerPushdownSafe (innerSel: SqlSelect) : bool =
    match innerSel.Body with
    | SingleSelect innerCore ->
        // R42E: provenance-backed source and projection check.
        // Accepts BaseTable OR DerivedTable when all projections resolve to
        // base columns or simple derived column references (not aggregates,
        // not window functions, not opaque expressions).
        let provenanceSafeSourceAndProjections =
            match innerCore.Source with
            | Some(BaseTable _) | Some(DerivedTable _) ->
                Provenance.allProjectionsResolved innerCore
            | _ -> false
        provenanceSafeSourceAndProjections
        // P-S2: No GROUP BY
        && innerCore.GroupBy.IsEmpty
        // P-S3: No window functions in projections
        && not (innerCore.Projections |> ProjectionSetOps.toList |> List.exists (fun p -> hasWindowFunction p.Expr))
        // P-S4: No LIMIT/OFFSET
        && innerCore.Limit.IsNone
        && innerCore.Offset.IsNone
        // P-S5: No DISTINCT
        && not innerCore.Distinct
        // P-S9: No HAVING
        && innerCore.Having.IsNone
    | UnionAllSelect _ -> false // P-S8: Must be SingleSelect

/// Check if a single predicate conjunct is safe to push through a DerivedTable.
/// Combines inner-structural safety with predicate-level safety.
let isConjunctPushdownSafe
    (conjunct: SqlExpr)
    (innerSel: SqlSelect)
    (innerColumns: Set<string>)
    (derivedAlias: string) : bool =
    // Inner must be structurally safe
    isInnerPushdownSafe innerSel
    // P-S1: All refs available in inner
    && predicateRefsAvailable conjunct innerColumns derivedAlias
    // P-S6: No correlated references
    && not (hasCorrelatedRef conjunct)
    // P-S7: No aggregate calls
    && not (hasAggregateCall conjunct)

/// Split a predicate into top-level AND conjuncts.
/// Only splits Binary(_, And, _) at top level; never splits OR.
let rec splitConjuncts (expr: SqlExpr) : SqlExpr list =
    match expr with
    | Binary(l, And, r) -> splitConjuncts l @ splitConjuncts r
    | _ -> [expr]

/// Rejoin conjuncts with AND.
let joinConjuncts (conjuncts: SqlExpr list) : SqlExpr option =
    match conjuncts with
    | [] -> None
    | [single] -> Some single
    | first :: rest ->
        Some (rest |> List.fold (fun acc c -> Binary(acc, And, c)) first)
