module SoloDatabase.PushdownSafety

open SqlDu.Engine.C1.Spec
open SoloDatabase.ExpressionPredicates

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
let rec collectColumnRefs (expr: SqlExpr) : (string option * string) list =
    match expr with
    | Column(src, col) -> [(src, col)]
    | JsonExtractExpr(src, col, _) -> [(src, col)]
    | Binary(l, _, r) -> collectColumnRefs l @ collectColumnRefs r
    | Unary(_, e) -> collectColumnRefs e
    | FunctionCall(_, args) -> args |> List.collect collectColumnRefs
    | AggregateCall(_, arg, _, sep) ->
        (arg |> Option.map collectColumnRefs |> Option.defaultValue [])
        @ (sep |> Option.map collectColumnRefs |> Option.defaultValue [])
    | Coalesce(exprs) -> exprs |> List.collect collectColumnRefs
    | Cast(e, _) -> collectColumnRefs e
    | CaseExpr(branches, elseE) ->
        (branches |> List.collect (fun (c, r) -> collectColumnRefs c @ collectColumnRefs r))
        @ (elseE |> Option.map collectColumnRefs |> Option.defaultValue [])
    | Between(e, lo, hi) -> collectColumnRefs e @ collectColumnRefs lo @ collectColumnRefs hi
    | InList(e, list) -> collectColumnRefs e @ (list |> List.collect collectColumnRefs)
    | JsonSetExpr(t, assignments) ->
        collectColumnRefs t @ (assignments |> List.collect (fun (_, e) -> collectColumnRefs e))
    | JsonArrayExpr(elems) -> elems |> List.collect collectColumnRefs
    | JsonObjectExpr(props) -> props |> List.collect (fun (_, v) -> collectColumnRefs v)
    | WindowCall(spec) ->
        (spec.Arguments |> List.collect collectColumnRefs)
        @ (spec.PartitionBy |> List.collect collectColumnRefs)
        @ (spec.OrderBy |> List.collect (fun (e, _) -> collectColumnRefs e))
    | InSubquery _ | Exists _ | ScalarSubquery _ ->
        // Subquery references are self-contained; treat as correlated (P-S6 blocks)
        [("__correlated__" |> Some, "__subquery__")]
    | UpdateFragment(path, value) -> collectColumnRefs path @ collectColumnRefs value
    | Literal _ | Parameter _ -> []

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
        // P-S2: No GROUP BY
        innerCore.GroupBy.IsEmpty
        // P-S3: No window functions in projections
        && not (innerCore.Projections |> List.exists (fun p -> hasWindowFunction p.Expr))
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
