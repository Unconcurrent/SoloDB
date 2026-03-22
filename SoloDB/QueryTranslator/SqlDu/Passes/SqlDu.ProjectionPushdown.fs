module SoloDatabase.ProjectionPushdown

open SqlDu.Engine.C1.Spec
open SoloDatabase.ProjectionLiveness
open SoloDatabase.ExpressionPredicates

// ══════════════════════════════════════════════════════════════
// Projection Pushdown Transform
//
// Narrows inner query projections to only the columns referenced
// by the outer query. Dead projections are removed.
//
// Steps:
//   1. Find DerivedTable source in outer query
//   2. Compute live columns from outer references
//   3. Remove dead projections from inner query
//   4. Recurse into nested levels
//
// Must-not-push boundaries (refuse pushdown):
//   - Inner uses SELECT * (PROJ-3)
//   - Inner body is UnionAll
//   - Inner has window functions that make projections structural
//   - All inner projections are live (nothing to remove)
// ══════════════════════════════════════════════════════════════

/// Check if the outer (consuming) layer has evaluation-order boundaries
/// that make projection narrowing of the inner layer unsafe.
let private hasOuterEvaluationBoundary (outer: SelectCore) : bool =
    not outer.GroupBy.IsEmpty
    || outer.Having.IsSome
    || outer.Distinct
    || outer.Projections |> ProjectionSetOps.toList |> List.exists (fun p -> hasFunctionCall p.Expr && hasAggregateCall p.Expr)
    || outer.Projections |> ProjectionSetOps.toList |> List.exists (fun p -> hasWindowFunction p.Expr)

let private isSimpleProjectionExpr (expr: SqlExpr) : bool =
    match expr with
    | Column(Some _, _)
    | JsonExtractExpr(Some _, _, _) -> true
    | _ -> false

let private isConservativeOuterWrapper (outer: SelectCore) : bool =
    outer.GroupBy.IsEmpty
    && outer.Having.IsNone
    && not outer.Distinct
    && outer.OrderBy.IsEmpty
    && outer.Limit.IsNone
    && outer.Offset.IsNone
    && outer.Joins.IsEmpty
    && (outer.Projections |> ProjectionSetOps.toList |> List.forall (fun p -> isSimpleProjectionExpr p.Expr))

let private hasBaseTableInnerSource (innerSel: SqlSelect) : bool =
    match innerSel.Body with
    | SingleSelect innerCore ->
        match innerCore.Source with
        | Some(BaseTable _) -> true
        | _ -> false
    | UnionAllSelect _ -> false

/// Narrow inner projections to only those that are live.
/// Returns None if no narrowing is possible or allowed.
let private narrowProjections
    (outer: SelectCore)
    (innerSel: SqlSelect)
    (derivedAlias: string) : SelectCore option =
    if not (isProjectionPushdownAllowed innerSel) then None
    elif hasOuterEvaluationBoundary outer then None
    elif not (isConservativeOuterWrapper outer) then None
    // Provenance-backed source check: accept BaseTable OR DerivedTable with fully resolved projections.
    elif not (hasBaseTableInnerSource innerSel ||
              (match innerSel.Body with
               | SingleSelect innerCore -> Provenance.allProjectionsResolved innerCore
               | _ -> false)) then None
    else
        match innerSel.Body with
        | SingleSelect innerCore ->
            match computeDeadProjections outer innerCore derivedAlias with
            | None -> None
            | Some dead ->
                if dead.IsEmpty then None
                else
                    let narrowed =
                        innerCore.Projections
                        |> ProjectionSetOps.toList
                        |> List.filter (fun p ->
                            match projectionAlias p with
                            | Some alias -> not (Set.contains alias dead)
                            | None -> true)
                    if narrowed.IsEmpty then None
                    else
                        let newInnerCore = { innerCore with Projections = ProjectionSetOps.ofList narrowed }
                        let newInnerSel = { innerSel with Body = SingleSelect newInnerCore }
                        Some { outer with Source = Some(DerivedTable(newInnerSel, derivedAlias)) }
        | UnionAllSelect _ -> None

/// Apply projection pushdown to a single SelectCore.
let private pushdownInCore (changed: bool ref) (outer: SelectCore) : SelectCore =
    match outer.Source with
    | Some(DerivedTable(innerSel, alias)) ->
        match narrowProjections outer innerSel alias with
        | Some narrowedOuter -> changed.Value <- true; narrowedOuter
        | None -> outer
    | _ -> outer

/// Recursively apply projection pushdown to a SqlSelect at every nesting level.
let rec pushdownProjectionSelect (changed: bool ref) (sel: SqlSelect) : SqlSelect =
    let pushedBody =
        match sel.Body with
        | SingleSelect outer ->
            let outerWithRecursedSource =
                match outer.Source with
                | Some(DerivedTable(innerSel, alias)) ->
                    { outer with Source = Some(DerivedTable(pushdownProjectionSelect changed innerSel, alias)) }
                | _ -> outer

            let outerWithRecursedJoins =
                { outerWithRecursedSource with
                    Joins = outerWithRecursedSource.Joins |> List.map (fun j ->
                        match j with
                        | CrossJoin(DerivedTable(jSel, jAlias)) ->
                            CrossJoin(DerivedTable(pushdownProjectionSelect changed jSel, jAlias))
                        | ConditionedJoin(kind, DerivedTable(jSel, jAlias), onExpr) ->
                            ConditionedJoin(kind, DerivedTable(pushdownProjectionSelect changed jSel, jAlias), onExpr)
                        | CrossJoin _
                        | ConditionedJoin _ ->
                            j
                    )
                }

            SingleSelect(pushdownInCore changed outerWithRecursedJoins)

        | UnionAllSelect(head, tail) ->
            let pushHead = pushdownProjectionCore changed head
            let pushTail = tail |> List.map (pushdownProjectionCore changed)
            UnionAllSelect(pushHead, pushTail)

    let pushedCtes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = pushdownProjectionSelect changed cte.Query })
    { Ctes = pushedCtes; Body = pushedBody }

/// Pushdown within a SelectCore (for UNION ALL arms).
and private pushdownProjectionCore (changed: bool ref) (core: SelectCore) : SelectCore =
    let sel = { Ctes = []; Body = SingleSelect core }
    let pushed = pushdownProjectionSelect changed sel
    match pushed.Body with
    | SingleSelect c -> c
    | _ -> core

/// Push projections in a SqlStatement.
let pushdownProjectionStatement (stmt: SqlStatement) : struct(SqlStatement * bool) =
    match stmt with
    | SelectStmt sel ->
        let changed = ref false
        let result = pushdownProjectionSelect changed sel
        struct(SelectStmt result, changed.Value)
    | InsertStmt _ | UpdateStmt _ | DeleteStmt _ | DdlStmt _ -> struct(stmt, false)
