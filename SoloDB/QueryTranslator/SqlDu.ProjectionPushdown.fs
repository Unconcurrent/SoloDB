module SoloDatabase.ProjectionPushdown

open SqlDu.Engine.C1.Spec
open SoloDatabase.ProjectionLiveness

// ══════════════════════════════════════════════════════════════
// Projection Pushdown Transform (C7b)
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

/// Check if expression contains an aggregate call.
let rec private hasAggregateInExpr (expr: SqlExpr) : bool =
    match expr with
    | AggregateCall _ -> true
    | Binary(l, _, r) -> hasAggregateInExpr l || hasAggregateInExpr r
    | Unary(_, e) -> hasAggregateInExpr e
    | FunctionCall(_, args) -> args |> List.exists hasAggregateInExpr
    | Coalesce(exprs) -> exprs |> List.exists hasAggregateInExpr
    | Cast(e, _) -> hasAggregateInExpr e
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasAggregateInExpr c || hasAggregateInExpr r)
        || (elseE |> Option.map hasAggregateInExpr |> Option.defaultValue false)
    | _ -> false

/// Check if expression contains a window call.
let rec private hasWindowInExpr (expr: SqlExpr) : bool =
    match expr with
    | WindowCall _ -> true
    | Binary(l, _, r) -> hasWindowInExpr l || hasWindowInExpr r
    | Unary(_, e) -> hasWindowInExpr e
    | FunctionCall(_, args) -> args |> List.exists hasWindowInExpr
    | Coalesce(exprs) -> exprs |> List.exists hasWindowInExpr
    | Cast(e, _) -> hasWindowInExpr e
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasWindowInExpr c || hasWindowInExpr r)
        || (elseE |> Option.map hasWindowInExpr |> Option.defaultValue false)
    | _ -> false

/// Check if the outer (consuming) layer has evaluation-order boundaries
/// that make projection narrowing of the inner layer unsafe.
let private hasOuterEvaluationBoundary (outer: SelectCore) : bool =
    not outer.GroupBy.IsEmpty
    || outer.Having.IsSome
    || outer.Distinct
    || outer.Projections |> List.exists (fun p -> hasFunctionCall p.Expr && hasAggregateInExpr p.Expr)
    || outer.Projections |> List.exists (fun p -> hasWindowInExpr p.Expr)

/// Narrow inner projections to only those that are live.
/// Returns None if no narrowing is possible or allowed.
let private narrowProjections
    (outer: SelectCore)
    (innerSel: SqlSelect)
    (derivedAlias: string) : SelectCore option =
    if not (isProjectionPushdownAllowed innerSel) then None
    elif hasOuterEvaluationBoundary outer then None
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
                        |> List.filter (fun p ->
                            match projectionAlias p with
                            | Some alias -> not (Set.contains alias dead)
                            | None -> true)
                    if narrowed.IsEmpty then None
                    else
                        let newInnerCore = { innerCore with Projections = narrowed }
                        let newInnerSel = { innerSel with Body = SingleSelect newInnerCore }
                        Some { outer with Source = Some(DerivedTable(newInnerSel, derivedAlias)) }
        | UnionAllSelect _ -> None

/// Apply projection pushdown to a single SelectCore.
let private pushdownInCore (outer: SelectCore) : SelectCore =
    match outer.Source with
    | Some(DerivedTable(innerSel, alias)) ->
        match narrowProjections outer innerSel alias with
        | Some narrowedOuter -> narrowedOuter
        | None -> outer
    | _ -> outer

/// Recursively apply projection pushdown to a SqlSelect at every nesting level.
let rec pushdownProjectionSelect (sel: SqlSelect) : SqlSelect =
    let pushedBody =
        match sel.Body with
        | SingleSelect outer ->
            let outerWithRecursedSource =
                match outer.Source with
                | Some(DerivedTable(innerSel, alias)) ->
                    { outer with Source = Some(DerivedTable(pushdownProjectionSelect innerSel, alias)) }
                | _ -> outer

            let outerWithRecursedJoins =
                { outerWithRecursedSource with
                    Joins = outerWithRecursedSource.Joins |> List.map (fun j ->
                        match j.Source with
                        | DerivedTable(jSel, jAlias) ->
                            { j with Source = DerivedTable(pushdownProjectionSelect jSel, jAlias) }
                        | _ -> j
                    )
                }

            SingleSelect(pushdownInCore outerWithRecursedJoins)

        | UnionAllSelect(head, tail) ->
            let pushHead = pushdownProjectionCore head
            let pushTail = tail |> List.map pushdownProjectionCore
            UnionAllSelect(pushHead, pushTail)

    let pushedCtes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = pushdownProjectionSelect cte.Query })
    { Ctes = pushedCtes; Body = pushedBody }

/// Pushdown within a SelectCore (for UNION ALL arms).
and private pushdownProjectionCore (core: SelectCore) : SelectCore =
    let sel = { Ctes = []; Body = SingleSelect core }
    let pushed = pushdownProjectionSelect sel
    match pushed.Body with
    | SingleSelect c -> c
    | _ -> core

/// Push projections in a SqlStatement.
let pushdownProjectionStatement (stmt: SqlStatement) : SqlStatement =
    match stmt with
    | SelectStmt sel -> SelectStmt(pushdownProjectionSelect sel)
    | InsertStmt _ | UpdateStmt _ | DeleteStmt _ | DdlStmt _ -> stmt
