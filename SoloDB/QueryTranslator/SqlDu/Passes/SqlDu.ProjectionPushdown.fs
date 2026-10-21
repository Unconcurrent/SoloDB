module internal SoloDatabase.ProjectionPushdown

open SoloDatabase.SqlModel
open SoloDatabase.ProjectionLiveness
open SoloDatabase.ExpressionPredicates
open SoloDatabase.AliasRewrite

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

/// A bounded identity read need not acquire its unused raw document payload.
let private isBoundedRawPayloadWrapper (outer: SelectCore) (innerSel: SqlSelect) derivedAlias =
    let plainBounds =
        (outer.Limit.IsSome || outer.Offset.IsSome) && outer.Where.IsNone && outer.OrderBy.IsEmpty
        && outer.Joins.IsEmpty && outer.GroupBy.IsEmpty && outer.Having.IsNone && not outer.Distinct
        && not (ProjectionSetOps.isAllColumns outer.Projections)
    if not(plainBounds && innerSel.Ctes.IsEmpty) then false else
    let rowDependentBound =
        [outer.Limit; outer.Offset] |> List.exists (Option.exists (SqlExpr.exists (function
            | Column _ | JsonExtractExpr _ | JsonRootExtract _
            | InSubquery _ | Exists _ | ScalarSubquery _ -> true
            | _ -> false)))
    let columnsOnly =
        outer.Projections |> ProjectionSetOps.toList |> List.forall (fun projection ->
            match projection.Expr with
            | Column(Some owner, "Id") -> normalizeSourceAlias owner = normalizeSourceAlias derivedAlias
            | _ -> false)
    if rowDependentBound || not columnsOnly then false else
    match innerSel.Body with
    | SingleSelect inner when inner.Joins.IsEmpty ->
        match inner.Source with
        | Some(BaseTable(table, alias)) ->
            let source = alias |> Option.map normalizeSourceAlias |> Option.defaultValue table
            let ownColumn name = function
                | Column(Some owner, column) -> column = name && normalizeSourceAlias owner = source
                | _ -> false
            let fields = inner.Projections |> ProjectionSetOps.toList
            let identity = fields |> List.exists (fun field -> projectionAlias field = Some "Id" && ownColumn "Id" field.Expr)
            let payload = fields |> List.exists (fun field ->
                match field.Alias, field.Expr with
                | Some "Value", FunctionCall("jsonb", [value]) -> ownColumn "Value" value
                | _ -> false)
            let mutable independent = true
            mapCoreExpressions (fun expression ->
                if hasCorrelatedRef expression ||
                   SqlExpr.fold (fun found -> function
                       | Column(owner, column) when
                           System.String.Equals(column, "Value", System.StringComparison.OrdinalIgnoreCase) &&
                           (owner |> Option.forall (fun name -> normalizeSourceAlias name = "")) -> true
                       | _ -> found) false expression then independent <- false
                expression) id inner |> ignore
            fields.Length = 2 && identity && payload && independent
        | _ -> false
    | _ -> false

/// Narrow inner projections to only those that are live.
/// Returns None if no narrowing is possible or allowed.
let private narrowProjections
    (allowBounded: bool)
    (outer: SelectCore)
    (innerSel: SqlSelect)
    (derivedAlias: string) : SelectCore option =
    let boundedPayload = allowBounded && isBoundedRawPayloadWrapper outer innerSel derivedAlias
    if not (isProjectionPushdownAllowed innerSel) then None
    elif hasOuterEvaluationBoundary outer then None
    elif not (isConservativeOuterWrapper outer || boundedPayload) then None
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
                let dead = if boundedPayload then Set.intersect dead (Set.singleton "Value") else dead
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
let private pushdownInCore (changed: bool ref) allowBounded (outer: SelectCore) : SelectCore =
    match outer.Source with
    | Some(DerivedTable(innerSel, alias)) ->
        match narrowProjections allowBounded outer innerSel alias with
        | Some narrowedOuter -> changed.Value <- true; narrowedOuter
        | None -> outer
    | _ -> outer

/// Recursively apply projection pushdown to a SqlSelect at every nesting level.
let rec private pushdownSelect allowBounded (changed: bool ref) (sel: SqlSelect) : SqlSelect =
    let allowBounded = allowBounded && sel.Ctes.IsEmpty && (match sel.Body with SingleSelect _ -> true | _ -> false)
    let recurse = pushdownSelect allowBounded changed
    let core = mapDerivedSources recurse >> pushdownInCore changed allowBounded
    mapSelectParts (mapCores core) recurse sel

let pushdownProjectionSelect changed sel = pushdownSelect true changed sel

/// Push projections in a SqlStatement.
let pushdownProjectionStatement (stmt: SqlStatement) : struct(SqlStatement * bool) =
    match stmt with
    | SelectStmt sel ->
        let changed = ref false
        let result = pushdownProjectionSelect changed sel
        struct((if obj.ReferenceEquals(sel, result) then stmt else SelectStmt result), changed.Value)
    // INSERT … SELECT recurses so projection pushdown reaches the chain SELECT subtree
    // emitted for N-hop B4. UPDATE/DELETE/DDL have no SELECT subtree at this layer: the chain
    // SELECT used by N-hop B4 inside Update.Where is reached via FlattenTransform's
    // flattenExprDeep before this pass runs at the top level on the wrapped statement.
    | InsertStmt ins ->
        match ins.Source with
        | InsertValues _ -> struct(stmt, false)
        | InsertSelect sel ->
            let changed = ref false
            let pushed = pushdownProjectionSelect changed sel
            struct((if obj.ReferenceEquals(sel, pushed) then stmt else InsertStmt { ins with Source = InsertSelect pushed }), changed.Value)
    | UpdateStmt _ | DeleteStmt _ -> struct(stmt, false)
