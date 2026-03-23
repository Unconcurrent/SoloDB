module internal SoloDatabase.ProjectionLiveness

open SqlDu.Engine.C1.Spec
open SoloDatabase.ExpressionPredicates

// ══════════════════════════════════════════════════════════════
// Projection Liveness Analysis
//
// Determines which inner projection aliases are "live" — referenced
// by the outer query's projections, WHERE, ORDER BY, GROUP BY,
// HAVING, JOIN ON clauses, or subqueries.
//
// A projection alias not in the live set is "dead" and can be
// removed by the projection pushdown pass.
//
// Safety rules:
//   PROJ-1: Narrow inner projections to outer-referenced columns.
//   PROJ-2: Preserve columns referenced by side effects (defensive).
//   PROJ-3: Preserve star projections (SELECT *) — refuse pushdown.
//   PROJ-4: Preserve GROUP BY key columns in inner query.
//
// Must-not-push boundaries:
//   - Column referenced in JOIN ON clause
//   - Column referenced in WHERE/HAVING
//   - Column referenced in ORDER BY
//   - Column used as GROUP BY key
//   - Column used by window PARTITION BY or ORDER BY
//   - Column referenced in correlated subquery
// ══════════════════════════════════════════════════════════════

/// Collect all unqualified column names referenced in an expression.
/// For qualified references (alias.col), if the alias matches the given
/// derivedAlias, we collect the column name. Unqualified columns are
/// always collected.
let collectReferencedColumns (derivedAlias: string) (expr: SqlExpr) : Set<string> =
    SqlExpr.fold
        (fun acc node ->
            match node with
            | Column(Some src, col) when src = derivedAlias || src = "" ->
                Set.add col acc
            | Column(None, col) ->
                Set.add col acc
            | JsonExtractExpr(Some src, col, _) when src = derivedAlias || src = "" ->
                Set.add col acc
            | JsonExtractExpr(None, col, _) ->
                Set.add col acc
            | _ ->
                acc)
        Set.empty
        expr

/// Compute the set of inner projection aliases that are live —
/// referenced by outer query components.
///
/// Analyzes: outer projections, WHERE, ORDER BY, GROUP BY, HAVING, JOIN ON.
let computeLiveColumns (outer: SelectCore) (derivedAlias: string) : Set<string> =
    let fromProjections =
        outer.Projections
        |> ProjectionSetOps.toList
        |> List.map (fun p -> collectReferencedColumns derivedAlias p.Expr)
        |> Set.unionMany

    let fromWhere =
        outer.Where
        |> Option.map (collectReferencedColumns derivedAlias)
        |> Option.defaultValue Set.empty

    let fromOrderBy =
        outer.OrderBy
        |> List.map (fun ob -> collectReferencedColumns derivedAlias ob.Expr)
        |> Set.unionMany

    let fromGroupBy =
        outer.GroupBy
        |> List.map (collectReferencedColumns derivedAlias)
        |> Set.unionMany

    let fromHaving =
        outer.Having
        |> Option.map (collectReferencedColumns derivedAlias)
        |> Option.defaultValue Set.empty

    let fromJoinOn =
        outer.Joins
        |> List.choose (function
            | ConditionedJoin(_, _, onExpr) -> Some onExpr
            | CrossJoin _ -> None)
        |> List.map (collectReferencedColumns derivedAlias)
        |> Set.unionMany

    Set.unionMany [fromProjections; fromWhere; fromOrderBy; fromGroupBy; fromHaving; fromJoinOn]

/// Get the alias name for an inner projection.
/// Returns the explicit alias, or for bare Column references, the column name.
let projectionAlias (p: Projection) : string option =
    match p.Alias with
    | Some alias -> Some alias
    | None ->
        match p.Expr with
        | Column(_, colName) -> Some colName
        | _ -> None

/// Check if the inner query uses implicit projections (SELECT *)
/// that we cannot safely narrow. PROJ-3.
let hasStarProjection (innerCore: SelectCore) : bool =
    innerCore.Projections |> ProjectionSetOps.isAllColumns

/// Collect GROUP BY key column names from the inner query.
/// These must be preserved per PROJ-4.
let collectGroupByKeys (innerCore: SelectCore) : Set<string> =
    innerCore.GroupBy
    |> List.collect (fun expr ->
        match expr with
        | Column(_, col) -> [col]
        | JsonExtractExpr(_, col, _) -> [col]
        | _ -> [])
    |> Set.ofList

/// Check if a projection expression contains a function call
/// (defensive for PROJ-2: side-effect preservation).
let hasFunctionCall (expr: SqlExpr) : bool =
    ExpressionPredicates.hasFunctionCall expr

/// Determine which inner projections are dead (can be removed).
/// Returns the set of projection aliases that are NOT live.
///
/// Returns None if pushdown is refused (PROJ-3: star projections,
/// or other structural reasons).
let computeDeadProjections
    (outer: SelectCore)
    (innerCore: SelectCore)
    (derivedAlias: string) : Set<string> option =
    // PROJ-3: Refuse if inner uses SELECT * (empty projections)
    if hasStarProjection innerCore then None
    else
        let liveColumns = computeLiveColumns outer derivedAlias

        // PROJ-4: GROUP BY keys are always live
        let groupByKeys = collectGroupByKeys innerCore

        // Collect all inner projection aliases
        let allAliases =
            innerCore.Projections
            |> ProjectionSetOps.toList
            |> List.choose projectionAlias
            |> Set.ofList

        let dead =
            allAliases
            |> Set.filter (fun alias ->
                not (Set.contains alias liveColumns)
                && not (Set.contains alias groupByKeys))

        Some dead

/// Check if projection pushdown is structurally allowed on the inner query.
/// Returns false at must-not-push boundaries.
let isProjectionPushdownAllowed (innerSel: SqlSelect) : bool =
    match innerSel.Body with
    | SingleSelect innerCore ->
        not (hasStarProjection innerCore)
        && not (innerCore.Projections |> ProjectionSetOps.isAllColumns)
        && innerCore.GroupBy.IsEmpty
        && innerCore.Having.IsNone
        && not innerCore.Distinct
    | UnionAllSelect _ ->
        false
