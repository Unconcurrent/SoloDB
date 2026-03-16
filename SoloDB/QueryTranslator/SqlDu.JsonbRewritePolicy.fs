module SoloDatabase.JsonbRewritePolicy

open SqlDu.Engine.C1.Spec
open SoloDatabase.IndexModel
open SoloDatabase.PathCanonicalizer
open SoloDatabase.SetChainAnalyzer
open SoloDatabase.MaterializationPolicy
open SoloDatabase.ExpressionPredicates
open SoloDatabase.SelectCoreBoundary

// ══════════════════════════════════════════════════════════════
// JSONB rewrite policy pass.
//
// 7th pass in pipeline:
//   Identity -> ConstantFold -> SubqueryFlatten -> PredicatePushdown
//   -> ProjectionPushdown -> IndexPlanShaping
//   -> JsonbRewritePolicy
//
// Legal adjustments:
//   Path canonicalization (nested extract merge)
//   Extract wrapper normalization (disabled: return-type change risk)
//   jsonb_set chain flattening (disjoint paths only)
//   Materialization flatten/preserve policy (identity assignment removal)
//
// Must-not boundaries (same fence pattern as index shaping):
//   - GROUP BY / HAVING / AGGREGATE scopes
//   - WINDOW scopes
//   - UNION ALL / set-op compositions
//   - Correlated EXISTS / NOT EXISTS (outer unchanged)
//   - LEFT JOIN with materialization (PRESERVE_REQUIRED)
//   - json_each boundaries
//   - All DML statements
//
// Index visibility guard:
//   Before rewriting a JSONB expression, checks if any sub-expression
//   matches a known index entry. If so, preserves the expression to
//   protect index visibility (PRESERVED_FOR_PLAN / IndexVisibilityRisk).
// ══════════════════════════════════════════════════════════════

/// Check if an expression matches a specific index entry.
/// Ignores table qualifier during matching (indexes are stored unqualified).
let private exprMatchesIndexEntry (indexExpr: SqlExpr) (expr: SqlExpr) : bool =
    match indexExpr, expr with
    | JsonExtractExpr(None, c1, p1), JsonExtractExpr(_, c2, p2) ->
        c1 = c2 && p1 = p2
    | Cast(JsonExtractExpr(None, c1, p1), t1), Cast(JsonExtractExpr(_, c2, p2), t2) ->
        c1 = c2 && p1 = p2 && t1 = t2
    | _ -> indexExpr = expr

/// Check if an expression or any sub-expression matches any known index entry.
/// Used as a guard to prevent rewrites that would break index visibility.
let exprContainsIndexedForm (model: IndexModel) (expr: SqlExpr) : bool =
    SqlExpr.exists
        (fun node ->
            model.Indexes |> List.exists (fun idx -> exprMatchesIndexEntry idx.Expression node))
        expr

/// Apply path-canonicalization and set-chain rewrites to a single expression.
/// Extract-wrapper normalization is disabled due to return-type change risk.
/// Skips rewrite if expression contains an indexed form (index visibility guard).
let private rewriteExpr (model: IndexModel) (expr: SqlExpr) : SqlExpr =
    // Index visibility guard: if any sub-expression matches an index, preserve
    if exprContainsIndexedForm model expr then expr
    else
        // First try set-chain flattening
        let afterChain =
            match flattenSetChain expr with
            | Some flattened -> flattened
            | None -> expr
        // Then try path canonicalization
        match canonicalizeJsonbExpr afterChain with
        | Some canonical -> canonical
        | None -> afterChain

/// Apply rewrites to a predicate tree (walks AND/OR).
let rec private rewritePredicate (model: IndexModel) (expr: SqlExpr) : SqlExpr =
    match expr with
    | Binary(l, (And as op), r) ->
        Binary(rewritePredicate model l, op, rewritePredicate model r)
    | Binary(l, (Or as op), r) ->
        Binary(rewritePredicate model l, op, rewritePredicate model r)
    | Unary(Not, e) ->
        Unary(Not, rewritePredicate model e)
    | _ -> rewriteExpr model expr

/// Apply the JSONB rewrite policy to a single SelectCore.
let private rewriteInCore (model: IndexModel) (core: SelectCore) : SelectCore =
    // Refuse at must-not boundaries
    if hasMustNotBoundary core then core
    elif hasAggregateOrWindowProjections core then core
    else
        // Check for relation materialization — PRESERVE_REQUIRED
        if coreHasRelationMaterialization core then core
        // Check for group materialization — PRESERVE_REQUIRED
        elif coreHasGroupMaterialization core then core
        else
            let mutable shaped = core

            // Rewrite WHERE predicate (with index guard)
            shaped <-
                match shaped.Where with
                | Some where ->
                    let rewritten = rewritePredicate model where
                    if rewritten = where then shaped
                    else { shaped with Where = Some rewritten }
                | None -> shaped

            // Rewrite JOIN ON clauses (with index guard)
            shaped <-
                let newJoins =
                    shaped.Joins |> List.map (fun j ->
                        match j with
                        | ConditionedJoin(kind, source, onExpr) ->
                            let rewritten = rewritePredicate model onExpr
                            if rewritten = onExpr then j
                            else ConditionedJoin(kind, source, rewritten)
                        | CrossJoin _ -> j)
                if newJoins = shaped.Joins then shaped
                else { shaped with Joins = newJoins }

            // Rewrite ORDER BY expressions (with index guard)
            shaped <-
                if shaped.OrderBy.IsEmpty then shaped
                else
                    let newOrderBy =
                        shaped.OrderBy |> List.map (fun ob ->
                            let rewritten = rewriteExpr model ob.Expr
                            if rewritten = ob.Expr then ob
                            else { ob with Expr = rewritten })
                    if newOrderBy = shaped.OrderBy then shaped
                    else { shaped with OrderBy = newOrderBy }

            // Rewrite projections, excluding protected materialization patterns
            shaped <-
                let newProjs =
                    shaped.Projections |> ProjectionSetOps.toList |> List.map (fun p ->
                        // Skip materialization expressions
                        if isRelationMaterializationExpr p.Expr then p
                        elif isGroupMaterializationExpr p.Expr then p
                        elif isCoalesceInitializationExpr p.Expr then p
                        else
                            // Try identity-assignment flattening first
                            match flattenIdentityAssignments p.Expr with
                            | Some flattened -> { p with Expr = flattened }
                            | None ->
                                let rewritten = rewriteExpr model p.Expr
                                if rewritten = p.Expr then p
                                else { p with Expr = rewritten })
                if newProjs = (shaped.Projections |> ProjectionSetOps.toList) then shaped
                else { shaped with Projections = ProjectionSetOps.ofList newProjs }

            shaped

/// Recursively apply the JSONB rewrite policy to a SqlSelect.
let rec rewriteSelectWithModel (model: IndexModel) (sel: SqlSelect) : SqlSelect =
    let rewrittenBody =
        match sel.Body with
        | SingleSelect core ->
            // Recurse into DerivedTable source
            let coreWithRecursedSource =
                match core.Source with
                | Some(DerivedTable(innerSel, alias)) ->
                    { core with Source = Some(DerivedTable(rewriteSelectWithModel model innerSel, alias)) }
                | _ -> core

            // Recurse into JOIN sources
            let coreWithRecursedJoins =
                { coreWithRecursedSource with
                    Joins = coreWithRecursedSource.Joins |> List.map (fun j ->
                        match j with
                        | CrossJoin(DerivedTable(jSel, jAlias)) ->
                            CrossJoin(DerivedTable(rewriteSelectWithModel model jSel, jAlias))
                        | ConditionedJoin(kind, DerivedTable(jSel, jAlias), onExpr) ->
                            ConditionedJoin(kind, DerivedTable(rewriteSelectWithModel model jSel, jAlias), onExpr)
                        | CrossJoin _ ->
                            j
                        | ConditionedJoin _ ->
                            j)
                }

            // Apply rewrite at this level
            SingleSelect(rewriteInCore model coreWithRecursedJoins)

        | UnionAllSelect(head, tail) ->
            // UNION ALL is a hard must-not boundary — return entirely unchanged
            UnionAllSelect(head, tail)

    let rewrittenCtes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = rewriteSelectWithModel model cte.Query })
    { Ctes = rewrittenCtes; Body = rewrittenBody }

/// Apply the JSONB rewrite policy to a SqlStatement with index model.
let rewriteStatementWithModel (model: IndexModel) (stmt: SqlStatement) : SqlStatement =
    match stmt with
    | SelectStmt sel -> SelectStmt(rewriteSelectWithModel model sel)
    // DML passthrough — out of scope for JSONB rewrite policy
    | InsertStmt _ | UpdateStmt _ | DeleteStmt _ | DdlStmt _ -> stmt

/// Apply the JSONB rewrite policy to a SqlStatement (no index model — empty).
let rewriteStatement (stmt: SqlStatement) : SqlStatement =
    rewriteStatementWithModel emptyModel stmt
