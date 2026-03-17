module SoloDatabase.PushdownTransform

open SqlDu.Engine.C1.Spec
open SoloDatabase.PushdownSafety
open SoloDatabase.AliasRewrite

// ══════════════════════════════════════════════════════════════
// Predicate Pushdown Transform
//
// When a SELECT has an outer WHERE over a DerivedTable source, and the
// inner query passes structural safety checks (P-S2..P-S9), push
// safe predicate conjuncts into the inner query's WHERE.
//
// Steps:
//   1. Find outer WHERE on a DerivedTable source
//   2. Split into top-level AND conjuncts
//   3. For each conjunct, check pushdown safety (P-S1..P-S9)
//   4. Safe conjuncts: rewrite aliases and merge into inner WHERE
//   5. Unsafe conjuncts: leave in outer WHERE
//   6. If all conjuncts pushed, remove outer WHERE
//
// Alias substitution: outer references like alias.col are rewritten
// to the inner expression that the alias maps to.
// ══════════════════════════════════════════════════════════════

/// Build alias→expression map from inner projections.
/// Uses raw inner projection expressions with quote normalization only.
/// Matches FlattenTransform parity: no provenance resolution (avoids premature base-table binding).
let private buildAliasMap (innerCore: SelectCore) : Map<string, SqlExpr> =
    buildProjectionAliasMap innerCore
    |> Map.map (fun _ expr -> FlattenTransform.normalizeExprQuoting expr)

/// Rewrite a predicate expression, substituting DerivedTable alias references
/// with the corresponding inner expressions.
let private rewriteExpr (aliasMap: Map<string, SqlExpr>) (derivedAlias: string) (expr: SqlExpr) : SqlExpr =
    rewriteDerivedAliasExpr
        { MatchEmptyDerivedAlias = true
          OnSubquerySelect = id }
        aliasMap
        derivedAlias
        expr

/// Push predicates through a DerivedTable boundary in a single SelectCore.
/// Returns the modified outer core (with reduced/removed WHERE) and the
/// modified inner SqlSelect (with additional WHERE conjuncts).
let private pushdownInCore (outer: SelectCore) : SelectCore =
    match outer.Source with
    | Some(DerivedTable(innerSel, alias)) when outer.Where.IsSome ->
        // Check if inner is structurally safe for pushdown
        if not (isInnerPushdownSafe innerSel) then outer
        else
            match innerSel.Body with
            | SingleSelect innerCore ->
                let innerColumns = buildInnerColumnSet innerCore
                let aliasMap = buildAliasMap innerCore

                // Split outer WHERE into conjuncts
                let conjuncts = splitConjuncts outer.Where.Value

                // Classify each conjunct
                let pushable, stays =
                    conjuncts
                    |> List.partition (fun c ->
                        isConjunctPushdownSafe c innerSel innerColumns alias)

                if pushable.IsEmpty then
                    // Nothing to push — return unchanged
                    outer
                else
                    // Rewrite pushable conjuncts using alias map
                    let rewrittenPushable =
                        pushable |> List.map (rewriteExpr aliasMap alias)

                    // Merge with inner WHERE
                    let newInnerWhere =
                        match innerCore.Where with
                        | Some existingWhere ->
                            // AND the pushed conjuncts with existing inner WHERE
                            let pushed = joinConjuncts rewrittenPushable
                            match pushed with
                            | Some p -> Some(Binary(existingWhere, And, p))
                            | None -> Some existingWhere
                        | None ->
                            joinConjuncts rewrittenPushable

                    let newInnerCore = { innerCore with Where = newInnerWhere }
                    let newInnerSel = { innerSel with Body = SingleSelect newInnerCore }

                    // Remaining outer WHERE
                    let newOuterWhere = joinConjuncts stays

                    { outer with
                        Source = Some(DerivedTable(newInnerSel, alias))
                        Where = newOuterWhere }
            | _ -> outer // UnionAll — P-S8 blocks
    | _ -> outer // No DerivedTable source or no WHERE — nothing to push

/// Recursively apply pushdown to a SqlSelect at every nesting level.
let rec pushdownSelect (sel: SqlSelect) : SqlSelect =
    let pushedBody =
        match sel.Body with
        | SingleSelect outer ->
            // First, recurse into DerivedTable source
            let outerWithRecursedSource =
                match outer.Source with
                | Some(DerivedTable(innerSel, alias)) ->
                    { outer with Source = Some(DerivedTable(pushdownSelect innerSel, alias)) }
                | _ -> outer

            // Recurse into JOINs
            let outerWithRecursedJoins =
                { outerWithRecursedSource with
                    Joins = outerWithRecursedSource.Joins |> List.map (fun j ->
                        match j with
                        | CrossJoin(DerivedTable(jSel, jAlias)) ->
                            CrossJoin(DerivedTable(pushdownSelect jSel, jAlias))
                        | ConditionedJoin(kind, DerivedTable(jSel, jAlias), onExpr) ->
                            ConditionedJoin(kind, DerivedTable(pushdownSelect jSel, jAlias), onExpr)
                        | CrossJoin _ ->
                            j
                        | ConditionedJoin _ ->
                            j
                    )
                }

            // Now try pushdown at this level
            SingleSelect(pushdownInCore outerWithRecursedJoins)

        | UnionAllSelect(head, tail) ->
            // Recurse into each arm but don't push across UNION ALL
            let pushHead = pushdownSelectCore head
            let pushTail = tail |> List.map pushdownSelectCore
            UnionAllSelect(pushHead, pushTail)

    let pushedCtes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = pushdownSelect cte.Query })
    { Ctes = pushedCtes; Body = pushedBody }

/// Pushdown within a SelectCore (for UNION ALL arms).
and private pushdownSelectCore (core: SelectCore) : SelectCore =
    let sel = { Ctes = []; Body = SingleSelect core }
    let pushed = pushdownSelect sel
    match pushed.Body with
    | SingleSelect c -> c
    | _ -> core

/// Push predicates in a SqlStatement.
let pushdownStatement (stmt: SqlStatement) : SqlStatement =
    match stmt with
    | SelectStmt sel -> SelectStmt(pushdownSelect sel)
    // DML predicate optimization is out of scope for C6.
    // UpdateStmt, DeleteStmt, InsertStmt, DdlStmt pass through unchanged.
    | InsertStmt _ | UpdateStmt _ | DeleteStmt _ | DdlStmt _ -> stmt
