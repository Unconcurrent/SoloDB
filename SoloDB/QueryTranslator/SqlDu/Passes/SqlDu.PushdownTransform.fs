module internal SoloDatabase.PushdownTransform

open SoloDatabase.SqlModel
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
let private pushdownInCore (changed: bool ref) (outer: SelectCore) : SelectCore =
    match outer.Source with
    | Some(DerivedTable(innerSel, alias)) when outer.Where.IsSome ->
        match innerSel.Body with
        | UnionAllSelect(head, tail) ->
            // UNION ALL predicate distribution: push conjuncts into every arm
            // when ALL arms are structurally safe and conjunct refs exist in ALL arms.
            let allArms = head :: tail
            let allArmsSafe =
                allArms |> List.forall (fun arm ->
                    arm.GroupBy.IsEmpty
                    && arm.Having.IsNone
                    && arm.Limit.IsNone
                    && arm.Offset.IsNone
                    && not arm.Distinct
                    && not (arm.Projections |> ProjectionSetOps.toList |> List.exists (fun p -> ExpressionPredicates.hasWindowFunction p.Expr))
                    && not (arm.Projections |> ProjectionSetOps.toList |> List.exists (fun p -> ExpressionPredicates.hasAggregateCall p.Expr)))
            if not allArmsSafe then outer
            else
                // Column set = intersection of all arms' projections
                let armColumnSets = allArms |> List.map buildInnerColumnSet
                let commonColumns = armColumnSets |> List.reduce Set.intersect

                let conjuncts = splitConjuncts outer.Where.Value

                let pushable, stays =
                    conjuncts
                    |> List.partition (fun c ->
                        predicateRefsAvailable c commonColumns alias
                        && not (hasCorrelatedRef c)
                        && not (PushdownSafety.hasAggregateCall c))

                if pushable.IsEmpty then outer
                else
                    changed.Value <- true
                    // Distribute pushable conjuncts to every arm
                    let distributeToArm (arm: SelectCore) =
                        let armAliasMap = buildAliasMap arm
                        let rewrittenPushable =
                            pushable |> List.map (rewriteExpr armAliasMap alias)
                        let newWhere =
                            match arm.Where with
                            | Some existing ->
                                match joinConjuncts rewrittenPushable with
                                | Some p -> Some(Binary(existing, And, p))
                                | None -> Some existing
                            | None -> joinConjuncts rewrittenPushable
                        { arm with Where = newWhere }

                    let newHead = distributeToArm head
                    let newTail = tail |> List.map distributeToArm
                    let newInnerSel = { innerSel with Body = UnionAllSelect(newHead, newTail) }
                    let newOuterWhere = joinConjuncts stays

                    { outer with
                        Source = Some(DerivedTable(newInnerSel, alias))
                        Where = newOuterWhere }
        | _ ->
        // SingleSelect path: check if inner is structurally safe for pushdown
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
                    changed.Value <- true
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
            | _ -> outer // Unreachable: UnionAll handled above
    | _ -> outer // No DerivedTable source or no WHERE — nothing to push

/// Recursively apply pushdown to a SqlSelect at every nesting level.
let rec pushdownSelect (changed: bool ref) (sel: SqlSelect) : SqlSelect =
    let recurse = pushdownSelect changed
    let core = mapDerivedSources recurse >> pushdownInCore changed
    mapSelectParts (mapCores core) recurse sel

/// Push predicates in a SqlStatement.
let pushdownStatement (stmt: SqlStatement) : struct(SqlStatement * bool) =
    match stmt with
    | SelectStmt sel ->
        let changed = ref false
        let result = pushdownSelect changed sel
        struct((if obj.ReferenceEquals(sel, result) then stmt else SelectStmt result), changed.Value)
    // INSERT … SELECT recurses so predicate pushdown reaches the chain SELECT subtree
    // emitted for N-hop B4. UPDATE/DELETE WHERE is intentionally skipped: pushing predicates
    // through a DML WHERE has no inner subquery to push them into, and the chain SELECT used
    // by N-hop B4 lives inside Update.Where as InSubquery — flattenSelect (FlattenTransform)
    // already handles that nested-SELECT shape and pushdown runs on the SELECT before it is
    // wrapped into the predicate. DDL has no expression tree.
    | InsertStmt ins ->
        match ins.Source with
        | InsertValues _ -> struct(stmt, false)
        | InsertSelect sel ->
            let changed = ref false
            let pushed = pushdownSelect changed sel
            struct((if obj.ReferenceEquals(sel, pushed) then stmt else InsertStmt { ins with Source = InsertSelect pushed }), changed.Value)
    | UpdateStmt _ | DeleteStmt _ -> struct(stmt, false)
