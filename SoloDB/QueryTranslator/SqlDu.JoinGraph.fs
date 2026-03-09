module SoloDatabase.JoinGraph

open SqlDu.Engine.C1.Spec

// ══════════════════════════════════════════════════════════════
// Join Graph Analysis (C7c)
//
// Identifies connected INNER JOIN components that are safe to
// reorder. Refuses at outer-join, correlation, set-op, aggregate,
// window, and DML boundaries.
//
// Legal join reorder rules:
//   JOIN-1: Only reorder INNER JOINs.
//   JOIN-2: Only reorder when 2+ INNER JOINs exist in same FROM clause.
//   JOIN-3: Preserve ON clause column references (scope safety).
//   JOIN-4: Preserve result cardinality.
//
// Must-not-reorder cases:
//   - LEFT JOIN (not commutative)
//   - CROSS JOIN (order affects intermediate size)
//   - Single INNER JOIN (nothing to reorder with)
//   - INNER JOIN inside EXISTS/NOT EXISTS (correlation boundary)
//   - JOIN with LATERAL/dependent subquery
// ══════════════════════════════════════════════════════════════

/// Extract a canonical sort key from a TableSource.
/// Used for deterministic canonical ordering of join components.
let tableSortKey (source: TableSource) : string =
    match source with
    | BaseTable(table, aliasOpt) ->
        match aliasOpt with
        | Some alias -> alias
        | None -> table
    | DerivedTable(_, alias) -> alias
    | FromJsonEach(_, aliasOpt) ->
        match aliasOpt with
        | Some alias -> alias
        | None -> "__json_each__"

/// Collect all column source aliases referenced in a JOIN ON expression.
/// Returns the set of source aliases (table aliases or table names) used.
let rec collectOnClauseAliases (expr: SqlExpr) : Set<string> =
    match expr with
    | Column(Some src, _) -> Set.singleton src
    | Column(None, _) -> Set.empty
    | JsonExtractExpr(Some src, _, _) -> Set.singleton src
    | JsonExtractExpr(None, _, _) -> Set.empty
    | Binary(l, _, r) ->
        Set.union (collectOnClauseAliases l) (collectOnClauseAliases r)
    | Unary(_, e) -> collectOnClauseAliases e
    | FunctionCall(_, args) ->
        args |> List.map collectOnClauseAliases |> Set.unionMany
    | Coalesce(exprs) ->
        exprs |> List.map collectOnClauseAliases |> Set.unionMany
    | Cast(e, _) -> collectOnClauseAliases e
    | CaseExpr(branches, elseE) ->
        let branchRefs =
            branches |> List.collect (fun (c, r) -> [collectOnClauseAliases c; collectOnClauseAliases r])
            |> Set.unionMany
        let elseRefs = elseE |> Option.map collectOnClauseAliases |> Option.defaultValue Set.empty
        Set.union branchRefs elseRefs
    | _ -> Set.empty

/// Check if a join chain is a pure INNER JOIN chain (JOIN-1).
/// Returns false if any join in the chain is not INNER.
let isPureInnerJoinChain (joins: JoinShape list) : bool =
    joins |> List.forall (fun j -> j.Kind = Inner)

/// Check if a reordered join sequence maintains ON clause scope safety (JOIN-3).
/// Each ON clause must only reference sources that are already "in scope"
/// (the FROM source + all prior joins in the reordered sequence).
let isReorderScopeSafe (fromSource: TableSource) (reorderedJoins: JoinShape list) : bool =
    let fromKey = tableSortKey fromSource
    let mutable inScope = Set.singleton fromKey
    let mutable safe = true
    for join in reorderedJoins do
        match join.On with
        | Some onExpr ->
            let referencedAliases = collectOnClauseAliases onExpr
            if not (Set.isSubset referencedAliases inScope || referencedAliases.IsEmpty) then
                let joinKey = tableSortKey join.Source
                let withSelf = Set.add joinKey inScope
                if not (Set.isSubset referencedAliases withSelf) then
                    safe <- false
        | None -> ()
        inScope <- Set.add (tableSortKey join.Source) inScope
    safe

/// Determine if a SelectCore's join chain is reorderable.
/// Returns true only if:
///   - 2+ joins exist (JOIN-2)
///   - All joins are INNER (JOIN-1)
///   - There is a FROM source to anchor the chain
let isJoinChainReorderable (core: SelectCore) : bool =
    match core.Source with
    | Some fromSource ->
        let joins = core.Joins
        joins.Length >= 2
        && isPureInnerJoinChain joins
    | None -> false

/// Compute the canonical (deterministic) ordering for a join chain.
/// Uses stable sort by table sort key (alphabetical by alias/table name).
/// Only reorders if scope safety is maintained (JOIN-3).
let canonicalJoinOrder (fromSource: TableSource) (joins: JoinShape list) : JoinShape list option =
    let sorted = joins |> List.sortBy (fun j -> tableSortKey j.Source)
    if isReorderScopeSafe fromSource sorted then
        let changed =
            (joins, sorted) ||> List.exists2 (fun orig reord ->
                tableSortKey orig.Source <> tableSortKey reord.Source)
        if changed then Some sorted
        else None
    else
        None
