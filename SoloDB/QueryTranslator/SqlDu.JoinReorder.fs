module SoloDatabase.JoinReorder

open SqlDu.Engine.C1.Spec
open SoloDatabase.JoinGraph

// ══════════════════════════════════════════════════════════════
// Join Reorder Transform (C7d)
//
// Applies deterministic canonical ordering to legal INNER JOIN
// chains. No cost model — pure structural canonicalization.
//
// Steps:
//   1. Check if join chain is reorderable (2+ INNER JOINs)
//   2. Compute canonical order (alphabetical by table/alias)
//   3. Verify ON clause scope safety
//   4. Apply reorder if safe; preserve if not
//   5. Recurse into nested levels
// ══════════════════════════════════════════════════════════════

/// Apply join reordering to a single SelectCore.
let private reorderInCore (core: SelectCore) : SelectCore =
    if not (isJoinChainReorderable core) then core
    else
        match core.Source with
        | Some fromSource ->
            match canonicalJoinOrder fromSource core.Joins with
            | Some reordered -> { core with Joins = reordered }
            | None -> core
        | None -> core

/// Recursively apply join reordering to a SqlSelect at every nesting level.
let rec reorderJoinSelect (sel: SqlSelect) : SqlSelect =
    let reorderedBody =
        match sel.Body with
        | SingleSelect core ->
            let coreWithRecursedSource =
                match core.Source with
                | Some(DerivedTable(innerSel, alias)) ->
                    { core with Source = Some(DerivedTable(reorderJoinSelect innerSel, alias)) }
                | _ -> core

            let coreWithRecursedJoins =
                { coreWithRecursedSource with
                    Joins = coreWithRecursedSource.Joins |> List.map (fun j ->
                        match j.Source with
                        | DerivedTable(jSel, jAlias) ->
                            { j with Source = DerivedTable(reorderJoinSelect jSel, jAlias) }
                        | _ -> j
                    )
                }

            SingleSelect(reorderInCore coreWithRecursedJoins)

        | UnionAllSelect(head, tail) ->
            let reorderHead = reorderJoinCore head
            let reorderTail = tail |> List.map reorderJoinCore
            UnionAllSelect(reorderHead, reorderTail)

    let reorderedCtes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = reorderJoinSelect cte.Query })
    { Ctes = reorderedCtes; Body = reorderedBody }

/// Reorder within a SelectCore (for UNION ALL arms).
and private reorderJoinCore (core: SelectCore) : SelectCore =
    let sel = { Ctes = []; Body = SingleSelect core }
    let reordered = reorderJoinSelect sel
    match reordered.Body with
    | SingleSelect c -> c
    | _ -> core

/// Reorder joins in a SqlStatement.
let reorderJoinStatement (stmt: SqlStatement) : SqlStatement =
    match stmt with
    | SelectStmt sel -> SelectStmt(reorderJoinSelect sel)
    | InsertStmt _ | UpdateStmt _ | DeleteStmt _ | DdlStmt _ -> stmt
