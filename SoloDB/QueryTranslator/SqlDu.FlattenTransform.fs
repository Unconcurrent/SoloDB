module SoloDatabase.FlattenTransform

open SqlDu.Engine.C1.Spec
open SoloDatabase.FlattenSafety
open SoloDatabase.AliasRewrite

// ══════════════════════════════════════════════════════════════
// Subquery Flattening Transform
//
// When a SELECT has a DerivedTable source that passes the flatten-safety
// predicate, merge the inner query into the outer:
//
//   SELECT <outer-proj> FROM (SELECT <inner-proj> FROM T WHERE <inner-where>) alias
//   →
//   SELECT <rewritten-proj> FROM T WHERE <inner-where>
//
// Projection rewriting: outer projections reference inner columns by alias.
// After flattening, those references must resolve to the inner's actual expressions.
// ══════════════════════════════════════════════════════════════

/// Build a mapping from inner projection aliases to their expressions.
let private buildAliasMap (innerCore: SelectCore) : Map<string, SqlExpr> =
    buildProjectionAliasMap innerCore

/// Rewrite an expression, substituting alias references with inner expressions.
let rec private rewriteExpr (aliasMap: Map<string, SqlExpr>) (derivedAlias: string) (expr: SqlExpr) : SqlExpr =
    rewriteDerivedAliasExpr
        { MatchEmptyDerivedAlias = false
          OnSubquerySelect = flattenSelect }
        aliasMap
        derivedAlias
        expr

/// Flatten a single SelectCore that has a DerivedTable source.
and private flattenCore (outer: SelectCore) (innerCore: SelectCore) (derivedAlias: string) : SelectCore =
    let aliasMap = buildAliasMap innerCore

    // Rewrite outer projections using inner expressions
    let rewrittenProjections =
        outer.Projections
        |> List.map (fun p ->
            { p with Expr = rewriteExpr aliasMap derivedAlias p.Expr })

    // Merge: take inner's source, joins, where, order, limit, offset
    // Outer's where (if any) gets AND'd with inner's where
    let rewrittenOuterWhere =
        outer.Where |> Option.map (rewriteExpr aliasMap derivedAlias)

    let mergedWhere =
        match rewrittenOuterWhere, innerCore.Where with
        | None, w -> w
        | w, None -> w
        | Some ow, Some iw -> Some(Binary(ow, And, iw))

    { Source = innerCore.Source
      Joins = innerCore.Joins
      Projections = rewrittenProjections
      Where = mergedWhere
      GroupBy = innerCore.GroupBy  // Should be empty per safety check
      Having = innerCore.Having    // Should be None per safety check
      OrderBy = innerCore.OrderBy
      Limit = innerCore.Limit
      Offset = innerCore.Offset
      Distinct = innerCore.Distinct || outer.Distinct }

/// Recursively flatten a SqlSelect. Applies flattening at every nesting level.
and flattenSelect (sel: SqlSelect) : SqlSelect =
    let flattenedBody =
        match sel.Body with
        | SingleSelect outer ->
            // First, recursively flatten any nested DerivedTables in the source
            let outerWithFlattenedSource =
                match outer.Source with
                | Some(DerivedTable(innerSel, alias)) ->
                    let flatInner = flattenSelect innerSel
                    { outer with Source = Some(DerivedTable(flatInner, alias)) }
                | _ -> outer

            // Also flatten any DerivedTables in JOINs
            let outerWithFlattenedJoins =
                { outerWithFlattenedSource with
                    Joins = outerWithFlattenedSource.Joins |> List.map (fun j ->
                        match j.Source with
                        | DerivedTable(jSel, jAlias) ->
                            { j with Source = DerivedTable(flattenSelect jSel, jAlias) }
                        | _ -> j
                    )
                }

            // Now try to flatten this level
            match outerWithFlattenedJoins.Source with
            | Some(DerivedTable(innerSel, alias)) ->
                match innerSel.Body with
                | SingleSelect innerCore when innerSel.Ctes.IsEmpty && isFlattenSafe outerWithFlattenedJoins innerCore ->
                    SingleSelect(flattenCore outerWithFlattenedJoins innerCore alias)
                | _ -> SingleSelect outerWithFlattenedJoins
            | _ -> SingleSelect outerWithFlattenedJoins

        | UnionAllSelect(head, tail) ->
            // Flatten inside each arm but don't merge the union
            let flatHead = flattenSelectCore head
            let flatTail = tail |> List.map flattenSelectCore
            UnionAllSelect(flatHead, flatTail)

    let flatCtes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = flattenSelect cte.Query })
    { Ctes = flatCtes; Body = flattenedBody }

/// Flatten DerivedTables within a SelectCore (for UNION ALL arms).
and private flattenSelectCore (core: SelectCore) : SelectCore =
    let sel = { Ctes = []; Body = SingleSelect core }
    let flattened = flattenSelect sel
    match flattened.Body with
    | SingleSelect c -> c
    | _ -> core

/// Flatten a SqlStatement.
let flattenStatement (stmt: SqlStatement) : SqlStatement =
    match stmt with
    | SelectStmt sel -> SelectStmt(flattenSelect sel)
    | InsertStmt ins ->
        InsertStmt { ins with Returning = ins.Returning }
    | UpdateStmt upd ->
        let flatWhere =
            match upd.Where with
            | Some(InSubquery(e, sel)) -> Some(InSubquery(e, flattenSelect sel))
            | w -> w
        UpdateStmt { upd with Where = flatWhere }
    | DeleteStmt del ->
        let flatWhere =
            match del.Where with
            | Some(InSubquery(e, sel)) -> Some(InSubquery(e, flattenSelect sel))
            | w -> w
        DeleteStmt { del with Where = flatWhere }
    | DdlStmt _ -> stmt
