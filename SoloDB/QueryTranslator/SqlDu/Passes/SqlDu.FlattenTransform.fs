module internal SoloDatabase.FlattenTransform

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

/// Strip embedded quotes from a source alias to normalize quoting.
/// The LINQ translator generates Column(Some "\"Order\"", ...) with embedded quotes
/// but BaseTable("Order", None) without. After flatten merge, expressions must use
/// the same naming as the merged source.
let private stripQuotes (s: string) : string =
    if s.Length >= 2 && s.[0] = '"' && s.[s.Length - 1] = '"' then s.[1..s.Length - 2]
    else s

/// Normalize source alias quoting in an expression to match BaseTable naming.
let internal normalizeExprQuoting (expr: SqlExpr) : SqlExpr =
    SqlExpr.map (fun node ->
        match node with
        | Column(Some src, col) ->
            let norm = stripQuotes src
            if norm <> src then Column(Some norm, col) else node
        | JsonExtractExpr(Some src, col, path) ->
            let norm = stripQuotes src
            if norm <> src then JsonExtractExpr(Some norm, col, path) else node
        | JsonRootExtract(Some src, col) ->
            let norm = stripQuotes src
            if norm <> src then JsonRootExtract(Some norm, col) else node
        | _ -> node) expr

/// Build a mapping from inner projection aliases to their expressions.
/// Uses raw inner projection expressions with quote normalization only.
/// Provenance resolution is NOT applied here — it prematurely binds to base-table
/// qualifiers which causes drift when the DerivedTable boundary survives this iteration.
/// The rewriteExpr substitution handles composition correctly using raw inner expressions.
let private buildAliasMap (innerCore: SelectCore) : Map<string, SqlExpr> =
    buildProjectionAliasMap innerCore
    |> Map.map (fun _ expr -> normalizeExprQuoting expr)

/// Rewrite an expression, substituting alias references with inner expressions.
let rec private rewriteExpr (changed: bool ref) (aliasMap: Map<string, SqlExpr>) (derivedAlias: string) (expr: SqlExpr) : SqlExpr =
    rewriteDerivedAliasExpr
        { MatchEmptyDerivedAlias = false
          OnSubquerySelect = flattenSelect changed }
        aliasMap
        derivedAlias
        expr

/// Flatten a single SelectCore that has a DerivedTable source.
and private flattenCore (changed: bool ref) (outer: SelectCore) (innerCore: SelectCore) (derivedAlias: string) : SelectCore =
    let aliasMap = buildAliasMap innerCore

    // Rewrite outer projections using inner expressions
    let rewrittenProjections =
        outer.Projections
        |> ProjectionSetOps.map (fun p ->
            { p with Expr = rewriteExpr changed aliasMap derivedAlias p.Expr })

    // Merge: take inner's source, joins, where, order, limit, offset
    // Outer's where (if any) gets AND'd with inner's where
    let rewrittenOuterWhere =
        outer.Where |> Option.map (rewriteExpr changed aliasMap derivedAlias)

    let mergedWhere =
        match rewrittenOuterWhere, innerCore.Where with
        | None, w -> w
        | w, None -> w
        | Some ow, Some iw -> Some(Binary(ow, And, iw))

    let rewrittenOuterOrderBy =
        outer.OrderBy
        |> List.map (fun ob -> { ob with Expr = rewriteExpr changed aliasMap derivedAlias ob.Expr })

    let mergedOrderBy =
        if rewrittenOuterOrderBy.IsEmpty then innerCore.OrderBy else rewrittenOuterOrderBy

    let rewrittenOuterLimit =
        outer.Limit |> Option.map (rewriteExpr changed aliasMap derivedAlias)

    let mergedLimit =
        match rewrittenOuterLimit with
        | Some lim -> Some lim
        | None -> innerCore.Limit

    let rewrittenOuterOffset =
        outer.Offset |> Option.map (rewriteExpr changed aliasMap derivedAlias)

    let mergedOffset =
        match rewrittenOuterOffset with
        | Some off -> Some off
        | None -> innerCore.Offset

    { Source = innerCore.Source
      Joins = innerCore.Joins
      Projections = rewrittenProjections
      Where = mergedWhere
      GroupBy = innerCore.GroupBy  // Should be empty per safety check
      Having = innerCore.Having    // Should be None per safety check
      OrderBy = mergedOrderBy
      Limit = mergedLimit
      Offset = mergedOffset
      Distinct = innerCore.Distinct || outer.Distinct }

/// Recursively flatten a SqlSelect. Applies flattening at every nesting level.
and flattenSelect (changed: bool ref) (sel: SqlSelect) : SqlSelect =
    let flattenedBody =
        match sel.Body with
        | SingleSelect outer ->
            // First, recursively flatten any nested DerivedTables in the source
            let outerWithFlattenedSource =
                match outer.Source with
                | Some(DerivedTable(innerSel, alias)) ->
                    let flatInner = flattenSelect changed innerSel
                    { outer with Source = Some(DerivedTable(flatInner, alias)) }
                | _ -> outer

            // Also flatten any DerivedTables in JOINs
            let outerWithFlattenedJoins =
                { outerWithFlattenedSource with
                    Joins = outerWithFlattenedSource.Joins |> List.map (fun j ->
                        match j with
                        | CrossJoin(DerivedTable(jSel, jAlias)) ->
                            CrossJoin(DerivedTable(flattenSelect changed jSel, jAlias))
                        | ConditionedJoin(kind, DerivedTable(jSel, jAlias), onExpr) ->
                            ConditionedJoin(kind, DerivedTable(flattenSelect changed jSel, jAlias), onExpr)
                        | CrossJoin _ ->
                            j
                        | ConditionedJoin _ ->
                            j
                    )
                }

            // Now try to flatten this level
            match outerWithFlattenedJoins.Source with
            | Some(DerivedTable(innerSel, alias)) ->
                match innerSel.Body with
                | SingleSelect innerCore when innerSel.Ctes.IsEmpty && isFlattenSafe outerWithFlattenedJoins innerCore ->
                    changed.Value <- true
                    SingleSelect(flattenCore changed outerWithFlattenedJoins innerCore alias)
                | _ -> SingleSelect outerWithFlattenedJoins
            | _ -> SingleSelect outerWithFlattenedJoins

        | UnionAllSelect(head, tail) ->
            // Flatten inside each arm but don't merge the union
            let flatHead = flattenSelectCore changed head
            let flatTail = tail |> List.map (flattenSelectCore changed)
            UnionAllSelect(flatHead, flatTail)

    let flatCtes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = flattenSelect changed cte.Query })
    { Ctes = flatCtes; Body = flattenedBody }

/// Flatten DerivedTables within a SelectCore (for UNION ALL arms).
and private flattenSelectCore (changed: bool ref) (core: SelectCore) : SelectCore =
    let sel = { Ctes = []; Body = SingleSelect core }
    let flattened = flattenSelect changed sel
    match flattened.Body with
    | SingleSelect c -> c
    | _ -> core

/// Flatten a SqlStatement.
let flattenStatement (stmt: SqlStatement) : struct(SqlStatement * bool) =
    let changed = ref false
    match stmt with
    | SelectStmt sel ->
        let result = flattenSelect changed sel
        struct(SelectStmt result, changed.Value)
    | InsertStmt ins ->
        let result = InsertStmt { ins with Returning = ins.Returning }
        struct(result, false)
    | UpdateStmt upd ->
        let flatWhere =
            match upd.Where with
            | Some(InSubquery(e, sel)) ->
                let flatSel = flattenSelect changed sel
                Some(InSubquery(e, flatSel))
            | w -> w
        struct(UpdateStmt { upd with Where = flatWhere }, changed.Value)
    | DeleteStmt del ->
        let flatWhere =
            match del.Where with
            | Some(InSubquery(e, sel)) ->
                let flatSel = flattenSelect changed sel
                Some(InSubquery(e, flatSel))
            | w -> w
        struct(DeleteStmt { del with Where = flatWhere }, changed.Value)
    | DdlStmt _ -> struct(stmt, false)
