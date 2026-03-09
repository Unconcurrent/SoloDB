module SoloDatabase.FlattenTransform

open SqlDu.Engine.C1.Spec
open SoloDatabase.FlattenSafety

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
/// For unaliased projections, use the column name if the expr is a Column.
let private buildAliasMap (innerCore: SelectCore) (derivedAlias: string) : Map<string, SqlExpr> =
    innerCore.Projections
    |> List.choose (fun p ->
        match p.Alias with
        | Some alias -> Some(alias, p.Expr)
        | None ->
            // Unaliased projection: if it's a Column, use the column name
            match p.Expr with
            | Column(_, colName) -> Some(colName, p.Expr)
            | _ -> None
    )
    |> Map.ofList

/// Rewrite an expression, substituting alias references with inner expressions.
let rec private rewriteExpr (aliasMap: Map<string, SqlExpr>) (derivedAlias: string) (expr: SqlExpr) : SqlExpr =
    match expr with
    | Column(Some src, col) when src = derivedAlias ->
        // Reference to the derived table alias — look up in alias map
        match Map.tryFind col aliasMap with
        | Some innerExpr -> innerExpr
        | None -> expr // Keep as-is if not found (shouldn't happen in well-formed trees)
    | Column(None, col) ->
        // Unqualified column — try alias map
        match Map.tryFind col aliasMap with
        | Some innerExpr -> innerExpr
        | None -> expr
    | Binary(l, op, r) ->
        Binary(rewriteExpr aliasMap derivedAlias l, op, rewriteExpr aliasMap derivedAlias r)
    | Unary(op, e) ->
        Unary(op, rewriteExpr aliasMap derivedAlias e)
    | FunctionCall(name, args) ->
        FunctionCall(name, args |> List.map (rewriteExpr aliasMap derivedAlias))
    | AggregateCall(kind, arg, distinct, sep) ->
        AggregateCall(kind,
            arg |> Option.map (rewriteExpr aliasMap derivedAlias),
            distinct,
            sep |> Option.map (rewriteExpr aliasMap derivedAlias))
    | Coalesce(exprs) ->
        Coalesce(exprs |> List.map (rewriteExpr aliasMap derivedAlias))
    | Cast(e, t) ->
        Cast(rewriteExpr aliasMap derivedAlias e, t)
    | CaseExpr(branches, elseE) ->
        CaseExpr(
            branches |> List.map (fun (c, r) ->
                (rewriteExpr aliasMap derivedAlias c, rewriteExpr aliasMap derivedAlias r)),
            elseE |> Option.map (rewriteExpr aliasMap derivedAlias))
    | JsonExtractExpr(Some src, col, path) when src = derivedAlias ->
        // Rewrite source alias for json_extract on derived table
        match Map.tryFind col aliasMap with
        | Some(Column(innerSrc, innerCol)) -> JsonExtractExpr(innerSrc, innerCol, path)
        | _ -> expr
    | JsonSetExpr(target, assignments) ->
        JsonSetExpr(
            rewriteExpr aliasMap derivedAlias target,
            assignments |> List.map (fun (p, e) -> (p, rewriteExpr aliasMap derivedAlias e)))
    | JsonArrayExpr(elems) ->
        JsonArrayExpr(elems |> List.map (rewriteExpr aliasMap derivedAlias))
    | JsonObjectExpr(props) ->
        JsonObjectExpr(props |> List.map (fun (k, v) -> (k, rewriteExpr aliasMap derivedAlias v)))
    | InList(e, list) ->
        InList(rewriteExpr aliasMap derivedAlias e, list |> List.map (rewriteExpr aliasMap derivedAlias))
    | InSubquery(e, sel) ->
        InSubquery(rewriteExpr aliasMap derivedAlias e, flattenSelect sel)
    | Exists(sel) ->
        Exists(flattenSelect sel)
    | ScalarSubquery(sel) ->
        ScalarSubquery(flattenSelect sel)
    | Between(e, lo, hi) ->
        Between(rewriteExpr aliasMap derivedAlias e,
            rewriteExpr aliasMap derivedAlias lo,
            rewriteExpr aliasMap derivedAlias hi)
    | WindowCall(spec) ->
        WindowCall({
            spec with
                Arguments = spec.Arguments |> List.map (rewriteExpr aliasMap derivedAlias)
                PartitionBy = spec.PartitionBy |> List.map (rewriteExpr aliasMap derivedAlias)
                OrderBy = spec.OrderBy |> List.map (fun (e, d) -> (rewriteExpr aliasMap derivedAlias e, d))
        })
    | _ -> expr // Literal, Parameter, Column with other source, JsonExtractExpr with other source

/// Flatten a single SelectCore that has a DerivedTable source.
and private flattenCore (outer: SelectCore) (innerCore: SelectCore) (derivedAlias: string) : SelectCore =
    let aliasMap = buildAliasMap innerCore derivedAlias

    // Rewrite outer projections using inner expressions
    let rewrittenProjections =
        outer.Projections
        |> List.map (fun p ->
            { p with Expr = rewriteExpr aliasMap derivedAlias p.Expr })

    // Merge: take inner's source, joins, where, order, limit, offset
    // Outer's where (if any) gets AND'd with inner's where
    let mergedWhere =
        match outer.Where, innerCore.Where with
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
