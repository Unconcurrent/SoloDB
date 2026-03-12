module SoloDatabase.IndexPlanShaping

open SqlDu.Engine.C1.Spec
open SoloDatabase.IndexModel
open SoloDatabase.ExpressionMatcher
open SoloDatabase.ExpressionPredicates

// ══════════════════════════════════════════════════════════════
// Index plan shaping transform.
//
// Makes rewrites explicitly index-informed. Chooses between
// equivalent forms based on index availability.
//
// Legal adjustments in this transform:
//   Predicate canonicalization — normalize WHERE/ON
//          expression form to match known index
//   ORDER BY alignment — normalize sort expression
//          form to match index key order
//   Join probe selection — choose among already-legal
//          INNER JOIN orderings using index knowledge
//
// Must-not-shape boundaries:
//   - GROUP BY / HAVING / AGGREGATE scopes
//   - WINDOW scopes
//   - UNION ALL / set-op compositions
//   - Correlated EXISTS / NOT EXISTS
//   - LEFT JOIN patterns
//   - json_each boundaries
//   - All DML statements
// ══════════════════════════════════════════════════════════════

/// Resolve the table name from a TableSource.
let private resolveTableName (source: TableSource) : string option =
    match source with
    | BaseTable(name, _) -> Some name
    | DerivedTable _ -> None
    | FromJsonEach _ -> None

/// Check if a SelectCore has must-not-shape boundaries.
/// When these are present, shaping refuses all adjustments.
let private hasMustNotShapeBoundary (core: SelectCore) : bool =
    // GROUP BY
    not core.GroupBy.IsEmpty
    // HAVING
    || core.Having.IsSome
    // DISTINCT
    || core.Distinct
    // json_each source
    || (match core.Source with Some(FromJsonEach _) -> true | _ -> false)

/// Check if any projection contains an aggregate or window call.
let private hasAggregateOrWindowInExpr (expr: SqlExpr) : bool =
    hasAggregateCall expr || hasWindowFunction expr

/// Check if a core has aggregate or window functions in projections.
let private hasAggregateOrWindowProjections (core: SelectCore) : bool =
    core.Projections |> List.exists (fun p -> hasAggregateOrWindowInExpr p.Expr)

/// Canonicalize a single expression for index matching.
/// Returns the expression unchanged if no canonicalization is needed.
let private canonicalizeExpr (model: IndexModel) (tableName: string) (expr: SqlExpr) : SqlExpr =
    match canonicalizeForIndex model tableName expr with
    | Some canonicalized -> canonicalized
    | None -> expr

/// Canonicalize WHERE predicate expressions.
/// Walks the binary tree of AND/OR and canonicalizes leaf comparison operands.
let rec private canonicalizePredicate (model: IndexModel) (tableName: string) (expr: SqlExpr) : SqlExpr =
    match expr with
    | Binary(l, (Eq | Ne | Lt | Le | Gt | Ge | Like | Glob | Regexp as op), r) ->
        let cl = canonicalizeExpr model tableName l
        let cr = canonicalizeExpr model tableName r
        Binary(cl, op, cr)
    | Binary(l, And, r) ->
        Binary(canonicalizePredicate model tableName l, And, canonicalizePredicate model tableName r)
    | Binary(l, Or, r) ->
        Binary(canonicalizePredicate model tableName l, Or, canonicalizePredicate model tableName r)
    | Unary(Not, e) ->
        Unary(Not, canonicalizePredicate model tableName e)
    | Between(e, lo, hi) ->
        Between(canonicalizeExpr model tableName e, lo, hi)
    | InList(e, list) ->
        InList(canonicalizeExpr model tableName e, list)
    | _ -> expr

/// Canonicalize ORDER BY expressions for index alignment.
let private canonicalizeOrderBy (model: IndexModel) (tableName: string) (orderBy: OrderBy list) : OrderBy list =
    orderBy |> List.map (fun ob ->
        { ob with Expr = canonicalizeExpr model tableName ob.Expr })

/// Rewrite alias references in an expression to use the table name instead.
let rec private rewriteAliasToTable (alias: string) (tableName: string) (expr: SqlExpr) : SqlExpr =
    match expr with
    | JsonExtractExpr(Some src, col, path) when src = alias ->
        JsonExtractExpr(Some (sprintf "\"%s\"" tableName), col, path)
    | Cast(inner, sqlType) ->
        Cast(rewriteAliasToTable alias tableName inner, sqlType)
    | Binary(l, op, r) ->
        Binary(rewriteAliasToTable alias tableName l, op, rewriteAliasToTable alias tableName r)
    | Column(Some src, col) when src = alias ->
        Column(Some (sprintf "\"%s\"" tableName), col)
    | FunctionCall(name, args) ->
        FunctionCall(name, args |> List.map (rewriteAliasToTable alias tableName))
    | _ -> expr

/// Check if an expression (or any sub-expression) matches an index,
/// trying both the table name and an optional alias as the qualifier.
let rec private expressionMatchesIndexWithAlias (model: IndexModel) (tableName: string) (alias: string option) (expr: SqlExpr) : bool =
    // Try matching the expression directly
    if hasMatchingIndex model tableName expr then true
    else
        // Try alias rewrite at this level
        let aliasMatch =
            match alias with
            | Some a when a <> tableName ->
                let rewritten = rewriteAliasToTable a tableName expr
                if rewritten <> expr then hasMatchingIndex model tableName rewritten
                else false
            | _ -> false
        if aliasMatch then true
        else
            // Recurse into sub-expressions
            match expr with
            | Binary(l, _, r) ->
                expressionMatchesIndexWithAlias model tableName alias l
                || expressionMatchesIndexWithAlias model tableName alias r
            | Unary(_, e) -> expressionMatchesIndexWithAlias model tableName alias e
            | Cast(_, _) ->
                // The Cast wrapper itself was already checked above via hasMatchingIndex/aliasMatch.
                // Do not recurse into Cast inner to avoid false-positive cast matching (matching bare inner
                // against a CAST index).
                false
            | FunctionCall(_, args) -> args |> List.exists (expressionMatchesIndexWithAlias model tableName alias)
            | _ -> false

/// Reorder INNER JOIN chain based on index knowledge.
/// Among C7-legal orderings, prefer probing the side with a matching
/// index on its join key. Only operates on pure INNER JOIN chains.
let private reshapeJoinsForIndex (model: IndexModel) (core: SelectCore) : SelectCore =
    // Only consider pure INNER JOIN chains with 2+ joins
    if core.Joins.Length < 2 then core
    elif core.Joins |> List.exists (fun j -> j.Kind <> Inner) then core
    else
        // Check if any join has ON clause referencing an indexed expression
        // and reorder to put indexed-probe joins earlier.
        // Use a simple stable sort: joins whose ON clause references
        // an indexed expression on the join source table sort before others.
        let scoreJoin (j: JoinShape) : int =
            match j.Source, j.On with
            | BaseTable(tName, tAlias), Some onExpr ->
                // Check if any sub-expression in ON matches an index on the join's table,
                // accounting for alias-to-table name resolution.
                if expressionMatchesIndexWithAlias model tName tAlias onExpr then 0 else 1
            | _ -> 1
        let sorted = core.Joins |> List.sortBy scoreJoin
        // Only change if order actually differs
        if sorted = core.Joins then core
        else { core with Joins = sorted }

/// Apply index plan shaping to a single SelectCore.
let private shapeInCore (model: IndexModel) (core: SelectCore) : SelectCore =
    // Refuse at must-not-shape boundaries
    if hasMustNotShapeBoundary core then core
    elif hasAggregateOrWindowProjections core then core
    else
        // Resolve the base table name for index lookup
        let tableName =
            match core.Source with
            | Some source -> resolveTableName source
            | None -> None
        match tableName with
        | None -> core // No base table — nothing to shape (DerivedTable, json_each, etc.)
        | Some tName ->
            let mutable shaped = core

            // Canonicalize WHERE predicate
            shaped <-
                match shaped.Where with
                | Some where ->
                    let canonical = canonicalizePredicate model tName where
                    if canonical = where then shaped
                    else { shaped with Where = Some canonical }
                | None -> shaped

            // Canonicalize JOIN ON clauses
            shaped <-
                let newJoins =
                    shaped.Joins |> List.map (fun j ->
                        match j.On with
                        | Some onExpr ->
                            // For JOIN ON, we canonicalize against the join source's table
                            let joinTableName =
                                match resolveTableName j.Source with
                                | Some n -> n
                                | None -> tName // Fallback to FROM table
                            let canonical = canonicalizePredicate model joinTableName onExpr
                            // Also canonicalize against the FROM table
                            let canonical2 = canonicalizePredicate model tName canonical
                            if canonical2 = onExpr then j
                            else { j with On = Some canonical2 }
                        | None -> j)
                if newJoins = shaped.Joins then shaped
                else { shaped with Joins = newJoins }

            // Canonicalize ORDER BY
            shaped <-
                if shaped.OrderBy.IsEmpty then shaped
                else
                    let canonical = canonicalizeOrderBy model tName shaped.OrderBy
                    if canonical = shaped.OrderBy then shaped
                    else { shaped with OrderBy = canonical }

            // Reshape joins for index
            shaped <- reshapeJoinsForIndex model shaped

            shaped

/// Recursively apply index plan shaping to a SqlSelect.
let rec shapeIndexSelect (model: IndexModel) (sel: SqlSelect) : SqlSelect =
    let shapedBody =
        match sel.Body with
        | SingleSelect core ->
            // First recurse into DerivedTable source
            let coreWithRecursedSource =
                match core.Source with
                | Some(DerivedTable(innerSel, alias)) ->
                    { core with Source = Some(DerivedTable(shapeIndexSelect model innerSel, alias)) }
                | _ -> core

            // Recurse into JOINs
            let coreWithRecursedJoins =
                { coreWithRecursedSource with
                    Joins = coreWithRecursedSource.Joins |> List.map (fun j ->
                        match j.Source with
                        | DerivedTable(jSel, jAlias) ->
                            { j with Source = DerivedTable(shapeIndexSelect model jSel, jAlias) }
                        | _ -> j)
                }

            // Apply shaping at this level
            SingleSelect(shapeInCore model coreWithRecursedJoins)

        | UnionAllSelect(head, tail) ->
            // UNION ALL is a must-not-shape boundary — return entirely unchanged.
            // No arm-level shaping. Hard fence.
            UnionAllSelect(head, tail)

    let shapedCtes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = shapeIndexSelect model cte.Query })
    { Ctes = shapedCtes; Body = shapedBody }

/// Apply index plan shaping to a SqlStatement.
let shapeIndexStatement (model: IndexModel) (stmt: SqlStatement) : SqlStatement =
    match stmt with
    | SelectStmt sel -> SelectStmt(shapeIndexSelect model sel)
    // DML passthrough — out of scope for index shaping.
    | InsertStmt _ | UpdateStmt _ | DeleteStmt _ | DdlStmt _ -> stmt
