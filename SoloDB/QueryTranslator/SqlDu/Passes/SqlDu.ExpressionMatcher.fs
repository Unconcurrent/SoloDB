module SoloDatabase.ExpressionMatcher

open SqlDu.Engine.C1.Spec
open SoloDatabase.IndexModel

// ══════════════════════════════════════════════════════════════
// Expression-Form Matcher
//
// Checks if a DU expression structurally matches an index entry,
// accounting for:
//   1. Alias normalization: qualified "T".col matches unqualified col
//      when the source table is "T"
//   2. CAST preservation: Cast(expr, type) matches only if index also
//      has Cast. The matcher must not introduce CAST during matching.
//   3. JsonPath exact match: path segments must be identical.
//
// The matcher is form-exact: SQLite expression indexes are
// form-sensitive.
// ══════════════════════════════════════════════════════════════

/// Resolve the table name from a source alias.
/// In the DU, table-qualified expressions use the quoted form: "\"Users\""
/// or the unquoted table name directly.
let private unquoteAlias (alias: string) : string =
    if alias.StartsWith("\"") && alias.EndsWith("\"") && alias.Length > 2 then
        alias.Substring(1, alias.Length - 2)
    else
        alias

/// Check if two SqlExpr nodes are structurally equivalent for index matching,
/// with alias normalization: a qualified reference to tableName matches
/// the unqualified form in the index expression.
let rec expressionMatchesIndex (tableName: string) (expr: SqlExpr) (indexExpr: SqlExpr) : bool =
    match expr, indexExpr with
    // JsonExtractExpr: qualified expr matches unqualified index when alias resolves to tableName
    | JsonExtractExpr(Some alias, col, path), JsonExtractExpr(None, iCol, iPath) ->
        unquoteAlias alias = tableName && col = iCol && path = iPath
    // JsonExtractExpr: both unqualified — exact match
    | JsonExtractExpr(None, col, path), JsonExtractExpr(None, iCol, iPath) ->
        col = iCol && path = iPath
    // JsonExtractExpr: both qualified — exact alias match
    | JsonExtractExpr(Some a1, col, path), JsonExtractExpr(Some a2, iCol, iPath) ->
        unquoteAlias a1 = unquoteAlias a2 && col = iCol && path = iPath
    // CAST: both must have CAST with matching type, and inner expressions must match
    | Cast(inner, sqlType), Cast(iInner, iSqlType) ->
        sqlType.ToUpperInvariant() = iSqlType.ToUpperInvariant()
        && expressionMatchesIndex tableName inner iInner
    // Column: qualified expr matches unqualified index when alias resolves to tableName
    | Column(Some alias, col), Column(None, iCol) ->
        unquoteAlias alias = tableName && col = iCol
    // Column: both unqualified
    | Column(None, col), Column(None, iCol) ->
        col = iCol
    // Column: both qualified
    | Column(Some a1, col), Column(Some a2, iCol) ->
        unquoteAlias a1 = unquoteAlias a2 && col = iCol
    // FunctionCall: name + arg count + recursive match
    | FunctionCall(name, args), FunctionCall(iName, iArgs) ->
        name = iName
        && args.Length = iArgs.Length
        && List.forall2 (expressionMatchesIndex tableName) args iArgs
    // Everything else: no match
    | _ -> false

/// Find the first index entry that matches a given expression for a table.
let findMatchingIndex (model: IndexModel) (tableName: string) (expr: SqlExpr) : IndexEntry option =
    model.Indexes
    |> List.tryFind (fun entry ->
        entry.TableName = tableName
        && expressionMatchesIndex tableName expr entry.Expression)

/// Check if an expression has a matching index in the model.
let hasMatchingIndex (model: IndexModel) (tableName: string) (expr: SqlExpr) : bool =
    findMatchingIndex model tableName expr |> Option.isSome

/// Strip table qualification from an expression to match the index form.
/// Only normalizes alias references; never adds or removes CAST or other operations.
let rec private stripQualificationToMatch (tableName: string) (expr: SqlExpr) (indexExpr: SqlExpr) : SqlExpr =
    match expr, indexExpr with
    | JsonExtractExpr(Some alias, col, path), JsonExtractExpr(None, _, _)
        when unquoteAlias alias = tableName ->
        JsonExtractExpr(None, col, path)
    | Cast(inner, sqlType), Cast(iInner, _) ->
        Cast(stripQualificationToMatch tableName inner iInner, sqlType)
    | Column(Some alias, col), Column(None, _)
        when unquoteAlias alias = tableName ->
        Column(None, col)
    | FunctionCall(name, args), FunctionCall(_, iArgs) when args.Length = iArgs.Length ->
        FunctionCall(name, List.map2 (stripQualificationToMatch tableName) args iArgs)
    | _ -> expr

/// Canonicalize an expression to match an index form.
/// If the expression matches an index (modulo alias qualification),
/// return the canonicalized form that matches the index expression
/// exactly. Returns None if no canonicalization is needed or possible.
///
/// Canonicalization only strips/normalizes alias qualification.
/// It never adds CAST, changes paths, or invents operations.
let canonicalizeForIndex (model: IndexModel) (tableName: string) (expr: SqlExpr) : SqlExpr option =
    match findMatchingIndex model tableName expr with
    | None -> None
    | Some entry ->
        // Check if the expression already matches the index form exactly (no change needed)
        if expr = entry.Expression then None
        else
            // The expression matches modulo alias normalization.
            // Produce the canonical form by stripping table qualification
            // to match the unqualified index expression.
            Some (stripQualificationToMatch tableName expr entry.Expression)
