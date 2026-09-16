module internal SoloDatabase.ExpressionMatcher

open SoloDatabase.SqlModel
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
        && match entry.Terms with
           | [term] when term.Direction = Asc && System.String.Equals(term.Collation, "BINARY", System.StringComparison.OrdinalIgnoreCase) ->
               expressionMatchesIndex tableName expr term.Expression
           | _ -> false)

/// Check if an expression has a matching index in the model.
let hasMatchingIndex (model: IndexModel) (tableName: string) (expr: SqlExpr) : bool =
    findMatchingIndex model tableName expr |> Option.isSome

let private binaryCollation (term: IndexTerm) =
    System.String.Equals(term.Collation, "BINARY", System.StringComparison.OrdinalIgnoreCase)

let private direct = function Parameter _ | Literal _ -> true | _ -> false

/// Disjoint excluded ranges for one directly compared key, including NULL.
/// Callers must establish that the original predicate has a match before using
/// the partition: contradictory or NULL bounds can otherwise overlap.
let tryComparisonComplement predicate =
    let binary op left right = Binary(left, op, right)
    let comparison = function
        | Binary(key, op, value) when direct value -> Some(key, op, value)
        | Binary(value, op, key) when direct value ->
            let reversed = match op with
                           | BinaryOperator.Lt -> BinaryOperator.Gt
                           | BinaryOperator.Le -> BinaryOperator.Ge
                           | BinaryOperator.Gt -> BinaryOperator.Lt
                           | BinaryOperator.Ge -> BinaryOperator.Le
                           | other -> other
            Some(key, reversed, value)
        | _ -> None
    let opposite = function
        | BinaryOperator.Lt -> Some BinaryOperator.Ge
        | BinaryOperator.Le -> Some BinaryOperator.Gt
        | BinaryOperator.Gt -> Some BinaryOperator.Le
        | BinaryOperator.Ge -> Some BinaryOperator.Lt
        | _ -> None
    let single = function
        | Some(key, BinaryOperator.Eq, value) ->
            Some(key, [binary BinaryOperator.Lt key value; binary BinaryOperator.Gt key value; Unary(UnaryOperator.IsNull, key)])
        | Some(key, op, value) -> opposite op |> Option.map (fun inverse -> key, [binary inverse key value; Unary(UnaryOperator.IsNull, key)])
        | None -> None
    match predicate with
    | Binary(left, BinaryOperator.And, right) ->
        match comparison left, comparison right with
        | Some(key, lop, lower), Some(other, rop, upper) when key = other ->
            let lowerBound = function BinaryOperator.Ge | BinaryOperator.Gt -> true | _ -> false
            let upperBound = function BinaryOperator.Le | BinaryOperator.Lt -> true | _ -> false
            if (lowerBound lop && upperBound rop) || (upperBound lop && lowerBound rop) then
                match opposite lop, opposite rop with
                | Some l, Some r -> Some(key, [binary l key lower; binary r key upper; Unary(UnaryOperator.IsNull, key)])
                | _ -> None
            else None
        | _ -> None
    | other -> single (comparison other)

/// A covered conjunction can be counted without fetching document payloads.
/// Requiring a constrained leading term avoids costing an unbounded index scan.
let findCoveringFilterIndex (model: IndexModel) tableName predicate =
    let rec keys = function
        | Binary(left, BinaryOperator.And, right) ->
            match keys left, keys right with
            | Some left, Some right -> Some(left @ right)
            | _ -> None
        | Binary(left, (BinaryOperator.Eq | BinaryOperator.Is | BinaryOperator.Lt
                      | BinaryOperator.Le | BinaryOperator.Gt | BinaryOperator.Ge), right) ->
            if direct right then Some [left]
            elif direct left then Some [right]
            else None
        | _ -> None
    match keys predicate with
    | None | Some [] -> None
    | Some keys ->
        model.Indexes |> List.tryFind (fun entry ->
            entry.TableName = tableName && not entry.Terms.IsEmpty
            && (entry.Terms |> List.forall binaryCollation)
            && (keys |> List.exists (fun key -> expressionMatchesIndex tableName key entry.Terms.Head.Expression))
            && (keys |> List.forall (fun key ->
                entry.Terms |> List.exists (fun term -> expressionMatchesIndex tableName key term.Expression))))

/// Match a complete ordering, allowing equality-constrained keys to be omitted.
/// Collection Id is the rowid trailer, ascending in a forward index scan.
let findOrderingIndex (model: IndexModel) tableName predicate (ordering: OrderBy list) =
    let rec fixedKeys = function
        | Binary(left, BinaryOperator.And, right) -> fixedKeys left @ fixedKeys right
        | Binary(left, (BinaryOperator.Eq | BinaryOperator.Is), right) when direct right -> [left]
        | Binary(left, (BinaryOperator.Eq | BinaryOperator.Is), right) when direct left -> [right]
        | _ -> []
    let fixedKeys = predicate |> Option.map fixedKeys |> Option.defaultValue []
    let isFixed expression = fixedKeys |> List.exists (fun key -> expressionMatchesIndex tableName key expression)
    let direction reverse direction = if not reverse then direction else if direction = Asc then Desc else Asc
    let rec matches reverse terms orders =
        match terms, orders with
        | _, [] -> true
        | _, order :: rest when isFixed order.Expr -> matches reverse terms rest
        | term :: rest, _ when isFixed term.Expression -> matches reverse rest orders
        | term :: rest, order :: remaining ->
            direction reverse term.Direction = order.Direction
            && expressionMatchesIndex tableName order.Expr term.Expression
            && matches reverse rest remaining
        | [], _ -> false
    model.Indexes |> List.tryFind (fun entry ->
        let terms = entry.Terms @ [{ Expression = Column(None, "Id"); Direction = Asc; Collation = "BINARY" }]
        entry.TableName = tableName && not entry.Terms.IsEmpty
        && (entry.Terms |> List.forall binaryCollation)
        && (matches false terms ordering || matches true terms ordering))

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
    | Some { Terms = [term] } ->
        // Check if the expression already matches the index form exactly (no change needed)
        if expr = term.Expression then None
        else
            // The expression matches modulo alias normalization.
            // Produce the canonical form by stripping table qualification
            // to match the unqualified index expression.
            Some (stripQualificationToMatch tableName expr term.Expression)
    | Some _ -> None
