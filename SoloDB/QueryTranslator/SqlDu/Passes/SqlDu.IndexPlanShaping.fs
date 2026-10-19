module internal SoloDatabase.IndexPlanShaping

open SoloDatabase.SqlModel
open SoloDatabase.IndexModel
open SoloDatabase.ExpressionMatcher
open SoloDatabase.SelectCoreBoundary
open SoloDatabase.Numerics

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

/// Canonicalize a single expression for index matching.
/// Returns the expression unchanged if no canonicalization is needed.
let private canonicalizeExpr (model: IndexModel) (tableName: string) (expr: SqlExpr) : SqlExpr =
    match canonicalizeForIndex model tableName expr with
    | Some canonicalized -> canonicalized
    | None -> expr

let private characterComparison = function
    | Binary((FunctionCall("CHARACTER_VALUE",[Literal(Integer 7L);raw;Literal Null;Literal Null]) as operand),(Eq | Is),value)
    | Binary(value,(Eq | Is),(FunctionCall("CHARACTER_VALUE",[Literal(Integer 7L);raw;Literal Null;Literal Null]) as operand)) ->
        let extraction = match raw with JsonExtractExpr(_,"Value",_) | JsonRootExtract(_,"Value") | FunctionCall("jsonb_extract",[Column(_,"Value");Literal(String _)]) -> true | _ -> false
        let acquired = match value with Literal(Integer _) | Literal Null | Parameter _ -> true | _ -> false
        if extraction && acquired then Some(operand,raw,value) else None
    | _ -> None

/// The old decoder index admits only the canonical zero key as one-unit text.
/// Keep the real Char residual: longer fractional keys can share the zero bucket.
let private characterCandidates model tableName sourceAlias surrounding predicate =
    match characterComparison predicate with
    | None -> predicate
    | Some(operand,raw,value) ->
        let indexed expression =
            let expression = forSource tableName sourceAlias expression
            indexesForTable model tableName |> List.tryPick (fun entry ->
                match entry.Terms with
                | term :: _ when System.String.Equals(term.Collation,"BINARY",System.StringComparison.OrdinalIgnoreCase)
                                 && expressionMatchesIndex tableName expression term.Expression -> Some term.Expression
                | _ -> None)
        let old = FunctionCall("DECIMAL",[Literal(Integer(int64(int ExactNumericOperation.NativeValue)));Literal(Integer(int64(int ExactNumericOperation.ToUInt16)));raw])
        match indexed operand, indexed old with
        | None, Some definition ->
            let zeroUnit = int64 ((NumericKey.encode ExactNumber.zero).[0])
            let alternate = CaseExpr((Binary(value,Eq,Literal(Integer zeroUnit)),Literal(Integer 0L)),[],Some value)
            let already = surrounding |> SqlExpr.exists (function
                | InList(candidate,head,[tail]) when head = value && tail = alternate ->
                    expressionMatchesIndex tableName (forSource tableName sourceAlias candidate) definition
                | Binary(Binary(first,Is,head),Or,Binary(Binary(tested,Eq,Literal(Integer unit)),And,Binary(second,Eq,Literal(Integer 0L))))
                    when head = value && tested = value && unit = zeroUnit ->
                    expressionMatchesIndex tableName (forSource tableName sourceAlias first) definition &&
                    expressionMatchesIndex tableName (forSource tableName sourceAlias second) definition
                | _ -> false)
            if already then predicate else
            let candidates =
                match predicate with
                | Binary(_, Is, _) ->
                    Binary(Binary(old,Is,value),Or,Binary(Binary(value,Eq,Literal(Integer zeroUnit)),And,Binary(old,Eq,Literal(Integer 0L))))
                | _ -> InList(old,value,[alternate])
            Binary(candidates,And,predicate)
        | _ -> predicate

/// One predicate traversal serves canonical terms and the Char-only retained-index alignment.
let private canonicalizePredicate model tableName sourceAlias allowCharacter characterOnly expr =
    let rec visit expression =
        match expression with
        | Binary(l, (Eq | Ne | Is | IsNot | Lt | Le | Gt | Ge | Like | Glob | Regexp as op), r) ->
            let cl = if characterOnly then l else canonicalizeExpr model tableName l
            let cr = if characterOnly then r else canonicalizeExpr model tableName r
            let result = Binary(cl,op,cr)
            if allowCharacter then characterCandidates model tableName sourceAlias expr result else result
        | Binary(l,And,r) -> Binary(visit l,And,visit r)
        | Binary(l,Or,r) -> Binary(visit l,Or,visit r)
        | Unary(Not,e) -> Unary(Not,visit e)
        | Between(e,lo,hi) when not characterOnly -> Between(canonicalizeExpr model tableName e,lo,hi)
        | InList(e,head,tail) when not characterOnly -> InList(canonicalizeExpr model tableName e,head,tail)
        | _ -> expression
    visit expr

/// Mutation emission uses only this exact, effect-free retained-index alignment.
let alignCharacterPredicate loadModel tableName expression =
    if expression |> SqlExpr.exists (characterComparison >> Option.isSome) then
        let model = loadModel()
        canonicalizePredicate model tableName tableName true true expression
    else expression

/// Canonicalize ORDER BY expressions for index alignment.
let private canonicalizeOrderBy (model: IndexModel) (tableName: string) (orderBy: OrderBy list) : OrderBy list =
    orderBy |> List.map (fun ob ->
        { ob with Expr = canonicalizeExpr model tableName ob.Expr })

/// Preserve decoding while exposing the native storage range to an existing index.
let private partitionNativePredicate model tableName sourceAlias predicate =
    let indexed expression =
        let expression = forSource tableName sourceAlias expression
        indexesForTable model tableName |> List.exists (fun entry ->
            match entry.Terms with
            | term :: _ ->
                System.String.Equals(term.Collation, "BINARY", System.StringComparison.OrdinalIgnoreCase)
                && expressionMatchesIndex tableName expression term.Expression
            | [] -> false)
    let nativeInput = function
        | FunctionCall("DECIMAL", [Literal(Integer operation); Literal(Integer target); raw])
            when operation = int64(int ExactNumericOperation.NativeValue)
                 && target >= 0L && target <= int64 System.Int32.MaxValue ->
            let target = enum<ExactNumericOperation>(int target)
            let native =
                match target with
                | ExactNumericOperation.ToDouble | ExactNumericOperation.ToSingle
                | ExactNumericOperation.ToInt64 | ExactNumericOperation.ToInt32
                | ExactNumericOperation.ToInt16 | ExactNumericOperation.ToSByte
                | ExactNumericOperation.ToUInt32 | ExactNumericOperation.ToUInt16
                | ExactNumericOperation.ToByte -> true
                | _ -> false
            let extraction =
                match raw with
                | JsonExtractExpr(_, "Value", _) | JsonRootExtract(_, "Value")
                | FunctionCall("jsonb_extract", [Column(_, "Value"); Literal(String _)]) -> true
                | _ -> false
            if native && extraction then Some raw else None
        | _ -> None
    let nativeOperand operand =
        match nativeInput operand with
        | Some raw when not(indexed operand) && indexed raw -> Some raw
        | _ -> None
    let nullableOperand operand =
        match nativeInput operand with
        | Some raw when indexed operand || indexed raw -> Some raw
        | _ -> None
    let direct = function Literal _ | Parameter _ -> true | _ -> false
    let withCandidates raw native original =
        let boundary = Literal(String "")
        Binary(Binary(Binary(raw, Lt, boundary), And, native), Or,
               Binary(Binary(raw, Ge, boundary), And, original))
    let nullableEquality = function
        | CaseExpr((Unary(IsNull, rightRaw), Binary(leftRaw, Is, Literal Null)),
                   [(Unary(IsNull, leftAgain), Literal(Boolean false))],
                   Some(Binary(left, Is, right))) when leftRaw = leftAgain ->
            match nullableOperand left, nullableOperand right with
            | Some raw, _ when raw = leftRaw && right = rightRaw && direct right -> Some(raw, left, right)
            | _, Some raw when raw = rightRaw && left = leftRaw && direct left -> Some(raw, right, left)
            | _ -> None
        | _ -> None
    let nullableCandidates original raw operand value =
        // A full matching index admits every row to this fixed decoder, even if
        // SQLite recomputes it. Partial indexes never enter the index model.
        let candidates =
            if indexed operand then Binary(operand, Is, value)
            else Binary(Binary(raw, Is, value), Or, Binary(raw, Ge, Literal(String "")))
        let normalized expression = forSource tableName sourceAlias expression
        let already = predicate |> SqlExpr.exists (function
            | Binary(existing, And, residual) when residual = original -> normalized existing = normalized candidates
            | _ -> false)
        if already then original else Binary(candidates, And, original)
    let rec rewrite original =
        match original with
        | Binary(left, And, right) ->
            let l, r = rewrite left, rewrite right
            if obj.ReferenceEquals(l, left) && obj.ReferenceEquals(r, right) then original
            else Binary(l, And, r)
        | Binary(operand, Eq, value) when direct value ->
            match nativeOperand operand with
            | Some raw -> withCandidates raw (Binary(raw, Eq, value)) original
            | None -> original
        | Binary(value, Eq, operand) when direct value ->
            match nativeOperand operand with
            | Some raw -> withCandidates raw (Binary(value, Eq, raw)) original
            | None -> original
        | InList(operand, head, tail) when direct head && List.forall direct tail ->
            match nativeOperand operand with
            | Some raw -> withCandidates raw (InList(raw, head, tail)) original
            | None -> original
        | _ ->
            match nullableEquality original with
            | Some(raw, operand, value) -> nullableCandidates original raw operand value
            | None -> original
    rewrite predicate

/// A protected consumer also protects its otherwise simple derived input.
let private allowsNativePartition (core: SelectCore) =
    if not core.Joins.IsEmpty || hasMustNotBoundary core
       || not core.OrderBy.IsEmpty || core.Limit.IsSome || core.Offset.IsSome then false
    else
        let mutable protectedExpression = false
        core |> mapCoreExpressions (fun expression ->
            protectedExpression <- protectedExpression || SqlExpr.exists (function
                | InSubquery _ | Exists _ | ScalarSubquery _ | AggregateCall _ | WindowCall _
                | FunctionCall("COUNT", [_]) -> true
                | _ -> false) expression
            expression) id |> ignore
        not protectedExpression

/// Rewrite alias references in an expression to use the table name instead.
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
                let rewritten = forSource tableName a expr
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
/// Among legal orderings, prefer probing the side with a matching
/// index on its join key. Only operates on pure INNER JOIN chains.
let private reshapeJoinsForIndex (model: IndexModel) (core: SelectCore) : SelectCore =
    // Only consider pure INNER JOIN chains with 2+ joins
    if core.Joins.Length < 2 then core
    elif core.Joins |> List.exists (function | ConditionedJoin(Inner, _, _) -> false | _ -> true) then core
    else
        // Check if any join has ON clause referencing an indexed expression
        // and reorder to put indexed-probe joins earlier.
        // Use a simple stable sort: joins whose ON clause references
        // an indexed expression on the join source table sort before others.
        let scoreJoin (j: JoinShape) : int =
            match j with
            | ConditionedJoin(_, BaseTable(tName, tAlias), onExpr) ->
                // Check if any sub-expression in ON matches an index on the join's table,
                // accounting for alias-to-table name resolution.
                if expressionMatchesIndexWithAlias model tName tAlias onExpr then 0 else 1
            | _ -> 1
        let sorted = core.Joins |> List.sortBy scoreJoin
        // Only change if order actually differs
        if sorted = core.Joins then core
        else { core with Joins = sorted }

/// Apply index plan shaping to a single SelectCore.
let private shapeInCore (model: IndexModel) (changed: bool ref) allowPartition allowCharacter (core: SelectCore) : SelectCore =
    let core =
        match core.Source,core.Where with
        | Some(BaseTable(tableName,alias)), Some where when allowCharacter && core.Joins.IsEmpty ->
            let aligned = canonicalizePredicate model tableName (alias |> Option.defaultValue tableName) true true where
            if aligned = where then core else changed.Value <- true; {core with Where = Some aligned}
        | _ -> core
    // Char alignment stays at the original WHERE; it adds no decoder/error work.
    if hasMustNotBoundary core || hasAggregateOrWindowProjections core then core
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
            // Keep qualification intact on joined cores: canonicalization can
            // de-qualify expressions and produce ambiguous column references.
            let canCanonicalize = shaped.Joins.IsEmpty

            // Canonicalize WHERE predicate
            shaped <-
                if not canCanonicalize then shaped
                else
                    match shaped.Where with
                    | Some where ->
                        let alias = match shaped.Source with Some(BaseTable(_,Some alias)) -> alias | _ -> tName
                        let canonical = canonicalizePredicate model tName alias false false where
                        let canonical =
                            if allowPartition then
                                let alias = match shaped.Source with Some(BaseTable(_, Some alias)) -> alias | _ -> tName
                                partitionNativePredicate model tName alias canonical
                            else canonical
                        if canonical = where then shaped
                        else changed.Value <- true; { shaped with Where = Some canonical }
                    | None -> shaped

            // Canonicalize JOIN ON clauses
            shaped <-
                if not canCanonicalize then shaped
                else
                    let newJoins =
                        shaped.Joins |> List.map (fun j ->
                            match j with
                            | ConditionedJoin(kind, source, onExpr) ->
                                let joinTableName =
                                    match resolveTableName source with
                                    | Some n -> n
                                    | None -> tName
                                let canonical = canonicalizePredicate model joinTableName joinTableName false false onExpr
                                let canonical2 = canonicalizePredicate model tName tName false false canonical
                                if canonical2 = onExpr then j
                                else changed.Value <- true; ConditionedJoin(kind, source, canonical2)
                            | CrossJoin _ -> j)
                    if newJoins = shaped.Joins then shaped
                    else { shaped with Joins = newJoins }

            // Canonicalize ORDER BY
            shaped <-
                if (not canCanonicalize) || shaped.OrderBy.IsEmpty then shaped
                else
                    let canonical = canonicalizeOrderBy model tName shaped.OrderBy
                    if canonical = shaped.OrderBy then shaped
                    else changed.Value <- true; { shaped with OrderBy = canonical }

            // Reshape joins for index
            let reshaped = reshapeJoinsForIndex model shaped
            if not (reshaped.Joins = shaped.Joins) then changed.Value <- true
            shaped <- reshaped

            shaped

/// Recursively apply index plan shaping to a SqlSelect.
let rec private shapeSelect (model: IndexModel) (changed: bool ref) allowPartition allowCharacter (sel: SqlSelect) : SqlSelect =
    let allowPartition = allowPartition && sel.Ctes.IsEmpty
    let allowCharacter = allowCharacter && sel.Ctes.IsEmpty
    let core original =
        let allowed = allowPartition && allowsNativePartition original
        original |> mapDerivedSources (shapeSelect model changed allowed allowCharacter)
                 |> shapeInCore model changed allowed allowCharacter
    mapSelectParts (mapSingleCore core) (shapeSelect model changed false false) sel

let shapeIndexSelect model changed sel = shapeSelect model changed true true sel

/// Apply index plan shaping to a SqlStatement.
let shapeIndexStatement (model: IndexModel) (stmt: SqlStatement) : struct(SqlStatement * bool) =
    match stmt with
    | SelectStmt sel ->
        let changed = ref false
        let result = shapeIndexSelect model changed sel
        struct((if obj.ReferenceEquals(sel, result) then stmt else SelectStmt result), changed.Value)
    | InsertStmt ins ->
        // INSERT … VALUES has no SELECT subtree to shape; INSERT … SELECT must recurse so the
        // chain SELECT emitted for N-hop relation chains receives index-plan shaping (no
        // SCAN, no TEMP B-TREE). UPDATE/DELETE WHERE is left unshaped at this layer:
        // index choice for DML predicates is delegated to SQLite's planner via the surrounding
        // UPDATE/DELETE statement, which the JSON1-aware index model already covers at insert
        // time. DDL has no expression tree.
        match ins.Source with
        | InsertValues _ -> struct(stmt, false)
        | InsertSelect sel ->
            let changed = ref false
            let shaped = shapeSelect model changed false false sel
            struct((if obj.ReferenceEquals(sel, shaped) then stmt else InsertStmt { ins with Source = InsertSelect shaped }), changed.Value)
    | UpdateStmt _ | DeleteStmt _ -> struct(stmt, false)
