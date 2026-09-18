module internal SoloDatabase.ConstantFoldPass

open SoloDatabase.SqlModel
open SoloDatabase.PassTypes

// ══════════════════════════════════════════════════════════════
// Constant Folding Pass
//
// Folds trivial literal arithmetic at compile time:
//   Binary(Literal(Integer a), Add, Literal(Integer b)) → Literal(Integer(a + b))
//   Binary(Literal(Integer a), Sub, Literal(Integer b)) → Literal(Integer(a - b))
//   Binary(Literal(Integer a), Mul, Literal(Integer b)) → Literal(Integer(a * b))
//
// Does NOT fold:
//   - Float arithmetic (precision semantics differ between F# and SQLite)
//   - Integer overflow (SQLite promotes arithmetic to REAL)
//   - Division/modulo with zero or overflowing operands
//   - Nonconstant arithmetic and comparisons involving columns or parameters
// ══════════════════════════════════════════════════════════════

// SQLite length counts Unicode code points before the first NUL, not UTF-16 units.
let private textLength (text: string) =
    let mutable index = 0
    let mutable length = 0L
    while index < text.Length && text.[index] <> '\000' do
        if System.Char.IsHighSurrogate text.[index]
           && index + 1 < text.Length && System.Char.IsLowSurrogate text.[index + 1] then
            index <- index + 2
        else
            index <- index + 1
        length <- length + 1L
    length

// Each SQL operand crosses the UTF-8 boundary separately. Invalid UTF-16 must
// be replaced before comparing or concatenating operands, as SQLite receives it.
let private textOperand (text: string) =
    let mutable index = 0
    let mutable valid = true
    while valid && index < text.Length do
        let character = text.[index]
        if System.Char.IsHighSurrogate character
           && index + 1 < text.Length && System.Char.IsLowSurrogate text.[index + 1] then
            index <- index + 2
        elif System.Char.IsSurrogate character then
            valid <- false
        else
            index <- index + 1
    if valid then text
    else System.Text.Encoding.UTF8.GetString(System.Text.Encoding.UTF8.GetBytes text)

// Boolean identities may return an operand only when it already has SQLite's
// normalized 0/1/NULL result, rather than an arbitrary truthy scalar.
let private isBoolean = function
    | Literal Null | Literal(Boolean _) | Literal(Integer 0L) | Literal(Integer 1L)
    | Unary((Not | IsNull | IsNotNull), _)
    | Binary(_, (Eq | Ne | Lt | Le | Gt | Ge | And | Or | Like | Glob | Regexp | Is | IsNot | In | NotInOp), _)
    | Between _ | InList _ | InSubquery _ | Exists _ -> true
    | _ -> false

// A NULL comparison must still evaluate a potentially failing operand.
let private canDiscard = function
    | Column _ | Literal Null | Literal(Boolean _) | Literal(Integer _) -> true
    | Literal(Float value) -> not (System.Double.IsNaN value)
    | Literal(String value) -> not (isNull value)
    | Literal(Blob value) -> not (isNull value)
    | _ -> false

let private foldNode (node: SqlExpr) : SqlExpr =
    match node with
    // T1: Integer binary arithmetic
    | Binary(Literal(Integer a), ((Add | Sub | Mul) as operation), Literal(Integer b)) ->
        try
            let value =
                match operation with
                | Add -> Checked.(+) a b
                | Sub -> Checked.(-) a b
                | _ -> Checked.(*) a b
            Literal(Integer value)
        with :? System.OverflowException -> node
    // T1b: Integer division/modulo (non-zero divisor, no min-int/-1 overflow)
    | Binary(Literal(Integer a), Div, Literal(Integer b)) when b <> 0L && not (a = System.Int64.MinValue && b = -1L) -> Literal(Integer(a / b))
    | Binary(Literal(Integer a), Mod, Literal(Integer b)) when b <> 0L && not (a = System.Int64.MinValue && b = -1L) -> Literal(Integer(a % b))
    // T2b: SQLite 3VL edges (must come before general boolean identity to avoid shadowing)
    | Binary(Literal Null, And, Literal(Boolean false)) -> Literal(Boolean false)
    | Binary(Literal(Boolean false), And, Literal Null) -> Literal(Boolean false)
    | Binary(Literal Null, Or, Literal(Boolean true)) -> Literal(Boolean true)
    | Binary(Literal(Boolean true), Or, Literal Null) -> Literal(Boolean true)
    // T2: Boolean identity
    | Binary(x, And, Literal(Boolean true)) when isBoolean x -> x
    | Binary(Literal(Boolean true), And, x) when isBoolean x -> x
    | Binary(other, And, Literal(Boolean false)) when canDiscard other -> Literal(Boolean false)
    | Binary(Literal(Boolean false), And, other) when canDiscard other -> Literal(Boolean false)
    | Binary(x, Or, Literal(Boolean false)) when isBoolean x -> x
    | Binary(Literal(Boolean false), Or, x) when isBoolean x -> x
    | Binary(other, Or, Literal(Boolean true)) when canDiscard other -> Literal(Boolean true)
    | Binary(Literal(Boolean true), Or, other) when canDiscard other -> Literal(Boolean true)
    | Unary(Not, Literal(Boolean true)) -> Literal(Boolean false)
    | Unary(Not, Literal(Boolean false)) -> Literal(Boolean true)
    | Unary(Not, Unary(Not, x)) when isBoolean x -> x
    // T3: Null propagation (comparison with NULL → NULL)
    | Binary(Literal Null, (Eq | Ne | Lt | Le | Gt | Ge), other) when canDiscard other -> Literal Null
    | Binary(other, (Eq | Ne | Lt | Le | Gt | Ge), Literal Null) when canDiscard other -> Literal Null
    // T4: Integer comparison simplification
    | Binary(Literal(Integer a), Eq, Literal(Integer b)) -> Literal(Boolean(a = b))
    | Binary(Literal(Integer a), Ne, Literal(Integer b)) -> Literal(Boolean(a <> b))
    | Binary(Literal(Integer a), Lt, Literal(Integer b)) -> Literal(Boolean(a < b))
    | Binary(Literal(Integer a), Le, Literal(Integer b)) -> Literal(Boolean(a <= b))
    | Binary(Literal(Integer a), Gt, Literal(Integer b)) -> Literal(Boolean(a > b))
    | Binary(Literal(Integer a), Ge, Literal(Integer b)) -> Literal(Boolean(a >= b))
    // T4b: String equality/inequality (collation-safe)
    | Binary(Literal(String a), Eq, Literal(String b)) -> Literal(Boolean(textOperand a = textOperand b))
    | Binary(Literal(String a), Ne, Literal(String b)) -> Literal(Boolean(textOperand a <> textOperand b))
    // T5: CASE dead-branch elimination
    | CaseExpr(firstBranch, restBranches, elseExpr) ->
        let allBranches = firstBranch :: restBranches
        let removable (condition, result) = condition = Literal(Boolean false) && canDiscard result
        let liveBranches = allBranches |> List.filter (removable >> not)
        match liveBranches with
        | [] -> elseExpr |> Option.defaultValue (Literal Null)
        | (Literal(Boolean true), result) :: rest
            when rest |> List.forall (fun (condition, value) -> canDiscard condition && canDiscard value)
                 && (elseExpr |> Option.forall canDiscard) -> result
        | _ when liveBranches.Length = allBranches.Length -> node // no dead branches eliminated
        | first :: rest -> CaseExpr(first, rest, elseExpr)
    // T6: Whitelist pure function folds (literal args only)
    | FunctionCall("typeof", [Literal Null]) -> Literal(String "null")
    | FunctionCall("typeof", [Literal(Integer _)]) -> Literal(String "integer")
    | FunctionCall("typeof", [Literal(Boolean _)]) -> Literal(String "integer")
    | FunctionCall("typeof", [Literal(Float value)]) when not (System.Double.IsNaN value) -> Literal(String "real")
    | FunctionCall("typeof", [Literal(String _)]) -> Literal(String "text")
    | FunctionCall("typeof", [Literal(Blob _)]) -> Literal(String "blob")
    | FunctionCall("length", [Literal(String s)]) -> Literal(Integer(textLength s))
    | FunctionCall("abs", [Literal(Integer n)]) when n <> System.Int64.MinValue -> Literal(Integer(abs n))
    | FunctionCall("abs", [Literal(Float n)]) -> Literal(Float(abs n))
    // T6b: CONCAT literal fold (string concatenation)
    | FunctionCall("CONCAT", args) when args |> List.forall (function Literal(String _) -> true | _ -> false) ->
        let result = args |> List.map (function Literal(String s) -> textOperand s | _ -> "") |> String.concat ""
        Literal(String result)
    | _ -> node

let private same left right = obj.ReferenceEquals(left, right)

/// Fold expressions through the shared child mapper, retaining unchanged subtrees.
let rec private foldExpr (changed: bool ref) (expr: SqlExpr) : SqlExpr =
    let rec loop node =
        let mapped = SqlExprCombinators.mapChildren loop (foldSelect changed) node
        let folded = foldNode mapped
        if not (same folded node) then changed.Value <- true
        folded
    loop expr

and private foldCore changed (core: SelectCore) =
    let projection (original: Projection) =
        let expression = foldExpr changed original.Expr
        if same expression original.Expr then original else { original with Expr = expression }
    let projections =
        match core.Projections with
        | AllColumns -> core.Projections
        | Explicit(head, tail) ->
            let mappedHead = projection head
            let mappedTail = SqlExprCombinators.mapList projection tail
            if same head mappedHead && same tail mappedTail then core.Projections
            else Explicit(mappedHead, mappedTail)
    let where = SqlExprCombinators.mapOption (foldExpr changed) core.Where
    let having = SqlExprCombinators.mapOption (foldExpr changed) core.Having
    let groupBy = SqlExprCombinators.mapList (foldExpr changed) core.GroupBy
    let orderBy = SqlExprCombinators.mapList (fun original ->
        let expression = foldExpr changed original.Expr
        if same expression original.Expr then original else { original with Expr = expression }) core.OrderBy
    let limit = SqlExprCombinators.mapOption (foldExpr changed) core.Limit
    let offset = SqlExprCombinators.mapOption (foldExpr changed) core.Offset
    let source = SqlExprCombinators.mapOption (foldTableSource changed) core.Source
    let joins = SqlExprCombinators.mapList (foldJoin changed) core.Joins
    if same projections core.Projections && same where core.Where && same having core.Having
       && same groupBy core.GroupBy && same orderBy core.OrderBy && same limit core.Limit
       && same offset core.Offset && same source core.Source && same joins core.Joins then core
    else
        { core with Projections = projections; Where = where; Having = having
                    GroupBy = groupBy; OrderBy = orderBy; Limit = limit; Offset = offset
                    Source = source; Joins = joins }

and private foldTableSource changed source =
    match source with
    | BaseTable _ -> source
    | DerivedTable(query, alias) ->
        let mapped = foldSelect changed query
        if same query mapped then source else DerivedTable(mapped, alias)
    | FromJsonEach(expression, alias) ->
        let mapped = foldExpr changed expression
        if same expression mapped then source else FromJsonEach(mapped, alias)

and private foldJoin changed join =
    match join with
    | CrossJoin source ->
        let mapped = foldTableSource changed source
        if same source mapped then join else CrossJoin mapped
    | ConditionedJoin(kind, source, condition) ->
        let mappedSource = foldTableSource changed source
        let mappedCondition = foldExpr changed condition
        if same source mappedSource && same condition mappedCondition then join
        else ConditionedJoin(kind, mappedSource, mappedCondition)

and private foldSelect changed (query: SqlSelect) =
    let body =
        match query.Body with
        | SingleSelect core ->
            let mapped = foldCore changed core
            if same core mapped then query.Body else SingleSelect mapped
        | UnionAllSelect(head, tail) ->
            let mappedHead = foldCore changed head
            let mappedTail = SqlExprCombinators.mapList (foldCore changed) tail
            if same head mappedHead && same tail mappedTail then query.Body
            else UnionAllSelect(mappedHead, mappedTail)
    let ctes = SqlExprCombinators.mapList (fun original ->
        let mapped = foldSelect changed original.Query
        if same mapped original.Query then original else { original with Query = mapped }) query.Ctes
    if same body query.Body && same ctes query.Ctes then query
    else { Ctes = ctes; Body = body }

/// Fold constants in a statement without copying unchanged containers.
let private foldStatement (statement: SqlStatement) : struct(SqlStatement * bool) =
    let changed = ref false
    let result =
        match statement with
        | SelectStmt query ->
            let mapped = foldSelect changed query
            if same mapped query then statement else SelectStmt mapped
        | InsertStmt insert ->
            let source =
                match insert.Source with
                | InsertValues rows ->
                    let mapped = SqlExprCombinators.mapList (SqlExprCombinators.mapList (foldExpr changed)) rows
                    if same mapped rows then insert.Source else InsertValues mapped
                | InsertSelect query ->
                    let mapped = foldSelect changed query
                    if same mapped query then insert.Source else InsertSelect mapped
            let returning = SqlExprCombinators.mapOption (SqlExprCombinators.mapList (foldExpr changed)) insert.Returning
            if same source insert.Source && same returning insert.Returning then statement
            else InsertStmt { insert with Source = source; Returning = returning }
        | UpdateStmt update ->
            let clauses = SqlExprCombinators.mapList (fun ((column, expression) as original) ->
                let mapped = foldExpr changed expression
                if same mapped expression then original else column, mapped) update.SetClauses
            let where = SqlExprCombinators.mapOption (foldExpr changed) update.Where
            if same clauses update.SetClauses && same where update.Where then statement
            else UpdateStmt { update with SetClauses = clauses; Where = where }
        | DeleteStmt delete ->
            let where = SqlExprCombinators.mapOption (foldExpr changed) delete.Where
            if same where delete.Where then statement else DeleteStmt { delete with Where = where }
    struct(result, changed.Value)

/// The constant folding pass.
let constantFold : Pass = {
    Name = "ConstantFold"
    Transform = foldStatement
}
