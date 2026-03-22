module SoloDatabase.ConstantFoldPass

open SqlDu.Engine.C1.Spec
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
//   - String operations
//   - Division/modulo (division by zero must be handled at runtime)
//   - Any expression involving columns, parameters, or function calls
// ══════════════════════════════════════════════════════════════

let private foldNode (node: SqlExpr) : SqlExpr =
    match node with
    // T1: Integer binary arithmetic
    | Binary(Literal(Integer a), Add, Literal(Integer b)) -> Literal(Integer(a + b))
    | Binary(Literal(Integer a), Sub, Literal(Integer b)) -> Literal(Integer(a - b))
    | Binary(Literal(Integer a), Mul, Literal(Integer b)) -> Literal(Integer(a * b))
    // T1b: Integer division/modulo (non-zero divisor, no min-int/-1 overflow)
    | Binary(Literal(Integer a), Div, Literal(Integer b)) when b <> 0L && not (a = System.Int64.MinValue && b = -1L) -> Literal(Integer(a / b))
    | Binary(Literal(Integer a), Mod, Literal(Integer b)) when b <> 0L && not (a = System.Int64.MinValue && b = -1L) -> Literal(Integer(a % b))
    // T2b: SQLite 3VL edges (must come before general boolean identity to avoid shadowing)
    | Binary(Literal Null, And, Literal(Boolean false)) -> Literal(Boolean false)
    | Binary(Literal(Boolean false), And, Literal Null) -> Literal(Boolean false)
    | Binary(Literal Null, Or, Literal(Boolean true)) -> Literal(Boolean true)
    | Binary(Literal(Boolean true), Or, Literal Null) -> Literal(Boolean true)
    // T2: Boolean identity
    | Binary(x, And, Literal(Boolean true)) -> x
    | Binary(Literal(Boolean true), And, x) -> x
    | Binary(_, And, Literal(Boolean false)) -> Literal(Boolean false)
    | Binary(Literal(Boolean false), And, _) -> Literal(Boolean false)
    | Binary(x, Or, Literal(Boolean false)) -> x
    | Binary(Literal(Boolean false), Or, x) -> x
    | Binary(_, Or, Literal(Boolean true)) -> Literal(Boolean true)
    | Binary(Literal(Boolean true), Or, _) -> Literal(Boolean true)
    | Unary(Not, Literal(Boolean true)) -> Literal(Boolean false)
    | Unary(Not, Literal(Boolean false)) -> Literal(Boolean true)
    | Unary(Not, Unary(Not, x)) -> x
    // T3: Null propagation (comparison with NULL → NULL)
    | Binary(Literal Null, (Eq | Ne | Lt | Le | Gt | Ge), _) -> Literal Null
    | Binary(_, (Eq | Ne | Lt | Le | Gt | Ge), Literal Null) -> Literal Null
    // T4: Integer comparison simplification
    | Binary(Literal(Integer a), Eq, Literal(Integer b)) -> Literal(Boolean(a = b))
    | Binary(Literal(Integer a), Ne, Literal(Integer b)) -> Literal(Boolean(a <> b))
    | Binary(Literal(Integer a), Lt, Literal(Integer b)) -> Literal(Boolean(a < b))
    | Binary(Literal(Integer a), Le, Literal(Integer b)) -> Literal(Boolean(a <= b))
    | Binary(Literal(Integer a), Gt, Literal(Integer b)) -> Literal(Boolean(a > b))
    | Binary(Literal(Integer a), Ge, Literal(Integer b)) -> Literal(Boolean(a >= b))
    // T4b: String equality/inequality (collation-safe)
    | Binary(Literal(String a), Eq, Literal(String b)) -> Literal(Boolean(a = b))
    | Binary(Literal(String a), Ne, Literal(String b)) -> Literal(Boolean(a <> b))
    // T5: CASE dead-branch elimination
    | CaseExpr(firstBranch, restBranches, elseExpr) ->
        let allBranches = firstBranch :: restBranches
        let liveBranches = allBranches |> List.filter (fun (cond, _) -> cond <> Literal(Boolean false))
        match liveBranches with
        | [] -> elseExpr |> Option.defaultValue (Literal Null)
        | (Literal(Boolean true), result) :: _ -> result
        | first :: rest -> CaseExpr(first, rest, elseExpr)
    // T6: Whitelist pure function folds (literal args only)
    | FunctionCall("typeof", [Literal Null]) -> Literal(String "null")
    | FunctionCall("typeof", [Literal(Integer _)]) -> Literal(String "integer")
    | FunctionCall("typeof", [Literal(Boolean _)]) -> Literal(String "integer")
    | FunctionCall("typeof", [Literal(Float _)]) -> Literal(String "real")
    | FunctionCall("typeof", [Literal(String _)]) -> Literal(String "text")
    | FunctionCall("typeof", [Literal(Blob _)]) -> Literal(String "blob")
    | FunctionCall("length", [Literal(String s)]) -> Literal(Integer(int64 s.Length))
    | FunctionCall("abs", [Literal(Integer n)]) when n <> System.Int64.MinValue -> Literal(Integer(abs n))
    | FunctionCall("abs", [Literal(Float n)]) -> Literal(Float(abs n))
    // T6b: CONCAT literal fold (string concatenation)
    | FunctionCall("CONCAT", args) when args |> List.forall (function Literal(String _) -> true | _ -> false) ->
        let result = args |> List.map (function Literal(String s) -> s | _ -> "") |> String.concat ""
        Literal(String result)
    | _ -> node

/// Fold constant expressions in a SqlExpr tree.
let rec private foldExpr (expr: SqlExpr) : SqlExpr =
    SqlExpr.map
        (fun node ->
            let withSelects =
                match node with
                | InSubquery(valueExpr, sel) ->
                    InSubquery(valueExpr, foldSelect sel)
                | Exists(sel) ->
                    Exists(foldSelect sel)
                | ScalarSubquery(sel) ->
                    ScalarSubquery(foldSelect sel)
                | _ ->
                    node
            foldNode withSelects)
        expr

/// Fold constants in a SelectCore.
and private foldCore (core: SelectCore) : SelectCore =
    { core with
        Projections = core.Projections |> ProjectionSetOps.map (fun p -> { p with Expr = foldExpr p.Expr })
        Where = core.Where |> Option.map foldExpr
        Having = core.Having |> Option.map foldExpr
        GroupBy = core.GroupBy |> List.map foldExpr
        OrderBy = core.OrderBy |> List.map (fun ob -> { ob with Expr = foldExpr ob.Expr })
        Limit = core.Limit |> Option.map foldExpr
        Offset = core.Offset |> Option.map foldExpr
        Source = core.Source |> Option.map foldTableSource
        Joins = core.Joins |> List.map foldJoin
    }

/// Fold constants in a TableSource.
and private foldTableSource (src: TableSource) : TableSource =
    match src with
    | BaseTable _ -> src
    | DerivedTable(query, alias) -> DerivedTable(foldSelect query, alias)
    | FromJsonEach(expr, alias) -> FromJsonEach(foldExpr expr, alias)

/// Fold constants in a JoinShape.
and private foldJoin (join: JoinShape) : JoinShape =
    match join with
    | CrossJoin source ->
        CrossJoin(foldTableSource source)
    | ConditionedJoin(kind, source, onExpr) ->
        ConditionedJoin(kind, foldTableSource source, foldExpr onExpr)

/// Fold constants in a SqlSelect.
and private foldSelect (sel: SqlSelect) : SqlSelect =
    let body =
        match sel.Body with
        | SingleSelect core -> SingleSelect(foldCore core)
        | UnionAllSelect(head, tail) ->
            UnionAllSelect(foldCore head, tail |> List.map foldCore)
    let ctes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = foldSelect cte.Query })
    { Ctes = ctes; Body = body }

/// Fold constants in a SqlStatement.
let private foldStatement (stmt: SqlStatement) : SqlStatement =
    match stmt with
    | SelectStmt sel -> SelectStmt(foldSelect sel)
    | InsertStmt ins ->
        InsertStmt {
            ins with
                Values = ins.Values |> List.map (List.map foldExpr)
                Returning = ins.Returning |> Option.map (List.map foldExpr)
        }
    | UpdateStmt upd ->
        UpdateStmt {
            upd with
                SetClauses = upd.SetClauses |> List.map (fun (col, expr) -> (col, foldExpr expr))
                Where = upd.Where |> Option.map foldExpr
        }
    | DeleteStmt del ->
        DeleteStmt { del with Where = del.Where |> Option.map foldExpr }
    | DdlStmt _ -> stmt

/// The constant folding pass.
let constantFold : Pass = {
    Name = "ConstantFold"
    Transform = foldStatement
}
