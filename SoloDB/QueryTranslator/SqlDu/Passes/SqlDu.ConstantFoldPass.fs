module internal SoloDatabase.ConstantFoldPass

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
        | _ when liveBranches.Length = allBranches.Length -> node // no dead branches eliminated
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
/// Uses custom bottom-up recursion with per-node child-change tracking.
/// Each node tracks whether any child changed; only when a child or
/// foldNode produces a new value does the node reconstruct and propagate
/// the change flag upward. Leaf nodes and unchanged subtrees return the
/// original reference, so parent ReferenceEquals comparisons work correctly.
let rec private foldExpr (changed: bool ref) (expr: SqlExpr) : SqlExpr =
    let rec loop (node: SqlExpr) : SqlExpr =
        // Per-node flag: did any child in THIS node change?
        let mutable anyChildChanged = false
        let loopChild (child: SqlExpr) =
            let result = loop child
            if not (obj.ReferenceEquals(result, child)) then anyChildChanged <- true
            result
        let loopChildList (lst: SqlExpr list) =
            lst |> List.map loopChild
        let loopChildOpt (opt: SqlExpr option) =
            opt |> Option.map loopChild
        // Track SqlSelect children the same way as SqlExpr children:
        // use a local ref to detect whether foldSelect modified the query,
        // and propagate that into the per-node anyChildChanged flag.
        let loopSelect (query: SqlSelect) =
            let subChanged = ref false
            let result = foldSelect subChanged query
            if subChanged.Value then anyChildChanged <- true
            result
        let mappedNode =
            match node with
            | Column _ -> node
            | Literal _ -> node
            | Parameter _ -> node
            | JsonExtractExpr _ -> node
            | JsonRootExtract _ -> node
            | JsonSetExpr(target, assignments) ->
                JsonSetExpr(
                    loopChild target,
                    assignments |> List.map (fun (path, value) -> path, loopChild value))
            | JsonArrayExpr(elements) ->
                JsonArrayExpr(loopChildList elements)
            | JsonObjectExpr(properties) ->
                JsonObjectExpr(properties |> List.map (fun (key, value) -> key, loopChild value))
            | FunctionCall(name, arguments) ->
                FunctionCall(name, loopChildList arguments)
            | AggregateCall(kind, argument, distinct, separator) ->
                AggregateCall(
                    kind,
                    loopChildOpt argument,
                    distinct,
                    loopChildOpt separator)
            | WindowCall(spec) ->
                WindowCall({
                    spec with
                        Arguments = loopChildList spec.Arguments
                        PartitionBy = loopChildList spec.PartitionBy
                        OrderBy = spec.OrderBy |> List.map (fun (orderExpr, dir) -> loopChild orderExpr, dir)
                })
            | Unary(op, inner) ->
                Unary(op, loopChild inner)
            | Binary(left, op, right) ->
                Binary(loopChild left, op, loopChild right)
            | Between(valueExpr, lower, upper) ->
                Between(loopChild valueExpr, loopChild lower, loopChild upper)
            | InList(valueExpr, head, tail) ->
                InList(loopChild valueExpr, loopChild head, loopChildList tail)
            | InSubquery(valueExpr, query) ->
                InSubquery(loopChild valueExpr, loopSelect query)
            | Cast(inner, sqlType) ->
                Cast(loopChild inner, sqlType)
            | Coalesce(head, tail) ->
                Coalesce(loopChild head, loopChildList tail)
            | Exists query ->
                Exists(loopSelect query)
            | ScalarSubquery query ->
                ScalarSubquery(loopSelect query)
            | CaseExpr(firstBranch, restBranches, elseExpr) ->
                CaseExpr(
                    let mapBranch (condExpr, resultExpr) = loopChild condExpr, loopChild resultExpr
                    mapBranch firstBranch,
                    restBranches |> List.map mapBranch,
                    loopChildOpt elseExpr)
            | UpdateFragment(pathExpr, valueExpr) ->
                UpdateFragment(loopChild pathExpr, loopChild valueExpr)
        let folded = foldNode mappedNode
        let foldChanged = not (obj.ReferenceEquals(folded, mappedNode))
        if anyChildChanged || foldChanged then
            changed.Value <- true
            folded
        else
            node // No child or fold change — return original reference
    loop expr

/// Fold constants in a SelectCore.
and private foldCore (changed: bool ref) (core: SelectCore) : SelectCore =
    { core with
        Projections = core.Projections |> ProjectionSetOps.map (fun p -> { p with Expr = foldExpr changed p.Expr })
        Where = core.Where |> Option.map (foldExpr changed)
        Having = core.Having |> Option.map (foldExpr changed)
        GroupBy = core.GroupBy |> List.map (foldExpr changed)
        OrderBy = core.OrderBy |> List.map (fun ob -> { ob with Expr = foldExpr changed ob.Expr })
        Limit = core.Limit |> Option.map (foldExpr changed)
        Offset = core.Offset |> Option.map (foldExpr changed)
        Source = core.Source |> Option.map (foldTableSource changed)
        Joins = core.Joins |> List.map (foldJoin changed)
    }

/// Fold constants in a TableSource.
and private foldTableSource (changed: bool ref) (src: TableSource) : TableSource =
    match src with
    | BaseTable _ -> src
    | DerivedTable(query, alias) -> DerivedTable(foldSelect changed query, alias)
    | FromJsonEach(expr, alias) -> FromJsonEach(foldExpr changed expr, alias)

/// Fold constants in a JoinShape.
and private foldJoin (changed: bool ref) (join: JoinShape) : JoinShape =
    match join with
    | CrossJoin source ->
        CrossJoin(foldTableSource changed source)
    | ConditionedJoin(kind, source, onExpr) ->
        ConditionedJoin(kind, foldTableSource changed source, foldExpr changed onExpr)

/// Fold constants in a SqlSelect.
and private foldSelect (changed: bool ref) (sel: SqlSelect) : SqlSelect =
    let body =
        match sel.Body with
        | SingleSelect core -> SingleSelect(foldCore changed core)
        | UnionAllSelect(head, tail) ->
            UnionAllSelect(foldCore changed head, tail |> List.map (foldCore changed))
    let ctes =
        sel.Ctes |> List.map (fun cte -> { cte with Query = foldSelect changed cte.Query })
    { Ctes = ctes; Body = body }

/// Fold constants in a SqlStatement.
let private foldStatement (stmt: SqlStatement) : struct(SqlStatement * bool) =
    let changed = ref false
    match stmt with
    | SelectStmt sel ->
        let result = foldSelect changed sel
        struct(SelectStmt result, changed.Value)
    | InsertStmt ins ->
        let result =
            InsertStmt {
                ins with
                    Values = ins.Values |> List.map (List.map (foldExpr changed))
                    Returning = ins.Returning |> Option.map (List.map (foldExpr changed))
            }
        struct(result, changed.Value)
    | UpdateStmt upd ->
        let result =
            UpdateStmt {
                upd with
                    SetClauses = upd.SetClauses |> List.map (fun (col, expr) -> (col, foldExpr changed expr))
                    Where = upd.Where |> Option.map (foldExpr changed)
            }
        struct(result, changed.Value)
    | DeleteStmt del ->
        let result = DeleteStmt { del with Where = del.Where |> Option.map (foldExpr changed) }
        struct(result, changed.Value)
    | DdlStmt _ -> struct(stmt, false)

/// The constant folding pass.
let constantFold : Pass = {
    Name = "ConstantFold"
    Transform = foldStatement
}
