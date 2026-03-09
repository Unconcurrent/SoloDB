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

/// Recursively fold constant expressions in a SqlExpr tree.
let rec private foldExpr (expr: SqlExpr) : SqlExpr =
    match expr with
    // Integer binary arithmetic
    | Binary(Literal(Integer a), Add, Literal(Integer b)) -> Literal(Integer(a + b))
    | Binary(Literal(Integer a), Sub, Literal(Integer b)) -> Literal(Integer(a - b))
    | Binary(Literal(Integer a), Mul, Literal(Integer b)) -> Literal(Integer(a * b))

    // Recurse into compound expressions, fold children first then retry
    | Binary(left, op, right) ->
        let left' = foldExpr left
        let right' = foldExpr right
        let folded = Binary(left', op, right')
        match folded with
        | Binary(Literal(Integer a), Add, Literal(Integer b)) -> Literal(Integer(a + b))
        | Binary(Literal(Integer a), Sub, Literal(Integer b)) -> Literal(Integer(a - b))
        | Binary(Literal(Integer a), Mul, Literal(Integer b)) -> Literal(Integer(a * b))
        | other -> other

    | Unary(op, inner) ->
        Unary(op, foldExpr inner)

    | FunctionCall(name, args) ->
        FunctionCall(name, args |> List.map foldExpr)

    | AggregateCall(kind, arg, distinct, sep) ->
        AggregateCall(kind, arg |> Option.map foldExpr, distinct, sep |> Option.map foldExpr)

    | Coalesce(exprs) ->
        Coalesce(exprs |> List.map foldExpr)

    | Cast(inner, sqlType) ->
        Cast(foldExpr inner, sqlType)

    | CaseExpr(branches, elseExpr) ->
        CaseExpr(
            branches |> List.map (fun (cond, result) -> (foldExpr cond, foldExpr result)),
            elseExpr |> Option.map foldExpr)

    | InList(expr, list) ->
        InList(foldExpr expr, list |> List.map foldExpr)

    | InSubquery(expr, sel) ->
        InSubquery(foldExpr expr, foldSelect sel)

    | Exists(sel) ->
        Exists(foldSelect sel)

    | ScalarSubquery(sel) ->
        ScalarSubquery(foldSelect sel)

    | Between(expr, low, high) ->
        Between(foldExpr expr, foldExpr low, foldExpr high)

    | WindowCall(spec) ->
        WindowCall({
            spec with
                Arguments = spec.Arguments |> List.map foldExpr
                PartitionBy = spec.PartitionBy |> List.map foldExpr
                OrderBy = spec.OrderBy |> List.map (fun (e, d) -> (foldExpr e, d))
        })

    | JsonSetExpr(target, assignments) ->
        JsonSetExpr(foldExpr target, assignments |> List.map (fun (p, e) -> (p, foldExpr e)))

    | JsonArrayExpr(elements) ->
        JsonArrayExpr(elements |> List.map foldExpr)

    | JsonObjectExpr(props) ->
        JsonObjectExpr(props |> List.map (fun (k, v) -> (k, foldExpr v)))

    // Leaf nodes: Column, Literal, Parameter, JsonExtractExpr — no folding
    | other -> other

/// Fold constants in a SelectCore.
and private foldCore (core: SelectCore) : SelectCore =
    { core with
        Projections = core.Projections |> List.map (fun p -> { p with Expr = foldExpr p.Expr })
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
    { join with
        Source = foldTableSource join.Source
        On = join.On |> Option.map foldExpr
    }

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
