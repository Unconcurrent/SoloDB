module SoloDatabase.ExpressionPredicates

open SqlDu.Engine.C1.Spec

let hasWindowFunction (expr: SqlExpr) : bool =
    SqlExpr.exists
        (fun node ->
            match node with
            | WindowCall _ -> true
            | _ -> false)
        expr

let hasAggregateCall (expr: SqlExpr) : bool =
    SqlExpr.exists
        (fun node ->
            match node with
            | AggregateCall _ -> true
            | _ -> false)
        expr

let hasFunctionCall (expr: SqlExpr) : bool =
    SqlExpr.exists
        (fun node ->
            match node with
            | FunctionCall _
            | AggregateCall _
            | WindowCall _ -> true
            | _ -> false)
        expr

let hasCorrelatedRef (expr: SqlExpr) : bool =
    SqlExpr.exists
        (fun node ->
            match node with
            | InSubquery _
            | Exists _
            | ScalarSubquery _ -> true
            | _ -> false)
        expr
