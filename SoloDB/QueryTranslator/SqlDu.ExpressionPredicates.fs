module SoloDatabase.ExpressionPredicates

open SqlDu.Engine.C1.Spec

let rec hasWindowFunction (expr: SqlExpr) : bool =
    match expr with
    | WindowCall _ -> true
    | Binary(l, _, r) -> hasWindowFunction l || hasWindowFunction r
    | Unary(_, e) -> hasWindowFunction e
    | FunctionCall(_, args) -> args |> List.exists hasWindowFunction
    | Coalesce(exprs) -> exprs |> List.exists hasWindowFunction
    | Cast(e, _) -> hasWindowFunction e
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasWindowFunction c || hasWindowFunction r)
        || (elseE |> Option.map hasWindowFunction |> Option.defaultValue false)
    | Between(e, lo, hi) -> hasWindowFunction e || hasWindowFunction lo || hasWindowFunction hi
    | InList(e, list) -> hasWindowFunction e || (list |> List.exists hasWindowFunction)
    | JsonSetExpr(t, assignments) ->
        hasWindowFunction t || assignments |> List.exists (fun (_, e) -> hasWindowFunction e)
    | JsonArrayExpr(elems) -> elems |> List.exists hasWindowFunction
    | JsonObjectExpr(props) -> props |> List.exists (fun (_, v) -> hasWindowFunction v)
    | UpdateFragment(path, value) -> hasWindowFunction path || hasWindowFunction value
    | AggregateCall _ -> false
    | _ -> false

let rec hasAggregateCall (expr: SqlExpr) : bool =
    match expr with
    | AggregateCall _ -> true
    | Binary(l, _, r) -> hasAggregateCall l || hasAggregateCall r
    | Unary(_, e) -> hasAggregateCall e
    | FunctionCall(_, args) -> args |> List.exists hasAggregateCall
    | Coalesce(exprs) -> exprs |> List.exists hasAggregateCall
    | Cast(e, _) -> hasAggregateCall e
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasAggregateCall c || hasAggregateCall r)
        || (elseE |> Option.map hasAggregateCall |> Option.defaultValue false)
    | Between(e, lo, hi) -> hasAggregateCall e || hasAggregateCall lo || hasAggregateCall hi
    | InList(e, list) -> hasAggregateCall e || (list |> List.exists hasAggregateCall)
    | JsonSetExpr(t, assignments) ->
        hasAggregateCall t || assignments |> List.exists (fun (_, e) -> hasAggregateCall e)
    | JsonArrayExpr(elems) -> elems |> List.exists hasAggregateCall
    | JsonObjectExpr(props) -> props |> List.exists (fun (_, v) -> hasAggregateCall v)
    | WindowCall _ -> false
    | _ -> false


let rec hasFunctionCall (expr: SqlExpr) : bool =
    match expr with
    | FunctionCall _ -> true
    | AggregateCall _ -> true
    | WindowCall _ -> true
    | Binary(l, _, r) -> hasFunctionCall l || hasFunctionCall r
    | Unary(_, e) -> hasFunctionCall e
    | Coalesce(exprs) -> exprs |> List.exists hasFunctionCall
    | Cast(e, _) -> hasFunctionCall e
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasFunctionCall c || hasFunctionCall r)
        || (elseE |> Option.map hasFunctionCall |> Option.defaultValue false)
    | Between(e, lo, hi) -> hasFunctionCall e || hasFunctionCall lo || hasFunctionCall hi
    | InList(e, list) -> hasFunctionCall e || (list |> List.exists hasFunctionCall)
    | JsonSetExpr(t, assignments) ->
        hasFunctionCall t || assignments |> List.exists (fun (_, e) -> hasFunctionCall e)
    | JsonArrayExpr(elems) -> elems |> List.exists hasFunctionCall
    | JsonObjectExpr(props) -> props |> List.exists (fun (_, v) -> hasFunctionCall v)
    | UpdateFragment(path, value) -> hasFunctionCall path || hasFunctionCall value
    | _ -> false

let rec hasCorrelatedRef (expr: SqlExpr) : bool =
    match expr with
    | InSubquery _ | Exists _ | ScalarSubquery _ -> true
    | Binary(l, _, r) -> hasCorrelatedRef l || hasCorrelatedRef r
    | Unary(_, e) -> hasCorrelatedRef e
    | FunctionCall(_, args) -> args |> List.exists hasCorrelatedRef
    | AggregateCall(_, arg, _, sep) ->
        (arg |> Option.map hasCorrelatedRef |> Option.defaultValue false)
        || (sep |> Option.map hasCorrelatedRef |> Option.defaultValue false)
    | Coalesce(exprs) -> exprs |> List.exists hasCorrelatedRef
    | Cast(e, _) -> hasCorrelatedRef e
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasCorrelatedRef c || hasCorrelatedRef r)
        || (elseE |> Option.map hasCorrelatedRef |> Option.defaultValue false)
    | Between(e, lo, hi) -> hasCorrelatedRef e || hasCorrelatedRef lo || hasCorrelatedRef hi
    | InList(e, list) -> hasCorrelatedRef e || (list |> List.exists hasCorrelatedRef)
    | JsonSetExpr(t, assignments) ->
        hasCorrelatedRef t || (assignments |> List.exists (fun (_, e) -> hasCorrelatedRef e))
    | JsonArrayExpr(elems) -> elems |> List.exists hasCorrelatedRef
    | JsonObjectExpr(props) -> props |> List.exists (fun (_, v) -> hasCorrelatedRef v)
    | WindowCall(spec) ->
        (spec.Arguments |> List.exists hasCorrelatedRef)
        || (spec.PartitionBy |> List.exists hasCorrelatedRef)
        || (spec.OrderBy |> List.exists (fun (e, _) -> hasCorrelatedRef e))
    | UpdateFragment(path, value) -> hasCorrelatedRef path || hasCorrelatedRef value
    | _ -> false
