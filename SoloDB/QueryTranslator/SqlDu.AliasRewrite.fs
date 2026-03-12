module SoloDatabase.AliasRewrite

open SqlDu.Engine.C1.Spec

type AliasRewritePolicy = {
    MatchEmptyDerivedAlias: bool
    OnSubquerySelect: SqlSelect -> SqlSelect
}

let buildProjectionAliasMap (innerCore: SelectCore) : Map<string, SqlExpr> =
    innerCore.Projections
    |> List.choose (fun p ->
        match p.Alias with
        | Some alias -> Some(alias, p.Expr)
        | None ->
            match p.Expr with
            | Column(_, colName) -> Some(colName, p.Expr)
            | _ -> None)
    |> Map.ofList

let private matchesDerivedAlias (policy: AliasRewritePolicy) (derivedAlias: string) (src: string) : bool =
    src = derivedAlias || (policy.MatchEmptyDerivedAlias && derivedAlias = "" && src = "")

let rec rewriteDerivedAliasExpr (policy: AliasRewritePolicy) (aliasMap: Map<string, SqlExpr>) (derivedAlias: string) (expr: SqlExpr) : SqlExpr =
    match expr with
    | Column(Some src, col) when matchesDerivedAlias policy derivedAlias src ->
        match Map.tryFind col aliasMap with
        | Some innerExpr -> innerExpr
        | None -> expr
    | Column(None, col) ->
        match Map.tryFind col aliasMap with
        | Some innerExpr -> innerExpr
        | None -> expr
    | Binary(l, op, r) ->
        Binary(rewriteDerivedAliasExpr policy aliasMap derivedAlias l, op, rewriteDerivedAliasExpr policy aliasMap derivedAlias r)
    | Unary(op, e) ->
        Unary(op, rewriteDerivedAliasExpr policy aliasMap derivedAlias e)
    | FunctionCall(name, args) ->
        FunctionCall(name, args |> List.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias))
    | AggregateCall(kind, arg, distinct, sep) ->
        AggregateCall(kind,
            arg |> Option.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias),
            distinct,
            sep |> Option.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias))
    | Coalesce(exprs) ->
        Coalesce(exprs |> List.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias))
    | Cast(e, t) ->
        Cast(rewriteDerivedAliasExpr policy aliasMap derivedAlias e, t)
    | CaseExpr(branches, elseE) ->
        CaseExpr(
            branches |> List.map (fun (c, r) ->
                (rewriteDerivedAliasExpr policy aliasMap derivedAlias c, rewriteDerivedAliasExpr policy aliasMap derivedAlias r)),
            elseE |> Option.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias))
    | JsonExtractExpr(Some src, col, path) when matchesDerivedAlias policy derivedAlias src ->
        match Map.tryFind col aliasMap with
        | Some(Column(innerSrc, innerCol)) -> JsonExtractExpr(innerSrc, innerCol, path)
        | _ -> expr
    | JsonSetExpr(target, assignments) ->
        JsonSetExpr(
            rewriteDerivedAliasExpr policy aliasMap derivedAlias target,
            assignments |> List.map (fun (p, e) -> (p, rewriteDerivedAliasExpr policy aliasMap derivedAlias e)))
    | JsonArrayExpr(elems) ->
        JsonArrayExpr(elems |> List.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias))
    | JsonObjectExpr(props) ->
        JsonObjectExpr(props |> List.map (fun (k, v) -> (k, rewriteDerivedAliasExpr policy aliasMap derivedAlias v)))
    | InList(e, list) ->
        InList(rewriteDerivedAliasExpr policy aliasMap derivedAlias e, list |> List.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias))
    | InSubquery(e, sel) ->
        InSubquery(rewriteDerivedAliasExpr policy aliasMap derivedAlias e, policy.OnSubquerySelect sel)
    | Exists(sel) ->
        Exists(policy.OnSubquerySelect sel)
    | ScalarSubquery(sel) ->
        ScalarSubquery(policy.OnSubquerySelect sel)
    | Between(e, lo, hi) ->
        Between(
            rewriteDerivedAliasExpr policy aliasMap derivedAlias e,
            rewriteDerivedAliasExpr policy aliasMap derivedAlias lo,
            rewriteDerivedAliasExpr policy aliasMap derivedAlias hi)
    | WindowCall(spec) ->
        WindowCall({
            spec with
                Arguments = spec.Arguments |> List.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias)
                PartitionBy = spec.PartitionBy |> List.map (rewriteDerivedAliasExpr policy aliasMap derivedAlias)
                OrderBy = spec.OrderBy |> List.map (fun (e, d) -> (rewriteDerivedAliasExpr policy aliasMap derivedAlias e, d))
        })
    | _ -> expr
