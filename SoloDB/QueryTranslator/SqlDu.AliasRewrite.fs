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

let rewriteDerivedAliasExpr (policy: AliasRewritePolicy) (aliasMap: Map<string, SqlExpr>) (derivedAlias: string) (expr: SqlExpr) : SqlExpr =
    SqlExpr.map
        (fun node ->
            match node with
            | Column(Some src, col) when matchesDerivedAlias policy derivedAlias src ->
                match Map.tryFind col aliasMap with
                | Some innerExpr -> innerExpr
                | None -> node
            | Column(None, col) ->
                match Map.tryFind col aliasMap with
                | Some innerExpr -> innerExpr
                | None -> node
            | JsonExtractExpr(Some src, col, path) when matchesDerivedAlias policy derivedAlias src ->
                match Map.tryFind col aliasMap with
                | Some(Column(innerSrc, innerCol)) -> JsonExtractExpr(innerSrc, innerCol, path)
                | _ -> node
            | InSubquery(valueExpr, sel) ->
                InSubquery(valueExpr, policy.OnSubquerySelect sel)
            | Exists(sel) ->
                Exists(policy.OnSubquerySelect sel)
            | ScalarSubquery(sel) ->
                ScalarSubquery(policy.OnSubquerySelect sel)
            | _ ->
                node)
        expr
