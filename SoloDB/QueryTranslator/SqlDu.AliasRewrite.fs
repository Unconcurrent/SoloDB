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

let private appendJsonPath (JsonPath left) (JsonPath right) : JsonPath =
    JsonPath(left @ right)

let private jsonPathAsLiteral (JsonPath segments) : SqlExpr =
    let escaped =
        segments
        |> List.map (fun s -> s.Replace("\000", ""))
        |> String.concat "."
    Literal(SqlLiteral.String($"$.{escaped}"))

let rec private rewriteJsonExtractAliasTarget (path: JsonPath) (innerExpr: SqlExpr) : SqlExpr =
    match innerExpr with
    | Column(innerSrc, innerCol) ->
        JsonExtractExpr(innerSrc, innerCol, path)
    | JsonExtractExpr(innerSrc, innerCol, innerPath) ->
        JsonExtractExpr(innerSrc, innerCol, appendJsonPath innerPath path)
    | Cast(inner, sqlType) ->
        Cast(rewriteJsonExtractAliasTarget path inner, sqlType)
    | FunctionCall(name, args) ->
        FunctionCall("jsonb_extract", [FunctionCall(name, args); jsonPathAsLiteral path])
    | Literal _
    | Parameter _
    | JsonSetExpr _
    | JsonArrayExpr _
    | JsonObjectExpr _
    | AggregateCall _
    | WindowCall _
    | Unary _
    | Binary _
    | Between _
    | InList _
    | InSubquery _
    | Coalesce _
    | Exists _
    | ScalarSubquery _
    | CaseExpr _
    | UpdateFragment _ ->
        failwithf "Alias rewrite cannot compose JsonExtract path %A onto inner expression %A" path innerExpr

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
                | Some innerExpr -> rewriteJsonExtractAliasTarget path innerExpr
                | None -> node
            | InSubquery(valueExpr, sel) ->
                InSubquery(valueExpr, policy.OnSubquerySelect sel)
            | Exists(sel) ->
                Exists(policy.OnSubquerySelect sel)
            | ScalarSubquery(sel) ->
                ScalarSubquery(policy.OnSubquerySelect sel)
            | Column _
            | Literal _
            | Parameter _
            | JsonExtractExpr _
            | JsonSetExpr _
            | JsonArrayExpr _
            | JsonObjectExpr _
            | FunctionCall _
            | AggregateCall _
            | WindowCall _
            | Unary _
            | Binary _
            | Between _
            | InList _
            | Cast _
            | Coalesce _
            | CaseExpr _
            | UpdateFragment _ ->
                node)
        expr
