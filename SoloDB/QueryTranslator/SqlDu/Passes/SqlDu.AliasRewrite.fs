module internal SoloDatabase.AliasRewrite

open SoloDatabase.SqlModel

type AliasRewritePolicy = {
    MatchEmptyDerivedAlias: bool
    MatchUnqualifiedColumns: bool
    OnSubquerySelect: SqlSelect -> SqlSelect
}

/// Exposed output identity survives substitution of a bare column by an expression.
let projectionAlias (projection: Projection) =
    match projection.Alias, projection.Expr with
    | Some alias, _ | None, Column(_, alias) -> Some alias
    | _ -> None

let buildProjectionAliasMap (innerCore: SelectCore) =
    innerCore.Projections |> ProjectionSetOps.toList
    |> List.choose (fun projection -> projectionAlias projection |> Option.map (fun name -> name, projection.Expr))
    |> Map.ofList

/// Generated source aliases can retain their original SQL quotation.
let normalizeSourceAlias (name: string) =
    if name.Length >= 2 && name.[0] = '"' && name.[name.Length - 1] = '"' then name.[1..name.Length - 2]
    else name

let private matchesDerivedAlias (policy: AliasRewritePolicy) (derivedAlias: string) (src: string) : bool =
    let source = normalizeSourceAlias src
    let target = normalizeSourceAlias derivedAlias
    source = target || (policy.MatchEmptyDerivedAlias && target = "" && source = "")

let private appendJsonPath (left: JsonPath) (right: JsonPath) : JsonPath =
    JsonPathOps.ofSegments (JsonPathOps.toList left @ JsonPathOps.toList right)

let private jsonPathAsLiteral (path: JsonPath) : SqlExpr =
    Literal(SqlLiteral.String(JsonPathOps.text(JsonPathOps.toList path)))

let rec private rewriteJsonExtractAliasTarget (path: JsonPath) (innerExpr: SqlExpr) : SqlExpr =
    match innerExpr with
    | Column(innerSrc, innerCol) ->
        JsonExtractExpr(innerSrc, innerCol, path)
    | JsonExtractExpr(innerSrc, innerCol, innerPath) ->
        JsonExtractExpr(innerSrc, innerCol, appendJsonPath innerPath path)
    | JsonRootExtract(innerSrc, innerCol) ->
        JsonExtractExpr(innerSrc, innerCol, path)
    | Cast(inner, sqlType) ->
        Cast(rewriteJsonExtractAliasTarget path inner, sqlType)
    | FunctionCall(name, args) ->
        FunctionCall("jsonb_extract", [FunctionCall(name, args); jsonPathAsLiteral path])
    | Literal _
    | Parameter _
    | JsonSetExpr _
    | JsonArrayExpr _
    | RowValueExpr _
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
    | CaseExpr _ ->
        failwithf "Alias rewrite cannot compose JsonExtract path %A onto inner expression %A" path innerExpr

let rewriteDerivedAliasExpr (policy: AliasRewritePolicy) (aliasMap: Map<string, SqlExpr>) (derivedAlias: string) (expr: SqlExpr) : SqlExpr =
    let matchesSource alias =
        match alias with
        | Some source -> matchesDerivedAlias policy derivedAlias source
        | None -> policy.MatchUnqualifiedColumns
    let rootInput = function
        | Column _ as column -> column
        | value -> FunctionCall("json_quote", [value])
    let rec rewrite node =
        match node with
        | Binary(Column(alias, column), BinaryOperator.JsonText, (Literal(SqlLiteral.String "$") as path)) when matchesSource alias ->
            match Map.tryFind column aliasMap with
            | Some value -> Binary(rootInput value, BinaryOperator.JsonText, path)
            | None -> node
        | _ ->
            let node = mapChildren rewrite id node
            match node with
            | Column(Some src, col) when matchesDerivedAlias policy derivedAlias src ->
                match Map.tryFind col aliasMap with
                | Some innerExpr -> innerExpr
                | None -> node
            | Column(None, col) when policy.MatchUnqualifiedColumns ->
                match Map.tryFind col aliasMap with
                | Some innerExpr -> innerExpr
                | None -> node
            | JsonExtractExpr(Some src, col, path) when matchesDerivedAlias policy derivedAlias src ->
                match Map.tryFind col aliasMap with
                | Some innerExpr -> rewriteJsonExtractAliasTarget path innerExpr
                | None -> node
            | JsonRootExtract(alias, col) when matchesSource alias ->
                match Map.tryFind col aliasMap with
                | Some(Column(innerSrc, innerCol)) -> JsonRootExtract(innerSrc, innerCol)
                | Some innerExpr ->
                    FunctionCall("jsonb_extract", [rootInput innerExpr; Literal(SqlLiteral.String "$")])
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
            | JsonRootExtract _
            | JsonSetExpr _
            | JsonArrayExpr _
            | RowValueExpr _
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
            | CaseExpr _ ->
                node
    rewrite expr

/// Substitute qualified captured references without crossing a local binding.
let rewriteScopedAliasExpr aliasMap capturedAlias expression =
    let capturedName = normalizeSourceAlias capturedAlias
    let boundName name = System.String.Equals(capturedName, name, System.StringComparison.OrdinalIgnoreCase)
    let binds = function
        | BaseTable(name, None) -> boundName name
        | BaseTable(_, Some alias) | DerivedTable(_, alias) | FromJsonEach(_, Some alias) -> boundName (normalizeSourceAlias alias)
        | FromJsonEach(_, None) -> boundName "json_each"
    let rec rewriteSelect (query: SqlSelect) =
        let core (original: SelectCore) =
            let shadows =
                Option.exists binds original.Source
                || List.exists (function CrossJoin source | ConditionedJoin(_, source, _) -> binds source) original.Joins
            if shadows then original else mapCoreExpressions rewriteExpression rewriteSelect original
        mapSelectParts (mapCores core) rewriteSelect query
    and rewriteExpression value =
        rewriteDerivedAliasExpr
            { MatchEmptyDerivedAlias = false; MatchUnqualifiedColumns = false; OnSubquerySelect = rewriteSelect }
            aliasMap capturedAlias value
    rewriteExpression expression
