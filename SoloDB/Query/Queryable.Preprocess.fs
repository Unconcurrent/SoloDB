namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

module internal QueryableHelperPreprocess =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperBase
    let internal preprocessQuery (expression: Expression) : PreprocessedQuery seq = seq {
        let mutable expression = expression

        while expression <> null do
            match expression with
            | :? MethodCallExpression as mce ->
                match parseSupportedMethod mce.Method.Name with
                | Some SupportedLinqMethods.SelectMany when mce.Arguments.Count = 3 ->
                    match mce.Arguments.[0] with
                    | :? MethodCallExpression as groupJoin when groupJoin.Method.Name = "GroupJoin" ->
                        expression <- groupJoin.Arguments.[0]
                        let exprs = [| groupJoin :> Expression; mce.Arguments.[1]; mce.Arguments.[2] |]
                        Method {| Value = SupportedLinqMethods.SelectMany; Expressions = exprs; OriginalMethod = mce.Method |}
                    | _ ->
                        expression <- mce.Arguments.[0]
                        let exprs = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                        Method {| Value = SupportedLinqMethods.SelectMany; Expressions = exprs; OriginalMethod = mce.Method |}
                | Some value ->
                    expression <- mce.Arguments.[0]
                    let exprs = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                    Method {| Value = value; Expressions = exprs; OriginalMethod = mce.Method |}
                | None ->
                    raise (NotSupportedException(
                        sprintf "Error: Queryable method '%s' is not supported.\nReason: The expression cannot be translated to SQL.\nFix: Call AsEnumerable() before this method or rewrite the query to a supported shape." mce.Method.Name))
            | :? ConstantExpression as ce when typeof<IRootQueryable>.IsAssignableFrom ce.Type ->
                RootQuery (ce.Value :?> IRootQueryable)
                expression <- null
            | e ->
                raise (NotSupportedException(
                    sprintf "Error: Cannot preprocess expression of type %A.\nReason: The expression shape is not supported for SQL translation.\nFix: Simplify the expression or switch to AsEnumerable() before this operation." e.NodeType))
    }

    let internal isExpressionLikeArgument (expr: Expression) =
        match expr with
        | :? LambdaExpression -> true
        | _ -> typeof<Expression>.IsAssignableFrom expr.Type

    let internal tryGetTerminalDefaultValueExpression (expression: Expression) =
        match expression with
        | :? MethodCallExpression as mce ->
            let args = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
            match mce.Method.Name, args with
            | ("FirstOrDefault" | "SingleOrDefault"), [| defaultValue |]
                when not (isExpressionLikeArgument defaultValue) ->
                Some defaultValue
            | ("FirstOrDefault" | "SingleOrDefault"), [| predicate; defaultValue |]
                when isExpressionLikeArgument predicate ->
                Some defaultValue
            | _ ->
                None
        | _ ->
            None

    let internal tryExtractExcludePath (expressions: Expression array) =
        if expressions.Length < 1 then
            None
        else
            let selectorExprOpt =
                try
                    let arg = expressions.[0]
                    let lambdaExpr =
                        if arg.NodeType = ExpressionType.Quote then (arg :?> UnaryExpression).Operand :?> LambdaExpression
                        else arg :?> LambdaExpression
                    Some lambdaExpr
                with _ ->
                    None

            match selectorExprOpt with
            | None -> None
            | Some selectorExpr ->
                let rec extractPath (e: Expression) =
                    match e with
                    | :? MemberExpression as me ->
                        let parent = extractPath me.Expression
                        if parent = "" then me.Member.Name else parent + "." + me.Member.Name
                    | :? ParameterExpression -> ""
                    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> extractPath ue.Operand
                    | _ -> ""

                match extractPath selectorExpr.Body with
                | "" -> None
                | path -> Some path

    let internal extractRelationPathOrThrow (directiveName: string) (expressions: Expression array) =
        match tryExtractExcludePath expressions with
        | Some path -> path
        | None ->
            raise (NotSupportedException(
                $"Error: {directiveName} selector is not supported.\nReason: Only direct member-path selectors are supported for relation directives.\nFix: Use a selector like x => x.Ref or x => x.RefMany (member path only)."))

    let internal registerExcludePath (sourceCtx: QueryContext) path =
        if not (String.IsNullOrWhiteSpace path) then
            sourceCtx.ExcludedPaths.Add(path) |> ignore

    let internal registerIncludePath (sourceCtx: QueryContext) path =
        if not (String.IsNullOrWhiteSpace path) then
            sourceCtx.IncludedPaths.Add(path) |> ignore

    let internal validateIncludeExcludeConflicts (ctx: QueryContext) =
        for includedPath in ctx.IncludedPaths do
            // C-01: exact-match conflict
            if ctx.ExcludedPaths.Contains(includedPath) then
                raise (InvalidOperationException(
                    $"Error: Path '{includedPath}' is both included and excluded.\nReason: Include/Exclude conflict on same relation path.\nFix: Keep only one directive for this path."))
            // C-03: parent excluded, child included
            for excludedPath in ctx.ExcludedPaths do
                if includedPath.StartsWith(excludedPath + ".") then
                    raise (InvalidOperationException(
                        $"Error: Path '{includedPath}' is included but parent '{excludedPath}' is excluded.\nReason: Cannot include a child path under an excluded parent.\nFix: Remove the Exclude on '{excludedPath}' or remove the Include on '{includedPath}'."))

    let internal shouldLoadRelationPath (sourceCtx: QueryContext) (path: string) =
        if sourceCtx.ExcludedPaths.Contains(path) then false
        elif sourceCtx.WhitelistMode then sourceCtx.IncludedPaths.Contains(path)
        else true

    let internal cloneQueryContext (sourceCtx: QueryContext) =
        let typeCollections = Dictionary(System.StringComparer.Ordinal)
        for kv in sourceCtx.TypeCollections do
            typeCollections.[kv.Key] <- HashSet<string>(kv.Value, System.StringComparer.Ordinal)
        {
            RootTable = sourceCtx.RootTable
            LayerPosition = OuterLayer
            RootGraph =
                {
                    Roots = ResizeArray(sourceCtx.RootGraph.Roots |> Seq.map id)
                    SourceAliasCounter = sourceCtx.RootGraph.SourceAliasCounter
                }
            Joins = ResizeArray()
            AliasCounter = ref 0
            ExcludedPaths = HashSet(sourceCtx.ExcludedPaths, System.StringComparer.Ordinal)
            IncludedPaths = HashSet(sourceCtx.IncludedPaths, System.StringComparer.Ordinal)
            WhitelistMode = sourceCtx.WhitelistMode
            MaterializedPaths = HashSet(sourceCtx.MaterializedPaths, System.StringComparer.Ordinal)
            RelationTargets = Dictionary(sourceCtx.RelationTargets, System.StringComparer.Ordinal)
            RelationLinks = Dictionary(sourceCtx.RelationLinks, System.StringComparer.Ordinal)
            RelationOwnerUsesSource = Dictionary(sourceCtx.RelationOwnerUsesSource, System.StringComparer.Ordinal)
            TypeCollections = typeCollections
        }

    let internal serializeForCollection (value: 'T) =
        struct (
            match typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T> with
            | true -> JsonSerializator.JsonValue.SerializeWithType value
            | false -> JsonSerializator.JsonValue.Serialize value
            |> _.ToJsonString(), HasTypeId<'T>.Value
        )

    /// Build WHERE, ORDER BY, LIMIT, OFFSET, UnionAll from a UsedSQLStatements layer as DU values.
    /// Returns (where, orderBy, limit, offset, unionAlls).
    let internal buildClausesDu
        (sourceCtx: QueryContext)
        (vars: Dictionary<string, obj>)
        (statement: UsedSQLStatements)
        (contextTable: string) =

        let where =
            if statement.Filters.Count = 0 then None
            else
                let exprs =
                    statement.Filters
                    |> Seq.map (fun f -> translateExprDu sourceCtx contextTable f vars)
                    |> Seq.toList
                match exprs with
                | [single] -> Some single
                | multiple -> Some (multiple |> List.reduce (fun a b -> SqlExpr.Binary(a, BinaryOperator.And, b)))

        let unionAlls =
            statement.UnionAll |> Seq.map (fun buildFn -> buildFn contextTable vars) |> Seq.toList

        let orderBy =
            statement.Orders |> Seq.map (fun o ->
                let expr =
                    match o.RawExpr with
                    | Some duExpr -> duExpr
                    | None -> translateExprDu sourceCtx contextTable o.OrderingRule vars
                { Expr = expr; Direction = if o.Descending then SortDirection.Desc else SortDirection.Asc }
            ) |> Seq.toList

        let limit =
            match statement.Take with
            | Some take -> Some (allocateParam vars (QueryTranslator.evaluateExpr<obj> take))
            | None ->
                match statement.Skip with
                | Some _ -> Some (SqlExpr.Literal(SqlLiteral.Integer -1L))
                | None -> None

        let offset =
            match statement.Skip with
            | Some skip -> Some (allocateParam vars (QueryTranslator.evaluateExpr<obj> skip))
            | None -> None

        struct (where, orderBy, limit, offset, unionAlls)

    /// Parse a JoinKind string from the handler mechanism into a DU JoinKind.
    let internal parseJoinKind (kind: string) =
        match kind.Trim().ToUpperInvariant() with
        | s when s.Contains("LEFT") -> JoinKind.Left
        | s when s.Contains("INNER") -> JoinKind.Inner
        | s when s.Contains("CROSS") ->
            raise (InvalidOperationException("CROSS JOIN must use the CrossJoin DU constructor directly."))
        | other ->
            raise (NotSupportedException(sprintf "Error: Unrecognized join kind '%s'.\nFix: Use LEFT, INNER, or CROSS join." other))

    /// Build materialized Value projection (jsonb_set with JOIN data) for relation materialization.
    let internal buildMaterializedValueExpr (ctx: QueryContext) (effectiveTableName: string) (valueColumnExpr: SqlExpr) =
        let materializedJoins =
            ctx.Joins
            |> Seq.filter (fun j -> shouldLoadRelationPath ctx j.PropertyPath)
            |> Seq.toList
        if materializedJoins.IsEmpty then
            // All joins are excluded — no materialization needed, keep original qualified Value
            valueColumnExpr
        else
            // jsonb_set("Table".Value, '$.Prop1', CASE WHEN a1.Id IS NOT NULL THEN jsonb_array(a1.Id, a1.Value) ELSE jsonb_extract("Table".Value, '$.Prop1') END, ...)
            let args = ResizeArray<SqlExpr>()
            args.Add(SqlExpr.Column(Some effectiveTableName, "Value"))
            for j in materializedJoins do
                ctx.MaterializedPaths.Add(j.PropertyPath) |> ignore
                args.Add(SqlExpr.Literal(SqlLiteral.String ("$." + j.PropertyPath)))
                args.Add(SqlExpr.CaseExpr(
                    (SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some j.TargetAlias, "Id")),
                     SqlExpr.FunctionCall("jsonb_array", [SqlExpr.Column(Some j.TargetAlias, "Id"); SqlExpr.Column(Some j.TargetAlias, "Value")])),
                    [],
                    Some (SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(Some effectiveTableName, "Value"); SqlExpr.Literal(SqlLiteral.String ("$." + j.PropertyPath))]))))
            SqlExpr.FunctionCall("jsonb_set", args |> Seq.toList)

    /// Wrap a SelectBody with optional ORDER BY, LIMIT, OFFSET.
    /// For SingleSelect, these are already on the core (identity pass-through).
    /// For UnionAllSelect, wrapping is needed if ordering/limit is present.
    /// Strip source aliases from SqlExpr so column references become unqualified.
    /// Required when ORDER BY expressions translated against a base table (e.g. "Int32")
    /// are lifted to an outer derived-table wrapper where those aliases don't exist.
    let rec internal stripSourceAlias (expr: SqlExpr) : SqlExpr =
        match expr with
        | SqlExpr.Column(Some _, col) -> SqlExpr.Column(None, col)
        | SqlExpr.Column(None, _) -> expr
        | SqlExpr.Literal _ -> expr
        | SqlExpr.Parameter _ -> expr
        | SqlExpr.JsonExtractExpr(Some _, col, path) -> SqlExpr.JsonExtractExpr(None, col, path)
        | SqlExpr.JsonExtractExpr(None, _, _) -> expr
        | SqlExpr.JsonRootExtract(Some _, col) -> SqlExpr.JsonRootExtract(None, col)
        | SqlExpr.JsonRootExtract(None, _) -> expr
        | SqlExpr.JsonSetExpr(target, assignments) ->
            SqlExpr.JsonSetExpr(
                stripSourceAlias target,
                assignments |> List.map (fun (path, value) -> path, stripSourceAlias value))
        | SqlExpr.JsonArrayExpr elements ->
            SqlExpr.JsonArrayExpr(elements |> List.map stripSourceAlias)
        | SqlExpr.JsonObjectExpr properties ->
            SqlExpr.JsonObjectExpr(properties |> List.map (fun (name, value) -> name, stripSourceAlias value))
        | SqlExpr.FunctionCall(name, args) ->
            SqlExpr.FunctionCall(name, args |> List.map stripSourceAlias)
        | SqlExpr.AggregateCall(kind, argument, distinct, separator) ->
            SqlExpr.AggregateCall(kind, argument |> Option.map stripSourceAlias, distinct, separator |> Option.map stripSourceAlias)
        | SqlExpr.WindowCall spec ->
            SqlExpr.WindowCall {
                spec with
                    Arguments = spec.Arguments |> List.map stripSourceAlias
                    PartitionBy = spec.PartitionBy |> List.map stripSourceAlias
                    OrderBy = spec.OrderBy |> List.map (fun (e, d) -> stripSourceAlias e, d)
            }
        | SqlExpr.Unary(op, inner) -> SqlExpr.Unary(op, stripSourceAlias inner)
        | SqlExpr.Binary(l, op, r) -> SqlExpr.Binary(stripSourceAlias l, op, stripSourceAlias r)
        | SqlExpr.Between(e, lower, upper) -> SqlExpr.Between(stripSourceAlias e, stripSourceAlias lower, stripSourceAlias upper)
        | SqlExpr.InList(e, head, tail) -> SqlExpr.InList(stripSourceAlias e, stripSourceAlias head, tail |> List.map stripSourceAlias)
        | SqlExpr.InSubquery(e, subquery) -> SqlExpr.InSubquery(stripSourceAlias e, stripSourceAliasInSelect subquery)
        | SqlExpr.Cast(inner, ty) -> SqlExpr.Cast(stripSourceAlias inner, ty)
        | SqlExpr.Coalesce(head, tail) -> SqlExpr.Coalesce(stripSourceAlias head, tail |> List.map stripSourceAlias)
        | SqlExpr.Exists subquery -> SqlExpr.Exists(stripSourceAliasInSelect subquery)
        | SqlExpr.ScalarSubquery subquery -> SqlExpr.ScalarSubquery(stripSourceAliasInSelect subquery)
        | SqlExpr.CaseExpr(firstBranch, restBranches, elseExpr) ->
            SqlExpr.CaseExpr(
                let mapBranch (w, t) = stripSourceAlias w, stripSourceAlias t
                mapBranch firstBranch,
                restBranches |> List.map mapBranch,
                elseExpr |> Option.map stripSourceAlias)
        | SqlExpr.UpdateFragment(path, value) -> SqlExpr.UpdateFragment(stripSourceAlias path, stripSourceAlias value)

    and internal stripSourceAliasInTableSource (source: TableSource) : TableSource =
        match source with
        | BaseTable _ -> source
        | DerivedTable(query, alias) -> DerivedTable(stripSourceAliasInSelect query, alias)
        | FromJsonEach(valueExpr, alias) -> FromJsonEach(stripSourceAlias valueExpr, alias)

    and internal stripSourceAliasInJoin (join: JoinShape) : JoinShape =
        match join with
        | CrossJoin source ->
            CrossJoin(stripSourceAliasInTableSource source)
        | ConditionedJoin(kind, source, onExpr) ->
            ConditionedJoin(kind, stripSourceAliasInTableSource source, stripSourceAlias onExpr)

    and internal stripSourceAliasInProjection (projection: Projection) : Projection =
        { projection with Expr = stripSourceAlias projection.Expr }

    and internal stripSourceAliasInOrderBy (orderBy: OrderBy) : OrderBy =
        { orderBy with Expr = stripSourceAlias orderBy.Expr }

    and internal stripSourceAliasInCore (core: SelectCore) : SelectCore =
        {
            core with
                Source = core.Source |> Option.map stripSourceAliasInTableSource
                Joins = core.Joins |> List.map stripSourceAliasInJoin
                Projections = core.Projections |> ProjectionSetOps.map stripSourceAliasInProjection
                Where = core.Where |> Option.map stripSourceAlias
                GroupBy = core.GroupBy |> List.map stripSourceAlias
                Having = core.Having |> Option.map stripSourceAlias
                OrderBy = core.OrderBy |> List.map stripSourceAliasInOrderBy
                Limit = core.Limit |> Option.map stripSourceAlias
                Offset = core.Offset |> Option.map stripSourceAlias
        }

    and internal stripSourceAliasInSelect (select: SqlSelect) : SqlSelect =
        {
            select with
                Ctes = select.Ctes |> List.map (fun cte -> { cte with Query = stripSourceAliasInSelect cte.Query })
                Body =
                    match select.Body with
                    | SingleSelect core -> SingleSelect(stripSourceAliasInCore core)
                    | UnionAllSelect(head, tail) -> UnionAllSelect(stripSourceAliasInCore head, tail |> List.map stripSourceAliasInCore)
        }

    let internal wrapCoreBody (body: SelectBody) (orderBy: OrderBy list) (limit: SqlExpr option) (offset: SqlExpr option) : SqlSelect =
        match body with
        | SingleSelect core ->
            { Ctes = []; Body = SingleSelect core }
        | UnionAllSelect(head, tail) ->
            if orderBy.IsEmpty && limit.IsNone && offset.IsNone then
                { Ctes = []; Body = UnionAllSelect(head, tail) }
            else
                // Wrap the UNION ALL in a derived table to apply ORDER BY / LIMIT.
                // Strip source aliases from ORDER BY: expressions were translated against
                // the base table (e.g. "Int32") but the outer wrapper's columns are unqualified.
                let strippedOrderBy = orderBy |> List.map (fun o -> { o with Expr = stripSourceAlias o.Expr })
                let innerSelect = { Ctes = []; Body = UnionAllSelect(head, tail) }
                let outerCore =
                    { mkCore
                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                         { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                        (Some (DerivedTable(innerSelect, "o")))
                      with OrderBy = strippedOrderBy; Limit = limit; Offset = offset }
                { Ctes = []; Body = SingleSelect outerCore }

    /// Build SqlSelect from collected layers (DU construction path — no StringBuilder).
