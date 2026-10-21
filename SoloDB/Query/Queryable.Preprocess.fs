namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open NativeSQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.SqlModel

module internal QueryableHelperPreprocess =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperBase

    let private normalizeOrderKeyExpr (expr: SqlExpr) (clrType: Type) =
        DateTimeFunctions.canonicalizeForCompareOrOrder clrType expr
    let internal preprocessQuery (expression: Expression) : PreprocessedQuery array =
        let queries = ResizeArray<PreprocessedQuery>()
        let mutable expression = expression

        while expression <> null do
            match expression with
            | :? RelationQueryExpression as directive ->
                queries.Add(Relation directive)
                expression <- directive.Source
            | :? UnaryExpression as conversion when QueryTranslatorBaseHelpers.isReferenceUpcast conversion &&
                typeof<IQueryable>.IsAssignableFrom(conversion.Type) ->
                // F# inserts the ordered-query upcast when supplying a query to a terminal operator.
                expression <- conversion.Operand
            | :? MethodCallExpression as mce ->
                match parseSupportedMethod mce.Method.Name with
                | Some SupportedLinqMethods.SelectMany when mce.Arguments.Count = 3 ->
                    match mce.Arguments.[0] with
                    | :? MethodCallExpression as groupJoin when groupJoin.Method.Name = "GroupJoin" ->
                        expression <- groupJoin.Arguments.[0]
                        let exprs = [| groupJoin :> Expression; mce.Arguments.[1]; mce.Arguments.[2] |]
                        queries.Add(Method {| Value = SupportedLinqMethods.SelectMany; Expressions = exprs; OriginalMethod = mce.Method |})
                    | _ ->
                        expression <- mce.Arguments.[0]
                        let exprs = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                        queries.Add(Method {| Value = SupportedLinqMethods.SelectMany; Expressions = exprs; OriginalMethod = mce.Method |})
                | Some value ->
                    expression <- mce.Arguments.[0]
                    let exprs = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                    queries.Add(Method {| Value = value; Expressions = exprs; OriginalMethod = mce.Method |})
                | None ->
                    raise (NotSupportedException(
                        sprintf "Error: Queryable method '%s' is not supported.\nReason: The expression cannot be translated to SQL.\nFix: Call AsEnumerable() before this method or rewrite the query to a supported shape." mce.Method.Name))
            | :? ConstantExpression as ce when typeof<IRootQueryable>.IsAssignableFrom ce.Type ->
                queries.Add(RootQuery (ce.Value :?> IRootQueryable))
                expression <- null
            | e ->
                raise (NotSupportedException(
                    sprintf "Error: Cannot preprocess expression of type %A.\nReason: The expression shape is not supported for SQL translation.\nFix: Simplify the expression or switch to AsEnumerable() before this operation." e.NodeType))
        let ordered = queries.ToArray()
        Array.Reverse(ordered)
        ordered

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
            // Shared so a lookup made in the cloned layer is cached for the whole translation.
            MetadataSource = sourceCtx.MetadataSource
            BindQueryValue = sourceCtx.BindQueryValue
            GeneralPrefixMatch = sourceCtx.GeneralPrefixMatch
            LayerPosition = OuterLayer
            SerializationInput = sourceCtx.SerializationInput
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
            IsAtTopLevel = sourceCtx.IsAtTopLevel
        }

    let internal serializeForCollection (valueType: Type) (value: obj) =
        let includeType = typeof<JsonSerializator.JsonValue>.IsAssignableFrom valueType
        if includeType then value.GetType() |> ignore
        let text = JsonSerializator.JsonValue.WriteRuntimeBuffer(valueType,value,includeType,false)
        System.String(text.Array,text.Offset,text.Count)

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
                    |> Seq.map (fun f -> translateExprDuForPredicate sourceCtx contextTable f vars)
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
                { Expr = normalizeOrderKeyExpr expr (orderKeyClrType o.OrderingRule); Direction = o.Direction }
            ) |> Seq.toList

        let bindBound bound =
            match sourceCtx.BindQueryValue with
            | ValueSome binder when binder.IsValue bound -> binder.Scalar bound
            | _ -> allocateParam vars (ExpressionEvaluation.evaluateExpr<obj> bound)

        let limit =
            match statement.Take with
            | Some take -> Some (QueryTranslatorBaseHelpers.nonnegativeCount (bindBound take))
            | None ->
                match statement.Skip with
                | Some _ -> Some (SqlExpr.Literal(SqlLiteral.Integer -1L))
                | None -> None

        let offset =
            match statement.Skip with
            | Some skip -> Some (bindBound skip)
            | None -> None

        struct (where, orderBy, limit, offset, unionAlls)

    /// Build materialized Value projection (jsonb_set with JOIN data) for relation materialization.
    let internal buildMaterializedValueExpr (ctx: QueryContext) (effectiveTableName: string) (valueColumnExpr: SqlExpr) =
        let materializedJoins =
            ctx.Joins
            |> Seq.filter (fun j ->
                let ownSource = j.OnSourceAlias |> Option.forall (fun alias -> AliasRewrite.normalizeSourceAlias alias = AliasRewrite.normalizeSourceAlias effectiveTableName)
                shouldLoadRelationPath ctx j.PropertyPath && j.ReferenceOnOwnerRoot && ownSource)
            |> Seq.toList
        if materializedJoins.IsEmpty then
            // This root has no admitted materialized reference; retain its original value.
            valueColumnExpr
        else
            // jsonb_set("Table".Value, '$.Prop1', CASE WHEN a1.Id IS NOT NULL THEN jsonb_array(a1.Id, a1.Value) ELSE jsonb_extract("Table".Value, '$.Prop1') END, ...)
            let args = ResizeArray<SqlExpr>()
            args.Add(SqlExpr.Column(Some effectiveTableName, "Value"))
            for j in materializedJoins do
                ctx.MaterializedPaths.Add(j.PropertyPath) |> ignore
                args.Add(SqlExpr.Literal(SqlLiteral.String (JsonPathOps.text j.ReferencePath)))
                args.Add(SqlExpr.CaseExpr(
                    (SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some j.TargetAlias, "Id")),
                     SqlExpr.FunctionCall("jsonb_array", [SqlExpr.Column(Some j.TargetAlias, "Id"); j.TargetValue])),
                    [],
                    Some (SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(Some effectiveTableName, "Value"); SqlExpr.Literal(SqlLiteral.String (JsonPathOps.text j.ReferencePath))]))))
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
        | SqlExpr.JsonExtractExpr(Some _, col, path) -> SqlExpr.JsonExtractExpr(None, col, path)
        | SqlExpr.JsonRootExtract(Some _, col) -> SqlExpr.JsonRootExtract(None, col)
        | _ -> mapChildren stripSourceAlias stripSourceAliasInSelect expr

    and private stripSourceAliasInSelect (select: SqlSelect) : SqlSelect =
        mapSelectParts
            (mapCores (mapCoreExpressions stripSourceAlias stripSourceAliasInSelect))
            stripSourceAliasInSelect select

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
