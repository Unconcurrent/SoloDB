namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open SQLiteTools
open Utils
open SoloDatabase
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryableGroupByAliases
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryableBuildQueryGroupJoinChain
open SoloDatabase.QueryableBuildQueryGroupJoinElements
open SoloDatabase.DBRefManyDescriptor

/// GroupJoin handler — orchestration only. Chain lowering and element handling live in dedicated files.
module internal QueryableBuildQueryGroupJoinOps =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableHelperBase

    let internal applyGroupJoin<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> SqlSelect)
        (expressions: Expression array) =
        match expressions.Length with
        | 4 ->
            let innerExpression = expressions.[0]
            let outerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin outer key selector" expressions.[1]
            let innerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin inner key selector" expressions.[2]
            let resultSelector = unwrapLambdaExpressionOrThrow "GroupJoin result selector" expressions.[3]

            if isCompositeJoinKeyBody outerKeySelector.Body || isCompositeJoinKeyBody innerKeySelector.Body then
                raise (NotSupportedException(
                    "Error: GroupJoin composite key selectors are not supported.\n" +
                    "Reason: Anonymous-type and composite key equality lowering is not available.\n" +
                    "Fix: Join on a single scalar key or move the query after AsEnumerable()."))

            let innerRootTable =
                match tryGetJoinRootSourceTable innerExpression with
                | Some tn -> tn
                | None ->
                    raise (NotSupportedException(
                        "Error: GroupJoin inner source is not supported.\n" +
                        "Reason: The inner query does not resolve to a SoloDB root collection.\n" +
                        "Fix: Use another SoloDB IQueryable rooted in a collection or move the query after AsEnumerable()."))

            if resultSelector.Parameters.Count <> 2 then
                raise (NotSupportedException(
                    "Error: GroupJoin result selector must have two parameters (outer, group).\n" +
                    "Reason: The result selector shape is not recognized.\n" +
                    "Fix: Use (outer, group) => new { ... } pattern."))

            let outerParam = resultSelector.Parameters.[0]
            let groupParam = resultSelector.Parameters.[1]
            let capturedInnerExpr = innerExpression

            addComplexFinal statements (fun ctx ->
                let outerAlias = "o"
                let innerAlias = "gj"
                let innerCtx =
                    { sourceCtx with
                        RootTable = innerRootTable
                        RootGraph = QueryRootGraph.Single(innerRootTable)
                        Joins = ResizeArray() }
                let innerSelect = translateQueryFn innerCtx ctx.Vars capturedInnerExpr
                let innerSource = DerivedTable(innerSelect, innerAlias)

                let materializeDiscoveredJoins
                    (joins: ResizeArray<JoinEdge>)
                    (materializedRootAlias: string option)
                    (materializedRootPaths: Collections.Generic.HashSet<string> option) =
                    joins
                    |> Seq.map (fun j ->
                        let onExpr =
                            match materializedRootAlias, materializedRootPaths, j.OnSourceAlias with
                            | Some rootAlias, Some paths, Some sourceAlias
                                when sourceAlias = rootAlias && paths.Contains(j.PropertyPath) ->
                                SqlExpr.FunctionCall(
                                    "jsonb_extract",
                                    [ SqlExpr.Column(j.OnSourceAlias, "Value")
                                      SqlExpr.Literal(SqlLiteral.String($"$.{j.OnPropertyName}[0]")) ])
                            | _ ->
                                SqlExpr.JsonExtractExpr(j.OnSourceAlias, "Value", JsonPath(j.OnPropertyName, []))
                        ConditionedJoin(
                            parseJoinKind j.JoinKind,
                            BaseTable(j.TargetTable, Some j.TargetAlias),
                            SqlExpr.Binary(
                                SqlExpr.Column(Some j.TargetAlias, "Id"),
                                BinaryOperator.Eq,
                                onExpr)))
                    |> Seq.toList

                let outerCtx =
                    { sourceCtx with
                        Joins = ResizeArray() }

                let innerAggCtx =
                    { innerCtx with
                        Joins = ResizeArray() }

                let rec stripConvert (expr: Expression) =
                    match expr with
                    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> stripConvert ue.Operand
                    | _ -> expr

                let tryTranslateDbRefValueIdKey (parameter: ParameterExpression) (tableAlias: string) (expr: Expression) =
                    match stripConvert expr with
                    | :? MemberExpression as idMe when idMe.Member.Name = "Id" ->
                        match stripConvert idMe.Expression with
                        | :? MemberExpression as valueMe when valueMe.Member.Name = "Value" ->
                            match stripConvert valueMe.Expression with
                            | :? MemberExpression as relMe when Object.ReferenceEquals(stripConvert relMe.Expression, parameter) ->
                                Some (
                                    SqlExpr.FunctionCall(
                                        "jsonb_extract",
                                        [ SqlExpr.Column(Some tableAlias, "Value")
                                          SqlExpr.Literal(SqlLiteral.String($"$.{relMe.Member.Name}")) ]))
                            | _ -> None
                        | _ -> None
                    | _ -> None

                let outerKeyExpr =
                    match tryTranslateDbRefValueIdKey outerKeySelector.Parameters.[0] outerAlias outerKeySelector.Body with
                    | Some translated -> translated
                    | None -> translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body

                let innerJoinKeyExpr =
                    let innerKeyDirectCtx =
                        { innerCtx with
                            Joins = ResizeArray() }
                    let directExpr =
                        match tryTranslateDbRefValueIdKey innerKeySelector.Parameters.[0] innerAlias innerKeySelector.Body with
                        | Some translated -> translated
                        | None -> translateJoinSingleSourceExpression innerKeyDirectCtx innerAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body
                    if innerKeyDirectCtx.Joins.Count = 0 then
                        directExpr
                    else
                        let innerKeyCtx =
                            { innerCtx with
                                Joins = ResizeArray() }
                        let innerKeySourceAlias = sprintf "gjk%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                        let innerMaterializedPaths =
                            if innerCtx.MaterializedPaths.Count > 0 then Some innerCtx.MaterializedPaths else None
                        let correlatedExpr =
                            match tryTranslateDbRefValueIdKey innerKeySelector.Parameters.[0] innerKeySourceAlias innerKeySelector.Body with
                            | Some translated -> translated
                            | None -> translateJoinSingleSourceExpression innerKeyCtx innerKeySourceAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body
                        let keyCore =
                            { mkCore [{ Alias = None; Expr = correlatedExpr }] (Some (DerivedTable(innerSelect, innerKeySourceAlias)))
                                with
                                    Joins = materializeDiscoveredJoins innerKeyCtx.Joins (Some ("\"" + innerKeySourceAlias + "\"")) innerMaterializedPaths
                                    Where = Some (SqlExpr.Binary(SqlExpr.Column(Some innerKeySourceAlias, "Id"), BinaryOperator.Eq, SqlExpr.Column(Some innerAlias, "Id")))
                                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
                        SqlExpr.ScalarSubquery (wrapCore keyCore)

                let replaceExpression (target: Expression) (replacement: Expression) (expr: Expression) =
                    let visitor =
                        { new ExpressionVisitor() with
                            override _.Visit(node: Expression) =
                                if isNull node then null
                                elif Object.ReferenceEquals(node, target) then replacement
                                else base.Visit(node) }
                    visitor.Visit(expr)

                let translateOuterExpr (expr: Expression) =
                    translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars (Some outerParam) expr

                let runtime =
                    { InnerCtx = innerCtx
                      InnerRootTable = innerRootTable
                      InnerSelect = innerSelect
                      OuterAlias = outerAlias
                      OuterParam = outerParam
                      GroupParam = groupParam
                      OuterKeyExpr = outerKeyExpr
                      InnerKeySelector = innerKeySelector
                      Vars = ctx.Vars
                      TranslateJoinExpr = translateJoinSingleSourceExpression
                      MaterializeDiscoveredJoins = materializeDiscoveredJoins
                      TryTranslateDbRefValueIdKey = tryTranslateDbRefValueIdKey
                      ReplaceExpression = replaceExpression
                      ErrorExpr = fun message -> SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String($"__solodb_error__:{message}"))])
                      TranslateOuterExpr = translateOuterExpr }

                let rec translateGroupJoinArg (expr: Expression) : GroupJoinTranslatedArg =
                    let combineTranslated (parts: GroupJoinTranslatedArg list) (mkValue: SqlExpr list -> SqlExpr) =
                        { Value = mkValue (parts |> List.map (fun p -> p.Value))
                          Error = combineErrorExprs (parts |> List.map (fun p -> p.Error)) }

                    let parseFormatPieces (format: string) =
                        let pieces = ResizeArray<Choice<string, int>>()
                        let sb = System.Text.StringBuilder()
                        let flushLiteral () =
                            if sb.Length > 0 then
                                pieces.Add(Choice1Of2(sb.ToString()))
                                sb.Clear() |> ignore
                        let mutable i = 0
                        while i < format.Length do
                            match format.[i] with
                            | '{' when i + 1 < format.Length && format.[i + 1] = '{' ->
                                sb.Append('{') |> ignore
                                i <- i + 2
                            | '}' when i + 1 < format.Length && format.[i + 1] = '}' ->
                                sb.Append('}') |> ignore
                                i <- i + 2
                            | '{' ->
                                let close = format.IndexOf('}', i + 1)
                                if close < 0 then
                                    raise (NotSupportedException("Error: GroupJoin string format is malformed.\nFix: Use a valid composite format string."))
                                flushLiteral ()
                                let placeholder = format.Substring(i + 1, close - i - 1)
                                let commaIdx = placeholder.IndexOf(',')
                                let colonIdx = placeholder.IndexOf(':')
                                let endIdx =
                                    [ commaIdx; colonIdx ]
                                    |> List.filter (fun x -> x >= 0)
                                    |> function
                                        | [] -> placeholder.Length
                                        | xs -> List.min xs
                                let indexText = placeholder.Substring(0, endIdx).Trim()
                                let index =
                                    match Int32.TryParse(indexText) with
                                    | true, value -> value
                                    | _ ->
                                        raise (NotSupportedException(
                                            "Error: GroupJoin string format placeholder is not supported.\n" +
                                            "Reason: Only numeric placeholders like {0} are supported.\n" +
                                            "Fix: Use string interpolation without alignment or custom format specifiers."))
                                pieces.Add(Choice2Of2 index)
                                i <- close + 1
                            | ch ->
                                sb.Append(ch) |> ignore
                                i <- i + 1
                        flushLiteral ()
                        pieces |> Seq.toList

                    let rec translateScalarMethodCall (mc: MethodCallExpression) : GroupJoinTranslatedArg option =
                        let rec getInterpolationArgType (expr: Expression) =
                            match expr with
                            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked || ue.NodeType = ExpressionType.TypeAs ->
                                getInterpolationArgType ue.Operand
                            | _ -> expr.Type
                        let normalizeInterpolatedArg (originalExpr: Expression) (translated: GroupJoinTranslatedArg) =
                            if isDecimalOrNullableDecimal (getInterpolationArgType originalExpr) then
                                { translated with Value = SqlExpr.FunctionCall("DECIMAL_TEXT", [translated.Value]) }
                            else
                                translated
                        if mc.Method.DeclaringType = typeof<string> && mc.Method.Name = "Concat" then
                            let args =
                                if mc.Arguments.Count = 1
                                   && mc.Arguments.[0] :? NewArrayExpression then
                                    (mc.Arguments.[0] :?> NewArrayExpression).Expressions |> Seq.toList
                                else
                                    mc.Arguments |> Seq.toList
                            let parts = args |> List.map (fun arg -> translateGroupJoinArg arg |> normalizeInterpolatedArg arg)
                            Some (combineTranslated parts (fun values -> SqlExpr.FunctionCall("CONCAT", values)))
                        elif mc.Method.DeclaringType = typeof<string>
                             && mc.Method.Name = "Format"
                             && mc.Arguments.Count >= 2
                             && mc.Arguments.[0] :? ConstantExpression then
                            let fmt = (mc.Arguments.[0] :?> ConstantExpression).Value :?> string
                            let rawArgs =
                                if mc.Arguments.Count = 2
                                   && mc.Arguments.[1] :? NewArrayExpression then
                                    (mc.Arguments.[1] :?> NewArrayExpression).Expressions |> Seq.toList
                                else
                                    mc.Arguments |> Seq.skip 1 |> Seq.toList
                            let translatedArgs = rawArgs |> List.map (fun arg -> translateGroupJoinArg arg |> normalizeInterpolatedArg arg)
                            let translated =
                                parseFormatPieces fmt
                                |> List.map (function
                                    | Choice1Of2 literal -> translatedArg (SqlExpr.Literal(SqlLiteral.String literal))
                                    | Choice2Of2 index when index >= 0 && index < translatedArgs.Length -> translatedArgs.[index]
                                    | Choice2Of2 _ ->
                                        raise (NotSupportedException(
                                            "Error: GroupJoin string format index is out of range.\n" +
                                            "Fix: Ensure each placeholder refers to an existing interpolation argument.")))
                            Some (combineTranslated translated (fun values -> SqlExpr.FunctionCall("CONCAT", values)))
                        elif mc.Method.Name = "ToString" && mc.Arguments.Count = 0 && not (isNull mc.Object) then
                            Some (translateGroupJoinArg mc.Object)
                        else
                            None

                    if not (referencesParam groupParam expr) then
                        translatedArg (translateOuterExpr expr)
                    else
                    match expr with
                    | :? MemberExpression as me when not (isNull me.Expression) && me.Expression :? ParameterExpression && (me.Expression :?> ParameterExpression) = outerParam ->
                        translatedArg (translateOuterExpr expr)
                    | :? MemberExpression as me when not (isNull me.Expression) ->
                        match tryMatchGroupElementCall runtime me.Expression with
                        | Some groupCall ->
                            buildGroupElementDispatch runtime groupCall expr
                        | None ->
                            if referencesParam groupParam me.Expression then
                                raise (NotSupportedException(
                                    "Error: GroupJoin group element access with unsupported chain operators is not supported.\n" +
                                    "Reason: The group chain uses operators that cannot be translated in GroupJoin context.\n" +
                                    "Fix: Simplify the group chain or move the query after AsEnumerable()."))
                            else
                                translatedArg (translateOuterExpr expr)
                    | :? MethodCallExpression as mc ->
                        // PRIMARY PATH: Terminal DU dispatch via shared QueryDescriptor extraction
                        match tryExtractGroupTerminalChain runtime expr with
                        | Some (qdesc, terminal) ->
                            let toLambda (e: Expression) = unwrapLambdaExpressionOrThrow "GroupJoin terminal argument" e
                            let hasOps = hasQueryDescriptorChainOps qdesc
                            match terminal with
                            // Aggregate terminals — use Q version for full QueryDescriptor support (TakeWhile etc.)
                            | Terminal.Count | Terminal.LongCount ->
                                if hasOps then
                                    translatedArg (SqlExpr.Coalesce(buildAggregateOverChainQ runtime qdesc AggregateKind.Count None false, [SqlExpr.Literal(SqlLiteral.Integer 0L)]))
                                else
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None))
                            | Terminal.Sum sel ->
                                if hasOps then
                                    translatedArg (buildAggregateOverChainQ runtime qdesc AggregateKind.Sum (Some (toLambda sel)) true)
                                else
                                    let sel = toLambda sel
                                    let selDu = translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body
                                    translatedArg (SqlExpr.Coalesce(SqlExpr.AggregateCall(AggregateKind.Sum, Some selDu, false, None), [SqlExpr.Literal(SqlLiteral.Integer 0L)]))
                            | Terminal.SumProjected ->
                                if hasOps then
                                    translatedArg (buildAggregateOverChainQ runtime qdesc AggregateKind.Sum None true)
                                else
                                    raise (NotSupportedException("Error: GroupJoin Sum requires a selector.\nFix: Use .Sum(x => x.Property) or project first with .Select()."))
                            | Terminal.Min sel ->
                                if hasOps then
                                    translatedArg (buildAggregateOverChainQ runtime qdesc AggregateKind.Min (Some (toLambda sel)) false)
                                else
                                    let sel = toLambda sel
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Min, Some(translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None))
                            | Terminal.Max sel ->
                                if hasOps then
                                    translatedArg (buildAggregateOverChainQ runtime qdesc AggregateKind.Max (Some (toLambda sel)) false)
                                else
                                    let sel = toLambda sel
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Max, Some(translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None))
                            | Terminal.Average sel ->
                                if hasOps then
                                    translatedArg (buildAggregateOverChainQ runtime qdesc AggregateKind.Avg (Some (toLambda sel)) false)
                                else
                                    let sel = toLambda sel
                                    let selDu = translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body
                                    if isDecimalOrNullableDecimal sel.Body.Type then
                                        translatedArg (buildExactDecimalAverageExpr selDu)
                                    else
                                        translatedArg (SqlExpr.AggregateCall(AggregateKind.Avg, Some selDu, false, None))
                            | Terminal.MinProjected ->
                                if hasOps then translatedArg (buildAggregateOverChainQ runtime qdesc AggregateKind.Min None false)
                                else raise (NotSupportedException("Error: GroupJoin Min requires a selector.\nFix: Use .Min(x => x.Property) or project first with .Select()."))
                            | Terminal.MaxProjected ->
                                if hasOps then translatedArg (buildAggregateOverChainQ runtime qdesc AggregateKind.Max None false)
                                else raise (NotSupportedException("Error: GroupJoin Max requires a selector.\nFix: Use .Max(x => x.Property) or project first with .Select()."))
                            | Terminal.AverageProjected ->
                                if hasOps then translatedArg (buildAggregateOverChainQ runtime qdesc AggregateKind.Avg None false)
                                else raise (NotSupportedException("Error: GroupJoin Average requires a selector.\nFix: Use .Average(x => x.Property) or project first with .Select()."))
                            // Predicate/exists terminals — use Q version
                            | Terminal.Exists ->
                                if hasOps then
                                    translatedArg (buildExistsOverChainQ runtime qdesc None false)
                                else
                                    translatedArg (SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L)))
                            | Terminal.Any(Some pred) ->
                                if hasOps then
                                    translatedArg (buildExistsOverChainQ runtime qdesc (Some (toLambda pred)) false)
                                else
                                    let pred = toLambda pred
                                    let predDu = translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                                    translatedArg (SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L)))
                            | Terminal.Any None ->
                                translatedArg (SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L)))
                            | Terminal.All pred ->
                                if hasOps then
                                    translatedArg (buildExistsOverChainQ runtime qdesc (Some (toLambda pred)) true)
                                else
                                    let pred = toLambda pred
                                    let predDu = translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                                    translatedArg (SqlExpr.Binary(
                                        SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.Not, predDu), SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None),
                                        BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)))
                            | Terminal.Contains value ->
                                translatedArg (buildContainsOverChainQ runtime qdesc value)
                            // Select terminal → collection output.
                            // The Select lambda is in the terminal, not in desc.SelectProjection.
                            // Copy it into the descriptor so the builder projects correctly.
                            | Terminal.Select projExpr ->
                                let desc =
                                    if qdesc.SelectProjection.IsNone then
                                        match QueryTranslatorVisitPost.tryExtractLambdaExpression projExpr with
                                        | ValueSome lambda -> { qdesc with SelectProjection = Some lambda }
                                        | ValueNone -> qdesc
                                    else qdesc
                                translatedArg (buildGroupChainCollectionQ runtime desc)
                            // Element access terminals
                            | Terminal.First _ ->
                                buildGroupElementSubqueryQ runtime qdesc (FirstLike false) expr
                            | Terminal.FirstOrDefault _ ->
                                buildGroupElementSubqueryQ runtime qdesc (FirstLike true) expr
                            | Terminal.Last _ ->
                                buildGroupElementSubqueryQ runtime qdesc (LastLike false) expr
                            | Terminal.LastOrDefault _ ->
                                buildGroupElementSubqueryQ runtime qdesc (LastLike true) expr
                            | Terminal.Single _ ->
                                buildGroupElementSubqueryQ runtime qdesc (SingleLike false) expr
                            | Terminal.SingleOrDefault _ ->
                                buildGroupElementSubqueryQ runtime qdesc (SingleLike true) expr
                            | Terminal.ElementAt idx ->
                                buildGroupElementSubqueryQ runtime qdesc (ElementAtLike(idx, false)) expr
                            | Terminal.ElementAtOrDefault idx ->
                                buildGroupElementSubqueryQ runtime qdesc (ElementAtLike(idx, true)) expr
                            // Unsupported terminals — fail-closed, NO silent fallthrough
                            | Terminal.MinBy _ | Terminal.MaxBy _ | Terminal.DistinctBy _ | Terminal.CountBy _ ->
                                raise (NotSupportedException(
                                    $"Error: GroupJoin terminal '{terminal}' is not supported on group chains.\n" +
                                    "Reason: This terminal requires specialized lowering not yet available in GroupJoin context.\n" +
                                    "Fix: Move the query after AsEnumerable() or use a supported aggregate."))
                        | None ->
                            // FALLBACK: element access on bare group (g.First(), g.Last(), etc.)
                            match tryMatchGroupElementCall runtime mc with
                            | Some groupCall ->
                                    buildGroupElementDispatch runtime groupCall expr
                            | None ->
                                match translateScalarMethodCall mc with
                                | Some translated -> translated
                                | None ->
                                    // Not a group operation — translate as outer expression
                                    if referencesParam groupParam mc then
                                        raise (NotSupportedException(
                                            $"Error: GroupJoin group operation '{mc.Method.Name}' is not supported.\n" +
                                            "Reason: This operation on the group parameter could not be recognized as a supported terminal.\n" +
                                            "Fix: Use a supported terminal (Count, Sum, Any, All, First, etc.) or move after AsEnumerable()."))
                                    else
                                        translatedArg (translateOuterExpr expr)
                    | :? ConstantExpression ->
                        translatedArg (translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars None expr)
                    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked || ue.NodeType = ExpressionType.TypeAs ->
                        translateGroupJoinArg ue.Operand
                    | :? ConditionalExpression as ce ->
                        let test = translateGroupJoinArg ce.Test
                        let ifTrue = translateGroupJoinArg ce.IfTrue
                        let ifFalse = translateGroupJoinArg ce.IfFalse
                        { Value = SqlExpr.CaseExpr((test.Value, ifTrue.Value), [], Some(ifFalse.Value))
                          Error = combineErrorExprs [test.Error; ifTrue.Error; ifFalse.Error] }
                    | :? BinaryExpression as be ->
                        match tryTranslateGroupFirstLikeNullComparison runtime be with
                        | Some translated -> translatedArg translated
                        | None ->
                            let left = translateGroupJoinArg be.Left
                            let right = translateGroupJoinArg be.Right
                            let value =
                                match be.NodeType with
                                | ExpressionType.Coalesce -> SqlExpr.Coalesce(left.Value, [right.Value])
                                | ExpressionType.Add -> SqlExpr.Binary(left.Value, BinaryOperator.Add, right.Value)
                                | ExpressionType.Subtract -> SqlExpr.Binary(left.Value, BinaryOperator.Sub, right.Value)
                                | ExpressionType.Multiply -> SqlExpr.Binary(left.Value, BinaryOperator.Mul, right.Value)
                                | ExpressionType.Divide -> SqlExpr.Binary(left.Value, BinaryOperator.Div, right.Value)
                                | ExpressionType.Modulo -> SqlExpr.Binary(left.Value, BinaryOperator.Mod, right.Value)
                                | ExpressionType.Equal -> SqlExpr.Binary(left.Value, BinaryOperator.Eq, right.Value)
                                | ExpressionType.NotEqual -> SqlExpr.Binary(left.Value, BinaryOperator.Ne, right.Value)
                                | ExpressionType.GreaterThan -> SqlExpr.Binary(left.Value, BinaryOperator.Gt, right.Value)
                                | ExpressionType.GreaterThanOrEqual -> SqlExpr.Binary(left.Value, BinaryOperator.Ge, right.Value)
                                | ExpressionType.LessThan -> SqlExpr.Binary(left.Value, BinaryOperator.Lt, right.Value)
                                | ExpressionType.LessThanOrEqual -> SqlExpr.Binary(left.Value, BinaryOperator.Le, right.Value)
                                | ExpressionType.AndAlso -> SqlExpr.Binary(left.Value, BinaryOperator.And, right.Value)
                                | ExpressionType.OrElse -> SqlExpr.Binary(left.Value, BinaryOperator.Or, right.Value)
                                | _ -> raise (NotSupportedException($"GroupJoin result selector binary operator {be.NodeType} not supported."))
                            { Value = value
                              Error = combineErrorExprs [left.Error; right.Error] }
                    | _ ->
                        translatedArg (translateOuterExpr expr)

                let buildJsonObject (pairs: (string * SqlExpr) list) =
                    let args = pairs |> List.collect (fun (name, expr) -> [SqlExpr.Literal(SqlLiteral.String name); expr])
                    SqlExpr.FunctionCall(jsonObjectFn, args)

                let resultExpr =
                    match resultSelector.Body with
                    | :? NewExpression as newExpr when not (isNull newExpr.Members) ->
                        let memberNames = newExpr.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                        let translatedMembers =
                            [ for i in 0 .. newExpr.Arguments.Count - 1 ->
                                memberNames.[i], translateGroupJoinArg newExpr.Arguments.[i] ]
                        let jsonExpr = buildJsonObject [ for (name, arg) in translatedMembers -> name, arg.Value ]
                        match combineErrorExprs [ for (_, arg) in translatedMembers -> arg.Error ] with
                        | None -> jsonExpr
                        | Some errorExpr ->
                            SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, errorExpr), jsonExpr), [], Some errorExpr)
                    | :? MemberInitExpression as mi ->
                        let translatedMembers =
                            [ for binding in mi.Bindings do
                                match binding with
                                | :? MemberAssignment as ma ->
                                    yield ma.Member.Name, translateGroupJoinArg ma.Expression
                                | _ ->
                                    raise (NotSupportedException("GroupJoin result selector: only member assignments supported.")) ]
                        let jsonExpr = buildJsonObject [ for (name, arg) in translatedMembers -> name, arg.Value ]
                        match combineErrorExprs [ for (_, arg) in translatedMembers -> arg.Error ] with
                        | None -> jsonExpr
                        | Some errorExpr ->
                            SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, errorExpr), jsonExpr), [], Some errorExpr)
                    | _ ->
                        let translated = translateGroupJoinArg resultSelector.Body
                        match translated.Error with
                        | None -> translated.Value
                        | Some errorExpr ->
                            SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, errorExpr), translated.Value), [], Some errorExpr)

                let outerDiscoveredJoins =
                    materializeDiscoveredJoins outerCtx.Joins (Some ("\"" + outerAlias + "\"")) (Some sourceCtx.MaterializedPaths)
                let discoveredJoins =
                    materializeDiscoveredJoins innerAggCtx.Joins None None
                let allJoins =
                    outerDiscoveredJoins
                    @ [ConditionedJoin(
                        JoinKind.Left,
                        innerSource,
                        SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerJoinKeyExpr))]
                    @ discoveredJoins
                let core =
                    { mkCore
                        [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                         { Alias = Some "Value"; Expr = resultExpr }]
                        (Some (DerivedTable(ctx.Inner, outerAlias)))
                      with
                          Joins = allJoins
                          GroupBy = [SqlExpr.Column(Some outerAlias, "Id"); SqlExpr.Column(Some outerAlias, "Value")] }
                wrapCore core)
        | other ->
            raise (NotSupportedException(sprintf "Invalid number of arguments in GroupJoin: %A" other))
