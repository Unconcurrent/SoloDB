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
open SoloDatabase.SqlModel
open SoloDatabase.GroupJoinRuntimeTypes
open SoloDatabase.GroupJoinChainParts
open SoloDatabase.QueryableBuildQueryGroupJoinChain
open SoloDatabase.QueryableBuildQueryGroupJoinElements
open SoloDatabase.DBRefManyDescriptor

/// GroupJoin source and direct aggregates; composed sequences use OrderedChain.
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
            let innerExpression =
                match expressions.[0] with
                | :? MemberExpression | :? ConstantExpression -> readSoloDBQueryableUntyped expressions.[0]
                | expression -> expression
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
                      TranslateOuterExpr = translateOuterExpr }

                let orderedAdapter: OrderedChainPlan.Adapter = {
                    EntityMembershipById = false
                    LambdaContext = "group join"
                    Validate = fun plan ->
                        for stage in plan.Stages do
                            match stage with
                            | OrderedChainPlan.OfType(sourceType, _) | OrderedChainPlan.Cast(sourceType, _) -> DBRefManyHelpers.ensureOfTypeSupported sourceType
                            | _ -> ()
                    IsRoot = fun e -> obj.ReferenceEquals(e, groupParam)
                    IsValue = fun e -> QueryTranslatorBaseHelpers.isFullyConstant e || (innerCtx.BindQueryValue |> ValueOption.exists (fun b -> b.IsValue e))
                    Alias = fun () -> GroupJoinAliases.nextRowset innerCtx
                    Value = fun e -> translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars None e
                    Translate = fun _ a l ->
                        let sub = { innerCtx with Joins=ResizeArray() }
                        let value = translateGroupChainExpression runtime sub a l.Parameters.[0] l.Body
                        value, materializeInnerRowJoins runtime a sub.Joins
                    Source = fun _ _ ->
                        let a = GroupJoinAliases.nextRowset innerCtx
                        let sub = {innerCtx with Joins=ResizeArray()}
                        let key =
                            match tryTranslateDbRefValueIdKey innerKeySelector.Parameters.[0] a innerKeySelector.Body with
                            | Some value -> value
                            | None -> translateJoinSingleSourceExpression sub a ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body
                        let ps=[{Alias=Some "Id";Expr=SqlExpr.Column(Some a,"Id")}
                                {Alias=Some "Value";Expr=SqlExpr.Column(Some a,"Value")}
                                {Alias=Some "__ord";Expr=SqlExpr.Column(Some a,"Id")}]
                        {Ctes=[];Body=SingleSelect {OrderedChainRows.core (Some(DerivedTable(innerSelect,a))) ps with
                                                      Joins=materializeInnerRowJoins runtime a sub.Joins
                                                      Where=Some(SqlExpr.Binary(runtime.OuterKeyExpr,BinaryOperator.Eq,key))
                                                      OrderBy=[{Expr=SqlExpr.Column(Some a,"Id");Direction=SortDirection.Asc}]}}
                }
                let rec translateGroupJoinArg (expr: Expression) : SqlExpr =

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

                    let rec translateScalarMethodCall (mc: MethodCallExpression) : SqlExpr option =
                        let rec getInterpolationArgType (expr: Expression) =
                            match expr with
                            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked || ue.NodeType = ExpressionType.TypeAs ->
                                getInterpolationArgType ue.Operand
                            | _ -> expr.Type
                        let normalizeInterpolatedArg (originalExpr: Expression) (translated: SqlExpr) =
                            if isDecimalOrNullableDecimal (getInterpolationArgType originalExpr) then
                                SqlExpr.FunctionCall("DECIMAL_TEXT", [translated])
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
                            Some (SqlExpr.FunctionCall("CONCAT", parts))
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
                                    | Choice1Of2 literal -> SqlExpr.Literal(SqlLiteral.String literal)
                                    | Choice2Of2 index when index >= 0 && index < translatedArgs.Length -> translatedArgs.[index]
                                    | Choice2Of2 _ ->
                                        raise (NotSupportedException(
                                            "Error: GroupJoin string format index is out of range.\n" +
                                            "Fix: Ensure each placeholder refers to an existing interpolation argument.")))
                            Some (SqlExpr.FunctionCall("CONCAT", translated))
                        elif mc.Method.Name = "ToString" && mc.Arguments.Count = 0 && not (isNull mc.Object) then
                            Some (translateGroupJoinArg mc.Object)
                        else
                            None

                    let directAggregate =
                        match expr with
                        | :? MethodCallExpression as call
                            when call.Arguments.Count > 0
                                 && (call.Method.DeclaringType = typeof<System.Linq.Enumerable>
                                     || call.Method.DeclaringType = typeof<System.Linq.Queryable>) ->
                            match OrderedChainPlan.parse orderedAdapter.IsRoot call.Arguments.[0] with
                            | Some plan when plan.Stages.IsEmpty ->
                                let count = SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None)
                                let present = SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some innerAlias, "Id"))
                                let zero = SqlExpr.Literal(SqlLiteral.Integer 0L)
                                let one = SqlExpr.Literal(SqlLiteral.Integer 1L)
                                let translateArgument () =
                                    let selector = unwrapLambdaExpressionOrThrow "GroupJoin terminal argument" call.Arguments.[1]
                                    selector, translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some selector.Parameters.[0]) selector.Body
                                match call.Method.Name, call.Arguments.Count with
                                | ("Count" | "LongCount"), 1 -> Some count
                                | "Any", 1 -> Some(SqlExpr.Binary(count, BinaryOperator.Gt, zero))
                                | ("Sum" | "Min" | "Max" | "Average"), 2 ->
                                    let selector, value = translateArgument ()
                                    // A LEFT JOIN's padding row is not a group element,
                                    // even when the selector is a non-null constant.
                                    let value = SqlExpr.CaseExpr((present, value), [], None)
                                    let kind = match call.Method.Name with "Sum" -> AggregateKind.Sum | "Min" -> AggregateKind.Min | "Max" -> AggregateKind.Max | _ -> AggregateKind.Avg
                                    let aggregate = SqlExpr.AggregateCall(kind, Some value, false, None)
                                    Some(if call.Method.Name = "Sum" then SqlExpr.Coalesce(aggregate, [zero])
                                         elif call.Method.Name = "Average" && isDecimalOrNullableDecimal selector.Body.Type then buildExactDecimalAverageExpr value
                                         else aggregate)
                                | ("Any" | "All"), 2 ->
                                    let _, predicate = translateArgument ()
                                    let all = call.Method.Name = "All"
                                    let predicate = if all then SqlExpr.Unary(UnaryOperator.Not, predicate) else predicate
                                    let predicate = SqlExpr.Binary(present, BinaryOperator.And, predicate)
                                    let matched = SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((predicate, one), [], Some zero)), false, None)
                                    Some(SqlExpr.Binary(matched, (if all then BinaryOperator.Eq else BinaryOperator.Gt), zero))
                                | _ -> None
                            | _ -> None
                        | _ -> None
                    let retained =
                        match directAggregate with
                        | Some _ -> directAggregate
                        | None ->
                            match expr with
                            | :? MemberExpression as memberAccess when not (isNull memberAccess.Expression) ->
                                match tryMatchGroupElementCall runtime memberAccess.Expression with
                                | Some call -> Some(buildGroupElementDispatch runtime call expr)
                                | None -> OrderedChain.tryBuild orderedAdapter expr
                            | _ -> OrderedChain.tryBuild orderedAdapter expr
                    match retained with

                    | Some result -> result
                    | None ->
                    if not (referencesParam groupParam expr) then
                        translateOuterExpr expr
                    else
                    match expr with
                    | :? MemberExpression as me when not (isNull me.Expression) && me.Expression :? ParameterExpression && (me.Expression :?> ParameterExpression) = outerParam ->
                        translateOuterExpr expr
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
                                translateOuterExpr expr
                    | :? MethodCallExpression as mc ->
                        match translateScalarMethodCall mc with
                        | Some translated -> translated
                        | None ->
                            raise (NotSupportedException(
                                $"Error: GroupJoin group operation '{mc.Method.Name}' is not supported.\n" +
                                "Reason: This operation on the group parameter could not be recognized as a supported terminal.\n" +
                                "Fix: Use a supported terminal (Count, Sum, Any, All, First, etc.) or move after AsEnumerable()."))
                    | :? ConstantExpression ->
                        translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars None expr
                    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked || ue.NodeType = ExpressionType.TypeAs ->
                        translateGroupJoinArg ue.Operand
                    | :? ConditionalExpression as ce ->
                        let test = translateGroupJoinArg ce.Test
                        let ifTrue = translateGroupJoinArg ce.IfTrue
                        let ifFalse = translateGroupJoinArg ce.IfFalse
                        SqlExpr.CaseExpr((test, ifTrue), [], Some ifFalse)
                    | :? BinaryExpression as be ->
                        match tryTranslateGroupFirstLikeNullComparison runtime be with
                        | Some translated -> translated
                        | None ->
                            let left = translateGroupJoinArg be.Left
                            let right = translateGroupJoinArg be.Right
                            match be.NodeType with
                            | ExpressionType.Coalesce -> SqlExpr.Coalesce(left, [right])
                            | ExpressionType.Add -> SqlExpr.Binary(left, BinaryOperator.Add, right)
                            | ExpressionType.Subtract -> SqlExpr.Binary(left, BinaryOperator.Sub, right)
                            | ExpressionType.Multiply -> SqlExpr.Binary(left, BinaryOperator.Mul, right)
                            | ExpressionType.Divide -> SqlExpr.Binary(left, BinaryOperator.Div, right)
                            | ExpressionType.Modulo -> SqlExpr.Binary(left, BinaryOperator.Mod, right)
                            | ExpressionType.Equal -> SqlExpr.Binary(left, BinaryOperator.Eq, right)
                            | ExpressionType.NotEqual -> SqlExpr.Binary(left, BinaryOperator.Ne, right)
                            | ExpressionType.GreaterThan -> SqlExpr.Binary(left, BinaryOperator.Gt, right)
                            | ExpressionType.GreaterThanOrEqual -> SqlExpr.Binary(left, BinaryOperator.Ge, right)
                            | ExpressionType.LessThan -> SqlExpr.Binary(left, BinaryOperator.Lt, right)
                            | ExpressionType.LessThanOrEqual -> SqlExpr.Binary(left, BinaryOperator.Le, right)
                            | ExpressionType.AndAlso -> SqlExpr.Binary(left, BinaryOperator.And, right)
                            | ExpressionType.OrElse -> SqlExpr.Binary(left, BinaryOperator.Or, right)
                            | _ -> raise (NotSupportedException($"GroupJoin result selector binary operator {be.NodeType} not supported."))
                    | _ ->
                        translateOuterExpr expr

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
                        buildJsonObject translatedMembers
                    | :? MemberInitExpression as mi ->
                        let translatedMembers =
                            [ for binding in mi.Bindings do
                                match binding with
                                | :? MemberAssignment as ma ->
                                    yield ma.Member.Name, translateGroupJoinArg ma.Expression
                                | _ ->
                                    raise (NotSupportedException("GroupJoin result selector: only member assignments supported.")) ]
                        buildJsonObject translatedMembers
                    | _ ->
                        translateGroupJoinArg resultSelector.Body

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
