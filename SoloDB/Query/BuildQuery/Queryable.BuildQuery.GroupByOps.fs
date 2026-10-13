namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Runtime.CompilerServices
open Utils
open SoloDatabase.JsonSerializator
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.SqlModel

/// GroupBy deferred emission + GroupBy+Select fusion for root-level queries.
/// Separated from SequenceOps to keep files under 400 lines.
module internal QueryableBuildQueryGroupByOps =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperJoinBindings
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase

    let internal applyGroupByKeyOnly<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>) (expressions: Expression array) =
        addSelector statements (KeyProjection expressions.[0])

    // ── GroupBy aggregate translation helpers ──

    let private extractLambdaFromExpr (expr: Expression) : LambdaExpression =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote -> ue.Operand :?> LambdaExpression
        | :? LambdaExpression as le -> le
        | _ -> raise (NotSupportedException($"Expected lambda expression, got {expr.NodeType}"))

    let rec private isGroupSource (expr: Expression) (groupParam: ParameterExpression) =
        match expr with
        | :? ParameterExpression as p -> obj.ReferenceEquals(p, groupParam)
        | :? UnaryExpression as convert when isReferenceUpcast convert ->
            isGroupSource convert.Operand groupParam
        | _ -> false

    // F# anonymous records with conditional fields use nested lambda applications.
    // Reduce those bindings through the same substitution used by scalar translation.
    let private inlineProjectionBindings (expression: Expression) =
        let visitor =
            { new ExpressionVisitor() with
                override self.VisitMethodCall call =
                    if call.Method.Name = "Invoke" && not (isNull call.Object) then
                        match self.Visit call.Object with
                        | :? LambdaExpression as lambda ->
                            self.Visit(inlineLambdaInvocation lambda call.Arguments)
                        | _ -> base.VisitMethodCall call
                    else base.VisitMethodCall call
                override self.VisitInvocation invocation =
                    match self.Visit invocation.Expression with
                    | :? LambdaExpression as lambda ->
                        self.Visit(inlineLambdaInvocation lambda invocation.Arguments)
                    | _ -> base.VisitInvocation invocation }
        visitor.Visit expression

    /// Bounds commute with total aggregate projections, not element terminals
    /// such as Single or expressions that can fail for a discarded group.
    let internal canPageProjection (expression: Expression) =
        let lambda = extractLambdaFromExpr expression
        let rec total (expr: Expression) =
            match expr with
            | :? ConstantExpression | :? ParameterExpression -> true
            | :? MemberExpression as memberExpr ->
                not (isNull memberExpr.Expression) && total memberExpr.Expression
                && (memberExpr.Member :? PropertyInfo || memberExpr.Member :? FieldInfo)
            | :? NewExpression as creation when creation.Type.IsDefined(typeof<CompilerGeneratedAttribute>, false) ->
                creation.Arguments |> Seq.forall total
            | :? ConditionalExpression as choice -> total choice.Test && total choice.IfTrue && total choice.IfFalse
            | :? BinaryExpression as binary ->
                match binary.NodeType with
                | ExpressionType.Equal | ExpressionType.NotEqual | ExpressionType.LessThan | ExpressionType.LessThanOrEqual
                | ExpressionType.GreaterThan | ExpressionType.GreaterThanOrEqual | ExpressionType.AndAlso | ExpressionType.OrElse ->
                    total binary.Left && total binary.Right
                | _ -> false
            | :? UnaryExpression as unary when unary.NodeType = ExpressionType.Quote || unary.NodeType = ExpressionType.Not -> total unary.Operand
            | :? LambdaExpression as predicate -> total predicate.Body
            | :? MethodCallExpression as call when
                (typeof<Enumerable>.Equals(call.Method.DeclaringType) || typeof<Queryable>.Equals(call.Method.DeclaringType))
                && (call.Method.Name = "Count" || call.Method.Name = "LongCount")
                && call.Arguments.Count >= 1 && isGroupSource call.Arguments.[0] lambda.Parameters.[0] ->
                call.Arguments |> Seq.skip 1 |> Seq.forall total
            | _ -> false
        total (inlineProjectionBindings lambda.Body)

    let private tryTranslateProjectedAggregate
        (translateInner: LambdaExpression -> SqlExpr) (groupParam: ParameterExpression)
        (call: MethodCallExpression) (aggKind: AggregateKind) (coalesceZero: bool) : SqlExpr option =
        let source = call.Arguments[0]
        let isDistinct, innerSource =
            match source with
            | :? MethodCallExpression as mc when mc.Method.Name = "Distinct" && mc.Arguments.Count = 1 -> true, mc.Arguments.[0]
            | _ -> false, source
        let projLambda, innerSource2 =
            match innerSource with
            | :? MethodCallExpression as mc when mc.Method.Name = "Select" && mc.Arguments.Count = 2 ->
                Some (extractLambdaFromExpr mc.Arguments.[1]), mc.Arguments.[0]
            | _ -> None, innerSource
        let whereLambda, innerSource3 =
            match innerSource2 with
            | :? MethodCallExpression as mc when mc.Method.Name = "Where" && mc.Arguments.Count = 2 ->
                Some (extractLambdaFromExpr mc.Arguments.[1]), mc.Arguments.[0]
            | _ -> None, innerSource2
        if not (isGroupSource innerSource3 groupParam) || (aggKind = AggregateKind.Count && isDistinct) then None
        else
        if aggKind = AggregateKind.Count then
            projLambda |> Option.iter (fun projection -> translateInner projection |> ignore)
        let argExpr =
            match projLambda, whereLambda with
            // Element counts include NULL projections. Select preserves cardinality;
            // Distinct must retain its rowset so its NULL element is counted too.
            | _, Some where when aggKind = AggregateKind.Count ->
                SqlExpr.CaseExpr((translateInner where, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], None)
            | _, None when aggKind = AggregateKind.Count -> SqlExpr.Literal(SqlLiteral.Integer 1L)
            | Some proj, Some where -> SqlExpr.CaseExpr((translateInner where, translateInner proj), [], None)
            | Some proj, None -> translateInner proj
            | None, Some where -> SqlExpr.CaseExpr((translateInner where, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], None)
            | None, None ->
                if aggKind = AggregateKind.Count then SqlExpr.Literal(SqlLiteral.Integer 1L)
                else raise (NotSupportedException("Projected aggregate without Select is not supported."))
        let aggArg =
            if aggKind = AggregateKind.Count && whereLambda.IsNone then None
            else Some argExpr
        let aggExpr =
            if aggKind = AggregateKind.Count then SqlExpr.AggregateCall(aggKind,aggArg,isDistinct,None)
            else
                let inputType = match projLambda with Some projection -> projection.ReturnType | None -> QueryTranslatorNumeric.aggregateInput call.Method
                QueryTranslatorNumeric.aggregate call.Method inputType aggKind argExpr isDistinct
        if coalesceZero then Some (SqlExpr.Coalesce(aggExpr, [QueryTranslatorNumeric.zero call.Method.ReturnType]))
        else Some aggExpr

    let private groupingInput groupType keyInput itemInput =
        QuerySerializationInput.Grouping(groupType, keyInput, itemInput)

    let private isGroupKey groupParam (expr: Expression) =
        match expr with
        | :? MemberExpression as memberExpr -> obj.ReferenceEquals(memberExpr.Expression, groupParam) && memberExpr.Member.Name = "Key"
        | _ -> false

    let private groupKey alias key =
        let value = SqlExpr.Column(Some alias, syntheticGroupKeyAlias)
        let key = QueryTranslatorSerialization.scopeSerializationInput (Some alias) OuterLayer
                      { key with Source = ValueSome(jsonToken value) }
        DirectValue(value, key)

    let private completedKey (completion: QueryGroupCompletion) key = completion.Bind(groupKey "o" key)

    let private completedItems (completion: QueryGroupCompletion) resultType item =
        let row = QueryTranslatorSerialization.scopeSerializationInput (Some "o") OuterLayer item
        let payload, row = QueryProduced.embedded (SqlExpr.Column(Some "o", "Value"), row)
        let input = { QuerySerializationInput.Sequence(resultType, row) with
                        Operand = ValueSome { Descriptor = QuerySerializationSql.sequenceDescriptor resultType row; Accesses = Array.empty } }
        completion.Bind(DirectValue(SqlExpr.FunctionCall("jsonb_group_array", [jsonToken payload]), input))

    let private groupingValue nextName groupType key items =
        QueryProduced.construct nextName (fun values ->
            let values = values |> Array.map QueryProduced.embedded
            let keyValue, keyInput = values[0]
            let itemValue, itemInput = values[1]
            let value = SqlExpr.FunctionCall(jsonbObjectFn,
                [SqlExpr.Literal(SqlLiteral.String "Key"); keyValue;
                 SqlExpr.Literal(SqlLiteral.String "Items"); itemValue])
            let input = QuerySerializationInput.Members(groupType, [|"Key"; "Items"|], [|keyInput.OriginalType; itemInput.OriginalType|], [|keyInput; itemInput|])
            DirectValue(value, input)) [|key; items|]

    let private completedGrouping completion nextName groupType key (item: QuerySerializationInput) =
        groupingValue nextName groupType (completedKey completion key)
            (completedItems completion (item.OriginalType.MakeArrayType()) item)

    let private groupHandler
        (completion: QueryGroupCompletion) (sourceCtx: QueryContext) (innerSelect: SqlSelect) (groupParam: ParameterExpression)
        (sources: Dictionary<ParameterExpression, ParameterSource>) (vars: Dictionary<string, obj>) =
        let item = QueryTranslatorSerialization.serializationElement sourceCtx.SerializationInput
        let itemCtx = { sourceCtx with SerializationInput = item; LayerPosition = OuterLayer; Joins = completion.RowJoins }
        let rawSources = Dictionary<ParameterExpression, ParameterSource>()
        let rawContext = { sourceCtx with Joins = completion.RowJoins }
        let rawKey = groupKey "o" sourceCtx.SerializationInput.Children[0]
        let rec rawProducer expression =
            match expression with
            | expression when isGroupKey groupParam expression -> ValueSome rawKey
            | _ -> QueryableBuildQueryGroupByChained.tryTranslateGroupByChainedExpr rawContext innerSelect "o" groupParam vars rawSources rawProducer expression |> ValueOption.ofOption
        let rawGrouping = lazy(groupingValue sourceCtx.NextAlias groupParam.Type rawKey
                                (QueryableBuildQueryGroupByChained.collection rawContext innerSelect "o" groupParam vars rawSources rawProducer (item.OriginalType.MakeArrayType())))
        rawSources[groupParam] <- { Alias = Some "o"; Context = rawContext; Value = DeferredParameter rawGrouping }
        let translateArgument (lambda: LambdaExpression) =
            QueryTranslator.translateScopedBody itemCtx "o" lambda.Body lambda.Parameters
                (ValueSome rawSources) (ValueSome rawProducer) false vars |> QueryProduced.value sourceCtx.NextAlias
        let rec producer (expr: Expression) : QueryProducedValue voption =
            match expr with
            | expression when isGroupKey groupParam expression ->
                ValueSome(completedKey completion sourceCtx.SerializationInput.Children[0])
            | :? MethodCallExpression as call when call.Method.Name = "Items" && call.Arguments.Count = 1 && isGroupSource call.Arguments[0] groupParam ->
                ValueSome(completedItems completion call.Type item)
            | :? MethodCallExpression as mc when referencesParam groupParam mc &&
                    (typeof<Enumerable>.Equals(mc.Method.DeclaringType) || typeof<Queryable>.Equals(mc.Method.DeclaringType)) ->
                let scalar =
                    match mc.Method.Name with
                    | ("Count" | "LongCount") when mc.Arguments.Count = 1 && isGroupSource mc.Arguments.[0] groupParam ->
                        Some (SqlExpr.AggregateCall(AggregateKind.Count, None, false, None))
                    | ("Count" | "LongCount") when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                        let pred = extractLambdaFromExpr mc.Arguments.[1]
                        let predDu = translateArgument pred
                        Some (SqlExpr.AggregateCall(AggregateKind.Sum, Some (SqlExpr.CaseExpr(
                            (predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None))
                    | "Sum" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                        let sel = extractLambdaFromExpr mc.Arguments.[1]
                        let selDu = translateArgument sel
                        Some (SqlExpr.Coalesce(QueryTranslatorNumeric.aggregate mc.Method sel.ReturnType AggregateKind.Sum selDu false, [QueryTranslatorNumeric.zero mc.Method.ReturnType]))
                    | "Min" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                        let sel = extractLambdaFromExpr mc.Arguments.[1]
                        Some (QueryTranslatorNumeric.aggregate mc.Method sel.ReturnType AggregateKind.Min (translateArgument sel) false)
                    | "Max" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                        let sel = extractLambdaFromExpr mc.Arguments.[1]
                        Some (QueryTranslatorNumeric.aggregate mc.Method sel.ReturnType AggregateKind.Max (translateArgument sel) false)
                    | "Average" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                        let sel = extractLambdaFromExpr mc.Arguments.[1]
                        Some (QueryTranslatorNumeric.aggregate mc.Method sel.ReturnType AggregateKind.Avg (translateArgument sel) false)
                    | "Sum" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate translateArgument groupParam mc AggregateKind.Sum true
                    | ("Count" | "LongCount") when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate translateArgument groupParam mc AggregateKind.Count false
                    | "Min" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate translateArgument groupParam mc AggregateKind.Min false
                    | "Max" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate translateArgument groupParam mc AggregateKind.Max false
                    | "Average" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate translateArgument groupParam mc AggregateKind.Avg false
                    | "Any" when mc.Arguments.Count = 1 && isGroupSource mc.Arguments.[0] groupParam ->
                        Some (SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Count, None, false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L)))
                    | _ -> None
                match scalar with
                | Some value -> ValueSome(completion.Bind(QueryProduced.typed mc.Type value))
                | None -> QueryableBuildQueryGroupByChained.tryTranslateGroupByChainedExpr sourceCtx innerSelect completion.Alias groupParam vars sources producer expr |> ValueOption.ofOption
            | _ -> QueryableBuildQueryGroupByChained.tryTranslateGroupByChainedExpr sourceCtx innerSelect completion.Alias groupParam vars sources producer expr |> ValueOption.ofOption
        producer

    let private translateGroupBody completion sourceCtx innerSelect groupParam parameters vars expression =
        let sources = Dictionary<ParameterExpression, ParameterSource>()
        let producer = groupHandler completion sourceCtx innerSelect groupParam sources vars
        let grouping = lazy(completedGrouping completion sourceCtx.NextAlias groupParam.Type
                                (sourceCtx.SerializationInput.Children[0])
                                (QueryTranslatorSerialization.serializationElement sourceCtx.SerializationInput))
        sources[groupParam] <- { Alias = Some completion.Alias; Context = sourceCtx; Value = DeferredParameter grouping }
        QueryTranslator.translateScopedBody sourceCtx completion.Alias expression parameters (ValueSome sources) (ValueSome producer) true vars

    let private translateGroupOrders
        (completion: QueryGroupCompletion) (sourceCtx: QueryContext) (innerSelect: SqlSelect) (vars: Dictionary<string, obj>)
        (groupOrders: (Expression * bool) list) =
        groupOrders
        |> List.map (fun (orderingExpr, descending) ->
            let lambda = extractLambdaFromExpr orderingExpr
            let orderExpr =
                translateGroupBody completion sourceCtx innerSelect lambda.Parameters[0] lambda.Parameters vars lambda.Body
                |> QueryProduced.value sourceCtx.NextAlias
            { Expr = orderExpr
              Direction = if descending then SortDirection.Desc else SortDirection.Asc })

    let private nullableKeyFilter (keyType: Type) : SqlExpr option =
        if keyType.IsGenericType && typedefof<Nullable<_>>.Equals(keyType.GetGenericTypeDefinition()) then
            Some (SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some "o", syntheticGroupKeyAlias)))
        else None

    let rec private collectNullSensitiveKeyParts (expr: Expression) : Expression list =
        let expr =
            match expr with
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked ->
                ue.Operand
            | _ -> expr
        match expr with
        | :? BoundValueCallExpression as call when not(isNull call.ProjectionNames) ->
            call.Inputs |> Array.toList |> List.collect collectNullSensitiveKeyParts
        | :? NewExpression as ne when not (DateTimeFunctions.isDateTimeLikeType ne.Type) ->
            ne.Arguments |> Seq.toList |> List.collect collectNullSensitiveKeyParts
        | :? MemberInitExpression as mi ->
            let fromNew = collectNullSensitiveKeyParts mi.NewExpression
            let fromBindings =
                mi.Bindings
                |> Seq.cast<MemberAssignment>
                |> Seq.toList
                |> List.collect (fun binding -> collectNullSensitiveKeyParts binding.Expression)
            fromNew @ fromBindings
        | _ ->
            let isNullable =
                expr.Type.IsGenericType
                && typedefof<Nullable<_>>.Equals(expr.Type.GetGenericTypeDefinition())
            if isNullable || DateTimeFunctions.isDateTimeLikeType expr.Type then [ expr ] else []

    let private buildNullKeyFilter
        (sourceCtx: QueryContext)
        (ctxTableName: string)
        (vars: Dictionary<string, obj>)
        (keyLambdaExpr: Expression) : SqlExpr option =
        let keyLambda = extractLambdaFromExpr keyLambdaExpr
        let predicates =
            collectNullSensitiveKeyParts keyLambda.Body
            |> List.map (fun keyPart ->
                let keyPartExpr =
                    QueryTranslator.translateBodyToSqlExpr sourceCtx ctxTableName keyPart keyLambda.Parameters vars
                SqlExpr.Unary(UnaryOperator.IsNotNull, keyPartExpr))
        match predicates with
        | [] -> None
        | head :: tail -> Some (tail |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) head)

    let private groupFilter completion sourceCtx innerSelect vars predicates =
        predicates
        |> List.map (fun predicate ->
            let lambda = extractLambdaFromExpr predicate
            translateGroupBody completion sourceCtx innerSelect lambda.Parameters[0] lambda.Parameters vars lambda.Body
            |> QueryProduced.value sourceCtx.NextAlias)
        |> function
            | [] -> None
            | first :: rest -> Some(List.fold (fun left right -> SqlExpr.Binary(left, BinaryOperator.And, right)) first rest)

    let private groupRows innerSelect equalityName nullKeyFilter =
        { mkCore [{ Alias = Some syntheticGroupKeyAlias; Expr = SqlExpr.Column(Some "o", syntheticGroupKeyAlias) }]
                 (Some(DerivedTable(innerSelect, "o"))) with
            GroupBy = equalityName |> List.map (fun name -> SqlExpr.Column(Some "o", name))
            Where = nullKeyFilter }

    let private completedRows having orderBy =
        { mkCore [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                  { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }] None with
            Where = having; OrderBy = orderBy }

    let internal flushGroupByAsJsonGroupArray<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>)
        (expressions: Expression array) (havingPreds: Expression list) (groupOrders: (Expression * bool) list)
        (groupType: Type) =
        addComplexFinal statements (fun ctx ->
            let completion = QueryGroupCompletion("_groups" + sourceCtx.NextAlias(), sourceCtx.NextAlias)
            let key = ctx.KeyInput |> ValueOption.defaultWith (fun () -> invalidOp "Grouping requires its produced key.")
            let keyType = (extractLambdaFromExpr expressions.[0]).Body.Type
            let rows = if QueryDefaultEquality.requiresPrepared keyType key then QueryProduced.completedKeyRows sourceCtx.NextAlias ctx.Inner ctx.Input key else ctx.Inner
            let struct(rows, key, equalityName) = QueryDefaultEquality.prepareGroups sourceCtx.NextAlias syntheticGroupKeyAlias keyType rows ctx.Input key "_tree_ordinal"
            let groupedInput = groupingInput groupType key ctx.Input
            let groupCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = groupedInput; Joins = completion.SelectorJoins }
            let having = groupFilter completion groupCtx rows ctx.Vars havingPreds
            let orderBy = translateGroupOrders completion groupCtx rows ctx.Vars groupOrders
            let rowCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = ctx.Input; Joins = completion.RowJoins }
            let nullKeyFilter = buildNullKeyFilter rowCtx ctx.TableName ctx.Vars expressions.[0]
                                |> Option.orElseWith (fun () -> nullableKeyFilter keyType)
            let produced = completedGrouping completion sourceCtx.NextAlias groupType key ctx.Input
            let struct(query, output) = completion.Publish(groupRows rows equalityName nullKeyFilter, completedRows having orderBy, produced)
            { Query = query; SerializationInput = output; KeyInput = ValueNone })

    let internal applyGroupBySelect<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>)
        (groupByExpressions: Expression array) (havingPreds: Expression list) (groupOrders: (Expression * bool) list) (selectExpressions: Expression array)
        (groupType: Type) =
        let selectLambda = extractLambdaFromExpr selectExpressions[0]
        addComplexFinal statements (fun ctx ->
            let completion = QueryGroupCompletion("_groups" + sourceCtx.NextAlias(), sourceCtx.NextAlias)
            let keyInput = ctx.KeyInput |> ValueOption.defaultWith (fun () -> invalidOp "Grouping requires its produced key.")
            let keyType = (extractLambdaFromExpr groupByExpressions[0]).ReturnType
            let rows = if QueryDefaultEquality.requiresPrepared keyType keyInput then QueryProduced.completedKeyRows sourceCtx.NextAlias ctx.Inner ctx.Input keyInput else ctx.Inner
            let struct(rows, keyInput, equalityName) = QueryDefaultEquality.prepareGroups sourceCtx.NextAlias syntheticGroupKeyAlias keyType rows ctx.Input keyInput "_tree_ordinal"
            let input = groupingInput groupType keyInput ctx.Input
            let groupCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = input; Joins = completion.SelectorJoins }
            let translate (lambda: LambdaExpression) = translateGroupBody completion groupCtx rows lambda.Parameters[0] lambda.Parameters ctx.Vars lambda.Body
            let having = groupFilter completion groupCtx rows ctx.Vars havingPreds
            let produced = translate selectLambda
            let produced =
                if typeof<double>.Equals(selectLambda.ReturnType) || typeof<float32>.Equals(selectLambda.ReturnType) then
                    QueryProduced.map sourceCtx.NextAlias (fun value -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%!.17g"); value])) produced
                else produced
            let rowCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = ctx.Input; Joins = completion.RowJoins }
            let nullKeyFilter = buildNullKeyFilter rowCtx ctx.TableName ctx.Vars groupByExpressions[0]
                                |> Option.orElseWith (fun () -> nullableKeyFilter keyType)
            let orderBy = translateGroupOrders completion groupCtx rows ctx.Vars groupOrders
            let struct(query, output) = completion.Publish(groupRows rows equalityName nullKeyFilter, completedRows having orderBy, produced)
            { Query = query; SerializationInput = output; KeyInput = ValueNone })
