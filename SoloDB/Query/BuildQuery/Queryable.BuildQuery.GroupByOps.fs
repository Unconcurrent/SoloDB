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

    // ── GroupBy aggregate translation helpers ──

    let internal extractLambdaFromExpr (expr: Expression) : LambdaExpression =
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
            | _ -> QueryableBuildQueryGroupByChained.tryTranslateGroupByChainedExpr rawContext innerSelect "o" groupParam vars rawSources rawProducer expression |> Utils.optionToValueOption
        let rawGrouping = lazy(groupingValue sourceCtx.NextAlias groupParam.Type rawKey
                                (QueryableBuildQueryGroupByChained.collection rawContext innerSelect "o" groupParam vars rawSources rawProducer (item.OriginalType.MakeArrayType())))
        rawSources[groupParam] <- { Alias = Some "o"; Context = rawContext; Value = DeferredParameter rawGrouping }
        let translateArgument (lambda: ChainExpr.Selector) =
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
                let scalar = QueryGroupScalar.tryTranslate groupParam None translateArgument expr
                match scalar with
                | Some value -> ValueSome(completion.Bind(QueryProduced.typed mc.Type value))
                | None -> QueryableBuildQueryGroupByChained.tryTranslateGroupByChainedExpr sourceCtx innerSelect completion.Alias groupParam vars sources producer expr |> Utils.optionToValueOption
            | _ -> QueryableBuildQueryGroupByChained.tryTranslateGroupByChainedExpr sourceCtx innerSelect completion.Alias groupParam vars sources producer expr |> Utils.optionToValueOption
        producer

    let private translateGroupBodyWith translate completion sourceCtx innerSelect groupParam parameters vars expression =
        let sources = Dictionary<ParameterExpression, ParameterSource>()
        let producer = groupHandler completion sourceCtx innerSelect groupParam sources vars
        let grouping = lazy(completedGrouping completion sourceCtx.NextAlias groupParam.Type
                                (sourceCtx.SerializationInput.Children[0])
                                (QueryTranslatorSerialization.serializationElement sourceCtx.SerializationInput))
        sources[groupParam] <- { Alias = Some completion.Alias; Context = sourceCtx; Value = DeferredParameter grouping }
        translate sourceCtx completion.Alias expression parameters (ValueSome sources) (ValueSome producer) true vars

    let private translateGroupBody completion sourceCtx innerSelect groupParam parameters vars expression =
        translateGroupBodyWith QueryTranslator.translateScopedBody completion sourceCtx innerSelect groupParam parameters vars expression

    let private translateGroupOrders
        (completion: QueryGroupCompletion) (sourceCtx: QueryContext) (innerSelect: SqlSelect) (vars: Dictionary<string, obj>)
        (groupOrders: (Expression * bool) list) =
        groupOrders
        |> List.collect (fun (orderingExpr, descending) ->
            let lambda = extractLambdaFromExpr orderingExpr
            let key=translateGroupBodyWith QueryTranslator.translateScopedOrderKey completion sourceCtx innerSelect lambda.Parameters[0] lambda.Parameters vars lambda.Body
            let terms,bindings=QueryStructuralOrdering.prepare sourceCtx.NextAlias vars lambda.ReturnType (SortDirection.ofDescending descending) None key
            completion.OrderBindings.AddRange bindings
            terms)

    let private nullSensitiveKeyLeaf (expr: Expression) =
        let isNullable = expr.Type.IsGenericType && typedefof<Nullable<_>>.Equals(expr.Type.GetGenericTypeDefinition())
        if isNullable || DateTimeFunctions.isDateTimeLikeType expr.Type then [expr] else []

    let rec private collectNullSensitiveKeyParts (expr: Expression) : Expression list =
        let expr =
            match expr with
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked ->
                ue.Operand
            | _ -> expr
        match expr with
        | :? BlockExpression as block ->
            match QueryConstructionExpressions.orderedMembers block with
            | ValueSome(struct(_, _, expressions, _)) -> expressions |> Array.toList |> List.collect collectNullSensitiveKeyParts
            | ValueNone -> nullSensitiveKeyLeaf expr
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
        | _ -> nullSensitiveKeyLeaf expr

    let private buildNullKeyFilter
        (sourceCtx: QueryContext)
        (ctxTableName: string)
        (vars: Dictionary<string, obj>)
        (keyLambdaExpr: Expression) : SqlExpr option =
        let keyLambda = extractLambdaFromExpr keyLambdaExpr
        let nullableResult = not (isNull (Nullable.GetUnderlyingType keyLambda.ReturnType))
        let boxedNullable =
            match keyLambda.Body with
            | :? UnaryExpression as conversion when conversion.NodeType = ExpressionType.Convert && isNull conversion.Method
                                                   && typeof<obj>.Equals(conversion.Type) ->
                not (isNull (Nullable.GetUnderlyingType conversion.Operand.Type))
            | _ -> false
        let predicates =
            (if nullableResult || boxedNullable then [] else collectNullSensitiveKeyParts keyLambda.Body)
            |> List.map (fun keyPart ->
                let keyPartExpr =
                    QueryTranslator.translateBodyToSqlExpr sourceCtx ctxTableName keyPart keyLambda.Parameters vars
                SqlExpr.Unary(UnaryOperator.IsNotNull, keyPartExpr))
        match predicates with
        | [] -> None
        | head :: tail -> Some (tail |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) head)

    let internal applyGroupByInput (sourceCtx: QueryContext) (statements: ResizeArray<SQLSubquery>) (group: ChainExpr.GroupBySelectors) =
        addSelector statements (KeyProjection group.Key)
        match group.Element with
        | None -> ()
        | Some element ->
            addComplexFinal statements (fun ctx ->
                let alias = sourceCtx.NextAlias()
                let current = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = ctx.Input; Joins = ResizeArray() }
                let key = ctx.KeyInput |> ValueOption.defaultWith (fun () -> invalidOp "A selected group element requires its original key.")
                let preserveErrors = canExposeNullId ctx.Inner
                let nullKeyFilter =
                    buildNullKeyFilter current alias ctx.Vars group.Key
                    |> Option.map (fun predicate ->
                        if preserveErrors then
                            SqlExpr.Binary(SqlExpr.Unary(UnaryOperator.IsNull, SqlExpr.Column(Some alias, "Id")), BinaryOperator.Or, predicate)
                        else predicate)
                let produced = QueryTranslator.translateProduced current alias element ctx.Vars
                let rows = QueryProduced.completedKeyRows sourceCtx.NextAlias ctx.Inner ctx.Input key
                let projections =
                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some alias, "Id") }
                     { Alias = Some "Value"; Expr = SqlExpr.Column(Some alias, "Value") }
                     { Alias = Some syntheticGroupKeyAlias; Expr = SqlExpr.Column(Some alias, syntheticGroupKeyAlias) }
                     { Alias = Some "_tree_ordinal"; Expr = SqlExpr.Column(Some alias, "_tree_ordinal") }]
                    @ QueryTranslatorSerialization.companionProjections (Some alias) key
                let core = { mkCore projections (Some(DerivedTable(rows, alias))) with
                                Joins = QueryTranslatorReferenceJoin.preparedJoinClauses current.Joins; Where = nullKeyFilter
                                OrderBy = [{ Expr = SqlExpr.Column(Some alias, "_tree_ordinal"); Direction = SortDirection.Asc }] }
                let struct(query, output) = QueryProduced.publishGroupElement sourceCtx.NextAlias core produced preserveErrors
                { Query = QueryProduced.materializeGroupElements sourceCtx.NextAlias "_tree_ordinal" query; SerializationInput = output; KeyInput = ctx.KeyInput })

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
        let nullKeyFilter =
            nullKeyFilter |> Option.map (fun predicate ->
                if canExposeNullId innerSelect then
                    SqlExpr.Binary(SqlExpr.Unary(UnaryOperator.IsNull, SqlExpr.Column(Some "o", "Id")), BinaryOperator.Or, predicate)
                else predicate)
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
        (group: ChainExpr.GroupBySelectors) (havingPreds: Expression list) (groupOrders: (Expression * bool) list)
        (groupType: Type) =
        addComplexFinal statements (fun ctx ->
            let completion = QueryGroupCompletion("_groups" + sourceCtx.NextAlias(), sourceCtx.NextAlias)
            let bindings, inner = if group.Element.IsSome then ctx.Inner.Ctes, { ctx.Inner with Ctes = [] } else [], ctx.Inner
            let key = ctx.KeyInput |> ValueOption.defaultWith (fun () -> invalidOp "Grouping requires its produced key.")
            let keyType = (extractLambdaFromExpr group.Key).Body.Type
            let rows = if QueryDefaultEquality.requiresPrepared keyType key then QueryProduced.completedKeyRows sourceCtx.NextAlias inner ctx.Input key else inner
            let struct(rows, key, equalityName) = QueryDefaultEquality.prepareGroups sourceCtx.NextAlias syntheticGroupKeyAlias keyType rows ctx.Input key "_tree_ordinal"
            let groupedInput = groupingInput groupType key ctx.Input
            let groupCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = groupedInput; Joins = completion.SelectorJoins }
            let having = groupFilter completion groupCtx rows ctx.Vars havingPreds
            let orderBy = translateGroupOrders completion groupCtx rows ctx.Vars groupOrders
            let rowCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = ctx.Input; Joins = completion.RowJoins }
            let nullKeyFilter =
                (if group.Element.IsSome then None else buildNullKeyFilter rowCtx ctx.TableName ctx.Vars group.Key)
            let produced = completedGrouping completion sourceCtx.NextAlias groupType key ctx.Input
            let struct(query, output) = completion.Publish(groupRows rows equalityName nullKeyFilter, completedRows having orderBy, produced)
            { Query = { query with Ctes = bindings @ query.Ctes }; SerializationInput = output; KeyInput = ValueNone })

    let internal applyGroupBySelect<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>)
        (group: ChainExpr.GroupBySelectors) (havingPreds: Expression list) (groupOrders: (Expression * bool) list) (selectLambda: ChainExpr.Selector)
        (groupType: Type) =
        addComplexFinal statements (fun ctx ->
            let completion = QueryGroupCompletion("_groups" + sourceCtx.NextAlias(), sourceCtx.NextAlias)
            let bindings, inner = if group.Element.IsSome then ctx.Inner.Ctes, { ctx.Inner with Ctes = [] } else [], ctx.Inner
            let keyInput = ctx.KeyInput |> ValueOption.defaultWith (fun () -> invalidOp "Grouping requires its produced key.")
            let keyType = (extractLambdaFromExpr group.Key).ReturnType
            let rows = if QueryDefaultEquality.requiresPrepared keyType keyInput then QueryProduced.completedKeyRows sourceCtx.NextAlias inner ctx.Input keyInput else inner
            let struct(rows, keyInput, equalityName) = QueryDefaultEquality.prepareGroups sourceCtx.NextAlias syntheticGroupKeyAlias keyType rows ctx.Input keyInput "_tree_ordinal"
            let input = groupingInput groupType keyInput ctx.Input
            let groupCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = input; Joins = completion.SelectorJoins }
            let translate (lambda: ChainExpr.Selector) = translateGroupBody completion groupCtx rows lambda.Parameters[0] lambda.Parameters ctx.Vars lambda.Body
            let having = groupFilter completion groupCtx rows ctx.Vars havingPreds
            let produced = translate selectLambda
            let produced =
                if typeof<double>.Equals(selectLambda.ReturnType) || typeof<float32>.Equals(selectLambda.ReturnType) then
                    QueryProduced.map sourceCtx.NextAlias (fun value -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%!.17g"); value])) produced
                else produced
            let rowCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = ctx.Input; Joins = completion.RowJoins }
            let nullKeyFilter =
                (if group.Element.IsSome then None else buildNullKeyFilter rowCtx ctx.TableName ctx.Vars group.Key)
            let orderBy = translateGroupOrders completion groupCtx rows ctx.Vars groupOrders
            let struct(query, output) = completion.Publish(groupRows rows equalityName nullKeyFilter, completedRows having orderBy, produced)
            { Query = { query with Ctes = bindings @ query.Ctes }; SerializationInput = output; KeyInput = ValueNone })
