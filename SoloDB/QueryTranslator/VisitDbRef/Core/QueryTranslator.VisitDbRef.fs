namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitDbRefPeelers
open SoloDatabase.DBRefManyDescriptor
open DBRefTypeHelpers

/// Entry point for DBRefMany expression handling.
/// Binds relation sources and delegates sequence semantics to OrderedChain.
module internal QueryTranslatorVisitDbRef =
    let private handleDBRefManySource (qb: QueryBuilder) (exp: Expression) (sourceOverride: System.Collections.Generic.Dictionary<Expression, DBRefManyOwnerRef>) : bool =
        let actualExp =
            match exp with
            | :? LambdaExpression as le -> le.Body
            | _ -> exp
        // Normalize the existing relation API into LINQ terminals before dispatch.
        // These calls must not fall through to stored JSON-array translation.
        let actualExp =
            match actualExp with
            | :? MethodCallExpression as call when typeof<Extensions>.Equals(call.Method.DeclaringType)
                                                   && call.Method.Name = "Any" && call.Arguments.Count = 2
                                                   && isManyReferenceType (unwrapConvert call.Arguments.[0]).Type ->
                match tryExtractLambdaExpression call.Arguments.[1] with
                | ValueSome predicate ->
                    Expression.Call(typeof<System.Linq.Enumerable>, "Any", call.Method.GetGenericArguments(),
                                    call.Arguments.[0], predicate) :> Expression
                | _ ->
                    raise (NotSupportedException(
                        "Error: Cannot translate relation-backed DBRefMany.Any predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."))
            | :? MethodCallExpression as call when call.Method.Name = "Contains"
                                                   && not (isNull call.Object) && isManyReferenceType call.Object.Type
                                                   && call.Arguments.Count = 1 ->
                let elementType = ChainExpr.sequenceElementType call.Object.Type
                Expression.Call(typeof<System.Linq.Enumerable>, "Contains", [| elementType |], call.Object, call.Arguments.[0]) :> Expression
            | _ -> actualExp
        let mutable ownsSource = false
        let ordered =
            if sourceOverride.Count = 0 then None
            else
                let sources = System.Collections.Generic.Dictionary<Expression, DBRefManyDescriptor.DBRefManyOwnerRef>()
                let isRoot expression =
                    if isNull expression then false
                    elif sources.ContainsKey expression then true
                    elif not (isManyReferenceType expression.Type) then false
                    else
                        let owner =
                            match sourceOverride.TryGetValue expression with
                            | true, owner -> ValueSome owner
                            | _ -> tryGetDBRefManyOwnerRef qb expression
                        match owner with
                        | ValueSome owner ->
                            ownsSource <- true
                            sources.Add(expression, owner)
                            true
                        | _ -> false
                let adapter: OrderedChainPlan.Adapter = {
                    MembershipContext = ReceiverScopedEntityMembership
                    LambdaContext = "relation-backed DBRefMany"
                    Validate = fun plan ->
                        qb.StepTranslation()
                        if countDbRefManyDepth actualExp > Utils.maxRelationDepth then
                            raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))
                        let mutable ordered = DBRefManyBuilderCore.tryGetRelationOrderByForTakeWhile sources.[plan.Root] "" "" |> ValueOption.isSome
                        for stage in plan.Stages do
                            match stage with
                            | OrderedChainPlan.Order _ -> ordered <- true
                            | OrderedChainPlan.OfType(sourceType, _) | OrderedChainPlan.Cast(sourceType, _) -> DBRefManyHelpers.ensureOfTypeSupported sourceType
                            | OrderedChainPlan.While _ when not ordered ->
                                raise (InvalidOperationException(DBRefManyHelpers.takeWhileOrderingRequiredMessage))
                            | _ -> ()
                    IsRoot = isRoot
                    IsValue = fun e -> QueryTranslatorBaseHelpers.isFullyConstant e || (qb.SourceContext.BindQueryValue |> ValueOption.exists (fun b -> b.IsValue e))
                    Alias = fun () -> DBRefManyBuilderCore.nextAlias qb.SourceContext "_chain"
                    Value = fun e -> visitProduced e qb
                    Comparison = fun e -> visitComparisonProduced e qb
                    Translate = fun source input alias lambda ->
                        let sub = qb.ForSubquery(alias, lambda.Parameters, BaseLayer, input, subqueryRootTable = qb.SourceContext.RootTable)
                        let value = visitProduced lambda.Body sub
                        let joins = QueryTranslatorReferenceJoin.preparedJoinClauses sub.SourceContext.Joins
                        value, joins
                    Source = fun columns source ->
                        let struct(rows, input) = DBRefManyBuilderCore.buildRelationSource qb sources.[source] (columns = OrderedChainPlan.FullValue)
                        let alias = qb.SourceContext.NextAlias()
                        let projections = [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some alias, "Id") }
                                           { Alias = Some "Value"; Expr = SqlExpr.Column(Some alias, "Value") }
                                           { Alias = Some "__ord"; Expr = SqlExpr.Column(Some alias, "__ord") }]
                                          @ QueryTranslatorSerialization.companionProjections (Some alias) input
                        let ordered = DBRefManyBuilderCore.mkSubCore projections (Some(DerivedTable(rows, alias))) None
                        struct({ Ctes = []; Body = SingleSelect { ordered with OrderBy = [{ Expr = SqlExpr.Column(Some alias, "__ord"); Direction = SortDirection.Asc }] } }, input)
                }
                let expression =
                    match actualExp with
                    | :? MemberExpression as memberAccess when memberAccess.Member.Name = "Count" && not (isNull memberAccess.Expression) ->
                        let elementType = ChainExpr.sequenceElementType memberAccess.Expression.Type
                        Expression.Call(typeof<System.Linq.Enumerable>, "Count", [| elementType |], memberAccess.Expression) :> Expression
                    | _ -> actualExp
                OrderedChain.tryBuild adapter expression
        match ordered with
        | Some result -> qb.DuHandlerResult.Value <- ValueSome result; true
        | None ->
        match actualExp with
        | :? MemberExpression as memberAccess when isFileRefManyType memberAccess.Type && not qb.UpdateMode ->
            let owner =
                match sourceOverride.TryGetValue memberAccess with
                | true, owner -> ValueSome owner
                | _ -> tryGetDBRefManyOwnerRef qb memberAccess
            match owner with
            | ValueSome owner ->
                let value =
                    if not(QueryTranslatorVisitPostJoin.referenceSnapshotIncluded owner.Reference) then
                        SqlExpr.Literal(SqlLiteral.Null)
                    else
                        let struct(rows, _) = DBRefManyBuilderCore.buildRelationSource qb owner true
                        let alias = qb.SourceContext.NextAlias()
                        let aggregate = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.Column(Some alias, "Value")])
                        SqlExpr.ScalarSubquery({ Ctes = []; Body = SingleSelect(DBRefManyBuilderCore.mkSubCore [{ Alias = None; Expr = aggregate }] (Some(DerivedTable(rows, alias))) None) })
                qb.DuHandlerResult.Value <- ValueSome(QueryProduced.typed memberAccess.Type value)
                true
            | _ -> false
        | :? MethodCallExpression as call when ownsSource ->
            match call.Method.Name with
            | "Where" | "SelectMany" -> raise (NotSupportedException(filteredWhereUnsupportedTerminalMessage))
            | _ ->
                raise (NotSupportedException(
                    $"Error: Relation-backed operator '{call.Method.Name}' cannot be translated in this query shape.\n" +
                    "Fix: Use a supported LINQ composition or materialize the source before this operation."))
        | _ -> false

    /// Source acquisition and selected-owner binding precede one complete chain lowering.
    let private handleDBRefManyExpression (qb: QueryBuilder) (exp: Expression) =
        let rec hasRelationSource (expression: Expression) =
            if isNull expression then false
            elif isManyReferenceType expression.Type then true
            else
                match expression with
                | :? MethodCallExpression as call -> hasRelationSource (ChainExpr.getSource call)
                | :? MemberExpression as memberRead -> hasRelationSource memberRead.Expression
                | _ -> false
        if not(hasRelationSource exp) then false
        else
        let sources = System.Collections.Generic.Dictionary<Expression, DBRefManyOwnerRef>()
        let order = ResizeArray<Expression>()
        let isRoot expression =
            if isNull expression then false
            elif sources.ContainsKey expression then true
            elif not(isManyReferenceType expression.Type) then false
            else
                match tryGetDBRefManyOwnerRef qb expression with
                | ValueSome owner -> sources.Add(expression, owner); order.Add expression; true
                | _ -> false
        let rec prepare expression =
            match OrderedChainPlan.parse isRoot expression with
            | Some plan ->
                for stage in plan.Stages do
                    match stage with
                    | OrderedChainPlan.Set(_, right, _) -> prepare right
                    | _ -> ()
            | _ -> ()
        match exp with
        | :? MethodCallExpression as call -> prepare (ChainExpr.getSource call)
        | source when isManyReferenceType source.Type -> isRoot source |> ignore
        | :? MemberExpression as memberRead when not(isNull memberRead.Expression) -> prepare memberRead.Expression
        | _ -> ()
        let rec lower index =
            if index = order.Count then
                qb.DuHandlerResult.Value <- ValueNone
                if handleDBRefManySource qb exp sources then qb.DuHandlerResult.Value
                else ValueNone
            else
                let source = order[index]
                let owner = sources[source]
                match owner.Reference with
                | DirectValue _ -> lower (index + 1)
                | PackedValue _ ->
                    let result = QueryProduced.bind qb.SourceContext.NextAlias (fun value input ->
                        sources[source] <- { owner with Reference = DirectValue(value, input) }
                        lower (index + 1) |> ValueOption.defaultWith (fun () -> invalidOp "A resolved relation source requires its owning sequence lowerer.")) owner.Reference
                    ValueSome result
        let result = lower 0
        qb.DuHandlerResult.Value <- result
        result.IsSome

    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(QueryTranslatorVisitDbRefSingleRef.handleDBRefExpression))
    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefManyExpression))

    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(QueryTranslatorVisitGrouping.handle))

    /// Module initialization sentinel — accessing this value forces execution of module do-bindings.
    let internal handlerCount = preExpressionHandler.Count
