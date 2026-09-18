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
    let private handleDBRefManyExpression (qb: QueryBuilder) (exp: Expression) : bool =
        let actualExp =
            match exp with
            | :? LambdaExpression as le -> le.Body
            | _ -> exp
        // Normalize the existing relation API into LINQ terminals before dispatch.
        // These calls must not fall through to stored JSON-array translation.
        let actualExp =
            match actualExp with
            | :? MethodCallExpression as call when call.Method.DeclaringType = typeof<Extensions>
                                                   && call.Method.Name = "Any" && call.Arguments.Count = 2
                                                   && isDBRefManyType (unwrapConvert call.Arguments.[0]).Type ->
                match tryExtractLambdaExpression call.Arguments.[1] with
                | ValueSome predicate ->
                    Expression.Call(typeof<System.Linq.Enumerable>, "Any", call.Method.GetGenericArguments(),
                                    call.Arguments.[0], predicate) :> Expression
                | _ ->
                    raise (NotSupportedException(
                        "Error: Cannot translate relation-backed DBRefMany.Any predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."))
            | :? MethodCallExpression as call when call.Method.Name = "Contains"
                                                   && not (isNull call.Object) && isDBRefManyType call.Object.Type
                                                   && call.Arguments.Count = 1 ->
                let elementType = ChainExpr.sequenceElementType call.Object.Type
                Expression.Call(typeof<System.Linq.Enumerable>, "Contains", [| elementType |], call.Object, call.Arguments.[0]) :> Expression
            | _ -> actualExp
        let mutable ownsSource = false
        let ordered =
            let rec root (e: Expression) =
                if isDBRefManyType e.Type then Some e else
                match e with
                | :? MethodCallExpression as call when call.Arguments.Count > 0 -> root call.Arguments.[0]
                | :? MemberExpression as memberAccess when not (isNull memberAccess.Expression) -> root memberAccess.Expression
                | _ -> None
            match root actualExp with
            | None -> None
            | Some _ ->
                let sources = System.Collections.Generic.Dictionary<Expression, DBRefManyDescriptor.DBRefManyOwnerRef>()
                let isRoot expression =
                    if sources.ContainsKey expression then true
                    elif not (isDBRefManyType expression.Type) then false
                    else
                        match tryGetDBRefManyOwnerRef qb expression with
                        | ValueSome owner ->
                            ownsSource <- true
                            sources.Add(expression, owner)
                            true
                        | _ -> false
                let targetTable source =
                    let owner = sources.[source]
                    let targetType = owner.PropertyExpr.Type.GetGenericArguments().[0]
                    DBRefManyBuilderCore.resolveTargetTable qb.SourceContext owner.OwnerCollection owner.PropertyExpr.Member.Name targetType
                let adapter: OrderedChainPlan.Adapter = {
                    EntityMembershipById = true
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
                    Value = fun e -> visitDu e qb
                    Translate = fun source alias lambda ->
                        let sub = qb.ForSubquery(alias, lambda, subqueryRootTable = targetTable source)
                        let value = visitDu lambda.Body sub
                        value, DBRefManyHelpers.joinEdgesToClauses sub.SourceContext.Joins
                    Source = fun columns source ->
                        let id, payload, row, _ = DBRefManyBuilderCore.buildRelationCore qb sources.[source] (columns = OrderedChainPlan.FullValue) []
                        let projections =
                            [{ Alias = Some "Id"; Expr = id }
                             { Alias = Some "Value"; Expr = payload }
                             { Alias = Some "__ord"; Expr = id }]
                        let orderedRow =
                            { row with
                                Projections = ProjectionSetOps.ofList projections
                                OrderBy = [{ Expr = id; Direction = SortDirection.Asc }] }
                        { Ctes = []; Body = SingleSelect orderedRow }
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
        | :? MethodCallExpression as call when ownsSource ->
            match call.Method.Name with
            | "Where" | "SelectMany" -> raise (NotSupportedException(filteredWhereUnsupportedTerminalMessage))
            | _ ->
                raise (NotSupportedException(
                    $"Error: Relation-backed operator '{call.Method.Name}' cannot be translated in this query shape.\n" +
                    "Fix: Use a supported LINQ composition or materialize the source before this operation."))
        | _ -> false

    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(QueryTranslatorVisitDbRefSingleRef.handleDBRefExpression))
    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefManyExpression))

    /// Module initialization sentinel — accessing this value forces execution of module do-bindings.
    let internal handlerCount = preExpressionHandler.Count
