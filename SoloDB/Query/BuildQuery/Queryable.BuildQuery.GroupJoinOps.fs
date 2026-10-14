namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open NativeSQLiteTools
open Utils
open SoloDatabase
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.SqlModel
open SoloDatabase.GroupJoinRuntimeTypes
open SoloDatabase.QueryableBuildQueryGroupJoinElements
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.Metadata.Generation

/// GroupJoin source and direct aggregates; composed sequences use OrderedChain.
module internal QueryableBuildQueryGroupJoinOps =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperJoinBindings
    open QueryableHelperPreprocess
    open QueryableHelperBase

    let internal applyGroupJoin<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> TranslatedQuery)
        (expressions: Expression array) =
        match expressions.Length with
        | 4 ->
            let prepareInner = prepareInnerJoinSource "GroupJoin" sourceCtx translateQueryFn expressions.[0]
            let outerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin outer key selector" expressions.[1]
            let innerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin inner key selector" expressions.[2]
            let resultSelector = unwrapLambdaExpressionOrThrow "GroupJoin result selector" expressions.[3]

            if isCompositeJoinKeyBody outerKeySelector.Body || isCompositeJoinKeyBody innerKeySelector.Body then
                raise (NotSupportedException(
                    "Error: GroupJoin composite key selectors are not supported.\n" +
                    "Reason: Anonymous-type and composite key equality lowering is not available.\n" +
                    "Fix: Join on a single scalar key or move the query after AsEnumerable()."))

            if resultSelector.Parameters.Count <> 2 then
                raise (NotSupportedException(
                    "Error: GroupJoin result selector must have two parameters (outer, group).\n" +
                    "Reason: The result selector shape is not recognized.\n" +
                    "Fix: Use (outer, group) => new { ... } pattern."))

            let outerParam = resultSelector.Parameters.[0]
            let groupParam = resultSelector.Parameters.[1]

            addComplexFinal statements (fun ctx ->
                let outerAlias = "o"
                let innerAlias = "gj"
                let completion = QueryGroupCompletion("_groups" + sourceCtx.NextAlias(), sourceCtx.NextAlias)
                let struct(innerCtx, innerInput) = prepareInner ctx.Vars
                let outerSelect = QueryProduced.completedRows sourceCtx.NextAlias ctx.Inner ctx.Input

                let rowOuterCtx =
                    { sourceCtx with
                        LayerPosition = OuterLayer
                        SerializationInput = ctx.Input
                        Joins = ResizeArray() }

                let innerAggCtx =
                    { innerCtx with
                        Joins = ResizeArray() }

                let tryTranslateDbRefValueIdKey (input: QueryContext) (parameter: ParameterExpression) (tableAlias: string) (expr: Expression) =
                    match expr with
                    | :? MemberExpression as idMember when idMember.Member.Name = "Id" && typeof<int64>.Equals(idMember.Type) ->
                        match unwrapStructCopy idMember.Expression with
                        | :? MemberExpression as valueMember when QueryTranslatorBaseHelpers.isDBRefValueBoundary valueMember ->
                            match unwrapStructCopy valueMember.Expression with
                            | :? MemberExpression as reference when Object.ReferenceEquals(unwrapStructCopy reference.Expression, parameter) ->
                                let produced = translateJoinSingleSourceProduced input tableAlias ctx.Vars (Some parameter) reference
                                QueryTranslatorVisitPostJoin.validateReferenceNavigation produced |> ignore
                                Some(QueryProduced.referenceIdentity input.NextAlias produced)
                            | _ -> None
                        | _ -> None
                    | _ -> None

                let outerKeyExpr =
                    match tryTranslateDbRefValueIdKey rowOuterCtx outerKeySelector.Parameters.[0] outerAlias outerKeySelector.Body with
                    | Some translated -> translated
                    | None -> translateJoinSingleSourceExpression rowOuterCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body

                let outerInput = QueryTranslatorSerialization.scopeSerializationInput (Some outerAlias) OuterLayer ctx.Input
                let outerCompanions, publishedOuter =
                    QueryTranslatorSerialization.publishInput (fun () -> "_input" + sourceCtx.NextAlias()) outerInput
                let outerCtx =
                    { rowOuterCtx with
                        SerializationInput = QueryTranslatorSerialization.scopeSerializationInput (Some completion.Alias) OuterLayer publishedOuter
                        Joins = completion.SelectorJoins }
                let completedKey = completion.Bind(QueryProduced.typed outerKeySelector.ReturnType outerKeyExpr)
                                   |> QueryProduced.value sourceCtx.NextAlias

                // Lower the actual inner key once; reference edges stay beside that occurrence.
                let struct(innerSelect, keyName) =
                    publishInnerJoinKey innerCtx innerInput innerAlias (fun context alias ->
                        match tryTranslateDbRefValueIdKey context innerKeySelector.Parameters.[0] alias innerKeySelector.Body with
                        | Some value -> QueryProduced.typed innerKeySelector.ReturnType value
                        | None -> translateJoinSingleSourceProduced context alias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body)
                let innerSource = DerivedTable(innerSelect, innerAlias)
                let innerJoinKeyExpr = SqlExpr.Column(Some innerAlias, keyName)

                let replaceExpression (target: Expression) (replacement: Expression) (expr: Expression) =
                    let visitor =
                        { new ExpressionVisitor() with
                            override _.Visit(node: Expression) =
                                if isNull node then null
                                elif Object.ReferenceEquals(node, target) then replacement
                                else base.Visit(node) }
                    visitor.Visit(expr)

                let runtime =
                    { InnerCtx = innerCtx
                      InnerSelect = innerSelect
                      GroupParam = groupParam
                      OuterKeyExpr = completedKey
                      InnerParameter = innerKeySelector.Parameters.[0]
                      InnerKeyName = keyName
                      Vars = ctx.Vars
                      ReplaceExpression = replaceExpression }

                let orderedAdapter: OrderedChainPlan.Adapter = {
                    MembershipContext = DefaultMembership
                    LambdaContext = "group join"
                    Validate = fun plan ->
                        for stage in plan.Stages do
                            match stage with
                            | OrderedChainPlan.OfType(sourceType, _) | OrderedChainPlan.Cast(sourceType, _) -> DBRefManyHelpers.ensureOfTypeSupported sourceType
                            | _ -> ()
                    IsRoot = fun e -> obj.ReferenceEquals(e, groupParam)
                    IsValue = fun e -> QueryTranslatorBaseHelpers.isFullyConstant e || (innerCtx.BindQueryValue |> ValueOption.exists (fun b -> b.IsValue e))
                    Alias = fun () -> GroupJoinAliases.nextRowset innerCtx
                    Value = fun e -> translateJoinSingleSourceProduced outerCtx completion.Alias ctx.Vars (Some outerParam) e
                    Comparison = fun e -> QueryTranslator.translateComparisonScopedBody outerCtx completion.Alias e
                                               (System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>(Array.empty))
                                               (ValueSome(dict [outerParam, { Alias = Some completion.Alias; Context = outerCtx; Value = SourceParameter }] |> fun values -> Dictionary<ParameterExpression, ParameterSource>(values)))
                                               ValueNone false ctx.Vars
                    Translate = fun _ input a l handler ->
                        let sub = { innerCtx with LayerPosition = BaseLayer; SerializationInput = input; Joins=ResizeArray() }
                        let captures = Dictionary<ParameterExpression, ParameterSource>()
                        captures[outerParam] <- { Alias = Some completion.Alias; Context = outerCtx; Value = SourceParameter }
                        let value = QueryTranslator.translateScopedBody sub a l.Body l.Parameters (ValueSome captures) handler false ctx.Vars
                        value, QueryTranslatorReferenceJoin.preparedJoinClauses sub.Joins
                    Source = fun _ _ ->
                        let a = GroupJoinAliases.nextRowset innerCtx
                        let row = groupRows runtime a
                        let input = QueryTranslatorSerialization.scopeSerializationInput (Some a) OuterLayer innerInput.SerializationInput
                        let produced = QueryProduced.canonical innerCtx.NextAlias (DirectValue(SqlExpr.Column(Some a, "Value"), input))
                        QueryProduced.publishCore innerCtx.NextAlias row produced false
                }
                let groupProducer (expr: Expression) : QueryProducedValue voption =
                    let directAggregate =
                        match expr with
                        | :? MethodCallExpression as call
                            when call.Arguments.Count > 0
                                 && (typeof<System.Linq.Enumerable>.Equals(call.Method.DeclaringType)
                                     || typeof<System.Linq.Queryable>.Equals(call.Method.DeclaringType)) ->
                            match OrderedChainPlan.parse orderedAdapter.IsRoot call.Arguments.[0] with
                            | Some plan when plan.Stages.IsEmpty ->
                                let present = SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some innerAlias, "Id"))
                                let translateArgument (selector: LambdaExpression) =
                                    let captures = Dictionary<ParameterExpression, ParameterSource>()
                                    captures[outerParam] <- { Alias = Some outerAlias; Context = rowOuterCtx; Value = SourceParameter }
                                    QueryTranslator.translateCapturedBody innerAggCtx innerAlias selector.Body
                                        selector.Parameters (ValueSome captures) ctx.Vars
                                    |> QueryProduced.value sourceCtx.NextAlias
                                QueryGroupScalar.tryTranslate groupParam (Some present) translateArgument expr
                            | _ -> None
                        | _ -> None
                    match directAggregate with
                    | Some value -> ValueSome(completion.Bind(QueryProduced.typed expr.Type value))
                    | None ->
                        match expr with
                        | :? BinaryExpression as binary ->
                            // Correlated element comparisons consume the completed key
                            // beside its published group, rather than in the input SELECT.
                            match tryTranslateGroupFirstLikeNullComparison runtime binary with
                            | Some value -> ValueSome(QueryProduced.typed expr.Type value)
                            | None -> ValueNone
                        | :? MemberExpression as memberAccess when not(isNull memberAccess.Expression) ->
                            match tryMatchGroupElementCall runtime memberAccess.Expression with
                            | Some call -> ValueSome(buildGroupElementDispatch runtime call expr)
                            | None -> OrderedChain.tryBuild orderedAdapter expr |> ValueOption.ofOption
                        | _ -> OrderedChain.tryBuild orderedAdapter expr |> ValueOption.ofOption
                let captures = Dictionary<ParameterExpression, ParameterSource>()
                captures[outerParam] <- { Alias = Some completion.Alias; Context = outerCtx; Value = SourceParameter }
                let groupContext =
                    { innerCtx with
                        SerializationInput = QuerySerializationInput.Sequence(groupParam.Type, innerInput.SerializationInput) }
                captures[groupParam] <-
                    { Alias = Some completion.Alias; Context = groupContext
                      Value = DeferredParameter(lazy(OrderedChain.collection orderedAdapter groupParam groupParam.Type)) }
                let result = QueryTranslator.translateScopedBody outerCtx completion.Alias resultSelector.Body resultSelector.Parameters
                                 (ValueSome captures) (ValueSome groupProducer) true ctx.Vars

                let outerDiscoveredJoins =
                    QueryTranslatorReferenceJoin.preparedJoinClauses rowOuterCtx.Joins
                let discoveredJoins =
                    QueryTranslatorReferenceJoin.preparedJoinClauses innerAggCtx.Joins
                let allJoins =
                    outerDiscoveredJoins
                    @ [ConditionedJoin(
                        JoinKind.Left,
                        innerSource,
                        SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerJoinKeyExpr))]
                    @ discoveredJoins
                let grouped =
                    { mkCore
                        ([{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") };
                         { Alias = Some "Value"; Expr = SqlExpr.Column(Some outerAlias, "Value") }]
                        @ [{ Alias = Some "_tree_ordinal"; Expr = SqlExpr.Column(Some outerAlias, "_tree_ordinal") }]
                        @ outerCompanions)
                        (Some (DerivedTable(outerSelect, outerAlias)))
                      with
                          Joins = allJoins
                          GroupBy = [SqlExpr.Column(Some outerAlias, "_tree_ordinal")]
                          OrderBy = [{ Expr = SqlExpr.Column(Some outerAlias, "_tree_ordinal"); Direction = SortDirection.Asc }] }
                let completed =
                    { mkCore
                        [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some completion.Alias, "Id") }
                         { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }] None with
                        OrderBy = [{ Expr = SqlExpr.Column(Some completion.Alias, "_tree_ordinal"); Direction = SortDirection.Asc }] }
                let struct(query, output) = completion.Publish(grouped, completed, result)
                { Query = query; SerializationInput = output; KeyInput = ValueNone })
            ()
        | other ->
            raise (NotSupportedException(sprintf "Invalid number of arguments in GroupJoin: %A" other))
