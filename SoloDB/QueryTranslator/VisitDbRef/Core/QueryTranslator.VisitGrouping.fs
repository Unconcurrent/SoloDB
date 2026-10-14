namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorReferenceJoin
open SoloDatabase.QueryTranslatorVisitCore

/// Completed grouping rows supply their published sequence to the ordered owner.
module internal QueryTranslatorVisitGrouping =
    let private actualGrouping (builder: QueryBuilder) (parameter: ParameterExpression) =
        let input = (builder.ResolveParameterSource parameter).SerializationInput
        if isNull input.OriginalType then false else
        match JsonSerializator.JsonValue.SerializedValueFacts(Utils.optionalPayloadType input.OriginalType), input.Shape with
        | JsonSerializator.JsonSerializationShape.GroupingValues _, ValueSome(JsonSerializator.JsonSerializationShape.GroupingValues _) -> false
        | JsonSerializator.JsonSerializationShape.GroupingValues _, _ -> true
        | _ -> false

    let internal handle (builder: QueryBuilder) (expression: Expression) =
        let rec root (expression: Expression) =
            match expression with
            | :? ParameterExpression as parameter when actualGrouping builder parameter -> Some parameter
            | :? MethodCallExpression as call when call.Arguments.Count > 0 -> root call.Arguments[0]
            | :? MemberExpression as memberAccess when not(isNull memberAccess.Expression) -> root memberAccess.Expression
            | :? UnaryExpression as unary when isReferenceUpcast unary -> root unary.Operand
            | _ -> None
        match root expression with
        | None -> false
        | Some parameter ->
            let adapter: OrderedChainPlan.Adapter = {
                MembershipContext = DefaultMembership
                LambdaContext = "group"
                Validate = ignore
                IsRoot = fun source -> obj.ReferenceEquals(source, parameter)
                IsValue = fun source -> isFullyConstant source || (builder.SourceContext.BindQueryValue |> ValueOption.exists (fun binder -> binder.IsValue source))
                Alias = builder.SourceContext.NextAlias
                Value = fun value -> visitProduced value builder
                Comparison = fun value -> visitComparisonProduced value builder
                Translate = fun _ input alias selector handler ->
                    let child = builder.ForSubquery(alias, selector.Parameters, BaseLayer, input)
                    let child = { child with ProducedHandler = QueryGroupScalar.withFallback handler child.ProducedHandler }
                    let value = visitProduced selector.Body child
                    let joins = preparedJoinClauses child.SourceContext.Joins
                    value, joins
                Source = fun _ source ->
                    let produced = visitProduced source builder |> QueryProduced.sequence builder.SourceContext.NextAlias
                    match produced with
                    | PackedValue _ -> invalidOp "A grouping parameter requires its completed row binding."
                    | DirectValue(collection, input) ->
                        let alias = builder.SourceContext.NextAlias()
                        let fullKey = SqlExpr.Column(Some alias, "fullkey")
                        let key = SqlExpr.Column(Some alias, "key")
                        let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(collection, BinaryOperator.JsonText, fullKey)])
                        let item = QueryTranslatorSerialization.elementInput input key token
                        let itemType = ChainExpr.sequenceElementType parameter.Type
                        let id = if hasPhysicalRowIdentity itemType then SqlExpr.FunctionCall("jsonb_extract", [token; SqlExpr.Literal(SqlLiteral.String "$.Id")]) else key
                        let value = QueryProduced.scalarValue itemType token { item with Carrier = CanonicalToken }
                        let produced = QueryProduced.canonical builder.SourceContext.NextAlias (DirectValue(value, item))
                        let row =
                            { OrderedChainRows.core (Some(FromJsonEach(collection, Some alias)))
                                  [{ Alias = Some "Id"; Expr = id }; { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }; { Alias = Some "__ord"; Expr = key }] with
                                OrderBy = [{ Expr = key; Direction = SortDirection.Asc }] }
                        QueryProduced.publishCore builder.SourceContext.NextAlias row produced false
            }
            match OrderedChain.tryBuild adapter expression with
            | Some value -> builder.DuHandlerResult.Value <- ValueSome value; true
            | None -> false
