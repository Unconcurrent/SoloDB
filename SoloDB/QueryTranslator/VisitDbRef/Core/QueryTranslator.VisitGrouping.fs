namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorReferenceJoin
open SoloDatabase.QueryTranslatorVisitCore

/// Stored sequences and completed groups share the existing ordered rowset owner.
module internal QueryTranslatorVisitGrouping =
    let private actualGrouping (builder: QueryBuilder) (parameter: ParameterExpression) =
        let input = (builder.ResolveParameterSource parameter).SerializationInput
        if isNull input.OriginalType then false else
        match input.Shape with
        | ValueSome(JsonSerializator.JsonSerializationShape.GroupingValues _) -> false
        | _ ->
            match JsonSerializator.JsonValue.SerializedValueFacts(Utils.optionalPayloadType input.OriginalType) with
            | JsonSerializator.JsonSerializationShape.GroupingValues _ -> true
            | _ -> false

    let internal handle (builder: QueryBuilder) (expression: Expression) =
        let storedSequence (value: Expression) =
            if DBRefTypeHelpers.isManyReferenceType value.Type then false else
            match JsonSerializator.JsonValue.SerializedValueFacts value.Type with
            | JsonSerializator.JsonSerializationShape.SequenceValues _ -> true
            | _ -> false
        let rec root (expression: Expression) =
            match expression with
            | :? MethodCallExpression as call when call.Arguments.Count > 0 -> root call.Arguments[0]
            | :? UnaryExpression as unary when isReferenceUpcast unary -> root unary.Operand
            | :? ParameterExpression as parameter when actualGrouping builder parameter -> Some(expression)
            | _ when storedSequence expression -> Some expression
            | :? MemberExpression as memberAccess when not(isNull memberAccess.Expression) -> root memberAccess.Expression
            | :? BoundValueAccessExpression as access -> root access.Parent
            | _ -> None
        match root expression with
        | None -> false
        | Some parameter ->
            let translateWith lower _ input alias (selector: ChainExpr.Selector) handler =
                let child = builder.ForSubquery(alias, selector.Parameters, BaseLayer, input)
                let child = { child with ProducedHandler = QueryGroupScalar.withFallback handler child.ProducedHandler }
                let value = lower selector.Body child
                let joins = preparedJoinClauses child.SourceContext.Joins
                value, joins
            let adapter: OrderedChainPlan.Adapter = {
                MembershipContext = DefaultMembership
                LambdaContext = "group"
                Validate = ignore
                IsRoot = fun source -> obj.ReferenceEquals(source, parameter)
                IsValue = fun source -> isFullyConstant source || (builder.SourceContext.BindQueryValue |> ValueOption.exists (fun binder -> binder.IsValue source))
                Alias = builder.SourceContext.NextAlias
                Variables = builder.Variables
                DefaultValue = QueryDefault.value builder.Variables
                Value = fun value -> visitProduced value builder
                Comparison = fun value -> visitComparisonProduced value builder
                Translate = translateWith visitProduced
                TranslateOrderKey = translateWith (QueryStructuralOrdering.translate visitProduced)
                Source = fun _ source ->
                    let produced = visitProduced source builder |> QueryProduced.sequence builder.SourceContext.NextAlias
                    let struct(collection,input),bindings = QueryProduced.bindTerms builder.SourceContext.NextAlias (fun value input -> struct(value,input)) produced
                    let alias = builder.SourceContext.NextAlias()
                    let fullKey = SqlExpr.Column(Some alias, "fullkey")
                    let key = SqlExpr.Column(Some alias, "key")
                    let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(collection, BinaryOperator.JsonText, fullKey)])
                    let item = QueryTranslatorSerialization.elementInput input key token
                    let itemType = ChainExpr.sequenceElementType parameter.Type
                    let id = if hasPhysicalRowIdentity itemType then SqlExpr.FunctionCall("jsonb_extract", [token; SqlExpr.Literal(SqlLiteral.String "$.Id")]) else key
                    let value = QueryProduced.scalarValue itemType token { item with Carrier = CanonicalToken }
                    let produced = QueryProduced.canonical builder.SourceContext.NextAlias (DirectValue(value, item))
                    let source,joins =
                        match bindings with
                        | [] -> FromJsonEach(collection,Some alias),[]
                        | CrossJoin source :: rest -> source,rest @ [CrossJoin(FromJsonEach(collection,Some alias))]
                        | _ -> invalidOp "A bound sequence requires its original row binding."
                    let row =
                        { OrderedChainRows.core (Some source)
                              [{ Alias = Some "Id"; Expr = id }; { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }; { Alias = Some "__ord"; Expr = key }] with
                            Joins = joins
                            OrderBy = [{ Expr = key; Direction = SortDirection.Asc }] }
                    QueryProduced.publishCore builder.SourceContext.NextAlias row produced false
            }
            match OrderedChain.tryBuild adapter expression with
            | Some value -> builder.DuHandlerResult.Value <- ValueSome value; true
            | None -> false
