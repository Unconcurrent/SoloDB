namespace SoloDatabase

open System
open SoloDatabase.Metadata.Generation
open SoloDatabase.QueryTranslatorBaseTypes
open System.Linq
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.ChainExpr

open SoloDatabase.OrderedChainPlan
open SoloDatabase.OrderedChainRows

/// Consumes completed ordered rowsets through shared terminal semantics.
module internal OrderedChain =
    let private supportedRight (adapter: Adapter) stages =
        let rec valid p =
            p.Stages |> List.forall (function Set(_, r, _) -> (parse adapter.IsRoot r |> Option.exists valid) || adapter.IsValue r | _ -> true)
        valid { Root=Expression.Constant(null);Stages=stages }

    let private materialize (adapter: Adapter) rows completedInput resultType elementType =
        let alias = adapter.Alias()
        let selected = QueryTranslatorSerialization.scopeSerializationInput (Some alias) BaseLayer completedInput
        let payload, selected = QueryProduced.embedded (col alias "Value", selected)
        let array = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.FunctionCall("json", [payload])])
        let input = QuerySerializationInput.Sequence(resultType, QueryTranslatorSerialization.rebaseSerializationInput completedInput)
        let descriptor = QuerySerializationSql.sequenceDescriptor resultType selected
        let scalar expression =
            SqlExpr.ScalarSubquery(select (core (Some(DerivedTable(rows, alias))) [proj "v" expression]))
        if QuerySerializationSql.isConstantDescriptor descriptor then DirectValue(scalar array, input)
        else
            let produced = DirectValue(array, { input with Operand = ValueSome { Descriptor = descriptor; Accesses = Array.empty } })
            PackedValue(scalar (QueryProduced.packet produced), input)

    /// A consumed group parameter requests its actual completed sequence once.
    let internal collection (adapter: Adapter) (expression: Expression) resultType =
        let struct(rows, input) = build adapter FullValue { Root = expression; Stages = [] }
        materialize adapter rows input resultType (sequenceElementType expression.Type)

    let private groupedQuantifier (adapter: Adapter) (plan: Plan) =
        let rec split preceding = function
            | Group(key, false, producedType) :: remaining -> Some(List.rev preceding, key, producedType, remaining)
            | stage :: remaining -> split (stage :: preceding) remaining
            | [] -> None
        match split [] plan.Stages with
        | Some(preceding, key, producedType, remaining) when
            remaining |> List.forall (function Filter predicate -> QueryGroupScalar.supportsPredicate predicate.Parameters[0] predicate.Body | _ -> false) ->
            let struct(rows, input) = build adapter FullValue { plan with Stages = preceding }
            let alias = adapter.Alias()
            let fields = [{ Alias = Some "Id"; Expr = col alias "Id" }
                          { Alias = Some "Value"; Expr = col alias "Value" }
                          { Alias = Some "__ord"; Expr = col alias "__ord" }]
                         @ QueryTranslatorSerialization.companionProjections (Some alias) input
            let struct(keyRows, keyInput, keyName, equalityNames) =
                prepareGrouping adapter plan.Root input alias (core (Some(DerivedTable(rows, alias))) fields) key
            let groupAlias = adapter.Alias()
            let item = QueryTranslatorSerialization.scopeSerializationInput (Some groupAlias) BaseLayer input
            let keyInput = QueryTranslatorSerialization.scopeSerializationInput (Some groupAlias) OuterLayer keyInput
            let groupInput = QuerySerializationInput.Grouping(producedType, keyInput, item)
            let joins = ResizeArray<JoinShape>()
            let predicates = remaining |> List.map (function
                | Filter predicate ->
                    let parameter = predicate.Parameters[0]
                    let argument (lambda: LambdaExpression) =
                        let produced, edges = adapter.Translate plan.Root item groupAlias (selector lambda) ValueNone
                        joins.AddRange edges
                        QueryProduced.value adapter.Alias produced
                    let handler expression =
                        if QueryGroupScalar.isKey parameter expression then ValueSome(DirectValue(col groupAlias keyName, keyInput))
                        else QueryGroupScalar.tryTranslate parameter None argument expression
                             |> Option.map (QueryProduced.typed expression.Type) |> ValueOption.ofOption
                    let produced, edges = adapter.Translate plan.Root groupInput groupAlias predicate (ValueSome handler)
                    joins.AddRange edges
                    QueryProduced.value adapter.Alias produced
                | _ -> invalidOp "A scalar grouped predicate requires its admitted filter.")
            let having = predicates |> function
                | [] -> None
                | first :: remaining -> Some(List.fold (fun left right -> SqlExpr.Binary(left, BinaryOperator.And, right)) first remaining)
            Some { core (Some(DerivedTable(keyRows, groupAlias))) [proj "v" (integer 1L)] with
                        Joins = List.ofSeq joins; GroupBy = equalityNames |> List.map (col groupAlias)
                        Having = having; Limit = Some(integer 1L) }
        | _ -> None

    /// Returns None before any translation when the complete chain is not owned here.
    /// Direct, unchained aggregates retain their specialized aggregate path.
    let rec private tryBuildCore (adapter: Adapter) (expression: Expression) =
        match expression with
        | :? MemberExpression as memberAccess when not (isNull memberAccess.Expression) ->
            tryBuildCore adapter memberAccess.Expression
            |> Option.map (fun value ->
                if DateTimeFunctions.isDateTimeLikeType (Utils.unwrapNullableType memberAccess.Expression.Type) then
                    QueryProduced.consume adapter.Alias (fun value input ->
                        let value = QueryProduced.scalarValue memberAccess.Expression.Type value input
                        DateTimeFunctions.translateGroupKeyMemberAccess value memberAccess.Expression.Type memberAccess.Member.Name) value
                    |> QueryProduced.typed memberAccess.Type
                else
                    match TypeSupport.OptionalMember(memberAccess.Expression.Type, memberAccess.Member.Name) with
                    | ValueSome OptionalAccess.Payload -> QueryProduced.bind adapter.Alias (fun value input -> DirectValue(value, QueryTranslatorSerialization.serializationPayload input)) value
                    | ValueSome OptionalAccess.Present -> QueryProduced.typed memberAccess.Type (SqlExpr.Unary(UnaryOperator.IsNotNull, QueryProduced.presence adapter.Alias value))
                    | ValueSome OptionalAccess.Absent -> QueryProduced.typed memberAccess.Type (SqlExpr.Unary(UnaryOperator.IsNull, QueryProduced.presence adapter.Alias value))
                    | _ -> QueryTranslatorVisitCoreMemberAccess.accessValue adapter.Alias false (ClrMember(memberAccess.Member.Name, memberAccess.Type)) memberAccess.Type value)
        | :? MethodCallExpression as call when typeof<Enumerable>.Equals(call.Method.DeclaringType) || typeof<Queryable>.Equals(call.Method.DeclaringType) ->
            validateOverload call
            let name = call.Method.Name
            let terminal = ["Count";"LongCount";"MinBy";"MaxBy";"ToArray";"ToList";"Any";"All";"Contains";"Sum";"Min";"Max";"Average";"First";"FirstOrDefault";"Last";"LastOrDefault";"Single";"SingleOrDefault";"ElementAt";"ElementAtOrDefault"] |> List.contains name
            if not terminal then
                // A sequence-valued projection is an existing terminal surface;
                // do not turn every intermediate operator into implicit materialization.
                let sequenceResult =
                    match name with
                    | "Select" | "Distinct" | "DistinctBy" | "Order" | "OrderDescending"
                    | "DefaultIfEmpty" | "Cast" | "OfType" | "CountBy"
                    | "UnionBy" | "IntersectBy" | "ExceptBy" -> true
                    | _ -> false
                match if sequenceResult then parse adapter.IsRoot expression else None with
                | Some p when not p.Stages.IsEmpty && supportedRight adapter p.Stages ->
                    let elementType=sequenceElementType expression.Type
                    tryBuildCore adapter (Expression.Call(typeof<Enumerable>,"ToArray",[|elementType|],expression))
                | _ -> None
            else
            match parse adapter.IsRoot (getSource call) with
            | Some plan when supportedRight adapter plan.Stages ->
                let arg = getArg call
                // Inspect the open signature: an explicit default can itself have a
                // delegate type, but it is a value, not a predicate or selector.
                let argumentLambda =
                    let method = if call.Method.IsGenericMethod then call.Method.GetGenericMethodDefinition() else call.Method
                    let parameters = method.GetParameters()
                    if parameters.Length > 1 then
                        let declared = parameters.[1].ParameterType
                        let requiresLambda =
                            typeof<Delegate>.IsAssignableFrom declared
                            || (declared.IsGenericType && typedefof<Expression<_>>.Equals(declared.GetGenericTypeDefinition()))
                        if requiresLambda then
                            match arg |> Option.bind lambda with
                            | Some value -> Some value
                            | None ->
                                raise (NotSupportedException(
                                    $"Error: Cannot translate {adapter.LambdaContext}.{name} predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."))
                        else None
                    else None
                let plan =
                    match name, argumentLambda with
                    | "All", Some pred ->
                        let inverted = { pred with Body = Expression.Not(pred.Body) }
                        { plan with Stages=plan.Stages @ [Filter inverted] }
                    | ("Count"|"LongCount"|"Any"|"First"|"FirstOrDefault"|"Last"|"LastOrDefault"|"Single"|"SingleOrDefault"), Some pred -> { plan with Stages=plan.Stages @ [Filter pred] }
                    | _ -> plan
                // A filter-only cardinality terminal does not observe input order.
                // Keep bounds, projections and set stages on their ordered rowset path.
                let simpleCardinality =
                    (name = "Count" || name = "LongCount" || name = "Any" || name = "All")
                    && (plan.Stages |> List.forall (function Filter _ | Order _ -> true | _ -> false))
                let orderUnobserved =
                    let rec eligible bounded = function
                        | [] -> true
                        | Filter _ :: tail when not bounded -> eligible bounded tail
                        | Order _ :: tail -> eligible bounded tail
                        | (Skip _ | Take _) :: tail -> eligible true tail
                        | _ -> false
                    (name = "Count" || name = "LongCount" || name = "Any" || name = "All")
                    && eligible false plan.Stages
                let plan =
                    if orderUnobserved then { plan with Stages = plan.Stages |> List.filter (function Order _ -> false | _ -> true) }
                    else plan
                let rowAdapter =
                    if orderUnobserved then
                        let sourceWithoutOrder columns root =
                            let struct(source, input) = adapter.Source columns root
                            let source =
                                match source.Body with
                                | SingleSelect row ->
                                    let projections =
                                        if simpleCardinality then
                                            row.Projections |> ProjectionSetOps.map (fun projection ->
                                                match projection.Alias, projection.Expr with
                                                | Some "__ord", WindowCall { Kind = WindowFunctionKind.RowNumber; Arguments = []; PartitionBy = []; OrderBy = [] } ->
                                                    { projection with Expr = integer 0L }
                                                | _ -> projection)
                                        else row.Projections
                                    { source with Body = SingleSelect { row with Projections = projections; OrderBy = [] } }
                                | _ -> source
                            struct(source, input)
                        { adapter with Source = sourceWithoutOrder }
                    else adapter
                let elementType = sequenceElementType (getSource call).Type
                let identityOnly =
                    (adapter.MembershipContext = ReceiverScopedEntityMembership)
                    && (name = "Count" || name = "LongCount" || name = "Any" ||
                        (name = "Contains" && (adapter.IsValue arg.Value || QueryDefaultEquality.canUseIdentityOnly elementType)))
                    && (plan.Stages |> List.forall (function
                        | Skip _ | Take _ -> true
                        | Distinct elementType ->
                            QueryDefaultEquality.canUseIdentityOnly elementType
                        | Order(selector, _, _) ->
                            match selector.Body with
                            | :? MemberExpression as memberRead ->
                                memberRead.Member.Name = "Id" && obj.ReferenceEquals(memberRead.Expression, selector.Parameters.[0])
                            | _ -> false
                        | _ -> false))
                let grouped =
                    if name = "Any" || name = "All" then groupedQuantifier rowAdapter plan else None
                let struct(rows, completedInput) =
                    match grouped with
                    | Some row -> struct(select row, (QueryProduced.typed call.Type (integer 1L)).Input)
                    | None when name = "Any" -> existenceRows rowAdapter (if identityOnly then IdentityOnly else FullValue) plan
                    | None -> build rowAdapter (if identityOnly then IdentityOnly else FullValue) plan
                let value expression = adapter.Value expression |> QueryProduced.value adapter.Alias
                let translate input alias selector =
                    let produced, joins = adapter.Translate plan.Root input alias selector ValueNone
                    QueryProduced.value adapter.Alias produced, joins
                let a = adapter.Alias()
                let source = Some(DerivedTable(rows,a))
                let terminalCore expression =
                    match rows.Body with
                    | SingleSelect row when simpleCardinality && rows.Ctes.IsEmpty ->
                        { row with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = expression }]; OrderBy = [] }
                    | _ -> core source [proj "v" expression]
                let scalar e = SqlExpr.ScalarSubquery(select (terminalCore e))
                let selectedInput = QueryTranslatorSerialization.scopeSerializationInput (Some a) BaseLayer completedInput
                let payload, selectedInput = QueryProduced.embedded (col a "Value", selectedInput)
                let output = { QueryTranslatorSerialization.rebaseSerializationInput completedInput with
                                Carrier = if QueryTranslatorBaseTypes.isSqlScalarResult call.Type then SqlScalar else CanonicalToken }
                let selectedValue = QueryProduced.scalarValue call.Type payload { selectedInput with Carrier = CanonicalToken }
                let selected expression row =
                    let produced = DirectValue(expression, { selectedInput with Carrier = output.Carrier })
                    let packet = QueryProduced.packet produced
                    PackedValue(SqlExpr.ScalarSubquery(select { row with Projections = ProjectionSetOps.ofList [proj "v" packet] }), output)
                let typed expression = QueryProduced.typed call.Type expression
                match name with
                | "MinBy" | "MaxBy" ->
                    let selector = argumentLambda |> Option.defaultWith (fun () -> raise (NotSupportedException("A keyed element terminal requires a selector lambda.")))
                    let key, joins = translate completedInput a selector
                    let direction = if name = "MinBy" then SortDirection.Asc else SortDirection.Desc
                    let order =
                        (if not selector.ReturnType.IsValueType || not (isNull (TypeMetadata.nullableUnderlying selector.ReturnType)) then
                            [{ Expr = SqlExpr.Unary(UnaryOperator.IsNull, key); Direction = SortDirection.Asc }]
                         else [])
                        @ [{ Expr = key; Direction = direction }; { Expr = col a "__ord"; Direction = SortDirection.Asc }]
                    Some(selected selectedValue { core source [] with Joins = joins; OrderBy = order; Limit = Some(integer 1L) })
                | "Count" | "LongCount" -> Some(typed(scalar (SqlExpr.AggregateCall(AggregateKind.Count,None,false,None))))
                | "ToArray" | "ToList" -> Some(materialize adapter rows completedInput call.Type elementType)
                | ("First"|"FirstOrDefault"|"Last"|"LastOrDefault"|"Single"|"SingleOrDefault"|"ElementAt"|"ElementAtOrDefault") ->
                    let direction = if name.StartsWith("Last",StringComparison.Ordinal) then SortDirection.Desc else SortDirection.Asc
                    let offset = if name.StartsWith("ElementAt",StringComparison.Ordinal) then arg |> Option.map value else None
                    let selectedCore =
                        { core source [] with
                            OrderBy = [{ Expr = col a "__ord"; Direction = direction }]
                            Limit = Some(integer 1L); Offset = offset }
                    let one = selected selectedValue selectedCore
                    let one =
                        if name.EndsWith("OrDefault", StringComparison.Ordinal)
                           && call.Type.IsValueType && isNull(TypeMetadata.nullableUnderlying call.Type) then
                            let zero = adapter.Value(Expression.Constant(RelationsAccessorCache.activateDefault call.Type, call.Type))
                            QueryProduced.coalesce adapter.Alias call.Type one zero
                        else one
                    let fallback =
                        match name with
                        | "FirstOrDefault" | "LastOrDefault" | "SingleOrDefault" ->
                            if call.Arguments.Count = 3 then Some(adapter.Value call.Arguments[2])
                            elif argumentLambda.IsNone then arg |> Option.map adapter.Value
                            else None
                        | _ -> None
                    if name.StartsWith("Single", StringComparison.Ordinal) then
                        let n = scalar(SqlExpr.AggregateCall(AggregateKind.Count, None, false, None))
                        let absent = QueryProduced.typed call.Type (SqlExpr.Literal SqlLiteral.Null)
                        let empty = fallback |> Option.defaultValue absent
                        let emptyOrMultiple = QueryProduced.choose call.Type (SqlExpr.Binary(n, BinaryOperator.Eq, integer 0L)) empty absent
                        Some(QueryProduced.choose call.Type (SqlExpr.Binary(n, BinaryOperator.Eq, integer 1L)) one emptyOrMultiple)
                    else
                        match fallback with
                        | None -> Some one
                        | Some fallback ->
                            let present = SqlExpr.Exists(select { core source [proj "v" (integer 1L)] with Limit = Some(integer 1L) })
                            Some(QueryProduced.choose call.Type present one fallback)
                | "Any" ->
                    match rows.Body with
                    | SingleSelect row when (orderUnobserved || not row.GroupBy.IsEmpty) && rows.Ctes.IsEmpty ->
                        let limit = if not row.GroupBy.IsEmpty && row.Limit.IsNone then Some(integer 1L) else row.Limit
                        let checkedRow =
                            { row with
                                Projections = ProjectionSetOps.ofList [proj "v" (integer 1L)]
                                OrderBy = []
                                Limit = limit }
                        Some(typed(SqlExpr.Exists(select checkedRow)))
                    | _ -> Some(typed(SqlExpr.Exists(select {terminalCore (integer 1L) with Limit=Some(integer 1L)})))
                | "Contains" ->
                    let input = QueryTranslatorSerialization.scopeSerializationInput (Some a) BaseLayer completedInput
                    let receiver = DirectValue(col a "Value", input)
                    let comparison = QueryDefaultEquality.contains adapter.Alias elementType adapter.MembershipContext (adapter.IsValue arg.Value)
                                         receiver (fun () -> adapter.Comparison arg.Value)
                                         (fun () -> adapter.Comparison(Expression.PropertyOrField(arg.Value, "Id")))
                                         (fun _ -> invalidOp "A root value matcher is not an ordered membership context.")
                    Some(typed(SqlExpr.Exists(select {core source [proj "v" (integer 1L)] with Where=Some comparison;Limit=Some(integer 1L)})))
                | "All" ->
                    let checkedRows =
                        match rows.Body with
                        | SingleSelect row when not row.GroupBy.IsEmpty && rows.Ctes.IsEmpty ->
                            { row with Projections = ProjectionSetOps.ofList [proj "v" (integer 1L)]
                                       OrderBy = []; Limit = Some(integer 1L) }
                        | _ -> { terminalCore (integer 1L) with Limit = Some(integer 1L) }
                    Some(typed(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists(select checkedRows))))
                | _ ->
                    let kind=match name with "Sum"->AggregateKind.Sum|"Min"->AggregateKind.Min|"Max"->AggregateKind.Max|_->AggregateKind.Avg
                    let value, joins =
                        match argumentLambda with
                        | Some selector -> translate completedInput a selector
                        | None -> rootValue a, []
                    let inputType = match argumentLambda with Some selector -> selector.ReturnType | None -> elementType
                    let agg=QueryTranslatorNumeric.aggregate call.Method inputType kind (FlattenTransform.normalizeExprQuoting value) false
                    let result = if name="Sum" then SqlExpr.Coalesce(agg,[QueryTranslatorNumeric.zero call.Method.ReturnType]) else agg
                    Some(typed(SqlExpr.ScalarSubquery(select { terminalCore result with Joins = joins })))
            | _ -> None
        | _ -> None

    /// Source adapters select direct aggregates before calling this terminal owner.
    let tryBuild adapter expression = tryBuildCore adapter expression
