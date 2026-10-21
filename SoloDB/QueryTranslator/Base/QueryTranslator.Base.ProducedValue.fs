namespace SoloDatabase

open System
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QuerySerializationSql

/// Carries a selected value and its writer facts through one SQL binding.
module internal QueryProduced =
    let typed target value =
        let input = QuerySerializationInput.ForType target
        DirectValue(value, { input with Source = ValueSome(jsonToken value); Carrier = (if isSqlScalarResult target then SqlScalar else CanonicalToken) })

    /// Parameter metadata comes from the same acquisition as the parameter value.
    let parameter target expression writer =
        let produced = typed target expression
        match writer with
        | ValueNone -> produced
        | ValueSome writer ->
            let token = jsonToken expression
            let descriptor = SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 0L); writer; writerHeader token]
            DirectValue(expression, { produced.Input with Source = ValueSome token;
                                                        Operand = ValueSome { Descriptor = descriptor; Accesses = Array.empty } })

    /// Dynamic publication retains the original token beside its native scalar facet.
    let private publicationToken value (input: QuerySerializationInput) =
        match input.Facts, input.Source with
        | JsonSerializator.JsonSerializationShape.Dynamic, ValueSome token -> token
        | _ -> jsonToken value
    let packet = function
        | DirectValue(value, input) -> SqlExpr.JsonArrayExpr [publicationToken value input; inputDescriptor input]
        | PackedValue(packet, _) -> packet
    let private rawValue = function
        | DirectValue(value, input) ->
            let token = input.Source |> ValueOption.defaultWith (fun () -> canonicalToken input.Carrier value)
            DirectValue(token, { input with Carrier = CanonicalToken; Source = ValueSome token })
        | produced -> produced
    let private rawPacket produced = packet (rawValue produced)
    let private extracted column position =
        SqlExpr.FunctionCall("jsonb_extract", [column; SqlExpr.Literal(SqlLiteral.String ("$[" + string position + "]"))])

    /// Produced legacy character text stays raw until its actual consumer finishes it.
    let scalarValue target value (input: QuerySerializationInput) =
        let scalar =
            if input.Carrier = CanonicalToken && isSqlScalarResult target then
                SqlExpr.FunctionCall("jsonb_extract", [value; SqlExpr.Literal(SqlLiteral.String "$")])
            else value
        if input.LegacyCharacterText && typeof<char>.Equals(Utils.optionalPayloadType target) then scalar
        else QueryTranslatorNumeric.storedValue target scalar

    /// Comparison finishing retains the actual scalar producer behind transparent boxing.
    let comparisonValue target value (input: QuerySerializationInput) =
        let actual = if typeof<obj>.Equals target then input.OriginalType else target
        let value = scalarValue actual value input
        if typeof<obj>.Equals target && input.Facts = JsonSerializator.JsonSerializationShape.Dynamic then
            match input.Source with ValueSome token -> QueryTranslatorNumeric.nativeScalar token | ValueNone -> value
        else QueryTranslatorCharacters.comparable actual value input
    let private tokenValue (input: QuerySerializationInput) token =
        if input.Carrier = CanonicalToken then token
        elif isSqlScalarResult input.OriginalType then scalarValue input.OriginalType token { input with Carrier = CanonicalToken }
        else SqlExpr.FunctionCall("jsonb_extract", [token; SqlExpr.Literal(SqlLiteral.String "$")])
    let private bound consumeToken alias (input: QuerySerializationInput) =
        let column = SqlExpr.Column(Some alias, "value")
        let token = tupleLeafToken column 0
        let input = { fromDescriptor (extracted column 1) input with Source = ValueSome token; SourceOffset = 0 }
        consumeToken input token, input

    /// A row already bound to the canonical packet keeps its selected writer.
    let fromBoundPacket alias input =
        let value, input = bound tokenValue alias input
        DirectValue(value,input)
    let private singleton alias packet expression =
        SqlExpr.ScalarSubquery {
            Ctes = []
            Body = SingleSelect {
                Distinct = false
                Projections = Explicit({ Expr = expression; Alias = None }, [])
                Source = Some(FromJsonEach(SqlExpr.JsonArrayExpr [packet], Some alias))
                Joins = []; Where = None; GroupBy = []; Having = None; OrderBy = []
                Limit = None; Offset = None } }
    let private invariantLeaf (input: QuerySerializationInput) =
        match input.Facts with
        | shape when shape.IsScalarCarrier -> true
        | JsonSerializator.JsonSerializationShape.Uninterpreted -> true
        | _ -> false

    /// The callback consumes the value and descriptor from the same bound row.
    let consume nextName consume = function
        | DirectValue(value, input) -> consume value input
        | PackedValue(packet, input) ->
            let alias = nextName()
            let value, input = bound tokenValue alias input
            singleton alias packet (consume value input)

    /// One selected key can supply several terms without replaying its packet.
    let bindTerms nextName consume produced =
        let complex value = SqlExpr.exists (function ScalarSubquery _ | Exists _ | InSubquery _ -> true | _ -> false) value
        let produced =
            match produced with
            | DirectValue(value, _) when complex value -> PackedValue(packet produced, rebaseSerializationInput produced.Input)
            | _ -> produced
        match produced with
        | DirectValue(value, input) -> consume value input, []
        | PackedValue(packet, input) ->
            let alias = nextName()
            let value, input = bound tokenValue alias input
            consume value input, [CrossJoin(FromJsonEach(SqlExpr.JsonArrayExpr [packet], Some alias))]

    /// A transparent or child operation preserves its actual producer facts.
    let private bindCore raw nextName produce = function
        | DirectValue(value, input) -> produce value input
        | PackedValue(source, input) ->
            let alias = nextName()
            let value, input =
                if raw then
                    let value, input = bound (fun _ token -> token) alias input
                    value, { input with Carrier = CanonicalToken }
                else bound tokenValue alias input
            let result: QueryProducedValue = produce value input
            match result with
            | DirectValue(value, resultInput) when resultInput.OriginalStringUnits.IsNone && invariantLeaf resultInput && isConstantDescriptor(inputDescriptor resultInput) ->
                DirectValue(singleton alias source value, rebaseSerializationInput resultInput)
            | _ -> PackedValue(singleton alias source (packet result), rebaseSerializationInput result.Input)
    let bind nextName produce produced = bindCore false nextName produce produced
    let map nextName map produced =
        match produced with
        | DirectValue(value, input) -> DirectValue(map value, input)
        | PackedValue(source, input) ->
            let alias = nextName()
            let value, boundInput = bound tokenValue alias input
            let mapped = map value
            if obj.ReferenceEquals(value, mapped) then produced
            else PackedValue(singleton alias source (packet (DirectValue(mapped, boundInput))), rebaseSerializationInput input)

    /// A nullable native scalar enters tree operations without losing SQL absence.
    let treeToken nextName value (input: QuerySerializationInput) =
        if input.Carrier = CanonicalToken then value
        else
            let alias = nextName()
            let scalar = SqlExpr.Column(Some alias, "value")
            let token = SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, scalar), SqlExpr.Literal SqlLiteral.Null), [], Some(jsonToken scalar))
            singleton alias value token

    /// A constructed entity child retains its own physical identity in its payload.
    let embedded (value, input: QuerySerializationInput) =
        match input.PhysicalOwner with
        | ValueSome owner when hasPhysicalRowIdentity input.OriginalType ->
            let identity = owner.Identity
            let value = SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, identity), value), [],
                            Some(withRowIdentity value identity))
            value, { input with Source = ValueSome(jsonToken value) }
        | ValueSome owner when typeof<JsonSerializator.JsonValue>.IsAssignableFrom input.OriginalType ->
            let value = withTreeRowIdentity value owner.Identity
            value, { input with Source = ValueSome(jsonToken value) }
        | _ when input.Facts = JsonSerializator.JsonSerializationShape.Dynamic ->
            let token = publicationToken value input
            token, { input with Source = ValueSome token; Carrier = CanonicalToken }
        | _ -> value, input

    /// Component packets are evaluated in their authored construction order.
    let private constructCore raw nextName (produce: (SqlExpr * QuerySerializationInput) array -> QueryProducedValue) (components: QueryProducedValue array) =
        if components |> Array.exists (function PackedValue _ -> true | _ -> false) then
            let alias = nextName()
            let column = SqlExpr.Column(Some alias, "value")
            let values = components |> Array.mapi (fun index part ->
                let child = extracted column index
                let token = tupleLeafToken child 0
                let input = { fromDescriptor (extracted child 1) part.Input with Source = ValueSome token; SourceOffset = 0 }
                if raw then token, { input with Carrier = CanonicalToken } else tokenValue input token, input)
            let result = produce values
            let source = SqlExpr.JsonArrayExpr(components |> Array.map (if raw then rawPacket else packet) |> Array.toList)
            PackedValue(singleton alias source (packet result), rebaseSerializationInput result.Input)
        else
            produce (components |> Array.map (function DirectValue(value, input) -> value, input | _ -> invalidOp "A direct construction component is required."))
    let construct nextName produce components = constructCore false nextName produce components

    /// Presence-sensitive comparisons defer scalar conversion until their selected branch.
    let constructRaw nextName produce components = constructCore true nextName produce components
    let private referenceToken value (input: QuerySerializationInput) =
        input.Source |> ValueOption.defaultWith (fun () -> jsonToken value)

    /// Reference operations consume the actual paired token before any snapshot extraction.
    let referenceIdentity nextName produced =
        consume nextName (fun value input ->
            QueryTranslatorReferenceJoin.identityFromToken QueryTranslatorReferenceJoin.StoredOrHydratedId (referenceToken value input)) produced
    let referenceOperand nextName referenceType produced =
        consume nextName (fun value input -> QueryTranslatorReferenceJoin.defaultOperand referenceType (referenceToken value input)) produced

    /// CASE selects the complete result once; metadata never evaluates its condition again.
    let choose target condition (whenTrue: QueryProducedValue) (whenFalse: QueryProducedValue) =
        let input = mergeInputs target [|whenTrue.Input; whenFalse.Input|]
        match whenTrue, whenFalse with
        | DirectValue(first, firstInput), DirectValue(last, lastInput) ->
            let descriptor = inputDescriptor firstInput
            if firstInput.OriginalStringUnits.IsNone && lastInput.OriginalStringUnits.IsNone &&
               invariantLeaf firstInput && firstInput.Carrier = lastInput.Carrier &&
               isConstantDescriptor descriptor && descriptor = inputDescriptor lastInput then
                DirectValue(SqlExpr.CaseExpr((condition, first), [], Some last), rebaseSerializationInput firstInput)
            else PackedValue(SqlExpr.CaseExpr((condition, packet whenTrue), [], Some(packet whenFalse)), input)
        | _ -> PackedValue(SqlExpr.CaseExpr((condition, packet whenTrue), [], Some(packet whenFalse)), input)
    let chooseRaw target condition whenTrue whenFalse =
        choose target condition (rawValue whenTrue) (rawValue whenFalse)

    /// A selected row has a packet even when its payload is null.
    let defaultIfEmpty target selected fallback =
        match selected with
        | PackedValue(selected, input) ->
            PackedValue(SqlExpr.Coalesce(selected, [packet fallback]), mergeInputs target [|input; fallback.Input|])
        | _ -> invalidArg (nameof selected) "A default selection requires a row packet."

    /// A nullable result chooses its fallback lazily from the bound left value.
    let private coalesceCore raw nextName target left right =
        let left = PackedValue((if raw then rawPacket left else packet left), rebaseSerializationInput left.Input)
        bindCore raw nextName (fun value input ->
            let present = SqlExpr.Unary(UnaryOperator.IsNotNull, nullCarrier input.Carrier value)
            (if raw then chooseRaw else choose) target present (DirectValue(value, input)) right) left
    let coalesce nextName target left right = coalesceCore false nextName target left right
    let coalesceRaw nextName target left right = coalesceCore true nextName target left right
    let value nextName produced = consume nextName (fun value _ -> value) produced

    /// Ordered stages publish a canonical token while retaining its writer facts.
    let canonical nextName produced =
        bind nextName (fun value input ->
            let token =
                if input.Facts = JsonSerializator.JsonSerializationShape.Dynamic then publicationToken value input
                else canonicalToken input.Carrier value
            DirectValue(token, { input with Carrier = CanonicalToken })) produced

    /// A serialized grouping publishes its sequence under its authored Items child.
    let sequence nextName produced =
        bind nextName (fun value input ->
            let shape =
                match input.Shape with
                | ValueSome(JsonSerializator.JsonSerializationShape.GroupingValues _ as shape) -> shape
                | _ -> JsonSerializator.JsonValue.SerializedValueFacts(Utils.optionalPayloadType input.OriginalType)
            match shape with
            | JsonSerializator.JsonSerializationShape.GroupingValues _ ->
                let input = { input with Source = ValueSome(jsonToken value) }
                let struct(child, _) = serializationAccess input (StoredLabel(box "Items"))
                DirectValue(child.Source.Value, { child with Carrier = CanonicalToken })
            | _ -> DirectValue(value, input)) produced

    /// The original error row survives before any selected physical identity.
    let outputIdentity preserveErrors fallback (input: QuerySerializationInput) original =
        let selected = input.PhysicalOwner |> ValueOption.map (fun owner -> SqlExpr.Coalesce(owner.Identity, [fallback]))
                                          |> ValueOption.defaultValue fallback
        if selected = original then original
        elif preserveErrors then SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, original), original), [], Some selected)
        else selected

    /// Publication binds a packed result after the producer's grouping and bounds.
    let publishSlot nextName slot (core: SelectCore) produced preserveErrors =
        let projections = ProjectionSetOps.toList core.Projections
        let publishIdentity (input: QuerySerializationInput) (projection: Projection) =
            match slot, input.PhysicalOwner, projection.Alias, projection.Expr with
            | "Value", ValueSome owner, Some "Id", _
            | "Value", ValueSome owner, None, SqlExpr.Column(_, "Id") ->
                let identity = outputIdentity preserveErrors projection.Expr input projection.Expr
                if owner.Identity = projection.Expr then projection
                else { Alias = Some(defaultArg projection.Alias "Id"); Expr = identity }
            | _ -> projection
        let replaceValue value input = projections |> List.map (fun projection ->
            if projection.Alias = Some slot then { projection with Expr = value } else publishIdentity input projection)
        match produced with
        | DirectValue(value, input) ->
            let value =
                if input.Facts = JsonSerializator.JsonSerializationShape.Dynamic then publicationToken value input
                else value
            let companions, output = publishInput nextName input
            struct({ Ctes = []; Body = SingleSelect { core with Projections = ProjectionSetOps.ofList(replaceValue value input @ companions) } }, output)
        | PackedValue(packet, input) ->
            let sourceAlias = nextName()
            let packetName = "_value" + nextName()
            let elementAlias = nextName()
            let orderNames = core.OrderBy |> List.map (fun _ -> "_order" + nextName())
            let innerProjections = projections |> List.map (fun projection ->
                if projection.Alias = Some slot then { Expr = packet; Alias = Some packetName }
                else
                    match projection.Alias, projection.Expr with
                    | None, SqlExpr.Column(_, name) -> { projection with Alias = Some name }
                    | _ -> projection)
            let orderProjections = List.map2 (fun name ordering -> { Alias = Some name; Expr = ordering.Expr }) orderNames core.OrderBy
            let innerCore =
                { core with
                    Projections = ProjectionSetOps.ofList(innerProjections @ orderProjections)
                    OrderBy = List.map2 (fun name ordering -> { ordering with Expr = SqlExpr.Column(None, name) }) orderNames core.OrderBy }
            let inner = { Ctes = []; Body = SingleSelect innerCore }
            let selected, selectedInput = bound tokenValue elementAlias input
            let selected =
                if selectedInput.Facts = JsonSerializator.JsonSerializationShape.Dynamic then publicationToken selected selectedInput
                else selected
            let selected =
                if preserveErrors then
                    let error = extracted (SqlExpr.Column(Some elementAlias, "value")) 0
                    SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, SqlExpr.Column(Some sourceAlias, "Id")), error), [], Some selected)
                else selected
            let descriptorName = "_input" + nextName()
            let outerProjections = projections |> List.map (fun projection ->
                if projection.Alias = Some slot then { projection with Expr = selected }
                else
                    let name =
                        match projection.Alias, projection.Expr with
                        | Some name, _ | None, SqlExpr.Column(_, name) -> name
                        | _ -> invalidOp "A completed projection requires a declared column name."
                    publishIdentity selectedInput { projection with Expr = SqlExpr.Column(Some sourceAlias, name) })
            let descriptor = SqlExpr.Column(None, descriptorName)
            let output = fromDescriptor descriptor (rebaseSerializationInput input)
            struct({ Ctes = []; Body = SingleSelect {
                        Distinct = false
                        Projections = ProjectionSetOps.ofList(outerProjections @ [{ Alias = Some descriptorName; Expr = inputDescriptor selectedInput }])
                        Source = Some(DerivedTable(inner, sourceAlias))
                        Joins = [CrossJoin(FromJsonEach(SqlExpr.JsonArrayExpr [SqlExpr.Column(Some sourceAlias, packetName)], Some elementAlias))]
                        Where = None; GroupBy = []; Having = None
                        OrderBy = List.map2 (fun name ordering -> { ordering with Expr = SqlExpr.Column(Some sourceAlias, name) }) orderNames core.OrderBy
                        Limit = None; Offset = None } }, output)
    let publishCore nextName core produced preserveErrors = publishSlot nextName "Value" core produced preserveErrors

    /// A projection retains an existing error before evaluating its selected value.
    let preserveError identity errorValue produced =
        let missing = SqlExpr.Unary(UnaryOperator.IsNull, identity)
        match produced with
        | DirectValue(value, input) ->
            let preserve value = SqlExpr.CaseExpr((missing, errorValue), [], Some value)
            DirectValue(preserve value, { input with Source = input.Source |> ValueOption.map preserve })
        | PackedValue(packet, input) ->
            let error = SqlExpr.JsonArrayExpr [jsonToken errorValue; SqlExpr.Literal SqlLiteral.Null]
            PackedValue(SqlExpr.CaseExpr((missing, error), [], Some packet), input)

    /// A grouping element retains its source occurrence and binds its actual writer.
    let publishGroupElement nextName core produced preserveErrors =
        let name = "_element" + nextName()
        let rename original replacement (projection: Projection) =
            if projection.Alias = Some original then { projection with Alias = Some replacement } else projection
        let produced =
            if preserveErrors then
                let fields = ProjectionSetOps.toList core.Projections
                let field name = fields |> List.find (fun field -> field.Alias = Some name) |> _.Expr
                preserveError (field "Id") (field "Value") produced
            else produced
        let core = { core with Projections = core.Projections |> ProjectionSetOps.map (rename "Value" name) }
        let struct(query, input) = publishSlot nextName name core produced preserveErrors
        match query.Body with
        | SingleSelect core ->
            struct({ query with Body = SingleSelect { core with Projections = core.Projections |> ProjectionSetOps.map (rename name "Value") } }, input)
        | _ -> invalidOp "A grouping element requires its completed row source."

    /// Selected grouping rows are evaluated before post-group consumers.
    let materializeGroupElements nextName position (query: SqlSelect) =
        let name = "_elements" + nextName()
        match query.Body with
        | SingleSelect core ->
            let fields = core.Projections |> ProjectionSetOps.map (fun projection ->
                let label =
                    match projection.Alias, projection.Expr with
                    | Some label, _ | None, SqlExpr.Column(_, label) -> label
                    | _ -> invalidOp "A grouping element requires named published columns."
                { Alias = Some label; Expr = SqlExpr.Column(Some name, label) })
            { Ctes = [{ Name = name; Materialized = true; Query = query }]
              Body = SingleSelect { Distinct = false; Projections = fields; Source = Some(BaseTable(name, Some name))
                                    Joins = []; Where = None; GroupBy = []; Having = None
                                    OrderBy = [{ Expr = SqlExpr.Column(Some name, position); Direction = SortDirection.Asc }]
                                    Limit = None; Offset = None } }
        | _ -> invalidOp "A grouping element requires its completed row source."

    /// Presence reads null without invoking an unused numerical target conversion.
    let presence nextName = function
        | DirectValue(value, input) -> nullCarrier input.Carrier value
        | PackedValue(packet, input) ->
            let alias = nextName()
            let presenceToken (input: QuerySerializationInput) token =
                if input.Carrier = SqlScalar then
                    SqlExpr.FunctionCall("jsonb_extract", [token; SqlExpr.Literal(SqlLiteral.String "$")])
                else token
            let value, input = bound presenceToken alias input
            singleton alias packet (nullCarrier input.Carrier value)

    /// Position describes the selected row occurrence, independently of physical Id.
    let withPosition name (core: SelectCore) =
        { core with Projections = ProjectionSetOps.ofList(ProjectionSetOps.toList core.Projections
                        @ [{ Alias = Some name; Expr = QueryableBuildQueryWindowHelpers.rowNumberOver [] }]) }

    /// Preserve the actual completed columns, including authored keys and input companions.
    let completedRows nextName (source: SqlSelect) input =
        let alias = nextName()
        let original = match source.Body with SingleSelect core | UnionAllSelect(core, _) -> core
        let fields = ProjectionSetOps.toList original.Projections
                     |> List.filter (fun field -> field.Alias <> Some "_tree_ordinal")
                     |> List.map (fun projection ->
            let name = match projection.Alias, projection.Expr with
                       | Some name, _ | None, SqlExpr.Column(_, name) -> name
                       | _ -> invalidOp "A completed row requires named published columns."
            { Alias = Some name; Expr = SqlExpr.Column(Some alias, name) })
        let companions = companionProjections (Some alias) input
                         |> List.filter (fun companion -> not(fields |> List.exists (fun field -> field.Alias = companion.Alias)))
        { Ctes = []; Body = SingleSelect(withPosition "_tree_ordinal" {
            Distinct = false; Projections = ProjectionSetOps.ofList(fields @ companions)
            Source = Some(DerivedTable(source, alias)); Joins = []; Where = None; GroupBy = []; Having = None
            OrderBy = []; Limit = None; Offset = None }) }
    let completedKeyRows nextName source input keyInput =
        let rows = completedRows nextName source input
        match rows.Body with
        | SingleSelect core ->
            let alias = match core.Source with Some(DerivedTable(_, alias)) -> alias | _ -> invalidOp "Completed rows require their prepared source."
            let fields = ProjectionSetOps.toList core.Projections
            let companions = companionProjections (Some alias) keyInput
                             |> List.filter (fun companion -> not(fields |> List.exists (fun field -> field.Alias = companion.Alias)))
            { rows with Body = SingleSelect { core with Projections = ProjectionSetOps.ofList(fields @ companions) } }
        | _ -> invalidOp "Completed rows require their own select."
