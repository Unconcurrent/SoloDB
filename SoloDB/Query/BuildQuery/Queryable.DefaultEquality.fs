namespace SoloDatabase

open System
open SoloDatabase.JsonSerializator
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorSerialization

/// Default operands retain distinct tree, physical entity and native value policies.
module internal QueryDefaultEquality =
    let isTree (target: Type) = typeof<JsonValue>.IsAssignableFrom target
    let private column alias name = SqlExpr.Column(Some alias, name)
    let private projection name value = { Alias = Some name; Expr = value }
    let private select core = { Ctes = []; Body = SingleSelect core }
    let private nullValue = SqlExpr.Literal SqlLiteral.Null

    let private isRecordRole target = (TypeMetadata.recordFields target).IsSome

    /// Identity-only relation rows cannot supply structural record comparison fields.
    let canUseIdentityOnly target =
        DBRefTypeHelpers.isSingleReferenceType target ||
        (QueryTranslatorBaseHelpers.hasPhysicalRowIdentity target && not(isRecordRole target))

    let private physicalRoles (target: Type) (input: QuerySerializationInput) =
        input.PhysicalOwner |> ValueOption.map (fun owner ->
            let entities, records = ResizeArray<QueryPhysicalSource>(), ResizeArray<QueryPhysicalSource>()
            for source in owner.Sources do
                if target.IsAssignableFrom(source.OwnerType) && QueryTranslatorBaseHelpers.hasPhysicalRowIdentity source.OwnerType then
                    if isRecordRole source.OwnerType then records.Add source
                    else entities.Add source
            let selected (sources: ResizeArray<QueryPhysicalSource>) =
                if sources.Count = 0 then ValueNone
                elif sources.Count = owner.Sources.Length then ValueSome(SqlExpr.Literal(SqlLiteral.Integer 1L))
                else sources |> Seq.map (QueryTranslatorPhysicalSource.selectsSource owner)
                             |> Seq.reduce (fun left right -> SqlExpr.Binary(left, BinaryOperator.Or, right)) |> ValueSome
            struct(owner, selected entities, selected records))

    let private physicalEntity target input =
        physicalRoles target input |> ValueOption.bind (fun struct(owner, entities, _) ->
            entities |> ValueOption.map (fun selected -> owner, selected))

    let private referenceType (target: Type) (input: QuerySerializationInput) =
        if DBRefTypeHelpers.isSingleReferenceType target then ValueSome target
        elif DBRefTypeHelpers.isSingleReferenceType input.OriginalType && target.IsAssignableFrom(input.OriginalType) then ValueSome input.OriginalType
        else ValueNone

    let requiresPrepared target input =
        QueryTranslatorCharacters.requiresComparable target input || isTree target || (referenceType target input).IsSome || (physicalRoles target input |> ValueOption.exists (fun struct(_, entities, records) -> entities.IsSome || records.IsSome))

    let private nativeValue target value (input: QuerySerializationInput) =
        let value = QueryProduced.scalarValue target value input |> fun value -> QueryTranslatorCharacters.comparable target value input
        // Object values compare their actual storage classes, without a boxed CAST's affinity.
        if typeof<obj>.Equals target then SqlExpr.Unary(UnaryOperator.Pos, value) else value

    /// Logical fields stay separate from the authored value and its input descriptor.
    type Operand = { Role: SqlExpr; Qualifier: SqlExpr; Value: SqlExpr }

    let private integer value = SqlExpr.Literal(SqlLiteral.Integer value)
    let private operand role qualifier value = { Role = integer role; Qualifier = qualifier; Value = value }
    let private valueOperand value = operand 0L (integer 0L) value
    let private absent value = SqlExpr.Unary(UnaryOperator.IsNull, value)
    let private constant = function SqlExpr.Literal _ -> true | _ -> false

    /// Presence makes inactive fields harmless without reserving any public value.
    let private components key =
        let missing = absent key.Value
        let inactive value = SqlExpr.CaseExpr((missing, integer 0L), [], Some value)
        [missing; inactive key.Role; inactive key.Qualifier; SqlExpr.Coalesce(key.Value, [integer 0L])]

    let terms key =
        if constant key.Role && constant key.Qualifier then [key.Value]
        else components key

    let private pairedTerms left right =
        if constant left.Role && constant left.Qualifier &&
           left.Role = right.Role && left.Qualifier = right.Qualifier then
            [left.Value], [right.Value]
        else components left, components right

    let equal left right =
        let left, right = pairedTerms left right
        List.map2 (fun left right -> SqlExpr.Binary(left, BinaryOperator.Is, right)) left right
        |> List.reduce (fun left right -> SqlExpr.Binary(left, BinaryOperator.And, right))

    /// The actual producer retains its own physical subject through identity views.
    let key nextName target value (input: QuerySerializationInput) =
        if isTree target then
            let identity = input.PhysicalOwner |> ValueOption.map _.Identity |> ValueOption.defaultValue nullValue
            operand 1L (integer 0L) (SqlExpr.FunctionCall(JsonFunctions.treeValueFunction,
                [SqlExpr.FunctionCall("json", [QueryProduced.treeToken nextName value input]); identity;
                 SqlExpr.Literal(SqlLiteral.Integer(int64 TreeValueFormat.EqualityKey))]))
        elif (referenceType target input).IsSome then
            let reference = (referenceType target input).Value
            operand 2L (SqlExpr.Literal(SqlLiteral.String(TypeMetadata.typeIdentityKey reference)))
                (QueryProduced.referenceOperand nextName reference (DirectValue(value, input)))
        else
            let value = nativeValue target value input
            match physicalRoles target input with
            | ValueSome struct(owner, entities, records) ->
                let present = SqlExpr.Unary(UnaryOperator.IsNotNull, owner.Identity)
                let selected condition = SqlExpr.Binary(present, BinaryOperator.And, condition)
                let recordValue =
                    match records with
                    | ValueSome condition -> SqlExpr.CaseExpr((selected condition, withRowIdentity value owner.Identity), [], Some value)
                    | ValueNone -> value
                match entities, records, owner.Descriptor with
                | ValueSome(SqlExpr.Literal(SqlLiteral.Integer 1L)), ValueNone,
                  SqlExpr.JsonArrayExpr [SqlExpr.Literal _; SqlExpr.Literal _; _; _] ->
                    operand 3L owner.Collection owner.Identity
                | ValueSome condition, _, _ ->
                    let selected = selected condition
                    { Role = SqlExpr.CaseExpr((selected, integer 3L), [], Some(integer 0L))
                      Qualifier = SqlExpr.CaseExpr((selected, owner.Collection), [], Some(integer 0L))
                      Value = SqlExpr.CaseExpr((selected, owner.Identity), [], Some recordValue) }
                | _ -> valueOperand recordValue
            | ValueNone -> valueOperand value

    let private anchoredIdentity target context detached input =
        match context, detached with
        | ReceiverScopedEntityMembership, true ->
            physicalRoles target input |> ValueOption.bind (fun struct(owner, entities, records) ->
                if entities.IsSome || records.IsSome then ValueSome owner.Identity else ValueNone)
        | _ -> ValueNone

    /// Detached membership may use the original receiver anchor without giving the argument origin.
    let membershipKeys nextName target context detached (left, leftInput) (right, rightInput) rightIdentity =
        match rightInput.PhysicalOwner, anchoredIdentity target context detached leftInput with
        | ValueNone, ValueSome identity -> struct(valueOperand identity, valueOperand (rightIdentity()))
        | _ -> struct(key nextName target left leftInput, key nextName target right rightInput)

    /// Operation context supplies established detached admission without inventing origin.
    let contains nextName target context detached (receiver: QueryProducedValue)
        (argument: unit -> QueryProducedValue) (detachedIdentity: unit -> QueryProducedValue)
        (valueMatch: SqlExpr -> SqlExpr) =
        QueryProduced.consume nextName (fun value input ->
            let legacyValue = context = RootValueMembership && detached && not(isTree target)
                              && not(isSqlScalarResult target) && (QueryTranslatorNumeric.domain target).IsNone
                              && not(typeof<obj>.Equals(target))
            if legacyValue then valueMatch value
            else
                match anchoredIdentity target context detached input with
                | ValueSome identity ->
                    SqlExpr.Binary(identity, BinaryOperator.Is, QueryProduced.value nextName (detachedIdentity()))
                | _ ->
                    let left = key nextName target value input
                    QueryProduced.consume nextName (fun value input -> equal left (key nextName target value input)) (argument())) receiver

    /// A tree child or constructed value does not acquire the containing row identity.
    let outputIdentity target (input: QuerySerializationInput) identity =
        let fallback = if isTree target || (physicalEntity target input).IsSome then SqlExpr.Literal(SqlLiteral.Integer -1L) else identity
        QueryProduced.outputIdentity true fallback input identity

    /// Right membership stays independent of each left occurrence, including SQL absence.
    let membership left right (source: SelectCore) =
        let leftTerms, rightTerms = pairedTerms left right
        match leftTerms, rightTerms with
        | [left], [right] ->
            let nonNull = select { source with Projections = ProjectionSetOps.ofList [projection "Value" right]
                                               Where = Some(SqlExpr.Unary(UnaryOperator.IsNotNull, right)) }
            let absentRows = select { source with
                                          Projections = ProjectionSetOps.ofList [projection "Value" (integer 1L)]
                                          Where = Some(absent right) }
            SqlExpr.Binary(
                SqlExpr.Coalesce(SqlExpr.InSubquery(left, nonNull), [integer 0L]), BinaryOperator.Or,
                SqlExpr.Binary(absent left, BinaryOperator.And, SqlExpr.Exists absentRows))
        | _ ->
            let fields = rightTerms |> List.mapi (fun index value -> projection ("_comparison" + string index) value)
            SqlExpr.InSubquery(SqlExpr.RowValueExpr leftTerms, select { source with Projections = ProjectionSetOps.ofList fields })

    /// Rank one original occurrence, keeping every payload and input companion together.
    let firstRows nextName (source: SelectCore) equality order =
        let rankName = "_rank" + nextName()
        let rank = QueryableBuildQueryWindowHelpers.rowNumberExpr (terms equality) order
        let fields = ProjectionSetOps.toList source.Projections
        let ranked = select { source with Projections = ProjectionSetOps.ofList(fields @ [projection rankName rank]) }
        let alias = nextName()
        let output = fields |> List.map (fun field ->
            let name = field.Alias |> Option.defaultWith (fun () ->
                match field.Expr with Column(_, name) -> name | _ -> invalidOp "A retained row field requires its published name.")
            projection name (column alias name))
        let order = order |> List.map (fun (value, direction) ->
            let name = match value with Column(_, name) -> name | _ -> invalidOp "A representative order requires its completed column."
            { Expr = column alias name; Direction = direction })
        select { source with
                    Projections = ProjectionSetOps.ofList output
                    Source = Some(DerivedTable(ranked, alias)); Joins = []; GroupBy = []; Having = None
                    Where = Some(SqlExpr.Binary(column alias rankName, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                    OrderBy = order; Limit = None; Offset = None }

    /// One selected key packet supplies the public key and its exact producer facts.
    /// Item rows remain intact for the owning grouped aggregates and correlation.
    let prepareGroups nextName keyName target (source: SqlSelect) itemInput keyInput orderName =
        if not (requiresPrepared target keyInput) then struct(source, keyInput, [keyName])
        else
            let alias = nextName()
            let value = column alias keyName
            let keyInput = scopeSerializationInput (Some alias) OuterLayer { keyInput with Source = ValueSome(jsonToken value) }
            let equality = key nextName target value keyInput
            let payload, keyInput = QueryProduced.embedded (value, keyInput)
            let constantInput = QuerySerializationSql.isConstantDescriptor(QuerySerializationSql.inputDescriptor keyInput)
            let packet = if constantInput then payload else QueryProduced.packet (DirectValue(payload, keyInput))
            let packetName = "_key" + nextName()
            let comparison = terms equality
            let equalityNames = comparison |> List.map (fun _ -> "_equality" + nextName())
            let first = SqlExpr.WindowCall {
                Kind = NamedWindowFunction "FIRST_VALUE"; Arguments = [packet]; PartitionBy = comparison
                OrderBy = [column alias orderName, SortDirection.Asc] }
            let fields = [projection "Id" (column alias "Id"); projection "Value" (column alias "Value")]
                         @ companionProjections (Some alias) itemInput
                         @ (if orderName = "Id" || orderName = "Value" then [] else [projection orderName (column alias orderName)])
            let core = {
                Distinct = false; Projections = ProjectionSetOps.ofList(fields @ [projection packetName first] @ List.map2 projection equalityNames comparison)
                Source = Some(DerivedTable(source, alias)); Joins = []; Where = None; GroupBy = []; Having = None
                OrderBy = []; Limit = None; Offset = None }
            let completedAlias = nextName()
            let packet = column completedAlias packetName
            // Absence belongs to the same completed comparison as its first packet.
            let missing =
                if comparison.Length = 1 then absent (column completedAlias equalityNames.Head)
                else SqlExpr.Binary(column completedAlias equalityNames.Head, BinaryOperator.Is, integer 1L)
            let original = SqlExpr.CaseExpr((missing, nullValue), [],
                                           Some(if constantInput then packet else tupleLeafToken packet 0))
            let inputFields, outputInput =
                if constantInput then publishInput nextName keyInput
                else
                    let descriptor = SqlExpr.FunctionCall("jsonb_extract", [packet; SqlExpr.Literal(SqlLiteral.String "$[1]")])
                    let name = "_input" + nextName()
                    [projection name descriptor], fromDescriptor (SqlExpr.Column(None, name)) (rebaseSerializationInput keyInput)
            let fields = fields |> List.map (fun field -> projection field.Alias.Value (column completedAlias field.Alias.Value))
            let fields = fields @ [projection keyName original] @ inputFields
                                 @ (equalityNames |> List.map (fun name -> projection name (column completedAlias name)))
            struct(select { core with Projections = ProjectionSetOps.ofList fields; Source = Some(DerivedTable(select core, completedAlias)) }, outputInput, equalityNames)
