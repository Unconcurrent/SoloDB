namespace SoloDatabase

open System
open Utils
open System.Collections.Generic
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.Metadata.Generation
open SoloDatabase.QuerySerializationSql
open SoloDatabase.QueryTranslatorPhysicalSource
open SoloDatabase.QuerySerializationChildren

/// One input owner follows authored writers and the children of produced payloads.
module internal QueryTranslatorSerialization =

    let internal isStoredNumericalInput target =
        let target = Utils.optionalPayloadType target
        let target = if target.IsEnum then TypeMetadata.enumUnderlying target else target
        (TypeSupport.StoredNumeric target.FullName).IsSome ||
        (TypeSupport.StoredNumericReader target.FullName).IsSome

    let internal serializationStep access actualType =
        match access with
        | StoredLabel value ->
            { Kind = SqlExpr.Literal(SqlLiteral.Integer 0L)
              Label = match value with :? int as index -> SqlExpr.Literal(SqlLiteral.Integer(int64 index)) | :? string as name -> SqlExpr.Literal(SqlLiteral.String name) | _ -> SqlExpr.Literal SqlLiteral.Null
              MemberType = SqlExpr.Literal SqlLiteral.Null; ActualType = actualType }
        | ClrMember(name, valueType) ->
            TypeMetadata.registerType valueType
            { Kind = SqlExpr.Literal(SqlLiteral.Integer 1L); Label = SqlExpr.Literal(SqlLiteral.String name)
              MemberType = SqlExpr.Literal(SqlLiteral.String valueType.FullName); ActualType = actualType }

    let internal fromDescriptor descriptor (input: QuerySerializationInput) =
        let at path = SqlExpr.FunctionCall("jsonb_extract", [descriptor; SqlExpr.Literal(SqlLiteral.String path)])
        let framed = SqlExpr.Binary(at "$[0]", BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 3L))
        let owner position (original: QueryPhysicalOwner voption) = original |> ValueOption.map (fun owner ->
            { owner with Descriptor = SqlExpr.CaseExpr((framed, at ("$[" + string position + "]")), [], Some(SqlExpr.Literal SqlLiteral.Null)) })
        { input with Operand = ValueSome { Descriptor = SqlExpr.CaseExpr((framed, at "$[1]"), [], Some descriptor); Accesses = Array.empty }
                     PhysicalOwner = owner 2 input.PhysicalOwner
                     ReferenceOwner = input.ReferenceOwner |> ValueOption.map (fun original ->
                         { original with Descriptor = SqlExpr.CaseExpr((framed, at "$[3]"), [], Some(SqlExpr.Literal SqlLiteral.Null)) }) }

    /// Equivalent authored layouts retain each selected child's known source alternatives.
    let rec internal mergeInputs target (inputs: QuerySerializationInput array) =
        let first = inputs[0]
        let layout = inputs |> Array.tryFind (fun input -> input.Children.Length > 0)
        let layout = layout |> Option.filter (fun layout -> inputs |> Array.forall (fun input ->
            input.Children.Length = 0 || (input.OriginalType.Equals layout.OriginalType && input.Shape = layout.Shape && input.Children.Length = layout.Children.Length)))
        let sequence =
            if layout.IsSome || inputs |> Array.forall (fun input -> input.Children.Length = 0) then ValueNone
            else
                match JsonSerializator.JsonValue.SerializedValueFacts target with
                | JsonSerializator.JsonSerializationShape.SequenceValues element -> ValueSome element
                | _ -> ValueNone
        let children =
            match layout, sequence with
            | Some layout, _ -> Array.init layout.Children.Length (fun index ->
                let target = layout.Children[index].OriginalType
                mergeInputs target (inputs |> Array.map (fun input -> if input.Children.Length = 0 then QuerySerializationInput.ForType target else input.Children[index])))
            | None, ValueSome element ->
                let items = inputs |> Array.collect (fun input ->
                    if input.Children.Length = 0 then [|QuerySerializationInput.ForType element|] else input.Children)
                [|mergeInputs element items|]
            | _ -> Array.empty
        { QuerySerializationInput.ForType target with
            Shape = match sequence with
                    | ValueSome element -> ValueSome(JsonSerializator.JsonSerializationShape.SequenceValues element)
                    | ValueNone -> layout |> ValueOption.ofOption |> ValueOption.bind _.Shape
            Children = children
            Carrier = if inputs |> Array.forall (fun input -> input.Carrier = first.Carrier) then first.Carrier else CanonicalToken
            LegacyCharacterText = inputs |> Array.exists _.LegacyCharacterText
            PhysicalOwner = sharedPhysicalOwner inputs
            ReferenceOwner = sharedReferenceOwner inputs
            Navigation = inputs |> Array.collect _.Navigation |> Array.distinct }

    let internal scopeSerializationInput alias position (input: QuerySerializationInput) =
        let qualify expression = SqlExpr.map (function Column(None, name) -> Column(alias, name) | other -> other) expression
        let rec qualifyInput (input: QuerySerializationInput) =
            { input with
                Source = input.Source |> ValueOption.map qualify
                Operand = input.Operand |> ValueOption.map (fun operand ->
                    { Descriptor = qualify operand.Descriptor
                      Accesses = operand.Accesses |> Array.map (fun step ->
                        { Kind = qualify step.Kind; Label = qualify step.Label
                          MemberType = qualify step.MemberType; ActualType = qualify step.ActualType }) })
                Children = input.Children |> Array.map qualifyInput
                PhysicalOwner = input.PhysicalOwner |> ValueOption.map (fun owner -> { owner with Descriptor = qualify owner.Descriptor })
                ReferenceOwner = input.ReferenceOwner |> ValueOption.map (fun owner -> { owner with Descriptor = qualify owner.Descriptor }) }
        let input = qualifyInput input
        TypeMetadata.registerType input.OriginalType
        let source =
            match input.Source with
            | ValueSome source -> source
            | ValueNone ->
                let column = SqlExpr.Column(alias, "Value")
                match position with
                | BaseLayer -> SqlExpr.FunctionCall("jsonb", [column])
                | OuterLayer -> QueryTranslatorBaseTypes.canonicalToken input.Carrier column
        let operand =
            match input.Operand with
            | ValueSome operand -> ValueSome operand
            | ValueNone when input.Children.Length > 0 -> ValueNone
            | ValueNone ->
                match input.Facts with
                | JsonSerializator.JsonSerializationShape.ObjectMembers _
                | JsonSerializator.JsonSerializationShape.DeclaredMembers _
                | JsonSerializator.JsonSerializationShape.Dynamic ->
                    ValueSome { Descriptor = writerDescriptor { input with Source = ValueSome source }; Accesses = Array.empty }
                | _ -> ValueNone
        { input with Source = ValueSome source; Operand = operand }

    /// Group item descriptors aggregate in the same row order as the public items.
    let internal groupingDescriptor (input: QuerySerializationInput) alias =
        match input.Facts with
        | JsonSerializator.JsonSerializationShape.GroupingValues(key, item) ->
            TypeMetadata.registerType key
            TypeMetadata.registerType item
            TypeMetadata.registerType (item.MakeArrayType())
            let keyInput = scopeSerializationInput alias OuterLayer input.Children[0]
            let itemInput = scopeSerializationInput alias OuterLayer input.Children[1]
            let itemDescriptor = sequenceDescriptor (item.MakeArrayType()) itemInput
            let children = SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 0L);
                SqlExpr.JsonArrayExpr
                    [SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.String "Key"); SqlExpr.Literal(SqlLiteral.String key.FullName); inputDescriptor keyInput]
                     SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.String "Items"); SqlExpr.Literal(SqlLiteral.String (item.MakeArrayType()).FullName); itemDescriptor]]]
            SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 2L); rootDescriptor input; children]
        | _ -> inputDescriptor input

    /// One child resolver retains produced overrides and actual writer responsibility.
    let internal serializationAccess (parent: QuerySerializationInput) access =
        let selected = selectChild parent.OriginalType parent.Facts access
        let child =
            match selected with
            | ValueSome selected when selected.Rest && parent.Children.Length > 7 ->
                QuerySerializationInput.Positions(selected.ValueType, parent.Children[7..])
            | ValueSome selected ->
                match selected.Ordinal with
                | ValueSome index when index < parent.Children.Length ->
                    let child = parent.Children[index]
                    if selected.Sequence then QuerySerializationInput.Sequence(selected.ValueType, child) else child
                | _ -> QuerySerializationInput.ForType selected.ValueType
            | ValueNone ->
                { QuerySerializationInput.ForType typeof<JsonSerializator.JsonValue> with
                    Operand = parent.Operand |> ValueOption.map (fun operand ->
                        { operand with Accesses = Array.append operand.Accesses [|serializationStep access absentWriterHeader|] }) }
        let segments, offset =
            match access with
            | ClrMember(name, _) when isTuple parent.OriginalType -> serializedMemberStep parent.SourceOffset parent.OriginalType name
            | _ ->
                let label =
                    match selected, access with
                    | ValueSome selected, _ -> selected.StoredLabel
                    | _, StoredLabel label -> label
                    | _, ClrMember(name, _) -> box name
                match label with
                | :? int as index -> [JsonSerializator.JsonEqualitySupport.Index index], 0
                | :? string as name -> [JsonSerializator.JsonEqualitySupport.Member name], 0
                | _ -> [], 0
        let source = parent.Source |> ValueOption.map (fun source ->
            if segments.IsEmpty then source
            else SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(source, BinaryOperator.JsonText, SqlExpr.Literal(SqlLiteral.String(JsonPathOps.text segments)))]))
        let producedChildPosition =
            match parent.Facts with
            | JsonSerializator.JsonSerializationShape.GroupingValues _
            | JsonSerializator.JsonSerializationShape.DeclaredMembers _ -> ValueSome 2
            | JsonSerializator.JsonSerializationShape.ArrayMembers _
            | JsonSerializator.JsonSerializationShape.DeclaredArrayMembers _ -> ValueSome 1
            | _ -> ValueNone
        let operand =
            match parent.Operand with
            | ValueSome operand when parent.Children.Length > 0 && operand.Accesses.Length = 0 &&
                                     (selected |> ValueOption.exists (fun selected -> selected.Ordinal.IsSome && not selected.Rest)) &&
                                     producedChildPosition.IsSome ->
                let selected = selected.Value
                let path = JsonPathOps.text ([JsonSerializator.JsonEqualitySupport.Index 2;
                                            JsonSerializator.JsonEqualitySupport.Index 1;
                                            JsonSerializator.JsonEqualitySupport.Index selected.Ordinal.Value;
                                            JsonSerializator.JsonEqualitySupport.Index producedChildPosition.Value])
                let origin = serializationOperand operand
                let complete = SqlExpr.FunctionCall("jsonb_extract", [origin; SqlExpr.Literal(SqlLiteral.String path)])
                let actual = source |> ValueOption.map writerHeader |> ValueOption.defaultValue absentWriterHeader
                let inherited = serializationOperand { operand with Accesses = Array.append operand.Accesses [|serializationStep access actual|] }
                let produced = SqlExpr.Binary(SqlExpr.FunctionCall("jsonb_extract", [origin; SqlExpr.Literal(SqlLiteral.String "$[0]")]), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L))
                ValueSome { Descriptor = SqlExpr.CaseExpr((produced, complete), [], Some inherited); Accesses = Array.empty }
            | ValueSome operand when operand.Accesses.Length > 0 ||
                                     (match operand.Descriptor with
                                      | SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.Integer 0L); _; header] when header = absentWriterHeader -> false
                                      | _ -> true) ->
                let actual = source |> ValueOption.map writerHeader |> ValueOption.defaultValue absentWriterHeader
                ValueSome { operand with Accesses = Array.append operand.Accesses [|serializationStep access actual|] }
            | _ -> child.Operand
        let child = { child with Source = source; SourceOffset = offset; Operand = operand }
        let child =
            let retained = selected |> ValueOption.exists (fun selected ->
                selected.Rest || (selected.Ordinal |> ValueOption.exists (fun index -> index < parent.Children.Length)))
            if retained then child else
                let name =
                    match selected |> ValueOption.bind _.ClrName, access with
                    | ValueSome name, _ | _, ClrMember(name, _) -> ValueSome name
                    | _, StoredLabel(:? string as name) -> ValueSome name
                    | _ -> ValueNone
                let navigation = parent.Navigation |> Array.map (fun scope ->
                    { scope with
                        Path = name |> ValueOption.map (fun name -> scope.Path @ [name]) |> ValueOption.defaultValue scope.Path
                        Depth = scope.Depth + (if DBRefTypeHelpers.isSingleReferenceType child.OriginalType then 1 else 0) })
                { child with Navigation = navigation }
        let child =
            match selected, parent.Operand with
            | ValueSome selected, ValueSome operand when selected.Rest && parent.Children.Length > 7 && operand.Accesses.Length = 0 ->
                let children = parent.Children[7..] |> Array.mapi (fun index child ->
                    let path = JsonPathOps.text [JsonSerializator.JsonEqualitySupport.Index 2; JsonSerializator.JsonEqualitySupport.Index 1;
                                                JsonSerializator.JsonEqualitySupport.Index(index + 7); JsonSerializator.JsonEqualitySupport.Index 1]
                    fromDescriptor (SqlExpr.FunctionCall("jsonb_extract", [operand.Descriptor; SqlExpr.Literal(SqlLiteral.String path)])) child)
                { child with Children = children; Operand = ValueNone }
            | _ -> child
        let child =
            match child.Operand with
            | ValueSome operand when parent.Operand.IsSome && operand.Accesses.Length = 0 && producedChildPosition.IsSome && parent.Children.Length > 0 -> fromDescriptor operand.Descriptor child
            | _ -> child
        let child =
            if DBRefTypeHelpers.isSingleReferenceType child.OriginalType || DBRefTypeHelpers.isManyReferenceType child.OriginalType then
                let propertyName =
                    match selected |> ValueOption.bind _.ClrName, access with
                    | ValueSome name, _ -> name
                    | _, ClrMember(name, _) -> name
                    | _, StoredLabel(:? string as name) -> name
                    | _ -> null
                let inherited =
                    match parent.PhysicalOwner with
                    | ValueSome owner -> ValueSome {
                        Sources = owner.Sources |> Array.map (fun source -> { Owner = ValueSome source; PropertyName = propertyName })
                        Descriptor = SqlExpr.JsonArrayExpr [owner.Descriptor; SqlExpr.Literal(SqlLiteral.String propertyName)] }
                    | ValueNone -> ValueSome {
                        Sources = [|{ Owner = ValueNone; PropertyName = propertyName }|]
                        Descriptor = SqlExpr.JsonArrayExpr [SqlExpr.Literal SqlLiteral.Null; SqlExpr.Literal(SqlLiteral.String propertyName)] }
                let inheritedDescriptor = inherited |> ValueOption.map _.Descriptor |> ValueOption.defaultValue (SqlExpr.Literal SqlLiteral.Null)
                let existing = child.ReferenceOwner |> ValueOption.map (fun owner -> owner.Descriptor) |> ValueOption.defaultValue (SqlExpr.Literal SqlLiteral.Null)
                let owner = sharedReferenceOwner [|child; { parent with ReferenceOwner = inherited }|]
                let descriptor =
                    match parent.Operand with
                    | ValueSome operand when parent.Children.Length > 0 ->
                        let produced = SqlExpr.Binary(SqlExpr.FunctionCall("jsonb_extract", [serializationOperand operand; SqlExpr.Literal(SqlLiteral.String "$[0]")]), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L))
                        SqlExpr.CaseExpr((produced, (if child.ReferenceOwner.IsSome then existing else inheritedDescriptor)), [], Some inheritedDescriptor)
                    | _ when parent.PhysicalOwner.IsNone && child.ReferenceOwner.IsSome -> existing
                    | _ -> inheritedDescriptor
                { child with ReferenceOwner = owner |> ValueOption.map (fun owner -> { owner with Descriptor = descriptor }) }
            else child
        struct(child, segments)

    let internal serializationElement (input: QuerySerializationInput) =
        match input.Facts with
        | JsonSerializator.JsonSerializationShape.SequenceValues element
        | JsonSerializator.JsonSerializationShape.DictionaryValues element ->
            if input.Children.Length = 1 then input.Children.[0] else QuerySerializationInput.ForType element
        | JsonSerializator.JsonSerializationShape.GroupingValues(_, element) ->
            if input.Children.Length = 2 then input.Children.[1] else QuerySerializationInput.ForType element
        | JsonSerializator.JsonSerializationShape.DeclaredArrayMembers _
        | JsonSerializator.JsonSerializationShape.ArrayMembers _ when input.Children.Length > 0 ->
            mergeInputs (ModelMetadata.SequenceElement input.OriginalType) input.Children
        | _ -> QuerySerializationInput.ForType (ModelMetadata.SequenceElement input.OriginalType)

    /// Each array entry keeps the descriptor of its own prepared element.
    let internal elementInput input key token =
        let element = serializationElement input
        let step = { Kind = SqlExpr.Literal(SqlLiteral.Integer 0L); Label = key; MemberType = SqlExpr.Literal SqlLiteral.Null
                     ActualType = writerHeader token }
        let operand =
            if input.Children.Length = 1 then
                let descriptor = writerDescriptor input
                let at expression path = SqlExpr.FunctionCall("jsonb_extract", [expression; SqlExpr.Literal(SqlLiteral.String path)])
                let writer = descriptor
                let template = at writer "$[2][2]"
                let entries = at writer "$[2][1]"
                let entry = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(entries, BinaryOperator.JsonText, key)])
                let selected = SqlExpr.CaseExpr((SqlExpr.Binary(at writer "$[2][0]", BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), template), [], Some(at entry "$[1]"))
                ValueSome { Descriptor = selected; Accesses = Array.empty }
            else ValueSome { Descriptor = inputDescriptor input; Accesses = [|step|] }
        let element =
            { element with Source = ValueSome token; SourceOffset = 0
                           Navigation = if input.Children.Length = 0 then input.Navigation else element.Navigation
                           Carrier = if QueryTranslatorBaseTypes.isSqlScalarResult element.OriginalType then SqlScalar else CanonicalToken }
        match operand with
        | ValueSome operand when operand.Accesses.Length = 0 -> fromDescriptor operand.Descriptor element
        | _ -> { element with Operand = operand }

    let rec internal rebaseSerializationInput (input: QuerySerializationInput) =
        { input with Operand = ValueNone; Source = ValueNone; SourceOffset = 0
                     Children = Array.map rebaseSerializationInput input.Children
                     PhysicalOwner = input.PhysicalOwner |> ValueOption.map (fun owner -> { owner with Descriptor = SqlExpr.Literal SqlLiteral.Null })
                     ReferenceOwner = input.ReferenceOwner |> ValueOption.map (fun owner -> { owner with Descriptor = SqlExpr.Literal SqlLiteral.Null }) }

    /// Known produced children remain independently live; dynamic roots stay paired.
    let rec internal publishInput nextName (input: QuerySerializationInput) =
        let output = rebaseSerializationInput input
        if input.Operand.IsNone && input.PhysicalOwner.IsNone && input.ReferenceOwner.IsNone then
            let children = input.Children |> Array.map (publishInput nextName)
            children |> Array.toList |> List.collect fst,
            { output with Children = children |> Array.map snd }
        else
            let descriptor = inputDescriptor input
            let projections, descriptor =
                if isConstantDescriptor descriptor then [], descriptor
                else
                    let name = nextName()
                    [{ Alias = Some name; Expr = descriptor }], SqlExpr.Column(None, name)
            projections, fromDescriptor descriptor output

    /// A completed input keeps companions from the same row as its selected payload.
    let internal companionProjections alias (input: QuerySerializationInput) =
        let projections = ResizeArray<Projection>()
        let seen = HashSet<string>(StringComparer.Ordinal)
        let inspect expression =
            SqlExpr.fold (fun () expression ->
                match expression with
                | SqlExpr.Column(None, name) when seen.Add name -> projections.Add { Alias = Some name; Expr = SqlExpr.Column(alias, name) }
                | _ -> ()) () expression
        let rec collect input =
            match input.Operand with
            | ValueSome operand ->
                inspect operand.Descriptor
                for step in operand.Accesses do inspect step.Kind; inspect step.Label; inspect step.MemberType; inspect step.ActualType
            | ValueNone -> ()
            for child in input.Children do collect child
        collect input
        List.ofSeq projections

    let internal serializationPayload (input: QuerySerializationInput) =
        { input with OriginalType = Utils.unwrapNullableType input.OriginalType }

