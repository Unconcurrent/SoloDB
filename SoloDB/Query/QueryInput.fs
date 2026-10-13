namespace SoloDatabase

open SoloDatabase.SqlModel

type internal LayerPosition =
| BaseLayer
| OuterLayer

/// Authored accesses retain their original member, label or array-position identity.
type internal QuerySerializationAccess =
    | ClrMember of string * System.Type
    | StoredLabel of obj

type internal QuerySerializationStep = {
    Kind: SqlExpr
    Label: SqlExpr
    MemberType: SqlExpr
    ActualType: SqlExpr
}

/// A statement-local input operand travels with the value whose writer it describes.
type internal QuerySerializationOperand = {
    Descriptor: SqlExpr
    Accesses: QuerySerializationStep array
}

/// Original writer and child facts belong to the same produced query value.
type internal QueryValueCarrier = SqlScalar | CanonicalToken

/// Membership contexts preserve established public argument admission.
type internal QueryMembershipContext = DefaultMembership | RootValueMembership | ReceiverScopedEntityMembership

/// Loading directives remain attached to the source that authored them.
type internal QueryOwnerLoading = {
    Excluded: string array
    Included: string array
    Whitelist: bool
}

/// Logical navigation admission follows the authored source through projections.
type internal QueryNavigationScope = {
    Loading: QueryOwnerLoading
    Path: string list
    Depth: int
}

/// Known physical sources are authored by the payload producer.
type internal QueryPhysicalSource = {
    TableName: string
    OwnerType: System.Type
    Loading: QueryOwnerLoading
}

/// The selected source and identity travel with their exact value occurrence.
type internal QueryPhysicalOwner = {
    Sources: QueryPhysicalSource array
    Descriptor: SqlExpr
} with
    member private this.Field index =
        match this.Descriptor with
        | SqlExpr.JsonArrayExpr fields when fields.Length = 4 -> fields[index]
        | _ -> SqlExpr.FunctionCall("jsonb_extract", [this.Descriptor; SqlExpr.Literal(SqlLiteral.String($"$[{index}]"))])
    member this.Identity = this.Field 2
    member this.Collection = this.Field 0

/// Reference lookup retains the authored relation as well as its physical owner.
type internal QueryReferenceSource = {
    Owner: QueryPhysicalSource voption
    PropertyName: string
}

type internal QueryReferenceOwner = {
    Sources: QueryReferenceSource array
    Descriptor: SqlExpr
}

[<Struct>]
type internal QuerySerializationInput = {
    OriginalType: System.Type
    Shape: JsonSerializator.JsonSerializationShape voption
    Children: QuerySerializationInput array
    Operand: QuerySerializationOperand voption
    Source: SqlExpr voption
    SourceOffset: int
    Carrier: QueryValueCarrier
    /// The authored index may retain its original exceptional text carrier.
    LegacyCharacterText: bool
    PhysicalOwner: QueryPhysicalOwner voption
    ReferenceOwner: QueryReferenceOwner voption
    Navigation: QueryNavigationScope array
} with
    static member ForType(target) =
        { OriginalType = target; Shape = ValueNone; Children = Array.empty; Operand = ValueNone; Source = ValueNone; SourceOffset = 0; Carrier = CanonicalToken; LegacyCharacterText = false; PhysicalOwner = ValueNone; ReferenceOwner = ValueNone; Navigation = Array.empty }
    static member ForOwner(target, table, identity, ?loading: QueryOwnerLoading) =
        let loading = defaultArg loading { Excluded = Array.empty; Included = Array.empty; Whitelist = false }
        let texts values = SqlExpr.JsonArrayExpr(values |> Array.map (SqlLiteral.String >> SqlExpr.Literal) |> Array.toList)
        let policy = SqlExpr.JsonArrayExpr [texts loading.Excluded; texts loading.Included; SqlExpr.Literal(SqlLiteral.Integer(if loading.Whitelist then 1L else 0L))]
        let owner: QueryPhysicalOwner = {
            Sources = [|{ TableName = table; OwnerType = target; Loading = loading }|]
            Descriptor = SqlExpr.JsonArrayExpr [SqlExpr.Literal(SqlLiteral.String table); SqlExpr.Literal(SqlLiteral.String(TypeMetadata.typeIdentityKey target)); identity; policy]
        }
        { QuerySerializationInput.ForType target with PhysicalOwner = ValueSome owner; Navigation = [|{ Loading = loading; Path = []; Depth = 0 }|] }
    member this.Facts =
        match this.Shape with
        | ValueSome shape -> shape
        | ValueNone -> JsonSerializator.JsonValue.SerializedValueFacts (Utils.optionalPayloadType this.OriginalType)
    static member Grouping(target, key: QuerySerializationInput, item: QuerySerializationInput) =
        { OriginalType = target
          Shape = ValueSome(JsonSerializator.JsonValue.SerializedValueFacts target)
          Children = [|key; item|]
          Operand = ValueNone; Source = ValueNone; SourceOffset = 0; Carrier = CanonicalToken; LegacyCharacterText = false; PhysicalOwner = ValueNone; ReferenceOwner = ValueNone; Navigation = Array.empty }
    static member Sequence(target, item: QuerySerializationInput) =
        { OriginalType = target
          Shape = ValueSome(JsonSerializator.JsonSerializationShape.SequenceValues(ModelMetadata.SequenceElement target))
          Children = [|item|]
          Operand = ValueNone; Source = ValueNone; SourceOffset = 0; Carrier = CanonicalToken; LegacyCharacterText = false; PhysicalOwner = ValueNone; ReferenceOwner = ValueNone; Navigation = Array.empty }
    static member Members(target, names: string array, declared: System.Type array, children: QuerySerializationInput array) =
        { OriginalType = target
          Shape = ValueSome(JsonSerializator.JsonSerializationShape.DeclaredMembers(
              Array.mapi (fun index name -> struct(name, declared.[index])) names))
          Children = children
          Operand = ValueNone; Source = ValueNone; SourceOffset = 0; Carrier = CanonicalToken; LegacyCharacterText = false; PhysicalOwner = ValueNone; ReferenceOwner = ValueNone; Navigation = Array.empty }
    static member Positions(target, children: QuerySerializationInput array) =
        { OriginalType = target
          Shape = ValueSome(
              if target.IsArray then JsonSerializator.JsonSerializationShape.DeclaredArrayMembers(Array.create children.Length (target.GetElementType()))
              else JsonSerializator.JsonValue.SerializedValueFacts target)
          Children = children
          Operand = ValueNone; Source = ValueNone; SourceOffset = 0; Carrier = CanonicalToken; LegacyCharacterText = false; PhysicalOwner = ValueNone; ReferenceOwner = ValueNone; Navigation = Array.empty }

/// A producer retains both its value and the original writer responsibility.
/// Packed results select the value and descriptor in one SQL occurrence.
type internal QueryProducedValue =
    | DirectValue of value: SqlExpr * input: QuerySerializationInput
    | PackedValue of packet: SqlExpr * input: QuerySerializationInput
    member this.Input =
        match this with DirectValue(_, input) | PackedValue(_, input) -> input
