
module internal SoloDatabase.RelationsTypes

open System
open System.Reflection
open System.Collections.Generic
open System.Collections.Concurrent
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SoloDatabase.JsonSerializator
open NativeSQLiteTools
open JsonFunctions

type internal IRelationCollectionFactory =
    abstract CreateCollection<'T> : collectionName: string -> ISoloDBCollection<'T>

type internal RelationUpdateManyOp =
    | SetDBRefToId of PropertyPath: string * TargetType: Type * TargetId: int64
    | SetDBRefToTypedId of PropertyPath: string * TargetType: Type * TargetIdType: Type * TargetTypedId: obj
    | SetDBRefToNone of PropertyPath: string * TargetType: Type
    | AddDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
    | RemoveDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
    | ClearDBRefMany of PropertyPath: string * TargetType: Type
    | SetFileSystemRef of PropertyPath: string * TargetId: int64
    | RefreshFileSystemRef of PropertyPath: string
    | AddFileSystemRef of PropertyPath: string * TargetId: int64
    | RemoveFileSystemRef of PropertyPath: string * TargetId: int64
    | ClearFileSystemRefs of PropertyPath: string
    | AcceptFileSystemRefChanges of Tracker: IFileSystemRefMany * IsLoaded: bool

type internal RelationPlanKind =
    | Insert
    | Upsert
    | Update
    | UpdateMany

type internal RelationWritePlan = {
    Kind: RelationPlanKind
    OwnerType: Type
    Ops: RelationUpdateManyOp list
}

type internal RelationDeletePlan = {
    OwnerId: int64
    OwnerType: Type
}

type internal RelationDescriptor = {
    OwnerTable: string
    OwnerType: Type
    Property: RelationMember
    PropertyPath: string
    Kind: RelationKind
    TargetType: Type
    TargetTable: string
    LinkSourceTable: string
    LinkTargetTable: string
    OwnerUsesSourceColumn: bool
    RelationName: string
    LinkTable: string
    OnDelete: DeletePolicy
    OnOwnerDelete: DeletePolicy
    IsUnique: bool
    OrderBy: DBRefOrder
    TypedIdType: Type voption
    TargetSoloIdProperty: CustomIdMember voption
}

/// Resolved facts live only inside one public operation, never on a pooled collection.
type internal RelationPreparationEntry() =
    member val Descriptors: RelationDescriptor array voption = ValueNone with get, set
    member val DescriptorMap: Dictionary<string, RelationDescriptor> voption = ValueNone with get, set
    member val SchemaReady = false with get, set

type internal RelationPreparation() =
    let mutable allowReuse = true
    let entries = Dictionary<struct(string * Type), RelationPreparationEntry>()
    let targetTables = HashSet<string>(StringComparer.Ordinal)
    let mutable callbackVersion = -1L
    let graphInitial = Dictionary<struct(string * int64), HashSet<struct(string * int64)>>()
    member _.GraphInitial = graphInitial
    member _.AllowReuse = allowReuse
    member this.DisableReuse() =
        allowReuse <- false
        this.Invalidate()
    member _.Invalidate() =
        entries.Clear()
        targetTables.Clear()
    member this.Refresh(connection: NativeSqliteConnection) =
        let version =
            match connection with
            | :? NativeCachingDbConnection as cached -> cached.EventHandlerInvocationVersion
            | _ -> -1L
        if version <> callbackVersion then
            this.Invalidate()
            callbackVersion <- version
    member this.Entry(connection, table, ownerType) =
        this.Refresh(connection)
        let key = struct(table, ownerType)
        match entries.TryGetValue key with
        | true, entry -> entry
        | _ -> let entry = RelationPreparationEntry() in entries.Add(key, entry); entry
    member this.TargetTables(connection) =
        this.Refresh(connection)
        targetTables

type internal RelationTxContext = {
    Connection: SoloDatabase.NativeSqliteConnection
    OwnerTable: string
    OwnerType: Type
    InTransaction: bool
    Preparation: RelationPreparation
    CollectionFactory: IRelationCollectionFactory option
}

[<CLIMutable>]
type internal RelationMetadataRow = {
    Name: string
    SourceCollection: string
    TargetCollection: string
    PropertyName: string
    OwnerCollection: string
    RefKind: string
    OnDelete: string
    OnOwnerDelete: string
    IsUnique: int64
}

let internal ensureTransaction (tx: RelationTxContext) =
    if not tx.InTransaction then
        raise (InvalidOperationException(
            "Error: Relations API requires an active transaction.\nReason: Relation mutations must be atomic.\nFix: Wrap the call in WithTransaction or TransactionalSoloDB."))

let internal ensureTxContext (tx: RelationTxContext) =
    if isNull tx.Connection then nullArg "tx.Connection"
    if String.IsNullOrWhiteSpace tx.OwnerTable then
        raise (ArgumentException("OwnerTable is required.", "tx.OwnerTable"))
    if isNull tx.OwnerType then nullArg "tx.OwnerType"

let internal ensureOwnerInstance (ownerType: Type) (owner: obj) (argName: string) =
    if isNull owner then nullArg argName
    if isNull ownerType then nullArg "ownerType"
    let actual = owner.GetType()
    if not (ownerType.IsAssignableFrom actual) then
        raise (ArgumentException($"Invalid owner instance type. Expected assignable to {ownerType.FullName}, got {actual.FullName}.", argName))

let internal emptyPlan (kind: RelationPlanKind) ownerType =
    { Kind = kind
      OwnerType = ownerType
      Ops = [] }

type internal DeleteTraversalContext =
    { GuardSet: System.Collections.Generic.HashSet<string> }

let internal createDeleteTraversalContext () =
    { GuardSet = System.Collections.Generic.HashSet<string>(StringComparer.Ordinal) }

let internal quoteIdentifier (name: string) =
    "\"" + name.Replace("\"", "\"\"") + "\""

let internal relationName (ownerTable: string) (propertyName: string) =
    $"{ownerTable}_{propertyName}"

let internal canonicalManyRelationName (leftTable: string) (rightTable: string) =
    if StringComparer.Ordinal.Compare(leftTable, rightTable) <= 0 then
        $"{leftTable}_{rightTable}"
    else
        $"{rightTable}_{leftTable}"

let internal linkTableFromRelationName (name: string) =
    "SoloDBRelLink_" + name

/// For shared many-to-many link tables, the lexicographically smaller collection
/// owns SourceId and the larger owns TargetId. This must stay canonical across
/// schema validation and delete-time metadata layout resolution.
let internal sharedManyOwnerUsesSourceColumn (ownerTable: string) (targetTable: string) =
    StringComparer.Ordinal.Compare(ownerTable, targetTable) <= 0

let internal stringToRelationKind (value: string) =
    match value with
    | "Single" -> Single
    | "Many" -> Many
    | _ -> raise (InvalidOperationException(
        $"Error: Invalid relation kind '{value}'.\nReason: The stored relation kind is not recognized.\nFix: Ensure metadata is valid or rebuild the schema."))

let internal relationKindToString kind =
    match kind with
    | Single -> "Single"
    | Many -> "Many"

let internal parseDeletePolicy (value: string) =
    let token =
        if isNull value then
            ""
        else
            value.Trim()
    if String.IsNullOrWhiteSpace token then
        raise (InvalidOperationException(
            "Error: Invalid delete policy '<empty>'.\nReason: The policy value is missing.\nFix: Use Restrict, Cascade, Unlink, or Deletion as appropriate."))
    elif StringComparer.OrdinalIgnoreCase.Equals(token, "Restrict") then
        DeletePolicy.Restrict
    elif StringComparer.OrdinalIgnoreCase.Equals(token, "Cascade") then
        DeletePolicy.Cascade
    elif StringComparer.OrdinalIgnoreCase.Equals(token, "Unlink") then
        DeletePolicy.Unlink
    elif StringComparer.OrdinalIgnoreCase.Equals(token, "Deletion") then
        DeletePolicy.Deletion
    else
        raise (InvalidOperationException(
            $"Error: Invalid delete policy '{token}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Cascade, Unlink, or Deletion as appropriate."))

let internal parseOnDeletePolicy (value: string) =
    if String.IsNullOrWhiteSpace value then
        raise (InvalidOperationException(
            "Error: Invalid delete policy '<empty>'.\nReason: OnDelete policy value is missing.\nFix: Use Restrict, Cascade, or Unlink for OnDelete."))
    let policy = parseDeletePolicy value
    match policy with
    | DeletePolicy.Deletion ->
        raise (InvalidOperationException(
            "Error: Invalid delete policy.\nReason: OnDelete cannot be Deletion.\nFix: Use Restrict, Cascade, or Unlink for OnDelete."))
    | DeletePolicy.Restrict
    | DeletePolicy.Cascade
    | DeletePolicy.Unlink -> policy
    | _ ->
        raise (InvalidOperationException(
            $"Error: Invalid delete policy '{policy}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Cascade, or Unlink for OnDelete."))

let internal parseOnOwnerDeletePolicy (value: string) =
    if String.IsNullOrWhiteSpace value then
        raise (InvalidOperationException(
            "Error: Invalid owner-delete policy '<empty>'.\nReason: OnOwnerDelete policy value is missing.\nFix: Use Restrict, Unlink, or Deletion for OnOwnerDelete."))
    let policy = parseDeletePolicy value
    match policy with
    | DeletePolicy.Cascade ->
        raise (InvalidOperationException(
            "Error: Invalid owner-delete policy.\nReason: OnOwnerDelete cannot be Cascade.\nFix: Use Deletion instead."))
    | DeletePolicy.Restrict
    | DeletePolicy.Unlink
    | DeletePolicy.Deletion -> policy
    | _ ->
        raise (InvalidOperationException(
            $"Error: Invalid owner-delete policy '{policy}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Unlink, or Deletion for OnOwnerDelete."))

let internal requireWritableInt64Id (t: Type) =
    if not (EntityIdentity.hasWritableInt64Id t) then
        raise (InvalidOperationException(
            $"Error: Relation target type '{t.FullName}' does not expose a writable int64 Id.\nReason: DBRef/DBRefMany requires an int64 Id.\nFix: Add an int64 Id property or use supported relation types."))

let internal readEntityIdOrZero (targetType: Type) (entity: obj) =
    if isNull entity then 0L
    elif not (targetType.IsAssignableFrom(entity.GetType())) then
        raise (InvalidOperationException(
            $"Error: Invalid relation target instance type.\nReason: Expected assignable to {targetType.FullName}, got {entity.GetType().FullName}.\nFix: Use the correct target type for this relation."))
    else
        EntityIdentity.compiledInt64IdReader(targetType).Invoke(entity)

let internal writeEntityId (targetType: Type) (entity: obj) (id: int64) =
    EntityIdentity.compiledInt64IdWriter(targetType).Invoke(entity, id)

let internal serializeEntityForStorage (targetType: Type) (entity: obj) =
    let includeType = TypeMetadata.requiresTypeInformation targetType
    JsonValue.WriteRuntimeBuffer(targetType,entity,includeType,EntityIdentity.hasWritableInt64Id targetType)
