
module internal SoloDatabase.RelationsTypes

open System
open System.Reflection
open System.Collections.Concurrent
open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SoloDatabase.JsonSerializator
open SQLiteTools
open JsonFunctions

type internal RelationTxContext = {
    Connection: Microsoft.Data.Sqlite.SqliteConnection
    OwnerTable: string
    OwnerType: Type
    InTransaction: bool
}

type internal RelationUpdateManyOp =
    | SetDBRefToId of PropertyPath: string * TargetType: Type * TargetId: int64
    | SetDBRefToTypedId of PropertyPath: string * TargetType: Type * TargetIdType: Type * TargetTypedId: obj
    | SetDBRefToNone of PropertyPath: string * TargetType: Type
    | AddDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
    | RemoveDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
    | ClearDBRefMany of PropertyPath: string * TargetType: Type

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

type internal RelationKind =
    | Single
    | Many

type internal RelationDescriptor = {
    OwnerTable: string
    OwnerType: Type
    Property: PropertyInfo
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
    TargetSoloIdProperty: PropertyInfo voption
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

let internal relationSpecsCache = ConcurrentDictionary<Type, (PropertyInfo * RelationKind * Type * Type voption * DeletePolicy * DeletePolicy * bool * DBRefOrder) array>()

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

let internal tryGetWritableInt64IdProperty (t: Type) =
    let prop = t.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
    if isNull prop || not prop.CanRead || not prop.CanWrite || prop.PropertyType <> typeof<int64> then
        ValueNone
    else
        ValueSome prop

let internal getWritableInt64IdPropertyOrThrow (t: Type) =
    match tryGetWritableInt64IdProperty t with
    | ValueSome prop -> prop
    | ValueNone ->
        raise (InvalidOperationException(
            $"Error: Relation target type '{t.FullName}' does not expose a writable int64 Id.\nReason: DBRef/DBRefMany requires an int64 Id.\nFix: Add an int64 Id property or use supported relation types."))

let internal readEntityIdOrZero (targetType: Type) (entity: obj) =
    if isNull entity then 0L
    elif not (targetType.IsAssignableFrom(entity.GetType())) then
        raise (InvalidOperationException(
            $"Error: Invalid relation target instance type.\nReason: Expected assignable to {targetType.FullName}, got {entity.GetType().FullName}.\nFix: Use the correct target type for this relation."))
    else
        RelationsAccessorCache.compiledInt64IdReader(targetType).Invoke(entity)

let internal writeEntityId (targetType: Type) (entity: obj) (id: int64) =
    RelationsAccessorCache.compiledInt64IdWriter(targetType).Invoke(entity, id)

let internal jsonSerializeMethod =
    typeof<JsonValue>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    |> Array.find (fun m -> m.Name = "Serialize" && m.IsGenericMethodDefinition && m.GetParameters().Length = 1)

let internal jsonSerializeWithTypeMethod =
    typeof<JsonValue>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    |> Array.find (fun m -> m.Name = "SerializeWithType" && m.IsGenericMethodDefinition && m.GetParameters().Length = 1)

let internal serializeEntityForStorage (targetType: Type) (entity: obj) =
    let includeType = mustIncludeTypeInformationInSerializationFn targetType
    let baseMethod = if includeType then jsonSerializeWithTypeMethod else jsonSerializeMethod
    let serializer = RelationsAccessorCache.compiledSerializer targetType includeType baseMethod
    let json = serializer.Invoke(entity) :?> JsonValue
    match json with
    | JsonValue.Object objMap when (tryGetWritableInt64IdProperty targetType).IsSome ->
        objMap.Remove("Id") |> ignore
    | _ -> ()
    json.ToJsonString()

// ─── Per-owner RelationVersion baseline tracking ─────────────────────────────

/// Weak table mapping entity instances to their loaded RelationVersion.
/// Entries are collected when the entity instance is GC'd.
let internal relationVersionBaseline = ConditionalWeakTable<obj, obj>()

/// Record the RelationVersion that was current when this entity was loaded.
let internal captureRelationVersion (entity: obj) (version: int64) =
    // ConditionalWeakTable.AddOrUpdate is not available on netstandard2.0.
    // Remove first if present, then add.
    relationVersionBaseline.Remove(entity) |> ignore
    relationVersionBaseline.Add(entity, box version)

/// Retrieve the RelationVersion captured at load time for this entity, or 0L if not tracked.
let internal getLoadedRelationVersion (entity: obj) =
    match relationVersionBaseline.TryGetValue(entity) with
    | true, v -> unbox<int64> v
    | _ -> 0L

let [<Literal>] internal relationVersionMetadataPath = "$.RelationVersion"

/// Read the current persisted RelationVersion from the Metadata column. Returns 0 if not set.
let internal readPersistedRelationVersion (connection: SqliteConnection) (ownerTable: string) (ownerId: int64) =
    let qOwner = quoteIdentifier ownerTable
    connection.QueryFirstOrDefault<int64>(
        $"SELECT COALESCE(CAST(jsonb_extract(Metadata, @path) AS INTEGER), 0) FROM {qOwner} WHERE Id = @id;",
        {| path = relationVersionMetadataPath; id = ownerId |})
