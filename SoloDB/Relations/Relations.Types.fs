
module internal SoloDatabase.RelationsTypes

open System
open System.Reflection
open System.Collections.Concurrent
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SoloDatabase.JsonSerializator
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

type internal RelationWritePlan = {
    Kind: string
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

let internal emptyPlan kind ownerType =
    { Kind = kind
      OwnerType = ownerType
      Ops = [] }

let internal relationSpecsCache = ConcurrentDictionary<Type, (PropertyInfo * RelationKind * Type * Type voption * DeletePolicy * DeletePolicy * bool * DBRefOrder) array>()
let internal deleteGuard = new System.Threading.ThreadLocal<System.Collections.Generic.HashSet<string>>(fun () -> System.Collections.Generic.HashSet<string>(StringComparer.Ordinal))

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
    let value =
        if String.IsNullOrWhiteSpace value then
            "Restrict"
        else
            value.Trim()
    match Enum.TryParse<DeletePolicy>(value, true) with
    | true, policy -> policy
    | _ -> raise (InvalidOperationException(
        $"Error: Invalid delete policy '{value}'.\nReason: The policy value is unsupported.\nFix: Use Restrict, Cascade, Unlink, or Deletion as appropriate."))

let internal parseOnDeletePolicy (value: string) =
    let policy =
        if String.IsNullOrWhiteSpace value then
            DeletePolicy.Restrict
        else
            parseDeletePolicy value
    match policy with
    | DeletePolicy.Deletion ->
        raise (InvalidOperationException(
            "Error: Invalid delete policy.\nReason: OnDelete cannot be Deletion.\nFix: Use Restrict, Cascade, or Unlink for OnDelete."))
    | _ -> policy

let internal parseOnOwnerDeletePolicy (value: string) =
    let policy =
        if String.IsNullOrWhiteSpace value then
            DeletePolicy.Deletion
        else
            parseDeletePolicy value
    match policy with
    | DeletePolicy.Cascade ->
        raise (InvalidOperationException(
            "Error: Invalid owner-delete policy.\nReason: OnOwnerDelete cannot be Cascade.\nFix: Use Deletion instead."))
    | _ -> policy

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
        let idProp = getWritableInt64IdPropertyOrThrow targetType
        idProp.GetValue(entity) :?> int64

let internal writeEntityId (targetType: Type) (entity: obj) (id: int64) =
    let idProp = getWritableInt64IdPropertyOrThrow targetType
    idProp.SetValue(entity, box id)

let internal jsonSerializeMethod =
    typeof<JsonValue>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    |> Array.find (fun m -> m.Name = "Serialize" && m.IsGenericMethodDefinition && m.GetParameters().Length = 1)

let internal jsonSerializeWithTypeMethod =
    typeof<JsonValue>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    |> Array.find (fun m -> m.Name = "SerializeWithType" && m.IsGenericMethodDefinition && m.GetParameters().Length = 1)

let internal serializeEntityForStorage (targetType: Type) (entity: obj) =
    let includeType = mustIncludeTypeInformationInSerializationFn targetType
    let serializer =
        if includeType then
            jsonSerializeWithTypeMethod.MakeGenericMethod(targetType)
        else
            jsonSerializeMethod.MakeGenericMethod(targetType)

    let json = serializer.Invoke(null, [| entity |]) :?> JsonValue
    match json with
    | JsonValue.Object objMap when (tryGetWritableInt64IdProperty targetType).IsSome ->
        objMap.Remove("Id") |> ignore
    | _ -> ()
    json.ToJsonString()
