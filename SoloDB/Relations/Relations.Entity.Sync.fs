module internal SoloDatabase.RelationsEntitySync

open System
open System.Reflection
open System.Runtime.Serialization
open System.Collections.Generic
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SQLiteTools
open RelationsTypes
open RelationsSchema

let internal readMetadataByOwner (connection: SqliteConnection) (ownerTable: string) =
    connection.Query<RelationMetadataRow>(
        """
	SELECT Name,
	       SourceCollection,
	       TargetCollection,
	       PropertyName,
	       OwnerCollection,
	       RefKind,
	       OnDelete,
	       OnOwnerDelete,
	       IsUnique
	FROM SoloDBRelation
	WHERE OwnerCollection = @ownerCollection;
	""",
        {| ownerCollection = ownerTable |})
    |> Seq.toArray

let internal readMetadataByTarget (connection: SqliteConnection) (targetTable: string) =
    connection.Query<RelationMetadataRow>(
        """
	SELECT Name,
	       SourceCollection,
	       TargetCollection,
	       PropertyName,
	       OwnerCollection,
	       RefKind,
	       OnDelete,
	       OnOwnerDelete,
	       IsUnique
	FROM SoloDBRelation
	WHERE TargetCollection = @targetCollection;
	""",
        {| targetCollection = targetTable |})
    |> Seq.toArray

let internal relationDescriptorByPath (tx: RelationTxContext) (ownerType: Type) =
    let map = Dictionary<string, RelationDescriptor>(StringComparer.Ordinal)
    for descriptor in buildRelationDescriptors tx ownerType do
        map.[descriptor.PropertyPath] <- descriptor
    map

let internal insertTargetEntity (tx: RelationTxContext) (targetTable: string) (targetType: Type) (entity: obj) =
    if isNull entity then
        raise (ArgumentNullException("entity", "Cascade insert target entity cannot be null."))

    // Reject negative id before any schema side effects.
    let id = readEntityIdOrZero targetType entity
    if id < 0L then
        raise (InvalidOperationException(
            $"Error: Invalid Id {id} on pending relation target '{targetType.FullName}'.\nReason: Id must be >= 0.\nFix: Use 0 for new entities or a positive Id for existing entities."))

    ensureCollectionTableExists tx.Connection targetTable

    let json = serializeEntityForStorage targetType entity
    let qTarget = quoteIdentifier targetTable
    let insertedId =
        if id > 0L then
            tx.Connection.QueryFirst<int64>($"INSERT INTO {qTarget}(Id, Value) VALUES (@id, jsonb(@jsonText)) RETURNING Id;", {| id = id; jsonText = json |})
        else
            tx.Connection.QueryFirst<int64>($"INSERT INTO {qTarget}(Value) VALUES (jsonb(@jsonText)) RETURNING Id;", {| jsonText = json |})

    writeEntityId targetType entity insertedId
    insertedId

let internal ensureTargetExists (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureCollectionTableExists tx.Connection targetTable
    let exists =
        tx.Connection.QueryFirst<int64>($"SELECT CASE WHEN EXISTS (SELECT 1 FROM {quoteIdentifier targetTable} WHERE Id = @id) THEN 1 ELSE 0 END", {| id = targetId |}) = 1L
    if not exists then
        raise (InvalidOperationException(
            $"Error: Relation target '{targetTable}' with Id={targetId} does not exist.\nReason: The target row is missing.\nFix: Insert the target first or correct the Id."))

let internal readDbRefId (dbRefObj: obj) =
    if isNull dbRefObj then
        0L
    else
        RelationsAccessorCache.compiledDbRefIdReader(dbRefObj.GetType()).Invoke(dbRefObj)

let internal tryGetValueOptionValue (valueOpt: obj) =
    if isNull valueOpt then
        ValueNone
    else
        match RelationsAccessorCache.compiledValueOptionAccessors (valueOpt.GetType()) with
        | ValueNone -> ValueNone
        | ValueSome acc ->
            if acc.IsSome.Invoke(valueOpt) then
                let value = acc.GetValue.Invoke(valueOpt)
                if isNull value then ValueNone else ValueSome value
            else
                ValueNone

let internal tryGetVOptionProperty (dbRefObj: obj) (propertyName: string) =
    if isNull dbRefObj then
        ValueNone
    else
        let t = dbRefObj.GetType()
        let prop = t.GetProperty(propertyName, BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)
        if isNull prop then
            ValueNone
        else
            let getter = RelationsAccessorCache.compiledPropGetter prop
            getter.Invoke(dbRefObj) |> tryGetValueOptionValue

let internal tryGetPendingEntity (dbRefObj: obj) =
    tryGetVOptionProperty dbRefObj "PendingEntity"

let internal tryGetPendingTypedId (dbRefObj: obj) =
    tryGetVOptionProperty dbRefObj "PendingTypedId"

let internal createDbRefTo (dbRefType: Type) (id: int64) =
    let factories = RelationsAccessorCache.compiledDbRefFactories dbRefType
    factories.ToOrResolved.Invoke(id)

let internal createDbRefLoaded (dbRefType: Type) (id: int64) (entity: obj) =
    let factories = RelationsAccessorCache.compiledDbRefFactories dbRefType
    factories.Loaded.Invoke(id, entity)

let internal updateDbRefJson (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) (propertyPath: string) (targetId: int64) =
    let path = "$." + propertyPath
    let jsonText = if targetId = 0L then "null" else string targetId
    tx.Connection.Execute(
        $"UPDATE {quoteIdentifier ownerTable} SET Value = jsonb_set(Value, @path, jsonb(@jsonText)) WHERE Id = @ownerId;",
        {| path = path; jsonText = jsonText; ownerId = ownerId |}) |> ignore

let internal shouldSyncDbRefJson (descriptor: RelationDescriptor) =
    isNull (descriptor.Property.GetCustomAttribute<IgnoreDataMemberAttribute>(true))

let internal ensureLinkWriteApplied (tx: RelationTxContext) (linkTable: string) (sourceId: int64) (targetId: int64) (opName: string) (rowsAffected: int) =
    if rowsAffected <= 0 then
        let exists =
            tx.Connection.QueryFirst<int64>(
                $"SELECT CASE WHEN EXISTS (SELECT 1 FROM {quoteIdentifier linkTable} WHERE SourceId = @sourceId AND TargetId = @targetId) THEN 1 ELSE 0 END",
                {| sourceId = sourceId; targetId = targetId |}) = 1L
        if not exists then
            raise (InvalidOperationException(
                $"Error: Relation link write failed in '{linkTable}'.\nReason: {opName} reported no affected rows and no link row exists for ({sourceId}, {targetId}).\nFix: Verify relation mutation inputs and constraints before retrying."))

let internal resolveTypedIdToTargetId (tx: RelationTxContext) (descriptor: RelationDescriptor) (typedId: obj) =
    match descriptor.TypedIdType, descriptor.TargetSoloIdProperty with
    | ValueSome idType, ValueSome soloIdProp ->
        ensureCollectionTableExists tx.Connection descriptor.TargetTable
        if isNull typedId then
            raise (InvalidOperationException(
                $"Error: Null typed id on relation '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}'.\nReason: DBRef<'TTarget,'TId>.To requires a non-null typed id.\nFix: Use DBRef.None or provide a valid typed id."))
        if not (idType.IsAssignableFrom(typedId.GetType())) then
            raise (InvalidOperationException(
                $"Error: Typed id mismatch on relation '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}'.\nReason: Expected id type '{idType.FullName}', got '{typedId.GetType().FullName}'.\nFix: Use the exact target [SoloId] type."))

        let jsonPath = "$." + soloIdProp.Name
        let matches =
            tx.Connection.Query<int64>(
                $"SELECT Id FROM {quoteIdentifier descriptor.TargetTable} WHERE jsonb_extract(Value, @path) = @typedId;",
                {| path = jsonPath; typedId = typedId |})
            |> Seq.toArray

        match matches with
        | [| id |] -> id
        | [||] ->
            raise (InvalidOperationException(
                $"Error: Typed relation target not found for '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}'.\nReason: No row in '{descriptor.TargetTable}' matches [SoloId] value '{typedId}'.\nFix: Insert the target first or correct the typed id."))
        | _ ->
            raise (InvalidOperationException(
                $"Error: Ambiguous typed relation target for '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}'.\nReason: Multiple rows in '{descriptor.TargetTable}' share [SoloId] value '{typedId}'.\nFix: Enforce unique index on the [SoloId] path and deduplicate data."))
    | _ ->
        raise (InvalidOperationException(
            $"Error: Relation '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' is not configured for typed-id resolution.\nReason: Missing DBRef<'TTarget,'TId> metadata.\nFix: Use DBRef<T> with row id or configure typed-id relation correctly."))

