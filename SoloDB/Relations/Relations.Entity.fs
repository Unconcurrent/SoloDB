
module internal SoloDatabase.RelationsEntity

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
       COALESCE(NULLIF(OnDelete, ''), 'Restrict') AS OnDelete,
       COALESCE(NULLIF(OnOwnerDelete, ''), 'Deletion') AS OnOwnerDelete,
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
       COALESCE(NULLIF(OnDelete, ''), 'Restrict') AS OnDelete,
       COALESCE(NULLIF(OnOwnerDelete, ''), 'Deletion') AS OnOwnerDelete,
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

    ensureCollectionTableExists tx.Connection targetTable

    let id = readEntityIdOrZero targetType entity
    if id < 0L then
        raise (InvalidOperationException($"Invalid Id {id} on pending relation target '{targetType.FullName}'. Id must be >= 0."))

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
        raise (InvalidOperationException($"Relation target '{targetTable}' with Id={targetId} does not exist."))

let internal readDbRefId (dbRefObj: obj) =
    if isNull dbRefObj then
        0L
    else
        let idProp = dbRefObj.GetType().GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull idProp then 0L else idProp.GetValue(dbRefObj) :?> int64

let internal tryGetValueOptionValue (valueOpt: obj) =
    if isNull valueOpt then
        ValueNone
    else
        let t = valueOpt.GetType()
        let isSomeProp = t.GetProperty("IsSome", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull isSomeProp then
            ValueNone
        elif isSomeProp.GetValue(valueOpt) :?> bool then
            let valueProp = t.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
            if isNull valueProp then ValueNone else
            let value = valueProp.GetValue(valueOpt)
            if isNull value then ValueNone else ValueSome value
        else
            ValueNone

let internal tryGetPendingEntity (dbRefObj: obj) =
    if isNull dbRefObj then
        ValueNone
    else
        let pendingProp = dbRefObj.GetType().GetProperty("PendingEntity", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)
        if isNull pendingProp then
            ValueNone
        else
            pendingProp.GetValue(dbRefObj) |> tryGetValueOptionValue

let internal createDbRefTo (dbRefType: Type) (id: int64) =
    let toMethod = dbRefType.GetMethod("To", BindingFlags.Public ||| BindingFlags.Static, null, [| typeof<int64> |], null)
    if isNull toMethod then
        raise (InvalidOperationException($"Could not resolve DBRef.To on type {dbRefType.FullName}."))
    toMethod.Invoke(null, [| box id |])

let internal createDbRefLoaded (dbRefType: Type) (id: int64) (entity: obj) =
    let loadedMethod =
        dbRefType.GetMethods(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
        |> Array.find (fun m -> m.Name = "Loaded" && m.GetParameters().Length = 2)
    loadedMethod.Invoke(null, [| box id; entity |])

let internal resolveSingleTargetIdAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) =
    let value = descriptor.Property.GetValue(owner)
    let id = readDbRefId value
    if id > 0L then
        id
    else
        match tryGetPendingEntity value with
        | ValueSome pending ->
            let insertedId = insertTargetEntity tx descriptor.TargetTable descriptor.TargetType pending
            let dbRef = createDbRefTo descriptor.Property.PropertyType insertedId
            descriptor.Property.SetValue(owner, dbRef)
            insertedId
        | ValueNone ->
            0L

let internal collectManyTargetIdsAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) (includeWhenUnloaded: bool) =
    let trackerObj = descriptor.Property.GetValue(owner)
    if isNull trackerObj then
        if includeWhenUnloaded then ValueSome [||] else ValueNone
    else
        match trackerObj with
        | :? IDBRefManyInternal as tracker ->
            if not includeWhenUnloaded && not tracker.IsLoaded then
                ValueNone
            else
                let ids = System.Collections.Generic.HashSet<int64>()
                for item in tracker.GetCurrentItemsBoxed() do
                    if isNull item then
                        ()
                    else
                        let mutable id = readEntityIdOrZero descriptor.TargetType item
                        if id <= 0L then
                            id <- insertTargetEntity tx descriptor.TargetTable descriptor.TargetType item
                        if id > 0L then
                            ids.Add(id) |> ignore
                ValueSome (ids |> Seq.toArray)
        | _ ->
            raise (InvalidOperationException($"Property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' is expected to implement IDBRefManyInternal."))

let internal updateDbRefJson (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) (propertyPath: string) (targetId: int64) =
    let path = "$." + propertyPath
    let jsonText = if targetId = 0L then "null" else string targetId
    tx.Connection.Execute(
        $"UPDATE {quoteIdentifier ownerTable} SET Value = jsonb_set(Value, @path, jsonb(@jsonText)) WHERE Id = @ownerId;",
        {| path = path; jsonText = jsonText; ownerId = ownerId |}) |> ignore

let internal shouldSyncDbRefJson (descriptor: RelationDescriptor) =
    isNull (descriptor.Property.GetCustomAttribute<IgnoreDataMemberAttribute>(true))

let internal readSingleIdNoCascade (descriptor: RelationDescriptor) (owner: obj) =
    descriptor.Property.GetValue(owner) |> readDbRefId

let internal applyOps (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) (updateOwnerJson: bool) =
    let descriptorMap = relationDescriptorByPath tx tx.OwnerType

    let resolveDescriptor propertyPath =
        match descriptorMap.TryGetValue(propertyPath) with
        | true, descriptor -> descriptor
        | _ ->
            let firstSegment =
                let idx = propertyPath.IndexOf('.')
                if idx <= 0 then propertyPath else propertyPath.Substring(0, idx)
            match descriptorMap.TryGetValue(firstSegment) with
            | true, descriptor -> descriptor
            | _ ->
                raise (InvalidOperationException($"Unknown relation property path '{propertyPath}' for owner type '{tx.OwnerType.FullName}'."))

    let deleteSingleLink (descriptor: RelationDescriptor) =
        tx.Connection.Execute(
            $"DELETE FROM {quoteIdentifier descriptor.LinkTable} WHERE SourceId = @sourceId;",
            {| sourceId = ownerId |}) |> ignore

    let insertSingleLink (descriptor: RelationDescriptor) (targetId: int64) =
        tx.Connection.Execute(
            $"INSERT OR REPLACE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
            {| sourceId = ownerId; targetId = targetId |}) |> ignore

    let manyColumns (descriptor: RelationDescriptor) =
        if descriptor.OwnerUsesSourceColumn then "SourceId", "TargetId"
        else "TargetId", "SourceId"

    for op in plan.Ops do
        match op with
        | SetDBRefToId(propertyPath, targetType, targetId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException($"Relation '{propertyPath}' is not DBRef<T>."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException($"Relation target type mismatch on '{propertyPath}'. Expected {descriptor.TargetType.FullName}, got {targetType.FullName}."))
            ensureTargetExists tx descriptor.TargetTable targetId
            deleteSingleLink descriptor
            insertSingleLink descriptor targetId
            if updateOwnerJson && shouldSyncDbRefJson descriptor then
                updateDbRefJson tx tx.OwnerTable ownerId descriptor.PropertyPath targetId

        | SetDBRefToNone(propertyPath, targetType) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException($"Relation '{propertyPath}' is not DBRef<T>."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException($"Relation target type mismatch on '{propertyPath}'. Expected {descriptor.TargetType.FullName}, got {targetType.FullName}."))
            deleteSingleLink descriptor
            if updateOwnerJson && shouldSyncDbRefJson descriptor then
                updateDbRefJson tx tx.OwnerTable ownerId descriptor.PropertyPath 0L

        | AddDBRefMany(propertyPath, targetType, targetId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Many then
                raise (InvalidOperationException($"Relation '{propertyPath}' is not DBRefMany<T>."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException($"Relation target type mismatch on '{propertyPath}'. Expected {descriptor.TargetType.FullName}, got {targetType.FullName}."))
            ensureTargetExists tx descriptor.TargetTable targetId
            let sourceId, targetIdValue =
                if descriptor.OwnerUsesSourceColumn then ownerId, targetId
                else targetId, ownerId
            tx.Connection.Execute(
                $"INSERT OR IGNORE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                {| sourceId = sourceId; targetId = targetIdValue |}) |> ignore

        | RemoveDBRefMany(propertyPath, targetType, targetId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Many then
                raise (InvalidOperationException($"Relation '{propertyPath}' is not DBRefMany<T>."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException($"Relation target type mismatch on '{propertyPath}'. Expected {descriptor.TargetType.FullName}, got {targetType.FullName}."))
            let ownerColumn, targetColumn = manyColumns descriptor
            let sql = $"DELETE FROM {quoteIdentifier descriptor.LinkTable} WHERE {ownerColumn} = @ownerId AND {targetColumn} = @targetId;"
            tx.Connection.Execute(
                sql,
                {| ownerId = ownerId; targetId = targetId |}) |> ignore

        | ClearDBRefMany(propertyPath, targetType) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Many then
                raise (InvalidOperationException($"Relation '{propertyPath}' is not DBRefMany<T>."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException($"Relation target type mismatch on '{propertyPath}'. Expected {descriptor.TargetType.FullName}, got {targetType.FullName}."))
            let ownerColumn, _ = manyColumns descriptor
            let sql = $"DELETE FROM {quoteIdentifier descriptor.LinkTable} WHERE {ownerColumn} = @ownerId;"
            tx.Connection.Execute(
                sql,
                {| ownerId = ownerId |}) |> ignore
