
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

let internal tryGetVOptionProperty (dbRefObj: obj) (propertyName: string) =
    if isNull dbRefObj then
        ValueNone
    else
        let prop = dbRefObj.GetType().GetProperty(propertyName, BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)
        if isNull prop then
            ValueNone
        else
            prop.GetValue(dbRefObj) |> tryGetValueOptionValue

let internal tryGetPendingEntity (dbRefObj: obj) =
    tryGetVOptionProperty dbRefObj "PendingEntity"

let internal tryGetPendingTypedId (dbRefObj: obj) =
    tryGetVOptionProperty dbRefObj "PendingTypedId"

let internal createDbRefTo (dbRefType: Type) (id: int64) =
    let toMethod = dbRefType.GetMethod("To", BindingFlags.Public ||| BindingFlags.Static, null, [| typeof<int64> |], null)
    if not (isNull toMethod) then
        toMethod.Invoke(null, [| box id |])
    else
        let resolvedMethod = dbRefType.GetMethod("Resolved", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Static, null, [| typeof<int64> |], null)
        if isNull resolvedMethod then
            raise (InvalidOperationException(
                $"Error: Could not resolve DBRef.To/Resolved on type {dbRefType.FullName}.\nReason: The DBRef target type could not be inferred.\nFix: Ensure the DBRef is correctly typed and initialized."))
        resolvedMethod.Invoke(null, [| box id |])

let internal createDbRefLoaded (dbRefType: Type) (id: int64) (entity: obj) =
    let loadedMethod =
        dbRefType.GetMethods(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
        |> Array.find (fun m -> m.Name = "Loaded" && m.GetParameters().Length = 2)
    loadedMethod.Invoke(null, [| box id; entity |])

let internal updateDbRefJson (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) (propertyPath: string) (targetId: int64) =
    let path = "$." + propertyPath
    let jsonText = if targetId = 0L then "null" else string targetId
    tx.Connection.Execute(
        $"UPDATE {quoteIdentifier ownerTable} SET Value = jsonb_set(Value, @path, jsonb(@jsonText)) WHERE Id = @ownerId;",
        {| path = path; jsonText = jsonText; ownerId = ownerId |}) |> ignore

let internal shouldSyncDbRefJson (descriptor: RelationDescriptor) =
    isNull (descriptor.Property.GetCustomAttribute<IgnoreDataMemberAttribute>(true))

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

// ─── Reference-identity comparer for netstandard2.0 circular guard ───────────

type private ReferenceComparer() =
    interface IEqualityComparer<obj> with
        member _.Equals(x, y) = Object.ReferenceEquals(x, y)
        member _.GetHashCode(x) = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(x)

let internal refComparer = ReferenceComparer() :> IEqualityComparer<obj>

// ─── Recursive cascade-insert with full-graph circular guard ─────────────────

let rec internal cascadeInsertDeep
    (tx: RelationTxContext)
    (targetTable: string)
    (targetType: Type)
    (entity: obj)
    (visited: HashSet<obj>)
    =
    // Circular guard: detect if this exact object instance was already visited.
    if not (visited.Add(entity)) then
        raise (InvalidOperationException(
            $"Circular cascade-insert detected for type '{targetType.FullName}'. " +
            "Circular DBRef.From chains are not supported. Use DBRef.To(id) to break the cycle."))

    // 1. Insert the entity itself (shallow).
    let insertedId = insertTargetEntity tx targetTable targetType entity

    // 2. Build a tx context for the target's collection.
    let childTx: RelationTxContext = {
        Connection = tx.Connection
        OwnerTable = targetTable
        OwnerType = targetType
        InTransaction = tx.InTransaction
    }

    // 3. Process the target entity's own relation properties.
    let descriptors = buildRelationDescriptors childTx targetType

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let value = descriptor.Property.GetValue(entity)
            let refId = readDbRefId value
            if refId > 0L then
                // Already-resolved reference — create link row, validate target exists.
                ensureRelationSchema childTx descriptor
                ensureTargetExists childTx descriptor.TargetTable refId
                childTx.Connection.Execute(
                    $"INSERT OR REPLACE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                    {| sourceId = insertedId; targetId = refId |}) |> ignore
                if shouldSyncDbRefJson descriptor then
                    updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath refId
            else
                match tryGetPendingEntity value with
                | ValueSome pending ->
                    let entityId = readEntityIdOrZero descriptor.TargetType pending
                    if entityId > 0L then
                        // Existing entity, link-only.
                        let dbRef = createDbRefTo descriptor.Property.PropertyType entityId
                        descriptor.Property.SetValue(entity, dbRef)
                        ensureRelationSchema childTx descriptor
                        ensureTargetExists childTx descriptor.TargetTable entityId
                        childTx.Connection.Execute(
                            $"INSERT OR REPLACE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                            {| sourceId = insertedId; targetId = entityId |}) |> ignore
                        if shouldSyncDbRefJson descriptor then
                            updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath entityId
                    else
                        // RECURSIVE cascade-insert.
                        ensureRelationSchema childTx descriptor
                        let childId = cascadeInsertDeep childTx descriptor.TargetTable descriptor.TargetType pending visited
                        let dbRef = createDbRefTo descriptor.Property.PropertyType childId
                        descriptor.Property.SetValue(entity, dbRef)
                        childTx.Connection.Execute(
                            $"INSERT OR REPLACE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                            {| sourceId = insertedId; targetId = childId |}) |> ignore
                        if shouldSyncDbRefJson descriptor then
                            updateDbRefJson childTx targetTable insertedId descriptor.PropertyPath childId
                | ValueNone -> ()

        | Many ->
            let trackerObj = descriptor.Property.GetValue(entity)
            match trackerObj with
            | :? IDBRefManyInternal as tracker ->
                // Always process Many items during cascade-insert, loaded or not.
                ensureRelationSchema childTx descriptor
                for item in tracker.GetCurrentItemsBoxed() do
                    if not (isNull item) then
                        let mutable itemId = readEntityIdOrZero descriptor.TargetType item
                        if itemId <= 0L then
                            // RECURSIVE cascade-insert for Many items.
                            itemId <- cascadeInsertDeep childTx descriptor.TargetTable descriptor.TargetType item visited
                        if itemId > 0L then
                            let sourceId, targetId =
                                if descriptor.OwnerUsesSourceColumn then insertedId, itemId
                                else itemId, insertedId
                            childTx.Connection.Execute(
                                $"INSERT OR IGNORE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
                                {| sourceId = sourceId; targetId = targetId |}) |> ignore
            | _ -> ()

    insertedId

let internal resolveSingleTargetIdAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) (visited: HashSet<obj>) =
    let value = descriptor.Property.GetValue(owner)
    let id = readDbRefId value
    if id > 0L then
        // Preflight target-exists for DBRef.To(id) before owner mutation.
        ensureTargetExists tx descriptor.TargetTable id
        id
    else
        match tryGetPendingTypedId value with
        | ValueSome typedId ->
            ensureRelationSchema tx descriptor
            let resolvedId = resolveTypedIdToTargetId tx descriptor typedId
            let dbRef = createDbRefTo descriptor.Property.PropertyType resolvedId
            descriptor.Property.SetValue(owner, dbRef)
            resolvedId
        | ValueNone ->
            match tryGetPendingEntity value with
            | ValueSome pending ->
                let entityId = readEntityIdOrZero descriptor.TargetType pending
                if entityId > 0L then
                    // Entity already persisted (Id > 0) — link-only, no re-insert.
                    // Preflight target-exists for From(existing) before owner mutation.
                    ensureTargetExists tx descriptor.TargetTable entityId
                    let dbRef = createDbRefTo descriptor.Property.PropertyType entityId
                    descriptor.Property.SetValue(owner, dbRef)
                    entityId
                else
                    let insertedId = cascadeInsertDeep tx descriptor.TargetTable descriptor.TargetType pending visited
                    let dbRef = createDbRefTo descriptor.Property.PropertyType insertedId
                    descriptor.Property.SetValue(owner, dbRef)
                    insertedId
            | ValueNone ->
                0L

let internal collectManyTargetIdsAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) (includeWhenUnloaded: bool) (visited: HashSet<obj>) =
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
                            id <- cascadeInsertDeep tx descriptor.TargetTable descriptor.TargetType item visited
                        elif id > 0L then
                            // Preflight target-exists for pre-existing many items before owner mutation.
                            ensureTargetExists tx descriptor.TargetTable id
                        if id > 0L then
                            ids.Add(id) |> ignore
                ValueSome (ids |> Seq.toArray)
        | _ ->
            raise (InvalidOperationException(
                $"Error: Property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' does not implement IDBRefManyInternal.\nReason: The relation property type is incorrect.\nFix: Use DBRefMany<T> or DBRefMany<TTarget,'TId> for multi-relations."))

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
                raise (InvalidOperationException(
                    $"Error: Unknown relation property path '{propertyPath}' for owner type '{tx.OwnerType.FullName}'.\nReason: The path does not match any relation on the type.\nFix: Use a valid relation property path."))

    let deleteSingleLink (descriptor: RelationDescriptor) =
        tx.Connection.Execute(
            $"DELETE FROM {quoteIdentifier descriptor.LinkTable} WHERE SourceId = @sourceId;",
            {| sourceId = ownerId |}) |> ignore

    let insertSingleLink (descriptor: RelationDescriptor) (targetId: int64) =
        tx.Connection.Execute(
            $"INSERT OR REPLACE INTO {quoteIdentifier descriptor.LinkTable}(SourceId, TargetId) VALUES(@sourceId, @targetId);",
            {| sourceId = ownerId; targetId = targetId |}) |> ignore

    let applySingleSetAndSync (descriptor: RelationDescriptor) (targetId: int64) =
        deleteSingleLink descriptor
        insertSingleLink descriptor targetId
        if updateOwnerJson && shouldSyncDbRefJson descriptor then
            updateDbRefJson tx tx.OwnerTable ownerId descriptor.PropertyPath targetId

    let manyColumns (descriptor: RelationDescriptor) =
        if descriptor.OwnerUsesSourceColumn then "SourceId", "TargetId"
        else "TargetId", "SourceId"

    for op in plan.Ops do
        match op with
        | SetDBRefToId(propertyPath, targetType, targetId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRef<T>.\nReason: The relation type does not match the expected DBRef.\nFix: Use a DBRef<T> relation or correct the property path."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            ensureTargetExists tx descriptor.TargetTable targetId
            applySingleSetAndSync descriptor targetId

        | SetDBRefToTypedId(propertyPath, targetType, targetIdType, targetTypedId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRef<T>.\nReason: The relation type does not match the expected DBRef.\nFix: Use a DBRef<T> relation or correct the property path."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            match descriptor.TypedIdType with
            | ValueSome configured when configured = targetIdType -> ()
            | ValueSome configured ->
                raise (InvalidOperationException(
                    $"Error: Typed id type mismatch on '{propertyPath}'.\nReason: Expected {configured.FullName}, got {targetIdType.FullName}.\nFix: Use the correct typed id type for this relation."))
            | ValueNone ->
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' does not support typed-id UpdateMany Set.\nReason: This relation is DBRef<T> row-id based.\nFix: Use DBRef.To(int64) or change relation type to DBRef<TTarget,'TId>."))
            let targetId = resolveTypedIdToTargetId tx descriptor targetTypedId
            applySingleSetAndSync descriptor targetId

        | SetDBRefToNone(propertyPath, targetType) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRef<T>.\nReason: The relation type does not match the expected DBRef.\nFix: Use a DBRef<T> relation or correct the property path."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            deleteSingleLink descriptor
            if updateOwnerJson && shouldSyncDbRefJson descriptor then
                updateDbRefJson tx tx.OwnerTable ownerId descriptor.PropertyPath 0L

        | AddDBRefMany(propertyPath, targetType, targetId) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Many then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRefMany<T>.\nReason: The relation type does not match the expected DBRefMany.\nFix: Use a DBRefMany<T> relation or correct the property path."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
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
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRefMany<T>.\nReason: The relation type does not match the expected DBRefMany.\nFix: Use a DBRefMany<T> relation or correct the property path."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            let ownerColumn, targetColumn = manyColumns descriptor
            let sql = $"DELETE FROM {quoteIdentifier descriptor.LinkTable} WHERE {ownerColumn} = @ownerId AND {targetColumn} = @targetId;"
            tx.Connection.Execute(
                sql,
                {| ownerId = ownerId; targetId = targetId |}) |> ignore

        | ClearDBRefMany(propertyPath, targetType) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Many then
                raise (InvalidOperationException(
                    $"Error: Relation '{propertyPath}' is not DBRefMany<T>.\nReason: The relation type does not match the expected DBRefMany.\nFix: Use a DBRefMany<T> relation or correct the property path."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException(
                    $"Error: Relation target type mismatch on '{propertyPath}'.\nReason: Expected {descriptor.TargetType.FullName}, got {targetType.FullName}.\nFix: Use the correct target type for this relation."))
            let ownerColumn, _ = manyColumns descriptor
            let sql = $"DELETE FROM {quoteIdentifier descriptor.LinkTable} WHERE {ownerColumn} = @ownerId;"
            tx.Connection.Execute(
                sql,
                {| ownerId = ownerId |}) |> ignore
