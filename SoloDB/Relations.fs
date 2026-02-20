
module internal SoloDatabase.Relations

open System
open System.Reflection
open System.Collections.Generic
open System.Collections.Concurrent
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SoloDatabase.JsonSerializator
open SQLiteTools
open JsonFunctions

type internal RelationTxContext = {
    Connection: SqliteConnection
    OwnerTable: string
    OwnerType: Type
    InTransaction: bool
}

type internal RelationUpdateManyOp =
    | SetDBRefToId of PropertyPath: string * TargetType: Type * TargetId: int64
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

type private RelationKind =
    | Single
    | Many

type private RelationDescriptor = {
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
}

[<CLIMutable>]
type private RelationMetadataRow = {
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

let private ensureTransaction (tx: RelationTxContext) =
    if not tx.InTransaction then
        raise (InvalidOperationException("Relations API requires active transaction."))

let private ensureTxContext (tx: RelationTxContext) =
    if isNull tx.Connection then nullArg "tx.Connection"
    if String.IsNullOrWhiteSpace tx.OwnerTable then
        raise (ArgumentException("OwnerTable is required.", "tx.OwnerTable"))
    if isNull tx.OwnerType then nullArg "tx.OwnerType"

let private ensureOwnerInstance (ownerType: Type) (owner: obj) (argName: string) =
    if isNull owner then nullArg argName
    if isNull ownerType then nullArg "ownerType"
    let actual = owner.GetType()
    if not (ownerType.IsAssignableFrom actual) then
        raise (ArgumentException($"Invalid owner instance type. Expected assignable to {ownerType.FullName}, got {actual.FullName}.", argName))

let private emptyPlan kind ownerType =
    { Kind = kind
      OwnerType = ownerType
      Ops = [] }

let private relationSpecsCache = ConcurrentDictionary<Type, (PropertyInfo * RelationKind * Type * DeletePolicy * DeletePolicy * bool) array>()
let private deleteGuard = new System.Threading.ThreadLocal<HashSet<string>>(fun () -> HashSet<string>(StringComparer.Ordinal))

let private quoteIdentifier (name: string) =
    "\"" + name.Replace("\"", "\"\"") + "\""

let private relationName (ownerTable: string) (propertyName: string) =
    $"{ownerTable}_{propertyName}"

let private canonicalManyRelationName (leftTable: string) (rightTable: string) =
    if StringComparer.Ordinal.Compare(leftTable, rightTable) <= 0 then
        $"{leftTable}_{rightTable}"
    else
        $"{rightTable}_{leftTable}"

let private linkTableFromRelationName (name: string) =
    "SoloDBRelLink_" + name

let private stringToRelationKind (value: string) =
    match value with
    | "Single" -> Single
    | "Many" -> Many
    | _ -> raise (InvalidOperationException($"Invalid relation kind '{value}'."))

let private relationKindToString kind =
    match kind with
    | Single -> "Single"
    | Many -> "Many"

let private parseDeletePolicy (value: string) =
    let value =
        if String.IsNullOrWhiteSpace value then
            "Restrict"
        else
            value.Trim()
    match Enum.TryParse<DeletePolicy>(value, true) with
    | true, policy -> policy
    | _ -> raise (InvalidOperationException($"Invalid delete policy '{value}'."))

let private parseOnDeletePolicy (value: string) =
    let policy =
        if String.IsNullOrWhiteSpace value then
            DeletePolicy.Restrict
        else
            parseDeletePolicy value
    match policy with
    | DeletePolicy.Deletion ->
        raise (InvalidOperationException("OnDelete cannot be Deletion."))
    | _ -> policy

let private parseOnOwnerDeletePolicy (value: string) =
    let policy =
        if String.IsNullOrWhiteSpace value then
            DeletePolicy.Deletion
        else
            parseDeletePolicy value
    match policy with
    | DeletePolicy.Cascade ->
        raise (InvalidOperationException("OnOwnerDelete cannot be Cascade. Use Deletion instead."))
    | _ -> policy

let private tryGetWritableInt64IdProperty (t: Type) =
    let prop = t.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
    if isNull prop || not prop.CanRead || not prop.CanWrite || prop.PropertyType <> typeof<int64> then
        ValueNone
    else
        ValueSome prop

let private getWritableInt64IdPropertyOrThrow (t: Type) =
    match tryGetWritableInt64IdProperty t with
    | ValueSome prop -> prop
    | ValueNone ->
        raise (InvalidOperationException($"Type '{t.FullName}' used in DBRef/DBRefMany relations must expose writable int64 Id. Custom-id DBRef<,> is deferred for Batch 6."))

let private readEntityIdOrZero (targetType: Type) (entity: obj) =
    if isNull entity then 0L
    elif not (targetType.IsAssignableFrom(entity.GetType())) then
        raise (InvalidOperationException($"Invalid relation target instance type. Expected assignable to {targetType.FullName}, got {entity.GetType().FullName}."))
    else
        let idProp = getWritableInt64IdPropertyOrThrow targetType
        idProp.GetValue(entity) :?> int64

let private writeEntityId (targetType: Type) (entity: obj) (id: int64) =
    let idProp = getWritableInt64IdPropertyOrThrow targetType
    idProp.SetValue(entity, box id)

let private jsonSerializeMethod =
    typeof<JsonValue>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    |> Array.find (fun m -> m.Name = "Serialize" && m.IsGenericMethodDefinition && m.GetParameters().Length = 1)

let private jsonSerializeWithTypeMethod =
    typeof<JsonValue>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    |> Array.find (fun m -> m.Name = "SerializeWithType" && m.IsGenericMethodDefinition && m.GetParameters().Length = 1)

let private serializeEntityForStorage (targetType: Type) (entity: obj) =
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

let private ensureRelationCatalogTable (connection: SqliteConnection) =
    ignore (connection.Execute("""
CREATE TABLE IF NOT EXISTS SoloDBRelation (
    Id INTEGER PRIMARY KEY,
    Name TEXT NOT NULL UNIQUE,
    SourceCollection TEXT NOT NULL,
    TargetCollection TEXT NOT NULL,
    PropertyName TEXT NOT NULL,
    OwnerCollection TEXT NOT NULL,
    RefKind TEXT NOT NULL CHECK(RefKind IN ('Single','Many')),
    OnDelete TEXT NOT NULL DEFAULT 'Restrict' CHECK(OnDelete IN ('Restrict','Cascade','Unlink')),
    OnOwnerDelete TEXT NOT NULL DEFAULT 'Deletion' CHECK(OnOwnerDelete IN ('Restrict','Unlink','Deletion')),
    IsUnique INTEGER NOT NULL DEFAULT 0,
    UNIQUE(OwnerCollection, PropertyName)
) STRICT;
"""))

let private getSQLForTriggersForTable (name: string) =
    let updateTriggerName = $"SoloDB_Update_{name}"
    let insertTriggerName = $"SoloDB_Insert_{name}"
    let deleteTriggerName = $"SoloDB_Delete_{name}"
    let updatedTriggerName = $"SoloDB_Updated_{name}"
    let insertedTriggerName = $"SoloDB_Inserted_{name}"
    let deletedTriggerName = $"SoloDB_Deleted_{name}"
    $"""
CREATE TRIGGER IF NOT EXISTS "{insertTriggerName}"
BEFORE INSERT ON "{name}"
FOR EACH ROW
WHEN SHOULD_HANDLE_INSERTING('{name}') = 1
BEGIN
    SELECT CASE
        WHEN message IS NULL THEN NULL
        ELSE RAISE(ABORT, message)
    END
    FROM (
        SELECT ON_INSERTING_HANDLER('{name}', json(NEW.Value)) AS message
    );
END;

CREATE TRIGGER IF NOT EXISTS "{updateTriggerName}"
BEFORE UPDATE ON "{name}"
FOR EACH ROW
WHEN SHOULD_HANDLE_UPDATING('{name}') = 1
BEGIN
    SELECT CASE
        WHEN message IS NULL THEN NULL
        ELSE RAISE(ABORT, message)
    END
    FROM (
        SELECT ON_UPDATING_HANDLER('{name}', json(OLD.Value), json(NEW.Value)) AS message
    );
END;

CREATE TRIGGER IF NOT EXISTS "{deleteTriggerName}"
BEFORE DELETE ON "{name}"
FOR EACH ROW
WHEN SHOULD_HANDLE_DELETING('{name}') = 1
BEGIN
    SELECT CASE
        WHEN message IS NULL THEN NULL
        ELSE RAISE(ABORT, message)
    END
    FROM (
        SELECT ON_DELETING_HANDLER('{name}', json(OLD.Value)) AS message
    );
END;

CREATE TRIGGER IF NOT EXISTS "{insertedTriggerName}"
AFTER INSERT ON "{name}"
FOR EACH ROW
WHEN SHOULD_HANDLE_INSERTED('{name}') = 1
BEGIN
    SELECT CASE
        WHEN message IS NULL THEN NULL
        ELSE RAISE(ABORT, message)
    END
    FROM (
        SELECT ON_INSERTED_HANDLER('{name}', json(NEW.Value)) AS message
    );
END;

CREATE TRIGGER IF NOT EXISTS "{updatedTriggerName}"
AFTER UPDATE ON "{name}"
FOR EACH ROW
WHEN SHOULD_HANDLE_UPDATED('{name}') = 1
BEGIN
    SELECT CASE
        WHEN message IS NULL THEN NULL
        ELSE RAISE(ABORT, message)
    END
    FROM (
        SELECT ON_UPDATED_HANDLER('{name}', json(OLD.Value), json(NEW.Value)) AS message
    );
END;

CREATE TRIGGER IF NOT EXISTS "{deletedTriggerName}"
AFTER DELETE ON "{name}"
FOR EACH ROW
WHEN SHOULD_HANDLE_DELETED('{name}') = 1
BEGIN
    SELECT CASE
        WHEN message IS NULL THEN NULL
        ELSE RAISE(ABORT, message)
    END
    FROM (
        SELECT ON_DELETED_HANDLER('{name}', json(OLD.Value)) AS message
    );
END;
"""

let private ensureCollectionTableExists (connection: SqliteConnection) (tableName: string) =
    let qTable = quoteIdentifier tableName
    let exists =
        connection.QueryFirst<int64>("SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END", {| name = tableName |}) = 1L

    if not exists then
        connection.Execute($"CREATE TABLE {qTable} (Id INTEGER NOT NULL PRIMARY KEY UNIQUE, Value JSONB NOT NULL);") |> ignore
        connection.Execute(getSQLForTriggersForTable tableName) |> ignore

    connection.Execute(
        "INSERT INTO SoloDBCollections(Name) SELECT @name WHERE NOT EXISTS (SELECT 1 FROM SoloDBCollections WHERE Name = @name);",
        {| name = tableName |}) |> ignore
let private buildRelationSpecs (ownerType: Type) =
    ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
    |> Array.choose (fun prop ->
        if not prop.CanRead then
            None
        else
            let propType = prop.PropertyType
            if not propType.IsGenericType then
                None
            else
                let generic = propType.GetGenericTypeDefinition()
                if generic = typedefof<DBRef<_>> || generic = typedefof<DBRefMany<_>> then
                    let targetType = propType.GetGenericArguments().[0]
                    let attr = prop.GetCustomAttribute<SoloRefAttribute>(true)
                    let onDelete = if isNull attr then DeletePolicy.Restrict else attr.OnDelete
                    let onOwnerDelete = if isNull attr then DeletePolicy.Deletion else attr.OnOwnerDelete
                    let isUnique = not (isNull attr) && attr.Unique
                    let kind = if generic = typedefof<DBRef<_>> then Single else Many

                    match kind with
                    | Many when onOwnerDelete = DeletePolicy.Cascade ->
                        raise (InvalidOperationException($"Invalid relation policy on {ownerType.FullName}.{prop.Name}: OnOwnerDelete cannot be Cascade. Use Deletion instead."))
                    | _ -> ()

                    Some (prop, kind, targetType, onDelete, onOwnerDelete, isUnique)
                else
                    None)

let private getRelationSpecs (ownerType: Type) =
    relationSpecsCache.GetOrAdd(ownerType, Func<_, _>(buildRelationSpecs))

let private hasManyBackReference (ownerType: Type) (targetType: Type) =
    getRelationSpecs targetType
    |> Array.exists (fun (_, kind, candidateTargetType, _, _, _) ->
        kind = Many && candidateTargetType = ownerType)

let private typeCollectionMapTableExists (connection: SqliteConnection) =
    connection.QueryFirst<int64>(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'SoloDBTypeCollectionMap') THEN 1 ELSE 0 END") = 1L

let private tryGetStoredTargetCollection (connection: SqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    connection.QueryFirstOrDefault<string>(
        """
SELECT TargetCollection
FROM SoloDBRelation
WHERE OwnerCollection = @ownerCollection AND PropertyName = @propertyName
LIMIT 1;
""",
        {| ownerCollection = ownerTable; propertyName = propertyName |})
    |> Option.ofObj

let private tryGetStoredRelationName (connection: SqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    connection.QueryFirstOrDefault<string>(
        """
SELECT Name
FROM SoloDBRelation
WHERE OwnerCollection = @ownerCollection AND PropertyName = @propertyName
LIMIT 1;
""",
        {| ownerCollection = ownerTable; propertyName = propertyName |})
    |> Option.ofObj

let private readMappedCollectionsForType (connection: SqliteConnection) (targetType: Type) =
    if not (typeCollectionMapTableExists connection) then
        [||]
    else
        connection.Query<string>(
            "SELECT CollectionName FROM SoloDBTypeCollectionMap WHERE TypeKey = @typeKey;",
            {| typeKey = Utils.typeIdentityKey targetType |})
        |> Seq.map formatName
        |> Seq.distinct
        |> Seq.toArray

let private collectionExistsByName (connection: SqliteConnection) (collectionName: string) =
    connection.QueryFirst<int64>(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM SoloDBCollections WHERE Name = @name) THEN 1 ELSE 0 END",
        {| name = collectionName |}) = 1L

let private sqliteTableExistsByName (connection: SqliteConnection) (tableName: string) =
    connection.QueryFirst<int64>(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
        {| name = tableName |}) = 1L

let private resolveTargetCollectionName (connection: SqliteConnection) (ownerTable: string) (propertyName: string) (targetType: Type) =
    let ownerTable = formatName ownerTable
    let defaultTable = formatName targetType.Name

    match tryGetStoredTargetCollection connection ownerTable propertyName with
    | Some stored when not (String.IsNullOrWhiteSpace stored) ->
        formatName stored
    | _ ->
        let mapped = readMappedCollectionsForType connection targetType
        if mapped |> Array.contains defaultTable then
            defaultTable
        elif collectionExistsByName connection defaultTable then
            defaultTable
        elif mapped.Length = 1 then
            mapped.[0]
        elif mapped.Length > 1 then
            let mappedList = String.Join(", ", mapped)
            raise (InvalidOperationException($"Ambiguous target collection mapping for relation {ownerTable}.{propertyName} and type '{targetType.FullName}'. Mapped collections: {mappedList}."))
        else
            defaultTable

let private buildRelationDescriptors (tx: RelationTxContext) (ownerType: Type) =
    let ownerTable = formatName tx.OwnerTable
    getRelationSpecs ownerType
    |> Array.map (fun (prop, kind, targetType, onDelete, onOwnerDelete, isUnique) ->
        let targetTable = resolveTargetCollectionName tx.Connection ownerTable prop.Name targetType
        let defaultRelName = relationName ownerTable prop.Name
        let canonicalManyName = canonicalManyRelationName ownerTable targetTable
        let isMutualMany = kind = Many && hasManyBackReference ownerType targetType
        let proposedRelName = defaultRelName
        let relName =
            match tryGetStoredRelationName tx.Connection ownerTable prop.Name with
            | Some stored when not (String.IsNullOrWhiteSpace stored) -> formatName stored
            | _ -> proposedRelName

        let canonicalSharedLinkTable = linkTableFromRelationName canonicalManyName
        let isSharedMany =
            kind = Many
            && (isMutualMany || sqliteTableExistsByName tx.Connection canonicalSharedLinkTable)
        let ownerUsesSourceColumn =
            if isSharedMany then
                StringComparer.Ordinal.Compare(ownerTable, targetTable) <= 0
            else
                true
        let linkSourceTable, linkTargetTable =
            if ownerUsesSourceColumn then ownerTable, targetTable
            else targetTable, ownerTable

        {
            OwnerTable = ownerTable
            OwnerType = ownerType
            Property = prop
            PropertyPath = prop.Name
            Kind = kind
            TargetType = targetType
            TargetTable = targetTable
            LinkSourceTable = linkSourceTable
            LinkTargetTable = linkTargetTable
            OwnerUsesSourceColumn = ownerUsesSourceColumn
            RelationName = relName
            LinkTable = if isSharedMany then canonicalSharedLinkTable else linkTableFromRelationName relName
            OnDelete = onDelete
            OnOwnerDelete = onOwnerDelete
            IsUnique = isUnique
        })

let private ensureRelationSchema (tx: RelationTxContext) (descriptor: RelationDescriptor) =
    ensureCollectionTableExists tx.Connection descriptor.TargetTable

    let qLink = quoteIdentifier descriptor.LinkTable
    let qSource = quoteIdentifier descriptor.LinkSourceTable
    let qTarget = quoteIdentifier descriptor.LinkTargetTable
    let constraints =
        match descriptor.Kind with
        | Single when descriptor.IsUnique ->
            "UNIQUE(SourceId), UNIQUE(TargetId)"
        | Single ->
            "UNIQUE(SourceId)"
        | Many ->
            "UNIQUE(SourceId, TargetId)"

    ignore (tx.Connection.Execute($"""
CREATE TABLE IF NOT EXISTS {qLink} (
    Id INTEGER PRIMARY KEY,
    SourceId INTEGER NOT NULL,
    TargetId INTEGER NOT NULL,
    FOREIGN KEY (SourceId) REFERENCES {qSource}(Id) ON DELETE RESTRICT,
    FOREIGN KEY (TargetId) REFERENCES {qTarget}(Id) ON DELETE RESTRICT,
    {constraints}
) STRICT;
"""))

    let sourceIdx = formatName($"IX_{descriptor.LinkTable}_Source")
    let targetIdx = formatName($"IX_{descriptor.LinkTable}_Target")
    tx.Connection.Execute($"CREATE INDEX IF NOT EXISTS {quoteIdentifier sourceIdx} ON {qLink}(SourceId);") |> ignore
    tx.Connection.Execute($"CREATE INDEX IF NOT EXISTS {quoteIdentifier targetIdx} ON {qLink}(TargetId);") |> ignore

    tx.Connection.Execute(
        """
INSERT INTO SoloDBRelation(Name, SourceCollection, TargetCollection, PropertyName, OwnerCollection, RefKind, OnDelete, OnOwnerDelete, IsUnique)
VALUES (@name, @sourceCollection, @targetCollection, @propertyName, @ownerCollection, @refKind, @onDelete, @onOwnerDelete, @isUnique)
ON CONFLICT(OwnerCollection, PropertyName) DO UPDATE SET
    Name = excluded.Name,
    SourceCollection = excluded.SourceCollection,
    TargetCollection = excluded.TargetCollection,
    RefKind = excluded.RefKind,
    OnDelete = excluded.OnDelete,
    OnOwnerDelete = excluded.OnOwnerDelete,
    IsUnique = excluded.IsUnique;
""",
        {|
            name = descriptor.RelationName
            sourceCollection = descriptor.OwnerTable
            targetCollection = descriptor.TargetTable
            propertyName = descriptor.Property.Name
            ownerCollection = descriptor.OwnerTable
            refKind = relationKindToString descriptor.Kind
            onDelete = descriptor.OnDelete.ToString()
            onOwnerDelete = descriptor.OnOwnerDelete.ToString()
            isUnique = if descriptor.IsUnique then 1 else 0
        |}) |> ignore

let private readMetadataByOwner (connection: SqliteConnection) (ownerTable: string) =
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

let private readMetadataByTarget (connection: SqliteConnection) (targetTable: string) =
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

let private relationDescriptorByPath (tx: RelationTxContext) (ownerType: Type) =
    let map = Dictionary<string, RelationDescriptor>(StringComparer.Ordinal)
    for descriptor in buildRelationDescriptors tx ownerType do
        map.[descriptor.PropertyPath] <- descriptor
    map

let private insertTargetEntity (tx: RelationTxContext) (targetTable: string) (targetType: Type) (entity: obj) =
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

let private ensureTargetExists (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureCollectionTableExists tx.Connection targetTable
    let exists =
        tx.Connection.QueryFirst<int64>($"SELECT CASE WHEN EXISTS (SELECT 1 FROM {quoteIdentifier targetTable} WHERE Id = @id) THEN 1 ELSE 0 END", {| id = targetId |}) = 1L
    if not exists then
        raise (InvalidOperationException($"Relation target '{targetTable}' with Id={targetId} does not exist."))

let private readDbRefId (dbRefObj: obj) =
    if isNull dbRefObj then
        0L
    else
        let idProp = dbRefObj.GetType().GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull idProp then 0L else idProp.GetValue(dbRefObj) :?> int64

let private tryGetValueOptionValue (valueOpt: obj) =
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

let private tryGetPendingEntity (dbRefObj: obj) =
    if isNull dbRefObj then
        ValueNone
    else
        let pendingProp = dbRefObj.GetType().GetProperty("PendingEntity", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)
        if isNull pendingProp then
            ValueNone
        else
            pendingProp.GetValue(dbRefObj) |> tryGetValueOptionValue

let private createDbRefTo (dbRefType: Type) (id: int64) =
    let toMethod = dbRefType.GetMethod("To", BindingFlags.Public ||| BindingFlags.Static, null, [| typeof<int64> |], null)
    if isNull toMethod then
        raise (InvalidOperationException($"Could not resolve DBRef.To on type {dbRefType.FullName}."))
    toMethod.Invoke(null, [| box id |])

let private createDbRefLoaded (dbRefType: Type) (id: int64) (entity: obj) =
    let loadedMethod =
        dbRefType.GetMethods(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
        |> Array.find (fun m -> m.Name = "Loaded" && m.GetParameters().Length = 2)
    loadedMethod.Invoke(null, [| box id; entity |])

let private resolveSingleTargetIdAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) =
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

let private collectManyTargetIdsAndCascade (tx: RelationTxContext) (descriptor: RelationDescriptor) (owner: obj) (includeWhenUnloaded: bool) =
    let trackerObj = descriptor.Property.GetValue(owner)
    if isNull trackerObj then
        if includeWhenUnloaded then ValueSome [||] else ValueNone
    else
        match trackerObj with
        | :? IDBRefManyInternal as tracker ->
            if not includeWhenUnloaded && not tracker.IsLoaded then
                ValueNone
            else
                let ids = HashSet<int64>()
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

let private updateDbRefJson (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) (propertyPath: string) (targetId: int64) =
    let path = "$." + propertyPath
    let jsonText = if targetId = 0L then "null" else string targetId
    tx.Connection.Execute(
        $"UPDATE {quoteIdentifier ownerTable} SET Value = jsonb_set(Value, @path, jsonb(@jsonText)) WHERE Id = @ownerId;",
        {| path = path; jsonText = jsonText; ownerId = ownerId |}) |> ignore

let resetDbRefManyTrackers (owner: obj) (committedLinkedIdsByProperty: IReadOnlyDictionary<string, int64 array>) =
    if isNull owner then nullArg "owner"
    if isNull committedLinkedIdsByProperty then nullArg "committedLinkedIdsByProperty"

    let props = owner.GetType().GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
    for prop in props do
        let value = prop.GetValue owner
        match value with
        | :? IDBRefManyInternal as tracker ->
            match committedLinkedIdsByProperty.TryGetValue prop.Name with
            | true, ids when not (isNull ids) -> tracker.ResetTracker ids
            | _ -> tracker.ResetTracker Seq.empty
        | _ -> ()
let private applyOps (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) (updateOwnerJson: bool) =
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
            if updateOwnerJson then
                updateDbRefJson tx tx.OwnerTable ownerId descriptor.PropertyPath targetId

        | SetDBRefToNone(propertyPath, targetType) ->
            let descriptor = resolveDescriptor propertyPath
            if descriptor.Kind <> Single then
                raise (InvalidOperationException($"Relation '{propertyPath}' is not DBRef<T>."))
            if descriptor.TargetType <> targetType then
                raise (InvalidOperationException($"Relation target type mismatch on '{propertyPath}'. Expected {descriptor.TargetType.FullName}, got {targetType.FullName}."))
            deleteSingleLink descriptor
            if updateOwnerJson then
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

let private withDeleteGuard (tableName: string) (id: int64) (fn: unit -> unit) =
    let key = $"{tableName}|{id}"
    let set = deleteGuard.Value
    if set.Contains(key) then
        ()
    else
        set.Add(key) |> ignore
        try fn()
        finally set.Remove(key) |> ignore

let private metadataLinkLayout (connection: SqliteConnection) (row: RelationMetadataRow) (relationKind: RelationKind) =
    match relationKind with
    | Single ->
        linkTableFromRelationName row.Name, "SourceId", "TargetId"
    | Many ->
        let canonicalTable = linkTableFromRelationName (canonicalManyRelationName row.OwnerCollection row.TargetCollection)
        let useShared = sqliteTableExistsByName connection canonicalTable
        let ownerUsesSourceColumn =
            if useShared then
                StringComparer.Ordinal.Compare(row.OwnerCollection, row.TargetCollection) <= 0
            else
                true
        let ownerColumn, targetColumn =
            if ownerUsesSourceColumn then "SourceId", "TargetId"
            else "TargetId", "SourceId"
        let linkTable = if useShared then canonicalTable else linkTableFromRelationName row.Name
        linkTable, ownerColumn, targetColumn

let rec private applyOwnerDeletePoliciesCore (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) (deleteTargets: bool) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then
        raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    let ownerTable = formatName ownerTable
    let rows = readMetadataByOwner tx.Connection ownerTable

    withDeleteGuard ownerTable ownerId (fun () ->
        for row in rows do
            let relationKind = stringToRelationKind row.RefKind
            let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row relationKind
            let qLink = quoteIdentifier linkTable
            let targetIds =
                tx.Connection.Query<int64>($"SELECT {targetColumn} FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |})
                |> Seq.toArray
            let hasLinks = targetIds.Length > 0
            match relationKind with
            | Single ->
                if hasLinks then
                    tx.Connection.Execute($"DELETE FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |}) |> ignore

            | Many ->
                match parseOnOwnerDeletePolicy row.OnOwnerDelete with
                | DeletePolicy.Restrict ->
                    if hasLinks then
                        raise (InvalidOperationException($"Cannot delete owner '{ownerTable}' Id={ownerId} because relation '{row.PropertyName}' uses OnOwnerDelete=Restrict and still has links."))
                | DeletePolicy.Unlink ->
                    if hasLinks then
                        tx.Connection.Execute($"DELETE FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |}) |> ignore
                | DeletePolicy.Deletion ->
                    if hasLinks then
                        tx.Connection.Execute($"DELETE FROM {qLink} WHERE {ownerColumn} = @ownerId;", {| ownerId = ownerId |}) |> ignore
                        if deleteTargets then
                            let distinctTargetIds = targetIds |> Seq.distinct |> Seq.toArray
                            for targetId in distinctTargetIds do
                                if globalRefCountCore tx row.TargetCollection targetId = 0L then
                                    applyTargetDeletePoliciesCore tx row.TargetCollection targetId
                                    applyOwnerDeletePoliciesCore tx row.TargetCollection targetId true
                                    tx.Connection.Execute($"DELETE FROM {quoteIdentifier row.TargetCollection} WHERE Id = @id;", {| id = targetId |}) |> ignore
                | DeletePolicy.Cascade ->
                    raise (InvalidOperationException($"Invalid relation metadata: OnOwnerDelete=Cascade is not supported on '{ownerTable}.{row.PropertyName}'."))
                | _ ->
                    raise (InvalidOperationException($"Invalid OnOwnerDelete policy on relation '{ownerTable}.{row.PropertyName}'."))
)

and private applyTargetDeletePoliciesCore (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace targetTable then
        raise (ArgumentException("targetTable is required.", "targetTable"))
    if targetId <= 0L then
        raise (ArgumentOutOfRangeException("targetId", targetId, "targetId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    let targetTable = formatName targetTable
    let rows = readMetadataByTarget tx.Connection targetTable

    withDeleteGuard targetTable targetId (fun () ->
        for row in rows do
            let onDelete = parseOnDeletePolicy row.OnDelete
            let relationKind = stringToRelationKind row.RefKind
            let linkTable, ownerColumn, targetColumn = metadataLinkLayout tx.Connection row relationKind
            let qLink = quoteIdentifier linkTable
            let ownerIds =
                tx.Connection.Query<int64>($"SELECT {ownerColumn} FROM {qLink} WHERE {targetColumn} = @targetId;", {| targetId = targetId |})
                |> Seq.distinct
                |> Seq.toArray

            if ownerIds.Length > 0 then
                match onDelete with
                | DeletePolicy.Restrict ->
                    raise (InvalidOperationException($"Cannot delete '{targetTable}' Id={targetId}. Relation '{row.OwnerCollection}.{row.PropertyName}' uses OnDelete=Restrict."))

                | DeletePolicy.Unlink ->
                    tx.Connection.Execute($"DELETE FROM {qLink} WHERE {targetColumn} = @targetId;", {| targetId = targetId |}) |> ignore
                    if relationKind = Single then
                        for ownerId in ownerIds do
                            updateDbRefJson tx row.OwnerCollection ownerId row.PropertyName 0L

                | DeletePolicy.Cascade ->
                    let qOwner = quoteIdentifier row.OwnerCollection
                    for ownerId in ownerIds do
                        applyOwnerDeletePoliciesCore tx row.OwnerCollection ownerId true
                        applyTargetDeletePoliciesCore tx row.OwnerCollection ownerId
                        tx.Connection.Execute($"DELETE FROM {qOwner} WHERE Id = @id;", {| id = ownerId |}) |> ignore

                | DeletePolicy.Deletion ->
                    raise (InvalidOperationException("OnDelete cannot be Deletion."))
                | _ ->
                    raise (InvalidOperationException($"Invalid OnDelete policy on relation '{row.OwnerCollection}.{row.PropertyName}'."))
)

and private globalRefCountCore (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace targetTable then
        raise (ArgumentException("targetTable is required.", "targetTable"))
    if targetId <= 0L then
        raise (ArgumentOutOfRangeException("targetId", targetId, "targetId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    let targetTable = formatName targetTable
    let rows = readMetadataByTarget tx.Connection targetTable
    let mutable count = 0L
    for row in rows do
        let relationKind = stringToRelationKind row.RefKind
        let linkTable, _, targetColumn = metadataLinkLayout tx.Connection row relationKind
        count <- count + tx.Connection.QueryFirst<int64>($"SELECT COUNT(*) FROM {quoteIdentifier linkTable} WHERE {targetColumn} = @targetId;", {| targetId = targetId |})
    count
let ensureSchemaForOwnerType (tx: RelationTxContext) (ownerType: Type) =
    ensureTxContext tx
    if isNull ownerType then nullArg "ownerType"
    ensureRelationCatalogTable tx.Connection
    let descriptors = buildRelationDescriptors tx ownerType
    for descriptor in descriptors do
        ensureRelationSchema tx descriptor

let private asReadOnlyDict (input: Dictionary<string, int64 array>) =
    input :> IReadOnlyDictionary<string, int64 array>

let private readSingleIdNoCascade (descriptor: RelationDescriptor) (owner: obj) =
    descriptor.Property.GetValue(owner) |> readDbRefId

let prepareInsert (tx: RelationTxContext) (owner: obj) =
    ensureTxContext tx
    ensureOwnerInstance tx.OwnerType owner "owner"
    let descriptors = buildRelationDescriptors tx tx.OwnerType
    let ops = ResizeArray<RelationUpdateManyOp>()
    let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let targetId = resolveSingleTargetIdAndCascade tx descriptor owner
            if targetId > 0L then
                ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, targetId))

        | Many ->
            match collectManyTargetIdsAndCascade tx descriptor owner true with
            | ValueSome ids ->
                resetMap.[descriptor.Property.Name] <- ids
                for id in ids do
                    ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
            | ValueNone -> ()

    if resetMap.Count > 0 then
        resetDbRefManyTrackers owner (asReadOnlyDict resetMap)

    {
        Kind = "Insert"
        OwnerType = tx.OwnerType
        Ops = ops |> Seq.toList
    }

let prepareUpsert (tx: RelationTxContext) (oldOwner: obj voption) (newOwner: obj) =
    ensureTxContext tx
    let hadOldOwner =
        match oldOwner with
        | ValueSome old ->
            ensureOwnerInstance tx.OwnerType old "oldOwner"
            true
        | ValueNone -> false

    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    let descriptors = buildRelationDescriptors tx tx.OwnerType
    let ops = ResizeArray<RelationUpdateManyOp>()
    let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let oldId =
                match oldOwner with
                | ValueSome old -> readSingleIdNoCascade descriptor old
                | ValueNone -> 0L
            let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner
            if oldId <> newId then
                if newId > 0L then
                    ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
                else
                    ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))

        | Many ->
            match collectManyTargetIdsAndCascade tx descriptor newOwner true with
            | ValueSome ids ->
                resetMap.[descriptor.Property.Name] <- ids
                if hadOldOwner then
                    ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                for id in ids do
                    ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
            | ValueNone ->
                if hadOldOwner then
                    ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))

    if resetMap.Count > 0 then
        resetDbRefManyTrackers newOwner (asReadOnlyDict resetMap)

    {
        Kind = "Upsert"
        OwnerType = tx.OwnerType
        Ops = ops |> Seq.toList
    }

let prepareUpdate (tx: RelationTxContext) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    let descriptors = buildRelationDescriptors tx tx.OwnerType
    let ops = ResizeArray<RelationUpdateManyOp>()
    let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let oldId = readSingleIdNoCascade descriptor oldOwner
            let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner
            if oldId <> newId then
                if newId > 0L then
                    ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
                else
                    ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))

        | Many ->
            match collectManyTargetIdsAndCascade tx descriptor newOwner false with
            | ValueNone -> ()
            | ValueSome newIds ->
                resetMap.[descriptor.Property.Name] <- newIds
                let oldIds =
                    match collectManyTargetIdsAndCascade tx descriptor oldOwner false with
                    | ValueSome ids -> ids
                    | ValueNone -> [||]

                let oldSet = HashSet<int64>(oldIds)
                let newSet = HashSet<int64>(newIds)
                if not (oldSet.SetEquals(newSet)) then
                    ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                    for id in newIds do
                        ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))

    if resetMap.Count > 0 then
        resetDbRefManyTrackers newOwner (asReadOnlyDict resetMap)

    {
        Kind = "Update"
        OwnerType = tx.OwnerType
        Ops = ops |> Seq.toList
    }

let prepareDeleteOwner (tx: RelationTxContext) (ownerId: int64) (owner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType owner "owner"
    { OwnerId = ownerId
      OwnerType = tx.OwnerType }
let syncInsert (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureSchemaForOwnerType tx tx.OwnerType
    applyOps tx ownerId plan false

let syncUpsert (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureSchemaForOwnerType tx tx.OwnerType
    applyOps tx ownerId plan false

let syncUpdate (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureSchemaForOwnerType tx tx.OwnerType
    let updateOwnerJson = StringComparer.Ordinal.Equals(plan.Kind, "UpdateMany")
    applyOps tx ownerId plan updateOwnerJson

let syncReplaceOne (tx: RelationTxContext) (ownerId: int64) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    ensureSchemaForOwnerType tx tx.OwnerType
    let plan = prepareUpdate tx oldOwner newOwner
    applyOps tx ownerId plan false

let syncReplaceMany (tx: RelationTxContext) (ownerIds: int64 seq) (oldOwners: obj seq) (newOwner: obj) =
    ensureTxContext tx
    ensureTransaction tx
    if isNull ownerIds then nullArg "ownerIds"
    if isNull oldOwners then nullArg "oldOwners"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    for id in ownerIds do
        if id <= 0L then
            raise (ArgumentOutOfRangeException("ownerIds", id, "Every owner id must be > 0."))
    for oldOwner in oldOwners do
        ensureOwnerInstance tx.OwnerType oldOwner "oldOwners"

    ensureSchemaForOwnerType tx tx.OwnerType

    let ownerIdArr = ownerIds |> Seq.toArray
    let oldOwnerArr = oldOwners |> Seq.toArray
    if ownerIdArr.Length <> oldOwnerArr.Length then
        raise (ArgumentException("ownerIds and oldOwners must have the same length.", "oldOwners"))

    for i = 0 to ownerIdArr.Length - 1 do
        let plan = prepareUpdate tx oldOwnerArr.[i] newOwner
        applyOps tx ownerIdArr.[i] plan false

let syncDeleteOwner (tx: RelationTxContext) (plan: RelationDeletePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if plan.OwnerId <= 0L then
        raise (ArgumentOutOfRangeException("plan.OwnerId", plan.OwnerId, "ownerId must be > 0."))
    ensureRelationCatalogTable tx.Connection
    applyOwnerDeletePoliciesCore tx tx.OwnerTable plan.OwnerId true
    applyTargetDeletePoliciesCore tx tx.OwnerTable plan.OwnerId

let applyTargetDeletePolicies (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    applyTargetDeletePoliciesCore tx targetTable targetId

let applyOwnerDeletePolicies (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then
        raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))

    ensureRelationCatalogTable tx.Connection
    applyOwnerDeletePoliciesCore tx ownerTable ownerId false

let globalRefCount (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    globalRefCountCore tx targetTable targetId

/// Batch-load all DBRef<T> single-reference properties for a set of owner entities.
/// excludedPaths: property names to skip (from Exclude() operator).
/// ownerEntities: (ownerId, ownerObject) pairs.
let batchLoadDBRefProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    =
    if ownerEntities.Length = 0 then ()
    else

    let specs = getRelationSpecs ownerType
    let singleSpecs =
        specs
        |> Array.filter (fun (_, kind, _, _, _, _) -> kind = Single)

    if singleSpecs.Length = 0 then ()
    else

    let ownerTable = formatName ownerTable

    for (prop, _kind, targetType, _onDelete, _onOwnerDelete, _isUnique) in singleSpecs do
        if excludedPaths.Contains(prop.Name) then ()
        else

        let targetTable = resolveTargetCollectionName connection ownerTable prop.Name targetType
        let qTarget = quoteIdentifier targetTable
        let idProp = getWritableInt64IdPropertyOrThrow targetType

        let ownerTargets = ResizeArray<obj * int64>()
        let distinctTargetIds = HashSet<int64>()

        for (_ownerId, ownerObj) in ownerEntities do
            let dbRefObj = prop.GetValue(ownerObj)
            let targetId = readDbRefId dbRefObj
            if targetId > 0L then
                ownerTargets.Add(ownerObj, targetId)
                distinctTargetIds.Add(targetId) |> ignore

        if ownerTargets.Count > 0 then
            let loadedTargets = Dictionary<int64, obj>()
            let targetIds = distinctTargetIds |> Seq.toArray

            let batchSize = 500
            let chunks =
                let mutable i = 0
                [| while i < targetIds.Length do
                       let len = min batchSize (targetIds.Length - i)
                       yield targetIds.[i .. i + len - 1]
                       i <- i + len |]

            for chunk in chunks do
                let paramNames = chunk |> Array.mapi (fun i _ -> $"@_tid{i}")
                let inClause = String.Join(", ", paramNames)
                let sql = $"SELECT Id, json_quote(Value) as ValueJSON FROM {qTarget} WHERE Id IN ({inClause});"
                let parameters = Dictionary<string, obj>(chunk.Length)
                for i = 0 to chunk.Length - 1 do
                    parameters.[paramNames.[i]] <- box chunk.[i]

                for row in connection.Query<{| Id: int64; ValueJSON: string |}>(sql, parameters) do
                    let targetId = row.Id
                    let valueJson = row.ValueJSON

                    if not (isNull valueJson) then
                        let json = JsonValue.Parse valueJson
                        let targetObj = json.ToObject(targetType)
                        idProp.SetValue(targetObj, box targetId)
                        loadedTargets.[targetId] <- targetObj

            for (ownerObj, targetId) in ownerTargets do
                match loadedTargets.TryGetValue(targetId) with
                | true, targetObj ->
                    let loadedRef = createDbRefLoaded prop.PropertyType targetId targetObj
                    prop.SetValue(ownerObj, loadedRef)
                | _ -> ()

/// Batch-load all DBRefMany properties for a set of owner entities.
/// For each Many-kind relation descriptor, issues a batched SELECT against the link table
/// joined with the target collection, groups results by SourceId, and calls SetLoadedBoxed
/// on each entity's DBRefMany property.
/// excludedPaths: property names to skip (from Exclude() operator).
/// ownerEntities: (ownerId, ownerObject) pairs.
let batchLoadDBRefManyProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    =
    if ownerEntities.Length = 0 then ()
    else

    let ownerTable = formatName ownerTable
    let tx: RelationTxContext = {
        Connection = connection
        OwnerTable = ownerTable
        OwnerType = ownerType
        InTransaction = false
    }
    let manyDescriptors =
        buildRelationDescriptors tx ownerType
        |> Array.filter (fun d -> d.Kind = Many)

    if manyDescriptors.Length = 0 then ()
    else

    for descriptor in manyDescriptors do
        // Skip excluded paths.
        if excludedPaths.Contains(descriptor.Property.Name) then ()
        else

        let qLink = quoteIdentifier descriptor.LinkTable
        let qTarget = quoteIdentifier descriptor.TargetTable

        // Chunk owner IDs into batches of 500 per Ruling 2.
        let batchSize = 500
        let ownerIds = ownerEntities |> Array.map fst

        // Accumulate results across chunks: OwnerId -> (targetId, targetObj) list.
        let grouped = Dictionary<int64, ResizeArray<int64 * obj>>()

        let idProp = getWritableInt64IdPropertyOrThrow descriptor.TargetType
        let ownerColumn = if descriptor.OwnerUsesSourceColumn then "SourceId" else "TargetId"
        let targetColumn = if descriptor.OwnerUsesSourceColumn then "TargetId" else "SourceId"

        let chunks =
            let mutable i = 0
            [| while i < ownerIds.Length do
                   let len = min batchSize (ownerIds.Length - i)
                   yield ownerIds.[i .. i + len - 1]
                   i <- i + len |]

        for chunk in chunks do
            // Build parameterized IN clause.
            let paramNames = chunk |> Array.mapi (fun i _ -> $"@_bid{i}")
            let inClause = String.Join(", ", paramNames)
            let sql =
                $"SELECT lnk.{ownerColumn} AS OwnerId, {qTarget}.Id AS TargetId, json_quote({qTarget}.Value) as ValueJSON " +
                $"FROM {qLink} lnk JOIN {qTarget} ON {qTarget}.Id = lnk.{targetColumn} " +
                $"WHERE lnk.{ownerColumn} IN ({inClause});"
            let parameters = Dictionary<string, obj>(chunk.Length)
            for i = 0 to chunk.Length - 1 do
                parameters.[paramNames.[i]] <- box chunk.[i]

            for row in connection.Query<{| OwnerId: int64; TargetId: int64; ValueJSON: string |}>(sql, parameters) do
                let ownerId = row.OwnerId
                let targetId = row.TargetId
                let valueJson = row.ValueJSON

                if not (isNull valueJson) then
                    let json = JsonValue.Parse valueJson
                    let targetObj = json.ToObject(descriptor.TargetType)
                    idProp.SetValue(targetObj, box targetId)

                    match grouped.TryGetValue(ownerId) with
                    | true, list -> list.Add(targetId, targetObj)
                    | _ ->
                        let list = ResizeArray()
                        list.Add(targetId, targetObj)
                        grouped.[ownerId] <- list

        // Populate each owner entity's DBRefMany property.
        for (ownerId, ownerObj) in ownerEntities do
            let tracker = descriptor.Property.GetValue(ownerObj)
            match tracker with
            | :? IDBRefManyInternal as internal' ->
                match grouped.TryGetValue(ownerId) with
                | true, items ->
                    let ids = items |> Seq.map fst
                    let objs = items |> Seq.map snd
                    internal'.SetLoadedBoxed objs ids
                | _ ->
                    // No linked items — set loaded with empty.
                    internal'.SetLoadedBoxed Seq.empty Seq.empty
            | null ->
                // Property is null — instantiate a new DBRefMany and set it.
                let dbRefManyType = typedefof<DBRefMany<_>>.MakeGenericType(descriptor.TargetType)
                let instance = Activator.CreateInstance(dbRefManyType)
                descriptor.Property.SetValue(ownerObj, instance)
                let internal' = instance :?> IDBRefManyInternal
                match grouped.TryGetValue(ownerId) with
                | true, items ->
                    internal'.SetLoadedBoxed (items |> Seq.map snd) (items |> Seq.map fst)
                | _ ->
                    internal'.SetLoadedBoxed Seq.empty Seq.empty
            | _ -> ()
