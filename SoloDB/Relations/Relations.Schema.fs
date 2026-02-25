
module internal SoloDatabase.RelationsSchema

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SQLiteTools
open RelationsTypes

let internal ensureRelationCatalogTable (connection: SqliteConnection) =
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

let internal getSQLForTriggersForTable (name: string) =
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

let internal ensureCollectionTableExists (connection: SqliteConnection) (tableName: string) =
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

let internal getRelationSpecs (ownerType: Type) =
    relationSpecsCache.GetOrAdd(ownerType, Func<_, _>(buildRelationSpecs))

let internal hasManyBackReference (ownerType: Type) (targetType: Type) =
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

let internal tryGetStoredRelationName (connection: SqliteConnection) (ownerTable: string) (propertyName: string) =
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

let internal sqliteTableExistsByName (connection: SqliteConnection) (tableName: string) =
    connection.QueryFirst<int64>(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
        {| name = tableName |}) = 1L

let internal resolveTargetCollectionName (connection: SqliteConnection) (ownerTable: string) (propertyName: string) (targetType: Type) =
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

let private br04Message (ownerType: Type) (ownerTable: string) (linkTable: string) (propNames: string array) =
    let props = String.Join(",", propNames)
    $"error[SDBREL0004] patternId=NLR-SCH-04 phase=build shape=contradictory-shared-many ownerType={ownerType.FullName} ownerCollection={ownerTable} linkTable={linkTable} properties={props} collisions={props} message=Multiple DBRefMany properties on '{ownerType.Name}' resolve to the same link table '{linkTable}'. This contradictory shared-many topology is not supported."

let private validateSharedManyContradictions (ownerType: Type) (ownerTable: string) (descriptors: RelationDescriptor array) =
    descriptors
    |> Array.filter (fun d -> d.Kind = Many)
    |> Array.groupBy (fun d -> d.LinkTable)
    |> Array.iter (fun (linkTable, grp) ->
        if grp.Length > 1 then
            let propNames = grp |> Array.map (fun d -> d.Property.Name) |> Array.sort
            raise (InvalidOperationException(br04Message ownerType ownerTable linkTable propNames)))

let internal buildRelationDescriptors (tx: RelationTxContext) (ownerType: Type) =
    let ownerTable = formatName tx.OwnerTable
    let descriptors =
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

    validateSharedManyContradictions ownerType ownerTable descriptors
    descriptors

let private br05Message (ownerTable: string) (propertyName: string) (phase: string) =
    $"error[SDBREL0005] patternId=NLR-SCH-05 phase={phase} shape=missing-relation-metadata ownerCollection={ownerTable} property={propertyName} message=Relation metadata missing for '{ownerTable}.{propertyName}'. Cannot auto-heal: prior relation evidence exists."

let private hasCatalogRow (connection: SqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    connection.QueryFirst<int64>(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM SoloDBRelation WHERE OwnerCollection = @owner AND PropertyName = @prop) THEN 1 ELSE 0 END",
        {| owner = ownerTable; prop = propertyName |}) = 1L

/// BR-05 guard: prevent metadata resurrection when prior relation evidence exists.
/// The link table is the definitive evidence that a relation was previously established.
/// Target collections can exist independently (they store their own entities) and must not
/// be treated as relation evidence.
let internal ensureMetadataNotResurrected (tx: RelationTxContext) (descriptor: RelationDescriptor) =
    let ownerExists = collectionExistsByName tx.Connection descriptor.OwnerTable
    if ownerExists then
        let linkExists = sqliteTableExistsByName tx.Connection descriptor.LinkTable
        if linkExists && not (hasCatalogRow tx.Connection descriptor.OwnerTable descriptor.Property.Name) then
            raise (InvalidOperationException(br05Message descriptor.OwnerTable descriptor.Property.Name "build"))

let internal ensureRelationSchema (tx: RelationTxContext) (descriptor: RelationDescriptor) =
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
