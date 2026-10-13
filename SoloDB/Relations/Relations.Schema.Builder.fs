module internal SoloDatabase.RelationsSchemaBuilder

open System
open System.Reflection
open System.Collections.Generic
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open NativeSQLiteTools
open RelationsTypes

let internal ensureRelationCatalogTable (connection: NativeSqliteConnection) =
    let mutable statement = connection.OpenStatement("""
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
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable index = connection.OpenStatement("CREATE INDEX IF NOT EXISTS SoloDBRelation_Target ON SoloDBRelation(TargetCollection)")
    try
        index.Step() |> ignore
        index.Dispose()
    with error -> index.Abort(error); reraise()


let internal getSQLForTriggersForTable (name: string) =
    RelationsSharedSql.getSQLForTriggersForTable name

let internal ensureCollectionMetadataColumn (connection: NativeSqliteConnection) (tableName: string) =
    let normalizedTableName = Helper.normalizeCatalogNameOrThrow "relation metadata ensure" tableName
    let qTable = quoteIdentifier normalizedTableName
    let mutable metadata = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM pragma_table_info(@table) WHERE name = 'Metadata')")
    let hasMetadata =
        try
            metadata.BindText(1, normalizedTableName)
            if not (metadata.Step()) then raise (InvalidOperationException("Table metadata was not returned."))
            let exists = metadata.Row.GetBoolean(0)
            metadata.Dispose()
            exists
        with error -> metadata.Abort(error); reraise()
    if not hasMetadata then
        let mutable statement = connection.OpenStatement($"ALTER TABLE {qTable} ADD COLUMN Metadata JSONB NOT NULL DEFAULT '{{}}';")
        try
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

let internal ensureCollectionTableExists (connection: NativeSqliteConnection) (tableName: string) =
    let tableName = Helper.normalizeCatalogNameOrThrow "relation table ensure" tableName
    let qTable = quoteIdentifier tableName
    let exists =
        let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name)")
        try
            statement.BindText(1, tableName)
            if not (statement.Step()) then raise (InvalidOperationException("Table existence was not returned."))
            let exists = statement.Row.GetBoolean(0)
            statement.Dispose()
            exists
        with error -> statement.Abort(error); reraise()

    if not exists then
        let mutable table = connection.OpenStatement(RelationsSharedSql.createCollectionTableSql qTable)
        try
            table.Step() |> ignore
            table.Dispose()
        with error -> table.Abort(error); reraise()
        let mutable triggers = connection.AcquireScript(NativeStatementSource.Text (getSQLForTriggersForTable tableName), null)
        try
            triggers.Consume() |> ignore
            triggers.Dispose()
        with error -> triggers.Abort(error); reraise()

    let mutable statement = connection.OpenStatement("INSERT INTO SoloDBCollections(Name) VALUES (@name) ON CONFLICT(Name) DO NOTHING;")
    try
        statement.BindText(1, tableName)
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()

let private tryGetStoredTargetCollection (connection: NativeSqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    let mutable statement = connection.OpenStatement("SELECT TargetCollection FROM SoloDBRelation WHERE OwnerCollection = @ownerCollection AND PropertyName = @propertyName LIMIT 1;")
    try
        statement.BindText(1, ownerTable)
        statement.BindText(2, propertyName)
        let value = if statement.Step() && not (statement.Row.IsDBNull(0)) then Some(statement.Row.GetString(0)) else None
        statement.Dispose()
        value
    with error -> statement.Abort(error); reraise()

let internal tryGetStoredRelationName (connection: NativeSqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    let mutable statement = connection.OpenStatement("SELECT Name FROM SoloDBRelation WHERE OwnerCollection = @ownerCollection AND PropertyName = @propertyName LIMIT 1;")
    try
        statement.BindText(1, ownerTable)
        statement.BindText(2, propertyName)
        let value = if statement.Step() && not (statement.Row.IsDBNull(0)) then Some(statement.Row.GetString(0)) else None
        statement.Dispose()
        value
    with error -> statement.Abort(error); reraise()

let private readMappedCollectionsForType (connection: NativeSqliteConnection) (targetType: Type) =
    let mutable statement = connection.OpenStatement("SELECT CollectionName FROM SoloDBTypeCollectionMap WHERE TypeKey = @typeKey;")
    try
        statement.BindText(1, TypeMetadata.typeIdentityKey targetType)
        let seen = HashSet<string>()
        let names = ResizeArray<string>()
        while statement.Step() do
            let name = formatName (statement.Row.GetString(0))
            if seen.Add(name) then names.Add(name)
        statement.Dispose()
        names.ToArray()
    with error -> statement.Abort(error); reraise()

let internal collectionExistsByName (connection: NativeSqliteConnection) (collectionName: string) =
    let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM SoloDBCollections WHERE Name = @name)")
    try
        statement.BindText(1, collectionName)
        if not (statement.Step()) then raise (InvalidOperationException("Collection existence was not returned."))
        let exists = statement.Row.GetBoolean(0)
        statement.Dispose()
        exists
    with error -> statement.Abort(error); reraise()

let internal sqliteTableExistsByName (connection: NativeSqliteConnection) (tableName: string) =
    let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name)")
    try
        statement.BindText(1, tableName)
        if not (statement.Step()) then raise (InvalidOperationException("Table existence was not returned."))
        let exists = statement.Row.GetBoolean(0)
        statement.Dispose()
        exists
    with error -> statement.Abort(error); reraise()

type internal MissingReadMetadataState =
    | AbsentNoEvidence
    | PoisonedWithEvidence

let internal classifyMissingReadMetadata
    (connection: NativeSqliteConnection)
    (ownerTable: string)
    (propertyName: string)
    (kind: RelationKind)
    (targetType: Type) =
    let ownerTable = formatName ownerTable
    match kind with
    | Single ->
        AbsentNoEvidence
    | Many ->
        let targetTable = formatName targetType.Name
        let defaultLinkTable = linkTableFromRelationName (relationName ownerTable propertyName)
        let canonicalLinkTable = linkTableFromRelationName (canonicalManyRelationName ownerTable targetTable)
        let hasEvidence =
            [| defaultLinkTable; canonicalLinkTable |]
            |> Array.distinct
            |> Array.exists (sqliteTableExistsByName connection)
        if hasEvidence then PoisonedWithEvidence else AbsentNoEvidence

let internal resolveTargetCollectionName (connection: NativeSqliteConnection) (ownerTable: string) (propertyName: string) (targetType: Type) =
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
            raise (InvalidOperationException(
                $"Error: Ambiguous target collection mapping for relation {ownerTable}.{propertyName} and type '{targetType.FullName}'.\nReason: Multiple collections are mapped ({mappedList}).\nFix: Register exactly one target collection for this type."))
        else
            defaultTable


/// Shared filesystem link storage. The declaration FK enforces kind and cardinality at the engine boundary.
let internal ensureFileSystemReferenceTable (connection: NativeSqliteConnection) =
    ensureRelationCatalogTable connection
    let mutable statement = connection.OpenStatement("""
CREATE UNIQUE INDEX IF NOT EXISTS SoloDBRelation_Declaration ON SoloDBRelation(OwnerCollection,PropertyName,RefKind,TargetCollection);
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable statement = connection.OpenStatement("""
CREATE TABLE IF NOT EXISTS SoloDBFileSystemReference (
    OwnerCollection TEXT NOT NULL,
    OwnerId INTEGER NOT NULL CHECK(OwnerId > 0),
    Property TEXT NOT NULL,
    RefKind TEXT NOT NULL CHECK(RefKind IN ('Single','Many')),
    FileId INTEGER REFERENCES SoloDBFileHeader(Id) ON DELETE RESTRICT CHECK(FileId > 0),
    DirectoryId INTEGER REFERENCES SoloDBDirectoryHeader(Id) ON DELETE RESTRICT CHECK(DirectoryId > 0),
    TargetCollection TEXT GENERATED ALWAYS AS (CASE WHEN FileId IS NOT NULL THEN 'SoloDBFileHeader' ELSE 'SoloDBDirectoryHeader' END) STORED,
    CHECK((FileId IS NOT NULL) != (DirectoryId IS NOT NULL)),
    FOREIGN KEY(OwnerCollection,Property,RefKind,TargetCollection) REFERENCES SoloDBRelation(OwnerCollection,PropertyName,RefKind,TargetCollection) ON DELETE CASCADE
) STRICT;
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable statement = connection.OpenStatement("""
CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileSystemReference_FileMember ON SoloDBFileSystemReference(OwnerCollection,OwnerId,Property,FileId) WHERE FileId IS NOT NULL;
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable statement = connection.OpenStatement("""
CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileSystemReference_DirectoryMember ON SoloDBFileSystemReference(OwnerCollection,OwnerId,Property,DirectoryId) WHERE DirectoryId IS NOT NULL;
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable statement = connection.OpenStatement("""
CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileSystemReference_Single ON SoloDBFileSystemReference(OwnerCollection,OwnerId,Property) WHERE RefKind='Single';
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable statement = connection.OpenStatement("""
CREATE INDEX IF NOT EXISTS SoloDBFileSystemReference_File ON SoloDBFileSystemReference(FileId) WHERE FileId IS NOT NULL;
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable statement = connection.OpenStatement("""
CREATE INDEX IF NOT EXISTS SoloDBFileSystemReference_Directory ON SoloDBFileSystemReference(DirectoryId) WHERE DirectoryId IS NOT NULL;
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()

    let mutable statement = connection.OpenStatement("CREATE INDEX IF NOT EXISTS SoloDBFileSystemReference_Owner ON SoloDBFileSystemReference(OwnerCollection,OwnerId,Property)")
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable statement = connection.OpenStatement("CREATE INDEX IF NOT EXISTS SoloDBFileSystemReference_Declaration ON SoloDBFileSystemReference(OwnerCollection,Property,RefKind,TargetCollection)")
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()

let internal ensureFileSystemOwnerCleanup (connection: NativeSqliteConnection) (ownerTable: string) =
    let qTable = quoteIdentifier ownerTable
    let qTrigger = quoteIdentifier ("SoloDB_FileSystemReference_Delete_" + ownerTable)
    let ownerLiteral = ownerTable.Replace("'", "''")
    let mutable statement = connection.OpenStatement($"""
CREATE TRIGGER IF NOT EXISTS {qTrigger} AFTER DELETE ON {qTable}
BEGIN
    DELETE FROM SoloDBFileSystemReference WHERE OwnerCollection='{ownerLiteral}' AND OwnerId=OLD.Id;
END;
    """)
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
