module internal SoloDatabase.RelationsSchemaBuilder

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
    RelationsSharedSql.getSQLForTriggersForTable name

let internal ensureCollectionMetadataColumn (connection: SqliteConnection) (tableName: string) =
    let normalizedTableName = Helper.normalizeCatalogNameOrThrow "relation metadata ensure" tableName
    let qTable = quoteIdentifier normalizedTableName
    let tableLit = Helper.sqlLiteralEscape normalizedTableName
    let hasMetadata =
        connection.QueryFirst<int64>(
            $"SELECT CASE WHEN EXISTS (SELECT 1 FROM pragma_table_info('{tableLit}') WHERE name = 'Metadata') THEN 1 ELSE 0 END") = 1L
    if not hasMetadata then
        connection.Execute($"ALTER TABLE {qTable} ADD COLUMN Metadata JSONB NOT NULL DEFAULT '{{}}';") |> ignore

let internal ensureCollectionTableExists (connection: SqliteConnection) (tableName: string) =
    let tableName = Helper.normalizeCatalogNameOrThrow "relation table ensure" tableName
    let qTable = quoteIdentifier tableName
    let exists =
        connection.QueryFirst<int64>("SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END", {| name = tableName |}) = 1L

    if not exists then
        connection.Execute(RelationsSharedSql.createCollectionTableSql qTable) |> ignore
        connection.Execute(getSQLForTriggersForTable tableName) |> ignore

    connection.Execute(
        "INSERT INTO SoloDBCollections(Name) VALUES (@name) ON CONFLICT(Name) DO NOTHING;",
        {| name = tableName |}) |> ignore

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
    connection.Query<string>(
        "SELECT CollectionName FROM SoloDBTypeCollectionMap WHERE TypeKey = @typeKey;",
        {| typeKey = Utils.typeIdentityKey targetType |})
    |> Seq.map formatName
    |> Seq.distinct
    |> Seq.toArray

let internal collectionExistsByName (connection: SqliteConnection) (collectionName: string) =
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
            raise (InvalidOperationException(
                $"Error: Ambiguous target collection mapping for relation {ownerTable}.{propertyName} and type '{targetType.FullName}'.\nReason: Multiple collections are mapped ({mappedList}).\nFix: Register exactly one target collection for this type."))
        else
            defaultTable
