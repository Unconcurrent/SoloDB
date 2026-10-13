namespace SoloDatabase

open SoloDatabase.JsonSerializator
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open SoloDatabase.Types
open System.IO
open System.Text
open NativeSQLiteTools
open JsonFunctions
open FileStorage
open Connections
open Utils
open System.Runtime.CompilerServices
open System.Reflection
open System.Data
open System.Globalization
open System.Linq
open SoloDatabase.Attributes

/// <summary>
/// Contains internal helper functions for database and collection management.
/// </summary>
module internal Helper =
    let internal listCollectionNamesSnapshot (connection: NativeSqliteConnection) : string array =
        let mutable statement = connection.OpenStatement("SELECT Name FROM SoloDBCollections")
        try
            let names = ResizeArray<string>()
            while statement.Step() do names.Add(statement.Row.GetString(0))
            statement.Dispose()
            names.ToArray()
        with error -> statement.Abort(error); reraise()

    let internal sqlLiteralEscape (value: string) =
        value.Replace("'", "''")

    let internal normalizeCatalogNameOrThrow (step: string) (name: string) =
        let normalized = Utils.sanitizeName name
        if String.IsNullOrWhiteSpace name || normalized <> name then
            raise (InvalidOperationException($"Schema migration failed at {step}. Invalid collection name in SoloDBCollections: '{name}'."))
        normalized

    /// <summary>
    /// Checks if a collection with the specified name exists in the database.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <param name="connection">The active SQLite connection.</param>
    /// <returns>True if the collection exists, otherwise false.</returns>
    let internal existsCollection (name: string) (connection: NativeSqliteConnection) =
        let mutable statement = connection.OpenStatement("SELECT Name FROM SoloDBCollections WHERE Name = @name LIMIT 1")
        try
            statement.BindText(1, name)
            let exists = statement.Step() && not (statement.Row.IsDBNull(0))
            statement.Dispose()
            exists
        with error -> statement.Abort(error); reraise()

    let internal registerTypeCollection<'T> (collectionName: string) (connection: NativeSqliteConnection) =
        CustomIdRunner.Register<'T>()
        if not (typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>) then
            let mutable statement = connection.OpenStatement("INSERT INTO SoloDBTypeCollectionMap(TypeKey, CollectionName) VALUES(@typeKey, @collectionName) ON CONFLICT(TypeKey, CollectionName) DO NOTHING;")
            try
                statement.BindText(1, TypeMetadata.typeIdentityKey typeof<'T>)
                statement.BindText(2, collectionName)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()

    /// <summary>
    /// Drops all event triggers associated with a collection table, if they exist.
    /// </summary>
    /// <param name="name">The name of the collection whose trigger should be dropped.</param>
    /// <param name="conn">The active SQLite connection.</param>
    let internal dropTriggersForTable (name: string) (conn: NativeSqliteConnection) =
        for prefix in [| "Update"; "Insert"; "Delete"; "Updated"; "Inserted"; "Deleted" |] do
            let mutable statement = conn.OpenStatement($"DROP TRIGGER IF EXISTS \"SoloDB_{prefix}_{name}\"")
            try
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()

    /// <summary>
    /// Drops a collection's table and removes its metadata entry.
    /// </summary>
    /// <param name="name">The name of the collection to drop.</param>
    /// <param name="connection">The active SQLite connection.</param>
    let internal dropCollection (name: string) (connection: NativeSqliteConnection) =
        let relationCatalogExists =
            let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'SoloDBRelation')")
            try
                statement.Step() |> ignore
                let exists = statement.Row.GetBoolean(0)
                statement.Dispose()
                exists
            with error -> statement.Abort(error); reraise()

        if relationCatalogExists then
            let inboundCount =
                let mutable statement = connection.OpenStatement("SELECT COUNT(*) FROM SoloDBRelation WHERE TargetCollection = @name AND OwnerCollection <> @name")
                try
                    statement.BindText(1, name)
                    statement.Step() |> ignore
                    let count = statement.Row.GetInt64(0)
                    statement.Dispose()
                    count
                with error -> statement.Abort(error); reraise()
            if inboundCount > 0L then
                raise (InvalidOperationException(
                    sprintf "Error: Cannot drop collection '%s'.\nReason: It is referenced by active relations.\nFix: Remove relations first, then drop the collection." name))

            let hasFileSystemRefs =
                let mutable statement = connection.OpenStatement("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='SoloDBFileSystemReference')")
                try
                    statement.Step() |> ignore
                    let exists = statement.Row.GetBoolean(0)
                    statement.Dispose()
                    exists
                with error -> statement.Abort(error); reraise()
            if hasFileSystemRefs then
                let mutable statement = connection.OpenStatement("DELETE FROM SoloDBFileSystemReference WHERE OwnerCollection=@name")
                try
                    statement.BindText(1, name)
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
            // Snapshot the owned catalog rows before dropping their physical tables.
            let outboundRows =
                let mutable statement = connection.OpenStatement("SELECT Name, TargetCollection, RefKind FROM SoloDBRelation WHERE OwnerCollection = @name AND TargetCollection NOT IN ('SoloDBFileHeader','SoloDBDirectoryHeader')")
                try
                    statement.BindText(1, name)
                    let rows = ResizeArray<struct (string * string * string)>()
                    while statement.Step() do
                        let row = statement.Row
                        rows.Add(struct (row.GetString(0), row.GetString(1), row.GetString(2)))
                    statement.Dispose()
                    rows
                with error -> statement.Abort(error); reraise()
            for struct (relationName, targetCollection, refKind) in outboundRows do
                let canonicalLinkTable =
                    RelationsTypes.canonicalManyRelationName name targetCollection
                    |> RelationsTypes.linkTableFromRelationName
                let useSharedManyLinkTable =
                    refKind = RelationsTypes.relationKindToString RelationKind.Many
                    && (StringComparer.Ordinal.Equals(name, targetCollection)
                        || (let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name)")
                            try
                                statement.BindText(1, canonicalLinkTable)
                                statement.Step() |> ignore
                                let exists = statement.Row.GetBoolean(0)
                                statement.Dispose()
                                exists
                            with error -> statement.Abort(error); reraise()))
                let linkTable =
                    if useSharedManyLinkTable then canonicalLinkTable
                    else "SoloDBRelLink_" + relationName
                let mutable statement = connection.OpenStatement(sprintf "DROP TABLE IF EXISTS \"%s\"" linkTable)
                try
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
            let mutable statement = connection.OpenStatement("DELETE FROM SoloDBRelation WHERE OwnerCollection = @name")
            try
                statement.BindText(1, name)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()

        dropTriggersForTable name connection
        let mutable table = connection.OpenStatement(sprintf "DROP TABLE IF EXISTS \"%s\"" name)
        try
            table.Step() |> ignore
            table.Dispose()
        with error -> table.Abort(error); reraise()
        let mutable mapping = connection.OpenStatement("DELETE FROM SoloDBTypeCollectionMap WHERE CollectionName = @name")
        try
            mapping.BindText(1, name)
            mapping.Step() |> ignore
            mapping.Dispose()
        with error -> mapping.Abort(error); reraise()
        let mutable catalog = connection.OpenStatement("DELETE FROM SoloDBCollections Where Name = @name")
        try
            catalog.BindText(1, name)
            catalog.Step() |> ignore
            catalog.Dispose()
        with error -> catalog.Abort(error); reraise()
        // Drop/recreate must rebuild relation descriptors from a clean cache snapshot.
        TypeMetadataCache.ClearReferenceSpecs()

    /// <summary>
    /// Inserts serialized JSON into a specified table, with options for replacement and explicit ID.
    /// </summary>
    /// <param name="orReplace">If true, uses 'INSERT OR REPLACE'.</param>
    /// <param name="id">An optional ID to insert. If None, an ID is generated by SQLite.</param>
    /// <param name="name">The name of the target table (collection).</param>
    /// <param name="json">The owned JSON character range to insert.</param>
    /// <param name="connection">The active SQLite connection.</param>
    /// <returns>The ID of the inserted row.</returns>
    let private insertJson (orReplace: bool) (id: int64 option) (name: string) (json: ArraySegment<char>) (connection: NativeSqliteConnection) =
        let includeId = id.IsSome

        let queryStringBuilder = StringBuilder(64 + name.Length + (if orReplace then 11 else 0) + (if includeId then 7 else 0))

        let queryString =
            queryStringBuilder
                .Append("INSERT ")
                .Append(if orReplace then "OR REPLACE " else String.Empty)
                .Append(" INTO \"")
                .Append(name)
                .Append("\"(")
                .Append(if includeId then "Id," else String.Empty)
                .Append("Value) VALUES(")
                .Append(if includeId then "@id," else String.Empty)
                .Append("jsonb(@jsonText)) RETURNING Id;")
                .ToString()

        let mutable statement = connection.OpenStatement(queryString)
        try
            if includeId then statement.BindInt64(1, id.Value)
            statement.BindTextBuffer((if includeId then 2 else 1), json)
            if not (statement.Step()) then Seq.head (Seq.empty<unit>)
            let insertedId = statement.Row.GetInt64(0)
            statement.Dispose()
            insertedId
        with error -> statement.Abort(error); reraise()

    /// <summary>
    /// Core implementation for inserting an item into a collection. Handles ID generation and serialization.
    /// </summary>
    /// <param name="typed">If true, serializes the object with its .NET type information.</param>
    /// <param name="item">The item to insert.</param>
    /// <param name="connection">The active SQLite connection.</param>
    /// <param name="name">The name of the collection.</param>
    /// <param name="orReplace">If true, performs an 'INSERT OR REPLACE' operation.</param>
    /// <param name="collection">The collection instance, used for ID generation context.</param>
    /// <returns>The ID of the inserted item.</returns>
    let private insertImpl<'T when 'T :> obj> (typed: bool) (item: 'T) (connection: NativeSqliteConnection) (name: string) (orReplace: bool) (collection: ISoloDBCollection<'T>) =
        let customIdGen = CustomTypeId<'T>.Get()
        let existsWritebleDirectId = EntityDocumentId<'T>.Value


        CustomIdRunner.RunIfEmpty(item, collection)

        let json = toDocument typed item


        let id =
            if existsWritebleDirectId && -1L >= EntityDocumentId<'T>.Read item then
                raise (InvalidOperationException
                    "Error: Invalid Id value.\nReason: Id must be 0 for auto-generated ids or > 0 for explicit ids.\nFix: Use Id=0 for new entities or a positive Id for explicit insert.")
            elif existsWritebleDirectId && 0L <> EntityDocumentId<'T>.Read item then
                // Inserting with Id
                insertJson orReplace (Some (EntityDocumentId<'T>.Read item)) name json connection
            else
                // Inserting without Id
                insertJson orReplace None name json connection

        if existsWritebleDirectId then
            EntityDocumentId<'T>.Write item id

        id

    /// <summary>
    /// Inserts an item into a collection.
    /// </summary>
    let internal insertInner (typed: bool) (item: 'T) (connection: NativeSqliteConnection) (name: string) (collection: ISoloDBCollection<'T>) =
        insertImpl typed item connection name false collection

    /// <summary>
    /// Inserts or replaces an item in a collection.
    /// </summary>
    let internal insertOrReplaceInner (typed: bool) (item: 'T) (connection: NativeSqliteConnection) (name: string) (collection: ISoloDBCollection<'T>) =
        insertImpl typed item connection name true collection

    /// <summary>
    /// Sanitizes a collection name to prevent SQL injection and ensure it's a valid table name.
    /// </summary>
    /// <param name="name">The raw collection name.</param>
    /// <returns>A sanitized name containing only letters, digits, and underscores.</returns>
    let internal formatName (name: string) =
        Utils.sanitizeName name

    let internal validateUserCollectionName (name: string) =
        Utils.validateUserCollectionName name

    /// <summary>
    /// Creates a Dictionary from a sequence of KeyValuePairs.
    /// </summary>
    let internal createDict(items: KeyValuePair<'a, 'b> seq) =
        let dic = new Dictionary<'a, 'b>()

        for kvp in items do
            dic.Add(kvp.Key, kvp.Value)

        dic

    /// <summary>
    /// Retrieves properties of a type that are marked with the [Indexed] attribute.
    /// Delegates to the canonical shared implementation in HelperSchema.
    /// </summary>
    let internal getIndexesFields<'a>() =
        IndexMetadata.fields<'a>()

    /// <summary>
    /// Translates a LINQ expression into an SQL index definition and a sanitized index name.
    /// Delegates to the canonical shared implementation in HelperSchema.
    /// </summary>
    let internal getIndexWhereAndName<'T, 'R> (name: string) (expression: Expression<System.Func<'T, 'R>>) =
        HelperSchema.getIndexWhereAndNameShared<'T, 'R> name expression

    /// <summary>
    /// Ensures a non-unique index exists for the given expression.
    /// </summary>
    let internal ensureIndex<'T, 'R> (collectionName: string) (conn: NativeSqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndName<'T,'R> collectionName expression

        let indexSQL = $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"

        let mutable statement = conn.OpenStatement(indexSQL)
        try
            statement.Step() |> ignore
            let affected = statement.RecordsAffected
            statement.Dispose()
            affected
        with error -> statement.Abort(error); reraise()

    /// <summary>
    /// Ensures a unique index exists for the given expression.
    /// </summary>
    let internal ensureUniqueAndIndex<'T,'R> (collectionName: string) (conn: NativeSqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndName<'T,'R> collectionName expression

        let indexSQL = $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"

        let mutable statement = conn.OpenStatement(indexSQL)
        try
            statement.Step() |> ignore
            let affected = statement.RecordsAffected
            statement.Dispose()
            affected
        with error -> statement.Abort(error); reraise()

    /// <summary>
    /// Ensures that all indexes declared via the [Indexed] attribute on the type 'T exist.
    /// </summary>
    let internal ensureDeclaredIndexesFields<'T> (name: string) (conn: NativeSqliteConnection) =
        for index in getIndexesFields<'T>() do HelperSchema.ensureDeclaredIndex name conn false index

    let internal getSQLForTriggersForTable (name: string) =
        HelperSchema.getSQLForTriggersForTable name

    let internal createTriggersForTable (name: string) (conn: NativeSqliteConnection) =
        HelperSchema.createTriggersForTable name conn

    let internal createTableInner<'T> (name: string) (conn: NativeSqliteConnection) =
        HelperSchema.createTableInner<'T> name conn

    /// <summary>
    /// Internal DTO to pass data from the main DB instance to collection instances.
    /// </summary>
    let internal collectionNameOf<'T> =
        Utils.collectionNameOf<'T>()
