namespace SoloDatabase

open SoloDatabase.JsonSerializator
open Microsoft.Data.Sqlite
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open SoloDatabase.Types
open System.IO
open System.Text
open SQLiteTools
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
    /// <summary>
    /// Checks if a collection with the specified name exists in the database.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <param name="connection">The active SQLite connection.</param>
    /// <returns>True if the collection exists, otherwise false.</returns>
    let internal existsCollection (name: string) (connection: SqliteConnection) =
        connection.QueryFirstOrDefault<string>("SELECT Name FROM SoloDBCollections WHERE Name = @name LIMIT 1", {|name = name|}) <> null

    let internal registerTypeCollection<'T> (collectionName: string) (connection: SqliteConnection) =
        if not (typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>) then
            connection.Execute(
                "INSERT INTO SoloDBTypeCollectionMap(TypeKey, CollectionName) VALUES(@typeKey, @collectionName) ON CONFLICT(TypeKey, CollectionName) DO NOTHING;",
                {| typeKey = Utils.typeIdentityKey typeof<'T>; collectionName = collectionName |}) |> ignore

    /// <summary>
    /// Drops all event triggers associated with a collection table, if they exist.
    /// </summary>
    /// <param name="name">The name of the collection whose trigger should be dropped.</param>
    /// <param name="conn">The active SQLite connection.</param>
    let internal dropTriggersForTable (name: string) (conn: SqliteConnection) =
        let updateTriggerName = $"SoloDB_Update_{name}"
        let insertTriggerName = $"SoloDB_Insert_{name}"
        let deleteTriggerName = $"SoloDB_Delete_{name}"
        let updatedTriggerName = $"SoloDB_Updated_{name}"
        let insertedTriggerName = $"SoloDB_Inserted_{name}"
        let deletedTriggerName = $"SoloDB_Deleted_{name}"

        conn.Execute($"DROP TRIGGER IF EXISTS \"{updateTriggerName}\"") |> ignore
        conn.Execute($"DROP TRIGGER IF EXISTS \"{insertTriggerName}\"") |> ignore
        conn.Execute($"DROP TRIGGER IF EXISTS \"{deleteTriggerName}\"") |> ignore
        conn.Execute($"DROP TRIGGER IF EXISTS \"{updatedTriggerName}\"") |> ignore
        conn.Execute($"DROP TRIGGER IF EXISTS \"{insertedTriggerName}\"") |> ignore
        conn.Execute($"DROP TRIGGER IF EXISTS \"{deletedTriggerName}\"") |> ignore

    /// <summary>
    /// Drops a collection's table and removes its metadata entry.
    /// </summary>
    /// <param name="name">The name of the collection to drop.</param>
    /// <param name="connection">The active SQLite connection.</param>
    let internal dropCollection (name: string) (connection: SqliteConnection) =
        // Phase 2 guard: check SoloDBRelation for inbound/outbound references before dropping.
        let relationCatalogExists =
            connection.QueryFirst<int64>(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'SoloDBRelation') THEN 1 ELSE 0 END") = 1L

        if relationCatalogExists then
            // Edge case 1: inbound references — other collections reference this one as target.
            let inboundCount =
                connection.QueryFirst<int64>(
                    "SELECT COUNT(*) FROM SoloDBRelation WHERE TargetCollection = @name AND OwnerCollection <> @name",
                    {| name = name |})
            if inboundCount > 0L then
                raise (InvalidOperationException(
                    sprintf "Error: Cannot drop collection '%s'.\nReason: It is referenced by active relations.\nFix: Remove relations first, then drop the collection." name))

            // Edge case 2: outbound-only references — this collection owns relations. Clean up link tables + catalog rows.
            let outboundRows =
                connection.Query<{| Name: string |}>(
                    "SELECT Name FROM SoloDBRelation WHERE OwnerCollection = @name",
                    {| name = name |})
                |> Seq.toArray
            for row in outboundRows do
                let linkTable = "SoloDBRelLink_" + row.Name
                connection.Execute(sprintf "DROP TABLE IF EXISTS \"%s\"" linkTable) |> ignore
            if outboundRows.Length > 0 then
                connection.Execute("DELETE FROM SoloDBRelation WHERE OwnerCollection = @name", {| name = name |}) |> ignore

        dropTriggersForTable name connection
        connection.Execute(sprintf "DROP TABLE IF EXISTS \"%s\"" name) |> ignore
        connection.Execute("DELETE FROM SoloDBTypeCollectionMap WHERE CollectionName = @name", {|name = name|}) |> ignore
        connection.Execute("DELETE FROM SoloDBCollections Where Name = @name", {|name = name|}) |> ignore
        // Drop/recreate must rebuild relation descriptors from a clean cache snapshot.
        RelationsTypes.relationSpecsCache.Clear()

    /// <summary>
    /// Inserts a JSON string into a specified table, with options for replacement and explicit ID.
    /// </summary>
    /// <param name="orReplace">If true, uses 'INSERT OR REPLACE'.</param>
    /// <param name="id">An optional ID to insert. If None, an ID is generated by SQLite.</param>
    /// <param name="name">The name of the target table (collection).</param>
    /// <param name="json">The JSON string to insert.</param>
    /// <param name="connection">The active SQLite connection.</param>
    /// <returns>The ID of the inserted row.</returns>
    let private insertJson (orReplace: bool) (id: int64 option) (name: string) (json: string) (connection: SqliteConnection) =
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

        let parameters: obj = 
            if includeId then {|
                name = name
                jsonText = json
                id = id.Value
            |}
            else {|
                name = name
                jsonText = json
            |}

        connection.QueryFirst<int64>(queryString, parameters)

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
    let private insertImpl<'T when 'T :> obj> (typed: bool) (item: 'T) (connection: SqliteConnection) (name: string) (orReplace: bool) (collection: ISoloDBCollection<'T>) =
        let customIdGen = CustomTypeId<'T>.Value
        let existsWritebleDirectId = HasTypeId<'T>.Value


        match customIdGen with
        | Some x ->
            let oldId = x.GetId item
            match x.Generator with
            | :? SoloDatabase.Attributes.IIdGenerator as generator ->
                if generator.IsEmpty oldId then
                    let id = generator.GenerateId collection item
                    x.SetId id item
            | :? SoloDatabase.Attributes.IIdGenerator<'T> as generator ->
                if generator.IsEmpty oldId then
                    let id = generator.GenerateId collection item
                    x.SetId id item
            | other -> raise (InvalidOperationException(
                sprintf "Error: Invalid Id generator type.\nReason: Type '%s' is not supported.\nFix: Use a supported Id generator or configure a custom Id strategy." (other.GetType().ToString())))
        | None -> ()
        
        let json = if typed then toTypedJson item else toJson item
        

        let id =
            if existsWritebleDirectId && -1L >= HasTypeId<'T>.Read item then 
                raise (InvalidOperationException
                    "Error: Invalid Id value.\nReason: Id must be 0 for auto-generated ids or > 0 for explicit ids.\nFix: Use Id=0 for new entities or a positive Id for explicit insert.")
            elif existsWritebleDirectId && 0L <> HasTypeId<'T>.Read item then 
                // Inserting with Id
                insertJson orReplace (Some (HasTypeId<'T>.Read item)) name json connection
            else
                // Inserting without Id
                insertJson orReplace None name json connection

        if existsWritebleDirectId then
            HasTypeId<'T>.Write item id

        id

    /// <summary>
    /// Inserts an item into a collection.
    /// </summary>
    let inline internal insertInner (typed: bool) (item: 'T) (connection: SqliteConnection) (name: string) (collection: ISoloDBCollection<'T>) =
        insertImpl typed item connection name false collection

    /// <summary>
    /// Inserts or replaces an item in a collection.
    /// </summary>
    let inline internal insertOrReplaceInner (typed: bool) (item: 'T) (connection: SqliteConnection) (name: string) (collection: ISoloDBCollection<'T>) =
        insertImpl typed item connection name true collection

    /// <summary>
    /// Sanitizes a collection name to prevent SQL injection and ensure it's a valid table name.
    /// </summary>
    /// <param name="name">The raw collection name.</param>
    /// <returns>A sanitized name containing only letters, digits, and underscores.</returns>
    let internal formatName (name: string) =
        Utils.formatName (name)

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
    /// </summary>
    /// <returns>An array of tuples, each containing the PropertyInfo and the IndexedAttribute instance.</returns>
    let internal getIndexesFields<'a>() =
        // Get all serializable properties
        typeof<'a>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.choose(
            fun p -> // That have the IndexedAttribute
                match p.GetCustomAttribute<SoloDatabase.Attributes.IndexedAttribute>(true) with
                | a when isNull a -> None
                | a -> Some(p, a)) // With its uniqueness information.

    /// <summary>
    /// Translates a LINQ expression into an SQL index definition and a sanitized index name.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <param name="expression">The LINQ expression defining the index key(s).</param>
    /// <returns>A tuple containing the generated index name and the SQL for the indexed columns.</returns>
    let internal getIndexWhereAndName<'T, 'R> (name: string) (expression: Expression<System.Func<'T, 'R>>) =
        if isNull expression then raise (ArgumentNullException(nameof(expression)))

        let isDirectIdAccess =
            match expression.Body with
            | :? MemberExpression as me ->
                me.Member.Name = "Id" &&
                match me.Expression with
                | :? ParameterExpression -> true
                | _ -> false
            | _ -> false

        if isDirectIdAccess then raise (ArgumentException "The Id of a collection is always stored in an index.")

        let whereSQL, variables = QueryTranslator.translate name expression
        let whereSQL = whereSQL.Replace($"\"{name}\".", "") // Table-qualified references are not allowed in index expressions.
        if variables.Count > 0 then raise (ArgumentException "Cannot have variables in index.")
        if whereSQL.Contains "SELECT" then
            raise (ArgumentException "Cannot index a relation expression that resolves through link tables (e.g. DBRefMany.Count, DBRef.Value.Property). Only direct column expressions and DBRef.Id are supported.")
        let expressionBody = expression.Body

        if QueryTranslator.isAnyConstant expressionBody then raise(InvalidOperationException "Cannot index an outside or constant expression.")

        let whereSQL =
            match expressionBody with
            | :? NewExpression as ne when isTuple ne.Type
                -> whereSQL.Substring("json_array".Length)
            | :? MethodCallExpression
            | :? MemberExpression ->
                $"({whereSQL})"
            | other -> raise (ArgumentException (sprintf "Cannot index an expression with type: %s" (other.GetType().FullName)))

        let expressionStr = whereSQL.ToCharArray() |> Seq.filter(fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"
        indexName, whereSQL

    /// <summary>
    /// Ensures a non-unique index exists for the given expression.
    /// </summary>
    let internal ensureIndex<'T, 'R> (collectionName: string) (conn: SqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndName<'T,'R> collectionName expression

        let indexSQL = $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"

        conn.Execute(indexSQL)

    /// <summary>
    /// Ensures a unique index exists for the given expression.
    /// </summary>
    let internal ensureUniqueAndIndex<'T,'R> (collectionName: string) (conn: SqliteConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndName<'T,'R> collectionName expression

        let indexSQL = $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"

        conn.Execute(indexSQL)

    /// <summary>
    /// Ensures that all indexes declared via the [Indexed] attribute on the type 'T exist.
    /// </summary>
    let internal ensureDeclaredIndexesFields<'T> (name: string) (conn: SqliteConnection) =
        for (pi, indexed) in getIndexesFields<'T>() do
            let ensureIndexesFn = if indexed.Unique then ensureUniqueAndIndex else ensureIndex
            let _code = ensureIndexesFn name conn (ExpressionHelper.get<obj, obj>(fun row -> row.Dyn<obj>(pi.Name)))
            ()

    let internal getSQLForTriggersForTable (name: string) =
        HelperSchema.getSQLForTriggersForTable name

    let internal createTriggersForTable (name: string) (conn: SqliteConnection) =
        HelperSchema.createTriggersForTable name conn

    let internal createTableInner<'T> (name: string) (conn: SqliteConnection) =
        HelperSchema.createTableInner<'T> name conn

    /// <summary>
    /// Internal DTO to pass data from the main DB instance to collection instances.
    /// </summary>
    let internal collectionNameOf<'T> =
        Utils.collectionNameOf<'T>()
