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
/// Caches event-context database resources to avoid repeated allocations and redundant metadata queries.
/// </summary>
type internal EventDbCache(connectionString: string, directConnection: SqliteConnection, guardedConnection: Connection, parentData: SoloDBToCollectionData, knownCollectionName: string) =
    let knownCollectionName = Helper.formatName knownCollectionName
    let mutable knownCollectionExists = true
    let maxCachedCollections = 4
    let cachedCollections = Array.zeroCreate<obj> maxCachedCollections
    let cachedCollectionNames = Array.zeroCreate<string> maxCachedCollections
    let cachedCollectionTypes = Array.zeroCreate<Type> maxCachedCollections
    let mutable cachedCount = 0
    let mutable cachedFileSystem: IFileSystem | null = null

    member private this.TryGetCached<'U>(collectionName: string) =
        let targetType = typeof<'U>
        let mutable found = Unchecked.defaultof<ISoloDBCollection<'U> | null>
        let mutable i = 0
        while isNull found && i < cachedCount do
            let cached = cachedCollections.[i]
            if not (isNull cached) && cachedCollectionNames.[i] = collectionName && cachedCollectionTypes.[i] = targetType then
                found <- cached :?> ISoloDBCollection<'U>
            i <- i + 1
        if isNull found then None else Some found

    member private this.InvalidateIfCached(collectionName: string) =
        for i in 0 .. cachedCount - 1 do
            let cached = cachedCollections.[i]
            if not (isNull cached) && cachedCollectionNames.[i] = collectionName then
                cachedCollections.[i] <- null
                cachedCollectionNames.[i] <- null
                cachedCollectionTypes.[i] <- null

    member private this.TryStoreCached<'U>(collectionName: string) (collection: ISoloDBCollection<'U>) =
        let targetType = typeof<'U>
        let mutable slot = -1
        let mutable i = 0
        while slot < 0 && i < cachedCount do
            if isNull cachedCollections.[i] then
                slot <- i
            i <- i + 1

        if slot < 0 && cachedCount < maxCachedCollections then
            slot <- cachedCount
            cachedCount <- cachedCount + 1

        if slot >= 0 then
            cachedCollections.[slot] <- collection :> obj
            cachedCollectionNames.[slot] <- collectionName
            cachedCollectionTypes.[slot] <- targetType
            true
        else false

    member private this.IsCachedName(collectionName: string) =
        let mutable found = false
        let mutable i = 0
        while not found && i < cachedCount do
            let cached = cachedCollections.[i]
            if not (isNull cached) && cachedCollectionNames.[i] = collectionName then
                found <- true
            i <- i + 1
        found

    member private this.EnsureExists<'U>(collectionName: string) =
        if collectionName = knownCollectionName then
            if not knownCollectionExists then
                if not (Helper.existsCollection collectionName directConnection) then
                    Helper.createTableInner<'U> collectionName directConnection
                knownCollectionExists <- true
        else if not (Helper.existsCollection collectionName directConnection) then
            Helper.createTableInner<'U> collectionName directConnection

        Helper.registerTypeCollection<'U> collectionName directConnection

        // Validate relation topology after table creation.
        let hasRelations = RelationsSchema.getRelationSpecs typeof<'U> |> Array.isEmpty |> not
        if hasRelations then
            let relationTx: Relations.RelationTxContext = {
                Connection = directConnection
                OwnerTable = collectionName
                OwnerType = typeof<'U>
                InTransaction = true // Event context is always inside a transaction.
            }
            Relations.ensureSchemaForOwnerType relationTx typeof<'U>

    member this.FileSystem =
        if isNull cachedFileSystem then
            cachedFileSystem <- (FileSystem guardedConnection :> IFileSystem)
        cachedFileSystem

    member this.GetCollection<'U>(collectionName: string) =
        let collectionName = Helper.formatName collectionName
        if collectionName.StartsWith "SoloDB" then
            raise (ArgumentException $"The SoloDB* prefix is forbidden in Collection names.")

        match this.TryGetCached<'U>(collectionName) with
        | Some cached -> cached
        | None ->
            this.EnsureExists<'U>(collectionName)
            let collection = Collection<'U>(guardedConnection, collectionName, connectionString, parentData) :> ISoloDBCollection<'U>
            this.TryStoreCached<'U>(collectionName) collection |> ignore
            collection

    member this.CollectionExists(collectionName: string) =
        let collectionName = Helper.formatName collectionName
        if collectionName = knownCollectionName && knownCollectionExists then true
        elif this.IsCachedName(collectionName) then true
        else Helper.existsCollection collectionName directConnection

    member this.DropCollectionIfExists(collectionName: string) =
        raise (InvalidOperationException
            "Error: Dropping collections is not supported from event handlers.\nReason: Event handlers must not perform schema changes.\nFix: Drop collections outside event handlers.")

    member this.DropCollection(collectionName: string) =
        raise (InvalidOperationException
            "Error: Dropping collections is not supported from event handlers.\nReason: Event handlers must not perform schema changes.\nFix: Drop collections outside event handlers.")

/// <summary>
/// Represents a collection of documents of type 'T stored in the database. Provides methods for CRUD operations, indexing, and LINQ querying.
/// </summary>
/// <param name="connection">The connection provider.</param>
/// <param name="name">The name of the collection (table name).</param>
/// <param name="connectionString">The database connection string.</param>
/// <param name="parentData">Data from the parent SoloDB instance.</param>
and internal Collection<'T>(connection: Connection, name: string, connectionString: string, parentData: SoloDBToCollectionData) as this =
    member val private SoloDBQueryable = SoloDBCollectionQueryable<'T, 'T>(SoloDBCollectionQueryProvider(this, parentData), Expression.Constant(RootQueryable<'T>(this))) :> IOrderedQueryable<'T>
    member val private ConnectionString = connectionString
    /// <summary>Gets the name of the collection.</summary>
    member val Name = name
    /// <summary>Gets a value indicating whether the collection is operating within a transaction.</summary>
    member val InTransaction =
        match connection with
        | Transactional _ -> true
        | Guarded (_, inner) ->
            match inner with
            | Transactional _ -> true
            | _ -> false
        | Pooled _ -> false
    /// <summary>Gets a value indicating whether type information should be included during serialization for documents in this collection.</summary>
    member val IncludeType = mustIncludeTypeInformationInSerialization<'T>
    /// <summary>True when the document type declares DBRef/DBRefMany properties.</summary>
    member val private HasRelations =
        typeof<'T>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.exists (fun p -> DBRefTypeHelpers.isAnyRelationRefType p.PropertyType)

    member private this.HasIncomingRelations(connection: SqliteConnection) =
        let relationCatalogExists =
            connection.QueryFirst<int64>(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'SoloDBRelation') THEN 1 ELSE 0 END") = 1L

        relationCatalogExists &&
            connection.QueryFirst<int64>(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM SoloDBRelation WHERE TargetCollection = @name LIMIT 1) THEN 1 ELSE 0 END",
                {| name = name |}) = 1L

    member private this.RequiresRelationDeleteHandling(connection: SqliteConnection) =
        this.HasRelations || this.HasIncomingRelations(connection)

    /// <summary>Gets the event registration API for this collection.</summary>
    member val private Events: ISoloDBCollectionEvents<'T> =
        let createDb (directConnection: SqliteConnection) (guard: unit -> unit) : ISoloDB =
            let guardedConnection = Guarded(guard, Transactional directConnection)
            let cache = EventDbCache(connectionString, directConnection, guardedConnection, parentData, name)

            { new ISoloDB with
                member _.ConnectionString = connectionString
                member _.FileSystem = cache.FileSystem
                member _.GetCollection<'U>() =
                    cache.GetCollection<'U>(Helper.collectionNameOf<'U>)
                member _.GetCollection<'U>(collectionName: string) =
                    cache.GetCollection<'U>(collectionName)
                member _.GetUntypedCollection(collectionName: string) =
                    cache.GetCollection<JsonSerializator.JsonValue>(collectionName)
                member _.CollectionExists(collectionName: string) =
                    cache.CollectionExists(collectionName)
                member _.CollectionExists<'U>() =
                    cache.CollectionExists(Helper.collectionNameOf<'U>)
                member _.DropCollectionIfExists(collectionName: string) =
                    cache.DropCollectionIfExists(collectionName)
                member _.DropCollectionIfExists<'U>() =
                    cache.DropCollectionIfExists(Helper.collectionNameOf<'U>)
                member _.DropCollection(collectionName: string) =
                    cache.DropCollection(collectionName)
                member _.DropCollection<'U>() =
                    cache.DropCollection(Helper.collectionNameOf<'U>)
                member _.ListCollectionNames() =
                    directConnection.Query<string>("SELECT Name FROM SoloDBCollections")
                member _.Optimize() =
                    directConnection.Execute "PRAGMA optimize;" |> ignore
                member _.WithTransaction<'R>(_func: Func<ISoloDB, 'R>) : 'R =
                    raise (NotSupportedException(
                        "Error: Nested transactions are not supported inside event handler contexts.\n" +
                        "Reason: Event handlers execute during active SQL statements where SQLite cannot open SAVEPOINTs.\n" +
                        "Fix: Perform transactional work outside event handlers, or use the event context directly without nesting."))
                member _.WithTransaction(_func: Action<ISoloDB>) : unit =
                    raise (NotSupportedException(
                        "Error: Nested transactions are not supported inside event handler contexts.\n" +
                        "Reason: Event handlers execute during active SQL statements where SQLite cannot open SAVEPOINTs.\n" +
                        "Fix: Perform transactional work outside event handlers, or use the event context directly without nesting."))
                member _.WithTransactionAsync<'R>(_func: Func<ISoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> =
                    raise (NotSupportedException(
                        "Error: Nested transactions are not supported inside event handler contexts.\n" +
                        "Reason: Event handlers execute during active SQL statements where SQLite cannot open SAVEPOINTs.\n" +
                        "Fix: Perform transactional work outside event handlers, or use the event context directly without nesting."))
                member _.WithTransactionAsync(_func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task =
                    raise (NotSupportedException(
                        "Error: Nested transactions are not supported inside event handler contexts.\n" +
                        "Reason: Event handlers execute during active SQL statements where SQLite cannot open SAVEPOINTs.\n" +
                        "Fix: Perform transactional work outside event handlers, or use the event context directly without nesting."))
                member _.Dispose() = ()
            }

        CollectionEventSystem<'T>(
            name,
            parentData.EventSystem,
            createDb)

    /// <summary>Gets the internal connection provider for this collection.</summary>
    member val internal Connection = connection

    /// Wraps an operation in a relation-safe auto-transaction when not already in one.
    /// Routes through Connection.WithTransaction (hybrid: BEGIN IMMEDIATE top-level, SAVEPOINT nested).
    /// Used by Insert, Update, Replace, and batch methods that need relation atomicity.
    member private this.WithRelationAutoTx (f: Collection<'T> -> 'R) : 'R =
        connection.WithTransaction(fun conn ->
            let directConnection = Transactional conn
            let transientCollection = Collection<'T>(directConnection, name, connectionString, parentData)
            f transientCollection)

    /// <summary>
    /// Inserts a new document into the collection.
    /// </summary>
    /// <param name="item">The document to insert.</param>
    /// <returns>The ID of the newly inserted document.</returns>
    member this.Insert (item: 'T) =
        if this.HasRelations then
            connection.WithTransaction(fun conn ->
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>
                let plan = Relations.prepareInsert tx (box item)
                let id = Helper.insertInner this.IncludeType item conn name this
                Relations.syncInsert tx id plan
                id
            )
        else
            use conn = connection.Get()
            Helper.insertInner this.IncludeType item conn name this

    /// <summary>
    /// Inserts a new document or replaces an existing one if a document with the same ID already exists.
    /// </summary>
    /// <param name="item">The document to insert or replace.</param>
    /// <returns>The ID of the inserted or replaced document.</returns>
    member this.InsertOrReplace (item: 'T) =
        if this.HasRelations then
            connection.WithTransaction(fun conn ->
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>

                let oldRow =
                    if HasTypeId<'T>.Value then
                        let id = HasTypeId<'T>.Read item
                        if id > 0L then
                            match conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {| id = id |}) with
                            | row when Object.ReferenceEquals(row, null) -> ValueNone
                            | row -> ValueSome row
                        else ValueNone
                    else
                        match CustomTypeId<'T>.Value with
                        | Some customId ->
                            let idValue = customId.GetId(item |> box)
                            let idProp = customId.Property
                            let filter, variables = QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<obj>(idProp) = idValue))
                            match conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter} LIMIT 1", variables) with
                            | row when Object.ReferenceEquals(row, null) -> ValueNone
                            | row -> ValueSome row
                        | None -> ValueNone

                let oldOwner, oldOwnerId =
                    match oldRow with
                    | ValueSome row -> ValueSome (fromSQLite<'T> row |> box), ValueSome row.Id.Value
                    | ValueNone -> ValueNone, ValueNone

                match oldOwnerId with
                | ValueSome ownerId -> Relations.applyOwnerDeletePolicies tx name ownerId
                | ValueNone -> ()

                let plan = Relations.prepareUpsert tx oldOwner (box item)
                let id = Helper.insertOrReplaceInner this.IncludeType item conn name this
                Relations.syncUpsert tx id plan
                id
            )
        else
            use conn = connection.Get()
            Helper.insertOrReplaceInner this.IncludeType item conn name this

    /// <summary>
    /// Inserts a sequence of documents in a single transaction for efficiency.
    /// </summary>
    /// <param name="items">The sequence of documents to insert.</param>
    /// <returns>A list of IDs for the newly inserted documents.</returns>
    member this.InsertBatch (items: 'T seq) =
        if isNull items then raise (ArgumentNullException(nameof(items)))

        this.WithRelationAutoTx (fun transientCollection ->
            let connection = transientCollection.Connection.Get()
            let tx =
                if this.HasRelations then
                    let x: Relations.RelationTxContext = {
                        Connection = connection
                        OwnerTable = name
                        OwnerType = typeof<'T>
                        InTransaction = true
                    }
                    Relations.ensureSchemaForOwnerType x typeof<'T>
                    ValueSome x
                else ValueNone

            let ids = List<int64>()
            for item in items do
                match tx with
                | ValueSome tx ->
                    let plan = Relations.prepareInsert tx (box item)
                    let id = Helper.insertInner this.IncludeType item connection name transientCollection
                    Relations.syncInsert tx id plan
                    ids.Add id
                | ValueNone ->
                    Helper.insertInner this.IncludeType item connection name transientCollection |> ids.Add
            ids)

    /// <summary>
    /// Inserts or replaces a sequence of documents in a single transaction.
    /// </summary>
    /// <param name="items">The sequence of documents to insert or replace.</param>
    /// <returns>A list of IDs for the inserted or replaced documents.</returns>
    member this.InsertOrReplaceBatch (items: 'T seq) =
        if isNull items then raise (ArgumentNullException(nameof(items)))

        this.WithRelationAutoTx (fun transientCollection ->
            let connection = transientCollection.Connection.Get()
            let tx =
                if this.HasRelations then
                    let x: Relations.RelationTxContext = {
                        Connection = connection
                        OwnerTable = name
                        OwnerType = typeof<'T>
                        InTransaction = true
                    }
                    Relations.ensureSchemaForOwnerType x typeof<'T>
                    ValueSome x
                else ValueNone

            let ids = List<int64>()
            for item in items do
                match tx with
                | ValueSome tx ->
                    let oldRow =
                        if HasTypeId<'T>.Value then
                            let id = HasTypeId<'T>.Read item
                            if id > 0L then
                                match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {| id = id |}) with
                                | row when Object.ReferenceEquals(row, null) -> ValueNone
                                | row -> ValueSome row
                            else ValueNone
                        else
                            match CustomTypeId<'T>.Value with
                            | Some customId ->
                                let idValue = customId.GetId(item |> box)
                                let idProp = customId.Property
                                let filter, variables = QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<obj>(idProp) = idValue))
                                match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter} LIMIT 1", variables) with
                                | row when Object.ReferenceEquals(row, null) -> ValueNone
                                | row -> ValueSome row
                            | None -> ValueNone

                    let oldOwner, oldOwnerId =
                        match oldRow with
                        | ValueSome row -> ValueSome (fromSQLite<'T> row |> box), ValueSome row.Id.Value
                        | ValueNone -> ValueNone, ValueNone

                    match oldOwnerId with
                    | ValueSome ownerId -> Relations.applyOwnerDeletePolicies tx name ownerId
                    | ValueNone -> ()

                    let plan = Relations.prepareUpsert tx oldOwner (box item)
                    let id = Helper.insertOrReplaceInner this.IncludeType item connection name transientCollection
                    Relations.syncUpsert tx id plan
                    ids.Add id
                | ValueNone ->
                    Helper.insertOrReplaceInner this.IncludeType item connection name transientCollection |> ids.Add
            ids)

    /// <summary>
    /// Tries to find a document by its 64-bit integer ID.
    /// </summary>
    /// <param name="id">The ID of the document to find.</param>
    /// <returns>An option containing the document if found; otherwise, None.</returns>
    member this.TryGetById(id: int64) =
        use connection = connection.Get()
        match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {|id = id|}) with
        | json when Object.ReferenceEquals(json, null) -> None
        | json ->
            let entity = fromSQLite<'T> json
            // Batch-load Single and Many relation properties for full entity hydration.
            if this.HasRelations then
                let excludedPaths = System.Collections.Generic.HashSet<string>(StringComparer.Ordinal)
                let includedPaths = System.Collections.Generic.HashSet<string>(StringComparer.Ordinal)
                Relations.batchLoadDBRefProperties connection name typeof<'T> excludedPaths includedPaths [| (id, box entity) |]
                Relations.batchLoadDBRefManyProperties connection name typeof<'T> excludedPaths includedPaths [| (id, box entity) |] this.InTransaction
                Relations.captureRelationVersionForEntities connection name [| (id, box entity) |]
            entity |> Some

    /// <summary>
    /// Finds a document by its 64-bit integer ID.
    /// </summary>
    /// <param name="id">The ID of the document to find.</param>
    /// <returns>The document with the specified ID.</returns>
    /// <exception cref="KeyNotFoundException">Thrown if no document with the specified ID is found.</exception>
    member this.GetById(id: int64) =
        match this.TryGetById id with
        | None -> raise (KeyNotFoundException (sprintf "There is no element with id '%i' inside collection '%s'" id name)) 
        | Some x -> x

    /// <summary>
    /// Deletes a document by its 64-bit integer ID.
    /// </summary>
    /// <param name="id">The ID of the document to delete.</param>
    /// <returns>The number of documents deleted (0 or 1).</returns>
    member this.DeleteById(id: int64) =
        connection.WithTransaction(fun conn ->
            let requiresRelationHandling = this.RequiresRelationDeleteHandling(conn)
            if requiresRelationHandling then
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>
                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {| id = id |})
                if isNull oldRow then
                    0
                else
                    let owner = fromSQLite<'T> oldRow
                    let deletePlan = Relations.prepareDeleteOwner tx id (box owner)
                    Relations.syncDeleteOwner tx deletePlan
                    conn.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {|id = id|})
            else
                conn.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {|id = id|})
        )

    /// <summary>
    /// Tries to find a document by its custom ID, as defined by the [Id] attribute on the document type.
    /// </summary>
    /// <typeparam name="'IdType">The type of the custom ID.</typeparam>
    /// <param name="id">The custom ID of the document to find.</param>
    /// <returns>An option containing the document if found; otherwise, None.</returns>
    member this.TryGetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T option =
        // Use this function only if you know that 'IdType = 'R.
        let inline genericReinterpret (a: 'IdType) = Unsafe.As<'IdType, 'R>(&Unsafe.AsRef(&a))
        let inline int64FromUInt64OrThrow (value: uint64) =
            if value > uint64 Int64.MaxValue then
                raise (ArgumentOutOfRangeException(nameof id, "Identifier is out of range for Int64."))
            int64 value
        let tryGetByCustomId() =
            let custom = CustomTypeId<'T>.Value
            let idProp =
                match custom with
                | Some c -> c.Property
                | None -> raise (InvalidOperationException("This collection has no custom [Id] property. Use the Int64 Id overload."))
            let filter, variables = QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<'IdType>(idProp) = id))
            use connection = connection.Get()
            match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter} LIMIT 1", variables) with
            | json when Object.ReferenceEquals(json, null) -> None
            | json ->
                let entity = fromSQLite<'T> json
                if this.HasRelations then
                    let excludedPaths = System.Collections.Generic.HashSet<string>(StringComparer.Ordinal)
                    let includedPaths = System.Collections.Generic.HashSet<string>(StringComparer.Ordinal)
                    Relations.batchLoadDBRefProperties connection name typeof<'T> excludedPaths includedPaths [| (json.Id.Value, box entity) |]
                    Relations.batchLoadDBRefManyProperties connection name typeof<'T> excludedPaths includedPaths [| (json.Id.Value, box entity) |] this.InTransaction
                    Relations.captureRelationVersionForEntities connection name [| (json.Id.Value, box entity) |]
                entity |> Some

        // Primitive fallback should only run for direct int64 Id collections.
        if HasTypeId<'T>.Value then
            match typeof<'IdType> with
            // x.Equals is faster than structural comparison (=)
            | x when x.Equals typeof<int8> ->
                let id: int8 = genericReinterpret id
                this.TryGetById(int64 id)

            | x when x.Equals typeof<int16> ->
                let id: int16 = genericReinterpret id
                this.TryGetById(int64 id)

            | x when x.Equals typeof<int32> ->
                let id: int32 = genericReinterpret id
                this.TryGetById(int64 id)

            | x when x.Equals typeof<nativeint> ->
                let id: nativeint = genericReinterpret id
                this.TryGetById(int64 id)

            | x when x.Equals typeof<int64> ->
                let id: int64 = genericReinterpret id
                this.TryGetById(id)

            | x when x.Equals typeof<uint8> ->
                let id: uint8 = genericReinterpret id
                this.TryGetById(int64 id)

            | x when x.Equals typeof<uint16> ->
                let id: uint16 = genericReinterpret id
                this.TryGetById(int64 id)

            | x when x.Equals typeof<uint32> ->
                let id: uint32 = genericReinterpret id
                this.TryGetById(int64 id)

            | x when x.Equals typeof<unativeint> ->
                let id: unativeint = genericReinterpret id
                this.TryGetById(id |> uint64 |> int64FromUInt64OrThrow)

            | x when x.Equals typeof<uint64> ->
                let id: uint64 = genericReinterpret id
                this.TryGetById(id |> int64FromUInt64OrThrow)

            | _ ->
                tryGetByCustomId()
        else
            tryGetByCustomId()

    /// <summary>
    /// Finds a document by its custom ID, as defined by the [Id] attribute on the document type.
    /// </summary>
    /// <typeparam name="'IdType">The type of the custom ID.</typeparam>
    /// <param name="id">The custom ID of the document to find.</param>
    /// <returns>The document with the specified ID.</returns>
    /// <exception cref="KeyNotFoundException">Thrown if no document with the specified ID is found.</exception>
    member this.GetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T =
        match this.TryGetById id with
        | None -> raise (KeyNotFoundException (sprintf "There is no element with id '%A' inside collection '%s'" id name)) 
        | Some x -> x

    /// <summary>
    /// Deletes a document by its custom ID, as defined by the [Id] attribute on the document type.
    /// </summary>
    /// <typeparam name="'IdType">The type of the custom ID.</typeparam>
    /// <param name="id">The custom ID of the document to delete.</param>
    /// <returns>The number of documents deleted.</returns>
    member this.DeleteById<'IdType when 'IdType : equality>(id: 'IdType) : int =
        // Use this function only if you know that 'IdType = 'R.
        let inline genericReinterpret (a: 'IdType) = Unsafe.As<'IdType, 'R>(&Unsafe.AsRef(&a))
        let inline int64FromUInt64OrThrow (value: uint64) =
            if value > uint64 Int64.MaxValue then
                raise (ArgumentOutOfRangeException(nameof id, "Identifier is out of range for Int64."))
            int64 value
        let deleteByCustomId() =
            let custom = CustomTypeId<'T>.Value
            let idProp =
                match custom with
                | Some c -> c.Property
                | None -> raise (InvalidOperationException("This collection has no custom [Id] property. Use the Int64 Id overload."))
            let filter, variables = QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<'IdType>(idProp) = id))
            connection.WithTransaction(fun conn ->
                let requiresRelationHandling = this.RequiresRelationDeleteHandling(conn)
                if requiresRelationHandling then
                    let tx: Relations.RelationTxContext = {
                        Connection = conn
                        OwnerTable = name
                        OwnerType = typeof<'T>
                        InTransaction = true
                    }
                    Relations.ensureSchemaForOwnerType tx typeof<'T>

                    let oldRow = conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter} LIMIT 1", variables)
                    if isNull oldRow then
                        0
                    else
                        let owner = fromSQLite<'T> oldRow
                        let deletePlan = Relations.prepareDeleteOwner tx oldRow.Id.Value (box owner)
                        Relations.syncDeleteOwner tx deletePlan
                        conn.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {| id = oldRow.Id.Value |})
                else
                    conn.Execute ($"DELETE FROM \"{name}\" WHERE {filter}", variables)
            )

        // Primitive fallback should only run for direct int64 Id collections.
        if HasTypeId<'T>.Value then
            match typeof<'IdType> with
            // x.Equals is faster than structural comparison (=)
            | x when x.Equals typeof<int8> ->
                let id: int8 = genericReinterpret id
                this.DeleteById(int64 id)

            | x when x.Equals typeof<int16> ->
                let id: int16 = genericReinterpret id
                this.DeleteById(int64 id)

            | x when x.Equals typeof<int32> ->
                let id: int32 = genericReinterpret id
                this.DeleteById(int64 id)

            | x when x.Equals typeof<nativeint> ->
                let id: nativeint = genericReinterpret id
                this.DeleteById(int64 id)

            | x when x.Equals typeof<int64> ->
                let id: int64 = genericReinterpret id
                this.DeleteById(id)

            | x when x.Equals typeof<uint8> ->
                let id: uint8 = genericReinterpret id
                this.DeleteById(int64 id)

            | x when x.Equals typeof<uint16> ->
                let id: uint16 = genericReinterpret id
                this.DeleteById(int64 id)

            | x when x.Equals typeof<uint32> ->
                let id: uint32 = genericReinterpret id
                this.DeleteById(int64 id)

            | x when x.Equals typeof<unativeint> ->
                let id: unativeint = genericReinterpret id
                this.DeleteById(id |> uint64 |> int64FromUInt64OrThrow)

            | x when x.Equals typeof<uint64> ->
                let id: uint64 = genericReinterpret id
                this.DeleteById(id |> int64FromUInt64OrThrow)

            | _ ->
                deleteByCustomId()
        else
            deleteByCustomId()

    /// <summary>
    /// Updates an existing document in the collection. The document must have a valid ID.
    /// </summary>
    /// <param name="item">The document with updated values.</param>
    /// <exception cref="KeyNotFoundException">Thrown if no document with the item's ID is found.</exception>
    /// <exception cref="InvalidOperationException">Thrown if the document type does not have a recognizable ID property.</exception>
    member this.Update(item: 'T) =
        let filter, variables =
            if HasTypeId<'T>.Value then
                let id = HasTypeId<'T>.Read item
                QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<int64>("Id") = id))
            else match CustomTypeId<'T>.Value with
                 | Some customId ->
                    let id = customId.GetId (item |> box)
                    let idProp = CustomTypeId<'T>.Value.Value.Property
                    QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<obj>(idProp) = id))
                 | None ->
                    raise (InvalidOperationException
                        $"Error: Item type {typeof<'T>.Name} has no int64 Id or custom Id.\nReason: Updates require a stable identifier.\nFix: Add an int64 Id property or configure a custom Id strategy.")

        if this.HasRelations then
            connection.WithTransaction(fun conn ->
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>

                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter} LIMIT 1", variables)
                if isNull oldRow then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id.")

                let oldOwner = fromSQLite<'T> oldRow
                let excludedPaths = System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal)
                let includedPaths = System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal)
                Relations.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths [| (oldRow.Id.Value, box oldOwner) |] true
                let writePlan = Relations.prepareUpdate tx oldRow.Id.Value (box oldOwner) (box item)

                variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item
                let count = conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE " + filter, variables)
                if count <= 0 then
                    raise (KeyNotFoundException "Could not Update any entities with specified Id.")

                Relations.syncUpdate tx oldRow.Id.Value writePlan
            )
        else
            use conn = connection.Get()
            variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item
            let count = conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE " + filter, variables)
            if count <= 0 then
                raise (KeyNotFoundException "Could not Update any entities with specified Id.")

    /// <summary>
    /// Deletes all documents that match the specified filter.
    /// </summary>
    /// <param name="filter">A LINQ expression to filter the documents to delete.</param>
    /// <returns>The number of documents deleted.</returns>
    member this.DeleteMany(filter: Expression<Func<'T, bool>>) =
        if isNull filter then raise (ArgumentNullException(nameof(filter)))
        connection.WithTransaction(fun conn ->
            let requiresRelationHandling = this.RequiresRelationDeleteHandling(conn)
            let filter, variables = QueryTranslator.translate name filter

            if requiresRelationHandling then
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>

                let rows = conn.Query<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter}", variables) |> Seq.toArray
                if rows.Length = 0 then
                    0
                else
                    for row in rows do
                        let owner = fromSQLite<'T> row
                        let ownerId = row.Id.Value
                        let deletePlan = Relations.prepareDeleteOwner tx ownerId (box owner)
                        Relations.syncDeleteOwner tx deletePlan

                    let deleteVars = Dictionary<string, obj>(rows.Length)
                    let ids = ResizeArray<string>(rows.Length)
                    for i in 0 .. rows.Length - 1 do
                        let key = $"id{i}"
                        ids.Add("@" + key)
                        deleteVars.[key] <- rows.[i].Id.Value :> obj

                    let idList = String.Join(",", ids)
                    let sql = $"DELETE FROM \"{name}\" WHERE Id IN ({idList})"
                    conn.Execute(sql, deleteVars)
            else
                conn.Execute ($"DELETE FROM \"{name}\" WHERE " + filter, variables)
        )

    /// <summary>
    /// Deletes the first document that matches the specified filter.
    /// </summary>
    /// <param name="filter">A LINQ expression to filter the document to delete.</param>
    /// <returns>The number of documents deleted (0 or 1).</returns>
    member this.DeleteOne(filter: Expression<Func<'T, bool>>) =
        if isNull filter then raise (ArgumentNullException(nameof(filter)))
        connection.WithTransaction(fun conn ->
            let requiresRelationHandling = this.RequiresRelationDeleteHandling(conn)
            let filter, variables = QueryTranslator.translate name filter

            if requiresRelationHandling then
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>

                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE ({filter}) LIMIT 1", variables)
                if isNull oldRow then
                    0
                else
                    let owner = fromSQLite<'T> oldRow
                    let deletePlan = Relations.prepareDeleteOwner tx oldRow.Id.Value (box owner)
                    Relations.syncDeleteOwner tx deletePlan
                    conn.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {| id = oldRow.Id.Value |})
            else
                conn.Execute ($"DELETE FROM \"{name}\" WHERE Id in (SELECT Id FROM \"{name}\" WHERE ({filter}) LIMIT 1)", variables)
        )

    /// <summary>
    /// Replaces the content of all documents matching the filter with a new document.
    /// </summary>
    /// <param name="item">The new document content to use for replacement.</param>
    /// <param name="filter">A LINQ expression to filter the documents to replace.</param>
    /// <returns>The number of documents replaced.</returns>
    member this.ReplaceMany(item: 'T)(filter: Expression<Func<'T, bool>>) =
        if isNull filter then raise (ArgumentNullException(nameof(filter)))
        let filter, variables = QueryTranslator.translate name filter
        if this.HasRelations then
            connection.WithTransaction(fun conn ->
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>

                let oldRows = conn.Query<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter}", variables) |> Seq.toArray
                if oldRows.Length = 0 then
                    0
                else
                    let oldOwners = oldRows |> Array.map (fun row -> fromSQLite<'T> row |> box)
                    let ownerIds = oldRows |> Array.map (fun row -> row.Id.Value)
                    let excludedPaths = System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal)
                    let includedPaths = System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal)
                    let ownerPairs = Array.zip ownerIds oldOwners
                    Relations.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths ownerPairs true
                    Relations.syncReplaceMany tx (ownerIds :> seq<_>) (oldOwners :> seq<_>) (box item)
                    variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item
                    conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE " + filter, variables)
            )
        else
            use conn = connection.Get()
            variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item
            conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE " + filter, variables)

    /// <summary>
    /// Replaces the content of the first document matching the filter with a new document.
    /// </summary>
    /// <param name="item">The new document content to use for replacement.</param>
    /// <param name="filter">A LINQ expression to filter the document to replace.</param>
    /// <returns>The number of documents replaced (0 or 1).</returns>
    member this.ReplaceOne(item: 'T)(filter: Expression<Func<'T, bool>>) =
        if isNull filter then raise (ArgumentNullException(nameof(filter)))
        let filter, variables = QueryTranslator.translate name filter
        if this.HasRelations then
            connection.WithTransaction(fun conn ->
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>

                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE ({filter}) LIMIT 1", variables)
                if isNull oldRow then
                    0
                else
                    let oldOwner = fromSQLite<'T> oldRow
                    let excludedPaths = System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal)
                    let includedPaths = System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal)
                    Relations.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths [| (oldRow.Id.Value, box oldOwner) |] true
                    Relations.syncReplaceOne tx oldRow.Id.Value (box oldOwner) (box item)
                    variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item
                    conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE Id = @id", {| item = variables.["item"]; id = oldRow.Id.Value |})
            )
        else
            use conn = connection.Get()
            variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item
            conn.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE Id in (SELECT Id FROM \"{name}\" WHERE ({filter}) LIMIT 1)", variables)

    /// <summary>
    /// Applies a set of transformations to update all documents that match the specified filter.
    /// </summary>
    /// <param name="transform">An array of expressions defining the updates to apply.</param>
    /// <param name="filter">A LINQ expression to filter the documents to update.</param>
    /// <returns>The number of documents updated.</returns>
    member this.UpdateMany(transform: Expression<System.Action<'T>> array)(filter: Expression<Func<'T, bool>>) =
        let transform = nullArgCheck (nameof transform) transform
        let filter = nullArgCheck (nameof filter) filter
        match transform.Length with
        | 0 -> 0 // If no transformations provided.
        | _ ->

        let relationTransforms = ResizeArray<QueryTranslatorBase.UpdateManyRelationTransform>()
        let jsonTransforms = ResizeArray<Expression<System.Action<'T>>>()

        for expression in transform do
            match QueryTranslatorVisitPost.tryTranslateUpdateManyRelationTransform expression with
            | ValueSome op -> relationTransforms.Add op
            | ValueNone -> jsonTransforms.Add expression

        let executeJsonUpdates (conn: SqliteConnection) =
            if jsonTransforms.Count = 0 then
                0
            else
                let variables = Dictionary<string, obj>()
                let fullSQL = StringBuilder()
                let inline append (txt: string) = ignore (fullSQL.Append txt)

                append "UPDATE \""
                append name
                append "\" SET Value = jsonb_set(Value, "

                for expression in jsonTransforms do
                    QueryTranslator.translateUpdateMode name expression fullSQL variables

                fullSQL.Remove(fullSQL.Length - 1, 1) |> ignore // Remove the ',' at the end.

                append ")  WHERE "
                QueryTranslator.translateQueryable name filter fullSQL variables

                conn.Execute (fullSQL.ToString(), variables)

        if this.HasRelations then
            connection.WithTransaction(fun conn ->
                let tx: Relations.RelationTxContext = {
                    Connection = conn
                    OwnerTable = name
                    OwnerType = typeof<'T>
                    InTransaction = true
                }
                Relations.ensureSchemaForOwnerType tx typeof<'T>

                let relationRows =
                    if relationTransforms.Count = 0 then
                        Array.empty
                    else
                        let filterSql, filterVars = QueryTranslator.translate name filter
                        conn.Query<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filterSql}", filterVars) |> Seq.toArray

                let mutable affected = executeJsonUpdates conn

                if relationTransforms.Count > 0 then
                    let mappedOps =
                        relationTransforms
                        |> Seq.map (function
                            | QueryTranslatorBaseTypes.SetDBRefToId(path, targetType, targetId) -> RelationsTypes.SetDBRefToId(path, targetType, targetId)
                            | QueryTranslatorBaseTypes.SetDBRefToTypedId(path, targetType, targetIdType, targetTypedId) -> RelationsTypes.SetDBRefToTypedId(path, targetType, targetIdType, targetTypedId)
                            | QueryTranslatorBaseTypes.SetDBRefToNone(path, targetType) -> RelationsTypes.SetDBRefToNone(path, targetType)
                            | QueryTranslatorBaseTypes.AddDBRefMany(path, targetType, targetId) -> RelationsTypes.AddDBRefMany(path, targetType, targetId)
                            | QueryTranslatorBaseTypes.RemoveDBRefMany(path, targetType, targetId) -> RelationsTypes.RemoveDBRefMany(path, targetType, targetId)
                            | QueryTranslatorBaseTypes.ClearDBRefMany(path, targetType) -> RelationsTypes.ClearDBRefMany(path, targetType))
                        |> Seq.toList

                    for row in relationRows do
                        let writePlan: Relations.RelationWritePlan = {
                            Kind = RelationsTypes.RelationPlanKind.UpdateMany
                            OwnerType = typeof<'T>
                            Ops = mappedOps
                        }
                        Relations.syncUpdate tx row.Id.Value writePlan

                    if jsonTransforms.Count = 0 then
                        affected <- relationRows.Length

                affected
            )
        else
            use conn = connection.Get()
            executeJsonUpdates conn

    /// <summary>
    /// Ensures that a non-unique index exists for the specified expression. Creates the index if it does not exist.
    /// </summary>
    /// <typeparam name="'R">The type of the indexed property.</typeparam>
    /// <param name="expression">A LINQ expression that selects the property to be indexed.</param>
    member this.EnsureIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        use connection = connection.Get()
        Helper.ensureIndex name connection expression

    /// <summary>
    /// Ensures that a unique index exists for the specified expression. Creates the index if it does not exist.
    /// </summary>
    /// <typeparam name="'R">The type of the indexed property.</typeparam>
    /// <param name="expression">A LINQ expression that selects the property to be indexed.</param>
    member this.EnsureUniqueAndIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        use connection = connection.Get()
        Helper.ensureUniqueAndIndex name connection expression

    /// <summary>
    /// Drops an index if it exists for the specified expression.
    /// </summary>
    /// <typeparam name="'R">The type of the indexed property.</typeparam>
    /// <param name="expression">A LINQ expression that selects the property on which the index is defined.</param>
    member this.DropIndexIfExists<'R>(expression: Expression<System.Func<'T, 'R>>) =
        if isNull expression then raise (ArgumentNullException(nameof(expression)))
        let indexName, _whereSQL = Helper.getIndexWhereAndName<'T, 'R> name expression

        let indexSQL = $"DROP INDEX IF EXISTS \"{indexName}\""

        use connection = connection.Get()
        connection.Execute(indexSQL)
        
    /// <summary>
    /// Ensures that all indexes declared on the document type 'T using the [Indexed] attribute exist in the database.
    /// </summary>
    member this.EnsureAddedAttributeIndexes() =
        // Ignore the untyped collections.
        if not (typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>) then
            use conn = connection.Get()
            Helper.ensureDeclaredIndexesFields<'T> name conn

    /// <summary>
    /// Registers a handler invoked before an item insert.
    /// </summary>
    member this.OnInserting(handler) =
        this.Events.OnInserting(handler)

    /// <summary>
    /// Registers a handler invoked before an item delete.
    /// </summary>
    member this.OnDeleting(handler) =
        this.Events.OnDeleting(handler)

    /// <summary>
    /// Registers a handler invoked before an item update.
    /// </summary>
    member this.OnUpdating(handler) =
        this.Events.OnUpdating(handler)

    /// <summary>
    /// Registers a handler invoked after an item insert.
    /// </summary>
    member this.OnInserted(handler) =
        this.Events.OnInserted(handler)

    /// <summary>
    /// Registers a handler invoked after an item delete.
    /// </summary>
    member this.OnDeleted(handler) =
        this.Events.OnDeleted(handler)

    /// <summary>
    /// Registers a handler invoked after an item update.
    /// </summary>
    member this.OnUpdated(handler) =
        this.Events.OnUpdated(handler)

    /// <summary>
    /// Unregisters a previously registered insert handler.
    /// </summary>
    member this.Unregister(handler: InsertingHandler<'T>) =
        this.Events.Unregister(handler)

    /// <summary>
    /// Unregisters a previously registered delete handler.
    /// </summary>
    member this.Unregister(handler: DeletingHandler<'T>) =
        this.Events.Unregister(handler)

    /// <summary>
    /// Unregisters a previously registered update handler.
    /// </summary>
    member this.Unregister(handler: UpdatingHandler<'T>) =
        this.Events.Unregister(handler)

    /// <summary>
    /// Unregisters a previously registered after-insert handler.
    /// </summary>
    member this.Unregister(handler: InsertedHandler<'T>) =
        this.Events.Unregister(handler)

    /// <summary>
    /// Unregisters a previously registered after-delete handler.
    /// </summary>
    member this.Unregister(handler: DeletedHandler<'T>) =
        this.Events.Unregister(handler)

    /// <summary>
    /// Unregisters a previously registered after-update handler.
    /// </summary>
    member this.Unregister(handler: UpdatedHandler<'T>) =
        this.Events.Unregister(handler)


    override this.Equals(other) = 
        match other with
        | :? Collection<'T> as other ->
            (this :> IEquatable<Collection<'T>>).Equals other
        | other -> false

    override this.GetHashCode() = hash this

    interface IEquatable<Collection<'T>> with
        member this.Equals (other) =
            this.ConnectionString = other.ConnectionString && this.Name = other.Name


    interface IOrderedQueryable<'T>

    interface IQueryable<'T> with
        member this.Provider = this.SoloDBQueryable.Provider
        member this.Expression = this.SoloDBQueryable.Expression
        member this.ElementType = typeof<'T>

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            this.SoloDBQueryable.GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

    interface ISoloDBCollection<'T> with
        member this.InTransaction = this.InTransaction
        member this.IncludeType = this.IncludeType 
        member this.Name = this.Name

        member this.DropIndexIfExists(expression) = this.DropIndexIfExists(expression)
        member this.EnsureAddedAttributeIndexes() = this.EnsureAddedAttributeIndexes()
        member this.EnsureIndex(expression) = this.EnsureIndex(expression)
        member this.EnsureUniqueAndIndex(expression) = this.EnsureUniqueAndIndex(expression)
        member this.GetById(id) = this.GetById(id)
        member this.GetById(id: 'IdType) = this.GetById<'IdType>(id)
        member this.Insert(item) = this.Insert(item)
        member this.InsertBatch(items) = this.InsertBatch(items)
        member this.InsertOrReplace(item) = this.InsertOrReplace(item)
        member this.InsertOrReplaceBatch(items) = this.InsertOrReplaceBatch(items)
        member this.TryGetById(id) = this.TryGetById(id)
        member this.TryGetById(id: 'IdType) = this.TryGetById<'IdType>(id)
        member this.GetInternalConnection () = this.Connection.Get()
        member this.Delete(id: 'IdType) = this.DeleteById<'IdType>(id)
        member this.Delete(id) = this.DeleteById(id)
        member this.Update(item) = this.Update(item)
        member this.DeleteMany(filter) = this.DeleteMany(filter)
        member this.DeleteOne(filter) = this.DeleteOne(filter)
        member this.ReplaceMany(item,filter) = this.ReplaceMany(filter)(item)
        member this.ReplaceOne(item,filter) = this.ReplaceOne(filter)(item)
        member this.UpdateMany(filter, t) = this.UpdateMany(t)(filter)

    interface ISoloDBCollectionEvents<'T> with
        member this.OnInserting handler =
            this.OnInserting handler

        member this.OnDeleting handler =
            this.OnDeleting handler

        member this.OnUpdating handler =
            this.OnUpdating handler

        member this.OnInserted handler =
            this.OnInserted handler

        member this.OnDeleted handler =
            this.OnDeleted handler

        member this.OnUpdated handler =
            this.OnUpdated handler

        member this.Unregister (handler: InsertingHandler<'T>) =
            this.Unregister handler

        member this.Unregister (handler: DeletingHandler<'T>) =
            this.Unregister handler

        member this.Unregister (handler: UpdatingHandler<'T>) =
            this.Unregister handler

        member this.Unregister (handler: InsertedHandler<'T>) =
            this.Unregister handler

        member this.Unregister (handler: DeletedHandler<'T>) =
            this.Unregister handler

        member this.Unregister (handler: UpdatedHandler<'T>) =
            this.Unregister handler


/// <summary>
/// Represents a database context within an explicit transaction. All operations are part of the same transaction.
/// </summary>
/// <param name="connection">The transactional connection.</param>
type TransactionalSoloDB internal (connection: TransactionalConnection, parentData: SoloDBToCollectionData) =
    let connectionString = connection.ConnectionString

    /// <summary>
    /// Gets the underlying transactional connection.
    /// </summary>
    member val Connection = connection
    /// <summary>
    /// Gets the SQLite connection string used by this transactional context.
    /// </summary>
    member val ConnectionString = connectionString
    /// <summary>
    /// Gets a file system instance that operates within the current transaction.
    /// </summary>
    member val FileSystem: IFileSystem = FileSystem (Connection.Transactional connection) :> IFileSystem

    member private this.InitializeCollection<'T> name =
        if not (Helper.existsCollection name connection) then
            Helper.createTableInner<'T> name connection

        Helper.registerTypeCollection<'T> name connection

        // Validate relation topology after table creation.
        let hasRelations = RelationsSchema.getRelationSpecs typeof<'T> |> Array.isEmpty |> not
        if hasRelations then
            let relationTx: Relations.RelationTxContext = {
                Connection = connection
                OwnerTable = name
                OwnerType = typeof<'T>
                InTransaction = true // TransactionalSoloDB is always inside a transaction.
            }
            Relations.ensureSchemaForOwnerType relationTx typeof<'T>

        Collection<'T>(Transactional connection, name, connectionString, { ClearCacheFunction = ignore; EventSystem = parentData.EventSystem }) :> ISoloDBCollection<'T>

    /// <summary>
    /// Gets a collection of a specified type, using the type's name as the collection name. Creates the collection if it doesn't exist.
    /// </summary>
    /// <typeparam name="'T">The type of the documents in the collection.</typeparam>
    /// <returns>An <c>ISoloDBCollection<'T></c> instance.</returns>
    member this.GetCollection<'T>() =
        let name = Helper.collectionNameOf<'T>
        
        this.InitializeCollection<'T>(name)

    /// <summary>
    /// Gets a collection of a specified type with a custom name. Creates the collection if it doesn't exist.
    /// </summary>
    /// <typeparam name="'T">The type of the documents in the collection.</typeparam>
    /// <param name="name">The custom name for the collection.</param>
    /// <returns>An <c>ISoloDBCollection<'T></c> instance.</returns>
    member this.GetCollection<'T>(name) =       
        this.InitializeCollection<'T>(Helper.formatName name)

    /// <summary>
    /// Gets a collection that stores untyped JSON data.
    /// </summary>
    /// <param name="name">The name for the collection.</param>
    /// <returns>An <c>ISoloDBCollection<JsonValue></c> instance.</returns>
    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.formatName
        
        this.InitializeCollection<JsonSerializator.JsonValue>(name)

    /// <summary>
    /// Checks if a collection with the specified name exists.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <returns>True if the collection exists, otherwise false.</returns>
    member this.CollectionExists name =
        let name = Helper.formatName name
        Helper.existsCollection name connection

    /// <summary>
    /// Checks if a collection for the specified type exists.
    /// </summary>
    /// <typeparam name="'T">The document type.</typeparam>
    /// <returns>True if the collection exists, otherwise false.</returns>
    member this.CollectionExists<'T>() =
        let name = Helper.collectionNameOf<'T>
        Helper.existsCollection name connection

    /// <summary>
    /// Drops a collection if it exists.
    /// </summary>
    /// <param name="name">The name of the collection to drop.</param>
    /// <returns>True if the collection was dropped, false if it did not exist.</returns>
    member this.DropCollectionIfExists name =
        let name = Helper.formatName name

        if Helper.existsCollection name connection then
            Helper.dropCollection name connection
            true
        else false

    /// <summary>
    /// Drops a collection for the specified type if it exists.
    /// </summary>
    /// <typeparam name="'T">The document type.</typeparam>
    /// <returns>True if the collection was dropped, false if it did not exist.</returns>
    member this.DropCollectionIfExists<'T>() =
        let name = Helper.collectionNameOf<'T>
        this.DropCollectionIfExists name

    /// <summary>
    /// Drops a collection for the specified type.
    /// </summary>
    /// <typeparam name="'T">The document type.</typeparam>
    /// <exception cref="KeyNotFoundException">Thrown if the collection does not exist.</exception>
    member this.DropCollection<'T>() =
        if this.DropCollectionIfExists<'T>() = false then
            let name = Helper.collectionNameOf<'T>
            raise (KeyNotFoundException (sprintf "Collection %s does not exists." name))

    /// <summary>
    /// Drops a collection with the specified name.
    /// </summary>
    /// <param name="name">The name of the collection to drop.</param>
    /// <exception cref="KeyNotFoundException">Thrown if the collection does not exist.</exception>
    member this.DropCollection name =
        if this.DropCollectionIfExists name = false then
            raise (KeyNotFoundException (sprintf "Collection %s does not exists." name))

    /// <summary>
    /// Lists the names of all existing collections in the database.
    /// </summary>
    /// <returns>A sequence of collection names.</returns>
    member this.ListCollectionNames() =
        connection.Query<string>("SELECT Name FROM SoloDBCollections")

    /// <summary>
    /// Asks the SQLite engine to run analysis to optimize query plans within the current transaction.
    /// </summary>
    member this.Optimize() =
        connection.Execute "PRAGMA optimize;" |> ignore

    /// <summary>
    /// Executes a series of database operations within a nested savepoint.
    /// On success the savepoint is released (merged into the parent transaction).
    /// On exception the savepoint is rolled back without affecting the outer transaction.
    /// </summary>
    /// <param name="func">A function that takes a transactional <c>ISoloDB</c> context and returns a result.</param>
    /// <typeparam name="'R">The return type of the function.</typeparam>
    /// <returns>The result of the function.</returns>
    member this.WithTransaction<'R>(func: Func<ISoloDB, 'R>) : 'R =
        withSavepoint connection (fun _conn -> func.Invoke(this :> ISoloDB))

    /// <summary>
    /// Executes a series of database operations within a nested savepoint.
    /// On success the savepoint is released. On exception the savepoint is rolled back.
    /// </summary>
    /// <param name="func">An action that takes a transactional <c>ISoloDB</c> context.</param>
    member this.WithTransaction(func: Action<ISoloDB>) : unit =
        this.WithTransaction<unit>(fun ctx -> func.Invoke ctx)

    /// <summary>
    /// Executes an asynchronous series of database operations within a nested savepoint.
    /// On success the savepoint is released. On exception the savepoint is rolled back.
    /// </summary>
    /// <param name="func">An async function that takes a transactional <c>ISoloDB</c> context and returns a result.</param>
    /// <typeparam name="'R">The return type of the function.</typeparam>
    /// <returns>A task representing the asynchronous operation.</returns>
    member this.WithTransactionAsync<'R>(func: Func<ISoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> =
        withSavepointAsync connection (fun _conn -> func.Invoke(this :> ISoloDB))

    /// <summary>
    /// Executes an asynchronous series of database operations within a nested savepoint.
    /// On success the savepoint is released. On exception the savepoint is rolled back.
    /// </summary>
    /// <param name="func">An async function that takes a transactional <c>ISoloDB</c> context.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    member this.WithTransactionAsync(func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task =
        withSavepointAsync connection (fun _conn -> task { do! func.Invoke(this :> ISoloDB) }) :> Threading.Tasks.Task

    interface ISoloDB with
        member this.ConnectionString = this.ConnectionString
        member this.FileSystem = this.FileSystem
        member this.GetCollection<'T>() = this.GetCollection<'T>()
        member this.GetCollection<'T>(name) = this.GetCollection<'T>(name)
        member this.GetUntypedCollection(name) = this.GetUntypedCollection(name)
        member this.CollectionExists(name) = this.CollectionExists(name)
        member this.CollectionExists<'T>() = this.CollectionExists<'T>()
        member this.DropCollectionIfExists(name) = this.DropCollectionIfExists(name)
        member this.DropCollectionIfExists<'T>() = this.DropCollectionIfExists<'T>()
        member this.DropCollection(name) = this.DropCollection(name)
        member this.DropCollection<'T>() = this.DropCollection<'T>()
        member this.ListCollectionNames() = this.ListCollectionNames()
        member this.Optimize() = this.Optimize()
        member this.WithTransaction<'R>(func: Func<ISoloDB, 'R>) : 'R = this.WithTransaction(func)
        member this.WithTransaction(func: Action<ISoloDB>) : unit = this.WithTransaction(func)
        member this.WithTransactionAsync<'R>(func: Func<ISoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> = this.WithTransactionAsync(func)
        member this.WithTransactionAsync(func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task = this.WithTransactionAsync(func)
        member this.Dispose() = ()

/// <summary>
/// The main database class, representing a single SQLite database file or in-memory instance.
/// Provides access to collections and file system storage.
/// </summary>
type SoloDB private (connectionManager: ConnectionManager, connectionString: string, location: SoloDBLocation, config: SoloDBConfiguration, eventSystem: EventSystem) =
    let mutable disposed = false
    // Serializes DDL operations (InitializeCollection, DropCollectionIfExists) to prevent
    // concurrent sqlite3_prepare_v2 calls from racing with schema changes in the ADO.NET layer.
    // Static because multiple SoloDB instances on the same shared-cache database share the
    // same sqlite3 schema state.
    static let ddlLock = obj()

    /// <summary>
    /// Initializes a new instance of the SoloDB class.
    /// </summary>
    /// <param name="source">
    /// The path to the database file, or an in-memory identifier prefixed with "memory:".
    /// For example: "C:\data\mydb.db" or "memory:shared_db".
    /// </param>
    new(source: string) =
        let connectionString, location = Bootstrap.parseSource source
        let eventSystem = EventSystem ()
        let setup = Bootstrap.createSetup eventSystem
        let defaultConfig: SoloDBConfiguration = { CachingEnabled = true }
        let manager = new ConnectionManager(connectionString, setup, defaultConfig)

        do
            use dbConnection = manager.Borrow()
            Bootstrap.initializeSchema dbConnection

        new SoloDB(manager, connectionString, location, defaultConfig, eventSystem)

    /// <summary>
    /// Gets the underlying connection manager for the database.
    /// </summary>
    member this.Connection = connectionManager
    /// <summary>
    /// Gets the connection string used by this database instance.
    /// </summary>
    member this.ConnectionString = connectionString
    /// <summary>
    /// Gets the storage location (File or Memory) of the database.
    /// </summary>
    member val internal DataLocation = location
    /// <summary>
    /// Gets the configuration settings for this database instance.
    /// </summary>
    member val Config = config
    /// <summary>
    /// Gets an API for interacting with the virtual file system within the database.
    /// </summary>
    member val FileSystem: IFileSystem = FileSystem (Connection.Pooled connectionManager) :> IFileSystem

    /// <summary>Gets the internal event system for this database instance.</summary>
    member val internal Events = eventSystem

    member private this.GetNewConnection() = connectionManager.Borrow()
        
    member private this.InitializeCollection<'T> (name: string) =
        if disposed then raise (ObjectDisposedException(nameof(SoloDB)))
        if name.StartsWith "SoloDB" then raise (ArgumentException $"The SoloDB* prefix is forbidden in Collection names.")

        lock ddlLock (fun () ->
            // Transaction callback returns unit; Collection construction stays outside.
            // Relation schema validation runs after createTableInner inside the transaction.
            connectionManager.WithTransaction(fun connection ->
                let shouldCreate = not (Helper.existsCollection name connection)
                if shouldCreate then
                    Helper.createTableInner<'T> name connection

                Helper.registerTypeCollection<'T> name connection

                // Validate relation topology after table creation but before returning.
                // Must run after createTableInner so self-referential types don't hit "table already exists".
                let hasRelations = RelationsSchema.getRelationSpecs typeof<'T> |> Array.isEmpty |> not
                if hasRelations then
                    let relationTx: Relations.RelationTxContext = {
                        Connection = connection
                        OwnerTable = name
                        OwnerType = typeof<'T>
                        InTransaction = true
                    }
                    Relations.ensureSchemaForOwnerType relationTx typeof<'T>
            )
        )

        Collection<'T>(Pooled connectionManager, name, connectionString, { ClearCacheFunction = this.ClearCache; EventSystem = this.Events }) :> ISoloDBCollection<'T>

    /// <summary>
    /// Gets a collection of a specified type, using the type's name as the collection name. Creates the collection if it doesn't exist.
    /// </summary>
    /// <typeparam name="'T">The type of the documents in the collection.</typeparam>
    /// <returns>An <c>ISoloDBCollection<'T></c> instance.</returns>
    member this.GetCollection<'T>() =
        let name = Helper.collectionNameOf<'T>
        
        this.InitializeCollection<'T>(name)

    /// <summary>
    /// Gets a collection of a specified type with a custom name. Creates the collection if it doesn't exist.
    /// </summary>
    /// <typeparam name="'T">The type of the documents in the collection.</typeparam>
    /// <param name="name">The custom name for the collection.</param>
    /// <returns>An <c>ISoloDBCollection<'T></c> instance.</returns>
    member this.GetCollection<'T>(name) =       
        this.InitializeCollection<'T>(Helper.formatName name)

    /// <summary>
    /// Gets a collection that stores untyped JSON data.
    /// </summary>
    /// <param name="name">The name for the collection.</param>
    /// <returns>An <c>ISoloDBCollection<JsonValue></c> instance.</returns>
    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.formatName
        
        this.InitializeCollection<JsonSerializator.JsonValue>(name)

    /// <summary>
    /// Checks if a collection with the specified name exists.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <returns>True if the collection exists, otherwise false.</returns>
    member this.CollectionExists name =
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    /// <summary>
    /// Checks if a collection for the specified type exists.
    /// </summary>
    /// <typeparam name="'T">The document type.</typeparam>
    /// <returns>True if the collection exists, otherwise false.</returns>
    member this.CollectionExists<'T>() =
        let name = Helper.collectionNameOf<'T>
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    /// <summary>
    /// Drops a collection if it exists.
    /// </summary>
    /// <param name="name">The name of the collection to drop.</param>
    /// <returns>True if the collection was dropped, false if it did not exist.</returns>
    member this.DropCollectionIfExists name =
        let name = Helper.formatName name

        lock ddlLock (fun () ->
            connectionManager.WithTransaction(fun connection ->
                if Helper.existsCollection name connection then
                    Helper.dropCollection name connection
                    true
                else
                    false
            )
        )

    /// <summary>
    /// Drops a collection for the specified type if it exists.
    /// </summary>
    /// <typeparam name="'T">The document type.</typeparam>
    /// <returns>True if the collection was dropped, false if it did not exist.</returns>
    member this.DropCollectionIfExists<'T>() =
        let name = Helper.collectionNameOf<'T>
        this.DropCollectionIfExists name

    /// <summary>
    /// Drops a collection for the specified type.
    /// </summary>
    /// <typeparam name="'T">The document type.</typeparam>
    /// <exception cref="KeyNotFoundException">Thrown if the collection does not exist.</exception>
    member this.DropCollection<'T>() =
        if this.DropCollectionIfExists<'T>() = false then
            let name = Helper.collectionNameOf<'T>
            raise (KeyNotFoundException (sprintf "Collection %s does not exists." name))

    /// <summary>
    /// Drops a collection with the specified name.
    /// </summary>
    /// <param name="name">The name of the collection to drop.</param>
    /// <exception cref="KeyNotFoundException">Thrown if the collection does not exist.</exception>
    member this.DropCollection name =
        if this.DropCollectionIfExists name = false then
            raise (KeyNotFoundException (sprintf "Collection %s does not exists." name))

    /// <summary>
    /// Lists the names of all existing collections in the database.
    /// </summary>
    /// <returns>A sequence of collection names.</returns>
    member this.ListCollectionNames() =
        use dbConnection = connectionManager.Borrow()
        dbConnection.Query<string>("SELECT Name FROM SoloDBCollections")

    /// <summary>
    /// Performs an online backup of this database to another SoloDB instance.
    /// </summary>
    /// <param name="otherDb">The destination database instance.</param>
    member this.BackupTo(otherDb: SoloDB) =
        use dbConnection = connectionManager.Borrow()
        use otherConnection = otherDb.GetNewConnection()
        dbConnection.Inner.BackupDatabase otherConnection.Inner

    /// <summary>
    /// Rebuilds the database file and writes it to a new location, defragmenting and optimizing it.
    /// </summary>
    /// <param name="location">The file path for the new, vacuumed database file.</param>
    /// <exception cref="InvalidOperationException">Thrown if the source database is in-memory.</exception>
    member this.VacuumTo(location: string) =
        match this.DataLocation with
        | Memory _ -> (raise << InvalidOperationException) "Cannot vacuum backup from or to memory."
        | other ->

        let location = Path.GetFullPath location
        if File.Exists location then File.Delete location

        use dbConnection = connectionManager.Borrow()
        dbConnection.Execute($"VACUUM INTO '{location}'")

    /// <summary>
    /// Rebuilds the database file in-place to defragment it and reclaim free space.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the database is in-memory.</exception>
    member this.Vacuum() =
        match this.DataLocation with
        | Memory _ -> (raise << InvalidOperationException) "Cannot vacuum memory databases."
        | other ->

        use dbConnection = connectionManager.Borrow()
        dbConnection.Execute($"VACUUM;")

    /// <summary>
    /// Executes a series of database operations within a single atomic transaction.
    /// </summary>
    /// <param name="func">A function that takes a <c>TransactionalSoloDB</c> instance and returns a result.</param>
    /// <typeparam name="'R">The return type of the function.</typeparam>
    /// <returns>The result of the function.</returns>
    member this.WithTransaction<'R>(func: Func<TransactionalSoloDB, 'R>) : 'R =
        use connectionForTransaction = connectionManager.CreateForTransaction()
        try
            Connections.beginImmediateWithRetry connectionForTransaction
            let transactionalDb = new TransactionalSoloDB(connectionForTransaction, { ClearCacheFunction = ignore; EventSystem = this.Events })

            try
                let ret = func.Invoke transactionalDb
                connectionForTransaction.Execute "COMMIT;" |> ignore
                ret
            with _ex ->
                connectionForTransaction.Execute "ROLLBACK;" |> ignore
                reraise()
        finally connectionForTransaction.DisposeReal(true)

    /// <summary>
    /// Executes a series of database operations within a single atomic transaction.
    /// </summary>
    /// <param name="func">An action that takes a <c>TransactionalSoloDB</c> instance.</param>
    member this.WithTransaction(func: Action<TransactionalSoloDB>) : unit =
        this.WithTransaction<unit>(fun tx -> func.Invoke tx)

    /// <summary>
    /// Executes an asynchronous series of database operations within a single atomic transaction.
    /// </summary>
    /// <param name="func">An async function that takes a <c>TransactionalSoloDB</c> instance and returns a result.</param>
    /// <typeparam name="'R">The return type of the function.</typeparam>
    /// <returns>A task representing the asynchronous transactional operation.</returns>
    member this.WithTransactionAsync<'R>(func: Func<TransactionalSoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> = task {
        use connectionForTransaction = connectionManager.CreateForTransaction()
        try
            Connections.beginImmediateWithRetry connectionForTransaction
            let transactionalDb = new TransactionalSoloDB(connectionForTransaction, { ClearCacheFunction = ignore; EventSystem = this.Events })

            try
                let! ret = func.Invoke transactionalDb
                connectionForTransaction.Execute "COMMIT;" |> ignore
                return ret
            with _ex ->
                connectionForTransaction.Execute "ROLLBACK;" |> ignore
                return reraiseAnywhere _ex
        finally connectionForTransaction.DisposeReal(true)
    }

    /// <summary>
    /// Executes an asynchronous series of database operations within a single atomic transaction.
    /// </summary>
    /// <param name="func">An async function that takes a <c>TransactionalSoloDB</c> instance.</param>
    /// <returns>A task representing the asynchronous transactional operation.</returns>
    member this.WithTransactionAsync(func: Func<TransactionalSoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task =
        this.WithTransactionAsync<unit>(fun tx -> task { do! func.Invoke tx }) :> Threading.Tasks.Task

    /// <summary>
    /// Asks the database engine to run analysis to optimize query plans.
    /// It is recommended to run this after making significant changes to data or indexes.
    /// </summary>
    member this.Optimize() =
        use dbConnection = connectionManager.Borrow()
        dbConnection.Execute "PRAGMA optimize;" |> ignore

    /// <summary>
    /// Disables the in-memory cache of prepared SQL commands for all connections.
    /// </summary>
    member this.DisableCaching() =
        config.CachingEnabled <- false

    /// <summary>
    /// Clears the in-memory cache of prepared SQL commands across all connections without disabling it.
    /// </summary>
    member this.ClearCache() =
        connectionManager.All |> Seq.iter _.ClearCache()

    /// <summary>
    /// Enables the in-memory cache of prepared SQL commands for all connections. Caching is enabled by default.
    /// </summary>
    member this.EnableCaching() =
        config.CachingEnabled <- true

    /// <summary>
    /// Disposes the database instance, closing all connections and releasing resources.
    /// </summary>
    member this.Dispose() =
        disposed <- true
        (connectionManager :> IDisposable).Dispose()

    interface IDisposable with
        member this.Dispose() = this.Dispose()

    interface ISoloDB with
        member this.ConnectionString = this.ConnectionString
        member this.FileSystem = this.FileSystem
        member this.GetCollection<'T>() = this.GetCollection<'T>()
        member this.GetCollection<'T>(name) = this.GetCollection<'T>(name)
        member this.GetUntypedCollection(name) = this.GetUntypedCollection(name)
        member this.CollectionExists(name) = this.CollectionExists(name)
        member this.CollectionExists<'T>() = this.CollectionExists<'T>()
        member this.DropCollectionIfExists(name) = this.DropCollectionIfExists(name)
        member this.DropCollectionIfExists<'T>() = this.DropCollectionIfExists<'T>()
        member this.DropCollection(name) = this.DropCollection(name)
        member this.DropCollection<'T>() = this.DropCollection<'T>()
        member this.ListCollectionNames() = this.ListCollectionNames()
        member this.Optimize() = this.Optimize()
        member this.WithTransaction<'R>(func: Func<ISoloDB, 'R>) : 'R =
            this.WithTransaction(fun (tx: TransactionalSoloDB) -> func.Invoke(tx :> ISoloDB))
        member this.WithTransaction(func: Action<ISoloDB>) : unit =
            this.WithTransaction(fun (tx: TransactionalSoloDB) -> func.Invoke(tx :> ISoloDB))
        member this.WithTransactionAsync<'R>(func: Func<ISoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> =
            this.WithTransactionAsync(fun (tx: TransactionalSoloDB) -> func.Invoke(tx :> ISoloDB))
        member this.WithTransactionAsync(func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task =
            this.WithTransactionAsync(fun (tx: TransactionalSoloDB) -> func.Invoke(tx :> ISoloDB))


    /// <summary>
    /// Analyzes the provided LINQ query and returns the query plan that SQLite would use to execute it.
    /// </summary>
    /// <remarks>Calling this function will clear the in-memory cache of prepared SQL commands.</remarks>
    /// <param name="query">The LINQ query to analyze.</param>
    /// <returns>A string describing the query plan.</returns>
    static member ExplainQueryPlan(query: IQueryable<'T>) = QueryUtils.explainQueryPlan query

    /// <summary>
    /// Translates the provided LINQ query into its corresponding SQL statement.
    /// </summary>
    /// <param name="query">The LINQ query to translate.</param>
    /// <returns>The generated SQL string.</returns>
    static member GetSQL(query: IQueryable<'T>) = QueryUtils.getSQL query
