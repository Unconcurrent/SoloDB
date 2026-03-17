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
open System.Reflection
open System.Data
open System.Globalization
open System.Linq
open SoloDatabase.Attributes
open System.Collections.Concurrent

type internal IEventDbCollectionFactory =
    abstract CreateCollection<'U> : collectionName: string -> ISoloDBCollection<'U>

/// <summary>
/// Caches event-context database resources to avoid repeated allocations and redundant metadata queries.
/// </summary>
type internal EventDbCache(connectionString: string, directConnection: SqliteConnection, guardedConnection: Connection, parentData: SoloDBToCollectionData, knownCollectionName: string, collectionFactory: IEventDbCollectionFactory) =
    let knownCollectionName = Helper.formatName knownCollectionName
    let mutable knownCollectionExists = true
    let cacheStore = EventDbCacheStore(4)
    let mutable cachedFileSystem: IFileSystem | null = null

    member private this.TryGetCached<'U>(collectionName: string) =
        cacheStore.TryGetCached<'U>(collectionName)

    member private this.InvalidateIfCached(collectionName: string) =
        cacheStore.InvalidateIfCached(collectionName)

    member private this.TryStoreCached<'U>(collectionName: string) (collection: ISoloDBCollection<'U>) =
        cacheStore.TryStoreCached<'U>(collectionName) collection

    member private this.IsCachedName(collectionName: string) =
        cacheStore.IsCachedName(collectionName)

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
            let collection = collectionFactory.CreateCollection<'U>(collectionName)
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
type internal Collection<'T>(connection: Connection, name: string, connectionString: string, parentData: SoloDBToCollectionData) as this =
    let hasRelations =
        typeof<'T>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.exists (fun p -> DBRefTypeHelpers.isAnyRelationRefType p.PropertyType)
    let scaffold = CollectionScaffold<'T>(connection, connectionString, name, hasRelations)

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
    member val private HasRelations = hasRelations

    member private this.HasIncomingRelations(connection: SqliteConnection) =
        scaffold.HasIncomingRelations(connection)

    member private this.RequiresRelationDeleteHandling(connection: SqliteConnection) =
        scaffold.RequiresRelationDeleteHandling(connection)

    member private this.SelectMutationRows(connection: SqliteConnection, filter: Expression<Func<'T, bool>>, takeOne: bool) =
        let filtered = Queryable.Where(this.SoloDBQueryable, filter)
        let expression =
            if takeOne then Queryable.Take(filtered, 1).Expression
            else filtered.Expression
        let query, variables = QueryableTranslation.startFilterTranslationWithConnection connection this expression
        connection.Query<DbObjectRow>(query, variables) |> Seq.toArray

    member private this.ExecuteJsonUpdateManyByRows(connection: SqliteConnection, rows: DbObjectRow array, expressions: ResizeArray<Expression<System.Action<'T>>>) =
        if expressions.Count = 0 || rows.Length = 0 then
            0
        else
            let variables = Dictionary<string, obj>()
            let fullSQL = StringBuilder()
            let inline append (txt: string) = ignore (fullSQL.Append txt)

            append "UPDATE \""
            append name
            append "\" SET Value = jsonb_set(Value, "

            for expression in expressions do
                QueryTranslator.translateUpdateMode name expression fullSQL variables

            fullSQL.Remove(fullSQL.Length - 1, 1) |> ignore
            append ") WHERE Id IN ("

            for i in 0 .. rows.Length - 1 do
                if i > 0 then append ","
                let key = $"id{i}"
                append "@"
                append key
                variables.[key] <- rows.[i].Id.Value :> obj

            append ")"
            connection.Execute(fullSQL.ToString(), variables)

    /// <summary>Gets the event registration API for this collection.</summary>
    member val private Events: ISoloDBCollectionEvents<'T> =
        let createDb (directConnection: SqliteConnection) (guard: unit -> unit) : ISoloDB =
            let guardedConnection = Guarded(guard, Transactional directConnection)
            let collectionFactory =
                { new IEventDbCollectionFactory with
                    member _.CreateCollection<'U>(collectionName: string) =
                        Collection<'U>(guardedConnection, collectionName, connectionString, parentData) :> ISoloDBCollection<'U> }
            let cache = EventDbCache(connectionString, directConnection, guardedConnection, parentData, name, collectionFactory)
            let throwNestedTxInEventContext () =
                raise (NotSupportedException(
                    "Error: Nested transactions are not supported inside event handler contexts.\n" +
                    "Reason: Event handlers execute during active SQL statements where SQLite cannot open SAVEPOINTs.\n" +
                    "Fix: Perform transactional work outside event handlers, or use the event context directly without nesting."))

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
                    Helper.listCollectionNamesSnapshot directConnection :> seq<string>
                member _.Optimize() =
                    directConnection.Execute "PRAGMA optimize;" |> ignore
                member _.WithTransaction<'R>(_func: Func<ISoloDB, 'R>) : 'R =
                    throwNestedTxInEventContext()
                member _.WithTransaction(_func: Action<ISoloDB>) : unit =
                    throwNestedTxInEventContext()
                member _.WithTransactionAsync<'R>(_func: Func<ISoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> =
                    throwNestedTxInEventContext()
                member _.WithTransactionAsync(_func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task =
                    throwNestedTxInEventContext()
                member _.Dispose() = ()
            }

        CollectionEventSystem<'T>(
            name,
            parentData.EventSystem,
            createDb)

    /// <summary>Gets the internal connection provider for this collection.</summary>
    member val internal Connection = connection

    member internal this.RefreshIndexModelSnapshot(conn: SqliteConnection) =
        scaffold.RefreshIndexModelSnapshot(conn)

    member internal this.InvalidateIndexModelSnapshot() =
        scaffold.InvalidateIndexModelSnapshot()

    member internal this.GetIndexModelSnapshot() =
        scaffold.GetIndexModelSnapshot()

    /// Wraps an operation in a relation-safe auto-transaction when not already in one.
    /// Routes through Connection.WithTransaction (hybrid: BEGIN IMMEDIATE top-level, SAVEPOINT nested).
    /// Used by Insert, Update, Replace, and batch methods that need relation atomicity.
    member private this.WithRelationAutoTx (f: Collection<'T> -> 'R) : 'R =
        connection.WithTransaction(fun conn ->
            let directConnection = Transactional conn
            let transientCollection = Collection<'T>(directConnection, name, connectionString, parentData)
            f transientCollection)

    member private this.mkRelationTx (conn: SqliteConnection) : Relations.RelationTxContext =
        scaffold.MkRelationTx(conn)

    static member private mkRelationPathSets() =
        CollectionScaffold<'T>.MkRelationPathSets()

    member private this.ensureRelationTx (conn: SqliteConnection) : Relations.RelationTxContext =
        scaffold.EnsureRelationTx(conn)

    member private this.tryEnsureRelationTx (conn: SqliteConnection) : Relations.RelationTxContext voption =
        scaffold.TryEnsureRelationTx(conn)

    member private this.setSerializedItem (variables: IDictionary<string, obj>) (item: 'T) =
        variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item

    member private this.tryLoadExistingOwnerForUpsert (conn: SqliteConnection) (item: 'T) : obj voption * int64 voption =
        let oldRow =
            if HasTypeId<'T>.Value then
                let id = HasTypeId<'T>.Read item
                if id > 0L then
                    match conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {| id = id |}) with
                    | row when Object.ReferenceEquals(row, null) -> ValueNone
                    | row -> ValueSome row
                else
                    ValueNone
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

        match oldRow with
        | ValueSome row -> ValueSome (fromSQLite<'T> row |> box), ValueSome row.Id.Value
        | ValueNone -> ValueNone, ValueNone

    /// <summary>
    /// Inserts a new document into the collection.
    /// </summary>
    /// <param name="item">The document to insert.</param>
    /// <returns>The ID of the newly inserted document.</returns>
    member this.Insert (item: 'T) =
        CollectionInsertOps<'T>.Insert
            item
            this.HasRelations
            connection.WithTransaction
            this.ensureRelationTx
            (fun conn -> Helper.insertInner this.IncludeType item conn name this)

    /// <summary>
    /// Inserts a new document or replaces an existing one if a document with the same ID already exists.
    /// </summary>
    /// <param name="item">The document to insert or replace.</param>
    /// <returns>The ID of the inserted or replaced document.</returns>
    member this.InsertOrReplace (item: 'T) =
        CollectionInsertOps<'T>.InsertOrReplace
            item
            this.HasRelations
            name
            connection.WithTransaction
            this.ensureRelationTx
            (fun conn -> this.tryLoadExistingOwnerForUpsert conn item)
            (fun conn -> Helper.insertOrReplaceInner this.IncludeType item conn name this)

    /// <summary>
    /// Inserts a sequence of documents in a single transaction for efficiency.
    /// </summary>
    /// <param name="items">The sequence of documents to insert.</param>
    /// <returns>A list of IDs for the newly inserted documents.</returns>
    member this.InsertBatch (items: 'T seq) =
        let items = CollectionInsertOps<'T>.ValidateBatchItems(items)
        this.WithRelationAutoTx (fun transientCollection ->
            let connection = transientCollection.Connection.Get()
            let tx = this.tryEnsureRelationTx connection
            CollectionInsertOps<'T>.InsertBatchCore
                items
                tx
                (fun item -> Helper.insertInner this.IncludeType item connection name transientCollection))

    /// <summary>
    /// Inserts or replaces a sequence of documents in a single transaction.
    /// </summary>
    /// <param name="items">The sequence of documents to insert or replace.</param>
    /// <returns>A list of IDs for the inserted or replaced documents.</returns>
    member this.InsertOrReplaceBatch (items: 'T seq) =
        let items = CollectionInsertOps<'T>.ValidateBatchItems(items)
        this.WithRelationAutoTx (fun transientCollection ->
            let connection = transientCollection.Connection.Get()
            let tx = this.tryEnsureRelationTx connection
            CollectionInsertOps<'T>.InsertOrReplaceBatchCore
                items
                tx
                name
                (fun item -> this.tryLoadExistingOwnerForUpsert connection item)
                (fun item -> Helper.insertOrReplaceInner this.IncludeType item connection name transientCollection))

    /// <summary>
    /// Tries to find a document by its 64-bit integer ID.
    /// </summary>
    /// <param name="id">The ID of the document to find.</param>
    /// <returns>An option containing the document if found; otherwise, None.</returns>
    member this.TryGetById(id: int64) =
        CollectionReadDeleteOps<'T>.TryGetByIdInt64
            id
            name
            this.HasRelations
            (fun () -> connection.Get())
            (fun conn entityId entity ->
                let excludedPaths, includedPaths = Collection<'T>.mkRelationPathSets()
                Relations.batchLoadDBRefProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
                Relations.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
                Relations.captureRelationVersionForEntities conn name [| (entityId, entity) |])

    /// <summary>
    /// Finds a document by its 64-bit integer ID.
    /// </summary>
    /// <param name="id">The ID of the document to find.</param>
    /// <returns>The document with the specified ID.</returns>
    /// <exception cref="KeyNotFoundException">Thrown if no document with the specified ID is found.</exception>
    member this.GetById(id: int64) =
        CollectionReadDeleteOps<'T>.RequireByIdInt64(id, name, this.TryGetById id)

    /// <summary>
    /// Deletes a document by its 64-bit integer ID.
    /// </summary>
    /// <param name="id">The ID of the document to delete.</param>
    /// <returns>The number of documents deleted (0 or 1).</returns>
    member this.DeleteById(id: int64) =
        CollectionReadDeleteOps<'T>.DeleteByIdInt64
            id
            name
            connection.WithTransaction
            this.RequiresRelationDeleteHandling
            this.ensureRelationTx
            (fun tx ownerId owner ->
                let deletePlan = Relations.prepareDeleteOwner tx ownerId owner
                Relations.syncDeleteOwner tx deletePlan)

    /// <summary>
    /// Tries to find a document by its custom ID, as defined by the [Id] attribute on the document type.
    /// </summary>
    /// <typeparam name="'IdType">The type of the custom ID.</typeparam>
    /// <param name="id">The custom ID of the document to find.</param>
    /// <returns>An option containing the document if found; otherwise, None.</returns>
    member this.TryGetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T option =
        let hydrateRelations (conn: SqliteConnection) (entityId: int64) (entity: obj) =
            let excludedPaths, includedPaths = Collection<'T>.mkRelationPathSets()
            Relations.batchLoadDBRefProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
            Relations.batchLoadDBRefManyProperties conn name typeof<'T> excludedPaths includedPaths false [| (entityId, entity) |] this.InTransaction
            Relations.captureRelationVersionForEntities conn name [| (entityId, entity) |]

        let tryGetByCustomId () =
            CollectionReadDeleteOps<'T>.TryGetByCustomId
                id
                name
                this.HasRelations
                (fun () -> connection.Get())
                hydrateRelations

        CollectionReadDeleteOps<'T>.TryGetByIdWithFallback
            id
            (nameof id)
            (fun id64 -> this.TryGetById(id64))
            tryGetByCustomId

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
        let syncDeleteOwner (tx: Relations.RelationTxContext) (ownerId: int64) (owner: obj) =
            let deletePlan = Relations.prepareDeleteOwner tx ownerId owner
            Relations.syncDeleteOwner tx deletePlan

        let deleteByCustomId () =
            CollectionReadDeleteOps<'T>.DeleteByCustomId
                id
                name
                connection.WithTransaction
                this.RequiresRelationDeleteHandling
                this.ensureRelationTx
                syncDeleteOwner

        CollectionReadDeleteOps<'T>.DeleteByIdWithFallback
            id
            (nameof id)
            (fun id64 -> this.DeleteById(id64))
            deleteByCustomId

    /// <summary>
    /// Updates an existing document in the collection. The document must have a valid ID.
    /// </summary>
    /// <param name="item">The document with updated values.</param>
    /// <exception cref="KeyNotFoundException">Thrown if no document with the item's ID is found.</exception>
    /// <exception cref="InvalidOperationException">Thrown if the document type does not have a recognizable ID property.</exception>
    member this.Update(item: 'T) =
        CollectionMutationOps<'T>.Update
            item
            name
            this.HasRelations
            connection.WithTransaction
            this.ensureRelationTx
            Collection<'T>.mkRelationPathSets
            this.setSerializedItem

    /// <summary>
    /// Deletes all documents that match the specified filter.
    /// </summary>
    /// <param name="filter">A LINQ expression to filter the documents to delete.</param>
    /// <returns>The number of documents deleted.</returns>
    member this.DeleteMany(filter: Expression<Func<'T, bool>>) =
        CollectionMutationOps<'T>.DeleteMany
            filter
            name
            connection.WithTransaction
            this.RequiresRelationDeleteHandling
            this.ensureRelationTx
            (fun conn currentFilter takeOne -> this.SelectMutationRows(conn, currentFilter, takeOne))

    /// <summary>
    /// Deletes the first document that matches the specified filter.
    /// </summary>
    /// <param name="filter">A LINQ expression to filter the document to delete.</param>
    /// <returns>The number of documents deleted (0 or 1).</returns>
    member this.DeleteOne(filter: Expression<Func<'T, bool>>) =
        CollectionMutationOps<'T>.DeleteOne
            filter
            name
            connection.WithTransaction
            this.RequiresRelationDeleteHandling
            this.ensureRelationTx
            (fun conn currentFilter takeOne -> this.SelectMutationRows(conn, currentFilter, takeOne))

    /// <summary>
    /// Replaces the content of all documents matching the filter with a new document.
    /// </summary>
    /// <param name="item">The new document content to use for replacement.</param>
    /// <param name="filter">A LINQ expression to filter the documents to replace.</param>
    /// <returns>The number of documents replaced.</returns>
    member this.ReplaceMany(item: 'T)(filter: Expression<Func<'T, bool>>) =
        CollectionMutationOps<'T>.ReplaceMany
            item
            filter
            name
            this.HasRelations
            connection.WithTransaction
            this.ensureRelationTx
            Collection<'T>.mkRelationPathSets
            this.setSerializedItem

    /// <summary>
    /// Replaces the content of the first document matching the filter with a new document.
    /// </summary>
    /// <param name="item">The new document content to use for replacement.</param>
    /// <param name="filter">A LINQ expression to filter the document to replace.</param>
    /// <returns>The number of documents replaced (0 or 1).</returns>
    member this.ReplaceOne(item: 'T)(filter: Expression<Func<'T, bool>>) =
        CollectionMutationOps<'T>.ReplaceOne
            item
            filter
            name
            this.HasRelations
            connection.WithTransaction
            this.ensureRelationTx
            Collection<'T>.mkRelationPathSets
            this.setSerializedItem

    /// <summary>
    /// Applies a set of transformations to update all documents that match the specified filter.
    /// </summary>
    /// <param name="transform">An array of expressions defining the updates to apply.</param>
    /// <param name="filter">A LINQ expression to filter the documents to update.</param>
    /// <returns>The number of documents updated.</returns>
    member this.UpdateMany(transform: Expression<System.Action<'T>> array)(filter: Expression<Func<'T, bool>>) =
        CollectionMutationOps<'T>.UpdateMany
            transform
            filter
            name
            this.HasRelations
            (fun () -> connection.Get())
            connection.WithTransaction
            this.ensureRelationTx
            (fun conn currentFilter takeOne -> this.SelectMutationRows(conn, currentFilter, takeOne))
            (fun conn rows expressions -> this.ExecuteJsonUpdateManyByRows(conn, rows, expressions))

    /// <summary>
    /// Ensures that a non-unique index exists for the specified expression. Creates the index if it does not exist.
    /// </summary>
    /// <typeparam name="'R">The type of the indexed property.</typeparam>
    /// <param name="expression">A LINQ expression that selects the property to be indexed.</param>
    member this.EnsureIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        CollectionSurfaceOps.ensureIndex<'T, 'R>
            name
            (fun () -> connection.Get())
            this.InvalidateIndexModelSnapshot
            expression

    /// <summary>
    /// Ensures that a unique index exists for the specified expression. Creates the index if it does not exist.
    /// </summary>
    /// <typeparam name="'R">The type of the indexed property.</typeparam>
    /// <param name="expression">A LINQ expression that selects the property to be indexed.</param>
    member this.EnsureUniqueAndIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        CollectionSurfaceOps.ensureUniqueAndIndex<'T, 'R>
            name
            (fun () -> connection.Get())
            this.InvalidateIndexModelSnapshot
            expression

    /// <summary>
    /// Drops an index if it exists for the specified expression.
    /// </summary>
    /// <typeparam name="'R">The type of the indexed property.</typeparam>
    /// <param name="expression">A LINQ expression that selects the property on which the index is defined.</param>
    member this.DropIndexIfExists<'R>(expression: Expression<System.Func<'T, 'R>>) =
        CollectionSurfaceOps.dropIndexIfExists<'T, 'R>
            name
            (fun () -> connection.Get())
            this.InvalidateIndexModelSnapshot
            expression
        
    /// <summary>
    /// Ensures that all indexes declared on the document type 'T using the [Indexed] attribute exist in the database.
    /// </summary>
    member this.EnsureAddedAttributeIndexes() =
        CollectionSurfaceOps.ensureAddedAttributeIndexes<'T>
            name
            (fun () -> connection.Get())
            this.InvalidateIndexModelSnapshot

    /// <summary>
    /// Registers a handler invoked before an item insert.
    /// </summary>
    member this.OnInserting(handler) =
        CollectionSurfaceOps.onInserting this.Events handler

    /// <summary>
    /// Registers a handler invoked before an item delete.
    /// </summary>
    member this.OnDeleting(handler) =
        CollectionSurfaceOps.onDeleting this.Events handler

    /// <summary>
    /// Registers a handler invoked before an item update.
    /// </summary>
    member this.OnUpdating(handler) =
        CollectionSurfaceOps.onUpdating this.Events handler

    /// <summary>
    /// Registers a handler invoked after an item insert.
    /// </summary>
    member this.OnInserted(handler) =
        CollectionSurfaceOps.onInserted this.Events handler

    /// <summary>
    /// Registers a handler invoked after an item delete.
    /// </summary>
    member this.OnDeleted(handler) =
        CollectionSurfaceOps.onDeleted this.Events handler

    /// <summary>
    /// Registers a handler invoked after an item update.
    /// </summary>
    member this.OnUpdated(handler) =
        CollectionSurfaceOps.onUpdated this.Events handler

    /// <summary>
    /// Unregisters a previously registered insert handler.
    /// </summary>
    member this.Unregister(handler: InsertingHandler<'T>) =
        CollectionSurfaceOps.unregisterInserting this.Events handler

    /// <summary>
    /// Unregisters a previously registered delete handler.
    /// </summary>
    member this.Unregister(handler: DeletingHandler<'T>) =
        CollectionSurfaceOps.unregisterDeleting this.Events handler

    /// <summary>
    /// Unregisters a previously registered update handler.
    /// </summary>
    member this.Unregister(handler: UpdatingHandler<'T>) =
        CollectionSurfaceOps.unregisterUpdating this.Events handler

    /// <summary>
    /// Unregisters a previously registered after-insert handler.
    /// </summary>
    member this.Unregister(handler: InsertedHandler<'T>) =
        CollectionSurfaceOps.unregisterInserted this.Events handler

    /// <summary>
    /// Unregisters a previously registered after-delete handler.
    /// </summary>
    member this.Unregister(handler: DeletedHandler<'T>) =
        CollectionSurfaceOps.unregisterDeleted this.Events handler

    /// <summary>
    /// Unregisters a previously registered after-update handler.
    /// </summary>
    member this.Unregister(handler: UpdatedHandler<'T>) =
        CollectionSurfaceOps.unregisterUpdated this.Events handler


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
