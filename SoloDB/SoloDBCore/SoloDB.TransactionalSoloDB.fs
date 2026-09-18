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

/// <summary>
/// Represents a database context within an explicit transaction. All operations are part of the same transaction.
/// </summary>
/// <param name="connection">The transactional connection.</param>
type TransactionalSoloDB internal (connection: CachingDbConnection, parentData: SoloDBToCollectionData) =
    let connectionString = connection.ConnectionString

    // The transaction owns its connection only for the duration of the callback. Afterwards the
    // connection returns to the pool and may serve unrelated work, so a context that outlives the
    // callback must fail closed rather than write through a connection it no longer owns.
    // Written once by ExitScope and read by arbitrary later callers, possibly on another thread
    // after an async callback. Volatile so callback exit happens-before every later scope check.
    let mutable scopeActive = 1

    let checkScope () =
        if System.Threading.Volatile.Read(&scopeActive) = 0 then
            raise (InvalidOperationException(
                "Error: This transactional context is no longer valid.\n" +
                "Reason: The transaction callback has already completed, and its connection was released.\n" +
                "Fix: Do not store or reuse the transactional instance outside the WithTransaction callback."))

    let guardedConnection = Connection.Guarded(checkScope, Connection.Transactional connection)

    /// <summary>
    /// Ends the transactional scope. After this call every operation on this instance fails closed.
    /// Invoked by the transaction runner once the user callback has returned or thrown.
    /// </summary>
    member internal _.ExitScope() = System.Threading.Volatile.Write(&scopeActive, 0)

    /// <summary>
    /// Gets the underlying transactional connection.
    /// </summary>
    member _.Connection =
        checkScope ()
        connection
    /// <summary>
    /// Gets the SQLite connection string used by this transactional context.
    /// </summary>
    member val ConnectionString = connectionString
    /// <summary>
    /// Gets a file system instance that operates within the current transaction.
    /// </summary>
    member val FileSystem: IFileSystem = FileSystem guardedConnection :> IFileSystem

    member private this.InitializeCollection<'T> name =
        checkScope ()
        if not (Helper.existsCollection name connection) then
            Helper.createTableInner<'T> name connection

        Helper.registerTypeCollection<'T> name connection

        // Validate relation topology after table creation.
        let hasRelations = RelationsSchemaValidator.getRelationSpecs typeof<'T> |> Array.isEmpty |> not
        if hasRelations then
            let relationTx: RelationsTypes.RelationTxContext = {
                Connection = connection
                OwnerTable = name
                OwnerType = typeof<'T>
                InTransaction = true // TransactionalSoloDB is always inside a transaction.
                CollectionFactory = None
            }
            RelationsCore.ensureSchemaForOwnerType relationTx typeof<'T>

        let collection = Collection<'T>(guardedConnection, name, connectionString, { ClearCacheFunction = ignore; EventSystem = parentData.EventSystem })
        collection.RefreshIndexModelSnapshot(connection)
        collection :> ISoloDBCollection<'T>

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
        this.InitializeCollection<'T>(Helper.validateUserCollectionName name)

    /// <summary>
    /// Gets a collection that stores untyped JSON data.
    /// </summary>
    /// <param name="name">The name for the collection.</param>
    /// <returns>An <c>ISoloDBCollection<JsonValue></c> instance.</returns>
    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.validateUserCollectionName
        
        this.InitializeCollection<JsonSerializator.JsonValue>(name)

    /// <summary>
    /// Checks if a collection with the specified name exists.
    /// </summary>
    /// <param name="name">The name of the collection.</param>
    /// <returns>True if the collection exists, otherwise false.</returns>
    member this.CollectionExists name =
        checkScope ()
        let name = Helper.validateUserCollectionName name
        Helper.existsCollection name connection

    /// <summary>
    /// Checks if a collection for the specified type exists.
    /// </summary>
    /// <typeparam name="'T">The document type.</typeparam>
    /// <returns>True if the collection exists, otherwise false.</returns>
    member this.CollectionExists<'T>() =
        checkScope ()
        let name = Helper.collectionNameOf<'T>
        Helper.existsCollection name connection

    /// <summary>
    /// Drops a collection if it exists.
    /// </summary>
    /// <param name="name">The name of the collection to drop.</param>
    /// <returns>True if the collection was dropped, false if it did not exist.</returns>
    member this.DropCollectionIfExists name =
        checkScope ()
        let name = Helper.validateUserCollectionName name

        if Helper.existsCollection name connection then
            Helper.dropCollection name connection
            RuntimeIndexModelCache.invalidate connectionString name
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
        checkScope ()
        Helper.listCollectionNamesSnapshot connection

    /// <summary>
    /// Asks the SQLite engine to run analysis to optimize query plans within the current transaction.
    /// </summary>
    member this.Optimize() =
        checkScope ()
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
        checkScope ()
        ConnectionsTransactionHelpers.withSavepoint connection (fun _conn -> func.Invoke(this :> ISoloDB))

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
        checkScope ()
        ConnectionsTransactionHelpers.withSavepointAsync connection (fun _conn -> func.Invoke(this :> ISoloDB))

    /// <summary>
    /// Executes an asynchronous series of database operations within a nested savepoint.
    /// On success the savepoint is released. On exception the savepoint is rolled back.
    /// </summary>
    /// <param name="func">An async function that takes a transactional <c>ISoloDB</c> context.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    member this.WithTransactionAsync(func: Func<ISoloDB, Threading.Tasks.Task>) : Threading.Tasks.Task =
        ConnectionsTransactionHelpers.withSavepointAsync connection (fun _conn -> task { do! func.Invoke(this :> ISoloDB) }) :> Threading.Tasks.Task

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
