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
        manager.SetHandlerDispatchGuard(fun () -> eventSystem.GlobalLock.IsHeldByCurrentThread)

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
    member internal _.Config = config
    /// <summary>
    /// Gets an API for interacting with the virtual file system within the database.
    /// </summary>
    member val FileSystem: IFileSystem = FileSystem (Connection.Pooled connectionManager) :> IFileSystem

    /// <summary>Gets the internal event system for this database instance.</summary>
    member val internal Events = eventSystem

    member private this.GetNewConnection() = connectionManager.Borrow()
    member private this.CheckDisposed() =
        if System.Threading.Volatile.Read(&disposed) then
            raise (ObjectDisposedException(nameof(SoloDB)))
        
    member private this.InitializeCollection<'T> (name: string) =
        SoloDBRootOps.initializeCollection<'T>
            this.CheckDisposed
            ddlLock
            connectionManager
            connectionString
            this.ClearCache
            this.Events
            name

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
        this.CheckDisposed()
        let name = Helper.validateUserCollectionName name
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    /// <summary>
    /// Checks if a collection for the specified type exists.
    /// </summary>
    /// <typeparam name="'T">The document type.</typeparam>
    /// <returns>True if the collection exists, otherwise false.</returns>
    member this.CollectionExists<'T>() =
        this.CheckDisposed()
        let name = Helper.collectionNameOf<'T>
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    /// <summary>
    /// Drops a collection if it exists.
    /// </summary>
    /// <param name="name">The name of the collection to drop.</param>
    /// <returns>True if the collection was dropped, false if it did not exist.</returns>
    member this.DropCollectionIfExists name =
        this.CheckDisposed()
        let name = Helper.validateUserCollectionName name

        lock ddlLock (fun () ->
            connectionManager.WithTransaction(fun connection ->
                if Helper.existsCollection name connection then
                    Helper.dropCollection name connection
                    RuntimeIndexModelCache.invalidate connectionString name
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
        this.CheckDisposed()
        use dbConnection = connectionManager.Borrow()
        Helper.listCollectionNamesSnapshot dbConnection

    /// <summary>
    /// Performs an online backup of this database to another SoloDB instance.
    /// </summary>
    /// <param name="otherDb">The destination database instance.</param>
    member this.BackupTo(otherDb: SoloDB) =
        this.CheckDisposed()
        use dbConnection = connectionManager.Borrow()
        use otherConnection = otherDb.GetNewConnection()
        dbConnection.Inner.BackupDatabase otherConnection.Inner

    /// <summary>
    /// Rebuilds the database file and writes it to a new location, defragmenting and optimizing it.
    /// </summary>
    /// <param name="location">The file path for the new, vacuumed database file.</param>
    /// <exception cref="InvalidOperationException">Thrown if the source database is in-memory.</exception>
    member this.VacuumTo(location: string) =
        this.CheckDisposed()
        match this.DataLocation with
        | Memory _ -> (raise << InvalidOperationException) "Cannot vacuum backup from or to memory."
        | other ->

        let location = Path.GetFullPath location
        if File.Exists location then File.Delete location

        use dbConnection = connectionManager.Borrow()
        let escapedLocation = QueryTranslator.escapeSQLiteString location
        dbConnection.Execute($"VACUUM INTO '{escapedLocation}'")

    /// <summary>
    /// Rebuilds the database file in-place to defragment it and reclaim free space.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the database is in-memory.</exception>
    member this.Vacuum() =
        this.CheckDisposed()
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
        SoloDBRootOps.withTransaction<'R>
            this.CheckDisposed
            connectionManager
            this.Events
            func

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
        return! SoloDBRootOps.withTransactionAsync<'R>
            this.CheckDisposed
            connectionManager
            this.Events
            func
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
        this.CheckDisposed()
        use dbConnection = connectionManager.Borrow()
        dbConnection.Execute "PRAGMA optimize;" |> ignore

    /// <summary>
    /// Disables the in-memory cache of prepared SQL commands for all connections.
    /// </summary>
    member this.DisableCaching() =
        this.CheckDisposed()
        config.CachingEnabled <- false

    /// <summary>
    /// Clears the in-memory cache of prepared SQL commands across all connections without disabling it.
    /// </summary>
    member this.ClearCache() =
        this.CheckDisposed()
        connectionManager.All |> Seq.iter _.ClearCache()

    /// <summary>
    /// Enables the in-memory cache of prepared SQL commands for all connections. Caching is enabled by default.
    /// </summary>
    member this.EnableCaching() =
        this.CheckDisposed()
        config.CachingEnabled <- true

    /// <summary>
    /// Disposes the database instance, closing all connections and releasing resources.
    /// </summary>
    member this.Dispose() =
        System.Threading.Volatile.Write(&disposed, true)
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
