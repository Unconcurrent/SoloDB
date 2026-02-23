namespace SoloDatabase.MongoDB

open System
open System.IO
open SoloDatabase
open System.Runtime.InteropServices

/// <summary>
/// Represents a database, providing access to its collections. This is an internal wrapper around a <see cref="SoloDB"/> instance.
/// </summary>
/// <param name="soloDB">The underlying SoloDB instance.</param>
type MongoDatabase internal (soloDB: SoloDB) =
    /// <summary>
    /// Gets a collection with a specific document type.
    /// </summary>
    member this.GetCollection<'doc> (name: string) =
        soloDB.GetCollection<'doc> name

    /// <summary>
    /// Creates or gets an untyped collection, which will work with <see cref="BsonDocument"/>.
    /// </summary>
    member this.CreateCollection (name: string) =
        soloDB.GetUntypedCollection name

    /// <summary>
    /// Lists the names of all collections in the database.
    /// </summary>
    member this.ListCollections () =
        soloDB.ListCollectionNames ()

    /// <summary>
    /// Disposes the underlying database connection and resources.
    /// </summary>
    member this.Dispose () =
        soloDB.Dispose ()

    /// <summary>
    /// Disposes the object.
    /// </summary>
    interface IDisposable with
        override this.Dispose (): unit =
            this.Dispose ()

/// <summary>
/// Internal discriminated union to represent the storage location of a MongoClient.
/// </summary>
type internal MongoClientLocation =
/// <summary>An on-disk database with a directory and a lock file.</summary>
| Disk of {| Directory: DirectoryInfo; LockingFile: FileStream |}
/// <summary>An in-memory database identified by a source string.</summary>
| Memory of string

/// <summary>
/// The main client for connecting to a SoloDB data source with a MongoDB-like API.
/// It can connect to on-disk or in-memory databases.
/// </summary>
/// <param name="directoryDatabaseSource">
/// The data source. For on-disk, this is a directory path. For in-memory, it should start with "memory:".
/// </param>
/// <exception cref="System.Exception">Thrown if the source string starts with "mongodb://".</exception>
type MongoClient(directoryDatabaseSource: string) =
    do if directoryDatabaseSource.StartsWith "mongodb://" then failwithf "SoloDB does not support mongo connections."

    /// <summary>
    /// The determined location (Disk or Memory) of the database.
    /// </summary>
    let location =
        if directoryDatabaseSource.StartsWith "memory:" then
            Memory directoryDatabaseSource
        else
            let directoryDatabasePath = Path.GetFullPath directoryDatabaseSource
            let directory = Directory.CreateDirectory directoryDatabasePath
            Disk {|
                Directory = directory
                LockingFile = File.Open(Path.Combine (directory.FullName, ".lock"), FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)
            |}

    /// <summary>
    /// A list of weak references to the databases created by this client.
    /// </summary>
    let connectedDatabases = ResizeArray<WeakReference<MongoDatabase>> ()
    /// <summary>
    /// A flag to indicate if the client has been disposed.
    /// </summary>
    let mutable disposed = false

    /// <summary>
    /// Gets a handle to a database.
    /// </summary>
    member this.GetDatabase([<Optional; DefaultParameterValue(null: string)>] name : string) =
        if disposed then raise (ObjectDisposedException(nameof(MongoClient)))

        let name = match name with null -> "Master" | n -> n
        let name = name + ".solodb"
        let dbSource =
            match location with
            | Disk disk ->
                Path.Combine (disk.Directory.FullName, name)
            | Memory source ->
                source + "-" + name

        let db = new SoloDB (dbSource)
        let db = new MongoDatabase (db)

        let _remoteCount = connectedDatabases.RemoveAll(fun x -> match x.TryGetTarget() with false, _ -> true | true, _ -> false)
        connectedDatabases.Add (WeakReference<MongoDatabase> db)

        db

    /// <summary>
    /// Disposes the client, along with all databases it has created and releases any file locks.
    /// </summary>
    interface IDisposable with
        override this.Dispose () =
            disposed <- true
            for x in connectedDatabases do
                match x.TryGetTarget() with
                | true, soloDB -> soloDB.Dispose()
                | false, _ -> ()

            connectedDatabases.Clear()
            match location with
            | Disk disk ->
                disk.LockingFile.Dispose ()
            | _other -> ()
