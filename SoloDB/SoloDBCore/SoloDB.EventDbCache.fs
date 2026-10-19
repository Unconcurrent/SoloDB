namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Collections.Generic
open FileStorage
open Connections
open NativeSQLiteTools

type internal EventDbCacheStore(initialCapacity: int) =
    let cache = ConcurrentDictionary<string, obj>(Environment.ProcessorCount, initialCapacity, StringComparer.Ordinal)

    member _.TryGetCached<'U>(collectionName: string) : ISoloDBCollection<'U> option =
        match cache.TryGetValue collectionName with
        | true, (:? ISoloDBCollection<'U> as collection) -> Some collection
        | _ -> None

    member _.TryStoreCached<'U>(collectionName: string) (collection: ISoloDBCollection<'U>) =
        cache.TryAdd(collectionName, box collection)

    member _.IsCachedName(collectionName: string) =
        cache.ContainsKey collectionName

type internal IEventDbCollectionFactory =
    abstract CreateCollection<'U> : collectionName: string -> ISoloDBCollection<'U>

/// <summary>
/// Caches event-context database resources to avoid repeated allocations and redundant metadata queries.
/// </summary>
type internal EventDbCache(connectionString: string, directConnection: NativeSqliteConnection, guardedConnection: NativeConnection, parentData: SoloDBToCollectionData, knownCollectionName: string option, collectionFactory: IEventDbCollectionFactory, fileSystemEvents: unit -> ISoloDBFileSystemEvents) =
    let knownCollectionName = Option.map Helper.formatName knownCollectionName
    let mutable knownCollectionExists = Option.isSome knownCollectionName
    let cacheStore = EventDbCacheStore(4)
    let mutable cachedFileSystem: IFileSystem | null = null

    member private _.TryGetCached<'U>(collectionName: string) : ISoloDBCollection<'U> option =
        cacheStore.TryGetCached<'U>(collectionName)

    member private _.TryStoreCached<'U>(collectionName: string) (collection: ISoloDBCollection<'U>) =
        cacheStore.TryStoreCached<'U>(collectionName) collection

    member private _.IsCachedName(collectionName: string) =
        cacheStore.IsCachedName(collectionName)

    member private _.EnsureExists<'U>(collectionName: string) =
        if Some collectionName = knownCollectionName then
            if not knownCollectionExists then
                if not (Helper.existsCollection collectionName directConnection) then
                    Helper.createTableInner<'U> collectionName directConnection
                knownCollectionExists <- true
        else if not (Helper.existsCollection collectionName directConnection) then
            Helper.createTableInner<'U> collectionName directConnection

        Helper.registerTypeCollection<'U> collectionName directConnection

        let hasRelations = RelationsSchemaValidator.hasReferenceProperties typeof<'U>
        if hasRelations then
            let relationTx: RelationsTypes.RelationTxContext = {
                Connection = directConnection
                OwnerTable = collectionName
                OwnerType = typeof<'U>
                InTransaction = true
                Preparation = RelationsTypes.RelationPreparation()
                CollectionFactory = None
            }
            RelationsCore.ensureSchemaForOwnerType relationTx typeof<'U>

    member this.FileSystem : IFileSystem =
        if isNull cachedFileSystem then
            cachedFileSystem <- (FileSystem(guardedConnection, fileSystemEvents) :> IFileSystem)
        cachedFileSystem

    member this.GetCollection<'U>(collectionName: string) : ISoloDBCollection<'U> =
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
        if Some collectionName = knownCollectionName && knownCollectionExists then true
        elif this.IsCachedName(collectionName) then true
        else Helper.existsCollection collectionName directConnection

    member _.DropCollectionIfExists(_collectionName: string) =
        raise (InvalidOperationException
            "Error: Dropping collections is not supported from event handlers.\nReason: Event handlers must not perform schema changes.\nFix: Drop collections outside event handlers.")

    member _.DropCollection(_collectionName: string) =
        raise (InvalidOperationException
            "Error: Dropping collections is not supported from event handlers.\nReason: Event handlers must not perform schema changes.\nFix: Drop collections outside event handlers.")


module internal EventContextDatabase =
    let rec create connectionString parentData knownCollectionName
        (factory: NativeConnection -> IEventDbCollectionFactory)
        (directConnection: NativeSqliteConnection) (guard: unit -> unit) : ISoloDB =
        let guardedConnection = NativeGuarded(guard, NativeTransactional directConnection)
        let fileSystemEvents () =
            FileSystemEventRegistration.get parentData.EventSystem
                (create connectionString parentData None factory)
        let cache = EventDbCache(connectionString, directConnection, guardedConnection, parentData,
                                 knownCollectionName, factory guardedConnection, fileSystemEvents)
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
                let mutable statement = directConnection.OpenStatement("PRAGMA optimize;")
                try
                    while statement.Step() do ()
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
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
