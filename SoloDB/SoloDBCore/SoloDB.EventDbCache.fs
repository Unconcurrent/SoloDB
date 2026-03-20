namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Collections.Generic
open Microsoft.Data.Sqlite
open FileStorage
open Connections

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
type internal EventDbCache(connectionString: string, directConnection: SqliteConnection, guardedConnection: Connection, parentData: SoloDBToCollectionData, knownCollectionName: string, collectionFactory: IEventDbCollectionFactory) =
    let knownCollectionName = Helper.formatName knownCollectionName
    let mutable knownCollectionExists = true
    let cacheStore = EventDbCacheStore(4)
    let mutable cachedFileSystem: IFileSystem | null = null

    member private _.TryGetCached<'U>(collectionName: string) : ISoloDBCollection<'U> option =
        cacheStore.TryGetCached<'U>(collectionName)

    member private _.TryStoreCached<'U>(collectionName: string) (collection: ISoloDBCollection<'U>) =
        cacheStore.TryStoreCached<'U>(collectionName) collection

    member private _.IsCachedName(collectionName: string) =
        cacheStore.IsCachedName(collectionName)

    member private _.EnsureExists<'U>(collectionName: string) =
        if collectionName = knownCollectionName then
            if not knownCollectionExists then
                if not (Helper.existsCollection collectionName directConnection) then
                    Helper.createTableInner<'U> collectionName directConnection
                knownCollectionExists <- true
        else if not (Helper.existsCollection collectionName directConnection) then
            Helper.createTableInner<'U> collectionName directConnection

        Helper.registerTypeCollection<'U> collectionName directConnection

        let hasRelations = RelationsSchema.getRelationSpecs typeof<'U> |> Array.isEmpty |> not
        if hasRelations then
            let relationTx: Relations.RelationTxContext = {
                Connection = directConnection
                OwnerTable = collectionName
                OwnerType = typeof<'U>
                InTransaction = true
            }
            Relations.ensureSchemaForOwnerType relationTx typeof<'U>

    member this.FileSystem : IFileSystem =
        if isNull cachedFileSystem then
            cachedFileSystem <- (FileSystem guardedConnection :> IFileSystem)
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
        if collectionName = knownCollectionName && knownCollectionExists then true
        elif this.IsCachedName(collectionName) then true
        else Helper.existsCollection collectionName directConnection

    member _.DropCollectionIfExists(_collectionName: string) =
        raise (InvalidOperationException
            "Error: Dropping collections is not supported from event handlers.\nReason: Event handlers must not perform schema changes.\nFix: Drop collections outside event handlers.")

    member _.DropCollection(_collectionName: string) =
        raise (InvalidOperationException
            "Error: Dropping collections is not supported from event handlers.\nReason: Event handlers must not perform schema changes.\nFix: Drop collections outside event handlers.")
