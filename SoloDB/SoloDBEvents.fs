namespace SoloDatabase

open Connections
open SoloDatabase.RawSqliteFunctions
open Microsoft.Data.Sqlite
open System.Collections.Concurrent
open System.Collections.Generic
open System
open SoloDatabase.Types
open SQLitePCL
open System.Runtime.CompilerServices


type internal EventType =
| Inserting = 1uy

type internal EventSystem internal () =
    member val internal UpdatingHandlerMapping = CowByteSpanMap<HashSet<InsertingHandlerSystem>>()

    member this.CreateFunctions(connection: SqliteConnection) =
        connection.CreateRawFunction("SHOULD_HANDLE_UPDATING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_UPDATING must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.UpdatingHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_UPDATING_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonBItem -> 
            if sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_UPDATING must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()

            let mutable handlers = Unchecked.defaultof<HashSet<InsertingHandlerSystem>>
            let exists = this.UpdatingHandlerMapping.TryGetValue (sqliteCollectionNameUTF8, &handlers)

            match exists with
            | false -> sqliteCtx.SetNull()
            | true ->
            
            if jsonBItem.IsBlob then invalidArg (nameof jsonBItem) "The second argument of SHOULD_HANDLE_UPDATING must be a jsonb blob"
            let jsonBItemSpan = jsonBItem.GetBlobSpan()


            for handler in handlers do
                if handler.Invoke(connection, jsonBItemSpan) = RemoveHandler then
                    ignore (handlers.Remove handler)

            sqliteCtx.SetNull()
        ))
        ()

    interface ISoloDBEvents with
        member this.OnInserting(collectionName: System.ReadOnlySpan<byte>, handler: InsertingHandlerSystem): unit = 
            raise (System.NotImplementedException())


type internal SoloDBEventsContext<'T> internal (createCollection: SqliteConnection -> ISoloDBCollection<'T>) =
    let mutable currentSqliteConnection = Unchecked.defaultof<SqliteConnection>
    let mutable currentCollection = Unchecked.defaultof<ISoloDBCollection<'T> | null>

    member this.Set(connection) =
        currentCollection <- null
        currentSqliteConnection <- connection

    member this.Reset() =
        currentCollection <- null

    interface ISoloDBEventsContext<'T> with
        member this.CollectionInstance: ISoloDBCollection<'T> = 
            if isNull currentCollection then currentCollection <- createCollection currentSqliteConnection
            currentCollection

        member this.Read(lazyVar: byref<SoloDBLazyItem<'T>>): byref<'T> = 
            do raise (System.NotImplementedException())

            
            if not (lazyVar.HasValue()) then
                ()
            
            let mutable valueUnsafe = &lazyVar.valueUnsafe
            &valueUnsafe

type internal CollectionEventSystem<'T> internal (eventSystem: EventSystem, createCollection: SqliteConnection -> ISoloDBCollection<'T>) =
    static member CallHandler(conn: SqliteConnection, jsonB: ReadOnlySpan<byte>, handler: InsertingHandler<'T>, cachedCtx: SoloDBEventsContext<'T>): SoloDBEventsResult =
        try
            let mutable lazyVar1 = SoloDBLazyItem<'T> (
                Unchecked.defaultof<'T>,
                jsonB
            )

            cachedCtx.Set conn
            handler.Invoke(cachedCtx, &lazyVar1)
        finally
            cachedCtx.Reset()


    interface ISoloDBCollectionEvents<'T> with
        member this.OnInserting(handler: InsertingHandler<'T>): unit =
            let hashSet = eventSystem.UpdatingHandlerMapping.GetOrAdd(Utils.collectionNameOfBytes<'T>(), CowByteSpanMapValueFactory<HashSet<InsertingHandlerSystem>>(fun _span -> HashSet()))
            let ctx = SoloDBEventsContext<'T>(createCollection)
            let _added = hashSet.Add(InsertingHandlerSystem(fun conn span -> CollectionEventSystem<'T>.CallHandler(conn, span, handler, ctx)))
            raise (System.NotImplementedException())