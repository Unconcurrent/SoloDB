namespace SoloDatabase

open CowByteSpanMap
open Connections
open SoloDatabase.RawSqliteFunctions
open Microsoft.Data.Sqlite
open System.Collections.Concurrent
open System.Collections.Generic
open System
open SoloDatabase.Types
open SQLitePCL
open System.Runtime.CompilerServices
open Microsoft.FSharp.NativeInterop
open Utils
open System.Threading
open SoloDatabase.JsonSerializator
open System.Text

#nowarn "9"

type internal EventSystem internal () =
    let mutable sessionIndex = 0L
    member val internal GlobalLock = ReentrantSpinLock()
    member val internal UpdatingHandlerMapping = CowByteSpanMap<ResizeArray<UpdatingHandlerSystem>>()

    member this.CreateFunctions(connection: SqliteConnection) =
        connection.CreateRawFunction("SHOULD_HANDLE_UPDATING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_UPDATING must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.UpdatingHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_UPDATING_HANDLER", RawScalarFunc3(fun sqliteCtx sqliteCollectionName jsonOld jsonNew -> 
            if not sqliteCollectionName.IsText then
                invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"

            if not jsonOld.IsText then
                invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"

            if not jsonNew.IsText then
                invalidArg (nameof jsonNew) "Third argument must be TEXT JSON (use json(NEW.Value) in trigger)"

            let nameUtf8 = sqliteCollectionName.GetBlobSpan()

            let mutable handlers = Unchecked.defaultof<ResizeArray<UpdatingHandlerSystem>>
            let exists = this.UpdatingHandlerMapping.TryGetValue(nameUtf8, &handlers)

            match exists with
            | false -> sqliteCtx.SetInt32 0
            | true ->
            
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()

            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()

            let mutable handlerFailed = false

            try 
                this.GlobalLock.Enter()

                let session = Interlocked.Increment &sessionIndex

                let mutable handlersToRemove = NativePtr.nullPtr<bool>
                let mutable handlersToRemoveCount = 0
                let mutable i = 0

                try
                    for h in handlers do
                        if not handlerFailed then
                            if NativePtr.isNullPtr handlersToRemove then
                                handlersToRemoveCount <- handlers.Count
                                handlersToRemove <- NativePtr.stackalloc<bool> handlersToRemoveCount
                                NativePtr.initBlock handlersToRemove 0uy (uint32 handlersToRemoveCount * uint32 sizeof<bool>)

                            try
                                if h.Invoke(connection, session, oldUtf8, oldUtf8Size, newUtf8, newUtf8Size) = RemoveHandler then
                                    NativePtr.set handlersToRemove i true
                            with _ex ->
                                handlerFailed <- true
                        i <- i + 1
                finally
                    if not (NativePtr.isNullPtr handlersToRemove) then
                        for j = handlersToRemoveCount - 1 downto 0 do
                            if NativePtr.get handlersToRemove j then
                                handlers.RemoveAt j

                        if handlers.Count = 0 then
                            ignore (this.UpdatingHandlerMapping.Remove nameUtf8)

            finally
                this.GlobalLock.Exit()

            sqliteCtx.SetInt32(if handlerFailed then 1 else 0)
        ))
        ()

type internal SoloDBUpdatingEventContext<'T> internal (createCollection: SqliteConnection -> ISoloDBCollection<'T>) =
    let mutable currentSqliteConnection = Unchecked.defaultof<SqliteConnection>
    let mutable currentCollection = Unchecked.defaultof<ISoloDBCollection<'T> | null>
    let mutable previousSession = -1L
    let mutable jsonOld = struct (NativePtr.nullPtr<byte>, 0)
    let mutable jsonNew = struct (NativePtr.nullPtr<byte>, 0)

    let mutable itemOld: 'T = Unchecked.defaultof<'T>
    let mutable itemNew: 'T = Unchecked.defaultof<'T>

    member this.Reset (connection, session: int64, jsonOldBytes: nativeptr<byte>, jsonOldSize: int, jsonNewBytes: nativeptr<byte>, jsonNewSize: int) =
        if previousSession <> session then
            previousSession <- session
            currentCollection <- null
            currentSqliteConnection <- connection
            jsonOld <- struct (jsonOldBytes, jsonOldSize)
            jsonNew <- struct (jsonNewBytes, jsonNewSize)

    interface ISoloDBUpdatingEventContext<'T> with
        member this.CollectionInstance: ISoloDBCollection<'T> = 
            if isNull currentCollection then currentCollection <- createCollection currentSqliteConnection
            currentCollection

        member this.ReadOldItem(): 'T =
            let struct (ptr, len) = jsonOld
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                itemOld <- JsonValue.ParseInto<'T> span
                jsonOld <- struct (NativePtr.nullPtr<byte>, 0)

            itemOld

        member this.ReadNewItem(): 'T =
            let struct (ptr, len) = jsonNew
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                itemNew <- JsonValue.ParseInto<'T> span
                jsonNew <- struct (NativePtr.nullPtr<byte>, 0)

            itemNew

type internal CollectionEventSystem<'T> internal (collectionName: string, eventSystem: EventSystem, createCollection: SqliteConnection -> ISoloDBCollection<'T>) =
    interface ISoloDBCollectionEvents<'T> with
        member this.OnUpdating(handler: UpdatingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.UpdatingHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))

            try
                eventSystem.GlobalLock.Enter()

                let ctxs = ConcurrentStack<SoloDBUpdatingEventContext<'T>>()
                let _added = list.Add(UpdatingHandlerSystem(fun conn session jsonOld jsonOldSize jsonNew jsonNewSize -> 
                    let mutable ctx = Unchecked.defaultof<SoloDBUpdatingEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then
                        ctx <- SoloDBUpdatingEventContext<'T>(createCollection)

                    try 
                        ctx.Reset (conn, session, jsonOld, jsonOldSize, jsonNew, jsonNewSize)

                        handler.Invoke ctx
                    finally ctxs.Push ctx
                    ))
                ()
            finally
                eventSystem.GlobalLock.Exit()
