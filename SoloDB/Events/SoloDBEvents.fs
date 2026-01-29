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
open System.Runtime.InteropServices
open Microsoft.FSharp.NativeInterop
open Utils
open System.Threading
open SoloDatabase.JsonSerializator
open System.Text

#nowarn "9"

/// <summary>
/// Internal event system that hosts SQLite functions and handler mappings.
/// </summary>
type internal EventSystem internal () =
    let mutable sessionIndex = 0L
    member val internal GlobalLock = ReentrantSpinLock()
    member val internal InsertingHandlerMapping = CowByteSpanMap<ResizeArray<InsertingHandlerSystem>>()
    member val internal UpdatingHandlerMapping = CowByteSpanMap<ResizeArray<UpdatingHandlerSystem>>()

    /// <summary>
    /// Registers SQLite scalar functions used by the event triggers.
    /// </summary>
    member this.CreateFunctions(connection: SqliteConnection) =
        connection.CreateRawFunction("SHOULD_HANDLE_INSERTING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_INSERTING must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.InsertingHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_INSERTING_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonNew ->
            if not sqliteCollectionName.IsText then
                invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"

            if not jsonNew.IsText then
                invalidArg (nameof jsonNew) "Second argument must be TEXT JSON (use json(NEW.Value) in trigger)"

            let nameUtf8 = sqliteCollectionName.GetBlobSpan()

            let mutable handlers = Unchecked.defaultof<ResizeArray<InsertingHandlerSystem>>
            let exists = this.InsertingHandlerMapping.TryGetValue(nameUtf8, &handlers)

            match exists with
            | false -> sqliteCtx.SetNull()
            | true ->

            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let newSpan = ReadOnlySpan<byte>(NativePtr.toVoidPtr newUtf8, newUtf8Size)

            let mutable handlerFailed = false
            let mutable handlerFailureMessage = ""

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
                                if h.Invoke(connection, session, newSpan) = RemoveHandler then
                                    NativePtr.set handlersToRemove i true
                            with ex ->
                                handlerFailed <- true
                                handlerFailureMessage <- ex.Message
                        i <- i + 1
                finally
                    if not (NativePtr.isNullPtr handlersToRemove) then
                        for j = handlersToRemoveCount - 1 downto 0 do
                            if NativePtr.get handlersToRemove j then
                                handlers.RemoveAt j

                        if handlers.Count = 0 then
                            ignore (this.InsertingHandlerMapping.Remove nameUtf8)

            finally
                this.GlobalLock.Exit()

            if handlerFailed then
                let msg =
                    if String.IsNullOrWhiteSpace handlerFailureMessage then
                        "SoloDB inserting handler failed"
                    else handlerFailureMessage
                sqliteCtx.SetText msg
            else
                sqliteCtx.SetNull()
        ))

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
            | false -> sqliteCtx.SetNull()
            | true ->
            
            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()

            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()

            let mutable handlerFailed = false
            let mutable handlerFailureMessage = ""

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
                            with ex ->
                                handlerFailed <- true
                                handlerFailureMessage <- ex.Message
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

            if handlerFailed then
                let msg =
                    if String.IsNullOrWhiteSpace handlerFailureMessage then
                        "SoloDB updating handler failed"
                    else handlerFailureMessage
                sqliteCtx.SetText msg
            else
                sqliteCtx.SetNull()
        ))
        ()

/// <summary>
/// Provides cached access to item event data for a single handler invocation.
/// </summary>
type internal SoloDBItemEventContext<'T> internal (createCollection: SqliteConnection -> ISoloDBCollection<'T>) =
    let mutable currentSqliteConnection = Unchecked.defaultof<SqliteConnection>
    let mutable currentCollection = Unchecked.defaultof<ISoloDBCollection<'T> | null>
    let mutable previousSession = -1L
    let mutable json = struct (NativePtr.nullPtr<byte>, 0)
    let mutable item: 'T = Unchecked.defaultof<'T>

    /// <summary>
    /// Initializes the context for the given SQLite trigger invocation.
    /// </summary>
    member this.Reset (connection, session: int64, jsonBytes: nativeptr<byte>, jsonSize: int) =
        if previousSession <> session then
            previousSession <- session
            currentCollection <- null
            currentSqliteConnection <- connection
            json <- struct (jsonBytes, jsonSize)

    interface ISoloDBItemEventContext<'T> with
        member this.CollectionInstance: ISoloDBCollection<'T> = 
            if isNull currentCollection then currentCollection <- createCollection currentSqliteConnection
            currentCollection

        member this.ReadItem(): 'T =
            let struct (ptr, len) = json
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                item <- JsonValue.ParseInto<'T> span
                json <- struct (NativePtr.nullPtr<byte>, 0)

            item

/// <summary>
/// Provides cached access to update event data for a single handler invocation.
/// </summary>
type internal SoloDBUpdatingEventContext<'T> internal (createCollection: SqliteConnection -> ISoloDBCollection<'T>) =
    let mutable currentSqliteConnection = Unchecked.defaultof<SqliteConnection>
    let mutable currentCollection = Unchecked.defaultof<ISoloDBCollection<'T> | null>
    let mutable previousSession = -1L
    let mutable jsonOld = struct (NativePtr.nullPtr<byte>, 0)
    let mutable jsonNew = struct (NativePtr.nullPtr<byte>, 0)

    let mutable itemOld: 'T = Unchecked.defaultof<'T>
    let mutable itemNew: 'T = Unchecked.defaultof<'T>

    /// <summary>
    /// Initializes the context for the given SQLite trigger invocation.
    /// </summary>
    member this.Reset (connection, session: int64, jsonOldBytes: nativeptr<byte>, jsonOldSize: int, jsonNewBytes: nativeptr<byte>, jsonNewSize: int) =
        if previousSession <> session then
            previousSession <- session
            currentCollection <- null
            currentSqliteConnection <- connection
            jsonOld <- struct (jsonOldBytes, jsonOldSize)
            jsonNew <- struct (jsonNewBytes, jsonNewSize)
            itemOld <- Unchecked.defaultof<'T>
            itemNew <- Unchecked.defaultof<'T>

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

/// <summary>
/// Collection-scoped event system for registering and unregistering handlers.
/// </summary>
type internal CollectionEventSystem<'T> internal (collectionName: string, eventSystem: EventSystem, createCollection: SqliteConnection -> ISoloDBCollection<'T>) =
    let insertingHandlerMap = Dictionary<InsertingHandler<'T>, ResizeArray<InsertingHandlerSystem>>(HashIdentity.Reference)
    let updatingHandlerMap = Dictionary<UpdatingHandler<'T>, ResizeArray<UpdatingHandlerSystem>>(HashIdentity.Reference)
    interface ISoloDBCollectionEvents<'T> with
        /// <summary>
        /// Registers a handler for insert events on this collection.
        /// </summary>
        member this.OnInserting(handler: InsertingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.InsertingHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))

            try
                eventSystem.GlobalLock.Enter()

                let ctxs = ConcurrentStack<SoloDBItemEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<InsertingHandlerSystem>

                let removeFromMap () =
                    match insertingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then
                            insertingHandlerMap.Remove handler |> ignore
                    | _ -> ()

                let sysHandler = InsertingHandlerSystem(fun conn session json ->
                    let mutable ctx = Unchecked.defaultof<SoloDBItemEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then
                        ctx <- SoloDBItemEventContext<'T>(createCollection)

                    let jsonPtr = NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try 
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            handler.Invoke ctx
                        finally
                            ctxs.Push ctx

                    if result = RemoveHandler then
                        removeFromMap ()

                    result
                    )

                sysHandlerRef <- sysHandler

                let _added = list.Add(sysHandler)

                match insertingHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    insertingHandlerMap.[handler] <- handlers
                ()
            finally
                eventSystem.GlobalLock.Exit()

        /// <summary>
        /// Registers a handler for update events on this collection.
        /// </summary>
        member this.OnUpdating(handler: UpdatingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.UpdatingHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))

            try
                eventSystem.GlobalLock.Enter()

                let ctxs = ConcurrentStack<SoloDBUpdatingEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<UpdatingHandlerSystem>

                let removeFromMap () =
                    match updatingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then
                            updatingHandlerMap.Remove handler |> ignore
                    | _ -> ()

                let sysHandler = UpdatingHandlerSystem(fun conn session jsonOld jsonOldSize jsonNew jsonNewSize -> 
                    let mutable ctx = Unchecked.defaultof<SoloDBUpdatingEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then
                        ctx <- SoloDBUpdatingEventContext<'T>(createCollection)

                    let result =
                        try 
                            ctx.Reset (conn, session, jsonOld, jsonOldSize, jsonNew, jsonNewSize)
                            handler.Invoke ctx
                        finally
                            ctxs.Push ctx

                    if result = RemoveHandler then
                        removeFromMap ()

                    result
                    )

                sysHandlerRef <- sysHandler

                let _added = list.Add(sysHandler)

                match updatingHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    updatingHandlerMap.[handler] <- handlers
                ()
            finally
                eventSystem.GlobalLock.Exit()

        /// <summary>
        /// Unregisters a previously registered insert handler.
        /// </summary>
        member this.Unregister(handler: InsertingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            try
                eventSystem.GlobalLock.Enter()

                let mutable list = Unchecked.defaultof<ResizeArray<InsertingHandlerSystem>>
                if eventSystem.InsertingHandlerMapping.TryGetValue(collectionNameBytes, &list) then
                    match insertingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        for sysHandler in handlers do
                            for i = list.Count - 1 downto 0 do
                                if Object.ReferenceEquals(list.[i], sysHandler) then
                                    list.RemoveAt i

                        insertingHandlerMap.Remove handler |> ignore

                        if list.Count = 0 then
                            ignore (eventSystem.InsertingHandlerMapping.Remove collectionNameBytes)
                    | false, _ -> ()
            finally
                eventSystem.GlobalLock.Exit()

        /// <summary>
        /// Unregisters a previously registered update handler.
        /// </summary>
        member this.Unregister(handler: UpdatingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            try
                eventSystem.GlobalLock.Enter()

                let mutable list = Unchecked.defaultof<ResizeArray<UpdatingHandlerSystem>>
                if eventSystem.UpdatingHandlerMapping.TryGetValue(collectionNameBytes, &list) then
                    match updatingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        for sysHandler in handlers do
                            for i = list.Count - 1 downto 0 do
                                if Object.ReferenceEquals(list.[i], sysHandler) then
                                    list.RemoveAt i

                        updatingHandlerMap.Remove handler |> ignore

                        if list.Count = 0 then
                            ignore (eventSystem.UpdatingHandlerMapping.Remove collectionNameBytes)
                    | false, _ -> ()
            finally
                eventSystem.GlobalLock.Exit()
