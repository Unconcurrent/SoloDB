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
    let buildLockedException (ex: exn) =
        match ex with
        | :? SqliteException as se ->
            let message = se.Message
            let isLocked =
                se.SqliteErrorCode = 6 ||
                se.SqliteExtendedErrorCode = 6 ||
                se.SqliteErrorCode = 5 ||
                se.SqliteExtendedErrorCode = 5 ||
                message.IndexOf("database is locked", StringComparison.OrdinalIgnoreCase) >= 0 ||
                message.IndexOf("database table is locked", StringComparison.OrdinalIgnoreCase) >= 0

            if isLocked then
                let wrapped = InvalidOperationException(
                    "Event handlers must use ctx.Database. Using any other SoloDB/collection inside a handler can lock the database. " +
                    $"Original error: {message}",
                    se)
                ValueSome (wrapped :> exn)
            else
                ValueNone
        | _ -> ValueNone
    member val internal GlobalLock = ReentrantSpinLock()
    member val internal InsertingHandlerMapping = CowByteSpanMap<ResizeArray<InsertingHandlerSystem>>()
    member val internal DeletingHandlerMapping = CowByteSpanMap<ResizeArray<DeletingHandlerSystem>>()
    member val internal UpdatingHandlerMapping = CowByteSpanMap<ResizeArray<UpdatingHandlerSystem>>()
    member val internal InsertedHandlerMapping = CowByteSpanMap<ResizeArray<InsertingHandlerSystem>>()
    member val internal DeletedHandlerMapping = CowByteSpanMap<ResizeArray<DeletingHandlerSystem>>()
    member val internal UpdatedHandlerMapping = CowByteSpanMap<ResizeArray<UpdatingHandlerSystem>>()


    /// <summary>
    /// Registers SQLite scalar functions used by the event triggers.
    /// </summary>
    member this.CreateFunctions(connection: SqliteConnection) =
        connection.CreateRawFunction("SHOULD_HANDLE_INSERTING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_INSERTING must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            let should = if this.InsertingHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0
            sqliteCtx.SetInt32 should
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
            let defaultMessage = "SoloDB inserting handler failed"
            let mutable handlerFailureMessage = defaultMessage

            let mutable disableHandle: IDisposable = null
            let mutable restoreTransactionState = false
            let mutable previousTransactionState = false
            let mutable cachedConnection = Unchecked.defaultof<SQLiteTools.CachingDbConnection>
            try 
                this.GlobalLock.Enter()
                match (connection :> obj) with
                | :? SQLiteTools.CachingDbConnection as cachingConn ->
                    cachedConnection <- cachingConn
                    previousTransactionState <- cachingConn.InsideTransaction
                    if not previousTransactionState then
                        cachingConn.InsideTransaction <- true
                        restoreTransactionState <- true
                | _ -> ()
                match (connection :> obj) with
                | :? SQLiteTools.IDisableDispose as disable -> disableHandle <- disable.DisableDispose()
                | _ -> ()

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
                                match h.Invoke(connection, session, newSpan): (SoloDBEventsResult | null) with
                                | EventHandled
                                // Someone will definitely return null from C#.
                                | null -> ()
                                | RemoveHandler -> NativePtr.set handlersToRemove i true
                            with ex ->
                                let finalEx =
                                    match buildLockedException ex with
                                    | ValueSome rewritten -> rewritten
                                    | ValueNone -> ex
                                handlerFailed <- true
                                if not (String.IsNullOrWhiteSpace finalEx.Message) then
                                    handlerFailureMessage <- finalEx.Message
                        i <- i + 1
                finally
                    if not (NativePtr.isNullPtr handlersToRemove) then
                        for j = handlersToRemoveCount - 1 downto 0 do
                            if NativePtr.get handlersToRemove j then
                                handlers.RemoveAt j

                        if handlers.Count = 0 then
                            ignore (this.InsertingHandlerMapping.Remove nameUtf8)

            finally
                if not (isNull disableHandle) then
                    disableHandle.Dispose()
                if restoreTransactionState then
                    cachedConnection.InsideTransaction <- previousTransactionState
                this.GlobalLock.Exit()

            if handlerFailed then
                sqliteCtx.SetText handlerFailureMessage
            else
                sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_DELETING", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_DELETING must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.DeletingHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_DELETING_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonOld ->
            if not sqliteCollectionName.IsText then
                invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"

            if not jsonOld.IsText then
                invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"

            let nameUtf8 = sqliteCollectionName.GetBlobSpan()

            let mutable handlers = Unchecked.defaultof<ResizeArray<DeletingHandlerSystem>>
            let exists = this.DeletingHandlerMapping.TryGetValue(nameUtf8, &handlers)

            match exists with
            | false -> sqliteCtx.SetNull()
            | true ->

            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let oldSpan = ReadOnlySpan<byte>(NativePtr.toVoidPtr oldUtf8, oldUtf8Size)

            let mutable handlerFailed = false
            let defaultMessage = "SoloDB deleting handler failed"
            let mutable handlerFailureMessage = defaultMessage

            let mutable disableHandle: IDisposable = null
            let mutable restoreTransactionState = false
            let mutable previousTransactionState = false
            let mutable cachedConnection = Unchecked.defaultof<SQLiteTools.CachingDbConnection>
            try 
                this.GlobalLock.Enter()
                match (connection :> obj) with
                | :? SQLiteTools.CachingDbConnection as cachingConn ->
                    cachedConnection <- cachingConn
                    previousTransactionState <- cachingConn.InsideTransaction
                    if not previousTransactionState then
                        cachingConn.InsideTransaction <- true
                        restoreTransactionState <- true
                | _ -> ()
                match (connection :> obj) with
                | :? SQLiteTools.IDisableDispose as disable -> disableHandle <- disable.DisableDispose()
                | _ -> ()

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
                                if h.Invoke(connection, session, oldSpan) = RemoveHandler then
                                    NativePtr.set handlersToRemove i true
                            with ex ->
                                let finalEx =
                                    match buildLockedException ex with
                                    | ValueSome rewritten -> rewritten
                                    | ValueNone -> ex
                                handlerFailed <- true
                                if not (String.IsNullOrWhiteSpace finalEx.Message) then
                                    handlerFailureMessage <- finalEx.Message
                        i <- i + 1
                finally
                    if not (NativePtr.isNullPtr handlersToRemove) then
                        for j = handlersToRemoveCount - 1 downto 0 do
                            if NativePtr.get handlersToRemove j then
                                handlers.RemoveAt j

                        if handlers.Count = 0 then
                            ignore (this.DeletingHandlerMapping.Remove nameUtf8)

            finally
                if not (isNull disableHandle) then
                    disableHandle.Dispose()
                if restoreTransactionState then
                    cachedConnection.InsideTransaction <- previousTransactionState
                this.GlobalLock.Exit()

            if handlerFailed then
                sqliteCtx.SetText handlerFailureMessage
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
            let defaultMessage = "SoloDB updating handler failed"
            let mutable handlerFailureMessage = defaultMessage

            let mutable disableHandle: IDisposable = null
            let mutable restoreTransactionState = false
            let mutable previousTransactionState = false
            let mutable cachedConnection = Unchecked.defaultof<SQLiteTools.CachingDbConnection>
            try 
                this.GlobalLock.Enter()
                match (connection :> obj) with
                | :? SQLiteTools.CachingDbConnection as cachingConn ->
                    cachedConnection <- cachingConn
                    previousTransactionState <- cachingConn.InsideTransaction
                    if not previousTransactionState then
                        cachingConn.InsideTransaction <- true
                        restoreTransactionState <- true
                | _ -> ()
                match (connection :> obj) with
                | :? SQLiteTools.IDisableDispose as disable -> disableHandle <- disable.DisableDispose()
                | _ -> ()

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
                                let finalEx =
                                    match buildLockedException ex with
                                    | ValueSome rewritten -> rewritten
                                    | ValueNone -> ex
                                handlerFailed <- true
                                if not (String.IsNullOrWhiteSpace finalEx.Message) then
                                    handlerFailureMessage <- finalEx.Message
                        i <- i + 1
                finally
                    if not (NativePtr.isNullPtr handlersToRemove) then
                        for j = handlersToRemoveCount - 1 downto 0 do
                            if NativePtr.get handlersToRemove j then
                                handlers.RemoveAt j

                        if handlers.Count = 0 then
                            ignore (this.UpdatingHandlerMapping.Remove nameUtf8)

            finally
                if not (isNull disableHandle) then
                    disableHandle.Dispose()
                if restoreTransactionState then
                    cachedConnection.InsideTransaction <- previousTransactionState
                this.GlobalLock.Exit()

            if handlerFailed then
                sqliteCtx.SetText handlerFailureMessage
            else
                sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_INSERTED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_INSERTED must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.InsertedHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_INSERTED_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonNew ->
            if not sqliteCollectionName.IsText then
                invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"

            if not jsonNew.IsText then
                invalidArg (nameof jsonNew) "Second argument must be TEXT JSON (use json(NEW.Value) in trigger)"

            let nameUtf8 = sqliteCollectionName.GetBlobSpan()

            let mutable handlers = Unchecked.defaultof<ResizeArray<InsertingHandlerSystem>>
            let exists = this.InsertedHandlerMapping.TryGetValue(nameUtf8, &handlers)

            match exists with
            | false -> sqliteCtx.SetNull()
            | true ->

            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()
            let newSpan = ReadOnlySpan<byte>(NativePtr.toVoidPtr newUtf8, newUtf8Size)

            let mutable handlerFailed = false
            let defaultMessage = "SoloDB inserted handler failed"
            let mutable handlerFailureMessage = defaultMessage

            let mutable disableHandle: IDisposable = null
            let mutable restoreTransactionState = false
            let mutable previousTransactionState = false
            let mutable cachedConnection = Unchecked.defaultof<SQLiteTools.CachingDbConnection>
            try 
                this.GlobalLock.Enter()
                match (connection :> obj) with
                | :? SQLiteTools.CachingDbConnection as cachingConn ->
                    cachedConnection <- cachingConn
                    previousTransactionState <- cachingConn.InsideTransaction
                    if not previousTransactionState then
                        cachingConn.InsideTransaction <- true
                        restoreTransactionState <- true
                | _ -> ()
                match (connection :> obj) with
                | :? SQLiteTools.IDisableDispose as disable -> disableHandle <- disable.DisableDispose()
                | _ -> ()

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
                                let finalEx =
                                    match buildLockedException ex with
                                    | ValueSome rewritten -> rewritten
                                    | ValueNone -> ex
                                handlerFailed <- true
                                if not (String.IsNullOrWhiteSpace finalEx.Message) then
                                    handlerFailureMessage <- finalEx.Message
                        i <- i + 1
                finally
                    if not (NativePtr.isNullPtr handlersToRemove) then
                        for j = handlersToRemoveCount - 1 downto 0 do
                            if NativePtr.get handlersToRemove j then
                                handlers.RemoveAt j

                        if handlers.Count = 0 then
                            ignore (this.InsertedHandlerMapping.Remove nameUtf8)

            finally
                if not (isNull disableHandle) then
                    disableHandle.Dispose()
                if restoreTransactionState then
                    cachedConnection.InsideTransaction <- previousTransactionState
                this.GlobalLock.Exit()

            if handlerFailed then
                sqliteCtx.SetText handlerFailureMessage
            else
                sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_DELETED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_DELETED must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.DeletedHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_DELETED_HANDLER", RawScalarFunc2(fun sqliteCtx sqliteCollectionName jsonOld ->
            if not sqliteCollectionName.IsText then
                invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"

            if not jsonOld.IsText then
                invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"

            let nameUtf8 = sqliteCollectionName.GetBlobSpan()

            let mutable handlers = Unchecked.defaultof<ResizeArray<DeletingHandlerSystem>>
            let exists = this.DeletedHandlerMapping.TryGetValue(nameUtf8, &handlers)

            match exists with
            | false -> sqliteCtx.SetNull()
            | true ->

            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()
            let oldSpan = ReadOnlySpan<byte>(NativePtr.toVoidPtr oldUtf8, oldUtf8Size)

            let mutable handlerFailed = false
            let defaultMessage = "SoloDB deleted handler failed"
            let mutable handlerFailureMessage = defaultMessage

            let mutable disableHandle: IDisposable = null
            let mutable restoreTransactionState = false
            let mutable previousTransactionState = false
            let mutable cachedConnection = Unchecked.defaultof<SQLiteTools.CachingDbConnection>
            try 
                this.GlobalLock.Enter()
                match (connection :> obj) with
                | :? SQLiteTools.CachingDbConnection as cachingConn ->
                    cachedConnection <- cachingConn
                    previousTransactionState <- cachingConn.InsideTransaction
                    if not previousTransactionState then
                        cachingConn.InsideTransaction <- true
                        restoreTransactionState <- true
                | _ -> ()
                match (connection :> obj) with
                | :? SQLiteTools.IDisableDispose as disable -> disableHandle <- disable.DisableDispose()
                | _ -> ()

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
                                if h.Invoke(connection, session, oldSpan) = RemoveHandler then
                                    NativePtr.set handlersToRemove i true
                            with ex ->
                                let finalEx =
                                    match buildLockedException ex with
                                    | ValueSome rewritten -> rewritten
                                    | ValueNone -> ex
                                handlerFailed <- true
                                if not (String.IsNullOrWhiteSpace finalEx.Message) then
                                    handlerFailureMessage <- finalEx.Message
                        i <- i + 1
                finally
                    if not (NativePtr.isNullPtr handlersToRemove) then
                        for j = handlersToRemoveCount - 1 downto 0 do
                            if NativePtr.get handlersToRemove j then
                                handlers.RemoveAt j

                        if handlers.Count = 0 then
                            ignore (this.DeletedHandlerMapping.Remove nameUtf8)

            finally
                if not (isNull disableHandle) then
                    disableHandle.Dispose()
                if restoreTransactionState then
                    cachedConnection.InsideTransaction <- previousTransactionState
                this.GlobalLock.Exit()

            if handlerFailed then
                sqliteCtx.SetText handlerFailureMessage
            else
                sqliteCtx.SetNull()
        ))

        connection.CreateRawFunction("SHOULD_HANDLE_UPDATED", RawScalarFunc1(fun sqliteCtx sqliteCollectionName ->
            if not sqliteCollectionName.IsText then invalidArg (nameof sqliteCollectionName) "The first argument of SHOULD_HANDLE_UPDATED must be a string"

            let sqliteCollectionNameUTF8 = sqliteCollectionName.GetBlobSpan()
            sqliteCtx.SetInt32(if this.UpdatedHandlerMapping.ContainsKey sqliteCollectionNameUTF8 then 1 else 0)
        ))

        connection.CreateRawFunction("ON_UPDATED_HANDLER", RawScalarFunc3(fun sqliteCtx sqliteCollectionName jsonOld jsonNew -> 
            if not sqliteCollectionName.IsText then
                invalidArg (nameof sqliteCollectionName) "First argument must be TEXT collection name"

            if not jsonOld.IsText then
                invalidArg (nameof jsonOld) "Second argument must be TEXT JSON (use json(OLD.Value) in trigger)"

            if not jsonNew.IsText then
                invalidArg (nameof jsonNew) "Third argument must be TEXT JSON (use json(NEW.Value) in trigger)"

            let nameUtf8 = sqliteCollectionName.GetBlobSpan()

            let mutable handlers = Unchecked.defaultof<ResizeArray<UpdatingHandlerSystem>>
            let exists = this.UpdatedHandlerMapping.TryGetValue(nameUtf8, &handlers)

            match exists with
            | false -> sqliteCtx.SetNull()
            | true ->

            let oldUtf8Size = jsonOld.GetByteCount()
            let oldUtf8 = jsonOld.GetBlobPointer()

            let newUtf8Size = jsonNew.GetByteCount()
            let newUtf8 = jsonNew.GetBlobPointer()

            let mutable handlerFailed = false
            let defaultMessage = "SoloDB updated handler failed"
            let mutable handlerFailureMessage = defaultMessage

            let mutable disableHandle: IDisposable = null
            let mutable restoreTransactionState = false
            let mutable previousTransactionState = false
            let mutable cachedConnection = Unchecked.defaultof<SQLiteTools.CachingDbConnection>
            try 
                this.GlobalLock.Enter()
                match (connection :> obj) with
                | :? SQLiteTools.CachingDbConnection as cachingConn ->
                    cachedConnection <- cachingConn
                    previousTransactionState <- cachingConn.InsideTransaction
                    if not previousTransactionState then
                        cachingConn.InsideTransaction <- true
                        restoreTransactionState <- true
                | _ -> ()
                match (connection :> obj) with
                | :? SQLiteTools.IDisableDispose as disable -> disableHandle <- disable.DisableDispose()
                | _ -> ()

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
                                let finalEx =
                                    match buildLockedException ex with
                                    | ValueSome rewritten -> rewritten
                                    | ValueNone -> ex
                                handlerFailed <- true
                                if not (String.IsNullOrWhiteSpace finalEx.Message) then
                                    handlerFailureMessage <- finalEx.Message
                        i <- i + 1
                finally
                    if not (NativePtr.isNullPtr handlersToRemove) then
                        for j = handlersToRemoveCount - 1 downto 0 do
                            if NativePtr.get handlersToRemove j then
                                handlers.RemoveAt j

                        if handlers.Count = 0 then
                            ignore (this.UpdatedHandlerMapping.Remove nameUtf8)

            finally
                if not (isNull disableHandle) then
                    disableHandle.Dispose()
                if restoreTransactionState then
                    cachedConnection.InsideTransaction <- previousTransactionState
                this.GlobalLock.Exit()

            if handlerFailed then
                sqliteCtx.SetText handlerFailureMessage
            else
                sqliteCtx.SetNull()
        ))
        ()

/// <summary>
/// Provides cached access to item event data for a single handler invocation.
/// </summary>
type internal SoloDBItemEventContext<'T> internal (collectionName: string, createDb: SqliteConnection -> (unit -> unit) -> ISoloDB) =
    let mutable currentSqliteConnection = Unchecked.defaultof<SqliteConnection>
    let mutable currentDb = Unchecked.defaultof<ISoloDB | null>
    let mutable previousSession = -1L
    let mutable json = struct (NativePtr.nullPtr<byte>, 0)
    let mutable item: 'T = Unchecked.defaultof<'T>
    let mutable disposed = true
    let disposedMessage = "The event context database can only be used during the handler execution."

    member private this.ReadDB() =
        this.ThrowIfDisposed()
        if isNull currentDb then currentDb <- createDb currentSqliteConnection this.ThrowIfDisposed
        currentDb

    member private this.ThrowIfDisposed() =
        if disposed then
            raise (ObjectDisposedException("EventContextDatabase", disposedMessage))

    /// <summary>
    /// Initializes the context for the given SQLite trigger invocation.
    /// </summary>
    member this.Reset (connection: SqliteConnection, session: int64, jsonBytes: nativeptr<byte>, jsonSize: int) =
        disposed <- false
        if previousSession <> session then
            previousSession <- session
            currentDb <- null
            currentSqliteConnection <- connection
            json <- struct (jsonBytes, jsonSize)

    /// <summary>
    /// Marks the context as disposed after handler execution.
    /// </summary>
    member this.MarkDisposed() =
        disposed <- true

    interface ISoloDBItemEventContext<'T> with
        member this.CollectionName: string =
            collectionName

        member this.Item with get(): 'T =
            this.ThrowIfDisposed()
            let struct (ptr, len) = json
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                item <- JsonValue.ParseInto<'T> span
                json <- struct (NativePtr.nullPtr<byte>, 0)

            item

    interface ISoloDB with
        member this.ConnectionString: string = this.ReadDB().ConnectionString
        member this.FileSystem: IFileSystem = this.ReadDB().FileSystem
        member this.GetCollection<'A> () = this.ReadDB().GetCollection<'A> ()
        member this.GetCollection<'A> (name: string) = this.ReadDB().GetCollection<'A> (name)
        member this.GetUntypedCollection (name) = this.ReadDB().GetUntypedCollection(name)
        member this.CollectionExists (name) = this.ReadDB().CollectionExists (name)
        member this.CollectionExists<'A> () = this.ReadDB().CollectionExists<'A> ()
        member this.DropCollectionIfExists (name) = this.ReadDB().DropCollectionIfExists (name)
        member this.DropCollectionIfExists<'A> () = this.ReadDB().DropCollectionIfExists<'A> () 
        member this.DropCollection (name) = this.ReadDB().DropCollection (name)
        member this.DropCollection<'A> () = this.ReadDB().DropCollection<'A> ()
        member this.ListCollectionNames () = this.ReadDB().ListCollectionNames ()
        member this.Optimize () = this.ReadDB().Optimize ()
        member this.Dispose () = this.ReadDB().Dispose ()
        

/// <summary>
/// Provides cached access to update event data for a single handler invocation.
/// </summary>
type internal SoloDBUpdatingEventContext<'T> internal (collectionName: string, createDb: SqliteConnection -> (unit -> unit) -> ISoloDB) =
    let mutable currentSqliteConnection = Unchecked.defaultof<SqliteConnection>
    let mutable currentDb = Unchecked.defaultof<ISoloDB | null>
    let mutable previousSession = -1L
    let mutable jsonOld = struct (NativePtr.nullPtr<byte>, 0)
    let mutable jsonNew = struct (NativePtr.nullPtr<byte>, 0)

    let mutable itemOld: 'T = Unchecked.defaultof<'T>
    let mutable itemNew: 'T = Unchecked.defaultof<'T>
    let mutable disposed = true
    let disposedMessage = "The event context database can only be used during the handler execution."

    member private this.ReadDB() =
        this.ThrowIfDisposed()
        if isNull currentDb then currentDb <- createDb currentSqliteConnection this.ThrowIfDisposed
        currentDb

    member private this.ThrowIfDisposed() =
        if disposed then
            raise (ObjectDisposedException("EventContextDatabase", disposedMessage))

    /// <summary>
    /// Initializes the context for the given SQLite trigger invocation.
    /// </summary>
    member this.Reset (connection: SqliteConnection, session: int64, jsonOldBytes: nativeptr<byte>, jsonOldSize: int, jsonNewBytes: nativeptr<byte>, jsonNewSize: int) =
        disposed <- false
        if previousSession <> session then
            previousSession <- session
            currentDb <- null
            currentSqliteConnection <- connection
            jsonOld <- struct (jsonOldBytes, jsonOldSize)
            jsonNew <- struct (jsonNewBytes, jsonNewSize)
            itemOld <- Unchecked.defaultof<'T>
            itemNew <- Unchecked.defaultof<'T>

    /// <summary>
    /// Marks the context as disposed after handler execution.
    /// </summary>
    member this.MarkDisposed() =
        disposed <- true

    interface ISoloDBUpdatingEventContext<'T> with
        member this.CollectionName: string =
            collectionName

        member this.OldItem with get(): 'T =
            this.ThrowIfDisposed()
            let struct (ptr, len) = jsonOld
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                itemOld <- JsonValue.ParseInto<'T> span
                jsonOld <- struct (NativePtr.nullPtr<byte>, 0)

            itemOld

        member this.Item with get(): 'T =
            this.ThrowIfDisposed()
            let struct (ptr, len) = jsonNew
            if len <> 0 then
                let span = ReadOnlySpan<byte>(NativePtr.toVoidPtr ptr, len)
                itemNew <- JsonValue.ParseInto<'T> span
                jsonNew <- struct (NativePtr.nullPtr<byte>, 0)

            itemNew

    interface ISoloDB with
        member this.ConnectionString: string = this.ReadDB().ConnectionString
        member this.FileSystem: IFileSystem = this.ReadDB().FileSystem
        member this.GetCollection<'A> () = this.ReadDB().GetCollection<'A> ()
        member this.GetCollection<'A> (name: string) = this.ReadDB().GetCollection<'A> (name)
        member this.GetUntypedCollection (name) = this.ReadDB().GetUntypedCollection(name)
        member this.CollectionExists (name) = this.ReadDB().CollectionExists (name)
        member this.CollectionExists<'A> () = this.ReadDB().CollectionExists<'A> ()
        member this.DropCollectionIfExists (name) = this.ReadDB().DropCollectionIfExists (name)
        member this.DropCollectionIfExists<'A> () = this.ReadDB().DropCollectionIfExists<'A> () 
        member this.DropCollection (name) = this.ReadDB().DropCollection (name)
        member this.DropCollection<'A> () = this.ReadDB().DropCollection<'A> ()
        member this.ListCollectionNames () = this.ReadDB().ListCollectionNames ()
        member this.Optimize () = this.ReadDB().Optimize ()
        member this.Dispose () = this.ReadDB().Dispose ()

/// <summary>
/// Collection-scoped event system for registering and unregistering handlers.
/// </summary>
type internal CollectionEventSystem<'T> internal (collectionName: string, eventSystem: EventSystem, createDb: SqliteConnection -> (unit -> unit) -> ISoloDB) =
    let insertingHandlerMap = Dictionary<InsertingHandler<'T>, ResizeArray<InsertingHandlerSystem>>(HashIdentity.Reference)
    let deletingHandlerMap = Dictionary<DeletingHandler<'T>, ResizeArray<DeletingHandlerSystem>>(HashIdentity.Reference)
    let updatingHandlerMap = Dictionary<UpdatingHandler<'T>, ResizeArray<UpdatingHandlerSystem>>(HashIdentity.Reference)
    let insertedHandlerMap = Dictionary<InsertedHandler<'T>, ResizeArray<InsertingHandlerSystem>>(HashIdentity.Reference)
    let deletedHandlerMap = Dictionary<DeletedHandler<'T>, ResizeArray<DeletingHandlerSystem>>(HashIdentity.Reference)
    let updatedHandlerMap = Dictionary<UpdatedHandler<'T>, ResizeArray<UpdatingHandlerSystem>>(HashIdentity.Reference)
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
                        ctx <- new SoloDBItemEventContext<'T>(collectionName, createDb)

                    let jsonPtr =
                        if json.IsEmpty then NativePtr.nullPtr<byte>
                        else NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try 
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            handler.Invoke ctx
                        finally
                            ctx.MarkDisposed()
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
        /// Registers a handler for delete events on this collection.
        /// </summary>
        member this.OnDeleting(handler: DeletingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.DeletingHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))

            try
                eventSystem.GlobalLock.Enter()

                let ctxs = ConcurrentStack<SoloDBItemEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<DeletingHandlerSystem>

                let removeFromMap () =
                    match deletingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then
                            deletingHandlerMap.Remove handler |> ignore
                    | _ -> ()

                let sysHandler = DeletingHandlerSystem(fun conn session json ->
                    let mutable ctx = Unchecked.defaultof<SoloDBItemEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then
                        ctx <- new SoloDBItemEventContext<'T>(collectionName, createDb)

                    let jsonPtr =
                        if json.IsEmpty then NativePtr.nullPtr<byte>
                        else NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try 
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            handler.Invoke ctx
                        finally
                            ctx.MarkDisposed()
                            ctxs.Push ctx

                    if result = RemoveHandler then
                        removeFromMap ()

                    result
                    )

                sysHandlerRef <- sysHandler

                let _added = list.Add(sysHandler)

                match deletingHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    deletingHandlerMap.[handler] <- handlers
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
                        ctx <- new SoloDBUpdatingEventContext<'T>(collectionName, createDb)

                    let result =
                        try 
                            ctx.Reset (conn, session, jsonOld, jsonOldSize, jsonNew, jsonNewSize)
                            handler.Invoke ctx
                        finally
                            ctx.MarkDisposed()
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
        /// Registers a handler for after-insert events on this collection.
        /// </summary>
        member this.OnInserted(handler: InsertedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.InsertedHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))

            try
                eventSystem.GlobalLock.Enter()

                let ctxs = ConcurrentStack<SoloDBItemEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<InsertingHandlerSystem>

                let removeFromMap () =
                    match insertedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then
                            insertedHandlerMap.Remove handler |> ignore
                    | _ -> ()

                let sysHandler = InsertingHandlerSystem(fun conn session json ->
                    let mutable ctx = Unchecked.defaultof<SoloDBItemEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then
                        ctx <- new SoloDBItemEventContext<'T>(collectionName, createDb)

                    let jsonPtr =
                        if json.IsEmpty then NativePtr.nullPtr<byte>
                        else NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try 
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            handler.Invoke ctx
                        finally
                            ctx.MarkDisposed()
                            ctxs.Push ctx

                    if result = RemoveHandler then
                        removeFromMap ()

                    result
                    )

                sysHandlerRef <- sysHandler

                let _added = list.Add(sysHandler)

                match insertedHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    insertedHandlerMap.[handler] <- handlers
                ()
            finally
                eventSystem.GlobalLock.Exit()

        /// <summary>
        /// Registers a handler for after-delete events on this collection.
        /// </summary>
        member this.OnDeleted(handler: DeletedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.DeletedHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))

            try
                eventSystem.GlobalLock.Enter()

                let ctxs = ConcurrentStack<SoloDBItemEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<DeletingHandlerSystem>

                let removeFromMap () =
                    match deletedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then
                            deletedHandlerMap.Remove handler |> ignore
                    | _ -> ()

                let sysHandler = DeletingHandlerSystem(fun conn session json ->
                    let mutable ctx = Unchecked.defaultof<SoloDBItemEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then
                        ctx <- new SoloDBItemEventContext<'T>(collectionName, createDb)

                    let jsonPtr =
                        if json.IsEmpty then NativePtr.nullPtr<byte>
                        else NativePtr.ofVoidPtr (Unsafe.AsPointer(&MemoryMarshal.GetReference json))
                    let result =
                        try 
                            ctx.Reset (conn, session, jsonPtr, json.Length)
                            handler.Invoke ctx
                        finally
                            ctx.MarkDisposed()
                            ctxs.Push ctx

                    if result = RemoveHandler then
                        removeFromMap ()

                    result
                    )

                sysHandlerRef <- sysHandler

                let _added = list.Add(sysHandler)

                match deletedHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    deletedHandlerMap.[handler] <- handlers
                ()
            finally
                eventSystem.GlobalLock.Exit()

        /// <summary>
        /// Registers a handler for after-update events on this collection.
        /// </summary>
        member this.OnUpdated(handler: UpdatedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            let list = eventSystem.UpdatedHandlerMapping.GetOrAdd(collectionNameBytes, CowByteSpanMapValueFactory(fun _span -> ResizeArray()))

            try
                eventSystem.GlobalLock.Enter()

                let ctxs = ConcurrentStack<SoloDBUpdatingEventContext<'T>>()
                let mutable sysHandlerRef = Unchecked.defaultof<UpdatingHandlerSystem>

                let removeFromMap () =
                    match updatedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        handlers.Remove sysHandlerRef |> ignore
                        if handlers.Count = 0 then
                            updatedHandlerMap.Remove handler |> ignore
                    | _ -> ()

                let sysHandler = UpdatingHandlerSystem(fun conn session jsonOld jsonOldSize jsonNew jsonNewSize -> 
                    let mutable ctx = Unchecked.defaultof<SoloDBUpdatingEventContext<'T> | null>
                    if not (ctxs.TryPop(&ctx)) then
                        ctx <- new SoloDBUpdatingEventContext<'T>(collectionName, createDb)

                    let result =
                        try 
                            ctx.Reset (conn, session, jsonOld, jsonOldSize, jsonNew, jsonNewSize)
                            handler.Invoke ctx
                        finally
                            ctx.MarkDisposed()
                            ctxs.Push ctx

                    if result = RemoveHandler then
                        removeFromMap ()

                    result
                    )

                sysHandlerRef <- sysHandler

                let _added = list.Add(sysHandler)

                match updatedHandlerMap.TryGetValue handler with
                | true, handlers -> handlers.Add sysHandler
                | false, _ ->
                    let handlers = ResizeArray()
                    handlers.Add sysHandler
                    updatedHandlerMap.[handler] <- handlers
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
        /// Unregisters a previously registered delete handler.
        /// </summary>
        member this.Unregister(handler: DeletingHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            try
                eventSystem.GlobalLock.Enter()

                let mutable list = Unchecked.defaultof<ResizeArray<DeletingHandlerSystem>>
                if eventSystem.DeletingHandlerMapping.TryGetValue(collectionNameBytes, &list) then
                    match deletingHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        for sysHandler in handlers do
                            for i = list.Count - 1 downto 0 do
                                if Object.ReferenceEquals(list.[i], sysHandler) then
                                    list.RemoveAt i

                        deletingHandlerMap.Remove handler |> ignore

                        if list.Count = 0 then
                            ignore (eventSystem.DeletingHandlerMapping.Remove collectionNameBytes)
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

        /// <summary>
        /// Unregisters a previously registered after-insert handler.
        /// </summary>
        member this.Unregister(handler: InsertedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            try
                eventSystem.GlobalLock.Enter()

                let mutable list = Unchecked.defaultof<ResizeArray<InsertingHandlerSystem>>
                if eventSystem.InsertedHandlerMapping.TryGetValue(collectionNameBytes, &list) then
                    match insertedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        for sysHandler in handlers do
                            for i = list.Count - 1 downto 0 do
                                if Object.ReferenceEquals(list.[i], sysHandler) then
                                    list.RemoveAt i

                        insertedHandlerMap.Remove handler |> ignore

                        if list.Count = 0 then
                            ignore (eventSystem.InsertedHandlerMapping.Remove collectionNameBytes)
                    | false, _ -> ()
            finally
                eventSystem.GlobalLock.Exit()

        /// <summary>
        /// Unregisters a previously registered after-delete handler.
        /// </summary>
        member this.Unregister(handler: DeletedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            try
                eventSystem.GlobalLock.Enter()

                let mutable list = Unchecked.defaultof<ResizeArray<DeletingHandlerSystem>>
                if eventSystem.DeletedHandlerMapping.TryGetValue(collectionNameBytes, &list) then
                    match deletedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        for sysHandler in handlers do
                            for i = list.Count - 1 downto 0 do
                                if Object.ReferenceEquals(list.[i], sysHandler) then
                                    list.RemoveAt i

                        deletedHandlerMap.Remove handler |> ignore

                        if list.Count = 0 then
                            ignore (eventSystem.DeletedHandlerMapping.Remove collectionNameBytes)
                    | false, _ -> ()
            finally
                eventSystem.GlobalLock.Exit()

        /// <summary>
        /// Unregisters a previously registered after-update handler.
        /// </summary>
        member this.Unregister(handler: UpdatedHandler<'T>): unit =
            let collectionNameBytes = Encoding.UTF8.GetBytes collectionName
            try
                eventSystem.GlobalLock.Enter()

                let mutable list = Unchecked.defaultof<ResizeArray<UpdatingHandlerSystem>>
                if eventSystem.UpdatedHandlerMapping.TryGetValue(collectionNameBytes, &list) then
                    match updatedHandlerMap.TryGetValue handler with
                    | true, handlers ->
                        for sysHandler in handlers do
                            for i = list.Count - 1 downto 0 do
                                if Object.ReferenceEquals(list.[i], sysHandler) then
                                    list.RemoveAt i

                        updatedHandlerMap.Remove handler |> ignore

                        if list.Count = 0 then
                            ignore (eventSystem.UpdatedHandlerMapping.Remove collectionNameBytes)
                    | false, _ -> ()
            finally
                eventSystem.GlobalLock.Exit()
