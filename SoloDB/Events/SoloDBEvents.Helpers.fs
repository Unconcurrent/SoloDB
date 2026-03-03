namespace SoloDatabase

open CowByteSpanMap
open Microsoft.Data.Sqlite
open System.Collections.Generic
open System
open SoloDatabase.Types
open Microsoft.FSharp.NativeInterop
open Utils
open System.Threading

// NativePtr operations for zero-copy JSON parsing from SQLite trigger callbacks
#nowarn "9"

module internal SoloDBEventsHelpers =
    let private callbackScopeDepth = new ThreadLocal<int>(fun () -> 0)
    let private deferredRecursiveRemovals = new ThreadLocal<ResizeArray<obj * obj>>(fun () -> ResizeArray<obj * obj>())

    let private unregisterInsideHandlerMessage =
        "Cannot call Unregister from within a handler. Return SoloDBEventsResult.RemoveHandler instead."

    let internal redirectException (ex: exn) =
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
                    "Event handlers must use the ctx parameter (which implements ISoloDB). Using any other SoloDB instance inside a handler will lock the database. " +
                    $"Original error: {message}",
                    se)
                ValueSome (wrapped :> exn)
            else
                ValueNone

        | _ -> ValueNone

    let internal runHandlerBody<'THandler>
        (globalLock: ReentrantSpinLock)
        (sessionIndex: int64 byref)
        (connection: SqliteConnection)
        (handlers: ResizeArray<'THandler>)
        (invokeHandler: 'THandler -> SqliteConnection -> int64 -> SoloDBEventsResult)
        (removeMapping: unit -> unit)
        (defaultMessage: string) =

        let mutable handlerFailed = false
        let mutable handlerFailureMessage = defaultMessage

        let mutable restoreTransactionState = false
        let mutable previousTransactionState = false
        let mutable cachedConnection = Unchecked.defaultof<SQLiteTools.CachingDbConnection>
        try
            // EVENT-PATH INVARIANT 1 of 3: GlobalLock serialization.
            // All handler invocations are serialized under ReentrantSpinLock.
            // This prevents concurrent handler mutation and ensures handler-list
            // iteration is safe. The lock is reentrant so triggers that fire
            // during handler execution (via ISoloDB proxy writes) do not deadlock.
            globalLock.Enter()

            // EVENT-PATH INVARIANT 2 of 3: InsideTransaction flag override.
            // Forces InsideTransaction=true on the CachingDbConnection so that
            // any Collection API called from within the handler sees itself as
            // "in transaction" and skips auto-transaction creation (which would
            // attempt BEGIN IMMEDIATE inside an already-active transaction and fail).
            // The original state is saved and restored in the finally block.
            // InsideTransaction override must remain: it serves Dispose suppression for event handlers.
            match (connection :> obj) with
            | :? SQLiteTools.CachingDbConnection as cachingConn ->
                cachedConnection <- cachingConn
                previousTransactionState <- cachingConn.InsideTransaction
                if not previousTransactionState then
                    cachingConn.InsideTransaction <- true
                    restoreTransactionState <- true
            | _ -> ()

            // EVENT-PATH INVARIANT 3 of 3: Dispose suppression via InsideTransaction flag.
            // CachingDbConnection.Dispose() checks InsideTransaction and suppresses pool return
            // when true. Since invariant 2 above sets InsideTransaction = true, any `use conn = ...`
            // binding exit inside a handler callback is safe — Dispose becomes a no-op.
            callbackScopeDepth.Value <- callbackScopeDepth.Value + 1

            let session = Interlocked.Increment &sessionIndex
            let isRecursiveDispatch = callbackScopeDepth.Value > 1

            try
                try
                    if isRecursiveDispatch then
                        let snapshot = handlers.ToArray()
                        let handlersToRemove = ResizeArray<'THandler>()
                        for h in snapshot do
                            match invokeHandler h connection session |> Option.ofObj with
                            | None
                            | Some EventHandled -> ()
                            | Some RemoveHandler -> handlersToRemove.Add h

                        if handlersToRemove.Count > 0 then
                            let pending = deferredRecursiveRemovals.Value
                            let handlersObj = handlers :> obj
                            for removedHandler in handlersToRemove do
                                pending.Add((handlersObj, box removedHandler))
                    else
                        let mutable handlersToRemove = NativePtr.nullPtr<bool>
                        let mutable handlersToRemoveCount = 0
                        let mutable i = 0

                        for h in handlers do
                            if NativePtr.isNullPtr handlersToRemove then
                                handlersToRemoveCount <- handlers.Count
                                handlersToRemove <- NativePtr.stackalloc<bool> handlersToRemoveCount
                                NativePtr.initBlock handlersToRemove 0uy (uint32 handlersToRemoveCount * uint32 sizeof<bool>)

                            match invokeHandler h connection session |> Option.ofObj with
                            | None
                            | Some EventHandled -> ()
                            | Some RemoveHandler -> NativePtr.set handlersToRemove i true

                            i <- i + 1

                        if not (NativePtr.isNullPtr handlersToRemove) then
                            for j = handlersToRemoveCount - 1 downto 0 do
                                if NativePtr.get handlersToRemove j then
                                    handlers.RemoveAt j
                with ex ->
                    let finalEx =
                        match redirectException ex with
                        | ValueSome rewritten -> rewritten
                        | ValueNone -> ex
                    handlerFailed <- true
                    if not (String.IsNullOrWhiteSpace finalEx.Message) then
                        handlerFailureMessage <- finalEx.Message
            finally
                if callbackScopeDepth.Value = 1 then
                    let pending = deferredRecursiveRemovals.Value
                    if pending.Count > 0 then
                        let handlersObj = handlers :> obj
                        for idx = pending.Count - 1 downto 0 do
                            let (targetList, removedHandler) = pending.[idx]
                            if Object.ReferenceEquals(targetList, handlersObj) then
                                for j = handlers.Count - 1 downto 0 do
                                    if Object.ReferenceEquals(box handlers.[j], removedHandler) then
                                        handlers.RemoveAt j
                                pending.RemoveAt idx

                if handlers.Count = 0 then
                    removeMapping ()

        finally
            // EVENT-PATH CLEANUP: reverse order of invariant acquisition.
            // 1. Restore original InsideTransaction state (invariant 2).
            //    This also re-enables CachingDbConnection.Dispose pool-return (invariant 3).
            let scopeDepthAfterExit = callbackScopeDepth.Value - 1
            callbackScopeDepth.Value <- if scopeDepthAfterExit < 0 then 0 else scopeDepthAfterExit
            if restoreTransactionState then
                cachedConnection.InsideTransaction <- previousTransactionState
            // 2. Release GlobalLock (invariant 1).
            globalLock.Exit()

        struct (handlerFailed, handlerFailureMessage)

    let internal unregisterHandler<'THandler, 'TSys when 'THandler : not struct and 'TSys : not struct>
        (globalLock: ReentrantSpinLock)
        (collectionName: string)
        (globalMapping: CowByteSpanMap<ResizeArray<'TSys>>)
        (localMap: Dictionary<'THandler, ResizeArray<'TSys>>)
        (handler: 'THandler) =
        if callbackScopeDepth.Value > 0 then
            raise (InvalidOperationException(unregisterInsideHandlerMessage))
        let collectionNameBytes = System.Text.Encoding.UTF8.GetBytes collectionName
        try
            globalLock.Enter()
            let mutable list = Unchecked.defaultof<ResizeArray<'TSys>>
            if globalMapping.TryGetValue(collectionNameBytes, &list) then
                match localMap.TryGetValue handler with
                | true, handlers ->
                    for sysHandler in handlers do
                        for i = list.Count - 1 downto 0 do
                            if Object.ReferenceEquals(list.[i], sysHandler) then
                                list.RemoveAt i
                    localMap.Remove handler |> ignore
                    if list.Count = 0 then
                        ignore (globalMapping.Remove collectionNameBytes)
                | false, _ -> ()
        finally
            globalLock.Exit()
