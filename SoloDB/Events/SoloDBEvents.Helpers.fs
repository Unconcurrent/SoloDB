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

        | :? InvalidOperationException as ioe when ioe.Message.Contains("Collection was modified") ->
            let wrapped = InvalidOperationException(
                "Cannot call Unregister from within a handler. Return SoloDBEventsResult.RemoveHandler instead.",
                ioe)
            ValueSome (wrapped :> exn)

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

        let mutable disableHandle: IDisposable = null
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
            // [SC5-REPLACE: remove when wrapper dispatch replaces InTransaction branching]
            match (connection :> obj) with
            | :? SQLiteTools.CachingDbConnection as cachingConn ->
                cachedConnection <- cachingConn
                previousTransactionState <- cachingConn.InsideTransaction
                if not previousTransactionState then
                    cachingConn.InsideTransaction <- true
                    restoreTransactionState <- true
            | _ -> ()

            // EVENT-PATH INVARIANT 3 of 3: Dispose suppression (exempt from SC1-SC4).
            // Prevents CachingDbConnection.Dispose() from returning the connection
            // to the pool while handler code executes. Without this, any `use conn = ...`
            // binding exit inside a handler callback would trigger onDispose -> TakeBack,
            // returning the connection to the pool while it is still mid-statement execution
            // inside a SQLite trigger. This is the ONLY remaining justified DisableDispose
            // site after SC3 completes.
            // [SC5-REPLACE: remove when event-path ownership lease API lands]
            match (connection :> obj) with
            | :? SQLiteTools.IDisableDispose as disable -> disableHandle <- disable.DisableDispose()
            | _ -> ()

            let session = Interlocked.Increment &sessionIndex

            let mutable handlersToRemove = NativePtr.nullPtr<bool>
            let mutable handlersToRemoveCount = 0
            let mutable i = 0

            try
                try
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
                with ex ->
                    let finalEx =
                        match redirectException ex with
                        | ValueSome rewritten -> rewritten
                        | ValueNone -> ex
                    handlerFailed <- true
                    if not (String.IsNullOrWhiteSpace finalEx.Message) then
                        handlerFailureMessage <- finalEx.Message
            finally
                if not (NativePtr.isNullPtr handlersToRemove) then
                    for j = handlersToRemoveCount - 1 downto 0 do
                        if NativePtr.get handlersToRemove j then
                            handlers.RemoveAt j

                    if handlers.Count = 0 then
                        removeMapping ()

        finally
            // EVENT-PATH CLEANUP: reverse order of invariant acquisition.
            // 1. Release dispose suppression (invariant 3).
            if not (isNull disableHandle) then
                disableHandle.Dispose()
            // 2. Restore original InsideTransaction state (invariant 2).
            if restoreTransactionState then
                cachedConnection.InsideTransaction <- previousTransactionState
            // 3. Release GlobalLock (invariant 1).
            globalLock.Exit()

        struct (handlerFailed, handlerFailureMessage)

    let internal unregisterHandler<'THandler, 'TSys when 'THandler : not struct and 'TSys : not struct>
        (globalLock: ReentrantSpinLock)
        (collectionName: string)
        (globalMapping: CowByteSpanMap<ResizeArray<'TSys>>)
        (localMap: Dictionary<'THandler, ResizeArray<'TSys>>)
        (handler: 'THandler) =
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
