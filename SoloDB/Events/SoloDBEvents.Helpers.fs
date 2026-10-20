namespace SoloDatabase

open CowByteSpanMap
open System.Collections.Generic
open System
open SoloDatabase.Types
open Microsoft.FSharp.NativeInterop
open Utils
open System.Threading

// NativePtr operations for zero-copy JSON parsing from SQLite trigger callbacks
#nowarn "9"

type internal EventHandlerList<'THandler>(removeMapping: unit -> unit) =
    member val Handlers = ResizeArray<'THandler>()
    member _.RemoveMapping = removeMapping

module internal SoloDBEventsHelpers =
    let private unregisterInsideHandlerMessage =
        "Cannot call Unregister from within a handler. Return SoloDBEventsResult.RemoveHandler instead."

    let internal redirectException (ex: exn) =
        match ex with
        | :? NativeSqliteException as se ->
            let message = se.Message
            let isLocked =
                se.SqliteErrorCode = 6 ||
                se.SqliteExtendedErrorCode = 6 ||
                se.SqliteErrorCode = 5 ||
                se.SqliteExtendedErrorCode = 5

            if isLocked then
                let wrapped = InvalidOperationException(
                    "Event handlers must use the ctx parameter (which implements ISoloDB). Using any other SoloDB instance inside a handler will lock the database. " +
                    $"Original error: {message}",
                    se)
                ValueSome (wrapped :> exn)
            else
                ValueNone

        | _ -> ValueNone

    let private standaloneDispatchStates =
        System.Runtime.CompilerServices.ConditionalWeakTable<NativeSqliteConnection, EventDispatch.EventDispatchState>()
    let private createStandaloneDispatchState =
        System.Runtime.CompilerServices.ConditionalWeakTable<NativeSqliteConnection, EventDispatch.EventDispatchState>.CreateValueCallback(fun _ -> EventDispatch.EventDispatchState())

    let internal runHandlerBody<'THandler, 'TFacts>
        (globalLock: UtilsReflection.ReentrantSpinLock)
        (sessionIndex: int64 byref)
        (connection: NativeSqliteConnection)
        (handlers: ResizeArray<'THandler>)
        (facts: 'TFacts)
        (invokeHandler: 'THandler -> NativeSqliteConnection -> int64 -> 'TFacts -> SoloDBEventsResult)
        (removeMapping: unit -> unit)
        (defaultMessage: string) =

        let mutable handlerFailed = false
        let mutable handlerFailureMessage = defaultMessage
        let cachedConnection =
            match connection with
            | :? NativeSQLiteTools.NativeCachingDbConnection as cached -> cached
            | _ -> Unchecked.defaultof<NativeSQLiteTools.NativeCachingDbConnection>
        let dispatchState =
            if isNull (box cachedConnection) then standaloneDispatchStates.GetValue(connection, createStandaloneDispatchState)
            else cachedConnection.EventDispatchState
        let mutable restoreTransactionState = false

        globalLock.Enter()
        try
            // Keep same-connection operations in their active transaction and suppress pool
            // return while callbacks run. Every event kind shares this connection-owned depth.
            if not (isNull (box cachedConnection)) && not cachedConnection.InsideTransaction then
                cachedConnection.InsideTransaction <- true
                restoreTransactionState <- true
            dispatchState.Enter()
            try
                let session = Interlocked.Increment &sessionIndex
                // Registration may append reentrantly, but removals wait until all active
                // dispatches finish. The entry count therefore defines a stable invocation order
                // without copying the list, including recursive callbacks and duplicate handlers.
                let count = handlers.Count
                try
                    for i = 0 to count - 1 do
                        let handler = handlers.[i]
                        let result = invokeHandler handler connection session facts
                        if not (isNull (box result)) && result = RemoveHandler then
                            dispatchState.PendingRemovals.Add(struct (handlers :> System.Collections.IList, box handler, removeMapping))
                with ex ->
                    let finalEx =
                        match redirectException ex with
                        | ValueSome rewritten -> rewritten
                        | ValueNone -> ex
                    handlerFailed <- true
                    if not (String.IsNullOrWhiteSpace finalEx.Message) then
                        handlerFailureMessage <- finalEx.Message
            finally
                try
                    if dispatchState.Depth = 1 then
                        // Apply every nested list's removals even after a later handler fails.
                        // Returning a pooled connection must never discard an unhonored removal.
                        let pending = dispatchState.PendingRemovals
                        try
                            for i = pending.Count - 1 downto 0 do
                                let struct (target, removed, removeTargetMapping) = pending.[i]
                                for j = target.Count - 1 downto 0 do
                                    if Object.ReferenceEquals(target.[j], removed) then target.RemoveAt(j)
                                if target.Count = 0 then removeTargetMapping()
                        finally
                            pending.Clear()
                    if handlers.Count = 0 then removeMapping()
                finally
                    dispatchState.Exit()
        finally
            if restoreTransactionState then cachedConnection.InsideTransaction <- false
            globalLock.Exit()

        struct (handlerFailed, handlerFailureMessage)

    let internal unregisterHandler<'THandler, 'TSys when 'THandler : not struct and 'TSys : not struct>
        (globalLock: UtilsReflection.ReentrantSpinLock)
        (collectionName: string)
        (globalMapping: CowByteSpanMap<EventHandlerList<'TSys>>)
        (localMap: Dictionary<'THandler, ResizeArray<'TSys>>)
        (handler: 'THandler) =
        if globalLock.IsHeldByCurrentThread then
            raise (InvalidOperationException(unregisterInsideHandlerMessage))
        let collectionNameBytes = System.Text.Encoding.UTF8.GetBytes collectionName
        try
            globalLock.Enter()
            let mutable entry = Unchecked.defaultof<EventHandlerList<'TSys>>
            if globalMapping.TryGetValue(collectionNameBytes, &entry) then
                let list = entry.Handlers
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
