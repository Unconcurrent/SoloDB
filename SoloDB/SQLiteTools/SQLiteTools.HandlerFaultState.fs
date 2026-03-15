namespace SoloDatabase

open System
open System.Collections.Generic
open System.Data
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Microsoft.Data.Sqlite
open System.Data.Common
open SQLiteToolsParams
open SQLiteToolsMapper


[<AutoOpen>]
module internal SQLiteToolsHandlerFaultState =
    type private EventScopeCounter() =
        member val Depth = 0 with get, set

    type private HandlerFaultEntry =
        { Depth: int
          Exception: exn
          mutable Swallowed: bool }

    type private HandlerFaultState() =
        let gate = obj()
        let recorded = ResizeArray<HandlerFaultEntry>()
        let pendingDepths = ResizeArray<int>()

        member _.PushPendingDepth(depth: int) =
            lock gate (fun () ->
                pendingDepths.Add(depth))

        member _.TryTakePendingDepth() =
            lock gate (fun () ->
                if pendingDepths.Count = 0 then None
                else
                    let i = pendingDepths.Count - 1
                    let depth = pendingDepths.[i]
                    pendingDepths.RemoveAt(i)
                    Some depth)

        member _.Record(depth: int, ex: exn) =
            lock gate (fun () ->
                let alreadyRecordedDeeper =
                    recorded
                    |> Seq.exists (fun entry -> obj.ReferenceEquals(entry.Exception, ex) && entry.Depth > depth)
                if not alreadyRecordedDeeper then
                    recorded.Add({ Depth = depth; Exception = ex; Swallowed = false }))

        member _.TryGetBlocking(depth: int) =
            lock gate (fun () ->
                recorded
                |> Seq.tryFind (fun entry -> entry.Swallowed || (depth > 0 && entry.Depth = depth))
                |> Option.map (fun entry -> entry.Exception))

        member _.MarkDepthAsSwallowed(depth: int) =
            lock gate (fun () ->
                for entry in recorded do
                    if entry.Depth = depth then
                        entry.Swallowed <- true)

        member _.ClearNonSwallowedDeeperThan(depth: int) =
            lock gate (fun () ->
                let mutable i = recorded.Count - 1
                while i >= 0 do
                    let entry = recorded.[i]
                    if entry.Depth > depth && not entry.Swallowed then
                        recorded.RemoveAt(i)
                    i <- i - 1)

        member _.ClearNonSwallowedAtDepth(depth: int) =
            lock gate (fun () ->
                let mutable i = recorded.Count - 1
                while i >= 0 do
                    let entry = recorded.[i]
                    if entry.Depth = depth && not entry.Swallowed then
                        recorded.RemoveAt(i)
                    i <- i - 1)

        member _.TryGetRecordedDepth(ex: exn) =
            lock gate (fun () ->
                let mutable found = false
                let mutable maxDepth = 0
                for entry in recorded do
                    if obj.ReferenceEquals(entry.Exception, ex) then
                        if not found || entry.Depth > maxDepth then
                            found <- true
                            maxDepth <- entry.Depth
                if found then Some maxDepth else None)

        member _.Take() =
            lock gate (fun () ->
                if recorded.Count = 0 then None
                else
                    let current = recorded.[0]
                    recorded.RemoveAt(0)
                    Some current.Exception)

        member _.Clear() =
            lock gate (fun () ->
                recorded.Clear())

    let private standaloneEventScopes = ConditionalWeakTable<SqliteConnection, EventScopeCounter>()
    let private handlerFaults = ConditionalWeakTable<SqliteConnection, HandlerFaultState>()

    let private tryGetConnectionEventHandlerDepth (connection: SqliteConnection) =
        let property = connection.GetType().GetProperty("EventHandlerDepth", System.Reflection.BindingFlags.Instance ||| System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Public)
        if not (isNull property) && property.PropertyType = typeof<int> then
            Some (unbox<int>(property.GetValue(connection)))
        else None

    let internal enterStandaloneEventHandlerScope (connection: SqliteConnection) =
        let counter = standaloneEventScopes.GetOrCreateValue(connection)
        counter.Depth <- counter.Depth + 1

    let internal exitStandaloneEventHandlerScopeOrFail (connection: SqliteConnection) =
        let mutable counter = Unchecked.defaultof<EventScopeCounter>
        if not (standaloneEventScopes.TryGetValue(connection, &counter)) || isNull (box counter) || counter.Depth <= 0 then
            raise (InvalidOperationException("Event handler scope underflow detected. ExitEventHandlerScope was called without a matching EnterEventHandlerScope."))
        counter.Depth <- counter.Depth - 1

    let internal getEventHandlerDepth (connection: SqliteConnection) =
        match tryGetConnectionEventHandlerDepth connection with
        | Some depth -> depth
        | None ->
            match standaloneEventScopes.TryGetValue(connection) with
            | true, counter when not (isNull (box counter)) -> counter.Depth
            | _ -> 0

    let internal isInEventHandlerScope (connection: SqliteConnection) =
        getEventHandlerDepth connection > 0

    let internal captureCurrentHandlerFaultDepth (connection: SqliteConnection) =
        let depth = getEventHandlerDepth connection
        if depth > 0 then
            handlerFaults.GetOrCreateValue(connection).PushPendingDepth(depth)

    let internal tryRecordHandlerFault (connection: SqliteConnection) (ex: exn) =
        let state = handlerFaults.GetOrCreateValue(connection)
        let depth =
            match state.TryTakePendingDepth() with
            | Some capturedDepth -> capturedDepth
            | None -> getEventHandlerDepth connection
        if depth > 0 then
            state.Record(depth, ex)

    let internal takeHandlerFault (connection: SqliteConnection) =
        let mutable state = Unchecked.defaultof<HandlerFaultState>
        if handlerFaults.TryGetValue(connection, &state) && not (isNull (box state)) then
            state.Take()
        else None

    let internal clearHandlerFault (connection: SqliteConnection) =
        let mutable state = Unchecked.defaultof<HandlerFaultState>
        if handlerFaults.TryGetValue(connection, &state) && not (isNull (box state)) then
            state.Clear()

    let internal markCurrentHandlerFaultsAsSwallowed (connection: SqliteConnection) =
        let depth = getEventHandlerDepth connection
        if depth > 0 then
            let mutable state = Unchecked.defaultof<HandlerFaultState>
            if handlerFaults.TryGetValue(connection, &state) && not (isNull (box state)) then
                state.MarkDepthAsSwallowed(depth)

    let internal clearNonSwallowedHandlerFaultsDeeperThanCurrent (connection: SqliteConnection) =
        let depth = getEventHandlerDepth connection
        if depth > 0 then
            let mutable state = Unchecked.defaultof<HandlerFaultState>
            if handlerFaults.TryGetValue(connection, &state) && not (isNull (box state)) then
                state.ClearNonSwallowedDeeperThan(depth)

    let internal tryGetRecordedHandlerFaultDepth (connection: SqliteConnection) (ex: exn) =
        let mutable state = Unchecked.defaultof<HandlerFaultState>
        if handlerFaults.TryGetValue(connection, &state) && not (isNull (box state)) then
            state.TryGetRecordedDepth(ex)
        else None

    let internal clearNonSwallowedHandlerFaultsAtDepth (connection: SqliteConnection) (depth: int) =
        if depth > 0 then
            let mutable state = Unchecked.defaultof<HandlerFaultState>
            if handlerFaults.TryGetValue(connection, &state) && not (isNull (box state)) then
                state.ClearNonSwallowedAtDepth(depth)

    let internal raiseIfHandlerFaultRecorded (connection: SqliteConnection) =
        let depth = getEventHandlerDepth connection
        let mutable state = Unchecked.defaultof<HandlerFaultState>
        if handlerFaults.TryGetValue(connection, &state) && not (isNull (box state)) then
            match state.TryGetBlocking(depth) with
            | Some handlerEx ->
                let commitEx = InvalidOperationException(
                    "Error: Database operation cannot complete because handler-scoped database work failed.
Reason: Event handlers run on the active connection while SAVEPOINT is suppressed, so swallowed database faults would otherwise leak partial side effects.
Fix: Let handler-side database faults abort the outer operation, or avoid swallowing them.",
                    handlerEx)
                commitEx.Data["SoloDB.HandlerScopedFault"] <- handlerEx
                raise commitEx
            | None -> ()
