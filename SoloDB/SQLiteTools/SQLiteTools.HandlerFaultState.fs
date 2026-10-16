namespace SoloDatabase

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open SQLiteToolsMapper


[<AutoOpen>]
module internal SQLiteToolsHandlerFaultState =
    type internal IEventHandlerScopeState =
        abstract EventHandlerDepth: int

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

    let private standaloneEventScopes = ConditionalWeakTable<NativeDatabase, EventScopeCounter>()
    let private handlerFaults = ConditionalWeakTable<NativeDatabase, HandlerFaultState>()

    let internal enterStandaloneEventHandlerScope (connection: NativeSqliteConnection) =
        let counter = standaloneEventScopes.GetOrCreateValue(connection.DatabaseOwner)
        counter.Depth <- counter.Depth + 1

    let internal exitStandaloneEventHandlerScopeOrFail (connection: NativeSqliteConnection) =
        let mutable counter = Unchecked.defaultof<EventScopeCounter>
        if not (standaloneEventScopes.TryGetValue(connection.DatabaseOwner, &counter)) || isNull (box counter) || counter.Depth <= 0 then
            raise (InvalidOperationException("Event handler scope underflow detected. ExitEventHandlerScope was called without a matching EnterEventHandlerScope."))
        counter.Depth <- counter.Depth - 1

    let internal getEventHandlerDepth (connection: NativeSqliteConnection) =
        match connection :> obj with
        | :? IEventHandlerScopeState as state -> state.EventHandlerDepth
        | _ ->
            let mutable counter = Unchecked.defaultof<EventScopeCounter>
            if standaloneEventScopes.TryGetValue(connection.DatabaseOwner, &counter) then counter.Depth else 0

    let internal isInEventHandlerScope (connection: NativeSqliteConnection) =
        getEventHandlerDepth connection > 0

    let internal captureCurrentHandlerFaultDepth (connection: NativeSqliteConnection) =
        let depth = getEventHandlerDepth connection
        if depth > 0 then
            handlerFaults.GetOrCreateValue(connection.DatabaseOwner).PushPendingDepth(depth)

    let internal tryRecordHandlerFault (connection: NativeSqliteConnection) (ex: exn) =
        let state = handlerFaults.GetOrCreateValue(connection.DatabaseOwner)
        let depth =
            match state.TryTakePendingDepth() with
            | Some capturedDepth -> capturedDepth
            | None -> getEventHandlerDepth connection
        if depth > 0 then
            state.Record(depth, ex)

    let internal takeHandlerFault (connection: NativeSqliteConnection) =
        let mutable state = Unchecked.defaultof<HandlerFaultState>
        if handlerFaults.TryGetValue(connection.DatabaseOwner, &state) && not (isNull (box state)) then
            state.Take()
        else None

    let internal clearHandlerFault (connection: NativeSqliteConnection) =
        let mutable state = Unchecked.defaultof<HandlerFaultState>
        if handlerFaults.TryGetValue(connection.DatabaseOwner, &state) && not (isNull (box state)) then
            state.Clear()

    let internal markCurrentHandlerFaultsAsSwallowed (connection: NativeSqliteConnection) =
        let depth = getEventHandlerDepth connection
        if depth > 0 then
            let mutable state = Unchecked.defaultof<HandlerFaultState>
            if handlerFaults.TryGetValue(connection.DatabaseOwner, &state) && not (isNull (box state)) then
                state.MarkDepthAsSwallowed(depth)

    let internal clearNonSwallowedHandlerFaultsDeeperThanCurrent (connection: NativeSqliteConnection) =
        let depth = getEventHandlerDepth connection
        if depth > 0 then
            let mutable state = Unchecked.defaultof<HandlerFaultState>
            if handlerFaults.TryGetValue(connection.DatabaseOwner, &state) && not (isNull (box state)) then
                state.ClearNonSwallowedDeeperThan(depth)

    let internal tryGetRecordedHandlerFaultDepth (connection: NativeSqliteConnection) (ex: exn) =
        let mutable state = Unchecked.defaultof<HandlerFaultState>
        if handlerFaults.TryGetValue(connection.DatabaseOwner, &state) && not (isNull (box state)) then
            state.TryGetRecordedDepth(ex)
        else None

    let internal clearNonSwallowedHandlerFaultsAtDepth (connection: NativeSqliteConnection) (depth: int) =
        if depth > 0 then
            let mutable state = Unchecked.defaultof<HandlerFaultState>
            if handlerFaults.TryGetValue(connection.DatabaseOwner, &state) && not (isNull (box state)) then
                state.ClearNonSwallowedAtDepth(depth)

    let internal raiseIfHandlerFaultRecorded (connection: NativeSqliteConnection) =
        let depth = getEventHandlerDepth connection
        let mutable state = Unchecked.defaultof<HandlerFaultState>
        if handlerFaults.TryGetValue(connection.DatabaseOwner, &state) && not (isNull (box state)) then
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

    // A public presentation may be replaced during a callback. Faults belong only to
    // the physical owner admitted by that operation. Native callers retain their owner.
    let inline internal withHandlerFaultWrapWhen (connection: NativeSqliteConnection) isCurrent body =
        try
            let result = body()
            if isCurrent() then raiseIfHandlerFaultRecorded connection
            result
        with ex ->
            if isCurrent() then tryRecordHandlerFault connection ex
            reraise()

    let internal withHandlerFaultWrap<'T> (connection: NativeSqliteConnection) (body: unit -> 'T) : 'T =
        withHandlerFaultWrapWhen connection (fun () -> true) body
