namespace SoloDatabase

open System
open System.Collections.Generic

module internal EventDispatch =
    type internal EventDispatchState() =
        let mutable depth = 0
        let pendingRemovals = ResizeArray<struct (System.Collections.IList * obj * (unit -> unit))>()
        let mutationVersions = ResizeArray<int64>([|0L|])

        member _.Depth = depth
        member _.PendingRemovals = pendingRemovals
        member _.IsClean = depth = 0 && pendingRemovals.Count = 0
        member _.MutationVersion = mutationVersions.[depth]
        member _.RecordMutation() =
            // A producer observes mutations performed by callbacks below its depth,
            // not its own row writes or mutations on another physical connection.
            for i = 0 to depth - 1 do mutationVersions.[i] <- mutationVersions.[i] + 1L
        member _.Enter() =
            depth <- depth + 1
            if depth = mutationVersions.Count then mutationVersions.Add(0L)
        member _.Exit() =
            if depth <= 0 then
                raise (InvalidOperationException("Event dispatch scope underflow detected. ExitEventDispatchScope was called without a matching EnterEventDispatchScope."))
            depth <- depth - 1
        member _.Reset() =
            depth <- 0
            pendingRemovals.Clear()

