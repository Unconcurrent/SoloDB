namespace SoloDatabase

open System
open System.Runtime.InteropServices
open System.Threading

/// Private pager locks and WAL-index regions for one actual byte object.
type internal StorageMemoryDomain() =
    member val Sessions = ResizeArray<StorageMemorySession>()
    member val Regions = ResizeArray<nativeint>()
    member val RegionSize = 0 with get,set
and internal StorageMemorySession(gate: obj, domain: StorageMemoryDomain) as this =
    let mutable closed = false
    let mutable level = 0
    let mutable mapped = false
    let slots = Array.zeroCreate<int> 8
    let run action =
        lock gate (fun () ->
            if closed then raise(ObjectDisposedException("ISqliteStorageCoordinationSession"))
            action())
    let conflict predicate =
        let mutable found = false
        let mutable i = 0
        while not found && i < domain.Sessions.Count do
            let peer = domain.Sessions.[i]
            if not(Object.ReferenceEquals(peer, this)) && predicate peer then found <- true
            i <- i + 1
        found
    let unmap () =
        Array.Clear(slots, 0, slots.Length)
        mapped <- false
        let mutable live = false
        for peer in domain.Sessions do if peer.Mapped then live <- true
        if not live then
            for address in domain.Regions do Marshal.FreeHGlobal(address)
            domain.Regions.Clear()
            domain.RegionSize <- 0
        0
    do lock gate (fun () -> domain.Sessions.Add(this))
    member private _.Level = level
    member private _.Mapped = mapped
    member private _.Slots = slots
    interface ISqliteStorageCoordinationSession with
        member _.DomainIdentity = domain
        member _.HasSharedMemory = true
        member _.Lock(requested) =
            run (fun () ->
                if requested < 1 || requested > 4 then 3850
                elif level >= requested then 0
                elif (requested = 1 && conflict (fun p -> p.Level >= 3)) ||
                     (requested >= 2 && conflict (fun p -> p.Level >= 2)) then 5
                else
                    // A failed EXCLUSIVE upgrade keeps PENDING, barring new
                    // readers while the already admitted readers finish.
                    if requested >= 3 then level <- 3
                    if requested = 4 && conflict (fun p -> p.Level >= 1) then 5
                    else level <- requested; 0)
        member _.Unlock(requested) =
            run (fun () ->
                if requested <> 0 && requested <> 1 then 2058
                else level <- min level requested; 0)
        member _.CheckReservedLock() =
            run (fun () ->
                let mutable found = false
                for peer in domain.Sessions do if peer.Level >= 2 then found <- true
                StorageReservedLockResult(0, found))
        member _.MapSharedMemory(region, size, extend) =
            run (fun () ->
                // Recovery uses extend=2: extension is a C truth value.
                if region < 0 || size <= 0 || (domain.RegionSize <> 0 && size <> domain.RegionSize) then
                    StorageSharedMemoryResult(5386, 0n)
                elif region >= domain.Regions.Count && extend = 0 then StorageSharedMemoryResult(0, 0n)
                else
                    domain.RegionSize <- size
                    while region >= domain.Regions.Count do domain.Regions.Add(StorageNative.alloc size)
                    mapped <- true
                    StorageSharedMemoryResult(0, domain.Regions.[region]))
        member _.LockSharedMemory(offset, count, flags) =
            run (fun () ->
                if offset < 0 || count <= 0 || offset > 8 - count ||
                   (flags <> 5 && flags <> 6 && flags <> 9 && flags <> 10) ||
                   (count > 1 && flags &&& 4 <> 0) then 5130 else
                let releasing = flags &&& 1 <> 0
                let exclusive = flags &&& 8 <> 0
                let mutable invalid = false
                for i = offset to offset + count - 1 do
                    let held = slots.[i]
                    if held <> 0 && (held = 2) <> exclusive then invalid <- true
                let blocked =
                    not releasing && conflict (fun peer ->
                        let mutable found = false
                        for i = offset to offset + count - 1 do
                            if peer.Slots.[i] = 2 || (exclusive && peer.Slots.[i] <> 0) then found <- true
                        found)
                if invalid then 5130
                elif blocked then 5
                else
                    for i = offset to offset + count - 1 do
                        slots.[i] <- if releasing then 0 elif exclusive then 2 else 1
                    0)
        member _.SharedMemoryBarrier() = run Thread.MemoryBarrier
        member _.UnmapSharedMemory(_) = run unmap
        member _.FileControl(operation, argument) =
            run (fun () ->
                match operation with
                | 1 -> Marshal.WriteInt32(argument, level); 0
                | 40 -> Marshal.WriteInt32(argument, 0); 0
                | _ -> 12)
        member _.Close() =
            lock gate (fun () ->
                if closed then 0 else
                closed <- true
                try unmap()
                finally domain.Sessions.Remove(this) |> ignore)

module internal StorageMemory =
    let openSession (main: ISqliteStorageFile) profile =
        if isNull (box main) then nullArg "main"
        StorageValidation.profile profile
        let node = StorageNode(main)
        let state = node.ObjectState
        lock state.Gate (fun () ->
            if isNull state.PrivateDomain then state.PrivateDomain <- StorageMemoryDomain()
            StorageMemorySession(state.Gate, state.PrivateDomain :?> StorageMemoryDomain) :> ISqliteStorageCoordinationSession)
