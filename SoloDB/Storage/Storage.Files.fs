namespace SoloDatabase

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading

[<Sealed>]
type internal StorageFileError(failure: StorageFailure) =
    inherit IO.IOException("The storage operation failed.", failure.Cause)
    member _.Failure = failure

/// Shared SQLite lock and WAL-index ownership for one actual supplied byte object.
type internal StorageDomain() =
    member val Handles = ResizeArray<StorageHandle>()
    member val Regions = ResizeArray<nativeint>()
    member val RegionSize = 0 with get,set
and internal StorageNode(file: ISqliteStorageFile) =
    let identity =
        match file with
        | :? StreamStorageFile as stream -> stream.StreamIdentity :> obj
        | _ -> box file
    let state =
        if isNull identity then StorageObjectState() // The input validates its named role.
        else StorageObjects.get identity
    let domain =
        lock state.Gate (fun () ->
            if isNull state.PrivateDomain then state.PrivateDomain <- StorageDomain()
            state.PrivateDomain :?> StorageDomain)
    member _.File = file
    member _.Gate = state.Gate
    member _.Handles = domain.Handles
    member _.Regions = domain.Regions
    member _.RegionSize with get() = domain.RegionSize and set value = domain.RegionSize <- value
and internal StorageHandle(node: StorageNode, flags: int, coordination: StorageNativeFile option) =
    member _.Node = node
    member _.Flags = flags
    member _.Coordination = coordination
    member val Lock = 0 with get,set
    member val Mapped = false with get,set
    member val PersistWal = 0 with get,set
    member val ShmLocks = Array.zeroCreate<int> 8

module internal StorageFiles =
    let check failure = if not(isNull failure) then raise(StorageFileError(failure))
    let length (node: StorageNode) =
        let result = node.File.GetLength()
        check result.Failure
        if result.Length < 0L then raise(IO.InvalidDataException("A storage file returned a negative length."))
        result.Length
    let read (node: StorageNode) position (destination: Span<byte>) =
        StorageValidation.bounds position destination.Length
        let mutable count = 0
        let mutable ended = false
        while count < destination.Length && not ended do
            let result = node.File.ReadAt(position + int64 count, destination.Slice(count))
            check result.Failure
            if result.Count < 0 || result.Count > destination.Length - count then
                raise(IO.InvalidDataException("A storage read returned an invalid byte count."))
            if result.Count = 0 then ended <- true else count <- count + result.Count
        if count < destination.Length then destination.Slice(count).Clear()
        count
    let private conflict (state: StorageHandle) predicate =
        let mutable found = false
        let mutable i = 0
        while not found && i < state.Node.Handles.Count do
            let peer = state.Node.Handles.[i]
            if not(Object.ReferenceEquals(peer, state)) && predicate peer then found <- true
            i <- i + 1
        found
    let attach (state: StorageHandle) =
        lock state.Node.Gate (fun () ->
            for peer in state.Node.Handles do
                let main = state.Flags &&& 0x100 <> 0
                let peerMain = peer.Flags &&& 0x100 <> 0
                if main <> peerMain then
                    raise(StorageConfigurationError("One live backing cannot be both the database and its journal or log."))
                if main && state.Coordination.IsSome <> peer.Coordination.IsSome then
                    raise(StorageConfigurationError("Participants sharing one live backing must use the same private or File coordination."))
            state.Node.Handles.Add(state))
    let lockFile (state: StorageHandle) level =
        match state.Coordination with
        | Some native -> native.Lock(level)
        | None ->
            lock state.Node.Gate (fun () ->
                if level < 1 || level > 4 then 3850
                elif state.Lock >= level then 0
                elif (level = 1 && conflict state (fun p -> p.Lock >= 3)) ||
                     (level >= 2 && conflict state (fun p -> p.Lock >= 2)) then 5
                else
                    // A failed EXCLUSIVE upgrade retains PENDING and its prior
                    // SHARED grant, barring new readers while existing readers finish.
                    if level >= 3 then state.Lock <- 3
                    if level = 4 && conflict state (fun p -> p.Lock >= 1) then 5
                    else state.Lock <- level; 0)
    let unlock (state: StorageHandle) level =
        match state.Coordination with
        | Some native -> native.Unlock(level)
        | None ->
            lock state.Node.Gate (fun () ->
                if level <> 0 && level <> 1 then 2058
                else state.Lock <- min state.Lock level; 0)
    let reserved (state: StorageHandle) output =
        match state.Coordination with
        | Some native -> native.Reserved(output)
        | None ->
            lock state.Node.Gate (fun () ->
                let mutable found = false
                for peer in state.Node.Handles do if peer.Lock >= 2 then found <- true
                Marshal.WriteInt32(output, if found then 1 else 0)
                0)
    let shmLock (state: StorageHandle) offset count flags =
        if offset < 0 || count <= 0 || offset > 8 - count ||
           (flags <> 5 && flags <> 6 && flags <> 9 && flags <> 10) ||
           (count > 1 && flags &&& 4 <> 0) then 5130 else
        match state.Coordination with
        | Some native -> native.ShmLock(offset, count, flags)
        | None ->
            lock state.Node.Gate (fun () ->
                let releasing = flags &&& 1 <> 0
                let exclusive = flags &&& 8 <> 0
                let mutable invalid = false
                for i = offset to offset + count - 1 do
                    let held = state.ShmLocks.[i]
                    if (releasing && held <> 0 && (held = 2) <> exclusive) ||
                       (not releasing && held <> 0 && (held = 2) <> exclusive) then invalid <- true
                let blocked =
                    not releasing && conflict state (fun peer ->
                        let mutable found = false
                        for i = offset to offset + count - 1 do
                            if peer.ShmLocks.[i] = 2 || (exclusive && peer.ShmLocks.[i] <> 0) then found <- true
                        found)
                if invalid then 5130
                elif blocked then 5
                else
                    for i = offset to offset + count - 1 do
                        state.ShmLocks.[i] <- if releasing then 0 elif exclusive then 2 else 1
                    0)
    let map (state: StorageHandle) region size extend output =
        Marshal.WriteIntPtr(output, 0n)
        // SQLite passes writeLock=2 during wal-index recovery. xShmMap's
        // extension argument is a C truth value, not the integer range 0..1.
        if region < 0 || size <= 0 then 5386 else
        match state.Coordination with
        | Some native -> native.Map(region, size, extend, output)
        | None ->
            lock state.Node.Gate (fun () ->
                let node = state.Node
                if node.RegionSize <> 0 && size <> node.RegionSize then 5386
                elif region >= node.Regions.Count && extend = 0 then 0
                else
                    node.RegionSize <- size
                    while region >= node.Regions.Count do node.Regions.Add(StorageNative.alloc size)
                    state.Mapped <- true
                    Marshal.WriteIntPtr(output, node.Regions.[region])
                    0)
    let barrier (state: StorageHandle) =
        match state.Coordination with
        | Some native -> native.Barrier()
        | None -> Thread.MemoryBarrier()
    let unmap (state: StorageHandle) delete =
        match state.Coordination with
        | Some native -> native.Unmap(delete)
        | None ->
            lock state.Node.Gate (fun () ->
                Array.Clear(state.ShmLocks, 0, state.ShmLocks.Length)
                state.Mapped <- false
                let mutable live = false
                for peer in state.Node.Handles do if peer.Mapped then live <- true
                if not live then
                    for address in state.Node.Regions do Marshal.FreeHGlobal(address)
                    state.Node.Regions.Clear()
                    state.Node.RegionSize <- 0
                0)
    let close (state: StorageHandle) =
        let result =
            match state.Coordination with
            | Some native -> native.Close()
            | None -> unmap state 0
        lock state.Node.Gate (fun () -> state.Node.Handles.Remove(state) |> ignore)
        result
