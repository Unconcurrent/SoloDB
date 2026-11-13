namespace SoloDatabase

open System
open Microsoft.FSharp.NativeInterop

#nowarn "9"

[<Sealed>]
type internal StorageFileError(failure: StorageFailure) =
    inherit IO.IOException("The storage operation failed.", failure.Cause)
    member _.Failure = failure

/// Live byte-role admission for one actual supplied object.
type internal StorageDomain() =
    member val Handles = ResizeArray<StorageHandle>()
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
            if isNull state.LiveDomain then state.LiveDomain <- StorageDomain()
            state.LiveDomain :?> StorageDomain)
    member _.File = file
    member _.Gate = state.Gate
    member _.Handles = domain.Handles
    member _.ObjectState = state
and internal StorageHandle(node: StorageNode, flags: int, coordination: ISqliteStorageCoordinationSession) =
    let identity = coordination.DomainIdentity
    do if isNull identity then invalidArg "coordination" "A coordination session requires a nonnull domain identity."
    member _.Node = node
    member _.Flags = flags
    member _.Coordination = coordination
    member _.CoordinationIdentity = identity
    member val PersistWal = 0 with get,set

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
    let auxiliaryRole (node: StorageNode) =
        lock node.Gate (fun () ->
            if length node = 0L then 0 else
            // fileformat.html 4.1: the WAL magic is 0x377f0682 or 0x377f0683.
            // A cold PERSIST journal has a zero header and remains a journal.
            let bytes = NativePtr.stackalloc<byte> 4
            let prefix = Span<byte>(NativePtr.toVoidPtr bytes, 4)
            let count = read node 0L prefix
            if count = 4 && prefix.[0] = 0x37uy && prefix.[1] = 0x7Fuy &&
               prefix.[2] = 0x06uy && prefix.[3] &&& 0xFEuy = 0x82uy then 1 else 2)
    let attach (state: StorageHandle) =
        lock state.Node.Gate (fun () ->
            for peer in state.Node.Handles do
                let main = state.Flags &&& 0x100 <> 0
                let peerMain = peer.Flags &&& 0x100 <> 0
                if main <> peerMain then
                    raise(StorageConfigurationError("One live backing cannot be both the database and its journal or log."))
                if main && not(Object.Equals(state.CoordinationIdentity, peer.CoordinationIdentity)) then
                    raise(StorageConfigurationError("Participants sharing one live backing must use compatible coordination domains."))
            state.Node.Handles.Add(state))
    let close (state: StorageHandle) =
        try state.Coordination.Close()
        finally lock state.Node.Gate (fun () -> state.Node.Handles.Remove(state) |> ignore)
