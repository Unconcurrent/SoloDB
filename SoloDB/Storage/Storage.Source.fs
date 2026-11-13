namespace SoloDatabase

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Threading

/// A physical open's explicit first-fault channel. Backup may install a shared
/// operation sink on both endpoints; no ambient or process-wide last error exists.
[<Sealed>]
type internal StorageFaultChannel() =
    let gate = obj()
    let mutable first: struct (int * exn) voption = ValueNone
    let mutable sink: Action<int,exn> = null
    member _.Record(code, cause) =
        let target =
            lock gate (fun () ->
                if isNull sink && first.IsNone then first <- ValueSome(struct (code, cause))
                sink)
        if not(isNull target) then target.Invoke(code, cause)
    member _.Take() = lock gate (fun () -> let value = first in first <- ValueNone; value)
    member _.Clear() = lock gate (fun () -> first <- ValueNone)
    member _.UseSink(target: Action<int,exn>) =
        let previous = lock gate (fun () -> let previous = sink in sink <- target; previous)
        let mutable active = 1
        { new IDisposable with
            member _.Dispose() =
                if Interlocked.Exchange(&active, 0) = 1 then lock gate (fun () -> sink <- previous) }

/// Released only by the physical native owner after successful native close.
[<Sealed>]
type internal SqliteStorageLease(name: string, filename: string, profile: SqliteStorageProfile,
                                vfs: IDisposable, faults: StorageFaultChannel,
                                report: exn -> unit, release: unit -> unit) =
    let gate = obj()
    let mutable close: Action = null
    let mutable pending = false
    let mutable completed = false
    member _.Name = name
    member _.Filename = filename
    member _.Profile = profile
    member _.Faults = faults
    member _.Report(error) = report error
    member _.RequestClose() =
        let action = lock gate (fun () -> pending <- true; if completed then null else close)
        if not(isNull action) then action.Invoke()
    member _.SetCloseRequest(action: Action) =
        let requested = lock gate (fun () -> close <- action; pending && not completed)
        if requested then action.Invoke()
    member _.Complete() =
        let claimed =
            lock gate (fun () ->
                if completed then false
                else completed <- true; close <- null; true)
        if claimed then
            try
                if not(isNull vfs) then
                    try vfs.Dispose() with error -> report error
            finally release()

/// One root's logical attachment, separate from native physical completion.
[<Sealed; AllowNullLiteral>]
type internal StorageRootLease(release: StorageRootLease -> unit) =
    let mutable active = 1
    member val Close: Action = null with get,set
    member this.Dispose() =
        if Interlocked.Exchange(&active, 0) = 1 then release this
    interface IDisposable with member this.Dispose() = this.Dispose()

module private StorageInput =
    let distinct profile (files: ISqliteStorageFile[]) =
        for i = 0 to files.Length - 1 do
            if isNull (box files.[i]) then nullArg (if i = 0 then "main" elif profile = SqliteStorageProfile.Simple then "journal" else "log")
            for j = 0 to i - 1 do
                let identical =
                    Object.ReferenceEquals(files.[i], files.[j]) ||
                    match files.[i], files.[j] with
                    | (:? StreamStorageFile as a), (:? StreamStorageFile as b) -> Object.ReferenceEquals(a.StreamIdentity, b.StreamIdentity)
                    | _ -> false
                if identical then invalidArg "files" "Each logical role requires an independent file view."
    let stream name writable (file: Stream) =
        if isNull file then nullArg name
        if not file.CanSeek || not file.CanRead || (writable && not file.CanWrite) then
            invalidArg name "Storage requires a readable, seekable Stream and write access for a writable profile."
    let adapters profile writable leaveOpen (streams: Stream[]) =
        for i = 0 to streams.Length - 1 do
            stream (if i = 0 then "main" elif profile = SqliteStorageProfile.Simple then "journal" else "log") writable streams.[i]
            for j = 0 to i - 1 do
                if Object.ReferenceEquals(streams.[i], streams.[j]) then
                    invalidArg "streams" "Each logical role requires an independent Stream cursor."
        let acquired = ResizeArray<StreamStorageFile>()
        try
            for stream in streams do acquired.Add(new StreamStorageFile(stream, leaveOpen, writable))
            acquired.ToArray()
        with _ ->
            for adapter in acquired do try (adapter :> IDisposable).Dispose() with _ -> ()
            reraise()
/// A shared storage input. Dispose deterministically; abandoned native callback
/// cycles may retain all resources. Positional files are borrowed.
[<Sealed>]
type SqliteStorage private (profile: SqliteStorageProfile, files: ISqliteStorageFile[], owned: IDisposable[],
                            coordination: ISqliteStorageCoordination) =
    let gate = obj()
    let mutable nodes = files |> Array.map StorageNode
    let mutable ownedResources = owned
    let identity = "solodb-storage-" + Guid.NewGuid().ToString("N")
    let leases = HashSet<SqliteStorageLease>()
    let roots = HashSet<StorageRootLease>()
    let mutable retired = false
    let mutable references = 0
    let mutable cleanupState = 0
    let mutable closingFailure: exn = null
    let report error = lock gate (fun () -> if isNull closingFailure then closingFailure <- error)
    let cleanup () =
        let resources =
            lock gate (fun () ->
                if retired && references = 0 && cleanupState = 0 then
                    cleanupState <- 1
                    let resources = ownedResources
                    ownedResources <- Array.empty
                    nodes <- Array.empty
                    ValueSome resources
                else ValueNone)
        match resources with
        | ValueNone -> ()
        | ValueSome resources ->
            try
                for resource in resources do
                    try resource.Dispose() with error -> report error
            finally lock gate (fun () -> cleanupState <- 2)
    let retire () =
        let closers, pending =
            lock gate (fun () ->
                retired <- true
                let closers = roots |> Seq.map (fun root -> let close = root.Close in root.Close <- null; close) |> Seq.toArray
                roots.Clear()
                closers, leases |> Seq.toArray)
        for close in closers do
            if not(isNull close) then
                try close.Invoke() with error -> report error
        for lease in pending do
            try lease.RequestClose() with error -> report error
        cleanup()
    let detach (root: StorageRootLease) =
        let last =
            lock gate (fun () ->
                root.Close <- null
                if roots.Remove(root) && roots.Count = 0 && not retired then
                    retired <- true
                    true
                else false)
        if last then retire()
    let inspect () =
        lock nodes.[0].Gate (fun () ->
            let mainLength = StorageFiles.length nodes.[0]
            let logLength = if profile <> SqliteStorageProfile.ReadOnly then lock nodes.[1].Gate (fun () -> StorageFiles.length nodes.[1]) else 0L
            struct (mainLength, logLength))
    let validateExisting main log =
        if main = 0L && log <> 0L && StorageFiles.auxiliaryRole nodes.[1] = 1 then
            raise(StorageConfigurationError("A nonempty WAL requires its main image. Preserve both backings and restore a coherent pair."))
    let initialize timeout (state: StorageHandle) =
        if profile = SqliteStorageProfile.ReadOnly then 0 else
        let timer = Stopwatch.StartNew()
        let mutable result = 5
        let mutable finished = false
        while not finished do
            if lock gate (fun () -> retired) then raise(ObjectDisposedException("SqliteStorage"))
            result <- state.Coordination.Lock(1)
            try
                if result = 0 then
                    let struct (main, log) = inspect()
                    validateExisting main log
            finally
                // Native pager transactions create and change the image. This
                // admission only inspects under SHARED and releases before retry.
                let unlocked = state.Coordination.Unlock(0)
                if result = 0 then result <- unlocked
            finished <- result <> 5 || (timeout > 0 && timer.Elapsed.TotalSeconds >= float timeout)
            if not finished then Thread.Sleep(1)
        result
    do
        StorageValidation.profile profile
        StorageInput.distinct profile files
    member _.Profile = profile
    member _.ClosingFailure = lock gate (fun () -> closingFailure)
    member _.IsClosed = lock gate (fun () -> retired && references = 0 && cleanupState = 2)
    member internal _.ReportClosingFailure(error: exn) = report error
    member internal _.Identity = identity
    member internal _.ConnectionString =
        let builder = Data.Common.DbConnectionStringBuilder()
        match coordination with
        | :? SqliteStorageCoordination as file ->
            builder.["Data Source"] <- "custom-storage"
            builder.["Storage Coordination"] <- file.Filename
            if not(isNull file.RegisteredVfs) then builder.["Storage Vfs"] <- file.RegisteredVfs
        | _ -> builder.["Data Source"] <- identity
        builder.ConnectionString
    member internal _.IsRetired = lock gate (fun () -> retired)
    member internal _.Attach() =
        lock gate (fun () ->
            if retired then raise(ObjectDisposedException("SqliteStorage"))
            let root = new StorageRootLease(detach)
            roots.Add(root) |> ignore
            root)
    member internal _.BindRootClose(root: StorageRootLease, action: Action) =
        let closing =
            lock gate (fun () ->
                if not retired && roots.Contains(root) then root.Close <- action; false
                else true)
        if closing then action.Invoke()
    member internal _.Acquire(timeout: int) =
        lock gate (fun () ->
            if retired || roots.Count = 0 then raise(ObjectDisposedException("SqliteStorage"))
            references <- references + 1)
        let faults = StorageFaultChannel()
        let mutable registration: IDisposable = null
        let mutable lease: SqliteStorageLease = Unchecked.defaultof<_>
        let release () =
            lock gate (fun () ->
                if not(isNull (box lease)) then leases.Remove(lease) |> ignore
                references <- references - 1)
            cleanup()
        try
            StorageNative.verifyProvider()
            let name = identity + "-" + Guid.NewGuid().ToString("N")
            let vfs = new ManagedStorageVfs(name, identity, nodes, profile, coordination,
                                            Action<int,exn>(fun code cause -> faults.Record(code, cause)), initialize timeout)
            registration <- vfs
            let filename = if profile = SqliteStorageProfile.ReadOnly then "file:" + identity + "?immutable=1" else identity
            lease <- SqliteStorageLease(name, filename, profile, registration, faults, report, release)
            let closing = lock gate (fun () -> leases.Add(lease) |> ignore; retired)
            if closing then lease.RequestClose()
            lease
        with _ ->
            if not(isNull registration) then try registration.Dispose() with error -> report error
            release()
            reraise()
    member _.Dispose() = retire()
    interface IDisposable with member this.Dispose() = this.Dispose()
    static member private Managed(profile, files, owned, coordination) =
        new SqliteStorage(profile, files, owned, coordination)
    static member private Streams(profile, streams, leaveOpen, coordination) =
        let adapters = StorageInput.adapters profile (profile <> SqliteStorageProfile.ReadOnly) leaveOpen streams
        try SqliteStorage.Managed(profile, adapters |> Array.map (fun f -> f :> ISqliteStorageFile),
                                  adapters |> Array.map (fun f -> f :> IDisposable), coordination)
        with _ ->
            for adapter in adapters do try (adapter :> IDisposable).Dispose() with _ -> ()
            reraise()
    static member Simple(main: ISqliteStorageFile, journal: ISqliteStorageFile, coordination: ISqliteStorageCoordination) =
        if isNull (box coordination) then nullArg "coordination"
        SqliteStorage.Managed(SqliteStorageProfile.Simple, [|main; journal|], Array.empty, coordination)
    static member Wal(main: ISqliteStorageFile, log: ISqliteStorageFile, coordination: ISqliteStorageCoordination) =
        if isNull (box coordination) then nullArg "coordination"
        SqliteStorage.Managed(SqliteStorageProfile.Wal, [|main; log|], Array.empty, coordination)
    static member ReadOnly(main: ISqliteStorageFile) =
        SqliteStorage.Managed(SqliteStorageProfile.ReadOnly, [|main|], Array.empty, SqliteStorageCoordination.SingleProcessInMemory)
    static member Simple(main: Stream, journal: Stream, leaveOpen: bool, coordination: ISqliteStorageCoordination) =
        if isNull (box coordination) then nullArg "coordination"
        SqliteStorage.Streams(SqliteStorageProfile.Simple, [|main; journal|], leaveOpen, coordination)
    static member Wal(main: Stream, log: Stream, leaveOpen: bool, coordination: ISqliteStorageCoordination) =
        if isNull (box coordination) then nullArg "coordination"
        SqliteStorage.Streams(SqliteStorageProfile.Wal, [|main; log|], leaveOpen, coordination)
    static member ReadOnly(main: Stream, leaveOpen: bool) =
        SqliteStorage.Streams(SqliteStorageProfile.ReadOnly, [|main|], leaveOpen, SqliteStorageCoordination.SingleProcessInMemory)
