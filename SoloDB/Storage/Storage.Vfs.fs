namespace SoloDatabase

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading
open StorageNative

#nowarn "9"

/// One native registration per physical connection; byte views/private locks belong
/// to actual supplied objects. Dispose only after actual native close returns.
[<Sealed>]
type internal ManagedStorageVfs(name: string, filename: string, nodes: StorageNode[], profile: SqliteStorageProfile,
                              coordination: ISqliteStorageCoordination,
                              fault: Action<int,exn>, initialize: StorageHandle -> int) =
    let parent = StorageNativeVfs(null)
    let callbacks = ResizeArray<Delegate>()
    let nameBytes = bytes name
    let methodSize = methodFields + 18 * ptr
    let vfsSize = vfsFields + 19 * ptr
    let memory = alloc (2 * methodSize + vfsSize + nameBytes.Length)
    let methods = memory
    let rollbackMethods = memory + nativeint methodSize
    let vfs = rollbackMethods + nativeint methodSize
    let namePointer = vfs + nativeint vfsSize
    let mutable registered = false
    let mutable files = 0
    let mutable disposed = false
    let keep (callback: Delegate) =
        callbacks.Add(callback)
        Marshal.GetFunctionPointerForDelegate(callback)
    let handle file = GCHandle.FromIntPtr(Marshal.ReadIntPtr(file, ptr)).Target :?> StorageHandle
    let fail code (error: exn) =
        let code, cause =
            match error with
            | :? StorageFileError as error ->
                let result =
                    match error.Failure.Kind with
                    | StorageFailureKind.Full -> 13
                    | StorageFailureKind.ReadOnly -> 8
                    | _ -> code
                result, error.Failure.Cause
            | :? StorageNativeError as error -> error.Code, error
            | :? OutOfMemoryException -> 7, error
            | _ -> code, error
        // No managed exception may leave a reverse native callback, including
        // a secondary diagnostic failure. A missing cause does not erase the code.
        try fault.Invoke(code, cause) with _ -> ()
        code
    let protect code operation = try operation() with error -> fail code error
    let contain fallback operation =
        try operation() with error -> fail 10 error |> ignore; fallback
    let role (path: string) =
        if path = filename then 0
        elif path = filename + "-wal" then 1
        elif path = filename + "-journal" then 2
        elif not(isNull path) && path.StartsWith(filename + "-", StringComparison.Ordinal) then -2
        else -1
    let nodeForRole role =
        match role with
        | 0 -> Some nodes.[0]
        | 1 | 2 when profile <> SqliteStorageProfile.ReadOnly -> Some nodes.[1]
        | _ -> None
    let returnText argument message result =
        let data = bytes message
        let memory = sqlite3_malloc64(uint64 data.Length)
        if memory = 0n then 7 else
        Marshal.Copy(data, 0, memory, data.Length)
        Marshal.WriteIntPtr(argument, memory)
        result
    let control (state: StorageHandle) operation argument =
        match operation with
        | 1 -> state.Coordination.FileControl(operation, argument)
        | 5 | 8 | 11 | 19 | 21 | 22 | 37 | 39 | 45 -> 0 // Advisory, no byte/length or durability change.
        | 10 ->
            let value = Marshal.ReadInt32(argument)
            if value < 0 then Marshal.WriteInt32(argument, state.PersistWal)
            else state.PersistWal <- if value = 0 then 0 else 1
            0
        | 12 -> returnText argument name 0
        | 13 ->
            // The backend makes no powersafe-overwrite promise. Never enable it
            // by setting a bit borrowed from the unrelated coordination file.
            if Marshal.ReadInt32(argument) > 0 then 12
            else Marshal.WriteInt32(argument, 0); 0
        | 14 -> 12 // SQLite owns journal and synchronization mode semantics.
        | 18 -> Marshal.WriteInt64(argument, 0L); 0 // Managed bytes have no stable native mapping.
        | 20 -> Marshal.WriteInt32(argument, 0); 0 // Permanent bound identity cannot move.
        | 15 | 16 | 24 | 34 | 40 | 44 -> state.Coordination.FileControl(operation, argument)
        // Core owns FILE_POINTER, VFS_POINTER, JOURNAL_POINTER, DATA_VERSION and
        // RESET_CACHE. OS handles, media/atomic promises and extension controls
        // do not describe this byte backend. Their defined fallback is NOTFOUND.
        | _ -> 12
    do
        try
            Marshal.Copy(nameBytes, 0, namePointer, nameBytes.Length)
            Marshal.WriteInt32(vfs, 0, 3)
            Marshal.WriteInt32(vfs, 4, max (2 * ptr) parent.FileSize)
            Marshal.WriteInt32(vfs, 8, max (filename.Length * 4 + 32) parent.MaxPath)
            setField vfs vfsFields 1 namePointer
            Marshal.WriteInt32(methods, 3)
            let set index callback = setField vfs vfsFields index (keep callback)
            let methodAt index callback = setField methods methodFields index (keep callback)
            set 3 (Open(fun _ path file flags output ->
                Marshal.WriteIntPtr(file, 0n)
                protect 14 (fun () ->
                    let logical = role(text path)
                    if logical = -1 then
                        parent.Open(path, file, flags, output)
                    else
                        match nodeForRole logical with
                        | None -> 14
                        | Some node ->
                            let expected = if logical = 0 then 0x100 elif logical = 1 then 0x80000 else 0x800
                            if flags &&& expected = 0 || flags &&& openExclusive <> 0 then 14
                            elif flags &&& openDeleteOnClose <> 0 && flags &&& 2 = 0 then 8
                            elif profile = SqliteStorageProfile.ReadOnly && flags &&& 2 <> 0 then 8
                            else
                                let mutable session: ISqliteStorageCoordinationSession = Unchecked.defaultof<_>
                                let mutable state: StorageHandle = Unchecked.defaultof<_>
                                let mutable attached = false
                                let mutable root = Unchecked.defaultof<GCHandle>
                                try
                                    session <-
                                        if logical = 0 then coordination.Open(node.File, profile)
                                        else SqliteStorageCoordination.SingleProcessInMemory.Open(node.File, profile)
                                    if isNull (box session) then invalidOp "The coordination provider returned no session."
                                    state <- StorageHandle(node, flags, session)
                                    StorageFiles.attach state
                                    attached <- true
                                    let result = if logical = 0 then initialize state else 0
                                    if result <> 0 then
                                        attached <- false
                                        session <- Unchecked.defaultof<_>
                                        StorageFiles.close state |> ignore
                                        result
                                    else
                                        root <- GCHandle.Alloc(state)
                                        Marshal.WriteIntPtr(file, ptr, GCHandle.ToIntPtr(root))
                                        let table = if state.Coordination.HasSharedMemory then methods else rollbackMethods
                                        Marshal.WriteIntPtr(file, table)
                                        Interlocked.Increment(&files) |> ignore
                                        if output <> 0n then Marshal.WriteInt32(output, flags)
                                        0
                                with error ->
                                    if root.IsAllocated then root.Free()
                                    Marshal.WriteIntPtr(file, ptr, 0n)
                                    try
                                        if attached then StorageFiles.close state |> ignore
                                        elif not(isNull (box session)) then session.Close() |> ignore
                                    with cleanup -> error.Data.["SoloDB.OpenCleanupException"] <- cleanup
                                    reraise())))
            set 4 (Path(fun _ path sync -> protect 2570 (fun () ->
                let logical = role(text path)
                if logical = -1 then parent.Delete(path, sync)
                elif profile = SqliteStorageProfile.ReadOnly then 8
                else
                    match nodeForRole logical with
                    | None -> 5898
                    | Some node ->
                        lock node.Gate (fun () ->
                            let present = if logical = 0 then 0 else StorageFiles.auxiliaryRole node
                            if logical = 0 || present = 0 || present = logical then
                                node.File.SetLength(0L) |> StorageFiles.check
                                node.File.Flush() |> StorageFiles.check
                            0))))
            set 5 (Access(fun _ path flags output -> protect 3338 (fun () ->
                let logical = role(text path)
                if logical = -1 then parent.Access(path, flags, output)
                else
                    let accessible =
                        match nodeForRole logical with
                        | None -> false
                        | Some node ->
                            match flags with
                            | 0 ->
                                if logical = 0 then lock node.Gate (fun () -> StorageFiles.length node > 0L)
                                else StorageFiles.auxiliaryRole node = logical
                            | 1 -> profile <> SqliteStorageProfile.ReadOnly
                            | 2 -> true
                            | _ -> raise(ArgumentOutOfRangeException("flags"))
                    Marshal.WriteInt32(output, if accessible then 1 else 0)
                    0)))
            set 6 (FullPath(fun _ path capacity output -> protect 14 (fun () ->
                let text = text path
                if role text = -1 then parent.FullPath(path, capacity, output)
                elif role text = -2 then 14
                else
                    let data = bytes text
                    if data.Length > capacity then 14
                    else Marshal.Copy(data, 0, output, data.Length); 0)))
            set 7 (DlOpen(fun _ path -> contain 0n (fun () -> parent.DlOpen(path))))
            set 8 (DlError(fun _ size output -> contain () (fun () -> parent.DlError(size, output))))
            set 9 (DlSym(fun _ library symbol -> contain 0n (fun () -> parent.DlSym(library, symbol))))
            set 10 (DlClose(fun _ library -> contain () (fun () -> parent.DlClose(library))))
            set 11 (Random(fun _ size output -> contain 0 (fun () -> parent.Random(size, output))))
            set 12 (IntArg(fun _ delay -> contain 0 (fun () -> parent.Sleep(delay))))
            set 13 (Output(fun _ output -> protect 10 (fun () -> parent.Time(output))))
            set 14 (Random(fun _ size output -> contain 0 (fun () -> parent.Error(size, output))))
            set 15 (Output(fun _ output -> protect 10 (fun () -> parent.Time64(output))))
            set 16 (SetSystemCall(fun _ name callback -> protect 10 (fun () -> parent.SetSystemCall(name, callback))))
            set 17 (DlOpen(fun _ name -> contain 0n (fun () -> parent.GetSystemCall(name))))
            set 18 (DlOpen(fun _ name -> contain 0n (fun () -> parent.NextSystemCall(name))))
            methodAt 0 (One(fun file ->
                protect 4106 (fun () ->
                    let root = GCHandle.FromIntPtr(Marshal.ReadIntPtr(file, ptr))
                    let state = root.Target :?> StorageHandle
                    try
                        let result = StorageFiles.close state
                        // SQLite can complete physical close without returning xClose's error.
                        if result <> 0 then fail result null |> ignore
                        if result = 0 && state.Flags &&& openDeleteOnClose <> 0 then
                            lock state.Node.Gate (fun () ->
                                state.Node.File.SetLength(0L) |> StorageFiles.check
                                state.Node.File.Flush() |> StorageFiles.check)
                        result
                    finally
                        root.Free()
                        Marshal.WriteIntPtr(file, ptr, 0n)
                        Marshal.WriteIntPtr(file, 0n)
                        Interlocked.Decrement(&files) |> ignore)))
            methodAt 1 (Io(fun file data count position ->
              try
                if count < 0 then 266 else
                StorageValidation.bounds position count
                let state = handle file
                // The Span is constructed inside the serialized operation, never
                // captured by a closure or copied through a per-handle byte array.
                lock state.Node.Gate (fun () ->
                    let actual = StorageFiles.read state.Node position (Span<byte>(data.ToPointer(), count))
                    if actual = count then 0 else 522)
              with error -> fail 266 error))
            methodAt 2 (Io(fun file data count position ->
              try
                if count < 0 then 778 else
                StorageValidation.bounds position count
                let state = handle file
                if state.Flags &&& 2 = 0 then 8 else
                lock state.Node.Gate (fun () ->
                    state.Node.File.WriteAt(position, ReadOnlySpan<byte>(data.ToPointer(), count)) |> StorageFiles.check
                    0)
              with error -> fail 778 error))
            methodAt 3 (Resize(fun file length ->
              try
                if length < 0L then 1546 else
                let state = handle file
                if state.Flags &&& 2 = 0 then 8 else
                lock state.Node.Gate (fun () -> state.Node.File.SetLength(length) |> StorageFiles.check; 0)
              with error -> fail 1546 error))
            methodAt 4 (IntArg(fun file _ ->
              try
                let state = handle file
                lock state.Node.Gate (fun () -> state.Node.File.Flush() |> StorageFiles.check; 0)
              with error -> fail 1034 error))
            methodAt 5 (Output(fun file output ->
              try
                let state = handle file
                lock state.Node.Gate (fun () -> Marshal.WriteInt64(output, StorageFiles.length state.Node); 0)
              with error -> fail 1802 error))
            methodAt 6 (IntArg(fun file level -> protect 3850 (fun () -> (handle file).Coordination.Lock(level))))
            methodAt 7 (IntArg(fun file level -> protect 2058 (fun () -> (handle file).Coordination.Unlock(level))))
            methodAt 8 (Output(fun file output ->
                Marshal.WriteInt32(output, 0)
                protect 3594 (fun () ->
                    let result = (handle file).Coordination.CheckReservedLock()
                    Marshal.WriteInt32(output, if result.IsReserved then 1 else 0)
                    result.ResultCode)))
            methodAt 9 (Control(fun file operation argument -> protect 10 (fun () -> control (handle file) operation argument)))
            methodAt 10 (One(fun _ -> 4096))
            methodAt 11 (One(fun _ -> 0x8000 ||| (if profile = SqliteStorageProfile.ReadOnly then 0x2000 else 0)))
            methodAt 12 (ShmMap(fun file region size extend output ->
                Marshal.WriteIntPtr(output, 0n)
                protect 5386 (fun () ->
                    let result = (handle file).Coordination.MapSharedMemory(region, size, extend)
                    Marshal.WriteIntPtr(output, result.Address)
                    result.ResultCode)))
            methodAt 13 (ShmLock(fun file offset count flags -> protect 5130 (fun () -> (handle file).Coordination.LockSharedMemory(offset, count, flags))))
            methodAt 14 (Barrier(fun file -> contain () (fun () -> (handle file).Coordination.SharedMemoryBarrier())))
            methodAt 15 (IntArg(fun file delete -> protect 5130 (fun () -> (handle file).Coordination.UnmapSharedMemory(delete))))
            methodAt 16 (Fetch(fun _ _ _ output -> Marshal.WriteIntPtr(output, 0n); 0))
            methodAt 17 (Unfetch(fun _ _ _ -> 0))
            // Publish the real main-file capability. SQLite then declines WAL
            // natively when the selected coordination receiver has no shm methods.
            Marshal.WriteInt32(rollbackMethods, 1)
            for i = 0 to 17 do
                setField rollbackMethods methodFields i (field methods methodFields i)
            let result = sqlite3_vfs_register(vfs, 0)
            if result <> 0 then raise(StorageNativeError(result, "The SQLite VFS could not be registered."))
            registered <- true
        with _ ->
            Marshal.FreeHGlobal(memory)
            callbacks.Clear()
            reraise()
    member _.Name = name
    member _.FileCount = files
    interface IDisposable with
        member _.Dispose() =
            if not disposed then
                if files <> 0 then invalidOp "Native files still own this VFS."
                if registered then sqlite3_vfs_unregister(vfs) |> ignore
                registered <- false
                Marshal.FreeHGlobal(memory)
                callbacks.Clear()
                disposed <- true
