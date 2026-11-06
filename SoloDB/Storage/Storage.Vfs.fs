namespace SoloDatabase

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading
open StorageNative

#nowarn "9"

/// One native registration per physical connection; byte views/private locks belong
/// to the description. Disposal is permitted only after actual native close returns.
[<Sealed>]
type internal ManagedStorageVfs(name: string, filename: string, nodes: StorageNode[], profile: SqliteStorageProfile,
                              sectorSize: int, coordination: SqliteStorageCoordination,
                              fault: Action<int,exn>, initialize: StorageHandle -> int) =
    let parent = StorageNativeVfs(null)
    let coordinator =
        if isNull coordination then None
        else
            // The closed File choice uses the engine's OS implementation, even
            // if an application changed the default VFS. Custom parents are explicit.
            let name =
                if not(isNull coordination.RegisteredVfs) then coordination.RegisteredVfs
                elif RuntimeInformation.IsOSPlatform(OSPlatform.Windows) then "win32"
                else "unix"
            Some(StorageNativeVfs(name))
    let callbacks = ResizeArray<Delegate>()
    let nameBytes = bytes name
    let methodSize = methodFields + 18 * ptr
    let vfsSize = vfsFields + 19 * ptr
    let memory = alloc (methodSize + vfsSize + nameBytes.Length)
    let methods = memory
    let vfs = memory + nativeint methodSize
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
        | 1 when profile = SqliteStorageProfile.Wal -> Some nodes.[1]
        | 2 when profile = SqliteStorageProfile.Simple -> Some nodes.[1]
        | _ -> None
    let returnText argument message result =
        let data = bytes message
        let memory = sqlite3_malloc64(uint64 data.Length)
        if memory = 0n then 7 else
        Marshal.Copy(data, 0, memory, data.Length)
        Marshal.WriteIntPtr(argument, memory)
        result
    let pragma argument =
        let name = text(Marshal.ReadIntPtr(argument, ptr))
        let value = text(Marshal.ReadIntPtr(argument, 2 * ptr))
        if isNull value then 12
        elif name.Equals("journal_mode", StringComparison.OrdinalIgnoreCase) &&
             not(value.Equals((if profile = SqliteStorageProfile.Wal then "wal" else "persist"), StringComparison.OrdinalIgnoreCase)) then
            returnText argument "The journal mode is fixed by this storage profile." 1
        elif name.Equals("synchronous", StringComparison.OrdinalIgnoreCase) then
            let valid =
                match value.ToLowerInvariant() with
                | "normal" | "full" | "extra" | "on" | "true" | "yes" -> true
                | _ ->
                    match Int32.TryParse(value, Globalization.NumberStyles.Integer, Globalization.CultureInfo.InvariantCulture) with
                    | true, level when level >= 1 && level <= 3 -> true
                    | _ -> false
            if valid then 12 else returnText argument "The synchronous mode must be NORMAL, FULL or EXTRA." 1
        else 12
    let control (state: StorageHandle) operation argument =
        match operation with
        | 1 ->
            match state.Coordination with
            | Some native -> native.Control(operation, argument)
            | None -> lock state.Node.Gate (fun () -> Marshal.WriteInt32(argument, state.Lock); 0)
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
        | 14 -> pragma argument
        | 18 -> Marshal.WriteInt64(argument, 0L); 0 // Managed bytes have no stable native mapping.
        | 20 -> Marshal.WriteInt32(argument, 0); 0 // Permanent bound identity cannot move.
        | 15 | 16 | 24 | 34 | 40 | 44 ->
            match state.Coordination with
            | Some native -> native.Control(operation, argument)
            | None when operation = 40 -> Marshal.WriteInt32(argument, 0); 0
            | None -> 12
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
                            if flags &&& expected = 0 || flags &&& (openDeleteOnClose ||| openExclusive) <> 0 then 14
                            elif profile = SqliteStorageProfile.ReadOnly && flags &&& 2 <> 0 then 8
                            else
                                let native =
                                    if logical = 0 then
                                        coordinator |> Option.map (fun parent -> StorageNativeFile(parent, coordination.Filename, (profile = SqliteStorageProfile.Wal)))
                                    else None
                                let state = StorageHandle(node, flags, native)
                                let mutable attached = false
                                let mutable root = Unchecked.defaultof<GCHandle>
                                try
                                    lock node.Gate (fun () -> node.Handles.Add(state))
                                    attached <- true
                                    let result = if logical = 0 then initialize state else 0
                                    if result <> 0 then
                                        StorageFiles.close state |> ignore
                                        attached <- false
                                        result
                                    else
                                        root <- GCHandle.Alloc(state)
                                        Marshal.WriteIntPtr(file, ptr, GCHandle.ToIntPtr(root))
                                        Marshal.WriteIntPtr(file, methods)
                                        Interlocked.Increment(&files) |> ignore
                                        if output <> 0n then
                                            Marshal.WriteInt32(output, flags)
                                        0
                                with _ ->
                                    if root.IsAllocated then root.Free()
                                    if attached then StorageFiles.close state |> ignore
                                    reraise())))
            set 4 (Path(fun _ path sync -> protect 2570 (fun () ->
                let logical = role(text path)
                if logical = -1 then parent.Delete(path, sync)
                elif logical = 0 then 2570
                else
                    match nodeForRole logical with
                    | None -> 5898
                    | Some node ->
                        lock node.Gate (fun () ->
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
                            | 0 -> lock node.Gate (fun () -> StorageFiles.length node > 0L)
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
                    try StorageFiles.close state
                    finally
                        root.Free()
                        Marshal.WriteIntPtr(file, ptr, 0n)
                        Marshal.WriteIntPtr(file, 0n)
                        Interlocked.Decrement(&files) |> ignore)))
            methodAt 1 (Io(fun file data count position -> protect 266 (fun () ->
                if count < 0 then 266 else
                StorageValidation.bounds position count
                let state = handle file
                // The Span is constructed inside the serialized operation, never
                // captured by a closure or copied through a per-handle byte array.
                lock state.Node.Gate (fun () ->
                    let actual = StorageFiles.read state.Node position (Span<byte>(data.ToPointer(), count))
                    if actual = count then 0 else 522))))
            methodAt 2 (Io(fun file data count position -> protect 778 (fun () ->
                if count < 0 then 778 else
                StorageValidation.bounds position count
                let state = handle file
                if state.Flags &&& 2 = 0 then 8 else
                lock state.Node.Gate (fun () ->
                    state.Node.File.WriteAt(position, ReadOnlySpan<byte>(data.ToPointer(), count)) |> StorageFiles.check
                    0))))
            methodAt 3 (Resize(fun file length -> protect 1546 (fun () ->
                if length < 0L then 1546 else
                let state = handle file
                if state.Flags &&& 2 = 0 then 8 else
                lock state.Node.Gate (fun () -> state.Node.File.SetLength(length) |> StorageFiles.check; 0))))
            methodAt 4 (IntArg(fun file _ -> protect 1034 (fun () ->
                let state = handle file
                lock state.Node.Gate (fun () -> state.Node.File.Flush() |> StorageFiles.check; 0))))
            methodAt 5 (Output(fun file output -> protect 1802 (fun () ->
                let state = handle file
                lock state.Node.Gate (fun () -> Marshal.WriteInt64(output, StorageFiles.length state.Node); 0))))
            methodAt 6 (IntArg(fun file level -> protect 3850 (fun () -> StorageFiles.lockFile (handle file) level)))
            methodAt 7 (IntArg(fun file level -> protect 2058 (fun () -> StorageFiles.unlock (handle file) level)))
            methodAt 8 (Output(fun file output -> protect 3594 (fun () -> StorageFiles.reserved (handle file) output)))
            methodAt 9 (Control(fun file operation argument -> protect 10 (fun () -> control (handle file) operation argument)))
            methodAt 10 (One(fun _ -> sectorSize))
            methodAt 11 (One(fun _ -> 0x8800 ||| (if profile = SqliteStorageProfile.ReadOnly then 0x2000 else 0)))
            methodAt 12 (ShmMap(fun file region size extend output ->
                Marshal.WriteIntPtr(output, 0n)
                protect 5386 (fun () -> StorageFiles.map (handle file) region size extend output)))
            methodAt 13 (ShmLock(fun file offset count flags -> protect 5130 (fun () -> StorageFiles.shmLock (handle file) offset count flags)))
            methodAt 14 (Barrier(fun file -> contain () (fun () -> StorageFiles.barrier (handle file))))
            methodAt 15 (IntArg(fun file delete -> protect 5130 (fun () -> StorageFiles.unmap (handle file) delete)))
            methodAt 16 (Fetch(fun _ _ _ output -> Marshal.WriteIntPtr(output, 0n); 0))
            methodAt 17 (Unfetch(fun _ _ _ -> 0))
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
