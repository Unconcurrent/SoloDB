namespace SoloDatabase

open System
open System.Runtime.InteropServices
open Microsoft.FSharp.NativeInterop
open SQLitePCL

#nowarn "9"

[<Sealed>]
type internal StorageNativeError(code: int, message: string) =
    inherit IO.IOException(message)
    member _.Code = code

/// SQLite 3 sqlite3_vfs and sqlite3_io_methods ABI. The pointer arrays start at
/// native pointer alignment, following three and one 32-bit integers respectively.
/// https://sqlite.org/c3ref/vfs.html and https://sqlite.org/c3ref/io_methods.html
module internal StorageNative =
    [<Literal>]
    let openDeleteOnClose = 0x08
    [<Literal>]
    let openExclusive = 0x10
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern nativeint sqlite3_vfs_find(nativeint name)
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern int sqlite3_vfs_register(nativeint vfs, int makeDefault)
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern int sqlite3_vfs_unregister(nativeint vfs)
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern nativeint sqlite3_malloc64(uint64 size)
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern void sqlite3_free(nativeint memory)
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern nativeint sqlite3_libversion()
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern int sqlite3_close(nativeint db)
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern nativeint sqlite3_create_filename(nativeint database, nativeint journal, nativeint wal, int count, nativeint parameters)
    [<DllImport("e_sqlite3", CallingConvention=CallingConvention.Cdecl)>]
    extern void sqlite3_free_filename(nativeint filename)

    let ptr = IntPtr.Size
    let vfsFields = (12 + ptr - 1) / ptr * ptr
    let methodFields = (4 + ptr - 1) / ptr * ptr
    let field address start index = Marshal.ReadIntPtr(address, start + index * ptr)
    let setField address start index value = Marshal.WriteIntPtr(address, start + index * ptr, value)
    let binding<'T when 'T :> Delegate> address =
        if address = 0n then Unchecked.defaultof<'T>
        else Marshal.GetDelegateForFunctionPointer<'T>(address)
    let bindAt<'T when 'T :> Delegate> version address start index minimum =
        if version < minimum then Unchecked.defaultof<'T>
        else binding<'T>(field address start index)
    let text (address: nativeint) =
        if address = 0n then null else
        let mutable length = 0
        while Marshal.ReadByte(address, length) <> 0uy do length <- length + 1
        Text.Encoding.UTF8.GetString(NativePtr.ofNativeInt<byte> address, length)
    let bytes (text: string) = Text.Encoding.UTF8.GetBytes(text + "\000")
    let alloc (size: int) =
        let address = Marshal.AllocHGlobal(size)
        Span<byte>(address.ToPointer(), size).Clear()
        address
    let utf8 value =
        let data = bytes value
        let address = Marshal.AllocHGlobal(data.Length)
        Marshal.Copy(data, 0, address, data.Length)
        address
    let filename value =
        let db = utf8 value
        try
            let journal = utf8 (value + "-journal")
            try
                let wal = utf8 (value + "-wal")
                try
                    let result = sqlite3_create_filename(db, journal, wal, 0, 0n)
                    if result = 0n then raise(OutOfMemoryException())
                    result
                finally Marshal.FreeHGlobal(wal)
            finally Marshal.FreeHGlobal(journal)
        finally Marshal.FreeHGlobal(db)
    let verifyProvider () =
        // Direct storage calls and provider-owned handles must belong to one engine.
        let version = raw.sqlite3_libversion()
        use address = fixed version
        if NativePtr.toNativeInt address <> sqlite3_libversion() then
            raise(NotSupportedException("Custom storage and its provider must use the same SQLite engine instance."))

    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Open = delegate of nativeint * nativeint * nativeint * int * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Path = delegate of nativeint * nativeint * int -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Access = delegate of nativeint * nativeint * int * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type FullPath = delegate of nativeint * nativeint * int * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Io = delegate of nativeint * nativeint * int * int64 -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Resize = delegate of nativeint * int64 -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type One = delegate of nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type IntArg = delegate of nativeint * int -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Output = delegate of nativeint * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Control = delegate of nativeint * int * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type ShmMap = delegate of nativeint * int * int * int * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type ShmLock = delegate of nativeint * int * int * int -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Barrier = delegate of nativeint -> unit
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Fetch = delegate of nativeint * int64 * int * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Unfetch = delegate of nativeint * int64 * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type Random = delegate of nativeint * int * nativeint -> int
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type DlOpen = delegate of nativeint * nativeint -> nativeint
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type DlError = delegate of nativeint * int * nativeint -> unit
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type DlSym = delegate of nativeint * nativeint * nativeint -> nativeint
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type DlClose = delegate of nativeint * nativeint -> unit
    [<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
    type SetSystemCall = delegate of nativeint * nativeint * nativeint -> int

/// Delegates are bound once per native file, never on each locking operation.
[<Sealed>]
type internal StorageNativeMethods(address: nativeint) =
    let version = Marshal.ReadInt32(address)
    let close = StorageNative.bindAt<StorageNative.One> version address StorageNative.methodFields 0 1
    let lockFile = StorageNative.bindAt<StorageNative.IntArg> version address StorageNative.methodFields 6 1
    let unlock = StorageNative.bindAt<StorageNative.IntArg> version address StorageNative.methodFields 7 1
    let reserved = StorageNative.bindAt<StorageNative.Output> version address StorageNative.methodFields 8 1
    let control = StorageNative.bindAt<StorageNative.Control> version address StorageNative.methodFields 9 1
    let map = StorageNative.bindAt<StorageNative.ShmMap> version address StorageNative.methodFields 12 2
    let shmLock = StorageNative.bindAt<StorageNative.ShmLock> version address StorageNative.methodFields 13 2
    let barrier = StorageNative.bindAt<StorageNative.Barrier> version address StorageNative.methodFields 14 2
    let unmap = StorageNative.bindAt<StorageNative.IntArg> version address StorageNative.methodFields 15 2
    member _.HasLocks = not(isNull lockFile || isNull unlock || isNull reserved)
    member _.HasSharedMemory = not(isNull map || isNull shmLock || isNull barrier || isNull unmap)
    member _.Close(file) = close.Invoke(file)
    member _.Lock(file, level) = lockFile.Invoke(file, level)
    member _.Unlock(file, level) = unlock.Invoke(file, level)
    member _.Reserved(file, output) = reserved.Invoke(file, output)
    member _.Control(file, operation, argument) = if isNull control then 12 else control.Invoke(file, operation, argument)
    member this.Map(file, region, size, extend, output) =
        if this.HasSharedMemory then map.Invoke(file, region, size, extend, output)
        else Marshal.WriteIntPtr(output, 0n); 5386
    member this.ShmLock(file, offset, count, flags) =
        if this.HasSharedMemory then shmLock.Invoke(file, offset, count, flags) else 5130
    member _.Barrier(file) = if not(isNull barrier) then barrier.Invoke(file)
    member _.Unmap(file, delete) = if isNull unmap then 0 else unmap.Invoke(file, delete)

/// Borrows a registered parent. The registration's external owner must outlive use.
[<Sealed>]
type internal StorageNativeVfs(name: string) =
    let address =
        if isNull name then StorageNative.sqlite3_vfs_find(0n)
        else
            let text = StorageNative.utf8 name
            try StorageNative.sqlite3_vfs_find(text) finally Marshal.FreeHGlobal(text)
    do if address = 0n then invalidOp "The native VFS is not registered."
    let version = Marshal.ReadInt32(address)
    let openFile = StorageNative.bindAt<StorageNative.Open> version address StorageNative.vfsFields 3 1
    let delete = StorageNative.bindAt<StorageNative.Path> version address StorageNative.vfsFields 4 1
    let access = StorageNative.bindAt<StorageNative.Access> version address StorageNative.vfsFields 5 1
    let fullPath = StorageNative.bindAt<StorageNative.FullPath> version address StorageNative.vfsFields 6 1
    let dlOpen = StorageNative.bindAt<StorageNative.DlOpen> version address StorageNative.vfsFields 7 1
    let dlError = StorageNative.bindAt<StorageNative.DlError> version address StorageNative.vfsFields 8 1
    let dlSym = StorageNative.bindAt<StorageNative.DlSym> version address StorageNative.vfsFields 9 1
    let dlClose = StorageNative.bindAt<StorageNative.DlClose> version address StorageNative.vfsFields 10 1
    let random = StorageNative.bindAt<StorageNative.Random> version address StorageNative.vfsFields 11 1
    let sleep = StorageNative.bindAt<StorageNative.IntArg> version address StorageNative.vfsFields 12 1
    let time = StorageNative.bindAt<StorageNative.Output> version address StorageNative.vfsFields 13 1
    let error = StorageNative.bindAt<StorageNative.Random> version address StorageNative.vfsFields 14 1
    let time64 = StorageNative.bindAt<StorageNative.Output> version address StorageNative.vfsFields 15 2
    let setSystem = StorageNative.bindAt<StorageNative.SetSystemCall> version address StorageNative.vfsFields 16 3
    let getSystem = StorageNative.bindAt<StorageNative.DlOpen> version address StorageNative.vfsFields 17 3
    let nextSystem = StorageNative.bindAt<StorageNative.DlOpen> version address StorageNative.vfsFields 18 3
    member _.Identity = address
    member _.Name = StorageNative.text(StorageNative.field address StorageNative.vfsFields 1)
    member _.FileSize = Marshal.ReadInt32(address, 4)
    member _.MaxPath = Marshal.ReadInt32(address, 8)
    member _.Open(path, file, flags, output) = openFile.Invoke(address, path, file, flags, output)
    member _.Delete(path, sync) = delete.Invoke(address, path, sync)
    member _.Access(path, flags, output) = access.Invoke(address, path, flags, output)
    member _.FullPath(path, size, output) = fullPath.Invoke(address, path, size, output)
    member _.DlOpen(path) = if isNull dlOpen then 0n else dlOpen.Invoke(address, path)
    member _.DlError(size, output) =
        if isNull dlError then
            if size > 0 then Marshal.WriteByte(output, 0uy)
        else dlError.Invoke(address, size, output)
    member _.DlSym(library, symbol) = if isNull dlSym then 0n else dlSym.Invoke(address, library, symbol)
    member _.DlClose(library) = if not(isNull dlClose) then dlClose.Invoke(address, library)
    member _.Random(size, output) = random.Invoke(address, size, output)
    member _.Sleep(delay) = sleep.Invoke(address, delay)
    member _.Time(output) = time.Invoke(address, output)
    member _.Error(size, output) =
        if isNull error then
            if size > 0 then Marshal.WriteByte(output, 0uy)
            0
        else error.Invoke(address, size, output)
    member _.Time64(output) =
        if not(isNull time64) then time64.Invoke(address, output)
        else
            let memory = StorageNative.alloc 8
            try
                let result = time.Invoke(address, memory)
                if result = 0 then Marshal.WriteInt64(output, int64(Marshal.PtrToStructure<double>(memory) * 86400000.0))
                result
            finally Marshal.FreeHGlobal(memory)
    member _.SetSystemCall(name, callback) = if isNull setSystem then 12 else setSystem.Invoke(address, name, callback)
    member _.GetSystemCall(name) = if isNull getSystem then 0n else getSystem.Invoke(address, name)
    member _.NextSystemCall(name) = if isNull nextSystem then 0n else nextSystem.Invoke(address, name)

/// One actual native file receiver and filename, retained through its xClose.
[<Sealed>]
type internal StorageNativeFile(parent: StorageNativeVfs, name: string, wal: bool) =
    let mutable file = StorageNative.alloc parent.FileSize
    let mutable filename = 0n
    let mutable identity = struct (0n, "")
    let mutable methods: StorageNativeMethods = Unchecked.defaultof<_>
    do
        try
            let path = StorageNative.utf8 name
            let output = StorageNative.alloc (parent.MaxPath + 1)
            try
                let result = parent.FullPath(path, parent.MaxPath + 1, output)
                if result <> 0 then raise(StorageNativeError(result, "The coordination path could not be resolved."))
                let resolved = StorageNative.text output
                filename <- StorageNative.filename resolved
                identity <- struct (parent.Identity, resolved)
            finally
                Marshal.FreeHGlobal(output)
                Marshal.FreeHGlobal(path)
            let output = StorageNative.alloc 4
            let result, flags =
                try
                    let result = parent.Open(filename, file, 0x106, output)
                    result, Marshal.ReadInt32(output)
                finally Marshal.FreeHGlobal(output)
            let table = Marshal.ReadIntPtr(file)
            if table <> 0n then methods <- StorageNativeMethods(table)
            if result <> 0 then raise(StorageNativeError(result, "The native coordination file could not be opened."))
            if flags &&& 2 = 0 then raise(StorageNativeError(8, "The coordination file requires native write access."))
            if isNull (box methods) || not methods.HasLocks || (wal && not methods.HasSharedMemory) then
                raise(StorageConfigurationError("The coordination VFS lacks the required native locking or shared-memory methods."))
        with _ ->
            if not(isNull (box methods)) then methods.Close(file) |> ignore
            Marshal.FreeHGlobal(file)
            StorageNative.sqlite3_free_filename(filename)
            file <- 0n
            filename <- 0n
            reraise()
    member _.HasSharedMemory = methods.HasSharedMemory
    member _.Identity = identity
    member _.Lock(level) = methods.Lock(file, level)
    member _.Unlock(level) = methods.Unlock(file, level)
    member _.Reserved(output) = methods.Reserved(file, output)
    member _.Control(operation, argument) = methods.Control(file, operation, argument)
    member _.Map(region, size, extend, output) = methods.Map(file, region, size, extend, output)
    member _.ShmLock(offset, count, flags) = methods.ShmLock(file, offset, count, flags)
    member _.Barrier() = methods.Barrier(file)
    member _.Unmap(delete) = methods.Unmap(file, delete)
    member _.Close() =
        if file = 0n then 0 else
        let mutable result = 0
        let mutable failure: exn = null
        try result <- methods.Unmap(file, 0) with error -> failure <- error
        try
            let closed = methods.Close(file)
            if result = 0 then result <- closed
        with error -> if isNull failure then failure <- error
        Marshal.FreeHGlobal(file)
        StorageNative.sqlite3_free_filename(filename)
        file <- 0n
        filename <- 0n
        if not(isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()
        result
    interface ISqliteStorageCoordinationSession with
        member _.DomainIdentity = box identity
        member _.HasSharedMemory = methods.HasSharedMemory
        member this.Lock(level) = this.Lock(level)
        member this.Unlock(level) = this.Unlock(level)
        member _.CheckReservedLock() =
            let output = NativePtr.stackalloc<int> 1
            NativePtr.write output 0
            let result = methods.Reserved(file, NativePtr.toNativeInt output)
            StorageReservedLockResult(result, NativePtr.read output <> 0)
        member _.MapSharedMemory(region, size, extend) =
            let output = NativePtr.stackalloc<nativeint> 1
            NativePtr.write output 0n
            let result = methods.Map(file, region, size, extend, NativePtr.toNativeInt output)
            StorageSharedMemoryResult(result, NativePtr.read output)
        member this.LockSharedMemory(offset, count, flags) = this.ShmLock(offset, count, flags)
        member this.SharedMemoryBarrier() = this.Barrier()
        member this.UnmapSharedMemory(delete) = this.Unmap(delete)
        member this.FileControl(operation, argument) = this.Control(operation, argument)
        member this.Close() = this.Close()
