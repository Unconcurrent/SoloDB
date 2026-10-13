namespace SoloDatabase

open System
open SoloDatabase.RawSqliteFunctions
open SoloDBEventsHelpers
open Microsoft.FSharp.NativeInterop

#nowarn "9"

module internal FileSystemHandlerInvocation =
    let invoke (handler: FileSystemHandlerSystem) connection session facts =
        handler.Invoke(connection, session, facts)
    let text (value: SqliteRawValue) =
        if value.IsNull then { Pointer = 0n; Length = -1 }
        else { Pointer = NativePtr.toNativeInt(value.GetBlobPointer()); Length = value.GetByteCount() }
    let row (args: ReadOnlySpan<SqliteRawValue>) start =
        { Path = text args.[start]; Name = text args.[start + 1]
          Length = args.[start + 2].GetInt64()
          Created = args.[start + 3].GetInt64(); Modified = args.[start + 4].GetInt64() }

type private FileSystemDeletePaths() =
    let paths = ResizeArray<struct (int64 * bool * BorrowedEventText)>()
    member _.Begin(owner, directory, path: BorrowedEventText) =
        let pointer = System.Runtime.InteropServices.Marshal.AllocHGlobal(path.Length)
        try
            ReadOnlySpan<byte>(path.Pointer.ToPointer(), path.Length).CopyTo(Span<byte>(pointer.ToPointer(), path.Length))
            paths.Add(struct (owner, directory, {Pointer = pointer; Length = path.Length}))
        with _ ->
            System.Runtime.InteropServices.Marshal.FreeHGlobal(pointer)
            reraise()
    member _.Find(owner, directory) =
        let mutable result = Unchecked.defaultof<BorrowedEventText>
        let mutable i = paths.Count - 1
        while i >= 0 && result.Pointer = 0n do
            let struct (id, isDirectory, path) = paths.[i]
            if id = owner && directory = isDirectory then result <- path
            i <- i - 1
        result
    member _.End(owner, directory) =
        let mutable i = paths.Count - 1
        let mutable found = false
        while i >= 0 && not found do
            let struct (id, isDirectory, path) = paths.[i]
            if id = owner && directory = isDirectory then
                System.Runtime.InteropServices.Marshal.FreeHGlobal(path.Pointer)
                paths.RemoveAt(i)
                found <- true
            i <- i - 1
    member _.Restore(depth) =
        while paths.Count > depth do
            let last = paths.Count - 1
            let struct (_, _, path) = paths.[last]
            System.Runtime.InteropServices.Marshal.FreeHGlobal(path.Pointer)
            paths.RemoveAt(last)
    member this.Reset() = this.Restore(0)
    override this.Finalize() = this.Reset()
    interface IFileSystemCallbackResources with
        member _.Depth = paths.Count
        member this.Restore(depth) = this.Restore(depth)
        member this.Reset() = this.Reset()

/// SQL row producers and managed logical producers use the same dispatch boundary.
type internal FileSystemEventDispatch(globalLock: UtilsReflection.ReentrantSpinLock) =
    let mutable sessionIndex = 0L
    let handlers = Array.init 18 (fun _ -> EventHandlerList<FileSystemHandlerSystem>(ignore))
    member _.Handlers = handlers
    member val Registration: ISoloDBFileSystemEvents = Unchecked.defaultof<_> with get, set

    member _.Run(connection, kind: FileSystemEventKind, change) =
        let entry = handlers.[int kind]
        runHandlerBody globalLock &sessionIndex connection entry.Handlers change
            FileSystemHandlerInvocation.invoke entry.RemoveMapping "Filesystem event handler failed."

    member this.CreateFunctions(connection: NativeSqliteConnection) =
        let paths = FileSystemDeletePaths()

        match connection with
        | :? NativeSQLiteTools.NativeCachingDbConnection as cached ->
            cached.FileSystemEventDispatch <- this
            cached.FileSystemCallbackResources <- paths
        | _ -> ()
        connection.CreateRawFunction("SOLO_FS_DELETING_BEGIN", RawScalarFunc3(fun ctx owner directory path ->
            let isDirectory = directory.GetInt32() <> 0
            let start = if isDirectory then 14 else 12
            if handlers.[start].Handlers.Count <> 0 || handlers.[start + 1].Handlers.Count <> 0 then
                paths.Begin(owner.GetInt64(), isDirectory, FileSystemHandlerInvocation.text path)
            ctx.SetNull()))
        connection.CreateRawFunction("SOLO_FS_DELETING_END", RawScalarFunc2(fun ctx owner directory ->
            paths.End(owner.GetInt64(), directory.GetInt32() <> 0)
            ctx.SetNull()))
        connection.CreateRawFunction("SOLO_FS_DELETING_PATH", RawScalarFunc2(fun ctx owner directory ->
            let path = paths.Find(owner.GetInt64(), directory.GetInt32() <> 0)
            if path.Pointer = 0n then ctx.SetNull()
            else ctx.SetBlob(ReadOnlySpan<byte>(path.Pointer.ToPointer(), path.Length))))
        connection.CreateRawFunction("SOLO_FS_SHOULD_HANDLE", RawScalarFunc1(fun ctx kind ->
            let index = kind.GetInt32()
            if uint index >= uint handlers.Length then invalidArg (nameof kind) "Unknown filesystem event kind."
            let observeMutation =
                index < 12 &&
                    match connection with
                    | :? NativeSQLiteTools.NativeCachingDbConnection as cached -> cached.EventDispatchState.Depth > 0
                    | _ -> false
            ctx.SetInt32(if handlers.[index].Handlers.Count = 0 && not observeMutation then 0 else 1)))
        connection.CreateRawFunction("SOLO_FS_ROW", 11, RawScalarFunc(fun ctx args ->
            let kind = enum<FileSystemEventKind>(args.[0].GetInt32())
            match connection with
            | :? NativeSQLiteTools.NativeCachingDbConnection as cached -> cached.EventDispatchState.RecordMutation()
            | _ -> ()
            let change = Row(FileSystemHandlerInvocation.row args 1, FileSystemHandlerInvocation.row args 6)
            let struct (failed, message) = this.Run(connection, kind, change)
            if failed then ctx.SetText(message) else ctx.SetNull()))
        connection.CreateRawFunction("SOLO_FS_METADATA", 6, RawScalarFunc(fun ctx args ->
            let kind = enum<FileSystemEventKind>(args.[0].GetInt32())
            let change = Metadata {
                Path = FileSystemHandlerInvocation.text args.[1]
                Key = FileSystemHandlerInvocation.text args.[2]
                OldValue = FileSystemHandlerInvocation.text args.[3]
                NewValue = FileSystemHandlerInvocation.text args.[4]
                Kind = enum<FileMetadataChangeKind>(args.[5].GetInt32()) }
            let struct (failed, message) = this.Run(connection, kind, change)
            if failed then ctx.SetText(message) else ctx.SetNull()))

    interface IFileSystemEventDispatch with
        member _.HasContentHandlers = handlers.[16].Handlers.Count <> 0 || handlers.[17].Handlers.Count <> 0
        member this.Dispatch(connection, kind, change) =
            let struct (failed, message) = this.Run(connection, kind, change)
            if failed then raise (NativeSqliteException(message, SQLitePCL.raw.SQLITE_CONSTRAINT, SQLitePCL.raw.SQLITE_CONSTRAINT_TRIGGER))
