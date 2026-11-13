namespace SoloDatabase

open System
open System.IO

/// Built-in SQLite coordination providers. Providers are borrowed; each physical
/// main-file open owns a separate session until native close completes.
[<Sealed; AllowNullLiteral>]
type SqliteStorageCoordination private (filename: string, registeredVfs: string) =
    static let singleProcess =
        { new ISqliteStorageCoordination with
            member _.Open(main, profile) = StorageMemory.openSession main profile }
    member internal _.Filename = filename
    member internal _.RegisteredVfs = registeredVfs
    /// Private locks and WAL memory shared by actual main backing object identity.
    /// This provider supplies no multiprocess correctness guarantee.
    static member SingleProcessInMemory = singleProcess
    /// Uses the native filesystem's locks and shared memory at a dedicated path.
    /// All participants must expose coherent bytes and keep that path intact.
    static member File(filename: string) =
        StorageValidation.filename "filename" filename
        SqliteStorageCoordination(Path.GetFullPath(filename), null)
    /// Borrows the named native VFS. Keep its actual registration alive through every session.
    static member File(filename: string, registeredVfs: string) =
        StorageValidation.filename "filename" filename
        StorageValidation.filename "registeredVfs" registeredVfs
        SqliteStorageCoordination(filename, registeredVfs)
    interface ISqliteStorageCoordination with
        member _.Open(main, profile) =
            if isNull (box main) then nullArg "main"
            StorageValidation.profile profile
            StorageNativeFile(StorageNativeVfs(registeredVfs), filename, (profile = SqliteStorageProfile.Wal))
                :> ISqliteStorageCoordinationSession
