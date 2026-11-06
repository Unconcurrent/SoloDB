namespace SoloDatabase

open System
open System.IO
open System.Runtime.CompilerServices

/// State attached to the supplied object, not a database or coordination identity.
[<Sealed>]
type internal StorageObjectState() =
    member val Gate = obj()
    member val References = 0 with get,set
    member val CloseRequested = false with get,set
    member val Closing = false with get,set

module internal StorageObjects =
    let private states = ConditionalWeakTable<obj, StorageObjectState>()
    let get (value: obj) = states.GetValue(value, fun _ -> StorageObjectState())

/// Library-established admission failures, distinct from exceptions thrown by a backend.
[<Sealed>]
type internal StorageConfigurationError(message: string) =
    inherit NotSupportedException(message)

/// The requested SQLite journal or immutable-image profile.
type SqliteStorageProfile =
    | Simple = 0
    | Wal = 1
    | ReadOnly = 2

/// A structured backing failure, independent of exception message text.
type StorageFailureKind =
    | IoError = 0
    | Full = 1
    | ReadOnly = 2

module internal StorageValidation =
    let failureKind kind =
        match kind with
        | StorageFailureKind.IoError | StorageFailureKind.Full | StorageFailureKind.ReadOnly -> ()
        | _ -> raise(ArgumentOutOfRangeException("kind"))
    let profile value =
        match value with
        | SqliteStorageProfile.Simple | SqliteStorageProfile.Wal | SqliteStorageProfile.ReadOnly -> ()
        | _ -> raise(ArgumentOutOfRangeException("profile"))
    let filename name (value: string) =
        if isNull value then nullArg name
        if value.Length = 0 || value.IndexOf('\000') >= 0 then
            invalidArg name "The name must be nonempty and cannot contain NUL."
    let bounds position count =
        if position < 0L || int64 count > Int64.MaxValue - position then
            raise(ArgumentOutOfRangeException("position"))
    let writable readOnly =
        if readOnly then invalidOp "ReadOnly storage cannot perform write administration."

/// An immutable failure and its optional original managed cause. Null means success.
[<Sealed; AllowNullLiteral>]
type StorageFailure(kind: StorageFailureKind, cause: exn) =
    do StorageValidation.failureKind kind
    member _.Kind = kind
    member _.Cause = cause

/// Bytes supplied by one positional read, or its structured failure.
[<Struct>]
type StorageReadResult =
    val Count: int
    val Failure: StorageFailure
    new(count, failure) = { Count = count; Failure = failure }

/// The current logical length, or the operation's structured failure.
[<Struct>]
type StorageLengthResult =
    val Length: int64
    val Failure: StorageFailure
    new(length, failure) = { Length = length; Failure = failure }

/// A logical byte file. Calls for one supplied view are serialized and may change threads.
/// Reads may fragment; zero means EOF. Successful writes fill the whole span and expose
/// zero-filled gaps. Length changes discard the truncated tail and zero-fill extension.
/// Failed writes preserve bytes outside their range, cannot shrink length, and cannot grow
/// beyond max(previous length, requested end). Failed resize preserves its retained prefix.
/// Flush supplies the backend's actual ordered bytes/length durability barrier.
/// Shared objects are serialized by identity; independent views must expose coherent bytes.
/// Methods must not reenter the database.
type ISqliteStorageFile =
    abstract ReadAt: position: int64 * destination: Span<byte> -> StorageReadResult
    abstract WriteAt: position: int64 * source: ReadOnlySpan<byte> -> StorageFailure
    abstract Flush: unit -> StorageFailure
    abstract GetLength: unit -> StorageLengthResult
    abstract SetLength: length: int64 -> StorageFailure

/// A Stream may use this exception to supply a known structured failure kind.
[<Sealed>]
type StorageIOException(kind: StorageFailureKind, message: string, cause: exn) =
    inherit IOException(message, cause)
    do StorageValidation.failureKind kind
    member _.Kind = kind

/// Reusable same-host coordination configuration. Each physical open acquires its own session.
/// Participants use independent coherent byte views and the same dedicated identity, which
/// must not be independently closed, unlinked or replaced while participation continues.
[<Sealed; AllowNullLiteral>]
type SqliteStorageCoordination private (filename: string, registeredVfs: string) =
    member internal _.Filename = filename
    member internal _.RegisteredVfs = registeredVfs
    /// Uses the native filesystem's locks and shared memory at a dedicated path.
    static member File(filename: string) =
        StorageValidation.filename "filename" filename
        SqliteStorageCoordination(Path.GetFullPath(filename), null)
    /// Borrows the named native VFS. Keep its actual registration alive through every session.
    static member File(filename: string, registeredVfs: string) =
        StorageValidation.filename "filename" filename
        StorageValidation.filename "registeredVfs" registeredVfs
        SqliteStorageCoordination(filename, registeredVfs)
