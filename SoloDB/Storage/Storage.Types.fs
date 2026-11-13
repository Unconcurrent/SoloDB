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
    member val LiveDomain: obj = null with get,set
    member val PrivateDomain: obj = null with get,set

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

/// SQLite's result code and the reserved-lock query output. Output is meaningful on success.
[<Struct>]
type StorageReservedLockResult =
    val ResultCode: int
    val IsReserved: bool
    new(resultCode, isReserved) = { ResultCode = resultCode; IsReserved = isReserved }

/// SQLite's result code and a WAL-index region. A successful null address means absent.
[<Struct>]
type StorageSharedMemoryResult =
    val ResultCode: int
    val Address: nativeint
    new(resultCode, address) = { ResultCode = resultCode; Address = address }

/// One physical main-file open's SQLite coordination. Calls may change threads and
/// must not reenter the database. Return SQLite result codes (including extended errors);
/// thrown exceptions retain their cause through the physical connection's fault channel.
/// Close releases only this session's grants/mappings and returns its SQLite close result.
[<AllowNullLiteral>]
type ISqliteStorageCoordinationSession =
    /// Stable nonnull equality identity for compatible sessions on this database's bytes.
    /// Distinct databases must not share locks or WAL-index regions accidentally.
    abstract DomainIdentity: obj
    abstract HasSharedMemory: bool
    /// SQLite lock levels: SHARED=1, RESERVED=2, PENDING=3, EXCLUSIVE=4.
    /// A failed upgrade must retain the grants required by SQLite's locking contract.
    abstract Lock: level: int -> int
    /// SQLite unlock targets: NONE=0 or SHARED=1.
    abstract Unlock: level: int -> int
    abstract CheckReservedLock: unit -> StorageReservedLockResult
    /// Every nonzero extend value permits extension, including SQLite recovery value 2.
    /// Successful mapped addresses remain stable until this session unmaps or closes.
    abstract MapSharedMemory: region: int * size: int * extend: int -> StorageSharedMemoryResult
    /// SQLite WAL slot flags: UNLOCK=1, LOCK=2, SHARED=4, EXCLUSIVE=8.
    abstract LockSharedMemory: offset: int * count: int * flags: int -> int
    abstract SharedMemoryBarrier: unit -> unit
    abstract UnmapSharedMemory: delete: int -> int
    /// Only coordination controls are forwarded; return SQLITE_NOTFOUND=12 when unsupported.
    /// The native argument is borrowed for this call and follows SQLite's control ABI.
    abstract FileControl: operation: int * argument: nativeint -> int
    /// Called once, after native completion or failed opening; release all owned resources.
    abstract Close: unit -> int

/// A borrowed reusable provider. Return a distinct session for every physical main open.
/// The main byte view and provider must remain usable through the session's Close.
/// Independently opened byte views must be coherent regardless of coordination choice.
[<AllowNullLiteral>]
type ISqliteStorageCoordination =
    abstract Open: main: ISqliteStorageFile * profile: SqliteStorageProfile -> ISqliteStorageCoordinationSession
