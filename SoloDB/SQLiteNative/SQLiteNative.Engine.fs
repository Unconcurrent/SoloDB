namespace SoloDatabase

open System
open System.Data.Common
open System.Diagnostics
open System.Threading
open SQLitePCL

/// The logical state of a native database connection.
type SqliteConnectionState =
    | Closed = 0
    | Open = 1

module internal NativeConnectionDefaults =
    [<Literal>]
    let commandTimeout = 30

/// Shared numeric error contract for native and public callback failures.
type internal ISqliteError =
    abstract ResultCode: int

/// A SQLite failure with its primary and extended engine result codes.
type NativeSqliteException(message: string, errorCode: int, extendedErrorCode: int) =
    inherit Exception(message)
    new(message: string, errorCode: int) = NativeSqliteException(message, errorCode, errorCode)
    member _.SqliteErrorCode = errorCode
    member _.SqliteExtendedErrorCode = extendedErrorCode
    interface ISqliteError with member _.ResultCode = extendedErrorCode
    static member internal FromResult(handle: sqlite3, result: int) =
        let primary = result &&& 255
        let extended = if isNull handle || handle.IsInvalid then result else raw.sqlite3_extended_errcode(handle)
        let text = if isNull handle || handle.IsInvalid then raw.sqlite3_errstr(result).utf8_to_string() else raw.sqlite3_errmsg(handle).utf8_to_string()
        NativeSqliteException($"SQLite Error {primary}: '{text}'.", primary, extended)

[<AllowNullLiteral>]
type internal INativeExecutionObserver =
    abstract After: unit -> unit
    abstract Failed: exn -> unit
    abstract Checkpoint: unit -> int
    abstract Restore: int -> unit

/// A scoped observation of native transaction completion, independent of presentation APIs.
[<Sealed>]
type internal NativeTransactionWatch(epoch: int64, release: Action) =
    let mutable disposed = false
    member _.Epoch = epoch
    member _.Dispose() =
        if not disposed then
            disposed <- true
            release.Invoke()
    interface IDisposable with member this.Dispose() = this.Dispose()

/// Owns the configured engine handle and the logical connection lifetime.
[<Sealed>]
type internal NativeDatabase(connectionString: string) =
    static let initialization = lazy (
        // A caller may have selected a provider before the first connection.
        try raw.GetNativeLibraryName() |> ignore
        with _ -> Batteries_V2.Init())
    let settings = DbConnectionStringBuilder(ConnectionString = connectionString)
    let mutable handle: sqlite3 = null
    let mutable openState = false
    let mutable generation = 0L
    let mutable disposed = false
    let mutable nativeUtf16 = false
    let mutable observer: INativeExecutionObserver = null
    let mutable callbackEpoch = 0L
    let mutable callbackFailure: exn = null
    let mutable transactionEpoch = 0L
    let mutable rollbackGeneration = -1L
    let mutable completionGeneration = -1L
    let mutable completionEpoch = -1L
    let mutable transactionWatchCount = 0
    let mutable rollbackCallback: delegate_rollback = null
    let setting key fallback = if settings.ContainsKey key then string settings.[key] else fallback
    let source = setting "Data Source" (setting "DataSource" (setting "Filename" ""))
    let mode = setting "Mode" "ReadWriteCreate"
    let cache = setting "Cache" "Default"
    let mutable timeout = Int32.Parse(setting "Default Timeout" (setting "Command Timeout" (string NativeConnectionDefaults.commandTimeout)), Globalization.CultureInfo.InvariantCulture)

    static member internal EnsureInitialized() = initialization.Force()
    member _.CallbackEpoch = callbackEpoch
    member _.ObserveCallback() = callbackEpoch <- callbackEpoch + 1L
    member _.RecordCallbackFailure(expectedGeneration, error: exn) =
        if openState && generation = expectedGeneration then
            if isNull callbackFailure then callbackFailure <- error
            raw.sqlite3_interrupt(handle)
    member _.HasCallbackFailure = not (isNull callbackFailure)
    member _.RaiseCallbackFailure() =
        if not (isNull callbackFailure) then
            let error = callbackFailure
            callbackFailure <- null
            Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw()
    member val ReaderActive = false with get, set
    member val ReaderToken = 0L with get, set
    member this.TransactionEpoch = this.ObserveStatementCompletion(); transactionEpoch
    member this.EnsureRollbackTracking() =
        this.EnsureOpen()
        if rollbackGeneration <> generation then
            let expected = generation
            rollbackCallback <- delegate_rollback(fun _ ->
                if openState && generation = expected then
                    transactionEpoch <- transactionEpoch + 1L)
            raw.sqlite3_rollback_hook(handle, rollbackCallback, null)
            rollbackGeneration <- generation
    member this.WatchTransaction(onReleased: Action<NativeTransactionWatch>) =
        this.EnsureOpen()
        if raw.sqlite3_get_autocommit(handle) <> 0 then raise (InvalidOperationException("There is no active transaction to observe."))
        this.EnsureRollbackTracking()
        if completionGeneration <> generation || completionEpoch <> transactionEpoch then
            transactionWatchCount <- 0
            completionGeneration <- generation
            completionEpoch <- transactionEpoch
        transactionWatchCount <- transactionWatchCount + 1
        let expected = generation
        let expectedEpoch = transactionEpoch
        let mutable watch: NativeTransactionWatch = Unchecked.defaultof<_>
        watch <- new NativeTransactionWatch(transactionEpoch, Action(fun () ->
            try
                if openState && generation = expected && completionEpoch = expectedEpoch && transactionEpoch = expectedEpoch then
                    transactionWatchCount <- transactionWatchCount - 1
                    if transactionWatchCount = 0 then
                        raw.sqlite3_rollback_hook(handle, null, null)
                        rollbackGeneration <- -1L
            finally onReleased.Invoke(watch)))
        watch
    member _.ObserveStatementCompletion() =
        if transactionWatchCount > 0 then
            // Actual completion covers read-only commits too. Failed commits stay active.
            if completionEpoch = transactionEpoch && openState && raw.sqlite3_get_autocommit(handle) <> 0 then
                transactionEpoch <- transactionEpoch + 1L
            if completionEpoch <> transactionEpoch then
                transactionWatchCount <- 0
                raw.sqlite3_rollback_hook(handle, null, null)
                rollbackGeneration <- -1L
    member _.UsesNativeUtf16 = nativeUtf16
    member _.Password = setting "Password" ""
    member _.RecursiveTriggers = Boolean.Parse(setting "Recursive Triggers" "false")
    member _.SharedCache = cache.Equals("Shared", StringComparison.OrdinalIgnoreCase)
    member _.Generation = generation
    member this.EnsureGeneration(expected: int64) =
        this.EnsureOpen()
        if generation <> expected then raise (InvalidOperationException("The owning connection was closed and reopened."))
    member _.ConnectionString = connectionString
    member _.DataSource = source
    member _.State = if openState then SqliteConnectionState.Open else SqliteConnectionState.Closed
    member _.IsOpen = openState && not disposed
    member _.Observer with get() = observer and set value = observer <- value
    member _.DefaultTimeout with get() = timeout and set value = if value < 0 then invalidArg "value" "Timeout must be nonnegative." else timeout <- value
    member _.EnsureOpen() =
        if disposed then raise (ObjectDisposedException("NativeSqliteConnection"))
        if not openState then raise (InvalidOperationException("The connection is not open."))
    member this.Handle = this.EnsureOpen(); handle
    member _.Check(result: int) =
        if result <> raw.SQLITE_OK then raise (NativeSqliteException.FromResult(handle, result))
    member this.Open() =
        if disposed then raise (ObjectDisposedException("NativeSqliteConnection"))
        if not openState then
            initialization.Force()
            nativeUtf16 <- String.Equals(raw.GetNativeLibraryName(), "e_sqlite3", StringComparison.Ordinal)
            let flags =
                (match mode.ToUpperInvariant() with
                 | "READONLY" -> raw.SQLITE_OPEN_READONLY
                 | "READWRITE" -> raw.SQLITE_OPEN_READWRITE
                 | "MEMORY" -> raw.SQLITE_OPEN_READWRITE ||| raw.SQLITE_OPEN_CREATE ||| raw.SQLITE_OPEN_MEMORY
                 | "READWRITECREATE" -> raw.SQLITE_OPEN_READWRITE ||| raw.SQLITE_OPEN_CREATE
                 | _ -> invalidArg "connectionString" "Invalid SQLite open mode.")
                ||| raw.SQLITE_OPEN_URI ||| raw.SQLITE_OPEN_FULLMUTEX
                ||| (match cache.ToUpperInvariant() with
                     | "SHARED" -> raw.SQLITE_OPEN_SHAREDCACHE
                     | "PRIVATE" -> raw.SQLITE_OPEN_PRIVATECACHE
                     | "DEFAULT" -> 0
                     | _ -> invalidArg "connectionString" "Invalid SQLite cache mode.")
            let filename =
                if mode.Equals("Memory", StringComparison.OrdinalIgnoreCase) && not (source.StartsWith("file:", StringComparison.OrdinalIgnoreCase)) then "file:" + source
                else source
            let result = raw.sqlite3_open_v2(filename, &handle, flags, (null: string))
            if result <> raw.SQLITE_OK then
                let failure = NativeSqliteException.FromResult(handle, result)
                if not (isNull handle) then handle.Dispose(); handle <- null
                raise failure
            openState <- true
            generation <- generation + 1L
            try
                this.Check(raw.sqlite3_extended_result_codes(handle, 1))
                this.Check(raw.sqlite3_busy_timeout(handle, 0))
                if settings.ContainsKey "Foreign Keys" then
                    let enabled = Boolean.Parse(string settings.["Foreign Keys"])
                    let mutable actual = 0
                    this.Check(raw.sqlite3_db_config(handle, raw.SQLITE_DBCONFIG_ENABLE_FKEY, (if enabled then 1 else 0), &actual))
            with _ ->
                openState <- false
                handle.Dispose()
                handle <- null
                reraise()
    member this.Close() =
        callbackFailure <- null
        this.ReaderActive <- false
        if openState then
            openState <- false
            handle.Dispose()
            handle <- null
    member this.Dispose() =
        if not disposed then this.Close(); disposed <- true

/// One command owns its lock-wait budget across preparation and execution.
[<Struct>]
type internal NativeExecutionBudget =
    val private timeout: int
    val mutable private elapsed: int64
    new(timeout) = { timeout = timeout; elapsed = 0L }
    member this.CanRetry(started: int64) =
        this.timeout = 0 || float (this.elapsed + Stopwatch.GetTimestamp() - started) / float Stopwatch.Frequency < float this.timeout
    member this.Record(started: int64) = this.elapsed <- this.elapsed + Stopwatch.GetTimestamp() - started

module internal NativeExecution =
    let isLocked result =
        let primary = result &&& 255
        // A stale WAL snapshot cannot become writable by waiting. Its owner must
        // decide whether to restart the transaction; never replay it here.
        result <> 517 && (primary = raw.SQLITE_BUSY || primary = raw.SQLITE_LOCKED)
    let wait () = Thread.Sleep(150)
