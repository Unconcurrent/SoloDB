namespace SoloDatabase

open System
open System.Data.Common
open System.Diagnostics
open System.Runtime.InteropServices
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
type NativeSqliteException internal (message: string, errorCode: int, extendedErrorCode: int, cause: exn) =
    inherit DbException(message, cause)
    new(message: string, errorCode: int, extendedErrorCode: int) = NativeSqliteException(message, errorCode, extendedErrorCode, null)
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
    let mutable active = 1
    member _.Epoch = epoch
    member _.Dispose() = if Interlocked.Exchange(&active, 0) = 1 then release.Invoke()
    interface IDisposable with member this.Dispose() = this.Dispose()

/// A tracked use ends only after its completing native call and error handling return.
[<Sealed; AllowNullLiteral>]
type internal NativeLifetimeUse(release: Action) =
    let mutable active = 1
    member _.Dispose() = if Interlocked.Exchange(&active, 0) = 1 then release.Invoke()
    interface IDisposable with member this.Dispose() = this.Dispose()

/// Owns the configured engine handle and the logical connection lifetime.
[<Sealed>]
type internal NativeDatabase(connectionString: string, storage: SqliteStorage option) as this =
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
    let closeGate = obj()
    let mutable retired = false
    let mutable disposeRequested = false
    let mutable closing = false
    let mutable nativeClosed = true
    let mutable uses = 0
    let mutable activeUses = 0
    let mutable draining = false
    let mutable statementsRetired = false
    let mutable retireStatements: Action = null
    let releaseActive = if storage.IsNone then null else Action(fun () -> this.ReleaseUse(true))
    let releaseStatement = if storage.IsNone then null else Action(fun () -> this.ReleaseUse(false))
    let mutable storageLease: SqliteStorageLease = Unchecked.defaultof<_>
    let mutable nativeRoot = Unchecked.defaultof<GCHandle>
    let completed = ResizeArray<Action>()
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

    new(connectionString: string) = NativeDatabase(connectionString, None)
    static member internal EnsureInitialized() = initialization.Force()
    member _.IsStorage = storage.IsSome
    member _.IsManagedStorage = match storage with Some input -> not input.IsNative | None -> false
    member _.Storage = storage
    member _.ReportClosingFailure(error: exn) =
        match storage with Some input -> input.ReportClosingFailure(error) | None -> ()
    member _.IsRetiring = retired
    member _.SetStatementRetirement(action: Action) = if storage.IsSome then retireStatements <- action
    member _.BeginStorageOperation() =
        if not(isNull (box storageLease)) then storageLease.Faults.Clear()
    member _.BindStorageClose(action: Action) =
        if not(isNull (box storageLease)) then storageLease.SetCloseRequest(action)
    member _.StorageFaults =
        if isNull (box storageLease) then None else Some storageLease.Faults
    member _.Error(result: int) =
        let fault = if isNull (box storageLease) then ValueNone else storageLease.Faults.Take()
        match fault with
        | ValueSome(struct (_, (:? StorageConfigurationError as error))) -> NotSupportedException(error.Message, error) :> exn
        | ValueSome(struct (code, cause)) ->
            let message = raw.sqlite3_errstr(code).utf8_to_string()
            NativeSqliteException($"SQLite Error {code &&& 255}: '{message}'.", code &&& 255, code, cause) :> exn
        | ValueNone -> NativeSqliteException.FromResult(handle, result) :> exn
    member _.CheckStorageOperation() =
        if not(isNull (box storageLease)) then
            match storageLease.Faults.Take() with
            | ValueNone -> ()
            | ValueSome(struct (code, cause)) ->
                let message = raw.sqlite3_errstr(code).utf8_to_string()
                raise(NativeSqliteException($"SQLite Error {code &&& 255}: '{message}'.", code &&& 255, code, cause))
    member _.AfterStorageClose(action: Action) =
        if storage.IsSome then
            let closed =
                lock closeGate (fun () ->
                    if nativeClosed then true
                    else
                        completed.Add(action)
                        false)
            if closed then action.Invoke()
    member private _.TryCloseStorage() =
        let drain =
            lock closeGate (fun () ->
                if retired && not nativeClosed && not closing && not draining && activeUses = 0 && not statementsRetired && not(isNull retireStatements) then
                    draining <- true
                    retireStatements
                else null)
        if not(isNull drain) then
            try
                try
                    drain.Invoke()
                    lock closeGate (fun () -> statementsRetired <- true)
                with error -> this.ReportClosingFailure(error)
            finally lock closeGate (fun () -> draining <- false)
        let claim =
            lock closeGate (fun () ->
                if retired && not nativeClosed && not disposed && not closing && not draining && uses = 0 && not(isNull (box storageLease)) then
                    closing <- true
                    true
                else false)
        if claim then
            let lease = storageLease
            let original = handle
            try
                try
                    lease.Faults.Clear()
                    // PCL manual_close zeros the SafeHandle and frees hook state even
                    // on BUSY. This direct call leaves the whole live owner intact.
                    let result =
                        if isNull original || original.IsInvalid then raw.SQLITE_OK
                        else StorageNative.sqlite3_close(original.DangerousGetHandle())
                    if result = raw.SQLITE_OK then
                        nativeClosed <- true
                        openState <- false
                        disposed <- disposeRequested
                        this.ReaderActive <- false
                        // No API that dereferences sqlite3* is allowed after OK.
                        if not(isNull original) then original.SetHandleAsInvalid()
                        try
                            match lease.Faults.Take() with
                            | ValueSome(struct (code, cause)) ->
                                let message = raw.sqlite3_errstr(code).utf8_to_string()
                                lease.Report(NativeSqliteException($"SQLite Error {code &&& 255}: '{message}'.", code &&& 255, code, cause))
                            | ValueNone -> ()
                        with error -> lease.Report(error)
                        if not(isNull original) then
                            try
                                let extra = original.GetOrCreateExtra<IDisposable>(Func<IDisposable>(fun () -> null))
                                if not(isNull extra) then extra.Dispose()
                            with error -> lease.Report(error)
                            try original.Dispose() with error -> lease.Report(error)
                        handle <- null
                        for action in completed do
                            try action.Invoke() with error -> lease.Report(error)
                        completed.Clear()
                        callbackFailure <- null
                        if disposeRequested then observer <- null
                        rollbackCallback <- null
                        transactionWatchCount <- 0
                        rollbackGeneration <- -1L
                        try lease.Complete() with error -> lease.Report(error)
                        storageLease <- Unchecked.defaultof<_>
                        if nativeRoot.IsAllocated then nativeRoot.Free()
                    else lease.Report(this.Error(result))
                with error -> lease.Report(error)
            finally lock closeGate (fun () -> closing <- false)
    member private _.ReleaseUse(active: bool) =
        lock closeGate (fun () ->
            if uses <= 0 || (active && activeUses <= 0) then invalidOp "Native lifetime use underflow."
            uses <- uses - 1
            if active then activeUses <- activeUses - 1)
        this.TryCloseStorage()
    member _.RetainUse() =
        if storage.IsNone then null else
        let lifetime = new NativeLifetimeUse(releaseActive)
        lock closeGate (fun () ->
            if disposed || not openState || closing then raise(ObjectDisposedException("NativeSqliteConnection"))
            uses <- uses + 1
            activeUses <- activeUses + 1)
        lifetime
    member _.RetainStatementUse() =
        if storage.IsNone then null else
        let lifetime = new NativeLifetimeUse(releaseStatement)
        lock closeGate (fun () ->
            if disposed || not openState || closing then raise(ObjectDisposedException("NativeSqliteConnection"))
            uses <- uses + 1)
        lifetime
    member private _.TryEnterUse(expected: int64) =
        storage.IsSome &&
        lock closeGate (fun () ->
            if disposed || not openState || closing || generation <> expected then false
            else
                uses <- uses + 1
                activeUses <- activeUses + 1
                true)
    member _.AdmitUse() =
        if storage.IsNone then null else
        let lifetime = new NativeLifetimeUse(releaseActive)
        lock closeGate (fun () ->
            if retired || disposed || not openState || closing then raise(ObjectDisposedException("NativeSqliteConnection"))
            uses <- uses + 1
            activeUses <- activeUses + 1)
        lifetime
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
        use lifetime = this.AdmitUse()
        this.EnsureOpen()
        if rollbackGeneration <> generation then
            let expected = generation
            rollbackCallback <- delegate_rollback(fun _ ->
                if openState && generation = expected then
                    transactionEpoch <- transactionEpoch + 1L)
            raw.sqlite3_rollback_hook(handle, rollbackCallback, null)
            rollbackGeneration <- generation
    member this.WatchTransaction(onReleased: Action<NativeTransactionWatch>) =
        use lifetime = this.AdmitUse()
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
                let retained = this.TryEnterUse(expected)
                try
                    if storage.IsNone || retained then
                        let detach =
                            lock closeGate (fun () ->
                                if openState && generation = expected && completionEpoch = expectedEpoch && transactionEpoch = expectedEpoch then
                                    transactionWatchCount <- transactionWatchCount - 1
                                    if transactionWatchCount = 0 then
                                        rollbackGeneration <- -1L
                                        true
                                    else false
                                else false)
                        // Never hold the close guard while entering SQLite's mutex:
                        // an in-flight callback may itself request retirement.
                        if detach then raw.sqlite3_rollback_hook(handle, null, null)
                finally if retained then this.ReleaseUse(true)
            finally onReleased.Invoke(watch)))
        watch
    member _.ObserveStatementCompletion() =
        if transactionWatchCount > 0 then
            let retained = this.TryEnterUse(generation)
            try
                if (storage.IsNone || retained) && openState then
                    // Actual completion covers read-only commits too. Failed commits stay active.
                    if completionEpoch = transactionEpoch && raw.sqlite3_get_autocommit(handle) <> 0 then
                        transactionEpoch <- transactionEpoch + 1L
                    if completionEpoch <> transactionEpoch then
                        transactionWatchCount <- 0
                        raw.sqlite3_rollback_hook(handle, null, null)
                        rollbackGeneration <- -1L
            finally if retained then this.ReleaseUse(true)
    member _.UsesNativeUtf16 = nativeUtf16
    member _.Password = setting "Password" ""
    member _.RecursiveTriggers = Boolean.Parse(setting "Recursive Triggers" "false")
    member _.SharedCache = cache.Equals("Shared", StringComparison.OrdinalIgnoreCase)
    member _.Generation = generation
    member this.EnsureGeneration(expected: int64) =
        if storage.IsNone then this.EnsureOpen()
        elif not openState || disposed then raise(ObjectDisposedException("NativeSqliteConnection"))
        if generation <> expected then raise (InvalidOperationException("The owning connection was closed and reopened."))
    member _.ConnectionString = connectionString
    member _.DataSource = source
    member _.State = if openState && not retired then SqliteConnectionState.Open else SqliteConnectionState.Closed
    member _.IsOpen = openState && not disposed
    member _.Observer with get() = observer and set value = observer <- value
    member _.DefaultTimeout with get() = timeout and set value = if value < 0 then invalidArg "value" "Timeout must be nonnegative." else timeout <- value
    member _.EnsureOpen() =
        if disposed || retired then raise (ObjectDisposedException("NativeSqliteConnection"))
        if not openState then raise (InvalidOperationException("The connection is not open."))
    member _.Handle =
        if not openState || disposed then raise(InvalidOperationException("The connection is not open."))
        handle
    member this.Check(result: int) =
        if result <> raw.SQLITE_OK then raise(this.Error(result))
        this.CheckStorageOperation()
    member this.Open() =
        if disposed || disposeRequested then raise(ObjectDisposedException("NativeSqliteConnection"))
        if retired then
            match storage with
            | Some input when not input.IsRetired && isNull (box storageLease) ->
                retired <- false
                statementsRetired <- false
            | Some input when input.IsRetired -> raise(ObjectDisposedException("NativeSqliteConnection"))
            | _ -> invalidOp "The previous physical close has not completed."
        if not openState then
            initialization.Force()
            nativeUtf16 <- String.Equals(raw.GetNativeLibraryName(), "e_sqlite3", StringComparison.Ordinal)
            let configure () =
                this.Check(raw.sqlite3_extended_result_codes(handle, 1))
                this.Check(raw.sqlite3_busy_timeout(handle, 0))
                if this.IsManagedStorage then
                    this.Check(raw.sqlite3_enable_load_extension(handle, 0))
                    let mutable enabled = 0
                    this.Check(raw.sqlite3_db_config(handle, raw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0, &enabled))
                if settings.ContainsKey "Foreign Keys" then
                    let enabled = Boolean.Parse(string settings.["Foreign Keys"])
                    let mutable actual = 0
                    this.Check(raw.sqlite3_db_config(handle, raw.SQLITE_DBCONFIG_ENABLE_FKEY, (if enabled then 1 else 0), &actual))
            match storage with
            | Some input ->
                let lease = input.Acquire(timeout)
                storageLease <- lease
                nativeClosed <- false
                try nativeRoot <- GCHandle.Alloc(this)
                with _ ->
                    nativeClosed <- true
                    lease.Complete()
                    storageLease <- Unchecked.defaultof<_>
                    reraise()
                lock closeGate (fun () -> uses <- uses + 1; activeUses <- activeUses + 1)
                try
                    try
                        lease.SetCloseRequest(Action(fun () -> this.Dispose()))
                        if retired then raise(ObjectDisposedException("NativeSqliteConnection"))
                        let access = if lease.Profile = SqliteStorageProfile.ReadOnly then raw.SQLITE_OPEN_READONLY else raw.SQLITE_OPEN_READWRITE ||| raw.SQLITE_OPEN_CREATE
                        let flags = access ||| raw.SQLITE_OPEN_FULLMUTEX ||| raw.SQLITE_OPEN_URI ||| raw.SQLITE_OPEN_PRIVATECACHE
                        let result = raw.sqlite3_open_v2(lease.Filename, &handle, flags, lease.Name)
                        if result <> raw.SQLITE_OK then raise(this.Error(result))
                        openState <- true
                        generation <- generation + 1L
                        configure()
                        if retired then raise(ObjectDisposedException("NativeSqliteConnection"))
                    with _ -> this.Close(); reraise()
                finally this.ReleaseUse(true)
            | None ->
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
                    if mode.Equals("Memory", StringComparison.OrdinalIgnoreCase) && not(source.StartsWith("file:", StringComparison.OrdinalIgnoreCase)) then "file:" + source
                    else source
                let result = raw.sqlite3_open_v2(filename, &handle, flags, (null: string))
                if result <> raw.SQLITE_OK then
                    let failure = NativeSqliteException.FromResult(handle, result)
                    if not(isNull handle) then handle.Dispose(); handle <- null
                    raise failure
                openState <- true
                generation <- generation + 1L
                try configure()
                with _ ->
                    openState <- false
                    handle.Dispose()
                    handle <- null
                    reraise()
    member this.Close() =
        if storage.IsSome then
            lock closeGate (fun () -> retired <- true)
            this.TryCloseStorage()
        else
            callbackFailure <- null
            this.ReaderActive <- false
            if openState then
                openState <- false
                handle.Dispose()
                handle <- null
    member this.Dispose() =
        if storage.IsSome then
            disposeRequested <- true
            this.Close()
            if isNull (box storageLease) then disposed <- true
        elif not disposed then this.Close(); disposed <- true

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
    [<Literal>]
    let private waitMilliseconds = 150
    let wait () = Thread.Sleep(waitMilliseconds)
    let waitAsync () = Threading.Tasks.Task.Delay(waitMilliseconds)
