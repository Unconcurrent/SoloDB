namespace SoloDatabase

open System
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.ExceptionServices
open System.Threading.Tasks
open SQLitePCL

[<Struct>]
type private NativeStatementStepMode =
    | Ordinary
    | TransactionBoundary
    | Maintenance

/// Admission retains one observer checkpoint while preparation is pending.
[<Struct>]
type private NativeStatementAdmission =
    val Cache: NativeStatementCache
    val CheckReader: bool
    val Observer: INativeExecutionObserver
    val Generation: int64
    val mutable Checkpoint: int
    val mutable Admitted: bool
    new(cache: NativeStatementCache, checkReader, generation) =
        { Cache = cache; CheckReader = checkReader; Observer = cache.Database.Observer
          Generation = generation; Checkpoint = 0; Admitted = false }
    new(cache: NativeStatementCache, checkReader) = NativeStatementAdmission(cache, checkReader, cache.Database.Generation)
    member this.Check() =
        let database = this.Cache.Database
        database.EnsureGeneration(this.Generation)
        if this.CheckReader && database.ReaderActive then
            raise (InvalidOperationException("A raw SQL reader is already active on this connection."))
    member this.Enter() =
        this.Check()
        this.Checkpoint <- if isNull this.Observer then 0 else this.Observer.Checkpoint()
        this.Admitted <- true
    member this.Fail(error: exn, owned: NativePreparedStatement) =
        let database = this.Cache.Database
        if database.IsOpen && database.Generation = this.Generation && not (isNull this.Observer) then
            try this.Observer.Failed(error) with _ -> ()
            if this.Admitted then
                try this.Observer.Restore(this.Checkpoint) with _ -> ()
        if not (isNull (box owned)) then
            try this.Cache.Release(owned, owned.Borrow) with _ -> ()

/// Only the current native attempt is carried through a lock wait.
[<Struct>]
type private NativeStatementStepAttempt =
    val Mode: NativeStatementStepMode
    val Callbacks: int64
    val Started: int64
    val mutable Result: int
    new(mode, callbacks, started) =
        { Mode = mode; Callbacks = callbacks; Started = started; Result = 0 }

/// One checked borrow of a prepared statement. Consumers own binding and progression.
[<Struct>]
type internal NativeStatementScope =
    val private cache: NativeStatementCache
    val private statement: NativePreparedStatement
    val private observer: INativeExecutionObserver
    val private generation: int64
    val private borrow: int64
    val private checkpoint: int
    val mutable private budget: NativeExecutionBudget
    val mutable private result: int
    val mutable private affected: int
    val mutable private closed: bool
    private new(cache: NativeStatementCache, statement: NativePreparedStatement, admission: NativeStatementAdmission, budget: NativeExecutionBudget) =
        { cache = cache; statement = statement; observer = admission.Observer; generation = admission.Generation
          borrow = statement.Borrow; checkpoint = admission.Checkpoint; budget = budget; result = 0; affected = -1
          closed = false }
    new(cache: NativeStatementCache, source: NativeStatementSource, offset: int, budget: NativeExecutionBudget, checkReader: bool) =
        let mutable admission = NativeStatementAdmission(cache, checkReader)
        let mutable owned = Unchecked.defaultof<NativePreparedStatement>
        let mutable budget = budget
        let statement =
            try
                admission.Enter()
                owned <- cache.Acquire(source, offset, &budget)
                owned
            with error -> admission.Fail(error, owned); reraise()
        new NativeStatementScope(cache, statement, admission, budget)
    static member private WaitForPreparationAsync(cache: NativeStatementCache, initialAdmission: NativeStatementAdmission, pending: ValueTask<struct (NativePreparedStatement * NativeExecutionBudget)>) = task {
        let mutable admission = initialAdmission
        let mutable owned = Unchecked.defaultof<NativePreparedStatement>
        try
            let! struct (statement, budget) = pending
            owned <- statement
            admission.Check()
            return new NativeStatementScope(cache, statement, admission, budget)
        with error ->
            admission.Fail(error, owned)
            return (ExceptionDispatchInfo.Capture(error).Throw(); Unchecked.defaultof<_>)
    }
    static member CreateAsync(cache: NativeStatementCache, source: NativeStatementSource, offset: int, budget: NativeExecutionBudget, checkReader: bool, generation: int64) =
        let mutable admission = NativeStatementAdmission(cache, checkReader, generation)
        let mutable owned = Unchecked.defaultof<NativePreparedStatement>
        try
            admission.Enter()
            let pending = cache.AcquireAsync(source, offset, budget, admission.Generation)
            if pending.IsCompletedSuccessfully then
                let struct (statement, budget) = pending.Result
                owned <- statement
                admission.Check()
                ValueTask<NativeStatementScope>(new NativeStatementScope(cache, statement, admission, budget))
            else ValueTask<NativeStatementScope>(NativeStatementScope.WaitForPreparationAsync(cache, admission, pending))
        with error ->
            admission.Fail(error, owned)
            ValueTask<NativeStatementScope>(Task.FromException<NativeStatementScope>(error))
    static member CreateAsync(cache: NativeStatementCache, source: NativeStatementSource, offset: int, budget: NativeExecutionBudget, checkReader: bool) =
        NativeStatementScope.CreateAsync(cache, source, offset, budget, checkReader, cache.Database.Generation)
    member private this.Owns = not this.closed && this.statement.InUse && this.statement.Borrow = this.borrow
    member internal this.IsCurrent = this.Owns && this.cache.Database.IsOpen && this.cache.Database.Generation = this.generation
    member private this.Check() =
        if not this.Owns then raise (InvalidOperationException("The SQL statement scope has expired."))
        this.cache.Database.EnsureGeneration(this.generation)
    member private this.CheckStatement() =
        this.Check()
        if not this.statement.HasStatement then raise (InvalidOperationException("There is no prepared SQL statement."))
    member internal this.Statement = this.Check(); this.statement
    member internal this.Handle = this.CheckStatement(); this.statement.Handle
    member this.HasStatement = this.Check(); this.statement.HasStatement
    member this.Generation = this.generation
    member this.Budget = this.budget
    member internal this.SetBudget(value: NativeExecutionBudget) = this.budget <- value
    member this.RecordsAffected = this.affected
    member this.HasRow = this.result = raw.SQLITE_ROW && this.Owns
    member this.Row =
        this.Check()
        if this.result <> raw.SQLITE_ROW then raise (InvalidOperationException("There is no current SQL row."))
        SqlRow(this.statement, this.statement.RowVersion, this.generation)
    member this.BindInt64(index: int, value: int64) =
        let handle = this.Handle
        this.statement.HasBindings <- true
        this.cache.Database.Check(raw.sqlite3_bind_int64(handle, index, value))
    member this.BindDouble(index: int, value: double) =
        let handle = this.Handle
        this.statement.HasBindings <- true
        this.cache.Database.Check(NativeParameterBinding.real handle index value)
    member this.BindText(index: int, value: string) =
        let handle = this.Handle
        this.statement.HasBindings <- true
        this.cache.Database.Check(if isNull value then raw.sqlite3_bind_null(handle, index) else NativeParameterBinding.text handle index -1 value)
    member internal this.BindTextBuffer(index: int, value: ArraySegment<char>) =
        let handle = this.Handle
        this.statement.HasBindings <- true
        this.cache.Database.Check(NativeParameterBinding.textBuffer handle index -1 value)
    member this.BindBlob(index: int, value: byte[], length: int) =
        let handle = this.Handle
        this.statement.HasBindings <- true
        this.cache.Database.Check(if isNull value then raw.sqlite3_bind_null(handle, index) else NativeParameterBinding.blob handle index length value)
    member this.BindNull(index: int) =
        let handle = this.Handle
        this.statement.HasBindings <- true
        this.cache.Database.Check(raw.sqlite3_bind_null(handle, index))
    member this.BindValue(index: int, value: obj) =
        let handle = this.Handle
        this.statement.HasBindings <- true
        this.cache.Database.Check(NativeParameterBinding.bind handle index value)
    member this.BindValues(values: IReadOnlyDictionary<string, obj>) =
        this.CheckStatement()
        let names = this.statement.ParameterNames
        this.statement.HasBindings <- names.Length > 0
        let shortNames = this.statement.UnprefixedParameterNames
        for i = 0 to names.Length - 1 do
            let mutable value = null
            if not (values.TryGetValue(names.[i], &value)) && not (values.TryGetValue(shortNames.[i], &value)) then
                raise (InvalidOperationException("Missing SQL argument: " + names.[i]))
            this.cache.Database.Check(NativeParameterBinding.bind this.Handle (i + 1) value)
    member this.Bind(arguments: INativeArgumentBinding) =
        this.CheckStatement()
        this.statement.Bind(arguments)
    member private this.StepNative(attempt: byref<NativeStatementStepAttempt>) =
        attempt.Result <- raw.sqlite3_step(this.statement.Handle)
        this.Check()
        let database = this.cache.Database
        if database.CallbackEpoch <> attempt.Callbacks then database.RaiseCallbackFailure()
    member private this.StartStep(mode: NativeStatementStepMode) =
        this.Check()
        if not this.statement.HasStatement then ValueNone else
        this.statement.AdvanceRow()
        let mutable attempt = NativeStatementStepAttempt(mode, this.cache.Database.CallbackEpoch, Stopwatch.GetTimestamp())
        this.StepNative(&attempt)
        ValueSome attempt
    member private this.CanRetry(attempt: byref<NativeStatementStepAttempt>) =
        NativeExecution.isLocked attempt.Result && this.result = 0 &&
        (match attempt.Mode with
         | TransactionBoundary -> true
         | Maintenance -> raw.sqlite3_stmt_busy(this.statement.Handle) <> 0 || this.cache.Database.CallbackEpoch = attempt.Callbacks
         | Ordinary -> raw.sqlite3_stmt_busy(this.statement.Handle) <> 0 || (attempt.Result = 262 && this.cache.Database.CallbackEpoch = attempt.Callbacks)) &&
        this.budget.CanRetry(attempt.Started)
    [<System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)>]
    member private this.RaiseStepError(result: int) : unit =
        let error = NativeSqliteException.FromResult(this.cache.Database.Handle, result)
        error.Data.["Statement"] <- this.statement.Description
        raise error
    member private this.CompleteStep(attempt: byref<NativeStatementStepAttempt>) =
        let database = this.cache.Database
        this.budget.Record(attempt.Started)
        // A running result retains its compiled shape. Capture actual reprepare
        // at execution admission while every step still expires borrowed rows.
        if this.result <> raw.SQLITE_ROW then this.statement.ObserveResultShape()
        database.ObserveStatementCompletion()
        if attempt.Result <> raw.SQLITE_ROW && attempt.Result <> raw.SQLITE_DONE then
            this.RaiseStepError(attempt.Result)
        this.result <- attempt.Result
        if attempt.Result = raw.SQLITE_DONE && (this.statement.ColumnCount = 0 || raw.sqlite3_stmt_readonly(this.statement.Handle) = 0) then
            this.affected <- raw.sqlite3_changes(database.Handle)
        attempt.Result = raw.SQLITE_ROW
    member private this.Step(mode: NativeStatementStepMode) =
        match this.StartStep(mode) with
        | ValueNone -> false
        | ValueSome initial ->
            let mutable attempt = initial
            while this.CanRetry(&attempt) do
                NativeExecution.wait()
                this.Check()
                this.StepNative(&attempt)
            this.CompleteStep(&attempt)
    static member private WaitForStepAsync(initialScope: NativeStatementScope, initialAttempt: NativeStatementStepAttempt) = task {
        let mutable scope = initialScope
        let mutable attempt = initialAttempt
        try
            while scope.CanRetry(&attempt) do
                do! NativeExecution.waitAsync()
                scope.Check()
                scope.StepNative(&attempt)
            let row = scope.CompleteStep(&attempt)
            return struct (scope, row)
        with error ->
            scope.Abort(error)
            return (ExceptionDispatchInfo.Capture(error).Throw(); Unchecked.defaultof<_>)
    }
    member private this.StepAsync(mode: NativeStatementStepMode) =
        match this.StartStep(mode) with
        | ValueNone -> ValueTask<struct (NativeStatementScope * bool)>(struct (this, false))
        | ValueSome initial ->
            let mutable attempt = initial
            if this.CanRetry(&attempt) then
                ValueTask<struct (NativeStatementScope * bool)>(NativeStatementScope.WaitForStepAsync(this, attempt))
            else
                let row = this.CompleteStep(&attempt)
                ValueTask<struct (NativeStatementScope * bool)>(struct (this, row))
    member this.Step() = this.Step(Ordinary)
    member this.StepAsync() = this.StepAsync(Ordinary)
    // Only transaction-owner SQL without application expressions may restart a halted VM.
    // Shared-cache BEGIN can halt on LOCKED before acquiring its transaction.
    member this.StepTransactionBoundary() = this.Step(TransactionBoundary)
    member this.StepTransactionBoundaryAsync() = this.StepAsync(TransactionBoundary)
    // Fixed in-place maintenance may restart a halted VM before application callbacks.
    member this.StepMaintenance() = this.Step(Maintenance)
    member this.Reset() =
        this.Check()
        if this.statement.HasStatement then
            this.statement.AdvanceRow()
            let result = raw.sqlite3_reset(this.statement.Handle)
            this.cache.Database.ObserveStatementCompletion()
            this.cache.Database.Check(result)
            if this.affected < 0 && raw.sqlite3_stmt_readonly(this.statement.Handle) = 0 then
                this.affected <- raw.sqlite3_changes(this.cache.Database.Handle)
        this.result <- 0
    member private this.Finish(primary: exn, notifyAfter: bool, suppressPrimary: bool) =
        if this.Owns then
            this.closed <- true
            let database = this.cache.Database
            let current = database.IsOpen && database.Generation = this.generation
            let mutable failure = if suppressPrimary then null else primary
            if current && not (isNull primary) && not (isNull this.observer) then
                try this.observer.Failed(primary) with _ -> ()
            if this.statement.HasStatement then
                this.statement.AdvanceRow()
                try
                    let result = raw.sqlite3_reset(this.statement.Handle)
                    if current then
                        database.ObserveStatementCompletion()
                        if isNull primary && isNull failure then database.Check(result)
                with error -> if isNull failure then failure <- error
                try
                    let result = this.statement.ClearBindings()
                    if current && isNull failure then database.Check(result)
                with error -> if isNull failure then failure <- error
            if current && not (isNull this.observer) then
                if notifyAfter && isNull failure then
                    try this.observer.After() with error -> failure <- error
                try this.observer.Restore(this.checkpoint) with error -> if isNull failure then failure <- error
            try this.cache.Release(this.statement, this.borrow) with error -> if isNull failure then failure <- error
            if current && not (isNull failure) && (isNull primary || suppressPrimary) && not (isNull this.observer) then
                try this.observer.Failed(failure) with _ -> ()
            if not (isNull failure) then ExceptionDispatchInfo.Capture(failure).Throw()
    member this.Finish(primary: exn, notifyAfter: bool) = this.Finish(primary, notifyAfter, false)
    member this.FinishFailedTail(error: exn) = this.Finish(error, false, true)
    member this.Abort(error: exn) =
        try this.Finish(error, false) with _ -> ()
    member this.Dispose() = this.Finish(null, true)
    interface IDisposable with member this.Dispose() = this.Dispose()
