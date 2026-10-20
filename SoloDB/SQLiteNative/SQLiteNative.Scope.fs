namespace SoloDatabase

open System
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.ExceptionServices
open SQLitePCL

[<Struct>]
type private NativeStatementStepMode =
    | Ordinary
    | TransactionBoundary
    | Maintenance

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
    new(cache: NativeStatementCache, source: NativeStatementSource, offset: int, budget: NativeExecutionBudget, checkReader: bool) =
        let database = cache.Database
        let observer = database.Observer
        let generation = database.Generation
        let mutable checkpoint = 0
        let mutable admitted = false
        let mutable owned: NativePreparedStatement = Unchecked.defaultof<_>
        let mutable budget = budget
        let statement =
            try
                database.EnsureOpen()
                if checkReader && database.ReaderActive then raise (InvalidOperationException("A raw SQL reader is already active on this connection."))
                checkpoint <- if isNull observer then 0 else observer.Checkpoint()
                admitted <- true
                owned <- cache.Acquire(source, offset, &budget)
                owned
            with error ->
                if database.IsOpen && database.Generation = generation && not (isNull observer) then
                    try observer.Failed(error) with _ -> ()
                    if admitted then
                        try observer.Restore(checkpoint) with _ -> ()
                if not (isNull (box owned)) then
                    try cache.Release(owned, owned.Borrow) with _ -> ()
                reraise()
        { cache = cache; statement = statement; observer = observer; generation = generation
          borrow = statement.Borrow; checkpoint = checkpoint; budget = budget; result = 0; affected = -1
          closed = false }
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
    member private this.Step(mode: NativeStatementStepMode) =
        this.Check()
        if not this.statement.HasStatement then false
        else
            let database = this.cache.Database
            this.statement.AdvanceRow()
            let callbacksBefore = database.CallbackEpoch
            let started = Stopwatch.GetTimestamp()
            let mutable result = raw.sqlite3_step(this.statement.Handle)
            this.Check()
            if database.CallbackEpoch <> callbacksBefore then database.RaiseCallbackFailure()
            // Continuing a running VM does not replay its callbacks. A halted VM
            // needs explicit restart admission, and no mode may replay an exposed row.
            while NativeExecution.isLocked result && this.result = 0 &&
                  (match mode with
                   | TransactionBoundary -> true
                   | Maintenance -> raw.sqlite3_stmt_busy(this.statement.Handle) <> 0 || database.CallbackEpoch = callbacksBefore
                   | Ordinary -> raw.sqlite3_stmt_busy(this.statement.Handle) <> 0 || (result = 262 && database.CallbackEpoch = callbacksBefore)) &&
                  this.budget.CanRetry(started) do
                NativeExecution.wait()
                this.Check()
                result <- raw.sqlite3_step(this.statement.Handle)
                this.Check()
                if database.CallbackEpoch <> callbacksBefore then database.RaiseCallbackFailure()
            this.budget.Record(started)
            // A running result retains its compiled shape. Capture actual reprepare
            // at execution admission while every step still expires borrowed rows.
            if this.result <> raw.SQLITE_ROW then this.statement.ObserveResultShape()
            database.ObserveStatementCompletion()
            if result <> raw.SQLITE_ROW && result <> raw.SQLITE_DONE then
                let error = NativeSqliteException.FromResult(database.Handle, result)
                error.Data.["Statement"] <- this.statement.Description
                raise error
            this.result <- result
            if result = raw.SQLITE_DONE && (this.statement.ColumnCount = 0 || raw.sqlite3_stmt_readonly(this.statement.Handle) = 0) then
                this.affected <- raw.sqlite3_changes(database.Handle)
            result = raw.SQLITE_ROW
    member this.Step() = this.Step(Ordinary)
    // Only transaction-owner SQL without application expressions may restart a halted VM.
    // Shared-cache BEGIN can halt on LOCKED before acquiring its transaction.
    member this.StepTransactionBoundary() = this.Step(TransactionBoundary)
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
