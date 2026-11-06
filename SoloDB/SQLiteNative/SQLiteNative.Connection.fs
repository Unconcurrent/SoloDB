#nowarn "9"

namespace SoloDatabase

open System
open System.Data
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading
open SQLitePCL
open SoloDatabase.RawSqliteFunctions

/// An explicitly owned native transaction. Disposing an unfinished transaction rolls it back.
[<Sealed>]
type NativeSqliteTransaction internal (database: NativeDatabase, isolationLevel: IsolationLevel, execute: string -> unit, completed: unit -> unit) =
    let generation = database.Generation
    let mutable finished = false
    let lifetime = database.AdmitUse()
    let watch =
        try database.WatchTransaction(Action<NativeTransactionWatch>(ignore))
        with _ ->
            if not(isNull lifetime) then lifetime.Dispose()
            reraise()
    let epoch = watch.Epoch
    let externallyCompleted () = database.TransactionEpoch <> epoch
    let isOpen () = database.IsOpen && database.Generation = generation
    let complete () =
        finished <- true
        try
            try watch.Dispose()
            finally completed()
        finally if not(isNull lifetime) then lifetime.Dispose()
    let ensureActive () =
        if finished || externallyCompleted() || not (isOpen()) then
            raise (InvalidOperationException("This transaction has completed; it is no longer usable."))
    let finish sql =
        ensureActive()
        try execute sql
        finally
            if isOpen() && raw.sqlite3_get_autocommit(database.Handle) <> 0 then complete()
    let savepoint command (name: string) =
        if isNull name then nullArg "savepointName"
        ensureActive()
        execute (command + " \"" + name.Replace("\"", "\"\"") + "\"")
    member internal _.IsActive = not finished && not (externallyCompleted()) && isOpen()
    member internal _.DatabaseOwner = database
    member _.IsolationLevel = isolationLevel
    member _.SupportsSavepoints = true
    member _.Save(savepointName: string) = savepoint "SAVEPOINT" savepointName
    member _.Release(savepointName: string) = savepoint "RELEASE SAVEPOINT" savepointName
    member _.Rollback(savepointName: string) = savepoint "ROLLBACK TO SAVEPOINT" savepointName
    member _.Commit() =
        ensureActive()
        if not (isNull database.Observer) then database.Observer.After()
        finish "COMMIT"
    member _.Rollback() =
        if externallyCompleted() && not finished then complete() else finish "ROLLBACK"
    member this.Dispose() =
        if not finished then
            try if isOpen() && not (externallyCompleted()) then this.Rollback()
            finally if not finished then complete()
    interface IDisposable with member this.Dispose() = this.Dispose()

/// A configured SQLite database connection with native commands and rows.
[<AllowNullLiteral>]
type NativeSqliteConnection internal (connectionString: string, storage: SqliteStorage option) =
    let database = NativeDatabase(connectionString, storage)
    let statements = NativeStatementCache(database)
    let mutable transaction: NativeSqliteTransaction = Unchecked.defaultof<_>
    let mutable functions = Dictionary<struct (string * int), struct (string * NativeRegisteredFunction)>()
    let mutable collations: Dictionary<string, Comparison<string>> = null
    let mutable extensions: HashSet<struct (string * string)> = null
    let mutable extensionsEnabled = false
    let mutable storageRetirement: Action = null
    let mutable storageReopen: Action = null
    let checkRegistration () =
        if not database.CanRegister then raise(ObjectDisposedException("NativeSqliteConnection"))
    let checkExtensions () =
        checkRegistration()
        if database.IsManagedStorage then
            raise(NotSupportedException("Managed storage cannot expose native handles or load extensions. Use borrowed Native storage for that capability."))
    let loadExtension file procedure =
        use lifetime = database.AdmitUse()
        checkExtensions()
        if not extensionsEnabled then
            let mutable enabled = 0
            database.Check(raw.sqlite3_db_config(database.Handle, raw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, &enabled))
        let mutable message = utf8z.FromString(null)
        let result = raw.sqlite3_load_extension(database.Handle, utf8z.FromString(file), utf8z.FromString(procedure), &message)
        use messagePointer = fixed message
        try
            if result <> raw.SQLITE_OK then
                raise (NativeSqliteException($"SQLite Error {result}: '{message.utf8_to_string()}'.", result, result))
        finally
            raw.sqlite3_free(Microsoft.FSharp.NativeInterop.NativePtr.toNativeInt messagePointer)
    let registerFunction (name: string) arity callback =
        checkRegistration()
        if isNull name then nullArg "name"
        let key = struct (name.ToUpperInvariant(), arity)
        match callback with
        | ValueNone ->
            if database.IsOpen then RawFunctionRegistration.remove database.Handle name arity
            functions.Remove(key) |> ignore
        | ValueSome callback ->
            if database.IsOpen then NativeCallbackRegistration.install database name arity callback
            functions.[key] <- struct (name, callback)
    let registerScalar name arity callback flags =
        registerFunction name arity (if isNull callback then ValueNone else ValueSome(Scalar(callback, flags)))
    new(connectionString: string) = new NativeSqliteConnection(connectionString, None)
    member internal _.RegisterScalar(name, arity, callback, flags) = registerScalar name arity callback flags
    member internal _.RegisterManagedScalar(name, arity, callback, flags) =
        registerFunction name arity (if isNull callback then ValueNone else ValueSome(ManagedScalar(callback, flags)))
    member internal _.RegisterAggregate(name, arity, callback: NativeAggregateFunction, flags) =
        registerFunction name arity (if isNull (box callback) then ValueNone else ValueSome(Aggregate(callback, flags)))
    member internal _.EnableExtensions(enable) =
        if enable then checkExtensions() else checkRegistration()
        if database.IsOpen then database.Check(raw.sqlite3_enable_load_extension(database.Handle, if enable then 1 else 0))
        extensionsEnabled <- enable
    member internal _.LoadExtension(file, procedure) =
        checkExtensions()
        if database.IsOpen then loadExtension file procedure
        if isNull extensions then extensions <- HashSet()
        extensions.Add(struct (file, procedure)) |> ignore
    member internal _.RegisterCollation(name, comparison: Comparison<string>) =
        checkRegistration()
        if String.IsNullOrEmpty name then nullArg "name"
        if database.IsOpen then NativeCallbackRegistration.collation database name comparison
        if isNull collations then collations <- Dictionary(StringComparer.OrdinalIgnoreCase)
        if isNull comparison then collations.Remove(name) |> ignore else collations.[name] <- comparison
    member internal _.Registrations =
        { Functions=(if functions.Count = 0 then null else functions); Collations=collations
          Extensions=extensions; ExtensionsEnabled=extensionsEnabled }
    member internal this.DetachRegistrations() =
        let saved = this.Registrations
        if functions.Count > 0 then functions <- Dictionary()
        collations <- null
        extensions <- null
        extensionsEnabled <- false
        // A callback can close its connection while a native borrow is still running.
        // Such a handle must close and expire that execution, never enter the idle pool.
        if statements.HasActiveBorrows then this.Close()
        else
            statements.Clear()
            if database.IsOpen then
                if not (isNull (box transaction)) then transaction.Dispose()
                if raw.sqlite3_get_autocommit(database.Handle) = 0 then this.ExecuteSql("ROLLBACK", null) |> ignore
                if not (isNull saved.Functions) then
                    for KeyValue(struct (_, arity), struct (name, _)) in saved.Functions do RawFunctionRegistration.remove database.Handle name arity
                if not (isNull saved.Collations) then
                    for KeyValue(name, _) in saved.Collations do NativeCallbackRegistration.collation database name null
                database.Check(raw.sqlite3_enable_load_extension(database.Handle, 0))
        saved
    member internal _.RestoreRegistrations(saved: NativeConnectionRegistrations) =
        checkRegistration()
        if saved.ExtensionsEnabled || (not(isNull saved.Extensions) && saved.Extensions.Count > 0) then checkExtensions()
        if isNull saved.Functions then functions.Clear() else functions <- saved.Functions
        collations <- saved.Collations
        extensions <- saved.Extensions
        extensionsEnabled <- saved.ExtensionsEnabled
        if database.IsOpen then
            for KeyValue(struct (_, arity), struct (name, callback)) in functions do NativeCallbackRegistration.install database name arity callback
            if not (isNull collations) then
                for KeyValue(name, comparison) in collations do NativeCallbackRegistration.collation database name comparison
            if extensionsEnabled then database.Check(raw.sqlite3_enable_load_extension(database.Handle, 1))
            if not (isNull extensions) then
                for struct (file, procedure) in extensions do loadExtension file procedure
    member internal _.SetStorageRetirement(action: Action) = storageRetirement <- action
    member internal _.SetStorageReopen(action: Action) = storageReopen <- action
    member internal _.DatabaseOwner = database
    member internal _.StatementCache = statements
    member internal _.CheckNoActiveReader() =
        if database.ReaderActive then raise (InvalidOperationException("A raw SQL reader is already active on this connection."))
    member internal _.OpenTransactionStatement(sql: string) =
        new NativeStatementScope(statements, NativeStatementSource.Text sql, 0, NativeExecutionBudget(database.DefaultTimeout), true, true)
    member internal _.OpenStatement(sql: string) =
        new NativeStatementScope(statements, NativeStatementSource.Text sql, 0, NativeExecutionBudget(database.DefaultTimeout), true)
    member internal _.OpenStatementSource(source: NativeStatementSource) =
        new NativeStatementScope(statements, source, 0, NativeExecutionBudget(database.DefaultTimeout), true)
    member internal _.OpenStatementAsync(sql: string) =
        NativeStatementScope.CreateAsync(statements, NativeStatementSource.Text sql, 0, NativeExecutionBudget(database.DefaultTimeout), true)
    member internal _.ExecuteScriptAsync(sql: string) = task {
        let! prepared = NativeSqlScript.CreateAsync(statements, NativeStatementSource.Text sql, null, database.DefaultTimeout, true)
        let mutable script = prepared
        try
            let! struct (updated, affected) = script.ConsumeAsync()
            script <- updated
            script.Dispose()
            return affected
        with error ->
            script.Abort(error)
            return (System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw(); Unchecked.defaultof<_>)
    }
    member internal _.AcquireScript(source: NativeStatementSource, parameters: obj) =
        let mutable script = NativeSqlScript(statements, source, parameters, database.DefaultTimeout, true)
        try script.NextResult() |> ignore; script
        with error -> script.Abort(error); reraise()
    member internal _.AcquireScript(sql: string, plan: SqlParameterPlan<'Input, 'State>, input: 'Input) =
        let mutable script = NativeSqlScript.Create(statements, sql, plan, input, database.DefaultTimeout, true)
        try script.NextResult() |> ignore; script
        with error -> script.Abort(error); reraise()
    member internal _.AcquireRows(sql: string, parameters: obj, exclusive: bool, result: NativeBlobResult) =
        let mutable script = NativeSqlScript(statements, NativeStatementSource.Text sql, parameters, database.DefaultTimeout, true)
        let rows = new SqlRows(script, exclusive)
        try rows.NextResult(result) |> ignore; rows
        with error -> rows.Abort(error); reraise()
    member internal _.AcquireTimedRows(sql: string, plan: SqlParameterPlan<'Input, 'State>, input: 'Input, timeout: int, exclusive: bool, result: NativeBlobResult) =
        if timeout < 0 then invalidArg "timeout" "Timeout must be nonnegative."
        let mutable script = NativeSqlScript.Create(statements, sql, plan, input, timeout, true)
        let rows = new SqlRows(script, exclusive)
        try rows.NextResult(result) |> ignore; rows
        with error -> rows.Abort(error); reraise()
    member internal _.AcquireTimedScript(sql: string, plan: SqlParameterPlan<'Input, 'State>, input: 'Input, timeout: int) =
        if timeout < 0 then invalidArg "timeout" "Timeout must be nonnegative."
        let mutable script = NativeSqlScript.Create(statements, sql, plan, input, timeout, true)
        try script.NextResult() |> ignore; script
        with error -> script.Abort(error); reraise()
    member internal _.PrepareSql(sql: string, timeout: int) =
        use scope = new NativeStatementScope(statements, NativeStatementSource.Text sql, 0, NativeExecutionBudget(timeout), true)
        ()
    member internal _.OpenColumnStream(rows: SqlRows, ordinal: int, result: NativeBlobResult) =
        rows.OpenBlobStream(ordinal, rows.Row, result)
    member internal _.CurrentTransaction =
        if not (isNull (box transaction)) && not transaction.IsActive then transaction.Dispose()
        transaction
    member internal this.ExecuteSql(sql: string, parameters: obj) =
        let mutable statement = this.OpenStatement(sql)
        try
            let entry = statement.Statement
            let arguments = entry.Capture(parameters)
            if entry.HasStatement then statement.Bind(arguments)
            while statement.Step() do ()
            let result = statement.RecordsAffected
            if statement.IsCurrent && entry.HasTail then
                NativeSqlScript.ConsumeTail(statements, statement, arguments)
            else
                statement.Dispose()
                result
        with error -> statement.Abort(error); reraise()
    member internal this.ExecuteSql(sql: string, plan: SqlParameterPlan<'Input, 'State>, input: 'Input) =
        let mutable statement = this.OpenStatement(sql)
        try
            let entry = statement.Statement
            let arguments = entry.Capture(plan, input)
            if entry.HasStatement then statement.Bind(arguments)
            while statement.Step() do ()
            let result = statement.RecordsAffected
            if statement.IsCurrent && entry.HasTail then
                NativeSqlScript.ConsumeTail(statements, statement, arguments)
            else
                statement.Dispose()
                result
        with error -> statement.Abort(error); reraise()
    member internal _.VariableLimit = raw.sqlite3_limit(database.Handle, raw.SQLITE_LIMIT_VARIABLE_NUMBER, -1)
    member _.Handle =
        if database.IsManagedStorage then
            raise(NotSupportedException("Managed storage does not expose native handles. Use borrowed Native storage for raw access."))
        database.EnsureOpen()
        database.Handle
    member internal _.InternalHandle = database.Handle
    member _.ConnectionString = database.ConnectionString
    member _.DataSource = database.DataSource
    member _.State = database.State
    member _.DefaultTimeout with get() = database.DefaultTimeout and set value = database.DefaultTimeout <- value
    member internal this.HasOwnedTransaction = not (isNull (box this.CurrentTransaction))
    member this.Open() =
        if not database.IsOpen || database.IsRetiring then
            database.Open()
            use lifetime = database.RetainUse()
            database.BindStorageClose(Action(fun () ->
                if isNull storageRetirement then this.DisposeCore() else storageRetirement.Invoke()))
            statements.Reopen()
            try
                if database.Password.Length > 0 then
                    let library = raw.GetNativeLibraryName()
                    if library = "e_sqlite3" || library = "winsqlite3" then
                        raise (InvalidOperationException($"The SQLite library '{library}' does not support encryption."))
                    let mutable quoting = this.OpenStatement("SELECT quote(@password)")
                    let password =
                        try
                            quoting.BindText(1, database.Password)
                            quoting.Step() |> ignore
                            let text = quoting.Row.GetString(0)
                            quoting.Dispose()
                            text
                        with error -> quoting.Abort(error); reraise()
                    let mutable key = this.OpenStatement("PRAGMA key = " + password)
                    try key.Step() |> ignore; key.Dispose()
                    with error -> key.Abort(error); reraise()
                    let mutable decrypt = this.OpenStatement("SELECT COUNT(*) FROM sqlite_master")
                    try decrypt.Step() |> ignore; decrypt.Dispose()
                    with error -> decrypt.Abort(error); reraise()
                if database.RecursiveTriggers then
                    let mutable statement = this.OpenStatement("PRAGMA recursive_triggers = 1")
                    try
                        statement.Step() |> ignore
                        statement.Dispose()
                    with error -> statement.Abort(error); reraise()
                for KeyValue(struct (_, arity), struct (name, callback)) in functions do
                    NativeCallbackRegistration.install database name arity callback
                if not (isNull collations) then
                    for KeyValue(name, comparison) in collations do NativeCallbackRegistration.collation database name comparison
                if extensionsEnabled then database.Check(raw.sqlite3_enable_load_extension(database.Handle, 1))
                if not (isNull extensions) then
                    for struct (file, procedure) in extensions do loadExtension file procedure
                if database.Generation > 1L && not(isNull storageReopen) then storageReopen.Invoke()
                database.EnsureOpen()
            with _ -> database.Close(); reraise()
    member _.Close() =
        if database.IsStorage then
            // The physical owner drains idle VMs after the last admitted call or
            // resource completes; retirement must not wait for SQLite's mutex.
            database.Close()
        else
            let mutable failure: exn = null
            try if not (isNull (box transaction)) then transaction.Dispose()
            with error -> failure <- error
            try statements.Clear() with error -> if isNull failure then failure <- error
            try database.Close() with error -> if isNull failure then failure <- error
            database.ReaderActive <- false
            if not (isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()
    member this.BeginTransaction() = this.BeginTransaction(IsolationLevel.Serializable, false)
    member this.BeginTransaction(deferred: bool) = this.BeginTransaction(IsolationLevel.Serializable, deferred)
    member this.BeginTransaction(isolationLevel: IsolationLevel) = this.BeginTransaction(isolationLevel, (isolationLevel = IsolationLevel.ReadUncommitted))
    member this.BeginTransaction(isolationLevel: IsolationLevel, deferred: bool) =
        database.EnsureOpen()
        if this.HasOwnedTransaction then raise (InvalidOperationException("A transaction is already active."))
        let isolationLevel =
            match isolationLevel with
            | IsolationLevel.ReadUncommitted when database.SharedCache && deferred -> IsolationLevel.ReadUncommitted
            | IsolationLevel.ReadUncommitted | IsolationLevel.ReadCommitted | IsolationLevel.RepeatableRead | IsolationLevel.Unspecified | IsolationLevel.Serializable -> IsolationLevel.Serializable
            | _ -> invalidArg "isolationLevel" "Invalid SQLite transaction isolation level."
        let execute sql =
            let mutable statement = this.OpenTransactionStatement(sql)
            try
                statement.StepTransactionBoundary() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        if isolationLevel = IsolationLevel.ReadUncommitted then execute "PRAGMA read_uncommitted = 1"
        try execute (if deferred then "BEGIN" else "BEGIN IMMEDIATE")
        with _ ->
            if isolationLevel = IsolationLevel.ReadUncommitted then
                try execute "PRAGMA read_uncommitted = 0" with _ -> ()
            reraise()
        transaction <- new NativeSqliteTransaction(database, isolationLevel, execute, fun () ->
            try
                if isolationLevel = IsolationLevel.ReadUncommitted && database.IsOpen then execute "PRAGMA read_uncommitted = 0"
            finally transaction <- Unchecked.defaultof<_>)
        transaction
    member this.BackupDatabase(destination: NativeSqliteConnection) = this.BackupDatabase(destination, "main", "main")
    member _.BackupDatabase(destination: NativeSqliteConnection, destinationName: string, sourceName: string) =
        if isNull destination then nullArg "destination"
        if isNull destinationName then nullArg "destinationName"
        if isNull sourceName then nullArg "sourceName"
        use sourceUse = database.AdmitUse()
        database.EnsureOpen()
        let target = destination.DatabaseOwner
        if Object.ReferenceEquals(database, target) then invalidArg "destination" "The backup destination must be a different connection."
        let closeDestination = not target.IsOpen
        let mutable targetUse: NativeLifetimeUse = null
        let mutable sourceSink: IDisposable = null
        let mutable targetSink: IDisposable = null
        let faults = StorageFaultChannel()
        let sink = Action<int,exn>(fun code cause -> faults.Record(code, cause))
        let backupError (owner: NativeDatabase) result =
            match faults.Take() with
            | ValueSome(struct (code, cause)) ->
                let text = raw.sqlite3_errstr(code).utf8_to_string()
                NativeSqliteException($"SQLite Error {code &&& 255}: '{text}'.", code &&& 255, code, cause) :> exn
            | ValueNone -> owner.Error(result)
        let checkFault () =
            match faults.Take() with
            | ValueNone -> ()
            | ValueSome(struct (code, cause)) ->
                let text = raw.sqlite3_errstr(code).utf8_to_string()
                raise(NativeSqliteException($"SQLite Error {code &&& 255}: '{text}'.", code &&& 255, code, cause))
        let mutable backup: sqlite3_backup = null
        let mutable failure: exn = null
        let cleanup action =
            try action() with error ->
                if isNull failure then failure <- error
                else failure.Data.["SoloDB.CleanupException"] <- error
        try
            try
                if closeDestination then destination.Open()
                targetUse <- target.AdmitUse()
                database.BeginStorageOperation()
                target.BeginStorageOperation()
                match database.StorageFaults with Some channel -> sourceSink <- channel.UseSink(sink) | None -> ()
                match target.StorageFaults with Some channel -> targetSink <- channel.UseSink(sink) | None -> ()
                backup <- raw.sqlite3_backup_init(target.Handle, destinationName, database.Handle, sourceName)
                if isNull backup || backup.IsInvalid then raise(backupError target (raw.sqlite3_errcode(target.Handle)))
                checkFault()
                let mutable budget = NativeExecutionBudget(target.DefaultTimeout)
                let started = Diagnostics.Stopwatch.GetTimestamp()
                let mutable result = raw.sqlite3_backup_step(backup, -1)
                checkFault()
                while NativeExecution.isLocked result && budget.CanRetry(started) do
                    NativeExecution.wait()
                    result <- raw.sqlite3_backup_step(backup, -1)
                    checkFault()
                if result <> raw.SQLITE_DONE then raise(backupError target result)
            with error -> failure <- error
        finally
            if not(isNull backup) && not backup.IsInvalid then
                cleanup (fun () ->
                    let result = raw.sqlite3_backup_finish(backup)
                    if result <> raw.SQLITE_OK then raise(backupError target result)
                    checkFault())
            // Both real endpoint owners and the shared first-fault sink survive
            // through backup_finish, including rollback and its final callbacks.
            if not(isNull targetSink) then cleanup targetSink.Dispose
            if not(isNull sourceSink) then cleanup sourceSink.Dispose
            if closeDestination then cleanup destination.Close
            if not(isNull targetUse) then cleanup targetUse.Dispose
        if not(isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()

    member _.CreateFunction<'R>(name: string, functionValue: Func<'R>, [<Optional; DefaultParameterValue(false)>] isDeterministic: bool) =
        if isNull functionValue then registerScalar name 0 null SqliteFunctionFlags.None else
        let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
        registerScalar name 0 (RawScalarFunc(fun context _ -> NativeScalarFunctions.write &context (functionValue.Invoke()))) flags
    member _.CreateFunction<'A, 'R>(name: string, functionValue: Func<'A, 'R>, [<Optional; DefaultParameterValue(false)>] isDeterministic: bool) =
        if isNull functionValue then registerScalar name 1 null SqliteFunctionFlags.None else
        let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
        registerScalar name 1 (RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (functionValue.Invoke(NativeScalarFunctions.read<'A> name 0 args.[0])))) flags
    member _.CreateFunction<'A, 'B, 'R>(name: string, functionValue: Func<'A, 'B, 'R>, [<Optional; DefaultParameterValue(false)>] isDeterministic: bool) =
        if isNull functionValue then registerScalar name 2 null SqliteFunctionFlags.None else
        let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
        registerScalar name 2 (RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (functionValue.Invoke(NativeScalarFunctions.read<'A> name 0 args.[0], NativeScalarFunctions.read<'B> name 1 args.[1])))) flags
    member _.CreateFunction<'A, 'B, 'C, 'R>(name: string, functionValue: Func<'A, 'B, 'C, 'R>, [<Optional; DefaultParameterValue(false)>] isDeterministic: bool) =
        if isNull functionValue then registerScalar name 3 null SqliteFunctionFlags.None else
        let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
        registerScalar name 3 (RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (functionValue.Invoke(NativeScalarFunctions.read<'A> name 0 args.[0], NativeScalarFunctions.read<'B> name 1 args.[1], NativeScalarFunctions.read<'C> name 2 args.[2])))) flags
    member private this.DisposeCore() =
        try this.Close()
        finally
            functions.Clear()
            if not(isNull collations) then collations.Clear()
            if not(isNull extensions) then extensions.Clear()
            database.Dispose()
    abstract Dispose: bool -> unit
    default this.Dispose(disposing: bool) = if disposing then this.DisposeCore()
    member this.Dispose() = this.Dispose(true); GC.SuppressFinalize(this)
    interface IDisposable with member this.Dispose() = this.Dispose()

/// Internal consumers acquire the native owner without creating a public presentation.
type internal INativeConnectionSource =
    abstract AcquireNativeConnection: unit -> NativeSqliteConnection
