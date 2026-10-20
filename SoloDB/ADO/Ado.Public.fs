namespace SoloDatabase.Data.Sqlite

open System
open System.Data
open System.Data.Common
open SoloDatabase
open SoloDatabase.RawSqliteFunctions

// The concrete connection, command and transaction types mutually reference each other.
// F# requires these declarations in one recursive type group.
/// A native transaction owner or a borrowed association view of an existing scope.
[<Sealed; AllowNullLiteral>]
type SqliteTransaction internal (connection: DbConnection, session: AdoSession, transaction: NativeSqliteTransaction voption, owned: bool) as this =
    inherit DbTransaction()
    let version = session.Version
    let watch = session.WatchTransaction()
    let mutable disposed = false
    let mutable finished = false
    let check () =
        if disposed || finished then raise (InvalidOperationException("The transaction has completed."))
        session.Check(version)
        if session.Native.DatabaseOwner.TransactionEpoch <> watch.Epoch then
            raise (InvalidOperationException("The native transaction has completed."))
        match transaction with
        | ValueSome transaction when not transaction.IsActive || not (Object.ReferenceEquals(transaction, session.Native.CurrentTransaction)) ->
            raise (InvalidOperationException("The native transaction has completed."))
        | ValueNone when SQLitePCL.raw.sqlite3_get_autocommit(session.Native.Handle) <> 0 ->
            raise (InvalidOperationException("The native transaction has completed."))
        | _ -> ()
    let owner () =
        check()
        if not owned then raise (InvalidOperationException("This transaction belongs to its existing scope owner."))
        match transaction with ValueSome transaction -> transaction | _ -> raise (InvalidOperationException("There is no owned native transaction."))
    do session.Register(this)
    member internal _.CheckFor(native: NativeSqliteConnection) =
        check()
        if not (Object.ReferenceEquals(native, session.Native)) then
            raise (InvalidOperationException("The transaction belongs to a different connection."))
    member internal _.IsActive =
        not disposed && not finished && session.IsCurrent(version)
        && session.Native.DatabaseOwner.TransactionEpoch = watch.Epoch
    member this.Connection = this.DbConnection :?> SqliteConnection
    member _.IsOwned = owned
#if NET5_0_OR_GREATER
    override _.SupportsSavepoints = owned
#else
    member _.SupportsSavepoints = owned
#endif
    override _.IsolationLevel =
        match transaction with ValueSome transaction -> transaction.IsolationLevel | _ -> IsolationLevel.Serializable
    override _.DbConnection = if disposed || finished then null else connection
    override _.Commit() = check(); session.Guard(fun () ->
        let native = owner()
        native.Commit()
        finished <- true
        watch.Dispose()
        session.Unregister(this))
    override _.Rollback() = check(); session.Guard(fun () ->
        let native = owner()
        native.Rollback()
        finished <- true
        watch.Dispose()
        session.Unregister(this))
#if NET5_0_OR_GREATER
    override _.Save(savepointName: string) = check(); session.Guard(fun () -> (owner()).Save(savepointName))
    override _.Release(savepointName: string) = check(); session.Guard(fun () -> (owner()).Release(savepointName))
    override _.Rollback(savepointName: string) = check(); session.Guard(fun () -> (owner()).Rollback(savepointName))
#else
    member _.Save(savepointName: string) = check(); session.Guard(fun () -> (owner()).Save(savepointName))
    member _.Release(savepointName: string) = check(); session.Guard(fun () -> (owner()).Release(savepointName))
    member _.Rollback(savepointName: string) = check(); session.Guard(fun () -> (owner()).Rollback(savepointName))
#endif
    override _.Dispose(disposing) =
        if disposing && not disposed then
            disposed <- true
            try
                if owned && not finished then
                    match transaction with ValueSome transaction -> transaction.Dispose() | _ -> ()
            finally
                watch.Dispose()
                session.Unregister(this)
        base.Dispose(disposing)

/// Compatibility command state feeds the same native lease and ordinal binder as direct SQL.
and [<Sealed; AllowNullLiteral>] SqliteCommand internal (initialConnection: DbConnection, initialSession: AdoSession) =
    inherit DbCommand()
    let mutable connection = initialConnection
    let mutable session = initialSession
    let parameters = SqliteParameterCollection()
    let mutable text = ""
    let mutable blobSyntax: NativeBlobSyntax = null
    let mutable timeout = ValueNone
    let mutable transaction: SqliteTransaction = null
    let mutable reader: SqliteDataReader = null
    let mutable disposed = false
    let mutable designVisible = true
    let mutable updatedRowSource = UpdateRowSource.None
    let commandTimeout () =
        match timeout with
        | ValueSome value -> value
        | _ when isNull (box session) -> NativeConnectionDefaults.commandTimeout
        | _ -> session.DefaultTimeout
    let validate () =
        if disposed then raise (ObjectDisposedException("SqliteCommand"))
        if isNull connection then raise (InvalidOperationException("A command requires a connection."))
        session.Check()
        if not (isNull reader) && not reader.IsClosed then raise (InvalidOperationException("The command already has an active reader."))
        if not (isNull transaction) then transaction.CheckFor(session.Native)
        elif session.Native.HasOwnedTransaction then raise (InvalidOperationException("The command requires its active transaction association."))
    let acquire () =
        validate()
        let owner = session
        if isNull blobSyntax then blobSyntax <- NativeBlobSyntax()
        let result = NativeBlobResult(blobSyntax)
        let rows = owner.Native.AcquireTimedRows(text, parameters.Plan, parameters, commandTimeout(), false, result)
        struct (owner, rows, result)
    new() = new SqliteCommand((null: DbConnection), Unchecked.defaultof<AdoSession>)
    new(commandText: string, connection: DbConnection) as this =
        new SqliteCommand()
        then this.DbConnection <- connection; this.CommandText <- commandText
    member this.Connection
        with get() = connection :?> SqliteConnection
        and set (value: SqliteConnection) = this.DbConnection <- value
    member this.Transaction
        with get() = transaction
        and set (value: SqliteTransaction) = this.DbTransaction <- value
    new(commandText: string) as this = new SqliteCommand() then this.CommandText <- commandText
    new(commandText: string, connection: SqliteConnection) = new SqliteCommand(commandText, connection :> DbConnection)
    new(commandText: string, connection: SqliteConnection, transaction: SqliteTransaction) as this =
        new SqliteCommand(commandText, connection :> DbConnection)
        then this.Transaction <- transaction
    member _.Parameters = parameters
    member _.CreateParameter() = SqliteParameter()
    member this.ExecuteReader() = this.ExecuteReader(CommandBehavior.Default)
    member this.ExecuteReader(behavior: CommandBehavior) = this.ExecuteDbDataReader(behavior) :?> SqliteDataReader
    member this.ExecuteReaderAsync(behavior: CommandBehavior, cancellationToken: System.Threading.CancellationToken) =
        if cancellationToken.IsCancellationRequested then System.Threading.Tasks.Task.FromCanceled<SqliteDataReader>(cancellationToken)
        else
            try System.Threading.Tasks.Task.FromResult(this.ExecuteReader(behavior))
            with error -> System.Threading.Tasks.Task.FromException<SqliteDataReader>(error)
    member this.ExecuteReaderAsync(behavior: CommandBehavior) = this.ExecuteReaderAsync(behavior, System.Threading.CancellationToken.None)
    member this.ExecuteReaderAsync(cancellationToken: System.Threading.CancellationToken) = this.ExecuteReaderAsync(CommandBehavior.Default, cancellationToken)
    member this.ExecuteReaderAsync() = this.ExecuteReaderAsync(CommandBehavior.Default, System.Threading.CancellationToken.None)
    override _.DbParameterCollection = parameters :> DbParameterCollection
    override _.CommandText
        with get() = text
        and set value =
            let value = if isNull value then "" else value
            if text <> value then text <- value; blobSyntax <- null
    override _.CommandTimeout
        with get() = commandTimeout()
        and set value = if value < 0 then raise (ArgumentOutOfRangeException("value")) else timeout <- ValueSome value
    override _.CommandType
        with get() = CommandType.Text
        and set value = if value <> CommandType.Text then raise (ArgumentException("SQLite supports text commands only.", "value"))
    override _.DesignTimeVisible with get() = designVisible and set value = designVisible <- value
    override _.UpdatedRowSource with get() = updatedRowSource and set value = updatedRowSource <- value
    override _.DbConnection
        with get() = connection
        and set value =
            if not (isNull reader) && not reader.IsClosed then raise (InvalidOperationException("The command has an active reader."))
            match box value with
            | null -> connection <- null; session <- Unchecked.defaultof<_>
            | :? IAdoSessionHost as host -> connection <- value; session <- host.Session
            | _ -> invalidArg "value" "An SqliteConnection is required."
    override _.DbTransaction
        with get() = transaction :> DbTransaction
        and set value =
            match value with
            | null -> transaction <- null
            | :? SqliteTransaction as value -> transaction <- value
            | _ -> invalidArg "value" "An SqliteTransaction is required."
    override _.CreateDbParameter() = SqliteParameter() :> DbParameter
    override _.Prepare() = AdoSessionAccess.guard session (fun () ->
        validate()
        session.Native.PrepareSql(text, commandTimeout()))
    /// Cancellation is not supported; this call does not interrupt execution.
    override _.Cancel() = ()
    override _.ExecuteNonQuery() = AdoSessionAccess.guard session (fun () ->
        validate()
        let mutable script = session.Native.AcquireTimedScript(text, parameters.Plan, parameters, commandTimeout())
        try
            let result = script.Consume()
            script.Dispose()
            result
        with error -> script.Abort(error); reraise())
    override _.ExecuteScalar() = AdoSessionAccess.guard session (fun () ->
        validate()
        let mutable script = session.Native.AcquireTimedScript(text, parameters.Plan, parameters, commandTimeout())
        try
            let result = if script.Read() && script.FieldCount > 0 then script.Row.GetValue(0) else null
            script.Dispose()
            result
        with error -> script.Abort(error); reraise())
    override _.ExecuteDbDataReader(behavior) = AdoSessionAccess.guard session (fun () ->
        let allowed = CommandBehavior.SingleResult ||| CommandBehavior.SchemaOnly ||| CommandBehavior.KeyInfo ||| CommandBehavior.SingleRow ||| CommandBehavior.SequentialAccess ||| CommandBehavior.CloseConnection
        if behavior &&& ~~~allowed <> CommandBehavior.Default then raise (ArgumentOutOfRangeException("behavior"))
        let struct (owner, rows, admission) = acquire ()
        try
            SQLiteToolsHandlerFaultState.raiseIfHandlerFaultRecorded owner.Native
            let mutable result: SqliteDataReader = null
            result <- new SqliteDataReader(owner, rows, admission, behavior.HasFlag(CommandBehavior.CloseConnection), Action(fun () -> if Object.ReferenceEquals(reader, result) then reader <- null))
            reader <- result
            result :> DbDataReader
        with error -> admission.Dispose(); rows.Abort(error); reraise())
    override _.Dispose(disposing) =
        if disposing && not disposed then
            disposed <- true
            if not (isNull reader) then reader.Dispose()
        base.Dispose(disposing)

/// Standard ADO interfaces share SoloDB's native execution core.
and [<AllowNullLiteral>] SqliteConnection private (session: AdoSession) as this =
    inherit DbConnection()
    let mutable borrowed: SqliteTransaction = null
    let mutable borrowedEpoch = -1L
    internal new(connection: NativeSqliteConnection, leaveOpen: bool, guard: unit -> unit) =
        new SqliteConnection(AdoSession(connection, leaveOpen, false, guard))
    internal new(connection: NativeSqliteConnection, leaveOpen: bool) =
        if isNull connection then nullArg "connection"
        new SqliteConnection(AdoSession(connection, leaveOpen, false, ignore))
    internal new(connection: NativeSqliteConnection) = new SqliteConnection(connection, false)
    new(connectionString: string) = new SqliteConnection(AdoSessionFactory.createStandalone connectionString)
    new() = new SqliteConnection("")
    member internal _.NativeConnection = session.Native
    member internal _.Session = session
    member _.DefaultTimeout with get() = session.CheckSource(); session.DefaultTimeout and set value = session.GuardPresentation(fun () -> session.DefaultTimeout <- value)
    static member ClearAllPools() = AdoConnectionPools.clearAll()
    static member ClearPool(connection: SqliteConnection) =
        if isNull connection then nullArg "connection"
        connection.Session.CheckSource()
        AdoConnectionPools.clear connection.Session.ConnectionString
    member _.Handle = if session.State = ConnectionState.Open then session.Native.Handle else null
    member _.CreateFunction<'R>(name: string, ``function``: Func<'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () -> session.Native.CreateFunction(name, ``function``, isDeterministic))
    member _.CreateFunction<'A, 'R>(name: string, ``function``: Func<'A, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () -> session.Native.CreateFunction(name, ``function``, isDeterministic))
    member _.CreateFunction<'A, 'B, 'R>(name: string, ``function``: Func<'A, 'B, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () -> session.Native.CreateFunction(name, ``function``, isDeterministic))
    member _.CreateFunction<'A, 'B, 'C, 'R>(name: string, ``function``: Func<'A, 'B, 'C, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () -> session.Native.CreateFunction(name, ``function``, isDeterministic))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3])))
            session.Native.RegisterScalar(name, 4, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4])))
            session.Native.RegisterScalar(name, 5, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5])))
            session.Native.RegisterScalar(name, 6, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6])))
            session.Native.RegisterScalar(name, 7, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7])))
            session.Native.RegisterScalar(name, 8, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8])))
            session.Native.RegisterScalar(name, 9, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9])))
            session.Native.RegisterScalar(name, 10, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10])))
            session.Native.RegisterScalar(name, 11, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11])))
            session.Native.RegisterScalar(name, 12, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11], NativeScalarFunctions.read<'T13> name 12 args.[12])))
            session.Native.RegisterScalar(name, 13, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11], NativeScalarFunctions.read<'T13> name 12 args.[12], NativeScalarFunctions.read<'T14> name 13 args.[13])))
            session.Native.RegisterScalar(name, 14, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11], NativeScalarFunctions.read<'T13> name 12 args.[12], NativeScalarFunctions.read<'T14> name 13 args.[13], NativeScalarFunctions.read<'T15> name 14 args.[14])))
            session.Native.RegisterScalar(name, 15, callback, flags))
    member _.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'T16, 'R>(name: string, ``function``: Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'T16, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull ``function`` then null else RawScalarFunc(fun context args -> NativeScalarFunctions.write &context (``function``.Invoke(NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11], NativeScalarFunctions.read<'T13> name 12 args.[12], NativeScalarFunctions.read<'T14> name 13 args.[13], NativeScalarFunctions.read<'T15> name 14 args.[14], NativeScalarFunctions.read<'T16> name 15 args.[15])))
            session.Native.RegisterScalar(name, 16, callback, flags))
    member _.CreateFunction<'R>(name: string, ``function``: Func<obj[], 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        session.Guard(fun () ->
            let flags = if isDeterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback =
                if isNull ``function`` then null else
                RawScalarFunc(fun context args ->
                    let values = Array.zeroCreate<obj> args.Length
                    for i = 0 to args.Length - 1 do values.[i] <- NativeScalarFunctions.read<obj> name i args.[i]
                    NativeScalarFunctions.write &context (``function``.Invoke(values)))
            session.Native.RegisterScalar(name, -1, callback, flags))
    member this.CreateFunction<'State, 'R>(name: string, state: 'State, ``function``: Func<'State, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'R>(name, (if isNull ``function`` then null else Func<'R>(fun () -> ``function``.Invoke(state))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'R>(fun value0 -> ``function``.Invoke(state, value0))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'R>(fun value0 value1 -> ``function``.Invoke(state, value0, value1))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'R>(fun value0 value1 value2 -> ``function``.Invoke(state, value0, value1, value2))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'R>(fun value0 value1 value2 value3 -> ``function``.Invoke(state, value0, value1, value2, value3))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'R>(fun value0 value1 value2 value3 value4 -> ``function``.Invoke(state, value0, value1, value2, value3, value4))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'R>(fun value0 value1 value2 value3 value4 value5 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'R>(fun value0 value1 value2 value3 value4 value5 value6 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'R>(fun value0 value1 value2 value3 value4 value5 value6 value7 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6, value7))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'R>(fun value0 value1 value2 value3 value4 value5 value6 value7 value8 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6, value7, value8))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'R>(fun value0 value1 value2 value3 value4 value5 value6 value7 value8 value9 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6, value7, value8, value9))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'R>(fun value0 value1 value2 value3 value4 value5 value6 value7 value8 value9 value10 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'R>(fun value0 value1 value2 value3 value4 value5 value6 value7 value8 value9 value10 value11 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'R>(fun value0 value1 value2 value3 value4 value5 value6 value7 value8 value9 value10 value11 value12 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11, value12))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'R>(fun value0 value1 value2 value3 value4 value5 value6 value7 value8 value9 value10 value11 value12 value13 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11, value12, value13))), isDeterministic)
    member this.CreateFunction<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'R>(name: string, state: 'State, ``function``: Func<'State, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'R>(name, (if isNull ``function`` then null else Func<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'R>(fun value0 value1 value2 value3 value4 value5 value6 value7 value8 value9 value10 value11 value12 value13 value14 -> ``function``.Invoke(state, value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11, value12, value13, value14))), isDeterministic)
    member this.CreateFunction<'State, 'R>(name: string, state: 'State, ``function``: Func<'State, obj[], 'R>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateFunction<'R>(name, (if isNull ``function`` then null else Func<obj[], 'R>(fun values -> ``function``.Invoke(state, values))), isDeterministic)
    member _.CreateCollation(name: string, comparison: Comparison<string>) = session.Guard(fun () -> session.Native.RegisterCollation(name, comparison))
    member this.CreateCollation<'State>(name: string, state: 'State, comparison: Func<'State, string, string, int>) =
        this.CreateCollation(name, (if isNull comparison then null else Comparison<string>(fun left right -> comparison.Invoke(state, left, right))))
    member private _.RegisterAggregate<'Accumulate, 'Result>(name: string, arity: int, seed: 'Accumulate, accumulate: NativeAggregateStep<'Accumulate>, result: Func<'Accumulate, 'Result>, deterministic: bool) =
        session.Guard(fun () ->
            let flags = if deterministic then SqliteFunctionFlags.Deterministic else SqliteFunctionFlags.None
            let callback = if isNull accumulate then Unchecked.defaultof<NativeAggregateFunction> else NativeAggregateFunction<'Accumulate, 'Result>(seed, accumulate, result) :> NativeAggregateFunction
            session.Native.RegisterAggregate(name, arity, callback, flags))
    member this.CreateAggregate<'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current))
        this.RegisterAggregate(name, 0, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'Accumulate>(name: string, func: Func<'Accumulate, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0]))
        this.RegisterAggregate(name, 1, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1]))
        this.RegisterAggregate(name, 2, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2]))
        this.RegisterAggregate(name, 3, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3]))
        this.RegisterAggregate(name, 4, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4]))
        this.RegisterAggregate(name, 5, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5]))
        this.RegisterAggregate(name, 6, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6]))
        this.RegisterAggregate(name, 7, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7]))
        this.RegisterAggregate(name, 8, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8]))
        this.RegisterAggregate(name, 9, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9]))
        this.RegisterAggregate(name, 10, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10]))
        this.RegisterAggregate(name, 11, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11]))
        this.RegisterAggregate(name, 12, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11], NativeScalarFunctions.read<'T13> name 12 args.[12]))
        this.RegisterAggregate(name, 13, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11], NativeScalarFunctions.read<'T13> name 12 args.[12], NativeScalarFunctions.read<'T14> name 13 args.[13]))
        this.RegisterAggregate(name, 14, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step = if isNull func then null else NativeAggregateStep<'Accumulate>(fun current args -> func.Invoke(current, NativeScalarFunctions.read<'T1> name 0 args.[0], NativeScalarFunctions.read<'T2> name 1 args.[1], NativeScalarFunctions.read<'T3> name 2 args.[2], NativeScalarFunctions.read<'T4> name 3 args.[3], NativeScalarFunctions.read<'T5> name 4 args.[4], NativeScalarFunctions.read<'T6> name 5 args.[5], NativeScalarFunctions.read<'T7> name 6 args.[6], NativeScalarFunctions.read<'T8> name 7 args.[7], NativeScalarFunctions.read<'T9> name 8 args.[8], NativeScalarFunctions.read<'T10> name 9 args.[9], NativeScalarFunctions.read<'T11> name 10 args.[10], NativeScalarFunctions.read<'T12> name 11 args.[11], NativeScalarFunctions.read<'T13> name 12 args.[12], NativeScalarFunctions.read<'T14> name 13 args.[13], NativeScalarFunctions.read<'T15> name 14 args.[14]))
        this.RegisterAggregate(name, 15, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'Accumulate>(name: string, func: Func<'Accumulate, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'T8, 'T9, 'T10, 'T11, 'T12, 'T13, 'T14, 'T15, 'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member this.CreateAggregate<'Accumulate, 'Result>(name: string, seed: 'Accumulate, func: Func<'Accumulate, obj[], 'Accumulate>, resultSelector: Func<'Accumulate, 'Result>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        let step =
            if isNull func then null else
            NativeAggregateStep<'Accumulate>(fun current args ->
                let values = Array.zeroCreate<obj> args.Length
                for i = 0 to args.Length - 1 do values.[i] <- NativeScalarFunctions.read<obj> name i args.[i]
                func.Invoke(current, values))
        this.RegisterAggregate(name, -1, seed, step, resultSelector, isDeterministic)
    member this.CreateAggregate<'Accumulate>(name: string, seed: 'Accumulate, func: Func<'Accumulate, obj[], 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'Accumulate, 'Accumulate>(name, seed, func, Func<'Accumulate, 'Accumulate>(id), isDeterministic)
    member this.CreateAggregate<'Accumulate>(name: string, func: Func<'Accumulate, obj[], 'Accumulate>, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] isDeterministic: bool) =
        this.CreateAggregate<'Accumulate>(name, Unchecked.defaultof<'Accumulate>, func, isDeterministic)
    member _.EnableExtensions([<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(true)>] enable: bool) =
        session.Guard(fun () -> session.Native.EnableExtensions(enable))
    member _.LoadExtension(file: string, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(null: string)>] proc: string) =
        session.Guard(fun () -> session.Native.LoadExtension(file, proc))
    member _.CurrentTransaction =
        session.Guard(fun () ->
            session.Check()
            let native = session.Native
            let epoch = native.DatabaseOwner.TransactionEpoch
            if SQLitePCL.raw.sqlite3_get_autocommit(native.Handle) <> 0 then
                if not (isNull borrowed) then borrowed.Dispose(); borrowed <- null
                null
            else
                if isNull borrowed || borrowedEpoch <> epoch || not borrowed.IsActive then
                    if not (isNull borrowed) then borrowed.Dispose()
                    let transaction = native.CurrentTransaction
                    let transaction = if isNull (box transaction) then ValueNone else ValueSome transaction
                    borrowed <- new SqliteTransaction(this, session, transaction, false)
                    borrowedEpoch <- epoch
                borrowed)
    member _.CreateCommand() = session.GuardPresentation(fun () ->
        let command = new SqliteCommand(this, session)
        if session.State = ConnectionState.Open && session.Native.HasOwnedTransaction then
            command.Transaction <- this.CurrentTransaction
        command)
    override _.CreateDbCommand() = this.CreateCommand() :> DbCommand
    override _.DbProviderFactory = SqliteFactory.Instance
    override _.ConnectionString
        with get() = session.CheckSource(); session.ConnectionString
        and set value = session.SetConnectionString(value)
    override _.Database = session.CheckSource(); "main"
    override _.DataSource = session.CheckSource(); session.DataSource
    override _.ServerVersion = session.GuardPresentation(fun () ->
        NativeDatabase.EnsureInitialized()
        SQLitePCL.raw.sqlite3_libversion().utf8_to_string())
    override _.State = session.State
    override _.ConnectionTimeout = session.CheckSource(); session.DefaultTimeout
    member private this.NotifyState(previous) =
        if previous <> session.State then this.OnStateChange(StateChangeEventArgs(previous, session.State))
    override _.Open() = session.GuardPresentation(fun () ->
        let previous = session.State
        session.Open()
        this.NotifyState(previous))
    override _.Close() = session.GuardPresentation(fun () ->
        let previous = session.State
        session.Close()
        this.NotifyState(previous))
    override _.GetSchema() = AdoMetadata.schema session DbMetaDataCollectionNames.MetaDataCollections Array.empty
    override _.GetSchema(collectionName) = AdoMetadata.schema session collectionName Array.empty
    override _.GetSchema(collectionName, restrictionValues) = AdoMetadata.schema session collectionName restrictionValues
    member _.BackupDatabase(destination: SqliteConnection, destinationName: string, sourceName: string) =
        if isNull destination then nullArg "destination"
        session.Check()
        destination.Session.CheckSource()
        session.Guard(fun () -> session.Native.BackupDatabase(destination.NativeConnection, destinationName, sourceName))
    member this.BackupDatabase(destination: SqliteConnection) = this.BackupDatabase(destination, "main", "main")
    override _.ChangeDatabase(_) = raise (NotSupportedException("SQLite commands select an attached database through its SQL schema name."))
    member _.BeginTransaction(isolationLevel: IsolationLevel, deferred: bool) = session.Guard(fun () ->
        session.Check()
        let transaction = session.Native.BeginTransaction(isolationLevel, deferred)
        new SqliteTransaction(this, session, ValueSome transaction, true))
    member _.BeginTransaction(isolationLevel: IsolationLevel) = this.BeginTransaction(isolationLevel, (isolationLevel = IsolationLevel.ReadUncommitted))
    member _.BeginTransaction(deferred: bool) = this.BeginTransaction(IsolationLevel.Serializable, deferred)
    member _.BeginTransaction() = this.BeginTransaction(IsolationLevel.Serializable, false)
    override _.BeginDbTransaction(isolation) = this.BeginTransaction(isolation) :> DbTransaction
    override _.Dispose(disposing) =
        if disposing then AdoErrors.convert session.Dispose
        base.Dispose(disposing)
    interface IAdoSessionHost with member _.Session = session

/// Standard factory entry points create public compatibility presentations.
and [<Sealed>] SqliteFactory private () =
    inherit DbProviderFactory()
    static let instance = SqliteFactory()
    static member Instance = instance
    override _.CreateConnection() = new SqliteConnection() :> DbConnection
    override _.CreateCommand() = new SqliteCommand() :> DbCommand
    override _.CreateParameter() = SqliteParameter() :> DbParameter
    override _.CreateConnectionStringBuilder() = SqliteConnectionStringBuilder() :> DbConnectionStringBuilder
