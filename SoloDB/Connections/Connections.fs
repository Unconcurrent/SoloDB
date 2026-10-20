namespace SoloDatabase

open System.Data

/// <summary>
/// Contains types related to database connection management, pooling, and transactions.
/// </summary>
module Connections =
    open NativeSQLiteTools
    open ConnectionsTransactionHelpers
    open System
    open System.Collections.Concurrent
    open System.Threading
    open System.Threading.Tasks
    open Utils
    open System.IO

    let private ThrowOutsideEventContextUsage = ConnectionsTransactionHelpers.ThrowOutsideEventContextUsage
    let private eventHandlerScopeUnderflowMessage = ConnectionsTransactionHelpers.eventHandlerScopeUnderflowMessage
    let private takeHandlerFaultCommitException = ConnectionsTransactionHelpers.takeHandlerFaultCommitException
    let private rollbackBorrowedTransaction = ConnectionsTransactionHelpers.rollbackBorrowedTransaction
    let private commitOrRollbackBorrowedTransaction = ConnectionsTransactionHelpers.commitOrRollbackBorrowedTransaction
    let private cleanupBorrowedTransaction = ConnectionsTransactionHelpers.cleanupBorrowedTransaction
    let private cleanupDedicatedTransaction = ConnectionsTransactionHelpers.cleanupDedicatedTransaction
    let private withPooledTransactionCore = ConnectionsTransactionHelpers.withPooledTransactionCore
    let private shouldSuppressSavepointInHandler = ConnectionsTransactionHelpers.shouldSuppressSavepointInHandler
    let private beginImmediateWithRetry = ConnectionsTransactionHelpers.beginImmediateWithRetry
    let private resolveTxOutcome = ConnectionsTransactionHelpers.resolveTxOutcome

    /// <summary>
    /// Manages a pool of reusable <see cref="NativeCachingDbConnection"/> objects to reduce the overhead
    /// of opening and closing database connections. It also provides transaction management.
    /// </summary>
    /// <param name="connectionStr">The database connection string.</param>
    /// <param name="setup">An action to perform initial setup on a newly created connection.</param>
    /// <param name="config">The database configuration settings.</param>
    type ConnectionManager internal (connectionStr: string, setup: NativeSqliteConnection -> unit, config: Types.SoloDBConfiguration, releaseIndexOwner: unit -> unit, beginIndexTransaction: unit -> IDisposable, setupAsync: NativeSqliteConnection -> Task<unit>) =
        /// <summary>A collection of all connections ever created by this manager, for disposal purposes.</summary>
        let all = ConcurrentStack<NativeCachingDbConnection>()
        /// <summary>The pool of available, ready-to-use connections.</summary>
        let pool = ConcurrentStack<NativeCachingDbConnection>()
        /// <summary>A flag indicating whether the manager has been disposed.</summary>
        let mutable disposed = false
        let mutable disposeStarted = 0
        let mutable activeEventHandlerScopes = 0
        let mutable isCurrentThreadInHandlerDispatch = (fun () -> false)

        /// <summary>
        /// Checks if the manager has been disposed and throws an exception if it has.
        /// </summary>
        let checkDisposed () =
            if Volatile.Read(&disposed) then raise (ObjectDisposedException(nameof(ConnectionManager)))

        internal new(connectionStr: string, setup: NativeSqliteConnection -> unit, config: Types.SoloDBConfiguration, releaseIndexOwner: unit -> unit, beginIndexTransaction: unit -> IDisposable) =
            new ConnectionManager(connectionStr, setup, config, releaseIndexOwner, beginIndexTransaction,
                fun connection -> setup connection; Task.FromResult(()))
        internal new(connectionStr: string, setup: NativeSqliteConnection -> unit, config: Types.SoloDBConfiguration) =
            new ConnectionManager(connectionStr, setup, config, ignore, fun () -> null)

        /// <summary>
        /// Returns a used connection to the pool. Before returning, it verifies that any
        /// explicit transactions on the connection have been completed.
        /// </summary>
        /// <param name="pooledConn">The connection to return to the pool.</param>
        /// <exception cref="InvalidOperationException">Thrown if the connection is still inside a transaction.</exception>
        member internal this.TakeBack(pooledConn: NativeCachingDbConnection) =
            // A connection already known to be unfit is disposed without being probed. Probing it
            // would raise, and this runs from Dispose, so that exception would replace whatever
            // failure made the connection unusable in the first place.
            if pooledConn.Unusable then
                try pooledConn.DisposeReal() with _ -> ()
            else

                // Inspect SQLite's actual state without starting a transaction just
                // to prove that none exists. This also detects raw-SQL transactions.
                if pooledConn.State <> SqliteConnectionState.Open then
                    pooledConn.DisposeReal()
                elif pooledConn.HasManagedTransaction then
                    // Explicit transaction ownership rolls back on disposal. Raw BEGIN
                    // has no managed owner and must still be completed by its caller.
                    try pooledConn.DisposeReal() with _ -> ()
                elif SQLitePCL.raw.sqlite3_get_autocommit(pooledConn.Handle) = 0 then
                    raise (InvalidOperationException(
                        "Error: Connection returned to pool while a transaction is still active.\nReason: The transaction must be finished before returning the connection.\nFix: Commit or rollback the transaction before returning the connection."))
                else
                    pooledConn.ResetEventDispatchState()
                    pool.Push pooledConn

        /// <summary>
        /// Borrows a connection from the pool. If the pool is empty, a new connection is created.
        /// </summary>
        /// <returns>A ready-to-use <see cref="NativeCachingDbConnection"/>.</returns>
        member private this.TryBorrowNative() =
            checkDisposed()
            match pool.TryPop() with
            | true, connection ->
                if not connection.IsEventDispatchStateClean then
                    connection.ResetEventDispatchState()
                    raise (InvalidOperationException("Borrowed pooled connection had residual event dispatch state."))
                if connection.Inner.State <> SqliteConnectionState.Open then connection.Inner.Open()
                connection.RenewAcquisition()
                ValueSome connection
            | false, _ -> ValueNone
        member private this.NewConnection(pooled: bool) =
            checkDisposed()
            new NativeCachingDbConnection(connectionStr, (if pooled then this.TakeBack else ignore), config, this.EnterEventHandlerScope, this.ExitEventHandlerScope)
        member private _.CreationFailed(connection: NativeCachingDbConnection, error: exn) =
            let mutable cleanup = None
            try connection.DisposeReal() with failure -> cleanup <- Some failure
            match resolveTxOutcome (Some error) cleanup with
            | Some failure -> reraiseAnywhere failure
            | None -> raise (InvalidOperationException("Connection setup failed."))
        member private this.CreateConnection(pooled: bool) =
            let connection = this.NewConnection(pooled)
            try
                connection.Inner.Open()
                setup connection.Inner
                if pooled then all.Push connection
                connection
            with error -> this.CreationFailed(connection, error)
        member private this.CreateConnectionAsync(pooled: bool) = task {
            let connection = this.NewConnection(pooled)
            try
                connection.Inner.Open()
                do! setupAsync connection.Inner
                if pooled then
                    checkDisposed()
                    all.Push connection
                return connection
            with error -> return this.CreationFailed(connection, error)
        }
        member internal this.BorrowNative() =
            match this.TryBorrowNative() with
            | ValueSome connection -> connection
            | ValueNone -> this.CreateConnection(true)
        member private this.BorrowNativeAsync() =
            match this.TryBorrowNative() with
            | ValueSome connection -> ValueTask<NativeCachingDbConnection>(connection)
            | ValueNone -> ValueTask<NativeCachingDbConnection>(this.CreateConnectionAsync(true))
        /// <summary>
        /// Gets a collection of all connections (both in-pool and in-use) created by this manager.
        /// Used for final disposal.
        /// </summary>
        member internal this.All = all

        member this.Borrow() = new SQLiteTools.CachingDbConnection(this.BorrowNative(), false, ignore)

        /// <summary>
        /// Creates a caching connection dedicated to a single explicit transaction. It is never
        /// pooled and never tracked for reuse, so the object cannot outlive its transaction and
        /// cannot be handed to an unrelated borrower. Disposal is suppressed so a user
        /// <c>use</c> binding inside the callback cannot close a live transaction; the owning
        /// runner destroys it at scope exit.
        /// </summary>
        member internal this.CreateDedicatedTransactionConnection() = this.CreateConnection(false)
        member private this.CreateDedicatedTransactionConnectionAsync() =
            ValueTask<NativeCachingDbConnection>(this.CreateConnectionAsync(false))

        /// <summary>
        /// The core implementation for executing a synchronous function within a database transaction.
        /// It handles beginning the transaction and committing or rolling back based on the outcome.
        /// </summary>
        /// <param name="f">The function to execute within the transaction.</param>
        /// <returns>The result of the function <paramref name="f"/>.</returns>
        member private this.RunTransaction(acquire: unit -> NativeCachingDbConnection, release: NativeCachingDbConnection -> exn option ref -> unit, f: NativeCachingDbConnection -> 'T) =
            use indexLifetime = beginIndexTransaction ()
            let conn = acquire ()
            let primaryEx = ref None
            let cleanupEx = ref None
            let mutable result = Unchecked.defaultof<'T>
            try
                beginImmediateWithRetry conn
                conn.InsideTransaction <- true
                try
                    result <- f conn
                    commitOrRollbackBorrowedTransaction conn primaryEx cleanupEx
                with ex ->
                    rollbackBorrowedTransaction conn primaryEx cleanupEx ex
            finally
                release conn cleanupEx

            match resolveTxOutcome !primaryEx !cleanupEx with
            | Some ex -> raise ex
            | None -> result

        member private this.WithTransactionBorrowed(f: NativeCachingDbConnection -> 'T) =
            this.RunTransaction(this.BorrowNative, cleanupBorrowedTransaction, f)

        /// <summary>
        /// The core implementation for executing an asynchronous function within a database transaction.
        /// </summary>
        /// <param name="f">The asynchronous function to execute within the transaction.</param>
        /// <returns>A task that represents the asynchronous operation, containing the result of the function <paramref name="f"/>.</returns>
        member private this.RunTransactionAsync(acquire: unit -> ValueTask<NativeCachingDbConnection>, release: NativeCachingDbConnection -> exn option ref -> unit, f: NativeCachingDbConnection -> Task<'T>) = task {
            use indexLifetime = beginIndexTransaction ()
            let! conn = acquire ()
            let primaryEx = ref None
            let cleanupEx = ref None
            let mutable result = Unchecked.defaultof<'T>
            try
                do! ConnectionsTransactionHelpers.beginImmediateWithRetryAsync conn
                conn.InsideTransaction <- true
                try
                    let! ret = f conn
                    result <- ret
                    commitOrRollbackBorrowedTransaction conn primaryEx cleanupEx
                with ex ->
                    rollbackBorrowedTransaction conn primaryEx cleanupEx ex
            finally
                release conn cleanupEx

            match resolveTxOutcome !primaryEx !cleanupEx with
            | Some ex -> return reraiseAnywhere ex
            | None -> return result
        }

        member private this.WithTransactionBorrowedAsync(f: NativeCachingDbConnection -> Task<'T>) =
            this.RunTransactionAsync(this.BorrowNativeAsync, cleanupBorrowedTransaction, f)

        /// <summary>
        /// Executes a synchronous function within a database transaction using a pooled connection.
        /// </summary>
        /// <param name="f">The function to execute.</param>
        /// <returns>The result of the function.</returns>
        member internal this.WithTransaction(f: NativeCachingDbConnection -> 'T) =
            this.WithTransactionBorrowed f

        /// <summary>
        /// Runs an explicit user transaction on a dedicated caching connection that is destroyed
        /// when the scope ends, so no reference captured during the callback stays usable.
        /// </summary>
        member internal this.WithDedicatedTransaction(f: NativeCachingDbConnection -> 'T) =
            this.RunTransaction(this.CreateDedicatedTransactionConnection, cleanupDedicatedTransaction, f)

        /// <summary>
        /// Asynchronous counterpart of <see cref="WithDedicatedTransaction"/>.
        /// </summary>
        member internal this.WithDedicatedAsyncTransaction(f: NativeCachingDbConnection -> Task<'T>) =
            this.RunTransactionAsync(this.CreateDedicatedTransactionConnectionAsync, cleanupDedicatedTransaction, f)

        /// <summary>
        /// Executes an asynchronous function within a database transaction using a pooled connection.
        /// </summary>
        /// <param name="f">The asynchronous function to execute.</param>
        /// <returns>A task representing the asynchronous transactional operation.</returns>
        member internal this.WithAsyncTransaction(f: NativeCachingDbConnection -> Task<'T>) = task {
            return! this.WithTransactionBorrowedAsync f
        }

        member internal this.SetHandlerDispatchGuard(guardFn: unit -> bool) =
            isCurrentThreadInHandlerDispatch <- guardFn

        member internal this.EnterEventHandlerScope() =
            Interlocked.Increment(&activeEventHandlerScopes) |> ignore

        member internal this.ExitEventHandlerScope() =
            let rec decrementOrFail () =
                let snapshot = Volatile.Read(&activeEventHandlerScopes)
                if snapshot <= 0 then
                    if Interlocked.CompareExchange(&activeEventHandlerScopes, 0, snapshot) = snapshot then
                        raise (InvalidOperationException(eventHandlerScopeUnderflowMessage))
                    else
                        decrementOrFail ()
                else if Interlocked.CompareExchange(&activeEventHandlerScopes, snapshot - 1, snapshot) <> snapshot then
                    decrementOrFail ()
            decrementOrFail ()

        member internal this.HasActiveEventHandlerScope =
            Volatile.Read(&activeEventHandlerScopes) > 0

        member internal this.IsCurrentThreadInEventHandlerScope =
            isCurrentThreadInHandlerDispatch ()

        /// <summary>
        /// Disposes the connection manager, which closes and disposes all connections it has created.
        /// </summary>
        interface IDisposable with
            override this.Dispose() =
                if Interlocked.Exchange(&disposeStarted, 1) = 0 then
                    Volatile.Write(&disposed, true)
                    try
                        let mutable failure = None
                        for c in all do
                            try c.DisposeReal() with error ->
                                if failure.IsNone then failure <- Some error
                        all.Clear()
                        pool.Clear()
                        match failure with
                        | Some error -> reraiseAnywhere error
                        | None -> ()
                    finally
                        releaseIndexOwner ()

    
    /// <summary>
    /// A discriminated union that represents the different types of database connections available within the system.
    /// This allows for abstracting over whether a connection is from a pool or part of an explicit transaction.
    /// </summary>
    and internal NativeConnection =
        /// <summary>A connection sourced from a connection pool.</summary>
        | NativePooled of pool: ConnectionManager
        /// <summary>A dedicated, non-disposing connection for an ongoing transaction.</summary>
        | NativeTransactional of conn: NativeSqliteConnection
        /// <summary>A connection guarded by a validity check.</summary>
        | NativeGuarded of guard: (unit -> unit) * inner: NativeConnection

        /// <summary>
        /// Gets an active <see cref="NativeSqliteConnection"/> based on the connection type.
        /// If NativePooled, it borrows a connection. Otherwise, it returns the existing connection.
        /// </summary>
        /// <returns>An active <see cref="NativeSqliteConnection"/>.</returns>
        member internal this.IsEventHandlerScope =
            match this with
            | NativeTransactional connection -> SQLiteToolsHandlerFaultState.isInEventHandlerScope connection
            | NativeGuarded(guard, inner) -> guard(); inner.IsEventHandlerScope
            | NativePooled _ -> false

        member internal this.CheckAccess() =
            match this with
            | NativeGuarded(guard, inner) -> guard(); inner.CheckAccess()
            | _ -> ()

        member this.Get() : NativeSqliteConnection =
            match this with
            | NativePooled pool ->
                if pool.IsCurrentThreadInEventHandlerScope then
                    ThrowOutsideEventContextUsage()
                pool.BorrowNative()
            | NativeTransactional conn -> conn
            | NativeGuarded (guard, inner) ->
                guard()
                inner.Get()

        member this.GetPresentation() : SoloDatabase.Data.Sqlite.SqliteConnection =
            let native = this.Get()
            let leaveOpen =
                let rec borrowed source =
                    match source with
                    | NativePooled _ -> false
                    | NativeTransactional _ -> true
                    | NativeGuarded(_, inner) -> borrowed inner
                borrowed this
            ConnectionPresentation.create native leaveOpen this.CheckAccess

        /// <summary>
        /// Executes a synchronous function within a transaction. The behavior depends on the connection type.
        /// NativePooled: BEGIN IMMEDIATE (top-level). NativeTransactional: SAVEPOINT (nested).
        /// </summary>
        /// <param name="f">The function to execute within the transaction.</param>
        /// <returns>The result of the function.</returns>
        member this.WithTransaction(f: NativeSqliteConnection -> 'T) =
            match this with
            | NativePooled pool -> withPooledTransactionCore (fun () -> pool.IsCurrentThreadInEventHandlerScope) pool.WithTransaction f
            | NativeTransactional conn when shouldSuppressSavepointInHandler conn ->
                try
                    let result = f conn
                    SQLiteToolsHandlerFaultState.raiseIfHandlerFaultRecorded conn
                    result
                with error ->
                    SQLiteToolsHandlerFaultState.tryRecordHandlerFault conn error
                    reraise()
            | NativeTransactional conn -> ConnectionsTransactionHelpers.withSavepoint conn f
            | NativeGuarded (guard, inner) ->
                guard()
                inner.WithTransaction f

        /// <summary>
        /// Executes an asynchronous function within a transaction. The behavior depends on the connection type.
        /// NativePooled: BEGIN IMMEDIATE (top-level). NativeTransactional: SAVEPOINT (nested).
        /// </summary>
        /// <param name="f">The asynchronous function to execute.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        member this.WithAsyncTransaction(f: NativeSqliteConnection -> Task<'T>) =
            match this with
            | NativePooled pool -> withPooledTransactionCore (fun () -> pool.IsCurrentThreadInEventHandlerScope) pool.WithAsyncTransaction f
            | NativeTransactional conn when shouldSuppressSavepointInHandler conn -> task {
                try
                    let! result = f conn
                    SQLiteToolsHandlerFaultState.raiseIfHandlerFaultRecorded conn
                    return result
                with error ->
                    SQLiteToolsHandlerFaultState.tryRecordHandlerFault conn error
                    return reraiseAnywhere error
              }
            | NativeTransactional conn -> ConnectionsTransactionHelpers.withSavepointAsync conn f
            | NativeGuarded (guard, inner) ->
                guard()
                inner.WithAsyncTransaction f

    /// Public connection sources retain the documented provider-shaped callback surface.
    type Connection =
        | Pooled of pool: ConnectionManager
        | Transactional of conn: SoloDatabase.Data.Sqlite.SqliteConnection
        | Guarded of guard: (unit -> unit) * inner: Connection
        member private this.NativeSource =
            let rec source value =
                match value with
                | Pooled pool -> NativePooled pool
                | Transactional connection -> NativeGuarded(connection.Session.Check, NativeTransactional connection.NativeConnection)
                | Guarded(guard, inner) -> NativeGuarded(guard, source inner)
            source this
        member this.Get() = this.NativeSource.GetPresentation()
        member this.WithTransaction(f: SoloDatabase.Data.Sqlite.SqliteConnection -> 'T) =
            let source = this.NativeSource
            source.WithTransaction(fun native ->
                use presentation = ConnectionPresentation.create native true source.CheckAccess
                f presentation)
        member this.WithAsyncTransaction(f: SoloDatabase.Data.Sqlite.SqliteConnection -> Task<'T>) =
            let source = this.NativeSource
            source.WithAsyncTransaction(fun native -> task {
                use presentation = ConnectionPresentation.create native true source.CheckAccess
                return! f presentation
            })

    let internal EnterEventHandlerScope(connection: NativeSqliteConnection) =
        match connection with
        | :? NativeCachingDbConnection as c -> c.EnterEventHandlerScope()
        | _ -> enterStandaloneEventHandlerScope connection

    let internal ExitEventHandlerScope(connection: NativeSqliteConnection) =
        let mutable primaryEx: exn option = None

        try
            match connection with
            | :? NativeCachingDbConnection as c -> c.ExitEventHandlerScope()
            | _ -> exitStandaloneEventHandlerScopeOrFail connection
        with ex ->
            primaryEx <- Some ex

        match primaryEx with
        | Some p -> raise p
        | None -> ()
