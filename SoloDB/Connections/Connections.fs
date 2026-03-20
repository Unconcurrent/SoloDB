namespace SoloDatabase

open System.Data

/// <summary>
/// Contains types related to database connection management, pooling, and transactions.
/// </summary>
module Connections =
    open Microsoft.Data.Sqlite
    open SQLiteTools
    open ConnectionsTransactionHelpers
    open System
    open System.Collections.Concurrent
    open System.Threading
    open System.Threading.Tasks
    open Utils
    open System.IO

    let internal ThrowOutsideEventContextUsage = ConnectionsTransactionHelpers.ThrowOutsideEventContextUsage
    let private eventHandlerScopeUnderflowMessage = ConnectionsTransactionHelpers.eventHandlerScopeUnderflowMessage
    let internal takeHandlerFaultCommitException = ConnectionsTransactionHelpers.takeHandlerFaultCommitException
    let private rollbackBorrowedTransaction = ConnectionsTransactionHelpers.rollbackBorrowedTransaction
    let private commitOrRollbackBorrowedTransaction = ConnectionsTransactionHelpers.commitOrRollbackBorrowedTransaction
    let private cleanupBorrowedTransaction = ConnectionsTransactionHelpers.cleanupBorrowedTransaction
    let private withPooledTransactionCore = ConnectionsTransactionHelpers.withPooledTransactionCore
    let internal withSavepoint = ConnectionsTransactionHelpers.withSavepoint
    let internal withSavepointAsync = ConnectionsTransactionHelpers.withSavepointAsync
    let private shouldSuppressSavepointInHandler = ConnectionsTransactionHelpers.shouldSuppressSavepointInHandler
    let internal beginImmediateWithRetry = ConnectionsTransactionHelpers.beginImmediateWithRetry
    let internal resolveTxOutcome = ConnectionsTransactionHelpers.resolveTxOutcome

    /// <summary>
    /// Represents a specialized <see cref="SqliteConnection"/> whose <c>Dispose</c> method is a no-op.
    /// This is used to pass a connection to a user-defined transaction block without it being closed prematurely.
    /// The actual disposal is handled by the <see cref="ConnectionManager"/>.
    /// </summary>
    /// <param name="connectionStr">The connection string for the database.</param>
    type TransactionalConnection internal (connectionStr: string) =
        inherit SqliteConnection(connectionStr)

        /// <summary>
        /// Performs the actual disposal of the base <see cref="SqliteConnection"/>.
        /// This should only be called by the owning <see cref="ConnectionManager"/>.
        /// </summary>
        /// <param name="disposing">If true, disposes managed resources.</param>
        member internal this.DisposeReal(disposing) =
            base.Dispose disposing

        /// <summary>
        /// Overrides the default Dispose behavior to do nothing. This prevents the connection
        /// from being closed inside a 'use' binding within a transaction.
        /// </summary>
        /// <param name="disposing">Disposal flag.</param>
        override this.Dispose(disposing) =
            // This is intentionally a no-op.
            ()

    /// <summary>
    /// Manages a pool of reusable <see cref="CachingDbConnection"/> objects to reduce the overhead
    /// of opening and closing database connections. It also provides transaction management.
    /// </summary>
    /// <param name="connectionStr">The database connection string.</param>
    /// <param name="setup">An action to perform initial setup on a newly created connection.</param>
    /// <param name="config">The database configuration settings.</param>
    and ConnectionManager internal (connectionStr: string, setup: SqliteConnection -> unit, config: Types.SoloDBConfiguration) =
        /// <summary>A collection of all connections ever created by this manager, for disposal purposes.</summary>
        let all = ConcurrentStack<CachingDbConnection>()
        /// <summary>The pool of available, ready-to-use connections.</summary>
        let pool = ConcurrentStack<CachingDbConnection>()
        /// <summary>A flag indicating whether the manager has been disposed.</summary>
        let mutable disposed = false
        let mutable activeEventHandlerScopes = 0
        let mutable isCurrentThreadInHandlerDispatch = (fun () -> false)

        /// <summary>
        /// Checks if the manager has been disposed and throws an exception if it has.
        /// </summary>
        let checkDisposed () =
            if Volatile.Read(&disposed) then raise (ObjectDisposedException(nameof(ConnectionManager)))

        /// <summary>
        /// Returns a used connection to the pool. Before returning, it verifies that any
        /// explicit transactions on the connection have been completed.
        /// </summary>
        /// <param name="pooledConn">The connection to return to the pool.</param>
        /// <exception cref="InvalidOperationException">Thrown if the connection is still inside a transaction.</exception>
        member internal this.TakeBack(pooledConn: CachingDbConnection) =
            // Verify no stray transaction is active before returning to pool.
            let probeOk =
                try pooledConn.Execute("BEGIN; ROLLBACK;") |> ignore; true
                with
                | :? SqliteException as se when se.SqliteErrorCode = 1 && se.SqliteExtendedErrorCode = 1 ->
                    ("Error: Connection returned to pool while a transaction is still active.\nReason: The transaction must be finished before returning the connection.\nFix: Commit or rollback the transaction before returning the connection.", se)
                    |> InvalidOperationException |> raise
                | _ ->
                    // Connection is in unknown state (concurrent DDL schema churn, driver error, etc.).
                    // Fail-safe: dispose instead of returning to pool.
                    try pooledConn.DisposeReal() with _ -> ()
                    false

            if probeOk then
                pooledConn.ResetEventDispatchState()
                pool.Push pooledConn

        /// <summary>
        /// Borrows a connection from the pool. If the pool is empty, a new connection is created.
        /// </summary>
        /// <returns>A ready-to-use <see cref="CachingDbConnection"/>.</returns>
        member this.Borrow() =
            checkDisposed()
            match pool.TryPop() with
            | true, c -> 
                if not c.IsEventDispatchStateClean then
                    c.ResetEventDispatchState()
                    raise (InvalidOperationException("Borrowed pooled connection had residual event dispatch state."))

                if c.Inner.State <> ConnectionState.Open then
                    c.Inner.Open()
                c
            | false, _ ->
                let createAndSetup () =
                    let c = new CachingDbConnection(connectionStr, this.TakeBack, config, this.EnterEventHandlerScope, this.ExitEventHandlerScope)
                    let mutable primaryEx: exn option = None
                    let mutable cleanupEx: exn option = None
                    try
                        c.Inner.Open()
                        setup c.Inner
                        all.Push c
                        c
                    with ex ->
                        primaryEx <- Some ex
                        try c.DisposeReal() with d -> cleanupEx <- Some d
                        match resolveTxOutcome primaryEx cleanupEx with
                        | Some resolved -> reraiseAnywhere resolved
                        | None -> raise (InvalidOperationException("Connection setup failed."))
                // SQLITE_SCHEMA compensation: concurrent DDL can invalidate the schema cache
                // during connection setup (Open/PRAGMA/CreateFunction). Microsoft.Data.Sqlite
                // surfaces this as ArgumentOutOfRangeException or SqliteException with schema-churn
                // messages. Retry with backoff, matching beginImmediateWithRetry pattern.
                let isTransientSetupRace (ex: exn) =
                    match ex with
                    | :? ArgumentOutOfRangeException -> true
                    | :? SqliteException as se ->
                        se.SqliteErrorCode = 1
                        && (se.Message.IndexOf("no such", StringComparison.OrdinalIgnoreCase) >= 0
                            || se.Message.IndexOf("database schema has changed", StringComparison.OrdinalIgnoreCase) >= 0)
                    | _ -> false
                let mutable attempt = 0
                let mutable conn = Unchecked.defaultof<CachingDbConnection>
                while isNull (box conn) && attempt < 5 do
                    try conn <- createAndSetup()
                    with ex when isTransientSetupRace ex && attempt < 4 ->
                        attempt <- attempt + 1
                        Thread.Sleep(25 * attempt)
                conn

        /// <summary>
        /// Gets a collection of all connections (both in-pool and in-use) created by this manager.
        /// Used for final disposal.
        /// </summary>
        member internal this.All = all

        /// <summary>
        /// Creates a new <see cref="TransactionalConnection"/> that will not be closed prematurely.
        /// </summary>
        /// <returns>A new, open <see cref="TransactionalConnection"/>.</returns>
        member internal this.CreateForTransaction() =
            checkDisposed()
            let c = new TransactionalConnection(connectionStr)
            let mutable primaryEx: exn option = None
            let mutable cleanupEx: exn option = None
            try
                c.Open()
                setup c
                c
            with ex ->
                primaryEx <- Some ex
                try c.DisposeReal(true) with d -> cleanupEx <- Some d
                match primaryEx, cleanupEx with
                | Some p, Some d -> p.Data["SoloDB.CleanupException"] <- d; raise p
                | Some p, None -> raise p
                | None, Some d -> raise d
                | None, None -> raise (InvalidOperationException("Transactional connection setup failed."))

        /// <summary>
        /// The core implementation for executing a synchronous function within a database transaction.
        /// It handles beginning the transaction and committing or rolling back based on the outcome.
        /// </summary>
        /// <param name="f">The function to execute within the transaction.</param>
        /// <returns>The result of the function <paramref name="f"/>.</returns>
        member private this.WithTransactionBorrowed(f: CachingDbConnection -> 'T) =
            let conn = this.Borrow()
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
                cleanupBorrowedTransaction conn cleanupEx

            match resolveTxOutcome !primaryEx !cleanupEx with
            | Some ex -> raise ex
            | None -> result

        /// <summary>
        /// The core implementation for executing an asynchronous function within a database transaction.
        /// </summary>
        /// <param name="f">The asynchronous function to execute within the transaction.</param>
        /// <returns>A task that represents the asynchronous operation, containing the result of the function <paramref name="f"/>.</returns>
        member private this.WithTransactionBorrowedAsync(f: CachingDbConnection -> Task<'T>) = task {
            let conn = this.Borrow()
            let primaryEx = ref None
            let cleanupEx = ref None
            let mutable result = Unchecked.defaultof<'T>
            try
                beginImmediateWithRetry conn
                conn.InsideTransaction <- true
                try
                    let! ret = f conn
                    result <- ret
                    commitOrRollbackBorrowedTransaction conn primaryEx cleanupEx
                with ex ->
                    rollbackBorrowedTransaction conn primaryEx cleanupEx ex
            finally
                cleanupBorrowedTransaction conn cleanupEx

            match resolveTxOutcome !primaryEx !cleanupEx with
            | Some ex -> return reraiseAnywhere ex
            | None -> return result
        }

        /// <summary>
        /// Executes a synchronous function within a database transaction using a pooled connection.
        /// </summary>
        /// <param name="f">The function to execute.</param>
        /// <returns>The result of the function.</returns>
        member internal this.WithTransaction(f: CachingDbConnection -> 'T) =
            this.WithTransactionBorrowed f

        /// <summary>
        /// Executes an asynchronous function within a database transaction using a pooled connection.
        /// </summary>
        /// <param name="f">The asynchronous function to execute.</param>
        /// <returns>A task representing the asynchronous transactional operation.</returns>
        member internal this.WithAsyncTransaction(f: CachingDbConnection -> Task<'T>) = task {
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
                Volatile.Write(&disposed, true)
                for c in all do
                    c.DisposeReal()
                all.Clear()
                pool.Clear()
                ()

    
    /// <summary>
    /// A discriminated union that represents the different types of database connections available within the system.
    /// This allows for abstracting over whether a connection is from a pool or part of an explicit transaction.
    /// </summary>
    and Connection =
        /// <summary>A connection sourced from a connection pool.</summary>
        | Pooled of pool: ConnectionManager
        /// <summary>A dedicated, non-disposing connection for an ongoing transaction.</summary>
        | Transactional of conn: SqliteConnection
        /// <summary>A connection guarded by a validity check.</summary>
        | Guarded of guard: (unit -> unit) * inner: Connection

        /// <summary>
        /// Gets an active <see cref="SqliteConnection"/> based on the connection type.
        /// If Pooled, it borrows a connection. Otherwise, it returns the existing connection.
        /// </summary>
        /// <returns>An active <see cref="SqliteConnection"/>.</returns>
        member this.Get() : SqliteConnection =
            match this with
            | Pooled pool ->
                if pool.IsCurrentThreadInEventHandlerScope then
                    ThrowOutsideEventContextUsage()
                pool.Borrow()
            | Transactional conn -> conn
            | Guarded (guard, inner) ->
                guard()
                inner.Get()

        /// <summary>
        /// Executes a synchronous function within a transaction. The behavior depends on the connection type.
        /// Pooled: BEGIN IMMEDIATE (top-level). Transactional: SAVEPOINT (nested).
        /// </summary>
        /// <param name="f">The function to execute within the transaction.</param>
        /// <returns>The result of the function.</returns>
        member this.WithTransaction(f: SqliteConnection -> 'T) =
            match this with
            | Pooled pool -> withPooledTransactionCore (fun () -> pool.IsCurrentThreadInEventHandlerScope) pool.WithTransaction f
            | Transactional conn when shouldSuppressSavepointInHandler conn -> f conn
            | Transactional conn -> withSavepoint conn f
            | Guarded (guard, inner) ->
                guard()
                inner.WithTransaction f

        /// <summary>
        /// Executes an asynchronous function within a transaction. The behavior depends on the connection type.
        /// Pooled: BEGIN IMMEDIATE (top-level). Transactional: SAVEPOINT (nested).
        /// </summary>
        /// <param name="f">The asynchronous function to execute.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        member this.WithAsyncTransaction(f: SqliteConnection -> Task<'T>) =
            match this with
            | Pooled pool -> withPooledTransactionCore (fun () -> pool.IsCurrentThreadInEventHandlerScope) pool.WithAsyncTransaction f
            | Transactional conn when shouldSuppressSavepointInHandler conn -> f conn
            | Transactional conn -> withSavepointAsync conn f
            | Guarded (guard, inner) ->
                guard()
                inner.WithAsyncTransaction f

    let internal EnterEventHandlerScope(connection: SqliteConnection) =
        match connection with
        | :? CachingDbConnection as c -> c.EnterEventHandlerScope()
        | _ -> enterStandaloneEventHandlerScope connection

    let internal ExitEventHandlerScope(connection: SqliteConnection) =
        let mutable primaryEx: exn option = None

        try
            match connection with
            | :? CachingDbConnection as c -> c.ExitEventHandlerScope()
            | _ -> exitStandaloneEventHandlerScopeOrFail connection
        with ex ->
            primaryEx <- Some ex

        match primaryEx with
        | Some p -> raise p
        | None -> ()
