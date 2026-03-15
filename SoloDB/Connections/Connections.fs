namespace SoloDatabase

open System.Data

/// <summary>
/// Contains types related to database connection management, pooling, and transactions.
/// </summary>
module Connections =
    open Microsoft.Data.Sqlite
    open SQLiteTools
    open System
    open System.Collections.Concurrent
    open System.Threading
    open System.Threading.Tasks
    open Utils
    open System.IO

    let internal ThrowOutsideEventContextUsage() =
            raise (InvalidOperationException(
                "Error: Event handler used a non-context SoloDB instance.\nReason: Event handlers must use the provided ctx (ISoloDB). Using another instance can lock the database.\nFix: Use the ctx parameter for all database operations inside handlers."))
    let private eventHandlerScopeUnderflowMessage =
        "Event handler scope underflow detected. ExitEventHandlerScope was called without a matching EnterEventHandlerScope."

    // Global savepoint counter for unique savepoint names across all connections and threads.
    let mutable private savepointCounter = 0L

    let internal takeHandlerFaultCommitException (connection: SqliteConnection) : exn option =
        match takeHandlerFault connection with
        | Some handlerEx ->
            let commitEx = InvalidOperationException(
                "Error: Transaction cannot commit because handler-scoped database work failed.
Reason: Event handlers run on the active connection while SAVEPOINT is suppressed, so swallowed database faults would otherwise leak partial side effects.
Fix: Let handler-side database faults abort the outer transaction, or avoid swallowing them.",
                handlerEx)
            commitEx.Data["SoloDB.HandlerScopedFault"] <- handlerEx
            Some commitEx
        | None -> None

    let private cleanupSavepointRollback (conn: SqliteConnection) (sp: string) (ex: exn) =
        let faultDepth = tryGetRecordedHandlerFaultDepth conn ex
        let mutable rollbackFailure: exn option = None
        let mutable releaseFailure: exn option = None
        try conn.Execute(sprintf "ROLLBACK TO \"%s\";" sp) |> ignore with rb -> rollbackFailure <- Some rb
        try conn.Execute(sprintf "RELEASE \"%s\";" sp) |> ignore with rel -> releaseFailure <- Some rel
        match rollbackFailure, releaseFailure with
        | None, None ->
            match faultDepth with
            | Some depth -> clearNonSwallowedHandlerFaultsAtDepth conn depth
            | None -> ()
        | Some rb, Some rel ->
            let wrapped = InvalidOperationException("SAVEPOINT rollback cleanup failed: both ROLLBACK TO and RELEASE failed.", rb)
            wrapped.Data["SoloDB.SavepointReleaseException"] <- rel
            wrapped.Data["SoloDB.PrimaryException"] <- ex
            raise wrapped
        | Some rb, None ->
            let wrapped = InvalidOperationException("SAVEPOINT rollback cleanup failed: ROLLBACK TO failed.", rb)
            wrapped.Data["SoloDB.PrimaryException"] <- ex
            raise wrapped
        | None, Some rel ->
            let wrapped = InvalidOperationException("SAVEPOINT rollback cleanup failed: RELEASE failed.", rel)
            wrapped.Data["SoloDB.PrimaryException"] <- ex
            raise wrapped

    let private rollbackBorrowedTransaction (conn: CachingDbConnection) (primaryEx: exn option ref) (cleanupEx: exn option ref) (ex: exn) =
        primaryEx := Some ex
        try conn.Execute "ROLLBACK;" |> ignore with rb -> cleanupEx := Some rb

    let private commitOrRollbackBorrowedTransaction (conn: CachingDbConnection) (primaryEx: exn option ref) (cleanupEx: exn option ref) =
        match takeHandlerFaultCommitException conn with
        | Some ex -> rollbackBorrowedTransaction conn primaryEx cleanupEx ex
        | None -> conn.Execute "COMMIT;" |> ignore

    let private cleanupBorrowedTransaction (conn: CachingDbConnection) (cleanupEx: exn option ref) =
        conn.InsideTransaction <- false
        clearHandlerFault conn
        try (conn :> IDisposable).Dispose() with ex ->
            match !cleanupEx with
            | None -> cleanupEx := Some ex
            | Some _ -> ()

    let private withPooledTransactionCore isInHandlerScope runner f =
        if isInHandlerScope() then
            ThrowOutsideEventContextUsage()
        runner f

    /// Wraps a synchronous operation in a SAVEPOINT scope.
    /// On success: RELEASE (merge into parent). On failure: ROLLBACK TO + RELEASE (undo nested, keep parent).
    let internal withSavepoint (conn: SqliteConnection) (f: SqliteConnection -> 'T) =
        let sp = $"sp_{Interlocked.Increment(&savepointCounter)}"
        conn.Execute(sprintf "SAVEPOINT \"%s\";" sp) |> ignore
        try
            let ret = f conn
            conn.Execute(sprintf "RELEASE \"%s\";" sp) |> ignore
            ret
        with ex ->
            cleanupSavepointRollback conn sp ex
            reraise()

    /// Wraps an asynchronous operation in a SAVEPOINT scope.
    /// On success: RELEASE (merge into parent). On failure: ROLLBACK TO + RELEASE (undo nested, keep parent).
    let internal withSavepointAsync (conn: SqliteConnection) (f: SqliteConnection -> Task<'T>) = task {
        let sp = $"sp_{Interlocked.Increment(&savepointCounter)}"
        conn.Execute(sprintf "SAVEPOINT \"%s\";" sp) |> ignore
        try
            let! ret = f conn
            conn.Execute(sprintf "RELEASE \"%s\";" sp) |> ignore
            return ret
        with ex ->
            cleanupSavepointRollback conn sp ex
            return reraiseAnywhere ex
    }

    // Handler-context savepoint suppression.
    // Returns true only when the connection is a CachingDbConnection currently inside
    // a SQLite trigger callback (event handler scope). In this context, the original
    // INSERT/UPDATE/DELETE statement is still active, and opening a SAVEPOINT would
    // fail with SQLite Error 5 ("cannot open savepoint - SQL statements in progress").
    let private shouldSuppressSavepointInHandler (conn: SqliteConnection) =
        isInEventHandlerScope conn

    let internal beginImmediateWithRetry (connection: SqliteConnection) =
        clearHandlerFault connection
        let mutable attempt = 0
        let mutable started = false
        while not started && attempt < 5 do
            try
                connection.Execute("BEGIN IMMEDIATE;") |> ignore
                started <- true
            with :? SqliteException as se when (se.SqliteErrorCode = 5 || se.SqliteErrorCode = 6) && attempt < 4 ->
                attempt <- attempt + 1
                Thread.Sleep(25 * attempt)

    let internal resolveTxOutcome (primaryEx: exn option) (cleanupEx: exn option) : exn option =
        match primaryEx, cleanupEx with
        | Some p, Some c ->
            p.Data["SoloDB.CleanupException"] <- c
            Some p
        | Some p, None -> Some p
        | None, Some c -> Some c
        | None, None -> None

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
            // SQLite does not support nested transactions, therefore we can use this to check if the user forgot to 
            // end the transaction before returning it to the pool.
            try pooledConn.Execute("BEGIN; ROLLBACK;") |> ignore
            with 
            | :? SqliteException as se when se.SqliteErrorCode = 1 && se.SqliteExtendedErrorCode = 1 ->
                ("Error: Connection returned to pool while a transaction is still active.\nReason: The transaction must be finished before returning the connection.\nFix: Commit or rollback the transaction before returning the connection.", se)
                |> InvalidOperationException |> raise

            if not pooledConn.IsEventDispatchStateClean then
                pooledConn.ResetEventDispatchState()
                raise (InvalidOperationException("Event dispatch state was not clean when returning a pooled connection. Ensure handler dispatch scopes always unwind and pending removals are flushed."))

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
                    match primaryEx, cleanupEx with
                    | Some p, Some d -> p.Data["SoloDB.CleanupException"] <- d; raise p
                    | Some p, None -> raise p
                    | None, Some d -> raise d
                    | None, None -> raise (InvalidOperationException("Connection setup failed."))

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
