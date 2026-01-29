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
    open System.Threading.Tasks
    open Utils

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
        /// A static, reusable IDisposable object that performs no action upon disposal.
        /// </summary>
        static member private NoopDispose = { new IDisposable with override _.Dispose() = () }

        /// <summary>
        /// Implementation of the IDisableDispose interface.
        /// </summary>
        interface IDisableDispose with
            /// <summary>
            /// Returns a dummy IDisposable that does nothing.
            /// </summary>
            /// <returns>An IDisposable that can be safely "disposed" without affecting the connection.</returns>
            member this.DisableDispose(): IDisposable = 
                TransactionalConnection.NoopDispose

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

        /// <summary>
        /// Checks if the manager has been disposed and throws an exception if it has.
        /// </summary>
        let checkDisposed () =
            if disposed then raise (ObjectDisposedException(nameof(ConnectionManager)))

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
                ("The transaction must be finished before you return the connection to the pool.", se) |> InvalidOperationException |> raise

            pool.Push pooledConn

        /// <summary>
        /// Borrows a connection from the pool. If the pool is empty, a new connection is created.
        /// </summary>
        /// <returns>A ready-to-use <see cref="CachingDbConnection"/>.</returns>
        member this.Borrow() =
            checkDisposed()
            match pool.TryPop() with
            | true, c -> 
                if c.Inner.State <> ConnectionState.Open then
                    c.Inner.Open()
                c
            | false, _ -> 
                let c = new CachingDbConnection(connectionStr, this.TakeBack, config)
                c.Inner.Open()
                setup c.Inner
                all.Push c
                c

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
            c.Open()
            setup c
            c

        /// <summary>
        /// The core implementation for executing a synchronous function within a database transaction.
        /// It handles beginning the transaction and committing or rolling back based on the outcome.
        /// </summary>
        /// <param name="f">The function to execute within the transaction.</param>
        /// <returns>The result of the function <paramref name="f"/>.</returns>
        member private this.WithTransactionBorrowed(f: CachingDbConnection -> 'T) =
            use connectionForTransaction = this.Borrow()
            connectionForTransaction.Execute("BEGIN IMMEDIATE;") |> ignore
            try
                connectionForTransaction.InsideTransaction <- true
                try
                    let ret = f connectionForTransaction
                    connectionForTransaction.Execute "COMMIT;" |> ignore
                    ret
                with ex -> 
                    connectionForTransaction.Execute "ROLLBACK;" |> ignore
                    reraise()
            finally
                connectionForTransaction.InsideTransaction <- false

        /// <summary>
        /// The core implementation for executing an asynchronous function within a database transaction.
        /// </summary>
        /// <param name="f">The asynchronous function to execute within the transaction.</param>
        /// <returns>A task that represents the asynchronous operation, containing the result of the function <paramref name="f"/>.</returns>
        member private this.WithTransactionBorrowedAsync(f: CachingDbConnection -> Task<'T>) = task {
            use connectionForTransaction = this.Borrow()
            connectionForTransaction.Execute("BEGIN IMMEDIATE;") |> ignore
            try
                connectionForTransaction.InsideTransaction <- true
                try
                    let! ret = f connectionForTransaction
                    connectionForTransaction.Execute "COMMIT;" |> ignore
                    return ret
                with ex -> 
                    connectionForTransaction.Execute "ROLLBACK;" |> ignore
                    return reraiseAnywhere ex
            finally
                connectionForTransaction.InsideTransaction <- false
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

        /// <summary>
        /// Disposes the connection manager, which closes and disposes all connections it has created.
        /// </summary>
        interface IDisposable with
            override this.Dispose() =
                disposed <- true
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
        | Transactional of conn: TransactionalConnection
        /// <summary>A raw, pass-through connection, typically for internal operations.</summary>
        | Transitive of tc: SqliteConnection
        /// <summary>A connection guarded by a validity check.</summary>
        | Guarded of guard: (unit -> unit) * inner: Connection

        /// <summary>
        /// Gets an active <see cref="SqliteConnection"/> based on the connection type.
        /// If Pooled, it borrows a connection. Otherwise, it returns the existing connection.
        /// </summary>
        /// <returns>An active <see cref="SqliteConnection"/>.</returns>
        member this.Get() : SqliteConnection =
            match this with
            | Pooled pool -> pool.Borrow()
            | Transactional conn -> conn
            | Transitive c -> c
            | Guarded (guard, inner) ->
                guard()
                inner.Get()

        /// <summary>
        /// Executes a synchronous function within a transaction. The behavior depends on the connection type.
        /// </summary>
        /// <param name="f">The function to execute within the transaction.</param>
        /// <returns>The result of the function.</returns>
        /// <exception cref="InvalidOperationException">Thrown if attempting to start a transaction on a simple Transitive connection.</exception>
        member this.WithTransaction(f: SqliteConnection -> 'T) =
            match this with
            | Pooled pool -> pool.WithTransaction f
            | Transactional conn -> 
                f conn
            | Transitive conn when conn.IsWithinTransaction() ->
                f conn
            | Transitive _conn ->
                raise (InvalidOperationException "A simple Transitive Connection should never be used with a transation.")
            | Guarded (guard, inner) ->
                guard()
                inner.WithTransaction f

        /// <summary>
        /// Executes an asynchronous function within a transaction. The behavior depends on the connection type.
        /// </summary>
        /// <param name="f">The asynchronous function to execute.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">Thrown if attempting to start a transaction on a Transitive connection.</exception>
        member this.WithAsyncTransaction(f: SqliteConnection -> Task<'T>) =
            match this with
            | Pooled pool -> 
                pool.WithAsyncTransaction f
            | Transactional conn -> 
                f conn
            | Transitive _conn -> 
                raise (InvalidOperationException "A Transitive Connection should never be used with a transation.")
            | Guarded (guard, inner) ->
                guard()
                inner.WithAsyncTransaction f
        
    /// <summary>
    /// Extends the <see cref="SqliteConnection"/> class with helper methods.
    /// </summary>
    and SqliteConnection with
        /// <summary>
        /// Checks if the connection is currently operating within a transaction managed by this system.
        /// </summary>
        /// <returns><c>true</c> if the connection is within a transaction, otherwise <c>false</c>.</returns>
        member this.IsWithinTransaction() =
            match this with
            | :? TransactionalConnection -> true
            | :? CachingDbConnection as cc -> cc.InsideTransaction
            | _other -> false
