namespace SoloDatabase

open System.Data

module Connections =
    open Microsoft.Data.Sqlite
    open SQLiteTools
    open System
    open System.Collections.Concurrent
    open System.Threading.Tasks
    open Utils

    type DirectConnection internal (connection: IDbConnection) =
        interface IDbConnection with
            override this.BeginTransaction() = connection.BeginTransaction()
            override this.BeginTransaction (il: IsolationLevel) = connection.BeginTransaction il
            override this.ChangeDatabase (databaseName: string) = connection.ChangeDatabase databaseName
            override this.Close() = connection.Close()
            override this.CreateCommand() = connection.CreateCommand()
            override this.Open() = connection.Open()
            member this.ConnectionString with get() = connection.ConnectionString and set (cs) = connection.ConnectionString <- cs
            member this.ConnectionTimeout: int = connection.ConnectionTimeout
            member this.Database: string = connection.Database
            member this.State: ConnectionState = connection.State

        interface IDisposable with 
            override this.Dispose (): unit = 
                ()

        member this.DisposeReal() =
            connection.Dispose()

    type TransactionalConnection internal (connectionStr: string) =
        inherit SqliteConnection(connectionStr)

        member internal this.DisposeReal(disposing) =
            base.Dispose disposing

        override this.Dispose(disposing) =
            // Noop
            ()


    type PooledConnection internal (connectionStr: string, manager: ConnectionManager) =
        inherit SqliteConnection(connectionStr)

        member internal this.DisposeReal(disposing) =
            base.Dispose disposing

        override this.Dispose(disposing) =
            manager.TakeBack this

    and ConnectionManager internal (connectionStr: string, setup: SqliteConnection -> unit) =
        let all = ConcurrentStack<PooledConnection>()
        let pool = ConcurrentStack<PooledConnection>()
        let mutable disposed = false

        let checkDisposed () =
            if disposed then raise (ObjectDisposedException(nameof(ConnectionManager)))

        member internal this.TakeBack(pooledConn: PooledConnection) =
            // SQLite does not support nested transactions, therefore we can use it to check if the user forgot to 
            // end the transaction before returning it to the pool.
            try pooledConn.Execute("BEGIN; ROLLBACK;") |> ignore
            with 
            | :? SqliteException as se when se.SqliteErrorCode = 1 && se.SqliteExtendedErrorCode = 1 ->
                failwithf "The transaction must be finished before you return the connection to the pool."

            pool.Push pooledConn

        member this.Borrow() =
            checkDisposed()
            match pool.TryPop() with
            | true, c -> 
                if c.State <> ConnectionState.Open then
                    c.Open()
                c
            | false, _ -> 
                let c = new PooledConnection(connectionStr, this)
                c.Open()
                setup c
                all.Push c
                c

        member internal this.CreateForTransaction() =
            checkDisposed()
            let c = new TransactionalConnection(connectionStr)
            c.Open()
            setup c
            c

        member private this.WithTransactionBorrowed(f: SqliteConnection -> 'T) =
            use connectionForTransaction = this.Borrow()
            connectionForTransaction.Execute("BEGIN IMMEDIATE;") |> ignore
                
            try
                let ret = f connectionForTransaction
                connectionForTransaction.Execute "COMMIT;" |> ignore
                ret
            with ex -> 
                connectionForTransaction.Execute "ROLLBACK;" |> ignore
                reraise()

        member private this.WithTransactionBorrowedAsync(f: SqliteConnection -> Task<'T>) = task {
            use connectionForTransaction = this.Borrow()
            connectionForTransaction.Execute("BEGIN IMMEDIATE;") |> ignore
                
            try
                let! ret = f connectionForTransaction
                connectionForTransaction.Execute "COMMIT;" |> ignore
                return ret
            with ex -> 
                connectionForTransaction.Execute "ROLLBACK;" |> ignore
                return reraiseAnywhere ex
        }

        member private this.WithTransactionNewlyCreated(f: SqliteConnection -> 'T) =
            use connectionForTransaction = this.CreateForTransaction()
            try
                connectionForTransaction.Execute("BEGIN IMMEDIATE;") |> ignore
                
                try
                    let ret = f connectionForTransaction
                    connectionForTransaction.Execute "COMMIT;" |> ignore
                    ret
                with ex -> 
                    connectionForTransaction.Execute "ROLLBACK;" |> ignore
                    reraise()
            finally connectionForTransaction.DisposeReal(true)

        member private this.WithTransactionNewlyCreatedAsync(f: SqliteConnection -> Task<'T>) = task {
            use connectionForTransaction = this.CreateForTransaction()
            try
                connectionForTransaction.Execute("BEGIN IMMEDIATE;") |> ignore
        
                try
                    let! ret = f connectionForTransaction
                    connectionForTransaction.Execute "COMMIT;" |> ignore
                    return ret
                with ex -> 
                    connectionForTransaction.Execute "ROLLBACK;" |> ignore
                    return reraiseAnywhere ex
            finally connectionForTransaction.DisposeReal(true)
        }


        member internal this.WithTransaction(f: SqliteConnection -> 'T) =
            this.WithTransactionBorrowed f

        member internal this.WithAsyncTransaction(f: SqliteConnection -> Task<'T>) = task {
            return! this.WithTransactionBorrowedAsync f
        }

        interface IDisposable with
            override this.Dispose() =
                disposed <- true
                for c in all do
                    c.DisposeReal(true)
                all.Clear()
                pool.Clear()
                ()

    [<Struct>]
    type Connection =
        | Pooled of pool: ConnectionManager
        | Transactional of conn: TransactionalConnection
        | Transitive of tc: IDbConnection

        member this.Get() : IDbConnection =
            match this with
            | Pooled pool -> pool.Borrow()
            | Transactional conn -> conn
            | Transitive c -> c

    type IDbConnection with
        member this.IsWithinTransaction() =
            match this with
            | :? TransactionalConnection -> true
            | other -> false