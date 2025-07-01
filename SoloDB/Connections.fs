namespace SoloDatabase

open System.Data

module Connections =
    open Microsoft.Data.Sqlite
    open SQLiteTools
    open System
    open System.Collections.Concurrent
    open System.Threading.Tasks
    open Utils

    type TransactionalConnection internal (connectionStr: string) =
        inherit SqliteConnection(connectionStr)

        member internal this.DisposeReal(disposing) =
            base.Dispose disposing

        static member private NoopDispose = { new IDisposable with override _.Dispose() = () }

        interface IDisableDispose with
            member this.DisableDispose(): IDisposable = 
                TransactionalConnection.NoopDispose

        override this.Dispose(disposing) =
            // Noop
            ()

    and ConnectionManager internal (connectionStr: string, setup: SqliteConnection -> unit, config: Types.SoloDBConfiguration) =
        let all = ConcurrentStack<CachingDbConnection>()
        let pool = ConcurrentStack<CachingDbConnection>()
        let mutable disposed = false

        let checkDisposed () =
            if disposed then raise (ObjectDisposedException(nameof(ConnectionManager)))

        member internal this.TakeBack(pooledConn: CachingDbConnection) =
            // SQLite does not support nested transactions, therefore we can use it to check if the user forgot to 
            // end the transaction before returning it to the pool.
            try pooledConn.Execute("BEGIN; ROLLBACK;") |> ignore
            with 
            | :? SqliteException as se when se.SqliteErrorCode = 1 && se.SqliteExtendedErrorCode = 1 ->
                ("The transaction must be finished before you return the connection to the pool.", se) |> InvalidOperationException |> raise

            pool.Push pooledConn

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

        member internal this.All = all

        member internal this.CreateForTransaction() =
            checkDisposed()
            let c = new TransactionalConnection(connectionStr)
            c.Open()
            setup c
            c

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

        member internal this.WithTransaction(f: CachingDbConnection -> 'T) =
            this.WithTransactionBorrowed f

        member internal this.WithAsyncTransaction(f: CachingDbConnection -> Task<'T>) = task {
            return! this.WithTransactionBorrowedAsync f
        }

        interface IDisposable with
            override this.Dispose() =
                disposed <- true
                for c in all do
                    c.DisposeReal()
                all.Clear()
                pool.Clear()
                ()

    
    and [<Struct>] Connection =
        | Pooled of pool: ConnectionManager
        | Transactional of conn: TransactionalConnection
        | Transitive of tc: SqliteConnection

        member this.Get() : SqliteConnection =
            match this with
            | Pooled pool -> pool.Borrow()
            | Transactional conn -> conn
            | Transitive c -> c

        member this.WithTransaction(f: SqliteConnection -> 'T) =
            match this with
            | Pooled pool -> pool.WithTransaction f
            | Transactional conn -> 
                f conn
            | Transitive conn when conn.IsWithinTransaction() ->
                f conn
            | Transitive _conn ->
                raise (InvalidOperationException "A simple Transitive Connection should never be used with a transation.")

        member this.WithAsyncTransaction(f: SqliteConnection -> Task<'T>) =
            match this with
            | Pooled pool -> 
                pool.WithAsyncTransaction f
            | Transactional conn -> 
                f conn
            | Transitive _conn -> 
                raise (InvalidOperationException "A Transitive Connection should never be used with a transation.")
        

    and SqliteConnection with
        member this.IsWithinTransaction() =
            match this with
            | :? TransactionalConnection -> true
            // All pure DirectConnection usage is inside a transaction
            | :? CachingDbConnection as cc -> cc.InsideTransaction
            | _other -> false