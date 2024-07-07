namespace SoloDatabase
module Connections =
    open Microsoft.Data.Sqlite
    open Dapper
    open System
    open System.Collections.Concurrent

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

        member internal this.TakeBack(pooledConn: PooledConnection) =
            if Utils.debugTransactions then
                try
                    pooledConn.Execute("ROLLBACK;") |> ignore
                    failwithf "A transaction was not ended, before returning to the pool."
                with 
                | :? Microsoft.Data.Sqlite.SqliteException as e ->
                    // As expected.
                    ()
                | other -> 
                    reraise()

            pool.Push pooledConn

        member this.Borrow() =
            match pool.TryPop() with
            | true, c -> c
            | false, _ -> 
                let c = new PooledConnection(connectionStr, this)
                c.Open()
                setup c
                all.Push c
                c

        member this.CreateForTransaction() =
            let c = new TransactionalConnection(connectionStr)
            setup c
            c

        interface IDisposable with
            override this.Dispose() =
                for c in all do
                    c.DisposeReal(true)
                all.Clear()
                ()

    [<Struct>]
    type Connection =
        | Pooled of pool: ConnectionManager
        | Transactional of conn: TransactionalConnection

        member this.Get() : SqliteConnection =
            match this with
            | Pooled pool -> pool.Borrow()
            | Transactional conn -> conn