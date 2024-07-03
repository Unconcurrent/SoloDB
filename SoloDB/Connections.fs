module Connections

open Microsoft.Data.Sqlite
open Dapper
open System
open System.Collections.Concurrent
open System.Data

type PooledConnection(connectionStr: string, manager: ConnectionManager) =
    inherit SqliteConnection(connectionStr)

    member this.DisposeReal(disposing) =
        base.Dispose disposing

    override this.Dispose(disposing) =
        manager.TakeBack this

and ConnectionManager(connectionStr: string) =
    let all = ConcurrentStack<PooledConnection>()
    let pool = ConcurrentStack<PooledConnection>()

    member internal this.TakeBack(pooledConn: PooledConnection) =
        try
            pooledConn.Execute("ROLLBACK;") |> ignore
            failwithf "A transaction was not ended."
        with e ->
            let e = e
            reraise()
        pool.Push pooledConn

    member this.Borrow() =
        match pool.TryPop() with
        | true, c -> c
        | false, _ -> 
            let c = new PooledConnection(connectionStr, this)
            all.Push c
            c

    interface IDisposable with
        override this.Dispose() =
            for c in all do
                c.DisposeReal(true)
            all.Clear()
            ()