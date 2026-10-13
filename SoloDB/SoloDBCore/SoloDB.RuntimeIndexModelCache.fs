namespace SoloDatabase

open System
open System.Collections.Generic
open NativeSQLiteTools

/// Index facts shared only while their database or transaction owners remain live.
module internal RuntimeIndexModelCache =
    type private Bucket =
        { Snapshots: Dictionary<string, IndexModel.IndexModel>
          mutable Owners: int
          mutable Generation: int64 }

    let private gate = obj()
    let private buckets = Dictionary<string, Bucket>(StringComparer.Ordinal)

    let private isLive connectionString bucket =
        match buckets.TryGetValue connectionString with
        | true, current -> Object.ReferenceEquals(current, bucket)
        | _ -> false

    let private release connectionString bucket =
        bucket.Owners <- bucket.Owners - 1
        if bucket.Owners = 0 then
            bucket.Snapshots.Clear()
            bucket.Generation <- bucket.Generation + 1L
            if isLive connectionString bucket then buckets.Remove connectionString |> ignore

    /// The manager owns one lifetime; each outer transaction retains that same bucket.
    let acquireOwner connectionString =
        lock gate (fun () ->
            let bucket =
                match buckets.TryGetValue connectionString with
                | true, value -> value
                | _ ->
                    let value =
                        { Snapshots = Dictionary<string, IndexModel.IndexModel>(StringComparer.Ordinal)
                          Owners = 0; Generation = 0L }
                    buckets.Add(connectionString, value)
                    value
            bucket.Owners <- bucket.Owners + 1
            let mutable released = false
            let releaseOwner () =
                lock gate (fun () ->
                    if not released then
                        released <- true
                        release connectionString bucket)
            let beginTransaction () =
                lock gate (fun () ->
                    if released then raise (ObjectDisposedException("ConnectionManager"))
                    bucket.Owners <- bucket.Owners + 1
                    let generation = bucket.Generation
                    let mutable completed = false
                    { new IDisposable with
                        member _.Dispose() =
                            lock gate (fun () ->
                                if not completed then
                                    completed <- true
                                    // Another connection can load the old committed schema before commit.
                                    if generation <> bucket.Generation then
                                        bucket.Snapshots.Clear()
                                        bucket.Generation <- bucket.Generation + 1L
                                    release connectionString bucket) })
            struct (releaseOwner, beginTransaction))

    let private inTransaction (connection: NativeSqliteConnection) =
        match connection with
        | :? NativeCachingDbConnection as cached when cached.InsideTransaction -> true
        | _ -> SQLitePCL.raw.sqlite3_get_autocommit(connection.Handle) = 0

    let invalidate connectionString collectionName =
        lock gate (fun () ->
            match buckets.TryGetValue connectionString with
            | true, bucket ->
                bucket.Generation <- bucket.Generation + 1L
                bucket.Snapshots.Remove collectionName |> ignore
            | _ -> ())

    let private load (connection: NativeSqliteConnection) connectionString collectionName useSnapshot =
        let transactional = inTransaction connection
        let cached, publication =
            if transactional then None, None
            else
                lock gate (fun () ->
                    match buckets.TryGetValue connectionString with
                    | true, bucket ->
                        let cached =
                            if useSnapshot then
                                match bucket.Snapshots.TryGetValue collectionName with
                                | true, model -> Some model
                                | _ -> None
                            else None
                        cached, Some (bucket, bucket.Generation)
                    | _ -> None, None)
        match cached with
        | Some model -> model
        | None ->
            // SQLite and user callbacks never run under the metadata lock.
            let model = IndexModel.loadModelForTables connection [collectionName]
            match publication with
            | Some (bucket, generation) when not (inTransaction connection) ->
                lock gate (fun () ->
                    if isLive connectionString bucket && generation = bucket.Generation then
                        bucket.Snapshots.[collectionName] <- model)
            | _ -> ()
            model

    let loadAndStore connection connectionString collectionName =
        load connection connectionString collectionName false

    let loadModelForTables (connection: NativeSqliteConnection) (tableNames: seq<string>) =
        let get table = load connection connection.ConnectionString table true
        match tableNames |> Seq.distinct |> Seq.toList with
        | [] -> IndexModel.emptyModel
        | [table] -> get table
        | tables -> { IndexModel.Indexes = tables |> List.collect (fun table -> (get table).Indexes) }
