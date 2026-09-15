namespace SoloDatabase

open System.Collections.Concurrent
open Microsoft.Data.Sqlite
open SQLiteTools

/// One index metadata authority for collection operations and query translation.
module internal RuntimeIndexModelCache =
    let private snapshots = ConcurrentDictionary<struct (string * string), IndexModel.IndexModel>()

    let private inTransaction (connection: SqliteConnection) =
        match connection with
        | :? CachingDbConnection as cached -> cached.InsideTransaction
        | _ -> false

    let invalidate connectionString collectionName =
        snapshots.TryRemove(struct (connectionString, collectionName)) |> ignore

    let loadAndStore (connection: SqliteConnection) connectionString collectionName =
        let model = IndexModel.loadModelForTables connection [collectionName]
        // Uncommitted schema must never become another connection's cached metadata.
        if not (inTransaction connection) then
            snapshots.[struct (connectionString, collectionName)] <- model
        model

    let loadModelForTables (connection: SqliteConnection) (tableNames: seq<string>) =
        let connectionString = connection.ConnectionString
        let transactional = inTransaction connection
        let get table =
            match snapshots.TryGetValue(struct (connectionString, table)) with
            | true, model when not transactional -> model
            | _ -> loadAndStore connection connectionString table
        match tableNames |> Seq.distinct |> Seq.toList with
        | [] -> IndexModel.emptyModel
        | [table] -> get table
        | tables -> { IndexModel.Indexes = tables |> List.collect (fun table -> (get table).Indexes) }
