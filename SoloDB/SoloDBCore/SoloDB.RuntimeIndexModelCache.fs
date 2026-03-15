namespace SoloDatabase

open System.Collections.Concurrent
open Microsoft.Data.Sqlite

module internal RuntimeIndexModelCache =
    let private snapshots = ConcurrentDictionary<string, SoloDatabase.IndexModel.IndexModel>()

    let private mkKey (connectionString: string) (collectionName: string) =
        $"{connectionString}\u001F{collectionName}"

    let tryGet (connectionString: string) (collectionName: string) =
        match snapshots.TryGetValue(mkKey connectionString collectionName) with
        | true, model -> Some model
        | _ -> None

    let invalidate (connectionString: string) (collectionName: string) =
        snapshots.TryRemove(mkKey connectionString collectionName) |> ignore

    let loadAndStore (connection: SqliteConnection) (connectionString: string) (collectionName: string) =
        let model = SoloDatabase.IndexModel.loadModelForTables connection [collectionName]
        snapshots.[mkKey connectionString collectionName] <- model
        model
