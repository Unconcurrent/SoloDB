namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open Connections
open SQLiteTools
open Utils

module internal SoloDBRootOps =
    let initializeCollection<'T>
        (checkDisposed: unit -> unit)
        (ddlLock: obj)
        (connectionManager: ConnectionManager)
        (connectionString: string)
        (clearCache: unit -> unit)
        (events: EventSystem)
        (name: string) =
        checkDisposed()
        if name.StartsWith "SoloDB" then raise (ArgumentException $"The SoloDB* prefix is forbidden in Collection names.")

        let existsAlready =
            use connection = connectionManager.Borrow()
            Helper.existsCollection name connection

        let hasRelations = RelationsSchema.getRelationSpecs typeof<'T> |> Array.isEmpty |> not

        if not existsAlready then
            lock ddlLock (fun () ->
                connectionManager.WithTransaction(fun connection ->
                    let shouldCreate = not (Helper.existsCollection name connection)
                    if shouldCreate then
                        Helper.createTableInner<'T> name connection

                    Helper.registerTypeCollection<'T> name connection

                    if hasRelations then
                        let relationTx: Relations.RelationTxContext = {
                            Connection = connection
                            OwnerTable = name
                            OwnerType = typeof<'T>
                            InTransaction = true
                        }
                        Relations.ensureSchemaForOwnerType relationTx typeof<'T>
                )
            )
        elif hasRelations then
            let needsEnsure =
                use conn = connectionManager.Borrow()
                RelationsSchema.relationSchemaRequiresEnsure conn name typeof<'T>
            if needsEnsure then
                lock ddlLock (fun () ->
                    connectionManager.WithTransaction(fun connection ->
                        Helper.registerTypeCollection<'T> name connection

                        let relationTx: Relations.RelationTxContext = {
                            Connection = connection
                            OwnerTable = name
                            OwnerType = typeof<'T>
                            InTransaction = true
                        }
                        Relations.ensureSchemaForOwnerType relationTx typeof<'T>
                    )
                )

        let collection = Collection<'T>(Pooled connectionManager, name, connectionString, { ClearCacheFunction = clearCache; EventSystem = events })
        use snapshotConnection = connectionManager.Borrow()
        collection.RefreshIndexModelSnapshot(snapshotConnection)
        collection :> ISoloDBCollection<'T>

    let withTransaction<'R>
        (checkDisposed: unit -> unit)
        (connectionManager: ConnectionManager)
        (events: EventSystem)
        (func: Func<TransactionalSoloDB, 'R>) =
        checkDisposed()
        use connectionForTransaction = connectionManager.CreateForTransaction()
        let mutable primaryEx: exn option = None
        let mutable cleanupEx: exn option = None
        let mutable result = Unchecked.defaultof<'R>
        try
            Connections.beginImmediateWithRetry connectionForTransaction
            let transactionalDb = new TransactionalSoloDB(connectionForTransaction, { ClearCacheFunction = ignore; EventSystem = events })

            try
                result <- func.Invoke transactionalDb
                match Connections.takeHandlerFaultCommitException connectionForTransaction with
                | Some ex ->
                    primaryEx <- Some ex
                    try connectionForTransaction.Execute "ROLLBACK;" |> ignore with rb -> cleanupEx <- Some rb
                | None ->
                    connectionForTransaction.Execute "COMMIT;" |> ignore
            with ex ->
                primaryEx <- Some ex
                try connectionForTransaction.Execute "ROLLBACK;" |> ignore with rb -> cleanupEx <- Some rb
        finally
            clearHandlerFault connectionForTransaction
            connectionForTransaction.DisposeReal(true)

        match Connections.resolveTxOutcome primaryEx cleanupEx with
        | Some ex -> raise ex
        | None -> result

    let withTransactionAsync<'R>
        (checkDisposed: unit -> unit)
        (connectionManager: ConnectionManager)
        (events: EventSystem)
        (func: Func<TransactionalSoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> = task {
        checkDisposed()
        use connectionForTransaction = connectionManager.CreateForTransaction()
        let mutable primaryEx: exn option = None
        let mutable cleanupEx: exn option = None
        let mutable result = Unchecked.defaultof<'R>
        try
            Connections.beginImmediateWithRetry connectionForTransaction
            let transactionalDb = new TransactionalSoloDB(connectionForTransaction, { ClearCacheFunction = ignore; EventSystem = events })

            try
                let! ret = func.Invoke transactionalDb
                result <- ret
                match Connections.takeHandlerFaultCommitException connectionForTransaction with
                | Some ex ->
                    primaryEx <- Some ex
                    try connectionForTransaction.Execute "ROLLBACK;" |> ignore with rb -> cleanupEx <- Some rb
                | None ->
                    connectionForTransaction.Execute "COMMIT;" |> ignore
            with ex ->
                primaryEx <- Some ex
                try connectionForTransaction.Execute "ROLLBACK;" |> ignore with rb -> cleanupEx <- Some rb
        finally
            clearHandlerFault connectionForTransaction
            connectionForTransaction.DisposeReal(true)

        match Connections.resolveTxOutcome primaryEx cleanupEx with
        | Some ex -> return reraiseAnywhere ex
        | None -> return result
    }
