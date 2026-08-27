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

        let hasRelations = RelationsSchemaValidator.getRelationSpecs typeof<'T> |> Array.isEmpty |> not

        if not existsAlready then
            lock ddlLock (fun () ->
                connectionManager.WithTransaction(fun connection ->
                    let shouldCreate = not (Helper.existsCollection name connection)
                    if shouldCreate then
                        Helper.createTableInner<'T> name connection

                    Helper.registerTypeCollection<'T> name connection

                    if hasRelations then
                        let relationTx: RelationsTypes.RelationTxContext = {
                            Connection = connection
                            OwnerTable = name
                            OwnerType = typeof<'T>
                            InTransaction = true
                        }
                        RelationsCore.ensureSchemaForOwnerType relationTx typeof<'T>
                )
            )
        elif hasRelations then
            let needsEnsure =
                use conn = connectionManager.Borrow()
                RelationsSchemaLinkTableDDL.relationSchemaRequiresEnsure conn name typeof<'T>
            if needsEnsure then
                lock ddlLock (fun () ->
                    connectionManager.WithTransaction(fun connection ->
                        Helper.registerTypeCollection<'T> name connection

                        let relationTx: RelationsTypes.RelationTxContext = {
                            Connection = connection
                            OwnerTable = name
                            OwnerType = typeof<'T>
                            InTransaction = true
                        }
                        RelationsCore.ensureSchemaForOwnerType relationTx typeof<'T>
                    )
                )

        let collection = Collection<'T>(Pooled connectionManager, name, connectionString, { ClearCacheFunction = clearCache; EventSystem = events })

        use snapshotConnection = connectionManager.Borrow()
        collection.RefreshIndexModelSnapshot(snapshotConnection)
        collection :> ISoloDBCollection<'T>

    /// Runs a user transaction on a pooled caching connection. The BEGIN IMMEDIATE, handler-fault
    /// interception, commit/rollback, exception resolution and return-to-pool sequence is owned by
    /// ConnectionManager.WithTransaction; this function only builds the transactional context.
    let withTransaction<'R>
        (checkDisposed: unit -> unit)
        (connectionManager: ConnectionManager)
        (events: EventSystem)
        (func: Func<TransactionalSoloDB, 'R>) =
        checkDisposed()
        connectionManager.WithDedicatedTransaction(fun connectionForTransaction ->
            let transactionalDb = new TransactionalSoloDB(connectionForTransaction, { ClearCacheFunction = ignore; EventSystem = events })
            // The connection returns to the pool after this scope, so a context leaked out of the
            // callback must fail closed instead of writing through a connection it no longer owns.
            try func.Invoke transactionalDb
            finally transactionalDb.ExitScope())

    /// Async counterpart of withTransaction. Delegates the transaction lifecycle to
    /// ConnectionManager.WithAsyncTransaction so both paths share one corridor.
    let withTransactionAsync<'R>
        (checkDisposed: unit -> unit)
        (connectionManager: ConnectionManager)
        (events: EventSystem)
        (func: Func<TransactionalSoloDB, Threading.Tasks.Task<'R>>) : Threading.Tasks.Task<'R> =
        checkDisposed()
        connectionManager.WithDedicatedAsyncTransaction(fun connectionForTransaction ->
            let transactionalDb = new TransactionalSoloDB(connectionForTransaction, { ClearCacheFunction = ignore; EventSystem = events })
            task {
                try return! func.Invoke transactionalDb
                finally transactionalDb.ExitScope()
            })
