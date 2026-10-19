namespace SoloDatabase

open System
open Connections
open NativeSQLiteTools
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
            use connection = connectionManager.BorrowNative()
            Helper.existsCollection name connection

        if existsAlready then
            let needsMigration,needsIndexes =
                use connection=connectionManager.BorrowNative()
                (NumericMigration.sample connection name typeof<'T> |> NumericMigration.legacy),
                HelperSchema.unsignedIndexesRequireEnsure<'T> name connection
            if needsMigration || needsIndexes then
                lock ddlLock (fun () ->
                    connectionManager.WithTransaction(fun connection ->
                        if NumericMigration.sample connection name typeof<'T> |> NumericMigration.legacy then
                            NumericMigration.migrate connection name typeof<'T>
                        if needsIndexes then HelperSchema.ensureUnsignedIndexes<'T> name connection))

        let hasRelations = RelationsSchemaValidator.hasReferenceProperties typeof<'T>

        if not existsAlready then
            lock ddlLock (fun () ->
                connectionManager.WithTransaction(fun connection ->
                    let shouldCreate = not (Helper.existsCollection name connection)
                    if shouldCreate then
                        Helper.createTableInner<'T> name connection
                    else
                        if NumericMigration.sample connection name typeof<'T> |> NumericMigration.legacy then
                            NumericMigration.migrate connection name typeof<'T>
                        HelperSchema.ensureUnsignedIndexes<'T> name connection

                    Helper.registerTypeCollection<'T> name connection

                    if hasRelations then
                        let relationTx: RelationsTypes.RelationTxContext = {
                            Connection = connection
                            OwnerTable = name
                            OwnerType = typeof<'T>
                            InTransaction = true
                            Preparation = RelationsTypes.RelationPreparation()
                            CollectionFactory = None
                        }
                        RelationsCore.ensureSchemaForOwnerType relationTx typeof<'T>
                )
            )
        elif hasRelations then
            let needsEnsure =
                use conn = connectionManager.BorrowNative()
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
                            Preparation = RelationsTypes.RelationPreparation()
                            CollectionFactory = None
                        }
                        RelationsCore.ensureSchemaForOwnerType relationTx typeof<'T>
                    )
                )

        let collection = Collection<'T>(NativePooled connectionManager, name, connectionString, { ClearCacheFunction = clearCache; EventSystem = events })

        use snapshotConnection = connectionManager.BorrowNative()
        collection.RefreshIndexModelSnapshot(snapshotConnection)
        collection :> ISoloDBCollection<'T>

    /// Runs a user transaction on a dedicated caching connection. BEGIN IMMEDIATE, handler-fault
    /// interception, commit/rollback, exception resolution and connection destruction belong to
    /// ConnectionManager.WithDedicatedTransaction; this function builds the transactional context.
    let withTransaction<'R>
        (checkDisposed: unit -> unit)
        (connectionManager: ConnectionManager)
        (events: EventSystem)
        (func: Func<TransactionalSoloDB, 'R>) =
        checkDisposed()
        connectionManager.WithDedicatedTransaction(fun connectionForTransaction ->
            let transactionalDb = new TransactionalSoloDB(connectionForTransaction, { ClearCacheFunction = ignore; EventSystem = events })
            // The dedicated connection is destroyed after this scope, so a context leaked out of the
            // callback must fail closed instead of writing through a connection it no longer owns.
            try func.Invoke transactionalDb
            finally transactionalDb.ExitScope())

    /// Async counterpart of withTransaction. Delegates the transaction lifecycle to
    /// ConnectionManager.WithDedicatedAsyncTransaction, including connection destruction.
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
