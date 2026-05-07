namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open Connections
open SQLiteTools
open Utils

module internal SoloDBRootOps =

    /// Repairs target rows whose [<SoloId>] field is null/empty in the persisted JSON Value column —
    /// the historical state left by SoloDB cascade-insert before the IIdGenerator was wired into
    /// `insertTargetEntity`. One indexed probe per collection-open; if no bug, return; if bug found,
    /// fix all matching rows in one BEGIN IMMEDIATE transaction so a process death is a clean ROLLBACK.
    ///
    /// Probe leverages the unique index on jsonb_extract(Value, '$.<SoloId>') that typed relations
    /// require (RelationsSchemaLinkTableDDL). On healthy collections the probe is a single index seek
    /// that returns nothing — no UPDATEs, no cache, no full scan. The probe runs every GetCollection
    /// because the index makes it cheap; if it ever stops being cheap we should fix the index, not
    /// add a memo on top.
    ///
    /// Auto-heal is restricted to STRING SoloIds because their bug shape (NULL or empty) is
    /// unambiguous. Value-type SoloIds (int64 0L, Guid.Empty, default struct) cannot be safely
    /// distinguished from legitimate user-chosen ids and are therefore left alone — users who hit
    /// the value-type-default bug must regenerate manually.
    /// True iff the collection is currently registered as a TARGET of any typed-DBRef relation.
    /// Heal is gated on this: the historical cascade-insert-skipped-IIdGenerator bug only affects
    /// rows of typed-relation target collections, AND the unique index on jsonb_extract(Value, '$.<SoloId>')
    /// (created by RelationsSchemaLinkTableDDL) only exists for those collections — without it, the
    /// probe would scan every row of every collection on every GetCollection call. Gating restricts
    /// the probe to the subset where (a) the bug can exist and (b) the index makes the probe an
    /// index seek instead of a scan.
    let private isTypedRelationTarget (connectionManager: ConnectionManager) (collectionName: string) : bool =
        try
            use conn = connectionManager.Borrow()
            let exists =
                conn.QueryFirst<int64>(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM SoloDBRelation WHERE TargetCollection = @t) THEN 1 ELSE 0 END;",
                    {| t = collectionName |})
            exists = 1L
        with
        | _ -> false  // Catalog absent (very early bootstrap) → not a relation target by definition.

    let private healCustomIdRows<'T>
        (connectionManager: ConnectionManager)
        (name: string)
        (collection: ISoloDBCollection<'T>) =
        match CustomTypeId<'T>.Value with
        | None -> ()
        | Some custom ->
            let isStringSoloId = custom.Property.PropertyType = typeof<string>
            if not isStringSoloId then () // Value-type SoloIds are not auto-repaired; ambiguous between bug and intent.
            elif not (isTypedRelationTarget connectionManager name) then ()
            else
                let soloIdName = custom.Property.Name
                let qTable = sprintf "\"%s\"" (name.Replace("\"", "\"\""))
                // Bug shape for string SoloId: NULL or empty in the persisted JSON. Whitespace-only
                // is left to user-led repair — adding `trim(...)` here would defeat the index.
                let badRowPredicate =
                    sprintf "jsonb_extract(Value, '$.%s') IS NULL OR jsonb_extract(Value, '$.%s') = ''"
                        soloIdName soloIdName
                let probeSql =
                    sprintf "SELECT Id FROM %s WHERE %s ORDER BY Id LIMIT 1;" qTable badRowPredicate

                // Stage 1: cheap read probe on a borrowed connection — NO transaction. Healthy
                // collections (no bad rows) cost exactly one indexed SELECT and return immediately,
                // never touching BEGIN IMMEDIATE. Read-only DBs that are healthy never throw.
                let probeHit =
                    use conn = connectionManager.Borrow()
                    let row = conn.QueryFirstOrDefault<obj>(probeSql)
                    not (isNull row)

                if probeHit then
                    // Stage 2: bug confirmed by the read probe → escalate to write transaction.
                    // Re-probe under the write lock because another process may have repaired the
                    // rows in the window between our read probe and acquiring BEGIN IMMEDIATE
                    // (concurrency safety). Read-only DBs throw with the actionable message via
                    // SQLITE_READONLY (code 8) — stable across driver/SQLite-version message
                    // changes — only when an actual bug demands repair.
                    try
                        connectionManager.WithTransaction(fun conn ->
                            let reProbe = conn.QueryFirstOrDefault<obj>(probeSql)
                            if isNull reProbe then ()
                            else
                                let selectAllSql =
                                    sprintf "SELECT Id, json(Value) AS ValueJson FROM %s WHERE %s;" qTable badRowPredicate
                                let rows =
                                    conn.Query<{| Id: int64; ValueJson: string |}>(selectAllSql)
                                    |> Seq.toArray
                                for row in rows do
                                    let json = JsonSerializator.JsonValue.Parse row.ValueJson
                                    let entity = json.ToObject(typeof<'T>)
                                    // Heal context has the typed collection (we are inside
                                    // GetCollection<'T>'s body), so the shared primitive drives both
                                    // non-generic IIdGenerator and generic IIdGenerator<'T>.
                                    CustomIdRunner.Run<'T>(entity :?> 'T, collection)
                                    match SoloIdAccessor.TryGetBoxedValue(typeof<'T>, entity) with
                                    | ValueNone ->
                                        raise (InvalidOperationException(
                                            sprintf "Error: heal-repair could not produce a non-empty [<SoloId>] for row Id=%d in collection '%s'.\nReason: the registered IIdGenerator returned an empty value during heal.\nFix: ensure the generator's GenerateId returns a non-empty value." row.Id name))
                                    | ValueSome _ -> ()
                                    let regenerated = JsonSerializator.JsonValue.Serialize(entity).ToJsonString()
                                    let updateSql =
                                        sprintf "UPDATE %s SET Value = jsonb(@v) WHERE Id = @id;" qTable
                                    conn.Execute(updateSql, {| v = regenerated; id = row.Id |}) |> ignore
                        )
                    with
                    | :? Microsoft.Data.Sqlite.SqliteException as ex when ex.SqliteErrorCode = 8 ->
                        raise (InvalidOperationException(
                            sprintf "Error: collection '%s' contains rows with missing [<SoloId>] (a known historical defect from earlier typed-DBRef cascade-insert) but the database was opened read-only.\nFix: reopen the database with read+write access; the next GetCollection call will repair the affected rows. The repair runs once and is a no-op on subsequent opens." name, ex))

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

        // Heal historical NULL/empty-SoloId rows produced by pre-fix cascade-insert (string SoloIds
        // only). One indexed probe per GetCollection<'T>(); if no bug, no further work. If bug, all
        // affected rows are repaired in one transaction. The just-constructed typed collection is
        // threaded through so the shared generator/writeback primitive can drive generic
        // IIdGenerator<'T> registrations (e.g., CountryCodeGenerator) — they are not unhealable.
        healCustomIdRows<'T> connectionManager name (collection :> ISoloDBCollection<'T>)

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
