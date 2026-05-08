namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open Connections
open SQLiteTools
open Utils

module internal SoloDBRootOps =

    /// Per-target heal body. Operates on a single typed-relation TARGET type. Probes the target
    /// collection for bug-shape SoloId rows; repairs them inside a BEGIN IMMEDIATE transaction.
    /// The string-only predicate is preserved here; per-type widening lands in a follow-up commit.
    let private healOneTargetTyped<'TTarget>
        (connectionManager: ConnectionManager)
        (targetTable: string) =
        match CustomTypeId<'TTarget>.Value with
        | None -> ()
        | Some custom ->
            let isStringSoloId = custom.Property.PropertyType = typeof<string>
            if not isStringSoloId then ()
            else
                let soloIdName = custom.Property.Name
                let qTable = sprintf "\"%s\"" (targetTable.Replace("\"", "\"\""))
                let badRowPredicate =
                    sprintf "jsonb_extract(Value, '$.%s') IS NULL OR jsonb_extract(Value, '$.%s') = ''"
                        soloIdName soloIdName
                let probeSql =
                    sprintf "SELECT Id FROM %s WHERE %s ORDER BY Id LIMIT 1;" qTable badRowPredicate

                let probeHit =
                    use conn = connectionManager.Borrow()
                    let row = conn.QueryFirstOrDefault<obj>(probeSql)
                    not (isNull row)

                if probeHit then
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
                                    let entity = json.ToObject(typeof<'TTarget>)
                                    let typedNull : ISoloDBCollection<'TTarget> = Unchecked.defaultof<_>
                                    CustomIdRunner.RunIfEmpty<'TTarget>(entity :?> 'TTarget, typedNull)
                                    match SoloIdAccessor.TryGetBoxedValue(typeof<'TTarget>, entity) with
                                    | ValueNone ->
                                        raise (InvalidOperationException(
                                            sprintf "Error: heal-repair could not produce a non-empty [<SoloId>] for row Id=%d in collection '%s'.\nReason: the registered IIdGenerator returned an empty value during heal.\nFix: ensure the generator's GenerateId returns a non-empty value." row.Id targetTable))
                                    | ValueSome _ -> ()
                                    let regenerated = JsonSerializator.JsonValue.Serialize(entity).ToJsonString()
                                    let updateSql =
                                        sprintf "UPDATE %s SET Value = jsonb(@v) WHERE Id = @id;" qTable
                                    conn.Execute(updateSql, {| v = regenerated; id = row.Id |}) |> ignore
                        )
                    with
                    | :? Microsoft.Data.Sqlite.SqliteException as ex when ex.SqliteErrorCode = 8 ->
                        raise (InvalidOperationException(
                            sprintf "Error: collection '%s' contains rows with missing [<SoloId>] (a known historical defect from earlier typed-DBRef cascade-insert) but the database was opened read-only.\nFix: reopen the database with read+write access; the next GetCollection call will repair the affected rows." targetTable, ex))

    /// Trampoline cache: target Type → closed MethodInfo for healOneTargetTyped<TargetType>.
    let private healOneTargetMiCache =
        System.Collections.Concurrent.ConcurrentDictionary<System.Type, System.Reflection.MethodInfo>()

    let private healOneTargetOpenMethod : System.Reflection.MethodInfo =
        let asm = typeof<RelationsTypes.RelationKind>.Assembly
        let m = asm.GetType("SoloDatabase.SoloDBRootOps")
        if isNull m then null
        else
            m.GetMethods(System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Static)
            |> Array.tryFind (fun mi -> mi.Name = "healOneTargetTyped" && mi.IsGenericMethodDefinition)
            |> Option.toObj

    let private healOneTarget (connectionManager: ConnectionManager) (targetTable: string) (targetType: System.Type) =
        if isNull healOneTargetOpenMethod then ()
        else
            let mi = healOneTargetMiCache.GetOrAdd(targetType, fun (t: System.Type) -> healOneTargetOpenMethod.MakeGenericMethod(t))
            try
                mi.Invoke(null, [| box connectionManager; box targetTable |]) |> ignore
            with
            | :? System.Reflection.TargetInvocationException as tie when not (isNull tie.InnerException) ->
                raise tie.InnerException

    /// Owner-side heal trigger. When a typed collection opens, walk every DBRef/DBRefMany
    /// property declared on the opened type via reflection and heal each property's target
    /// collection. The .NET model is the authority for "what does this type reference".
    let private healOwnerRelationTargets<'TOwner> (connectionManager: ConnectionManager) (ownerTable: string) =
        let specs = RelationsSchema.getRelationSpecs typeof<'TOwner>
        if specs.Length > 0 then
            use conn = connectionManager.Borrow()
            for spec in specs do
                let prop, _kind, targetType, _typedIdType, _onDelete, _onOwnerDelete, _isUnique, _orderBy = spec
                let targetTable = RelationsSchema.resolveTargetCollectionName conn ownerTable prop.Name targetType
                healOneTarget connectionManager targetTable targetType

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

        healOwnerRelationTargets<'T> connectionManager name

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
