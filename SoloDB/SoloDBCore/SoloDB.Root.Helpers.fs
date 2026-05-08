namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open Connections
open SQLiteTools
open Utils

module internal SoloDBRootOps =

    /// Cache of (idType -> bug-shape SQL predicate fragment). Predicate is computed once per
    /// registered SoloId .NET type by serializing default<idType> through the JSON serializer
    /// and translating the resulting JsonValue into the SQLite-native value form.
    let private bugShapePredicateCache =
        System.Collections.Concurrent.ConcurrentDictionary<System.Type, string -> string>()

    let private serializeMethodOpen =
        typeof<JsonSerializator.JsonValue>.GetMethod("Serialize")

    let private buildBugShapePredicate (idType: System.Type) : string -> string =
        bugShapePredicateCache.GetOrAdd(idType, System.Func<System.Type, string -> string>(fun (t: System.Type) ->
            let defaultValue : obj = if t.IsValueType then Activator.CreateInstance(t) else null
            let serializeClosed = serializeMethodOpen.MakeGenericMethod(t)
            let jsonValue = serializeClosed.Invoke(null, [| defaultValue |]) :?> JsonSerializator.JsonValue
            let basePredicate (jsonPath: string) =
                match jsonValue with
                | JsonSerializator.JsonValue.Null ->
                    sprintf "jsonb_extract(Value, '%s') IS NULL" jsonPath
                | JsonSerializator.JsonValue.String s ->
                    let escaped = s.Replace("'", "''")
                    sprintf "jsonb_extract(Value, '%s') = '%s'" jsonPath escaped
                | JsonSerializator.JsonValue.Number n ->
                    sprintf "jsonb_extract(Value, '%s') = %s" jsonPath
                        (n.ToString(System.Globalization.CultureInfo.InvariantCulture))
                | JsonSerializator.JsonValue.Boolean b ->
                    sprintf "jsonb_extract(Value, '%s') = %d" jsonPath (if b then 1 else 0)
                | other ->
                    let jsonText = other.ToJsonString().Replace("'", "''")
                    sprintf "json(jsonb_extract(Value, '%s')) = '%s'" jsonPath jsonText
            if t = typeof<string> then
                fun jsonPath ->
                    sprintf "(%s OR jsonb_extract(Value, '%s') = '')" (basePredicate jsonPath) jsonPath
            else
                basePredicate))

    /// Serializes an arbitrary boxed value of `idType` to its JSON wire literal. Used to compute
    /// the post-regeneration SoloId literal that goes into the surgical jsonb_set update.
    let private serializeValueByType (idType: System.Type) (value: obj) : string =
        let serializeClosed = serializeMethodOpen.MakeGenericMethod(idType)
        let jsonValue = serializeClosed.Invoke(null, [| value |]) :?> JsonSerializator.JsonValue
        jsonValue.ToJsonString()

    /// Per-target heal body. Operates on a single typed-relation TARGET type. Probes the target
    /// collection for rows whose persisted [<SoloId>] equals the bug shape for that type;
    /// repairs each matching row inside a BEGIN IMMEDIATE transaction with the SoloDB-internal
    /// triggers dropped and recreated around the heal UPDATE batch.
    ///
    /// Predicate is per-type empirical: serialize default<idType> via the JSON serializer and
    /// translate to a SQLite-native equality fragment. String SoloIds widen to also catch
    /// empty-string seeds that user generators commonly classify as empty.
    ///
    /// Heal write is a surgical jsonb_set on the SoloId path — the rest of the entity's JSON
    /// bytes are preserved as stored, including any properties not present in the .NET model.
    let private healOneTargetTyped<'TTarget>
        (connectionManager: ConnectionManager)
        (targetTable: string) =
        match CustomTypeId<'TTarget>.Value with
        | None -> ()
        | Some custom ->
            let idType = custom.Property.PropertyType
            let soloIdName = custom.Property.Name
            let soloIdJsonPath = "$." + soloIdName
            let qTable = sprintf "\"%s\"" (targetTable.Replace("\"", "\"\""))
            let predicateFragment = buildBugShapePredicate idType soloIdJsonPath
            let probeSql =
                sprintf "SELECT Id FROM %s WHERE %s ORDER BY Id LIMIT 1;" qTable predicateFragment

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
                            // Capture EVERY trigger on this table — SoloDB-internal AND any
                            // user-registered triggers — drop them all, run heal UPDATEs, then
                            // recreate them from the captured SQL. SQLite transactional DDL
                            // ensures process death rolls back both the DROP and the heal
                            // updates, restoring the table+triggers to their pre-heal state.
                            let triggerRows =
                                conn.Query<{| Name: string; Sql: string |}>(
                                    "SELECT name AS Name, sql AS Sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = @t AND sql IS NOT NULL;",
                                    {| t = targetTable |})
                                |> Seq.toArray
                            for trig in triggerRows do
                                conn.Execute(sprintf "DROP TRIGGER IF EXISTS \"%s\";" (trig.Name.Replace("\"", "\"\""))) |> ignore
                            let selectAllSql =
                                sprintf "SELECT Id, json(Value) AS ValueJson FROM %s WHERE %s;" qTable predicateFragment
                            let rows =
                                conn.Query<{| Id: int64; ValueJson: string |}>(selectAllSql)
                                |> Seq.toArray
                            for row in rows do
                                let json = JsonSerializator.JsonValue.Parse row.ValueJson
                                let entity = json.ToObject(typeof<'TTarget>)
                                let typedNull : ISoloDBCollection<'TTarget> = Unchecked.defaultof<_>
                                CustomIdRunner.ForceRegenerate<'TTarget>(entity :?> 'TTarget, typedNull)
                                let regeneratedId = custom.GetId(entity)
                                if isNull regeneratedId then
                                    raise (InvalidOperationException(
                                        sprintf "Error: heal-repair could not produce a non-empty [<SoloId>] for row Id=%d in collection '%s'.\nReason: the registered IIdGenerator returned a null value during heal.\nFix: ensure the generator's GenerateId returns a non-null value." row.Id targetTable))
                                let newSoloIdLiteral = serializeValueByType idType regeneratedId
                                let updateSql =
                                    sprintf "UPDATE %s SET Value = jsonb_set(Value, '%s', jsonb(@v)) WHERE Id = @id;" qTable soloIdJsonPath
                                conn.Execute(updateSql, {| v = newSoloIdLiteral; id = row.Id |}) |> ignore
                            for trig in triggerRows do
                                conn.Execute(trig.Sql) |> ignore
                    )
                with
                | :? Microsoft.Data.Sqlite.SqliteException as ex when ex.SqliteErrorCode = 8 ->
                    raise (InvalidOperationException(
                        sprintf "Error: collection '%s' contains rows with a bug-shape [<SoloId>] but the database was opened read-only.\nFix: reopen the database with read+write access; the next collection open will repair the affected rows." targetTable, ex))

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
