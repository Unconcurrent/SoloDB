namespace SoloDatabase

open System
open System.Collections.Generic
open Connections
open NativeSQLiteTools

/// Non-recursive collection scaffold helper:
/// relation-tx bootstrap, relation delete checks, and index snapshot cache operations.
type internal CollectionScaffold<'T>(connection: NativeConnection, connectionString: string, name: string, hasRelations: bool, collectionFactory: NativeSqliteConnection -> RelationsTypes.IRelationCollectionFactory) =
    // NativeTransactional collections can serve several cascade calls on the same connection.
    let transactionalFactory =
        match connection with
        | NativeTransactional _
        | NativeGuarded (_, NativeTransactional _) when hasRelations ->
            Some (lazy (collectionFactory (connection.Get())))
        | _ -> None

    member _.RefreshIndexModelSnapshot(conn: NativeSqliteConnection) =
        RuntimeIndexModelCache.loadAndStore conn connectionString name |> ignore

    member _.InvalidateIndexModelSnapshot() =
        RuntimeIndexModelCache.invalidate connectionString name

    member _.GetIndexModelSnapshot() =
        use conn = connection.Get()
        RuntimeIndexModelCache.loadModelForTables conn [name]

    member _.MkRelationTx(conn: NativeSqliteConnection) : RelationsTypes.RelationTxContext = {
        Connection = conn
        OwnerTable = name
        OwnerType = typeof<'T>
        InTransaction = true
        Preparation = RelationsTypes.RelationPreparation()
        CollectionFactory =
            match transactionalFactory with
            | Some cached -> Some cached.Value
            | None -> Some (collectionFactory conn)
    }

    static member MkRelationPathSets() =
        HashSet<string>(StringComparer.Ordinal),
        HashSet<string>(StringComparer.Ordinal)

    member this.EnsureRelationTx(conn: NativeSqliteConnection) : RelationsTypes.RelationTxContext =
        let tx = this.MkRelationTx conn
        RelationsCore.ensureSchemaForOwnerType tx typeof<'T>
        this.InvalidateIndexModelSnapshot()
        tx

    member this.TryEnsureRelationTx(conn: NativeSqliteConnection) : RelationsTypes.RelationTxContext voption =
        if hasRelations then
            ValueSome (this.EnsureRelationTx conn)
        else
            ValueNone

    member _.HasIncomingRelations(connection: NativeSqliteConnection) =
        let mutable catalog = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'SoloDBRelation')")
        let relationCatalogExists =
            try
                if not (catalog.Step()) then raise (InvalidOperationException("Relation catalog existence was not returned."))
                let exists = catalog.Row.GetBoolean(0)
                catalog.Dispose()
                exists
            with error -> catalog.Abort(error); reraise()
        if not relationCatalogExists then false
        else
            let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM SoloDBRelation WHERE TargetCollection = @name LIMIT 1)")
            try
                statement.BindText(1, name)
                if not (statement.Step()) then raise (InvalidOperationException("Incoming relation existence was not returned."))
                let exists = statement.Row.GetBoolean(0)
                statement.Dispose()
                exists
            with error -> statement.Abort(error); reraise()

    member this.RequiresRelationDeleteHandling(connection: NativeSqliteConnection) =
        hasRelations || this.HasIncomingRelations(connection)
