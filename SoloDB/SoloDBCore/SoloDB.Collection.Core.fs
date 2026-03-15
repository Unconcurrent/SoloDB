namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open System.Collections.Generic
open Connections
open SQLiteTools

/// Non-recursive collection scaffold helper:
/// relation-tx bootstrap, relation delete checks, and index snapshot cache operations.
type internal CollectionScaffold<'T>(connection: Connection, connectionString: string, name: string, hasRelations: bool) =

    member _.RefreshIndexModelSnapshot(conn: SqliteConnection) =
        RuntimeIndexModelCache.loadAndStore conn connectionString name |> ignore

    member _.InvalidateIndexModelSnapshot() =
        RuntimeIndexModelCache.invalidate connectionString name

    member _.GetIndexModelSnapshot() =
        match RuntimeIndexModelCache.tryGet connectionString name with
        | Some model -> model
        | None ->
            use conn = connection.Get()
            RuntimeIndexModelCache.loadAndStore conn connectionString name

    member _.MkRelationTx(conn: SqliteConnection) : Relations.RelationTxContext = {
        Connection = conn
        OwnerTable = name
        OwnerType = typeof<'T>
        InTransaction = true
    }

    static member MkRelationPathSets() =
        HashSet<string>(StringComparer.Ordinal),
        HashSet<string>(StringComparer.Ordinal)

    member this.EnsureRelationTx(conn: SqliteConnection) : Relations.RelationTxContext =
        let tx = this.MkRelationTx conn
        Relations.ensureSchemaForOwnerType tx typeof<'T>
        this.InvalidateIndexModelSnapshot()
        tx

    member this.TryEnsureRelationTx(conn: SqliteConnection) : Relations.RelationTxContext voption =
        if hasRelations then
            ValueSome (this.EnsureRelationTx conn)
        else
            ValueNone

    member _.HasIncomingRelations(connection: SqliteConnection) =
        let relationCatalogExists =
            connection.QueryFirst<int64>(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'SoloDBRelation') THEN 1 ELSE 0 END") = 1L

        relationCatalogExists &&
            connection.QueryFirst<int64>(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM SoloDBRelation WHERE TargetCollection = @name LIMIT 1) THEN 1 ELSE 0 END",
                {| name = name |}) = 1L

    member this.RequiresRelationDeleteHandling(connection: SqliteConnection) =
        hasRelations || this.HasIncomingRelations(connection)
