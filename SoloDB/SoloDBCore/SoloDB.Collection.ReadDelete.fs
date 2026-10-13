namespace SoloDatabase

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open SoloDatabase.Types
open JsonFunctions
open Utils
open NativeSQLiteTools
open SoloDatabase
open SoloDatabase.SqlModel

[<AutoOpen>]
module internal CollectionReadDeletePrivate =
    let inline genericReinterpret<'A, 'B> (a: 'A) = Unsafe.As<'A, 'B>(&Unsafe.AsRef(&a))

    let inline int64FromUInt64OrThrow (idArgName: string) (value: uint64) =
        if value > uint64 Int64.MaxValue then
            raise (ArgumentOutOfRangeException(idArgName, "Identifier is out of range for Int64."))
        int64 value

    let readDocumentById (connection: NativeSqliteConnection) name id =
        let mutable statement = connection.OpenStatement($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1")
        try
            statement.BindInt64(1, id)
            let row = if statement.Step() then SqlRowValue.Document(statement.Row) else Unchecked.defaultof<_>
            statement.Dispose()
            row
        with error -> statement.Abort(error); reraise()

    let deleteDocumentById (connection: NativeSqliteConnection) name id =
        let mutable statement = connection.OpenStatement($"DELETE FROM \"{name}\" WHERE Id = @id")
        try
            statement.BindInt64(1, id)
            statement.Step() |> ignore
            let affected = statement.RecordsAffected
            statement.Dispose()
            affected
        with error -> statement.Abort(error); reraise()

    let readHydratedDocument (connection: NativeSqliteConnection) sql (variables: Dictionary<string, obj>) =
        let mutable statement = connection.OpenStatement(sql)
        try
            statement.BindValues(variables)
            let row = if statement.Step() then SqlRowValue.Document(statement.Row) else Unchecked.defaultof<_>
            statement.Dispose()
            row
        with error -> statement.Abort(error); reraise()

type internal CollectionReadDeleteOps<'T>() =

    static member TryGetByIdInt64
        (id: int64)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> NativeSqliteConnection)
        (hydrateRelations: NativeSqliteConnection -> int64 -> obj -> unit) =

        use connection = getConnection()
        try
            if hasRelations && HydrationSqlMetadata.hasRelationProperties typeof<'T> then
                RelationsCore.withRelationSqliteWrap "read" "TryGetById.hydrated" (fun () ->
                    // Single-SQL hydration for non-queryable path.
                    let vars = Dictionary<string, obj>()
                    vars.["id"] <- box id
                    let whereExpr =
                        SqlExpr.Binary(
                            SqlExpr.Column(Some "o", "Id"),
                            BinaryOperator.Eq,
                            SqlExpr.Parameter "id")
                    let sql, singleHydrated, manyHydrated =
                        HydrationSqlBuilder.buildHydratedGetByIdSql connection name typeof<'T> whereExpr vars true
                    match readHydratedDocument connection sql vars with
                    | json when Object.ReferenceEquals(json, null) -> None
                    | json ->
                        let entity = fromSQLite<'T> json
                        // Populate DBRefMany trackers from HydrationJSON.
                        if manyHydrated && not (isNull json.HydrationJSON) then
                            let hydMap = Dictionary<int64, string>()
                            hydMap.[json.Id.Value] <- json.HydrationJSON
                            HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (json.Id.Value, box entity) |] hydMap
                        if singleHydrated || manyHydrated then
                            let excludedPaths, includedPaths = HashSet<string>(), HashSet<string>()
                            RelationsSync.recurseLoadedRelationTargets connection name typeof<'T> excludedPaths includedPaths false [| (json.Id.Value, box entity) |] false
                        if (RelationsSchemaValidator.getFileSystemReferenceSpecs typeof<'T>).Length > 0 then
                            let paths = HashSet<string>()
                            if not singleHydrated then
                                RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection name typeof<'T> paths paths false [| (json.Id.Value, box entity) |] RelationKind.Single ""
                            RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection name typeof<'T> paths paths false [| (json.Id.Value, box entity) |] RelationKind.Many ""
                        Some entity)
            else
                match readDocumentById connection name id with
                | json when Object.ReferenceEquals(json, null) -> None
                | json ->
                    let entity = fromSQLite<'T> json
                    if hasRelations then
                        hydrateRelations connection id (box entity)
                    Some entity
        with error ->
            SQLiteToolsHandlerFaultState.tryRecordHandlerFault connection error
            reraise()

    static member RequireByIdInt64(id: int64, name: string, value: 'T option) =
        match value with
        | None -> raise (KeyNotFoundException (sprintf "There is no element with id '%i' inside collection '%s'" id name))
        | Some x -> x

    static member DeleteByIdInt64
        (id: int64)
        (name: string)
        (withTransaction: (NativeSqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: NativeSqliteConnection -> bool)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (syncDeleteOwner: RelationsTypes.RelationTxContext -> int64 -> obj -> unit) =

        withTransaction (fun conn ->
            let requiresRelationHandling = requiresRelationDeleteHandling conn
            if requiresRelationHandling then
                let tx = ensureRelationTx conn
                let oldRow = readDocumentById conn name id
                if isNull oldRow then
                    0
                else
                    let owner = fromSQLite<'T> oldRow
                    syncDeleteOwner tx id (box owner)
                    deleteDocumentById conn name id
            else
                deleteDocumentById conn name id
        )

    static member TryGetByCustomId<'IdType when 'IdType : equality>
        (id: 'IdType)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> NativeSqliteConnection)
        (hydrateRelations: NativeSqliteConnection -> int64 -> obj -> unit) : 'T option =

        let custom = CustomTypeId<'T>.Get()
        let idProp =
            match custom with
            | Some c -> c.Member
            | None -> raise (InvalidOperationException("This collection has no custom [Id] property. Use the Int64 Id overload."))

        use connection = getConnection()
        try
            if hasRelations && HydrationSqlMetadata.hasRelationProperties typeof<'T> then
                RelationsCore.withRelationSqliteWrap "read" "TryGetByCustomId.hydrated" (fun () ->
                    // Single-SQL hydration for custom-id non-queryable path.
                    // Build WHERE as SqlExpr DU (not raw string splice) to keep query in the DU tree.
                    // Custom-id filter is: jsonb_extract(Value, '$.PropName') = @param
                    let vars = Dictionary<string, obj>()
                    let idParameter =
                        QueryableHelperBase.allocateNamedStoredValueParam
                            vars "_cid0" idProp.IdType (box id)
                    let whereExpr =
                        SqlExpr.Binary(
                            StoredValueParameter.declaredExpression idProp.IdType
                                (SqlExpr.JsonExtractExpr(Some "o", "Value", SqlModel.JsonPathOps.ofList [idProp.Name])),
                            BinaryOperator.Eq,
                            idParameter)
                    let sql, singleHydrated, manyHydrated =
                        HydrationSqlBuilder.buildHydratedGetByIdSql connection name typeof<'T> whereExpr vars true
                    match readHydratedDocument connection sql vars with
                    | json when Object.ReferenceEquals(json, null) -> None
                    | json ->
                        let entity = fromSQLite<'T> json
                        if manyHydrated && not (isNull json.HydrationJSON) then
                            let hydMap = Dictionary<int64, string>()
                            hydMap.[json.Id.Value] <- json.HydrationJSON
                            HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (json.Id.Value, box entity) |] hydMap
                        if singleHydrated || manyHydrated then
                            let excludedPaths, includedPaths = HashSet<string>(), HashSet<string>()
                            RelationsSync.recurseLoadedRelationTargets connection name typeof<'T> excludedPaths includedPaths false [| (json.Id.Value, box entity) |] false
                        if (RelationsSchemaValidator.getFileSystemReferenceSpecs typeof<'T>).Length > 0 then
                            let paths = HashSet<string>()
                            if not singleHydrated then
                                RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection name typeof<'T> paths paths false [| (json.Id.Value, box entity) |] RelationKind.Single ""
                            RelationsFileSystemBatchLoad.batchLoadFileSystemProperties connection name typeof<'T> paths paths false [| (json.Id.Value, box entity) |] RelationKind.Many ""
                        Some entity)
            else
                // DU-built SELECT for custom-id no-relations fallback.
                let vars = Dictionary<string, obj>()
                let idParameter =
                    QueryableHelperBase.allocateNamedStoredValueParam
                        vars "_cid0" idProp.IdType (box id)
                let whereExpr =
                    SqlExpr.Binary(
                        StoredValueParameter.declaredExpression idProp.IdType
                            (SqlExpr.JsonExtractExpr(Some "o", "Value", SqlModel.JsonPathOps.ofList [idProp.Name])),
                        BinaryOperator.Eq,
                        idParameter)
                let sql, _ =
                    HydrationSqlBuilder.buildManyOnlyHydratedSql connection name typeof<'T> whereExpr vars true
                match readHydratedDocument connection sql vars with
                | json when Object.ReferenceEquals(json, null) -> None
                | json ->
                    let entity = fromSQLite<'T> json
                    Some entity
        with error ->
            SQLiteToolsHandlerFaultState.tryRecordHandlerFault connection error
            reraise()

    static member DeleteByCustomId<'IdType when 'IdType : equality>
        (id: 'IdType)
        (name: string)
        (withTransaction: (NativeSqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: NativeSqliteConnection -> bool)
        (ensureRelationTx: NativeSqliteConnection -> RelationsTypes.RelationTxContext)
        (syncDeleteOwner: RelationsTypes.RelationTxContext -> int64 -> obj -> unit) =

        let custom = CustomTypeId<'T>.Get()
        let idProp =
            match custom with
            | Some c -> c.Member
            | None -> raise (InvalidOperationException("This collection has no custom [Id] property. Use the Int64 Id overload."))

        let variables = Dictionary<string, obj>()
        let idParameter =
            QueryableHelperBase.allocateNamedStoredValueParam
                variables "_cid0" idProp.IdType (box id)
        let filterExpr =
            SqlExpr.Binary(
                StoredValueParameter.declaredExpression idProp.IdType
                    (SqlExpr.JsonExtractExpr(None, "Value", SqlModel.JsonPathOps.ofList [idProp.Name])),
                BinaryOperator.Eq,
                idParameter)
        let filterSql = HydrationSqlBuilder.emitExprToSql filterExpr

        withTransaction (fun conn ->
            let requiresRelationHandling = requiresRelationDeleteHandling conn
            if requiresRelationHandling then
                let tx = ensureRelationTx conn
                // DU-built SELECT for DeleteByCustomId old-state read.
                let vars = Dictionary<string, obj>()
                let idParameter =
                    QueryableHelperBase.allocateNamedStoredValueParam
                        vars "_cid0" idProp.IdType (box id)
                let whereExpr =
                    SqlExpr.Binary(
                        StoredValueParameter.declaredExpression idProp.IdType
                            (SqlExpr.JsonExtractExpr(Some "o", "Value", SqlModel.JsonPathOps.ofList [idProp.Name])),
                        BinaryOperator.Eq,
                        idParameter)
                let sql, _ =
                    HydrationSqlBuilder.buildManyOnlyHydratedSql conn name typeof<'T> whereExpr vars true
                let oldRow = readHydratedDocument conn sql vars
                if isNull oldRow then
                    0
                else
                    let owner = fromSQLite<'T> oldRow
                    syncDeleteOwner tx oldRow.Id.Value (box owner)
                    deleteDocumentById conn name oldRow.Id.Value
            else
                let mutable statement = conn.OpenStatement($"DELETE FROM \"{name}\" WHERE {filterSql}")
                try
                    statement.BindValues(variables)
                    statement.Step() |> ignore
                    let affected = statement.RecordsAffected
                    statement.Dispose()
                    affected
                with error -> statement.Abort(error); reraise()
        )

    static member TryGetByIdWithFallback<'IdType when 'IdType : equality>
        (id: 'IdType)
        (idArgName: string)
        (tryGetByInt64: int64 -> 'T option)
        (tryGetByCustomId: unit -> 'T option) =

        if EntityDocumentId<'T>.Value then
            match typeof<'IdType> with
            | x when x.Equals typeof<int8> ->
                let id: int8 = genericReinterpret id
                tryGetByInt64(int64 id)
            | x when x.Equals typeof<int16> ->
                let id: int16 = genericReinterpret id
                tryGetByInt64(int64 id)
            | x when x.Equals typeof<int32> ->
                let id: int32 = genericReinterpret id
                tryGetByInt64(int64 id)
            | x when x.Equals typeof<nativeint> ->
                let id: nativeint = genericReinterpret id
                tryGetByInt64(int64 id)
            | x when x.Equals typeof<int64> ->
                let id: int64 = genericReinterpret id
                tryGetByInt64(id)
            | x when x.Equals typeof<uint8> ->
                let id: uint8 = genericReinterpret id
                tryGetByInt64(int64 id)
            | x when x.Equals typeof<uint16> ->
                let id: uint16 = genericReinterpret id
                tryGetByInt64(int64 id)
            | x when x.Equals typeof<uint32> ->
                let id: uint32 = genericReinterpret id
                tryGetByInt64(int64 id)
            | x when x.Equals typeof<unativeint> ->
                let id: unativeint = genericReinterpret id
                tryGetByInt64(id |> uint64 |> int64FromUInt64OrThrow idArgName)
            | x when x.Equals typeof<uint64> ->
                let id: uint64 = genericReinterpret id
                tryGetByInt64(id |> int64FromUInt64OrThrow idArgName)
            | _ ->
                tryGetByCustomId()
        else
            tryGetByCustomId()

    static member DeleteByIdWithFallback<'IdType when 'IdType : equality>
        (id: 'IdType)
        (idArgName: string)
        (deleteByInt64: int64 -> int)
        (deleteByCustomId: unit -> int) =

        if EntityDocumentId<'T>.Value then
            match typeof<'IdType> with
            | x when x.Equals typeof<int8> ->
                let id: int8 = genericReinterpret id
                deleteByInt64(int64 id)
            | x when x.Equals typeof<int16> ->
                let id: int16 = genericReinterpret id
                deleteByInt64(int64 id)
            | x when x.Equals typeof<int32> ->
                let id: int32 = genericReinterpret id
                deleteByInt64(int64 id)
            | x when x.Equals typeof<nativeint> ->
                let id: nativeint = genericReinterpret id
                deleteByInt64(int64 id)
            | x when x.Equals typeof<int64> ->
                let id: int64 = genericReinterpret id
                deleteByInt64(id)
            | x when x.Equals typeof<uint8> ->
                let id: uint8 = genericReinterpret id
                deleteByInt64(int64 id)
            | x when x.Equals typeof<uint16> ->
                let id: uint16 = genericReinterpret id
                deleteByInt64(int64 id)
            | x when x.Equals typeof<uint32> ->
                let id: uint32 = genericReinterpret id
                deleteByInt64(int64 id)
            | x when x.Equals typeof<unativeint> ->
                let id: unativeint = genericReinterpret id
                deleteByInt64(id |> uint64 |> int64FromUInt64OrThrow idArgName)
            | x when x.Equals typeof<uint64> ->
                let id: uint64 = genericReinterpret id
                deleteByInt64(id |> int64FromUInt64OrThrow idArgName)
            | _ ->
                deleteByCustomId()
        else
            deleteByCustomId()
