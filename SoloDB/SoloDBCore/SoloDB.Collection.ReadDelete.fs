namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open SoloDatabase.Types
open JsonFunctions
open Utils
open SQLiteTools
open SoloDatabase
open SqlDu.Engine.C1.Spec

[<AutoOpen>]
module internal CollectionReadDeletePrivate =
    let inline genericReinterpret<'A, 'B> (a: 'A) = Unsafe.As<'A, 'B>(&Unsafe.AsRef(&a))

    let inline int64FromUInt64OrThrow (idArgName: string) (value: uint64) =
        if value > uint64 Int64.MaxValue then
            raise (ArgumentOutOfRangeException(idArgName, "Identifier is out of range for Int64."))
        int64 value

type internal CollectionReadDeleteOps<'T>() =

    static member TryGetByIdInt64
        (id: int64)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> SqliteConnection)
        (hydrateRelations: SqliteConnection -> int64 -> obj -> unit) =

        use connection = getConnection()
        if hasRelations && HydrationSqlBuilder.hasRelationProperties typeof<'T> then
            // R45-10: Single-SQL hydration for non-queryable path.
            let vars = Dictionary<string, obj>()
            vars.["id"] <- box id
            let whereExpr =
                SqlExpr.Binary(
                    SqlExpr.Column(Some "o", "Id"),
                    BinaryOperator.Eq,
                    SqlExpr.Parameter "id")
            let sql, singleHydrated, manyHydrated =
                HydrationSqlBuilder.buildHydratedGetByIdSql connection name typeof<'T> whereExpr vars true
            QueryCommandInstrumentation.Increment()
            match connection.QueryFirstOrDefault<DbObjectRow>(sql, vars) with
            | json when Object.ReferenceEquals(json, null) -> None
            | json ->
                let entity = fromSQLite<'T> json
                // Populate DBRefMany trackers from HydrationJSON.
                if manyHydrated && not (isNull json.HydrationJSON) then
                    let hydMap = Dictionary<int64, string>()
                    hydMap.[json.Id.Value] <- json.HydrationJSON
                    HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (json.Id.Value, box entity) |] hydMap
                Some entity
        else
            QueryCommandInstrumentation.Increment()
            match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {|id = id|}) with
            | json when Object.ReferenceEquals(json, null) -> None
            | json ->
                let entity = fromSQLite<'T> json
                if hasRelations then
                    hydrateRelations connection id (box entity)
                Some entity

    static member RequireByIdInt64(id: int64, name: string, value: 'T option) =
        match value with
        | None -> raise (KeyNotFoundException (sprintf "There is no element with id '%i' inside collection '%s'" id name))
        | Some x -> x

    static member DeleteByIdInt64
        (id: int64)
        (name: string)
        (withTransaction: (SqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: SqliteConnection -> bool)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (syncDeleteOwner: Relations.RelationTxContext -> int64 -> obj -> unit) =

        withTransaction (fun conn ->
            let requiresRelationHandling = requiresRelationDeleteHandling conn
            if requiresRelationHandling then
                let tx = ensureRelationTx conn
                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {| id = id |})
                if isNull oldRow then
                    0
                else
                    let owner = fromSQLite<'T> oldRow
                    syncDeleteOwner tx id (box owner)
                    conn.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {|id = id|})
            else
                conn.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {|id = id|})
        )

    static member TryGetByCustomId<'IdType when 'IdType : equality>
        (id: 'IdType)
        (name: string)
        (hasRelations: bool)
        (getConnection: unit -> SqliteConnection)
        (hydrateRelations: SqliteConnection -> int64 -> obj -> unit) : 'T option =

        let custom = CustomTypeId<'T>.Value
        let idProp =
            match custom with
            | Some c -> c.Property
            | None -> raise (InvalidOperationException("This collection has no custom [Id] property. Use the Int64 Id overload."))

        let filter, variables = QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<'IdType>(idProp) = id))

        use connection = getConnection()
        if hasRelations && HydrationSqlBuilder.hasRelationProperties typeof<'T> then
            // R45-10: Single-SQL hydration for custom-id non-queryable path.
            // The filter is already translated to raw SQL. Build hydrated projections
            // via DU builder (with table alias "o"), emit without WHERE, then splice raw filter.
            let ctx = QueryContext.SingleSource(name)
            HydrationSqlBuilder.preloadQueryContextMetadata ctx connection
            let shape : HydrationSqlBuilder.RelationShapeInfo = HydrationSqlBuilder.getRelationShape typeof<'T>
            let mutable aliasCounter = 0
            let tblAlias = "o"
            let valueExpr =
                if shape.HasSingle then
                    let hydrated = HydrationSqlBuilder.buildHydrationValueExpr ctx typeof<'T> name (SqlExpr.Column(Some tblAlias, "Value")) 0 "" &aliasCounter
                    if aliasCounter > 0 then
                        SqlExpr.FunctionCall("json_extract", [hydrated; SqlExpr.Literal(SqlLiteral.String "$")])
                    else
                        SqlExpr.FunctionCall("json_quote", [SqlExpr.Column(Some tblAlias, "Value")])
                else
                    SqlExpr.FunctionCall("json_quote", [SqlExpr.Column(Some tblAlias, "Value")])
            let manyOpt =
                if shape.HasMany then HydrationSqlBuilder.buildManyHydrationProjection ctx typeof<'T> name (SqlExpr.Column(Some tblAlias, "Id")) &aliasCounter
                else None
            let manyHydrated = shape.HasMany && manyOpt.IsSome
            let sb = System.Text.StringBuilder(256)
            let emitVars = Dictionary<string, obj>(variables)
            let projections =
                [{ Alias = None; Expr = SqlExpr.Column(Some tblAlias, "Id") }
                 { Alias = Some "ValueJSON"; Expr = valueExpr }]
                @ (match manyOpt with Some e -> [{ Alias = Some "HydrationJSON"; Expr = e }] | None -> [])
            let core =
                { QueryableHelperBase.mkCore projections (Some (BaseTable(name, Some tblAlias)))
                  with
                    Where = None
                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
            let select = QueryableHelperBase.wrapCore core
            let modelTableNames = QueryableHelperBase.collectIndexModelTableNames name select
            let indexModel = SoloDatabase.IndexModel.loadModelForTables connection modelTableNames
            QueryableHelperBase.emitSelectToSb sb emitVars indexModel select
            // Splice raw WHERE filter before LIMIT.
            let emittedSql = sb.ToString()
            let sql =
                let limitIdx = emittedSql.LastIndexOf("LIMIT")
                if limitIdx > 0 then
                    emittedSql.Insert(limitIdx, $"WHERE {filter} ")
                else
                    emittedSql + $" WHERE {filter}"
            QueryCommandInstrumentation.Increment()
            match connection.QueryFirstOrDefault<DbObjectRow>(sql, variables) with
            | json when Object.ReferenceEquals(json, null) -> None
            | json ->
                let entity = fromSQLite<'T> json
                if manyHydrated && not (isNull json.HydrationJSON) then
                    let hydMap = Dictionary<int64, string>()
                    hydMap.[json.Id.Value] <- json.HydrationJSON
                    HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (json.Id.Value, box entity) |] hydMap
                Some entity
        else
            QueryCommandInstrumentation.Increment()
            match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter} LIMIT 1", variables) with
            | json when Object.ReferenceEquals(json, null) -> None
            | json ->
                let entity = fromSQLite<'T> json
                if hasRelations then
                    hydrateRelations connection json.Id.Value (box entity)
                Some entity

    static member DeleteByCustomId<'IdType when 'IdType : equality>
        (id: 'IdType)
        (name: string)
        (withTransaction: (SqliteConnection -> int) -> int)
        (requiresRelationDeleteHandling: SqliteConnection -> bool)
        (ensureRelationTx: SqliteConnection -> Relations.RelationTxContext)
        (syncDeleteOwner: Relations.RelationTxContext -> int64 -> obj -> unit) =

        let custom = CustomTypeId<'T>.Value
        let idProp =
            match custom with
            | Some c -> c.Property
            | None -> raise (InvalidOperationException("This collection has no custom [Id] property. Use the Int64 Id overload."))

        let filter, variables = QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<'IdType>(idProp) = id))

        withTransaction (fun conn ->
            let requiresRelationHandling = requiresRelationDeleteHandling conn
            if requiresRelationHandling then
                let tx = ensureRelationTx conn
                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter} LIMIT 1", variables)
                if isNull oldRow then
                    0
                else
                    let owner = fromSQLite<'T> oldRow
                    syncDeleteOwner tx oldRow.Id.Value (box owner)
                    conn.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {| id = oldRow.Id.Value |})
            else
                conn.Execute ($"DELETE FROM \"{name}\" WHERE {filter}", variables)
        )

    static member TryGetByIdWithFallback<'IdType when 'IdType : equality>
        (id: 'IdType)
        (idArgName: string)
        (tryGetByInt64: int64 -> 'T option)
        (tryGetByCustomId: unit -> 'T option) =

        if HasTypeId<'T>.Value then
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

        if HasTypeId<'T>.Value then
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
