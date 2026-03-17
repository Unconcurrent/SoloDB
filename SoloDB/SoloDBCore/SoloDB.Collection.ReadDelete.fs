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
            // Build WHERE as SqlExpr DU (not raw string splice) to keep query in the DU tree.
            // Custom-id filter is: jsonb_extract(Value, '$.PropName') = @param
            let vars = Dictionary<string, obj>()
            vars.["_cid0"] <- box id
            let whereExpr =
                SqlExpr.Binary(
                    SqlExpr.FunctionCall("jsonb_extract",
                        [SqlExpr.Column(Some "o", "Value"); SqlExpr.Literal(SqlLiteral.String ("$." + idProp.Name))]),
                    BinaryOperator.Eq,
                    SqlExpr.Parameter "_cid0")
            let sql, _singleHydrated, manyHydrated =
                HydrationSqlBuilder.buildHydratedGetByIdSql connection name typeof<'T> whereExpr vars true
            QueryCommandInstrumentation.Increment()
            match connection.QueryFirstOrDefault<DbObjectRow>(sql, vars) with
            | json when Object.ReferenceEquals(json, null) -> None
            | json ->
                let entity = fromSQLite<'T> json
                if manyHydrated && not (isNull json.HydrationJSON) then
                    let hydMap = Dictionary<int64, string>()
                    hydMap.[json.Id.Value] <- json.HydrationJSON
                    HydrationManyPopulator.populateFromHydrationJson typeof<'T> [| (json.Id.Value, box entity) |] hydMap
                Some entity
        else
            // R45-11 S-1: DU-built SELECT for custom-id no-relations fallback.
            let vars = Dictionary<string, obj>()
            vars.["_cid0"] <- box id
            let whereExpr =
                SqlExpr.Binary(
                    SqlExpr.FunctionCall("jsonb_extract",
                        [SqlExpr.Column(Some "o", "Value"); SqlExpr.Literal(SqlLiteral.String ("$." + idProp.Name))]),
                    BinaryOperator.Eq,
                    SqlExpr.Parameter "_cid0")
            let sql, _ =
                HydrationSqlBuilder.buildManyOnlyHydratedSql connection name typeof<'T> whereExpr vars true
            QueryCommandInstrumentation.Increment()
            match connection.QueryFirstOrDefault<DbObjectRow>(sql, vars) with
            | json when Object.ReferenceEquals(json, null) -> None
            | json ->
                let entity = fromSQLite<'T> json
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
                // R45-11 S-2: DU-built SELECT for DeleteByCustomId old-state read.
                let vars = Dictionary<string, obj>()
                vars.["_cid0"] <- box id
                let whereExpr =
                    SqlExpr.Binary(
                        SqlExpr.FunctionCall("jsonb_extract",
                            [SqlExpr.Column(Some "o", "Value"); SqlExpr.Literal(SqlLiteral.String ("$." + idProp.Name))]),
                        BinaryOperator.Eq,
                        SqlExpr.Parameter "_cid0")
                let sql, _ =
                    HydrationSqlBuilder.buildManyOnlyHydratedSql conn name typeof<'T> whereExpr vars true
                QueryCommandInstrumentation.Increment()
                let oldRow = conn.QueryFirstOrDefault<DbObjectRow>(sql, vars)
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
