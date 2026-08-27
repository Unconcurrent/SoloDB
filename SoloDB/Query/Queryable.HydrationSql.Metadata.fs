namespace SoloDatabase

open System
open System.Reflection
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.RelationsTypes
open SoloDatabase.RelationsSchemaBuilder
open SoloDatabase.RelationsSchemaValidator
open SoloDatabase.RelationsSchemaLinkTableDDL

module internal HydrationSqlMetadata =
    [<Struct>]
    type internal RelationShapeInfo = {
        HasAny: bool
        HasSingle: bool
        HasMany: bool
    }

    let internal relationShapeCache = System.Collections.Concurrent.ConcurrentDictionary<Type, RelationShapeInfo>()

    let internal getRelationShape (t: Type) : RelationShapeInfo =
        relationShapeCache.GetOrAdd(t, Func<Type, RelationShapeInfo>(fun t ->
            let props = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            let hasSingle = props |> Array.exists (fun p -> DBRefTypeHelpers.isDBRefType p.PropertyType)
            let hasMany = props |> Array.exists (fun p -> DBRefTypeHelpers.isDBRefManyType p.PropertyType)
            { HasAny = hasSingle || hasMany; HasSingle = hasSingle; HasMany = hasMany }
        ))

    let internal hasRelationProperties (t: Type) =
        (getRelationShape t).HasAny

    let internal tableExists (connection: SqliteConnection) (tableName: string) =
        connection.QueryFirst<int64>(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
            {| name = tableName |}) = 1L
