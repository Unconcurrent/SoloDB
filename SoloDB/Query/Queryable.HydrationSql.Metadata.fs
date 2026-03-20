namespace SoloDatabase

open System
open System.Reflection
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.RelationsTypes
open SoloDatabase.RelationsSchema

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

    let internal preloadQueryContextMetadata (ctx: QueryContext) (connection: SqliteConnection) =
        let canonicalManyName (a: string) (b: string) =
            if StringComparer.Ordinal.Compare(a, b) <= 0 then $"{a}_{b}" else $"{b}_{a}"

        if tableExists connection "SoloDBRelation" then
            for relation in connection.Query<{|
                Name: string
                OwnerCollection: string
                PropertyName: string
                TargetCollection: string
                RefKind: string
            |}>("SELECT Name, OwnerCollection, PropertyName, TargetCollection, RefKind FROM SoloDBRelation;") do
                let name = relation.Name
                let ownerCollection = relation.OwnerCollection
                let propertyName = relation.PropertyName
                let targetCollection = relation.TargetCollection
                let refKind = relation.RefKind
                if not (isNull name || isNull ownerCollection || isNull propertyName || isNull targetCollection || isNull refKind) then
                    let relationKind = stringToRelationKind refKind
                    let canonicalName = canonicalManyName ownerCollection targetCollection
                    let canonicalLinkTable = "SoloDBRelLink_" + canonicalName
                    let defaultLinkTable = "SoloDBRelLink_" + name
                    let useSharedMany =
                        match relationKind with
                        | RelationKind.Many ->
                            tableExists connection canonicalLinkTable
                            && not (tableExists connection defaultLinkTable)
                        | RelationKind.Single -> false
                    let ownerUsesSource =
                        if useSharedMany then
                            StringComparer.Ordinal.Compare(ownerCollection, targetCollection) <= 0
                        else
                            true
                    let linkTable = if useSharedMany then canonicalLinkTable else defaultLinkTable
                    ctx.RegisterRelation(ownerCollection, propertyName, targetCollection, linkTable, ownerUsesSource)

        if tableExists connection "SoloDBTypeCollectionMap" then
            for mapping in connection.Query<{|
                TypeKey: string
                CollectionName: string
            |}>("SELECT TypeKey, CollectionName FROM SoloDBTypeCollectionMap;") do
                let typeKey = mapping.TypeKey
                let collectionName = mapping.CollectionName
                if not (isNull typeKey || isNull collectionName) then
                    ctx.RegisterTypeCollection(typeKey, collectionName)
