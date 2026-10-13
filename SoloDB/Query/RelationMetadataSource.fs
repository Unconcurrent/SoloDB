namespace SoloDatabase

open System.Collections.Generic
open NativeSQLiteTools
open SoloDatabase.SqlModel

/// Loads relation metadata for exactly what a translation asks for, at the moment it asks.
///
/// The catalogs are read through their existing unique indexes -- SoloDBRelation has
/// UNIQUE(OwnerCollection, PropertyName) and SoloDBTypeCollectionMap has UNIQUE(TypeKey,
/// CollectionName) -- so a lookup is a single indexed statement rather than a scan of every
/// relation in the database.
///
/// Resolution is deliberately lazy rather than preloaded: translation discovers additional query
/// roots (Join, GroupJoin, SelectMany, nested builders) only while running, and resolves them
/// against the outer context. Anything decided before translation therefore cannot know which
/// collections will be consulted.
///
/// The instance lives for one translation. It is shared by reference into nested contexts, so a
/// repeated lookup costs nothing, and it dies with the translation, so the next query observes
/// current catalog state.
/// Identity of one relation lookup. A struct key with ordinal equality, rather than a
/// concatenated string, so a collection or property name containing the old separator cannot
/// alias a different pair and produce a false hit or a false miss.
[<Struct; CustomEquality; NoComparison>]
type internal RelationCacheKey =
    { Owner: string; Property: string }
    member private this.Same(other: RelationCacheKey) =
        System.String.Equals(this.Owner, other.Owner, System.StringComparison.Ordinal)
        && System.String.Equals(this.Property, other.Property, System.StringComparison.Ordinal)
    override this.Equals(other: obj) =
        match other with
        | :? RelationCacheKey as other -> this.Same(other)
        | _ -> false
    interface System.IEquatable<RelationCacheKey> with
        member this.Equals(other) = this.Same(other)
    override this.GetHashCode() =
        let h1 = if isNull this.Owner then 0 else this.Owner.GetHashCode()
        let h2 = if isNull this.Property then 0 else this.Property.GetHashCode()
        (h1 * 397) ^^^ h2

/// One resolved relation edge.
[<Struct>]
type internal ResolvedRelation =
    { TargetCollection: string; LinkTable: string; OwnerUsesSource: bool }

/// Loads relation metadata for exactly what a translation asks for, at the moment it asks.
///
/// The catalogs are read through their existing unique indexes -- SoloDBRelation has
/// UNIQUE(OwnerCollection, PropertyName) and SoloDBTypeCollectionMap has UNIQUE(TypeKey,
/// CollectionName) -- so a lookup is a single indexed statement rather than a scan of every
/// relation in the database.
///
/// Resolution is deliberately lazy rather than preloaded: translation discovers additional query
/// roots (Join, GroupJoin, SelectMany, nested builders) only while running, and resolves them
/// against the outer context. Anything decided before translation therefore cannot know which
/// collections will be consulted.
///
/// This object is the single authority for both the results and the fact that a lookup already
/// happened. Contexts copy results out of it; they never record "already resolved" on their own,
/// because a context that copied a marker without the matching result would silently answer a
/// later question with a miss.
///
/// The instance lives for one translation, so the next query observes current catalog state.
type internal RelationMetadataSource(connection: SoloDatabase.NativeSqliteConnection) =
    let mutable relationCatalogChecked = false
    let mutable relationCatalogExists = false
    let mutable typeMapChecked = false
    let mutable typeMapExists = false
    // Authoritative results. ValueNone records a lookup that found nothing, so an absent
    // relation is queried once rather than on every access.
    let relations = Dictionary<RelationCacheKey, ResolvedRelation voption>()
    let typeMappings = Dictionary<string, string[]>(System.StringComparer.Ordinal)

    let tableExists (name: string) =
        let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name)")
        try
            statement.BindText(1, name)
            statement.Step() |> ignore
            let exists = statement.Row.GetBoolean(0)
            statement.Dispose()
            exists
        with error -> statement.Abort(error); reraise()

    /// Canonical shared-many table name. Ordinal comparison, matching the single naming
    /// authority used elsewhere; SQLite BINARY text ordering is not equivalent for non-ASCII
    /// names and must not be substituted here.
    let canonicalManyName (a: string) (b: string) =
        if System.StringComparer.Ordinal.Compare(a, b) <= 0 then a + "_" + b else b + "_" + a

    member _.Connection = connection

    member _.RelationCatalogExists =
        if not relationCatalogChecked then
            relationCatalogExists <- tableExists "SoloDBRelation"
            relationCatalogChecked <- true
        relationCatalogExists

    member _.TypeMapExists =
        if not typeMapChecked then
            typeMapExists <- tableExists "SoloDBTypeCollectionMap"
            typeMapChecked <- true
        typeMapExists

    /// Resolves one owner/property edge, querying at most once per translation.
    member this.GetRelation(ownerCollection: string, propertyName: string) : ResolvedRelation voption =
        let key = { Owner = ownerCollection; Property = propertyName }
        match relations.TryGetValue key with
        | true, cached -> cached
        | _ ->
            let resolved =
                if not this.RelationCatalogExists then ValueNone
                else
                // Served by UNIQUE(OwnerCollection, PropertyName). Rows with any null identity
                // column are rejected here rather than used to construct a table name.
                let row =
                    let mutable statement = connection.OpenStatement("SELECT Name, TargetCollection, RefKind FROM SoloDBRelation WHERE OwnerCollection = @owner AND PropertyName = @property AND Name IS NOT NULL AND TargetCollection IS NOT NULL AND RefKind IS NOT NULL LIMIT 1")
                    try
                        statement.BindText(1, ownerCollection)
                        statement.BindText(2, propertyName)
                        let row =
                            if statement.Step() then Some {| Name = statement.Row.GetString(0); TargetCollection = statement.Row.GetString(1); RefKind = statement.Row.GetString(2) |}
                            else None
                        statement.Dispose()
                        row
                    with error -> statement.Abort(error); reraise()

                match row with
                | None -> ValueNone
                | Some r when isNull r.Name || isNull r.TargetCollection || isNull r.RefKind -> ValueNone
                | Some r ->
                    let defaultLink = "SoloDBRelLink_" + r.Name
                    let canonicalLink = "SoloDBRelLink_" + canonicalManyName ownerCollection r.TargetCollection
                    // Both candidates are named in .NET and their existence decided in one
                    // statement, so the per-row probe loop is gone without moving the naming
                    // rule into SQLite's collation.
                    let useSharedMany =
                        if r.RefKind <> "Many" then false
                        else
                            let mutable statement = connection.OpenStatement("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (@a, @b)")
                            try
                                statement.BindText(1, defaultLink)
                                statement.BindText(2, canonicalLink)
                                let mutable hasDefault, hasCanonical = false, false
                                while statement.Step() do
                                    let name = statement.Row.GetString(0)
                                    if name = defaultLink then hasDefault <- true
                                    if name = canonicalLink then hasCanonical <- true
                                statement.Dispose()
                                hasCanonical && not hasDefault
                            with error -> statement.Abort(error); reraise()
                    ValueSome {
                        TargetCollection = r.TargetCollection
                        LinkTable = if useSharedMany then canonicalLink else defaultLink
                        OwnerUsesSource =
                            if useSharedMany then System.StringComparer.Ordinal.Compare(ownerCollection, r.TargetCollection) <= 0
                            else true
                    }
            relations.[key] <- resolved
            resolved

    /// Every collection registered for one type key. Keyed on the requested key and never
    /// narrowed by owner, so ambiguity is observed exactly as under a full catalog load.
    member this.GetTypeCollections(typeKey: string) : string[] =
        match typeMappings.TryGetValue typeKey with
        | true, cached -> cached
        | _ ->
            let names =
                if System.String.IsNullOrWhiteSpace typeKey || not this.TypeMapExists then Array.empty
                else
                    // Served by UNIQUE(TypeKey, CollectionName).
                    let mutable statement = connection.OpenStatement("SELECT CollectionName FROM SoloDBTypeCollectionMap WHERE TypeKey = @typeKey AND CollectionName IS NOT NULL")
                    try
                        statement.BindText(1, typeKey)
                        let names = ResizeArray<string>()
                        while statement.Step() do names.Add(statement.Row.GetString(0))
                        statement.Dispose()
                        names.ToArray()
                    with error -> statement.Abort(error); reraise()

            typeMappings.[typeKey] <- names
            names
