namespace SoloDatabase

open System.Collections.Generic
open SQLiteTools
open SqlDu.Engine.C1.Spec

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
    override this.Equals(other: obj) =
        match other with
        | :? RelationCacheKey as o ->
            System.String.Equals(this.Owner, o.Owner, System.StringComparison.Ordinal)
            && System.String.Equals(this.Property, o.Property, System.StringComparison.Ordinal)
        | _ -> false
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
type internal RelationMetadataSource(connection: Microsoft.Data.Sqlite.SqliteConnection) =
    let mutable relationCatalogChecked = false
    let mutable relationCatalogExists = false
    let mutable typeMapChecked = false
    let mutable typeMapExists = false
    // Authoritative results. ValueNone records a lookup that found nothing, so an absent
    // relation is queried once rather than on every access.
    let relations = Dictionary<RelationCacheKey, ResolvedRelation voption>(HashIdentity.Structural)
    let typeMappings = Dictionary<string, string[]>(System.StringComparer.Ordinal)

    let tableExists (name: string) =
        connection.QueryFirst<int64>(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
            {| name = name |}) = 1L

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
                    connection.Query<{| Name: string; TargetCollection: string; RefKind: string |}>(
                        "SELECT Name, TargetCollection, RefKind FROM SoloDBRelation
                         WHERE OwnerCollection = @owner AND PropertyName = @property
                           AND Name IS NOT NULL AND TargetCollection IS NOT NULL AND RefKind IS NOT NULL
                         LIMIT 1",
                        {| owner = ownerCollection; property = propertyName |})
                    |> Seq.tryHead
                match row with
                | None -> ValueNone
                | Some r when isNull r.Name || isNull r.TargetCollection || isNull r.RefKind -> ValueNone
                | Some r ->
                    let defaultLink = "SoloDBRelLink_" + r.Name
                    let canonicalLink = "SoloDBRelLink_" + canonicalManyName ownerCollection r.TargetCollection
                    // Both candidates are named in .NET and their existence decided in one
                    // statement, so the per-row probe loop is gone without moving the naming
                    // rule into SQLite's collation.
                    let present =
                        if r.RefKind <> "Many" then Set.empty
                        else
                            connection.Query<{| name: string |}>(
                                "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (@a, @b)",
                                {| a = defaultLink; b = canonicalLink |})
                            |> Seq.map _.name
                            |> Set.ofSeq
                    // Shared canonical table only when it exists and the default does not.
                    let useSharedMany =
                        r.RefKind = "Many" && present.Contains canonicalLink && not (present.Contains defaultLink)
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
                    connection.Query<{| CollectionName: string |}>(
                        "SELECT CollectionName FROM SoloDBTypeCollectionMap WHERE TypeKey = @typeKey AND CollectionName IS NOT NULL",
                        {| typeKey = typeKey |})
                    |> Seq.map _.CollectionName
                    |> Seq.filter (isNull >> not)
                    |> Seq.toArray
            typeMappings.[typeKey] <- names
            names

type internal LayerPosition =
| BaseLayer
| OuterLayer

/// Closed-enum kinds for translator-emitted runtime errors. The kind is encoded
/// into the typed-payload JSON object on Id=NULL rows; the materializer dispatches
/// by kind to the matching .NET exception type. Adding a new kind requires
/// JsonFunctions.fromSQLite (Id=NULL dispatch arm) and runtimeErrorPayload (kind name)
/// updates in lockstep.
type internal RuntimeErrorKind =
    | CardinalityError
    | CastError
    | RangeError

/// Represents a named source root in a LINQ multi-source query graph.
type internal QueryRootSource = {
    /// Logical source key (e.g., "orders", "authors")
    SourceKey: string
    /// Backing collection table name.
    TableName: string
    /// SQL alias used when this source is referenced.
    Alias: string
}

/// Holds source roots for query-graph translation.
type internal QueryRootGraph = {
    /// Ordered root list; first item is the primary source.
    Roots: ResizeArray<QueryRootSource>
    /// Counter for generated source aliases.
    mutable SourceAliasCounter: int
}
    with
    /// Construct a backward-compatible single-root graph.
    static member Single(tableName: string) =
        { Roots =
            ResizeArray [
                { SourceKey = "root"
                  TableName = tableName
                  Alias = "\"" + tableName + "\"" }
            ]
          SourceAliasCounter = 0 }

    /// Return an existing source by key, or allocate a new one.
    member this.ResolveRoot(sourceKey: string, tableName: string) =
        match this.Roots |> Seq.tryFind (fun r -> r.SourceKey = sourceKey) with
        | Some existing -> existing
        | None ->
            let alias = sprintf "_src%d" this.SourceAliasCounter
            this.SourceAliasCounter <- this.SourceAliasCounter + 1
            let created = {
                SourceKey = sourceKey
                TableName = tableName
                Alias = alias
            }
            this.Roots.Add(created)
            created

    member this.TryFindByAlias(alias: string) =
        this.Roots |> Seq.tryFind (fun r -> r.Alias = alias)

/// Represents a single JOIN edge in the query source graph.
/// Used by the query translator to emit LEFT JOIN clauses for DBRef property access.
type internal JoinEdge = {
    /// Alias for the joined table (e.g., "_ref0")
    TargetAlias: string
    /// Target collection table name (e.g., "Customers")
    TargetTable: string
    /// JOIN kind (e.g., "LEFT JOIN")
    JoinKind: string
    /// Source table alias for the ON condition (e.g., Some "\"Orders\"")
    OnSourceAlias: string option
    /// Property name for the jsonb_extract in ON condition (e.g., "Customer")
    OnPropertyName: string
    /// The property path that triggered this join (e.g., "Customer")
    PropertyPath: string
}

/// Query source context for single-source or multi-source (joined) queries.
/// When Joins is empty, the query pipeline produces byte-identical SQL to the pre-relation pipeline.
type internal QueryContext = {
    /// The primary (root) table name
    RootTable: string
    /// Whether this context translates against a base table or an outer/subquery layer.
    LayerPosition: LayerPosition
    /// Query source graph roots. Single-source queries keep exactly one root.
    RootGraph: QueryRootGraph
    /// Accumulated join edges (populated during expression translation)
    Joins: ResizeArray<JoinEdge>
    /// Monotonic alias counter (shared by reference across cloned contexts to prevent collisions)
    AliasCounter: int ref
    /// Property paths excluded via Exclude() — skip JOIN/load for these
    ExcludedPaths: HashSet<string>
    /// Property paths included via Include() whitelist for hydration.
    IncludedPaths: HashSet<string>
    /// When true, only explicitly Included paths load (activated by parameterless Exclude()).
    mutable WhitelistMode: bool
    /// Relation paths currently materialized into the payload Value column.
    MaterializedPaths: HashSet<string>
    /// Relation target table mapping keyed by "OwnerCollection|PropertyName"
    RelationTargets: Dictionary<string, string>
    /// Relation link table mapping keyed by "OwnerCollection|PropertyName"
    RelationLinks: Dictionary<string, string>
    /// Whether this owner/property maps owner rows to SourceId (true) or TargetId (false) in the link table.
    RelationOwnerUsesSource: Dictionary<string, bool>
    /// Type -> known collection names mapping (used to resolve custom collection names)
    TypeCollections: Dictionary<string, HashSet<string>>
    /// Loads relation/type metadata on demand. Shared by reference with nested contexts, so a
    /// lookup made anywhere in a translation is cached for the whole translation.
    /// ValueNone for contexts that never resolve relation metadata.
    MetadataSource: RelationMetadataSource voption
    /// True ONLY at the OUTERMOST translation context (top-level user query
    /// terminator). Cleared in CloneForSubquery (any nested subquery loses the flag)
    /// so cardinality emit sites can detect they are nested and emit bare scalars
    /// (default(T) propagation) rather than typed exceptions, per the SoloDB 1.2.2
    /// contract: top-level cardinality errors throw via the Id=NULL channel; nested
    /// cardinality silently propagates default values to the outer chain.
    mutable IsAtTopLevel: bool
}
    with
    /// Create a single-source context (backward-compatible default).
    static member SingleSource(tableName: string) =
        { RootTable = tableName
          LayerPosition = BaseLayer
          RootGraph = QueryRootGraph.Single(tableName)
          Joins = ResizeArray()
          AliasCounter = ref 0
          ExcludedPaths = HashSet()
          IncludedPaths = HashSet()
          WhitelistMode = false
          MaterializedPaths = HashSet(System.StringComparer.Ordinal)
          RelationTargets = Dictionary(System.StringComparer.Ordinal)
          RelationLinks = Dictionary(System.StringComparer.Ordinal)
          RelationOwnerUsesSource = Dictionary(System.StringComparer.Ordinal)
          TypeCollections = Dictionary(System.StringComparer.Ordinal)
          MetadataSource = ValueNone
          IsAtTopLevel = false }

    /// Create a multi-source context while preserving the first root as the primary table.
    static member MultiSource(rootTable: string, roots: seq<string * string>) =
        let graph = QueryRootGraph.Single(rootTable)
        for (sourceKey, tableName) in roots do
            if not (System.String.Equals(sourceKey, "root", System.StringComparison.Ordinal)) then
                graph.ResolveRoot(sourceKey, tableName) |> ignore
        { RootTable = rootTable
          LayerPosition = BaseLayer
          RootGraph = graph
          Joins = ResizeArray()
          AliasCounter = ref 0
          ExcludedPaths = HashSet()
          IncludedPaths = HashSet()
          WhitelistMode = false
          MaterializedPaths = HashSet(System.StringComparer.Ordinal)
          RelationTargets = Dictionary(System.StringComparer.Ordinal)
          RelationLinks = Dictionary(System.StringComparer.Ordinal)
          RelationOwnerUsesSource = Dictionary(System.StringComparer.Ordinal)
          TypeCollections = Dictionary(System.StringComparer.Ordinal)
          MetadataSource = ValueNone
          IsAtTopLevel = false }

    /// Generate a unique alias (_ref0, _ref1, ...).
    /// AliasCounter is a ref cell — shared across cloned contexts to prevent collisions.
    /// Uses Interlocked.Increment so mutation is atomic; sibling alias-generators in
    /// the GroupBy/GroupJoin chain builders use the same pattern on the same ref cell,
    /// and mixing atomic with non-atomic increments could lose counts under concurrent
    /// access of a cloned context.
    member this.NextAlias() =
        let n = System.Threading.Interlocked.Increment(this.AliasCounter) - 1
        sprintf "_ref%d" n

    /// Find existing join for a property path, or None (deduplication).
    member this.FindJoin(propertyPath: string) =
        this.Joins |> Seq.tryFind (fun j -> j.PropertyPath = propertyPath)

    member this.TryFindJoinByAlias(alias: string) =
        this.Joins |> Seq.tryFind (fun j -> j.TargetAlias = alias)

    /// Create a subquery-scoped clone with isolated Joins but shared metadata dictionaries.
    /// Used by ForSubquery to prevent DBRef JOIN leakage from inner correlated subqueries to the outer scope.
    /// A fresh single-source context belonging to the same translation as `parent`.
    /// Inner roots created during translation (Join, GroupJoin, SelectMany, nested builders)
    /// must keep the parent's metadata authority; without it relation access through the child
    /// silently falls back to defaults.
    static member ChildOf(parent: QueryContext, tableName: string) =
        { QueryContext.SingleSource(tableName) with MetadataSource = parent.MetadataSource }

    member this.CloneForSubquery(?rootTable: string) =
        { this with
            Joins = ResizeArray()
            RootTable = defaultArg rootTable this.RootTable
            IsAtTopLevel = false }

    /// Resolve or create an additional query root for multi-source planning.
    member this.ResolveRoot(sourceKey: string, tableName: string) =
        this.RootGraph.ResolveRoot(sourceKey, tableName)

    member this.TryFindRootByAlias(alias: string) =
        this.RootGraph.TryFindByAlias(alias)

    /// The root table's "TableName". prefix for backward-compat with existing QueryBuilder usage.
    member this.RootTableNameDot =
        if System.String.IsNullOrEmpty this.RootTable then System.String.Empty
        else "\"" + this.RootTable + "\"."

    member private this.RelationKey(ownerCollection: string, propertyName: string) =
        ownerCollection + "|" + propertyName

    member this.RegisterRelation(ownerCollection: string, propertyName: string, targetCollection: string, linkTable: string, ownerUsesSource: bool) =
        let key = this.RelationKey(ownerCollection, propertyName)
        this.RelationTargets.[key] <- targetCollection
        this.RelationLinks.[key] <- linkTable
        this.RelationOwnerUsesSource.[key] <- ownerUsesSource

    /// Copies the authoritative result for one owner/property into this context's dictionaries.
    /// Results are read from the shared source on every call, so a context that was cloned after
    /// a sibling resolved the same key still sees the value rather than a false miss.
    member private this.EnsureRelationLoaded(ownerCollection: string, propertyName: string) =
        match this.MetadataSource with
        | ValueNone -> ()
        | ValueSome source ->
            let key = this.RelationKey(ownerCollection, propertyName)
            if not (this.RelationTargets.ContainsKey key) then
                match source.GetRelation(ownerCollection, propertyName) with
                | ValueNone -> ()
                | ValueSome resolved ->
                    this.RegisterRelation(ownerCollection, propertyName, resolved.TargetCollection, resolved.LinkTable, resolved.OwnerUsesSource)

    member this.TryResolveRelationTarget(ownerCollection: string, propertyName: string) =
        this.EnsureRelationLoaded(ownerCollection, propertyName)
        let key = this.RelationKey(ownerCollection, propertyName)
        match this.RelationTargets.TryGetValue key with
        | true, value -> Some value
        | _ -> None

    member this.TryResolveRelationLink(ownerCollection: string, propertyName: string) =
        this.EnsureRelationLoaded(ownerCollection, propertyName)
        let key = this.RelationKey(ownerCollection, propertyName)
        match this.RelationLinks.TryGetValue key with
        | true, value -> Some value
        | _ -> None

    member this.TryResolveRelationOwnerUsesSource(ownerCollection: string, propertyName: string) =
        this.EnsureRelationLoaded(ownerCollection, propertyName)
        let key = this.RelationKey(ownerCollection, propertyName)
        match this.RelationOwnerUsesSource.TryGetValue key with
        | true, value -> Some value
        | _ -> None

    member this.RegisterTypeCollection(typeKey: string, collectionName: string) =
        if not (System.String.IsNullOrWhiteSpace(typeKey) || System.String.IsNullOrWhiteSpace(collectionName)) then
            let set =
                match this.TypeCollections.TryGetValue(typeKey) with
                | true, existing -> existing
                | _ ->
                    let created = HashSet<string>(System.StringComparer.Ordinal)
                    this.TypeCollections.[typeKey] <- created
                    created
            set.Add(collectionName) |> ignore

    /// Copies every collection registered for one type key into this context. Read from the
    /// shared source each time for the same reason as relations above.
    member private this.EnsureTypeKeyLoaded(typeKey: string) =
        match this.MetadataSource with
        | ValueNone -> ()
        | ValueSome source ->
            if not (this.TypeCollections.ContainsKey typeKey) then
                for name in source.GetTypeCollections typeKey do
                    this.RegisterTypeCollection(typeKey, name)

    member this.ResolveCollectionForType(typeKey: string, defaultCollection: string) =
        this.EnsureTypeKeyLoaded typeKey
        match this.TypeCollections.TryGetValue(typeKey) with
        | false, _ -> defaultCollection
        | true, names when names.Count = 0 -> defaultCollection
        | true, names when names.Contains(defaultCollection) -> defaultCollection
        | true, names when names.Count = 1 -> names |> Seq.head
        | true, _ ->
            raise (System.InvalidOperationException(
                $"Error: Ambiguous collection mapping for relation target type '{typeKey}'.\nReason: Multiple collections are registered for this type.\nFix: Register exactly one target collection for relation-backed queries."))
