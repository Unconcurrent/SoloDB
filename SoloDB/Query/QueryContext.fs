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
type internal RelationMetadataSource(connection: Microsoft.Data.Sqlite.SqliteConnection) =
    let mutable relationCatalogChecked = false
    let mutable relationCatalogExists = false
    let mutable typeMapChecked = false
    let mutable typeMapExists = false
    // Owner|Property keys already looked up, including those that resolved to nothing, so a
    // missing relation is not re-queried on every access.
    let resolvedRelationKeys = HashSet<string>(System.StringComparer.Ordinal)
    let resolvedTypeKeys = HashSet<string>(System.StringComparer.Ordinal)

    let tableExists (name: string) =
        connection.QueryFirst<int64>(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
            {| name = name |}) = 1L

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

    member _.MarkRelationResolved(key: string) = resolvedRelationKeys.Add key |> ignore
    member _.IsRelationResolved(key: string) = resolvedRelationKeys.Contains key
    member _.MarkTypeKeyResolved(typeKey: string) = resolvedTypeKeys.Add typeKey |> ignore
    member _.IsTypeKeyResolved(typeKey: string) = resolvedTypeKeys.Contains typeKey

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

    /// Loads one owner/property relation from the catalog if it has not been looked up yet.
    /// A lookup that finds nothing is still recorded, so a missing relation is queried once
    /// rather than on every access.
    member private this.EnsureRelationLoaded(ownerCollection: string, propertyName: string) =
        match this.MetadataSource with
        | ValueNone -> ()
        | ValueSome source ->
            let key = this.RelationKey(ownerCollection, propertyName)
            if not (source.IsRelationResolved key) then
                source.MarkRelationResolved key
                if source.RelationCatalogExists then
                    // The owner/property predicate is served by UNIQUE(OwnerCollection, PropertyName).
                    // Link-table existence is decided in the same statement rather than by follow-up
                    // probes, preserving the rule that the default table wins when both exist.
                    let rows =
                        source.Connection.Query<{|
                            PropertyName: string
                            TargetCollection: string
                            RefKind: string
                            DefaultLink: string
                            CanonicalLink: string
                            DefaultExists: int64
                            CanonicalExists: int64
                        |}>(
                            "SELECT r.PropertyName AS PropertyName,
                                    r.TargetCollection AS TargetCollection,
                                    r.RefKind AS RefKind,
                                    'SoloDBRelLink_' || r.Name AS DefaultLink,
                                    'SoloDBRelLink_' || CASE WHEN r.OwnerCollection <= r.TargetCollection
                                        THEN r.OwnerCollection || '_' || r.TargetCollection
                                        ELSE r.TargetCollection || '_' || r.OwnerCollection END AS CanonicalLink,
                                    (SELECT COUNT(*) FROM sqlite_master m
                                     WHERE m.type = 'table' AND m.name = 'SoloDBRelLink_' || r.Name) AS DefaultExists,
                                    (SELECT COUNT(*) FROM sqlite_master m
                                     WHERE m.type = 'table' AND m.name = 'SoloDBRelLink_' || CASE WHEN r.OwnerCollection <= r.TargetCollection
                                        THEN r.OwnerCollection || '_' || r.TargetCollection
                                        ELSE r.TargetCollection || '_' || r.OwnerCollection END) AS CanonicalExists
                             FROM SoloDBRelation r
                             WHERE r.OwnerCollection = @owner AND r.PropertyName = @property
                             AND r.Name IS NOT NULL AND r.TargetCollection IS NOT NULL AND r.RefKind IS NOT NULL",
                            {| owner = ownerCollection; property = propertyName |})
                    for row in rows do
                        // Shared canonical table only when it exists and the default does not.
                        let useSharedMany =
                            row.RefKind = "Many" && row.CanonicalExists > 0L && row.DefaultExists = 0L
                        let ownerUsesSource =
                            if useSharedMany then System.StringComparer.Ordinal.Compare(ownerCollection, row.TargetCollection) <= 0
                            else true
                        let linkTable = if useSharedMany then row.CanonicalLink else row.DefaultLink
                        this.RegisterRelation(ownerCollection, row.PropertyName, row.TargetCollection, linkTable, ownerUsesSource)

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

    /// Loads every collection registered for one type key. Keyed on the requested type key and
    /// never narrowed by owner, so the ambiguity branches below observe exactly the same set they
    /// would have observed under a full catalog load.
    member private this.EnsureTypeKeyLoaded(typeKey: string) =
        match this.MetadataSource with
        | ValueNone -> ()
        | ValueSome source ->
            if not (System.String.IsNullOrWhiteSpace typeKey) && not (source.IsTypeKeyResolved typeKey) then
                source.MarkTypeKeyResolved typeKey
                if source.TypeMapExists then
                    // Served by UNIQUE(TypeKey, CollectionName).
                    for mapping in source.Connection.Query<{| CollectionName: string |}>(
                                        "SELECT CollectionName FROM SoloDBTypeCollectionMap WHERE TypeKey = @typeKey AND CollectionName IS NOT NULL",
                                        {| typeKey = typeKey |}) do
                        this.RegisterTypeCollection(typeKey, mapping.CollectionName)

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
