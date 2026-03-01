namespace SoloDatabase

open System.Collections.Generic

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
    /// Full ON condition (e.g., "_ref0.Id = jsonb_extract(\"Orders\".Value, '$.Customer')")
    OnCondition: string
    /// The property path that triggered this join (e.g., "Customer")
    PropertyPath: string
}

/// Query source context for single-source or multi-source (joined) queries.
/// When Joins is empty, the query pipeline produces byte-identical SQL to the pre-relation pipeline.
type internal QueryContext = {
    /// The primary (root) table name
    RootTable: string
    /// Query source graph roots. Single-source queries keep exactly one root.
    RootGraph: QueryRootGraph
    /// Accumulated join edges (populated during expression translation)
    Joins: ResizeArray<JoinEdge>
    /// Monotonic alias counter (shared across entire query to prevent collisions)
    mutable AliasCounter: int
    /// Property paths excluded via Exclude() — skip JOIN/load for these
    ExcludedPaths: HashSet<string>
    /// Property paths included via Include() whitelist for hydration.
    IncludedPaths: HashSet<string>
    /// Relation target table mapping keyed by "OwnerCollection|PropertyName"
    RelationTargets: Dictionary<string, string>
    /// Relation link table mapping keyed by "OwnerCollection|PropertyName"
    RelationLinks: Dictionary<string, string>
    /// Whether this owner/property maps owner rows to SourceId (true) or TargetId (false) in the link table.
    RelationOwnerUsesSource: Dictionary<string, bool>
    /// Type -> known collection names mapping (used to resolve custom collection names)
    TypeCollections: Dictionary<string, HashSet<string>>
}
    with
    /// Create a single-source context (backward-compatible default).
    static member SingleSource(tableName: string) =
        { RootTable = tableName
          RootGraph = QueryRootGraph.Single(tableName)
          Joins = ResizeArray()
          AliasCounter = 0
          ExcludedPaths = HashSet()
          IncludedPaths = HashSet()
          RelationTargets = Dictionary(System.StringComparer.Ordinal)
          RelationLinks = Dictionary(System.StringComparer.Ordinal)
          RelationOwnerUsesSource = Dictionary(System.StringComparer.Ordinal)
          TypeCollections = Dictionary(System.StringComparer.Ordinal) }

    /// Create a multi-source context while preserving the first root as the primary table.
    static member MultiSource(rootTable: string, roots: seq<string * string>) =
        let graph = QueryRootGraph.Single(rootTable)
        for (sourceKey, tableName) in roots do
            if not (System.String.Equals(sourceKey, "root", System.StringComparison.Ordinal)) then
                graph.ResolveRoot(sourceKey, tableName) |> ignore
        { RootTable = rootTable
          RootGraph = graph
          Joins = ResizeArray()
          AliasCounter = 0
          ExcludedPaths = HashSet()
          IncludedPaths = HashSet()
          RelationTargets = Dictionary(System.StringComparer.Ordinal)
          RelationLinks = Dictionary(System.StringComparer.Ordinal)
          RelationOwnerUsesSource = Dictionary(System.StringComparer.Ordinal)
          TypeCollections = Dictionary(System.StringComparer.Ordinal) }

    /// Generate a unique alias (_ref0, _ref1, ...).
    member this.NextAlias() =
        let n = this.AliasCounter
        this.AliasCounter <- n + 1
        sprintf "_ref%d" n

    /// Find existing join for a property path, or None (deduplication).
    member this.FindJoin(propertyPath: string) =
        this.Joins |> Seq.tryFind (fun j -> j.PropertyPath = propertyPath)

    member this.TryFindJoinByAlias(alias: string) =
        this.Joins |> Seq.tryFind (fun j -> j.TargetAlias = alias)

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

    member this.TryResolveRelationTarget(ownerCollection: string, propertyName: string) =
        let key = this.RelationKey(ownerCollection, propertyName)
        match this.RelationTargets.TryGetValue key with
        | true, value -> Some value
        | _ -> None

    member this.TryResolveRelationLink(ownerCollection: string, propertyName: string) =
        let key = this.RelationKey(ownerCollection, propertyName)
        match this.RelationLinks.TryGetValue key with
        | true, value -> Some value
        | _ -> None

    member this.TryResolveRelationOwnerUsesSource(ownerCollection: string, propertyName: string) =
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

    member this.ResolveCollectionForType(typeKey: string, defaultCollection: string) =
        match this.TypeCollections.TryGetValue(typeKey) with
        | false, _ -> defaultCollection
        | true, names when names.Count = 0 -> defaultCollection
        | true, names when names.Contains(defaultCollection) -> defaultCollection
        | true, names when names.Count = 1 -> names |> Seq.head
        | true, _ ->
            raise (System.InvalidOperationException(
                $"Error: Ambiguous collection mapping for relation target type '{typeKey}'.\nReason: Multiple collections are registered for this type.\nFix: Register exactly one target collection for relation-backed queries."))
