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
}
    with
    /// Create a single-source context (backward-compatible default).
    static member SingleSource(tableName: string) =
        { RootTable = tableName
          RootGraph = QueryRootGraph.Single(tableName)
          Joins = ResizeArray()
          AliasCounter = 0
          ExcludedPaths = HashSet() }

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
          ExcludedPaths = HashSet() }

    /// Generate a unique alias (_ref0, _ref1, ...).
    member this.NextAlias() =
        let n = this.AliasCounter
        this.AliasCounter <- n + 1
        sprintf "_ref%d" n

    /// Find existing join for a property path, or None (deduplication).
    member this.FindJoin(propertyPath: string) =
        this.Joins |> Seq.tryFind (fun j -> j.PropertyPath = propertyPath)

    /// Resolve or create an additional query root for multi-source planning.
    member this.ResolveRoot(sourceKey: string, tableName: string) =
        this.RootGraph.ResolveRoot(sourceKey, tableName)

    member this.TryFindRootByAlias(alias: string) =
        this.RootGraph.TryFindByAlias(alias)

    /// The root table's "TableName". prefix for backward-compat with existing QueryBuilder usage.
    member this.RootTableNameDot =
        if System.String.IsNullOrEmpty this.RootTable then System.String.Empty
        else "\"" + this.RootTable + "\"."
