namespace SoloDatabase

open System.Collections.Generic
open NativeSQLiteTools
open SoloDatabase.SqlModel

/// Closed-enum kinds for translator-emitted runtime errors. The kind is encoded
/// into the typed-payload JSON object on Id=NULL rows; the materializer dispatches
/// by kind to the matching .NET exception type. Adding a new kind requires
/// JsonFunctions.fromSQLite (Id=NULL dispatch arm) and runtimeErrorPayload (kind name)
/// updates in lockstep.
type internal RuntimeErrorKind =
    | CardinalityError
    | CastError
    | RangeError

/// Layout constants are sampled once at their original producer position.
type internal QueryPreparedConstant =
    | PreparedConstant of obj
    | DeferredConstant of complete: ((obj -> QueryProducedValue) -> QueryProducedValue)

/// Compilation owns value evaluation; the translator owns SQL semantics.
type internal QueryValueBinder = {
    Order: QueryValueBindingOrder
    PrepareConstant: System.Linq.Expressions.Expression -> System.Type -> QueryPreparedConstant
    PrepareList: (System.Linq.Expressions.Expression -> ((obj -> SqlExpr list) -> SqlExpr)) voption
    PrepareChoice: (System.Linq.Expressions.Expression -> ((obj -> bool) -> SqlExpr)) voption
    Produced: bool -> System.Linq.Expressions.Expression -> QueryProducedValue
    IsValue: System.Linq.Expressions.Expression -> bool
    Parameter: System.Linq.Expressions.Expression -> SqlExpr
    Comparison: System.Linq.Expressions.Expression -> SqlExpr
    Scalar: System.Linq.Expressions.Expression -> SqlExpr
    PreparedScalar: System.Linq.Expressions.Expression -> SqlExpr
    Prefix: System.Linq.Expressions.Expression -> SqlExpr
    Local: System.Linq.Expressions.Expression -> System.Linq.Expressions.Expression
} with
    member this.TryBind comparison expression =
        if not (this.IsValue expression) then ValueNone
        else ValueSome(this.Produced comparison expression)


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
            let alias = "_src" + this.SourceAliasCounter.ToString(System.Globalization.CultureInfo.InvariantCulture)
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
    /// Canonical target payload from its actual document or filesystem producer.
    TargetValue: SqlExpr
    /// The authored reference input carries its actual serialized child identity.
    ReferenceValue: SqlExpr
    ReferencePath: JsonSerializator.JsonEqualitySupport.PathSegment list
    ReferenceOnOwnerRoot: bool
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
    /// Original writer input for this scope, independent of its SQL carrier.
    SerializationInput: QuerySerializationInput
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
    /// Resolves invocation values into SQL parameters during compiled translation.
    BindQueryValue: QueryValueBinder voption
    GeneralPrefixMatch: bool
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
          SerializationInput = QuerySerializationInput.ForType null
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
          GeneralPrefixMatch = false
          BindQueryValue = ValueNone
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
          SerializationInput = QuerySerializationInput.ForType null
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
          GeneralPrefixMatch = false
          BindQueryValue = ValueNone
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
        "_ref" + n.ToString(System.Globalization.CultureInfo.InvariantCulture)

    member this.TryFindJoinByAlias(alias: string) =
        this.Joins |> Seq.tryFind (fun j -> j.TargetAlias = alias)

    /// Create a subquery-scoped clone with isolated Joins but shared metadata dictionaries.
    /// Used by ForSubquery to prevent DBRef JOIN leakage from inner correlated subqueries to the outer scope.
    /// A fresh single-source context belonging to the same translation as `parent`.
    /// Inner roots created during translation (Join, GroupJoin, SelectMany, nested builders)
    /// must keep the parent's metadata authority; without it relation access through the child
    /// silently falls back to defaults.
    static member ChildOf(parent: QueryContext, tableName: string) =
        { QueryContext.SingleSource(tableName) with MetadataSource = parent.MetadataSource; BindQueryValue = parent.BindQueryValue; GeneralPrefixMatch = parent.GeneralPrefixMatch }

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

    /// Query navigation retains one owner-qualified target and fallback policy.
    member this.ResolveQueryRelationTarget(ownerCollection: string voption, propertyName: string, targetType: System.Type) =
        let mapped = ownerCollection |> Utils.valueOptionToOption |> Option.bind (fun owner -> this.TryResolveRelationTarget(owner, propertyName))
        match mapped with
        | Some mapped when not(System.String.IsNullOrWhiteSpace mapped) -> Utils.formatName mapped
        | _ -> this.ResolveCollectionForType(TypeMetadata.typeIdentityKey targetType, Utils.formatName targetType.Name)
