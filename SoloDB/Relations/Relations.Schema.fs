
module internal SoloDatabase.RelationsSchema

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SQLiteTools
open RelationsTypes

let internal ensureRelationCatalogTable (connection: SqliteConnection) =
    ignore (connection.Execute("""
CREATE TABLE IF NOT EXISTS SoloDBRelation (
    Id INTEGER PRIMARY KEY,
    Name TEXT NOT NULL UNIQUE,
    SourceCollection TEXT NOT NULL,
    TargetCollection TEXT NOT NULL,
    PropertyName TEXT NOT NULL,
    OwnerCollection TEXT NOT NULL,
    RefKind TEXT NOT NULL CHECK(RefKind IN ('Single','Many')),
    OnDelete TEXT NOT NULL DEFAULT 'Restrict' CHECK(OnDelete IN ('Restrict','Cascade','Unlink')),
    OnOwnerDelete TEXT NOT NULL DEFAULT 'Deletion' CHECK(OnOwnerDelete IN ('Restrict','Unlink','Deletion')),
    IsUnique INTEGER NOT NULL DEFAULT 0,
    UNIQUE(OwnerCollection, PropertyName)
) STRICT;
"""))

let internal getSQLForTriggersForTable (name: string) =
    RelationsSharedSql.getSQLForTriggersForTable name

let internal ensureCollectionMetadataColumn (connection: SqliteConnection) (tableName: string) =
    let normalizedTableName = Helper.normalizeCatalogNameOrThrow "relation metadata ensure" tableName
    let qTable = quoteIdentifier normalizedTableName
    let tableLit = Helper.sqlLiteralEscape normalizedTableName
    let hasMetadata =
        connection.QueryFirst<int64>(
            $"SELECT CASE WHEN EXISTS (SELECT 1 FROM pragma_table_info('{tableLit}') WHERE name = 'Metadata') THEN 1 ELSE 0 END") = 1L
    if not hasMetadata then
        connection.Execute($"ALTER TABLE {qTable} ADD COLUMN Metadata JSONB NOT NULL DEFAULT '{{}}';") |> ignore

let internal ensureCollectionTableExists (connection: SqliteConnection) (tableName: string) =
    let tableName = Helper.normalizeCatalogNameOrThrow "relation table ensure" tableName
    let qTable = quoteIdentifier tableName
    let exists =
        connection.QueryFirst<int64>("SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END", {| name = tableName |}) = 1L

    if not exists then
        connection.Execute(RelationsSharedSql.createCollectionTableSql qTable) |> ignore
        connection.Execute(getSQLForTriggersForTable tableName) |> ignore

    connection.Execute(
        "INSERT INTO SoloDBCollections(Name) VALUES (@name) ON CONFLICT(Name) DO NOTHING;",
        {| name = tableName |}) |> ignore

let private tryGetSoloIdProperty (targetType: Type) =
    let props =
        targetType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.filter (fun p -> not (isNull (p.GetCustomAttribute<SoloId>(true))))
    match props with
    | [||] -> ValueNone
    | [| p |] -> ValueSome p
    | _ ->
        raise (InvalidOperationException(
            $"Error: Invalid [SoloId] declaration on '{targetType.FullName}'.\nReason: Multiple [SoloId] properties are not supported.\nFix: Keep exactly one [SoloId] property on relation targets."))

let rec private containsNestedRelationRefType (t: Type) =
    if isNull t then false
    elif DBRefTypeHelpers.isAnyRelationRefType t then true
    elif t.IsArray then
        containsNestedRelationRefType (t.GetElementType())
    elif t.IsGenericType then
        t.GetGenericArguments() |> Array.exists containsNestedRelationRefType
    else
        false

let private validateRelationTargetType (ownerType: Type) (prop: PropertyInfo) (targetType: Type) =
    if targetType.IsInterface then
        raise (InvalidOperationException(
            $"Error: Invalid relation target on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' is an interface.\nFix: Use a concrete class with a writable int64 Id property."))
    elif targetType.IsAbstract then
        raise (InvalidOperationException(
            $"Error: Invalid relation target on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' is abstract.\nFix: Use a concrete class with a writable int64 Id property."))
    else
        getWritableInt64IdPropertyOrThrow targetType |> ignore

let private buildRelationSpecs (ownerType: Type) =
    ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
    |> Array.choose (fun prop ->
        if not prop.CanRead then
            None
        else
            let propType = prop.PropertyType
            if DBRefTypeHelpers.isOptionWrappedRelationRefType propType then
                raise (InvalidOperationException(
                    $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Option-wrapped DBRef/DBRefMany is not supported.\nFix: Use DBRef<T>/DBRef<TTarget,'TId>/DBRefMany<T>/DBRefMany<TTarget,'TId> directly (non-option)."))
            elif DBRefTypeHelpers.isAnyRelationRefType propType then
                if not prop.CanWrite then
                    raise (InvalidOperationException(
                        $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Relation properties must be writable.\nFix: Add a public setter, or remove relation type from this property."))
                else
                    let generic = propType.GetGenericTypeDefinition()
                    let args = propType.GetGenericArguments()
                    let targetType = args.[0]
                    let typedIdType =
                        if DBRefTypeHelpers.isDBRefTypedDefinition generic || DBRefTypeHelpers.isDBRefManyTypedDefinition generic then ValueSome args.[1]
                        else ValueNone
                    let attr = prop.GetCustomAttribute<SoloRefAttribute>(true)
                    let onDelete = if isNull attr then DeletePolicy.Restrict else attr.OnDelete
                    let onOwnerDelete = if isNull attr then DeletePolicy.Deletion else attr.OnOwnerDelete
                    let isUnique = not (isNull attr) && attr.Unique
                    let kind = if DBRefTypeHelpers.isDBRefManyDefinition generic then Many else Single

                    validateRelationTargetType ownerType prop targetType

                    match kind, onOwnerDelete with
                    | Single, DeletePolicy.Cascade
                    | Many, DeletePolicy.Cascade ->
                        raise (InvalidOperationException(
                            $"Error: Invalid relation policy on {ownerType.FullName}.{prop.Name}.\nReason: OnOwnerDelete cannot be Cascade.\nFix: Use Deletion instead."))
                    | Single, _
                    | Many, _ -> ()

                    match typedIdType with
                    | ValueSome idType ->
                        match tryGetSoloIdProperty targetType with
                        | ValueNone ->
                            raise (InvalidOperationException(
                                $"Error: Invalid typed relation on {ownerType.FullName}.{prop.Name}.\nReason: Target type '{targetType.FullName}' has no [SoloId] property.\nFix: Add exactly one [SoloId] property with type '{idType.FullName}'."))
                        | ValueSome soloIdProp when soloIdProp.PropertyType <> idType ->
                            raise (InvalidOperationException(
                                $"Error: Invalid typed relation on {ownerType.FullName}.{prop.Name}.\nReason: DBRef id type '{idType.FullName}' does not match target [SoloId] type '{soloIdProp.PropertyType.FullName}'.\nFix: Align DBRef<'TTarget,'TId> with target [SoloId] property type."))
                        | ValueSome _ -> ()
                    | ValueNone -> ()

                    let orderBy = if isNull attr then DBRefOrder.Undefined else attr.OrderBy
                    Some (prop, kind, targetType, typedIdType, onDelete, onOwnerDelete, isUnique, orderBy)
            elif containsNestedRelationRefType propType then
                raise (InvalidOperationException(
                    $"Error: Invalid relation property '{ownerType.FullName}.{prop.Name}'.\nReason: Container-wrapped relation-like shapes are not supported.\nFix: Use direct DBRef<T>/DBRef<TTarget,'TId>/DBRefMany<T>/DBRefMany<TTarget,'TId> property types."))
            else
                None)

let internal getRelationSpecs (ownerType: Type) =
    relationSpecsCache.GetOrAdd(ownerType, Func<_, _>(buildRelationSpecs))

let internal hasManyBackReference (ownerType: Type) (targetType: Type) =
    getRelationSpecs targetType
    |> Array.exists (fun (_, kind, candidateTargetType, _, _, _, _, _) ->
        kind = Many && candidateTargetType = ownerType)

let private tryGetStoredTargetCollection (connection: SqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    connection.QueryFirstOrDefault<string>(
        """
SELECT TargetCollection
FROM SoloDBRelation
WHERE OwnerCollection = @ownerCollection AND PropertyName = @propertyName
LIMIT 1;
""",
        {| ownerCollection = ownerTable; propertyName = propertyName |})
    |> Option.ofObj

let internal tryGetStoredRelationName (connection: SqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    connection.QueryFirstOrDefault<string>(
        """
SELECT Name
FROM SoloDBRelation
WHERE OwnerCollection = @ownerCollection AND PropertyName = @propertyName
LIMIT 1;
""",
        {| ownerCollection = ownerTable; propertyName = propertyName |})
    |> Option.ofObj

let private readMappedCollectionsForType (connection: SqliteConnection) (targetType: Type) =
    connection.Query<string>(
        "SELECT CollectionName FROM SoloDBTypeCollectionMap WHERE TypeKey = @typeKey;",
        {| typeKey = Utils.typeIdentityKey targetType |})
    |> Seq.map formatName
    |> Seq.distinct
    |> Seq.toArray

let private collectionExistsByName (connection: SqliteConnection) (collectionName: string) =
    connection.QueryFirst<int64>(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM SoloDBCollections WHERE Name = @name) THEN 1 ELSE 0 END",
        {| name = collectionName |}) = 1L

let internal sqliteTableExistsByName (connection: SqliteConnection) (tableName: string) =
    connection.QueryFirst<int64>(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
        {| name = tableName |}) = 1L

let internal resolveTargetCollectionName (connection: SqliteConnection) (ownerTable: string) (propertyName: string) (targetType: Type) =
    let ownerTable = formatName ownerTable
    let defaultTable = formatName targetType.Name

    match tryGetStoredTargetCollection connection ownerTable propertyName with
    | Some stored when not (String.IsNullOrWhiteSpace stored) ->
        formatName stored
    | _ ->
        let mapped = readMappedCollectionsForType connection targetType
        if mapped |> Array.contains defaultTable then
            defaultTable
        elif collectionExistsByName connection defaultTable then
            defaultTable
        elif mapped.Length = 1 then
            mapped.[0]
        elif mapped.Length > 1 then
            let mappedList = String.Join(", ", mapped)
            raise (InvalidOperationException(
                $"Error: Ambiguous target collection mapping for relation {ownerTable}.{propertyName} and type '{targetType.FullName}'.\nReason: Multiple collections are mapped ({mappedList}).\nFix: Register exactly one target collection for this type."))
        else
            defaultTable

let private br04Message (ownerType: Type) (ownerTable: string) (linkTable: string) (propNames: string array) =
    let props = String.Join(",", propNames)
    $"Error: contradictory shared-many topology detected for '{ownerType.FullName}'.\nReason: Multiple DBRefMany properties ({props}) on collection '{ownerTable}' resolve to the same link table '{linkTable}'.\nFix: ensure each DBRefMany relation resolves to a distinct link table or redesign the relation topology."

let private validateSharedManyContradictions (ownerType: Type) (ownerTable: string) (descriptors: RelationDescriptor array) =
    descriptors
    |> Array.filter (fun d -> d.Kind = Many)
    |> Array.groupBy (fun d -> d.LinkTable)
    |> Array.iter (fun (linkTable, grp) ->
        if grp.Length > 1 then
            let propNames = grp |> Array.map (fun d -> d.Property.Name) |> Array.sort
            raise (InvalidOperationException(br04Message ownerType ownerTable linkTable propNames)))

let private validateConflictingPolicies (ownerType: Type) (descriptors: RelationDescriptor array) =
    descriptors
    |> Array.groupBy (fun d -> d.Kind, d.TargetType)
    |> Array.iter (fun ((kind, targetType), grp) ->
        if grp.Length > 1 then
            let variants =
                grp
                |> Array.map (fun d -> d.OnDelete, d.OnOwnerDelete, d.IsUnique)
                |> Array.distinct
            if variants.Length > 1 then
                let propNames =
                    grp
                    |> Array.map (fun d -> d.Property.Name)
                    |> Array.sort
                let propNames = String.Join(", ", propNames)
                let kindName = if kind = Single then "DBRef" else "DBRefMany"
                raise (InvalidOperationException(
                    $"Error: Conflicting relation policies detected on '{ownerType.FullName}'.\nReason: {kindName} properties ({propNames}) targeting '{targetType.FullName}' do not agree on delete-policy settings.\nFix: Use consistent OnDelete/OnOwnerDelete/Unique settings for relations to the same target type."))
        )

let private validateCrossKindConflictingPolicies (ownerType: Type) (descriptors: RelationDescriptor array) =
    descriptors
    |> Array.groupBy (fun d -> d.TargetType)
    |> Array.iter (fun (targetType, grp) ->
        if grp.Length > 1 then
            let kinds =
                grp
                |> Array.map _.Kind
                |> Array.distinct
            if kinds.Length > 1 then
                let variants =
                    grp
                    |> Array.map (fun d -> d.OnDelete, d.OnOwnerDelete, d.IsUnique)
                    |> Array.distinct
                if variants.Length > 1 then
                    let propNames =
                        grp
                        |> Array.map (fun d -> d.Property.Name)
                        |> Array.sort
                    let propNames = String.Join(", ", propNames)
                    raise (InvalidOperationException(
                        $"Error: Conflicting relation policies detected on '{ownerType.FullName}'.\nReason: Mixed DBRef/DBRefMany properties ({propNames}) targeting '{targetType.FullName}' do not agree on delete-policy settings.\nFix: Use consistent OnDelete/OnOwnerDelete/Unique settings for relations to the same target type."))
        )

let internal buildRelationDescriptors (tx: RelationTxContext) (ownerType: Type) =
    let ownerTable = formatName tx.OwnerTable
    let descriptors =
        getRelationSpecs ownerType
        |> Array.map (fun (prop, kind, targetType, typedIdType, onDelete, onOwnerDelete, isUnique, orderBy) ->
        let targetTable = resolveTargetCollectionName tx.Connection ownerTable prop.Name targetType
        let defaultRelName = relationName ownerTable prop.Name
        let canonicalManyName = canonicalManyRelationName ownerTable targetTable
        let isMutualMany = kind = Many && hasManyBackReference ownerType targetType
        let proposedRelName = defaultRelName
        let relName =
            match tryGetStoredRelationName tx.Connection ownerTable prop.Name with
            | Some stored when not (String.IsNullOrWhiteSpace stored) -> formatName stored
            | _ -> proposedRelName

        let canonicalSharedLinkTable = linkTableFromRelationName canonicalManyName
        let isSharedMany =
            kind = Many
            && (isMutualMany || sqliteTableExistsByName tx.Connection canonicalSharedLinkTable)
        let ownerUsesSourceColumn =
            if isSharedMany then
                StringComparer.Ordinal.Compare(ownerTable, targetTable) <= 0
            else
                true
        let linkSourceTable, linkTargetTable =
            if ownerUsesSourceColumn then ownerTable, targetTable
            else targetTable, ownerTable

        {
            OwnerTable = ownerTable
            OwnerType = ownerType
            Property = prop
            PropertyPath = prop.Name
            Kind = kind
            TargetType = targetType
            TargetTable = targetTable
            LinkSourceTable = linkSourceTable
            LinkTargetTable = linkTargetTable
            OwnerUsesSourceColumn = ownerUsesSourceColumn
            RelationName = relName
            LinkTable = if isSharedMany then canonicalSharedLinkTable else linkTableFromRelationName relName
            OnDelete = onDelete
            OnOwnerDelete = onOwnerDelete
            IsUnique = isUnique
            OrderBy = orderBy
            TypedIdType = typedIdType
            TargetSoloIdProperty = tryGetSoloIdProperty targetType
        })

    validateSharedManyContradictions ownerType ownerTable descriptors
    validateConflictingPolicies ownerType descriptors
    validateCrossKindConflictingPolicies ownerType descriptors
    descriptors

let private br05Message (ownerTable: string) (propertyName: string) (phase: string) =
    $"Error: relation metadata missing for '{ownerTable}.{propertyName}'.\nReason: prior relation evidence exists and auto-heal is not safe (phase={phase}).\nFix: rebuild/repair relation metadata and link-table state before retrying."

let private hasCatalogRow (connection: SqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    connection.QueryFirst<int64>(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM SoloDBRelation WHERE OwnerCollection = @owner AND PropertyName = @prop) THEN 1 ELSE 0 END",
        {| owner = ownerTable; prop = propertyName |}) = 1L

/// Returns true when a SoloDBRelation row exists involving the same two
/// collections (in either direction) from a *different* (OwnerCollection, PropertyName)
/// pair. This identifies the shared-many bootstrap case where the other side of a
/// mutual M:N created the link table first.
let private linkTableOwnedByOtherProperty (connection: SqliteConnection) (descriptor: RelationDescriptor) =
    ensureRelationCatalogTable connection
    let a = descriptor.OwnerTable
    let b = descriptor.TargetTable
    connection.QueryFirst<int64>(
        """
SELECT CASE WHEN EXISTS (
    SELECT 1 FROM SoloDBRelation
    WHERE ((SourceCollection = @a AND TargetCollection = @b)
        OR (SourceCollection = @b AND TargetCollection = @a))
      AND NOT (OwnerCollection = @owner AND PropertyName = @prop)
) THEN 1 ELSE 0 END
""",
        {| a = a; b = b; owner = a; prop = descriptor.Property.Name |}) = 1L

/// Prevent metadata resurrection when prior relation evidence exists.
/// The link table is the definitive evidence that a relation was previously established.
/// Target collections can exist independently (they store their own entities) and must not
/// be treated as relation evidence. For shared-many relations, the link table may have been
/// created by the other side of a mutual M:N — this is normal bootstrap, not deletion.
let internal ensureMetadataNotResurrected (tx: RelationTxContext) (descriptor: RelationDescriptor) =
    let ownerExists = collectionExistsByName tx.Connection descriptor.OwnerTable
    if ownerExists then
        let linkExists = sqliteTableExistsByName tx.Connection descriptor.LinkTable
        if linkExists && not (hasCatalogRow tx.Connection descriptor.OwnerTable descriptor.Property.Name) then
            // Edge case: shared-many link table created by the other side of a mutual M:N.
            // If another (OwnerCollection, PropertyName) pair already owns this link table,
            // the current property is being bootstrapped for the first time — not resurrected.
            if not (linkTableOwnedByOtherProperty tx.Connection descriptor) then
                raise (InvalidOperationException(br05Message descriptor.OwnerTable descriptor.Property.Name "build"))

/// Detects evolution conflicts between the current descriptor and the stored catalog row.
/// Raises on target-type mismatch or Many→Single kind narrowing.
/// Performs atomic constraint migration for Single→Many kind widening.
let private detectAndMigrateEvolutionConflicts (tx: RelationTxContext) (descriptor: RelationDescriptor) =
    ensureRelationCatalogTable tx.Connection
    let storedRaw =
        tx.Connection.QueryFirstOrDefault<{| RefKind: string; TargetCollection: string |}>(
            "SELECT RefKind, TargetCollection FROM SoloDBRelation WHERE OwnerCollection = @owner AND PropertyName = @prop LIMIT 1;",
            {| owner = descriptor.OwnerTable; prop = descriptor.Property.Name |})
    let stored =
        if isNull (box storedRaw) then
            ValueNone
        else
            ValueSome {| Kind = stringToRelationKind storedRaw.RefKind; TargetCollection = storedRaw.TargetCollection |}

    match stored with
    | ValueNone -> ()
    | ValueSome stored ->
        // Target-type-change detection
        if not (StringComparer.OrdinalIgnoreCase.Equals(stored.TargetCollection, descriptor.TargetTable)) then
            raise (InvalidOperationException(
                $"Error: relation target type changed for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                $"Reason: stored target is '{stored.TargetCollection}' but current type resolves to '{descriptor.TargetTable}'. " +
                "Changing the target type of an existing relation is not supported because the link table's foreign keys reference the original target.\n" +
                "Fix: drop the collection or clear all relation data before changing the target type."))

        // Kind-change detection
        let storedKind = stored.Kind
        if storedKind <> descriptor.Kind then
            let linkTable = descriptor.LinkTable
            let linkExists = sqliteTableExistsByName tx.Connection linkTable
            match storedKind, descriptor.Kind with
            | Single, Many when linkExists ->
                // Forward migration: Single → Many (supported)
                // Existing single-link data satisfies Many constraint (subset).
                // Atomic: drop UNIQUE(SourceId) index, recreate table constraints via
                // standard SQLite table-recreate pattern inside the current transaction.
                let qLink = quoteIdentifier linkTable
                let qSource = quoteIdentifier descriptor.LinkSourceTable
                let qTarget = quoteIdentifier descriptor.LinkTargetTable
                let tmpTable = linkTable + "_migration_tmp"
                let qTmp = quoteIdentifier tmpTable
                // Capture pre-migration row count for invariant check
                let preCount = tx.Connection.QueryFirst<int64>($"SELECT COUNT(*) FROM {qLink};")
                tx.Connection.Execute(
                    $"CREATE TABLE {qTmp} (" +
                    "Id INTEGER PRIMARY KEY, " +
                    "SourceId INTEGER NOT NULL, " +
                    "TargetId INTEGER NOT NULL, " +
                    $"FOREIGN KEY (SourceId) REFERENCES {qSource}(Id) ON DELETE RESTRICT, " +
                    $"FOREIGN KEY (TargetId) REFERENCES {qTarget}(Id) ON DELETE RESTRICT, " +
                    "UNIQUE(SourceId, TargetId)" +
                    ") STRICT;") |> ignore
                tx.Connection.Execute($"INSERT INTO {qTmp}(Id, SourceId, TargetId) SELECT Id, SourceId, TargetId FROM {qLink};") |> ignore
                // Post-insert invariant: row count must match (no duplicates lost)
                let postCount = tx.Connection.QueryFirst<int64>($"SELECT COUNT(*) FROM {qTmp};")
                if preCount <> postCount then
                    // Abort migration: duplicate (SourceId, TargetId) pairs in source data
                    tx.Connection.Execute($"DROP TABLE {qTmp};") |> ignore
                    raise (InvalidOperationException(
                        $"Error: forward migration failed for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                        $"Reason: source link table '{linkTable}' contains {preCount} rows but migration target accepted only {postCount}. " +
                        "This indicates duplicate (SourceId, TargetId) pairs in the existing Single-relation data.\n" +
                        "Fix: resolve duplicate link rows before retrying the migration."))
                tx.Connection.Execute($"DROP TABLE {qLink};") |> ignore
                tx.Connection.Execute($"ALTER TABLE {qTmp} RENAME TO {qLink};") |> ignore
                // Recreate indexes
                let sourceIdx = formatName($"IX_{linkTable}_Source")
                let targetIdx = formatName($"IX_{linkTable}_Target")
                tx.Connection.Execute($"CREATE INDEX IF NOT EXISTS {quoteIdentifier sourceIdx} ON {qLink}(SourceId);") |> ignore
                tx.Connection.Execute($"CREATE INDEX IF NOT EXISTS {quoteIdentifier targetIdx} ON {qLink}(TargetId);") |> ignore
                // Post-migration invariant verification
                // 1. Row count preserved
                let finalCount = tx.Connection.QueryFirst<int64>($"SELECT COUNT(*) FROM {qLink};")
                if finalCount <> preCount then
                    raise (InvalidOperationException(
                        $"Error: forward migration invariant violation for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                        $"Reason: expected {preCount} rows after migration but found {finalCount} in '{linkTable}'.\n" +
                        "Fix: this indicates a migration defect. Report this error."))
                // 2. Verify exact unique index shape through pragma metadata.
                let uniqueIndexNames =
                    let tableLit = Helper.sqlLiteralEscape linkTable
                    try
                        tx.Connection.Query<string>($"SELECT name FROM pragma_index_list('{tableLit}') WHERE \"unique\" = 1;")
                        |> Seq.toArray
                    with :? SqliteException ->
                        tx.Connection.Query<{| name: string; ``unique``: int64 |}>($"PRAGMA index_list('{tableLit}');")
                        |> Seq.filter (fun idx -> idx.``unique`` = 1L)
                        |> Seq.map (fun idx -> idx.name)
                        |> Seq.toArray

                let uniqueColumnSets =
                    uniqueIndexNames
                    |> Array.map (fun indexName ->
                        let indexLit = Helper.sqlLiteralEscape indexName
                        try
                            tx.Connection.Query<{| name: string; seqno: int64 |}>($"SELECT name, seqno FROM pragma_index_info('{indexLit}');")
                            |> Seq.sortBy (fun row -> row.seqno)
                            |> Seq.map (fun row -> row.name)
                            |> Seq.toArray
                        with :? SqliteException ->
                            tx.Connection.Query<{| name: string; seqno: int64 |}>($"PRAGMA index_info('{indexLit}');")
                            |> Seq.sortBy (fun row -> row.seqno)
                            |> Seq.map (fun row -> row.name)
                            |> Seq.toArray)

                let hasExactSourceTargetUnique =
                    uniqueColumnSets
                    |> Array.exists (fun cols ->
                        cols.Length = 2 &&
                        ((cols.[0] = "SourceId" && cols.[1] = "TargetId")
                         || (cols.[0] = "TargetId" && cols.[1] = "SourceId")))

                if not hasExactSourceTargetUnique then
                    raise (InvalidOperationException(
                        $"Error: forward migration invariant violation for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                        $"Reason: migrated table '{linkTable}' is missing exact UNIQUE(SourceId,TargetId) constraint.\n" +
                        "Fix: this indicates a migration defect. Report this error."))

                let hasSourceOnlyUnique =
                    uniqueColumnSets
                    |> Array.exists (fun cols -> cols.Length = 1 && cols.[0] = "SourceId")

                if hasSourceOnlyUnique then
                    raise (InvalidOperationException(
                        $"Error: forward migration invariant violation for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                        $"Reason: migrated table '{linkTable}' still has obsolete UNIQUE(SourceId) constraint from Single cardinality.\n" +
                        "Fix: this indicates a migration defect. Report this error."))
                // 4. Source and target indexes exist (I4)
                let idxCount =
                    tx.Connection.QueryFirst<int64>(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND tbl_name = @tbl AND name IN (@src, @tgt);",
                        {| tbl = linkTable; src = sourceIdx; tgt = targetIdx |})
                if idxCount <> 2L then
                    raise (InvalidOperationException(
                        $"Error: forward migration invariant violation for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                        $"Reason: migrated table '{linkTable}' is missing expected indexes (found {idxCount}/2).\n" +
                        "Fix: this indicates a migration defect. Report this error."))
            | Many, Single ->
                // Reverse migration: Many → Single (not supported)
                raise (InvalidOperationException(
                    $"Error: relation kind narrowing not supported for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                    $"Reason: stored relation is 'Many' but current type declares 'Single'. " +
                    "Existing multi-target link rows would violate the Single cardinality constraint.\n" +
                    "Fix: drop the collection or clear all relation data before narrowing from DBRefMany to DBRef."))
            | Single, Many ->
                // No existing link table; ensureRelationSchema CREATE TABLE will handle it.
                ()
            | Single, Single
            | Many, Many ->
                // Same kind — filtered by outer guard, but explicit for compiler exhaustiveness.
                ()

let internal ensureRelationSchema (tx: RelationTxContext) (descriptor: RelationDescriptor) =
    ensureCollectionTableExists tx.Connection descriptor.TargetTable
    detectAndMigrateEvolutionConflicts tx descriptor

    match descriptor.TypedIdType, descriptor.TargetSoloIdProperty with
    | ValueSome _, ValueSome soloIdProp ->
        let needle = $"jsonb_extract(value,'$.{soloIdProp.Name}')".ToLowerInvariant()
        let hasUniqueSoloIdIndex =
            tx.Connection.QueryFirst<int64>(
                """
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM sqlite_master
    WHERE type = 'index'
      AND tbl_name = @tableName
      AND sql IS NOT NULL
      AND instr(lower(sql), 'create unique index') > 0
      AND instr(replace(replace(replace(replace(lower(sql), ' ', ''), char(10), ''), char(9), ''), '"', ''), @needle) > 0
) THEN 1 ELSE 0 END;
""",
                {| tableName = descriptor.TargetTable; needle = needle |}) = 1L
        if not hasUniqueSoloIdIndex then
            raise (InvalidOperationException(
                $"Error: Missing required unique index for typed relation '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\nReason: Typed-id resolver requires unique index on jsonb_extract(Value, '$.{soloIdProp.Name}') in target collection '{descriptor.TargetTable}'.\nFix: Add [SoloId] or EnsureUniqueAndIndex for this path before using DBRef<'TTarget,'TId>/DBRefMany<'TTarget,'TId>."))
    | ValueSome _, ValueNone
    | ValueNone, _ -> ()

    let qLink = quoteIdentifier descriptor.LinkTable
    let qSource = quoteIdentifier descriptor.LinkSourceTable
    let qTarget = quoteIdentifier descriptor.LinkTargetTable
    let constraints =
        match descriptor.Kind with
        | Single when descriptor.IsUnique ->
            "UNIQUE(SourceId), UNIQUE(TargetId)"
        | Single ->
            "UNIQUE(SourceId)"
        | Many ->
            "UNIQUE(SourceId, TargetId)"

    ignore (tx.Connection.Execute($"""
CREATE TABLE IF NOT EXISTS {qLink} (
    Id INTEGER PRIMARY KEY,
    SourceId INTEGER NOT NULL,
    TargetId INTEGER NOT NULL,
    FOREIGN KEY (SourceId) REFERENCES {qSource}(Id) ON DELETE RESTRICT,
    FOREIGN KEY (TargetId) REFERENCES {qTarget}(Id) ON DELETE RESTRICT,
    {constraints}
) STRICT;
"""))

    let sourceIdx = formatName($"IX_{descriptor.LinkTable}_Source")
    let targetIdx = formatName($"IX_{descriptor.LinkTable}_Target")
    tx.Connection.Execute($"CREATE INDEX IF NOT EXISTS {quoteIdentifier sourceIdx} ON {qLink}(SourceId);") |> ignore
    tx.Connection.Execute($"CREATE INDEX IF NOT EXISTS {quoteIdentifier targetIdx} ON {qLink}(TargetId);") |> ignore

    tx.Connection.Execute(
        """
INSERT INTO SoloDBRelation(Name, SourceCollection, TargetCollection, PropertyName, OwnerCollection, RefKind, OnDelete, OnOwnerDelete, IsUnique)
VALUES (@name, @sourceCollection, @targetCollection, @propertyName, @ownerCollection, @refKind, @onDelete, @onOwnerDelete, @isUnique)
ON CONFLICT(OwnerCollection, PropertyName) DO UPDATE SET
    Name = excluded.Name,
    SourceCollection = excluded.SourceCollection,
    TargetCollection = excluded.TargetCollection,
    RefKind = excluded.RefKind,
    OnDelete = excluded.OnDelete,
    OnOwnerDelete = excluded.OnOwnerDelete,
    IsUnique = excluded.IsUnique;
""",
        {|
            name = descriptor.RelationName
            sourceCollection = descriptor.OwnerTable
            targetCollection = descriptor.TargetTable
            propertyName = descriptor.Property.Name
            ownerCollection = descriptor.OwnerTable
            refKind = relationKindToString descriptor.Kind
            onDelete = descriptor.OnDelete.ToString()
            onOwnerDelete = descriptor.OnOwnerDelete.ToString()
            isUnique = if descriptor.IsUnique then 1 else 0
        |}) |> ignore

/// Read-only pre-flight check: returns true when the full write-locked ensure path
/// is needed for an existing collection. Avoids BEGIN IMMEDIATE contention on
/// concurrent readers during in-flight write transactions.
let internal relationSchemaRequiresEnsure (connection: SqliteConnection) (ownerTable: string) (ownerType: Type) =
    let ownerTable = formatName ownerTable
    // If catalog table doesn't exist at all, ensure is needed.
    let catalogExists =
        connection.QueryFirst<int64>(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'SoloDBRelation') THEN 1 ELSE 0 END") = 1L
    if not catalogExists then true
    else

    let specs = getRelationSpecs ownerType
    if specs.Length = 0 then false
    else

    // Load all stored catalog rows for this owner.
    let storedRows =
        connection.Query<{| PropertyName: string; RefKind: string; TargetCollection: string; OnDelete: string; OnOwnerDelete: string; IsUnique: int64 |}>(
            "SELECT PropertyName, RefKind, TargetCollection, OnDelete, OnOwnerDelete, IsUnique FROM SoloDBRelation WHERE OwnerCollection = @owner;",
            {| owner = ownerTable |})
        |> Seq.toArray
    let storedByProp = Dictionary<string, {| PropertyName: string; RefKind: string; TargetCollection: string; OnDelete: string; OnOwnerDelete: string; IsUnique: int64 |}>(StringComparer.Ordinal)
    for row in storedRows do
        storedByProp.[row.PropertyName] <- row

    // Check each current spec against stored state.
    let mutable needsEnsure = false
    for (prop, kind, targetType, _typedIdType, onDelete, onOwnerDelete, isUnique, _orderBy) in specs do
        if not needsEnsure then
            match storedByProp.TryGetValue prop.Name with
            | false, _ ->
                // Missing catalog row — check for BR-05 (link table without catalog = resurrection).
                // Either way, ensure must run: to bootstrap or to reject.
                needsEnsure <- true
            | true, stored ->
                // Check for evolution: kind change, target change, policy drift.
                let expectedKind = relationKindToString kind
                let expectedTarget =
                    resolveTargetCollectionName connection ownerTable prop.Name targetType
                let expectedOnDelete = onDelete.ToString()
                let expectedOnOwnerDelete = onOwnerDelete.ToString()
                let expectedIsUnique = if isUnique then 1L else 0L
                if stored.RefKind <> expectedKind
                   || not (StringComparer.OrdinalIgnoreCase.Equals(stored.TargetCollection, expectedTarget))
                   || stored.OnDelete <> expectedOnDelete
                   || stored.OnOwnerDelete <> expectedOnOwnerDelete
                   || stored.IsUnique <> expectedIsUnique then
                    needsEnsure <- true
                storedByProp.Remove prop.Name |> ignore

    // Any remaining stored rows are orphans (removed properties) — need ensure for cleanup.
    if not needsEnsure && storedByProp.Count > 0 then
        needsEnsure <- true

    needsEnsure
