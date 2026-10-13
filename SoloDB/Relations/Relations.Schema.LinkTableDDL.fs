module internal SoloDatabase.RelationsSchemaLinkTableDDL

open System
open System.Reflection
open System.Collections.Generic
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open NativeSQLiteTools
open RelationsTypes
open RelationsSchemaBuilder
open RelationsSchemaValidator

let private metadataResurrectionMessage (ownerTable: string) (propertyName: string) (_phase: string) =
    $"Error: Relation metadata not found for property {propertyName} on collection {ownerTable}.\nReason: Existing relation evidence was found but automatic recovery is not safe.\nFix: Rebuild or repair relation metadata and link-table state before retrying."

let private hasCatalogRow (connection: NativeSqliteConnection) (ownerTable: string) (propertyName: string) =
    ensureRelationCatalogTable connection
    let mutable statement = connection.OpenStatement("SELECT EXISTS (SELECT 1 FROM SoloDBRelation WHERE OwnerCollection = @owner AND PropertyName = @prop)")
    try
        statement.BindText(1, ownerTable)
        statement.BindText(2, propertyName)
        statement.Step() |> ignore
        let exists = statement.Row.GetBoolean(0)
        statement.Dispose()
        exists
    with error -> statement.Abort(error); reraise()

/// Returns true when a SoloDBRelation row exists involving the same two
/// collections (in either direction) from a *different* (OwnerCollection, PropertyName)
/// pair. This identifies the shared-many bootstrap case where the other side of a
/// mutual M:N created the link table first.
let private linkTableOwnedByOtherProperty (connection: NativeSqliteConnection) (descriptor: RelationDescriptor) =
    ensureRelationCatalogTable connection
    let a = descriptor.OwnerTable
    let b = descriptor.TargetTable
    let mutable statement = connection.OpenStatement("""
SELECT EXISTS (
    SELECT 1 FROM SoloDBRelation
    WHERE ((SourceCollection = @a AND TargetCollection = @b)
        OR (SourceCollection = @b AND TargetCollection = @a))
      AND NOT (OwnerCollection = @owner AND PropertyName = @prop)
)
    """)
    try
        statement.BindText(1, a)
        statement.BindText(2, b)
        statement.BindText(3, a)
        statement.BindText(4, descriptor.Property.Name)
        statement.Step() |> ignore
        let exists = statement.Row.GetBoolean(0)
        statement.Dispose()
        exists
    with error -> statement.Abort(error); reraise()

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
                raise (InvalidOperationException(metadataResurrectionMessage descriptor.OwnerTable descriptor.Property.Name "build"))

/// Detects evolution conflicts between the current descriptor and the stored catalog row.
/// Raises on target-type mismatch or Many→Single kind narrowing.
/// Performs atomic constraint migration for Single→Many kind widening.
let private detectAndMigrateEvolutionConflicts (tx: RelationTxContext) (descriptor: RelationDescriptor) =
    ensureRelationCatalogTable tx.Connection
    let stored =
        let mutable statement = tx.Connection.OpenStatement("SELECT RefKind, TargetCollection FROM SoloDBRelation WHERE OwnerCollection = @owner AND PropertyName = @prop LIMIT 1;")
        try
            statement.BindText(1, descriptor.OwnerTable)
            statement.BindText(2, descriptor.Property.Name)
            let stored =
                if statement.Step() then
                    ValueSome {| Kind = stringToRelationKind (statement.Row.GetString(0)); TargetCollection = statement.Row.GetString(1) |}
                else ValueNone
            statement.Dispose()
            stored
        with error -> statement.Abort(error); reraise()

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
                let preCount =
                    let mutable statement = tx.Connection.OpenStatement($"SELECT COUNT(*) FROM {qLink};")
                    try
                        statement.Step() |> ignore
                        let count = statement.Row.GetInt64(0)
                        statement.Dispose()
                        count
                    with error -> statement.Abort(error); reraise()
                let mutable table = tx.Connection.OpenStatement(
                    $"CREATE TABLE {qTmp} (" +
                    "Id INTEGER PRIMARY KEY, " +
                    "SourceId INTEGER NOT NULL, " +
                    "TargetId INTEGER NOT NULL, " +
                    $"FOREIGN KEY (SourceId) REFERENCES {qSource}(Id) ON DELETE RESTRICT, " +
                    $"FOREIGN KEY (TargetId) REFERENCES {qTarget}(Id) ON DELETE RESTRICT, " +
                    "UNIQUE(SourceId, TargetId)" +
                    ") STRICT;")
                try
                    table.Step() |> ignore
                    table.Dispose()
                with error -> table.Abort(error); reraise()
                let mutable statement = tx.Connection.OpenStatement($"INSERT INTO {qTmp}(Id, SourceId, TargetId) SELECT Id, SourceId, TargetId FROM {qLink};")
                try
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
                // Post-insert invariant: row count must match (no duplicates lost)
                let postCount =
                    let mutable statement = tx.Connection.OpenStatement($"SELECT COUNT(*) FROM {qTmp};")
                    try
                        statement.Step() |> ignore
                        let count = statement.Row.GetInt64(0)
                        statement.Dispose()
                        count
                    with error -> statement.Abort(error); reraise()
                if preCount <> postCount then
                    // Abort migration: duplicate (SourceId, TargetId) pairs in source data
                    let mutable statement = tx.Connection.OpenStatement($"DROP TABLE {qTmp};")
                    try
                        statement.Step() |> ignore
                        statement.Dispose()
                    with error -> statement.Abort(error); reraise()
                    raise (InvalidOperationException(
                        $"Error: forward migration failed for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                        $"Reason: source link table '{linkTable}' contains {preCount} rows but migration target accepted only {postCount}. " +
                        "This indicates duplicate (SourceId, TargetId) pairs in the existing Single-relation data.\n" +
                        "Fix: resolve duplicate link rows before retrying the migration."))
                let mutable statement = tx.Connection.OpenStatement($"DROP TABLE {qLink};")
                try
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
                let mutable statement = tx.Connection.OpenStatement($"ALTER TABLE {qTmp} RENAME TO {qLink};")
                try
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
                // Recreate indexes
                let sourceIdx = formatName($"IX_{linkTable}_Source")
                let targetIdx = formatName($"IX_{linkTable}_Target")
                let mutable statement = tx.Connection.OpenStatement($"CREATE INDEX IF NOT EXISTS {quoteIdentifier sourceIdx} ON {qLink}(SourceId);")
                try
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
                let mutable statement = tx.Connection.OpenStatement($"CREATE INDEX IF NOT EXISTS {quoteIdentifier targetIdx} ON {qLink}(TargetId);")
                try
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
                // Post-migration invariant verification
                // 1. Row count preserved
                let finalCount =
                    let mutable statement = tx.Connection.OpenStatement($"SELECT COUNT(*) FROM {qLink};")
                    try
                        statement.Step() |> ignore
                        let count = statement.Row.GetInt64(0)
                        statement.Dispose()
                        count
                    with error -> statement.Abort(error); reraise()
                if finalCount <> preCount then
                    raise (InvalidOperationException(
                        $"Error: forward migration invariant violation for '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\n" +
                        $"Reason: expected {preCount} rows after migration but found {finalCount} in '{linkTable}'.\n" +
                        "Fix: this indicates a migration defect. Report this error."))
                // 2. Verify exact unique index shape through pragma metadata.
                let uniqueIndexNames =
                    let tableLit = Helper.sqlLiteralEscape linkTable
                    let mutable statement = tx.Connection.OpenStatement($"PRAGMA index_list('{tableLit}');")
                    try
                        let names = ResizeArray<string>()
                        while statement.Step() do
                            if statement.Row.GetBoolean(2) then names.Add(statement.Row.GetString(1))
                        statement.Dispose()
                        names.ToArray()
                    with error -> statement.Abort(error); reraise()

                let uniqueColumnSets =
                    uniqueIndexNames
                    |> Array.map (fun indexName ->
                        let indexLit = Helper.sqlLiteralEscape indexName
                        let mutable statement = tx.Connection.OpenStatement($"PRAGMA index_info('{indexLit}');")
                        try
                            let columns = ResizeArray<struct (int64 * string)>()
                            while statement.Step() do
                                let row = statement.Row
                                let name = if row.IsDBNull(2) then null else row.GetString(2)
                                columns.Add(struct (row.GetInt64(0), name))
                            statement.Dispose()
                            columns.Sort(Comparison<_>(fun struct (left, _) struct (right, _) -> compare left right))
                            Array.init columns.Count (fun i -> let struct (_, name) = columns.[i] in name)
                        with error -> statement.Abort(error); reraise())

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
                    let mutable statement = tx.Connection.OpenStatement("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND tbl_name = @tbl AND name IN (@src, @tgt);")
                    try
                        statement.BindText(1, linkTable)
                        statement.BindText(2, sourceIdx)
                        statement.BindText(3, targetIdx)
                        statement.Step() |> ignore
                        let count = statement.Row.GetInt64(0)
                        statement.Dispose()
                        count
                    with error -> statement.Abort(error); reraise()
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
        // Typed-id relations require a unique index on the target's [<SoloId>]
        // property. Auto-create it idempotently so owner-before-target open is
        // order-independent. The validator below still runs to catch any
        // post-bootstrap schema regression (e.g. user raw-SQL DROP INDEX) that
        // would otherwise let typed-relation queries return wrong results
        // silently.
        HelperSchema.ensureUniqueIndexForIdentity descriptor.TargetTable tx.Connection soloIdProp
        let hasUniqueSoloIdIndex =
            HelperSchema.hasUniqueIndexForIdentity descriptor.TargetTable tx.Connection soloIdProp

        if not hasUniqueSoloIdIndex then
            raise (InvalidOperationException(
                $"Error: Missing required unique index for typed relation '{descriptor.OwnerTable}.{descriptor.Property.Name}'.\nReason: Typed-id resolver requires a unique index on the declared identity expression for '{soloIdProp.Name}' in target collection '{descriptor.TargetTable}'.\nFix: Add [SoloId] or EnsureUniqueAndIndex for this path before using DBRef<'TTarget,'TId>/DBRefMany<'TTarget,'TId>."))
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

    let mutable table = tx.Connection.OpenStatement($"""
CREATE TABLE IF NOT EXISTS {qLink} (
    Id INTEGER PRIMARY KEY,
    SourceId INTEGER NOT NULL,
    TargetId INTEGER NOT NULL,
    FOREIGN KEY (SourceId) REFERENCES {qSource}(Id) ON DELETE RESTRICT,
    FOREIGN KEY (TargetId) REFERENCES {qTarget}(Id) ON DELETE RESTRICT,
    {constraints}
) STRICT;
    """)
    try
        table.Step() |> ignore
        table.Dispose()
    with error -> table.Abort(error); reraise()

    let sourceIdx = formatName($"IX_{descriptor.LinkTable}_Source")
    let targetIdx = formatName($"IX_{descriptor.LinkTable}_Target")
    let mutable statement = tx.Connection.OpenStatement($"CREATE INDEX IF NOT EXISTS {quoteIdentifier sourceIdx} ON {qLink}(SourceId);")
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()
    let mutable statement = tx.Connection.OpenStatement($"CREATE INDEX IF NOT EXISTS {quoteIdentifier targetIdx} ON {qLink}(TargetId);")
    try
        statement.Step() |> ignore
        statement.Dispose()
    with error -> statement.Abort(error); reraise()

    let mutable declaration = tx.Connection.OpenStatement("""
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
    """)
    try
        declaration.BindText(1, descriptor.RelationName)
        declaration.BindText(2, descriptor.OwnerTable)
        declaration.BindText(3, descriptor.TargetTable)
        declaration.BindText(4, descriptor.Property.Name)
        declaration.BindText(5, descriptor.OwnerTable)
        declaration.BindText(6, relationKindToString descriptor.Kind)
        declaration.BindText(7, descriptor.OnDelete.ToString())
        declaration.BindText(8, descriptor.OnOwnerDelete.ToString())
        declaration.BindInt64(9, if descriptor.IsUnique then 1L else 0L)
        declaration.Step() |> ignore
        declaration.Dispose()
    with error -> declaration.Abort(error); reraise()

/// Read-only pre-flight check: returns true when the full write-locked ensure path
/// is needed for an existing collection. Avoids BEGIN IMMEDIATE contention on
/// concurrent readers during in-flight write transactions.
let internal relationSchemaRequiresEnsure (connection: NativeSqliteConnection) (ownerTable: string) (ownerType: Type) =
    let ownerTable = formatName ownerTable
    // If catalog table doesn't exist at all, ensure is needed.
    let catalogExists =
        sqliteTableExistsByName connection "SoloDBRelation"
    if not catalogExists then true
    else

    let specs = getRelationSpecs ownerType
    let filesystem = getFileSystemReferenceSpecs ownerType
    if specs.Length = 0 && filesystem.Length = 0 then false
    else

    let storedByProp = Dictionary<string, {| PropertyName: string; RefKind: string; TargetCollection: string; OnDelete: string; OnOwnerDelete: string; IsUnique: int64 |}>(StringComparer.Ordinal)
    let mutable statement = connection.OpenStatement("SELECT PropertyName, RefKind, TargetCollection, OnDelete, OnOwnerDelete, IsUnique FROM SoloDBRelation WHERE OwnerCollection = @owner;")
    try
        statement.BindText(1, ownerTable)
        while statement.Step() do
            let row = statement.Row
            let property = row.GetString(0)
            storedByProp.[property] <- {| PropertyName = property; RefKind = row.GetString(1); TargetCollection = row.GetString(2); OnDelete = row.GetString(3); OnOwnerDelete = row.GetString(4); IsUnique = row.GetInt64(5) |}
        statement.Dispose()
    with error -> statement.Abort(error); reraise()

    // Check each current spec against stored state.
    let mutable needsEnsure = false
    for (prop, kind, targetType, _typedIdType, onDelete, onOwnerDelete, isUnique, _orderBy) in specs do
        if not needsEnsure then
            match storedByProp.TryGetValue prop.Name with
            | false, _ ->
                // Missing catalog row — a link table without a catalog entry indicates
                // resurrection from a prior schema generation. Either way, ensure must run:
                // to bootstrap or to reject.
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

    for spec in filesystem do
        match storedByProp.TryGetValue spec.Property.Name with
        | false, _ -> needsEnsure <- true
        | true, stored ->
            if stored.RefKind <> relationKindToString spec.Kind || stored.TargetCollection <> spec.TargetTable then
                needsEnsure <- true
            storedByProp.Remove spec.Property.Name |> ignore
    if filesystem.Length > 0 && not (sqliteTableExistsByName connection "SoloDBFileSystemReference") then
        needsEnsure <- true

    // Any remaining stored rows are orphans (removed properties) — need ensure for cleanup.
    if not needsEnsure && storedByProp.Count > 0 then
        needsEnsure <- true

    needsEnsure


let internal ensureFileSystemReferenceSchema (tx: RelationTxContext) (spec: FileSystemReferenceSpec) =
    let owner = formatName tx.OwnerTable
    let kind = relationKindToString spec.Kind
    let stored =
        let mutable statement = tx.Connection.OpenStatement("SELECT RefKind,TargetCollection FROM SoloDBRelation WHERE OwnerCollection=@owner AND PropertyName=@property")
        try
            statement.BindText(1, owner)
            statement.BindText(2, spec.Property.Name)
            let stored =
                if statement.Step() then {| RefKind = statement.Row.GetString(0); TargetCollection = statement.Row.GetString(1) |}
                else Unchecked.defaultof<_>
            statement.Dispose()
            stored
        with error -> statement.Abort(error); reraise()

    if not (isNull (box stored)) && (stored.RefKind <> kind || stored.TargetCollection <> spec.TargetTable) then
        raise (InvalidOperationException($"Reference declaration '{owner}.{spec.Property.Name}' conflicts with its persisted cardinality or target kind."))
    let createDeclaration() =
        let mutable statement = tx.Connection.OpenStatement("""
    INSERT INTO SoloDBRelation(Name,SourceCollection,TargetCollection,PropertyName,OwnerCollection,RefKind,OnDelete,OnOwnerDelete,IsUnique)
    SELECT @name,@owner,@target,@property,@owner,@kind,'Restrict','Unlink',0
    WHERE NOT EXISTS(SELECT 1 FROM SoloDBRelation WHERE OwnerCollection=@owner AND PropertyName=@property)
        """)
        try
            statement.BindText(1, relationName owner spec.Property.Name)
            statement.BindText(2, owner)
            statement.BindText(3, spec.TargetTable)
            statement.BindText(4, spec.Property.Name)
            statement.BindText(5, kind)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
        // A newly declared Single may already have canonical identities in old JSON.
        // Establish protection once, inside declaration creation, without rewriting data.
        if isNull (box stored) && spec.Kind = Single then
            let column = if spec.TargetTable = "SoloDBFileHeader" then "FileId" else "DirectoryId"
            let statement, variables = RelationMutationStatements.filesystemSingleLinkFromJson owner spec.Property.Name column RelationMutationStatements.OwnerSelection.All
            StatementExecution.execute tx.Connection statement variables |> ignore

    if isNull (box stored) && spec.Kind = Single && not (ConnectionsTransactionHelpers.shouldSuppressSavepointInHandler tx.Connection) then
        // A caller may catch an opening failure and keep its outer transaction.
        // Do not publish a declaration whose initial protecting rows failed.
        ConnectionsTransactionHelpers.withSavepoint tx.Connection (fun _ -> createDeclaration())
    else
        // Active row callbacks use the established fault/commit-veto owner.
        createDeclaration()
