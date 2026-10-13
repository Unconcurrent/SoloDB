module internal SoloDatabase.RelationsCore

open System
open System.Reflection
open System.Collections.Generic
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SoloDatabase.JsonSerializator
open NativeSQLiteTools
open JsonFunctions
open RelationsTypes
open RelationsSchemaBuilder
open RelationsSchemaValidator
open RelationsSchemaLinkTableDDL
open RelationsEntitySync
open RelationsEntityCascade
open RelationsEntityApplyOps
open RelationsDelete
open RelationsBatchLoad
let private relationSqlWrapKey = "relation-sql-boundary-operation-failed"

let private isWrappedRelationSqlBoundaryError (ex: InvalidOperationException) =
    not (isNull ex)
    && not (isNull ex.Message)
    && ex.Message.IndexOf(relationSqlWrapKey, StringComparison.Ordinal) >= 0

let internal withRelationSqliteWrap (phase: string) (operation: string) (fn: unit -> 'T) : 'T =
    if String.IsNullOrWhiteSpace phase then nullArg "phase"
    if String.IsNullOrWhiteSpace operation then nullArg "operation"
    try
        fn()
    with
    | :? InvalidOperationException as ex when isWrappedRelationSqlBoundaryError ex ->
        raise ex
    | :? NativeSqliteException as ex when ex.SqliteErrorCode = 19 && ex.SqliteExtendedErrorCode = 2067 ->
        // SQLITE_CONSTRAINT_UNIQUE on a typed-relation INSERT/UPDATE means two rows share
        // the same [<SoloId>] typed-id value. The outer wrapper line keeps the existing
        // "Relation SQL operation failed" anchor that downstream tests grep for; the
        // reason line carries the more specific "Ambiguous typed relation target"
        // vocabulary the SELECT-resolver path uses for the equivalent multi-match
        // condition, so callers see a consistent message regardless of which side of
        // the relation flow surfaces the duplicate.
        let message =
            "Error: Relation SQL operation failed.\n" +
            "Reason: Ambiguous typed relation target during '" + operation + "' (" + phase +
            "). SQLite UNIQUE constraint violation (sqliteErrorCode=" + string ex.SqliteErrorCode +
            ", sqliteExtendedErrorCode=" + string ex.SqliteExtendedErrorCode +
            "). A typed-relation [SoloId] value collided with an existing row, or two rows in the target table share the same identity.\n" +
            "Fix: Enforce a unique index on the typed [<SoloId>] path, deduplicate target data, and retry."
        raise (InvalidOperationException(message, ex))
    | :? NativeSqliteException as ex ->
        let message =
            "Error: Relation SQL operation failed.\n" +
            $"Reason: Underlying SQLite error (sqliteErrorCode={ex.SqliteErrorCode}, sqliteExtendedErrorCode={ex.SqliteExtendedErrorCode}).\n" +
            "Fix: Check schema, relation metadata, link-table integrity, and constraints; then retry with valid state."
        raise (InvalidOperationException(message, ex))

let resetDbRefManyTrackers (owner: obj) (committedLinkedIdsByProperty: IReadOnlyDictionary<string, int64 array>) =
    if isNull owner then nullArg "owner"
    if isNull committedLinkedIdsByProperty then nullArg "committedLinkedIdsByProperty"

    let ownerType = owner.GetType()
    let specs = getRelationSpecs ownerType
    let manySpecs = specs |> Array.filter (fun (_, kind, _, _, _, _, _, _) -> kind = Many)
    for (prop, _kind, _, _, _, _, _, _) in manySpecs do
        let getter = prop.Read
        let value = getter.Invoke(owner)
        match value with
        | :? IDBRefManyInternal as tracker ->
            match committedLinkedIdsByProperty.TryGetValue prop.Name with
            | true, ids when not (isNull ids) -> tracker.ResetTracker ids
            | _ -> tracker.ResetTracker Seq.empty
        | _ -> ()

let private initializeSchemaForOwnerType (tx: RelationTxContext) (ownerType: Type) =
    ensureTxContext tx
    if isNull ownerType then nullArg "ownerType"
    withRelationSqliteWrap "build" "ensureSchemaForOwnerType" (fun () ->
        ensureRelationCatalogTable tx.Connection
        ensureCollectionMetadataColumn tx.Connection tx.OwnerTable
        let filesystem = getFileSystemReferenceSpecs ownerType
        if filesystem.Length > 0 then
            ensureFileSystemReferenceTable tx.Connection
            ensureFileSystemOwnerCleanup tx.Connection (formatName tx.OwnerTable)
        for spec in filesystem do ensureFileSystemReferenceSchema tx spec
        let descriptors = buildRelationDescriptors tx ownerType
        for descriptor in descriptors do
            ensureMetadataNotResurrected tx descriptor
            ensureRelationSchema tx descriptor

        // Orphan detection — detect removed relation properties at GetCollection time.
        // Query catalog for all stored relation properties of this owner collection.
        // Any property in catalog but NOT in current descriptors is orphaned.
        let ownerTable = formatName tx.OwnerTable
        let storedProperties =
            let mutable statement = tx.Connection.OpenStatement("SELECT PropertyName, Name, TargetCollection FROM SoloDBRelation WHERE OwnerCollection = @owner;")
            try
                statement.BindText(1, ownerTable)
                let rows = ResizeArray<struct (string * string * string)>()
                while statement.Step() do
                    let row = statement.Row
                    rows.Add(struct (row.GetString(0), row.GetString(1), row.GetString(2)))
                statement.Dispose()
                rows
            with error -> statement.Abort(error); reraise()
        let currentPropertyNames =
            Array.append (descriptors |> Array.map (fun d -> d.Property.Name)) (filesystem |> Array.map (fun d -> d.Property.Name)) |> Set.ofArray
        for struct (propertyName, relationName, targetCollection) in storedProperties do
            if not (currentPropertyNames.Contains propertyName) then
                // Orphaned catalog row: check if link table has persisted data
                let linkTable = linkTableFromRelationName relationName
                let hasPersistedLinks =
                    if targetCollection = "SoloDBFileHeader" || targetCollection = "SoloDBDirectoryHeader" then
                        let mutable statement = tx.Connection.OpenStatement("SELECT EXISTS(SELECT 1 FROM SoloDBFileSystemReference WHERE OwnerCollection=@owner AND Property=@property)")
                        try
                            statement.BindText(1, ownerTable)
                            statement.BindText(2, propertyName)
                            statement.Step() |> ignore
                            let exists = statement.Row.GetBoolean(0)
                            statement.Dispose()
                            exists
                        with error -> statement.Abort(error); reraise()
                    else
                        if not (sqliteTableExistsByName tx.Connection linkTable) then false
                        else
                            let mutable statement = tx.Connection.OpenStatement($"SELECT EXISTS (SELECT 1 FROM {quoteIdentifier linkTable} LIMIT 1)")
                            try
                                statement.Step() |> ignore
                                let exists = statement.Row.GetBoolean(0)
                                statement.Dispose()
                                exists
                            with error -> statement.Abort(error); reraise()
                if hasPersistedLinks then
                    // E2: ERROR — persisted links exist for removed property
                    raise (InvalidOperationException(
                        $"Error: relation property '{ownerTable}.{propertyName}' was removed but persisted link data exists.\n" +
                        $"Reason: link table '{linkTable}' contains rows that reference this relation. " +
                        "Removing a relation property with persisted links is not allowed because it would orphan link data silently.\n" +
                        "Fix: clear all link data for this relation before removing the property from the type, " +
                        "or drop the collection entirely."))
    )

let ensureSchemaForOwnerType (tx: RelationTxContext) (ownerType: Type) =
    ensureTxContext tx
    if isNull ownerType then nullArg "ownerType"
    let entry = tx.Preparation.Entry(tx.Connection, formatName tx.OwnerTable, ownerType)
    if not entry.SchemaReady || not tx.Preparation.AllowReuse then
        tx.Preparation.Invalidate()
        try
            initializeSchemaForOwnerType tx ownerType
            let admitted = tx.Preparation.Entry(tx.Connection, formatName tx.OwnerTable, ownerType)
            // Admission may publish or evolve the physical declaration.
            admitted.Descriptors <- ValueNone
            admitted.DescriptorMap <- ValueNone
            admitted.SchemaReady <- true
        with _ ->
            tx.Preparation.Invalidate()
            reraise()

let private asReadOnlyDict (input: Dictionary<string, int64 array>) =
    input :> IReadOnlyDictionary<string, int64 array>

let internal initPrepareContext (tx: RelationTxContext) (seedOwner: obj) =
    let descriptors = buildRelationDescriptors tx tx.OwnerType
    let ops = ResizeArray<RelationUpdateManyOp>()
    let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)
    let visited = HashSet<obj>(refComparer)
    visited.Add(seedOwner) |> ignore
    descriptors, ops, resetMap, visited, 0

let internal applyResetMapIfAny (owner: obj) (resetMap: Dictionary<string, int64 array>) =
    if resetMap.Count > 0 then
        resetDbRefManyTrackers owner (asReadOnlyDict resetMap)

let internal readPersistedManyTargetIds (tx: RelationTxContext) (descriptor: RelationDescriptor) (ownerId: int64) =
    if ownerId <= 0L then
        [||]
    else
        let ownerColumn, targetColumn =
            if descriptor.OwnerUsesSourceColumn then "SourceId", "TargetId"
            else "TargetId", "SourceId"
        let mutable statement = tx.Connection.OpenStatement($"SELECT {targetColumn} FROM {quoteIdentifier descriptor.LinkTable} WHERE {ownerColumn} = @ownerId;")
        try
            statement.BindInt64(1, ownerId)
            let ids = ResizeArray<int64>()
            while statement.Step() do ids.Add(statement.Row.GetInt64(0))
            statement.Dispose()
            ids.ToArray()
        with error -> statement.Abort(error); reraise()

let internal cloneOwnerForReplaceMany (ownerType: Type) (owner: obj) =
    ensureOwnerInstance ownerType owner "owner"
    let desc = RelationsAccessorCache.compiledCloneDescriptor ownerType
    let clone = desc.CreateInstance.Invoke()
    desc.CopyScalars.Invoke(owner, clone)
    for prop in getReferenceMembers ownerType do
      if not (isNull prop.Read) && not (isNull prop.Write) && typeof<IDBRefManyInternal>.IsAssignableFrom(prop.ReferenceType) then
        let value = prop.Read.Invoke(owner)
        if isNull value then
            prop.Write.Invoke(clone, null)
        else
            match value with
            | :? IDBRefManyInternal as srcTracker ->
                let dst = (RelationsAccessorCache.compiledDefaultCtor prop.ReferenceType).Invoke()
                let addFn = RelationsAccessorCache.compiledAddMethod prop.ReferenceType
                for item in srcTracker.GetCurrentItemsBoxed() do
                    addFn.Invoke(dst, item)
                prop.Write.Invoke(clone, dst)
            | _ ->
                prop.Write.Invoke(clone, value)
    clone
