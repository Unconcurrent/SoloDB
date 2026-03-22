module internal SoloDatabase.RelationsCore

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.Data.Sqlite
open SoloDatabase
open SoloDatabase.Attributes
open SoloDatabase.Utils
open SoloDatabase.JsonSerializator
open SQLiteTools
open JsonFunctions
open RelationsTypes
open RelationsSchema
open RelationsEntity
open RelationsDelete
open RelationsBatchLoad

type RelationTxContext = RelationsTypes.RelationTxContext
type RelationWritePlan = RelationsTypes.RelationWritePlan
type RelationDeletePlan = RelationsTypes.RelationDeletePlan
type RelationUpdateManyOp = RelationsTypes.RelationUpdateManyOp

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
    | :? SqliteException as ex ->
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
        let getter = RelationsAccessorCache.compiledPropGetter prop
        let value = getter.Invoke(owner)
        match value with
        | :? IDBRefManyInternal as tracker ->
            match committedLinkedIdsByProperty.TryGetValue prop.Name with
            | true, ids when not (isNull ids) -> tracker.ResetTracker ids
            | _ -> tracker.ResetTracker Seq.empty
        | _ -> ()

let ensureSchemaForOwnerType (tx: RelationTxContext) (ownerType: Type) =
    ensureTxContext tx
    if isNull ownerType then nullArg "ownerType"
    withRelationSqliteWrap "build" "ensureSchemaForOwnerType" (fun () ->
        ensureRelationCatalogTable tx.Connection
        ensureCollectionMetadataColumn tx.Connection tx.OwnerTable
        let descriptors = buildRelationDescriptors tx ownerType
        for descriptor in descriptors do
            ensureMetadataNotResurrected tx descriptor
            ensureRelationSchema tx descriptor

        // Orphan detection — detect removed relation properties at GetCollection time.
        // Query catalog for all stored relation properties of this owner collection.
        // Any property in catalog but NOT in current descriptors is orphaned.
        let ownerTable = formatName tx.OwnerTable
        let storedProperties =
            tx.Connection.Query<{| PropertyName: string; Name: string |}>(
                "SELECT PropertyName, Name FROM SoloDBRelation WHERE OwnerCollection = @owner;",
                {| owner = ownerTable |})
            |> Seq.toArray
        let currentPropertyNames =
            descriptors |> Array.map (fun d -> d.Property.Name) |> Set.ofArray
        for stored in storedProperties do
            if not (currentPropertyNames.Contains stored.PropertyName) then
                // Orphaned catalog row: check if link table has persisted data
                let linkTable = linkTableFromRelationName stored.Name
                let hasPersistedLinks =
                    sqliteTableExistsByName tx.Connection linkTable
                    && tx.Connection.QueryFirst<int64>(
                        $"SELECT CASE WHEN EXISTS (SELECT 1 FROM {quoteIdentifier linkTable} LIMIT 1) THEN 1 ELSE 0 END") = 1L
                if hasPersistedLinks then
                    // E2: ERROR — persisted links exist for removed property
                    raise (InvalidOperationException(
                        $"Error: relation property '{ownerTable}.{stored.PropertyName}' was removed but persisted link data exists.\n" +
                        $"Reason: link table '{linkTable}' contains rows that reference this relation. " +
                        "Removing a relation property with persisted links is not allowed because it would orphan link data silently.\n" +
                        "Fix: clear all link data for this relation before removing the property from the type, " +
                        "or drop the collection entirely."))
    )

let private asReadOnlyDict (input: Dictionary<string, int64 array>) =
    input :> IReadOnlyDictionary<string, int64 array>

let internal initPrepareContext (tx: RelationTxContext) (seedOwner: obj) =
    let descriptors = buildRelationDescriptors tx tx.OwnerType
    let ops = ResizeArray<RelationUpdateManyOp>()
    let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)
    let visited = HashSet<obj>(refComparer)
    visited.Add(seedOwner) |> ignore
    let typeStack = HashSet<Type>()
    typeStack.Add(tx.OwnerType) |> ignore
    descriptors, ops, resetMap, visited, typeStack

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
        tx.Connection.Query<int64>(
            $"SELECT {targetColumn} FROM {quoteIdentifier descriptor.LinkTable} WHERE {ownerColumn} = @ownerId;",
            {| ownerId = ownerId |})
        |> Seq.toArray

let internal hasLoadedRelationVersion (entity: obj) =
    match relationVersionBaseline.TryGetValue(entity) with
    | true, _ -> true
    | _ -> false

let internal checkRelationVersionStale (tx: RelationTxContext) (ownerId: int64) (newOwner: obj) =
    // Only check if a baseline was captured at load time. Entities that were never loaded
    // via GetById/query (e.g., fresh constructions, clones, patch-update patterns) skip the check.
    if hasLoadedRelationVersion newOwner then
        let loadedVersion = getLoadedRelationVersion newOwner
        let currentVersion = readPersistedRelationVersion tx.Connection tx.OwnerTable ownerId
        if currentVersion <> loadedVersion then
            raise (InvalidOperationException(
                $"Error: Stale relation state detected for '{tx.OwnerTable}' (owner id {ownerId}). " +
                $"The relation was modified since this entity was loaded (loaded version {loadedVersion}, current version {currentVersion}).\n" +
                "Fix: Reload the owner entity and retry, or use WithTransaction to ensure atomic relation updates."))


let internal cloneOwnerForReplaceMany (ownerType: Type) (owner: obj) =
    ensureOwnerInstance ownerType owner "owner"
    let desc = RelationsAccessorCache.compiledCloneDescriptor ownerType
    let clone = desc.CreateInstance.Invoke()
    desc.CopyScalars.Invoke(owner, clone)
    for prop in desc.ManyProperties do
        let getter = RelationsAccessorCache.compiledPropGetter prop
        let setter = RelationsAccessorCache.compiledPropSetter prop
        let value = getter.Invoke(owner)
        if isNull value then
            setter.Invoke(clone, null)
        else
            match value with
            | :? IDBRefManyInternal as srcTracker ->
                let dst = (RelationsAccessorCache.compiledDefaultCtor prop.PropertyType).Invoke()
                let addFn = RelationsAccessorCache.compiledAddMethod prop.PropertyType
                for item in srcTracker.GetCurrentItemsBoxed() do
                    addFn.Invoke(dst, item)
                setter.Invoke(clone, dst)
            | _ ->
                setter.Invoke(clone, value)
    clone

let internal incrementRelationVersion (tx: RelationTxContext) (ownerId: int64) =
    incrementRelationVersionForTable tx.Connection tx.OwnerTable ownerId
