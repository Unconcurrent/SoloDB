
module internal SoloDatabase.Relations

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
            $"phase={phase} operation={operation} message={relationSqlWrapKey}\n" +
            "Error: relation SQL operation failed.\n" +
            $"Reason: underlying SQLite error at relation boundary (sqliteErrorCode={ex.SqliteErrorCode}, sqliteExtendedErrorCode={ex.SqliteExtendedErrorCode}).\n" +
            "Fix: check schema, relation metadata, link-table integrity, and constraints; then retry with valid state."
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

let prepareInsert (tx: RelationTxContext) (owner: obj) =
    ensureTxContext tx
    ensureOwnerInstance tx.OwnerType owner "owner"
    withRelationSqliteWrap "prepare" "prepareInsert" (fun () ->
        let descriptors = buildRelationDescriptors tx tx.OwnerType
        let ops = ResizeArray<RelationUpdateManyOp>()
        let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)
        let visited = HashSet<obj>(refComparer)
        visited.Add(owner) |> ignore
        let typeStack = HashSet<Type>()
        typeStack.Add(tx.OwnerType) |> ignore

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let targetId = resolveSingleTargetIdAndCascade tx descriptor owner visited typeStack
                if targetId > 0L then
                    ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, targetId))
            | Many ->
                match collectManyTargetIdsAndCascade tx descriptor owner true visited typeStack with
                | ValueSome ids ->
                    resetMap.[descriptor.Property.Name] <- ids
                    for id in ids do
                        ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                | ValueNone -> ()

        if resetMap.Count > 0 then
            resetDbRefManyTrackers owner (asReadOnlyDict resetMap)

        { Kind = RelationPlanKind.Insert; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }
    )

let prepareUpsert (tx: RelationTxContext) (oldOwner: obj voption) (newOwner: obj) =
    ensureTxContext tx
    let hadOldOwner =
        match oldOwner with
        | ValueSome old -> ensureOwnerInstance tx.OwnerType old "oldOwner"; true
        | ValueNone -> false

    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    withRelationSqliteWrap "prepare" "prepareUpsert" (fun () ->
        let descriptors = buildRelationDescriptors tx tx.OwnerType
        let ops = ResizeArray<RelationUpdateManyOp>()
        let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)
        let visited = HashSet<obj>(refComparer)
        visited.Add(newOwner) |> ignore
        let typeStack = HashSet<Type>()
        typeStack.Add(tx.OwnerType) |> ignore

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let oldId = match oldOwner with | ValueSome old -> readSingleIdNoCascade descriptor old | ValueNone -> 0L
                let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner visited typeStack
                if oldId <> newId then
                    if newId > 0L then ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
                    else ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))
            | Many ->
                let trackerObj = (RelationsAccessorCache.compiledPropGetter descriptor.Property).Invoke(newOwner)
                if isNull trackerObj then
                    raise (ArgumentNullException(
                        descriptor.Property.Name,
                        $"Error: DBRefMany property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' is null.\nReason: DBRefMany<T> properties must not be set to null.\nFix: Use an empty DBRefMany<T> (new()) or call Clear() to remove all links."))
                match collectManyTargetIdsAndCascade tx descriptor newOwner true visited typeStack with
                | ValueSome ids ->
                    resetMap.[descriptor.Property.Name] <- ids
                    if hadOldOwner then ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                    for id in ids do ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                | ValueNone ->
                    if hadOldOwner then ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))

        if resetMap.Count > 0 then
            resetDbRefManyTrackers newOwner (asReadOnlyDict resetMap)

        { Kind = RelationPlanKind.Upsert; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }
    )

let private readPersistedManyTargetIds (tx: RelationTxContext) (descriptor: RelationDescriptor) (ownerId: int64) =
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

let private hasLoadedRelationVersion (entity: obj) =
    match relationVersionBaseline.TryGetValue(entity) with
    | true, _ -> true
    | _ -> false

let private checkRelationVersionStale (tx: RelationTxContext) (ownerId: int64) (newOwner: obj) =
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

let prepareUpdate (tx: RelationTxContext) (ownerId: int64) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    withRelationSqliteWrap "prepare" "prepareUpdate" (fun () ->
        // Stale-version guard before any relation mutation planning.
        checkRelationVersionStale tx ownerId newOwner

        let descriptors = buildRelationDescriptors tx tx.OwnerType
        let ops = ResizeArray<RelationUpdateManyOp>()
        let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)
        let visited = HashSet<obj>(refComparer)
        visited.Add(newOwner) |> ignore
        let typeStack = HashSet<Type>()
        typeStack.Add(tx.OwnerType) |> ignore

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let oldId = readSingleIdNoCascade descriptor oldOwner
                let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner visited typeStack
                if oldId <> newId then
                    if newId > 0L then ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
                    else ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))
            | Many ->
                match (RelationsAccessorCache.compiledPropGetter descriptor.Property).Invoke(newOwner) with
                | :? IDBRefManyInternal as tracker when tracker.IsLoaded ->
                    // Loaded tracker: diff against OriginalIds
                    match collectManyTargetIdsAndCascade tx descriptor newOwner false visited typeStack with
                    | ValueNone -> ()
                    | ValueSome newIds ->
                        resetMap.[descriptor.Property.Name] <- newIds
                        let oldIds = tracker.OriginalIds |> Seq.toArray
                        let oldSet = HashSet<int64>(oldIds)
                        let newSet = HashSet<int64>(newIds)
                        if not (oldSet.SetEquals(newSet)) then
                            if tracker.WasCleared then
                                ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                                for id in newIds do
                                    ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                            else
                                // Delta mode preserves concurrent changes outside this view's OriginalIds.
                                for id in oldIds do
                                    if not (newSet.Contains id) then
                                        ops.Add(RemoveDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                                for id in newIds do
                                    if not (oldSet.Contains id) then
                                        ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                | :? IDBRefManyInternal as tracker when not tracker.IsLoaded && tracker.HasPendingMutations ->
                    if tracker.WasCleared then
                        // Unloaded + Clear(): collect post-clear items to detect Clear()+Add() pattern.
                        match collectManyTargetIdsAndCascade tx descriptor newOwner true visited typeStack with
                        | ValueNone | ValueSome [||] ->
                            // Pure clear — no items after clear.
                            resetMap.[descriptor.Property.Name] <- [||]
                            ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                        | ValueSome newIds ->
                            // Clear()+Add(): emit clear then re-add for parity with loaded path.
                            resetMap.[descriptor.Property.Name] <- newIds
                            ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                            for id in newIds do
                                ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                    else
                        // Discriminator: reject only ambiguous unloaded AddOnly with unsaved targets.
                        // Persisted-target AddOnly on an unloaded tracker is treated as explicit replace intent.
                        let hasUnsavedPendingTarget =
                            tracker.GetCurrentItemsBoxed()
                            |> Seq.exists (fun item -> readEntityIdOrZero descriptor.TargetType item <= 0L)
                        if hasUnsavedPendingTarget then
                            raise (InvalidOperationException(
                                $"Error: Cannot apply add-only mutation on unloaded relation '{descriptor.PropertyPath}'.\nReason: The DBRefMany tracker is unloaded and has pending mutations without Clear(), which is ambiguous.\nFix: Load the relation first, or call Clear() then Add(...) before saving."))
                        else
                            // Deterministic replace payload on unloaded tracker with persisted targets.
                            match collectManyTargetIdsAndCascade tx descriptor newOwner true visited typeStack with
                            | ValueNone -> ()
                            | ValueSome newIds ->
                                resetMap.[descriptor.Property.Name] <- newIds
                                let oldIds = readPersistedManyTargetIds tx descriptor ownerId
                                let oldSet = HashSet<int64>(oldIds)
                                let newSet = HashSet<int64>(newIds)
                                if not (oldSet.SetEquals(newSet)) then
                                    ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                                    for id in newIds do ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                | :? IDBRefManyInternal ->
                    // Unloaded + no mutations: no relation operation required.
                    ()
                | null ->
                    raise (ArgumentNullException(
                        descriptor.Property.Name,
                        $"Error: DBRefMany property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' is null.\nReason: DBRefMany<T> properties must not be set to null.\nFix: Use an empty DBRefMany<T> (new()) or call Clear() to remove all links."))
                | _ ->
                    raise (InvalidOperationException(
                        $"Error: Property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' does not implement IDBRefManyInternal.\nReason: The relation property type is incorrect.\nFix: Use DBRefMany<T> for multi-relations."))

        if resetMap.Count > 0 then
            resetDbRefManyTrackers newOwner (asReadOnlyDict resetMap)

        { Kind = RelationPlanKind.Update; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }
    )

let prepareDeleteOwner (tx: RelationTxContext) (ownerId: int64) (owner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType owner "owner"
    withRelationSqliteWrap "prepare" "prepareDeleteOwner" (fun () ->
        // Stale-version guard before delete relation handling.
        checkRelationVersionStale tx ownerId owner
        { OwnerId = ownerId; OwnerType = tx.OwnerType }
    )

let private cloneOwnerForReplaceMany (ownerType: Type) (owner: obj) =
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

let private incrementRelationVersion (tx: RelationTxContext) (ownerId: int64) =
    let qOwner = quoteIdentifier tx.OwnerTable
    tx.Connection.Execute(
        $"UPDATE {qOwner} SET Metadata = jsonb_set(COALESCE(Metadata, jsonb('{{}}')), @path, " +
        "jsonb(CAST(COALESCE(jsonb_extract(Metadata, @path), 0) + 1 AS TEXT))) WHERE Id = @id;",
        {| path = relationVersionMetadataPath; id = ownerId |}) |> ignore

let private runSyncWithOwnerIdGuard (opName: string) (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) (updateOwnerJson: bool) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    withRelationSqliteWrap "sync" opName (fun () ->
        ensureSchemaForOwnerType tx tx.OwnerType
        applyOps tx ownerId plan updateOwnerJson
        if plan.Ops.Length > 0 then incrementRelationVersion tx ownerId
    )

let syncInsert (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    runSyncWithOwnerIdGuard "syncInsert" tx ownerId plan false

let syncUpsert (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    runSyncWithOwnerIdGuard "syncUpsert" tx ownerId plan false

let syncUpdate (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    let updateOwnerJson = plan.Kind = RelationPlanKind.UpdateMany
    runSyncWithOwnerIdGuard "syncUpdate" tx ownerId plan updateOwnerJson

let syncReplaceOne (tx: RelationTxContext) (ownerId: int64) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    withRelationSqliteWrap "sync" "syncReplaceOne" (fun () ->
        ensureSchemaForOwnerType tx tx.OwnerType
        let plan = prepareUpdate tx ownerId oldOwner newOwner
        applyOps tx ownerId plan false
        if plan.Ops.Length > 0 then incrementRelationVersion tx ownerId
    )

let syncReplaceMany (tx: RelationTxContext) (ownerIds: int64 seq) (oldOwners: obj seq) (newOwner: obj) =
    ensureTxContext tx
    ensureTransaction tx
    if isNull ownerIds then nullArg "ownerIds"
    if isNull oldOwners then nullArg "oldOwners"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    for id in ownerIds do
        if id <= 0L then raise (ArgumentOutOfRangeException("ownerIds", id, "Every owner id must be > 0."))
    for oldOwner in oldOwners do
        ensureOwnerInstance tx.OwnerType oldOwner "oldOwners"

    withRelationSqliteWrap "sync" "syncReplaceMany" (fun () ->
        ensureSchemaForOwnerType tx tx.OwnerType

        let ownerIdArr = ownerIds |> Seq.toArray
        let oldOwnerArr = oldOwners |> Seq.toArray
        if ownerIdArr.Length <> oldOwnerArr.Length then
            raise (ArgumentException("ownerIds and oldOwners must have the same length.", "oldOwners"))

        for i = 0 to ownerIdArr.Length - 1 do
            let newOwnerClone = cloneOwnerForReplaceMany tx.OwnerType newOwner
            // Propagate stale-version baseline from original entity to clone so the guard fires.
            if hasLoadedRelationVersion newOwner then
                captureRelationVersion newOwnerClone (getLoadedRelationVersion newOwner)
            let plan = prepareUpdate tx ownerIdArr.[i] oldOwnerArr.[i] newOwnerClone
            applyOps tx ownerIdArr.[i] plan false
            if plan.Ops.Length > 0 then incrementRelationVersion tx ownerIdArr.[i]
    )

let syncDeleteOwner (tx: RelationTxContext) (plan: RelationDeletePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if plan.OwnerId <= 0L then raise (ArgumentOutOfRangeException("plan.OwnerId", plan.OwnerId, "ownerId must be > 0."))
    let deleteCtx = createDeleteTraversalContext()
    withRelationSqliteWrap "sync" "syncDeleteOwner" (fun () ->
        ensureRelationCatalogTable tx.Connection
        applyOwnerDeletePoliciesCore deleteCtx tx tx.OwnerTable plan.OwnerId true false
        applyTargetDeletePoliciesCore deleteCtx tx tx.OwnerTable plan.OwnerId
    )

let applyTargetDeletePolicies (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    let deleteCtx = createDeleteTraversalContext()
    withRelationSqliteWrap "delete" "applyTargetDeletePolicies" (fun () ->
        applyTargetDeletePoliciesCore deleteCtx tx targetTable targetId
    )

let applyOwnerDeletePolicies (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    let deleteCtx = createDeleteTraversalContext()
    withRelationSqliteWrap "delete" "applyOwnerDeletePolicies" (fun () ->
        ensureRelationCatalogTable tx.Connection
        applyOwnerDeletePoliciesCore deleteCtx tx ownerTable ownerId false false
    )

let applyOwnerReplacePolicies (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    let deleteCtx = createDeleteTraversalContext()
    withRelationSqliteWrap "replace" "applyOwnerReplacePolicies" (fun () ->
        ensureRelationCatalogTable tx.Connection
        applyOwnerDeletePoliciesCore deleteCtx tx ownerTable ownerId false true
    )

let globalRefCount (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    withRelationSqliteWrap "delete" "globalRefCount" (fun () ->
        globalRefCountCore tx targetTable targetId
    )

let batchLoadDBRefProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (includedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    =
    withRelationSqliteWrap "batch-load" "batchLoadDBRefProperties" (fun () ->
        RelationsBatchLoad.batchLoadDBRefProperties connection ownerTable ownerType excludedPaths includedPaths ownerEntities
    )

let batchLoadDBRefManyProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (includedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    (inTransaction: bool)
    =
    withRelationSqliteWrap "batch-load" "batchLoadDBRefManyProperties" (fun () ->
        RelationsBatchLoad.batchLoadDBRefManyProperties connection ownerTable ownerType excludedPaths includedPaths ownerEntities inTransaction
    )

/// Capture the RelationVersion baseline for loaded entities.
/// Called after batch-loading relations so the version is recorded at load time.
let captureRelationVersionForEntities
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerEntities: (int64 * obj) array)
    =
    if ownerEntities.Length > 0 then
        for (ownerId, entity) in ownerEntities do
            let version = readPersistedRelationVersion connection ownerTable ownerId
            captureRelationVersion entity version
