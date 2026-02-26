
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

let private relationSqlWrapKey = "error[SDBREL0010]"

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
            $"error[SDBREL0010] phase={phase} operation={operation} message=relation-sql-boundary-operation-failed\n" +
            "Error: relation SQL operation failed.\n" +
            $"Reason: underlying SQLite error at relation boundary (sqliteErrorCode={ex.SqliteErrorCode}, sqliteExtendedErrorCode={ex.SqliteExtendedErrorCode}).\n" +
            "Fix: check schema, relation metadata, link-table integrity, and constraints; then retry with valid state."
        raise (InvalidOperationException(message, ex))

let resetDbRefManyTrackers (owner: obj) (committedLinkedIdsByProperty: IReadOnlyDictionary<string, int64 array>) =
    if isNull owner then nullArg "owner"
    if isNull committedLinkedIdsByProperty then nullArg "committedLinkedIdsByProperty"

    let props = owner.GetType().GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
    for prop in props do
        let value = prop.GetValue owner
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
        let descriptors = buildRelationDescriptors tx ownerType
        for descriptor in descriptors do
            ensureMetadataNotResurrected tx descriptor
            ensureRelationSchema tx descriptor
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

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let targetId = resolveSingleTargetIdAndCascade tx descriptor owner visited
                if targetId > 0L then
                    ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, targetId))
            | Many ->
                match collectManyTargetIdsAndCascade tx descriptor owner true visited with
                | ValueSome ids ->
                    resetMap.[descriptor.Property.Name] <- ids
                    for id in ids do
                        ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                | ValueNone -> ()

        if resetMap.Count > 0 then
            resetDbRefManyTrackers owner (asReadOnlyDict resetMap)

        { Kind = "Insert"; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }
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

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let oldId = match oldOwner with | ValueSome old -> readSingleIdNoCascade descriptor old | ValueNone -> 0L
                let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner visited
                if oldId <> newId then
                    if newId > 0L then ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
                    else ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))
            | Many ->
                match collectManyTargetIdsAndCascade tx descriptor newOwner true visited with
                | ValueSome ids ->
                    resetMap.[descriptor.Property.Name] <- ids
                    if hadOldOwner then ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                    for id in ids do ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                | ValueNone ->
                    if hadOldOwner then ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))

        if resetMap.Count > 0 then
            resetDbRefManyTrackers newOwner (asReadOnlyDict resetMap)

        { Kind = "Upsert"; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }
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

let prepareUpdate (tx: RelationTxContext) (ownerId: int64) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    withRelationSqliteWrap "prepare" "prepareUpdate" (fun () ->
        let descriptors = buildRelationDescriptors tx tx.OwnerType
        let ops = ResizeArray<RelationUpdateManyOp>()
        let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)
        let visited = HashSet<obj>(refComparer)
        visited.Add(newOwner) |> ignore

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let oldId = readSingleIdNoCascade descriptor oldOwner
                let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner visited
                if oldId <> newId then
                    if newId > 0L then ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
                    else ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))
            | Many ->
                match descriptor.Property.GetValue(newOwner) with
                | :? IDBRefManyInternal as tracker when tracker.IsLoaded ->
                    // Loaded tracker: diff against OriginalIds
                    match collectManyTargetIdsAndCascade tx descriptor newOwner false visited with
                    | ValueNone -> ()
                    | ValueSome newIds ->
                        resetMap.[descriptor.Property.Name] <- newIds
                        let oldIds = tracker.OriginalIds |> Seq.toArray
                        let oldSet = HashSet<int64>(oldIds)
                        let newSet = HashSet<int64>(newIds)
                        if not (oldSet.SetEquals(newSet)) then
                            ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                            for id in newIds do ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
                | :? IDBRefManyInternal as tracker when not tracker.IsLoaded && tracker.HasPendingMutations ->
                    if tracker.WasCleared then
                        // Unloaded + Clear(): collect post-clear items to detect Clear()+Add() pattern.
                        match collectManyTargetIdsAndCascade tx descriptor newOwner true visited with
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
                        // Unloaded + pending mutations (Add without load): diff against persisted ids
                        match collectManyTargetIdsAndCascade tx descriptor newOwner true visited with
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
                    ()
                | _ ->
                    raise (InvalidOperationException(
                        $"Error: Property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' does not implement IDBRefManyInternal.\nReason: The relation property type is incorrect.\nFix: Use DBRefMany<T> for multi-relations."))

        if resetMap.Count > 0 then
            resetDbRefManyTrackers newOwner (asReadOnlyDict resetMap)

        { Kind = "Update"; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }
    )

let prepareDeleteOwner (tx: RelationTxContext) (ownerId: int64) (owner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType owner "owner"
    withRelationSqliteWrap "prepare" "prepareDeleteOwner" (fun () ->
        { OwnerId = ownerId; OwnerType = tx.OwnerType }
    )

let syncInsert (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    withRelationSqliteWrap "sync" "syncInsert" (fun () ->
        ensureSchemaForOwnerType tx tx.OwnerType
        applyOps tx ownerId plan false
    )

let syncUpsert (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    withRelationSqliteWrap "sync" "syncUpsert" (fun () ->
        ensureSchemaForOwnerType tx tx.OwnerType
        applyOps tx ownerId plan false
    )

let syncUpdate (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    withRelationSqliteWrap "sync" "syncUpdate" (fun () ->
        ensureSchemaForOwnerType tx tx.OwnerType
        let updateOwnerJson = StringComparer.Ordinal.Equals(plan.Kind, "UpdateMany")
        applyOps tx ownerId plan updateOwnerJson
    )

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
            let plan = prepareUpdate tx ownerIdArr.[i] oldOwnerArr.[i] newOwner
            applyOps tx ownerIdArr.[i] plan false
    )

let syncDeleteOwner (tx: RelationTxContext) (plan: RelationDeletePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if plan.OwnerId <= 0L then raise (ArgumentOutOfRangeException("plan.OwnerId", plan.OwnerId, "ownerId must be > 0."))
    withRelationSqliteWrap "sync" "syncDeleteOwner" (fun () ->
        ensureRelationCatalogTable tx.Connection
        applyOwnerDeletePoliciesCore tx tx.OwnerTable plan.OwnerId true
        applyTargetDeletePoliciesCore tx tx.OwnerTable plan.OwnerId
    )

let applyTargetDeletePolicies (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    withRelationSqliteWrap "delete" "applyTargetDeletePolicies" (fun () ->
        applyTargetDeletePoliciesCore tx targetTable targetId
    )

let applyOwnerDeletePolicies (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    withRelationSqliteWrap "delete" "applyOwnerDeletePolicies" (fun () ->
        ensureRelationCatalogTable tx.Connection
        applyOwnerDeletePoliciesCore tx ownerTable ownerId false
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
    =
    withRelationSqliteWrap "batch-load" "batchLoadDBRefManyProperties" (fun () ->
        RelationsBatchLoad.batchLoadDBRefManyProperties connection ownerTable ownerType excludedPaths includedPaths ownerEntities
    )
