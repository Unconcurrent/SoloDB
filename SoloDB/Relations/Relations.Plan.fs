module internal SoloDatabase.RelationsPlan

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
open RelationsCore
let private emitSingleRelationAssignment (ops: ResizeArray<RelationUpdateManyOp>) (descriptor: RelationDescriptor) (newId: int64) =
    if newId > 0L then
        ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
    else
        ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))

let private emitClearManyOps (ops: ResizeArray<RelationUpdateManyOp>) (descriptor: RelationDescriptor) =
    ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))

let private emitAddManyOps (ops: ResizeArray<RelationUpdateManyOp>) (descriptor: RelationDescriptor) (ids: int64 array) =
    for id in ids do
        ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))

let private emitClearAndReAdd (ops: ResizeArray<RelationUpdateManyOp>) (descriptor: RelationDescriptor) (newIds: int64 array) =
    emitClearManyOps ops descriptor
    emitAddManyOps ops descriptor newIds

let prepareInsert (tx: RelationTxContext) (owner: obj) =
    ensureTxContext tx
    ensureOwnerInstance tx.OwnerType owner "owner"
    withRelationSqliteWrap "prepare" "prepareInsert" (fun () ->
        let descriptors, ops, resetMap, visited, depth = initPrepareContext tx owner

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let targetId = resolveSingleTargetIdAndCascade tx descriptor owner visited depth
                if targetId > 0L then
                    ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, targetId))
            | Many ->
                match collectManyTargetIdsAndCascade tx descriptor owner true visited depth with
                | ValueSome ids ->
                    resetMap.[descriptor.Property.Name] <- ids
                    emitAddManyOps ops descriptor ids
                | ValueNone -> ()

        prepareFileSystemReferences tx owner ValueNone false true true ops
        applyResetMapIfAny owner resetMap

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
        let descriptors, ops, resetMap, visited, depth = initPrepareContext tx newOwner

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let oldId = match oldOwner with | ValueSome old -> readSingleIdNoCascade descriptor old | ValueNone -> 0L
                let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner visited depth
                if oldId <> newId then
                    emitSingleRelationAssignment ops descriptor newId
            | Many ->
                let trackerObj = descriptor.Property.Read.Invoke(newOwner)
                if isNull trackerObj then
                    raise (ArgumentNullException(
                        descriptor.Property.Name,
                        $"Error: DBRefMany property '{descriptor.OwnerType.FullName}.{descriptor.Property.Name}' is null.\nReason: DBRefMany<T> properties must not be set to null.\nFix: Use an empty DBRefMany<T> (new()) or call Clear() to remove all links."))
                match collectManyTargetIdsAndCascade tx descriptor newOwner true visited depth with
                | ValueSome ids ->
                    resetMap.[descriptor.Property.Name] <- ids
                    if hadOldOwner then
                        emitClearAndReAdd ops descriptor ids
                    else
                        emitAddManyOps ops descriptor ids
                | ValueNone ->
                    if hadOldOwner then
                        emitClearManyOps ops descriptor

        prepareFileSystemReferences tx newOwner oldOwner true false true ops
        applyResetMapIfAny newOwner resetMap

        { Kind = RelationPlanKind.Upsert; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }
    )

let private prepareUpdateCore (replace: bool) (tx: RelationTxContext) (ownerId: int64) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    withRelationSqliteWrap "prepare" "prepareUpdate" (fun () ->
        RelationsGraphAdmission.capture tx ownerId
        let descriptors, ops, resetMap, visited, depth = initPrepareContext tx newOwner

        for descriptor in descriptors do
            match descriptor.Kind with
            | Single ->
                let oldId = readSingleIdNoCascade descriptor oldOwner
                let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner visited depth
                if oldId <> newId then
                    emitSingleRelationAssignment ops descriptor newId
            | Many ->
                match descriptor.Property.Read.Invoke(newOwner) with
                | :? IDBRefManyInternal as tracker when tracker.IsLoaded ->
                    // Loaded tracker: diff against OriginalIds
                    match collectManyTargetIdsAndCascade tx descriptor newOwner false visited depth with
                    | ValueNone -> ()
                    | ValueSome newIds ->
                        resetMap.[descriptor.Property.Name] <- newIds
                        let oldIds = tracker.OriginalIds |> Seq.toArray
                        let oldSet = HashSet<int64>(oldIds)
                        let newSet = HashSet<int64>(newIds)
                        if tracker.WasCleared || not (oldSet.SetEquals(newSet)) then
                            if tracker.WasCleared then
                                emitClearAndReAdd ops descriptor newIds
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
                        match collectManyTargetIdsAndCascade tx descriptor newOwner true visited depth with
                        | ValueNone | ValueSome [||] ->
                            // Pure clear — no items after clear.
                            resetMap.[descriptor.Property.Name] <- [||]
                            emitClearManyOps ops descriptor
                        | ValueSome newIds ->
                            // Clear()+Add(): emit clear then re-add for parity with loaded path.
                            resetMap.[descriptor.Property.Name] <- newIds
                            emitClearAndReAdd ops descriptor newIds
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
                            match collectManyTargetIdsAndCascade tx descriptor newOwner true visited depth with
                            | ValueNone -> ()
                            | ValueSome newIds ->
                                resetMap.[descriptor.Property.Name] <- newIds
                                let oldIds = readPersistedManyTargetIds tx descriptor ownerId
                                let oldSet = HashSet<int64>(oldIds)
                                let newSet = HashSet<int64>(newIds)
                                if not (oldSet.SetEquals(newSet)) then
                                    emitClearAndReAdd ops descriptor newIds
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

        prepareFileSystemReferences tx newOwner (ValueSome oldOwner) replace false (not replace) ops
        applyResetMapIfAny newOwner resetMap

        { Kind = RelationPlanKind.Update; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }
    )

let prepareUpdate tx ownerId oldOwner newOwner = prepareUpdateCore false tx ownerId oldOwner newOwner
let prepareReplacement tx ownerId oldOwner newOwner = prepareUpdateCore true tx ownerId oldOwner newOwner

let prepareDeleteOwner (tx: RelationTxContext) (ownerId: int64) (owner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType owner "owner"
    withRelationSqliteWrap "prepare" "prepareDeleteOwner" (fun () ->
        { OwnerId = ownerId; OwnerType = tx.OwnerType }
    )
