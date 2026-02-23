
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
    ensureRelationCatalogTable tx.Connection
    let descriptors = buildRelationDescriptors tx ownerType
    for descriptor in descriptors do
        ensureRelationSchema tx descriptor

let private asReadOnlyDict (input: Dictionary<string, int64 array>) =
    input :> IReadOnlyDictionary<string, int64 array>

let prepareInsert (tx: RelationTxContext) (owner: obj) =
    ensureTxContext tx
    ensureOwnerInstance tx.OwnerType owner "owner"
    let descriptors = buildRelationDescriptors tx tx.OwnerType
    let ops = ResizeArray<RelationUpdateManyOp>()
    let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let targetId = resolveSingleTargetIdAndCascade tx descriptor owner
            if targetId > 0L then
                ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, targetId))
        | Many ->
            match collectManyTargetIdsAndCascade tx descriptor owner true with
            | ValueSome ids ->
                resetMap.[descriptor.Property.Name] <- ids
                for id in ids do
                    ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
            | ValueNone -> ()

    if resetMap.Count > 0 then
        resetDbRefManyTrackers owner (asReadOnlyDict resetMap)

    { Kind = "Insert"; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }

let prepareUpsert (tx: RelationTxContext) (oldOwner: obj voption) (newOwner: obj) =
    ensureTxContext tx
    let hadOldOwner =
        match oldOwner with
        | ValueSome old -> ensureOwnerInstance tx.OwnerType old "oldOwner"; true
        | ValueNone -> false

    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    let descriptors = buildRelationDescriptors tx tx.OwnerType
    let ops = ResizeArray<RelationUpdateManyOp>()
    let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let oldId = match oldOwner with | ValueSome old -> readSingleIdNoCascade descriptor old | ValueNone -> 0L
            let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner
            if oldId <> newId then
                if newId > 0L then ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
                else ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))
        | Many ->
            match collectManyTargetIdsAndCascade tx descriptor newOwner true with
            | ValueSome ids ->
                resetMap.[descriptor.Property.Name] <- ids
                if hadOldOwner then ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                for id in ids do ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))
            | ValueNone ->
                if hadOldOwner then ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))

    if resetMap.Count > 0 then
        resetDbRefManyTrackers newOwner (asReadOnlyDict resetMap)

    { Kind = "Upsert"; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }

let prepareUpdate (tx: RelationTxContext) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    let descriptors = buildRelationDescriptors tx tx.OwnerType
    let ops = ResizeArray<RelationUpdateManyOp>()
    let resetMap = Dictionary<string, int64 array>(StringComparer.Ordinal)

    for descriptor in descriptors do
        match descriptor.Kind with
        | Single ->
            let oldId = readSingleIdNoCascade descriptor oldOwner
            let newId = resolveSingleTargetIdAndCascade tx descriptor newOwner
            if oldId <> newId then
                if newId > 0L then ops.Add(SetDBRefToId(descriptor.PropertyPath, descriptor.TargetType, newId))
                else ops.Add(SetDBRefToNone(descriptor.PropertyPath, descriptor.TargetType))
        | Many ->
            match collectManyTargetIdsAndCascade tx descriptor newOwner false with
            | ValueNone -> ()
            | ValueSome newIds ->
                resetMap.[descriptor.Property.Name] <- newIds
                let oldIds = match collectManyTargetIdsAndCascade tx descriptor oldOwner false with | ValueSome ids -> ids | ValueNone -> [||]
                let oldSet = HashSet<int64>(oldIds)
                let newSet = HashSet<int64>(newIds)
                if not (oldSet.SetEquals(newSet)) then
                    ops.Add(ClearDBRefMany(descriptor.PropertyPath, descriptor.TargetType))
                    for id in newIds do ops.Add(AddDBRefMany(descriptor.PropertyPath, descriptor.TargetType, id))

    if resetMap.Count > 0 then
        resetDbRefManyTrackers newOwner (asReadOnlyDict resetMap)

    { Kind = "Update"; OwnerType = tx.OwnerType; Ops = ops |> Seq.toList }

let prepareDeleteOwner (tx: RelationTxContext) (ownerId: int64) (owner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType owner "owner"
    { OwnerId = ownerId; OwnerType = tx.OwnerType }

let syncInsert (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureSchemaForOwnerType tx tx.OwnerType
    applyOps tx ownerId plan false

let syncUpsert (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureSchemaForOwnerType tx tx.OwnerType
    applyOps tx ownerId plan false

let syncUpdate (tx: RelationTxContext) (ownerId: int64) (plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureSchemaForOwnerType tx tx.OwnerType
    let updateOwnerJson = StringComparer.Ordinal.Equals(plan.Kind, "UpdateMany")
    applyOps tx ownerId plan updateOwnerJson

let syncReplaceOne (tx: RelationTxContext) (ownerId: int64) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    ensureSchemaForOwnerType tx tx.OwnerType
    let plan = prepareUpdate tx oldOwner newOwner
    applyOps tx ownerId plan false

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

    ensureSchemaForOwnerType tx tx.OwnerType

    let ownerIdArr = ownerIds |> Seq.toArray
    let oldOwnerArr = oldOwners |> Seq.toArray
    if ownerIdArr.Length <> oldOwnerArr.Length then
        raise (ArgumentException("ownerIds and oldOwners must have the same length.", "oldOwners"))

    for i = 0 to ownerIdArr.Length - 1 do
        let plan = prepareUpdate tx oldOwnerArr.[i] newOwner
        applyOps tx ownerIdArr.[i] plan false

let syncDeleteOwner (tx: RelationTxContext) (plan: RelationDeletePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if plan.OwnerId <= 0L then raise (ArgumentOutOfRangeException("plan.OwnerId", plan.OwnerId, "ownerId must be > 0."))
    ensureRelationCatalogTable tx.Connection
    applyOwnerDeletePoliciesCore tx tx.OwnerTable plan.OwnerId true
    applyTargetDeletePoliciesCore tx tx.OwnerTable plan.OwnerId

let applyTargetDeletePolicies (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    applyTargetDeletePoliciesCore tx targetTable targetId

let applyOwnerDeletePolicies (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureRelationCatalogTable tx.Connection
    applyOwnerDeletePoliciesCore tx ownerTable ownerId false

let globalRefCount (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    globalRefCountCore tx targetTable targetId

let batchLoadDBRefProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    =
    RelationsBatchLoad.batchLoadDBRefProperties connection ownerTable ownerType excludedPaths ownerEntities

let batchLoadDBRefManyProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (ownerEntities: (int64 * obj) array)
    =
    RelationsBatchLoad.batchLoadDBRefManyProperties connection ownerTable ownerType excludedPaths ownerEntities
