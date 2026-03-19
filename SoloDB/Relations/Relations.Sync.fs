module internal SoloDatabase.RelationsSync

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
open RelationsCore
open RelationsPlan

type RelationTxContext = RelationsTypes.RelationTxContext
type RelationWritePlan = RelationsTypes.RelationWritePlan
type RelationDeletePlan = RelationsTypes.RelationDeletePlan
type RelationUpdateManyOp = RelationsTypes.RelationUpdateManyOp

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
    (whitelistMode: bool)
    (ownerEntities: (int64 * obj) array)
    (inTransaction: bool)
    =
    withRelationSqliteWrap "batch-load" "batchLoadDBRefProperties" (fun () ->
        RelationsBatchLoad.batchLoadDBRefProperties connection ownerTable ownerType excludedPaths includedPaths whitelistMode ownerEntities inTransaction 0 (HashSet()) ""
    )

let batchLoadDBRefManyProperties
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (includedPaths: HashSet<string>)
    (whitelistMode: bool)
    (ownerEntities: (int64 * obj) array)
    (inTransaction: bool)
    =
    withRelationSqliteWrap "batch-load" "batchLoadDBRefManyProperties" (fun () ->
        RelationsBatchLoad.batchLoadDBRefManyProperties connection ownerTable ownerType excludedPaths includedPaths whitelistMode ownerEntities inTransaction 0 (HashSet()) ""
    )

let recurseLoadedRelationTargets
    (connection: SqliteConnection)
    (ownerTable: string)
    (ownerType: Type)
    (excludedPaths: HashSet<string>)
    (includedPaths: HashSet<string>)
    (whitelistMode: bool)
    (ownerEntities: (int64 * obj) array)
    (inTransaction: bool)
    =
    withRelationSqliteWrap "batch-load" "recurseLoadedRelationTargets" (fun () ->
        RelationsBatchLoad.recurseLoadedRelationTargets connection ownerTable ownerType excludedPaths includedPaths whitelistMode ownerEntities inTransaction
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
