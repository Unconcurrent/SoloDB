module internal SoloDatabase.Relations

open System
open System.Reflection
open System.Collections.Generic
open Microsoft.Data.Sqlite
open SoloDatabase

type internal RelationTxContext = {
    Connection: SqliteConnection
    OwnerTable: string
    OwnerType: Type
    InTransaction: bool
}

type internal RelationUpdateManyOp =
    | SetDBRefToId of PropertyPath: string * TargetType: Type * TargetId: int64
    | SetDBRefToNone of PropertyPath: string * TargetType: Type
    | AddDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
    | RemoveDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
    | ClearDBRefMany of PropertyPath: string * TargetType: Type

type internal RelationWritePlan = {
    Kind: string
    OwnerType: Type
    Ops: RelationUpdateManyOp list
}

type internal RelationDeletePlan = {
    OwnerId: int64
    OwnerType: Type
}

let private ensureTransaction (tx: RelationTxContext) =
    if not tx.InTransaction then
        raise (InvalidOperationException("Relations API requires active transaction."))

let private ensureTxContext (tx: RelationTxContext) =
    if isNull tx.Connection then nullArg "tx.Connection"
    if String.IsNullOrWhiteSpace tx.OwnerTable then
        raise (ArgumentException("OwnerTable is required.", "tx.OwnerTable"))
    if isNull tx.OwnerType then nullArg "tx.OwnerType"

let private ensureOwnerInstance (ownerType: Type) (owner: obj) (argName: string) =
    if isNull owner then nullArg argName
    if isNull ownerType then nullArg "ownerType"
    let actual = owner.GetType()
    if not (ownerType.IsAssignableFrom actual) then
        raise (ArgumentException($"Invalid owner instance type. Expected assignable to {ownerType.FullName}, got {actual.FullName}.", argName))

let private emptyPlan kind ownerType =
    { Kind = kind
      OwnerType = ownerType
      Ops = [] }

let ensureSchemaForOwnerType (tx: RelationTxContext) (ownerType: Type) =
    ensureTxContext tx
    if isNull ownerType then nullArg "ownerType"
    ()

let prepareInsert (tx: RelationTxContext) (owner: obj) =
    ensureTxContext tx
    ensureOwnerInstance tx.OwnerType owner "owner"
    emptyPlan "Insert" tx.OwnerType

let prepareUpsert (tx: RelationTxContext) (oldOwner: obj voption) (newOwner: obj) =
    ensureTxContext tx
    match oldOwner with
    | ValueSome old -> ensureOwnerInstance tx.OwnerType old "oldOwner"
    | ValueNone -> ()
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    emptyPlan "Upsert" tx.OwnerType

let prepareUpdate (tx: RelationTxContext) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    emptyPlan "Update" tx.OwnerType

let prepareDeleteOwner (tx: RelationTxContext) (ownerId: int64) (owner: obj) =
    ensureTxContext tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType owner "owner"
    { OwnerId = ownerId
      OwnerType = tx.OwnerType }

let syncInsert (tx: RelationTxContext) (ownerId: int64) (_plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ()

let syncUpsert (tx: RelationTxContext) (ownerId: int64) (_plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ()

let syncUpdate (tx: RelationTxContext) (ownerId: int64) (_plan: RelationWritePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ()

let syncReplaceOne (tx: RelationTxContext) (ownerId: int64) (oldOwner: obj) (newOwner: obj) =
    ensureTxContext tx
    ensureTransaction tx
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ensureOwnerInstance tx.OwnerType oldOwner "oldOwner"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    ()

let syncReplaceMany (tx: RelationTxContext) (ownerIds: int64 seq) (oldOwners: obj seq) (newOwner: obj) =
    ensureTxContext tx
    ensureTransaction tx
    if isNull ownerIds then nullArg "ownerIds"
    if isNull oldOwners then nullArg "oldOwners"
    ensureOwnerInstance tx.OwnerType newOwner "newOwner"
    for id in ownerIds do
        if id <= 0L then
            raise (ArgumentOutOfRangeException("ownerIds", id, "Every owner id must be > 0."))
    for oldOwner in oldOwners do
        ensureOwnerInstance tx.OwnerType oldOwner "oldOwners"
    ()

let syncDeleteOwner (tx: RelationTxContext) (plan: RelationDeletePlan) =
    ensureTxContext tx
    ensureTransaction tx
    if plan.OwnerId <= 0L then
        raise (ArgumentOutOfRangeException("plan.OwnerId", plan.OwnerId, "ownerId must be > 0."))
    ()

let applyTargetDeletePolicies (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace targetTable then
        raise (ArgumentException("targetTable is required.", "targetTable"))
    if targetId <= 0L then
        raise (ArgumentOutOfRangeException("targetId", targetId, "targetId must be > 0."))
    ()

let applyOwnerDeletePolicies (tx: RelationTxContext) (ownerTable: string) (ownerId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace ownerTable then
        raise (ArgumentException("ownerTable is required.", "ownerTable"))
    if ownerId <= 0L then
        raise (ArgumentOutOfRangeException("ownerId", ownerId, "ownerId must be > 0."))
    ()

let globalRefCount (tx: RelationTxContext) (targetTable: string) (targetId: int64) =
    ensureTxContext tx
    ensureTransaction tx
    if String.IsNullOrWhiteSpace targetTable then
        raise (ArgumentException("targetTable is required.", "targetTable"))
    if targetId <= 0L then
        raise (ArgumentOutOfRangeException("targetId", targetId, "targetId must be > 0."))
    0

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
