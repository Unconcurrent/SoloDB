module internal SoloDatabase.Relations

type RelationTxContext = RelationsTypes.RelationTxContext
type RelationWritePlan = RelationsTypes.RelationWritePlan
type RelationDeletePlan = RelationsTypes.RelationDeletePlan
type RelationUpdateManyOp = RelationsTypes.RelationUpdateManyOp

let internal withRelationSqliteWrap = RelationsCore.withRelationSqliteWrap
let resetDbRefManyTrackers = RelationsCore.resetDbRefManyTrackers
let ensureSchemaForOwnerType = RelationsCore.ensureSchemaForOwnerType

let prepareInsert = RelationsPlan.prepareInsert
let prepareUpsert = RelationsPlan.prepareUpsert
let prepareUpdate = RelationsPlan.prepareUpdate
let prepareDeleteOwner = RelationsPlan.prepareDeleteOwner

let syncInsert = RelationsSync.syncInsert
let syncUpsert = RelationsSync.syncUpsert
let syncUpdate = RelationsSync.syncUpdate
let syncReplaceOne = RelationsSync.syncReplaceOne
let syncReplaceMany = RelationsSync.syncReplaceMany
let syncDeleteOwner = RelationsSync.syncDeleteOwner

let applyTargetDeletePolicies = RelationsSync.applyTargetDeletePolicies
let applyOwnerDeletePolicies = RelationsSync.applyOwnerDeletePolicies
let applyOwnerReplacePolicies = RelationsSync.applyOwnerReplacePolicies
let globalRefCount = RelationsSync.globalRefCount

let batchLoadDBRefProperties = RelationsSync.batchLoadDBRefProperties
let batchLoadDBRefManyProperties = RelationsSync.batchLoadDBRefManyProperties
let captureRelationVersionForEntities = RelationsSync.captureRelationVersionForEntities
