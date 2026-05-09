module internal SoloDatabase.PassPipeline

open SoloDatabase.PassTypes
open SoloDatabase.IndexModel
open SoloDatabase.ConstantFoldPass
open SoloDatabase.FlattenPass
open SoloDatabase.PushdownPass
open SoloDatabase.ProjectionPass
open SoloDatabase.CompositeGroupByCanonicalizationPass
open SoloDatabase.IndexPlanShapingPass
open SoloDatabase.JsonbRewritePolicyPass

/// Canonical SqlDu pass list applied by both the SELECT-path translator
/// (`Queryable.HelperBase`) and the chain-executor mutation path
/// (`SoloDB.Collection.Mutation`). Single source of truth — adding or
/// reordering a pass updates both consumers without drift.
let internal standardWithIndexModel (indexModel: IndexModel) : Pass list = [
    constantFold
    subqueryFlatten
    predicatePushdown
    projectionPushdown
    compositeGroupByCanonicalization
    indexPlanShaping indexModel
    jsonbRewritePolicy indexModel
]
