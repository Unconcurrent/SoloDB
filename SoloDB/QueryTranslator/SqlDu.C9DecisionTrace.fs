module SoloDatabase.C9DecisionTrace

open SqlDu.Engine.C1.Spec
open SoloDatabase.PassRunner
open SoloDatabase.IndexModel
open SoloDatabase.PolicyModel
open SoloDatabase.MaterializationPolicy
open SoloDatabase.JsonbRewritePolicy
open SoloDatabase.ExpressionPredicates

// ══════════════════════════════════════════════════════════════
// C9f: Decision Trace Integration
//
// Per-shape classification: policy class + verdict + reason code.
// Maps internal policy classes to the trace verdict vocabulary.
//
// Vocabulary mapping (approved):
//   FLATTEN_ALLOWED + changed -> JSONB_REWRITTEN or MATERIALIZATION_FLATTENED
//   PRESERVE_REQUIRED + unchanged -> PRESERVED_FOR_BEHAVIOR or PRESERVED_FOR_PLAN
//   OUT_OF_SCOPE_REFUSAL + unchanged -> NO_CHANGE
//
// C8 IndexModel integration:
//   When a shape has JSONB extractions matching C8 index entries
//   and the shape was unchanged, classifies as PRESERVED_FOR_PLAN
//   with IndexVisibilityRisk reason.
// ══════════════════════════════════════════════════════════════

/// Check if a SelectCore has must-not-rewrite boundary indicators.
let private coreHasMustNotBoundary (core: SelectCore) : bool =
    not core.GroupBy.IsEmpty
    || core.Having.IsSome
    || core.Distinct
    || (match core.Source with Some(FromJsonEach _) -> true | _ -> false)

/// Check if any projection has aggregate or window calls.
let private hasAggOrWindow (expr: SqlExpr) : bool =
    hasAggregateCall expr || hasWindowFunction expr

/// Check if an expression contains any JsonSetExpr or JsonArrayExpr
/// (JSONB-relevant for materialization classification).
let rec private hasJsonbMaterialization (expr: SqlExpr) : bool =
    match expr with
    | JsonSetExpr _ -> true
    | JsonArrayExpr _ -> true
    | Binary(l, _, r) -> hasJsonbMaterialization l || hasJsonbMaterialization r
    | Unary(_, e) -> hasJsonbMaterialization e
    | Cast(e, _) -> hasJsonbMaterialization e
    | FunctionCall(_, args) -> args |> List.exists hasJsonbMaterialization
    | Coalesce(exprs) -> exprs |> List.exists hasJsonbMaterialization
    | CaseExpr(branches, elseE) ->
        branches |> List.exists (fun (c, r) -> hasJsonbMaterialization c || hasJsonbMaterialization r)
        || (elseE |> Option.map hasJsonbMaterialization |> Option.defaultValue false)
    | _ -> false

/// Check if a SelectCore has any JSONB extraction forms matching C8 index entries.
/// Checks WHERE, JOIN ON, and ORDER BY — the expression sites where index
/// visibility matters for query plan quality.
let private coreHasIndexMatchedExtractions (model: IndexModel) (core: SelectCore) : bool =
    let checkExpr expr = exprContainsIndexedForm model expr
    (core.Where |> Option.map checkExpr |> Option.defaultValue false)
    || core.Joins |> List.exists (fun j -> j.On |> Option.map checkExpr |> Option.defaultValue false)
    || core.OrderBy |> List.exists (fun ob -> checkExpr ob.Expr)

/// Classify a single shape's C9 decision with index model.
let classifyC9WithModel (model: IndexModel) (shapeId: string) (input: SqlStatement) (output: SqlStatement) : C9DecisionRecord =
    let beforeFp = fingerprint input
    let afterFp = fingerprint output
    let changed = beforeFp <> afterFp

    let policyClass, verdict, reason =
        match input with
        // DML passthrough
        | InsertStmt _ | UpdateStmt _ | DeleteStmt _ | DdlStmt _ ->
            OutOfScopeRefusal, C9NoChange, C9DmlPassthrough

        | SelectStmt sel ->
            match sel.Body with
            // UNION ALL boundary — hard fence
            | UnionAllSelect _ ->
                OutOfScopeRefusal, C9NoChange, C9UnionAllBoundary

            | SingleSelect core ->
                // Must-not boundary
                if coreHasMustNotBoundary core then
                    OutOfScopeRefusal, C9NoChange, MustNotBoundary
                elif core.Projections |> List.exists (fun p -> hasAggOrWindow p.Expr) then
                    OutOfScopeRefusal, C9NoChange, MustNotBoundary
                // Relation materialization — PRESERVE_REQUIRED
                elif coreHasRelationMaterialization core then
                    PreserveRequired, PreservedForBehavior, MaterializationContract
                // Group materialization — PRESERVE_REQUIRED
                elif coreHasGroupMaterialization core then
                    PreserveRequired, PreservedForBehavior, BehaviorRisk
                // Index visibility — PRESERVED_FOR_PLAN
                elif not changed && coreHasIndexMatchedExtractions model core then
                    PreserveRequired, PreservedForPlan, IndexVisibilityRisk
                elif changed then
                    // Something was rewritten — classify what kind
                    // Check if the change was a materialization flatten (JR-4):
                    // Input has a JsonSetExpr that was simplified (fewer assignments
                    // or replaced entirely) by identity assignment removal.
                    let inputJsonSetAssignmentCount =
                        core.Projections |> List.sumBy (fun p ->
                            match p.Expr with
                            | JsonSetExpr(_, assignments) -> assignments.Length
                            | _ -> 0)
                    let outputJsonSetAssignmentCount =
                        match output with
                        | SelectStmt outSel ->
                            match outSel.Body with
                            | SingleSelect outCore ->
                                outCore.Projections |> List.sumBy (fun p ->
                                    match p.Expr with
                                    | JsonSetExpr(_, assignments) -> assignments.Length
                                    | _ -> 0)
                            | _ -> 0
                        | _ -> 0
                    if inputJsonSetAssignmentCount > 0 && outputJsonSetAssignmentCount < inputJsonSetAssignmentCount then
                        FlattenAllowed, MaterializationFlattened, NoKnownBenefit
                    else
                        FlattenAllowed, JsonbRewritten, NoKnownBenefit
                else
                    // No change — determine why
                    let hasJsonb =
                        core.Projections |> List.exists (fun p -> hasJsonbMaterialization p.Expr)
                        || (core.Where |> Option.map hasJsonbMaterialization |> Option.defaultValue false)
                    if hasJsonb then
                        PreserveRequired, PreservedForBehavior, BehaviorRisk
                    else
                        OutOfScopeRefusal, C9NoChange, NoKnownBenefit

    { ShapeId = shapeId
      PolicyClass = policyClass
      Verdict = verdict
      ReasonCode = reason
      BeforeFingerprint = beforeFp
      AfterFingerprint = afterFp
      Changed = changed }

/// Classify a single shape's C9 decision (no index model — empty).
let classifyC9 (shapeId: string) (input: SqlStatement) (output: SqlStatement) : C9DecisionRecord =
    classifyC9WithModel emptyModel shapeId input output

/// Classify a batch of shapes.
let classifyC9Batch (shapes: (string * SqlStatement * SqlStatement) list) : C9DecisionRecord list =
    shapes |> List.map (fun (id, input, output) -> classifyC9 id input output)
