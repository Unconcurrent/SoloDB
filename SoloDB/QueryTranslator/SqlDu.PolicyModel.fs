module SoloDatabase.PolicyModel

// ══════════════════════════════════════════════════════════════
// C9a: Policy Decision Model
//
// Types for JSONB rewrite and materialization flatten/preserve
// policy decisions. Maps to Bellard policy classes and Carmack
// audit verdicts per the approved vocabulary mapping.
//
// Policy classes (Bellard):
//   FLATTEN_ALLOWED — rewrite/flatten is legal and behavior-preserving
//   PRESERVE_REQUIRED — must remain as-is for behavior/contract/plan
//   OUT_OF_SCOPE_REFUSAL — belongs to later cycle or outside C9 surface
//
// Audit verdicts (Carmack M6):
//   JSONB_REWRITTEN — JR-1/2/3 path/chain rewrite executed
//   MATERIALIZATION_FLATTENED — JR-4 wrapper removal executed
//   PRESERVED_FOR_BEHAVIOR — preserved due to behavior risk
//   PRESERVED_FOR_PLAN — preserved due to index visibility risk
//   NO_CHANGE — no rewrite candidate found or out of scope
//
// Reason codes (Carmack M6):
//   BehaviorRisk, PathSemanticsRisk, MaterializationContract,
//   IndexVisibilityRisk, NoKnownBenefit, OutOfScope,
//   JsonJsonbVariantRisk, ConflictingPaths, MustNotBoundary,
//   DmlPassthrough, UnionAllBoundary
// ══════════════════════════════════════════════════════════════

/// Bellard policy class: the decision.
type PolicyClass =
    | FlattenAllowed
    | PreserveRequired
    | OutOfScopeRefusal

/// Carmack audit verdict: what was done.
type C9Verdict =
    | JsonbRewritten
    | MaterializationFlattened
    | PreservedForBehavior
    | PreservedForPlan
    | C9NoChange

/// Carmack reason code: why the verdict was reached.
type C9Reason =
    | BehaviorRisk
    | PathSemanticsRisk
    | MaterializationContract
    | IndexVisibilityRisk
    | NoKnownBenefit
    | OutOfScope
    | JsonJsonbVariantRisk
    | ConflictingPaths
    | MustNotBoundary
    | C9DmlPassthrough
    | C9UnionAllBoundary

/// A single C9 decision trace record.
type C9DecisionRecord = {
    ShapeId: string
    PolicyClass: PolicyClass
    Verdict: C9Verdict
    ReasonCode: C9Reason
    BeforeFingerprint: string
    AfterFingerprint: string
    Changed: bool
}

/// Format a C9 verdict as string for JSONL output.
let verdictToString (v: C9Verdict) : string =
    match v with
    | JsonbRewritten -> "JSONB_REWRITTEN"
    | MaterializationFlattened -> "MATERIALIZATION_FLATTENED"
    | PreservedForBehavior -> "PRESERVED_FOR_BEHAVIOR"
    | PreservedForPlan -> "PRESERVED_FOR_PLAN"
    | C9NoChange -> "NO_CHANGE"

/// Format a C9 reason as string for JSONL output.
let reasonToString (r: C9Reason) : string =
    match r with
    | BehaviorRisk -> "BehaviorRisk"
    | PathSemanticsRisk -> "PathSemanticsRisk"
    | MaterializationContract -> "MaterializationContract"
    | IndexVisibilityRisk -> "IndexVisibilityRisk"
    | NoKnownBenefit -> "NoKnownBenefit"
    | OutOfScope -> "OutOfScope"
    | JsonJsonbVariantRisk -> "JsonJsonbVariantRisk"
    | ConflictingPaths -> "ConflictingPaths"
    | MustNotBoundary -> "MustNotBoundary"
    | C9DmlPassthrough -> "DmlPassthrough"
    | C9UnionAllBoundary -> "UnionAllBoundary"

/// Format a C9 policy class as string.
let policyClassToString (p: PolicyClass) : string =
    match p with
    | FlattenAllowed -> "FLATTEN_ALLOWED"
    | PreserveRequired -> "PRESERVE_REQUIRED"
    | OutOfScopeRefusal -> "OUT_OF_SCOPE_REFUSAL"

/// Format a decision record as a single-line JSON (for c9-audit.jsonl).
let formatAsJsonl (record: C9DecisionRecord) : string =
    sprintf "{\"shapeId\":\"%s\",\"policyClass\":\"%s\",\"verdict\":\"%s\",\"reasonCode\":\"%s\",\"beforeFingerprint\":\"%s\",\"afterFingerprint\":\"%s\",\"changed\":%s}"
        record.ShapeId
        (policyClassToString record.PolicyClass)
        (verdictToString record.Verdict)
        (reasonToString record.ReasonCode)
        record.BeforeFingerprint record.AfterFingerprint
        (if record.Changed then "true" else "false")
