module internal SoloDatabase.QueryableGroupByAliases

// ══════════════════════════════════════════════════════════════
// Shared constants for writer/optimizer synthetic alias and
// constructor-name contracts.
//
// Single source of truth for:
//   - the synthetic compound group-key projection alias,
//   - the slot-alias families emitted by GroupBy and SequenceOps,
//   - the SqlExpr.FunctionCall constructor names for JSON objects.
//
// All writer emission sites, the CompositeGroupByCanonicalization
// optimizer pass, and any downstream consumer MUST reference these
// constants rather than typing the literal string. This makes
// drift between writer and optimizer impossible by construction.
//
// Function-name match convention (two distinct rules by origin):
//   - Endogenous constructor names WE emit (json_object, jsonb_object):
//     exact-match against the shared constants jsonObjectFn / jsonbObjectFn.
//     Writer-side FunctionCall construction and the optimizer decomposer
//     both reference these constants — compile-time drift protection.
//   - Exogenous SQLite function names in user expressions
//     (non-deterministic list, now-sentinel time calls):
//     lowercase-normalized matching inside the consumer. User-origin names
//     have no cross-file enforcement seam; defensive normalization is the
//     correct posture there.
// ══════════════════════════════════════════════════════════════

/// Canonical alias for the writer-emitted synthetic compound group-key projection.
let internal syntheticGroupKeyAlias : string = "__solodb_group_key"

/// Slot alias family used by CompositeGroupByCanonicalization to inject
/// scalar component projections on the inner derived table:
/// "__solodb_group_key0", "__solodb_group_key1", ...
let internal syntheticGroupKeySlotAlias (i: int) : string =
    sprintf "%s%d" syntheticGroupKeyAlias i

/// Canonical alias family for SequenceOps-style scalar-slot projection
/// materialization: "__solodb_scalar_slot0", "__solodb_scalar_slot1", ...
let internal syntheticScalarSlotAlias (i: int) : string =
    sprintf "__solodb_scalar_slot%d" i

/// SqlExpr.FunctionCall constructor name for TEXT-JSON object construction.
/// Writer-side FunctionCall construction MUST use this constant; the
/// optimizer pass decomposer compares against it by exact-match.
let internal jsonObjectFn : string = "json_object"

/// SqlExpr.FunctionCall constructor name for binary-JSON object construction.
/// Same contract as jsonObjectFn.
let internal jsonbObjectFn : string = "jsonb_object"
