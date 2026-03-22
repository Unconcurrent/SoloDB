module SoloDatabase.JsonbRewritePolicyPass

open SoloDatabase.PassTypes
open SoloDatabase.IndexModel
open SoloDatabase.JsonbRewritePolicy

// ══════════════════════════════════════════════════════════════
// JSONB rewrite policy pass registration.
//
// No contract changes. Standard pass factory pattern.
// Closure-captures the read-only index model to inform
// index visibility preservation decisions.
// ══════════════════════════════════════════════════════════════

/// Create a JsonbRewritePolicy pass with an index model.
let jsonbRewritePolicy (model: IndexModel) : Pass = {
    Name = "JsonbRewritePolicy"
    Transform = rewriteStatementWithModel model
}
