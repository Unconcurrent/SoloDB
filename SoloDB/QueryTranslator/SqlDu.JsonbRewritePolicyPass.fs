module SoloDatabase.JsonbRewritePolicyPass

open SoloDatabase.PassTypes
open SoloDatabase.IndexModel
open SoloDatabase.JsonbRewritePolicy

// ══════════════════════════════════════════════════════════════
// JSONB Rewrite Policy Pass Registration (C9 closure capture)
//
// Zero C4 changes. Standard pass factory pattern (same as C8).
// Closure-captures the read-only C8 IndexModel to inform
// index visibility preservation decisions.
// ══════════════════════════════════════════════════════════════

/// Create a JsonbRewritePolicy pass with C8 index model.
let jsonbRewritePolicy (model: IndexModel) : Pass = {
    Name = "JsonbRewritePolicy"
    Transform = fun stmt -> rewriteStatementWithModel model stmt
}
