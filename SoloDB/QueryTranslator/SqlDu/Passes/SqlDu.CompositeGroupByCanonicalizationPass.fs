module internal SoloDatabase.CompositeGroupByCanonicalizationPass

open SoloDatabase.PassTypes
open SoloDatabase.CompositeGroupByCanonicalization

// ══════════════════════════════════════════════════════════════
// CompositeGroupByCanonicalization pass registration.
//
// Pass factory returns a standard Pass so the framework sees a
// standard SqlStatement -> struct(SqlStatement * bool) function.
// ══════════════════════════════════════════════════════════════

/// Create a CompositeGroupByCanonicalization pass.
let compositeGroupByCanonicalization : Pass = {
    Name = "CompositeGroupByCanonicalization"
    Transform = transform
}
