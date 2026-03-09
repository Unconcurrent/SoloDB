module SoloDatabase.IndexPlanShapingPass

open SoloDatabase.PassTypes
open SoloDatabase.IndexModel
open SoloDatabase.IndexPlanShaping

// ══════════════════════════════════════════════════════════════
// Index Plan Shaping Pass Registration (C8 closure capture)
//
// The pass factory takes an IndexModel and returns a standard
// C4 Pass via closure capture. The C4 framework sees a standard
// SqlStatement -> SqlStatement function. Zero C4 changes.
// ══════════════════════════════════════════════════════════════

/// Create an IndexPlanShaping pass that closes over the given index model.
let indexPlanShaping (model: IndexModel) : Pass = {
    Name = "IndexPlanShaping"
    Transform = fun stmt -> shapeIndexStatement model stmt
}
