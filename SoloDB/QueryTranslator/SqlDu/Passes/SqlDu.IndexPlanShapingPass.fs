module internal SoloDatabase.IndexPlanShapingPass

open SoloDatabase.PassTypes
open SoloDatabase.IndexModel
open SoloDatabase.IndexPlanShaping

// ══════════════════════════════════════════════════════════════
// Index plan shaping pass registration.
//
// The pass factory takes an IndexModel and returns a standard
// Optimizer pass via closure capture. The framework sees a standard
// SqlStatement -> SqlStatement function.
// ══════════════════════════════════════════════════════════════

/// Create an IndexPlanShaping pass that closes over the given index model.
let indexPlanShaping (model: IndexModel) : Pass = {
    Name = "IndexPlanShaping"
    Transform = fun stmt -> shapeIndexStatement model stmt
}
