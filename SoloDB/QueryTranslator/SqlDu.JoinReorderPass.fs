module SoloDatabase.JoinReorderPass

open SoloDatabase.PassTypes
open SoloDatabase.JoinReorder

/// C4 framework pass that applies join reordering.
/// Reorders legal INNER JOIN chains into deterministic canonical order.
/// Only reorders when 2+ INNER JOINs exist and scope safety is maintained.
let joinReorder : Pass = {
    Name = "JoinReorder"
    Transform = reorderJoinStatement
}
