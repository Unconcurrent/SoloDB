module SoloDatabase.ProjectionPass

open SoloDatabase.PassTypes
open SoloDatabase.ProjectionPushdown

/// C4 framework pass that applies projection pushdown.
/// Narrows inner query projections to only columns referenced
/// by outer query components (projections, WHERE, ORDER BY,
/// GROUP BY, HAVING, JOIN ON).
let projectionPushdown : Pass = {
    Name = "ProjectionPushdown"
    Transform = pushdownProjectionStatement
}
