module SoloDatabase.FlattenPass

open SoloDatabase.PassTypes
open SoloDatabase.FlattenTransform

/// Optimizer pass that applies subquery flattening.
/// Merges FROM-clause DerivedTables into outer queries when flatten-safe.
let subqueryFlatten : Pass = {
    Name = "SubqueryFlatten"
    Transform = flattenStatement
}
