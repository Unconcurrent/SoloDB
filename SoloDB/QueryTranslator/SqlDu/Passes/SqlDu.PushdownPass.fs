module internal SoloDatabase.PushdownPass

open SoloDatabase.PassTypes
open SoloDatabase.PushdownTransform

/// Optimizer pass that applies predicate pushdown.
/// Moves outer WHERE conjuncts through DerivedTable boundaries
/// when all pushdown-safety conditions (P-S1 through P-S9) are met.
let predicatePushdown : Pass = {
    Name = "PredicatePushdown"
    Transform = fun stmt -> pushdownStatement stmt
}
