module SoloDatabase.SelectCoreBoundary

open SqlDu.Engine.C1.Spec
open SoloDatabase.ExpressionPredicates

let hasMustNotBoundary (core: SelectCore) : bool =
    not core.GroupBy.IsEmpty
    || core.Having.IsSome
    || core.Distinct
    || (match core.Source with Some(FromJsonEach _) -> true | _ -> false)

let hasAggregateOrWindowProjections (core: SelectCore) : bool =
    core.Projections
    |> ProjectionSetOps.toList
    |> List.exists (fun p -> hasAggregateCall p.Expr || hasWindowFunction p.Expr)
