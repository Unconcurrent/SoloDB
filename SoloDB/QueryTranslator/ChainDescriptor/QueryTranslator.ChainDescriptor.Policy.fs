namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec

/// The shape of a chain-extraction policy.
///
/// This module owns the shape and never an instance: GroupBy and GroupJoin admit different
/// operators and report different messages, and merging those decisions would widen or narrow
/// what each accepts. Each builder supplies its own value at its semantic entry point.
module internal ChainPolicy =
    type ExtractorConfig =
        {
            EnsureOfTypeSupported: Type -> unit
            MultipleTakeSkipBoundariesMessage: string
            TooManyTakeWhileBoundariesMessage: string
        }

