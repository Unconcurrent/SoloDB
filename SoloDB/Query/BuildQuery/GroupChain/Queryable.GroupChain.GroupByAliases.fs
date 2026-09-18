namespace SoloDatabase

open System.Threading

/// Deterministic aliases for the correlated input and stages of a grouped chain.
module internal GroupByAliases =
    let nextSubquery (sourceCtx: QueryContext) =
        sprintf "gb_gsub%d" (Interlocked.Increment(sourceCtx.AliasCounter))
