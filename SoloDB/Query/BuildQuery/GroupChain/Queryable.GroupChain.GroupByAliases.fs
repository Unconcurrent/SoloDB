namespace SoloDatabase

open System.Threading

/// Deterministic aliases for the correlated input and stages of a grouped chain.
module internal GroupByAliases =
    let nextSubquery (sourceCtx: QueryContext) =
        "gb_gsub" + (Interlocked.Increment(sourceCtx.AliasCounter)).ToString(System.Globalization.CultureInfo.InvariantCulture)
