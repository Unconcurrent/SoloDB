namespace SoloDatabase

open System.Threading

/// Alias names for GroupJoin chain subqueries.
///
/// Named per purpose so a call site cannot silently reuse another label. The numeral appears
/// verbatim in emitted SQL and is zero-based here while the GroupBy builder is one-based; that
/// difference is a compatibility constraint on existing SQL, not an oversight.
module internal GroupJoinAliases =
    let private next (ctx: QueryContext) (prefix: string) =
        sprintf "%s%d" prefix (Interlocked.Increment(ctx.AliasCounter) - 1)

    let nextRowset (ctx: QueryContext) = next ctx "gjr"
    let nextCountRow (ctx: QueryContext) = next ctx "gjn"
