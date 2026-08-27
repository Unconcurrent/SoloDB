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

    let nextDistinct (ctx: QueryContext) = next ctx "gjd"
    let nextGroupBy (ctx: QueryContext) = next ctx "gjgb"
    let nextNested (ctx: QueryContext) = next ctx "gjn"
    let nextProjection (ctx: QueryContext) = next ctx "gjp"
    let nextProjectionBounded (ctx: QueryContext) = next ctx "gjpb"
    let nextRowset (ctx: QueryContext) = next ctx "gjr"
    let nextSetDistinct (ctx: QueryContext) = next ctx "gjsd"
    let nextSetFiltered (ctx: QueryContext) = next ctx "gjsf"
    let nextSetKeyed (ctx: QueryContext) = next ctx "gjsk"
    let nextSetMembership (ctx: QueryContext) = next ctx "gjsm"
    let nextSetRemaining (ctx: QueryContext) = next ctx "gjsr"
    let nextSetUnionLeft (ctx: QueryContext) = next ctx "gjsu"
    let nextSetValue (ctx: QueryContext) = next ctx "gjsv"
    let nextSetExtra (ctx: QueryContext) = next ctx "gjsx"
    let nextSetSecondary (ctx: QueryContext) = next ctx "gjsy"
    let nextTakeWhile (ctx: QueryContext) = next ctx "gjtw"
    let nextWhileFilter (ctx: QueryContext) = next ctx "gjwf"
    let nextCountSource (ctx: QueryContext) = next ctx "gjc"
    let nextCountRow (ctx: QueryContext) = next ctx "gjn"
    let nextUnionArm (ctx: QueryContext) = next ctx "gju"
    let nextExistsFilter (ctx: QueryContext) = next ctx "gjf"
    let nextCollectionRowset (ctx: QueryContext) = next ctx "gja"
    let nextAggregateRowset (ctx: QueryContext) = next ctx "gjg"
    let nextExistsRowset (ctx: QueryContext) = next ctx "gjx"
    let nextContainsRowset (ctx: QueryContext) = next ctx "gjh"
