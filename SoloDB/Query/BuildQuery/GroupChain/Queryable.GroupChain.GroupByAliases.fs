namespace SoloDatabase

open System.Threading


/// GroupBy chained-expression support — correlated subqueries for
/// group-item chains that go beyond simple aggregate whitelist.
/// Uses shared Terminal DU + walkChain extraction with per-context GroupBy building.

/// Alias names for GroupBy chain subqueries.
///
/// Each operation is named for what it labels. The numeral this module produces appears verbatim
/// in emitted SQL, so its base is a compatibility constraint, not an implementation detail: it is
/// deliberately one-based here while the GroupJoin builder is zero-based. Do not unify them
/// without regenerating every SQL fixture.
module internal GroupByAliases =
    let next (sourceCtx: QueryContext) (prefix: string) =
        sprintf "gb%s%d" prefix (Interlocked.Increment(sourceCtx.AliasCounter))

    let nextSubquery ctx = next ctx "_gsub"
    let nextAggregate ctx = next ctx "_gagg"
    let nextCountDistinct ctx = next ctx "_gcd"
    let nextElementWrap ctx = next ctx "_gew"
    let nextElementSubquery ctx = next ctx "_gex"
    let nextCountWrap ctx = next ctx "_gcw"
    let nextConcat ctx = next ctx "_gcon"
    let nextRightConstant ctx = next ctx "_grc"
    let nextRightExists ctx = next ctx "_gre"
    let nextRightNested ctx = next ctx "_grn"
    let nextRightProjection ctx = next ctx "_grp"
    let nextRightSet ctx = next ctx "_grs"
    let nextSelection ctx = next ctx "_gsel"
    let nextSetConcat ctx = next ctx "gsc"
    let nextSetDistinctRank ctx = next ctx "gsd"
    let nextSetExists ctx = next ctx "gse"
    let nextSetFiltered ctx = next ctx "gsf"
    let nextSetKeyed ctx = next ctx "gsk"
    let nextSetMembership ctx = next ctx "gsm"
    let nextSetBounded ctx = next ctx "gsn"
    let nextSetRemaining ctx = next ctx "gsr"
    let nextSetUnionLeft ctx = next ctx "gsu"
    let nextSetValue ctx = next ctx "gsv"
    let nextSetWrap ctx = next ctx "gsw"
    let nextSetExtra ctx = next ctx "gsx"
    let nextSetSecondary ctx = next ctx "gsy"
    let nextSetFinal ctx = next ctx "gsz"
