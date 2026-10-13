namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost

/// Shared helpers for DBRefMany query translation.
module internal DBRefManyHelpers =
    [<Literal>]
    let takeWhileOrderingRequiredMessage =
        "Error: TakeWhile/SkipWhile requires explicit ordering.\nReason: TakeWhile/SkipWhile needs a deterministic row order.\nFix: Add .OrderBy() before .TakeWhile(), or set OrderBy on the [SoloRef] attribute."

    let wrapAggregateEmptySemantics (resultType: Type) (aggKind: AggregateKind) (scalarExpr: SqlExpr) : SqlExpr =
        // DBRefMany aggregate sites are always nested. Per the SoloDB 1.2.2 contract,
        // nested cardinality DOES NOT throw — empty/multiple silently emits default(T)
        // via NULL propagation to the materializer (Unchecked.defaultof<T>).
        // Sum branch keeps Coalesce(scalar, 0) so Sum-over-empty returns 0 (matches
        // .NET LINQ Sum-on-empty contract: returns 0, no throw).
        if aggKind = AggregateKind.Sum then
            SqlExpr.Coalesce(scalarExpr, [QueryTranslatorNumeric.zero resultType])
        else
            scalarExpr

    let ensureOfTypeSupported (targetType: Type) =
        if not (TypeMetadata.requiresTypeInformation targetType) then
            raise (NotSupportedException(
                "Error: OfType requires type discriminator information.\nReason: The DBRefMany target type does not store $type metadata.\nFix: Add the Polymorphic attribute to the base type and reinsert all elements."))
