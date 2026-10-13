namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Reflection
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.SqlModel
open DBRefTypeHelpers

module internal QueryTranslatorVisitPostJoin =
    /// The actual produced reference retains its original logical admission scopes.
    let internal validateReferenceNavigation (reference: QueryProducedValue) =
        for scope in reference.Input.Navigation do
            if scope.Depth > maxRelationDepth then
                raise (NotSupportedException(
                    sprintf "Error: Relation chain is too deep or circular (depth > %d).\nReason: The query exceeds the supported relation traversal depth.\nFix: Reduce relation depth or split the query into multiple steps." maxRelationDepth))
            let path = String.Join(".", scope.Path)
            if scope.Loading.Excluded |> Array.contains path then
                raise (InvalidOperationException(
                    sprintf "Error: Cannot access excluded relation property '%s.Value'.\nReason: The property was excluded from the query.\nFix: Remove the Exclude call or use .Id instead." path))
        reference.Input.Navigation |> Array.tryHead |> Option.map (fun scope -> String.Join(".", scope.Path)) |> Option.defaultValue ""

    let internal referenceSnapshotIncluded (reference: QueryProducedValue) =
        reference.Input.Navigation |> Array.forall (fun scope ->
            let path = String.Join(".", scope.Path)
            not(Array.contains path scope.Loading.Excluded) &&
            (not scope.Loading.Whitelist || Array.contains path scope.Loading.Included))

    /// Indexed placement consumes the already prepared owner/property occurrence.
    let internal ensurePreparedDBRefJoin (qb: QueryBuilder) (referenceExpr: Expression) pathKey
        (prepared: struct(QueryProducedValue * JsonSerializator.JsonEqualitySupport.PathSegment list * bool)) : JoinEdge =
        qb.StepTranslation()
        let targetType = referenceTargetType referenceExpr.Type
        let struct(reference, referencePath, onOwnerRoot) = prepared
        let relation = reference.Input.ReferenceOwner.Value
        let source = relation.Sources[0]
        let owner = QueryTranslatorPhysicalSource.referenceOwner relation
        let sourceAlias =
            match owner.Identity with
            | SqlExpr.Column(alias, _) -> alias
            | _ -> None
        let context = qb.SourceContext
        let targetTable = context.ResolveQueryRelationTarget(source.Owner |> ValueOption.map _.TableName, source.PropertyName, targetType)
        let referenceValue =
            reference |> QueryProduced.consume qb.SourceContext.NextAlias (fun value input ->
                input.Source |> ValueOption.defaultWith (fun () -> jsonToken value))
        match QueryTranslatorReferenceJoin.tryFindEdge qb.SourceContext pathKey sourceAlias targetTable referenceValue with
        | Some edge -> edge
        | None ->
            let alias = qb.SourceContext.NextAlias()
            let targetValue =
                if isFileRefType referenceExpr.Type then fileSystemHeaderValueExpr targetType alias
                else SqlExpr.Column(Some alias, "Value")
            let edge = {
                TargetAlias = alias
                TargetTable = targetTable
                TargetValue = targetValue
                ReferenceValue = referenceValue
                ReferencePath = referencePath
                ReferenceOnOwnerRoot = onOwnerRoot && sourceAlias.IsSome
                JoinKind = "LEFT JOIN"
                OnSourceAlias = sourceAlias
                OnPropertyName = source.PropertyName
                PropertyPath = pathKey
            }
            qb.SourceContext.Joins.Add(edge)
            edge
