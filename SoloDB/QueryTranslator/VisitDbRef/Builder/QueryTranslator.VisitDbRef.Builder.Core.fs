namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.DBRefManyDescriptor
open DBRefTypeHelpers
open Utils
open SoloDatabase.Attributes

module internal DBRefManyBuilderCore =
    let dbRefManyLinkTable (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationLink(ownerTable, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> raise (NotSupportedException(sprintf "Error: Relation metadata not found for property %s on collection %s.\nReason: The property does not have relation metadata.\nFix: Ensure the collection is initialized with Insert or GetCollection before querying, or call AsEnumerable() before accessing this relation." propName ownerTable))

    let dbRefManyOwnerUsesSource (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationOwnerUsesSource(ownerTable, propName) with
        | Some value -> value
        | None -> raise (NotSupportedException(sprintf "Error: Relation metadata not found for property %s on collection %s.\nReason: The property does not have relation metadata.\nFix: Ensure the collection is initialized with Insert or GetCollection before querying, or call AsEnumerable() before accessing this relation." propName ownerTable))

    let resolveTargetTable (ctx: QueryContext) (ownerCollection: string) (propName: string) (targetType: Type) =
        let defaultTable = formatName targetType.Name
        match ctx.TryResolveRelationTarget(ownerCollection, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> ctx.ResolveCollectionForType(UtilsReflection.typeIdentityKey targetType, defaultTable)

    let tryGetRelationOrderByForTakeWhile
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (tgtAlias: string)
        (lnkAlias: string)
        : OrderBy list voption =
        let relationOrder =
            match ownerRef.PropertyExpr.Member with
            | :? Reflection.PropertyInfo as prop ->
                match prop.GetCustomAttributes(typeof<SoloRefAttribute>, true) |> Seq.tryHead with
                | Some attrObj -> (attrObj :?> SoloRefAttribute).OrderBy
                | None -> DBRefOrder.Undefined
            | _ -> DBRefOrder.Undefined

        match relationOrder with
        | DBRefOrder.TargetId ->
            ValueSome [{ Expr = SqlExpr.Column(Some tgtAlias, "Id"); Direction = SortDirection.Asc }]
        | _ ->
            ValueNone

    /// Per-QueryContext alias generator. Emitted SQL alias numerals are
    /// deterministic per query instead of process-history-dependent.
    let nextAlias (ctx: QueryContext) (prefix: string) : string =
        sprintf "%s%d" prefix (System.Threading.Interlocked.Increment(ctx.AliasCounter))

    let mkSubCore projections source where =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = where; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    let ownerIdExpr (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) =
        match ownerRef.OwnerIdExpr with
        | Some expr -> expr
        | None -> SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id")

    let private relationSource (qb: QueryBuilder) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) =
        let propName = ownerRef.PropertyExpr.Member.Name
        let ctx = qb.SourceContext
        let linkTable = dbRefManyLinkTable ctx ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource ctx ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetTable ctx ownerRef.OwnerCollection propName targetType
        let tgtAlias = nextAlias ctx "_tgt"
        let lnkAlias = nextAlias ctx "_lnk"

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"),
                BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn),
                BinaryOperator.Eq,
                ownerIdExpr ownerRef)
        tgtAlias, lnkAlias, targetTable, linkTable, ownerWhere, joinOn, targetColumn

    /// Builds only the correlated relation source; operator order belongs to OrderedChain.
    let buildRelationCore (qb: QueryBuilder) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) (includePayload: bool) (projections: Projection list) =
        let target, link, table, linkTable, ownerWhere, joinOn, targetColumn = relationSource qb ownerRef
        let row =
            { mkSubCore projections (Some(BaseTable(linkTable, Some link))) (Some ownerWhere) with
                Joins = if includePayload then [ConditionedJoin(Inner, BaseTable(table, Some target), joinOn)] else [] }
        let id = SqlExpr.Column(Some(if includePayload then target else link), if includePayload then "Id" else targetColumn)
        let payload = if includePayload then SqlExpr.Column(Some target, "Value") else SqlExpr.Literal SqlLiteral.Null
        id, payload, row, table
