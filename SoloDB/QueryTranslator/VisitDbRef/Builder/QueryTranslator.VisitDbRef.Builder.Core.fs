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

    let tryGetRelationOrderByForTakeWhile
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (tgtAlias: string)
        (lnkAlias: string)
        : OrderBy list voption =
        let declaredOrders = ownerRef.Reference.Input.ReferenceOwner.Value.Sources |> Array.choose (fun source ->
            source.Owner |> ValueOption.toOption |> Option.map (fun owner ->
                RelationMetadata.Get(owner.OwnerType)
                |> Array.tryFind(fun memberInfo -> memberInfo.Name = source.PropertyName)
                |> Option.map(fun memberInfo -> if isNull memberInfo.Policy then DBRefOrder.Undefined else memberInfo.Policy.OrderBy)
                |> Option.defaultValue DBRefOrder.Undefined))
        let relationOrder =
            if declaredOrders.Length > 0 && declaredOrders |> Array.forall (fun order -> order = DBRefOrder.TargetId) then DBRefOrder.TargetId
            else DBRefOrder.Undefined

        match relationOrder with
        | DBRefOrder.TargetId ->
            ValueSome [{ Expr = SqlExpr.Column(Some tgtAlias, "Id"); Direction = SortDirection.Asc }]
        | _ ->
            ValueNone

    /// Per-QueryContext alias generator. Emitted SQL alias numerals are
    /// deterministic per query instead of process-history-dependent.
    let nextAlias (ctx: QueryContext) (prefix: string) : string =
        prefix + (System.Threading.Interlocked.Increment(ctx.AliasCounter)).ToString(System.Globalization.CultureInfo.InvariantCulture)

    let mkSubCore projections source where =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = where; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    let private relationSource (qb: QueryBuilder) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) (source: QueryReferenceSource) =
        let ownerSource = source.Owner.Value
        let propName = source.PropertyName
        let ctx = qb.SourceContext
        let filesystem = isFileRefManyType ownerRef.ReferenceType
        let targetType = referenceTargetType ownerRef.ReferenceType
        let linkTable = if filesystem then "SoloDBFileSystemReference" else dbRefManyLinkTable ctx ownerSource.TableName propName
        let ownerUsesSource = filesystem || dbRefManyOwnerUsesSource ctx ownerSource.TableName propName
        let ownerColumn = if filesystem then "OwnerId" elif ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if filesystem then (if typeof<SoloDatabase.Types.SoloDBFileHeader>.Equals(targetType) then "FileId" else "DirectoryId") elif ownerUsesSource then "TargetId" else "SourceId"
        let targetTable = ctx.ResolveQueryRelationTarget(ValueSome ownerSource.TableName, propName, targetType)
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
                (QueryTranslatorPhysicalSource.referenceOwner ownerRef.Reference.Input.ReferenceOwner.Value).Identity)
        let ownerWhere =
            if filesystem then
                let equals column value = SqlExpr.Binary(SqlExpr.Column(Some lnkAlias, column), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String value))
                SqlExpr.Binary(ownerWhere, BinaryOperator.And,
                    SqlExpr.Binary(equals "OwnerCollection" ownerSource.TableName, BinaryOperator.And,
                        SqlExpr.Binary(equals "Property" propName, BinaryOperator.And, SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some lnkAlias, targetColumn)))))
            else ownerWhere
        let ownerWhere = SqlExpr.Binary(ownerWhere, BinaryOperator.And,
                             QueryTranslatorPhysicalSource.selectsReferenceSource ownerRef.Reference.Input.ReferenceOwner.Value source)
        tgtAlias, lnkAlias, targetTable, linkTable, ownerWhere, joinOn, targetColumn

    let private targetRow (ownerRef: DBRefManyOwnerRef) includePayload (ownerSource: QueryPhysicalSource) target link table targetColumn =
        let id = SqlExpr.Column(Some(if includePayload then target else link), if includePayload then "Id" else targetColumn)
        let payload =
            if not includePayload then
                if isFileRefManyType ownerRef.ReferenceType then jsonToken id
                else SqlExpr.Literal SqlLiteral.Null
            elif isFileRefManyType ownerRef.ReferenceType then
                SqlExpr.JsonArrayExpr [SqlExpr.Column(Some target, "Id"); fileSystemHeaderValueExpr (referenceTargetType ownerRef.ReferenceType) target]
            else SqlExpr.Column(Some target, "Value")
        let input = QuerySerializationInput.ForOwner(referenceTargetType ownerRef.ReferenceType, table, id, ownerSource.Loading)
        let input = { input with Source = ValueSome(SqlExpr.FunctionCall("jsonb", [payload])); Navigation = ownerRef.Reference.Input.Navigation }
        struct(id, payload, input)

    /// Known physical sources meet here before the ordered chain consumes one rowset.
    let buildRelationSource (qb: QueryBuilder) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) includePayload =
        let descriptorName = "_input" + qb.SourceContext.NextAlias()
        let sources = ownerRef.Reference.Input.ReferenceOwner.Value.Sources |> Array.filter (fun source -> source.Owner.IsSome)
        let branches = sources |> Array.map (fun source ->
            let ownerSource = source.Owner.Value
            let target, link, table, linkTable, ownerWhere, joinOn, targetColumn = relationSource qb ownerRef source
            let struct(id, payload, input) = targetRow ownerRef includePayload ownerSource target link table targetColumn
            let row =
                { mkSubCore [{ Alias = Some "Id"; Expr = id }; { Alias = Some "Value"; Expr = payload }
                             { Alias = Some "__ord"; Expr = id }; { Alias = Some descriptorName; Expr = QuerySerializationSql.inputDescriptor input }]
                            (Some(BaseTable(linkTable, Some link))) (Some ownerWhere) with
                    Joins = if includePayload then [ConditionedJoin(Inner, BaseTable(table, Some target), joinOn)] else [] }
            row, input)
        let rows = branches |> Array.map fst
        let query = { Ctes = []; Body = if rows.Length = 1 then SingleSelect rows[0] else UnionAllSelect(rows[0], rows[1..] |> Array.toList) }
        let input = QueryTranslatorSerialization.mergeInputs (referenceTargetType ownerRef.ReferenceType) (Array.map snd branches)
        struct(query, QueryTranslatorSerialization.fromDescriptor (SqlExpr.Column(None, descriptorName)) input)

    /// One completed owner rowset feeds guarded indexed edges for its known sources.
    let buildFlattenedRelationSource (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) source =
        let referenceOwner = ownerRef.Reference.Input.ReferenceOwner.Value
        let owner = QueryTranslatorPhysicalSource.referenceOwner referenceOwner
        let sources = referenceOwner.Sources |> Array.filter (fun source -> source.Owner.IsSome)
        let elementType = ModelMetadata.SequenceElement ownerRef.ReferenceType
        let kind = if sources.Length = 1 then Inner else Left
        let branches = sources |> Array.map (fun source ->
            let ownerSource = source.Owner.Value
            let target, link, table, linkTable, ownerWhere, joinOn, targetColumn = relationSource qb ownerRef source
            let struct(id, payload, input) = targetRow ownerRef true ownerSource target link table targetColumn
            let value = jsonTokenValue elementType payload
            let input = { input with Carrier = if isSqlScalarResult elementType then SqlScalar else CanonicalToken }
            QueryTranslatorPhysicalSource.selectsReferenceSource referenceOwner source, id, DirectValue(value, input),
            [ConditionedJoin(kind, BaseTable(linkTable, Some link), ownerWhere)
             ConditionedJoin(kind, BaseTable(table, Some target), joinOn)])
        let selected =
            if branches.Length = 1 then let _, _, value, _ = branches[0] in value
            else Array.foldBack (fun (condition, _, value, _) otherwise -> QueryProduced.choose elementType condition value otherwise)
                        branches (QueryProduced.typed elementType (SqlExpr.Literal SqlLiteral.Null))
        let identity =
            if branches.Length = 1 then let _, identity, _, _ = branches[0] in identity
            else Array.foldBack (fun (condition, identity, _, _) otherwise -> SqlExpr.CaseExpr((condition, identity), [], Some otherwise))
                        branches (SqlExpr.Literal SqlLiteral.Null)
        let row =
            { mkSubCore [{ Alias = Some "Id"; Expr = owner.Identity }; { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }]
                        (Some source) (Some(SqlExpr.Unary(UnaryOperator.IsNotNull, identity))) with
                Joins = branches |> Array.collect (fun (_, _, _, joins) -> Array.ofList joins) |> Array.toList }
        struct(row, selected)
