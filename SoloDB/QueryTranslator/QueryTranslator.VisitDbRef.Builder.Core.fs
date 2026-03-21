namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
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
        | _ -> raise (InvalidOperationException(sprintf "Error: relation metadata missing for '%s.%s'." ownerTable propName))

    let dbRefManyOwnerUsesSource (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationOwnerUsesSource(ownerTable, propName) with
        | Some value -> value
        | None -> raise (InvalidOperationException(sprintf "Error: relation metadata missing for '%s.%s'." ownerTable propName))

    let resolveTargetTable (ctx: QueryContext) (ownerCollection: string) (propName: string) (targetType: Type) =
        let defaultTable = formatName targetType.Name
        match ctx.TryResolveRelationTarget(ownerCollection, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> ctx.ResolveCollectionForType(typeIdentityKey targetType, defaultTable)

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

    let mutable private aliasCounter = 0L
    let nextAlias prefix =
        let id = System.Threading.Interlocked.Increment(&aliasCounter)
        sprintf "%s%d" prefix id

    let mkSubCore projections source where =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = where; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    let private translatePredicates
        (qb: QueryBuilder)
        (targetAlias: string)
        (targetTable: string)
        (joinEdges: ResizeArray<JoinEdge>)
        (predicateExprs: Expression list)
        (invalidLambdaMessage: string)
        : SqlExpr list =
        predicateExprs
        |> List.collect (fun predExpr ->
            match tryExtractLambdaExpression predExpr with
            | ValueSome predLambda ->
                let subQb = qb.ForSubquery(targetAlias, predLambda, subqueryRootTable = targetTable)
                let result = [visitDu predLambda.Body subQb]
                joinEdges.AddRange(subQb.SourceContext.Joins)
                result
            | ValueNone ->
                raise (NotSupportedException(invalidLambdaMessage)))

    let private translateSortKeys
        (qb: QueryBuilder)
        (targetAlias: string)
        (targetTable: string)
        (joinEdges: ResizeArray<JoinEdge>)
        (sortKeys: (Expression * SortDirection) list)
        (invalidKeyMessage: string)
        : OrderBy list =
        sortKeys
        |> List.map (fun (keyExpr, dir) ->
            match tryExtractLambdaExpression keyExpr with
            | ValueSome keyLambda ->
                let subQb = qb.ForSubquery(targetAlias, keyLambda, subqueryRootTable = targetTable)
                let result = { Expr = visitDu keyLambda.Body subQb; Direction = dir }
                joinEdges.AddRange(subQb.SourceContext.Joins)
                result
            | ValueNone ->
                raise (NotSupportedException(invalidKeyMessage)))

    let private buildLimitOffset (visitDu: Expression -> QueryBuilder -> SqlExpr) (qb: QueryBuilder) (limitExpr: Expression option) (offsetExpr: Expression option) =
        let limitDu =
            match limitExpr with
            | Some e ->
                match e with
                | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
                | _ -> Some(visitDu e qb)
            | None ->
                match offsetExpr with
                | Some _ -> Some(SqlExpr.Literal(SqlLiteral.Integer -1L))
                | None -> None
        let offsetDu =
            match offsetExpr with
            | Some e ->
                match e with
                | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
                | _ -> Some(visitDu e qb)
            | None -> None
        limitDu, offsetDu

    /// Build the correlated subquery core from a descriptor.
    /// Returns (tgtAlias, lnkAlias, core, targetTable) with JOIN + WHERE + ORDER BY + LIMIT/OFFSET.
    let buildCorrelatedCore
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (projections: Projection list)
        : string * string * SelectCore * string =

        let propName = ownerRef.PropertyExpr.Member.Name
        let ctx = qb.SourceContext
        let linkTable = dbRefManyLinkTable ctx ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource ctx ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetTable ctx ownerRef.OwnerCollection propName targetType
        if desc.OfTypeName.IsSome || desc.CastTypeName.IsSome then
            DBRefManyHelpers.ensureOfTypeSupported targetType
        let tgtAlias = nextAlias "_tgt"
        let lnkAlias = nextAlias "_lnk"

        let innerJoinEdges = ResizeArray<JoinEdge>()
        let wherePredDus =
            translatePredicates
                qb
                tgtAlias
                targetTable
                innerJoinEdges
                desc.WherePredicates
                "Error: Cannot translate relation-backed DBRefMany.Any predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."

        let ofTypePred =
            match desc.OfTypeName with
            | Some tn ->
                [SqlExpr.Binary(
                    SqlExpr.FunctionCall("jsonb_extract", [
                        SqlExpr.Column(Some tgtAlias, "Value")
                        SqlExpr.Literal(SqlLiteral.String "$.$type")]),
                    BinaryOperator.Eq,
                    SqlExpr.Literal(SqlLiteral.String tn))]
            | None -> []

        let allPreds = wherePredDus @ ofTypePred
        let sortKeyDus =
            translateSortKeys qb tgtAlias targetTable innerJoinEdges desc.SortKeys "Cannot extract key selector for OrderBy."
        let limitDu, offsetDu = buildLimitOffset visitDu qb desc.Limit desc.Offset

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"),
                BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn),
                BinaryOperator.Eq,
                SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
        let fullWhere = DBRefManyHelpers.appendPredicatesWithAnd ownerWhere allPreds

        let dbRefJoins = DBRefManyHelpers.joinEdgesToClauses innerJoinEdges
        let innerCore =
            { mkSubCore projections (Some(BaseTable(linkTable, Some lnkAlias))) (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] @ dbRefJoins
                OrderBy = sortKeyDus
                Limit = limitDu
                Offset = offsetDu }

        let hasPostBound =
            desc.PostBoundWherePredicates.Length > 0 ||
            desc.PostBoundSortKeys.Length > 0 ||
            desc.PostBoundLimit.IsSome ||
            desc.PostBoundOffset.IsSome

        let effectiveAlias, effectiveLnk, effectiveCore, effectiveTarget =
            if hasPostBound then
                let passThrough = { innerCore with Projections = AllColumns }
                let innerSel = { Ctes = []; Body = SingleSelect passThrough }
                let pbAlias = nextAlias "_pb"
                let pbJoinEdges = ResizeArray<JoinEdge>()
                let pbWhereDus =
                    translatePredicates
                        qb
                        pbAlias
                        targetTable
                        pbJoinEdges
                        desc.PostBoundWherePredicates
                        "Error: Cannot translate relation-backed DBRefMany predicate.\nReason: The predicate is not a translatable lambda expression.\nFix: Pass the predicate as an inline lambda, not a delegate variable."
                let pbSortKeyDus =
                    translateSortKeys qb pbAlias targetTable pbJoinEdges desc.PostBoundSortKeys "Cannot extract key selector for PostBound OrderBy."
                let pbLimitDu, pbOffsetDu = buildLimitOffset visitDu qb desc.PostBoundLimit desc.PostBoundOffset
                let pbDbRefJoins = DBRefManyHelpers.joinEdgesToClauses pbJoinEdges
                let outerCore =
                    { mkSubCore projections (Some(DerivedTable(innerSel, pbAlias))) (DBRefManyHelpers.foldPredicatesWithAnd pbWhereDus) with
                        Joins = pbDbRefJoins
                        OrderBy = pbSortKeyDus
                        Limit = pbLimitDu
                        Offset = pbOffsetDu }
                pbAlias, lnkAlias, outerCore, targetTable
            else
                tgtAlias, lnkAlias, innerCore, targetTable

        match desc.DefaultIfEmpty with
        | Some defaultValueExprOpt ->
            let defaultValueDu =
                match defaultValueExprOpt with
                | Some valueExpr ->
                    let v = evaluateExpr<obj> valueExpr
                    let jsonObj = JsonSerializator.JsonValue.Serialize v
                    let jsonText = jsonObj.ToJsonString()
                    SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
                | None ->
                    SqlExpr.Literal(SqlLiteral.Null)
            let existsCore = { effectiveCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
            let existsSel = { Ctes = []; Body = SingleSelect existsCore }
            let flatAlias = nextAlias "_dif"
            let flatProjs =
                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some flatAlias, "Id") }
                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some flatAlias, "Value") }]
            let flatSel = { Ctes = []; Body = SingleSelect { effectiveCore with Projections = AllColumns } }
            let mainCore = mkSubCore flatProjs (Some(DerivedTable(flatSel, flatAlias))) None
            let defaultProjs =
                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                 { Alias = Some "Value"; Expr = defaultValueDu }]
            let defaultCore =
                { mkSubCore defaultProjs None (Some(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists existsSel))) with
                    Joins = []; OrderBy = []; Limit = None; Offset = None }
            let unionSel = { Ctes = []; Body = UnionAllSelect(mainCore, [defaultCore]) }
            let dtAlias = nextAlias "_die"
            let wrappedCore = mkSubCore projections (Some(DerivedTable(unionSel, dtAlias))) None
            dtAlias, effectiveLnk, wrappedCore, effectiveTarget
        | None -> effectiveAlias, effectiveLnk, effectiveCore, effectiveTarget

    let private serializeDefaultValueExpr (jsonFn: string) (value: obj) =
        let jsonObj = JsonSerializator.JsonValue.Serialize value
        let jsonText = jsonObj.ToJsonString()
        SqlExpr.FunctionCall(jsonFn, [SqlExpr.Literal(SqlLiteral.String jsonText)])

    let computeDefaultValueDu (defaultValueExprOpt: Expression option) (projectedType: Type option) : SqlExpr =
        match defaultValueExprOpt with
        | Some valueExpr ->
            let v = evaluateExpr<obj> valueExpr
            SqlExpr.FunctionCall("jsonb_extract", [serializeDefaultValueExpr "jsonb" v; SqlExpr.Literal(SqlLiteral.String "$")])
        | None ->
            match projectedType with
            | Some t when t.IsValueType ->
                let defaultValue = Activator.CreateInstance(t)
                SqlExpr.FunctionCall("jsonb_extract", [serializeDefaultValueExpr "jsonb" defaultValue; SqlExpr.Literal(SqlLiteral.String "$")])
            | _ ->
                SqlExpr.Literal(SqlLiteral.Null)

    let computeDefaultValueForJsonArray (defaultValueExprOpt: Expression option) (projectedType: Type option) : SqlExpr =
        match defaultValueExprOpt with
        | Some valueExpr ->
            let v = evaluateExpr<obj> valueExpr
            serializeDefaultValueExpr "json" v
        | None ->
            match projectedType with
            | Some t when t.IsValueType ->
                let defaultValue = Activator.CreateInstance(t)
                serializeDefaultValueExpr "json" defaultValue
            | _ ->
                SqlExpr.Literal(SqlLiteral.Null)
