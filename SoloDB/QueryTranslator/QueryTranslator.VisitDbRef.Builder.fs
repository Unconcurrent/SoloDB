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

/// Builds SQL DU trees from a unified DBRefManyQueryDescriptor.
/// Replaces the order-dependent per-handler peeling in VisitDbRef.
module internal DBRefManyBuilder =


    [<Literal>]
    let private nestedDbRefManyNotSupportedMessage =
        "Error: Deeply nested DBRefMany query is not supported.\nReason: Only one level of DBRefMany nesting is allowed in relation predicates.\nFix: Rewrite to at most one nested DBRefMany level, or move deeper traversal after AsEnumerable()."


    type DBRefManyOwnerRef = DBRefManyDescriptor.DBRefManyOwnerRef

    let private dbRefManyLinkTable (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationLink(ownerTable, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> raise (InvalidOperationException(sprintf "Error: relation metadata missing for '%s.%s'." ownerTable propName))

    let private dbRefManyOwnerUsesSource (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationOwnerUsesSource(ownerTable, propName) with
        | Some value -> value
        | None -> raise (InvalidOperationException(sprintf "Error: relation metadata missing for '%s.%s'." ownerTable propName))

    let private resolveTargetTable (ctx: QueryContext) (ownerCollection: string) (propName: string) (targetType: Type) =
        let defaultTable = formatName targetType.Name
        match ctx.TryResolveRelationTarget(ownerCollection, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> ctx.ResolveCollectionForType(typeIdentityKey targetType, defaultTable)


    let mutable private aliasCounter = 0L
    let internal nextAlias prefix =
        let id = System.Threading.Interlocked.Increment(&aliasCounter)
        sprintf "%s%d" prefix id


    let internal mkSubCore projections source where =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = where; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    /// Build the correlated subquery core from a descriptor.
    /// Returns (tgtAlias, lnkAlias, core with JOIN + WHERE + ORDER BY + LIMIT/OFFSET).
    let internal buildCorrelatedCore
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyOwnerRef)
        (projections: Projection list)
        : string * string * SelectCore =

        let propName = ownerRef.PropertyExpr.Member.Name
        let ctx = qb.SourceContext
        let linkTable = dbRefManyLinkTable ctx ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource ctx ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetTable ctx ownerRef.OwnerCollection propName targetType
        let tgtAlias = nextAlias "_tgt"
        let lnkAlias = nextAlias "_lnk"

        let wherePredDus =
            desc.WherePredicates
            |> List.collect (fun predExpr ->
                match tryExtractLambdaExpression predExpr with
                | ValueSome predLambda ->
                    let subQb = qb.ForSubquery(tgtAlias, predLambda)
                    [visitDu predLambda.Body subQb]
                | ValueNone -> [])

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
            desc.SortKeys
            |> List.map (fun (keyExpr, dir) ->
                match tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda ->
                    let subQb = qb.ForSubquery(tgtAlias, keyLambda)
                    { Expr = visitDu keyLambda.Body subQb; Direction = dir }
                | ValueNone ->
                    raise (NotSupportedException("Cannot extract key selector for OrderBy.")))

        let limitDu =
            match desc.Limit with
            | Some e ->
                match e with
                | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
                | _ -> Some(visitDu e qb)
            | None ->
                match desc.Offset with
                | Some _ -> Some(SqlExpr.Literal(SqlLiteral.Integer -1L)) // SQLite requires LIMIT with OFFSET
                | None -> None
        let offsetDu =
            match desc.Offset with
            | Some e ->
                match e with
                | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
                | _ -> Some(visitDu e qb)
            | None -> None

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
        let fullWhere =
            allPreds |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

        let core =
            { mkSubCore projections (Some(BaseTable(linkTable, Some lnkAlias))) (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                OrderBy = sortKeyDus
                Limit = limitDu
                Offset = offsetDu }

        tgtAlias, lnkAlias, core


    let private countDbRefManyDepth (expr: Expression) : int =
        let rec visitExpr (e: Expression) : int =
            let e = unwrapConvert e
            match e with
            | null -> 0
            | :? MemberExpression as me ->
                let inner = unwrapConvert me.Expression
                let depth = if not (isNull inner) && isDBRefManyType inner.Type then 1 else 0
                depth + visitExpr me.Expression
            | :? MethodCallExpression as mc ->
                let objectDepth = if isNull mc.Object then 0 else visitExpr mc.Object
                let argsDepth = mc.Arguments |> Seq.map visitExpr |> Seq.fold max 0
                max objectDepth argsDepth
            | :? BinaryExpression as be -> max (visitExpr be.Left) (visitExpr be.Right)
            | :? UnaryExpression as ue -> visitExpr ue.Operand
            | :? LambdaExpression as le -> visitExpr le.Body
            | :? NewArrayExpression as nae -> nae.Expressions |> Seq.map visitExpr |> Seq.fold max 0
            | _ -> 0
        visitExpr expr


    /// Build EXISTS subquery (for Any terminal).
    let private buildExists (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlExpr =
        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let _, _, core = buildCorrelatedCore qb desc ownerRef oneProj
        SqlExpr.Exists { Ctes = []; Body = SingleSelect core }

    /// Build NOT EXISTS subquery (for All terminal).
    let private buildNotExists (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) (allPredExpr: Expression) : SqlExpr =
        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let tgtAlias, _, core = buildCorrelatedCore qb desc ownerRef oneProj
        match tryExtractLambdaExpression allPredExpr with
        | ValueSome allPredLambda ->
            let subQb = qb.ForSubquery(tgtAlias, allPredLambda)
            let allPredDu = visitDu allPredLambda.Body subQb
            let negatedPred = SqlExpr.Unary(UnaryOperator.Not, allPredDu)
            let extendedWhere =
                match core.Where with
                | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, negatedPred))
                | None -> Some negatedPred
            let coreWithNot = { core with Where = extendedWhere }

            if desc.Limit.IsSome || desc.Offset.IsSome then
                let boundedCore = { core with Where = core.Where; Projections = AllColumns }
                let boundedSel = { Ctes = []; Body = SingleSelect boundedCore }
                let bndAlias = nextAlias "_bnd"
                let bndQb = qb.ForSubquery(bndAlias, allPredLambda)
                let bndPredDu = visitDu allPredLambda.Body bndQb
                let outerWhere = SqlExpr.Unary(UnaryOperator.Not, bndPredDu)
                let outerCore = mkSubCore oneProj (Some(DerivedTable(boundedSel, bndAlias))) (Some outerWhere)
                let outerSel = { Ctes = []; Body = SingleSelect outerCore }
                SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists outerSel)
            else
                SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists { Ctes = []; Body = SingleSelect coreWithNot })
        | ValueNone ->
            raise (NotSupportedException("Cannot extract predicate for All."))

    /// Build scalar COUNT subquery.
    let private buildCount (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlExpr =
        let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
        let _, _, core = buildCorrelatedCore qb desc ownerRef countProj

        if desc.Limit.IsSome || desc.Offset.IsSome then
            let innerCore = { core with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
            let innerSel = { Ctes = []; Body = SingleSelect innerCore }
            let dtAlias = nextAlias "_pg"
            let outerCore = mkSubCore countProj (Some(DerivedTable(innerSel, dtAlias))) None
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
        else
            let countCore = { core with Limit = None; Offset = None }
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }

    /// Build scalar aggregate subquery (Sum/Min/Max/Average).
    let private buildAggregate (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) (selectorExpr: Expression) (aggKind: AggregateKind) : SqlExpr =
        match tryExtractLambdaExpression selectorExpr with
        | ValueSome selectorLambda ->
            let tgtAlias, _, baseCore = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, selectorLambda)
            let selectorDu = visitDu selectorLambda.Body subQb
            let aggExpr = SqlExpr.AggregateCall(aggKind, Some selectorDu, false, None)

            if desc.Limit.IsSome || desc.Offset.IsSome then
                let innerCore = { baseCore with Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = selectorDu }] }
                let innerSel = { Ctes = []; Body = SingleSelect innerCore }
                let pgAlias = nextAlias "_pg"
                let outerAgg = SqlExpr.AggregateCall(aggKind, Some(SqlExpr.Column(Some pgAlias, "v")), false, None)
                let outerCore = mkSubCore [{ Alias = None; Expr = outerAgg }] (Some(DerivedTable(innerSel, pgAlias))) None
                let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
                if aggKind = AggregateKind.Sum then SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                else scalarExpr
            else
                let aggProj = [{ Alias = None; Expr = aggExpr }]
                let core = { baseCore with Projections = ProjectionSetOps.ofList aggProj }
                let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                if aggKind = AggregateKind.Sum then SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                else scalarExpr
        | ValueNone ->
            raise (NotSupportedException("Cannot extract selector for aggregate."))

    /// Build Select projection (json_group_array).
    let private buildSelect (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlExpr =
        match desc.SelectProjection with
        | Some projLambda ->
            if countDbRefManyDepth projLambda.Body > 0 then
                raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))

            let tgtAlias, _, baseCore = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, projLambda)
            let projectedDu = visitDu projLambda.Body subQb

            let hasTakeSkipOrOrder = desc.SortKeys.Length > 0 || desc.Limit.IsSome || desc.Offset.IsSome
            let hasTakeWhile = desc.TakeWhileInfo.IsSome

            if desc.Distinct && not hasTakeSkipOrOrder && not hasTakeWhile then
                let distinctAgg = SqlExpr.AggregateCall(AggregateKind.JsonGroupArray, Some projectedDu, true, None)
                let core = { baseCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = distinctAgg }]; Limit = None; Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
            elif not hasTakeSkipOrOrder && not hasTakeWhile then
                let groupArray = SqlExpr.FunctionCall("json_group_array", [projectedDu])
                let core = { baseCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = groupArray }]; Limit = None; Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
            else
                // Multi-layer DerivedTable (ordering/pagination/TakeWhile).
                let innerProjs =
                    match desc.TakeWhileInfo with
                    | Some (twPredLambda, _) ->
                        let twSubQb = qb.ForSubquery(tgtAlias, twPredLambda)
                        let twPredDu = visitDu twPredLambda.Body twSubQb
                        let caseExpr = SqlExpr.CaseExpr(
                            (SqlExpr.Unary(UnaryOperator.Not, twPredDu), SqlExpr.Literal(SqlLiteral.Integer 1L)),
                            [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
                        let sortKeyDuPairs = baseCore.OrderBy |> List.map (fun ob -> (ob.Expr, ob.Direction))
                        let windowSpec = { Kind = NamedWindowFunction "SUM"; Arguments = [caseExpr]; PartitionBy = []; OrderBy = sortKeyDuPairs }
                        [{ Alias = Some "v"; Expr = projectedDu }; { Alias = Some "_cf"; Expr = SqlExpr.WindowCall windowSpec }]
                    | None ->
                        [{ Alias = Some "v"; Expr = projectedDu }]

                let innerCore = { baseCore with Projections = ProjectionSetOps.ofList innerProjs }
                let innerSel = { Ctes = []; Body = SingleSelect innerCore }
                let ordAlias = nextAlias "_ord"

                match desc.TakeWhileInfo with
                | Some (_, isTakeWhile) ->
                    let cfFilter =
                        if isTakeWhile then SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                        else SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
                    let middleCore = mkSubCore [{ Alias = Some "v"; Expr = SqlExpr.Column(Some ordAlias, "v") }] (Some(DerivedTable(innerSel, ordAlias))) (Some cfFilter)
                    let middleSel = { Ctes = []; Body = SingleSelect middleCore }
                    let midAlias = nextAlias "_tw"
                    let outerGA = SqlExpr.FunctionCall("json_group_array", [SqlExpr.Column(Some midAlias, "v")])
                    let outerCore = mkSubCore [{ Alias = None; Expr = outerGA }] (Some(DerivedTable(middleSel, midAlias))) None
                    SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
                | None ->
                    let outerGA = SqlExpr.FunctionCall("json_group_array", [SqlExpr.Column(Some ordAlias, "v")])
                    let outerCore = mkSubCore [{ Alias = None; Expr = outerGA }] (Some(DerivedTable(innerSel, ordAlias))) None
                    SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
        | None ->
            raise (NotSupportedException("Select projection required."))

    /// Build Contains (entity Id-based membership or projected scalar).
    let private buildContains (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) (valueExpr: Expression) : SqlExpr =
        // Entity Contains: EXISTS(SELECT 1 FROM link WHERE ownerId AND targetId = entity.Id)
        let propName = ownerRef.PropertyExpr.Member.Name
        let ctx = qb.SourceContext
        let linkTable = dbRefManyLinkTable ctx ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource ctx ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"

        let idProp = valueExpr.Type.GetProperty("Id")
        if isNull idProp then
            raise (NotSupportedException("Contains requires an entity with an Id property."))
        let idAccess = Expression.MakeMemberAccess(valueExpr, idProp)
        let entityIdDu = visitDu idAccess qb

        let ownerWhere = SqlExpr.Binary(SqlExpr.Column(None, ownerColumn), BinaryOperator.Eq, SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
        let targetWhere = SqlExpr.Binary(SqlExpr.Column(None, targetColumn), BinaryOperator.Eq, entityIdDu)
        let fullWhere = SqlExpr.Binary(ownerWhere, BinaryOperator.And, targetWhere)
        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let core = mkSubCore oneProj (Some(BaseTable(linkTable, None))) (Some fullWhere)
        SqlExpr.Exists { Ctes = []; Body = SingleSelect core }


    /// Build SQL from a QueryDescriptor with a pre-resolved owner ref.
    /// Returns Some(SqlExpr) if handled, None otherwise.
    let tryBuild (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlExpr voption =

        // GroupBy terminals — delegated to BuildSpecial.
        match desc.GroupByKey with
        | Some keyLambda ->
            DBRefManyBuildSpecial.tryBuildGroupBy qb desc buildCorrelatedCore mkSubCore nextAlias ownerRef keyLambda
        | None ->

        // Set operations — defer to existing handler (requires expression reconstruction).
        match desc.SetOp with
        | Some _ -> ValueNone
        | None ->

        // TakeWhile/SkipWhile — delegated to BuildSpecial.
        match desc.TakeWhileInfo with
        | Some (twPredLambda, isTakeWhile) ->
            DBRefManyBuildSpecial.tryBuildTakeWhile qb desc buildCorrelatedCore mkSubCore nextAlias ownerRef twPredLambda isTakeWhile
        | None ->

        // Distinct.Count — null-safe two-layer: inner SELECT DISTINCT, outer COUNT(*).
        if desc.Distinct && (match desc.Terminal with Terminal.Count | Terminal.LongCount -> true | _ -> false) then
            match desc.SelectProjection with
            | Some projLambda ->
                let tgtAlias, _, baseCore = buildCorrelatedCore qb desc ownerRef []
                let subQb = qb.ForSubquery(tgtAlias, projLambda)
                let projDu = visitDu projLambda.Body subQb
                let distinctCore = { baseCore with
                                        Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = projDu }]
                                        Distinct = true; Limit = None; Offset = None }
                let distinctSel = { Ctes = []; Body = SingleSelect distinctCore }
                let dtAlias = nextAlias "_dc"
                let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                let outerCore = mkSubCore countProj (Some(DerivedTable(distinctSel, dtAlias))) None
                ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore })
            | None -> ValueNone
        else

        // Non-GroupBy, non-SetOp terminals.
        let result =
            match desc.Terminal with
            | Terminal.Exists ->
                buildExists qb desc ownerRef
            | Terminal.Any(Some pred) ->
                let descWithPred = { desc with WherePredicates = desc.WherePredicates @ [pred] }
                buildExists qb descWithPred ownerRef
            | Terminal.Any None ->
                buildExists qb desc ownerRef
            | Terminal.All pred ->
                buildNotExists qb desc ownerRef pred
            | Terminal.Count | Terminal.LongCount ->
                buildCount qb desc ownerRef
            | Terminal.Sum sel -> buildAggregate qb desc ownerRef sel AggregateKind.Sum
            | Terminal.Min sel -> buildAggregate qb desc ownerRef sel AggregateKind.Min
            | Terminal.Max sel -> buildAggregate qb desc ownerRef sel AggregateKind.Max
            | Terminal.Average sel -> buildAggregate qb desc ownerRef sel AggregateKind.Avg
            | Terminal.Select _ ->
                buildSelect qb desc ownerRef
            | Terminal.Contains value ->
                buildContains qb desc ownerRef value

        ValueSome result
