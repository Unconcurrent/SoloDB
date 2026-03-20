namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitDbRefPeelers3
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
    /// Returns (tgtAlias, lnkAlias, core, targetTable) with JOIN + WHERE + ORDER BY + LIMIT/OFFSET.
    let internal buildCorrelatedCore
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyOwnerRef)
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
        if desc.OfTypeName.IsSome then
            DBRefManyHelpers.ensureOfTypeSupported targetType
        let tgtAlias = nextAlias "_tgt"
        let lnkAlias = nextAlias "_lnk"

        // Collect inner DBRef JOINs from isolated ForSubquery contexts.
        let innerJoinEdges = ResizeArray<JoinEdge>()

        let wherePredDus =
            desc.WherePredicates
            |> List.collect (fun predExpr ->
                match tryExtractLambdaExpression predExpr with
                | ValueSome predLambda ->
                    let subQb = qb.ForSubquery(tgtAlias, predLambda, subqueryRootTable = targetTable)
                    let result = [visitDu predLambda.Body subQb]
                    innerJoinEdges.AddRange(subQb.SourceContext.Joins)
                    result
                | ValueNone ->
                    raise (NotSupportedException(
                        "Error: Cannot translate relation-backed DBRefMany.Any predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable.")))

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
                    let subQb = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
                    let result = { Expr = visitDu keyLambda.Body subQb; Direction = dir }
                    innerJoinEdges.AddRange(subQb.SourceContext.Joins)
                    result
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

        // Edge case: DBRef.Value navigation inside predicates/sort keys adds JOINs to isolated ForSubquery contexts.
        // These JOINs must be emitted inside the correlated subquery, not leaked to the outer scope.
        let dbRefJoins = DBRefManyHelpers.joinEdgesToClauses innerJoinEdges

        let innerCore =
            { mkSubCore projections (Some(BaseTable(linkTable, Some lnkAlias))) (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] @ dbRefJoins
                OrderBy = sortKeyDus
                Limit = limitDu
                Offset = offsetDu }

        // PostBound wrapping: when Take/Skip precedes Where/OrderBy in the LINQ pipeline,
        // the descriptor has PostBound operators that must be applied AFTER bounding.
        // Wrap the inner core in a DerivedTable and apply PostBound on the outer layer.
        let hasPostBound =
            desc.PostBoundWherePredicates.Length > 0 ||
            desc.PostBoundSortKeys.Length > 0 ||
            desc.PostBoundLimit.IsSome ||
            desc.PostBoundOffset.IsSome

        if hasPostBound then
            // PostBound wrapping: inner core bounded by Take/Skip, outer applies post-bound operators.
            // Inner core projects pass-through columns for the DerivedTable.
            let passThrough = { innerCore with Projections = AllColumns }
            let innerSel = { Ctes = []; Body = SingleSelect passThrough }
            let pbAlias = nextAlias "_pb"

            // Translate PostBound WHERE predicates against the _pb alias.
            let pbJoinEdges = ResizeArray<JoinEdge>()
            let pbWhereDus =
                desc.PostBoundWherePredicates
                |> List.collect (fun predExpr ->
                    match tryExtractLambdaExpression predExpr with
                    | ValueSome predLambda ->
                        let subQb = qb.ForSubquery(pbAlias, predLambda, subqueryRootTable = targetTable)
                        let result = [visitDu predLambda.Body subQb]
                        pbJoinEdges.AddRange(subQb.SourceContext.Joins)
                        result
                    | ValueNone ->
                        raise (NotSupportedException(
                            "Error: Cannot translate relation-backed DBRefMany predicate.\nReason: The predicate is not a translatable lambda expression.\nFix: Pass the predicate as an inline lambda, not a delegate variable.")))

            // Translate PostBound sort keys.
            let pbSortKeyDus =
                desc.PostBoundSortKeys
                |> List.map (fun (keyExpr, dir) ->
                    match tryExtractLambdaExpression keyExpr with
                    | ValueSome keyLambda ->
                        let subQb = qb.ForSubquery(pbAlias, keyLambda, subqueryRootTable = targetTable)
                        let result = { Expr = visitDu keyLambda.Body subQb; Direction = dir }
                        pbJoinEdges.AddRange(subQb.SourceContext.Joins)
                        result
                    | ValueNone ->
                        raise (NotSupportedException("Cannot extract key selector for PostBound OrderBy.")))

            let pbLimitDu =
                match desc.PostBoundLimit with
                | Some e ->
                    match e with
                    | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
                    | _ -> Some(visitDu e qb)
                | None ->
                    // SQLite requires LIMIT before OFFSET. When only OFFSET is present, use LIMIT -1 (unlimited).
                    match desc.PostBoundOffset with
                    | Some _ -> Some(SqlExpr.Literal(SqlLiteral.Integer -1L))
                    | None -> None
            let pbOffsetDu =
                match desc.PostBoundOffset with
                | Some e ->
                    match e with
                    | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
                    | _ -> Some(visitDu e qb)
                | None -> None

            let pbWhere =
                match pbWhereDus with
                | [] -> None
                | preds -> Some(preds |> List.reduce (fun a b -> SqlExpr.Binary(a, BinaryOperator.And, b)))

            let pbDbRefJoins = DBRefManyHelpers.joinEdgesToClauses pbJoinEdges

            let outerCore =
                { mkSubCore projections (Some(DerivedTable(innerSel, pbAlias))) pbWhere with
                    Joins = pbDbRefJoins
                    OrderBy = pbSortKeyDus
                    Limit = pbLimitDu
                    Offset = pbOffsetDu }

            // Return _pb as the effective target alias for terminal builders.
            pbAlias, lnkAlias, outerCore, targetTable
        else
            tgtAlias, lnkAlias, innerCore, targetTable


    let private countDbRefManyDepth (expr: Expression) : int =
        let rec visitExpr (e: Expression) : int =
            let e = unwrapConvert e
            match e with
            | null -> 0
            | :? MemberExpression as me ->
                let selfDepth = if isDBRefManyType me.Type then 1 else 0
                selfDepth + visitExpr me.Expression
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
        let _, _, core, _ = buildCorrelatedCore qb desc ownerRef oneProj
        SqlExpr.Exists { Ctes = []; Body = SingleSelect core }

    /// Build NOT EXISTS subquery (for All terminal).
    let private buildNotExists (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) (allPredExpr: Expression) : SqlExpr =
        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let tgtAlias, _, core, targetTable = buildCorrelatedCore qb desc ownerRef oneProj
        match tryExtractLambdaExpression allPredExpr with
        | ValueSome allPredLambda ->
            let subQb = qb.ForSubquery(tgtAlias, allPredLambda, subqueryRootTable = targetTable)
            let allPredDu = visitDu allPredLambda.Body subQb
            let allPredJoins = DBRefManyHelpers.joinEdgesToClauses subQb.SourceContext.Joins
            let negatedPred = SqlExpr.Unary(UnaryOperator.Not, allPredDu)
            let extendedWhere =
                match core.Where with
                | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, negatedPred))
                | None -> Some negatedPred
            let coreWithNot = { core with Where = extendedWhere; Joins = core.Joins @ allPredJoins }

            if core.Limit.IsSome || core.Offset.IsSome then
                let boundedCore = { core with Where = core.Where; Projections = AllColumns }
                let boundedSel = { Ctes = []; Body = SingleSelect boundedCore }
                let bndAlias = nextAlias "_bnd"
                let bndQb = qb.ForSubquery(bndAlias, allPredLambda, subqueryRootTable = targetTable)
                let bndPredDu = visitDu allPredLambda.Body bndQb
                let bndJoins = DBRefManyHelpers.joinEdgesToClauses bndQb.SourceContext.Joins
                let outerWhere = SqlExpr.Unary(UnaryOperator.Not, bndPredDu)
                let outerCore = { mkSubCore oneProj (Some(DerivedTable(boundedSel, bndAlias))) (Some outerWhere) with Joins = bndJoins }
                let outerSel = { Ctes = []; Body = SingleSelect outerCore }
                SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists outerSel)
            else
                SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists { Ctes = []; Body = SingleSelect coreWithNot })
        | ValueNone ->
            raise (NotSupportedException(
                "Error: Cannot translate relation-backed DBRefMany.All predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."))

    /// Build scalar COUNT subquery.
    let private buildCount (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlExpr =
        let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
        let _, _, core, _ = buildCorrelatedCore qb desc ownerRef countProj

        if core.Limit.IsSome || core.Offset.IsSome then
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
            let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, selectorLambda, subqueryRootTable = targetTable)
            let selectorDu = visitDu selectorLambda.Body subQb
            let selectorJoins = DBRefManyHelpers.joinEdgesToClauses subQb.SourceContext.Joins
            let aggExpr = SqlExpr.AggregateCall(aggKind, Some selectorDu, false, None)

            if baseCore.Limit.IsSome || baseCore.Offset.IsSome then
                let innerCore = { baseCore with Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = selectorDu }]; Joins = baseCore.Joins @ selectorJoins }
                let innerSel = { Ctes = []; Body = SingleSelect innerCore }
                let pgAlias = nextAlias "_pg"
                let outerAgg = SqlExpr.AggregateCall(aggKind, Some(SqlExpr.Column(Some pgAlias, "v")), false, None)
                let outerCore = mkSubCore [{ Alias = None; Expr = outerAgg }] (Some(DerivedTable(innerSel, pgAlias))) None
                let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
                if aggKind = AggregateKind.Sum then SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                else scalarExpr
            else
                let aggProj = [{ Alias = None; Expr = aggExpr }]
                let core = { baseCore with Projections = ProjectionSetOps.ofList aggProj; Joins = baseCore.Joins @ selectorJoins }
                let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                if aggKind = AggregateKind.Sum then SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                else scalarExpr
        | ValueNone ->
            raise (NotSupportedException("Cannot extract selector for aggregate."))

    /// Build the projected scalar rowset that Select/Distinct/Take/Skip/OfType compose over.
    let private buildProjectedRowset (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlSelect =
        match desc.SelectProjection with
        | Some projLambda ->
            let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
            let projectedDu = visitDu projLambda.Body subQb
            let projJoins = DBRefManyHelpers.joinEdgesToClauses subQb.SourceContext.Joins
            let projectedCore =
                { baseCore with
                    Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = projectedDu }]
                    Distinct = false
                    Joins = baseCore.Joins @ projJoins }
            if desc.Distinct && (baseCore.Limit.IsSome || baseCore.Offset.IsSome) then
                let innerSel = { Ctes = []; Body = SingleSelect projectedCore }
                let dtAlias = nextAlias "_pd"
                let outerCore =
                    { mkSubCore [{ Alias = Some "v"; Expr = SqlExpr.Column(Some dtAlias, "v") }] (Some(DerivedTable(innerSel, dtAlias))) None with
                        Distinct = true }
                { Ctes = []; Body = SingleSelect outerCore }
            else
                let finalCore = { projectedCore with Distinct = desc.Distinct }
                { Ctes = []; Body = SingleSelect finalCore }
        | None ->
            raise (NotSupportedException("Projected scalar terminal requires Select projection."))

    let private buildProjectedAggregate (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) (aggKind: AggregateKind) : SqlExpr =
        if desc.TakeWhileInfo.IsSome then
            raise (NotSupportedException("Projected scalar aggregate after TakeWhile/SkipWhile is not supported."))

        let projectedSel = buildProjectedRowset qb desc ownerRef
        let projectedAlias = nextAlias "_pa"
        let projectedValue = SqlExpr.Column(Some projectedAlias, "v")
        let aggExpr = SqlExpr.AggregateCall(aggKind, Some projectedValue, false, None)
        let aggCore = mkSubCore [{ Alias = None; Expr = aggExpr }] (Some(DerivedTable(projectedSel, projectedAlias))) None
        let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect aggCore }
        if aggKind = AggregateKind.Sum then
            SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
        else
            scalarExpr

    let private buildProjectedPredicateExists (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) (predicateExpr: Expression) : SqlExpr =
        match tryExtractLambdaExpression predicateExpr with
        | ValueSome predLambda ->
            let projectedSel = buildProjectedRowset qb desc ownerRef
            let projectedAlias = nextAlias "_pp"
            let valueCore =
                mkSubCore
                    [{ Alias = Some "Value"; Expr = SqlExpr.Column(Some projectedAlias, "v") }]
                    (Some(DerivedTable(projectedSel, projectedAlias)))
                    None
            let valueSel = { Ctes = []; Body = SingleSelect valueCore }
            let predicateAlias = nextAlias "_pv"
            let predQb =
                { qb.ForSubquery(predicateAlias, predLambda) with
                    JsonExtractSelfValue = false }
            let predDu = visitDu predLambda.Body predQb
            let predJoins = DBRefManyHelpers.joinEdgesToClauses predQb.SourceContext.Joins
            let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
            let existsCore =
                { mkSubCore oneProj (Some(DerivedTable(valueSel, predicateAlias))) (Some predDu) with
                    Joins = predJoins }
            SqlExpr.Exists { Ctes = []; Body = SingleSelect existsCore }
        | ValueNone ->
            raise (NotSupportedException(
                "Error: Cannot translate relation-backed DBRefMany.Any predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."))

    let private appendTerminalPredicate (desc: QueryDescriptor) (predicateOpt: Expression option) =
        match predicateOpt with
        | Some pred -> { desc with WherePredicates = desc.WherePredicates @ [pred] }
        | None -> desc

    let private buildEntityValueExpr (tgtAlias: string) =
        SqlExpr.FunctionCall("jsonb_set", [
            SqlExpr.Column(Some tgtAlias, "Value")
            SqlExpr.Literal(SqlLiteral.String "$.Id")
            SqlExpr.Column(Some tgtAlias, "Id")
        ])

    let private buildOrderedElementSubquery
        (baseCore: SelectCore)
        (valueExpr: SqlExpr)
        (effectiveOrder: _ list)
        (pickLast: bool)
        : SqlSelect =
        let windowOrder = effectiveOrder |> List.map (fun ob -> (ob.Expr, ob.Direction))
        let innerCore =
            { baseCore with
                Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = valueExpr }
                        { Alias = Some "__ord"; Expr = SqlExpr.WindowCall({
                            Kind = WindowFunctionKind.RowNumber
                            Arguments = []
                            PartitionBy = []
                            OrderBy = windowOrder
                        }) }
                    ] }
        let innerSel = { Ctes = []; Body = SingleSelect innerCore }
        let ordAlias = nextAlias "_elt"
        let outerCore =
            { mkSubCore [{ Alias = None; Expr = SqlExpr.Column(Some ordAlias, "v") }] (Some(DerivedTable(innerSel, ordAlias))) None with
                OrderBy = [{ Expr = SqlExpr.Column(Some ordAlias, "__ord"); Direction = if pickLast then SortDirection.Desc else SortDirection.Asc }]
                Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L)) }
        { Ctes = []; Body = SingleSelect outerCore }

    let private buildEntityElement
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyOwnerRef)
        (predicateOpt: Expression option)
        (pickLast: bool)
        : SqlExpr =
        let desc = appendTerminalPredicate desc predicateOpt
        let tgtAlias, lnkAlias, baseCore, _ =
            buildCorrelatedCore qb desc ownerRef []
        let effectiveOrder =
            match baseCore.OrderBy with
            | _ :: _ when pickLast -> baseCore.OrderBy
            | _ :: _ -> baseCore.OrderBy
            | [] when pickLast -> [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Desc }]
            | [] -> []
        let elementSel = buildOrderedElementSubquery baseCore (buildEntityValueExpr tgtAlias) effectiveOrder (pickLast && baseCore.OrderBy.Length > 0)
        SqlExpr.ScalarSubquery elementSel

    let private buildProjectedElement
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyOwnerRef)
        (predicateOpt: Expression option)
        (pickLast: bool)
        : SqlExpr =
        let desc = appendTerminalPredicate desc predicateOpt
        match desc.SelectProjection with
        | Some projLambda ->
            let tgtAlias, lnkAlias, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
            let projectedDu = visitDu projLambda.Body subQb
            let projJoins = DBRefManyHelpers.joinEdgesToClauses subQb.SourceContext.Joins
            let baseCore = { baseCore with Joins = baseCore.Joins @ projJoins }
            let effectiveOrder =
                match baseCore.OrderBy with
                | _ :: _ -> baseCore.OrderBy
                | [] when pickLast -> [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Desc }]
                | [] -> []
            let elementSel = buildOrderedElementSubquery baseCore projectedDu effectiveOrder (pickLast && baseCore.OrderBy.Length > 0)
            SqlExpr.ScalarSubquery elementSel
        | None ->
            raise (NotSupportedException("Projected element terminal requires Select projection."))

    let private buildSingleLike
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyOwnerRef)
        (predicateOpt: Expression option)
        (orDefault: bool)
        : SqlExpr =
        let desc =
            appendTerminalPredicate desc predicateOpt
        let desc =
            match desc.Limit, desc.Offset with
            | Some _, _
            | _, Some _ -> desc
            | None, None ->
                { desc with Limit = Some(Expression.Constant(2)) }

        let rowsetSel =
            match desc.SelectProjection with
            | Some projLambda ->
                let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
                let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
                let projectedDu = visitDu projLambda.Body subQb
                let projJoins = DBRefManyHelpers.joinEdgesToClauses subQb.SourceContext.Joins
                let core =
                    { baseCore with
                        Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = projectedDu }]
                        Joins = baseCore.Joins @ projJoins }
                { Ctes = []; Body = SingleSelect core }
            | None ->
                let tgtAlias, _, baseCore, _ = buildCorrelatedCore qb desc ownerRef []
                let core =
                    { baseCore with
                        Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = buildEntityValueExpr tgtAlias }] }
                { Ctes = []; Body = SingleSelect core }

        let rowAlias = nextAlias "_sg"
        let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
        let firstValueExpr =
            SqlExpr.FunctionCall("MIN", [SqlExpr.Column(Some rowAlias, "v")])
        let noElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains no elements")])
        let manyElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains more than one element")])
        let zeroCase = if orDefault then SqlExpr.Literal(SqlLiteral.Null) else noElementsExpr
        let valueExpr =
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), zeroCase),
                [
                    (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)), firstValueExpr)
                ],
                Some manyElementsExpr)
        let outerCore = mkSubCore [{ Alias = None; Expr = valueExpr }] (Some(DerivedTable(rowsetSel, rowAlias))) None
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let private buildIndexedLike
        (qb: QueryBuilder)
        (rowsetSel: SqlSelect)
        (indexExpr: Expression)
        (orDefault: bool)
        : SqlExpr =
        let idx =
            match indexExpr with
            | :? ConstantExpression as ce -> SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value)))
            | _ -> visitDu indexExpr qb
        let indexedAlias = nextAlias "_ei"
        let indexedCore =
            { mkSubCore [{ Alias = Some "v"; Expr = SqlExpr.Column(Some indexedAlias, "v") }] (Some(DerivedTable(rowsetSel, indexedAlias))) None with
                Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                Offset = Some idx }
        let indexedSel = { Ctes = []; Body = SingleSelect indexedCore }
        let rowAlias = nextAlias "_eo"
        let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
        let firstValueExpr = SqlExpr.FunctionCall("MIN", [SqlExpr.Column(Some rowAlias, "v")])
        let noElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Index was out of range. Must be non-negative and less than the size of the collection.")])
        let valueExpr =
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)),
                 if orDefault then SqlExpr.Literal(SqlLiteral.Null) else noElementsExpr),
                [],
                Some firstValueExpr)
        let outerCore = mkSubCore [{ Alias = None; Expr = valueExpr }] (Some(DerivedTable(indexedSel, rowAlias))) None
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let private buildEntityElementAt
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyOwnerRef)
        (indexExpr: Expression)
        (orDefault: bool)
        : SqlExpr =
        let tgtAlias, lnkAlias, baseCore, _ = buildCorrelatedCore qb desc ownerRef []
        let effectiveOrder =
            match baseCore.OrderBy with
            | _ :: _ -> baseCore.OrderBy
            | [] -> [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Asc }]
        let rowCore =
            { baseCore with
                Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = buildEntityValueExpr tgtAlias }]
                OrderBy = effectiveOrder
                Limit = None
                Offset = baseCore.Offset }
        let rowSel = { Ctes = []; Body = SingleSelect rowCore }
        buildIndexedLike qb rowSel indexExpr orDefault

    let private buildProjectedElementAt
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyOwnerRef)
        (indexExpr: Expression)
        (orDefault: bool)
        : SqlExpr =
        let projectedSel = buildProjectedRowset qb desc ownerRef
        buildIndexedLike qb projectedSel indexExpr orDefault

    let private buildDistinctByEntitySequence
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyOwnerRef)
        (keyExpr: Expression)
        : SqlExpr =
        match tryExtractLambdaExpression keyExpr with
        | ValueSome keyLambda ->
            let tgtAlias, lnkAlias, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
            let keyDu = visitDu keyLambda.Body subQb
            let keyJoins = DBRefManyHelpers.joinEdgesToClauses subQb.SourceContext.Joins
            let effectiveOrder =
                if baseCore.OrderBy.Length > 0 then
                    baseCore.OrderBy
                else
                    [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Asc }]
            let rankExpr =
                SqlExpr.WindowCall({
                    Kind = WindowFunctionKind.RowNumber
                    Arguments = []
                    PartitionBy = [keyDu]
                    OrderBy = effectiveOrder |> List.map (fun ob -> (ob.Expr, ob.Direction))
                })
            let innerCore =
                { baseCore with
                    Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = buildEntityValueExpr tgtAlias }
                            { Alias = Some "__rk"; Expr = rankExpr }
                        ]
                    Joins = baseCore.Joins @ keyJoins }
            let innerSel = { Ctes = []; Body = SingleSelect innerCore }
            let rankAlias = nextAlias "_db"
            let filteredCore =
                mkSubCore
                    [{ Alias = Some "v"; Expr = SqlExpr.Column(Some rankAlias, "v") }]
                    (Some(DerivedTable(innerSel, rankAlias)))
                    (Some(SqlExpr.Binary(SqlExpr.Column(Some rankAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L))))
            let filteredSel = { Ctes = []; Body = SingleSelect filteredCore }
            let outAlias = nextAlias "_dba"
            let outerGA = SqlExpr.FunctionCall("json_group_array", [SqlExpr.FunctionCall("json", [SqlExpr.Column(Some outAlias, "v")])])
            let outerCore = mkSubCore [{ Alias = None; Expr = outerGA }] (Some(DerivedTable(filteredSel, outAlias))) None
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
        | ValueNone ->
            raise (NotSupportedException("Cannot extract key selector for DistinctBy."))

    /// Build Select projection (json_group_array).
    let private buildSelect (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlExpr =
        match desc.SelectProjection with
        | Some projLambda ->
            if countDbRefManyDepth projLambda.Body > 0 then
                raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))

            let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
            let projectedDu = visitDu projLambda.Body subQb
            // Edge case: DBRef.Value navigation inside Select projection (e.g., tag.TagType.Value.Code)
            // generates JOINs in the isolated ForSubquery context — capture and include in core.
            let projJoins = DBRefManyHelpers.joinEdgesToClauses subQb.SourceContext.Joins
            let baseCore = { baseCore with Joins = baseCore.Joins @ projJoins }

            let hasTakeSkipOrOrder = baseCore.OrderBy.Length > 0 || baseCore.Limit.IsSome || baseCore.Offset.IsSome
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
                        let twSubQb = qb.ForSubquery(tgtAlias, twPredLambda, subqueryRootTable = targetTable)
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
        if desc.SelectProjection.IsSome then
            let projectedSel = buildProjectedRowset qb desc ownerRef
            let projectedAlias = nextAlias "_pc"
            let valueDu = visitDu valueExpr qb
            let containsWhere = nullSafeEq (SqlExpr.Column(Some projectedAlias, "v")) valueDu
            let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
            let containsCore = mkSubCore oneProj (Some(DerivedTable(projectedSel, projectedAlias))) (Some containsWhere)
            SqlExpr.Exists { Ctes = []; Body = SingleSelect containsCore }
        else
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

        // TakeWhile/SkipWhile — delegated to BuildSpecial.
        match desc.TakeWhileInfo with
        | Some (twPredLambda, isTakeWhile) ->
            DBRefManyBuildSpecial.tryBuildTakeWhile qb desc buildCorrelatedCore mkSubCore nextAlias ownerRef twPredLambda isTakeWhile
        | None ->

        // GroupBy terminals — delegated to BuildSpecial.
        match desc.GroupByKey with
        | Some keyLambda ->
            DBRefManyBuildSpecial.tryBuildGroupBy qb desc buildCorrelatedCore mkSubCore nextAlias ownerRef keyLambda
        | None ->

        // Set operations — defer to existing handler (requires expression reconstruction).
        match desc.SetOp with
        | Some _ -> ValueNone
        | None ->

        // Distinct.Count — null-safe two-layer: inner SELECT DISTINCT, outer COUNT(*).
        if desc.Distinct && (match desc.Terminal with Terminal.Count | Terminal.LongCount -> true | _ -> false) then
            match desc.SelectProjection with
            | Some _ ->
                let distinctSel = buildProjectedRowset qb desc ownerRef
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
                if desc.SelectProjection.IsSome && desc.TakeWhileInfo.IsNone then
                    SqlExpr.Exists (buildProjectedRowset qb desc ownerRef)
                else
                    buildExists qb desc ownerRef
            | Terminal.Any(Some pred) ->
                if desc.SelectProjection.IsSome && desc.TakeWhileInfo.IsNone then
                    buildProjectedPredicateExists qb desc ownerRef pred
                else
                    let descWithPred = { desc with WherePredicates = desc.WherePredicates @ [pred] }
                    buildExists qb descWithPred ownerRef
            | Terminal.Any None ->
                if desc.SelectProjection.IsSome && desc.TakeWhileInfo.IsNone then
                    SqlExpr.Exists (buildProjectedRowset qb desc ownerRef)
                else
                    buildExists qb desc ownerRef
            | Terminal.All pred ->
                buildNotExists qb desc ownerRef pred
            | Terminal.Count | Terminal.LongCount ->
                if desc.SelectProjection.IsSome && desc.TakeWhileInfo.IsNone then
                    let projectedSel = buildProjectedRowset qb desc ownerRef
                    let projectedAlias = nextAlias "_pc"
                    let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                    let countCore = mkSubCore countProj (Some(DerivedTable(projectedSel, projectedAlias))) None
                    SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }
                else
                    buildCount qb desc ownerRef
            | Terminal.Sum sel -> buildAggregate qb desc ownerRef sel AggregateKind.Sum
            | Terminal.SumProjected -> buildProjectedAggregate qb desc ownerRef AggregateKind.Sum
            | Terminal.Min sel -> buildAggregate qb desc ownerRef sel AggregateKind.Min
            | Terminal.MinProjected -> buildProjectedAggregate qb desc ownerRef AggregateKind.Min
            | Terminal.Max sel -> buildAggregate qb desc ownerRef sel AggregateKind.Max
            | Terminal.MaxProjected -> buildProjectedAggregate qb desc ownerRef AggregateKind.Max
            | Terminal.Average sel -> buildAggregate qb desc ownerRef sel AggregateKind.Avg
            | Terminal.AverageProjected -> buildProjectedAggregate qb desc ownerRef AggregateKind.Avg
            | Terminal.Select _ ->
                buildSelect qb desc ownerRef
            | Terminal.Contains value ->
                buildContains qb desc ownerRef value
            | Terminal.First pred ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedElement qb desc ownerRef pred false
                | None -> buildEntityElement qb desc ownerRef pred false
            | Terminal.FirstOrDefault pred ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedElement qb desc ownerRef pred false
                | None -> buildEntityElement qb desc ownerRef pred false
            | Terminal.Last pred ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedElement qb desc ownerRef pred true
                | None -> buildEntityElement qb desc ownerRef pred true
            | Terminal.LastOrDefault pred ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedElement qb desc ownerRef pred true
                | None -> buildEntityElement qb desc ownerRef pred true
            | Terminal.Single pred ->
                buildSingleLike qb desc ownerRef pred false
            | Terminal.SingleOrDefault pred ->
                buildSingleLike qb desc ownerRef pred true
            | Terminal.ElementAt indexExpr ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedElementAt qb desc ownerRef indexExpr false
                | None -> buildEntityElementAt qb desc ownerRef indexExpr false
            | Terminal.ElementAtOrDefault indexExpr ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedElementAt qb desc ownerRef indexExpr true
                | None -> buildEntityElementAt qb desc ownerRef indexExpr true
            // R55: MinBy/MaxBy — ORDER BY key ASC/DESC + First element.
            | Terminal.MinBy keySel ->
                let desc = { desc with SortKeys = [(keySel, SortDirection.Asc)] }
                buildEntityElement qb desc ownerRef None false
            | Terminal.MaxBy keySel ->
                let desc = { desc with SortKeys = [(keySel, SortDirection.Desc)] }
                buildEntityElement qb desc ownerRef None false
            | Terminal.DistinctBy keySel ->
                buildDistinctByEntitySequence qb desc ownerRef keySel

        ValueSome result
