namespace SoloDatabase

open System
open System.Collections
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
open SoloDatabase.Attributes

/// Builds SQL DU trees from a unified DBRefManyQueryDescriptor.
/// Replaces the order-dependent per-handler peeling in VisitDbRef.
module internal DBRefManyBuilder =
    [<Literal>]
    let private nestedDbRefManyNotSupportedMessage =
        "Error: Deeply nested DBRefMany query is not supported.\nReason: Only one level of DBRefMany nesting is allowed in relation predicates.\nFix: Rewrite to at most one nested DBRefMany level, or move deeper traversal after AsEnumerable()."
    type DBRefManyOwnerRef = DBRefManyDescriptor.DBRefManyOwnerRef
    let private joinEdgesToClauses = DBRefManyHelpers.joinEdgesToClauses

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

    let private tryGetRelationOrderByForTakeWhile
        (ownerRef: DBRefManyOwnerRef)
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
    let tryBuild (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlExpr voption =
        let countDbRefManyDepth =
            DBRefManyBuilderTerminals.countDbRefManyDepth unwrapConvert isDBRefManyType
        let buildProjectedRowset qb desc ownerRef =
            DBRefManyBuilderTerminals.buildProjectedRowset buildCorrelatedCore nextAlias visitDu joinEdgesToClauses qb desc ownerRef
        let buildProjectedAggregate qb desc ownerRef aggKind =
            if desc.TakeWhileInfo.IsSome then
                raise (NotSupportedException("Projected scalar aggregate after TakeWhile/SkipWhile is not supported."))
            DBRefManyBuilderTerminals.buildProjectedAggregate nextAlias qb (buildProjectedRowset qb desc ownerRef) aggKind
        let buildExists = DBRefManyBuilderTerminals.buildExists buildCorrelatedCore
        let buildProjectedPredicateExists qb desc ownerRef pred =
            DBRefManyBuilderTerminals.buildProjectedPredicateExists nextAlias tryExtractLambdaExpression visitDu joinEdgesToClauses qb (buildProjectedRowset qb desc ownerRef) pred
        let buildNotExists =
            DBRefManyBuilderTerminals.buildNotExists buildCorrelatedCore mkSubCore nextAlias tryExtractLambdaExpression visitDu joinEdgesToClauses
        let buildCount = DBRefManyBuilderTerminals.buildCount buildCorrelatedCore mkSubCore nextAlias
        let buildAggregate =
            DBRefManyBuilderTerminals.buildAggregate buildCorrelatedCore mkSubCore nextAlias tryExtractLambdaExpression visitDu joinEdgesToClauses
        let buildSelect =
            DBRefManyBuilderTerminals.buildSelect countDbRefManyDepth nestedDbRefManyNotSupportedMessage buildCorrelatedCore nextAlias visitDu joinEdgesToClauses
        let buildContains =
            DBRefManyBuilderTerminals.buildContains dbRefManyLinkTable dbRefManyOwnerUsesSource nullSafeEq visitDu buildProjectedRowset
        let buildEntityElement = DBRefManyBuilderElements.buildEntityElement nextAlias buildCorrelatedCore
        let buildProjectedElement =
            DBRefManyBuilderElements.buildProjectedElement nextAlias buildCorrelatedCore visitDu joinEdgesToClauses
        let buildSingleLike =
            DBRefManyBuilderElements.buildSingleLike nextAlias buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses
        let buildEntityElementAt = DBRefManyBuilderElements.buildEntityElementAt buildCorrelatedCore nextAlias visitDu
        let buildProjectedElementAt qb desc ownerRef indexExpr orDefault =
            DBRefManyBuilderElements.buildProjectedElementAt nextAlias visitDu qb (buildProjectedRowset qb desc ownerRef) indexExpr orDefault
        let buildDistinctByEntitySequence =
            DBRefManyBuilderSetOps.buildDistinctByEntitySequence buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias DBRefManyBuilderElements.buildEntityValueExpr
        let buildByFilterEntitySequence =
            DBRefManyBuilderSetOps.buildByFilterEntitySequence buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias nullSafeEq DBRefManyBuilderElements.buildEntityValueExpr isFullyConstant (fun expr -> evaluateExpr<IEnumerable> expr)
        let buildUnionByEntitySequence =
            DBRefManyBuilderSetOps.buildUnionByEntitySequence buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias DBRefManyBuilderElements.buildEntityValueExpr

        // TakeWhile/SkipWhile — delegated to BuildSpecial.
        match desc.TakeWhileInfo with
        | Some (twPredLambda, isTakeWhile) ->
            DBRefManyBuildSpecial.tryBuildTakeWhile qb desc buildCorrelatedCore mkSubCore nextAlias tryGetRelationOrderByForTakeWhile ownerRef twPredLambda isTakeWhile
        | None ->

        // CountBy — delegated to BuildSpecial.
        match desc.Terminal with
        | Terminal.CountBy keySel ->
            DBRefManyBuildSpecial.tryBuildCountBy qb desc buildCorrelatedCore mkSubCore nextAlias ownerRef keySel
        | _ ->

        // GroupBy terminals — delegated to BuildSpecial.
        match desc.GroupByKey with
        | Some keyLambda ->
            DBRefManyBuildSpecial.tryBuildGroupBy qb desc buildCorrelatedCore mkSubCore nextAlias ownerRef keyLambda
        | None ->

        // Set operations.
        match desc.SetOp with
        | Some (SetOperation.IntersectBy(rightKeys, keySel)) ->
            ValueSome(buildByFilterEntitySequence qb desc ownerRef keySel rightKeys false)
        | Some (SetOperation.ExceptBy(rightKeys, keySel)) ->
            ValueSome(buildByFilterEntitySequence qb desc ownerRef keySel rightKeys true)
        | Some (SetOperation.UnionBy(rightSource, keySel)) ->
            ValueSome(buildUnionByEntitySequence qb desc ownerRef rightSource keySel)
        | Some _ ->
            ValueNone
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
            | Terminal.Select _ -> buildSelect qb desc ownerRef
            | Terminal.Contains value -> buildContains qb desc ownerRef value
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
            | Terminal.Single pred -> buildSingleLike qb desc ownerRef pred false
            | Terminal.SingleOrDefault pred -> buildSingleLike qb desc ownerRef pred true
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
            | Terminal.DistinctBy keySel -> buildDistinctByEntitySequence qb desc ownerRef keySel
            | Terminal.CountBy _ -> failwith "CountBy is handled above; this branch is unreachable."

        ValueSome result
