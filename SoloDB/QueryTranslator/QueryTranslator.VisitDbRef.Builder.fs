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
    let private nestedDbRefManyNotSupportedMessage =
        QueryTranslatorVisitDbRefPeelers.nestedDbRefManyNotSupportedMessage
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
        if desc.OfTypeName.IsSome || desc.CastTypeName.IsSome then
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

        let effectiveAlias, effectiveLnk, effectiveCore, effectiveTarget =
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

        // Pre-Select DefaultIfEmpty — inject UNION ALL synthetic default row.
        match desc.DefaultIfEmpty with
        | Some defaultValueExprOpt ->
            // Compute default value SQL expression.
            let defaultValueDu =
                match defaultValueExprOpt with
                | Some valueExpr ->
                    let v = evaluateExpr<obj> valueExpr
                    let jsonObj = JsonSerializator.JsonValue.Serialize v
                    let jsonText = jsonObj.ToJsonString()
                    SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
                | None ->
                    SqlExpr.Literal(SqlLiteral.Null)

            // Build NOT EXISTS guard using the full inner core (for emptiness check).
            let existsCore = { effectiveCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
            let existsSel = { Ctes = []; Body = SingleSelect existsCore }

            // Flatten the effective core into a DerivedTable with Id+Value columns,
            // so both sides of UNION ALL have matching column shapes.
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

    /// Compute default value SQL expression for DefaultIfEmpty.
    /// For value with explicit expression: evaluate + serialize.
    /// For no-arg: value types → default(T), reference types → NULL.
    let private computeDefaultValueDu (defaultValueExprOpt: Expression option) (projectedType: Type option) : SqlExpr =
        match defaultValueExprOpt with
        | Some valueExpr ->
            let v = evaluateExpr<obj> valueExpr
            let jsonObj = JsonSerializator.JsonValue.Serialize v
            let jsonText = jsonObj.ToJsonString()
            SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
        | None ->
            match projectedType with
            | Some t when t.IsValueType ->
                let defaultValue = Activator.CreateInstance(t)
                let jsonObj = JsonSerializator.JsonValue.Serialize defaultValue
                let jsonText = jsonObj.ToJsonString()
                SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
            | _ ->
                SqlExpr.Literal(SqlLiteral.Null)

    /// Compute default value for json_array wrapping (post-Select Select terminal).
    let private computeDefaultValueForJsonArray (defaultValueExprOpt: Expression option) (projectedType: Type option) : SqlExpr =
        match defaultValueExprOpt with
        | Some valueExpr ->
            let v = evaluateExpr<obj> valueExpr
            let jsonObj = JsonSerializator.JsonValue.Serialize v
            let jsonText = jsonObj.ToJsonString()
            SqlExpr.FunctionCall("json", [SqlExpr.Literal(SqlLiteral.String jsonText)])
        | None ->
            match projectedType with
            | Some t when t.IsValueType ->
                let defaultValue = Activator.CreateInstance(t)
                let jsonObj = JsonSerializator.JsonValue.Serialize defaultValue
                let jsonText = jsonObj.ToJsonString()
                SqlExpr.FunctionCall("json", [SqlExpr.Literal(SqlLiteral.String jsonText)])
            | _ ->
                SqlExpr.Literal(SqlLiteral.Null)

    let tryBuild (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyOwnerRef) : SqlExpr voption =
        qb.StepTranslation()
        // Depth guard — check all predicate/projection expressions for nested DBRefMany depth.
        let peelerDepth = QueryTranslatorVisitDbRefPeelers.countDbRefManyDepth
        let maxExprDepth =
            let predDepths = desc.WherePredicates |> List.map peelerDepth
            let projDepth = desc.SelectProjection |> Option.map (fun l -> peelerDepth l.Body) |> Option.defaultValue 0
            let termDepth =
                match desc.Terminal with
                | Terminal.Any(Some pred) | Terminal.All pred | Terminal.Contains pred
                | Terminal.Sum pred | Terminal.Min pred | Terminal.Max pred | Terminal.Average pred
                | Terminal.First(Some pred) | Terminal.FirstOrDefault(Some pred)
                | Terminal.Last(Some pred) | Terminal.LastOrDefault(Some pred)
                | Terminal.Single(Some pred) | Terminal.SingleOrDefault(Some pred) -> peelerDepth pred
                | _ -> 0
            (predDepths @ [projDepth; termDepth]) |> List.max
        if maxExprDepth >= maxRelationDepth then
            raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))

        let countDbRefManyDepth =
            DBRefManyBuilderTerminals.countDbRefManyDepth unwrapConvert isDBRefManyType
        let buildProjectedRowsetRaw qb desc ownerRef =
            DBRefManyBuilderTerminals.buildProjectedRowset buildCorrelatedCore nextAlias visitDu joinEdgesToClauses qb desc ownerRef
        let projectedType = desc.SelectProjection |> Option.map (fun l -> l.Body.Type)
        let buildProjectedRowset qb desc ownerRef =
            let baseSel = buildProjectedRowsetRaw qb desc ownerRef
            // Post-Select DefaultIfEmpty — wrap projected rowset with UNION ALL default.
            match desc.PostSelectDefaultIfEmpty with
            | Some defaultValueExprOpt ->
                let defaultValueDu = computeDefaultValueDu defaultValueExprOpt projectedType
                // Extract the main core from baseSel, build UNION ALL with default row.
                // Projected rowset uses alias "v" for the single projected column.
                match baseSel.Body with
                | SingleSelect mainCore ->
                    let existsCore = { mainCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
                    let existsSel = { Ctes = []; Body = SingleSelect existsCore }
                    let defaultProjs =
                        [{ Alias = Some "v"; Expr = defaultValueDu }]
                    let defaultCore =
                        { mkSubCore defaultProjs None (Some(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists existsSel))) with
                            Joins = []; OrderBy = []; Limit = None; Offset = None }
                    { baseSel with Body = UnionAllSelect(mainCore, [defaultCore]) }
                | UnionAllSelect _ -> baseSel // Already union — skip
            | None -> baseSel
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
        let buildDistinctByEntityRowset =
            DBRefManyBuilderSetOps.buildDistinctByEntityRowset buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias (DBRefManyBuilderElements.buildEntityValueExpr desc.CastTypeName)
        let buildByFilterEntityRowset =
            DBRefManyBuilderSetOps.buildByFilterEntityRowset buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias nullSafeEq (DBRefManyBuilderElements.buildEntityValueExpr desc.CastTypeName) isFullyConstant (fun expr -> evaluateExpr<IEnumerable> expr)
        let buildUnionByEntityRowset =
            DBRefManyBuilderSetOps.buildUnionByEntityRowset buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias (DBRefManyBuilderElements.buildEntityValueExpr desc.CastTypeName)

        let buildSetOpFilteredRowset (rowsetSel: SqlSelect) (predExprOpt: Expression option) : SqlSelect =
            match predExprOpt with
            | None -> rowsetSel
            | Some predExpr ->
                match tryExtractLambdaExpression predExpr with
                | ValueSome predLambda ->
                    let rowAlias = nextAlias "_sr"
                    let valueCore =
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "Value"; Expr = SqlExpr.Column(Some rowAlias, "v") }
                                { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rowAlias, "__ord") }
                            ]
                          Source = Some(DerivedTable(rowsetSel, rowAlias))
                          Joins = []
                          Where = None
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None }
                    let valueSel = { Ctes = []; Body = SingleSelect valueCore }
                    let predAlias = nextAlias "_sp"
                    let predQb =
                        { qb.ForSubquery(predAlias, predLambda) with
                            JsonExtractSelfValue = false }
                    let predDu = visitDu predLambda.Body predQb
                    let predJoins = joinEdgesToClauses predQb.SourceContext.Joins
                    let filteredCore =
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "v"; Expr = SqlExpr.Column(Some predAlias, "Value") }
                                { Alias = Some "__ord"; Expr = SqlExpr.Column(Some predAlias, "__ord") }
                            ]
                          Source = Some(DerivedTable(valueSel, predAlias))
                          Joins = predJoins
                          Where = Some predDu
                          GroupBy = []
                          Having = None
                          OrderBy = [{ Expr = SqlExpr.Column(Some predAlias, "__ord"); Direction = SortDirection.Asc }]
                          Limit = None
                          Offset = None }
                    { Ctes = []; Body = SingleSelect filteredCore }
                | ValueNone ->
                    raise (NotSupportedException(
                        "Error: Cannot translate relation-backed DBRefMany.First/FirstOrDefault predicate after set operation.\nReason: The predicate is not a translatable lambda expression.\nFix: Pass the predicate as an inline lambda, not a delegate variable."))

        let buildFirstLikeFromRowset (rowsetSel: SqlSelect) (predExprOpt: Expression option) : SqlExpr =
            let filteredSel = buildSetOpFilteredRowset rowsetSel predExprOpt
            let rowAlias = nextAlias "_sf"
            let firstCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some rowAlias, "v") }]
                  Source = Some(DerivedTable(filteredSel, rowAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = [{ Expr = SqlExpr.Column(Some rowAlias, "__ord"); Direction = SortDirection.Asc }]
                  Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                  Offset = None }
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect firstCore }

        let buildElementAtFromRowset (rowsetSel: SqlSelect) (indexExpr: Expression) (orDefault: bool) : SqlExpr =
            let idx =
                match indexExpr with
                | :? ConstantExpression as ce -> SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value)))
                | _ -> visitDu indexExpr qb
            let rowAlias = nextAlias "_si"
            let indexedCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some rowAlias, "v") }]
                  Source = Some(DerivedTable(rowsetSel, rowAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = [{ Expr = SqlExpr.Column(Some rowAlias, "__ord"); Direction = SortDirection.Asc }]
                  Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                  Offset = Some idx }
            let indexedSel = { Ctes = []; Body = SingleSelect indexedCore }
            let outerAlias = nextAlias "_so"
            let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
            let firstValueExpr = SqlExpr.FunctionCall("MIN", [SqlExpr.Column(Some outerAlias, "v")])
            let noElementsExpr =
                SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Index was out of range. Must be non-negative and less than the size of the collection.")])
            let valueExpr =
                SqlExpr.CaseExpr(
                    (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)),
                     if orDefault then SqlExpr.Literal(SqlLiteral.Null) else noElementsExpr),
                    [],
                    Some firstValueExpr)
            let outerCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = valueExpr }]
                  Source = Some(DerivedTable(indexedSel, outerAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

        let buildCountFromRowset (rowsetSel: SqlSelect) : SqlExpr =
            let rowAlias = nextAlias "_sc"
            let countCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                  Source = Some(DerivedTable(rowsetSel, rowAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }

        let buildExistsFromRowset (rowsetSel: SqlSelect) (predExprOpt: Expression option) : SqlExpr =
            let filteredSel = buildSetOpFilteredRowset rowsetSel predExprOpt
            SqlExpr.Exists filteredSel

        let buildSetOpTerminalFromRowset (rowsetSel: SqlSelect) : SqlExpr voption =
            match desc.Terminal with
            | Terminal.Select _
            | Terminal.DistinctBy _ ->
                ValueSome(DBRefManyBuilderSetOps.buildEntitySequenceAggregate nextAlias rowsetSel)
            | Terminal.Exists -> ValueSome(buildExistsFromRowset rowsetSel None)
            | Terminal.Any pred -> ValueSome(buildExistsFromRowset rowsetSel pred)
            | Terminal.Count
            | Terminal.LongCount -> ValueSome(buildCountFromRowset rowsetSel)
            | Terminal.First pred
            | Terminal.FirstOrDefault pred -> ValueSome(buildFirstLikeFromRowset rowsetSel pred)
            | Terminal.ElementAt indexExpr -> ValueSome(buildElementAtFromRowset rowsetSel indexExpr false)
            | Terminal.ElementAtOrDefault indexExpr -> ValueSome(buildElementAtFromRowset rowsetSel indexExpr true)
            | _ -> ValueNone

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
        | Some (SetOperation.DistinctBy(keySel)) ->
            buildSetOpTerminalFromRowset (buildDistinctByEntityRowset qb desc ownerRef keySel)
        | Some (SetOperation.IntersectBy(rightKeys, keySel)) ->
            buildSetOpTerminalFromRowset (buildByFilterEntityRowset qb desc ownerRef keySel rightKeys false)
        | Some (SetOperation.ExceptBy(rightKeys, keySel)) ->
            buildSetOpTerminalFromRowset (buildByFilterEntityRowset qb desc ownerRef keySel rightKeys true)
        | Some (SetOperation.UnionBy(rightSource, keySel)) ->
            buildSetOpTerminalFromRowset (buildUnionByEntityRowset qb desc ownerRef rightSource keySel)
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
            | Terminal.Select _ ->
                let selectResult = buildSelect qb desc ownerRef
                // Post-Select DefaultIfEmpty wrapping for Select terminal (json_group_array).
                match desc.PostSelectDefaultIfEmpty with
                | Some defaultValueExprOpt ->
                    let defaultValueDu = computeDefaultValueForJsonArray defaultValueExprOpt projectedType
                    // CASE WHEN json_array_length(result) = 0 THEN json_array(default) ELSE result END
                    SqlExpr.CaseExpr(
                        (SqlExpr.Binary(SqlExpr.FunctionCall("json_array_length", [selectResult]), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)),
                         SqlExpr.FunctionCall("jsonb_array", [defaultValueDu])),
                        [], Some selectResult)
                | None -> selectResult
            | Terminal.Contains value -> buildContains qb desc ownerRef value
            | Terminal.First pred ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedElement qb desc ownerRef pred false
                | None -> buildEntityElement qb desc ownerRef pred false
            | Terminal.FirstOrDefault pred ->
                match desc.SelectProjection with
                | Some _ ->
                    let elemResult = buildProjectedElement qb desc ownerRef pred false
                    // Post-Select DefaultIfEmpty wrapping for FirstOrDefault.
                    // Use CASE WHEN NOT EXISTS (not COALESCE) to avoid conflating
                    // empty-relation with legitimate NULL first values.
                    match desc.PostSelectDefaultIfEmpty with
                    | Some defaultValueExprOpt ->
                        let defaultValueDu = computeDefaultValueDu defaultValueExprOpt projectedType
                        // Build NOT EXISTS check using the projected rowset.
                        let projRowset = buildProjectedRowset qb desc ownerRef
                        let existsCore =
                            match projRowset.Body with
                            | SingleSelect c -> { c with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
                            | UnionAllSelect(h, _) -> { h with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
                        let existsSel = { Ctes = []; Body = SingleSelect existsCore }
                        // CASE WHEN NOT EXISTS(projected) THEN default ELSE elemResult END
                        SqlExpr.CaseExpr(
                            (SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists existsSel), defaultValueDu),
                            [], Some elemResult)
                    | None -> elemResult
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
            // MinBy/MaxBy — ORDER BY key ASC/DESC + First element.
            | Terminal.MinBy keySel ->
                let desc = { desc with SortKeys = [(keySel, SortDirection.Asc)] }
                buildEntityElement qb desc ownerRef None false
            | Terminal.MaxBy keySel ->
                let desc = { desc with SortKeys = [(keySel, SortDirection.Desc)] }
                buildEntityElement qb desc ownerRef None false
            | Terminal.DistinctBy keySel -> DBRefManyBuilderSetOps.buildEntitySequenceAggregate nextAlias (buildDistinctByEntityRowset qb desc ownerRef keySel)
            | Terminal.CountBy _ -> failwith "CountBy is handled above; this branch is unreachable."

        ValueSome result
