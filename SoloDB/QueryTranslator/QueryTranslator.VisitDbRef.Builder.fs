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
open SoloDatabase.DBRefManyBuilderCore

/// Builds SQL DU trees from a unified DBRefManyQueryDescriptor.
/// Replaces the order-dependent per-handler peeling in VisitDbRef.
module internal DBRefManyBuilder =
    let private nestedDbRefManyNotSupportedMessage =
        QueryTranslatorVisitDbRefPeelers.nestedDbRefManyNotSupportedMessage
    type DBRefManyOwnerRef = DBRefManyDescriptor.DBRefManyOwnerRef
    let private joinEdgesToClauses = DBRefManyHelpers.joinEdgesToClauses

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
            match desc.TakeWhileInfo with
            | Some (twPredLambda, isTakeWhile) ->
                match DBRefManyBuildSpecial.tryBuildTakeWhileProjectedRowset qb desc buildCorrelatedCore mkSubCore nextAlias tryGetRelationOrderByForTakeWhile ownerRef twPredLambda isTakeWhile with
                | ValueSome sel -> sel
                | ValueNone -> DBRefManyBuilderTerminals.buildProjectedRowset buildCorrelatedCore nextAlias visitDu joinEdgesToClauses qb desc ownerRef
            | None ->
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
                    let hasOrd =
                        mainCore.Projections
                        |> ProjectionSetOps.toList
                        |> List.exists (fun p -> p.Alias = Some "__ord")
                    let columnNames = if hasOrd then [ "v"; "__ord" ] else [ "v" ]
                    let mainCore =
                        normalizeUnionArm
                            mkSubCore
                            nextAlias
                            columnNames
                            mainCore
                    let existsCore = { mainCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
                    let existsSel = { Ctes = []; Body = SingleSelect existsCore }
                    let defaultProjs =
                        if hasOrd then
                            [{ Alias = Some "v"; Expr = defaultValueDu }
                             { Alias = Some "__ord"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }]
                        else
                            [{ Alias = Some "v"; Expr = defaultValueDu }]
                    let defaultCore =
                        { mkSubCore defaultProjs None (Some(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists existsSel))) with
                            Joins = []; OrderBy = []; Limit = None; Offset = None }
                    { baseSel with Body = UnionAllSelect(mainCore, [defaultCore]) }
                | UnionAllSelect _ -> baseSel // Already union — skip
            | None -> baseSel
        let buildProjectedAggregate qb desc ownerRef aggKind =
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
        let buildProjectedRowsetElement qb desc ownerRef pred pickLast =
            let desc =
                match pred with
                | Some pred -> { desc with WherePredicates = desc.WherePredicates @ [pred] }
                | None -> desc
            DBRefManyBuilderElements.buildOrderedRowsetElement nextAlias (buildProjectedRowset qb desc ownerRef) pickLast
        let buildSingleLike =
            DBRefManyBuilderElements.buildSingleLike nextAlias buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses
        let buildEntityElementAt = DBRefManyBuilderElements.buildEntityElementAt buildCorrelatedCore nextAlias visitDu
        let buildProjectedElementAt qb desc ownerRef indexExpr orDefault =
            DBRefManyBuilderElements.buildProjectedElementAt nextAlias visitDu qb (buildProjectedRowset qb desc ownerRef) indexExpr orDefault
        let buildProjectedSingleLike qb desc ownerRef pred orDefault =
            DBRefManyBuilderElements.buildSingleLikeFromRowset nextAlias tryExtractLambdaExpression visitDu joinEdgesToClauses qb (buildProjectedRowset qb desc ownerRef) pred orDefault
        let buildDistinctByEntityRowset =
            DBRefManyBuilderSetOps.buildDistinctByEntityRowset buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias (DBRefManyBuilderElements.buildEntityValueExpr desc.CastTypeName)
        let buildByFilterEntityRowset =
            DBRefManyBuilderSetOps.buildByFilterEntityRowset buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias nullSafeEq (DBRefManyBuilderElements.buildEntityValueExpr desc.CastTypeName) isFullyConstant (fun expr -> evaluateExpr<IEnumerable> expr)
        let buildUnionByEntityRowset =
            DBRefManyBuilderSetOps.buildUnionByEntityRowset buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias (DBRefManyBuilderElements.buildEntityValueExpr desc.CastTypeName)
        let buildSetOpTerminalFromRowset rowsetSel =
            DBRefManyBuilderSetOpTerminals.buildSetOpTerminalFromRowset nextAlias visitDu joinEdgesToClauses tryExtractLambdaExpression qb desc rowsetSel

        // TakeWhile/SkipWhile — delegated to BuildSpecial.
        match desc.TakeWhileInfo with
        | Some (twPredLambda, isTakeWhile) when desc.SelectProjection.IsNone && desc.GroupByKey.IsNone ->
            DBRefManyBuildSpecial.tryBuildTakeWhile qb desc buildCorrelatedCore mkSubCore nextAlias tryGetRelationOrderByForTakeWhile ownerRef twPredLambda isTakeWhile
        | _ ->

        // CountBy — delegated to BuildSpecial.
        match desc.Terminal with
        | Terminal.CountBy keySel ->
            DBRefManyBuildSpecial.tryBuildCountBy qb desc buildCorrelatedCore mkSubCore nextAlias ownerRef keySel
        | _ ->

        // GroupBy terminals — delegated to BuildSpecial.
        match desc.GroupByKey with
        | Some keyLambda ->
            DBRefManyBuildSpecial.tryBuildGroupBy qb desc buildCorrelatedCore mkSubCore nextAlias tryGetRelationOrderByForTakeWhile ownerRef keyLambda
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
                | Some _ when desc.TakeWhileInfo.IsSome -> buildProjectedRowsetElement qb desc ownerRef pred false
                | Some _ -> buildProjectedElement qb desc ownerRef pred false
                | None -> buildEntityElement qb desc ownerRef pred false
            | Terminal.FirstOrDefault pred ->
                match desc.SelectProjection with
                | Some _ ->
                    let elemResult =
                        if desc.TakeWhileInfo.IsSome then
                            buildProjectedRowsetElement qb desc ownerRef pred false
                        else
                            buildProjectedElement qb desc ownerRef pred false
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
                | Some _ when desc.TakeWhileInfo.IsSome -> buildProjectedRowsetElement qb desc ownerRef pred true
                | Some _ -> buildProjectedElement qb desc ownerRef pred true
                | None -> buildEntityElement qb desc ownerRef pred true
            | Terminal.LastOrDefault pred ->
                match desc.SelectProjection with
                | Some _ when desc.TakeWhileInfo.IsSome -> buildProjectedRowsetElement qb desc ownerRef pred true
                | Some _ -> buildProjectedElement qb desc ownerRef pred true
                | None -> buildEntityElement qb desc ownerRef pred true
            | Terminal.Single pred ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedSingleLike qb desc ownerRef pred false
                | None -> buildSingleLike qb desc ownerRef pred false
            | Terminal.SingleOrDefault pred ->
                match desc.SelectProjection with
                | Some _ -> buildProjectedSingleLike qb desc ownerRef pred true
                | None -> buildSingleLike qb desc ownerRef pred true
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
