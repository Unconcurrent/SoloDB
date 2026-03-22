namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.DBRefManyDescriptor

/// Specialized SQL builders for GroupBy and TakeWhile/SkipWhile terminals.
/// Separated from the main builder to keep file sizes under 400 lines.
module internal DBRefManyBuildSpecial =
    let private buildTakeWhileBase
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (tryGetRelationOrderByForTakeWhile: DBRefManyDescriptor.DBRefManyOwnerRef -> string -> string -> OrderBy list voption)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (twPredLambda: LambdaExpression)
        =
        let tgtAlias, lnkAlias, baseCore0, targetTable = buildCorrelatedCore qb desc ownerRef []
        let effectiveOrderBy =
            match baseCore0.OrderBy with
            | [] ->
                match tryGetRelationOrderByForTakeWhile ownerRef tgtAlias lnkAlias with
                | ValueSome orderBy -> orderBy
                | ValueNone ->
                    raise (InvalidOperationException(DBRefManyHelpers.takeWhileOrderingRequiredMessage))
            | orderBy -> orderBy
        let twSubQb = qb.ForSubquery(tgtAlias, twPredLambda, subqueryRootTable = targetTable)
        let twPredDu = visitDu twPredLambda.Body twSubQb
        let twJoins = DBRefManyHelpers.joinEdgesToClauses twSubQb.SourceContext.Joins
        let baseCore = { baseCore0 with Joins = baseCore0.Joins @ twJoins; OrderBy = effectiveOrderBy }
        let sortKeyDuPairs = effectiveOrderBy |> List.map (fun ob -> (ob.Expr, ob.Direction))
        let windowSpec = { Kind = NamedWindowFunction "SUM"; Arguments = [SqlExpr.Unary(UnaryOperator.Not, twPredDu)]; PartitionBy = []; OrderBy = sortKeyDuPairs }
        let cfExpr = SqlExpr.WindowCall windowSpec
        tgtAlias, targetTable, baseCore, cfExpr

    let private buildTakeWhileProjectedRowsetWithLambda
        (qb: QueryBuilder)
        (baseCore: SelectCore)
        (tgtAlias: string)
        (targetTable: string)
        (cfExpr: SqlExpr)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (desc: QueryDescriptor)
        (projLambda: LambdaExpression)
        (isTakeWhile: bool) : SqlSelect voption =
        let subQb2 = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
        let projDu = visitDu projLambda.Body subQb2
        let projJoins = DBRefManyHelpers.joinEdgesToClauses subQb2.SourceContext.Joins
        let orderBy =
            baseCore.OrderBy
            |> List.map (fun ob -> (ob.Expr, ob.Direction))
        let innerProjs2 =
            [ { Alias = Some "v"; Expr = projDu }
              { Alias = Some "_cf"; Expr = cfExpr }
              { Alias = Some "__ord"
                Expr =
                    SqlExpr.WindowCall({
                        Kind = WindowFunctionKind.RowNumber
                        Arguments = []
                        PartitionBy = []
                        OrderBy = orderBy }) } ]
        let innerCore2 = { baseCore with Projections = ProjectionSetOps.ofList innerProjs2; Joins = baseCore.Joins @ projJoins }
        let innerSel2 = { Ctes = []; Body = SingleSelect innerCore2 }
        let twAlias2 = nextAlias "_tw"
        let cfFilter2 = DBRefManyHelpers.buildTakeWhileCfFilter twAlias2 isTakeWhile
        let middleCore =
            { mkSubCore
                [ { Alias = Some "v"; Expr = SqlExpr.Column(Some twAlias2, "v") }
                  { Alias = Some "__ord"; Expr = SqlExpr.Column(Some twAlias2, "__ord") } ]
                (Some(DerivedTable(innerSel2, twAlias2)))
                (Some cfFilter2)
              with OrderBy = [{ Expr = SqlExpr.Column(Some twAlias2, "__ord"); Direction = SortDirection.Asc }] }
        let middleSel = { Ctes = []; Body = SingleSelect middleCore }
        if desc.Distinct then
            let distinctAlias = nextAlias "_twd"
            let distinctCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some distinctAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.AggregateCall(AggregateKind.Min, Some(SqlExpr.Column(Some distinctAlias, "__ord")), false, None) } ]
                  Source = Some(DerivedTable(middleSel, distinctAlias))
                  Joins = []
                  Where = None
                  GroupBy = [SqlExpr.Column(Some distinctAlias, "v")]
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let orderedAlias = nextAlias "_two"
            let orderedCore =
                { mkSubCore
                    [ { Alias = Some "v"; Expr = SqlExpr.Column(Some orderedAlias, "v") }
                      { Alias = Some "__ord"; Expr = SqlExpr.Column(Some orderedAlias, "__ord") } ]
                    (Some(DerivedTable({ Ctes = []; Body = SingleSelect distinctCore }, orderedAlias)))
                    None
                  with OrderBy = [{ Expr = SqlExpr.Column(Some orderedAlias, "__ord"); Direction = SortDirection.Asc }] }
            ValueSome { Ctes = []; Body = SingleSelect orderedCore }
        else
            ValueSome middleSel

    let private buildTakeWhileProjectedRowset
        (qb: QueryBuilder)
        (baseCore: SelectCore)
        (tgtAlias: string)
        (targetTable: string)
        (cfExpr: SqlExpr)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (desc: QueryDescriptor)
        (isTakeWhile: bool) : SqlSelect voption =
        match desc.SelectProjection with
        | Some projLambda -> buildTakeWhileProjectedRowsetWithLambda qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc projLambda isTakeWhile
        | None -> ValueNone

    let private buildTakeWhileEntityRowset
        (tgtAlias: string)
        (baseCore: SelectCore)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (cfExpr: SqlExpr)
        (isTakeWhile: bool) : SqlSelect =
        let innerProjs =
            [ { Alias = Some "Id"; Expr = SqlExpr.Column(Some tgtAlias, "Id") }
              { Alias = Some "Value"; Expr = SqlExpr.Column(Some tgtAlias, "Value") }
              { Alias = Some "_cf"; Expr = cfExpr }
              { Alias = Some "__ord"
                Expr =
                    SqlExpr.WindowCall({
                        Kind = WindowFunctionKind.RowNumber
                        Arguments = []
                        PartitionBy = []
                        OrderBy = baseCore.OrderBy |> List.map (fun ob -> (ob.Expr, ob.Direction)) }) } ]
        let innerCore = { baseCore with Projections = ProjectionSetOps.ofList innerProjs }
        let innerSel = { Ctes = []; Body = SingleSelect innerCore }
        let twAlias = nextAlias "_tw"
        let cfFilter = DBRefManyHelpers.buildTakeWhileCfFilter twAlias isTakeWhile
        let outerCore =
            mkSubCore
                [ { Alias = Some "Id"; Expr = SqlExpr.Column(Some twAlias, "Id") }
                  { Alias = Some "Value"; Expr = SqlExpr.Column(Some twAlias, "Value") }
                  { Alias = Some "__ord"; Expr = SqlExpr.Column(Some twAlias, "__ord") } ]
                (Some(DerivedTable(innerSel, twAlias)))
                (Some cfFilter)
        { Ctes = []; Body = SingleSelect outerCore }

    let private applyPostBoundWhileEntityRowset
        (qb: QueryBuilder)
        (nextAlias: string -> string)
        (targetTable: string)
        (rowsetSel: SqlSelect)
        (predLambda: LambdaExpression)
        (isTakeWhile: bool)
        : SqlSelect =
        let rowAlias = nextAlias "_pwb"
        let predQb =
            { qb.ForSubquery(rowAlias, predLambda, subqueryRootTable = targetTable) with
                JsonExtractSelfValue = false }
        let predDu = visitDu predLambda.Body predQb
        let predJoins = DBRefManyHelpers.joinEdgesToClauses predQb.SourceContext.Joins
        let cfExpr =
            SqlExpr.WindowCall({
                Kind = NamedWindowFunction "SUM"
                Arguments = [SqlExpr.Unary(UnaryOperator.Not, predDu)]
                PartitionBy = []
                OrderBy = [SqlExpr.Column(Some rowAlias, "__ord"), SortDirection.Asc] })
        let innerCore =
            { Distinct = false
              Projections =
                ProjectionSetOps.ofList [
                    { Alias = Some "Id"; Expr = SqlExpr.Column(Some rowAlias, "Id") }
                    { Alias = Some "Value"; Expr = SqlExpr.Column(Some rowAlias, "Value") }
                    { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rowAlias, "__ord") }
                    { Alias = Some "_cf"; Expr = cfExpr }
                ]
              Source = Some(DerivedTable(rowsetSel, rowAlias))
              Joins = predJoins
              Where = None
              GroupBy = []
              Having = None
              OrderBy = [{ Expr = SqlExpr.Column(Some rowAlias, "__ord"); Direction = SortDirection.Asc }]
              Limit = None
              Offset = None }
        let innerSel = { Ctes = []; Body = SingleSelect innerCore }
        let outerAlias = nextAlias "_pwo"
        let cfFilter = DBRefManyHelpers.buildTakeWhileCfFilter outerAlias isTakeWhile
        let outerCore =
            { Distinct = false
              Projections =
                ProjectionSetOps.ofList [
                    { Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                    { Alias = Some "Value"; Expr = SqlExpr.Column(Some outerAlias, "Value") }
                    { Alias = Some "__ord"; Expr = SqlExpr.Column(Some outerAlias, "__ord") }
                ]
              Source = Some(DerivedTable(innerSel, outerAlias))
              Joins = []
              Where = Some cfFilter
              GroupBy = []
              Having = None
              OrderBy = [{ Expr = SqlExpr.Column(Some outerAlias, "__ord"); Direction = SortDirection.Asc }]
              Limit = None
              Offset = None }
        { Ctes = []; Body = SingleSelect outerCore }

    let tryBuildTakeWhileProjectedRowset
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (tryGetRelationOrderByForTakeWhile: DBRefManyDescriptor.DBRefManyOwnerRef -> string -> string -> OrderBy list voption)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (twPredLambda: LambdaExpression)
        (isTakeWhile: bool) : SqlSelect voption =
        let tgtAlias, targetTable, baseCore, cfExpr =
            buildTakeWhileBase qb desc buildCorrelatedCore tryGetRelationOrderByForTakeWhile ownerRef twPredLambda
        buildTakeWhileProjectedRowset qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc isTakeWhile

    let tryBuildTakeWhileEntityRowset
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (tryGetRelationOrderByForTakeWhile: DBRefManyDescriptor.DBRefManyOwnerRef -> string -> string -> OrderBy list voption)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (twPredLambda: LambdaExpression)
        (isTakeWhile: bool) : SqlSelect =
        let tgtAlias, _, baseCore, cfExpr =
            buildTakeWhileBase qb desc buildCorrelatedCore tryGetRelationOrderByForTakeWhile ownerRef twPredLambda
        buildTakeWhileEntityRowset tgtAlias baseCore mkSubCore nextAlias cfExpr isTakeWhile

    let private buildAggregateOverRowset
        (insideJsonObjectProjection: bool)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (rowsetSel: SqlSelect)
        (aggKind: AggregateKind) : SqlExpr =

        let dtAlias = nextAlias "_twa"
        let aggExpr = SqlExpr.AggregateCall(aggKind, Some(SqlExpr.Column(Some dtAlias, "v")), false, None)
        let aggCore = mkSubCore [{ Alias = None; Expr = aggExpr }] (Some(DerivedTable(rowsetSel, dtAlias))) None
        let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect aggCore }
        DBRefManyHelpers.wrapAggregateEmptySemantics aggKind insideJsonObjectProjection scalarExpr

    /// Build GroupBy terminal SQL from a descriptor.
    let tryBuildGroupBy
        (qb: QueryBuilder) (desc: QueryDescriptor)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (tryGetRelationOrderByForTakeWhile: DBRefManyDescriptor.DBRefManyOwnerRef -> string -> string -> OrderBy list voption)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (keyLambda: LambdaExpression) : SqlExpr voption =

        let hasBoundary = desc.Limit.IsSome || desc.Offset.IsSome
        let groupDesc =
            match desc.TakeWhileInfo with
            | Some _ -> { desc with Limit = None; Offset = None }
            | None when hasBoundary -> desc  // Preserve Limit/Offset/SortKeys for bounded DerivedTable wrapping
            | None -> { desc with SortKeys = [] }
        let tgtAlias, baseCore, targetTable =
            match desc.TakeWhileInfo with
            | Some (twPredLambda, isTakeWhile) ->
                let rowsetSel = tryBuildTakeWhileEntityRowset qb groupDesc buildCorrelatedCore mkSubCore nextAlias tryGetRelationOrderByForTakeWhile ownerRef twPredLambda isTakeWhile
                let rowsetSel =
                    match desc.PostBoundTakeWhileInfo with
                    | Some (postPred, postIsTakeWhile) ->
                        let _, _, _, targetTable =
                            buildCorrelatedCore qb groupDesc ownerRef [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                        applyPostBoundWhileEntityRowset qb nextAlias targetTable rowsetSel postPred postIsTakeWhile
                    | None -> rowsetSel
                let rowAlias = nextAlias "_twg"
                let _, _, _, targetTable =
                    buildCorrelatedCore qb groupDesc ownerRef [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                rowAlias,
                (mkSubCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some(DerivedTable(rowsetSel, rowAlias))) None),
                targetTable
            | None when hasBoundary ->
                // Bounded chain (OrderBy+Take): build correlated core with boundaries,
                // wrap in DerivedTable so GROUP BY operates on the bounded set.
                let tgtAlias, _, boundedCore, targetTable =
                    buildCorrelatedCore qb groupDesc ownerRef [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                let boundedSel = { Ctes = []; Body = SingleSelect { boundedCore with Projections = AllColumns } }
                let dtAlias = nextAlias "_gbbd"
                dtAlias,
                (mkSubCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some(DerivedTable(boundedSel, dtAlias))) None),
                targetTable
            | None ->
                let tgtAlias, _, baseCore, targetTable =
                    buildCorrelatedCore qb groupDesc ownerRef [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                tgtAlias, baseCore, targetTable
        let subQbKey = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
        let groupKeyDu = visitDu keyLambda.Body subQbKey
        let keyJoins = DBRefManyHelpers.joinEdgesToClauses subQbKey.SourceContext.Joins
        let havingJoinEdges = ResizeArray<JoinEdge>()
        let havingOpt =
            match desc.GroupByHavingPredicate with
            | Some havingPredExpr ->
                match tryExtractLambdaExpression havingPredExpr with
                | ValueSome havingLambda ->
                    let groupParam = havingLambda.Parameters.[0]
                    Some (DBRefManyHelpers.translateGroupingPredicate qb tgtAlias targetTable groupKeyDu groupParam havingLambda.Body havingJoinEdges)
                | ValueNone -> None
            | None -> None
        let havingJoins = DBRefManyHelpers.joinEdgesToClauses havingJoinEdges
        let baseCore = { baseCore with Joins = baseCore.Joins @ keyJoins @ havingJoins }
        let projectedGroupRowset () =
            match desc.SelectProjection with
            | Some projLambda ->
                let groupParam = projLambda.Parameters.[0]
                let projJoinEdges = ResizeArray<JoinEdge>()
                let projDu = DBRefManyHelpers.translateGroupingPredicate qb tgtAlias targetTable groupKeyDu groupParam projLambda.Body projJoinEdges
                let projJoins = DBRefManyHelpers.joinEdgesToClauses projJoinEdges
                let gbCore =
                    { baseCore with
                        Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = projDu }]
                        Joins = baseCore.Joins @ projJoins
                        GroupBy = [groupKeyDu]
                        Having = havingOpt }
                Some { Ctes = []; Body = SingleSelect gbCore }
            | None ->
                None
        match desc.Terminal with
        | Terminal.Count | Terminal.LongCount ->
            let gbSel =
                match projectedGroupRowset () with
                | Some sel -> sel
                | None ->
                    let gbCore = { baseCore with GroupBy = [groupKeyDu]; Having = havingOpt }
                    { Ctes = []; Body = SingleSelect gbCore }
            let gbAlias = nextAlias "_gb"
            let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
            let outerCore = mkSubCore countProj (Some(DerivedTable(gbSel, gbAlias))) None
            ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore })
        | Terminal.Exists ->
            let gbCore = { baseCore with GroupBy = [groupKeyDu]; Having = havingOpt }
            ValueSome(SqlExpr.Exists { Ctes = []; Body = SingleSelect gbCore })
        | Terminal.All _ ->
            match havingOpt with
            | Some havingDu ->
                let negatedHaving = SqlExpr.Unary(UnaryOperator.Not, havingDu)
                let gbCore = { baseCore with GroupBy = [groupKeyDu]; Having = Some negatedHaving }
                ValueSome(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists { Ctes = []; Body = SingleSelect gbCore }))
            | None ->
                ValueSome(SqlExpr.Literal(SqlLiteral.Integer 1L))
        | Terminal.Select _ ->
            match projectedGroupRowset () with
            | Some gbSel ->
                let gbAlias = nextAlias "_gbs"
                let outerGA = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Column(Some gbAlias, "v")])])
                let outerCore = mkSubCore [{ Alias = None; Expr = outerGA }] (Some(DerivedTable(gbSel, gbAlias))) None
                ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore })
            | None ->
                ValueNone
        | _ -> ValueNone

    /// Build CountBy terminal SQL from a descriptor.
    /// Emits: SELECT json_group_array(json_object('Key', _gk, 'Value', _gc)) FROM (
    ///          SELECT <key_expr> AS _gk, COUNT(*) AS _gc FROM ... GROUP BY _gk
    ///        )
    let tryBuildCountBy
        (qb: QueryBuilder) (desc: QueryDescriptor)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (keySelectorExpr: Expression) : SqlExpr voption =

        let keyLambda =
            match tryExtractLambdaExpression keySelectorExpr with
            | ValueSome lambda -> lambda
            | ValueNone ->
                raise (NotSupportedException(
                    "Error: Cannot translate CountBy key selector.\n" +
                    "Reason: The key selector is not a translatable lambda expression.\n" +
                    "Fix: Pass the key selector as an inline lambda."))

        let tgtAlias, _, baseCore, targetTable =
            buildCorrelatedCore qb desc ownerRef
                [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]

        // If the descriptor has ORDER BY + LIMIT/OFFSET (bounded chain like OrderBy().Take(n)),
        // wrap the bounded core in a DerivedTable first so GROUP BY operates on the bounded set.
        let groupSourceAlias, groupSourceTable =
            if baseCore.Limit.IsSome || baseCore.Offset.IsSome then
                let boundedSel = { Ctes = []; Body = SingleSelect { baseCore with Projections = AllColumns } }
                let dtAlias = nextAlias "_cbd"
                dtAlias, targetTable
            else
                tgtAlias, targetTable

        let effectiveSource, effectiveJoins =
            if baseCore.Limit.IsSome || baseCore.Offset.IsSome then
                let boundedSel = { Ctes = []; Body = SingleSelect { baseCore with Projections = AllColumns } }
                let dtAlias = groupSourceAlias
                Some(DerivedTable(boundedSel, dtAlias)), []
            else
                baseCore.Source, baseCore.Joins

        let subQbKey = qb.ForSubquery(groupSourceAlias, keyLambda, subqueryRootTable = groupSourceTable)
        let groupKeyDu = visitDu keyLambda.Body subQbKey
        let keyJoins = DBRefManyHelpers.joinEdgesToClauses subQbKey.SourceContext.Joins

        // Inner: SELECT <key> AS _gk, COUNT(*) AS _gc FROM (bounded set) GROUP BY <key>
        let innerCore =
            if baseCore.Limit.IsSome || baseCore.Offset.IsSome then
                { mkSubCore
                    [{ Alias = Some "_gk"; Expr = groupKeyDu }
                     { Alias = Some "_gc"; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                    effectiveSource
                    None
                  with
                    Joins = keyJoins
                    GroupBy = [groupKeyDu] }
            else
                { baseCore with
                    Projections = ProjectionSetOps.ofList [
                        { Alias = Some "_gk"; Expr = groupKeyDu }
                        { Alias = Some "_gc"; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }
                    ]
                    Joins = baseCore.Joins @ keyJoins
                    GroupBy = [groupKeyDu] }
        let innerSel = { Ctes = []; Body = SingleSelect innerCore }

        // Outer: SELECT json_group_array(json_object('Key', _gk, 'Value', _gc))
        let gbAlias = nextAlias "_cb"
        let kvpObj = SqlExpr.FunctionCall("json_object", [
            SqlExpr.Literal(SqlLiteral.String "Key")
            SqlExpr.Column(Some gbAlias, "_gk")
            SqlExpr.Literal(SqlLiteral.String "Value")
            SqlExpr.Column(Some gbAlias, "_gc")
        ])
        let outerGA = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.FunctionCall("jsonb", [kvpObj])])
        let outerCore = mkSubCore [{ Alias = None; Expr = outerGA }] (Some(DerivedTable(innerSel, gbAlias))) None
        ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore })

    /// Build TakeWhile/SkipWhile terminal SQL from a descriptor.
    let tryBuildTakeWhile
        (qb: QueryBuilder) (desc: QueryDescriptor)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (tryGetRelationOrderByForTakeWhile: DBRefManyDescriptor.DBRefManyOwnerRef -> string -> string -> OrderBy list voption)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (twPredLambda: LambdaExpression) (isTakeWhile: bool) : SqlExpr voption =

        let tgtAlias, targetTable, baseCore, cfExpr =
            buildTakeWhileBase qb desc buildCorrelatedCore tryGetRelationOrderByForTakeWhile ownerRef twPredLambda
        let outerSel =
            let entitySel = buildTakeWhileEntityRowset tgtAlias baseCore mkSubCore nextAlias cfExpr isTakeWhile
            let entitySel =
                match desc.PostBoundTakeWhileInfo with
                | Some (postPred, postIsTakeWhile) ->
                    applyPostBoundWhileEntityRowset qb nextAlias targetTable entitySel postPred postIsTakeWhile
                | None -> entitySel
            let twAlias = nextAlias "_twc"
            let outerCore = mkSubCore [{ Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some(DerivedTable(entitySel, twAlias))) None
            { Ctes = []; Body = SingleSelect outerCore }

        match desc.Terminal with
        | Terminal.Count | Terminal.LongCount ->
            match buildTakeWhileProjectedRowset qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc isTakeWhile with
            | ValueSome projectedSel ->
                let dtAlias = nextAlias "_twc"
                let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                let countCore = mkSubCore countProj (Some(DerivedTable(projectedSel, dtAlias))) None
                ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore })
            | ValueNone ->
                let dtAlias = nextAlias "_twc"
                let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                let countCore = mkSubCore countProj (Some(DerivedTable(outerSel, dtAlias))) None
                ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore })
        | Terminal.Exists | Terminal.Any _ ->
            // When GroupBy is present with TakeWhile, apply GROUP BY + HAVING on the bounded set.
            match desc.GroupByKey with
            | Some keyLambda when desc.GroupByHavingPredicate.IsSome ->
                // Rebuild the TakeWhile-bounded set with the group key projected.
                let subQbKey = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
                let groupKeyDu = visitDu keyLambda.Body subQbKey
                let keyJoins = DBRefManyHelpers.joinEdgesToClauses subQbKey.SourceContext.Joins
                let innerWithKey =
                    { baseCore with
                        Projections = ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }
                            { Alias = Some "_cf"; Expr = cfExpr }
                            { Alias = Some "_gk"; Expr = groupKeyDu }
                        ]
                        Joins = baseCore.Joins @ keyJoins }
                let innerSelWithKey = { Ctes = []; Body = SingleSelect innerWithKey }
                let twAlias2 = nextAlias "_twg"
                let cfFilter2 = DBRefManyHelpers.buildTakeWhileCfFilter twAlias2 isTakeWhile
                // Outer: GROUP BY _gk HAVING predicate
                let havingDu =
                    match desc.GroupByHavingPredicate with
                    | Some havingPredExpr ->
                        match tryExtractLambdaExpression havingPredExpr with
                        | ValueSome havingLambda ->
                            let groupParam = havingLambda.Parameters.[0]
                            Some (DBRefManyHelpers.translateGroupingPredicate qb tgtAlias targetTable (SqlExpr.Column(Some twAlias2, "_gk")) groupParam havingLambda.Body (ResizeArray<JoinEdge>()))
                        | ValueNone -> None
                    | None -> None
                let gbCore =
                    { mkSubCore [{ Alias = Some "v"; Expr = SqlExpr.Column(Some twAlias2, "v") }] (Some(DerivedTable(innerSelWithKey, twAlias2))) (Some cfFilter2)
                      with GroupBy = [SqlExpr.Column(Some twAlias2, "_gk")]
                           Having = havingDu }
                ValueSome(SqlExpr.Exists { Ctes = []; Body = SingleSelect gbCore })
            | _ ->
                match desc.Terminal with
                | Terminal.Any(Some predExpr) ->
                    match tryExtractLambdaExpression predExpr with
                    | ValueSome predLambda ->
                        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
                        let p = Expression.Parameter(targetType, "x")
                        let identityLambda = Expression.Lambda(p, [| p |])
                        match buildTakeWhileProjectedRowsetWithLambda qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc identityLambda isTakeWhile with
                        | ValueSome rowsetSel ->
                            let rowAlias = nextAlias "_twp"
                            let valueCore =
                                { Distinct = false
                                  Projections = ProjectionSetOps.ofList [{ Alias = Some "Value"; Expr = SqlExpr.Column(Some rowAlias, "v") }]
                                  Source = Some(DerivedTable(rowsetSel, rowAlias))
                                  Joins = []
                                  Where = None
                                  GroupBy = []
                                  Having = None
                                  OrderBy = []
                                  Limit = None
                                  Offset = None }
                            let valueSel = { Ctes = []; Body = SingleSelect valueCore }
                            let predAlias = nextAlias "_tpp"
                            let predQb =
                                { qb.ForSubquery(predAlias, predLambda, subqueryRootTable = targetTable) with
                                    JsonExtractSelfValue = false }
                            let predDu = visitDu predLambda.Body predQb
                            let predJoins = DBRefManyHelpers.joinEdgesToClauses predQb.SourceContext.Joins
                            let existsCore =
                                { Distinct = false
                                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                                  Source = Some(DerivedTable(valueSel, predAlias))
                                  Joins = predJoins
                                  Where = Some predDu
                                  GroupBy = []
                                  Having = None
                                  OrderBy = []
                                  Limit = None
                                  Offset = None }
                            ValueSome(SqlExpr.Exists { Ctes = []; Body = SingleSelect existsCore })
                        | ValueNone ->
                            ValueSome(SqlExpr.Exists outerSel)
                    | ValueNone ->
                        ValueSome(SqlExpr.Exists outerSel)
                | _ ->
                    ValueSome(SqlExpr.Exists outerSel)
        | Terminal.Select _ ->
            match buildTakeWhileProjectedRowset qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc isTakeWhile with
            | ValueSome middleSel ->
                let midAlias = nextAlias "_twm"
                let outerGA = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.Column(Some midAlias, "v")])
                let outerCore2 = mkSubCore [{ Alias = None; Expr = outerGA }] (Some(DerivedTable(middleSel, midAlias))) None
                ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore2 })
            | ValueNone ->
                ValueNone
        | Terminal.SumProjected ->
            match buildTakeWhileProjectedRowset qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc isTakeWhile with
            | ValueSome rowsetSel -> ValueSome(buildAggregateOverRowset qb.InsideJsonObjectProjection mkSubCore nextAlias rowsetSel AggregateKind.Sum)
            | ValueNone -> ValueNone
        | Terminal.Sum selectorExpr ->
            match tryExtractLambdaExpression selectorExpr with
            | ValueSome selectorLambda ->
                match buildTakeWhileProjectedRowsetWithLambda qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc selectorLambda isTakeWhile with
                | ValueSome rowsetSel -> ValueSome(buildAggregateOverRowset qb.InsideJsonObjectProjection mkSubCore nextAlias rowsetSel AggregateKind.Sum)
                | ValueNone -> ValueNone
            | ValueNone -> ValueNone
        | Terminal.MinProjected ->
            match buildTakeWhileProjectedRowset qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc isTakeWhile with
            | ValueSome rowsetSel -> ValueSome(buildAggregateOverRowset qb.InsideJsonObjectProjection mkSubCore nextAlias rowsetSel AggregateKind.Min)
            | ValueNone -> ValueNone
        | Terminal.Min selectorExpr ->
            match tryExtractLambdaExpression selectorExpr with
            | ValueSome selectorLambda ->
                match buildTakeWhileProjectedRowsetWithLambda qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc selectorLambda isTakeWhile with
                | ValueSome rowsetSel -> ValueSome(buildAggregateOverRowset qb.InsideJsonObjectProjection mkSubCore nextAlias rowsetSel AggregateKind.Min)
                | ValueNone -> ValueNone
            | ValueNone -> ValueNone
        | Terminal.MaxProjected ->
            match buildTakeWhileProjectedRowset qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc isTakeWhile with
            | ValueSome rowsetSel -> ValueSome(buildAggregateOverRowset qb.InsideJsonObjectProjection mkSubCore nextAlias rowsetSel AggregateKind.Max)
            | ValueNone -> ValueNone
        | Terminal.Max selectorExpr ->
            match tryExtractLambdaExpression selectorExpr with
            | ValueSome selectorLambda ->
                match buildTakeWhileProjectedRowsetWithLambda qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc selectorLambda isTakeWhile with
                | ValueSome rowsetSel -> ValueSome(buildAggregateOverRowset qb.InsideJsonObjectProjection mkSubCore nextAlias rowsetSel AggregateKind.Max)
                | ValueNone -> ValueNone
            | ValueNone -> ValueNone
        | Terminal.AverageProjected ->
            match buildTakeWhileProjectedRowset qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc isTakeWhile with
            | ValueSome rowsetSel -> ValueSome(buildAggregateOverRowset qb.InsideJsonObjectProjection mkSubCore nextAlias rowsetSel AggregateKind.Avg)
            | ValueNone -> ValueNone
        | Terminal.Average selectorExpr ->
            match tryExtractLambdaExpression selectorExpr with
            | ValueSome selectorLambda ->
                match buildTakeWhileProjectedRowsetWithLambda qb baseCore tgtAlias targetTable cfExpr mkSubCore nextAlias desc selectorLambda isTakeWhile with
                | ValueSome rowsetSel -> ValueSome(buildAggregateOverRowset qb.InsideJsonObjectProjection mkSubCore nextAlias rowsetSel AggregateKind.Avg)
                | ValueNone -> ValueNone
            | ValueNone -> ValueNone
        | _ -> ValueNone
