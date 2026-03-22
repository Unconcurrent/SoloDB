namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

module internal DBRefManyBuilderSetOpTerminals =
    let private buildSetOpProjectedRowset
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (rowsetSel: SqlSelect)
        : SqlSelect =
        match desc.SelectProjection with
        | None -> rowsetSel
        | Some projLambda ->
            let rowAlias = nextAlias "_sv"
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
            let projAlias = nextAlias "_sq"
            let projQb =
                { qb.ForSubquery(projAlias, projLambda) with
                    JsonExtractSelfValue = false }
            let projectedDu = visitDu projLambda.Body projQb
            let projJoins = joinEdgesToClauses projQb.SourceContext.Joins
            let projectedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = projectedDu }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some projAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(valueSel, projAlias))
                  Joins = projJoins
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = [{ Expr = SqlExpr.Column(Some projAlias, "__ord"); Direction = SortDirection.Asc }]
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect projectedCore }

    let private buildSetOpFilteredRowset
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (qb: QueryBuilder)
        (rowsetSel: SqlSelect)
        (predExprOpt: Expression option)
        : SqlSelect =
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

    let private buildFirstLikeFromRowset (nextAlias: string -> string) (rowsetSel: SqlSelect) (orDefault: bool) : SqlExpr =
        let rowAlias = nextAlias "_sf"
        let firstCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some rowAlias, "v") }]
              Source = Some(DerivedTable(rowsetSel, rowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = [{ Expr = SqlExpr.Column(Some rowAlias, "__ord"); Direction = SortDirection.Asc }]
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }
        let firstSel = { Ctes = []; Body = SingleSelect firstCore }
        let outerAlias = nextAlias "_so"
        let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
        let firstValueExpr = SqlExpr.FunctionCall("MIN", [SqlExpr.Column(Some outerAlias, "v")])
        let noElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains no elements")])
        let valueExpr =
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)),
                 if orDefault then SqlExpr.Literal(SqlLiteral.Null) else noElementsExpr),
                [],
                Some firstValueExpr)
        let outerCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = valueExpr }]
              Source = Some(DerivedTable(firstSel, outerAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let private buildElementAtFromRowset
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (qb: QueryBuilder)
        (rowsetSel: SqlSelect)
        (indexExpr: Expression)
        (orDefault: bool)
        : SqlExpr =
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

    let private buildCountFromRowset (nextAlias: string -> string) (rowsetSel: SqlSelect) : SqlExpr =
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

    let private buildExistsFromRowset (rowsetSel: SqlSelect) : SqlExpr =
        SqlExpr.Exists rowsetSel

    let private buildProjectedAggregate (nextAlias: string -> string) (qb: QueryBuilder) (rowsetSel: SqlSelect) (aggKind: AggregateKind) : SqlExpr =
        let rowAlias = nextAlias "_sa"
        let aggExpr = SqlExpr.AggregateCall(aggKind, Some(SqlExpr.Column(Some rowAlias, "v")), false, None)
        let aggCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = aggExpr }]
              Source = Some(DerivedTable(rowsetSel, rowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect aggCore }
        DBRefManyHelpers.wrapAggregateEmptySemantics aggKind qb.InsideJsonObjectProjection scalarExpr

    let buildSetOpTerminalFromRowset
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (rowsetSel: SqlSelect)
        : SqlExpr voption =
        let rowsetSel = buildSetOpProjectedRowset nextAlias visitDu joinEdgesToClauses qb desc rowsetSel
        match desc.Terminal with
        | Terminal.Select _
        | Terminal.DistinctBy _ ->
            ValueSome(DBRefManyBuilderSetOps.buildEntitySequenceAggregate nextAlias rowsetSel)
        | Terminal.Exists -> ValueSome(buildExistsFromRowset rowsetSel)
        | Terminal.Any pred ->
            let filteredSel = buildSetOpFilteredRowset nextAlias visitDu joinEdgesToClauses tryExtractLambdaExpression qb rowsetSel pred
            ValueSome(buildExistsFromRowset filteredSel)
        | Terminal.Count
        | Terminal.LongCount -> ValueSome(buildCountFromRowset nextAlias rowsetSel)
        | Terminal.First pred ->
            let filteredSel = buildSetOpFilteredRowset nextAlias visitDu joinEdgesToClauses tryExtractLambdaExpression qb rowsetSel pred
            ValueSome(buildFirstLikeFromRowset nextAlias filteredSel false)
        | Terminal.FirstOrDefault pred ->
            let filteredSel = buildSetOpFilteredRowset nextAlias visitDu joinEdgesToClauses tryExtractLambdaExpression qb rowsetSel pred
            ValueSome(buildFirstLikeFromRowset nextAlias filteredSel true)
        | Terminal.ElementAt indexExpr -> ValueSome(buildElementAtFromRowset nextAlias visitDu qb rowsetSel indexExpr false)
        | Terminal.ElementAtOrDefault indexExpr -> ValueSome(buildElementAtFromRowset nextAlias visitDu qb rowsetSel indexExpr true)
        | Terminal.SumProjected -> ValueSome(buildProjectedAggregate nextAlias qb rowsetSel AggregateKind.Sum)
        | Terminal.MinProjected -> ValueSome(buildProjectedAggregate nextAlias qb rowsetSel AggregateKind.Min)
        | Terminal.MaxProjected -> ValueSome(buildProjectedAggregate nextAlias qb rowsetSel AggregateKind.Max)
        | Terminal.AverageProjected -> ValueSome(buildProjectedAggregate nextAlias qb rowsetSel AggregateKind.Avg)
        // Terminals not handled by set-op path — fall through to main builder.
        | Terminal.All _ | Terminal.Sum _
        | Terminal.Min _ | Terminal.Max _
        | Terminal.Average _ | Terminal.Contains _
        | Terminal.Last _ | Terminal.LastOrDefault _
        | Terminal.Single _ | Terminal.SingleOrDefault _
        | Terminal.MinBy _ | Terminal.MaxBy _ | Terminal.CountBy _ -> ValueNone
