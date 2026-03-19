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

    /// Build GroupBy terminal SQL from a descriptor.
    let tryBuildGroupBy
        (qb: QueryBuilder) (desc: QueryDescriptor)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (keyLambda: LambdaExpression) : SqlExpr voption =

        let tgtAlias, _, baseCore, targetTable =
            buildCorrelatedCore qb { desc with Limit = None; Offset = None; SortKeys = [] } ownerRef
                [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let subQbKey = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
        let groupKeyDu = visitDu keyLambda.Body subQbKey
        let keyJoins = DBRefManyHelpers.joinEdgesToClauses subQbKey.SourceContext.Joins

        let baseCore = { baseCore with Joins = baseCore.Joins @ keyJoins }
        match desc.Terminal with
        | Terminal.Count | Terminal.LongCount ->
            let gbCore = { baseCore with GroupBy = [groupKeyDu] }
            let gbSel = { Ctes = []; Body = SingleSelect gbCore }
            let gbAlias = nextAlias "_gb"
            let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
            let outerCore = mkSubCore countProj (Some(DerivedTable(gbSel, gbAlias))) None
            ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore })
        | Terminal.Exists ->
            match desc.GroupByHavingPredicate with
            | Some havingPredExpr ->
                match tryExtractLambdaExpression havingPredExpr with
                | ValueSome havingLambda ->
                    let groupParam = havingLambda.Parameters.[0]
                    let havingDu = DBRefManyHelpers.translateGroupingPredicate qb tgtAlias groupKeyDu groupParam havingLambda.Body
                    let gbCore = { baseCore with GroupBy = [groupKeyDu]; Having = Some havingDu }
                    ValueSome(SqlExpr.Exists { Ctes = []; Body = SingleSelect gbCore })
                | ValueNone -> ValueNone
            | None -> ValueNone
        | Terminal.All _ ->
            match desc.GroupByHavingPredicate with
            | Some havingPredExpr ->
                match tryExtractLambdaExpression havingPredExpr with
                | ValueSome havingLambda ->
                    let groupParam = havingLambda.Parameters.[0]
                    let havingDu = DBRefManyHelpers.translateGroupingPredicate qb tgtAlias groupKeyDu groupParam havingLambda.Body
                    let negatedHaving = SqlExpr.Unary(UnaryOperator.Not, havingDu)
                    let gbCore = { baseCore with GroupBy = [groupKeyDu]; Having = Some negatedHaving }
                    ValueSome(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists { Ctes = []; Body = SingleSelect gbCore }))
                | ValueNone -> ValueNone
            | None ->
                ValueSome(SqlExpr.Literal(SqlLiteral.Integer 1L))
        | _ -> ValueNone

    /// Build TakeWhile/SkipWhile terminal SQL from a descriptor.
    let tryBuildTakeWhile
        (qb: QueryBuilder) (desc: QueryDescriptor)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (twPredLambda: LambdaExpression) (isTakeWhile: bool) : SqlExpr voption =

        if desc.SortKeys.IsEmpty then
            raise (NotSupportedException(
                "Error: TakeWhile/SkipWhile requires an explicit OrderBy.\nFix: Add .OrderBy(key) before .TakeWhile(pred)."))

        let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
        let twSubQb = qb.ForSubquery(tgtAlias, twPredLambda, subqueryRootTable = targetTable)
        let twPredDu = visitDu twPredLambda.Body twSubQb
        let twJoins = DBRefManyHelpers.joinEdgesToClauses twSubQb.SourceContext.Joins
        let baseCore = { baseCore with Joins = baseCore.Joins @ twJoins }
        let caseExpr = SqlExpr.CaseExpr(
            (SqlExpr.Unary(UnaryOperator.Not, twPredDu), SqlExpr.Literal(SqlLiteral.Integer 1L)),
            [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
        let sortKeyDuPairs = baseCore.OrderBy |> List.map (fun ob -> (ob.Expr, ob.Direction))
        let windowSpec = { Kind = NamedWindowFunction "SUM"; Arguments = [caseExpr]; PartitionBy = []; OrderBy = sortKeyDuPairs }
        let cfExpr = SqlExpr.WindowCall windowSpec

        let innerProjs = [{ Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }; { Alias = Some "_cf"; Expr = cfExpr }]
        let innerCore = { baseCore with Projections = ProjectionSetOps.ofList innerProjs }
        let innerSel = { Ctes = []; Body = SingleSelect innerCore }
        let twAlias = nextAlias "_tw"

        let cfFilter =
            if isTakeWhile then SqlExpr.Binary(SqlExpr.Column(Some twAlias, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
            else SqlExpr.Binary(SqlExpr.Column(Some twAlias, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
        let outerCore = mkSubCore [{ Alias = Some "v"; Expr = SqlExpr.Column(Some twAlias, "v") }] (Some(DerivedTable(innerSel, twAlias))) (Some cfFilter)
        let outerSel = { Ctes = []; Body = SingleSelect outerCore }

        match desc.Terminal with
        | Terminal.Count | Terminal.LongCount ->
            let dtAlias = nextAlias "_twc"
            let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
            let countCore = mkSubCore countProj (Some(DerivedTable(outerSel, dtAlias))) None
            ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore })
        | Terminal.Exists | Terminal.Any _ ->
            ValueSome(SqlExpr.Exists outerSel)
        | Terminal.Select _ ->
            match desc.SelectProjection with
            | Some projLambda ->
                let subQb2 = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
                let projDu = visitDu projLambda.Body subQb2
                let projJoins = DBRefManyHelpers.joinEdgesToClauses subQb2.SourceContext.Joins
                let innerProjs2 = [{ Alias = Some "v"; Expr = projDu }; { Alias = Some "_cf"; Expr = cfExpr }]
                let innerCore2 = { baseCore with Projections = ProjectionSetOps.ofList innerProjs2; Joins = baseCore.Joins @ projJoins }
                let innerSel2 = { Ctes = []; Body = SingleSelect innerCore2 }
                let twAlias2 = nextAlias "_tw"
                let cfFilter2 =
                    if isTakeWhile then SqlExpr.Binary(SqlExpr.Column(Some twAlias2, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                    else SqlExpr.Binary(SqlExpr.Column(Some twAlias2, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
                let middleCore = mkSubCore [{ Alias = Some "v"; Expr = SqlExpr.Column(Some twAlias2, "v") }] (Some(DerivedTable(innerSel2, twAlias2))) (Some cfFilter2)
                let middleSel = { Ctes = []; Body = SingleSelect middleCore }
                let midAlias = nextAlias "_twm"
                let outerGA = SqlExpr.FunctionCall("json_group_array", [SqlExpr.Column(Some midAlias, "v")])
                let outerCore2 = mkSubCore [{ Alias = None; Expr = outerGA }] (Some(DerivedTable(middleSel, midAlias))) None
                ValueSome(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore2 })
            | None -> ValueNone
        | _ -> ValueNone
