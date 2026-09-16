namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.ChainExpr
open SoloDatabase.ChainPolicy
open SoloDatabase.ChainState
open SoloDatabase.ChainBounds
open SoloDatabase.ChainDescriptorBuild
open SoloDatabase.ChainWalk
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.GroupByAliases
open SoloDatabase.GroupByExtract
open SoloDatabase.GroupByRebind
open SoloDatabase.GroupByTerminals
open SoloDatabase.GroupByRowsetSetOps
open SoloDatabase.GroupByRowset

/// GroupBy chained-expression support — correlated subqueries for
/// group-item chains that go beyond simple aggregate whitelist.
/// Uses shared Terminal DU + walkChain extraction with per-context GroupBy building.
module internal QueryableBuildQueryGroupByChained =
    open QueryableHelperJoin
    open QueryableHelperBase
    open QueryableBuildQueryWindowHelpers

    /// Per-QueryContext alias generator. SQL alias numerals are deterministic
    let tryTranslateGroupByChainedExpr
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (expr: Expression) : SqlExpr option =
        // Handle MemberExpression wrapping: g.OrderBy().First().Property  or  g.OrderBy().First().Field.Member
        // memberAccess is a transform: SqlExpr (JSON blob from chain terminal) → SqlExpr (projected value)
        let memberAccess, innerExpr =
            match expr with
            | :? MemberExpression as me when not (isNull me.Expression) && referencesParam groupParam me.Expression ->
                match me.Expression with
                | :? MethodCallExpression ->
                    // One level: g.Chain().Property — route via type-aware helper (falls to json_extract for row types)
                    let transform (result: SqlExpr) =
                        DateTimeFunctions.translateGroupKeyMemberAccess result me.Expression.Type me.Member.Name
                    Some transform, me.Expression
                | :? MemberExpression as inner when not (isNull inner.Expression) ->
                    match inner.Expression with
                    | :? MethodCallExpression ->
                        // Two levels: g.Chain().Field.Member — extract field from row JSON, then translate member
                        let transform (result: SqlExpr) =
                            let fieldExpr = SqlExpr.FunctionCall("json_extract", [result; SqlExpr.Literal(SqlLiteral.String ("$." + inner.Member.Name))])
                            DateTimeFunctions.translateGroupKeyMemberAccess fieldExpr inner.Type me.Member.Name
                        Some transform, inner.Expression
                    | _ -> None, expr
                | _ -> None, expr
            | _ -> None, expr

        match tryExtractGroupByTerminalChain groupParam innerExpr with
        | None -> None
        | Some (desc, terminal) ->
            let setOps = desc.SetOps
            let useProjectedRowset = desc.SelectProjection.IsSome && (desc.Distinct || not setOps.IsEmpty)
            let projectedRowset = lazy (buildProjectedChainRowset sourceCtx baseTableName innerSelect groupRowAlias vars groupParam groupByExprs desc)
            let buildProjectedAggregateFromRowset aggKind coalesceZero =
                let rowsetAlias = GroupByAliases.nextRightProjection sourceCtx
                let aggExpr = SqlExpr.AggregateCall(aggKind, Some(SqlExpr.Column(Some rowsetAlias, "v")), false, None)
                let aggExpr = if coalesceZero then SqlExpr.Coalesce(aggExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)]) else aggExpr
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = aggExpr }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
            let buildProjectedCountFromRowset () =
                let rowsetAlias = GroupByAliases.nextRightConstant sourceCtx
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                SqlExpr.Coalesce(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
            let buildProjectedElementFromRowset pickLast (cardinality: ElementCardinality) (orDefault: bool) =
                let rowsetAlias = GroupByAliases.nextRightExists sourceCtx
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some rowsetAlias, "v") }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = if pickLast then SortDirection.Desc else SortDirection.Asc }]
                      Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                      Offset = None }
                let valueScalar = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                match cardinality with
                | FirstLike -> valueScalar
                | SingleLike ->
                    // Wrap with cardinality guard against the projected rowset.
                    // The base SelectCore for the count subquery is the rowset-derived
                    // shape; wrapCardinalityCase emits the COUNT(LIMIT 2) sibling.
                    let baseCore =
                        { Distinct = false
                          Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                          Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                          Joins = []; Where = None; GroupBy = []; Having = None; OrderBy = []; Limit = None; Offset = None }
                    wrapCardinalityCase sourceCtx baseCore orDefault valueScalar
            let buildProjectedContainsFromRowset value =
                let rowsetAlias = GroupByAliases.nextRightNested sourceCtx
                let valueDu = translateExprDu sourceCtx groupRowAlias value vars
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = Some(SqlExpr.Binary(SqlExpr.Column(Some rowsetAlias, "v"), BinaryOperator.Is, valueDu))
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                      Offset = None }
                SqlExpr.Exists { Ctes = []; Body = SingleSelect core }
            let buildProjectedSelectFromRowset () =
                let rowsetAlias = GroupByAliases.nextRightSet sourceCtx
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.Column(Some rowsetAlias, "v")]) }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }

            let buildProjectedElementAtFromRowset (descWithOffset: QueryDescriptor) =
                let projectedRowset = buildProjectedChainRowset sourceCtx baseTableName innerSelect groupRowAlias vars groupParam groupByExprs descWithOffset
                let rowsetAlias = GroupByAliases.nextRightExists sourceCtx
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some rowsetAlias, "v") }]
                      Source = Some(DerivedTable(projectedRowset, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Asc }]
                      Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                      Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }

            let result =
                match terminal with
                | Terminal.Count | Terminal.LongCount ->
                    if useProjectedRowset then buildProjectedCountFromRowset ()
                    else buildCountSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc
                | Terminal.Sum sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Sum (Some sel) true
                | Terminal.SumProjected ->
                    if useProjectedRowset then buildProjectedAggregateFromRowset AggregateKind.Sum true
                    else buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Sum None true
                | Terminal.Min sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Min (Some sel) false
                | Terminal.MinProjected ->
                    if useProjectedRowset then buildProjectedAggregateFromRowset AggregateKind.Min false
                    else buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Min None false
                | Terminal.Max sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Max (Some sel) false
                | Terminal.MaxProjected ->
                    if useProjectedRowset then buildProjectedAggregateFromRowset AggregateKind.Max false
                    else buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Max None false
                | Terminal.Average sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Avg (Some sel) false
                | Terminal.AverageProjected ->
                    if useProjectedRowset then buildProjectedAggregateFromRowset AggregateKind.Avg false
                    else buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Avg None false
                | Terminal.Exists ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc None false
                | Terminal.Any(Some pred) ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc (Some pred) false
                | Terminal.Any None ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc None false
                | Terminal.All pred ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc (Some pred) true
                | Terminal.First _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset false FirstLike false
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false FirstLike
                | Terminal.FirstOrDefault _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset false FirstLike true
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true FirstLike
                | Terminal.Last _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset true FirstLike false
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc true false FirstLike
                | Terminal.LastOrDefault _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset true FirstLike true
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc true true FirstLike
                | Terminal.Single _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset false SingleLike false
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false SingleLike
                | Terminal.SingleOrDefault _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset false SingleLike true
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true SingleLike
                | Terminal.ElementAt idx ->
                    let desc = { desc with Offset = Some idx }
                    if useProjectedRowset then buildProjectedElementAtFromRowset desc
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false FirstLike
                | Terminal.ElementAtOrDefault idx ->
                    let desc = { desc with Offset = Some idx }
                    if useProjectedRowset then buildProjectedElementAtFromRowset desc
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true FirstLike
                | Terminal.Contains value ->
                    if useProjectedRowset then buildProjectedContainsFromRowset value
                    else
                        let subAlias = GroupByAliases.nextConcat sourceCtx
                        let correlation = buildCorrelation subAlias groupRowAlias
                        let valueDu = translateExprDu sourceCtx groupRowAlias value vars
                        let projExpr =
                            match desc.SelectProjection with
                            | Some proj -> translateExprAgainst sourceCtx subAlias vars (proj :> Expression)
                            | None -> SqlExpr.Column(Some subAlias, "Value")
                        let containsWhere = SqlExpr.Binary(correlation, BinaryOperator.And, SqlExpr.Binary(projExpr, BinaryOperator.Is, valueDu))
                        let core =
                            { Distinct = false
                              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                              Source = Some(DerivedTable(innerSelect, subAlias))
                              Joins = []
                              Where = Some containsWhere
                              GroupBy = []
                              Having = None
                              OrderBy = []
                              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                              Offset = None }
                        SqlExpr.Exists { Ctes = []; Body = SingleSelect core }
                | Terminal.Select _ ->
                    if useProjectedRowset then buildProjectedSelectFromRowset ()
                    else
                        let subAlias = GroupByAliases.nextSelection sourceCtx
                        let _, core = buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc []
                        let core = { core with Source = Some(DerivedTable(innerSelect, subAlias)) }
                        let valueExpr =
                            match desc.SelectProjection with
                            | Some proj -> translateExprAgainst sourceCtx subAlias vars (proj :> Expression)
                            | None ->
                                SqlExpr.FunctionCall("jsonb_set", [
                                    SqlExpr.Column(Some subAlias, "Value")
                                    SqlExpr.Literal(SqlLiteral.String "$.Id")
                                    SqlExpr.Column(Some subAlias, "Id")])
                        let core = { core with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.FunctionCall("jsonb_group_array", [valueExpr]) }] }
                        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                // Unsupported terminals — fail closed
                | Terminal.MinBy _ | Terminal.MaxBy _ | Terminal.DistinctBy _ | Terminal.CountBy _ ->
                    raise (NotSupportedException(
                        $"Error: GroupBy chain terminal '{terminal}' is not yet supported.\n" +
                        "Fix: Move the query after AsEnumerable() or use a supported terminal."))

            // Apply member access if wrapping: g.OrderBy().First().Property  or  g.OrderBy().First().Field.Member
            let result =
                match memberAccess with
                | Some transform -> transform result
                | None -> result

            Some result
