namespace SoloDatabase
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.ChainBounds
open SoloDatabase.GroupByExtract
open SoloDatabase.GroupByRebind
open SoloDatabase.GroupByTerminals

/// Builds the rowset for a projected GroupBy chain: the projected core, its set operations, and
/// the bounded, ordered result the terminal builders consume.
module internal GroupByRowset =
    open QueryableHelperJoin
    open QueryableHelperBase
    open QueryableBuildQueryWindowHelpers
    let rec buildProjectedChainRowset
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupParam: ParameterExpression) (groupByExprs: Expression array) (desc: QueryDescriptor) : SqlSelect =

        let setOps = desc.SetOps
        let descBase =
            { desc with
                Distinct = false
                Limit = None
                Offset = None
                SortKeys = if setOps.IsEmpty then desc.SortKeys else [] }
        let projectedLambda =
            match desc.SelectProjection with
            | Some proj -> proj
            | None ->
                raise (NotSupportedException(
                    "Error: GroupBy set operations require a projected value chain.\n" +
                    "Fix: Project the group value first, for example g.Select(x => x.Code).UnionBy(...)."))

        let identityKeyLambda =
            let p = Expression.Parameter(projectedLambda.ReturnType, "x")
            Expression.Lambda(p :> Expression, p)

        let subAlias, baseCore =
            buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs descBase []
        let effectiveOrder =
            if baseCore.OrderBy.IsEmpty then
                [{ Expr = SqlExpr.Column(Some subAlias, "Id"); Direction = SortDirection.Asc }]
            else
                baseCore.OrderBy
        let projectedCore =
            { baseCore with
                Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"
                          Expr =
                            translateExprAgainst sourceCtx subAlias vars (projectedLambda :> Expression)
                            |> normalizeScalarExpr projectedLambda.ReturnType }
                        { Alias = Some "__ord"
                          Expr =
                            rowNumberOver (effectiveOrder |> List.map (fun ob -> ob.Expr, ob.Direction)) }
                    ]
                Distinct = false
                OrderBy = effectiveOrder
                Limit = None
                Offset = None }
        let projectedSel = { Ctes = []; Body = SingleSelect projectedCore }

        let evaluateConstantEnumerable (expr: Expression) : obj list =
            if not (QueryTranslatorBaseHelpers.isFullyConstant expr) then
                raise (NotSupportedException(
                    "Error: GroupBy set operator right side must be a correlated group chain or a constant sequence.\n" +
                    "Fix: Project the right operand from the same group, or use a constant array/list, or move the operator after AsEnumerable()."))
            match QueryTranslatorBaseHelpers.evaluateExpr<IEnumerable> expr with
            | null -> []
            | values -> [ for value in values -> value ]

        let buildConstantProjectedRowset (values: obj list) =
            let mkValueExpr value =
                match value with
                | null -> SqlExpr.Literal(SqlLiteral.Null)
                | _ -> allocateParam vars value
            let arms =
                values
                |> List.mapi (fun i value ->
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = mkValueExpr value }
                            { Alias = Some "__ord"; Expr = SqlExpr.Literal(SqlLiteral.Integer(int64 (i + 1))) }
                        ]
                      Source = None
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None })
            match arms with
            | [] ->
                { Ctes = []
                  Body =
                    SingleSelect
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Null) }
                                { Alias = Some "__ord"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }
                            ]
                          Source = None
                          Joins = []
                          Where = Some(SqlExpr.Literal(SqlLiteral.Boolean false))
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None } }
            | head :: tail ->
                { Ctes = []
                  Body =
                    match tail with
                    | [] -> SingleSelect head
                    | _ -> UnionAllSelect(head, tail) }

        let rec buildRightProjectedRowset (rightExpr: Expression) =
            match tryExtractGroupByQueryDescriptor groupParam rightExpr with
            | Some rightDesc when rightDesc.SelectProjection.IsSome ->
                buildProjectedChainRowset sourceCtx baseTableName innerSelect groupRowAlias vars groupParam groupByExprs rightDesc
            | Some _ ->
                raise (NotSupportedException(
                    "Error: GroupBy set operations require projected right-side values.\n" +
                    "Fix: Project the right operand first, for example g.Where(...).Select(x => x.Code)."))
            | None ->
                buildConstantProjectedRowset (evaluateConstantEnumerable rightExpr)

        let setOpSel =
            if setOps.IsEmpty then projectedSel
            else setOps |> List.fold (GroupByRowsetSetOps.applySetOp sourceCtx vars identityKeyLambda buildRightProjectedRowset) projectedSel

        let dedupedSel =
            if desc.Distinct then
                let distinctAlias = GroupByAliases.nextSetDistinctRank sourceCtx
                let distinctCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Column(Some distinctAlias, "v") }
                            { Alias = Some "__ord"; Expr = SqlExpr.AggregateCall(AggregateKind.Min, Some(SqlExpr.Column(Some distinctAlias, "__ord")), false, None) }
                        ]
                      Source = Some(DerivedTable(setOpSel, distinctAlias))
                      Joins = []
                      Where = None
                      GroupBy = [SqlExpr.Column(Some distinctAlias, "v")]
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect distinctCore }
            else
                setOpSel

        let boundedAlias = GroupByAliases.nextSetBounded sourceCtx
        let limitExpr, offsetExpr = buildLimitOffset desc.Limit desc.Offset
        let finalValueSel = GroupByRowsetSetOps.buildProjectedValueSel sourceCtx dedupedSel
        let translateProjectedOrderExpr alias keyExpr =
            match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
            | ValueSome lambda when isIdentityLambda (lambda :> Expression) ->
                SqlExpr.Column(Some alias, "Value")
            | _ ->
                translateExprAgainst sourceCtx alias vars keyExpr
        let boundedCore =
            { Distinct = false
              Projections =
                let finalOrderBy =
                    if desc.SortKeys.IsEmpty then
                        [{ Expr = SqlExpr.Column(Some boundedAlias, "__ord"); Direction = SortDirection.Asc }]
                    else
                        desc.SortKeys
                        |> List.map (fun (keyExpr, dir) ->
                            { Expr = translateProjectedOrderExpr boundedAlias keyExpr; Direction = dir })
                let finalOrdExpr =
                    if desc.SortKeys.IsEmpty then
                        SqlExpr.Column(Some boundedAlias, "__ord")
                    else
                        rowNumberOver (
                            finalOrderBy
                            |> List.map (fun ob -> ob.Expr, ob.Direction)
                            |> fun orderings -> orderings @ [SqlExpr.Column(Some boundedAlias, "__ord"), SortDirection.Asc])
                ProjectionSetOps.ofList [
                    { Alias = Some "v"; Expr = SqlExpr.Column(Some boundedAlias, "Value") }
                    { Alias = Some "__ord"; Expr = finalOrdExpr }
                ]
              Source = Some(DerivedTable(finalValueSel, boundedAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy =
                if desc.SortKeys.IsEmpty then
                    [{ Expr = SqlExpr.Column(Some boundedAlias, "__ord"); Direction = SortDirection.Asc }]
                else
                    desc.SortKeys
                    |> List.map (fun (keyExpr, dir) ->
                        { Expr = translateProjectedOrderExpr boundedAlias keyExpr; Direction = dir })
              Limit = limitExpr
              Offset = offsetExpr }
        { Ctes = []; Body = SingleSelect boundedCore }

    /// Main entry: translate a chained group-item expression to SqlExpr.
    /// Returns Some if the expression is a recognized chain rooted in groupParam.
