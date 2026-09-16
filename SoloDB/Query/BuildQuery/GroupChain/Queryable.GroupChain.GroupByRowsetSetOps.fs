namespace SoloDatabase
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryableHelperBase
open SoloDatabase.QueryableHelperJoin
open SoloDatabase.QueryableBuildQueryWindowHelpers

/// Projection shaping and set operations for a GroupBy chain rowset.
///
/// Everything it needs arrives as an explicit parameter. The one exception is the recursion back
/// into the chain builder, which `applySetOp` takes as a function because a set operation's right
/// operand may itself be a projected chain; that is a single value per chain build, not a record
/// of callbacks threaded into every helper.
module internal GroupByRowsetSetOps =
    let buildProjectedValueSel (sourceCtx: QueryContext) (rowsetSel: SqlSelect) =
            let valueAlias = GroupByAliases.nextSetValue sourceCtx
            let valueCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "Value"; Expr = SqlExpr.Column(Some valueAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some valueAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rowsetSel, valueAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect valueCore }

    let buildProjectedKeyedSel (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (rowsetSel: SqlSelect) (keyLambda: LambdaExpression) =
            let valueSel = buildProjectedValueSel sourceCtx rowsetSel
            let keyAlias = GroupByAliases.nextSetKeyed sourceCtx
            let keyExpr =
                if isIdentityLambda (keyLambda :> Expression) then
                    SqlExpr.Column(Some keyAlias, "Value")
                else
                    translateExprDu sourceCtx keyAlias (keyLambda :> Expression) vars
            let keyedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some keyAlias, "Value") }
                        { Alias = Some "k"; Expr = keyExpr }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some keyAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(valueSel, keyAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect keyedCore }

    let buildProjectedMembershipExists (sourceCtx: QueryContext) (leftExpr: SqlExpr) (rightValuesSel: SqlSelect) =
            let rightAlias = GroupByAliases.nextSetExists sourceCtx
            let existsCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                  Source = Some(DerivedTable(buildProjectedValueSel sourceCtx rightValuesSel, rightAlias))
                  Joins = []
                  Where = Some(SqlExpr.Binary(leftExpr, BinaryOperator.Is, SqlExpr.Column(Some rightAlias, "Value")))
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                  Offset = None }
            SqlExpr.Exists { Ctes = []; Body = SingleSelect existsCore }

    let buildDistinctByProjectedRowset (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (rowsetSel: SqlSelect) (keyLambda: LambdaExpression) =
            let keyedSel = buildProjectedKeyedSel sourceCtx vars rowsetSel keyLambda
            let rankAlias = GroupByAliases.nextSetDistinctRank sourceCtx
            let rankedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some rankAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rankAlias, "__ord") }
                        { Alias = Some "__rk"
                          Expr =
                            rowNumberByKey (SqlExpr.Column(Some rankAlias, "k")) [SqlExpr.Column(Some rankAlias, "__ord"), SortDirection.Asc] }
                    ]
                  Source = Some(DerivedTable(keyedSel, rankAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let rankedSel = { Ctes = []; Body = SingleSelect rankedCore }
            let filteredAlias = GroupByAliases.nextSetFiltered sourceCtx
            let filteredCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some filteredAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some filteredAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rankedSel, filteredAlias))
                  Joins = []
                  Where = Some(SqlExpr.Binary(SqlExpr.Column(Some filteredAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                  GroupBy = []
                  Having = None
                  OrderBy = [{ Expr = SqlExpr.Column(Some filteredAlias, "__ord"); Direction = SortDirection.Asc }]
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect filteredCore }

    let buildByFilterProjectedRowset (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (rowsetSel: SqlSelect) (keyLambda: LambdaExpression) (rightValuesSel: SqlSelect) (negate: bool) =
            let keyedSel = buildProjectedKeyedSel sourceCtx vars rowsetSel keyLambda
            let filterAlias = GroupByAliases.nextSetMembership sourceCtx
            let membershipPred =
                let existsExpr = buildProjectedMembershipExists sourceCtx (SqlExpr.Column(Some filterAlias, "k")) rightValuesSel
                if negate then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr
            let rankedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some filterAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some filterAlias, "__ord") }
                        { Alias = Some "__rk"
                          Expr =
                            rowNumberByKey (SqlExpr.Column(Some filterAlias, "k")) [SqlExpr.Column(Some filterAlias, "__ord"), SortDirection.Asc] }
                    ]
                  Source = Some(DerivedTable(keyedSel, filterAlias))
                  Joins = []
                  Where = Some membershipPred
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let rankedSel = { Ctes = []; Body = SingleSelect rankedCore }
            let filteredAlias = GroupByAliases.nextSetRemaining sourceCtx
            let filteredCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some filteredAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some filteredAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rankedSel, filteredAlias))
                  Joins = []
                  Where = Some(SqlExpr.Binary(SqlExpr.Column(Some filteredAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                  GroupBy = []
                  Having = None
                  OrderBy = [{ Expr = SqlExpr.Column(Some filteredAlias, "__ord"); Direction = SortDirection.Asc }]
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect filteredCore }

    let buildUnionByProjectedRowset (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (rowsetSel: SqlSelect) (rightRowsetSel: SqlSelect) (keyLambda: LambdaExpression) =
            let keyedSel = buildProjectedKeyedSel sourceCtx vars rowsetSel keyLambda
            let leftAlias = GroupByAliases.nextSetUnionLeft sourceCtx
            let leftCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some leftAlias, "v") }
                        { Alias = Some "k"; Expr = SqlExpr.Column(Some leftAlias, "k") }
                        { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some leftAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(keyedSel, leftAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let rightKeyedSel = buildProjectedKeyedSel sourceCtx vars rightRowsetSel keyLambda
            let rightAlias = GroupByAliases.nextSetValue sourceCtx
            let rightCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some rightAlias, "v") }
                        { Alias = Some "k"; Expr = SqlExpr.Column(Some rightAlias, "k") }
                        { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rightAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rightKeyedSel, rightAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let unionSel = { Ctes = []; Body = UnionAllSelect(leftCore, [rightCore]) }
            let unionAlias = GroupByAliases.nextSetExtra sourceCtx
            let rankedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some unionAlias, "v") }
                        { Alias = Some "__src"; Expr = SqlExpr.Column(Some unionAlias, "__src") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some unionAlias, "__ord") }
                        { Alias = Some "__rk"
                          Expr =
                            rowNumberByKey
                                (SqlExpr.Column(Some unionAlias, "k"))
                                [
                                    SqlExpr.Column(Some unionAlias, "__src"), SortDirection.Asc
                                    SqlExpr.Column(Some unionAlias, "__ord"), SortDirection.Asc
                                ] }
                    ]
                  Source = Some(DerivedTable(unionSel, unionAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let rankedSel = { Ctes = []; Body = SingleSelect rankedCore }
            let filteredAlias = GroupByAliases.nextSetSecondary sourceCtx
            let filteredCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some filteredAlias, "v") }
                        { Alias = Some "__ord"
                          Expr =
                            rowNumberOver [
                                SqlExpr.Column(Some filteredAlias, "__src"), SortDirection.Asc
                                SqlExpr.Column(Some filteredAlias, "__ord"), SortDirection.Asc
                            ] }
                    ]
                  Source = Some(DerivedTable(rankedSel, filteredAlias))
                  Joins = []
                  Where = Some(SqlExpr.Binary(SqlExpr.Column(Some filteredAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                  GroupBy = []
                  Having = None
                  OrderBy = [
                    { Expr = SqlExpr.Column(Some filteredAlias, "__src"); Direction = SortDirection.Asc }
                    { Expr = SqlExpr.Column(Some filteredAlias, "__ord"); Direction = SortDirection.Asc }
                  ]
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect filteredCore }

    let buildConcatProjectedRowset (sourceCtx: QueryContext) (rowsetSel: SqlSelect) (rightRowsetSel: SqlSelect) =
            let leftAlias = GroupByAliases.nextSetConcat sourceCtx
            let leftCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some leftAlias, "v") }
                        { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some leftAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rowsetSel, leftAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let rightAlias = GroupByAliases.nextSetWrap sourceCtx
            let rightCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some rightAlias, "v") }
                        { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rightAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rightRowsetSel, rightAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let unionSel = { Ctes = []; Body = UnionAllSelect(leftCore, [rightCore]) }
            let outAlias = GroupByAliases.nextSetFinal sourceCtx
            let outCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some outAlias, "v") }
                        { Alias = Some "__ord"
                          Expr =
                            rowNumberOver [
                                SqlExpr.Column(Some outAlias, "__src"), SortDirection.Asc
                                SqlExpr.Column(Some outAlias, "__ord"), SortDirection.Asc
                            ] }
                    ]
                  Source = Some(DerivedTable(unionSel, outAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = [
                    { Expr = SqlExpr.Column(Some outAlias, "__src"); Direction = SortDirection.Asc }
                    { Expr = SqlExpr.Column(Some outAlias, "__ord"); Direction = SortDirection.Asc }
                  ]
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect outCore }

    let applySetOp (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (identityKeyLambda: LambdaExpression) (buildRightProjectedRowset: Expression -> SqlSelect) rowsetSel setOp =
            match setOp with
            | SetOperation.DistinctBy keyExpr ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> buildDistinctByProjectedRowset sourceCtx vars rowsetSel keyLambda
                | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupBy DistinctBy."))
            | SetOperation.Intersect rightSource ->
                buildByFilterProjectedRowset sourceCtx vars rowsetSel identityKeyLambda (buildRightProjectedRowset rightSource) false
            | SetOperation.Except rightSource ->
                buildByFilterProjectedRowset sourceCtx vars rowsetSel identityKeyLambda (buildRightProjectedRowset rightSource) true
            | SetOperation.Union rightSource ->
                buildUnionByProjectedRowset sourceCtx vars rowsetSel (buildRightProjectedRowset rightSource) identityKeyLambda
            | SetOperation.Concat rightSource ->
                buildConcatProjectedRowset sourceCtx rowsetSel (buildRightProjectedRowset rightSource)
            | SetOperation.IntersectBy(rightKeys, keyExpr) ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> buildByFilterProjectedRowset sourceCtx vars rowsetSel keyLambda (buildRightProjectedRowset rightKeys) false
                | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupBy IntersectBy."))
            | SetOperation.ExceptBy(rightKeys, keyExpr) ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> buildByFilterProjectedRowset sourceCtx vars rowsetSel keyLambda (buildRightProjectedRowset rightKeys) true
                | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupBy ExceptBy."))
            | SetOperation.UnionBy(rightSource, keyExpr) ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> buildUnionByProjectedRowset sourceCtx vars rowsetSel (buildRightProjectedRowset rightSource) keyLambda
                | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupBy UnionBy."))

