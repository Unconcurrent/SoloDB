namespace SoloDatabase
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.GroupJoinRuntimeTypes

/// Small shared pieces of GroupJoin chain emission: materialising discovered joins onto an inner
/// row, the exact-decimal average expression, the count subqueries, and the entity JSON shape.
module internal GroupJoinChainParts =
    open QueryableHelperJoin
    open QueryableHelperState
    open QueryableHelperPreprocess
    open QueryableHelperBase
    let materializeInnerRowJoins (rt: GroupJoinRuntime) (alias: string) (joins: ResizeArray<JoinEdge>) =
        let innerMaterializedPaths =
            if rt.InnerCtx.MaterializedPaths.Count > 0 then Some rt.InnerCtx.MaterializedPaths else None
        rt.MaterializeDiscoveredJoins joins (Some ("\"" + alias + "\"")) innerMaterializedPaths
    let internal buildExactDecimalAverageExpr (argExpr: SqlExpr) =
        let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, Some argExpr, false, None)
        SqlExpr.CaseExpr(
            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), SqlExpr.Literal(SqlLiteral.Null)),
            [],
            Some(SqlExpr.FunctionCall("DECIMAL_DIV", [SqlExpr.AggregateCall(AggregateKind.Sum, Some argExpr, false, None); countExpr])))
    let buildCountSubquery (rt: GroupJoinRuntime) (baseCore: SelectCore) (limit: int option) =
        let countSourceAlias = GroupJoinAliases.nextCountSource rt.InnerCtx
        let countSourceCore =
            { baseCore with
                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                Limit =
                    match limit with
                    | Some n -> Some (SqlExpr.Literal(SqlLiteral.Integer(int64 n)))
                    | None -> baseCore.Limit }
        let countSourceSel = { Ctes = []; Body = SingleSelect countSourceCore }
        let countRowAlias = GroupJoinAliases.nextCountRow rt.InnerCtx
        let countCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
              Source = Some(DerivedTable(countSourceSel, countRowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }

    let buildCountSelectSubquery (rt: GroupJoinRuntime) (sourceSel: SqlSelect) (limit: int option) =
        let countSourceAlias = GroupJoinAliases.nextCountSource rt.InnerCtx
        let countSourceCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
              Source = Some(DerivedTable(sourceSel, countSourceAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = limit |> Option.map (fun n -> SqlExpr.Literal(SqlLiteral.Integer(int64 n)))
              Offset = None }
        buildCountSubquery rt countSourceCore None

    let entityJsonExpr alias =
        SqlExpr.FunctionCall("jsonb_set", [
            SqlExpr.Column(Some alias, "Value")
            SqlExpr.Literal(SqlLiteral.String "$.Id")
            SqlExpr.Column(Some alias, "Id")
        ])
    /// Helper: extract LambdaExpression from Expression (shared extractor stores them as Expression).
    let asLambda (e: Expression) : LambdaExpression = e :?> LambdaExpression

