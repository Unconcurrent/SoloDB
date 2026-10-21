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

/// Bounded grouped element counts share the original source and count projection.
module internal GroupJoinChainParts =
    open QueryableHelperJoin
    open QueryableHelperState
    open QueryableHelperPreprocess
    open QueryableHelperBase
    let buildCountSubquery (rt: GroupJoinRuntime) (baseCore: SelectCore) (limit: int option) =
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
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None, []) }]
              Source = Some(DerivedTable(countSourceSel, countRowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }
