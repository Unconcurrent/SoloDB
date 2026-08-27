namespace SoloDatabase
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.QueryTranslatorVisitPost
open SqlDu.Engine.C1.Spec
open SoloDatabase.DBRefManyDescriptor

/// The GroupJoin terminals: collection materialisation, aggregate, exists and contains over a
/// built chain rowset.
open SoloDatabase.QueryableBuildQueryGroupJoinChain
open SoloDatabase.GroupJoinChainParts
open SoloDatabase.GroupJoinRuntimeTypes

module internal GroupJoinTerminals =
    open QueryableHelperJoin
    open QueryableHelperState
    open QueryableHelperPreprocess
    open QueryableHelperBase
    let buildGroupChainCollectionQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
        let rowsetAlias = GroupJoinAliases.nextCollectionRowset rt.InnerCtx
        let valueExpr =
            if isProjected then SqlExpr.Column(Some rowsetAlias, "v")
            else entityJsonExpr rowsetAlias
        let outerCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.FunctionCall("jsonb_group_array", [valueExpr]) }]
              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let buildAggregateOverChainQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) (aggKind: AggregateKind) (selectorOpt: LambdaExpression option) (coalesceZero: bool) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
        let rowsetAlias = GroupJoinAliases.nextAggregateRowset rt.InnerCtx
        let aggCtx = QueryContext.ChildOf(rt.InnerCtx, rt.InnerRootTable)
        let aggregateType =
            match selectorOpt, desc.SelectProjection with
            | Some sel, _ -> Some sel.Body.Type
            | None, Some proj when isProjected -> Some proj.Body.Type
            | _ -> None
        let useExactDecimalAverage =
            aggKind = AggregateKind.Avg
            && (aggregateType |> Option.exists isDecimalOrNullableDecimal)
        let aggregateArg =
            match selectorOpt, desc.SelectProjection, isProjected with
            | Some _, Some _, _ ->
                raise (NotSupportedException(
                    "Error: GroupJoin chained aggregate cannot apply a selector after Select.\n" +
                    "Fix: Use the projected chain directly or remove the inner Select."))
            | Some sel, _, _ ->
                let selCtx = { aggCtx with Joins = ResizeArray() }
                let selExpr = rt.TranslateJoinExpr selCtx rowsetAlias rt.Vars (Some sel.Parameters.[0]) sel.Body
                let joins = materializeInnerRowJoins rt rowsetAlias selCtx.Joins
                Some(selExpr, joins)
            | None, Some _, true ->
                Some(SqlExpr.Column(Some rowsetAlias, "v"), [])
            | None, _, false when aggKind = AggregateKind.Count ->
                None
            | None, _, false ->
                raise (NotSupportedException(
                    "Error: GroupJoin chained aggregate requires a selector.\n" +
                    "Fix: Pass a selector lambda, or project the value first with .Select(...)."))
            | None, _, true ->
                Some(SqlExpr.Column(Some rowsetAlias, "v"), [])
        let aggregateSource, aggregateJoins, aggregateExpr =
            match aggregateArg with
            | Some (argExpr, joins) ->
                let expr =
                    if useExactDecimalAverage then buildExactDecimalAverageExpr argExpr
                    else SqlExpr.AggregateCall(aggKind, Some argExpr, false, None)
                Some(DerivedTable(rowsetSel, rowsetAlias)), joins, expr
            | None ->
                Some(DerivedTable(rowsetSel, rowsetAlias)), [], SqlExpr.AggregateCall(aggKind, None, false, None)
        let aggregateExpr =
            if coalesceZero then SqlExpr.Coalesce(aggregateExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
            else aggregateExpr
        SqlExpr.ScalarSubquery {
            Ctes = []
            Body = SingleSelect {
                Distinct = false
                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = aggregateExpr }]
                Source = aggregateSource
                Joins = aggregateJoins
                Where = None
                GroupBy = []
                Having = None
                OrderBy = []
                Limit = None
                Offset = None
            } }

    let buildExistsOverChainQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) (predOpt: LambdaExpression option) (negate: bool) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
        let rowsetAlias = GroupJoinAliases.nextExistsRowset rt.InnerCtx
        let predicateExpr, predicateJoins =
            match predOpt with
            | None -> None, []
            | Some pred when isProjected ->
                raise (NotSupportedException(
                    "Error: GroupJoin chained predicate after Select is not supported.\n" +
                    "Fix: Move the predicate before Select, or remove the inner Select."))
            | Some pred ->
                let predCtx = QueryContext.ChildOf(rt.InnerCtx, rt.InnerRootTable)
                let predCtx = { predCtx with Joins = ResizeArray() }
                let predExpr = rt.TranslateJoinExpr predCtx rowsetAlias rt.Vars (Some pred.Parameters.[0]) pred.Body
                Some predExpr, materializeInnerRowJoins rt rowsetAlias predCtx.Joins
        let existsCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
              Joins = predicateJoins
              Where =
                match predicateExpr with
                | Some pred when negate -> Some(SqlExpr.Unary(UnaryOperator.Not, pred))
                | Some pred -> Some pred
                | None -> None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }
        let existsExpr = SqlExpr.Exists { Ctes = []; Body = SingleSelect existsCore }
        if negate then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr

    let buildContainsOverChainQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) (valueExpr: Expression) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
        if not isProjected then
            raise (NotSupportedException(
                "Error: GroupJoin Contains requires a projected scalar chain.\n" +
                "Fix: Project the compared value first, for example g.Select(x => x.Region).Contains(value)."))
        let rowsetAlias = GroupJoinAliases.nextContainsRowset rt.InnerCtx
        let valueDu = rt.TranslateOuterExpr valueExpr
        let containsCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
              Joins = []
              Where = Some (SqlExpr.Binary(SqlExpr.Column(Some rowsetAlias, "v"), BinaryOperator.Is, valueDu))
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }
        SqlExpr.Exists { Ctes = []; Body = SingleSelect containsCore }

