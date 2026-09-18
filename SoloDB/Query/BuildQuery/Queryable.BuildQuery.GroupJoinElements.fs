namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.SqlModel
open SoloDatabase.GroupJoinRuntimeTypes
open SoloDatabase.GroupJoinChainParts
open SoloDatabase.QueryableBuildQueryGroupJoinChain
open SoloDatabase.DBRefManyDescriptor

module internal QueryableBuildQueryGroupJoinElements =
    open QueryableHelperBase

    let tryMatchGroupElementCall (rt: GroupJoinRuntime) (expr: Expression) =
        match expr with
        | :? MethodCallExpression as call when call.Arguments.Count >= 1 ->
            match OrderedChainPlan.parse (fun e -> Object.ReferenceEquals(e, rt.GroupParam)) call.Arguments.[0] with
            | Some plan when plan.Stages.IsEmpty ->
                let kind =
                    match call.Method.Name, call.Arguments.Count with
                    | "First", 1 -> Some(FirstLike false)
                    | "FirstOrDefault", 1 -> Some(FirstLike true)
                    | "Last", 1 -> Some(LastLike false)
                    | "LastOrDefault", 1 -> Some(LastLike true)
                    | "Single", 1 -> Some(SingleLike false)
                    | "SingleOrDefault", 1 -> Some(SingleLike true)
                    | "ElementAt", 2 -> Some(ElementAtLike(call.Arguments.[1], false))
                    | "ElementAtOrDefault", 2 -> Some(ElementAtLike(call.Arguments.[1], true))
                    | _ -> None
                kind |> Option.map (fun k -> { Call = call; Kind = k })
            | _ -> None
        | _ -> None

    let isNullConstant (expr: Expression) =
        match expr with
        | :? ConstantExpression as ce -> isNull ce.Value
        | _ -> false

    /// Bare-group scalar element access (no chain ops): translates the projection body
    /// directly against the inner select, without going through the rowset builder.
    /// Used for patterns like matches.FirstOrDefault().Region where the caller
    /// accesses a member on the element result.
    let buildBareGroupElementScalar (rt: GroupJoinRuntime) (groupCall: GroupJoinElementCall) (projectionBody: Expression) =
        let scalarAlias = sprintf "gjf%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let freshInnerCtx =
            { rt.InnerCtx with
                Joins = ResizeArray() }
        let innerParam = rt.InnerKeySelector.Parameters.[0]
        let rewrittenProjection = rt.ReplaceExpression (groupCall.Call :> Expression) (innerParam :> Expression) projectionBody
        let freshInnerKeyExpr =
            match rt.TryTranslateDbRefValueIdKey innerParam scalarAlias rt.InnerKeySelector.Body with
            | Some translated -> translated
            | None -> rt.TranslateJoinExpr freshInnerCtx scalarAlias rt.Vars (Some innerParam) rt.InnerKeySelector.Body
        let projectedExpr =
            rt.TranslateJoinExpr freshInnerCtx scalarAlias rt.Vars (Some innerParam) rewrittenProjection
        let baseScalarCore =
            { mkCore [{ Alias = None; Expr = projectedExpr }] (Some (DerivedTable(rt.InnerSelect, scalarAlias)))
                with
                    Joins = rt.MaterializeDiscoveredJoins freshInnerCtx.Joins None None
                    Where = Some (SqlExpr.Binary(rt.OuterKeyExpr, BinaryOperator.Eq, freshInnerKeyExpr)) }
        let ascIdOrder = [{ Expr = SqlExpr.Column(Some scalarAlias, "Id"); Direction = SortDirection.Asc }]
        let descIdOrder = [{ Expr = SqlExpr.Column(Some scalarAlias, "Id"); Direction = SortDirection.Desc }]
        let mkValue core = SqlExpr.ScalarSubquery (wrapCore core)
        let nullLit = SqlExpr.Literal(SqlLiteral.Null)
        match groupCall.Kind with
        | FirstLike _ ->
            mkValue { baseScalarCore with Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
        | LastLike _ ->
            mkValue
                { baseScalarCore with
                    OrderBy = descIdOrder
                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
        | SingleLike orDefault ->
            let valueCore =
                { baseScalarCore with
                    OrderBy = ascIdOrder
                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
            let countExpr = buildCountSubquery rt { baseScalarCore with OrderBy = ascIdOrder } (Some 2)
            let baseValue = mkValue valueCore
            if orDefault then
                SqlExpr.CaseExpr(
                    (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), nullLit),
                    [],
                    Some baseValue)
            else
                SqlExpr.CaseExpr(
                    (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), nullLit),
                    [ (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), nullLit) ],
                    Some baseValue)
        | ElementAtLike(indexExpr, _orDefault) ->
            let idx = Convert.ToInt64(QueryTranslatorBaseHelpers.evaluateExpr<obj> indexExpr)
            if idx < 0L then
                nullLit
            else
                mkValue
                    { baseScalarCore with
                        OrderBy = ascIdOrder
                        Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L))
                        Offset = Some (SqlExpr.Literal(SqlLiteral.Integer idx)) }

    let buildGroupElementDispatch (rt: GroupJoinRuntime) (groupCall: GroupJoinElementCall) (projectionBody: Expression) =
        buildBareGroupElementScalar rt groupCall projectionBody

    let tryTranslateGroupFirstLikeNullComparison (rt: GroupJoinRuntime) (expr: BinaryExpression) =
        let tryBuild groupExpr nullExpr nodeType =
            match tryMatchGroupElementCall rt groupExpr with
            | Some groupCall when isNullConstant nullExpr ->
                let existsExpr =
                    match groupCall.Kind with
                    | FirstLike _
                    | LastLike _
                    | ElementAtLike(_, true) ->
                        match buildGroupElementDispatch rt groupCall (Expression.Constant(1L) :> Expression) with
                        | SqlExpr.ScalarSubquery select -> Some (SqlExpr.Exists select)
                        | _ -> failwith "internal invariant violation: expected scalar subquery"
                    | _ -> None
                match existsExpr, nodeType with
                | Some existsExpr, ExpressionType.Equal -> Some (SqlExpr.Unary(UnaryOperator.Not, existsExpr))
                | Some existsExpr, ExpressionType.NotEqual -> Some existsExpr
                | Some _, _ -> failwith "internal invariant violation: expected == or !="
                | None, _ -> None
            | _ -> None
        match tryBuild expr.Left expr.Right expr.NodeType with
        | Some translated -> Some translated
        | None -> tryBuild expr.Right expr.Left expr.NodeType
