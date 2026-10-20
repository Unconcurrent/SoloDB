namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.SqlModel
open SoloDatabase.GroupJoinRuntimeTypes
open SoloDatabase.GroupJoinChainParts
open SoloDatabase.DBRefManyDescriptor

module internal QueryableBuildQueryGroupJoinElements =
    open QueryableHelperBase

    /// Correlation filters the finished input before local position is assigned.
    let groupRows (rt: GroupJoinRuntime) alias =
        { mkCore ([{ Alias = Some "Id"; Expr = SqlExpr.Column(Some alias, "Id") }
                   { Alias = Some "Value"; Expr = SqlExpr.Column(Some alias, "Value") }]
                  @ QueryTranslatorSerialization.companionProjections (Some alias) rt.InnerCtx.SerializationInput)
                 (Some(DerivedTable(rt.InnerSelect, alias))) with
            Where = Some(SqlExpr.Binary(rt.OuterKeyExpr, BinaryOperator.Eq, SqlExpr.Column(Some alias, rt.InnerKeyName))) }
        |> QueryProduced.withPosition "__ord"

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
    /// against the completed group rows, with its original scalar projection policy.
    /// Used for patterns like matches.FirstOrDefault().Region where the caller
    /// accesses a member on the element result.
    let buildBareGroupElementScalar (rt: GroupJoinRuntime) (groupCall: GroupJoinElementCall) (projectionBody: Expression) =
        let scalarAlias = "gjf" + (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1).ToString(System.Globalization.CultureInfo.InvariantCulture)
        let freshInnerCtx =
            { rt.InnerCtx with
                Joins = ResizeArray() }
        let innerParam = rt.InnerParameter
        let rewrittenProjection = rt.ReplaceExpression (groupCall.Call :> Expression) (innerParam :> Expression) projectionBody
        let rows = groupRows rt scalarAlias |> wrapCore
        let produced = QueryableHelperJoinBindings.translateJoinSingleSourceProduced freshInnerCtx scalarAlias rt.Vars (Some innerParam) rewrittenProjection
        let elementType = groupCall.Call.Type
        let needsDefault = elementType.IsValueType && isNull(TypeMetadata.nullableUnderlying elementType)
        let descriptor = QuerySerializationSql.inputDescriptor produced.Input
        let packed = needsDefault || produced.Input.PhysicalOwner.IsSome || not(QuerySerializationSql.isConstantDescriptor descriptor) || (match produced with PackedValue _ -> true | _ -> false)
        let projectedExpr = if packed then QueryProduced.packet produced else QueryProduced.value freshInnerCtx.NextAlias produced
        let baseScalarCore =
            { mkCore [{ Alias = None; Expr = projectedExpr }] (Some (DerivedTable(rows, scalarAlias)))
                with Joins = QueryTranslatorReferenceJoin.preparedJoinClauses freshInnerCtx.Joins }
        let ascPosition = [{ Expr = SqlExpr.Column(Some scalarAlias, "__ord"); Direction = SortDirection.Asc }]
        let descPosition = [{ Expr = SqlExpr.Column(Some scalarAlias, "__ord"); Direction = SortDirection.Desc }]
        let mkValue core =
            let value = SqlExpr.ScalarSubquery (wrapCore core)
            if packed then PackedValue(value, QueryTranslatorSerialization.rebaseSerializationInput produced.Input)
            else DirectValue(value, QueryTranslatorSerialization.rebaseSerializationInput produced.Input)
        let absent =
            if needsDefault then QueryDefault.value rt.Vars projectionBody.Type
            else QueryProduced.typed projectionBody.Type (SqlExpr.Literal(SqlLiteral.Null))
        let selectedOrDefault core =
            let selected = mkValue core
            if not needsDefault then selected else
                QueryProduced.defaultIfEmpty projectionBody.Type selected absent
        match groupCall.Kind with
        | FirstLike _ ->
            selectedOrDefault { baseScalarCore with OrderBy = ascPosition; Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
        | LastLike _ ->
            selectedOrDefault
                { baseScalarCore with
                    OrderBy = descPosition
                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
        | SingleLike _ ->
            let valueCore =
                { baseScalarCore with
                    OrderBy = ascPosition
                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
            let countExpr = buildCountSubquery rt { baseScalarCore with OrderBy = ascPosition } (Some 2)
            let baseValue = mkValue valueCore
            QueryProduced.choose projectionBody.Type (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L))) baseValue absent
        | ElementAtLike(indexExpr, _orDefault) ->
            let idx = Convert.ToInt64(ExpressionEvaluation.evaluateExpr<obj> indexExpr)
            if idx < 0L then
                absent
            else
                selectedOrDefault
                    { baseScalarCore with
                        OrderBy = ascPosition
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
                        match buildGroupElementDispatch rt groupCall (Expression.Constant(1L) :> Expression) |> QueryProduced.value rt.InnerCtx.NextAlias with
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
