namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryableBuildQueryPartBGroupJoinChain
open SoloDatabase.DBRefManyDescriptor

module internal QueryableBuildQueryPartBGroupJoinElements =
    open QueryableHelperBase

    let private defaultScalarExpr (targetType: Type) =
        if targetType.IsValueType then
            let defaultValue = Activator.CreateInstance(targetType)
            let jsonObj = JsonSerializator.JsonValue.Serialize defaultValue
            let jsonText = jsonObj.ToJsonString()
            SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
        else
            SqlExpr.Literal(SqlLiteral.Null)

    let tryMatchGroupElementCall (rt: GroupJoinRuntime) (expr: Expression) =
        match expr with
        | :? MethodCallExpression as mc
            when mc.Arguments.Count >= 1 ->
            let chainOpt =
                match mc.Arguments.[0] with
                | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) -> Some emptyChainDescriptor
                | source -> tryGetGroupChainDescriptor rt source
            match chainOpt with
            | Some chain ->
                match mc.Method.Name, mc.Arguments.Count with
                | "First", 1 -> Some { Call = mc; Kind = FirstLike false; Chain = chain }
                | "FirstOrDefault", 1 -> Some { Call = mc; Kind = FirstLike true; Chain = chain }
                | "Last", 1 -> Some { Call = mc; Kind = LastLike false; Chain = chain }
                | "LastOrDefault", 1 -> Some { Call = mc; Kind = LastLike true; Chain = chain }
                | "Single", 1 -> Some { Call = mc; Kind = SingleLike false; Chain = chain }
                | "SingleOrDefault", 1 -> Some { Call = mc; Kind = SingleLike true; Chain = chain }
                | "ElementAt", 2 -> Some { Call = mc; Kind = ElementAtLike(mc.Arguments.[1], false); Chain = chain }
                | "ElementAtOrDefault", 2 -> Some { Call = mc; Kind = ElementAtLike(mc.Arguments.[1], true); Chain = chain }
                | _ -> None
            | None -> None
        | _ -> None

    let isNullConstant (expr: Expression) =
        match expr with
        | :? ConstantExpression as ce -> isNull ce.Value
        | _ -> false

    let buildGroupElementSubqueryQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) (kind: GroupJoinElementKind) (projectionBody: Expression) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
        let rowsetAlias = sprintf "gje%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let mkValueCore orderBy limit offset valueExpr =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = valueExpr }]
              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = orderBy
              Limit = limit
              Offset = offset }
        let wrapOrDefault valueExpr orDefault =
            if orDefault && isProjected then
                SqlExpr.Coalesce(valueExpr, [defaultScalarExpr projectionBody.Type])
            else
                valueExpr
        let mkValue core = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
        let noElements = "Sequence contains no elements"
        let manyElements = "Sequence contains more than one element"
        let outOfRange = "Index was out of range. Must be non-negative and less than the size of the collection."
        let ascOrd = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Asc }]
        let descOrd = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Desc }]
        let valueExpr =
            if isProjected then SqlExpr.Column(Some rowsetAlias, "v")
            else entityJsonExpr rowsetAlias
        match kind with
        | FirstLike orDefault ->
            let valueCore = mkValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None valueExpr
            let error =
                if orDefault then None
                else
                    let countExpr = buildCountSelectSubquery rt rowsetSel (Some 1)
                    Some (SqlExpr.CaseExpr(
                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr noElements),
                        [],
                        Some(SqlExpr.Literal(SqlLiteral.Null))))
            { Value = wrapOrDefault (mkValue valueCore) orDefault; Error = error }
        | LastLike orDefault ->
            let valueCore = mkValueCore descOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None valueExpr
            let error =
                if orDefault then None
                else
                    let countExpr = buildCountSelectSubquery rt rowsetSel (Some 1)
                    Some (SqlExpr.CaseExpr(
                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr noElements),
                        [],
                        Some(SqlExpr.Literal(SqlLiteral.Null))))
            { Value = wrapOrDefault (mkValue valueCore) orDefault; Error = error }
        | SingleLike orDefault ->
            let valueCore = mkValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None valueExpr
            let countExpr = buildCountSelectSubquery rt rowsetSel (Some 2)
            let error =
                if orDefault then
                    Some (SqlExpr.CaseExpr(
                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), rt.ErrorExpr manyElements),
                        [],
                        Some(SqlExpr.Literal(SqlLiteral.Null))))
                else
                    Some (SqlExpr.CaseExpr(
                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr noElements),
                        [ (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), rt.ErrorExpr manyElements) ],
                        Some(SqlExpr.Literal(SqlLiteral.Null))))
            { Value = wrapOrDefault (mkValue valueCore) orDefault; Error = error }
        | ElementAtLike(indexExpr, orDefault) ->
            let idx = Convert.ToInt64(QueryTranslator.evaluateExpr<obj> indexExpr)
            if idx < 0L then
                { Value = wrapOrDefault (SqlExpr.Literal(SqlLiteral.Null)) orDefault
                  Error = if orDefault then None else Some(rt.ErrorExpr outOfRange) }
            else
                let valueCore =
                    mkValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) (Some (SqlExpr.Literal(SqlLiteral.Integer idx))) valueExpr
                let error =
                    if orDefault then None
                    else
                        let countExpr = buildCountSubquery rt valueCore None
                        Some (SqlExpr.CaseExpr(
                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr outOfRange),
                            [],
                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                { Value = wrapOrDefault (mkValue valueCore) orDefault; Error = error }

    let buildGroupElementSubquery (rt: GroupJoinRuntime) (groupCall: GroupJoinElementCall) (projectionBody: Expression) =
        if hasGroupChainOps groupCall.Chain then
            let rowsetSel, isProjected = buildGroupChainRowset rt groupCall.Chain
            let rowsetAlias = sprintf "gje%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
            let rowValueExpr, rowValueJoins =
                if Object.ReferenceEquals(projectionBody, groupCall.Call :> Expression) then
                    if isProjected then SqlExpr.Column(Some rowsetAlias, "v"), []
                    else entityJsonExpr rowsetAlias, []
                elif isProjected then
                    raise (NotSupportedException(
                        "Error: GroupJoin chained member access after Select is not supported.\n" +
                        "Fix: Access the projected member inside Select, or remove the inner Select."))
                else
                    let projCtx = QueryContext.SingleSource(rt.InnerRootTable)
                    let projCtx = { projCtx with Joins = ResizeArray() }
                    let innerParam = rt.InnerKeySelector.Parameters.[0]
                    let rewrittenProjection = rt.ReplaceExpression (groupCall.Call :> Expression) (innerParam :> Expression) projectionBody
                    let projExpr = rt.TranslateJoinExpr projCtx rowsetAlias rt.Vars (Some innerParam) rewrittenProjection
                    projExpr, rt.MaterializeDiscoveredJoins projCtx.Joins None None
            let rowsetValueCore orderBy limit offset =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = rowValueExpr }]
                  Source = Some(DerivedTable(rowsetSel, rowsetAlias))
                  Joins = rowValueJoins
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = orderBy
                  Limit = limit
                  Offset = offset }
            let mkValue core = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
            let noElements = "Sequence contains no elements"
            let manyElements = "Sequence contains more than one element"
            let outOfRange = "Index was out of range. Must be non-negative and less than the size of the collection."
            let ascOrd = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Asc }]
            let descOrd = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Desc }]
            match groupCall.Kind with
            | FirstLike orDefault ->
                let valueCore = rowsetValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None
                let error =
                    if orDefault then None
                    else
                        let countExpr = buildCountSelectSubquery rt rowsetSel (Some 1)
                        Some (SqlExpr.CaseExpr(
                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr noElements),
                            [],
                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                { Value = mkValue valueCore; Error = error }
            | LastLike orDefault ->
                let valueCore = rowsetValueCore descOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None
                let error =
                    if orDefault then None
                    else
                        let countExpr = buildCountSelectSubquery rt rowsetSel (Some 1)
                        Some (SqlExpr.CaseExpr(
                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr noElements),
                            [],
                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                { Value = mkValue valueCore; Error = error }
            | SingleLike orDefault ->
                let valueCore = rowsetValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None
                let countExpr = buildCountSelectSubquery rt rowsetSel (Some 2)
                let error =
                    if orDefault then
                        Some (SqlExpr.CaseExpr(
                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), rt.ErrorExpr manyElements),
                            [],
                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                    else
                        Some (SqlExpr.CaseExpr(
                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr noElements),
                            [ (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), rt.ErrorExpr manyElements) ],
                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                { Value = mkValue valueCore; Error = error }
            | ElementAtLike(indexExpr, orDefault) ->
                let idx = Convert.ToInt64(QueryTranslator.evaluateExpr<obj> indexExpr)
                if idx < 0L then
                    { Value = SqlExpr.Literal(SqlLiteral.Null)
                      Error = if orDefault then None else Some(rt.ErrorExpr outOfRange) }
                else
                    let valueCore =
                        rowsetValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) (Some (SqlExpr.Literal(SqlLiteral.Integer idx)))
                    let error =
                        if orDefault then None
                        else
                            let countExpr = buildCountSubquery rt valueCore None
                            Some (SqlExpr.CaseExpr(
                                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr outOfRange),
                                [],
                                Some(SqlExpr.Literal(SqlLiteral.Null))))
                    { Value = mkValue valueCore; Error = error }
        else
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
            let noElements = "Sequence contains no elements"
            let manyElements = "Sequence contains more than one element"
            let outOfRange = "Index was out of range. Must be non-negative and less than the size of the collection."
            match groupCall.Kind with
            | FirstLike _ ->
                translatedArg (mkValue { baseScalarCore with Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) })
            | LastLike orDefault ->
                let valueCore =
                    { baseScalarCore with
                        OrderBy = descIdOrder
                        Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
                let error =
                    if orDefault then None
                    else
                        let countExpr = buildCountSubquery rt valueCore (Some 1)
                        Some (SqlExpr.CaseExpr(
                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr noElements),
                            [],
                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                { Value = mkValue valueCore; Error = error }
            | SingleLike orDefault ->
                let valueCore =
                    { baseScalarCore with
                        OrderBy = ascIdOrder
                        Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
                let countExpr = buildCountSubquery rt { baseScalarCore with OrderBy = ascIdOrder } (Some 2)
                let error =
                    if orDefault then
                        Some (SqlExpr.CaseExpr(
                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), rt.ErrorExpr manyElements),
                            [],
                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                    else
                        Some (SqlExpr.CaseExpr(
                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr noElements),
                            [ (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), rt.ErrorExpr manyElements) ],
                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                { Value = mkValue valueCore; Error = error }
            | ElementAtLike(indexExpr, orDefault) ->
                let idx = Convert.ToInt64(QueryTranslator.evaluateExpr<obj> indexExpr)
                if idx < 0L then
                    { Value = SqlExpr.Literal(SqlLiteral.Null)
                      Error = if orDefault then None else Some(rt.ErrorExpr outOfRange) }
                else
                    let valueCore =
                        { baseScalarCore with
                            OrderBy = ascIdOrder
                            Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L))
                            Offset = Some (SqlExpr.Literal(SqlLiteral.Integer idx)) }
                    let error =
                        if orDefault then None
                        else
                            let countExpr = buildCountSubquery rt valueCore None
                            Some (SqlExpr.CaseExpr(
                                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), rt.ErrorExpr outOfRange),
                                [],
                                Some(SqlExpr.Literal(SqlLiteral.Null))))
                    { Value = mkValue valueCore; Error = error }

    let tryTranslateGroupFirstLikeNullComparison (rt: GroupJoinRuntime) (expr: BinaryExpression) =
        let tryBuild groupExpr nullExpr nodeType =
            match tryMatchGroupElementCall rt groupExpr with
            | Some groupCall when isNullConstant nullExpr ->
                let existsExpr =
                    match groupCall.Kind with
                    | FirstLike _
                    | LastLike _
                    | ElementAtLike(_, true) ->
                        match buildGroupElementSubquery rt groupCall (Expression.Constant(1L) :> Expression) with
                        | { Value = SqlExpr.ScalarSubquery select } -> Some (SqlExpr.Exists select)
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
