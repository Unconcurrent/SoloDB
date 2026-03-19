namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost

/// Shared helpers for DBRefMany query translation.
/// Used by both the unified Builder and the legacy handler.
module internal DBRefManyHelpers =

    let joinEdgesToClauses (edges: ResizeArray<JoinEdge>) : JoinShape list =
        [ for j in edges ->
            ConditionedJoin(
                Left,
                BaseTable(j.TargetTable, Some j.TargetAlias),
                SqlExpr.Binary(
                    SqlExpr.Column(Some j.TargetAlias, "Id"),
                    BinaryOperator.Eq,
                    SqlExpr.JsonExtractExpr(j.OnSourceAlias, "Value", JsonPath(j.OnPropertyName, [])))) ]

    let ensureOfTypeSupported (targetType: Type) =
        if not (JsonFunctions.mustIncludeTypeInformationInSerializationFn targetType) then
            raise (NotSupportedException(
                "Error: OfType requires type discriminator information.\nReason: The DBRefMany target type does not store $type metadata.\nFix: Add the Polymorphic attribute to the base type and reinsert all elements."))

    /// Translate an IGrouping predicate body (g => g.Count() > N) to a HAVING DU expression.
    /// Recognizes g.Count(), g.Sum(sel), g.Min(sel), g.Max(sel), g.Average(sel), g.Key,
    /// and binary comparisons/logic.
    let translateGroupingPredicate (qb: QueryBuilder) (tgtAlias: string) (groupKeyDu: SqlExpr) (groupParam: ParameterExpression) (body: Expression) : SqlExpr =
        let rec visit (e: Expression) : SqlExpr =
            let e = unwrapConvert e
            match e with
            | :? BinaryExpression as be ->
                let op =
                    match be.NodeType with
                    | ExpressionType.GreaterThan -> BinaryOperator.Gt
                    | ExpressionType.GreaterThanOrEqual -> BinaryOperator.Ge
                    | ExpressionType.LessThan -> BinaryOperator.Lt
                    | ExpressionType.LessThanOrEqual -> BinaryOperator.Le
                    | ExpressionType.Equal -> BinaryOperator.Eq
                    | ExpressionType.NotEqual -> BinaryOperator.Ne
                    | ExpressionType.AndAlso -> BinaryOperator.And
                    | ExpressionType.OrElse -> BinaryOperator.Or
                    | _ -> raise (NotSupportedException(sprintf "Unsupported binary operator in GroupBy predicate: %A" be.NodeType))
                SqlExpr.Binary(visit be.Left, op, visit be.Right)
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? int as v -> SqlExpr.Literal(SqlLiteral.Integer(int64 v))
                | :? int64 as v -> SqlExpr.Literal(SqlLiteral.Integer v)
                | :? double as v -> SqlExpr.Literal(SqlLiteral.Float v)
                | :? string as v -> SqlExpr.Literal(SqlLiteral.String v)
                | :? bool as v -> SqlExpr.Literal(SqlLiteral.Integer(if v then 1L else 0L))
                | _ -> visitDu e qb
            | :? MemberExpression as me ->
                if me.Member.Name = "Key" && me.Expression :? ParameterExpression then
                    let pe = me.Expression :?> ParameterExpression
                    if Object.ReferenceEquals(pe, groupParam) then groupKeyDu
                    else visitDu e qb
                else visitDu e qb
            | :? MethodCallExpression as mc ->
                let isGroupMethod =
                    let source =
                        if not (isNull mc.Object) then mc.Object
                        elif mc.Arguments.Count > 0 then mc.Arguments.[0]
                        else null
                    if isNull source then false
                    else
                        match unwrapConvert source with
                        | :? ParameterExpression as pe -> Object.ReferenceEquals(pe, groupParam)
                        | _ -> false
                if isGroupMethod then
                    match mc.Method.Name with
                    | "Count" | "LongCount" ->
                        SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
                    | "Sum" | "Min" | "Max" | "Average" ->
                        let aggKind =
                            match mc.Method.Name with
                            | "Sum" -> AggregateKind.Sum
                            | "Min" -> AggregateKind.Min
                            | "Max" -> AggregateKind.Max
                            | "Average" -> AggregateKind.Avg
                            | _ -> failwith "unreachable"
                        let selectorExpr =
                            if not (isNull mc.Object) then
                                if mc.Arguments.Count >= 1 then mc.Arguments.[0] else null
                            elif mc.Arguments.Count >= 2 then mc.Arguments.[1]
                            else null
                        if isNull selectorExpr then
                            raise (NotSupportedException(sprintf "GroupBy aggregate %s requires a selector lambda." mc.Method.Name))
                        match tryExtractLambdaExpression selectorExpr with
                        | ValueSome selectorLambda ->
                            let subQb = qb.ForSubquery(tgtAlias, selectorLambda)
                            let selectorDu = visitDu selectorLambda.Body subQb
                            let aggExpr = SqlExpr.AggregateCall(aggKind, Some selectorDu, false, None)
                            if mc.Method.Name = "Sum" then
                                SqlExpr.Coalesce(aggExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                            else aggExpr
                        | ValueNone ->
                            raise (NotSupportedException("Cannot extract selector for GroupBy aggregate."))
                    | other ->
                        raise (NotSupportedException(sprintf "Unsupported group method: %s" other))
                else visitDu e qb
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
                SqlExpr.Unary(UnaryOperator.Not, visit ue.Operand)
            | _ -> visitDu e qb
        visit body
