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
    [<Literal>]
    let multipleTakeSkipBoundariesMessage =
        "Error: Multiple Take/Skip boundaries in DBRefMany query are not supported.\nReason: The descriptor model admits only one semantic pagination boundary.\nFix: Keep at most one Take or Skip in the DBRefMany chain, or move additional pagination after AsEnumerable()."

    [<Literal>]
    let takeWhileOrderingRequiredMessage =
        "Error: TakeWhile/SkipWhile requires explicit ordering.\nReason: TakeWhile/SkipWhile needs a deterministic row order.\nFix: Add .OrderBy() before .TakeWhile(), or set OrderBy on the [SoloRef] attribute."

    let appendPredicatesWithAnd (head: SqlExpr) (tail: SqlExpr list) =
        tail |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) head

    let foldPredicatesWithAnd (predicates: SqlExpr list) =
        match predicates with
        | [] -> None
        | h :: t -> Some(appendPredicatesWithAnd h t)

    let wrapAggregateEmptySemantics (aggKind: AggregateKind) (insideJsonObjectProjection: bool) (scalarExpr: SqlExpr) : SqlExpr =
        if aggKind = AggregateKind.Sum then
            SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
        elif insideJsonObjectProjection then
            scalarExpr
        else
            SqlExpr.CaseExpr(
                (SqlExpr.Unary(UnaryOperator.IsNull, scalarExpr),
                 SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains no elements")),
                [],
                Some scalarExpr)

    let buildTakeWhileCfFilter (alias: string) (isTakeWhile: bool) =
        if isTakeWhile then
            SqlExpr.Binary(SqlExpr.Column(Some alias, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
        else
            SqlExpr.Binary(SqlExpr.Column(Some alias, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))

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
    let translateGroupingPredicate (qb: QueryBuilder) (tgtAlias: string) (targetTable: string) (groupKeyDu: SqlExpr) (groupParam: ParameterExpression) (body: Expression) (aggregateJoinEdges: ResizeArray<JoinEdge>) : SqlExpr =
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
            | :? NewExpression as ne when not (isNull ne.Members) ->
                SqlExpr.JsonObjectExpr(
                    [ for i in 0 .. ne.Arguments.Count - 1 ->
                        ne.Members.[i].Name, visit ne.Arguments.[i] ])
            | :? MemberInitExpression as mi ->
                SqlExpr.JsonObjectExpr(
                    [ for binding in mi.Bindings do
                        match binding with
                        | :? MemberAssignment as ma ->
                            yield ma.Member.Name, visit ma.Expression
                        | _ ->
                            raise (NotSupportedException("GroupBy projection supports only member assignments.")) ])
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
                            let subQb = qb.ForSubquery(tgtAlias, selectorLambda, subqueryRootTable = targetTable)
                            let selectorDu = visitDu selectorLambda.Body subQb
                            aggregateJoinEdges.AddRange(subQb.SourceContext.Joins)
                            let aggExpr = SqlExpr.AggregateCall(aggKind, Some selectorDu, false, None)
                            wrapAggregateEmptySemantics aggKind true aggExpr
                        | ValueNone ->
                            raise (NotSupportedException("Cannot extract selector for GroupBy aggregate."))
                    | other ->
                        raise (NotSupportedException(sprintf "Unsupported group method: %s" other))
                else visitDu e qb
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
                SqlExpr.Unary(UnaryOperator.Not, visit ue.Operand)
            | _ -> visitDu e qb
        visit body
