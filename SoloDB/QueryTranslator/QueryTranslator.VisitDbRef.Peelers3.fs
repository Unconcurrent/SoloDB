namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitDbRefPeelers
open SoloDatabase.QueryTranslatorVisitDbRefPeelers2
open SoloDatabase.QueryTranslatorVisitPostJoin
open DBRefTypeHelpers
open Utils

/// Set-op helpers, GroupBy translator, and filtered builders.
module internal QueryTranslatorVisitDbRefPeelers3 =
    let internal nullSafeEq (left: SqlExpr) (right: SqlExpr) : SqlExpr =
        SqlExpr.Binary(left, BinaryOperator.Is, right)

    /// Check whether a source expression is a set operation (Intersect/Except/Union/Concat)
    /// on two Select-projected DBRefMany operands.
    let internal tryMatchSetOperation (expr: Expression) : (string * Expression * Expression) voption =
        match expr with
        | :? MethodCallExpression as mce
            when mce.Method.Name = "Intersect"
              || mce.Method.Name = "Except"
              || mce.Method.Name = "Union"
              || mce.Method.Name = "Concat" ->
            let leftSource =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let rightSource =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then mce.Arguments.[0] else null
                elif mce.Arguments.Count >= 2 then mce.Arguments.[1]
                else null
            if isNull leftSource || isNull rightSource then ValueNone
            else ValueSome (mce.Method.Name, leftSource, rightSource)
        | _ -> ValueNone

    /// Peel .GroupBy(keySelector) from a source expression.
    /// Returns (innerExpr, keySelector lambda) if the source is a GroupBy on a DBRefMany chain.
    let internal tryPeelGroupByFromSource (expr: Expression) : (Expression * LambdaExpression) voption =
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "GroupBy" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let keyExpr =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
                elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
                else None
            if isNull sourceExpr then ValueNone
            else
                match keyExpr with
                | Some ke ->
                    match tryExtractLambdaExpression ke with
                    | ValueSome keyLambda -> ValueSome (sourceExpr, keyLambda)
                    | ValueNone -> ValueNone
                | None -> ValueNone
        | _ -> ValueNone

    /// Translate an IGrouping predicate body (g => g.Count() > N) to a HAVING DU expression.
    /// Recognizes g.Count(), g.Sum(sel), g.Min(sel), g.Max(sel), g.Average(sel), g.Key,
    /// and binary comparisons/logic.
    let internal translateGroupingPredicate (qb: QueryBuilder) (tgtAlias: string) (targetTable: string) (groupKeyDu: SqlExpr) (groupParam: ParameterExpression) (body: Expression) : SqlExpr =
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
                // g.Key → the GROUP BY key expression
                if me.Member.Name = "Key" && me.Expression :? ParameterExpression then
                    let pe = me.Expression :?> ParameterExpression
                    if System.Object.ReferenceEquals(pe, groupParam) then
                        groupKeyDu
                    else visitDu e qb
                else visitDu e qb
            | :? MethodCallExpression as mc ->
                // Group aggregate methods: g.Count(), g.Sum(sel), etc.
                let isGroupMethod =
                    let source =
                        if not (isNull mc.Object) then mc.Object
                        elif mc.Arguments.Count > 0 then mc.Arguments.[0]
                        else null
                    if isNull source then false
                    else
                        match unwrapConvert source with
                        | :? ParameterExpression as pe -> System.Object.ReferenceEquals(pe, groupParam)
                        | _ -> false
                if isGroupMethod then
                    match mc.Method.Name with
                    | "Count" ->
                        // g.Count() → COUNT(*)
                        SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
                    | "Sum" | "Min" | "Max" | "Average" ->
                        let aggKind =
                            match mc.Method.Name with
                            | "Sum" -> AggregateKind.Sum
                            | "Min" -> AggregateKind.Min
                            | "Max" -> AggregateKind.Max
                            | "Average" -> AggregateKind.Avg
                            | _ -> failwith "unreachable"
                        // Extract selector: g.Sum(x => x.Field)
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
                            let aggExpr = SqlExpr.AggregateCall(aggKind, Some selectorDu, false, None)
                            DBRefManyHelpers.wrapAggregateEmptySemantics aggKind true aggExpr
                        | ValueNone ->
                            raise (NotSupportedException("Cannot extract selector for GroupBy aggregate."))
                    | "LongCount" ->
                        SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
                    | other ->
                        raise (NotSupportedException(sprintf "Unsupported group method: %s. Only Count, Sum, Min, Max, Average are admitted." other))
                else visitDu e qb
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
                SqlExpr.Unary(UnaryOperator.Not, visit ue.Operand)
            | _ -> visitDu e qb
        visit body

    /// Build a filtered COUNT subquery for DBRefMany.Where(pred).Count().
    /// Emits: ScalarSubquery(SELECT COUNT(*) FROM link JOIN target WHERE ownerLink AND pred1 AND pred2 ... [ORDER BY key])
    /// sortKeys are emitted faithfully even though ordering is vacuous for COUNT.
    let internal buildFilteredCountSubquery (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (predicateExprs: Expression list) (sortKeys: (Expression * SortDirection) list) (ofTypeName: string option) : SqlExpr =
        let propName = ownerRef.PropertyExpr.Member.Name
        let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
        let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
        let tgtAlias = sprintf "_tgt%d" aliasId
        let lnkAlias = sprintf "_lnk%d" aliasId

        let predicateDus = buildFilteredPredicateDus qb tgtAlias targetTable predicateExprs
        let sortKeyDus = if sortKeys.IsEmpty then [] else buildSortKeyDus qb tgtAlias targetTable sortKeys

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"),
                BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn),
                BinaryOperator.Eq,
                SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
        let fullWhere =
            let withPreds = DBRefManyHelpers.appendPredicatesWithAnd ownerWhere predicateDus
            match ofTypeName with
            | Some tn -> SqlExpr.Binary(withPreds, BinaryOperator.And, buildOfTypePredicate tgtAlias tn)
            | None -> withPreds
        let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
        let core =
            { mkSubCore countProj (Some(BaseTable(linkTable, Some lnkAlias))) (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                OrderBy = sortKeyDus }
        let subSelect = { Ctes = []; Body = SingleSelect core }
        SqlExpr.ScalarSubquery subSelect

    /// Build a filtered EXISTS subquery for DBRefMany.Where(pred).Any().
    /// sortKeys are emitted faithfully even though ordering is vacuous for EXISTS.
    let internal buildFilteredExistsSubquery (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (predicateExprs: Expression list) (sortKeys: (Expression * SortDirection) list) (ofTypeName: string option) : SqlExpr =
        let propName = ownerRef.PropertyExpr.Member.Name
        let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
        let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
        let tgtAlias = sprintf "_tgt%d" aliasId
        let lnkAlias = sprintf "_lnk%d" aliasId
        let predicateDus = buildFilteredPredicateDus qb tgtAlias targetTable predicateExprs
        let sortKeyDus = if sortKeys.IsEmpty then [] else buildSortKeyDus qb tgtAlias targetTable sortKeys

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"),
                BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn),
                BinaryOperator.Eq,
                SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
        let fullWhere =
            let withPreds = DBRefManyHelpers.appendPredicatesWithAnd ownerWhere predicateDus
            match ofTypeName with
            | Some tn -> SqlExpr.Binary(withPreds, BinaryOperator.And, buildOfTypePredicate tgtAlias tn)
            | None -> withPreds
        let core =
            { mkSubCore
                [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                (Some(BaseTable(linkTable, Some lnkAlias)))
                (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                OrderBy = sortKeyDus }
        let subSelect = { Ctes = []; Body = SingleSelect core }
        SqlExpr.Exists subSelect

    /// preExpressionHandler for DBRefMany.Count, Any(), Any(pred) as correlated subqueries.
