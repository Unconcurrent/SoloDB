namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open SQLiteTools
open Utils
open SoloDatabase
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

/// GroupJoin handler — extracted from PartB for file size compliance.
module internal QueryableBuildQueryPartBGroupJoin =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableHelperBase

    let internal applyGroupJoin<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> SqlSelect)
        (expressions: Expression array) =
        match expressions.Length with
        | 4 ->
            let innerExpression = expressions.[0]
            let outerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin outer key selector" expressions.[1]
            let innerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin inner key selector" expressions.[2]
            let resultSelector = unwrapLambdaExpressionOrThrow "GroupJoin result selector" expressions.[3]

            if isCompositeJoinKeyBody outerKeySelector.Body || isCompositeJoinKeyBody innerKeySelector.Body then
                raise (NotSupportedException(
                    "Error: GroupJoin composite key selectors are not supported.\n" +
                    "Reason: Anonymous-type and composite key equality lowering is deferred.\n" +
                    "Fix: Join on a single scalar key or move the query after AsEnumerable()."))

            // Find the root table for the inner source (needed for QueryContext).
            let innerRootTable =
                match tryGetJoinRootSourceTable innerExpression with
                | Some tn -> tn
                | None ->
                    raise (NotSupportedException(
                        "Error: GroupJoin inner source is not supported.\n" +
                        "Reason: The inner query does not resolve to a SoloDB root collection.\n" +
                        "Fix: Use another SoloDB IQueryable rooted in a collection or move the query after AsEnumerable()."))

            if resultSelector.Parameters.Count <> 2 then
                raise (NotSupportedException(
                    "Error: GroupJoin result selector must have two parameters (outer, group).\n" +
                    "Reason: The result selector shape is not recognized.\n" +
                    "Fix: Use (outer, group) => new { ... } pattern."))

            let outerParam = resultSelector.Parameters.[0]
            let groupParam = resultSelector.Parameters.[1]
            // Capture the inner expression for building a subquery inside addComplexFinal.
            let capturedInnerExpr = innerExpression

            addComplexFinal statements (fun ctx ->
                let outerAlias = "o"
                let innerAlias = "gj"
                let innerCtx = QueryContext.SingleSource(innerRootTable)

                // Build the inner source as a full subquery (supports composed inner sources with Where, etc.)
                let innerSelect = translateQueryFn innerCtx ctx.Vars capturedInnerExpr
                let innerSource = DerivedTable(innerSelect, innerAlias)

                let outerKeyExpr =
                    translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body
                // Translate inner key against the inner alias (the DerivedTable exposes Id + Value columns).
                let innerKeyExpr =
                    translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body

                // Translate the result selector body. Outer references → outer table.
                // Group references → SQL aggregates over the inner table.
                let rec translateGroupJoinArg (expr: Expression) : SqlExpr =
                    match expr with
                    // outer.Prop — direct outer member access
                    | :? MemberExpression as me when not (isNull me.Expression) && me.Expression :? ParameterExpression && (me.Expression :?> ParameterExpression) = outerParam ->
                        translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerParam) expr
                    // group.Count() — parameterless
                    | :? MethodCallExpression as mc when mc.Method.Name = "Count" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None)
                    // group.Count(predicate) — with predicate
                    | :? MethodCallExpression as mc when mc.Method.Name = "Count" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        let pred = unwrapLambdaExpressionOrThrow "GroupJoin Count predicate" mc.Arguments.[1]
                        let predDu = translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                        SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None)
                    // group.LongCount() — parameterless
                    | :? MethodCallExpression as mc when mc.Method.Name = "LongCount" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None)
                    // group.Sum(selector)
                    | :? MethodCallExpression as mc when mc.Method.Name = "Sum" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        let sel = unwrapLambdaExpressionOrThrow "GroupJoin Sum selector" mc.Arguments.[1]
                        let selDu = translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body
                        SqlExpr.Coalesce(SqlExpr.AggregateCall(AggregateKind.Sum, Some selDu, false, None), [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                    // group.Min(selector)
                    | :? MethodCallExpression as mc when mc.Method.Name = "Min" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        let sel = unwrapLambdaExpressionOrThrow "GroupJoin Min selector" mc.Arguments.[1]
                        SqlExpr.AggregateCall(AggregateKind.Min, Some(translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None)
                    // group.Max(selector)
                    | :? MethodCallExpression as mc when mc.Method.Name = "Max" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        let sel = unwrapLambdaExpressionOrThrow "GroupJoin Max selector" mc.Arguments.[1]
                        SqlExpr.AggregateCall(AggregateKind.Max, Some(translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None)
                    // group.Average(selector)
                    | :? MethodCallExpression as mc when mc.Method.Name = "Average" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        let sel = unwrapLambdaExpressionOrThrow "GroupJoin Average selector" mc.Arguments.[1]
                        SqlExpr.AggregateCall(AggregateKind.Avg, Some(translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None)
                    // group.Any() — parameterless
                    | :? MethodCallExpression as mc when mc.Method.Name = "Any" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
                    // group.Any(predicate) — with predicate
                    | :? MethodCallExpression as mc when mc.Method.Name = "Any" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        let pred = unwrapLambdaExpressionOrThrow "GroupJoin Any predicate" mc.Arguments.[1]
                        let predDu = translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                        SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
                    // group.All(predicate)
                    | :? MethodCallExpression as mc when mc.Method.Name = "All" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                        let pred = unwrapLambdaExpressionOrThrow "GroupJoin All predicate" mc.Arguments.[1]
                        let predDu = translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                        SqlExpr.Binary(
                            SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.Not, predDu), SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None),
                            BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                    // Constant / no-group expression
                    | :? ConstantExpression ->
                        translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars None expr
                    // Conditional (ternary)
                    | :? ConditionalExpression as ce ->
                        SqlExpr.CaseExpr((translateGroupJoinArg ce.Test, translateGroupJoinArg ce.IfTrue), [], Some(translateGroupJoinArg ce.IfFalse))
                    // Binary
                    | :? BinaryExpression as be ->
                        let op =
                            match be.NodeType with
                            | ExpressionType.Add -> BinaryOperator.Add
                            | ExpressionType.Subtract -> BinaryOperator.Sub
                            | ExpressionType.Multiply -> BinaryOperator.Mul
                            | ExpressionType.Divide -> BinaryOperator.Div
                            | ExpressionType.Modulo -> BinaryOperator.Mod
                            | ExpressionType.Equal -> BinaryOperator.Eq
                            | ExpressionType.NotEqual -> BinaryOperator.Ne
                            | ExpressionType.GreaterThan -> BinaryOperator.Gt
                            | ExpressionType.GreaterThanOrEqual -> BinaryOperator.Ge
                            | ExpressionType.LessThan -> BinaryOperator.Lt
                            | ExpressionType.LessThanOrEqual -> BinaryOperator.Le
                            | ExpressionType.AndAlso -> BinaryOperator.And
                            | ExpressionType.OrElse -> BinaryOperator.Or
                            | _ -> raise (NotSupportedException($"GroupJoin result selector binary operator {be.NodeType} not supported."))
                        SqlExpr.Binary(translateGroupJoinArg be.Left, op, translateGroupJoinArg be.Right)
                    // Fallback — try outer translation
                    | _ ->
                        translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerParam) expr

                // Build the result projection from the selector body.
                // Use json_object (not jsonb_object) so downstream jsonb_extract returns typed SQL values.
                let buildJsonObject (pairs: (string * SqlExpr) list) =
                    let args = pairs |> List.collect (fun (name, expr) -> [SqlExpr.Literal(SqlLiteral.String name); expr])
                    SqlExpr.FunctionCall("json_object", args)

                let resultExpr =
                    match resultSelector.Body with
                    | :? NewExpression as newExpr when not (isNull newExpr.Members) ->
                        let memberNames =
                            newExpr.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                        buildJsonObject
                            [ for i in 0 .. newExpr.Arguments.Count - 1 ->
                                memberNames.[i], translateGroupJoinArg newExpr.Arguments.[i] ]
                    | :? MemberInitExpression as mi ->
                        buildJsonObject
                            [ for binding in mi.Bindings do
                                match binding with
                                | :? MemberAssignment as ma ->
                                    yield ma.Member.Name, translateGroupJoinArg ma.Expression
                                | _ ->
                                    raise (NotSupportedException("GroupJoin result selector: only member assignments supported.")) ]
                    | _ ->
                        raise (NotSupportedException(
                            "Error: GroupJoin result selector must produce an anonymous type or object initializer.\n" +
                            "Reason: Scalar result selectors are not supported for GroupJoin.\n" +
                            "Fix: Use new { ... } in the result selector."))

                // Collect DBRef JOINs discovered during inner aggregate translation.
                // Rewrite OnSourceAlias: root table references → DerivedTable alias.
                let quotedInnerRoot = "\"" + innerRootTable + "\""
                let discoveredJoins =
                    innerCtx.Joins
                    |> Seq.map (fun j ->
                        let rewrittenSource =
                            match j.OnSourceAlias with
                            | Some src when src = quotedInnerRoot || src = innerRootTable -> Some innerAlias
                            | other -> other
                        ConditionedJoin(
                            parseJoinKind j.JoinKind,
                            BaseTable(j.TargetTable, Some j.TargetAlias),
                            SqlExpr.Binary(
                                SqlExpr.Column(Some j.TargetAlias, "Id"),
                                BinaryOperator.Eq,
                                SqlExpr.JsonExtractExpr(rewrittenSource, "Value", JsonPath(j.OnPropertyName, [])))))
                    |> Seq.toList

                // LEFT JOIN inner subquery + discovered DBRef JOINs + GROUP BY outer.Id, outer.Value
                let allJoins =
                    [ConditionedJoin(
                        JoinKind.Left,
                        innerSource,
                        SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerKeyExpr))]
                    @ discoveredJoins
                let core =
                    { mkCore
                        [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                         { Alias = Some "Value"; Expr = resultExpr }]
                        (Some (DerivedTable(ctx.Inner, outerAlias)))
                      with
                          Joins = allJoins
                          GroupBy = [SqlExpr.Column(Some outerAlias, "Id"); SqlExpr.Column(Some outerAlias, "Value")] }
                wrapCore core
            )
        | other ->
            raise (NotSupportedException(sprintf "Invalid number of arguments in GroupJoin: %A" other))
