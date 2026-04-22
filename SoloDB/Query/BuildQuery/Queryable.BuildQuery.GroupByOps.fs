namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

/// GroupBy deferred emission + GroupBy+Select fusion for root-level queries.
/// Separated from SequenceOps to keep files under 400 lines.
module internal QueryableBuildQueryGroupByOps =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase

    let internal applyGroupByKeyOnly<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>) (expressions: Expression array) =
        addLoweredKeySelector statements (lowerKeySelectorLambda sourceCtx tableName expressions.[0] GroupByKey)

    // ── GroupBy aggregate translation helpers ──

    let private extractLambdaFromExpr (expr: Expression) : LambdaExpression =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote -> ue.Operand :?> LambdaExpression
        | :? LambdaExpression as le -> le
        | _ -> raise (NotSupportedException($"Expected lambda expression, got {expr.NodeType}"))

    let private isGroupSource (expr: Expression) (groupParam: ParameterExpression) =
        match expr with
        | :? ParameterExpression as p -> obj.ReferenceEquals(p, groupParam)
        | _ -> false

    let private tryTranslateProjectedAggregate
        (sourceCtx: QueryContext) (ctxTableName: string) (groupRowTableName: string) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (source: Expression) (aggKind: AggregateKind) (coalesceZero: bool) : SqlExpr option =
        let isDistinct, innerSource =
            match source with
            | :? MethodCallExpression as mc when mc.Method.Name = "Distinct" && mc.Arguments.Count = 1 -> true, mc.Arguments.[0]
            | _ -> false, source
        let projLambda, innerSource2 =
            match innerSource with
            | :? MethodCallExpression as mc when mc.Method.Name = "Select" && mc.Arguments.Count = 2 ->
                Some (extractLambdaFromExpr mc.Arguments.[1]), mc.Arguments.[0]
            | _ -> None, innerSource
        let whereLambda, innerSource3 =
            match innerSource2 with
            | :? MethodCallExpression as mc when mc.Method.Name = "Where" && mc.Arguments.Count = 2 ->
                Some (extractLambdaFromExpr mc.Arguments.[1]), mc.Arguments.[0]
            | _ -> None, innerSource2
        if not (isGroupSource innerSource3 groupParam) then None
        else
        let translateInner (lambda: LambdaExpression) =
            translateExprDu sourceCtx groupRowTableName (lambda :> Expression) vars
        let argExpr =
            match projLambda, whereLambda with
            | Some proj, Some where -> SqlExpr.CaseExpr((translateInner where, translateInner proj), [], None)
            | Some proj, None -> translateInner proj
            | None, Some where -> SqlExpr.CaseExpr((translateInner where, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], None)
            | None, None ->
                if aggKind = AggregateKind.Count then SqlExpr.Literal(SqlLiteral.Integer 1L)
                else raise (NotSupportedException("Projected aggregate without Select is not supported."))
        let aggArg =
            if aggKind = AggregateKind.Count && projLambda.IsNone && whereLambda.IsNone then None
            else Some argExpr
        let aggExpr = SqlExpr.AggregateCall(aggKind, aggArg, isDistinct, None)
        if coalesceZero then Some (SqlExpr.Coalesce(aggExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)]))
        else Some aggExpr

    let rec private tryTranslateGroupArg
        (sourceCtx: QueryContext) (ctxTableName: string) (groupRowTableName: string) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (expr: Expression) : SqlExpr option =
        match expr with
        | :? MemberExpression as me when not (isNull me.Expression) && me.Expression :? ParameterExpression && obj.ReferenceEquals(me.Expression, groupParam) && me.Member.Name = "Key" ->
            Some (SqlExpr.Column(Some "o", "__solodb_group_key"))
        | :? MemberExpression as me when not (isNull me.Expression) ->
            match tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars me.Expression with
            | Some receiverExpr ->
                Some (DateTimeFunctions.translateGroupKeyMemberAccess receiverExpr me.Expression.Type me.Member.Name)
            | None -> None
        | :? NewExpression as ne when ne.Type = typeof<DateTime> && ne.Arguments.Count = 3 && referencesParam groupParam ne ->
            let translated = ne.Arguments |> Seq.toList |> List.map (tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars)
            match translated with
            | [Some y; Some m; Some d] ->
                Some (SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%04d-%02d-%02d"); y; m; d]))
            | _ -> None
        | :? MethodCallExpression as mc when referencesParam groupParam mc ->
            match mc.Method.Name with
            | "ToString" when mc.Arguments.Count <= 1 && not (isNull mc.Object)
                            && (mc.Object.Type = typeof<DateTime> || mc.Object.Type = typeof<DateTimeOffset>) ->
                match tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars mc.Object with
                | Some objExpr ->
                    let fmtOpt =
                        if mc.Arguments.Count = 0 then Some "G"
                        else DateTimeFunctions.tryExtractConstantFormat mc.Arguments.[0]
                    match fmtOpt with
                    | None -> raise (NotSupportedException "DateTime.ToString(format): the format argument must be a compile-time constant for SQL translation. Use a string literal, or call AsEnumerable() before ToString to evaluate client-side.")
                    | Some fmtStr ->
                        let mode =
                            match mc.Object with
                            | :? NewExpression as ne2 when ne2.Type = typeof<DateTime> -> DateTimeTranslationMode.FromIsoString
                            | _ -> DateTimeTranslationMode.FromEpoch mc.Object.Type
                        Some (DateTimeFunctions.translateDateTimeToString objExpr mode fmtStr)
                | None -> None
            | "Count" when mc.Arguments.Count = 1 && isGroupSource mc.Arguments.[0] groupParam ->
                Some (SqlExpr.AggregateCall(AggregateKind.Count, None, false, None))
            | "Count" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let pred = extractLambdaFromExpr mc.Arguments.[1]
                let predDu = translateExprDu sourceCtx groupRowTableName (pred :> Expression) vars
                Some (SqlExpr.AggregateCall(AggregateKind.Sum, Some (SqlExpr.CaseExpr(
                    (predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None))
            | "Sum" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let sel = extractLambdaFromExpr mc.Arguments.[1]
                let selDu = translateExprDu sourceCtx groupRowTableName (sel :> Expression) vars
                Some (SqlExpr.Coalesce(SqlExpr.AggregateCall(AggregateKind.Sum, Some selDu, false, None), [SqlExpr.Literal(SqlLiteral.Integer 0L)]))
            | "Min" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let sel = extractLambdaFromExpr mc.Arguments.[1]
                Some (SqlExpr.AggregateCall(AggregateKind.Min, Some (translateExprDu sourceCtx groupRowTableName (sel :> Expression) vars), false, None))
            | "Max" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let sel = extractLambdaFromExpr mc.Arguments.[1]
                Some (SqlExpr.AggregateCall(AggregateKind.Max, Some (translateExprDu sourceCtx groupRowTableName (sel :> Expression) vars), false, None))
            | "Average" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let sel = extractLambdaFromExpr mc.Arguments.[1]
                Some (SqlExpr.AggregateCall(AggregateKind.Avg, Some (translateExprDu sourceCtx groupRowTableName (sel :> Expression) vars), false, None))
            | "Sum" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupRowTableName groupParam vars mc.Arguments.[0] AggregateKind.Sum true
            | "Count" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupRowTableName groupParam vars mc.Arguments.[0] AggregateKind.Count false
            | "Min" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupRowTableName groupParam vars mc.Arguments.[0] AggregateKind.Min false
            | "Max" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupRowTableName groupParam vars mc.Arguments.[0] AggregateKind.Max false
            | "Average" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupRowTableName groupParam vars mc.Arguments.[0] AggregateKind.Avg false
            | "Any" when mc.Arguments.Count = 1 && isGroupSource mc.Arguments.[0] groupParam ->
                Some (SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Count, None, false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L)))
            | "Items" when mc.Arguments.Count = 1 && isGroupSource mc.Arguments.[0] groupParam ->
                Some (SqlExpr.FunctionCall("jsonb_group_array", [
                    SqlExpr.FunctionCall("jsonb_set", [
                        SqlExpr.Column(Some "o", "Value")
                        SqlExpr.Literal(SqlLiteral.String "$.Id")
                        SqlExpr.Column(Some "o", "Id")
                    ])
                ]))
            | _ -> None
        | :? BinaryExpression as be when referencesParam groupParam be ->
            let left = tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars be.Left
            let right = tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars be.Right
            match left, right with
            | Some l, Some r ->
                let op =
                    match be.NodeType with
                    | ExpressionType.Equal -> BinaryOperator.Eq
                    | ExpressionType.NotEqual -> BinaryOperator.Ne
                    | ExpressionType.GreaterThan -> BinaryOperator.Gt
                    | ExpressionType.GreaterThanOrEqual -> BinaryOperator.Ge
                    | ExpressionType.LessThan -> BinaryOperator.Lt
                    | ExpressionType.LessThanOrEqual -> BinaryOperator.Le
                    | ExpressionType.AndAlso -> BinaryOperator.And
                    | ExpressionType.OrElse -> BinaryOperator.Or
                    | ExpressionType.Add -> BinaryOperator.Add
                    | ExpressionType.Subtract -> BinaryOperator.Sub
                    | ExpressionType.Multiply -> BinaryOperator.Mul
                    | ExpressionType.Divide -> BinaryOperator.Div
                    | ExpressionType.Modulo -> BinaryOperator.Mod
                    | _ -> raise (NotSupportedException($"Binary operator {be.NodeType} not supported in GroupBy HAVING"))
                Some (SqlExpr.Binary(l, op, r))
            | _ -> None
        | :? ConditionalExpression as ce when referencesParam groupParam ce ->
            match tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars ce.Test,
                  tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars ce.IfTrue,
                  tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars ce.IfFalse with
            | Some t, Some tr, Some fa -> Some (SqlExpr.CaseExpr((t, tr), [], Some fa))
            | _ -> None
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not && referencesParam groupParam ue ->
            match tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName groupParam vars ue.Operand with
            | Some inner -> Some (SqlExpr.Unary(UnaryOperator.Not, inner))
            | None -> None
        | :? ConstantExpression as ce ->
            Some (allocateParam vars (box ce.Value))
        | _ when not (referencesParam groupParam expr) ->
            Some (translateExprDu sourceCtx ctxTableName expr vars)
        | _ -> None

    let private translateGroupOrders
        (sourceCtx: QueryContext) (ctxTableName: string) (groupRowTableName: string) (vars: Dictionary<string, obj>)
        (groupOrders: (Expression * bool) list) =
        groupOrders
        |> List.map (fun (orderingExpr, descending) ->
            let lambda = extractLambdaFromExpr orderingExpr
            let orderExpr =
                match tryTranslateGroupArg sourceCtx ctxTableName groupRowTableName lambda.Parameters.[0] vars lambda.Body with
                | Some sqlExpr -> sqlExpr
                | None ->
                    raise (NotSupportedException(
                        "Error: GroupBy ordering cannot be translated to SQL.\n" +
                        "Fix: Order by g.Key or a supported aggregate projection, or call AsEnumerable() before OrderBy."))
            { Expr = orderExpr
              Direction = if descending then SortDirection.Desc else SortDirection.Asc })

    let internal flushGroupByAsJsonGroupArray<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>)
        (expressions: Expression array) (havingPreds: Expression list) (groupOrders: (Expression * bool) list) =
        addComplexFinal statements (fun ctx ->
            let havingDu =
                if havingPreds.IsEmpty then None
                else
                    havingPreds
                    |> List.map (fun pred -> translateExprDu sourceCtx ctx.TableName pred ctx.Vars)
                    |> List.reduce (fun a b -> SqlExpr.Binary(a, BinaryOperator.And, b))
                    |> Some
            let orderBy = translateGroupOrders sourceCtx ctx.TableName "o" ctx.Vars groupOrders
            let keyType = (extractLambdaFromExpr expressions.[0]).Body.Type
            let nullKeyFilter =
                if keyType.IsGenericType && keyType.GetGenericTypeDefinition() = typedefof<Nullable<_>> then
                    Some (SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some "o", "__solodb_group_key")))
                else None
            let core =
                { mkCore
                    [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                     { Alias = Some "Value"; Expr =
                        SqlExpr.FunctionCall("jsonb_object", [
                            SqlExpr.Literal(SqlLiteral.String "Key")
                            SqlExpr.Column(Some "o", "__solodb_group_key")
                            SqlExpr.Literal(SqlLiteral.String "Items")
                            SqlExpr.FunctionCall("jsonb_group_array", [
                                SqlExpr.FunctionCall("jsonb_set", [
                                    SqlExpr.Column(Some "o", "Value")
                                    SqlExpr.Literal(SqlLiteral.String "$.Id")
                                    SqlExpr.Column(Some "o", "Id")
                                ])
                            ])
                        ]) }]
                    (Some (DerivedTable(ctx.Inner, "o")))
                  with GroupBy = [SqlExpr.Column(Some "o", "__solodb_group_key")]
                       OrderBy = orderBy
                       Having = havingDu
                       Where = nullKeyFilter }
            wrapCore core
        )

    /// Unwrap Convert expressions (boxing for string interpolation).
    let rec private unwrapConvertExpr (expr: Expression) : Expression =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked ->
            unwrapConvertExpr ue.Operand
        | _ -> expr

    /// Translate a GroupBy projection argument through both the simple aggregate path and the chained descriptor path.
    let private translateGroupByProjectionArg
        (sourceCtx: QueryContext) (tableName: string) (innerSelect: SqlSelect) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (groupByExpressions: Expression array) (ctxTableName: string) (expr: Expression) : SqlExpr option =
        let expr = unwrapConvertExpr expr
        match tryTranslateGroupArg sourceCtx ctxTableName "o" groupParam vars expr with
        | Some sqlExpr -> Some sqlExpr
        | None ->
            QueryableBuildQueryGroupByChained.tryTranslateGroupByChainedExpr sourceCtx tableName innerSelect "o" groupParam vars groupByExpressions expr

    /// Handle string interpolation (String.Concat / String.Format) in GroupBy Select body.
    let private tryTranslateGroupByStringInterpolation
        (sourceCtx: QueryContext) (tableName: string) (innerSelect: SqlSelect) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (groupByExpressions: Expression array) (ctxTableName: string) (body: Expression) : SqlExpr option =
        match body with
        | :? MethodCallExpression as mc when mc.Method.DeclaringType = typeof<string> && mc.Method.Name = "Concat" ->
            let args =
                if mc.Arguments.Count = 1 && mc.Arguments.[0] :? NewArrayExpression then
                    (mc.Arguments.[0] :?> NewArrayExpression).Expressions |> Seq.toList
                else
                    mc.Arguments |> Seq.toList
            let translated =
                args |> List.map (fun arg ->
                    match translateGroupByProjectionArg sourceCtx tableName innerSelect groupParam vars groupByExpressions ctxTableName arg with
                    | Some sqlExpr -> sqlExpr
                    | None -> raise (NotSupportedException($"Error: GroupBy string interpolation argument cannot be translated.\nFix: Simplify the expression or call AsEnumerable() before the Select.")))
            Some (SqlExpr.FunctionCall("CONCAT", translated))
        | :? MethodCallExpression as mc when mc.Method.DeclaringType = typeof<string> && mc.Method.Name = "Format" && mc.Arguments.Count >= 2 && (mc.Arguments.[0] :? ConstantExpression) ->
            let fmt = (mc.Arguments.[0] :?> ConstantExpression).Value :?> string
            let rawArgs =
                if mc.Arguments.Count = 2 && mc.Arguments.[1] :? NewArrayExpression then
                    (mc.Arguments.[1] :?> NewArrayExpression).Expressions |> Seq.toList
                else
                    mc.Arguments |> Seq.skip 1 |> Seq.toList
            let translatedArgs =
                rawArgs |> List.map (fun arg ->
                    match translateGroupByProjectionArg sourceCtx tableName innerSelect groupParam vars groupByExpressions ctxTableName arg with
                    | Some sqlExpr -> sqlExpr
                    | None -> raise (NotSupportedException($"Error: GroupBy string format argument cannot be translated.\nFix: Simplify the expression or call AsEnumerable() before the Select.")))
            // Simple format piece extraction — replace {N} with the translated arg
            let pieces = ResizeArray<SqlExpr>()
            let sb = StringBuilder()
            let mutable i = 0
            // Parse format string and substitute translated args
            let rec parseAt pos =
                if pos >= fmt.Length then ()
                else
                    match fmt.[pos] with
                    | '{' when pos + 1 < fmt.Length && fmt.[pos + 1] = '{' ->
                        sb.Append('{') |> ignore; parseAt (pos + 2)
                    | '}' when pos + 1 < fmt.Length && fmt.[pos + 1] = '}' ->
                        sb.Append('}') |> ignore; parseAt (pos + 2)
                    | '{' ->
                        let close = fmt.IndexOf('}', pos + 1)
                        if close >= 0 then
                            if sb.Length > 0 then
                                pieces.Add(SqlExpr.Literal(SqlLiteral.String(sb.ToString())))
                                sb.Clear() |> ignore
                            let placeholder = fmt.Substring(pos + 1, close - pos - 1)
                            let endIdx =
                                let comma = placeholder.IndexOf(',')
                                let colon = placeholder.IndexOf(':')
                                [comma; colon] |> List.filter (fun x -> x >= 0) |> function [] -> placeholder.Length | xs -> List.min xs
                            match Int32.TryParse(placeholder.Substring(0, endIdx).Trim()) with
                            | true, idx when idx >= 0 && idx < translatedArgs.Length ->
                                pieces.Add(translatedArgs.[idx])
                            | _ -> ()
                            parseAt (close + 1)
                    | ch ->
                        sb.Append(ch) |> ignore; parseAt (pos + 1)
            parseAt 0
            if sb.Length > 0 then pieces.Add(SqlExpr.Literal(SqlLiteral.String(sb.ToString())))
            Some (SqlExpr.FunctionCall("CONCAT", pieces |> Seq.toList))
        | _ -> None

    let internal applyGroupBySelect<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>)
        (groupByExpressions: Expression array) (havingPreds: Expression list) (groupOrders: (Expression * bool) list) (selectExpressions: Expression array) =
        let selectLambda = extractLambdaFromExpr selectExpressions.[0]
        let groupParam = selectLambda.Parameters.[0]
        let body = selectLambda.Body
        addComplexFinal statements (fun ctx ->
            let havingExpr =
                if havingPreds.IsEmpty then None
                else
                    havingPreds
                    |> List.map (fun pred ->
                        let lambda = extractLambdaFromExpr pred
                        match tryTranslateGroupArg sourceCtx ctx.TableName "o" lambda.Parameters.[0] ctx.Vars lambda.Body with
                        | Some sqlExpr -> sqlExpr
                        | None ->
                            raise (NotSupportedException(
                                "Error: GroupBy HAVING predicate cannot be translated to SQL.\n" +
                                "Fix: Simplify the HAVING predicate or call AsEnumerable() before the Where.")))
                    |> List.reduce (fun a b -> SqlExpr.Binary(a, BinaryOperator.And, b))
                    |> Some
            let projections =
                match body with
                | :? NewExpression as newExpr when not (isNull newExpr.Members) ->
                    [for i = 0 to newExpr.Arguments.Count - 1 do
                        let memberName = newExpr.Members.[i].Name
                        let arg = newExpr.Arguments.[i]
                        match tryTranslateGroupArg sourceCtx ctx.TableName "o" groupParam ctx.Vars arg with
                        | Some sqlExpr -> yield (memberName, sqlExpr)
                        | None ->
                            // Fallback: try shared descriptor extraction for chained group patterns
                            match QueryableBuildQueryGroupByChained.tryTranslateGroupByChainedExpr sourceCtx tableName ctx.Inner "o" groupParam ctx.Vars groupByExpressions arg with
                            | Some sqlExpr -> yield (memberName, sqlExpr)
                            | None ->
                                raise (NotSupportedException(
                                    $"Error: GroupBy.Select projection member '{memberName}' cannot be translated to SQL.\n" +
                                    "Reason: The expression contains an unsupported aggregate pattern.\n" +
                                    "Fix: Simplify the aggregate or call AsEnumerable() before the Select."))]
                | _ ->
                    match tryTranslateGroupByStringInterpolation sourceCtx tableName ctx.Inner groupParam ctx.Vars groupByExpressions ctx.TableName body with
                    | Some sqlExpr -> ["Value", sqlExpr]
                    | None ->
                        match translateGroupByProjectionArg sourceCtx tableName ctx.Inner groupParam ctx.Vars groupByExpressions ctx.TableName body with
                        | Some sqlExpr -> ["Value", sqlExpr]
                        | None ->
                            raise (NotSupportedException(
                                "Error: GroupBy.Select projection cannot be translated to SQL.\n" +
                                "Fix: Use a new { } anonymous type or call AsEnumerable() before the Select."))
            let valueExpr =
                match projections with
                | [("Value", expr)] ->
                    // Floating-point scalars go through the ValueJSON string lane. Wrap with printf('%!.17g', ...)
                    // so the string carries full IEEE 754 precision and round-trips without ULP loss.
                    let bodyType = selectLambda.Body.Type
                    if bodyType = typeof<double> || bodyType = typeof<float32> then
                        SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%!.17g"); expr])
                    else expr
                | _ ->
                    let jsonObjArgs = projections |> List.collect (fun (name, expr) -> [SqlExpr.Literal(SqlLiteral.String name); expr])
                    // Use json_object (TEXT JSON) so downstream json_extract returns typed SQL values for ORDER BY/TakeWhile.
                    SqlExpr.FunctionCall("json_object", jsonObjArgs)
            let orderBy = translateGroupOrders sourceCtx ctx.TableName "o" ctx.Vars groupOrders
            let keyType = (extractLambdaFromExpr groupByExpressions.[0]).Body.Type
            let nullKeyFilter =
                if keyType.IsGenericType && keyType.GetGenericTypeDefinition() = typedefof<Nullable<_>> then
                    Some (SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some "o", "__solodb_group_key")))
                else None
            let core =
                { mkCore
                    [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                     { Alias = Some "Value"; Expr = valueExpr }]
                    (Some (DerivedTable(ctx.Inner, "o")))
                  with GroupBy = [SqlExpr.Column(Some "o", "__solodb_group_key")]
                       OrderBy = orderBy
                       Having = havingExpr
                       Where = nullKeyFilter }
            wrapCore core
        )
