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

module internal QueryableBuildQueryPartA =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase
    let internal apply<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (pendingDistinctByScalarReuse: byref<LoweredKeySelector option>)
        (isPostScalarProjection: byref<bool>)
        (simpleCurrent: unit -> UsedSQLStatements)
        (installTerminalOrdering: Expression -> bool -> SqlExpr option -> unit)
        (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
                match m.Value with
                | SupportedLinqMethods.Where -> 
                    addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName m.Expressions.[0] WherePredicate)
                | SupportedLinqMethods.Select ->
                    let selectFingerprint = expressionFingerprint m.Expressions.[0]
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = selectFingerprint ->
                        // Edge case 13: PostScalarProjection path — SupportedLinqMethods.DistinctBy → Select reuse consuming __solodb_scalar_slot0
                        addSelector statements (DuSelector (fun tableName _vars ->
                            let hasTableName = not (String.IsNullOrEmpty tableName)
                            let idExpr = if hasTableName then SqlExpr.Column(Some tableName, "Id") else SqlExpr.Column(None, "Id")
                            [{ Alias = Some "Id"; Expr = idExpr }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(None, "__solodb_scalar_slot0") }]
                        ))
                        pendingDistinctByScalarReuse <- None
                        isPostScalarProjection <- true
                    | Some lowered when lowered.RelationAccess = HasRelationAccess ->
                        // C12 path: post-SupportedLinqMethods.DistinctBy Select on a DIFFERENT relation scalar than
                        // the SupportedLinqMethods.DistinctBy key. The base layer will materialize the relation into
                        // Value via jsonb_set, so we extract from the materialized JSON payload.
                        // Edge case 20: Relation-backed SupportedLinqMethods.DistinctBy scalar slot reuse
                        match tryExtractRelationJsonPath m.Expressions.[0] with
                        | Some jsonPath ->
                            let capturedPath = jsonPath
                            addSelector statements (DuSelector (fun _tableName _vars ->
                                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                                 { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String capturedPath)]) }]
                            ))
                            isPostScalarProjection <- true
                        | None ->
                            addSelector statements (Expression m.Expressions.[0])
                        pendingDistinctByScalarReuse <- None
                    | _ ->
                        addSelector statements (Expression m.Expressions.[0])
                        pendingDistinctByScalarReuse <- None
                | SupportedLinqMethods.Order | SupportedLinqMethods.OrderDescending ->
                    addOrder statements (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) (m.Value = SupportedLinqMethods.OrderDescending)
                | SupportedLinqMethods.OrderBy | SupportedLinqMethods.OrderByDescending  ->
                    let descending = (m.Value = SupportedLinqMethods.OrderByDescending)
                    let orderFingerprint = expressionFingerprint m.Expressions.[0]
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = orderFingerprint ->
                        let current = ifSelectorNewStatement statements
                        current.Orders.Clear()
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawExpr = Some (SqlExpr.Column(None, "__solodb_scalar_slot0")) })
                    | _ ->
                        addOrder statements m.Expressions.[0] descending
                | SupportedLinqMethods.ThenBy | SupportedLinqMethods.ThenByDescending ->
                    let descending = (m.Value = SupportedLinqMethods.ThenByDescending)
                    let orderFingerprint = expressionFingerprint m.Expressions.[0]
                    let current = simpleCurrent()
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = orderFingerprint ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawExpr = Some (SqlExpr.Column(None, "__solodb_scalar_slot0")) })
                    | _ ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawExpr = None })
                | SupportedLinqMethods.Skip ->
                    let current = simpleCurrent()
                    match current.Skip with
                    | Some _ -> statements.Add(Simple { emptySQLStatement () with Skip = Some m.Expressions.[0] })
                    | None   -> current.Skip <- Some m.Expressions.[0]
                | SupportedLinqMethods.Take ->
                    addTake statements m.Expressions.[0]

                | SupportedLinqMethods.Sum ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                             { Alias = Some "Value"; Expr = SqlExpr.Coalesce(SqlExpr.FunctionCall("SUM", [extractVal]), [SqlExpr.Literal(SqlLiteral.Integer 0L)]) }]
                        ))
                    else
                        // SUM() return NULL if all elements are NULL, TOTAL() return 0.0.
                        // TOTAL() always returns a float, therefore we will just check for NULL
                        zeroIfNullAggregateTranslator sourceCtx "SUM" statements m.OriginalMethod m.Expressions

                | SupportedLinqMethods.Average ->
                    rejectDecimalAverageIfNeeded m.OriginalMethod
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, extractVal)
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("AVG", [extractVal]) }]
                        ))
                        // Edge case 14: Aggregate NULL handling — error message for empty sequences
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.Null)), [], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
                             { Alias = Some "Value"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.String "Sequence contains no elements")), [], Some(SqlExpr.Column(None, "Value"))) }]
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "AVG" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | SupportedLinqMethods.Min ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, extractVal)
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("MIN", [extractVal]) }]
                        ))
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.Null)), [], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
                             { Alias = Some "Value"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.String "Sequence contains no elements")), [], Some(SqlExpr.Column(None, "Value"))) }]
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "MIN" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | SupportedLinqMethods.Max ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, extractVal)
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("MAX", [extractVal]) }]
                        ))
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.Null)), [], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
                             { Alias = Some "Value"; Expr = SqlExpr.CaseExpr((isNullCheck, SqlExpr.Literal(SqlLiteral.String "Sequence contains no elements")), [], Some(SqlExpr.Column(None, "Value"))) }]
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "MAX" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | SupportedLinqMethods.MinBy | SupportedLinqMethods.MaxBy ->
                    match m.Expressions.Length with
                    | 1 ->
                        let descending = (m.Value = SupportedLinqMethods.MaxBy)
                        let orderFingerprint = expressionFingerprint m.Expressions.[0]
                        let rawExpr =
                            match pendingDistinctByScalarReuse with
                            | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = orderFingerprint ->
                                Some (SqlExpr.Column(None, "__solodb_scalar_slot0"))
                            | _ ->
                                None
                        installTerminalOrdering m.Expressions.[0] descending rawExpr
                        addTake statements (ExpressionHelper.constant 1)
                    | 2 ->
                        raise (NotSupportedException("MinBy/MaxBy comparer overloads are not supported."))
                    | other ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                
                | SupportedLinqMethods.Distinct ->
                    let mutable carriedOrders : PreprocessedOrder array = Array.empty
                    let mutable latestSelectorInfo : (string * bool) option = None
                    for i = statements.Count - 1 downto 0 do
                        if latestSelectorInfo.IsNone then
                            match statements.[i] with
                            | Simple s ->
                                match s.Selector with
                                | Some (Expression selector) ->
                                    let selectorType =
                                        match unwrapQuotedLambda selector with
                                        | :? LambdaExpression as le -> le.Body.Type
                                        | other -> other.Type
                                    latestSelectorInfo <-
                                        Some (expressionFingerprint selector, QueryTranslator.isPrimitiveSQLiteType selectorType)
                                | _ -> ()
                            | _ -> ()
                        if carriedOrders.Length = 0 then
                            match statements.[i] with
                            | Simple s when s.Orders.Count > 0 ->
                                carriedOrders <-
                                    s.Orders
                                    |> Seq.map (fun o ->
                                        { OrderingRule = o.OrderingRule
                                          Descending = o.Descending
                                          RawExpr = o.RawExpr })
                                    |> Seq.toArray
                                s.Orders.Clear()
                            | _ -> ()
                    addComplexFinal statements (fun ctx ->
                        // SELECT -1 AS Id, Value FROM (inner) o GROUP BY {identity expr}
                        let groupByExpr = translateExprDu sourceCtx ctx.TableName (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) ctx.Vars
                        let core =
                            { mkCore
                                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                                 { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                                (Some (DerivedTable(ctx.Inner, "o")))
                              with GroupBy = [groupByExpr] }
                        wrapCore core
                    )
                    if carriedOrders.Length > 0 then
                        let orderedLayer = emptySQLStatement ()
                        for order in carriedOrders do
                            let rawExpr =
                                match latestSelectorInfo with
                                | Some (selectorFingerprint, true) when expressionFingerprint order.OrderingRule = selectorFingerprint ->
                                    Some (SqlExpr.Column(None, "Value"))
                                | _ ->
                                    order.RawExpr
                            orderedLayer.Orders.Add({
                                OrderingRule = order.OrderingRule
                                Descending = order.Descending
                                RawExpr = rawExpr
                            })
                        statements.Add(Simple orderedLayer)

                | SupportedLinqMethods.DistinctBy ->
                    // Edge case 7: SupportedLinqMethods.DistinctBy with ROW_NUMBER OVER — window function + derived table
                    let lowered = lowerKeySelectorLambda sourceCtx tableName m.Expressions.[0] DistinctByKey
                    addLoweredKeySelector statements lowered
                    pendingDistinctByScalarReuse <-
                        match lowered.RelationAccess with
                        | HasRelationAccess -> Some lowered
                        | NoRelationAccess -> None
                    addComplexFinal statements (fun ctx ->
                        // Inner: SELECT o.Id, o.Value, [o.__solodb_group_key AS __solodb_scalar_slot0,] ROW_NUMBER() OVER (PARTITION BY o.__solodb_group_key ORDER BY o.Id) AS __solodb_rn FROM (inner) o
                        let innerProjs =
                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }]
                            @ (if lowered.RelationAccess = HasRelationAccess then
                                   [{ Alias = Some "__solodb_scalar_slot0"; Expr = SqlExpr.Column(Some "o", "__solodb_group_key") }]
                               else [])
                            @ [{ Alias = Some "__solodb_rn"; Expr = SqlExpr.WindowCall({
                                    Kind = WindowFunctionKind.RowNumber
                                    Arguments = []
                                    PartitionBy = [SqlExpr.Column(Some "o", "__solodb_group_key")]
                                    OrderBy = [(SqlExpr.Column(Some "o", "Id"), SortDirection.Asc)]
                                }) }]
                        let innerCore = mkCore innerProjs (Some (DerivedTable(ctx.Inner, "o")))
                        let innerSel = wrapCore innerCore

                        // Outer: SELECT o.Id, o.Value [, o.__solodb_scalar_slot0] FROM (inner) o WHERE o.__solodb_rn = 1
                        let outerProjs =
                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }]
                            @ (if lowered.RelationAccess = HasRelationAccess then
                                   [{ Alias = Some "__solodb_scalar_slot0"; Expr = SqlExpr.Column(Some "o", "__solodb_scalar_slot0") }]
                               else [])
                        let outerCore =
                            { mkCore outerProjs (Some (DerivedTable(innerSel, "o")))
                              with Where = Some (SqlExpr.Binary(SqlExpr.Column(Some "o", "__solodb_rn"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L))) }
                        wrapCore outerCore
                    )

                | SupportedLinqMethods.GroupBy ->
                    // GroupBy is intercepted in BuildQuery.Main.fs for look-ahead fusion.
                    raise (InvalidOperationException("GroupBy should be intercepted in BuildQuery.Main.fs, not dispatched to PartA."))
                | SupportedLinqMethods.TakeWhile
                | SupportedLinqMethods.SkipWhile ->
                    let isTakeWhile = (m.Value = SupportedLinqMethods.TakeWhile)
                    let opName = if isTakeWhile then "TakeWhile" else "SkipWhile"
                    let mutable foundOrders : PreprocessedOrder array = Array.empty
                    for i = statements.Count - 1 downto 0 do
                        if foundOrders.Length = 0 then
                            match statements.[i] with
                            | Simple s when s.Orders.Count > 0 ->
                                foundOrders <- s.Orders |> Seq.toArray
                                s.Orders.Clear()
                            | _ -> ()
                    if foundOrders.Length = 0 then
                        raise (InvalidOperationException(
                            $"Error: {opName} requires explicit ordering.\n" +
                            "Reason: Window-based conditional filtering needs a deterministic row order.\n" +
                            $"Fix: Add .OrderBy() or .OrderByDescending() before .{opName}()."))
                    let capturedOrders = foundOrders
                    let predExpr = m.Expressions.[0]
                    addComplexFinal statements (fun ctx ->
                        let predDu = translateExprDu sourceCtx ctx.TableName predExpr ctx.Vars
                        let orderDus =
                            capturedOrders |> Array.map (fun o ->
                                let expr =
                                    match o.RawExpr with
                                    | Some duExpr -> duExpr
                                    | None -> translateExprDu sourceCtx ctx.TableName o.OrderingRule ctx.Vars
                                { Expr = expr; Direction = if o.Descending then SortDirection.Desc else SortDirection.Asc }
                            ) |> Array.toList
                        // SUM(NOT pred) OVER (ORDER BY key) — cumulative failure window.
                        let windowSpec = {
                            Kind = NamedWindowFunction "SUM"
                            Arguments = [SqlExpr.Unary(UnaryOperator.Not, predDu)]
                            PartitionBy = []
                            OrderBy = orderDus |> List.map (fun ob -> (ob.Expr, ob.Direction))
                        }
                        let cfExpr = SqlExpr.WindowCall windowSpec
                        let innerCore =
                            { mkCore
                                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }
                                 { Alias = Some "_cf"; Expr = cfExpr }]
                                (Some(DerivedTable(ctx.Inner, "o")))
                              with OrderBy = orderDus }
                        let innerSelect = wrapCore innerCore
                        let twAlias = "_tw"
                        let cfFilter =
                            if isTakeWhile then
                                SqlExpr.Binary(SqlExpr.Column(Some twAlias, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                            else
                                SqlExpr.Binary(SqlExpr.Column(Some twAlias, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
                        let outerCore =
                            { mkCore
                                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some twAlias, "Id") }
                                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some twAlias, "Value") }]
                                (Some(DerivedTable(innerSelect, twAlias)))
                              with Where = Some cfFilter }
                        wrapCore outerCore
                    )
                | _ -> ()

    // ═══════════════════════════════════════════════════════════════
    // GroupBy deferred emission + GroupBy+Select fusion
    // ═══════════════════════════════════════════════════════════════

    let internal applyGroupByKeyOnly<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>) (expressions: Expression array) =
        addLoweredKeySelector statements (lowerKeySelectorLambda sourceCtx tableName expressions.[0] GroupByKey)

    let internal flushGroupByAsJsonGroupArray<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>)
        (expressions: Expression array) (havingPreds: Expression list) =
        ()
        addComplexFinal statements (fun ctx ->
            let havingDu =
                if havingPreds.IsEmpty then None
                else
                    havingPreds
                    |> List.map (fun pred -> translateExprDu sourceCtx ctx.TableName pred ctx.Vars)
                    |> List.reduce (fun a b -> SqlExpr.Binary(a, BinaryOperator.And, b))
                    |> Some
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
                       Having = havingDu }
            wrapCore core
        )

    // ── GroupBy aggregate translation helpers ──

    let private extractLambdaFromExpr (expr: Expression) : LambdaExpression =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote -> ue.Operand :?> LambdaExpression
        | :? LambdaExpression as le -> le
        | _ -> raise (NotSupportedException($"Expected lambda expression, got {expr.NodeType}"))

    let rec private referencesParam (param: ParameterExpression) (expr: Expression) =
        if isNull expr then false
        elif obj.ReferenceEquals(expr, param) then true
        else
            match expr with
            | :? MethodCallExpression as mc ->
                (not (isNull mc.Object) && referencesParam param mc.Object) ||
                (mc.Arguments |> Seq.exists (referencesParam param))
            | :? MemberExpression as me -> not (isNull me.Expression) && referencesParam param me.Expression
            | :? UnaryExpression as ue -> referencesParam param ue.Operand
            | :? BinaryExpression as be -> referencesParam param be.Left || referencesParam param be.Right
            | :? ConditionalExpression as ce -> referencesParam param ce.Test || referencesParam param ce.IfTrue || referencesParam param ce.IfFalse
            | :? NewExpression as ne -> ne.Arguments |> Seq.exists (referencesParam param)
            | _ -> false

    let private isGroupSource (expr: Expression) (groupParam: ParameterExpression) =
        match expr with
        | :? ParameterExpression as p -> obj.ReferenceEquals(p, groupParam)
        | _ -> false

    let private tryTranslateProjectedAggregate
        (sourceCtx: QueryContext) (ctxTableName: string) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
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
            translateExprDu sourceCtx ctxTableName (lambda :> Expression) vars
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
        (sourceCtx: QueryContext) (ctxTableName: string) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (expr: Expression) : SqlExpr option =
        match expr with
        | :? MemberExpression as me when not (isNull me.Expression) && me.Expression :? ParameterExpression && obj.ReferenceEquals(me.Expression, groupParam) && me.Member.Name = "Key" ->
            Some (SqlExpr.Column(Some "o", "__solodb_group_key"))
        | :? MemberExpression as me when not (isNull me.Expression) ->
            match me.Expression with
            | :? MemberExpression as inner when not (isNull inner.Expression) && inner.Expression :? ParameterExpression && obj.ReferenceEquals(inner.Expression, groupParam) && inner.Member.Name = "Key" ->
                Some (SqlExpr.FunctionCall("json_extract", [
                    SqlExpr.Column(Some "o", "__solodb_group_key")
                    SqlExpr.Literal(SqlLiteral.String ("$." + me.Member.Name))
                ]))
            | _ -> None
        | :? MethodCallExpression as mc when referencesParam groupParam mc ->
            match mc.Method.Name with
            | "Count" when mc.Arguments.Count = 1 && isGroupSource mc.Arguments.[0] groupParam ->
                Some (SqlExpr.AggregateCall(AggregateKind.Count, None, false, None))
            | "Count" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let pred = extractLambdaFromExpr mc.Arguments.[1]
                let predDu = translateExprDu sourceCtx ctxTableName (pred :> Expression) vars
                Some (SqlExpr.AggregateCall(AggregateKind.Sum, Some (SqlExpr.CaseExpr(
                    (predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None))
            | "Sum" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let sel = extractLambdaFromExpr mc.Arguments.[1]
                let selDu = translateExprDu sourceCtx ctxTableName (sel :> Expression) vars
                Some (SqlExpr.Coalesce(SqlExpr.AggregateCall(AggregateKind.Sum, Some selDu, false, None), [SqlExpr.Literal(SqlLiteral.Integer 0L)]))
            | "Min" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let sel = extractLambdaFromExpr mc.Arguments.[1]
                Some (SqlExpr.AggregateCall(AggregateKind.Min, Some (translateExprDu sourceCtx ctxTableName (sel :> Expression) vars), false, None))
            | "Max" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let sel = extractLambdaFromExpr mc.Arguments.[1]
                Some (SqlExpr.AggregateCall(AggregateKind.Max, Some (translateExprDu sourceCtx ctxTableName (sel :> Expression) vars), false, None))
            | "Average" when mc.Arguments.Count = 2 && isGroupSource mc.Arguments.[0] groupParam ->
                let sel = extractLambdaFromExpr mc.Arguments.[1]
                Some (SqlExpr.AggregateCall(AggregateKind.Avg, Some (translateExprDu sourceCtx ctxTableName (sel :> Expression) vars), false, None))
            | "Sum" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupParam vars mc.Arguments.[0] AggregateKind.Sum true
            | "Count" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupParam vars mc.Arguments.[0] AggregateKind.Count false
            | "Min" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupParam vars mc.Arguments.[0] AggregateKind.Min false
            | "Max" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupParam vars mc.Arguments.[0] AggregateKind.Max false
            | "Average" when mc.Arguments.Count = 1 -> tryTranslateProjectedAggregate sourceCtx ctxTableName groupParam vars mc.Arguments.[0] AggregateKind.Avg false
            | "Any" when mc.Arguments.Count = 1 && isGroupSource mc.Arguments.[0] groupParam ->
                Some (SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Count, None, false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L)))
            // g.Items() — materialize all group elements as json_group_array
            | "Items" when mc.Arguments.Count = 1 && isGroupSource mc.Arguments.[0] groupParam ->
                Some (SqlExpr.FunctionCall("json_group_array", [
                    SqlExpr.FunctionCall("jsonb_set", [
                        SqlExpr.Column(Some "o", "Value")
                        SqlExpr.Literal(SqlLiteral.String "$.Id")
                        SqlExpr.Column(Some "o", "Id")
                    ])
                ]))
            | _ -> None
        | :? BinaryExpression as be when referencesParam groupParam be ->
            let left = tryTranslateGroupArg sourceCtx ctxTableName groupParam vars be.Left
            let right = tryTranslateGroupArg sourceCtx ctxTableName groupParam vars be.Right
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
            match tryTranslateGroupArg sourceCtx ctxTableName groupParam vars ce.Test,
                  tryTranslateGroupArg sourceCtx ctxTableName groupParam vars ce.IfTrue,
                  tryTranslateGroupArg sourceCtx ctxTableName groupParam vars ce.IfFalse with
            | Some t, Some tr, Some fa -> Some (SqlExpr.CaseExpr((t, tr), [], Some fa))
            | _ -> None
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not && referencesParam groupParam ue ->
            match tryTranslateGroupArg sourceCtx ctxTableName groupParam vars ue.Operand with
            | Some inner -> Some (SqlExpr.Unary(UnaryOperator.Not, inner))
            | None -> None
        | :? ConstantExpression as ce ->
            Some (allocateParam vars (box ce.Value))
        | _ when not (referencesParam groupParam expr) ->
            Some (translateExprDu sourceCtx ctxTableName expr vars)
        | _ -> None

    let internal applyGroupBySelect<'T>
        (sourceCtx: QueryContext) (tableName: string) (statements: ResizeArray<SQLSubquery>)
        (groupByExpressions: Expression array) (havingPreds: Expression list) (selectExpressions: Expression array) =
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
                        match tryTranslateGroupArg sourceCtx ctx.TableName lambda.Parameters.[0] ctx.Vars lambda.Body with
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
                        match tryTranslateGroupArg sourceCtx ctx.TableName groupParam ctx.Vars arg with
                        | Some sqlExpr -> yield (memberName, sqlExpr)
                        | None ->
                            raise (NotSupportedException(
                                $"Error: GroupBy.Select projection member '{memberName}' cannot be translated to SQL.\n" +
                                "Reason: The expression contains an unsupported aggregate pattern.\n" +
                                "Fix: Simplify the aggregate or call AsEnumerable() before the Select."))]
                | _ ->
                    match tryTranslateGroupArg sourceCtx ctx.TableName groupParam ctx.Vars body with
                    | Some sqlExpr -> ["Value", sqlExpr]
                    | None ->
                        raise (NotSupportedException(
                            "Error: GroupBy.Select projection cannot be translated to SQL.\n" +
                            "Fix: Use a new { } anonymous type or call AsEnumerable() before the Select."))
            let jsonObjArgs = projections |> List.collect (fun (name, expr) -> [SqlExpr.Literal(SqlLiteral.String name); expr])
            // Use json_object (TEXT JSON) so downstream json_extract returns typed SQL values for ORDER BY/TakeWhile.
            let valueExpr = SqlExpr.FunctionCall("json_object", jsonObjArgs)
            let core =
                { mkCore
                    [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                     { Alias = Some "Value"; Expr = valueExpr }]
                    (Some (DerivedTable(ctx.Inner, "o")))
                  with GroupBy = [SqlExpr.Column(Some "o", "__solodb_group_key")]
                       Having = havingExpr }
            wrapCore core
        )
