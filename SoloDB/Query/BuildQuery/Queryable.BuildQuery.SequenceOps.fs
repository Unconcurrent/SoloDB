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
open SoloDatabase.QueryableGroupByAliases
open SqlDu.Engine.C1.Spec

module internal QueryableBuildQuerySequenceOps =
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
                             { Alias = Some "Value"; Expr = SqlExpr.Column(None, (syntheticScalarSlotAlias 0)) }]
                        ))
                        pendingDistinctByScalarReuse <- None
                        isPostScalarProjection <- true
                    | Some lowered when lowered.RelationAccess = HasRelationAccess ->
                        // Post-DistinctBy Select on a DIFFERENT relation scalar than
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
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawExpr = Some (SqlExpr.Column(None, (syntheticScalarSlotAlias 0))) })
                    | _ ->
                        addOrder statements m.Expressions.[0] descending
                | SupportedLinqMethods.ThenBy | SupportedLinqMethods.ThenByDescending ->
                    let descending = (m.Value = SupportedLinqMethods.ThenByDescending)
                    let orderFingerprint = expressionFingerprint m.Expressions.[0]
                    let current = simpleCurrent()
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = orderFingerprint ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawExpr = Some (SqlExpr.Column(None, (syntheticScalarSlotAlias 0))) })
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
                                Some (SqlExpr.Column(None, (syntheticScalarSlotAlias 0)))
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
                                   [{ Alias = Some (syntheticScalarSlotAlias 0); Expr = SqlExpr.Column(Some "o", syntheticGroupKeyAlias) }]
                               else [])
                            @ [{ Alias = Some "__solodb_rn"; Expr = SqlExpr.WindowCall({
                                    Kind = WindowFunctionKind.RowNumber
                                    Arguments = []
                                    PartitionBy = [SqlExpr.Column(Some "o", syntheticGroupKeyAlias)]
                                    OrderBy = [(SqlExpr.Column(Some "o", "Id"), SortDirection.Asc)]
                                }) }]
                        let innerCore = mkCore innerProjs (Some (DerivedTable(ctx.Inner, "o")))
                        let innerSel = wrapCore innerCore

                        // Outer: SELECT o.Id, o.Value [, o.__solodb_scalar_slot0] FROM (inner) o WHERE o.__solodb_rn = 1
                        let outerProjs =
                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }]
                            @ (if lowered.RelationAccess = HasRelationAccess then
                                   [{ Alias = Some (syntheticScalarSlotAlias 0); Expr = SqlExpr.Column(Some "o", (syntheticScalarSlotAlias 0)) }]
                               else [])
                        let outerCore =
                            { mkCore outerProjs (Some (DerivedTable(innerSel, "o")))
                              with Where = Some (SqlExpr.Binary(SqlExpr.Column(Some "o", "__solodb_rn"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L))) }
                        wrapCore outerCore
                    )

                | SupportedLinqMethods.CountBy ->
                    let keySelectorExpr, comparerExprOpt =
                        match m.Expressions with
                        | [| keySelector |] -> keySelector, None
                        | [| keySelector; comparer |] -> keySelector, Some comparer
                        | [||] ->
                            raise (NotSupportedException(
                                "Error: CountBy requires a key selector.\n" +
                                "Reason: CountBy groups rows by a projected key before counting.\n" +
                                "Fix: Pass a key selector lambda, for example .CountBy(x => x.Category)."))
                        | other ->
                            raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other.Length))

                    match comparerExprOpt with
                    | Some comparerExpr ->
                        let keyType = (GenericMethodArgCache.Get m.OriginalMethod).[1]
                        let comparerValue =
                            try QueryTranslator.evaluateExpr<obj> comparerExpr
                            with _ ->
                                raise (NotSupportedException(
                                    "Error: CountBy comparer overload is not supported.\n" +
                                    "Reason: SoloDB can translate CountBy only with the default equality comparer in SQL.\n" +
                                    "Fix: Remove the comparer or normalize the key inside the selector."))
                        if not (isNull comparerValue) then
                            let eqType = typedefof<EqualityComparer<_>>.MakeGenericType([| keyType |])
                            let defaultProp = eqType.GetProperty("Default", BindingFlags.Public ||| BindingFlags.Static)
                            let defaultComparer = if isNull defaultProp then null else defaultProp.GetValue(null)
                            let isDefaultComparer =
                                not (isNull defaultComparer)
                                && eqType.IsAssignableFrom(comparerValue.GetType())
                                && (obj.ReferenceEquals(comparerValue, defaultComparer)
                                    || comparerValue.GetType() = defaultComparer.GetType())
                            if not isDefaultComparer then
                                raise (NotSupportedException(
                                    "Error: CountBy comparer overload is not supported.\n" +
                                    "Reason: SoloDB can translate CountBy only with the default equality comparer in SQL.\n" +
                                    "Fix: Remove the comparer or normalize the key inside the selector."))
                    | None -> ()

                    addLoweredKeySelector statements (lowerKeySelectorLambda sourceCtx tableName keySelectorExpr GroupByKey)
                    addComplexFinal statements (fun ctx ->
                        let core =
                            { mkCore
                                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                                 { Alias = Some "Value"; Expr =
                                    SqlExpr.FunctionCall(jsonObjectFn, [
                                        SqlExpr.Literal(SqlLiteral.String "Key")
                                        SqlExpr.Column(Some "o", syntheticGroupKeyAlias)
                                        SqlExpr.Literal(SqlLiteral.String "Value")
                                        SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
                                    ]) }]
                                (Some (DerivedTable(ctx.Inner, "o")))
                              with GroupBy = [SqlExpr.Column(Some "o", syntheticGroupKeyAlias)] }
                        wrapCore core
                    )

                | SupportedLinqMethods.GroupBy ->
                    // GroupBy is intercepted in BuildQuery.Main.fs for look-ahead fusion.
                    raise (InvalidOperationException("GroupBy should be intercepted in BuildQuery.Main.fs, not dispatched to SequenceOps."))
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
                            "Error: TakeWhile/SkipWhile requires explicit ordering.\n" +
                            "Reason: TakeWhile/SkipWhile needs a deterministic row order.\n" +
                            "Fix: Add .OrderBy() before .TakeWhile(), or set OrderBy on the [SoloRef] attribute."))
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

    // GroupBy fusion functions moved to Queryable.BuildQuery.GroupByOps.fs.
