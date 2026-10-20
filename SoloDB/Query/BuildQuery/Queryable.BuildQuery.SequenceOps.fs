namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open NativeSQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.SqlModel

module internal QueryableBuildQuerySequenceOps =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase

    let private normalizeOrderKeyExpr (expr: SqlExpr) (clrType: Type) =
        DateTimeFunctions.canonicalizeForCompareOrOrder clrType expr
    let private applySelect (statements: ResizeArray<SQLSubquery>) (pendingDistinctByScalarReuse: byref<LoweredKeySelector option>) (isPostScalarProjection: byref<bool>) (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
        match pendingDistinctByScalarReuse with
        | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = expressionFingerprint m.Expressions.[0] ->
            // Edge case 13: PostScalarProjection path — SupportedLinqMethods.DistinctBy → Select reuse consuming __solodb_scalar_slot0
            addSelector statements (DuSelector (fun _currentCtx input tableName _vars ->
                let hasTableName = not (String.IsNullOrEmpty tableName)
                let idExpr = if hasTableName then SqlExpr.Column(Some tableName, "Id") else SqlExpr.Column(None, "Id")
                let facts = input.KeyInput |> ValueOption.defaultWith (fun () -> invalidOp "A reused key requires its produced input.")
                struct([{ Alias = Some "Id"; Expr = idExpr }
                        { Alias = Some "Value"; Expr = SqlExpr.Column(None, syntheticScalarSlotAlias 0) }], facts)
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
                let _ = jsonPath
                addSelector statements (Expression m.Expressions[0])
                isPostScalarProjection <- true
            | None ->
                addSelector statements (Expression m.Expressions.[0])
            pendingDistinctByScalarReuse <- None
        | _ ->
            addSelector statements (Expression m.Expressions.[0])
            pendingDistinctByScalarReuse <- None

    let private applyAggregate (sourceCtx: QueryContext) (statements: ResizeArray<SQLSubquery>) (isPostScalarProjection: bool) (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
        let name =
            match m.Value with
            | SupportedLinqMethods.Sum -> "SUM"
            | SupportedLinqMethods.Average -> "AVG"
            | SupportedLinqMethods.Min -> "MIN"
            | SupportedLinqMethods.Max -> "MAX"
            | _ -> invalidArg "m" "An aggregate terminal is required."
        if isPostScalarProjection && m.Expressions.Length = 0 then
            let inputType=QueryTranslatorNumeric.aggregateInput m.OriginalMethod
            let value=SqlExpr.Column(None,"Value")
            let extractVal =
                if JsonFunctions.consumesSqlScalar inputType then value
                else SqlExpr.FunctionCall("jsonb_extract", [value; SqlExpr.Literal(SqlLiteral.String "$")])
            let aggregate = QueryTranslatorNumeric.aggregate m.OriginalMethod inputType (QueryTranslatorNumeric.kind name) extractVal false
            let result = if name = "SUM" then SqlExpr.Coalesce(aggregate,[QueryTranslatorNumeric.zero m.OriginalMethod.ReturnType]) else aggregate
            addSelector statements (DuSelector (fun _currentCtx _input _tableName _vars ->
                let produced = QueryProduced.typed m.OriginalMethod.ReturnType result
                struct([{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                        { Alias = Some "Value"; Expr = result }], produced.Input)))
            if name <> "SUM" then checkEmptyAggregate statements m.OriginalMethod "Sequence contains no elements"
        elif name = "SUM" then
            zeroIfNullAggregateTranslator sourceCtx name statements m.OriginalMethod m.Expressions
        else
            raiseIfNullAggregateTranslator sourceCtx name statements m.OriginalMethod m.Expressions "Sequence contains no elements"


    let private applyDistinctBy (sourceCtx: QueryContext) (statements: ResizeArray<SQLSubquery>) (pendingDistinctByScalarReuse: byref<LoweredKeySelector option>) (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
        OrderedChainPlan.validateDefaultComparerArity m.OriginalMethod.Name (m.Expressions.Length + 1)
        // Edge case 7: SupportedLinqMethods.DistinctBy with ROW_NUMBER OVER — window function + derived table
        let lowered = lowerKeySelectorLambda m.Expressions.[0]
        addSelector statements (KeyProjection lowered.KeyExpression)
        pendingDistinctByScalarReuse <-
            match lowered.RelationAccess with
            | HasRelationAccess -> Some lowered
            | NoRelationAccess -> None
        addComplexFinal statements (fun ctx ->
            let source = QueryProduced.completedKeyRows sourceCtx.NextAlias ctx.Inner ctx.Input ctx.KeyInput.Value
            let ordinal = "_tree_ordinal"
            // Rank representatives by completed position while retaining scalar-slot reuse.
            let companions = QueryTranslatorSerialization.companionProjections (Some "o") ctx.Input
                             @ (ctx.KeyInput |> ValueOption.map (QueryTranslatorSerialization.companionProjections (Some "o")) |> ValueOption.defaultValue [])
            let innerProjs =
                [{ Alias = Some "Id"; Expr = QueryDefaultEquality.outputIdentity (m.OriginalMethod.GetGenericArguments().[0])
                                              (QueryTranslatorSerialization.scopeSerializationInput (Some "o") OuterLayer ctx.Input) (SqlExpr.Column(Some "o", "Id")) }
                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }]
                @ [{ Alias = Some "_tree_ordinal"; Expr = SqlExpr.Column(Some "o", "_tree_ordinal") }]
                @ (if lowered.RelationAccess = HasRelationAccess then
                       [{ Alias = Some (syntheticScalarSlotAlias 0); Expr = SqlExpr.Column(Some "o", syntheticGroupKeyAlias) }]
                   else [])
                @ companions
                @ [{ Alias = Some "__solodb_rn"; Expr = QueryableBuildQueryWindowHelpers.rowNumberExpr
                        (QueryDefaultEquality.terms (QueryDefaultEquality.key sourceCtx.NextAlias (m.OriginalMethod.GetGenericArguments().[1]) (SqlExpr.Column(Some "o", syntheticGroupKeyAlias))
                            (QueryTranslatorSerialization.scopeSerializationInput (Some "o") OuterLayer ctx.KeyInput.Value)))
                        [(SqlExpr.Column(Some "o", ordinal), SortDirection.Asc)] }]
            let innerCore = mkCore innerProjs (Some (DerivedTable(source, "o")))
            let innerSel = wrapCore innerCore

            // Outer: SELECT o.Id, o.Value [, o.__solodb_scalar_slot0] FROM (inner) o WHERE o.__solodb_rn = 1
            let outerProjs =
                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }]
                @ (if lowered.RelationAccess = HasRelationAccess then
                       [{ Alias = Some (syntheticScalarSlotAlias 0); Expr = SqlExpr.Column(Some "o", (syntheticScalarSlotAlias 0)) }]
                   else [])
            let outerCore =
                { mkCore (outerProjs @ companions) (Some (DerivedTable(innerSel, "o")))
                  with Where = Some (SqlExpr.Binary(SqlExpr.Column(Some "o", "__solodb_rn"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                       OrderBy = [{ Expr = SqlExpr.Column(Some "o", "_tree_ordinal"); Direction = SortDirection.Asc }] }
            { Query = wrapCore outerCore; SerializationInput = ctx.Input; KeyInput = ctx.KeyInput }
        )


    let private applyCountBy (sourceCtx: QueryContext) (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) (statements: ResizeArray<SQLSubquery>) =
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

        comparerExprOpt |> Option.iter (fun comparer ->
            ChainExpr.validateGroupingComparer "CountBy" (TypeMetadata.GenericMethodArgCache.Get m.OriginalMethod).[1] comparer)

        addSelector statements (KeyProjection keySelectorExpr)
        addComplexFinal statements (fun ctx ->
            let key = ctx.KeyInput |> ValueOption.defaultWith (fun () -> invalidOp "CountBy requires its produced key.")
            let rows = if QueryDefaultEquality.requiresPrepared (m.OriginalMethod.GetGenericArguments().[1]) key then QueryProduced.completedKeyRows sourceCtx.NextAlias ctx.Inner ctx.Input key else ctx.Inner
            let struct(rows, key, equalityName) = QueryDefaultEquality.prepareGroups sourceCtx.NextAlias syntheticGroupKeyAlias
                                                    (m.OriginalMethod.GetGenericArguments().[1]) rows ctx.Input key "_tree_ordinal"
            let key = QueryTranslatorSerialization.scopeSerializationInput (Some "o") OuterLayer
                          { key with Source = ValueSome(jsonToken (SqlExpr.Column(None, syntheticGroupKeyAlias))) }
            let count = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
            let resultType = ModelMetadata.SequenceElement m.OriginalMethod.ReturnType
            let input = QuerySerializationInput.Members(resultType, [|"Key"; "Value"|], [|key.OriginalType; typeof<int>|],
                            [| key; (QueryProduced.typed typeof<int> count).Input |])
            let value = SqlExpr.FunctionCall(jsonbObjectFn, [SqlExpr.Literal(SqlLiteral.String "Key"); jsonToken (SqlExpr.Column(Some "o", syntheticGroupKeyAlias));
                                                          SqlExpr.Literal(SqlLiteral.String "Value"); count])
            let core =
                { mkCore [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = value }]
                         (Some (DerivedTable(rows, "o")))
                  with GroupBy = equalityName |> List.map (fun name -> SqlExpr.Column(Some "o", name)) }
            let struct(query, output) = QueryProduced.publishCore sourceCtx.NextAlias core (DirectValue(value, input)) false
            { Query = query; SerializationInput = output; KeyInput = ValueNone }
        )


    let private applyWhile (sourceCtx: QueryContext) (statements: ResizeArray<SQLSubquery>) (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
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
            let currentCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = ctx.Input }
            let predDu = translateExprDu currentCtx ctx.TableName predExpr ctx.Vars
            let orderDus =
                capturedOrders |> Array.map (fun o ->
                    let expr =
                        match o.RawExpr with
                        | Some duExpr -> duExpr
                        | None -> translateExprDu currentCtx ctx.TableName o.OrderingRule ctx.Vars
                    { Expr = normalizeOrderKeyExpr expr (orderKeyClrType o.OrderingRule); Direction = o.Direction }
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
                    ([{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") };
                     { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") };
                     { Alias = Some "_cf"; Expr = cfExpr }]
                     @ QueryTranslatorSerialization.companionProjections (Some "o") ctx.Input)
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
                    ([{ Alias = Some "Id"; Expr = SqlExpr.Column(Some twAlias, "Id") };
                     { Alias = Some "Value"; Expr = SqlExpr.Column(Some twAlias, "Value") }]
                     @ QueryTranslatorSerialization.companionProjections (Some twAlias) ctx.Input)
                    (Some(DerivedTable(innerSelect, twAlias)))
                  with Where = Some cfFilter }
            { Query = wrapCore outerCore; SerializationInput = ctx.Input; KeyInput = ValueNone }
        )

    let internal apply
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (pendingDistinctByScalarReuse: byref<LoweredKeySelector option>)
        (isPostScalarProjection: byref<bool>)
        (simpleCurrent: unit -> UsedSQLStatements)
        (installTerminalOrdering: Expression -> SortDirection -> SqlExpr option -> unit)
        (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
                match m.Value with
                | SupportedLinqMethods.Where -> 
                    addFilter statements m.Expressions.[0]
                | SupportedLinqMethods.Select ->
                    applySelect statements &pendingDistinctByScalarReuse &isPostScalarProjection m

                | SupportedLinqMethods.Order | SupportedLinqMethods.OrderDescending ->
                    addOrder statements (TypeMetadata.GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> Expression.Parameter) (m.Value = SupportedLinqMethods.OrderDescending)
                | SupportedLinqMethods.OrderBy | SupportedLinqMethods.OrderByDescending  ->
                    let descending = (m.Value = SupportedLinqMethods.OrderByDescending)
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = expressionFingerprint m.Expressions.[0] ->
                        let current = ifSelectorNewStatement statements
                        current.Orders.Clear()
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Direction = SortDirection.ofDescending descending; RawExpr = Some (SqlExpr.Column(None, (syntheticScalarSlotAlias 0))) })
                    | _ ->
                        addOrder statements m.Expressions.[0] descending
                | SupportedLinqMethods.ThenBy | SupportedLinqMethods.ThenByDescending ->
                    let descending = (m.Value = SupportedLinqMethods.ThenByDescending)
                    let current = simpleCurrent()
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = expressionFingerprint m.Expressions.[0] ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Direction = SortDirection.ofDescending descending; RawExpr = Some (SqlExpr.Column(None, (syntheticScalarSlotAlias 0))) })
                    | _ ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Direction = SortDirection.ofDescending descending; RawExpr = None })
                | SupportedLinqMethods.Skip ->
                    let current = simpleCurrent()
                    match current.Skip with
                    | Some _ -> statements.Add(Simple { emptySQLStatement () with Skip = Some m.Expressions.[0] })
                    | None   -> current.Skip <- Some m.Expressions.[0]
                | SupportedLinqMethods.Take ->
                    addTake statements m.Expressions.[0]

                | SupportedLinqMethods.Sum
                | SupportedLinqMethods.Average
                | SupportedLinqMethods.Min
                | SupportedLinqMethods.Max ->
                    applyAggregate sourceCtx statements isPostScalarProjection m

                | SupportedLinqMethods.MinBy | SupportedLinqMethods.MaxBy ->
                    match m.Expressions.Length with
                    | 1 ->
                        let descending = (m.Value = SupportedLinqMethods.MaxBy)
                        let rawExpr =
                            match pendingDistinctByScalarReuse with
                            | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = expressionFingerprint m.Expressions.[0] ->
                                Some (SqlExpr.Column(None, (syntheticScalarSlotAlias 0)))
                            | _ ->
                                None
                        let keyType = orderKeyClrType m.Expressions.[0]
                        let direction =
                            if not descending && (not keyType.IsValueType || not (isNull (Nullable.GetUnderlyingType keyType))) then
                                SortDirection.AscNullsLast
                            else SortDirection.ofDescending descending
                        installTerminalOrdering m.Expressions.[0] direction rawExpr
                        addTake statements (TypeMetadata.ExpressionHelper.constant 1)
                    | 2 ->
                        raise (NotSupportedException("MinBy/MaxBy comparer overloads are not supported."))
                    | other ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                
                | SupportedLinqMethods.Distinct ->
                    QueryableBuildQueryDistinct.apply sourceCtx statements m

                | SupportedLinqMethods.DistinctBy ->
                    applyDistinctBy sourceCtx statements &pendingDistinctByScalarReuse m

                | SupportedLinqMethods.CountBy ->
                    applyCountBy sourceCtx m statements

                | SupportedLinqMethods.GroupBy ->
                    // GroupBy is intercepted in BuildQuery.Main.fs for look-ahead fusion.
                    raise (InvalidOperationException("GroupBy should be intercepted in BuildQuery.Main.fs, not dispatched to SequenceOps."))
                | SupportedLinqMethods.TakeWhile
                | SupportedLinqMethods.SkipWhile ->
                    applyWhile sourceCtx statements m

                | _ -> ()

    // GroupBy fusion functions moved to Queryable.BuildQuery.GroupByOps.fs.
