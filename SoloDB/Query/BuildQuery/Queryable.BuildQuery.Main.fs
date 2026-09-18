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
open SoloDatabase.SqlModel

module internal QueryableBuildQueryMain =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase
    open QueryableBuildQuerySequenceOps
    open QueryableBuildQueryJoinAndTerminalOps
    open QueryableBuildQuerySetAndTypeOps
    let rec internal buildQuery<'T> (translationStepCounter: int ref) (sourceCtx: QueryContext) (statements: SQLSubquery ResizeArray) (e: Expression) =
        let next = translationStepCounter.Value + 1
        translationStepCounter.Value <- next
        if next > maxTranslationSteps then
            raise (NotSupportedException translationStepLimitMessage)
        statements.Add (emptySQLStatement () |> Simple)

        let mutable tableName = ""
        let mutable pendingDistinctByScalarReuse : LoweredKeySelector option = None
        // Set when Select consumes a carried scalar slot (SupportedLinqMethods.DistinctBy → Select reuse path).
        // Terminal zero-arg aggregates use this to consume Value directly instead of retranslating.
        let mutable isPostScalarProjection = false
        let preprocessed = preprocessQuery e

        // Register Include/Exclude/ThenInclude/ThenExclude paths up-front so behavior is deterministic regardless method-call order.
        // Process in expression-tree order: root → Include → ThenInclude → ThenExclude
        // to accumulate dotted chain paths for ThenInclude/ThenExclude.
        let mutable chainPath = ""
        for q in preprocessed do
            match q with
            | Method m when m.Value = SupportedLinqMethods.Include ->
                let path = extractRelationPathOrThrow "Include" m.Expressions
                chainPath <- path
                registerIncludePath sourceCtx path
            | Method m when m.Value = SupportedLinqMethods.ThenInclude ->
                let hop = extractRelationPathOrThrow "ThenInclude" m.Expressions
                chainPath <- if chainPath = "" then hop else chainPath + "." + hop
                registerIncludePath sourceCtx chainPath
            | Method m when m.Value = SupportedLinqMethods.ThenExclude ->
                let hop = extractRelationPathOrThrow "ThenExclude" m.Expressions
                let excludePath = if chainPath = "" then hop else chainPath + "." + hop
                registerExcludePath sourceCtx excludePath
                // ThenExclude does NOT advance chainPath — it stays at the parent level
            | Method m when m.Value = SupportedLinqMethods.Exclude && m.Expressions.Length > 0 ->
                let path = extractRelationPathOrThrow "Exclude" m.Expressions
                chainPath <- ""
                registerExcludePath sourceCtx path
            | Method m when m.Value = SupportedLinqMethods.Exclude && m.Expressions.Length = 0 ->
                // Parameterless Exclude() — whitelist mode
                sourceCtx.WhitelistMode <- true
            | _ -> ()

        validateIncludeExcludeConflicts sourceCtx

        let reversed = preprocessed
        let mutable pendingGroupByExprs : Expression array option = None
        let mutable pendingGroupByHavingPreds : Expression list = []
        let mutable pendingGroupByOrders : (Expression * bool) list = []
        let mutable idx = 0
        while idx < reversed.Length do
            // Keep aggregate projection fused when group paging precedes Select.
            // Only cross consecutive bounds: filters and new orderings after a
            // bound must keep their original scope.
            if pendingGroupByExprs.IsSome then
                let mutable projection = idx
                let isBound = function
                    | Method m -> m.Value = SupportedLinqMethods.Skip || m.Value = SupportedLinqMethods.Take
                    | _ -> false
                while projection < reversed.Length && isBound reversed.[projection] do
                    projection <- projection + 1
                if projection > idx && projection < reversed.Length then
                    match reversed.[projection] with
                    | Method m when m.Value = SupportedLinqMethods.Select
                                    && QueryableBuildQueryGroupByOps.canPageProjection m.Expressions.[0] ->
                        let selector = reversed.[projection]
                        for position = projection downto idx + 1 do
                            reversed.[position] <- reversed.[position - 1]
                        reversed.[idx] <- selector
                    | _ -> ()
            let q = reversed.[idx]
            match q with
            | RootQuery rq -> tableName <- rq.SourceTableName
            | Method m ->
                let inline simpleCurrent() = simpleCurrent statements
                let inline installTerminalOrdering (ordering: Expression) (descending: bool) (rawExpr: SqlExpr option) =
                    let current = ifSelectorNewStatement statements
                    let existingOrders = current.Orders |> Seq.toList
                    current.Orders.Clear()
                    current.Orders.Add({ OrderingRule = ordering; Descending = descending; RawExpr = rawExpr })
                    if List.isEmpty existingOrders then
                        current.Orders.Add({ OrderingRule = UtilsReflection.ExpressionHelper.get(fun (x: obj) -> x.Dyn<int64>("Id")); Descending = false; RawExpr = None })
                    else
                        for order in existingOrders do
                            current.Orders.Add(order)

                if m.Value <> SupportedLinqMethods.Select && m.Value <> SupportedLinqMethods.DistinctBy
                   && m.Value <> SupportedLinqMethods.OrderBy && m.Value <> SupportedLinqMethods.OrderByDescending
                   && m.Value <> SupportedLinqMethods.ThenBy && m.Value <> SupportedLinqMethods.ThenByDescending then
                    pendingDistinctByScalarReuse <- None

                // GroupBy look-ahead state machine.
                let mutable pendingGroupByHandled = false
                if pendingGroupByExprs.IsSome then
                    match m.Value with
                    | SupportedLinqMethods.Where ->
                        pendingGroupByHavingPreds <- m.Expressions.[0] :: pendingGroupByHavingPreds
                        pendingGroupByHandled <- true
                    | SupportedLinqMethods.OrderBy
                    | SupportedLinqMethods.OrderByDescending
                    | SupportedLinqMethods.ThenBy
                    | SupportedLinqMethods.ThenByDescending ->
                        pendingGroupByOrders <- pendingGroupByOrders @ [m.Expressions.[0], (m.Value = SupportedLinqMethods.OrderByDescending || m.Value = SupportedLinqMethods.ThenByDescending)]
                        pendingGroupByHandled <- true
                    | SupportedLinqMethods.Select ->
                        let groupByExprs = pendingGroupByExprs.Value
                        let havingPreds = pendingGroupByHavingPreds |> List.rev
                        let groupOrders = pendingGroupByOrders
                        pendingGroupByExprs <- None
                        pendingGroupByHavingPreds <- []
                        pendingGroupByOrders <- []
                        QueryableBuildQueryGroupByOps.applyGroupBySelect<'T>
                            sourceCtx tableName statements groupByExprs havingPreds groupOrders m.Expressions
                        pendingGroupByHandled <- true
                    | _ ->
                        let groupByExprs = pendingGroupByExprs.Value
                        pendingGroupByExprs <- None
                        let havingPreds = pendingGroupByHavingPreds |> List.rev
                        let groupOrders = pendingGroupByOrders
                        pendingGroupByHavingPreds <- []
                        pendingGroupByOrders <- []
                        QueryableBuildQueryGroupByOps.flushGroupByAsJsonGroupArray<'T>
                            sourceCtx tableName statements groupByExprs havingPreds groupOrders

                if not pendingGroupByHandled then
                    match m.Value with
                    | SupportedLinqMethods.Where
                    | SupportedLinqMethods.Select
                    | SupportedLinqMethods.Order
                    | SupportedLinqMethods.OrderDescending
                    | SupportedLinqMethods.OrderBy
                    | SupportedLinqMethods.OrderByDescending
                    | SupportedLinqMethods.ThenBy
                    | SupportedLinqMethods.ThenByDescending
                    | SupportedLinqMethods.Skip
                    | SupportedLinqMethods.Take
                    | SupportedLinqMethods.Sum
                    | SupportedLinqMethods.Average
                    | SupportedLinqMethods.Min
                    | SupportedLinqMethods.Max
                    | SupportedLinqMethods.MinBy
                    | SupportedLinqMethods.MaxBy
                    | SupportedLinqMethods.Distinct
                    | SupportedLinqMethods.DistinctBy
                    | SupportedLinqMethods.CountBy
                    | SupportedLinqMethods.TakeWhile
                    | SupportedLinqMethods.SkipWhile ->
                        QueryableBuildQuerySequenceOps.apply
                            sourceCtx
                            tableName
                            statements
                            &pendingDistinctByScalarReuse
                            &isPostScalarProjection
                            simpleCurrent
                            installTerminalOrdering
                            m
                    | SupportedLinqMethods.GroupBy ->
                        let last = m.OriginalMethod.GetParameters() |> Array.last
                        if last.ParameterType.IsGenericType
                           && last.ParameterType.GetGenericTypeDefinition() = typedefof<System.Collections.Generic.IEqualityComparer<_>> then
                            ChainExpr.validateGroupingComparer "GroupBy" (m.OriginalMethod.GetGenericArguments().[1]) (Array.last m.Expressions)
                        QueryableBuildQueryGroupByOps.applyGroupByKeyOnly<'T>
                            sourceCtx tableName statements m.Expressions
                        pendingGroupByExprs <- Some m.Expressions
                    | SupportedLinqMethods.Count
                    | SupportedLinqMethods.LongCount
                    | SupportedLinqMethods.SelectMany
                    | SupportedLinqMethods.Join
                    | SupportedLinqMethods.GroupJoin
                    | SupportedLinqMethods.Single
                    | SupportedLinqMethods.SingleOrDefault
                    | SupportedLinqMethods.First
                    | SupportedLinqMethods.FirstOrDefault
                    | SupportedLinqMethods.ElementAt
                    | SupportedLinqMethods.ElementAtOrDefault
                    | SupportedLinqMethods.DefaultIfEmpty
                    | SupportedLinqMethods.Last
                    | SupportedLinqMethods.LastOrDefault ->
                        QueryableBuildQueryJoinAndTerminalOps.apply<'T>
                            sourceCtx
                            tableName
                            statements
                            (fun innerCtx vars expression -> translateQuery<'T> translationStepCounter innerCtx vars expression)
                            m
                    | SupportedLinqMethods.All
                    | SupportedLinqMethods.Any
                    | SupportedLinqMethods.Contains
                    | SupportedLinqMethods.Append
                    | SupportedLinqMethods.Concat
                    | SupportedLinqMethods.Except
                    | SupportedLinqMethods.Intersect
                    | SupportedLinqMethods.ExceptBy
                    | SupportedLinqMethods.IntersectBy
                    | SupportedLinqMethods.UnionBy
                    | SupportedLinqMethods.Cast
                    | SupportedLinqMethods.OfType
                    | SupportedLinqMethods.Exclude
                    | SupportedLinqMethods.Include
                    | SupportedLinqMethods.ThenInclude
                    | SupportedLinqMethods.ThenExclude
                    | SupportedLinqMethods.Aggregate ->
                        QueryableBuildQuerySetAndTypeOps.apply<'T>
                            sourceCtx
                            tableName
                            statements
                            (fun innerCtx vars expression -> translateQuery<'T> translationStepCounter innerCtx vars expression)
                            m

            idx <- idx + 1

        // Flush any trailing pending GroupBy.
        if pendingGroupByExprs.IsSome then
            let groupByExprs = pendingGroupByExprs.Value
            let havingPreds = pendingGroupByHavingPreds |> List.rev
            let groupOrders = pendingGroupByOrders
            pendingGroupByExprs <- None
            pendingGroupByHavingPreds <- []
            pendingGroupByOrders <- []
            QueryableBuildQueryGroupByOps.flushGroupByAsJsonGroupArray<'T>
                sourceCtx tableName statements groupByExprs havingPreds groupOrders

        match statements.[0] with
        | Simple s ->
            statements.[0] <- Simple {s with TableName = tableName}
        | ComplexDu _ ->
            ()

    and internal translateQuery<'T> (translationStepCounter: int ref) (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (expression: Expression) : SqlSelect =
        let statements = ResizeArray<SQLSubquery>()
        buildQuery<'T> translationStepCounter sourceCtx statements expression
        buildLayersDu<'T> sourceCtx vars statements
    
