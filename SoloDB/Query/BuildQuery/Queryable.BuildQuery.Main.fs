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
        let mutable rootElementType: Type = null
        let mutable pendingDistinctByScalarReuse : LoweredKeySelector option = None
        // Set when Select consumes a carried scalar slot (SupportedLinqMethods.DistinctBy → Select reuse path).
        // Terminal zero-arg aggregates use this to consume Value directly instead of retranslating.
        let mutable isPostScalarProjection = false
        let preprocessed = preprocessQuery e

        // Register Include/Exclude/ThenInclude/ThenExclude paths up-front so behavior is deterministic regardless method-call order.
        // Process in expression-tree order: root → Include → ThenInclude → ThenExclude
        // to accumulate dotted chain paths for ThenInclude/ThenExclude.
        let mutable chainPath = ""
        let registerRelation operation expressions =
            match operation with
            | SupportedLinqMethods.Include ->
                let path = extractRelationPathOrThrow "Include" expressions
                chainPath <- path
                registerIncludePath sourceCtx path
            | SupportedLinqMethods.ThenInclude ->
                let hop = extractRelationPathOrThrow "ThenInclude" expressions
                chainPath <- if chainPath = "" then hop else chainPath + "." + hop
                registerIncludePath sourceCtx chainPath
            | SupportedLinqMethods.ThenExclude ->
                let hop = extractRelationPathOrThrow "ThenExclude" expressions
                let excludePath = if chainPath = "" then hop else chainPath + "." + hop
                registerExcludePath sourceCtx excludePath
                // ThenExclude does NOT advance chainPath — it stays at the parent level
            | SupportedLinqMethods.Exclude when expressions.Length > 0 ->
                let path = extractRelationPathOrThrow "Exclude" expressions
                chainPath <- ""
                registerExcludePath sourceCtx path
            | SupportedLinqMethods.Exclude ->
                // Parameterless Exclude() — whitelist mode
                sourceCtx.WhitelistMode <- true
            | _ -> ()
        for q in preprocessed do
            match q with
            | Method m -> registerRelation m.Value m.Expressions
            | Relation directive ->
                let operation =
                    match directive.Operation with
                    | RelationQueryOperation.Include -> SupportedLinqMethods.Include
                    | RelationQueryOperation.ThenInclude -> SupportedLinqMethods.ThenInclude
                    | RelationQueryOperation.Exclude -> SupportedLinqMethods.Exclude
                    | RelationQueryOperation.ThenExclude -> SupportedLinqMethods.ThenExclude
                let expressions = if isNull directive.Selector then Array.empty else [|directive.Selector :> Expression|]
                registerRelation operation expressions
            | _ -> ()

        validateIncludeExcludeConflicts sourceCtx

        let reversed = preprocessed
        let mutable pendingGroupByExprs : struct(ChainExpr.GroupBySelectors * Type) option = None
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
            | RootQuery rq ->
                tableName <- rq.SourceTableName
                rootElementType <- QueryRootFacts.elementType rq
            | Relation _ -> ()
            | Method m ->
                let inline simpleCurrent() = simpleCurrent statements
                let inline installTerminalOrdering (ordering: Expression) (direction: SortDirection) (rawExpr: SqlExpr option) =
                    if hasBoundedInput statements then
                        addComplexFinal statements (fun ctx ->
                            { Query = QueryProduced.completedRows sourceCtx.NextAlias ctx.Inner ctx.Input
                              SerializationInput = ctx.Input; KeyInput = ctx.KeyInput })
                        let current = simpleCurrent()
                        current.Orders.Add({ OrderingRule = ordering; Direction = direction; RawExpr = rawExpr })
                        current.Orders.Add({ OrderingRule = Expression.Parameter(typeof<int64>); Direction = SortDirection.Asc
                                             RawExpr = Some(SqlExpr.Column(None, "_tree_ordinal")) })
                    else
                        let current = ifSelectorNewStatement statements
                        let existingOrders = current.Orders |> Seq.toList
                        current.Orders.Clear()
                        current.Orders.Add({ OrderingRule = ordering; Direction = direction; RawExpr = rawExpr })
                        if List.isEmpty existingOrders then
                            current.Orders.Add({ OrderingRule = TypeMetadata.ExpressionHelper.get(fun (x: obj) -> x.Dyn<int64>("Id")); Direction = SortDirection.Asc; RawExpr = None })
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
                        let struct(groupByExprs, groupType) = pendingGroupByExprs.Value
                        let havingPreds = pendingGroupByHavingPreds |> List.rev
                        let groupOrders = pendingGroupByOrders
                        pendingGroupByExprs <- None
                        pendingGroupByHavingPreds <- []
                        pendingGroupByOrders <- []
                        QueryableBuildQueryGroupByOps.applyGroupBySelect<'T>
                            sourceCtx tableName statements groupByExprs havingPreds groupOrders m.Expressions groupType
                        pendingGroupByHandled <- true
                    | _ ->
                        let struct(groupByExprs, groupType) = pendingGroupByExprs.Value
                        pendingGroupByExprs <- None
                        let havingPreds = pendingGroupByHavingPreds |> List.rev
                        let groupOrders = pendingGroupByOrders
                        pendingGroupByHavingPreds <- []
                        pendingGroupByOrders <- []
                        QueryableBuildQueryGroupByOps.flushGroupByAsJsonGroupArray<'T>
                            sourceCtx tableName statements groupByExprs havingPreds groupOrders groupType

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
                        let group = ChainExpr.groupBySelectors m.OriginalMethod m.Expressions
                        QueryableBuildQueryGroupByOps.applyGroupByInput sourceCtx statements group
                        match group.Result with
                        | None -> pendingGroupByExprs <- Some(struct(group, group.GroupType))
                        | Some result ->
                            let selector = ChainExpr.groupingResult group.GroupType result
                            QueryableBuildQueryGroupByOps.applyGroupBySelect<'T>
                                sourceCtx tableName statements group [] [] [|selector :> Expression|] group.GroupType
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
                            m |> ignore
                    | SupportedLinqMethods.All
                    | SupportedLinqMethods.Any
                    | SupportedLinqMethods.Contains
                    | SupportedLinqMethods.Append
                    | SupportedLinqMethods.Concat
                    | SupportedLinqMethods.Union
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
                            m |> ignore

            idx <- idx + 1

        // Flush any trailing pending GroupBy.
        if pendingGroupByExprs.IsSome then
            let struct(groupByExprs, groupType) = pendingGroupByExprs.Value
            let havingPreds = pendingGroupByHavingPreds |> List.rev
            let groupOrders = pendingGroupByOrders
            pendingGroupByExprs <- None
            pendingGroupByHavingPreds <- []
            pendingGroupByOrders <- []
            QueryableBuildQueryGroupByOps.flushGroupByAsJsonGroupArray<'T>
                sourceCtx tableName statements groupByExprs havingPreds groupOrders groupType

        match statements.[0] with
        | Simple s ->
            statements.[0] <- Simple {s with TableName = tableName}
        | ComplexDu _ ->
            ()
        if isNull rootElementType then
            raise (NotSupportedException("The query must contain a typed collection root."))
        rootElementType

    and internal translateQuery<'T> (translationStepCounter: int ref) (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (expression: Expression) : TranslatedQuery =
        let statements = ResizeArray<SQLSubquery>()
        let rootElementType = buildQuery<'T> translationStepCounter sourceCtx statements expression
        buildLayersDu rootElementType sourceCtx vars statements
