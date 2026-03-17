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

module internal QueryableBuildQueryMain =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase
    open QueryableBuildQueryPartA
    open QueryableBuildQueryPartB
    open QueryableBuildQueryPartC
    let rec internal buildQuery<'T> (sourceCtx: QueryContext) (statements: SQLSubquery ResizeArray) (e: Expression) =
        statements.Add (emptySQLStatement () |> Simple)

        let mutable tableName = ""
        let mutable pendingDistinctByScalarReuse : LoweredKeySelector option = None
        // Set when Select consumes a carried scalar slot (SupportedLinqMethods.DistinctBy → Select reuse path).
        // Terminal zero-arg aggregates use this to consume Value directly instead of retranslating.
        let mutable isPostScalarProjection = false
        let preprocessed = preprocessQuery e |> Seq.toArray

        // Register Include/Exclude/ThenInclude/ThenExclude paths up-front so behavior is deterministic regardless method-call order.
        // Process in reverse (expression-tree order: root → Include → ThenInclude → ThenExclude)
        // to accumulate dotted chain paths for ThenInclude/ThenExclude.
        let mutable chainPath = ""
        for q in preprocessed |> Array.rev do
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
            | Method m when m.Value = SupportedLinqMethods.Exclude ->
                let path = extractRelationPathOrThrow "Exclude" m.Expressions
                chainPath <- ""
                registerExcludePath sourceCtx path
            | _ -> ()

        validateIncludeExcludeConflicts sourceCtx

        for q in preprocessed |> Array.rev do
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
                        current.Orders.Add({ OrderingRule = ExpressionHelper.get(fun (x: obj) -> x.Dyn<int64>("Id")); Descending = false; RawExpr = None })
                    else
                        for order in existingOrders do
                            current.Orders.Add(order)

                if m.Value <> SupportedLinqMethods.Select && m.Value <> SupportedLinqMethods.DistinctBy
                   && m.Value <> SupportedLinqMethods.OrderBy && m.Value <> SupportedLinqMethods.OrderByDescending
                   && m.Value <> SupportedLinqMethods.ThenBy && m.Value <> SupportedLinqMethods.ThenByDescending then
                    pendingDistinctByScalarReuse <- None

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
                | SupportedLinqMethods.GroupBy ->
                    QueryableBuildQueryPartA.apply<'T>
                        sourceCtx
                        tableName
                        statements
                        &pendingDistinctByScalarReuse
                        &isPostScalarProjection
                        simpleCurrent
                        installTerminalOrdering
                        m
                | SupportedLinqMethods.Count
                | SupportedLinqMethods.CountBy
                | SupportedLinqMethods.LongCount
                | SupportedLinqMethods.SelectMany
                | SupportedLinqMethods.Join
                | SupportedLinqMethods.Single
                | SupportedLinqMethods.SingleOrDefault
                | SupportedLinqMethods.First
                | SupportedLinqMethods.FirstOrDefault
                | SupportedLinqMethods.DefaultIfEmpty
                | SupportedLinqMethods.Last
                | SupportedLinqMethods.LastOrDefault ->
                    QueryableBuildQueryPartB.apply<'T>
                        sourceCtx
                        tableName
                        statements
                        (fun innerCtx vars expression -> translateQuery<'T> innerCtx vars expression)
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
                | SupportedLinqMethods.Cast
                | SupportedLinqMethods.OfType
                | SupportedLinqMethods.Exclude
                | SupportedLinqMethods.Include
                | SupportedLinqMethods.ThenInclude
                | SupportedLinqMethods.ThenExclude
                | SupportedLinqMethods.Aggregate ->
                    QueryableBuildQueryPartC.apply<'T>
                        sourceCtx
                        tableName
                        statements
                        (fun innerCtx vars expression -> translateQuery<'T> innerCtx vars expression)
                        m

        match statements.[0] with
        | Simple s ->
            statements.[0] <- Simple {s with TableName = tableName}
        | ComplexDu _ ->
            ()

    and internal translateQuery<'T> (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (expression: Expression) : SqlSelect =
        let statements = ResizeArray<SQLSubquery>()
        buildQuery<'T> sourceCtx statements expression
        buildLayersDu<'T> sourceCtx vars statements
    
