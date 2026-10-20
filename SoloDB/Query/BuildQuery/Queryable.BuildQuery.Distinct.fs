namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection
open SoloDatabase.SqlModel

/// Root default equality selects one completed occurrence without losing its input.
module internal QueryableBuildQueryDistinct =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase

    let apply (sourceCtx: QueryContext) (statements: ResizeArray<SQLSubquery>) (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
        OrderedChainPlan.validateDefaultComparerArity m.OriginalMethod.Name (m.Expressions.Length + 1)
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
                            Some (expressionFingerprint selector, QueryTranslatorBaseTypes.isPrimitiveSQLiteType selectorType)
                    | _ -> ()
                | _ -> ()
            if carriedOrders.Length = 0 then
                match statements.[i] with
                | Simple s when s.Orders.Count > 0 ->
                    carriedOrders <-
                        s.Orders
                        |> Seq.map (fun o ->
                            { OrderingRule = o.OrderingRule
                              Direction = o.Direction
                              RawExpr = o.RawExpr })
                        |> Seq.toArray
                | _ -> ()
        addComplexFinal statements (fun ctx ->
            // Default equality retains the complete selected row and its admitted identity.
            let target = TypeMetadata.GenericMethodArgCache.Get(m.OriginalMethod).[0]
            let input = QueryTranslatorSerialization.scopeSerializationInput (Some "o") OuterLayer ctx.Input
            let groupByExpr = QueryDefaultEquality.key sourceCtx.NextAlias target (SqlExpr.Column(Some "o", "Value")) input
            let core =
                { mkCore
                    ([{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                      { Alias = Some "Value"; Expr = SqlExpr.Column(None, "Value") }]
                     @ QueryTranslatorSerialization.companionProjections (Some "o") ctx.Input)
                    (Some (DerivedTable(ctx.Inner, "o")))
                  with GroupBy = QueryDefaultEquality.terms groupByExpr }
            let query =
                if QueryDefaultEquality.requiresPrepared target ctx.Input then
                    let rows = QueryProduced.completedRows sourceCtx.NextAlias ctx.Inner ctx.Input
                    let input = QueryTranslatorSerialization.scopeSerializationInput (Some "o") BaseLayer ctx.Input
                    let equality = QueryDefaultEquality.key sourceCtx.NextAlias target (SqlExpr.Column(Some "o", "Value")) input
                    let identity = QueryDefaultEquality.outputIdentity target input (SqlExpr.Column(Some "o", "Id"))
                    let fields = [{ Alias = Some "Id"; Expr = identity }; { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }
                                  { Alias = Some "_tree_ordinal"; Expr = SqlExpr.Column(Some "o", "_tree_ordinal") }]
                                 @ QueryTranslatorSerialization.companionProjections (Some "o") ctx.Input
                    QueryDefaultEquality.firstRows sourceCtx.NextAlias (mkCore fields (Some(DerivedTable(rows, "o")))) equality
                        [SqlExpr.Column(Some "o", "_tree_ordinal"), SortDirection.Asc]
                else wrapCore core
            { Query = query; SerializationInput = ctx.Input; KeyInput = ValueNone }
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
                    Direction = order.Direction
                    RawExpr = rawExpr
                })
            statements.Add(Simple orderedLayer)


