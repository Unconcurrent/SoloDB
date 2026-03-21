namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open SqlDu.Engine.C1.Spec
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.SharedDescriptorExtract
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers

/// GroupBy chained-expression support — correlated subqueries for
/// group-item chains that go beyond simple aggregate whitelist.
/// Uses shared Terminal DU + walkChain extraction with per-context GroupBy building.
module internal QueryableBuildQueryGroupByChained =
    open QueryableHelperJoin
    open QueryableHelperBase

    let private aliasCounter = ref 0L
    let private nextAlias prefix =
        let id = Interlocked.Increment(aliasCounter)
        sprintf "gb%s%d" prefix id

    let private extractorConfig : ExtractorConfig = {
        EnsureOfTypeSupported = fun _ -> ()
        MultipleTakeSkipBoundariesMessage =
            "Error: Multiple Take/Skip boundaries in GroupBy chain are not supported.\nFix: Simplify the chain or call AsEnumerable() before the GroupBy."
        TooManyTakeWhileBoundariesMessage =
            "Error: Too many TakeWhile/SkipWhile boundaries in GroupBy chain are not supported.\nFix: Simplify the chain or call AsEnumerable() before the GroupBy."
    }

    /// Try to extract a chained group-item expression as a QueryDescriptor + Terminal.
    let tryExtractGroupByTerminalChain (groupParam: ParameterExpression) (expr: Expression) : (QueryDescriptor * Terminal) option =
        let expr, outerDistinct, _ = preprocessRoot expr
        match expr with
        | :? MethodCallExpression as mce ->
            match tryRecognizeTerminal mce with
            | None -> None
            | Some recognized ->
                let isGroupRoot (e: Expression) =
                    match unwrapConvert e with
                    | :? ParameterExpression as p -> Object.ReferenceEquals(p, groupParam)
                    | _ -> false
                if not (isRootedChain unwrapConvert isGroupRoot recognized.Source) then None
                else
                    let state = createState ()
                    let source = normalizeCountBySource recognized.Terminal recognized.Source state
                    let _innerSource = walkChain extractorConfig state source
                    finalizeState state
                    placeCountPredicate state recognized.CountPredicate
                    let desc : QueryDescriptor = {
                        Source = expr
                        OfTypeName = state.OfTypeName
                        CastTypeName = state.CastTypeName
                        WherePredicates = state.Wheres |> Seq.toList
                        SortKeys = state.SortKeys |> Seq.toList
                        Limit = state.Limit
                        Offset = state.Offset
                        PostBoundWherePredicates = state.PostBoundWheres |> Seq.toList
                        PostBoundSortKeys = state.PostBoundSortKeys |> Seq.toList
                        PostBoundLimit = state.PostBoundLimit
                        PostBoundOffset = state.PostBoundOffset
                        TakeWhileInfo = state.TakeWhileInfo
                        PostBoundTakeWhileInfo = state.PostBoundTakeWhileInfo
                        GroupByKey = state.GroupByKey
                        Distinct = state.Distinct || outerDistinct
                        SelectProjection = state.SelectProjection
                        SetOp = state.SetOp
                        Terminal = recognized.Terminal
                        GroupByHavingPredicate = state.GroupByHaving
                        DefaultIfEmpty = state.DefaultIfEmpty
                        PostSelectDefaultIfEmpty = state.PostSelectDefaultIfEmpty
                    }
                    Some (desc, recognized.Terminal)
        | _ -> None

    /// Translate a lambda expression against the correlated subquery alias.
    let private translateLambdaAgainst (sourceCtx: QueryContext) (subAlias: string) (vars: Dictionary<string, obj>) (lambda: LambdaExpression) : SqlExpr =
        translateExprDu sourceCtx subAlias (lambda :> Expression) vars

    /// Build the null-safe correlation predicate.
    /// Translates the group key expression against the subquery alias and compares to the outer group key.
    let private buildCorrelation (sourceCtx: QueryContext) (subAlias: string) (groupRowAlias: string) (vars: Dictionary<string, obj>) (groupByExprs: Expression array) : SqlExpr =
        let outerKey = SqlExpr.Column(Some groupRowAlias, "__solodb_group_key")
        // Translate the group key against the subquery's base table alias
        let innerKey = translateExprDu sourceCtx subAlias groupByExprs.[0] vars
        SqlExpr.Binary(innerKey, BinaryOperator.Is, outerKey)

    /// Build a correlated subquery core for GroupBy chains.
    /// Selects from innerSelect (pre-grouping rows) correlated by group key.
    let private buildCorrelatedCore
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string)
        (vars: Dictionary<string, obj>) (groupByExprs: Expression array) (desc: QueryDescriptor) (projections: Projection list) : string * SelectCore =

        let subAlias = nextAlias "_gsub"

        // Null-safe group key correlation using the original key expression
        let correlation = buildCorrelation sourceCtx subAlias groupRowAlias vars groupByExprs

        // Translate Where predicates against subquery alias
        let wherePreds =
            desc.WherePredicates
            |> List.map (fun pred ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression pred with
                | ValueSome lambda -> translateLambdaAgainst sourceCtx subAlias vars lambda
                | ValueNone -> raise (NotSupportedException("Error: Cannot translate GroupBy chain Where predicate.")))

        let fullWhere =
            match wherePreds with
            | [] -> Some correlation
            | preds -> Some (preds |> List.fold (fun acc p -> SqlExpr.Binary(acc, BinaryOperator.And, p)) correlation)

        // Translate OrderBy sort keys
        let orderBy =
            desc.SortKeys
            |> List.map (fun (keyExpr, dir) ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome lambda ->
                    { Expr = translateLambdaAgainst sourceCtx subAlias vars lambda; Direction = dir }
                | ValueNone ->
                    { Expr = SqlExpr.Column(Some subAlias, "Id"); Direction = SortDirection.Asc })

        // Default ordering by Id if no explicit order
        let orderBy =
            if orderBy.IsEmpty then [{ Expr = SqlExpr.Column(Some subAlias, "Id"); Direction = SortDirection.Asc }]
            else orderBy

        // Limit/Offset
        let limitDu =
            match desc.Limit with
            | Some e -> match e with | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value)))) | _ -> None
            | None -> None
        let offsetDu =
            match desc.Offset with
            | Some e -> match e with | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value)))) | _ -> None
            | None -> None

        let core =
            { Distinct = desc.Distinct && desc.SelectProjection.IsSome
              Projections = ProjectionSetOps.ofList projections
              Source = Some(DerivedTable(innerSelect, subAlias))
              Joins = []
              Where = fullWhere
              GroupBy = []
              Having = None
              OrderBy = orderBy
              Limit = limitDu
              Offset = offsetDu }

        subAlias, core

    /// Build a scalar subquery expression for an aggregate terminal on a group chain.
    let private buildAggregateSubquery
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (desc: QueryDescriptor) (aggKind: AggregateKind) (selectorOpt: LambdaExpression option) (coalesceZero: bool) : SqlExpr =

        let subAlias = nextAlias "_gagg"

        let correlation = buildCorrelation sourceCtx subAlias groupRowAlias vars groupByExprs

        let wherePreds =
            desc.WherePredicates
            |> List.map (fun pred ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression pred with
                | ValueSome lambda -> translateLambdaAgainst sourceCtx subAlias vars lambda
                | ValueNone -> raise (NotSupportedException("Error: Cannot translate GroupBy chain aggregate predicate.")))

        let fullWhere =
            match wherePreds with
            | [] -> Some correlation
            | preds -> Some (preds |> List.fold (fun acc p -> SqlExpr.Binary(acc, BinaryOperator.And, p)) correlation)

        let aggArg =
            match selectorOpt with
            | Some sel -> Some (translateLambdaAgainst sourceCtx subAlias vars sel)
            | None when desc.SelectProjection.IsSome ->
                match desc.SelectProjection with
                | Some proj -> Some (translateLambdaAgainst sourceCtx subAlias vars proj)
                | None -> None
            | None when aggKind = AggregateKind.Count -> None
            | None -> None

        let aggExpr = SqlExpr.AggregateCall(aggKind, aggArg, false, None)
        let aggExpr = if coalesceZero then SqlExpr.Coalesce(aggExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)]) else aggExpr

        let core =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = aggExpr }]
              Source = Some(DerivedTable(innerSelect, subAlias))
              Joins = []
              Where = fullWhere
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }

        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }

    /// Build a scalar subquery for an element terminal (First, FirstOrDefault, Last, etc.)
    let private buildElementSubquery
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (desc: QueryDescriptor) (pickLast: bool) (orDefault: bool) : SqlExpr =

        // Build correlated core first — this determines the subquery alias
        let dummyProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let subAlias, baseCore = buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc dummyProj

        // Now build projections using the SAME alias
        let projections =
            match desc.SelectProjection with
            | Some proj ->
                let selDu = translateLambdaAgainst sourceCtx subAlias vars proj
                [{ Alias = Some "v"; Expr = selDu }]
            | None ->
                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some subAlias, "Id") }
                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some subAlias, "Value") }]

        let core = { baseCore with Projections = ProjectionSetOps.ofList projections }

        // Flip order for Last
        let core =
            if pickLast then
                { core with OrderBy = core.OrderBy |> List.map (fun ob -> { ob with Direction = if ob.Direction = SortDirection.Asc then SortDirection.Desc else SortDirection.Asc }) }
            else core

        // LIMIT 1 for element access
        let core = { core with Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L)) }

        let valueExpr =
            match desc.SelectProjection with
            | Some _ -> SqlExpr.Column(Some subAlias, "v")
            | None ->
                SqlExpr.FunctionCall("jsonb_set", [
                    SqlExpr.Column(Some subAlias, "Value")
                    SqlExpr.Literal(SqlLiteral.String "$.Id")
                    SqlExpr.Column(Some subAlias, "Id")
                ])

        // Wrap in scalar subquery
        let wrapAlias = nextAlias "_gew"
        let resultCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some wrapAlias, if desc.SelectProjection.IsSome then "v" else "Value") }]
              Source = Some(DerivedTable({ Ctes = []; Body = SingleSelect core }, wrapAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }

        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect resultCore }

    /// Build an EXISTS subquery for Any/All terminals.
    let private buildExistsSubquery
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (desc: QueryDescriptor) (predOpt: LambdaExpression option) (negate: bool) : SqlExpr =

        let subAlias = nextAlias "_gex"
        let correlation = buildCorrelation sourceCtx subAlias groupRowAlias vars groupByExprs

        let wherePreds =
            desc.WherePredicates
            |> List.map (fun pred ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression pred with
                | ValueSome lambda -> translateLambdaAgainst sourceCtx subAlias vars lambda
                | ValueNone -> raise (NotSupportedException("Error: Cannot translate GroupBy chain predicate.")))

        let predExpr =
            match predOpt with
            | Some pred ->
                let predDu = translateLambdaAgainst sourceCtx subAlias vars pred
                if negate then [SqlExpr.Unary(UnaryOperator.Not, predDu)]
                else [predDu]
            | None -> []

        let allPreds = [correlation] @ wherePreds @ predExpr
        let fullWhere = allPreds |> List.reduce (fun a b -> SqlExpr.Binary(a, BinaryOperator.And, b))

        let core =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
              Source = Some(DerivedTable(innerSelect, subAlias))
              Joins = []
              Where = Some fullWhere
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }

        let existsExpr = SqlExpr.Exists { Ctes = []; Body = SingleSelect core }
        if negate then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr

    /// Build a count subquery for Count terminal on a group chain.
    let private buildCountSubquery
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (desc: QueryDescriptor) : SqlExpr =

        let subAlias = nextAlias "_gcnt"
        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let _, core = buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc oneProj
        let core = { core with Source = Some(DerivedTable(innerSelect, subAlias)) }

        // If Distinct + SelectProjection, count distinct projected values
        let innerCore =
            if desc.Distinct && desc.SelectProjection.IsSome then
                let proj = desc.SelectProjection.Value
                let projDu = translateLambdaAgainst sourceCtx subAlias vars proj
                { core with Distinct = true; Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = projDu }] }
            else
                { core with Projections = ProjectionSetOps.ofList oneProj }

        let countAlias = nextAlias "_gcw"
        let countCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
              Source = Some(DerivedTable({ Ctes = []; Body = SingleSelect innerCore }, countAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }

        SqlExpr.Coalesce(
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore },
            [SqlExpr.Literal(SqlLiteral.Integer 0L)])

    /// Main entry: translate a chained group-item expression to SqlExpr.
    /// Returns Some if the expression is a recognized chain rooted in groupParam.
    let tryTranslateGroupByChainedExpr
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (expr: Expression) : SqlExpr option =
        // Handle MemberExpression wrapping: g.OrderBy().First().Property
        let memberAccess, innerExpr =
            match expr with
            | :? MemberExpression as me when not (isNull me.Expression) && referencesParam groupParam me.Expression ->
                match me.Expression with
                | :? MethodCallExpression -> Some me.Member.Name, me.Expression
                | _ -> None, expr
            | _ -> None, expr

        match tryExtractGroupByTerminalChain groupParam innerExpr with
        | None -> None
        | Some (desc, terminal) ->
            let toLambda (e: Expression) =
                match QueryTranslatorVisitPost.tryExtractLambdaExpression e with
                | ValueSome lambda -> lambda
                | ValueNone -> raise (NotSupportedException("Error: Cannot extract lambda from GroupBy chain argument."))

            let result =
                match terminal with
                | Terminal.Count | Terminal.LongCount ->
                    buildCountSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc
                | Terminal.Sum sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Sum (Some(toLambda sel)) true
                | Terminal.SumProjected ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Sum None true
                | Terminal.Min sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Min (Some(toLambda sel)) false
                | Terminal.MinProjected ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Min None false
                | Terminal.Max sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Max (Some(toLambda sel)) false
                | Terminal.MaxProjected ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Max None false
                | Terminal.Average sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Avg (Some(toLambda sel)) false
                | Terminal.AverageProjected ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Avg None false
                | Terminal.Exists ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc None false
                | Terminal.Any(Some pred) ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc (Some(toLambda pred)) false
                | Terminal.Any None ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc None false
                | Terminal.All pred ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc (Some(toLambda pred)) true
                | Terminal.First _ ->
                    buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false
                | Terminal.FirstOrDefault _ ->
                    buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true
                | Terminal.Last _ ->
                    buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc true false
                | Terminal.LastOrDefault _ ->
                    buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc true true
                | Terminal.Single _ ->
                    buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false
                | Terminal.SingleOrDefault _ ->
                    buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true
                | Terminal.ElementAt idx ->
                    let offsetVal = match idx with | :? ConstantExpression as ce -> Convert.ToInt64(ce.Value) | _ -> 0L
                    let desc = { desc with Offset = Some idx }
                    buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false
                | Terminal.ElementAtOrDefault idx ->
                    let desc = { desc with Offset = Some idx }
                    buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true
                | Terminal.Contains value ->
                    // Build EXISTS with value comparison
                    let subAlias = nextAlias "_gcon"
                    let correlation = buildCorrelation sourceCtx subAlias groupRowAlias vars groupByExprs
                    let valueDu = translateExprDu sourceCtx groupRowAlias value vars
                    let projExpr =
                        match desc.SelectProjection with
                        | Some proj -> translateLambdaAgainst sourceCtx subAlias vars proj
                        | None -> SqlExpr.Column(Some subAlias, "Value")
                    let containsWhere = SqlExpr.Binary(correlation, BinaryOperator.And, SqlExpr.Binary(projExpr, BinaryOperator.Is, valueDu))
                    let core =
                        { Distinct = false
                          Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                          Source = Some(DerivedTable(innerSelect, subAlias))
                          Joins = []
                          Where = Some containsWhere
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                          Offset = None }
                    SqlExpr.Exists { Ctes = []; Body = SingleSelect core }
                | Terminal.Select _ ->
                    // Collection output — json_group_array of projected values
                    let subAlias = nextAlias "_gsel"
                    let _, core = buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc []
                    let core = { core with Source = Some(DerivedTable(innerSelect, subAlias)) }
                    let valueExpr =
                        match desc.SelectProjection with
                        | Some proj -> translateLambdaAgainst sourceCtx subAlias vars proj
                        | None ->
                            SqlExpr.FunctionCall("jsonb_set", [
                                SqlExpr.Column(Some subAlias, "Value")
                                SqlExpr.Literal(SqlLiteral.String "$.Id")
                                SqlExpr.Column(Some subAlias, "Id")])
                    let core = { core with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.FunctionCall("jsonb_group_array", [valueExpr]) }] }
                    SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                // Unsupported terminals — fail closed
                | Terminal.MinBy _ | Terminal.MaxBy _ | Terminal.DistinctBy _ | Terminal.CountBy _ ->
                    raise (NotSupportedException(
                        $"Error: GroupBy chain terminal '{terminal}' is not yet supported.\n" +
                        "Fix: Move the query after AsEnumerable() or use a supported terminal."))

            // Apply member access if wrapping: g.OrderBy().First().Property
            let result =
                match memberAccess with
                | Some propName ->
                    SqlExpr.FunctionCall("json_extract", [result; SqlExpr.Literal(SqlLiteral.String("$." + propName))])
                | None -> result

            Some result
