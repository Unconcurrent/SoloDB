namespace SoloDatabase
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec
module internal QueryableBuildQueryPartBGroupJoinChain =
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableHelperBase
    type GroupJoinElementKind =
        | FirstLike of orDefault: bool
        | LastLike of orDefault: bool
        | SingleLike of orDefault: bool
        | ElementAtLike of indexExpr: Expression * orDefault: bool
    type GroupJoinGroupChainDescriptor =
        { WherePredicates: LambdaExpression list
          OrderKeys: (LambdaExpression * SortDirection) list
          Skip: Expression option
          Take: Expression option
          SelectProjection: LambdaExpression option
          Distinct: bool
          UsesDefaultIfEmpty: bool }
    type GroupJoinElementCall =
        { Call: MethodCallExpression
          Kind: GroupJoinElementKind
          Chain: GroupJoinGroupChainDescriptor }
    type GroupJoinTranslatedArg =
        { Value: SqlExpr
          Error: SqlExpr option }
    type GroupJoinRuntime =
        { InnerCtx: QueryContext
          InnerRootTable: string
          InnerSelect: SqlSelect
          OuterAlias: string
          OuterParam: ParameterExpression
          GroupParam: ParameterExpression
          OuterKeyExpr: SqlExpr
          InnerKeySelector: LambdaExpression
          Vars: Dictionary<string, obj>
          TranslateJoinExpr: QueryContext -> string -> Dictionary<string, obj> -> ParameterExpression option -> Expression -> SqlExpr
          MaterializeDiscoveredJoins: ResizeArray<JoinEdge> -> string option -> Collections.Generic.HashSet<string> option -> JoinShape list
          TryTranslateDbRefValueIdKey: ParameterExpression -> string -> Expression -> SqlExpr option
          ReplaceExpression: Expression -> Expression -> Expression -> Expression
          ErrorExpr: string -> SqlExpr
          TranslateOuterExpr: Expression -> SqlExpr }
    let translatedArg value = { Value = value; Error = None }
    let combineErrorExprs (errors: SqlExpr option list) =
        errors
        |> List.choose id
        |> function
            | [] -> None
            | [single] -> Some single
            | head :: tail -> Some(SqlExpr.Coalesce(head, tail))
    let emptyChainDescriptor =
        { WherePredicates = []
          OrderKeys = []
          Skip = None
          Take = None
          SelectProjection = None
          Distinct = false
          UsesDefaultIfEmpty = false }
    let hasGroupChainOps (desc: GroupJoinGroupChainDescriptor) =
        not desc.WherePredicates.IsEmpty
        || not desc.OrderKeys.IsEmpty
        || desc.Skip.IsSome
        || desc.Take.IsSome
        || desc.SelectProjection.IsSome
        || desc.Distinct
        || desc.UsesDefaultIfEmpty
    let evalNonNegativeInt64 (expr: Expression) =
        let raw = QueryTranslator.evaluateExpr<obj> expr
        let value = Convert.ToInt64(raw)
        if value < 0L then 0L else value
    let buildLimitOffset (takeExpr: Expression option) (skipExpr: Expression option) =
        let takeValue = takeExpr |> Option.map evalNonNegativeInt64
        let skipValue = skipExpr |> Option.map evalNonNegativeInt64
        let limit =
            match takeValue, skipValue with
            | Some n, _ -> Some (SqlExpr.Literal(SqlLiteral.Integer n))
            | None, Some _ -> Some (SqlExpr.Literal(SqlLiteral.Integer -1L))
            | None, None -> None
        let offset = skipValue |> Option.map (fun n -> SqlExpr.Literal(SqlLiteral.Integer n))
        limit, offset
    let rec walkGroupChain (rt: GroupJoinRuntime) (expr: Expression) =
        match expr with
        | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) ->
            Some emptyChainDescriptor
        | :? MethodCallExpression as mc when mc.Arguments.Count >= 1 ->
            match walkGroupChain rt mc.Arguments.[0] with
            | Some desc ->
                match mc.Method.Name, mc.Arguments.Count with
                | "Where", 2 when desc.SelectProjection.IsNone ->
                    let pred = unwrapLambdaExpressionOrThrow "GroupJoin chain Where" mc.Arguments.[1]
                    Some { desc with WherePredicates = desc.WherePredicates @ [pred] }
                | "OrderBy", 2 when desc.SelectProjection.IsNone ->
                    let keySel = unwrapLambdaExpressionOrThrow "GroupJoin chain OrderBy" mc.Arguments.[1]
                    Some { desc with OrderKeys = [keySel, SortDirection.Asc] }
                | "OrderByDescending", 2 when desc.SelectProjection.IsNone ->
                    let keySel = unwrapLambdaExpressionOrThrow "GroupJoin chain OrderByDescending" mc.Arguments.[1]
                    Some { desc with OrderKeys = [keySel, SortDirection.Desc] }
                | "ThenBy", 2 when desc.SelectProjection.IsNone ->
                    let keySel = unwrapLambdaExpressionOrThrow "GroupJoin chain ThenBy" mc.Arguments.[1]
                    Some { desc with OrderKeys = desc.OrderKeys @ [keySel, SortDirection.Asc] }
                | "ThenByDescending", 2 when desc.SelectProjection.IsNone ->
                    let keySel = unwrapLambdaExpressionOrThrow "GroupJoin chain ThenByDescending" mc.Arguments.[1]
                    Some { desc with OrderKeys = desc.OrderKeys @ [keySel, SortDirection.Desc] }
                | "Skip", 2 ->
                    Some { desc with Skip = Some mc.Arguments.[1] }
                | "Take", 2 ->
                    Some { desc with Take = Some mc.Arguments.[1] }
                | "Select", 2 when desc.SelectProjection.IsNone ->
                    let proj = unwrapLambdaExpressionOrThrow "GroupJoin chain Select" mc.Arguments.[1]
                    Some { desc with SelectProjection = Some proj }
                | "Distinct", 1 ->
                    Some { desc with Distinct = true }
                | "DefaultIfEmpty", 1 ->
                    Some { desc with UsesDefaultIfEmpty = true }
                | _ -> None
            | None -> None
        | _ -> None

    let tryGetGroupChainDescriptor (rt: GroupJoinRuntime) (expr: Expression) =
        match walkGroupChain rt expr with
        | Some desc when hasGroupChainOps desc -> Some desc
        | _ -> None
    let buildCountSubquery (rt: GroupJoinRuntime) (baseCore: SelectCore) (limit: int option) =
        let countSourceAlias = sprintf "gjc%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let countSourceCore =
            { baseCore with
                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                Limit =
                    match limit with
                    | Some n -> Some (SqlExpr.Literal(SqlLiteral.Integer(int64 n)))
                    | None -> baseCore.Limit }
        let countSourceSel = { Ctes = []; Body = SingleSelect countSourceCore }
        let countRowAlias = sprintf "gjn%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let countCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
              Source = Some(DerivedTable(countSourceSel, countRowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }

    let entityJsonExpr alias =
        SqlExpr.FunctionCall("jsonb_set", [
            SqlExpr.Column(Some alias, "Value")
            SqlExpr.Literal(SqlLiteral.String "$.Id")
            SqlExpr.Column(Some alias, "Id")
        ])
    let buildGroupChainRowset (rt: GroupJoinRuntime) (chain: GroupJoinGroupChainDescriptor) =
        let rowAlias = sprintf "gjr%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let numberedAlias = sprintf "gjn%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let distinctAlias = sprintf "gjd%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let innerParam = rt.InnerKeySelector.Parameters.[0]
        let chainCtx =
            { rt.InnerCtx with
                Joins = ResizeArray() }
        let rowKeyExpr =
            match rt.TryTranslateDbRefValueIdKey innerParam rowAlias rt.InnerKeySelector.Body with
            | Some translated -> translated
            | None -> rt.TranslateJoinExpr chainCtx rowAlias rt.Vars (Some innerParam) rt.InnerKeySelector.Body
        let correlation = SqlExpr.Binary(rt.OuterKeyExpr, BinaryOperator.Eq, rowKeyExpr)
        let whereExpr =
            chain.WherePredicates
            |> List.map (fun pred -> rt.TranslateJoinExpr chainCtx rowAlias rt.Vars (Some pred.Parameters.[0]) pred.Body)
            |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) correlation
        let orderBy =
            if chain.OrderKeys.IsEmpty then
                [{ Expr = SqlExpr.Column(Some rowAlias, "Id"); Direction = SortDirection.Asc }]
            else
                chain.OrderKeys
                |> List.map (fun (keySel, dir) ->
                    { Expr = rt.TranslateJoinExpr chainCtx rowAlias rt.Vars (Some keySel.Parameters.[0]) keySel.Body
                      Direction = dir })
        let rowProjectionBase =
            match chain.SelectProjection with
            | Some proj ->
                [{ Alias = Some "v"; Expr = rt.TranslateJoinExpr chainCtx rowAlias rt.Vars (Some proj.Parameters.[0]) proj.Body }]
            | None ->
                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some rowAlias, "Id") }
                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some rowAlias, "Value") }]
        let numberedCore =
            { mkCore
                (rowProjectionBase @ [
                    { Alias = Some "__ord"
                      Expr = SqlExpr.WindowCall({
                          Kind = WindowFunctionKind.RowNumber
                          Arguments = []
                          PartitionBy = []
                          OrderBy = orderBy |> List.map (fun ob -> ob.Expr, ob.Direction) }) }
                ])
                (Some (DerivedTable(rt.InnerSelect, rowAlias)))
              with
                Joins = rt.MaterializeDiscoveredJoins chainCtx.Joins None None
                Where = Some whereExpr }
        let numberedSel = { Ctes = []; Body = SingleSelect numberedCore }
        let dedupedSel, dedupedAlias =
            if chain.Distinct then
                match chain.SelectProjection with
                | None ->
                    raise (NotSupportedException(
                        "Error: GroupJoin Distinct requires a Select projection.\n" +
                        "Fix: Project the group value first, for example g.Select(x => x.Region).Distinct()."))
                | Some _ ->
                    let distinctCore =
                        { Distinct = false
                          Projections = ProjectionSetOps.ofList [
                              { Alias = Some "v"; Expr = SqlExpr.Column(Some distinctAlias, "v") }
                              { Alias = Some "__ord"; Expr = SqlExpr.AggregateCall(AggregateKind.Min, Some(SqlExpr.Column(Some distinctAlias, "__ord")), false, None) }
                          ]
                          Source = Some(DerivedTable(numberedSel, distinctAlias))
                          Joins = []
                          Where = None
                          GroupBy = [SqlExpr.Column(Some distinctAlias, "v")]
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None }
                    { Ctes = []; Body = SingleSelect distinctCore }, distinctAlias
            else
                numberedSel, numberedAlias
        let limitExpr, offsetExpr = buildLimitOffset chain.Take chain.Skip
        let boundedProjections =
            match chain.SelectProjection with
            | Some _ ->
                [{ Alias = Some "v"; Expr = SqlExpr.Column(Some dedupedAlias, "v") }
                 { Alias = Some "__ord"; Expr = SqlExpr.Column(Some dedupedAlias, "__ord") }]
            | None ->
                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some dedupedAlias, "Id") }
                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some dedupedAlias, "Value") }
                 { Alias = Some "__ord"; Expr = SqlExpr.Column(Some dedupedAlias, "__ord") }]
        let boundedCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList boundedProjections
              Source = Some(DerivedTable(dedupedSel, dedupedAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = [{ Expr = SqlExpr.Column(Some dedupedAlias, "__ord"); Direction = SortDirection.Asc }]
              Limit = limitExpr
              Offset = offsetExpr }
        let boundedSel = { Ctes = []; Body = SingleSelect boundedCore }
        let finalSel =
            if chain.UsesDefaultIfEmpty then
                let normalizedBoundedCore =
                    normalizeUnionArm
                        (fun () -> sprintf "gju%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1))
                        (boundedProjections |> List.map (fun p -> p.Alias.Value))
                        boundedCore
                let existsAlias = sprintf "gjf%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
                let existsCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                      Source = Some(DerivedTable({ Ctes = []; Body = SingleSelect normalizedBoundedCore }, existsAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                let defaultProjections =
                    match chain.SelectProjection with
                    | Some _ ->
                        [{ Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Null) }
                         { Alias = Some "__ord"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }]
                    | None ->
                        [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Null) }
                         { Alias = Some "Value"; Expr = SqlExpr.Literal(SqlLiteral.Null) }
                         { Alias = Some "__ord"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }]
                let defaultCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList defaultProjections
                      Source = None
                      Joins = []
                      Where = Some (SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists { Ctes = []; Body = SingleSelect existsCore }))
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = UnionAllSelect(normalizedBoundedCore, [defaultCore]) }
            else
                boundedSel
        finalSel, chain.SelectProjection.IsSome

    let buildGroupChainCollection (rt: GroupJoinRuntime) (chain: GroupJoinGroupChainDescriptor) =
        let rowsetSel, isProjected = buildGroupChainRowset rt chain
        let rowsetAlias = sprintf "gja%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let valueExpr =
            if isProjected then SqlExpr.Column(Some rowsetAlias, "v")
            else entityJsonExpr rowsetAlias
        let outerCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.FunctionCall("jsonb_group_array", [valueExpr]) }]
              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let buildAggregateOverChain (rt: GroupJoinRuntime) (chain: GroupJoinGroupChainDescriptor) (aggKind: AggregateKind) (selectorOpt: LambdaExpression option) (coalesceZero: bool) =
        let rowsetSel, isProjected = buildGroupChainRowset rt chain
        let rowsetAlias = sprintf "gjg%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let aggCtx = QueryContext.SingleSource(rt.InnerRootTable)
        let aggregateArg =
            match selectorOpt, chain.SelectProjection, isProjected with
            | Some _, Some _, _ ->
                raise (NotSupportedException(
                    "Error: GroupJoin chained aggregate cannot apply a selector after Select.\n" +
                    "Fix: Use the projected chain directly or remove the inner Select."))
            | Some sel, _, _ ->
                let selCtx = { aggCtx with Joins = ResizeArray() }
                let selExpr = rt.TranslateJoinExpr selCtx rowsetAlias rt.Vars (Some sel.Parameters.[0]) sel.Body
                let joins = rt.MaterializeDiscoveredJoins selCtx.Joins None None
                Some(selExpr, joins)
            | None, Some _, true ->
                Some(SqlExpr.Column(Some rowsetAlias, "v"), [])
            | None, _, false when aggKind = AggregateKind.Count ->
                None
            | None, _, false ->
                raise (NotSupportedException(
                    "Error: GroupJoin chained aggregate requires a selector.\n" +
                    "Fix: Pass a selector lambda, or project the value first with .Select(...)."))
            | None, _, true ->
                Some(SqlExpr.Column(Some rowsetAlias, "v"), [])
        let aggregateSource, aggregateJoins, aggregateExpr =
            match aggregateArg with
            | Some (argExpr, joins) ->
                Some(DerivedTable(rowsetSel, rowsetAlias)), joins, SqlExpr.AggregateCall(aggKind, Some argExpr, false, None)
            | None ->
                Some(DerivedTable(rowsetSel, rowsetAlias)), [], SqlExpr.AggregateCall(aggKind, None, false, None)
        let aggregateExpr =
            if coalesceZero then SqlExpr.Coalesce(aggregateExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
            else aggregateExpr
        SqlExpr.ScalarSubquery {
            Ctes = []
            Body = SingleSelect {
                Distinct = false
                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = aggregateExpr }]
                Source = aggregateSource
                Joins = aggregateJoins
                Where = None
                GroupBy = []
                Having = None
                OrderBy = []
                Limit = None
                Offset = None
            } }

    let buildCountPredicateOverChain (rt: GroupJoinRuntime) (chain: GroupJoinGroupChainDescriptor) (pred: LambdaExpression) =
        match chain.SelectProjection with
        | Some _ ->
            raise (NotSupportedException(
                "Error: GroupJoin chained Count(predicate) after Select is not supported.\n" +
                "Fix: Move the predicate before Select, or count the projected chain without a predicate."))
        | None ->
            let rowsetSel, _ = buildGroupChainRowset rt chain
            let rowsetAlias = sprintf "gjp%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
            let predCtx = QueryContext.SingleSource(rt.InnerRootTable)
            let predCtx = { predCtx with Joins = ResizeArray() }
            let predExpr = rt.TranslateJoinExpr predCtx rowsetAlias rt.Vars (Some pred.Parameters.[0]) pred.Body
            let countCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                  Source = Some(DerivedTable(rowsetSel, rowsetAlias))
                  Joins = rt.MaterializeDiscoveredJoins predCtx.Joins None None
                  Where = Some predExpr
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }

    let buildExistsOverChain (rt: GroupJoinRuntime) (chain: GroupJoinGroupChainDescriptor) (predOpt: LambdaExpression option) (negate: bool) =
        let rowsetSel, isProjected = buildGroupChainRowset rt chain
        let rowsetAlias = sprintf "gjx%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let predicateExpr, predicateJoins =
            match predOpt with
            | None -> None, []
            | Some pred when isProjected ->
                raise (NotSupportedException(
                    "Error: GroupJoin chained predicate after Select is not supported.\n" +
                    "Fix: Move the predicate before Select, or remove the inner Select."))
            | Some pred ->
                let predCtx = QueryContext.SingleSource(rt.InnerRootTable)
                let predCtx = { predCtx with Joins = ResizeArray() }
                let predExpr = rt.TranslateJoinExpr predCtx rowsetAlias rt.Vars (Some pred.Parameters.[0]) pred.Body
                Some predExpr, rt.MaterializeDiscoveredJoins predCtx.Joins None None
        let existsCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
              Joins = predicateJoins
              Where =
                match predicateExpr with
                | Some pred when negate -> Some(SqlExpr.Unary(UnaryOperator.Not, pred))
                | Some pred -> Some pred
                | None -> None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }
        let existsExpr = SqlExpr.Exists { Ctes = []; Body = SingleSelect existsCore }
        if negate then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr

    let buildContainsOverChain (rt: GroupJoinRuntime) (chain: GroupJoinGroupChainDescriptor) (valueExpr: Expression) =
        let rowsetSel, isProjected = buildGroupChainRowset rt chain
        if not isProjected then
            raise (NotSupportedException(
                "Error: GroupJoin Contains requires a projected scalar chain.\n" +
                "Fix: Project the compared value first, for example g.Select(x => x.Region).Contains(value)."))
        let rowsetAlias = sprintf "gjh%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let valueDu = rt.TranslateOuterExpr valueExpr
        let containsCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
              Joins = []
              Where = Some (SqlExpr.Binary(SqlExpr.Column(Some rowsetAlias, "v"), BinaryOperator.Is, valueDu))
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }
        SqlExpr.Exists { Ctes = []; Body = SingleSelect containsCore }
