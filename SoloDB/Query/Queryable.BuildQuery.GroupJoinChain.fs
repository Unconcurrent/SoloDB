namespace SoloDatabase
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost
open SqlDu.Engine.C1.Spec
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.DBRefManyExtractorHelpers
open SoloDatabase.SharedDescriptorExtract
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
          DefaultIfEmpty: Expression option option }
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
          DefaultIfEmpty = None }
    let hasGroupChainOps (desc: GroupJoinGroupChainDescriptor) =
        not desc.WherePredicates.IsEmpty
        || not desc.OrderKeys.IsEmpty
        || desc.Skip.IsSome
        || desc.Take.IsSome
        || desc.SelectProjection.IsSome
        || desc.Distinct
        || desc.DefaultIfEmpty.IsSome

    let hasQueryDescriptorChainOps (desc: QueryDescriptor) =
        not desc.WherePredicates.IsEmpty
        || not desc.SortKeys.IsEmpty
        || desc.Offset.IsSome
        || desc.Limit.IsSome
        || desc.SelectProjection.IsSome
        || desc.Distinct
        || desc.DefaultIfEmpty.IsSome
        || desc.TakeWhileInfo.IsSome
        || desc.GroupByKey.IsSome
        || desc.SetOp.IsSome

    /// Convert a QueryDescriptor to the legacy GroupJoinGroupChainDescriptor for builders
    /// that have not yet been migrated. TEMPORARY — these builders should consume QueryDescriptor directly.
    let toGroupChainDescriptor (desc: QueryDescriptor) : GroupJoinGroupChainDescriptor =
        let tryLambda (e: Expression) =
            match tryExtractLambdaExpression e with
            | ValueSome l -> Some l
            | ValueNone -> None
        {
            WherePredicates = desc.WherePredicates |> List.choose tryLambda
            OrderKeys =
                desc.SortKeys
                |> List.choose (fun (expr, dir) -> tryLambda expr |> Option.map (fun l -> l, dir))
            Skip = desc.Offset
            Take = desc.Limit
            SelectProjection = desc.SelectProjection
            Distinct = desc.Distinct
            DefaultIfEmpty =
                match desc.DefaultIfEmpty, desc.PostSelectDefaultIfEmpty with
                | Some d, _ -> Some d
                | None, Some d -> Some d
                | None, None -> None
        }
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
    let private extractorConfig =
        {
            EnsureOfTypeSupported = DBRefManyHelpers.ensureOfTypeSupported
            MultipleTakeSkipBoundariesMessage =
                "Error: Multiple Take/Skip boundaries in GroupJoin chain are not supported.\n" +
                "Reason: The current descriptor admits one semantic pagination boundary.\n" +
                "Fix: Keep at most one Take or Skip inside the GroupJoin group chain."
            TooManyTakeWhileBoundariesMessage =
                "Error: More than two TakeWhile/SkipWhile boundaries are not supported in GroupJoin chain.\n" +
                "Reason: The current descriptor supports one inner and one outer boundary.\n" +
                "Fix: Simplify the GroupJoin group chain or move additional windowing after AsEnumerable()."
        }

    let private tryExtractGroupQueryDescriptor (rt: GroupJoinRuntime) (expr: Expression) =
        let expr, outerDistinct, _outerMaterialize = preprocessRoot expr
        match expr with
        | :? MethodCallExpression as mce ->
            match tryRecognizeTerminal mce with
            | None -> None
            | Some recognized ->
                let isGroupRoot (e: Expression) =
                    match unwrapConvert e with
                    | :? ParameterExpression as p -> Object.ReferenceEquals(p, rt.GroupParam)
                    | _ -> false
                if not (isRootedChain unwrapConvert isGroupRoot recognized.Source) then None
                else
                    let state = createState ()
                    let source = normalizeCountBySource recognized.Terminal recognized.Source state
                    let innerSource = walkChain extractorConfig state source
                    finalizeState state
                    placeCountPredicate state recognized.CountPredicate
                    let tryLambda e =
                        match tryExtractLambdaExpression e with
                        | ValueSome l -> Some l
                        | ValueNone -> None
                    let orderKeys =
                        state.SortKeys
                        |> Seq.map (fun (expr, dir) -> tryLambda expr, dir)
                        |> Seq.choose (fun (lam, dir) -> lam |> Option.map (fun l -> l, dir))
                        |> Seq.toList
                    let desc =
                        {
                            WherePredicates =
                                state.Wheres
                                |> Seq.choose tryLambda
                                |> Seq.toList
                            OrderKeys = orderKeys
                            Skip = state.Offset
                            Take = state.Limit
                            SelectProjection = state.SelectProjection
                            Distinct = state.Distinct || outerDistinct
                            DefaultIfEmpty =
                                match state.DefaultIfEmpty, state.PostSelectDefaultIfEmpty with
                                | Some d, _ -> Some d
                                | None, Some d -> Some d
                                | None, None -> None
                        }
                    match unwrapConvert innerSource with
                    | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) ->
                        Some desc
                    | _ -> None
        | _ -> None

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
                    Some { desc with DefaultIfEmpty = Some None }
                | _ -> None
            | None -> None
        | _ -> None

    let tryGetGroupChainDescriptor (rt: GroupJoinRuntime) (expr: Expression) =
        match tryExtractGroupQueryDescriptor rt expr with
        | Some desc when hasGroupChainOps desc -> Some desc
        | _ ->
        match walkGroupChain rt expr with
        | Some desc when hasGroupChainOps desc -> Some desc
        | _ -> None

    let tryExtractGroupTerminalChain (rt: GroupJoinRuntime) (expr: Expression) : (QueryDescriptor * Terminal) option =
        let expr, outerDistinct, _outerMaterialize = preprocessRoot expr
        match expr with
        | :? MethodCallExpression as mce ->
            match tryRecognizeTerminal mce with
            | None -> None
            | Some recognized ->
                let isGroupRoot (e: Expression) =
                    match unwrapConvert e with
                    | :? ParameterExpression as p -> Object.ReferenceEquals(p, rt.GroupParam)
                    | _ -> false
                if not (isRootedChain unwrapConvert isGroupRoot recognized.Source) then None
                else
                    let state = createState ()
                    let source = normalizeCountBySource recognized.Terminal recognized.Source state
                    let innerSource = walkChain extractorConfig state source
                    finalizeState state
                    placeCountPredicate state recognized.CountPredicate
                    let desc : QueryDescriptor =
                        {
                            Source = innerSource
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
                    match unwrapConvert innerSource with
                    | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) ->
                        Some (desc, recognized.Terminal)
                    | _ -> None
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

    let buildCountSelectSubquery (rt: GroupJoinRuntime) (sourceSel: SqlSelect) (limit: int option) =
        let countSourceAlias = sprintf "gjc%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let countSourceCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
              Source = Some(DerivedTable(sourceSel, countSourceAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = limit |> Option.map (fun n -> SqlExpr.Literal(SqlLiteral.Integer(int64 n)))
              Offset = None }
        buildCountSubquery rt countSourceCore None

    let entityJsonExpr alias =
        SqlExpr.FunctionCall("jsonb_set", [
            SqlExpr.Column(Some alias, "Value")
            SqlExpr.Literal(SqlLiteral.String "$.Id")
            SqlExpr.Column(Some alias, "Id")
        ])
    /// Helper: extract LambdaExpression from Expression (shared extractor stores them as Expression).
    let private asLambda (e: Expression) : LambdaExpression = e :?> LambdaExpression

    let buildGroupChainRowsetQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) =
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
            desc.WherePredicates
            |> List.map (fun predExpr ->
                let pred = asLambda predExpr
                rt.TranslateJoinExpr chainCtx rowAlias rt.Vars (Some pred.Parameters.[0]) pred.Body)
            |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) correlation
        let orderBy =
            if desc.SortKeys.IsEmpty then
                [{ Expr = SqlExpr.Column(Some rowAlias, "Id"); Direction = SortDirection.Asc }]
            else
                desc.SortKeys
                |> List.map (fun (keyExpr, dir) ->
                    let keySel = asLambda keyExpr
                    { Expr = rt.TranslateJoinExpr chainCtx rowAlias rt.Vars (Some keySel.Parameters.[0]) keySel.Body
                      Direction = dir })
        let rowProjectionBase =
            match desc.SelectProjection with
            | Some proj ->
                [{ Alias = Some "v"; Expr = rt.TranslateJoinExpr chainCtx rowAlias rt.Vars (Some proj.Parameters.[0]) proj.Body }]
            | None ->
                [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some rowAlias, "Id") }
                 { Alias = Some "Value"; Expr = SqlExpr.Column(Some rowAlias, "Value") }]
        // TakeWhile/SkipWhile: add cumulative stop window
        let extraProjections, hasTakeWhile =
            match desc.TakeWhileInfo with
            | Some (twPred, _isTakeWhile) ->
                let twPredDu = rt.TranslateJoinExpr chainCtx rowAlias rt.Vars (Some twPred.Parameters.[0]) twPred.Body
                let windowSpec =
                    { Kind = NamedWindowFunction "SUM"
                      Arguments = [SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.Not, twPredDu), SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))]
                      PartitionBy = []
                      OrderBy = orderBy |> List.map (fun ob -> ob.Expr, ob.Direction) }
                [{ Alias = Some "_cf"; Expr = SqlExpr.WindowCall windowSpec }], true
            | None -> [], false
        let numberedCore =
            { mkCore
                (rowProjectionBase @ extraProjections @ [
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
        // If TakeWhile/SkipWhile, wrap with filter on _cf
        let filteredSel, filteredAlias =
            if hasTakeWhile then
                let isTakeWhile = match desc.TakeWhileInfo with Some (_, tw) -> tw | None -> true
                let twAlias = sprintf "gjtw%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
                let cfFilter = DBRefManyHelpers.buildTakeWhileCfFilter twAlias isTakeWhile
                let filteredProjs =
                    match desc.SelectProjection with
                    | Some _ ->
                        [{ Alias = Some "v"; Expr = SqlExpr.Column(Some twAlias, "v") }
                         { Alias = Some "__ord"; Expr = SqlExpr.Column(Some twAlias, "__ord") }]
                    | None ->
                        [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some twAlias, "Id") }
                         { Alias = Some "Value"; Expr = SqlExpr.Column(Some twAlias, "Value") }
                         { Alias = Some "__ord"; Expr = SqlExpr.Column(Some twAlias, "__ord") }]
                let filteredCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList filteredProjs
                      Source = Some(DerivedTable(numberedSel, twAlias))
                      Joins = []
                      Where = Some cfFilter
                      GroupBy = []
                      Having = None
                      OrderBy = [{ Expr = SqlExpr.Column(Some twAlias, "__ord"); Direction = SortDirection.Asc }]
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect filteredCore }, twAlias
            else
                numberedSel, numberedAlias
        // GroupByKey (CountBy): group the rowset by key, project key as "v"
        let groupedSel, groupedAlias =
            match desc.GroupByKey with
            | Some groupKeyLambda ->
                let gbAlias = sprintf "gjgb%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
                let gbCtx = { rt.InnerCtx with Joins = ResizeArray() }
                let keyExpr = rt.TranslateJoinExpr gbCtx filteredAlias rt.Vars (Some groupKeyLambda.Parameters.[0]) groupKeyLambda.Body
                let gbCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [
                          { Alias = Some "v"; Expr = keyExpr }
                          { Alias = Some "__ord"; Expr = SqlExpr.AggregateCall(AggregateKind.Min, Some(SqlExpr.Column(Some filteredAlias, "__ord")), false, None) }
                      ]
                      Source = Some(DerivedTable(filteredSel, filteredAlias))
                      Joins = rt.MaterializeDiscoveredJoins gbCtx.Joins None None
                      Where = None
                      GroupBy = [keyExpr]
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect gbCore }, gbAlias
            | None -> filteredSel, filteredAlias
        let dedupedSel, dedupedAlias =
            if desc.Distinct then
                match desc.SelectProjection with
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
                          Source = Some(DerivedTable(groupedSel, distinctAlias))
                          Joins = []
                          Where = None
                          GroupBy = [SqlExpr.Column(Some distinctAlias, "v")]
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None }
                    { Ctes = []; Body = SingleSelect distinctCore }, distinctAlias
            else
                groupedSel, groupedAlias
        let limitExpr, offsetExpr = buildLimitOffset desc.Limit desc.Offset
        let isProjected = desc.SelectProjection.IsSome || desc.GroupByKey.IsSome
        let boundedProjections =
            if isProjected then
                [{ Alias = Some "v"; Expr = SqlExpr.Column(Some dedupedAlias, "v") }
                 { Alias = Some "__ord"; Expr = SqlExpr.Column(Some dedupedAlias, "__ord") }]
            else
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
        let defaultIfEmpty =
            match desc.DefaultIfEmpty, desc.PostSelectDefaultIfEmpty with
            | Some d, _ -> Some d
            | None, Some d -> Some d
            | None, None -> None
        let finalSel =
            if defaultIfEmpty.IsSome then
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
                    if isProjected then
                        let defaultExpr =
                            match defaultIfEmpty with
                            | Some (Some defaultValueExpr) -> rt.TranslateOuterExpr defaultValueExpr
                            | _ -> SqlExpr.Literal(SqlLiteral.Null)
                        [{ Alias = Some "v"; Expr = defaultExpr }
                         { Alias = Some "__ord"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }]
                    else
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
        finalSel, (desc.SelectProjection.IsSome || desc.GroupByKey.IsSome)

    /// Legacy wrapper — delegates to buildGroupChainRowsetQ via toGroupChainDescriptor conversion.
    let buildGroupChainRowset (rt: GroupJoinRuntime) (chain: GroupJoinGroupChainDescriptor) =
        let qdesc : QueryDescriptor =
            { Source = Expression.Constant(null) :> Expression
              OfTypeName = None; CastTypeName = None
              WherePredicates = chain.WherePredicates |> List.map (fun l -> l :> Expression)
              SortKeys = chain.OrderKeys |> List.map (fun (l, d) -> (l :> Expression, d))
              Limit = chain.Take; Offset = chain.Skip
              PostBoundWherePredicates = []; PostBoundSortKeys = []
              PostBoundLimit = None; PostBoundOffset = None
              TakeWhileInfo = None; PostBoundTakeWhileInfo = None
              GroupByKey = None; Distinct = chain.Distinct
              SelectProjection = chain.SelectProjection; SetOp = None
              Terminal = Terminal.Count; GroupByHavingPredicate = None
              DefaultIfEmpty = chain.DefaultIfEmpty; PostSelectDefaultIfEmpty = None }
        buildGroupChainRowsetQ rt qdesc

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

    let buildAggregateOverChainQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) (aggKind: AggregateKind) (selectorOpt: LambdaExpression option) (coalesceZero: bool) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
        let rowsetAlias = sprintf "gjg%d" (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let aggCtx = QueryContext.SingleSource(rt.InnerRootTable)
        let aggregateArg =
            match selectorOpt, desc.SelectProjection, isProjected with
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

    let buildExistsOverChainQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) (predOpt: LambdaExpression option) (negate: bool) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
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
