namespace SoloDatabase
open System
open System.Collections
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
    open QueryableHelperState
    open QueryableHelperPreprocess
    open QueryableHelperBase
    type GroupJoinElementKind =
        | FirstLike of orDefault: bool
        | LastLike of orDefault: bool
        | SingleLike of orDefault: bool
        | ElementAtLike of indexExpr: Expression * orDefault: bool
    type GroupJoinElementCall =
        { Call: MethodCallExpression
          Kind: GroupJoinElementKind
          Chain: QueryDescriptor }
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
        || not desc.SetOps.IsEmpty

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
    let private materializeInnerRowJoins (rt: GroupJoinRuntime) (alias: string) (joins: ResizeArray<JoinEdge>) =
        let innerMaterializedPaths =
            if rt.InnerCtx.MaterializedPaths.Count > 0 then Some rt.InnerCtx.MaterializedPaths else None
        rt.MaterializeDiscoveredJoins joins (Some ("\"" + alias + "\"")) innerMaterializedPaths
    let internal buildExactDecimalAverageExpr (argExpr: SqlExpr) =
        let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, Some argExpr, false, None)
        SqlExpr.CaseExpr(
            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), SqlExpr.Literal(SqlLiteral.Null)),
            [],
            Some(SqlExpr.FunctionCall("DECIMAL_DIV", [SqlExpr.AggregateCall(AggregateKind.Sum, Some argExpr, false, None); countExpr])))
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

    let private tryExtractGroupQueryDescriptor (rt: GroupJoinRuntime) (expr: Expression) : QueryDescriptor option =
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
                    match unwrapConvert innerSource with
                    | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) ->
                        Some {
                            Source = Expression.Constant(null) :> Expression
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
                            SetOp = state.SetOps |> Seq.tryHead
                            SetOps = state.SetOps |> Seq.toList
                            Terminal = recognized.Terminal
                            GroupByHavingPredicate = state.GroupByHaving
                            DefaultIfEmpty = state.DefaultIfEmpty
                            PostSelectDefaultIfEmpty = state.PostSelectDefaultIfEmpty
                            SelectManyInnerLambda = state.SelectManyLambda
                        }
                    | _ -> None
        | _ -> None

    /// Extract a non-terminal source chain (g.Where().OrderBy().Select()) as a QueryDescriptor.
    /// Uses shared walkChain for non-terminal group source chains.
    let tryExtractGroupSourceDescriptor (rt: GroupJoinRuntime) (expr: Expression) : QueryDescriptor option =
        let isGroupRoot (e: Expression) =
            match unwrapConvert e with
            | :? ParameterExpression as p -> Object.ReferenceEquals(p, rt.GroupParam)
            | _ -> false
        if not (isRootedChain unwrapConvert isGroupRoot expr) then None
        else
            let state = createState ()
            let innerSource = walkChain extractorConfig state expr
            finalizeState state
            match unwrapConvert innerSource with
            | :? ParameterExpression as p when Object.ReferenceEquals(p, rt.GroupParam) ->
                // Fail-closed guard: reject operators that shared walkChain admits
                // but GroupJoin rowset builders do not consume.
                if state.OfTypeName.IsSome
                   || state.CastTypeName.IsSome
                   || state.TakeWhileInfo.IsSome
                   || state.PostBoundTakeWhileInfo.IsSome
                   || state.GroupByKey.IsSome
                   || state.GroupByHaving.IsSome
                   || not (state.SetOps |> Seq.isEmpty)
                   || state.SelectManyLambda.IsSome
                   || not (state.PostBoundWheres |> Seq.isEmpty)
                   || not (state.PostBoundSortKeys |> Seq.isEmpty)
                   || state.PostBoundLimit.IsSome
                   || state.PostBoundOffset.IsSome then
                    None
                else
                Some {
                    Source = Expression.Constant(null) :> Expression
                    OfTypeName = None
                    CastTypeName = None
                    WherePredicates = state.Wheres |> Seq.toList
                    SortKeys = state.SortKeys |> Seq.toList
                    Limit = state.Limit
                    Offset = state.Offset
                    PostBoundWherePredicates = state.PostBoundWheres |> Seq.toList
                    PostBoundSortKeys = state.PostBoundSortKeys |> Seq.toList
                    PostBoundLimit = state.PostBoundLimit
                    PostBoundOffset = state.PostBoundOffset
                    TakeWhileInfo = None
                    PostBoundTakeWhileInfo = None
                    GroupByKey = None
                    Distinct = state.Distinct
                    SelectProjection = state.SelectProjection
                    SetOp = None
                    SetOps = []
                    Terminal = Terminal.Count
                    GroupByHavingPredicate = None
                    DefaultIfEmpty = state.DefaultIfEmpty
                    PostSelectDefaultIfEmpty = state.PostSelectDefaultIfEmpty
                    SelectManyInnerLambda = None
                }
            | _ -> None

    let tryGetGroupChainDescriptor (rt: GroupJoinRuntime) (expr: Expression) : QueryDescriptor option =
        match tryExtractGroupQueryDescriptor rt expr with
        | Some desc when hasQueryDescriptorChainOps desc -> Some desc
        | _ ->
        match tryExtractGroupSourceDescriptor rt expr with
        | Some desc when hasQueryDescriptorChainOps desc -> Some desc
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
                            SetOp = state.SetOps |> Seq.tryHead
                            SetOps = state.SetOps |> Seq.toList
                            Terminal = recognized.Terminal
                            GroupByHavingPredicate = state.GroupByHaving
                            DefaultIfEmpty = state.DefaultIfEmpty
                            PostSelectDefaultIfEmpty = state.PostSelectDefaultIfEmpty
                            SelectManyInnerLambda = None
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
        let nextAlias prefix = sprintf "%s%d" prefix (Interlocked.Increment(rt.InnerCtx.AliasCounter) - 1)
        let rec translateExpr (ctx: QueryContext) (alias: string) (currentParam: ParameterExpression) (expr: Expression) =
            if not (referencesParam rt.OuterParam expr) then
                rt.TranslateJoinExpr ctx alias rt.Vars (Some currentParam) expr
            elif not (referencesParam currentParam expr) then
                rt.TranslateOuterExpr expr
            else
                match expr with
                | :? BinaryExpression as be ->
                    let left = translateExpr ctx alias currentParam be.Left
                    let right = translateExpr ctx alias currentParam be.Right
                    let op =
                        match be.NodeType with
                        | ExpressionType.Coalesce -> None
                        | ExpressionType.Add -> Some BinaryOperator.Add
                        | ExpressionType.Subtract -> Some BinaryOperator.Sub
                        | ExpressionType.Multiply -> Some BinaryOperator.Mul
                        | ExpressionType.Divide -> Some BinaryOperator.Div
                        | ExpressionType.Modulo -> Some BinaryOperator.Mod
                        | ExpressionType.Equal -> Some BinaryOperator.Eq
                        | ExpressionType.NotEqual -> Some BinaryOperator.Ne
                        | ExpressionType.GreaterThan -> Some BinaryOperator.Gt
                        | ExpressionType.GreaterThanOrEqual -> Some BinaryOperator.Ge
                        | ExpressionType.LessThan -> Some BinaryOperator.Lt
                        | ExpressionType.LessThanOrEqual -> Some BinaryOperator.Le
                        | ExpressionType.AndAlso -> Some BinaryOperator.And
                        | ExpressionType.OrElse -> Some BinaryOperator.Or
                        | _ -> None
                    match op with
                    | Some op -> SqlExpr.Binary(left, op, right)
                    | None when be.NodeType = ExpressionType.Coalesce -> SqlExpr.Coalesce(left, [right])
                    | None ->
                        raise (NotSupportedException(
                            $"Error: GroupJoin mixed outer-capture expression '{be.NodeType}' is not supported.\n" +
                            "Fix: Simplify the group predicate or move it after AsEnumerable()."))
                | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked || ue.NodeType = ExpressionType.TypeAs ->
                    translateExpr ctx alias currentParam ue.Operand
                | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
                    SqlExpr.Unary(UnaryOperator.Not, translateExpr ctx alias currentParam ue.Operand)
                | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Negate || ue.NodeType = ExpressionType.NegateChecked ->
                    SqlExpr.Unary(UnaryOperator.Neg, translateExpr ctx alias currentParam ue.Operand)
                | _ ->
                    raise (NotSupportedException(
                        "Error: GroupJoin mixed outer-capture expression is not supported.\n" +
                        "Reason: The predicate mixes group-item and outer-row access in an unsupported shape.\n" +
                        "Fix: Simplify the expression or move it after AsEnumerable()."))

        let innerParam = rt.InnerKeySelector.Parameters.[0]

        let buildEntityRowset () =
            let rowAlias = nextAlias "gjr"
            let baseCtx =
                { rt.InnerCtx with
                    Joins = ResizeArray() }
            let rowKeyExpr =
                match rt.TryTranslateDbRefValueIdKey innerParam rowAlias rt.InnerKeySelector.Body with
                | Some translated -> translated
                | None -> rt.TranslateJoinExpr baseCtx rowAlias rt.Vars (Some innerParam) rt.InnerKeySelector.Body
            let correlation = SqlExpr.Binary(rt.OuterKeyExpr, BinaryOperator.Eq, rowKeyExpr)
            let predicateDus =
                desc.WherePredicates
                |> List.map (fun predExpr ->
                    let pred = asLambda predExpr
                    translateExpr baseCtx rowAlias pred.Parameters.[0] pred.Body)
            let whereExpr = predicateDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) correlation
            let orderBy =
                if desc.SortKeys.IsEmpty then
                    [{ Expr = SqlExpr.Column(Some rowAlias, "Id"); Direction = SortDirection.Asc }]
                else
                    desc.SortKeys
                    |> List.map (fun (keyExpr, dir) ->
                        let keySel = asLambda keyExpr
                        { Expr = translateExpr baseCtx rowAlias keySel.Parameters.[0] keySel.Body
                          Direction = dir })
            let numberedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "Id"; Expr = SqlExpr.Column(Some rowAlias, "Id") }
                        { Alias = Some "Value"; Expr = SqlExpr.Column(Some rowAlias, "Value") }
                        { Alias = Some "__ord"
                          Expr =
                            SqlExpr.WindowCall({
                                Kind = WindowFunctionKind.RowNumber
                                Arguments = []
                                PartitionBy = []
                                OrderBy = orderBy |> List.map (fun ob -> ob.Expr, ob.Direction) }) }
                    ]
                  Source = Some(DerivedTable(rt.InnerSelect, rowAlias))
                  Joins = materializeInnerRowJoins rt rowAlias baseCtx.Joins
                  Where = Some whereExpr
                  GroupBy = []
                  Having = None
                  OrderBy = orderBy
                  Limit = None
                  Offset = None }
            let rec applyWhile (rowsetSel: SqlSelect) (whileInfo: (LambdaExpression * bool) option) =
                match whileInfo with
                | None -> rowsetSel
                | Some (predLambda, isTakeWhile) ->
                    let whileAlias = nextAlias "gjtw"
                    let whileCtx = QueryContext.SingleSource(rt.InnerRootTable)
                    let whileCtx = { whileCtx with Joins = ResizeArray() }
                    let predDu = translateExpr whileCtx whileAlias predLambda.Parameters.[0] predLambda.Body
                    let innerCore =
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "Id"; Expr = SqlExpr.Column(Some whileAlias, "Id") }
                                { Alias = Some "Value"; Expr = SqlExpr.Column(Some whileAlias, "Value") }
                                { Alias = Some "__ord"; Expr = SqlExpr.Column(Some whileAlias, "__ord") }
                                { Alias = Some "_cf"
                                  Expr =
                                    SqlExpr.WindowCall({
                                    Kind = NamedWindowFunction "SUM"
                                    Arguments = [SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.Not, predDu), SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))]
                                    PartitionBy = []
                                    OrderBy = [SqlExpr.Column(Some whileAlias, "__ord"), SortDirection.Asc] }) }
                            ]
                          Source = Some(DerivedTable(rowsetSel, whileAlias))
                          Joins = materializeInnerRowJoins rt whileAlias whileCtx.Joins
                          Where = None
                          GroupBy = []
                          Having = None
                          OrderBy = [{ Expr = SqlExpr.Column(Some whileAlias, "__ord"); Direction = SortDirection.Asc }]
                          Limit = None
                          Offset = None }
                    let innerSel = { Ctes = []; Body = SingleSelect innerCore }
                    let outerAlias = nextAlias "gjwf"
                    let outerCore =
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                                { Alias = Some "Value"; Expr = SqlExpr.Column(Some outerAlias, "Value") }
                                { Alias = Some "__ord"; Expr = SqlExpr.Column(Some outerAlias, "__ord") }
                            ]
                          Source = Some(DerivedTable(innerSel, outerAlias))
                          Joins = []
                          Where = Some (DBRefManyHelpers.buildTakeWhileCfFilter outerAlias isTakeWhile)
                          GroupBy = []
                          Having = None
                          OrderBy = [{ Expr = SqlExpr.Column(Some outerAlias, "__ord"); Direction = SortDirection.Asc }]
                          Limit = None
                          Offset = None }
                    { Ctes = []; Body = SingleSelect outerCore }
            let numberedSel = { Ctes = []; Body = SingleSelect numberedCore }
            numberedSel
            |> fun sel -> applyWhile sel desc.TakeWhileInfo
            |> fun sel -> applyWhile sel desc.PostBoundTakeWhileInfo

        let entityRowset = buildEntityRowset ()
        let isProjected = desc.SelectProjection.IsSome || desc.GroupByKey.IsSome

        let projectedSel =
            match desc.GroupByKey, desc.SelectProjection with
            | Some groupKeyLambda, _ ->
                let gbAlias = nextAlias "gjgb"
                let gbCtx = QueryContext.SingleSource(rt.InnerRootTable)
                let gbCtx = { gbCtx with Joins = ResizeArray() }
                let keyExpr = translateExpr gbCtx gbAlias groupKeyLambda.Parameters.[0] groupKeyLambda.Body
                let gbCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [
                          { Alias = Some "v"; Expr = keyExpr }
                          { Alias = Some "__ord"; Expr = SqlExpr.AggregateCall(AggregateKind.Min, Some(SqlExpr.Column(Some gbAlias, "__ord")), false, None) }
                      ]
                      Source = Some(DerivedTable(entityRowset, gbAlias))
                      Joins = materializeInnerRowJoins rt gbAlias gbCtx.Joins
                      Where = None
                      GroupBy = [keyExpr]
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect gbCore }
            | None, Some projLambda ->
                let projAlias = nextAlias "gjp"
                let projCtx = QueryContext.SingleSource(rt.InnerRootTable)
                let projCtx = { projCtx with Joins = ResizeArray() }
                let projExpr = translateExpr projCtx projAlias projLambda.Parameters.[0] projLambda.Body
                let projCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [
                          { Alias = Some "v"; Expr = projExpr }
                          { Alias = Some "__ord"; Expr = SqlExpr.Column(Some projAlias, "__ord") }
                      ]
                      Source = Some(DerivedTable(entityRowset, projAlias))
                      Joins = materializeInnerRowJoins rt projAlias projCtx.Joins
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = [{ Expr = SqlExpr.Column(Some projAlias, "__ord"); Direction = SortDirection.Asc }]
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect projCore }
            | None, None -> entityRowset

        let setOpSel =
            let evaluateConstantEnumerable (expr: Expression) : obj list =
                if not (QueryTranslatorBase.isFullyConstant expr) then
                    raise (NotSupportedException(
                        "Error: GroupJoin By-set operator requires a constant second sequence.\n" +
                        "Reason: The second sequence cannot be translated on the correlated SQL route.\n" +
                        "Fix: Use a constant array/list, or move the operator after AsEnumerable()."))
                match QueryTranslator.evaluateExpr<IEnumerable> expr with
                | null -> []
                | values -> [ for value in values -> value ]

            let compileObjectSelector (selectorExpr: Expression) =
                match tryExtractLambdaExpression selectorExpr with
                | ValueSome selectorLambda ->
                    let argObj = Expression.Parameter(typeof<obj>, "o")
                    let inlinedBody : Expression =
                        QueryTranslatorBase.inlineLambdaInvocation selectorLambda [| Expression.Convert(argObj, selectorLambda.Parameters.[0].Type) :> Expression |]
                    let boxedBody =
                        if inlinedBody.Type = typeof<obj> then inlinedBody
                        else Expression.Convert(inlinedBody, typeof<obj>) :> Expression
                    Expression.Lambda<Func<obj, obj>>(boxedBody, argObj).Compile(true).Invoke
                | ValueNone ->
                    raise (NotSupportedException("Cannot extract key selector for GroupJoin By-set operator."))

            let buildProjectedValueSel (rowsetSel: SqlSelect) =
                let valueAlias = nextAlias "gjsv"
                let valueCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "Value"; Expr = SqlExpr.Column(Some valueAlias, "v") }
                            { Alias = Some "__ord"; Expr = SqlExpr.Column(Some valueAlias, "__ord") }
                        ]
                      Source = Some(DerivedTable(rowsetSel, valueAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect valueCore }

            let buildProjectedKeyedSel (rowsetSel: SqlSelect) (keyLambda: LambdaExpression) =
                let valueSel = buildProjectedValueSel rowsetSel
                let keyAlias = nextAlias "gjsk"
                let keyCtx = QueryContext.SingleSource(rt.InnerRootTable)
                let keyCtx = { keyCtx with Joins = ResizeArray() }
                let keyExpr =
                    if isIdentityLambda (keyLambda :> Expression) then
                        SqlExpr.Column(Some keyAlias, "Value")
                    else
                        translateExpr keyCtx keyAlias keyLambda.Parameters.[0] keyLambda.Body
                let keyedCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Column(Some keyAlias, "Value") }
                            { Alias = Some "k"; Expr = keyExpr }
                            { Alias = Some "__ord"; Expr = SqlExpr.Column(Some keyAlias, "__ord") }
                        ]
                      Source = Some(DerivedTable(valueSel, keyAlias))
                      Joins = rt.MaterializeDiscoveredJoins keyCtx.Joins None None
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect keyedCore }

            let buildDistinctByProjectedRowset (rowsetSel: SqlSelect) (keyLambda: LambdaExpression) =
                let keyedSel = buildProjectedKeyedSel rowsetSel keyLambda
                let rankAlias = nextAlias "gjsd"
                let rankedCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Column(Some rankAlias, "v") }
                            { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rankAlias, "__ord") }
                            { Alias = Some "__rk"
                              Expr =
                                SqlExpr.WindowCall({
                                    Kind = WindowFunctionKind.RowNumber
                                    Arguments = []
                                    PartitionBy = [SqlExpr.Column(Some rankAlias, "k")]
                                    OrderBy = [SqlExpr.Column(Some rankAlias, "__ord"), SortDirection.Asc] }) }
                        ]
                      Source = Some(DerivedTable(keyedSel, rankAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                let rankedSel = { Ctes = []; Body = SingleSelect rankedCore }
                let filteredAlias = nextAlias "gjsf"
                let filteredCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Column(Some filteredAlias, "v") }
                            { Alias = Some "__ord"; Expr = SqlExpr.Column(Some filteredAlias, "__ord") }
                        ]
                      Source = Some(DerivedTable(rankedSel, filteredAlias))
                      Joins = []
                      Where = Some(SqlExpr.Binary(SqlExpr.Column(Some filteredAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                      GroupBy = []
                      Having = None
                      OrderBy = [{ Expr = SqlExpr.Column(Some filteredAlias, "__ord"); Direction = SortDirection.Asc }]
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect filteredCore }

            let buildMembershipPredicate (keyExpr: SqlExpr) (values: obj list) (negate: bool) =
                let terms =
                    values
                    |> List.map (fun value ->
                        let rightExpr =
                            match value with
                            | null -> SqlExpr.Literal(SqlLiteral.Null)
                            | _ -> allocateParam rt.Vars value
                        SqlExpr.Binary(keyExpr, BinaryOperator.Is, rightExpr))
                match terms with
                | [] -> None
                | head :: tail ->
                    let disjunction = tail |> List.fold (fun acc term -> SqlExpr.Binary(acc, BinaryOperator.Or, term)) head
                    Some(if negate then SqlExpr.Unary(UnaryOperator.Not, disjunction) else disjunction)

            let buildByFilterProjectedRowset (rowsetSel: SqlSelect) (keyLambda: LambdaExpression) (rightKeysExpr: Expression) (negate: bool) =
                let rightKeys = evaluateConstantEnumerable rightKeysExpr
                match rightKeys with
                | [] when negate -> buildDistinctByProjectedRowset rowsetSel keyLambda
                | [] ->
                    let emptyCore =
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Null) }
                                { Alias = Some "__ord"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }
                            ]
                          Source = None
                          Joins = []
                          Where = Some(SqlExpr.Literal(SqlLiteral.Boolean false))
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None }
                    { Ctes = []; Body = SingleSelect emptyCore }
                | _ ->
                    let keyedSel = buildProjectedKeyedSel rowsetSel keyLambda
                    let filterAlias = nextAlias "gjsm"
                    let membershipPred =
                        buildMembershipPredicate (SqlExpr.Column(Some filterAlias, "k")) rightKeys negate
                        |> Option.defaultValue (SqlExpr.Literal(SqlLiteral.Boolean negate))
                    let rankedCore =
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "v"; Expr = SqlExpr.Column(Some filterAlias, "v") }
                                { Alias = Some "__ord"; Expr = SqlExpr.Column(Some filterAlias, "__ord") }
                                { Alias = Some "__rk"
                                  Expr =
                                    SqlExpr.WindowCall({
                                        Kind = WindowFunctionKind.RowNumber
                                        Arguments = []
                                        PartitionBy = [SqlExpr.Column(Some filterAlias, "k")]
                                        OrderBy = [SqlExpr.Column(Some filterAlias, "__ord"), SortDirection.Asc] }) }
                            ]
                          Source = Some(DerivedTable(keyedSel, filterAlias))
                          Joins = []
                          Where = Some membershipPred
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None }
                    let rankedSel = { Ctes = []; Body = SingleSelect rankedCore }
                    let filteredAlias = nextAlias "gjsr"
                    let filteredCore =
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "v"; Expr = SqlExpr.Column(Some filteredAlias, "v") }
                                { Alias = Some "__ord"; Expr = SqlExpr.Column(Some filteredAlias, "__ord") }
                            ]
                          Source = Some(DerivedTable(rankedSel, filteredAlias))
                          Joins = []
                          Where = Some(SqlExpr.Binary(SqlExpr.Column(Some filteredAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                          GroupBy = []
                          Having = None
                          OrderBy = [{ Expr = SqlExpr.Column(Some filteredAlias, "__ord"); Direction = SortDirection.Asc }]
                          Limit = None
                          Offset = None }
                    { Ctes = []; Body = SingleSelect filteredCore }

            let buildUnionByProjectedRowset (rowsetSel: SqlSelect) (rightSourceExpr: Expression) (keyLambda: LambdaExpression) =
                let keyedSel = buildProjectedKeyedSel rowsetSel keyLambda
                let leftAlias = nextAlias "gjsu"
                let leftCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Column(Some leftAlias, "v") }
                            { Alias = Some "k"; Expr = SqlExpr.Column(Some leftAlias, "k") }
                            { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }
                            { Alias = Some "__ord"; Expr = SqlExpr.Column(Some leftAlias, "__ord") }
                        ]
                      Source = Some(DerivedTable(keyedSel, leftAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                let rightItems = evaluateConstantEnumerable rightSourceExpr
                let projectKey = compileObjectSelector (keyLambda :> Expression)
                let rightCores =
                    rightItems
                    |> List.mapi (fun i item ->
                        let keyValue = projectKey item
                        { Distinct = false
                          Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "v"; Expr = allocateParam rt.Vars item }
                                { Alias = Some "k"; Expr = match keyValue with null -> SqlExpr.Literal(SqlLiteral.Null) | _ -> allocateParam rt.Vars keyValue }
                                { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }
                                { Alias = Some "__ord"; Expr = SqlExpr.Literal(SqlLiteral.Integer(int64 (i + 1))) }
                            ]
                          Source = None
                          Joins = []
                          Where = None
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None })
                let unionSel =
                    match rightCores with
                    | [] -> { Ctes = []; Body = SingleSelect leftCore }
                    | head :: tail -> { Ctes = []; Body = UnionAllSelect(leftCore, head :: tail) }
                let unionAlias = nextAlias "gjsx"
                let rankedCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Column(Some unionAlias, "v") }
                            { Alias = Some "__src"; Expr = SqlExpr.Column(Some unionAlias, "__src") }
                            { Alias = Some "__ord"; Expr = SqlExpr.Column(Some unionAlias, "__ord") }
                            { Alias = Some "__rk"
                              Expr =
                                SqlExpr.WindowCall({
                                    Kind = WindowFunctionKind.RowNumber
                                    Arguments = []
                                    PartitionBy = [SqlExpr.Column(Some unionAlias, "k")]
                                    OrderBy = [
                                        SqlExpr.Column(Some unionAlias, "__src"), SortDirection.Asc
                                        SqlExpr.Column(Some unionAlias, "__ord"), SortDirection.Asc
                                    ] }) }
                        ]
                      Source = Some(DerivedTable(unionSel, unionAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                let rankedSel = { Ctes = []; Body = SingleSelect rankedCore }
                let filteredAlias = nextAlias "gjsy"
                let filteredCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Column(Some filteredAlias, "v") }
                            { Alias = Some "__ord"
                              Expr =
                                SqlExpr.WindowCall({
                                    Kind = WindowFunctionKind.RowNumber
                                    Arguments = []
                                    PartitionBy = []
                                    OrderBy = [
                                        SqlExpr.Column(Some filteredAlias, "__src"), SortDirection.Asc
                                        SqlExpr.Column(Some filteredAlias, "__ord"), SortDirection.Asc
                                    ] }) }
                        ]
                      Source = Some(DerivedTable(rankedSel, filteredAlias))
                      Joins = []
                      Where = Some(SqlExpr.Binary(SqlExpr.Column(Some filteredAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                      GroupBy = []
                      Having = None
                      OrderBy = [
                        { Expr = SqlExpr.Column(Some filteredAlias, "__src"); Direction = SortDirection.Asc }
                        { Expr = SqlExpr.Column(Some filteredAlias, "__ord"); Direction = SortDirection.Asc }
                      ]
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect filteredCore }

            let setOps =
                if desc.SetOps.IsEmpty then
                    desc.SetOp |> Option.toList
                else
                    desc.SetOps
            let applySetOp rowsetSel setOp =
                match setOp with
                | SetOperation.DistinctBy keyExpr ->
                    match tryExtractLambdaExpression keyExpr with
                    | ValueSome keyLambda -> buildDistinctByProjectedRowset rowsetSel keyLambda
                    | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupJoin DistinctBy."))
                | SetOperation.IntersectBy(rightKeys, keyExpr) ->
                    match tryExtractLambdaExpression keyExpr with
                    | ValueSome keyLambda -> buildByFilterProjectedRowset rowsetSel keyLambda rightKeys false
                    | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupJoin IntersectBy."))
                | SetOperation.ExceptBy(rightKeys, keyExpr) ->
                    match tryExtractLambdaExpression keyExpr with
                    | ValueSome keyLambda -> buildByFilterProjectedRowset rowsetSel keyLambda rightKeys true
                    | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupJoin ExceptBy."))
                | SetOperation.UnionBy(rightSource, keyExpr) ->
                    match tryExtractLambdaExpression keyExpr with
                    | ValueSome keyLambda -> buildUnionByProjectedRowset rowsetSel rightSource keyLambda
                    | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupJoin UnionBy."))
                | _ ->
                    raise (NotSupportedException(
                        "Error: GroupJoin set operation is not supported on this chain.\n" +
                        "Fix: Use a By-key set operator on a projected value chain, or move the operation after AsEnumerable()."))
            match setOps with
            | [] -> projectedSel
            | _ when not isProjected ->
                raise (NotSupportedException(
                    "Error: GroupJoin set operations require a projected value chain.\n" +
                    "Fix: Project the group value first, for example g.Select(x => x.Code).UnionBy(...)."))
            | _ ->
                setOps |> List.fold applySetOp projectedSel

        let dedupedSel =
            if desc.Distinct then
                if not isProjected then
                    raise (NotSupportedException(
                        "Error: GroupJoin Distinct requires a Select projection.\n" +
                        "Fix: Project the group value first, for example g.Select(x => x.Region).Distinct()."))
                let distinctAlias = nextAlias "gjd"
                let distinctCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [
                          { Alias = Some "v"; Expr = SqlExpr.Column(Some distinctAlias, "v") }
                          { Alias = Some "__ord"; Expr = SqlExpr.AggregateCall(AggregateKind.Min, Some(SqlExpr.Column(Some distinctAlias, "__ord")), false, None) }
                      ]
                      Source = Some(DerivedTable(setOpSel, distinctAlias))
                      Joins = []
                      Where = None
                      GroupBy = [SqlExpr.Column(Some distinctAlias, "v")]
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect distinctCore }
            else
                setOpSel

        let boundedAlias = nextAlias "gjn"
        let limitExpr, offsetExpr = buildLimitOffset desc.Limit desc.Offset
        let boundedCore =
            { Distinct = false
              Projections =
                if isProjected then
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some boundedAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some boundedAlias, "__ord") }
                    ]
                else
                    ProjectionSetOps.ofList [
                        { Alias = Some "Id"; Expr = SqlExpr.Column(Some boundedAlias, "Id") }
                        { Alias = Some "Value"; Expr = SqlExpr.Column(Some boundedAlias, "Value") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some boundedAlias, "__ord") }
                    ]
              Source = Some(DerivedTable(dedupedSel, boundedAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = [{ Expr = SqlExpr.Column(Some boundedAlias, "__ord"); Direction = SortDirection.Asc }]
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
                        (if isProjected then [ "v"; "__ord" ] else [ "Id"; "Value"; "__ord" ])
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
        finalSel, isProjected

    let buildGroupChainCollectionQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
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
        let aggregateType =
            match selectorOpt, desc.SelectProjection with
            | Some sel, _ -> Some sel.Body.Type
            | None, Some proj when isProjected -> Some proj.Body.Type
            | _ -> None
        let useExactDecimalAverage =
            aggKind = AggregateKind.Avg
            && (aggregateType |> Option.exists isDecimalOrNullableDecimal)
        let aggregateArg =
            match selectorOpt, desc.SelectProjection, isProjected with
            | Some _, Some _, _ ->
                raise (NotSupportedException(
                    "Error: GroupJoin chained aggregate cannot apply a selector after Select.\n" +
                    "Fix: Use the projected chain directly or remove the inner Select."))
            | Some sel, _, _ ->
                let selCtx = { aggCtx with Joins = ResizeArray() }
                let selExpr = rt.TranslateJoinExpr selCtx rowsetAlias rt.Vars (Some sel.Parameters.[0]) sel.Body
                let joins = materializeInnerRowJoins rt rowsetAlias selCtx.Joins
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
                let expr =
                    if useExactDecimalAverage then buildExactDecimalAverageExpr argExpr
                    else SqlExpr.AggregateCall(aggKind, Some argExpr, false, None)
                Some(DerivedTable(rowsetSel, rowsetAlias)), joins, expr
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
                Some predExpr, materializeInnerRowJoins rt rowsetAlias predCtx.Joins
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

    let buildContainsOverChainQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) (valueExpr: Expression) =
        let rowsetSel, isProjected = buildGroupChainRowsetQ rt desc
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

