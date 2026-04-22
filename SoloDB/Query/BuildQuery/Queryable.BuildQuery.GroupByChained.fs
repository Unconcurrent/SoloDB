namespace SoloDatabase

open System
open System.Collections
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
    open QueryableBuildQueryWindowHelpers

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

    let private buildDescriptor (source: Expression) (outerDistinct: bool) (state: ExtractionState) (terminal: Terminal) : QueryDescriptor =
        {
            Source = source
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
            Terminal = terminal
            GroupByHavingPredicate = state.GroupByHaving
            DefaultIfEmpty = state.DefaultIfEmpty
            PostSelectDefaultIfEmpty = state.PostSelectDefaultIfEmpty
            SelectManyInnerLambda = None
        }

    let private tryExtractGroupByQueryDescriptor (groupParam: ParameterExpression) (expr: Expression) : QueryDescriptor option =
        let expr, outerDistinct, _ = preprocessRoot expr
        let isGroupRoot (e: Expression) =
            match unwrapConvert e with
            | :? ParameterExpression as p -> Object.ReferenceEquals(p, groupParam)
            | _ -> false
        if not (isRootedChain unwrapConvert isGroupRoot expr) then
            None
        else
            let state = createState ()
            let _innerSource = walkChain extractorConfig state expr
            finalizeState state
            Some (buildDescriptor expr outerDistinct state (Terminal.Select expr))

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
                    Some (buildDescriptor expr outerDistinct state recognized.Terminal, recognized.Terminal)
        | _ -> None

    let private tryFindSingleParameter (expr: Expression) =
        let seen = ResizeArray<ParameterExpression>()
        let rec visit (e: Expression) =
            if not (isNull e) then
                match e with
                | :? ParameterExpression as p ->
                    if not (seen |> Seq.exists (fun existing -> Object.ReferenceEquals(existing, p))) then
                        seen.Add(p)
                | :? LambdaExpression as lambda ->
                    visit lambda.Body
                | :? UnaryExpression as u ->
                    visit u.Operand
                | :? BinaryExpression as b ->
                    visit b.Left
                    visit b.Right
                    visit b.Conversion
                | :? MethodCallExpression as mc ->
                    visit mc.Object
                    mc.Arguments |> Seq.iter visit
                | :? MemberExpression as m ->
                    visit m.Expression
                | :? ConditionalExpression as c ->
                    visit c.Test
                    visit c.IfTrue
                    visit c.IfFalse
                | :? InvocationExpression as i ->
                    visit i.Expression
                    i.Arguments |> Seq.iter visit
                | :? NewExpression as n ->
                    n.Arguments |> Seq.iter visit
                | :? NewArrayExpression as na ->
                    na.Expressions |> Seq.iter visit
                | :? MemberInitExpression as mi ->
                    visit mi.NewExpression
                    mi.Bindings
                    |> Seq.iter (function
                        | :? MemberAssignment as ma -> visit ma.Expression
                        | _ -> ())
                | :? ListInitExpression as li ->
                    visit li.NewExpression
                    li.Initializers |> Seq.collect (fun init -> init.Arguments) |> Seq.iter visit
                | _ -> ()
        visit expr
        match seen |> Seq.toList with
        | [param] -> Some param
        | _ -> None

    /// Translate a group-chain expression against the correlated subquery alias.
    let private translateExprAgainst (sourceCtx: QueryContext) (subAlias: string) (vars: Dictionary<string, obj>) (expr: Expression) : SqlExpr =
        match QueryTranslatorVisitPost.tryExtractLambdaExpression expr with
        | ValueSome lambda ->
            translateExprDu sourceCtx subAlias (lambda :> Expression) vars
        | ValueNone ->
            translateJoinSingleSourceExpression sourceCtx subAlias vars (tryFindSingleParameter expr) expr

    let private normalizeScalarExpr (exprType: Type) (expr: SqlExpr) =
        if QueryTranslator.isPrimitiveSQLiteType exprType then
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(
                    SqlExpr.FunctionCall("typeof", [expr]),
                    BinaryOperator.Eq,
                    SqlExpr.Literal(SqlLiteral.String "blob")),
                 SqlExpr.FunctionCall("json_extract", [expr; SqlExpr.Literal(SqlLiteral.String "$")])),
                [],
                Some expr)
        else
            expr

    /// Build the null-safe correlation predicate.
    /// Translates the group key expression against the subquery alias and compares to the outer group key.
    let private buildCorrelation (sourceCtx: QueryContext) (subAlias: string) (groupRowAlias: string) (vars: Dictionary<string, obj>) (groupByExprs: Expression array) : SqlExpr =
        let outerKey = SqlExpr.Column(Some groupRowAlias, "__solodb_group_key")
        let innerKey = SqlExpr.Column(Some subAlias, "__solodb_group_key")
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
        let wherePreds = desc.WherePredicates |> List.map (translateExprAgainst sourceCtx subAlias vars)

        let fullWhere =
            match wherePreds with
            | [] -> Some correlation
            | preds -> Some (preds |> List.fold (fun acc p -> SqlExpr.Binary(acc, BinaryOperator.And, p)) correlation)

        // Translate OrderBy sort keys
        let orderBy =
            desc.SortKeys
            |> List.map (fun (keyExpr, dir) ->
                { Expr = translateExprAgainst sourceCtx subAlias vars keyExpr; Direction = dir })

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
        (groupByExprs: Expression array) (desc: QueryDescriptor) (aggKind: AggregateKind) (selectorOpt: Expression option) (coalesceZero: bool) : SqlExpr =

        let subAlias = nextAlias "_gagg"

        let correlation = buildCorrelation sourceCtx subAlias groupRowAlias vars groupByExprs

        let wherePreds = desc.WherePredicates |> List.map (translateExprAgainst sourceCtx subAlias vars)

        let fullWhere =
            match wherePreds with
            | [] -> Some correlation
            | preds -> Some (preds |> List.fold (fun acc p -> SqlExpr.Binary(acc, BinaryOperator.And, p)) correlation)

        let aggArg =
            match selectorOpt with
            | Some sel -> Some (translateExprAgainst sourceCtx subAlias vars sel |> normalizeScalarExpr sel.Type)
            | None when desc.SelectProjection.IsSome ->
                match desc.SelectProjection with
                | Some proj -> Some (translateExprAgainst sourceCtx subAlias vars (proj :> Expression) |> normalizeScalarExpr proj.ReturnType)
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
                let selDu = translateExprAgainst sourceCtx subAlias vars (proj :> Expression) |> normalizeScalarExpr proj.ReturnType
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
        (groupByExprs: Expression array) (desc: QueryDescriptor) (predOpt: Expression option) (negate: bool) : SqlExpr =

        let subAlias = nextAlias "_gex"
        let correlation = buildCorrelation sourceCtx subAlias groupRowAlias vars groupByExprs

        let wherePreds = desc.WherePredicates |> List.map (translateExprAgainst sourceCtx subAlias vars)

        let predExpr =
            match predOpt with
            | Some pred ->
                let predDu = translateExprAgainst sourceCtx subAlias vars pred
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

        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let subAlias, core = buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc oneProj

        // If Distinct + SelectProjection, count distinct projected values
        let innerCore =
            if desc.GroupByKey.IsSome then
                let keyExpr = translateExprAgainst sourceCtx subAlias vars (desc.GroupByKey.Value :> Expression) |> normalizeScalarExpr desc.GroupByKey.Value.ReturnType
                { core with Distinct = true; Projections = ProjectionSetOps.ofList [{ Alias = Some "k"; Expr = keyExpr }] }
            elif desc.Distinct && desc.SelectProjection.IsSome then
                let proj = desc.SelectProjection.Value
                let projDu = translateExprAgainst sourceCtx subAlias vars (proj :> Expression) |> normalizeScalarExpr proj.ReturnType
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

    let private getSetOps (desc: QueryDescriptor) =
        if desc.SetOps.IsEmpty then
            desc.SetOp |> Option.toList
        else
            desc.SetOps

    let private tryExtractInt64 (exprOpt: Expression option) =
        match exprOpt with
        | Some (:? ConstantExpression as ce) -> Some (SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
        | _ -> None

    let private buildLimitOffset (takeExpr: Expression option) (skipExpr: Expression option) =
        let limit =
            match tryExtractInt64 takeExpr, tryExtractInt64 skipExpr with
            | Some n, _ -> Some n
            | None, Some _ -> Some(SqlExpr.Literal(SqlLiteral.Integer -1L))
            | None, None -> None
        limit, tryExtractInt64 skipExpr

    let rec private buildProjectedChainRowset
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (vars: Dictionary<string, obj>)
        (groupParam: ParameterExpression) (groupByExprs: Expression array) (desc: QueryDescriptor) : SqlSelect =

        let setOps = getSetOps desc
        let descBase =
            { desc with
                Distinct = false
                Limit = None
                Offset = None
                SortKeys = if setOps.IsEmpty then desc.SortKeys else [] }
        let projectedLambda =
            match desc.SelectProjection with
            | Some proj -> proj
            | None ->
                raise (NotSupportedException(
                    "Error: GroupBy set operations require a projected value chain.\n" +
                    "Fix: Project the group value first, for example g.Select(x => x.Code).UnionBy(...)."))
        let normalizeProjectedValue (valueType: Type) (expr: SqlExpr) =
            normalizeScalarExpr valueType expr

        let identityKeyLambda =
            let p = Expression.Parameter(projectedLambda.ReturnType, "x")
            Expression.Lambda(p :> Expression, p)

        let subAlias, baseCore =
            buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs descBase []
        let effectiveOrder =
            if baseCore.OrderBy.IsEmpty then
                [{ Expr = SqlExpr.Column(Some subAlias, "Id"); Direction = SortDirection.Asc }]
            else
                baseCore.OrderBy
        let projectedCore =
            { baseCore with
                Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"
                          Expr =
                            translateExprAgainst sourceCtx subAlias vars (projectedLambda :> Expression)
                            |> normalizeProjectedValue projectedLambda.ReturnType }
                        { Alias = Some "__ord"
                          Expr =
                            rowNumberOver (effectiveOrder |> List.map (fun ob -> ob.Expr, ob.Direction)) }
                    ]
                Distinct = false
                OrderBy = effectiveOrder
                Limit = None
                Offset = None }
        let projectedSel = { Ctes = []; Body = SingleSelect projectedCore }

        let evaluateConstantEnumerable (expr: Expression) : obj list =
            if not (QueryTranslatorBase.isFullyConstant expr) then
                raise (NotSupportedException(
                    "Error: GroupBy set operator right side must be a correlated group chain or a constant sequence.\n" +
                    "Fix: Project the right operand from the same group, or use a constant array/list, or move the operator after AsEnumerable()."))
            match QueryTranslator.evaluateExpr<IEnumerable> expr with
            | null -> []
            | values -> [ for value in values -> value ]

        let buildConstantProjectedRowset (values: obj list) =
            let mkValueExpr value =
                match value with
                | null -> SqlExpr.Literal(SqlLiteral.Null)
                | _ -> allocateParam vars value
            let arms =
                values
                |> List.mapi (fun i value ->
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = mkValueExpr value }
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
            match arms with
            | [] ->
                { Ctes = []
                  Body =
                    SingleSelect
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
                          Offset = None } }
            | head :: tail ->
                { Ctes = []
                  Body =
                    match tail with
                    | [] -> SingleSelect head
                    | _ -> UnionAllSelect(head, tail) }

        let rec buildRightProjectedRowset (rightExpr: Expression) =
            match tryExtractGroupByQueryDescriptor groupParam rightExpr with
            | Some rightDesc when rightDesc.SelectProjection.IsSome ->
                buildProjectedChainRowset sourceCtx baseTableName innerSelect groupRowAlias vars groupParam groupByExprs rightDesc
            | Some _ ->
                raise (NotSupportedException(
                    "Error: GroupBy set operations require projected right-side values.\n" +
                    "Fix: Project the right operand first, for example g.Where(...).Select(x => x.Code)."))
            | None ->
                buildConstantProjectedRowset (evaluateConstantEnumerable rightExpr)

        let buildProjectedValueSel (rowsetSel: SqlSelect) =
            let valueAlias = nextAlias "gsv"
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
            let keyAlias = nextAlias "gsk"
            let keyExpr =
                if isIdentityLambda (keyLambda :> Expression) then
                    SqlExpr.Column(Some keyAlias, "Value")
                else
                    translateExprDu sourceCtx keyAlias (keyLambda :> Expression) vars
            let keyedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some keyAlias, "Value") }
                        { Alias = Some "k"; Expr = keyExpr }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some keyAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(valueSel, keyAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect keyedCore }

        let buildProjectedMembershipExists (leftExpr: SqlExpr) (rightValuesSel: SqlSelect) =
            let rightAlias = nextAlias "gse"
            let existsCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                  Source = Some(DerivedTable(buildProjectedValueSel rightValuesSel, rightAlias))
                  Joins = []
                  Where = Some(SqlExpr.Binary(leftExpr, BinaryOperator.Is, SqlExpr.Column(Some rightAlias, "Value")))
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                  Offset = None }
            SqlExpr.Exists { Ctes = []; Body = SingleSelect existsCore }

        let buildDistinctByProjectedRowset (rowsetSel: SqlSelect) (keyLambda: LambdaExpression) =
            let keyedSel = buildProjectedKeyedSel rowsetSel keyLambda
            let rankAlias = nextAlias "gsd"
            let rankedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some rankAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rankAlias, "__ord") }
                        { Alias = Some "__rk"
                          Expr =
                            rowNumberByKey (SqlExpr.Column(Some rankAlias, "k")) [SqlExpr.Column(Some rankAlias, "__ord"), SortDirection.Asc] }
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
            let filteredAlias = nextAlias "gsf"
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

        let buildByFilterProjectedRowset (rowsetSel: SqlSelect) (keyLambda: LambdaExpression) (rightValuesSel: SqlSelect) (negate: bool) =
            let keyedSel = buildProjectedKeyedSel rowsetSel keyLambda
            let filterAlias = nextAlias "gsm"
            let membershipPred =
                let existsExpr = buildProjectedMembershipExists (SqlExpr.Column(Some filterAlias, "k")) rightValuesSel
                if negate then SqlExpr.Unary(UnaryOperator.Not, existsExpr) else existsExpr
            let rankedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some filterAlias, "v") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some filterAlias, "__ord") }
                        { Alias = Some "__rk"
                          Expr =
                            rowNumberByKey (SqlExpr.Column(Some filterAlias, "k")) [SqlExpr.Column(Some filterAlias, "__ord"), SortDirection.Asc] }
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
            let filteredAlias = nextAlias "gsr"
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

        let buildUnionByProjectedRowset (rowsetSel: SqlSelect) (rightRowsetSel: SqlSelect) (keyLambda: LambdaExpression) =
            let keyedSel = buildProjectedKeyedSel rowsetSel keyLambda
            let leftAlias = nextAlias "gsu"
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
            let rightKeyedSel = buildProjectedKeyedSel rightRowsetSel keyLambda
            let rightAlias = nextAlias "gsv"
            let rightCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some rightAlias, "v") }
                        { Alias = Some "k"; Expr = SqlExpr.Column(Some rightAlias, "k") }
                        { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rightAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rightKeyedSel, rightAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let unionSel = { Ctes = []; Body = UnionAllSelect(leftCore, [rightCore]) }
            let unionAlias = nextAlias "gsx"
            let rankedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some unionAlias, "v") }
                        { Alias = Some "__src"; Expr = SqlExpr.Column(Some unionAlias, "__src") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some unionAlias, "__ord") }
                        { Alias = Some "__rk"
                          Expr =
                            rowNumberByKey
                                (SqlExpr.Column(Some unionAlias, "k"))
                                [
                                    SqlExpr.Column(Some unionAlias, "__src"), SortDirection.Asc
                                    SqlExpr.Column(Some unionAlias, "__ord"), SortDirection.Asc
                                ] }
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
            let filteredAlias = nextAlias "gsy"
            let filteredCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some filteredAlias, "v") }
                        { Alias = Some "__ord"
                          Expr =
                            rowNumberOver [
                                SqlExpr.Column(Some filteredAlias, "__src"), SortDirection.Asc
                                SqlExpr.Column(Some filteredAlias, "__ord"), SortDirection.Asc
                            ] }
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

        let buildConcatProjectedRowset (rowsetSel: SqlSelect) (rightRowsetSel: SqlSelect) =
            let leftAlias = nextAlias "gsc"
            let leftCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some leftAlias, "v") }
                        { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some leftAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rowsetSel, leftAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let rightAlias = nextAlias "gsw"
            let rightCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some rightAlias, "v") }
                        { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some rightAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(rightRowsetSel, rightAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let unionSel = { Ctes = []; Body = UnionAllSelect(leftCore, [rightCore]) }
            let outAlias = nextAlias "gsz"
            let outCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some outAlias, "v") }
                        { Alias = Some "__ord"
                          Expr =
                            rowNumberOver [
                                SqlExpr.Column(Some outAlias, "__src"), SortDirection.Asc
                                SqlExpr.Column(Some outAlias, "__ord"), SortDirection.Asc
                            ] }
                    ]
                  Source = Some(DerivedTable(unionSel, outAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = [
                    { Expr = SqlExpr.Column(Some outAlias, "__src"); Direction = SortDirection.Asc }
                    { Expr = SqlExpr.Column(Some outAlias, "__ord"); Direction = SortDirection.Asc }
                  ]
                  Limit = None
                  Offset = None }
            { Ctes = []; Body = SingleSelect outCore }

        let applySetOp rowsetSel setOp =
            match setOp with
            | SetOperation.DistinctBy keyExpr ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> buildDistinctByProjectedRowset rowsetSel keyLambda
                | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupBy DistinctBy."))
            | SetOperation.Intersect rightSource ->
                buildByFilterProjectedRowset rowsetSel identityKeyLambda (buildRightProjectedRowset rightSource) false
            | SetOperation.Except rightSource ->
                buildByFilterProjectedRowset rowsetSel identityKeyLambda (buildRightProjectedRowset rightSource) true
            | SetOperation.Union rightSource ->
                buildUnionByProjectedRowset rowsetSel (buildRightProjectedRowset rightSource) identityKeyLambda
            | SetOperation.Concat rightSource ->
                buildConcatProjectedRowset rowsetSel (buildRightProjectedRowset rightSource)
            | SetOperation.IntersectBy(rightKeys, keyExpr) ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> buildByFilterProjectedRowset rowsetSel keyLambda (buildRightProjectedRowset rightKeys) false
                | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupBy IntersectBy."))
            | SetOperation.ExceptBy(rightKeys, keyExpr) ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> buildByFilterProjectedRowset rowsetSel keyLambda (buildRightProjectedRowset rightKeys) true
                | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupBy ExceptBy."))
            | SetOperation.UnionBy(rightSource, keyExpr) ->
                match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
                | ValueSome keyLambda -> buildUnionByProjectedRowset rowsetSel (buildRightProjectedRowset rightSource) keyLambda
                | ValueNone -> raise (NotSupportedException("Cannot extract key selector for GroupBy UnionBy."))

        let setOpSel =
            if setOps.IsEmpty then projectedSel
            else setOps |> List.fold applySetOp projectedSel

        let dedupedSel =
            if desc.Distinct then
                let distinctAlias = nextAlias "gsd"
                let distinctCore =
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
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

        let boundedAlias = nextAlias "gsn"
        let limitExpr, offsetExpr = buildLimitOffset desc.Limit desc.Offset
        let finalValueSel = buildProjectedValueSel dedupedSel
        let translateProjectedOrderExpr alias keyExpr =
            match QueryTranslatorVisitPost.tryExtractLambdaExpression keyExpr with
            | ValueSome lambda when isIdentityLambda (lambda :> Expression) ->
                SqlExpr.Column(Some alias, "Value")
            | _ ->
                translateExprAgainst sourceCtx alias vars keyExpr
        let boundedCore =
            { Distinct = false
              Projections =
                let finalOrderBy =
                    if desc.SortKeys.IsEmpty then
                        [{ Expr = SqlExpr.Column(Some boundedAlias, "__ord"); Direction = SortDirection.Asc }]
                    else
                        desc.SortKeys
                        |> List.map (fun (keyExpr, dir) ->
                            { Expr = translateProjectedOrderExpr boundedAlias keyExpr; Direction = dir })
                let finalOrdExpr =
                    if desc.SortKeys.IsEmpty then
                        SqlExpr.Column(Some boundedAlias, "__ord")
                    else
                        rowNumberOver (
                            finalOrderBy
                            |> List.map (fun ob -> ob.Expr, ob.Direction)
                            |> fun orderings -> orderings @ [SqlExpr.Column(Some boundedAlias, "__ord"), SortDirection.Asc])
                ProjectionSetOps.ofList [
                    { Alias = Some "v"; Expr = SqlExpr.Column(Some boundedAlias, "Value") }
                    { Alias = Some "__ord"; Expr = finalOrdExpr }
                ]
              Source = Some(DerivedTable(finalValueSel, boundedAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy =
                if desc.SortKeys.IsEmpty then
                    [{ Expr = SqlExpr.Column(Some boundedAlias, "__ord"); Direction = SortDirection.Asc }]
                else
                    desc.SortKeys
                    |> List.map (fun (keyExpr, dir) ->
                        { Expr = translateProjectedOrderExpr boundedAlias keyExpr; Direction = dir })
              Limit = limitExpr
              Offset = offsetExpr }
        { Ctes = []; Body = SingleSelect boundedCore }

    /// Main entry: translate a chained group-item expression to SqlExpr.
    /// Returns Some if the expression is a recognized chain rooted in groupParam.
    let tryTranslateGroupByChainedExpr
        (sourceCtx: QueryContext) (baseTableName: string) (innerSelect: SqlSelect) (groupRowAlias: string) (groupParam: ParameterExpression) (vars: Dictionary<string, obj>)
        (groupByExprs: Expression array) (expr: Expression) : SqlExpr option =
        // Handle MemberExpression wrapping: g.OrderBy().First().Property  or  g.OrderBy().First().Field.Member
        // memberAccess is a transform: SqlExpr (JSON blob from chain terminal) → SqlExpr (projected value)
        let memberAccess, innerExpr =
            match expr with
            | :? MemberExpression as me when not (isNull me.Expression) && referencesParam groupParam me.Expression ->
                match me.Expression with
                | :? MethodCallExpression ->
                    // One level: g.Chain().Property — route via type-aware helper (falls to json_extract for row types)
                    let transform (result: SqlExpr) =
                        DateTimeFunctions.translateGroupKeyMemberAccess result me.Expression.Type me.Member.Name
                    Some transform, me.Expression
                | :? MemberExpression as inner when not (isNull inner.Expression) ->
                    match inner.Expression with
                    | :? MethodCallExpression ->
                        // Two levels: g.Chain().Field.Member — extract field from row JSON, then translate member
                        let transform (result: SqlExpr) =
                            let fieldExpr = SqlExpr.FunctionCall("json_extract", [result; SqlExpr.Literal(SqlLiteral.String ("$." + inner.Member.Name))])
                            DateTimeFunctions.translateGroupKeyMemberAccess fieldExpr inner.Type me.Member.Name
                        Some transform, inner.Expression
                    | _ -> None, expr
                | _ -> None, expr
            | _ -> None, expr

        match tryExtractGroupByTerminalChain groupParam innerExpr with
        | None -> None
        | Some (desc, terminal) ->
            let setOps = getSetOps desc
            let useProjectedRowset = desc.SelectProjection.IsSome && (desc.Distinct || not setOps.IsEmpty)
            let projectedRowset = lazy (buildProjectedChainRowset sourceCtx baseTableName innerSelect groupRowAlias vars groupParam groupByExprs desc)
            let buildProjectedAggregateFromRowset aggKind coalesceZero =
                let rowsetAlias = nextAlias "_grp"
                let aggExpr = SqlExpr.AggregateCall(aggKind, Some(SqlExpr.Column(Some rowsetAlias, "v")), false, None)
                let aggExpr = if coalesceZero then SqlExpr.Coalesce(aggExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)]) else aggExpr
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = aggExpr }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
            let buildProjectedCountFromRowset () =
                let rowsetAlias = nextAlias "_grc"
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                SqlExpr.Coalesce(SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
            let buildProjectedElementFromRowset pickLast =
                let rowsetAlias = nextAlias "_gre"
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some rowsetAlias, "v") }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = if pickLast then SortDirection.Desc else SortDirection.Asc }]
                      Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                      Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
            let buildProjectedContainsFromRowset value =
                let rowsetAlias = nextAlias "_grn"
                let valueDu = translateExprDu sourceCtx groupRowAlias value vars
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = Some(SqlExpr.Binary(SqlExpr.Column(Some rowsetAlias, "v"), BinaryOperator.Is, valueDu))
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                      Offset = None }
                SqlExpr.Exists { Ctes = []; Body = SingleSelect core }
            let buildProjectedSelectFromRowset () =
                let rowsetAlias = nextAlias "_grs"
                let core =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.Column(Some rowsetAlias, "v")]) }]
                      Source = Some(DerivedTable(projectedRowset.Value, rowsetAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }

            let result =
                match terminal with
                | Terminal.Count | Terminal.LongCount ->
                    if useProjectedRowset then buildProjectedCountFromRowset ()
                    else buildCountSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc
                | Terminal.Sum sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Sum (Some sel) true
                | Terminal.SumProjected ->
                    if useProjectedRowset then buildProjectedAggregateFromRowset AggregateKind.Sum true
                    else buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Sum None true
                | Terminal.Min sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Min (Some sel) false
                | Terminal.MinProjected ->
                    if useProjectedRowset then buildProjectedAggregateFromRowset AggregateKind.Min false
                    else buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Min None false
                | Terminal.Max sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Max (Some sel) false
                | Terminal.MaxProjected ->
                    if useProjectedRowset then buildProjectedAggregateFromRowset AggregateKind.Max false
                    else buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Max None false
                | Terminal.Average sel ->
                    buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Avg (Some sel) false
                | Terminal.AverageProjected ->
                    if useProjectedRowset then buildProjectedAggregateFromRowset AggregateKind.Avg false
                    else buildAggregateSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc AggregateKind.Avg None false
                | Terminal.Exists ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc None false
                | Terminal.Any(Some pred) ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc (Some pred) false
                | Terminal.Any None ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc None false
                | Terminal.All pred ->
                    buildExistsSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc (Some pred) true
                | Terminal.First _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset false
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false
                | Terminal.FirstOrDefault _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset false
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true
                | Terminal.Last _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset true
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc true false
                | Terminal.LastOrDefault _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset true
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc true true
                | Terminal.Single _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset false
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false
                | Terminal.SingleOrDefault _ ->
                    if useProjectedRowset then buildProjectedElementFromRowset false
                    else buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true
                | Terminal.ElementAt idx ->
                    let desc = { desc with Offset = Some idx }
                    if useProjectedRowset then
                        let projectedRowset = buildProjectedChainRowset sourceCtx baseTableName innerSelect groupRowAlias vars groupParam groupByExprs desc
                        let rowsetAlias = nextAlias "_gre"
                        let core =
                            { Distinct = false
                              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some rowsetAlias, "v") }]
                              Source = Some(DerivedTable(projectedRowset, rowsetAlias))
                              Joins = []
                              Where = None
                              GroupBy = []
                              Having = None
                              OrderBy = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Asc }]
                              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                              Offset = None }
                        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                    else
                        buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false false
                | Terminal.ElementAtOrDefault idx ->
                    let desc = { desc with Offset = Some idx }
                    if useProjectedRowset then
                        let projectedRowset = buildProjectedChainRowset sourceCtx baseTableName innerSelect groupRowAlias vars groupParam groupByExprs desc
                        let rowsetAlias = nextAlias "_gre"
                        let core =
                            { Distinct = false
                              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some rowsetAlias, "v") }]
                              Source = Some(DerivedTable(projectedRowset, rowsetAlias))
                              Joins = []
                              Where = None
                              GroupBy = []
                              Having = None
                              OrderBy = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Asc }]
                              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
                              Offset = None }
                        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                    else
                        buildElementSubquery sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc false true
                | Terminal.Contains value ->
                    if useProjectedRowset then buildProjectedContainsFromRowset value
                    else
                        let subAlias = nextAlias "_gcon"
                        let correlation = buildCorrelation sourceCtx subAlias groupRowAlias vars groupByExprs
                        let valueDu = translateExprDu sourceCtx groupRowAlias value vars
                        let projExpr =
                            match desc.SelectProjection with
                            | Some proj -> translateExprAgainst sourceCtx subAlias vars (proj :> Expression)
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
                    if useProjectedRowset then buildProjectedSelectFromRowset ()
                    else
                        let subAlias = nextAlias "_gsel"
                        let _, core = buildCorrelatedCore sourceCtx baseTableName innerSelect groupRowAlias vars groupByExprs desc []
                        let core = { core with Source = Some(DerivedTable(innerSelect, subAlias)) }
                        let valueExpr =
                            match desc.SelectProjection with
                            | Some proj -> translateExprAgainst sourceCtx subAlias vars (proj :> Expression)
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

            // Apply member access if wrapping: g.OrderBy().First().Property  or  g.OrderBy().First().Field.Member
            let result =
                match memberAccess with
                | Some transform -> transform result
                | None -> result

            Some result
