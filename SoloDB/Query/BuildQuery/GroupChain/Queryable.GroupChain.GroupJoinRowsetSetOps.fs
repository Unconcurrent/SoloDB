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
open SoloDatabase.SqlModel
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryableHelperBase
open SoloDatabase.QueryableHelperState
open SoloDatabase.QueryableBuildQueryWindowHelpers
open SoloDatabase.GroupJoinRuntimeTypes

/// Set operations over a GroupJoin chain rowset.
///
/// Everything arrives as an explicit parameter. `translateExpr` is passed as a function because a
/// set operand may contain expressions that must be translated against a nested alias; it is one
/// value per rowset build, not a record of callbacks threaded through every helper.
module internal GroupJoinRowsetSetOps =
    let buildSetOpSel
        (rt: GroupJoinRuntime)
        (desc: QueryDescriptor)
        (translateExpr: QueryContext -> string -> ParameterExpression -> Expression -> SqlExpr)
        (isProjected: bool)
        (projectedSel: SqlSelect)
        : SqlSelect =
        let evaluateConstantEnumerable (expr: Expression) : obj list =
            if not (QueryTranslatorBaseHelpers.isFullyConstant expr) then
                raise (NotSupportedException(
                    "Error: GroupJoin By-set operator requires a constant second sequence.\n" +
                    "Reason: The second sequence cannot be translated on the correlated SQL route.\n" +
                    "Fix: Use a constant array/list, or move the operator after AsEnumerable()."))
            match QueryTranslatorBaseHelpers.evaluateExpr<IEnumerable> expr with
            | null -> []
            | values -> [ for value in values -> value ]

        let compileObjectSelector (selectorExpr: Expression) =
            match tryExtractLambdaExpression selectorExpr with
            | ValueSome selectorLambda ->
                let argObj = Expression.Parameter(typeof<obj>, "o")
                let inlinedBody : Expression =
                    QueryTranslatorBaseHelpers.inlineLambdaInvocation selectorLambda [| Expression.Convert(argObj, selectorLambda.Parameters.[0].Type) :> Expression |]
                let boxedBody =
                    if inlinedBody.Type = typeof<obj> then inlinedBody
                    else Expression.Convert(inlinedBody, typeof<obj>) :> Expression
                Expression.Lambda<Func<obj, obj>>(boxedBody, argObj).Compile(true).Invoke
            | ValueNone ->
                raise (NotSupportedException("Cannot extract key selector for GroupJoin By-set operator."))

        let buildProjectedValueSel (rowsetSel: SqlSelect) =
            let valueAlias = GroupJoinAliases.nextSetValue rt.InnerCtx
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
            let keyAlias = GroupJoinAliases.nextSetKeyed rt.InnerCtx
            let keyCtx = QueryContext.ChildOf(rt.InnerCtx, rt.InnerRootTable)
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
            let rankAlias = GroupJoinAliases.nextSetDistinct rt.InnerCtx
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
            let filteredAlias = GroupJoinAliases.nextSetFiltered rt.InnerCtx
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
                let filterAlias = GroupJoinAliases.nextSetMembership rt.InnerCtx
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
                let filteredAlias = GroupJoinAliases.nextSetRemaining rt.InnerCtx
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
            let leftAlias = GroupJoinAliases.nextSetUnionLeft rt.InnerCtx
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
            let unionAlias = GroupJoinAliases.nextSetExtra rt.InnerCtx
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
            let filteredAlias = GroupJoinAliases.nextSetSecondary rt.InnerCtx
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

        let setOps = desc.SetOps
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

