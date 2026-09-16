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
open SoloDatabase.ChainExpr
open SoloDatabase.ChainPolicy
open SoloDatabase.ChainState
open SoloDatabase.ChainBounds
open SoloDatabase.ChainDescriptorBuild
open SoloDatabase.ChainWalk
open SoloDatabase.GroupJoinRuntimeTypes
open SoloDatabase.GroupJoinAliases
open SoloDatabase.GroupJoinExtract
open SoloDatabase.GroupJoinRowsetSetOps
open SoloDatabase.GroupJoinChainParts
module internal QueryableBuildQueryGroupJoinChain =
    open QueryableHelperJoin
    open QueryableHelperState
    open QueryableHelperPreprocess
    open QueryableHelperBase
    open QueryableBuildQueryWindowHelpers


    let buildGroupChainRowsetQ (rt: GroupJoinRuntime) (desc: QueryDescriptor) =
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
            let rowAlias = GroupJoinAliases.nextRowset rt.InnerCtx
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
                            rowNumberOver (orderBy |> List.map (fun ob -> ob.Expr, ob.Direction)) }
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
                    let whileAlias = GroupJoinAliases.nextTakeWhile rt.InnerCtx
                    let whileCtx = QueryContext.ChildOf(rt.InnerCtx, rt.InnerRootTable)
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
                    let outerAlias = GroupJoinAliases.nextWhileFilter rt.InnerCtx
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
                let gbAlias = GroupJoinAliases.nextGroupBy rt.InnerCtx
                let gbCtx = QueryContext.ChildOf(rt.InnerCtx, rt.InnerRootTable)
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
                let projAlias = GroupJoinAliases.nextProjection rt.InnerCtx
                let projCtx = QueryContext.ChildOf(rt.InnerCtx, rt.InnerRootTable)
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

        let setOpSel = GroupJoinRowsetSetOps.buildSetOpSel rt desc translateExpr isProjected projectedSel
        let dedupedSel =
            if desc.Distinct then
                if not isProjected then
                    raise (NotSupportedException(
                        "Error: GroupJoin Distinct requires a Select projection.\n" +
                        "Fix: Project the group value first, for example g.Select(x => x.Region).Distinct()."))
                let distinctAlias = GroupJoinAliases.nextDistinct rt.InnerCtx
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

        let boundedAlias = GroupJoinAliases.nextNested rt.InnerCtx
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

        // PostBound layer: apply outer Where/OrderBy/Limit/Offset after the inner boundary.
        let boundedSel =
            if desc.HasPostBoundWrapperFields then
                let pbAlias = GroupJoinAliases.nextProjectionBounded rt.InnerCtx
                let pbLimitExpr, pbOffsetExpr = buildLimitOffset desc.PostBoundLimit desc.PostBoundOffset
                let pbWhereDus =
                    desc.PostBoundWherePredicates
                    |> List.choose (fun predExpr ->
                        match tryExtractLambdaExpression predExpr with
                        | ValueSome predLambda ->
                            let predCtx = QueryContext.ChildOf(rt.InnerCtx, rt.InnerRootTable)
                            let predCtx = { predCtx with Joins = ResizeArray() }
                            Some (rt.TranslateJoinExpr predCtx pbAlias rt.Vars (Some predLambda.Parameters.[0]) predLambda.Body)
                        | ValueNone -> None)
                let pbSortDus =
                    desc.PostBoundSortKeys
                    |> List.choose (fun (keyExpr, dir) ->
                        match tryExtractLambdaExpression keyExpr with
                        | ValueSome keyLambda ->
                            let keyCtx = QueryContext.ChildOf(rt.InnerCtx, rt.InnerRootTable)
                            let keyCtx = { keyCtx with Joins = ResizeArray() }
                            let keyDu = rt.TranslateJoinExpr keyCtx pbAlias rt.Vars (Some keyLambda.Parameters.[0]) keyLambda.Body
                            Some { Expr = keyDu; Direction = dir }
                        | ValueNone -> None)
                let pbWhereExpr =
                    match pbWhereDus with
                    | [] -> None
                    | [single] -> Some single
                    | head :: tail -> Some (tail |> List.fold (fun acc p -> SqlExpr.Binary(acc, BinaryOperator.And, p)) head)
                let pbProjs =
                    if isProjected then
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = SqlExpr.Column(Some pbAlias, "v") }
                            { Alias = Some "__ord"; Expr = SqlExpr.Column(Some pbAlias, "__ord") }
                        ]
                    else
                        ProjectionSetOps.ofList [
                            { Alias = Some "Id"; Expr = SqlExpr.Column(Some pbAlias, "Id") }
                            { Alias = Some "Value"; Expr = SqlExpr.Column(Some pbAlias, "Value") }
                            { Alias = Some "__ord"; Expr = SqlExpr.Column(Some pbAlias, "__ord") }
                        ]
                let pbOrderBy =
                    if pbSortDus.IsEmpty then [{ Expr = SqlExpr.Column(Some pbAlias, "__ord"); Direction = SortDirection.Asc }]
                    else pbSortDus
                let pbCore =
                    { Distinct = false
                      Projections = pbProjs
                      Source = Some(DerivedTable(boundedSel, pbAlias))
                      Joins = []
                      Where = pbWhereExpr
                      GroupBy = []
                      Having = None
                      OrderBy = pbOrderBy
                      Limit = pbLimitExpr
                      Offset = pbOffsetExpr }
                { Ctes = []; Body = SingleSelect pbCore }
            else
                boundedSel

        let defaultIfEmpty =
            match desc.DefaultIfEmpty, desc.PostSelectDefaultIfEmpty with
            | Some d, _ -> Some d
            | None, Some d -> Some d
            | None, None -> None
        let finalSel =
            if defaultIfEmpty.IsSome then
                let normalizedBoundedCore =
                    normalizeUnionArm
                        (fun () -> GroupJoinAliases.nextUnionArm rt.InnerCtx)
                        (if isProjected then [ "v"; "__ord" ] else [ "Id"; "Value"; "__ord" ])
                        boundedCore
                let existsAlias = GroupJoinAliases.nextExistsFilter rt.InnerCtx
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
