namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Threading
open SQLiteTools
open Utils
open SoloDatabase
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

/// GroupJoin handler — extracted from PartB for file size compliance.
module internal QueryableBuildQueryPartBGroupJoin =
    open QueryableHelperState
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
          SelectProjection: LambdaExpression option }

    type GroupJoinElementCall =
        { Call: MethodCallExpression
          Kind: GroupJoinElementKind
          Chain: GroupJoinGroupChainDescriptor }

    type GroupJoinTranslatedArg =
        { Value: SqlExpr
          Error: SqlExpr option }

    let internal applyGroupJoin<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> SqlSelect)
        (expressions: Expression array) =
        match expressions.Length with
        | 4 ->
            let innerExpression = expressions.[0]
            let outerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin outer key selector" expressions.[1]
            let innerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin inner key selector" expressions.[2]
            let resultSelector = unwrapLambdaExpressionOrThrow "GroupJoin result selector" expressions.[3]

            if isCompositeJoinKeyBody outerKeySelector.Body || isCompositeJoinKeyBody innerKeySelector.Body then
                raise (NotSupportedException(
                    "Error: GroupJoin composite key selectors are not supported.\n" +
                    "Reason: Anonymous-type and composite key equality lowering is deferred.\n" +
                    "Fix: Join on a single scalar key or move the query after AsEnumerable()."))

            // Find the root table for the inner source (needed for QueryContext).
            let innerRootTable =
                match tryGetJoinRootSourceTable innerExpression with
                | Some tn -> tn
                | None ->
                    raise (NotSupportedException(
                        "Error: GroupJoin inner source is not supported.\n" +
                        "Reason: The inner query does not resolve to a SoloDB root collection.\n" +
                        "Fix: Use another SoloDB IQueryable rooted in a collection or move the query after AsEnumerable()."))

            if resultSelector.Parameters.Count <> 2 then
                raise (NotSupportedException(
                    "Error: GroupJoin result selector must have two parameters (outer, group).\n" +
                    "Reason: The result selector shape is not recognized.\n" +
                    "Fix: Use (outer, group) => new { ... } pattern."))

            let outerParam = resultSelector.Parameters.[0]
            let groupParam = resultSelector.Parameters.[1]
            // Capture the inner expression for building a subquery inside addComplexFinal.
            let capturedInnerExpr = innerExpression

            addComplexFinal statements (fun ctx ->
                let outerAlias = "o"
                let innerAlias = "gj"
                let innerCtx = QueryContext.SingleSource(innerRootTable)

                // Build the inner source as a full subquery (supports composed inner sources with Where, etc.)
                let innerSelect = translateQueryFn innerCtx ctx.Vars capturedInnerExpr
                let innerSource = DerivedTable(innerSelect, innerAlias)

                let materializeDiscoveredJoins
                    (joins: ResizeArray<JoinEdge>)
                    (materializedRootAlias: string option)
                    (materializedRootPaths: Collections.Generic.HashSet<string> option) =
                    joins
                    |> Seq.map (fun j ->
                        let onExpr =
                            match materializedRootAlias, materializedRootPaths, j.OnSourceAlias with
                            | Some rootAlias, Some paths, Some sourceAlias
                                when sourceAlias = rootAlias && paths.Contains(j.PropertyPath) ->
                                SqlExpr.FunctionCall(
                                    "jsonb_extract",
                                    [ SqlExpr.Column(j.OnSourceAlias, "Value")
                                      SqlExpr.Literal(SqlLiteral.String($"$.{j.OnPropertyName}[0]")) ])
                            | _ ->
                                SqlExpr.JsonExtractExpr(j.OnSourceAlias, "Value", JsonPath(j.OnPropertyName, []))
                        ConditionedJoin(
                            parseJoinKind j.JoinKind,
                            BaseTable(j.TargetTable, Some j.TargetAlias),
                            SqlExpr.Binary(
                                SqlExpr.Column(Some j.TargetAlias, "Id"),
                                BinaryOperator.Eq,
                                onExpr)))
                    |> Seq.toList

                let outerCtx =
                    { sourceCtx with
                        Joins = ResizeArray() }

                let innerAggCtx =
                    { innerCtx with
                        Joins = ResizeArray() }
                let innerKeyCtx =
                    { innerCtx with
                        Joins = ResizeArray() }

                let rec stripConvert (expr: Expression) =
                    match expr with
                    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> stripConvert ue.Operand
                    | _ -> expr

                let tryTranslateDbRefValueIdKey (parameter: ParameterExpression) (tableAlias: string) (expr: Expression) =
                    match stripConvert expr with
                    | :? MemberExpression as idMe when idMe.Member.Name = "Id" ->
                        match stripConvert idMe.Expression with
                        | :? MemberExpression as valueMe when valueMe.Member.Name = "Value" ->
                            match stripConvert valueMe.Expression with
                            | :? MemberExpression as relMe when Object.ReferenceEquals(stripConvert relMe.Expression, parameter) ->
                                Some (
                                    SqlExpr.FunctionCall(
                                        "jsonb_extract",
                                        [ SqlExpr.Column(Some tableAlias, "Value")
                                          SqlExpr.Literal(SqlLiteral.String($"$.{relMe.Member.Name}")) ]))
                            | _ -> None
                        | _ -> None
                    | _ -> None

                let outerKeyExpr =
                    match tryTranslateDbRefValueIdKey outerKeySelector.Parameters.[0] outerAlias outerKeySelector.Body with
                    | Some translated -> translated
                    | None -> translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body

                let innerJoinKeyExpr =
                    let innerKeyDirectCtx =
                        { innerCtx with
                            Joins = ResizeArray() }
                    let directExpr =
                        match tryTranslateDbRefValueIdKey innerKeySelector.Parameters.[0] innerAlias innerKeySelector.Body with
                        | Some translated -> translated
                        | None -> translateJoinSingleSourceExpression innerKeyDirectCtx innerAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body
                    if innerKeyDirectCtx.Joins.Count = 0 then
                        directExpr
                    else
                        let innerKeySourceAlias = sprintf "gjk%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                        let correlatedExpr =
                            match tryTranslateDbRefValueIdKey innerKeySelector.Parameters.[0] innerKeySourceAlias innerKeySelector.Body with
                            | Some translated -> translated
                            | None -> translateJoinSingleSourceExpression innerKeyCtx innerKeySourceAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body
                        let keyCore =
                            { mkCore [{ Alias = None; Expr = correlatedExpr }] (Some (DerivedTable(innerSelect, innerKeySourceAlias)))
                                with
                                    Joins = materializeDiscoveredJoins innerKeyCtx.Joins None None
                                    Where = Some (SqlExpr.Binary(SqlExpr.Column(Some innerKeySourceAlias, "Id"), BinaryOperator.Eq, SqlExpr.Column(Some innerAlias, "Id")))
                                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
                        SqlExpr.ScalarSubquery (wrapCore keyCore)

                let replaceExpression (target: Expression) (replacement: Expression) (expr: Expression) =
                    let visitor =
                        { new ExpressionVisitor() with
                            override _.Visit(node: Expression) =
                                if isNull node then null
                                elif Object.ReferenceEquals(node, target) then replacement
                                else base.Visit(node) }
                    visitor.Visit(expr)

                let translateOuterExpr (expr: Expression) =
                    translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars (Some outerParam) expr

                let translatedArg value =
                    { Value = value
                      Error = None }

                let combineErrorExprs (errors: SqlExpr option list) =
                    errors
                    |> List.choose id
                    |> function
                        | [] -> None
                        | [single] -> Some single
                        | head :: tail -> Some(SqlExpr.Coalesce(head, tail))

                let errorExpr message =
                    SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String($"__solodb_error__:{message}"))])

                let emptyChainDescriptor =
                    { WherePredicates = []
                      OrderKeys = []
                      Skip = None
                      Take = None
                      SelectProjection = None }

                let hasGroupChainOps (desc: GroupJoinGroupChainDescriptor) =
                    not desc.WherePredicates.IsEmpty
                    || not desc.OrderKeys.IsEmpty
                    || desc.Skip.IsSome
                    || desc.Take.IsSome
                    || desc.SelectProjection.IsSome

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

                let rec walkGroupChain (expr: Expression) =
                    match expr with
                    | :? ParameterExpression as p when Object.ReferenceEquals(p, groupParam) ->
                        Some emptyChainDescriptor
                    | :? MethodCallExpression as mc when mc.Arguments.Count >= 1 ->
                        match walkGroupChain mc.Arguments.[0] with
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
                            | _ -> None
                        | None -> None
                    | _ -> None

                let tryGetGroupChainDescriptor (expr: Expression) =
                    match walkGroupChain expr with
                    | Some desc when hasGroupChainOps desc -> Some desc
                    | _ -> None

                let tryMatchGroupElementCall (expr: Expression) =
                    match expr with
                    | :? MethodCallExpression as mc
                        when mc.Arguments.Count >= 1 ->
                        let chainOpt =
                            match mc.Arguments.[0] with
                            | :? ParameterExpression as p when Object.ReferenceEquals(p, groupParam) -> Some emptyChainDescriptor
                            | source -> tryGetGroupChainDescriptor source
                        match chainOpt with
                        | Some chain ->
                            match mc.Method.Name, mc.Arguments.Count with
                            | "First", 1 -> Some { Call = mc; Kind = FirstLike false; Chain = chain }
                            | "FirstOrDefault", 1 -> Some { Call = mc; Kind = FirstLike true; Chain = chain }
                            | "Last", 1 -> Some { Call = mc; Kind = LastLike false; Chain = chain }
                            | "LastOrDefault", 1 -> Some { Call = mc; Kind = LastLike true; Chain = chain }
                            | "Single", 1 -> Some { Call = mc; Kind = SingleLike false; Chain = chain }
                            | "SingleOrDefault", 1 -> Some { Call = mc; Kind = SingleLike true; Chain = chain }
                            | "ElementAt", 2 -> Some { Call = mc; Kind = ElementAtLike(mc.Arguments.[1], false); Chain = chain }
                            | "ElementAtOrDefault", 2 -> Some { Call = mc; Kind = ElementAtLike(mc.Arguments.[1], true); Chain = chain }
                            | _ -> None
                        | None -> None
                    | _ -> None

                let isNullConstant (expr: Expression) =
                    match expr with
                    | :? ConstantExpression as ce -> isNull ce.Value
                    | _ -> false

                let buildCountSubquery (baseCore: SelectCore) (limit: int option) =
                    let countSourceAlias = sprintf "gjc%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                    let countSourceCore =
                        { baseCore with
                            Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                            Limit =
                                match limit with
                                | Some n -> Some (SqlExpr.Literal(SqlLiteral.Integer(int64 n)))
                                | None -> baseCore.Limit }
                    let countSourceSel = { Ctes = []; Body = SingleSelect countSourceCore }
                    let countRowAlias = sprintf "gjn%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
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

                let buildGroupChainRowset (chain: GroupJoinGroupChainDescriptor) =
                    let rowAlias = sprintf "gjr%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                    let numberedAlias = sprintf "gjn%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                    let innerParam = innerKeySelector.Parameters.[0]
                    let chainCtx =
                        { innerCtx with
                            Joins = ResizeArray() }
                    let rowKeyExpr =
                        match tryTranslateDbRefValueIdKey innerParam rowAlias innerKeySelector.Body with
                        | Some translated -> translated
                        | None -> translateJoinSingleSourceExpression chainCtx rowAlias ctx.Vars (Some innerParam) innerKeySelector.Body
                    let correlation = SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, rowKeyExpr)
                    let whereExpr =
                        chain.WherePredicates
                        |> List.map (fun pred -> translateJoinSingleSourceExpression chainCtx rowAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body)
                        |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) correlation
                    let orderBy =
                        if chain.OrderKeys.IsEmpty then
                            [{ Expr = SqlExpr.Column(Some rowAlias, "Id"); Direction = SortDirection.Asc }]
                        else
                            chain.OrderKeys
                            |> List.map (fun (keySel, dir) ->
                                { Expr = translateJoinSingleSourceExpression chainCtx rowAlias ctx.Vars (Some keySel.Parameters.[0]) keySel.Body
                                  Direction = dir })
                    let rowProjectionBase =
                        match chain.SelectProjection with
                        | Some proj ->
                            [{ Alias = Some "v"; Expr = translateJoinSingleSourceExpression chainCtx rowAlias ctx.Vars (Some proj.Parameters.[0]) proj.Body }]
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
                            (Some (DerivedTable(innerSelect, rowAlias)))
                          with
                            Joins = materializeDiscoveredJoins chainCtx.Joins None None
                            Where = Some whereExpr }
                    let numberedSel = { Ctes = []; Body = SingleSelect numberedCore }
                    let limitExpr, offsetExpr = buildLimitOffset chain.Take chain.Skip
                    let boundedProjections =
                        match chain.SelectProjection with
                        | Some _ ->
                            [{ Alias = Some "v"; Expr = SqlExpr.Column(Some numberedAlias, "v") }
                             { Alias = Some "__ord"; Expr = SqlExpr.Column(Some numberedAlias, "__ord") }]
                        | None ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some numberedAlias, "Id") }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some numberedAlias, "Value") }
                             { Alias = Some "__ord"; Expr = SqlExpr.Column(Some numberedAlias, "__ord") }]
                    let boundedCore =
                        { Distinct = false
                          Projections = ProjectionSetOps.ofList boundedProjections
                          Source = Some(DerivedTable(numberedSel, numberedAlias))
                          Joins = []
                          Where = None
                          GroupBy = []
                          Having = None
                          OrderBy = [{ Expr = SqlExpr.Column(Some numberedAlias, "__ord"); Direction = SortDirection.Asc }]
                          Limit = limitExpr
                          Offset = offsetExpr }
                    { Ctes = []; Body = SingleSelect boundedCore }, chain.SelectProjection.IsSome

                let buildGroupChainCollection (chain: GroupJoinGroupChainDescriptor) =
                    let rowsetSel, isProjected = buildGroupChainRowset chain
                    let rowsetAlias = sprintf "gja%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
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

                let buildGroupElementSubquery (groupCall: GroupJoinElementCall) (projectionBody: Expression) =
                    if hasGroupChainOps groupCall.Chain then
                        let rowsetSel, isProjected = buildGroupChainRowset groupCall.Chain
                        let rowsetAlias = sprintf "gje%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                        let rowsetSourceCore =
                            match rowsetSel.Body with
                            | SingleSelect core -> core
                            | _ -> failwith "internal invariant violation: expected single-select rowset"
                        let rowValueExpr, rowValueJoins =
                            if Object.ReferenceEquals(projectionBody, groupCall.Call :> Expression) then
                                if isProjected then SqlExpr.Column(Some rowsetAlias, "v"), []
                                else entityJsonExpr rowsetAlias, []
                            elif isProjected then
                                raise (NotSupportedException(
                                    "Error: GroupJoin chained member access after Select is not supported.\n" +
                                    "Fix: Access the projected member inside Select, or remove the inner Select."))
                            else
                                let projCtx = QueryContext.SingleSource(innerRootTable)
                                let projCtx = { projCtx with Joins = ResizeArray() }
                                let innerParam = innerKeySelector.Parameters.[0]
                                let rewrittenProjection = replaceExpression (groupCall.Call :> Expression) (innerParam :> Expression) projectionBody
                                let projExpr = translateJoinSingleSourceExpression projCtx rowsetAlias ctx.Vars (Some innerParam) rewrittenProjection
                                projExpr, materializeDiscoveredJoins projCtx.Joins None None
                        let rowsetValueCore orderBy limit offset =
                            { Distinct = false
                              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = rowValueExpr }]
                              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
                              Joins = rowValueJoins
                              Where = None
                              GroupBy = []
                              Having = None
                              OrderBy = orderBy
                              Limit = limit
                              Offset = offset }
                        let mkValue core = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                        let noElements = "Sequence contains no elements"
                        let manyElements = "Sequence contains more than one element"
                        let outOfRange = "Index was out of range. Must be non-negative and less than the size of the collection."
                        let ascOrd = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Asc }]
                        let descOrd = [{ Expr = SqlExpr.Column(Some rowsetAlias, "__ord"); Direction = SortDirection.Desc }]
                        match groupCall.Kind with
                        | FirstLike orDefault ->
                            let valueCore = rowsetValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None
                            let error =
                                if orDefault then None
                                else
                                    let countExpr = buildCountSubquery rowsetSourceCore (Some 1)
                                    Some (SqlExpr.CaseExpr(
                                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), errorExpr noElements),
                                        [],
                                        Some(SqlExpr.Literal(SqlLiteral.Null))))
                            { Value = mkValue valueCore; Error = error }
                        | LastLike orDefault ->
                            let valueCore = rowsetValueCore descOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None
                            let error =
                                if orDefault then None
                                else
                                    let countExpr = buildCountSubquery rowsetSourceCore (Some 1)
                                    Some (SqlExpr.CaseExpr(
                                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), errorExpr noElements),
                                        [],
                                        Some(SqlExpr.Literal(SqlLiteral.Null))))
                            { Value = mkValue valueCore; Error = error }
                        | SingleLike orDefault ->
                            let valueCore = rowsetValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) None
                            let countExpr = buildCountSubquery rowsetSourceCore (Some 2)
                            let error =
                                if orDefault then
                                    Some (SqlExpr.CaseExpr(
                                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), errorExpr manyElements),
                                        [],
                                        Some(SqlExpr.Literal(SqlLiteral.Null))))
                                else
                                    Some (SqlExpr.CaseExpr(
                                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), errorExpr noElements),
                                        [ (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), errorExpr manyElements) ],
                                        Some(SqlExpr.Literal(SqlLiteral.Null))))
                            { Value = mkValue valueCore; Error = error }
                        | ElementAtLike(indexExpr, orDefault) ->
                            let idx = Convert.ToInt64(QueryTranslator.evaluateExpr<obj> indexExpr)
                            if idx < 0L then
                                { Value = SqlExpr.Literal(SqlLiteral.Null)
                                  Error = if orDefault then None else Some(errorExpr outOfRange) }
                            else
                                let valueCore =
                                    rowsetValueCore ascOrd (Some (SqlExpr.Literal(SqlLiteral.Integer 1L))) (Some (SqlExpr.Literal(SqlLiteral.Integer idx)))
                                let error =
                                    if orDefault then None
                                    else
                                        let countExpr = buildCountSubquery valueCore None
                                        Some (SqlExpr.CaseExpr(
                                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), errorExpr outOfRange),
                                            [],
                                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                                { Value = mkValue valueCore; Error = error }
                    else
                        let scalarAlias = sprintf "gjf%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                        let freshInnerCtx =
                            { innerCtx with
                                Joins = ResizeArray() }
                        let innerParam = innerKeySelector.Parameters.[0]
                        let rewrittenProjection = replaceExpression (groupCall.Call :> Expression) (innerParam :> Expression) projectionBody
                        let freshInnerKeyExpr =
                            match tryTranslateDbRefValueIdKey innerParam scalarAlias innerKeySelector.Body with
                            | Some translated -> translated
                            | None -> translateJoinSingleSourceExpression freshInnerCtx scalarAlias ctx.Vars (Some innerParam) innerKeySelector.Body
                        let projectedExpr =
                            translateJoinSingleSourceExpression freshInnerCtx scalarAlias ctx.Vars (Some innerParam) rewrittenProjection
                        let baseScalarCore =
                            { mkCore [{ Alias = None; Expr = projectedExpr }] (Some (DerivedTable(innerSelect, scalarAlias)))
                                with
                                    Joins = materializeDiscoveredJoins freshInnerCtx.Joins None None
                                    Where = Some (SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, freshInnerKeyExpr)) }
                        let ascIdOrder = [{ Expr = SqlExpr.Column(Some scalarAlias, "Id"); Direction = SortDirection.Asc }]
                        let descIdOrder = [{ Expr = SqlExpr.Column(Some scalarAlias, "Id"); Direction = SortDirection.Desc }]
                        let mkValue core = SqlExpr.ScalarSubquery (wrapCore core)
                        let noElements = "Sequence contains no elements"
                        let manyElements = "Sequence contains more than one element"
                        let outOfRange = "Index was out of range. Must be non-negative and less than the size of the collection."
                        match groupCall.Kind with
                        | FirstLike _ ->
                            translatedArg (mkValue { baseScalarCore with Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) })
                        | LastLike orDefault ->
                            let valueCore =
                                { baseScalarCore with
                                    OrderBy = descIdOrder
                                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
                            let error =
                                if orDefault then None
                                else
                                    let countExpr = buildCountSubquery valueCore (Some 1)
                                    Some (SqlExpr.CaseExpr(
                                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), errorExpr noElements),
                                        [],
                                        Some(SqlExpr.Literal(SqlLiteral.Null))))
                            { Value = mkValue valueCore; Error = error }
                        | SingleLike orDefault ->
                            let valueCore =
                                { baseScalarCore with
                                    OrderBy = ascIdOrder
                                    Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L)) }
                            let countExpr = buildCountSubquery { baseScalarCore with OrderBy = ascIdOrder } (Some 2)
                            let error =
                                if orDefault then
                                    Some (SqlExpr.CaseExpr(
                                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), errorExpr manyElements),
                                        [],
                                        Some(SqlExpr.Literal(SqlLiteral.Null))))
                                else
                                    Some (SqlExpr.CaseExpr(
                                        (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), errorExpr noElements),
                                        [ (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 2L)), errorExpr manyElements) ],
                                        Some(SqlExpr.Literal(SqlLiteral.Null))))
                            { Value = mkValue valueCore; Error = error }
                        | ElementAtLike(indexExpr, orDefault) ->
                            let idx = Convert.ToInt64(QueryTranslator.evaluateExpr<obj> indexExpr)
                            if idx < 0L then
                                { Value = SqlExpr.Literal(SqlLiteral.Null)
                                  Error = if orDefault then None else Some(errorExpr outOfRange) }
                            else
                                let valueCore =
                                    { baseScalarCore with
                                        OrderBy = ascIdOrder
                                        Limit = Some (SqlExpr.Literal(SqlLiteral.Integer 1L))
                                        Offset = Some (SqlExpr.Literal(SqlLiteral.Integer idx)) }
                                let error =
                                    if orDefault then None
                                    else
                                        let countExpr = buildCountSubquery valueCore None
                                        Some (SqlExpr.CaseExpr(
                                            (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), errorExpr outOfRange),
                                            [],
                                            Some(SqlExpr.Literal(SqlLiteral.Null))))
                                { Value = mkValue valueCore; Error = error }

                let buildAggregateOverChain (chain: GroupJoinGroupChainDescriptor) (aggKind: AggregateKind) (selectorOpt: LambdaExpression option) (coalesceZero: bool) =
                    let rowsetSel, isProjected = buildGroupChainRowset chain
                    let rowsetAlias = sprintf "gjg%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                    let aggCtx = QueryContext.SingleSource(innerRootTable)
                    let aggregateArg =
                        match selectorOpt, chain.SelectProjection, isProjected with
                        | Some _, Some _, _ ->
                            raise (NotSupportedException(
                                "Error: GroupJoin chained aggregate cannot apply a selector after Select.\n" +
                                "Fix: Use the projected chain directly (for example .Select(...).Sum()) or remove the inner Select."))
                        | Some sel, _, _ ->
                            let selCtx = { aggCtx with Joins = ResizeArray() }
                            let selExpr = translateJoinSingleSourceExpression selCtx rowsetAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body
                            let joins = materializeDiscoveredJoins selCtx.Joins None None
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

                let buildCountPredicateOverChain (chain: GroupJoinGroupChainDescriptor) (pred: LambdaExpression) =
                    match chain.SelectProjection with
                    | Some _ ->
                        raise (NotSupportedException(
                            "Error: GroupJoin chained Count(predicate) after Select is not supported.\n" +
                            "Fix: Move the predicate before Select, or count the projected chain without a predicate."))
                    | None ->
                        let rowsetSel, _ = buildGroupChainRowset chain
                        let rowsetAlias = sprintf "gjp%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                        let predCtx = QueryContext.SingleSource(innerRootTable)
                        let predCtx = { predCtx with Joins = ResizeArray() }
                        let predExpr = translateJoinSingleSourceExpression predCtx rowsetAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                        let countCore =
                            { Distinct = false
                              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                              Source = Some(DerivedTable(rowsetSel, rowsetAlias))
                              Joins = materializeDiscoveredJoins predCtx.Joins None None
                              Where = Some predExpr
                              GroupBy = []
                              Having = None
                              OrderBy = []
                              Limit = None
                              Offset = None }
                        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }

                let buildExistsOverChain (chain: GroupJoinGroupChainDescriptor) (predOpt: LambdaExpression option) (negate: bool) =
                    let rowsetSel, isProjected = buildGroupChainRowset chain
                    let rowsetAlias = sprintf "gjx%d" (Interlocked.Increment(innerCtx.AliasCounter) - 1)
                    let predicateExpr, predicateJoins =
                        match predOpt with
                        | None -> None, []
                        | Some pred when isProjected ->
                            raise (NotSupportedException(
                                "Error: GroupJoin chained predicate after Select is not supported.\n" +
                                "Fix: Move the predicate before Select, or remove the inner Select."))
                        | Some pred ->
                            let predCtx = QueryContext.SingleSource(innerRootTable)
                            let predCtx = { predCtx with Joins = ResizeArray() }
                            let predExpr = translateJoinSingleSourceExpression predCtx rowsetAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                            Some predExpr, materializeDiscoveredJoins predCtx.Joins None None
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

                let tryTranslateGroupFirstLikeNullComparison (expr: BinaryExpression) =
                    let tryBuild groupExpr nullExpr nodeType =
                        match tryMatchGroupElementCall groupExpr with
                        | Some groupCall when isNullConstant nullExpr ->
                            let existsExpr =
                                match groupCall.Kind with
                                | FirstLike _
                                | LastLike _
                                | ElementAtLike(_, true) ->
                                    match buildGroupElementSubquery groupCall (Expression.Constant(1L) :> Expression) with
                                    | { Value = SqlExpr.ScalarSubquery select } -> Some (SqlExpr.Exists select)
                                    | _ -> failwith "internal invariant violation: expected scalar subquery"
                                | _ -> None
                            match existsExpr, nodeType with
                            | Some existsExpr, ExpressionType.Equal -> Some (SqlExpr.Unary(UnaryOperator.Not, existsExpr))
                            | Some existsExpr, ExpressionType.NotEqual -> Some existsExpr
                            | Some _, _ -> failwith "internal invariant violation: expected == or !="
                            | None, _ -> None
                        | _ -> None
                    match tryBuild expr.Left expr.Right expr.NodeType with
                    | Some translated -> Some translated
                    | None -> tryBuild expr.Right expr.Left expr.NodeType

                // Translate the result selector body. Outer references → outer table.
                // Group references → SQL aggregates over the inner table.
                let rec translateGroupJoinArg (expr: Expression) : GroupJoinTranslatedArg =
                    match expr with
                    // outer.Prop — direct outer member access
                    | :? MemberExpression as me when not (isNull me.Expression) && me.Expression :? ParameterExpression && (me.Expression :?> ParameterExpression) = outerParam ->
                        translatedArg (translateOuterExpr expr)
                    // group.First().Prop / group.FirstOrDefault().Prop
                    | :? MemberExpression as me when not (isNull me.Expression) ->
                        match tryMatchGroupElementCall me.Expression with
                        | Some groupCall -> buildGroupElementSubquery groupCall expr
                        | None ->
                            translatedArg (translateOuterExpr expr)
                    // group.First() / group.FirstOrDefault()
                    | :? MethodCallExpression as mc ->
                        match tryMatchGroupElementCall mc with
                        | Some groupCall ->
                            buildGroupElementSubquery groupCall expr
                        | None ->
                            match tryGetGroupChainDescriptor expr with
                            | Some chain ->
                                translatedArg (buildGroupChainCollection chain)
                            | None ->
                                let chainSource =
                                    if mc.Arguments.Count >= 1 then tryGetGroupChainDescriptor mc.Arguments.[0] else None
                                match chainSource with
                                | Some chain when mc.Method.Name = "Count" && mc.Arguments.Count = 1 ->
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Count None false)
                                | Some chain when mc.Method.Name = "LongCount" && mc.Arguments.Count = 1 ->
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Count None false)
                                | Some chain when mc.Method.Name = "Count" && mc.Arguments.Count = 2 ->
                                    let pred = unwrapLambdaExpressionOrThrow "GroupJoin chained Count predicate" mc.Arguments.[1]
                                    translatedArg (buildCountPredicateOverChain chain pred)
                                | Some chain when mc.Method.Name = "Any" && mc.Arguments.Count = 1 ->
                                    translatedArg (buildExistsOverChain chain None false)
                                | Some chain when mc.Method.Name = "Any" && mc.Arguments.Count = 2 ->
                                    let pred = unwrapLambdaExpressionOrThrow "GroupJoin chained Any predicate" mc.Arguments.[1]
                                    translatedArg (buildExistsOverChain chain (Some pred) false)
                                | Some chain when mc.Method.Name = "All" && mc.Arguments.Count = 2 ->
                                    let pred = unwrapLambdaExpressionOrThrow "GroupJoin chained All predicate" mc.Arguments.[1]
                                    translatedArg (buildExistsOverChain chain (Some pred) true)
                                | Some chain when mc.Method.Name = "Sum" && mc.Arguments.Count = 1 ->
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Sum None true)
                                | Some chain when mc.Method.Name = "Min" && mc.Arguments.Count = 1 ->
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Min None false)
                                | Some chain when mc.Method.Name = "Max" && mc.Arguments.Count = 1 ->
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Max None false)
                                | Some chain when mc.Method.Name = "Average" && mc.Arguments.Count = 1 ->
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Avg None false)
                                | Some chain when mc.Method.Name = "Sum" && mc.Arguments.Count = 2 ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin chained Sum selector" mc.Arguments.[1]
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Sum (Some sel) true)
                                | Some chain when mc.Method.Name = "Min" && mc.Arguments.Count = 2 ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin chained Min selector" mc.Arguments.[1]
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Min (Some sel) false)
                                | Some chain when mc.Method.Name = "Max" && mc.Arguments.Count = 2 ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin chained Max selector" mc.Arguments.[1]
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Max (Some sel) false)
                                | Some chain when mc.Method.Name = "Average" && mc.Arguments.Count = 2 ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin chained Average selector" mc.Arguments.[1]
                                    translatedArg (buildAggregateOverChain chain AggregateKind.Avg (Some sel) false)
                                | _ when mc.Method.Name = "Count" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None))
                                | _ when mc.Method.Name = "Count" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let pred = unwrapLambdaExpressionOrThrow "GroupJoin Count predicate" mc.Arguments.[1]
                                    let predDu = translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None))
                                | _ when mc.Method.Name = "LongCount" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None))
                                | _ when mc.Method.Name = "Sum" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin Sum selector" mc.Arguments.[1]
                                    let selDu = translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body
                                    translatedArg (SqlExpr.Coalesce(SqlExpr.AggregateCall(AggregateKind.Sum, Some selDu, false, None), [SqlExpr.Literal(SqlLiteral.Integer 0L)]))
                                | _ when mc.Method.Name = "Min" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin Min selector" mc.Arguments.[1]
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Min, Some(translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None))
                                | _ when mc.Method.Name = "Max" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin Max selector" mc.Arguments.[1]
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Max, Some(translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None))
                                | _ when mc.Method.Name = "Average" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let sel = unwrapLambdaExpressionOrThrow "GroupJoin Average selector" mc.Arguments.[1]
                                    translatedArg (SqlExpr.AggregateCall(AggregateKind.Avg, Some(translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some sel.Parameters.[0]) sel.Body), false, None))
                                | _ when mc.Method.Name = "Any" && mc.Arguments.Count = 1 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    translatedArg (SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Count, Some(SqlExpr.Column(Some innerAlias, "Id")), false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L)))
                                | _ when mc.Method.Name = "Any" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let pred = unwrapLambdaExpressionOrThrow "GroupJoin Any predicate" mc.Arguments.[1]
                                    let predDu = translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                                    translatedArg (SqlExpr.Binary(SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((predDu, SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L)))
                                | _ when mc.Method.Name = "All" && mc.Arguments.Count = 2 && mc.Arguments.[0] :? ParameterExpression && (mc.Arguments.[0] :?> ParameterExpression) = groupParam ->
                                    let pred = unwrapLambdaExpressionOrThrow "GroupJoin All predicate" mc.Arguments.[1]
                                    let predDu = translateJoinSingleSourceExpression innerAggCtx innerAlias ctx.Vars (Some pred.Parameters.[0]) pred.Body
                                    translatedArg (SqlExpr.Binary(
                                        SqlExpr.AggregateCall(AggregateKind.Sum, Some(SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.Not, predDu), SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))), false, None),
                                        BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)))
                                | _ ->
                                    translatedArg (translateOuterExpr expr)
                    // Constant / no-group expression
                    | :? ConstantExpression ->
                        translatedArg (translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars None expr)
                    // Conditional (ternary)
                    | :? ConditionalExpression as ce ->
                        let test = translateGroupJoinArg ce.Test
                        let ifTrue = translateGroupJoinArg ce.IfTrue
                        let ifFalse = translateGroupJoinArg ce.IfFalse
                        { Value = SqlExpr.CaseExpr((test.Value, ifTrue.Value), [], Some(ifFalse.Value))
                          Error = combineErrorExprs [test.Error; ifTrue.Error; ifFalse.Error] }
                    // Binary
                    | :? BinaryExpression as be ->
                        match tryTranslateGroupFirstLikeNullComparison be with
                        | Some translated -> translatedArg translated
                        | None ->
                            let op =
                                match be.NodeType with
                                | ExpressionType.Add -> BinaryOperator.Add
                                | ExpressionType.Subtract -> BinaryOperator.Sub
                                | ExpressionType.Multiply -> BinaryOperator.Mul
                                | ExpressionType.Divide -> BinaryOperator.Div
                                | ExpressionType.Modulo -> BinaryOperator.Mod
                                | ExpressionType.Equal -> BinaryOperator.Eq
                                | ExpressionType.NotEqual -> BinaryOperator.Ne
                                | ExpressionType.GreaterThan -> BinaryOperator.Gt
                                | ExpressionType.GreaterThanOrEqual -> BinaryOperator.Ge
                                | ExpressionType.LessThan -> BinaryOperator.Lt
                                | ExpressionType.LessThanOrEqual -> BinaryOperator.Le
                                | ExpressionType.AndAlso -> BinaryOperator.And
                                | ExpressionType.OrElse -> BinaryOperator.Or
                                | _ -> raise (NotSupportedException($"GroupJoin result selector binary operator {be.NodeType} not supported."))
                            let left = translateGroupJoinArg be.Left
                            let right = translateGroupJoinArg be.Right
                            { Value = SqlExpr.Binary(left.Value, op, right.Value)
                              Error = combineErrorExprs [left.Error; right.Error] }
                    // Fallback — try outer translation
                    | _ ->
                        translatedArg (translateOuterExpr expr)

                // Build the result projection from the selector body.
                // Use json_object (not jsonb_object) so downstream jsonb_extract returns typed SQL values.
                let buildJsonObject (pairs: (string * SqlExpr) list) =
                    let args = pairs |> List.collect (fun (name, expr) -> [SqlExpr.Literal(SqlLiteral.String name); expr])
                    SqlExpr.FunctionCall("json_object", args)

                let resultExpr =
                    match resultSelector.Body with
                    | :? NewExpression as newExpr when not (isNull newExpr.Members) ->
                        let memberNames =
                            newExpr.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                        let translatedMembers =
                            [ for i in 0 .. newExpr.Arguments.Count - 1 ->
                                memberNames.[i], translateGroupJoinArg newExpr.Arguments.[i] ]
                        let jsonExpr = buildJsonObject [ for (name, arg) in translatedMembers -> name, arg.Value ]
                        match combineErrorExprs [ for (_, arg) in translatedMembers -> arg.Error ] with
                        | None -> jsonExpr
                        | Some errorExpr ->
                            SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, errorExpr), jsonExpr), [], Some errorExpr)
                    | :? MemberInitExpression as mi ->
                        let translatedMembers =
                            [ for binding in mi.Bindings do
                                match binding with
                                | :? MemberAssignment as ma ->
                                    yield ma.Member.Name, translateGroupJoinArg ma.Expression
                                | _ ->
                                    raise (NotSupportedException("GroupJoin result selector: only member assignments supported.")) ]
                        let jsonExpr = buildJsonObject [ for (name, arg) in translatedMembers -> name, arg.Value ]
                        match combineErrorExprs [ for (_, arg) in translatedMembers -> arg.Error ] with
                        | None -> jsonExpr
                        | Some errorExpr ->
                            SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.IsNull, errorExpr), jsonExpr), [], Some errorExpr)
                    | _ ->
                        raise (NotSupportedException(
                            "Error: GroupJoin result selector must produce an anonymous type or object initializer.\n" +
                            "Reason: Scalar result selectors are not supported for GroupJoin.\n" +
                            "Fix: Use new { ... } in the result selector."))

                // Collect DBRef JOINs discovered during inner aggregate translation.
                // Outer DBRef joins must precede the GroupJoin itself because outerKeyExpr and
                // outer projections may depend on them. Inner aggregate joins come after.
                let outerDiscoveredJoins =
                    materializeDiscoveredJoins outerCtx.Joins (Some ("\"" + outerAlias + "\"")) (Some sourceCtx.MaterializedPaths)
                let discoveredJoins =
                    materializeDiscoveredJoins innerAggCtx.Joins None None

                // LEFT JOIN inner subquery + discovered DBRef JOINs + GROUP BY outer.Id, outer.Value
                let allJoins =
                    outerDiscoveredJoins
                    @ [ConditionedJoin(
                        JoinKind.Left,
                        innerSource,
                        SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerJoinKeyExpr))]
                    @ discoveredJoins
                let core =
                    { mkCore
                        [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                         { Alias = Some "Value"; Expr = resultExpr }]
                        (Some (DerivedTable(ctx.Inner, outerAlias)))
                      with
                          Joins = allJoins
                          GroupBy = [SqlExpr.Column(Some outerAlias, "Id"); SqlExpr.Column(Some outerAlias, "Value")] }
                wrapCore core
            )
        | other ->
            raise (NotSupportedException(sprintf "Invalid number of arguments in GroupJoin: %A" other))
