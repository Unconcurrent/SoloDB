namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Collections
open System.Collections.Generic
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

module internal DBRefManyBuilderSetOps =
    let evaluateConstantEnumerable (isFullyConstant: Expression -> bool) (evaluateExpr: Expression -> IEnumerable) (expr: Expression) : obj list =
        if not (isFullyConstant expr) then
            raise (NotSupportedException(
                "Error: DBRefMany By-set operator requires a constant second sequence.\n" +
                "Reason: The second sequence cannot be translated on the correlated SQL route.\n" +
                "Fix: Use a constant array/list, or move the operator after AsEnumerable()."))
        match evaluateExpr expr with
        | null -> []
        | values -> [ for value in values -> value ]

    let compileObjectSelector (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>) (selectorExpr: Expression) : (obj -> obj) =
        match tryExtractLambdaExpression selectorExpr with
        | ValueSome selectorLambda ->
            let argObj = Expression.Parameter(typeof<obj>, "o")
            let inlinedBody =
                inlineLambdaInvocation selectorLambda [| Expression.Convert(argObj, selectorLambda.Parameters.[0].Type) :> Expression |]
            let boxedBody =
                if inlinedBody.Type = typeof<obj> then inlinedBody
                else Expression.Convert(inlinedBody, typeof<obj>) :> Expression
            Expression.Lambda<Func<obj, obj>>(boxedBody, argObj).Compile(true).Invoke
        | ValueNone ->
            raise (NotSupportedException("Cannot extract key selector for DBRefMany By-set operator."))

    let buildNullSafeMembershipPredicate
        (nullSafeEq: SqlExpr -> SqlExpr -> SqlExpr)
        (qb: QueryBuilder)
        (leftExpr: SqlExpr)
        (values: obj list)
        (negate: bool)
        : SqlExpr option =
        let terms =
            values
            |> List.map (fun value ->
                let rightExpr =
                    match value with
                    | null -> SqlExpr.Literal(SqlLiteral.Null)
                    | _ -> qb.AllocateParamExpr(value)
                nullSafeEq leftExpr rightExpr)

        match terms with
        | [] -> None
        | head :: tail ->
            let disjunction = tail |> List.fold (fun acc term -> SqlExpr.Binary(acc, BinaryOperator.Or, term)) head
            Some(if negate then SqlExpr.Unary(UnaryOperator.Not, disjunction) else disjunction)

    let buildEntitySequenceAggregate (nextAlias: string -> string) (rowsetSel: SqlSelect) : SqlExpr =
        let outAlias = nextAlias "_set"
        let outerGA = SqlExpr.FunctionCall("json_group_array", [SqlExpr.FunctionCall("json", [SqlExpr.Column(Some outAlias, "v")])])
        let outerCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = outerGA }]
              Source = Some(DerivedTable(rowsetSel, outAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let buildDistinctByEntitySequence
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (nextAlias: string -> string)
        (buildEntityValueExpr: string -> SqlExpr)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (keyExpr: Expression)
        : SqlExpr =
        match tryExtractLambdaExpression keyExpr with
        | ValueSome keyLambda ->
            let tgtAlias, lnkAlias, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
            let keyDu = visitDu keyLambda.Body subQb
            let keyJoins = joinEdgesToClauses subQb.SourceContext.Joins
            let effectiveOrder =
                if baseCore.OrderBy.Length > 0 then
                    baseCore.OrderBy
                else
                    [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Asc }]
            let rankExpr =
                SqlExpr.WindowCall({
                    Kind = WindowFunctionKind.RowNumber
                    Arguments = []
                    PartitionBy = [keyDu]
                    OrderBy = effectiveOrder |> List.map (fun ob -> (ob.Expr, ob.Direction))
                })
            let innerCore =
                { baseCore with
                    Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = buildEntityValueExpr tgtAlias }
                            { Alias = Some "__rk"; Expr = rankExpr }
                        ]
                    Joins = baseCore.Joins @ keyJoins }
            let innerSel = { Ctes = []; Body = SingleSelect innerCore }
            let rankAlias = nextAlias "_db"
            let filteredCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some rankAlias, "v") }]
                  Source = Some(DerivedTable(innerSel, rankAlias))
                  Joins = []
                  Where = Some(SqlExpr.Binary(SqlExpr.Column(Some rankAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let filteredSel = { Ctes = []; Body = SingleSelect filteredCore }
            buildEntitySequenceAggregate nextAlias filteredSel
        | ValueNone ->
            raise (NotSupportedException("Cannot extract key selector for DistinctBy."))

    let buildByFilterEntitySequence
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (nextAlias: string -> string)
        (nullSafeEq: SqlExpr -> SqlExpr -> SqlExpr)
        (buildEntityValueExpr: string -> SqlExpr)
        (isFullyConstant: Expression -> bool)
        (evaluateExpr: Expression -> IEnumerable)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (keyExpr: Expression)
        (rightKeysExpr: Expression)
        (negate: bool)
        : SqlExpr =
        match tryExtractLambdaExpression keyExpr with
        | ValueSome keyLambda ->
            let rightKeys = evaluateConstantEnumerable isFullyConstant evaluateExpr rightKeysExpr
            match rightKeys with
            | [] when negate ->
                buildDistinctByEntitySequence buildCorrelatedCore tryExtractLambdaExpression visitDu joinEdgesToClauses nextAlias buildEntityValueExpr qb desc ownerRef keyExpr
            | [] ->
                let emptyCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Null) }]
                      Source = None
                      Joins = []
                      Where = Some(SqlExpr.Literal(SqlLiteral.Boolean false))
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                buildEntitySequenceAggregate nextAlias { Ctes = []; Body = SingleSelect emptyCore }
            | _ ->
                let tgtAlias, lnkAlias, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
                let subQb = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
                let keyDu = visitDu keyLambda.Body subQb
                let keyJoins = joinEdgesToClauses subQb.SourceContext.Joins
                let membershipPred =
                    buildNullSafeMembershipPredicate nullSafeEq qb keyDu rightKeys negate
                    |> Option.defaultValue (SqlExpr.Literal(SqlLiteral.Boolean negate))
                let whereExpr =
                    match baseCore.Where with
                    | Some w -> SqlExpr.Binary(w, BinaryOperator.And, membershipPred)
                    | None -> membershipPred
                let effectiveOrder =
                    if baseCore.OrderBy.Length > 0 then
                        baseCore.OrderBy
                    else
                        [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Asc }]
                let rankExpr =
                    SqlExpr.WindowCall({
                        Kind = WindowFunctionKind.RowNumber
                        Arguments = []
                        PartitionBy = [keyDu]
                        OrderBy = effectiveOrder |> List.map (fun ob -> (ob.Expr, ob.Direction))
                    })
                let core =
                    { baseCore with
                        Projections =
                            ProjectionSetOps.ofList [
                                { Alias = Some "v"; Expr = buildEntityValueExpr tgtAlias }
                                { Alias = Some "__rk"; Expr = rankExpr }
                            ]
                        Joins = baseCore.Joins @ keyJoins
                        Where = Some whereExpr }
                let innerSel = { Ctes = []; Body = SingleSelect core }
                let rankAlias = nextAlias "_bf"
                let filteredCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some rankAlias, "v") }]
                      Source = Some(DerivedTable(innerSel, rankAlias))
                      Joins = []
                      Where = Some(SqlExpr.Binary(SqlExpr.Column(Some rankAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                buildEntitySequenceAggregate nextAlias { Ctes = []; Body = SingleSelect filteredCore }
        | ValueNone ->
            raise (NotSupportedException("Cannot extract key selector for DBRefMany By-set operator."))

    let buildUnionByEntitySequence
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (nextAlias: string -> string)
        (buildEntityValueExpr: string -> SqlExpr)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (rightSourceExpr: Expression)
        (keyExpr: Expression)
        : SqlExpr =
        match tryExtractLambdaExpression keyExpr with
        | ValueSome keyLambda ->
            let rightItems = evaluateConstantEnumerable isFullyConstant (evaluateExpr<IEnumerable>) rightSourceExpr
            let projectKey = compileObjectSelector tryExtractLambdaExpression keyExpr

            let tgtAlias, lnkAlias, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let keyQb = qb.ForSubquery(tgtAlias, keyLambda, subqueryRootTable = targetTable)
            let keyDu = visitDu keyLambda.Body keyQb
            let keyJoins = joinEdgesToClauses keyQb.SourceContext.Joins
            let effectiveOrder =
                if baseCore.OrderBy.Length > 0 then baseCore.OrderBy
                else [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Asc }]
            let leftOrdExpr =
                SqlExpr.WindowCall({
                    Kind = WindowFunctionKind.RowNumber
                    Arguments = []
                    PartitionBy = []
                    OrderBy = effectiveOrder |> List.map (fun ob -> (ob.Expr, ob.Direction))
                })
            let leftCoreInner =
                { baseCore with
                    Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = buildEntityValueExpr tgtAlias }
                            { Alias = Some "k"; Expr = keyDu }
                            { Alias = Some "__ord"; Expr = leftOrdExpr }
                        ]
                    Joins = baseCore.Joins @ keyJoins }
            let leftSel = { Ctes = []; Body = SingleSelect leftCoreInner }
            let leftAlias = nextAlias "_ubl"
            let leftCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some leftAlias, "v") }
                        { Alias = Some "k"; Expr = SqlExpr.Column(Some leftAlias, "k") }
                        { Alias = Some "__src"; Expr = SqlExpr.Literal(SqlLiteral.Integer 0L) }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some leftAlias, "__ord") }
                    ]
                  Source = Some(DerivedTable(leftSel, leftAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }

            let rightCores =
                rightItems
                |> List.mapi (fun i item ->
                    let keyValue = projectKey item
                    { Distinct = false
                      Projections =
                        ProjectionSetOps.ofList [
                            { Alias = Some "v"; Expr = qb.AllocateParamExpr(item) }
                            { Alias = Some "k"; Expr = match keyValue with null -> SqlExpr.Literal(SqlLiteral.Null) | _ -> qb.AllocateParamExpr(keyValue) }
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

            let unionAlias = nextAlias "_ubu"
            let rankExpr =
                SqlExpr.WindowCall({
                    Kind = WindowFunctionKind.RowNumber
                    Arguments = []
                    PartitionBy = [SqlExpr.Column(Some unionAlias, "k")]
                    OrderBy =
                        [
                            SqlExpr.Column(Some unionAlias, "__src"), SortDirection.Asc
                            SqlExpr.Column(Some unionAlias, "__ord"), SortDirection.Asc
                        ]
                })
            let rankedCore =
                { Distinct = false
                  Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = SqlExpr.Column(Some unionAlias, "v") }
                        { Alias = Some "__src"; Expr = SqlExpr.Column(Some unionAlias, "__src") }
                        { Alias = Some "__ord"; Expr = SqlExpr.Column(Some unionAlias, "__ord") }
                        { Alias = Some "__rk"; Expr = rankExpr }
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
            let rankAlias = nextAlias "_ubr"
            let filteredCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some rankAlias, "v") }]
                  Source = Some(DerivedTable(rankedSel, rankAlias))
                  Joins = []
                  Where = Some(SqlExpr.Binary(SqlExpr.Column(Some rankAlias, "__rk"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                  GroupBy = []
                  Having = None
                  OrderBy =
                    [
                        { Expr = SqlExpr.Column(Some rankAlias, "__src"); Direction = SortDirection.Asc }
                        { Expr = SqlExpr.Column(Some rankAlias, "__ord"); Direction = SortDirection.Asc }
                    ]
                  Limit = None
                  Offset = None }
            buildEntitySequenceAggregate nextAlias { Ctes = []; Body = SingleSelect filteredCore }
        | ValueNone ->
            raise (NotSupportedException("Cannot extract key selector for DBRefMany UnionBy."))
