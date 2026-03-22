namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Collections.Generic
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

module internal DBRefManyBuilderElements =
    let private castMissingTypeMessage =
        "The type of item is not stored in the database, if you want to include it, then add the Polymorphic attribute to the type and reinsert all elements."

    let private castMismatchMessage =
        "Unable to cast object to the specified type, because the types are different."

    let private castJsonErrorExpr (message: string) =
        SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String message)])

    let private castScalarErrorExpr (message: string) =
        SqlExpr.Literal(SqlLiteral.String($"__solodb_error__:{message}"))

    let wrapProjectedCastExpr (castTypeNameOpt: string option) (tgtAlias: string) (projectedExpr: SqlExpr) =
        match castTypeNameOpt with
        | None -> projectedExpr
        | Some typeName ->
            let typeExtract =
                SqlExpr.FunctionCall("jsonb_extract", [
                    SqlExpr.Column(Some tgtAlias, "Value")
                    SqlExpr.Literal(SqlLiteral.String "$.$type")
                ])
            let typeIsNull = SqlExpr.Unary(UnaryOperator.IsNull, typeExtract)
            let typeMismatch = SqlExpr.Binary(typeExtract, BinaryOperator.Ne, SqlExpr.Literal(SqlLiteral.String typeName))
            SqlExpr.CaseExpr(
                (typeIsNull, castScalarErrorExpr castMissingTypeMessage),
                [(typeMismatch, castScalarErrorExpr castMismatchMessage)],
                Some projectedExpr)

    let buildEntityValueExpr (castTypeNameOpt: string option) (tgtAlias: string) =
        let normalEntityExpr =
            SqlExpr.FunctionCall("jsonb_set", [
                SqlExpr.Column(Some tgtAlias, "Value")
                SqlExpr.Literal(SqlLiteral.String "$.Id")
                SqlExpr.Column(Some tgtAlias, "Id")
            ])
        match castTypeNameOpt with
        | None -> normalEntityExpr
        | Some typeName ->
            let typeExtract =
                SqlExpr.FunctionCall("jsonb_extract", [
                    SqlExpr.Column(Some tgtAlias, "Value")
                    SqlExpr.Literal(SqlLiteral.String "$.$type")
                ])
            let typeIsNull = SqlExpr.Unary(UnaryOperator.IsNull, typeExtract)
            let typeMismatch = SqlExpr.Binary(typeExtract, BinaryOperator.Ne, SqlExpr.Literal(SqlLiteral.String typeName))
            SqlExpr.CaseExpr(
                (typeIsNull, castJsonErrorExpr castMissingTypeMessage),
                [(typeMismatch, castJsonErrorExpr castMismatchMessage)],
                Some normalEntityExpr)

    let buildOrderedElementSubquery
        (baseCore: SelectCore)
        (valueExpr: SqlExpr)
        (effectiveOrder: _ list)
        (pickLast: bool)
        : SqlSelect =
        let windowOrder = effectiveOrder |> List.map (fun ob -> (ob.Expr, ob.Direction))
        let innerCore =
            { baseCore with
                Projections =
                    ProjectionSetOps.ofList [
                        { Alias = Some "v"; Expr = valueExpr }
                        { Alias = Some "__ord"; Expr = SqlExpr.WindowCall({
                            Kind = WindowFunctionKind.RowNumber
                            Arguments = []
                            PartitionBy = []
                            OrderBy = windowOrder
                        }) }
                    ] }
        let innerSel = { Ctes = []; Body = SingleSelect innerCore }
        let ordAlias = "_elt" + Guid.NewGuid().ToString("N").Substring(0, 6)
        let outerCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some ordAlias, "v") }]
              Source = Some(DerivedTable(innerSel, ordAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = [{ Expr = SqlExpr.Column(Some ordAlias, "__ord"); Direction = if pickLast then SortDirection.Desc else SortDirection.Asc }]
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }
        { Ctes = []; Body = SingleSelect outerCore }

    let buildSingleLike
        (nextAlias: string -> string)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (predicateOpt: Expression option)
        (orDefault: bool)
        : SqlExpr =
        let appendTerminalPredicate (desc: QueryDescriptor) (predicateOpt: Expression option) =
            match predicateOpt with
            | Some pred -> { desc with WherePredicates = desc.WherePredicates @ [pred] }
            | None -> desc
        let desc =
            appendTerminalPredicate desc predicateOpt
        let desc =
            match desc.Limit, desc.Offset with
            | Some _, _
            | _, Some _ -> desc
            | None, None ->
                { desc with Limit = Some(Expression.Constant(2)) }

        let rowsetSel =
            match desc.SelectProjection with
            | Some projLambda ->
                let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
                let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
                let projectedDu = visitDu projLambda.Body subQb
                let projJoins = joinEdgesToClauses subQb.SourceContext.Joins
                let core =
                    { baseCore with
                        Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = wrapProjectedCastExpr desc.CastTypeName tgtAlias projectedDu }]
                        Joins = baseCore.Joins @ projJoins }
                { Ctes = []; Body = SingleSelect core }
            | None ->
                let tgtAlias, _, baseCore, _ = buildCorrelatedCore qb desc ownerRef []
                let core =
                    { baseCore with
                        Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = buildEntityValueExpr desc.CastTypeName tgtAlias }] }
                { Ctes = []; Body = SingleSelect core }

        let rowAlias = nextAlias "_sg"
        let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
        let firstValueExpr =
            SqlExpr.FunctionCall("MIN", [SqlExpr.Column(Some rowAlias, "v")])
        let noElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains no elements")])
        let manyElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains more than one element")])
        let zeroCase = if orDefault then SqlExpr.Literal(SqlLiteral.Null) else noElementsExpr
        let valueExpr =
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), zeroCase),
                [
                    (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)), firstValueExpr)
                ],
                Some manyElementsExpr)
        let outerCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = valueExpr }]
              Source = Some(DerivedTable(rowsetSel, rowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let buildIndexedLike
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (qb: QueryBuilder)
        (rowsetSel: SqlSelect)
        (indexExpr: Expression)
        (orDefault: bool)
        : SqlExpr =
        let idx =
            match indexExpr with
            | :? ConstantExpression as ce -> SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value)))
            | _ -> visitDu indexExpr qb
        let indexedAlias = nextAlias "_ei"
        let indexedCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some indexedAlias, "v") }]
              Source = Some(DerivedTable(rowsetSel, indexedAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = Some idx }
        let indexedSel = { Ctes = []; Body = SingleSelect indexedCore }
        let rowAlias = nextAlias "_eo"
        let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
        let firstValueExpr = SqlExpr.FunctionCall("MIN", [SqlExpr.Column(Some rowAlias, "v")])
        let noElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Index was out of range. Must be non-negative and less than the size of the collection.")])
        let valueExpr =
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)),
                 if orDefault then SqlExpr.Literal(SqlLiteral.Null) else noElementsExpr),
                [],
                Some firstValueExpr)
        let outerCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = valueExpr }]
              Source = Some(DerivedTable(indexedSel, rowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let private buildFilteredProjectedRowset
        (nextAlias: string -> string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder)
        (rowsetSel: SqlSelect)
        (predicateOpt: Expression option)
        : SqlSelect =
        match predicateOpt with
        | None -> rowsetSel
        | Some predExpr ->
            match tryExtractLambdaExpression predExpr with
            | ValueSome predLambda ->
                let rowAlias = nextAlias "_sp"
                let valueCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = Some "Value"; Expr = SqlExpr.Column(Some rowAlias, "v") }]
                      Source = Some(DerivedTable(rowsetSel, rowAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                let valueSel = { Ctes = []; Body = SingleSelect valueCore }
                let predAlias = nextAlias "_sv"
                let predQb =
                    { qb.ForSubquery(predAlias, predLambda) with
                        JsonExtractSelfValue = false }
                let predDu = visitDu predLambda.Body predQb
                let predJoins = joinEdgesToClauses predQb.SourceContext.Joins
                let filteredCore =
                    { Distinct = false
                      Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some predAlias, "Value") }]
                      Source = Some(DerivedTable(valueSel, predAlias))
                      Joins = predJoins
                      Where = Some predDu
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect filteredCore }
            | ValueNone ->
                raise (NotSupportedException(
                    "Error: Cannot translate relation-backed DBRefMany.Single/SingleOrDefault predicate.\nReason: The predicate is not a translatable lambda expression.\nFix: Pass the predicate as an inline lambda, not a delegate variable."))

    let buildSingleLikeFromRowset
        (nextAlias: string -> string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder)
        (rowsetSel: SqlSelect)
        (predicateOpt: Expression option)
        (orDefault: bool)
        : SqlExpr =
        let rowsetSel = buildFilteredProjectedRowset nextAlias tryExtractLambdaExpression visitDu joinEdgesToClauses qb rowsetSel predicateOpt
        let rowAlias = nextAlias "_sg"
        let countExpr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
        let firstValueExpr = SqlExpr.FunctionCall("MIN", [SqlExpr.Column(Some rowAlias, "v")])
        let noElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains no elements")])
        let manyElementsExpr =
            SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains more than one element")])
        let zeroCase = if orDefault then SqlExpr.Literal(SqlLiteral.Null) else noElementsExpr
        let valueExpr =
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)), zeroCase),
                [(SqlExpr.Binary(countExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)), firstValueExpr)],
                Some manyElementsExpr)
        let outerCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = valueExpr }]
              Source = Some(DerivedTable(rowsetSel, rowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }

    let buildOrderedRowsetElement
        (nextAlias: string -> string)
        (rowsetSel: SqlSelect)
        (pickLast: bool)
        : SqlExpr =
        let rowAlias = nextAlias "_re"
        let elementCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Column(Some rowAlias, "v") }]
              Source = Some(DerivedTable(rowsetSel, rowAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = [{ Expr = SqlExpr.Column(Some rowAlias, "__ord"); Direction = if pickLast then SortDirection.Desc else SortDirection.Asc }]
              Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L))
              Offset = None }
        SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect elementCore }

    let buildEntityElement
        (nextAlias: string -> string)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (predicateOpt: Expression option)
        (pickLast: bool)
        : SqlExpr =
        let desc =
            match predicateOpt with
            | Some pred -> { desc with WherePredicates = desc.WherePredicates @ [pred] }
            | None -> desc
        let tgtAlias, lnkAlias, baseCore, _ =
            buildCorrelatedCore qb desc ownerRef []
        let effectiveOrder =
            match baseCore.OrderBy with
            | _ :: _ -> baseCore.OrderBy
            | [] when pickLast -> [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Desc }]
            | [] -> []
        let elementSel = buildOrderedElementSubquery baseCore (buildEntityValueExpr desc.CastTypeName tgtAlias) effectiveOrder (pickLast && baseCore.OrderBy.Length > 0)
        SqlExpr.ScalarSubquery elementSel

    let buildProjectedElement
        (nextAlias: string -> string)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (predicateOpt: Expression option)
        (pickLast: bool)
        : SqlExpr =
        let desc =
            match predicateOpt with
            | Some pred -> { desc with WherePredicates = desc.WherePredicates @ [pred] }
            | None -> desc
        match desc.SelectProjection with
        | Some projLambda ->
            let tgtAlias, lnkAlias, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
            let projectedDu = visitDu projLambda.Body subQb |> wrapProjectedCastExpr desc.CastTypeName tgtAlias
            let projJoins = joinEdgesToClauses subQb.SourceContext.Joins
            let baseCore = { baseCore with Joins = baseCore.Joins @ projJoins }
            let effectiveOrder =
                match baseCore.OrderBy with
                | _ :: _ -> baseCore.OrderBy
                | [] when pickLast -> [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Desc }]
                | [] -> []
            let elementSel = buildOrderedElementSubquery baseCore projectedDu effectiveOrder (pickLast && baseCore.OrderBy.Length > 0)
            SqlExpr.ScalarSubquery elementSel
        | None ->
            raise (NotSupportedException("Projected element terminal requires Select projection."))

    let buildEntityElementAt
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (indexExpr: Expression)
        (orDefault: bool)
        : SqlExpr =
        let tgtAlias, lnkAlias, baseCore, _ = buildCorrelatedCore qb desc ownerRef []
        let effectiveOrder =
            match baseCore.OrderBy with
            | _ :: _ -> baseCore.OrderBy
            | [] -> [{ Expr = SqlExpr.Column(Some lnkAlias, "rowid"); Direction = SortDirection.Asc }]
        let rowCore =
            { baseCore with
                Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = buildEntityValueExpr desc.CastTypeName tgtAlias }]
                OrderBy = effectiveOrder
                Limit = None
                Offset = baseCore.Offset }
        let rowSel = { Ctes = []; Body = SingleSelect rowCore }
        buildIndexedLike nextAlias visitDu qb rowSel indexExpr orDefault

    let buildProjectedElementAt
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (qb: QueryBuilder)
        (projectedSel: SqlSelect)
        (indexExpr: Expression)
        (orDefault: bool)
        : SqlExpr =
        buildIndexedLike nextAlias visitDu qb projectedSel indexExpr orDefault
