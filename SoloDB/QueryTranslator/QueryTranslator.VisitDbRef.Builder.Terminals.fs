namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.DBRefManyDescriptor
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec
open Utils

module internal DBRefManyBuilderTerminals =
    let private wrapAggregateEmptySemantics (aggKind: AggregateKind) (insideJsonObjectProjection: bool) (scalarExpr: SqlExpr) : SqlExpr =
        if aggKind = AggregateKind.Sum then
            SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
        elif insideJsonObjectProjection then
            scalarExpr
        else
            SqlExpr.CaseExpr(
                (SqlExpr.Unary(UnaryOperator.IsNull, scalarExpr),
                 SqlExpr.Literal(SqlLiteral.String "__solodb_error__:Sequence contains no elements")),
                [],
                Some scalarExpr)

    let countDbRefManyDepth (unwrapConvert: Expression -> Expression) (isDBRefManyType: Type -> bool) (expr: Expression) : int =
        let rec visitExpr (e: Expression) : int =
            let e = unwrapConvert e
            match e with
            | null -> 0
            | :? MemberExpression as me ->
                let selfDepth = if isDBRefManyType me.Type then 1 else 0
                selfDepth + visitExpr me.Expression
            | :? MethodCallExpression as mc ->
                let objectDepth = if isNull mc.Object then 0 else visitExpr mc.Object
                let argsDepth = mc.Arguments |> Seq.map visitExpr |> Seq.fold max 0
                max objectDepth argsDepth
            | :? BinaryExpression as be -> max (visitExpr be.Left) (visitExpr be.Right)
            | :? UnaryExpression as ue -> visitExpr ue.Operand
            | :? LambdaExpression as le -> visitExpr le.Body
            | :? NewArrayExpression as nae -> nae.Expressions |> Seq.map visitExpr |> Seq.fold max 0
            | _ -> 0
        visitExpr expr

    let buildExists (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string) (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) : SqlExpr =
        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let _, _, core, _ = buildCorrelatedCore qb desc ownerRef oneProj
        SqlExpr.Exists { Ctes = []; Body = SingleSelect core }

    let buildNotExists
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) (allPredExpr: Expression) : SqlExpr =
        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
        let tgtAlias, _, core, targetTable = buildCorrelatedCore qb desc ownerRef oneProj
        match tryExtractLambdaExpression allPredExpr with
        | ValueSome allPredLambda ->
            let subQb = qb.ForSubquery(tgtAlias, allPredLambda, subqueryRootTable = targetTable)
            let allPredDu = visitDu allPredLambda.Body subQb
            let allPredJoins = joinEdgesToClauses subQb.SourceContext.Joins
            let negatedPred = SqlExpr.Unary(UnaryOperator.Not, allPredDu)
            let extendedWhere =
                match core.Where with
                | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, negatedPred))
                | None -> Some negatedPred
            let coreWithNot = { core with Where = extendedWhere; Joins = core.Joins @ allPredJoins }

            if core.Limit.IsSome || core.Offset.IsSome then
                let boundedCore = { core with Where = core.Where; Projections = AllColumns }
                let boundedSel = { Ctes = []; Body = SingleSelect boundedCore }
                let bndAlias = nextAlias "_bnd"
                let bndQb = qb.ForSubquery(bndAlias, allPredLambda, subqueryRootTable = targetTable)
                let bndPredDu = visitDu allPredLambda.Body bndQb
                let bndJoins = joinEdgesToClauses bndQb.SourceContext.Joins
                let outerWhere = SqlExpr.Unary(UnaryOperator.Not, bndPredDu)
                let outerCore = { mkSubCore oneProj (Some(DerivedTable(boundedSel, bndAlias))) (Some outerWhere) with Joins = bndJoins }
                let outerSel = { Ctes = []; Body = SingleSelect outerCore }
                SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists outerSel)
            else
                SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists { Ctes = []; Body = SingleSelect coreWithNot })
        | ValueNone ->
            raise (NotSupportedException(
                "Error: Cannot translate relation-backed DBRefMany.All predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."))

    let buildCount (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string) (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore) (nextAlias: string -> string) (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) : SqlExpr =
        let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
        let _, _, core, _ = buildCorrelatedCore qb desc ownerRef countProj
        if core.Limit.IsSome || core.Offset.IsSome then
            let innerCore = { core with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
            let innerSel = { Ctes = []; Body = SingleSelect innerCore }
            let dtAlias = nextAlias "_pg"
            let outerCore = mkSubCore countProj (Some(DerivedTable(innerSel, dtAlias))) None
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
        else
            let countCore = { core with Limit = None; Offset = None }
            SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect countCore }

    let buildAggregate
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) (selectorExpr: Expression) (aggKind: AggregateKind) : SqlExpr =
        match tryExtractLambdaExpression selectorExpr with
        | ValueSome selectorLambda ->
            let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, selectorLambda, subqueryRootTable = targetTable)
            let selectorDu = visitDu selectorLambda.Body subQb
            let selectorJoins = joinEdgesToClauses subQb.SourceContext.Joins
            let aggExpr = SqlExpr.AggregateCall(aggKind, Some selectorDu, false, None)

            if baseCore.Limit.IsSome || baseCore.Offset.IsSome then
                let innerCore = { baseCore with Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = selectorDu }]; Joins = baseCore.Joins @ selectorJoins }
                let innerSel = { Ctes = []; Body = SingleSelect innerCore }
                let pgAlias = nextAlias "_pg"
                let outerAgg = SqlExpr.AggregateCall(aggKind, Some(SqlExpr.Column(Some pgAlias, "v")), false, None)
                let outerCore = mkSubCore [{ Alias = None; Expr = outerAgg }] (Some(DerivedTable(innerSel, pgAlias))) None
                let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
                wrapAggregateEmptySemantics aggKind qb.InsideJsonObjectProjection scalarExpr
            else
                let aggProj = [{ Alias = None; Expr = aggExpr }]
                let core = { baseCore with Projections = ProjectionSetOps.ofList aggProj; Joins = baseCore.Joins @ selectorJoins }
                let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
                wrapAggregateEmptySemantics aggKind qb.InsideJsonObjectProjection scalarExpr
        | ValueNone ->
            raise (NotSupportedException("Cannot extract selector for aggregate."))

    let buildProjectedRowset
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) : SqlSelect =
        match desc.SelectProjection with
        | Some projLambda ->
            let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
            let projectedDu = visitDu projLambda.Body subQb
            let projJoins = joinEdgesToClauses subQb.SourceContext.Joins
            let projectedCore =
                { baseCore with
                    Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = DBRefManyBuilderElements.wrapProjectedCastExpr desc.CastTypeName tgtAlias projectedDu }]
                    Distinct = false
                    Joins = baseCore.Joins @ projJoins }
            if desc.Distinct && (baseCore.Limit.IsSome || baseCore.Offset.IsSome) then
                let innerSel = { Ctes = []; Body = SingleSelect projectedCore }
                let dtAlias = nextAlias "_pd"
                let outerCore =
                    { Distinct = true
                      Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some dtAlias, "v") }]
                      Source = Some(DerivedTable(innerSel, dtAlias))
                      Joins = []
                      Where = None
                      GroupBy = []
                      Having = None
                      OrderBy = []
                      Limit = None
                      Offset = None }
                { Ctes = []; Body = SingleSelect outerCore }
            else
                let finalCore = { projectedCore with Distinct = desc.Distinct }
                { Ctes = []; Body = SingleSelect finalCore }
        | None ->
            raise (NotSupportedException("Projected scalar terminal requires Select projection."))

    let buildProjectedAggregate (nextAlias: string -> string) (qb: QueryBuilder) (projectedSel: SqlSelect) (aggKind: AggregateKind) : SqlExpr =
        let projectedAlias = nextAlias "_pa"
        let projectedValue = SqlExpr.Column(Some projectedAlias, "v")
        let aggExpr = SqlExpr.AggregateCall(aggKind, Some projectedValue, false, None)
        let aggCore =
            { Distinct = false
              Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = aggExpr }]
              Source = Some(DerivedTable(projectedSel, projectedAlias))
              Joins = []
              Where = None
              GroupBy = []
              Having = None
              OrderBy = []
              Limit = None
              Offset = None }
        let scalarExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect aggCore }
        wrapAggregateEmptySemantics aggKind qb.InsideJsonObjectProjection scalarExpr

    let buildProjectedPredicateExists
        (nextAlias: string -> string)
        (tryExtractLambdaExpression: Expression -> ValueOption<LambdaExpression>)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder)
        (projectedSel: SqlSelect)
        (predicateExpr: Expression)
        : SqlExpr =
        match tryExtractLambdaExpression predicateExpr with
        | ValueSome predLambda ->
            let projectedAlias = nextAlias "_pp"
            let valueCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = Some "Value"; Expr = SqlExpr.Column(Some projectedAlias, "v") }]
                  Source = Some(DerivedTable(projectedSel, projectedAlias))
                  Joins = []
                  Where = None
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            let valueSel = { Ctes = []; Body = SingleSelect valueCore }
            let predicateAlias = nextAlias "_pv"
            let predQb =
                { qb.ForSubquery(predicateAlias, predLambda) with
                    JsonExtractSelfValue = false }
            let predDu = visitDu predLambda.Body predQb
            let predJoins = joinEdgesToClauses predQb.SourceContext.Joins
            let existsCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                  Source = Some(DerivedTable(valueSel, predicateAlias))
                  Joins = predJoins
                  Where = Some predDu
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            SqlExpr.Exists { Ctes = []; Body = SingleSelect existsCore }
        | ValueNone ->
            raise (NotSupportedException(
                "Error: Cannot translate relation-backed DBRefMany.Any predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."))

    let buildSelect
        (countDbRefManyDepth: Expression -> int)
        (nestedDbRefManyNotSupportedMessage: string)
        (buildCorrelatedCore: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> Projection list -> string * string * SelectCore * string)
        (nextAlias: string -> string)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (joinEdgesToClauses: ResizeArray<JoinEdge> -> JoinShape list)
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        : SqlExpr =
        match desc.SelectProjection with
        | Some projLambda ->
            if countDbRefManyDepth projLambda.Body >= maxRelationDepth then
                raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))
            let tgtAlias, _, baseCore, targetTable = buildCorrelatedCore qb desc ownerRef []
            let subQb = qb.ForSubquery(tgtAlias, projLambda, subqueryRootTable = targetTable)
            let projectedDu = visitDu projLambda.Body subQb |> DBRefManyBuilderElements.wrapProjectedCastExpr desc.CastTypeName tgtAlias
            let projJoins = joinEdgesToClauses subQb.SourceContext.Joins
            let baseCore = { baseCore with Joins = baseCore.Joins @ projJoins }
            let hasTakeSkipOrOrder = baseCore.OrderBy.Length > 0 || baseCore.Limit.IsSome || baseCore.Offset.IsSome
            let hasTakeWhile = desc.TakeWhileInfo.IsSome
            if desc.Distinct && not hasTakeSkipOrOrder && not hasTakeWhile then
                let distinctAgg = SqlExpr.AggregateCall(AggregateKind.JsonGroupArray, Some projectedDu, true, None)
                let core = { baseCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = distinctAgg }]; Limit = None; Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
            elif not hasTakeSkipOrOrder && not hasTakeWhile then
                let groupArray = SqlExpr.FunctionCall("jsonb_group_array", [projectedDu])
                let core = { baseCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = groupArray }]; Limit = None; Offset = None }
                SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect core }
            else
                let innerProjs =
                    match desc.TakeWhileInfo with
                    | Some (twPredLambda, _) ->
                        let twSubQb = qb.ForSubquery(tgtAlias, twPredLambda, subqueryRootTable = targetTable)
                        let twPredDu = visitDu twPredLambda.Body twSubQb
                        let caseExpr = SqlExpr.CaseExpr((SqlExpr.Unary(UnaryOperator.Not, twPredDu), SqlExpr.Literal(SqlLiteral.Integer 1L)), [], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
                        let sortKeyDuPairs = baseCore.OrderBy |> List.map (fun ob -> (ob.Expr, ob.Direction))
                        let windowSpec = { Kind = NamedWindowFunction "SUM"; Arguments = [caseExpr]; PartitionBy = []; OrderBy = sortKeyDuPairs }
                        [{ Alias = Some "v"; Expr = projectedDu }; { Alias = Some "_cf"; Expr = SqlExpr.WindowCall windowSpec }]
                    | None ->
                        [{ Alias = Some "v"; Expr = projectedDu }]
                let innerCore = { baseCore with Projections = ProjectionSetOps.ofList innerProjs }
                let innerSel = { Ctes = []; Body = SingleSelect innerCore }
                let ordAlias = nextAlias "_ord"
                match desc.TakeWhileInfo with
                | Some (_, isTakeWhile) ->
                    let cfFilter =
                        if isTakeWhile then SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                        else SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
                    let middleCore =
                        { Distinct = false
                          Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = SqlExpr.Column(Some ordAlias, "v") }]
                          Source = Some(DerivedTable(innerSel, ordAlias))
                          Joins = []
                          Where = Some cfFilter
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None }
                    let middleSel = { Ctes = []; Body = SingleSelect middleCore }
                    let midAlias = nextAlias "_tw"
                    let outerGA = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.Column(Some midAlias, "v")])
                    let outerCore =
                        { Distinct = false
                          Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = outerGA }]
                          Source = Some(DerivedTable(middleSel, midAlias))
                          Joins = []
                          Where = None
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None }
                    SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
                | None ->
                    let outerGA = SqlExpr.FunctionCall("jsonb_group_array", [SqlExpr.Column(Some ordAlias, "v")])
                    let outerCore =
                        { Distinct = false
                          Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = outerGA }]
                          Source = Some(DerivedTable(innerSel, ordAlias))
                          Joins = []
                          Where = None
                          GroupBy = []
                          Having = None
                          OrderBy = []
                          Limit = None
                          Offset = None }
                    SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect outerCore }
        | None ->
            raise (NotSupportedException("Select projection required."))

    let buildContains
        (dbRefManyLinkTable: QueryContext -> string -> string -> string)
        (dbRefManyOwnerUsesSource: QueryContext -> string -> string -> bool)
        (nullSafeEq: SqlExpr -> SqlExpr -> SqlExpr)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (buildProjectedRowset: QueryBuilder -> QueryDescriptor -> DBRefManyDescriptor.DBRefManyOwnerRef -> SqlSelect)
        (qb: QueryBuilder) (desc: QueryDescriptor) (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) (valueExpr: Expression) : SqlExpr =
        if desc.SelectProjection.IsSome then
            let projectedSel = buildProjectedRowset qb desc ownerRef
            let projectedAlias = "_pc" + Guid.NewGuid().ToString("N").Substring(0, 6)
            let valueDu = visitDu valueExpr qb
            let containsWhere = nullSafeEq (SqlExpr.Column(Some projectedAlias, "v")) valueDu
            let containsCore =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                  Source = Some(DerivedTable(projectedSel, projectedAlias))
                  Joins = []
                  Where = Some containsWhere
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            SqlExpr.Exists { Ctes = []; Body = SingleSelect containsCore }
        else
            let propName = ownerRef.PropertyExpr.Member.Name
            let ctx = qb.SourceContext
            let linkTable = dbRefManyLinkTable ctx ownerRef.OwnerCollection propName
            let ownerUsesSource = dbRefManyOwnerUsesSource ctx ownerRef.OwnerCollection propName
            let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
            let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
            let idProp = valueExpr.Type.GetProperty("Id")
            if isNull idProp then
                raise (NotSupportedException("Contains requires an entity with an Id property."))
            let idAccess = Expression.MakeMemberAccess(valueExpr, idProp)
            let entityIdDu = visitDu idAccess qb
            let ownerWhere = SqlExpr.Binary(SqlExpr.Column(None, ownerColumn), BinaryOperator.Eq, SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
            let targetWhere = SqlExpr.Binary(SqlExpr.Column(None, targetColumn), BinaryOperator.Eq, entityIdDu)
            let fullWhere = SqlExpr.Binary(ownerWhere, BinaryOperator.And, targetWhere)
            let core =
                { Distinct = false
                  Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                  Source = Some(BaseTable(linkTable, None))
                  Joins = []
                  Where = Some fullWhere
                  GroupBy = []
                  Having = None
                  OrderBy = []
                  Limit = None
                  Offset = None }
            SqlExpr.Exists { Ctes = []; Body = SingleSelect core }
