namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.DBRefManyDescriptor
open DBRefTypeHelpers
open Utils
open SoloDatabase.Attributes

module internal DBRefManyBuilderCore =
    let dbRefManyLinkTable (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationLink(ownerTable, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> raise (NotSupportedException(sprintf "Error: relation metadata missing for '%s.%s'.\nReason: The property '%s' on '%s' does not have relation metadata.\nFix: Ensure the property is a DBRef/DBRefMany and the collection is initialized, or call AsEnumerable() before accessing nested relations." propName ownerTable propName ownerTable))

    let dbRefManyOwnerUsesSource (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationOwnerUsesSource(ownerTable, propName) with
        | Some value -> value
        | None -> raise (NotSupportedException(sprintf "Error: relation metadata missing for '%s.%s'.\nReason: The property '%s' on '%s' does not have relation metadata.\nFix: Ensure the property is a DBRef/DBRefMany and the collection is initialized, or call AsEnumerable() before accessing nested relations." propName ownerTable propName ownerTable))

    let resolveTargetTable (ctx: QueryContext) (ownerCollection: string) (propName: string) (targetType: Type) =
        let defaultTable = formatName targetType.Name
        match ctx.TryResolveRelationTarget(ownerCollection, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> ctx.ResolveCollectionForType(typeIdentityKey targetType, defaultTable)

    let tryGetRelationOrderByForTakeWhile
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (tgtAlias: string)
        (lnkAlias: string)
        : OrderBy list voption =
        let relationOrder =
            match ownerRef.PropertyExpr.Member with
            | :? Reflection.PropertyInfo as prop ->
                match prop.GetCustomAttributes(typeof<SoloRefAttribute>, true) |> Seq.tryHead with
                | Some attrObj -> (attrObj :?> SoloRefAttribute).OrderBy
                | None -> DBRefOrder.Undefined
            | _ -> DBRefOrder.Undefined

        match relationOrder with
        | DBRefOrder.TargetId ->
            ValueSome [{ Expr = SqlExpr.Column(Some tgtAlias, "Id"); Direction = SortDirection.Asc }]
        | _ ->
            ValueNone

    let mutable private aliasCounter = 0L
    let nextAlias prefix =
        let id = System.Threading.Interlocked.Increment(&aliasCounter)
        sprintf "%s%d" prefix id

    let mkSubCore projections source where =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = where; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    let ownerIdExpr (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef) =
        match ownerRef.OwnerIdExpr with
        | Some expr -> expr
        | None -> SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id")

    let normalizeUnionArm
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (columnNames: string list)
        (core: SelectCore)
        : SelectCore =
        if core.OrderBy.Length > 0 || core.Distinct || core.Limit.IsSome || core.Offset.IsSome then
            let normAlias = nextAlias "_dun"
            let normSel = { Ctes = []; Body = SingleSelect core }
            let projections =
                columnNames
                |> List.map (fun name -> { Alias = Some name; Expr = SqlExpr.Column(Some normAlias, name) })
            mkSubCore projections (Some(DerivedTable(normSel, normAlias))) None
        else
            core

    let projectUnionArmColumns
        (mkSubCore: Projection list -> TableSource option -> SqlExpr option -> SelectCore)
        (nextAlias: string -> string)
        (columnNames: string list)
        (core: SelectCore)
        : SelectCore =
        let flatAlias = nextAlias "_dif"
        let flatSel = { Ctes = []; Body = SingleSelect core }
        let flatProjections =
            columnNames
            |> List.map (fun name -> { Alias = Some name; Expr = SqlExpr.Column(Some flatAlias, name) })
        mkSubCore flatProjections (Some(DerivedTable(flatSel, flatAlias))) None

    let private translatePredicates
        (qb: QueryBuilder)
        (targetAlias: string)
        (targetTable: string)
        (joinEdges: ResizeArray<JoinEdge>)
        (predicateExprs: Expression list)
        (invalidLambdaMessage: string)
        : SqlExpr list =
        predicateExprs
        |> List.collect (fun predExpr ->
            match tryExtractLambdaExpression predExpr with
            | ValueSome predLambda ->
                let subQb = qb.ForSubquery(targetAlias, predLambda, subqueryRootTable = targetTable)
                let result = [visitDu predLambda.Body subQb]
                joinEdges.AddRange(subQb.SourceContext.Joins)
                result
            | ValueNone ->
                raise (NotSupportedException(invalidLambdaMessage)))

    let private translateSortKeys
        (qb: QueryBuilder)
        (targetAlias: string)
        (targetTable: string)
        (joinEdges: ResizeArray<JoinEdge>)
        (sortKeys: (Expression * SortDirection) list)
        (invalidKeyMessage: string)
        : OrderBy list =
        sortKeys
        |> List.map (fun (keyExpr, dir) ->
            match tryExtractLambdaExpression keyExpr with
            | ValueSome keyLambda ->
                let subQb = qb.ForSubquery(targetAlias, keyLambda, subqueryRootTable = targetTable)
                let result = { Expr = visitDu keyLambda.Body subQb; Direction = dir }
                joinEdges.AddRange(subQb.SourceContext.Joins)
                result
            | ValueNone ->
                raise (NotSupportedException(invalidKeyMessage)))

    let private buildLimitOffset (visitDu: Expression -> QueryBuilder -> SqlExpr) (qb: QueryBuilder) (limitExpr: Expression option) (offsetExpr: Expression option) =
        let limitDu =
            match limitExpr with
            | Some e ->
                match e with
                | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
                | _ -> Some(visitDu e qb)
            | None ->
                match offsetExpr with
                | Some _ -> Some(SqlExpr.Literal(SqlLiteral.Integer -1L))
                | None -> None
        let offsetDu =
            match offsetExpr with
            | Some e ->
                match e with
                | :? ConstantExpression as ce -> Some(SqlExpr.Literal(SqlLiteral.Integer(Convert.ToInt64(ce.Value))))
                | _ -> Some(visitDu e qb)
            | None -> None
        limitDu, offsetDu

    /// Walk a SelectMany inner lambda body to find the DBRefMany property and chain operators.
    /// Returns (dbRefManyMemberExpr, innerOfTypeName, innerWherePredicates, innerSelectProjection).
    let private parseSelectManyInnerLambda (lambda: LambdaExpression) =
        let rec unwrap (e: Expression) =
            match e with
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked -> unwrap ue.Operand
            | _ -> e

        // Walk the chain to collect operators and find the DBRefMany root.
        let mutable innerOfType: string option = None
        // Cast is rejected (returns None) — no innerCastType needed.
        let innerWheres = ResizeArray<Expression>()
        let mutable innerSelect: LambdaExpression option = None
        let rec walk (e: Expression) : MemberExpression option =
            let e = unwrap e
            match e with
            | :? MemberExpression as me when isDBRefManyType me.Type ->
                Some me
            | :? MethodCallExpression as mc ->
                let src =
                    if not (isNull mc.Object) then mc.Object
                    elif mc.Arguments.Count > 0 then mc.Arguments.[0]
                    else null
                let arg =
                    if not (isNull mc.Object) then
                        if mc.Arguments.Count >= 1 then Some mc.Arguments.[0] else None
                    elif mc.Arguments.Count >= 2 then Some mc.Arguments.[1]
                    else None
                match mc.Method.Name with
                | "Where" ->
                    match arg with Some pred -> innerWheres.Add(pred) | None -> ()
                    walk src
                | "Select" ->
                    match arg with
                    | Some proj ->
                        match tryExtractLambdaExpression proj with
                        | ValueSome l -> innerSelect <- Some l
                        | ValueNone -> ()
                    | None -> ()
                    walk src
                | "OfType" ->
                    let genericArgs = mc.Method.GetGenericArguments()
                    if genericArgs.Length = 1 then
                        match typeToName genericArgs.[0] with
                        | Some tn -> innerOfType <- Some tn
                        | None -> ()
                    walk src
                | "Cast" ->
                    // Reject: Cast has throw-on-mismatch semantics that cannot be replicated in SQL.
                    // Users should use OfType (which filters) instead.
                    None
                | "ToList" | "ToArray" ->
                    walk src
                | _ ->
                    // Fail closed: unrecognized inner chain operator — do not silently strip.
                    None
            | _ -> None

        match walk lambda.Body with
        | Some me -> Some (me, innerOfType, innerWheres |> Seq.toList, innerSelect)
        | None -> None

    /// Build the correlated subquery core from a descriptor.
    /// Returns (tgtAlias, lnkAlias, core, targetTable) with JOIN + WHERE + ORDER BY + LIMIT/OFFSET.
    let buildCorrelatedCore
        (qb: QueryBuilder)
        (desc: QueryDescriptor)
        (ownerRef: DBRefManyDescriptor.DBRefManyOwnerRef)
        (projections: Projection list)
        : string * string * SelectCore * string =

        let propName = ownerRef.PropertyExpr.Member.Name
        let ctx = qb.SourceContext
        let linkTable = dbRefManyLinkTable ctx ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource ctx ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetTable ctx ownerRef.OwnerCollection propName targetType
        if desc.OfTypeName.IsSome || desc.CastTypeName.IsSome then
            DBRefManyHelpers.ensureOfTypeSupported targetType
        let tgtAlias = nextAlias "_tgt"
        let lnkAlias = nextAlias "_lnk"

        // Two-hop SelectMany: owner → link1 → target1 → link2 → target2.
        // The inner lambda tells us the second hop's DBRefMany property + chain ops.
        let hop2Joins, hop2Preds, effectiveTgtAlias, effectiveTargetTable =
            match desc.SelectManyInnerLambda with
            | Some innerLambda ->
                match parseSelectManyInnerLambda innerLambda with
                | Some (innerMemberExpr, innerOfType, innerWheres, _innerSelect) ->
                    let innerPropName = innerMemberExpr.Member.Name
                    // Target1 is the intermediate table (e.g., Books). Use it as the owner for hop2.
                    let hop1TargetTable = targetTable
                    let hop1TargetType = targetType
                    let hop1TargetCollection =
                        ctx.ResolveCollectionForType(typeIdentityKey hop1TargetType, hop1TargetTable.Trim('"'))
                    let link2Table = dbRefManyLinkTable ctx hop1TargetCollection innerPropName
                    let owner2UsesSource = dbRefManyOwnerUsesSource ctx hop1TargetCollection innerPropName
                    let owner2Column = if owner2UsesSource then "SourceId" else "TargetId"
                    let target2Column = if owner2UsesSource then "TargetId" else "SourceId"
                    let innerTargetType = innerMemberExpr.Type.GetGenericArguments().[0]
                    let target2Table = resolveTargetTable ctx hop1TargetCollection innerPropName innerTargetType
                    let tgt2Alias = nextAlias "_tgt2"
                    let lnk2Alias = nextAlias "_lnk2"

                    // link2 joins to target1 (hop1 target)
                    let lnk2JoinOn =
                        SqlExpr.Binary(
                            SqlExpr.Column(Some lnk2Alias, owner2Column),
                            BinaryOperator.Eq,
                            SqlExpr.Column(Some tgtAlias, "Id"))
                    // target2 joins to link2
                    let tgt2JoinOn =
                        SqlExpr.Binary(
                            SqlExpr.Column(Some tgt2Alias, "Id"),
                            BinaryOperator.Eq,
                            SqlExpr.Column(Some lnk2Alias, target2Column))

                    // Inner chain predicates: OfType + Where on target2
                    let innerTypePred =
                        let innerTypeName = innerOfType
                        match innerTypeName with
                        | Some tn ->
                            [SqlExpr.Binary(
                                SqlExpr.FunctionCall("jsonb_extract", [
                                    SqlExpr.Column(Some tgt2Alias, "Value")
                                    SqlExpr.Literal(SqlLiteral.String "$.$type")]),
                                BinaryOperator.Eq,
                                SqlExpr.Literal(SqlLiteral.String tn))]
                        | None -> []

                    let innerJoinEdges2 = ResizeArray<JoinEdge>()
                    let innerWhereDus =
                        translatePredicates
                            qb tgt2Alias target2Table innerJoinEdges2 innerWheres
                            "Error: Cannot translate SelectMany inner predicate."
                    let innerDbRefJoins2 = DBRefManyHelpers.joinEdgesToClauses innerJoinEdges2

                    let hop2JoinClauses =
                        [ConditionedJoin(Inner, BaseTable(link2Table, Some lnk2Alias), lnk2JoinOn)
                         ConditionedJoin(Inner, BaseTable(target2Table, Some tgt2Alias), tgt2JoinOn)]
                        @ innerDbRefJoins2
                    let hop2Predicates = innerWhereDus @ innerTypePred

                    hop2JoinClauses, hop2Predicates, tgt2Alias, target2Table
                | None ->
                    // Fail closed: SelectMany inner lambda present but contains unrecognized chain shape.
                    raise (NotSupportedException(
                        "Error: SelectMany inner collection contains unsupported operators.\n" +
                        "Reason: The inner chain uses operators that cannot be translated in a correlated subquery (e.g., OrderBy, Distinct, Take, Skip inside SelectMany).\n" +
                        "Fix: Simplify the inner collection selector to Where/Select/OfType only, or call AsEnumerable() before SelectMany."))
            | None ->
                [], [], tgtAlias, targetTable

        let innerJoinEdges = ResizeArray<JoinEdge>()
        let wherePredDus =
            translatePredicates
                qb
                effectiveTgtAlias
                effectiveTargetTable
                innerJoinEdges
                desc.WherePredicates
                "Error: Cannot translate relation-backed DBRefMany.Any predicate.\nReason: The predicate is not a translatable lambda expression (e.g., Func<> delegate instead of Expression<Func<>>).\nFix: Pass the predicate as an inline lambda, not a delegate variable."

        let ofTypePred =
            match desc.OfTypeName with
            | Some tn ->
                [SqlExpr.Binary(
                    SqlExpr.FunctionCall("jsonb_extract", [
                        SqlExpr.Column(Some effectiveTgtAlias, "Value")
                        SqlExpr.Literal(SqlLiteral.String "$.$type")]),
                    BinaryOperator.Eq,
                    SqlExpr.Literal(SqlLiteral.String tn))]
            | None -> []

        let allPreds = wherePredDus @ ofTypePred @ hop2Preds
        let sortKeyDus =
            translateSortKeys qb effectiveTgtAlias effectiveTargetTable innerJoinEdges desc.SortKeys "Cannot extract key selector for OrderBy."
        let limitDu, offsetDu = buildLimitOffset visitDu qb desc.Limit desc.Offset

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"),
                BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn),
                BinaryOperator.Eq,
                ownerIdExpr ownerRef)
        let fullWhere = DBRefManyHelpers.appendPredicatesWithAnd ownerWhere allPreds

        let dbRefJoins = DBRefManyHelpers.joinEdgesToClauses innerJoinEdges
        let innerCore =
            { mkSubCore projections (Some(BaseTable(linkTable, Some lnkAlias))) (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] @ hop2Joins @ dbRefJoins
                OrderBy = sortKeyDus
                Limit = limitDu
                Offset = offsetDu }

        let hasPostBound =
            desc.PostBoundWherePredicates.Length > 0 ||
            desc.PostBoundSortKeys.Length > 0 ||
            desc.PostBoundLimit.IsSome ||
            desc.PostBoundOffset.IsSome

        let effectiveAlias, effectiveLnk, effectiveCore, effectiveTarget =
            if hasPostBound then
                let passThrough = { innerCore with Projections = AllColumns }
                let innerSel = { Ctes = []; Body = SingleSelect passThrough }
                let pbAlias = nextAlias "_pb"
                let pbJoinEdges = ResizeArray<JoinEdge>()
                let pbWhereDus =
                    translatePredicates
                        qb
                        pbAlias
                        targetTable
                        pbJoinEdges
                        desc.PostBoundWherePredicates
                        "Error: Cannot translate relation-backed DBRefMany predicate.\nReason: The predicate is not a translatable lambda expression.\nFix: Pass the predicate as an inline lambda, not a delegate variable."
                let pbSortKeyDus =
                    translateSortKeys qb pbAlias targetTable pbJoinEdges desc.PostBoundSortKeys "Cannot extract key selector for PostBound OrderBy."
                let pbLimitDu, pbOffsetDu = buildLimitOffset visitDu qb desc.PostBoundLimit desc.PostBoundOffset
                let pbDbRefJoins = DBRefManyHelpers.joinEdgesToClauses pbJoinEdges
                let outerCore =
                    { mkSubCore projections (Some(DerivedTable(innerSel, pbAlias))) (DBRefManyHelpers.foldPredicatesWithAnd pbWhereDus) with
                        Joins = pbDbRefJoins
                        OrderBy = pbSortKeyDus
                        Limit = pbLimitDu
                        Offset = pbOffsetDu }
                pbAlias, lnkAlias, outerCore, targetTable
            else
                effectiveTgtAlias, lnkAlias, innerCore, effectiveTargetTable

        match desc.DefaultIfEmpty with
        | Some defaultValueExprOpt ->
            let defaultValueDu =
                match defaultValueExprOpt with
                | Some valueExpr ->
                    let v = evaluateExpr<obj> valueExpr
                    let jsonObj = JsonSerializator.JsonValue.Serialize v
                    let jsonText = jsonObj.ToJsonString()
                    SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
                | None ->
                    SqlExpr.Literal(SqlLiteral.Null)
            let existsCore = { effectiveCore with Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] }
            let existsSel = { Ctes = []; Body = SingleSelect existsCore }
            let mainCore =
                projectUnionArmColumns
                    mkSubCore
                    nextAlias
                    [ "Id"; "Value" ]
                    { effectiveCore with Projections = AllColumns }
            let defaultProjs =
                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                 { Alias = Some "Value"; Expr = defaultValueDu }]
            let defaultCore =
                { mkSubCore defaultProjs None (Some(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists existsSel))) with
                    Joins = []; OrderBy = []; Limit = None; Offset = None }
            let unionSel = { Ctes = []; Body = UnionAllSelect(mainCore, [defaultCore]) }
            let dtAlias = nextAlias "_die"
            let wrappedCore = mkSubCore projections (Some(DerivedTable(unionSel, dtAlias))) None
            dtAlias, effectiveLnk, wrappedCore, effectiveTarget
        | None -> effectiveAlias, effectiveLnk, effectiveCore, effectiveTarget

    let private serializeDefaultValueExpr (jsonFn: string) (value: obj) =
        let jsonObj = JsonSerializator.JsonValue.Serialize value
        let jsonText = jsonObj.ToJsonString()
        SqlExpr.FunctionCall(jsonFn, [SqlExpr.Literal(SqlLiteral.String jsonText)])

    let computeDefaultValueDu (defaultValueExprOpt: Expression option) (projectedType: Type option) : SqlExpr =
        match defaultValueExprOpt with
        | Some valueExpr ->
            let v = evaluateExpr<obj> valueExpr
            SqlExpr.FunctionCall("jsonb_extract", [serializeDefaultValueExpr "jsonb" v; SqlExpr.Literal(SqlLiteral.String "$")])
        | None ->
            match projectedType with
            | Some t when t.IsValueType ->
                let defaultValue = Activator.CreateInstance(t)
                SqlExpr.FunctionCall("jsonb_extract", [serializeDefaultValueExpr "jsonb" defaultValue; SqlExpr.Literal(SqlLiteral.String "$")])
            | _ ->
                SqlExpr.Literal(SqlLiteral.Null)

    let computeDefaultValueForJsonArray (defaultValueExprOpt: Expression option) (projectedType: Type option) : SqlExpr =
        match defaultValueExprOpt with
        | Some valueExpr ->
            let v = evaluateExpr<obj> valueExpr
            serializeDefaultValueExpr "json" v
        | None ->
            match projectedType with
            | Some t when t.IsValueType ->
                let defaultValue = Activator.CreateInstance(t)
                serializeDefaultValueExpr "json" defaultValue
            | _ ->
                SqlExpr.Literal(SqlLiteral.Null)
