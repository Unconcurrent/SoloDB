namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq.Expressions
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open JsonFunctions
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitPostJoin
open DBRefTypeHelpers

module internal QueryTranslatorVisitDbRef =
    [<Literal>]
    let private nestedDbRefManyNotSupportedMessage =
        "Error: Deeply nested DBRefMany query is not supported.\nReason: Only one level of DBRefMany nesting is allowed in relation predicates.\nFix: Rewrite to at most one nested DBRefMany level, or move deeper traversal after AsEnumerable()."

    [<Literal>]
    let private filteredWhereOnlyAnyCountLongCountMessage =
        "Error: DBRefMany.Where() is not supported with this terminal operator in this cycle.\nReason: Only .Where().Any(), .Where().Count(), and .Where().LongCount() are admitted in L1.\nFix: Use one of the admitted operators, or move the query after AsEnumerable()."

    [<Literal>]
    let private filteredWhereRelationNavigationMessage =
        "Error: DBRefMany.Where() predicate cannot navigate DBRef or DBRefMany relations.\nReason: L1 admits only scalar predicates over the target entity.\nFix: Filter on scalar target properties only, or move the relation navigation after AsEnumerable()."

    [<Literal>]
    let private filteredWhereOuterCaptureMessage =
        "Error: DBRefMany.Where() predicate cannot capture the outer owner entity.\nReason: L1 binds the inner predicate only to the relation target alias.\nFix: Rewrite the predicate to depend only on the relation target, or move it after AsEnumerable()."

    // Unique alias counter for nested EXISTS subqueries (prevents alias collision).
    let mutable private subqueryAliasCounter = 0L

    /// Count the maximum DBRefMany nesting depth in an expression tree.
    /// depth 0 = no nested DBRefMany, depth 1 = one nested level (admitted), depth 2+ = rejected.
    let private countDbRefManyDepth (expr: Expression) : int =
        let rec visitExpr (e: Expression) : int =
            let e = unwrapConvert e
            match e with
            | null -> 0
            | :? MemberExpression as me ->
                // Member access (e.g., c.Children) is just property access — not a nesting level.
                // Only MethodCallExpression (e.g., .Any(), .Count) counts as a nesting level.
                if not (isNull me.Expression) then visitExpr me.Expression else 0
            | :? MethodCallExpression as mc ->
                let sourceIsDbRefMany =
                    if not (isNull mc.Object) then
                        isDBRefManyType (unwrapConvert mc.Object).Type
                    elif mc.Arguments.Count > 0 then
                        isDBRefManyType (unwrapConvert mc.Arguments.[0]).Type
                    else
                        false
                let childMax =
                    let objDepth = if not (isNull mc.Object) then visitExpr mc.Object else 0
                    let argDepth = mc.Arguments |> Seq.map visitExpr |> Seq.fold max 0
                    max objDepth argDepth
                // Additive: each DBRefMany method call adds 1 to the nesting depth
                if sourceIsDbRefMany then 1 + childMax else childMax
            | :? BinaryExpression as be ->
                max (visitExpr be.Left) (visitExpr be.Right)
            | :? UnaryExpression as ue ->
                visitExpr ue.Operand
            | :? ConditionalExpression as ce ->
                max (max (visitExpr ce.Test) (visitExpr ce.IfTrue)) (visitExpr ce.IfFalse)
            | :? InvocationExpression as ie ->
                max (visitExpr ie.Expression) (ie.Arguments |> Seq.map visitExpr |> Seq.fold max 0)
            | :? LambdaExpression as le ->
                visitExpr le.Body
            | :? NewExpression as ne ->
                ne.Arguments |> Seq.map visitExpr |> Seq.fold max 0
            | :? NewArrayExpression as nae ->
                nae.Expressions |> Seq.map visitExpr |> Seq.fold max 0
            | _ -> 0
        visitExpr expr

    let private containsRelationNavigation (expr: Expression) =
        let rec visitExpr (e: Expression) =
            match unwrapConvert e with
            | null -> false
            | :? MemberExpression as me ->
                let inner = unwrapConvert me.Expression
                let directDbRefNav =
                    not (isNull inner)
                    && isDBRefType inner.Type
                let directDbRefManyNav =
                    not (isNull inner)
                    && isDBRefManyType inner.Type
                directDbRefNav
                || directDbRefManyNav
                || visitExpr me.Expression
            | :? MethodCallExpression as mc ->
                let objectNav =
                    not (isNull mc.Object)
                    && (isDBRefType (unwrapConvert mc.Object).Type || isDBRefManyType (unwrapConvert mc.Object).Type)
                let argNav =
                    mc.Arguments
                    |> Seq.exists (fun a ->
                        let ua = unwrapConvert a
                        isDBRefType ua.Type || isDBRefManyType ua.Type || visitExpr a)
                objectNav || argNav || visitExpr mc.Object
            | :? BinaryExpression as be ->
                visitExpr be.Left || visitExpr be.Right
            | :? UnaryExpression as ue ->
                visitExpr ue.Operand
            | :? ConditionalExpression as ce ->
                visitExpr ce.Test || visitExpr ce.IfTrue || visitExpr ce.IfFalse
            | :? InvocationExpression as ie ->
                visitExpr ie.Expression || (ie.Arguments |> Seq.exists visitExpr)
            | :? NewExpression as ne ->
                ne.Arguments |> Seq.exists visitExpr
            | :? NewArrayExpression as nae ->
                nae.Expressions |> Seq.exists visitExpr
            | :? MemberInitExpression as mie ->
                visitExpr mie.NewExpression
                || (mie.Bindings |> Seq.exists (function
                    | :? MemberAssignment as ma -> visitExpr ma.Expression
                    | _ -> false))
            | :? ListInitExpression as lie ->
                visitExpr lie.NewExpression
                || (lie.Initializers |> Seq.exists (fun init -> init.Arguments |> Seq.exists visitExpr))
            | :? LambdaExpression as le ->
                visitExpr le.Body
            | _ -> false
        visitExpr expr

    let private containsOuterCapture (lambda: LambdaExpression) =
        let root =
            if lambda.Parameters.Count = 1 then ValueSome lambda.Parameters.[0]
            else ValueNone

        let rec visitExpr (e: Expression) =
            match unwrapConvert e with
            | null -> false
            | :? ParameterExpression as pe ->
                match root with
                | ValueSome rp -> not (Object.ReferenceEquals(pe, rp))
                | ValueNone -> true
            | :? MemberExpression as me ->
                visitExpr me.Expression
            | :? MethodCallExpression as mc ->
                (not (isNull mc.Object) && visitExpr mc.Object)
                || (mc.Arguments |> Seq.exists visitExpr)
            | :? BinaryExpression as be ->
                visitExpr be.Left || visitExpr be.Right
            | :? UnaryExpression as ue ->
                visitExpr ue.Operand
            | :? ConditionalExpression as ce ->
                visitExpr ce.Test || visitExpr ce.IfTrue || visitExpr ce.IfFalse
            | :? InvocationExpression as ie ->
                visitExpr ie.Expression || (ie.Arguments |> Seq.exists visitExpr)
            | :? NewExpression as ne ->
                ne.Arguments |> Seq.exists visitExpr
            | :? NewArrayExpression as nae ->
                nae.Expressions |> Seq.exists visitExpr
            | :? MemberInitExpression as mie ->
                visitExpr mie.NewExpression
                || (mie.Bindings |> Seq.exists (function
                    | :? MemberAssignment as ma -> visitExpr ma.Expression
                    | _ -> false))
            | :? ListInitExpression as lie ->
                visitExpr lie.NewExpression
                || (lie.Initializers |> Seq.exists (fun init -> init.Arguments |> Seq.exists visitExpr))
            | :? LambdaExpression as innerLambda ->
                // Nested lambdas are out of scope for this cycle.
                not (Object.ReferenceEquals(innerLambda, lambda)) || visitExpr innerLambda.Body
            | _ -> false
        visitExpr lambda.Body

    /// preExpressionHandler for DBRef member access translation.
    let private handleDBRefExpression (qb: QueryBuilder) (exp: Expression) : bool =
        let tryMakeValueMemberFromInvokeArg (arg: Expression) =
            match unwrapConvert arg with
            | :? MemberExpression as dbrefPropExpr when isDBRefType dbrefPropExpr.Type ->
                let valueProp = dbrefPropExpr.Type.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
                if isNull valueProp then ValueNone
                else ValueSome (Expression.MakeMemberAccess(dbrefPropExpr, valueProp))
            | _ ->
                ValueNone

        match exp with
        | :? MemberExpression as topMe when not (isNull topMe.Expression) ->
            let innerExpr = unwrapConvert topMe.Expression

            let inline prefixToAlias (prefix: string) =
                if String.IsNullOrEmpty prefix then None
                else Some(prefix.TrimEnd('.'))

            // Case 1: Direct member on DBRef<T> — o.Ref.Id, o.Ref.HasValue
            if isDBRefType innerExpr.Type then
                match topMe.Member.Name with
                | "Id" ->
                    match innerExpr with
                    | :? MemberExpression as dbrefPropExpr ->
                        let struct(prefix, prop) = resolveDBRefPropertyLocation qb dbrefPropExpr
                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.JsonExtractExpr(prefixToAlias prefix, "Value", JsonPath(prop, [])))
                        true
                    | _ -> false
                | "HasValue" ->
                    match innerExpr with
                    | :? MemberExpression as dbrefPropExpr ->
                        let struct(prefix, prop) = resolveDBRefPropertyLocation qb dbrefPropExpr
                        let extract = SqlExpr.JsonExtractExpr(prefixToAlias prefix, "Value", JsonPath(prop, []))
                        qb.DuHandlerResult.Value <- ValueSome(
                            SqlExpr.Binary(
                                SqlExpr.Unary(UnaryOperator.IsNotNull, extract),
                                BinaryOperator.And,
                                SqlExpr.Binary(extract, BinaryOperator.Ne, SqlExpr.Literal(SqlLiteral.Integer 0L))))
                        true
                    | _ -> false
                | _ -> false

            // Case 2: Property through DBRef<T>.Value — o.Ref.Value.Name, o.Ref.Value.Address.City
            else
                let rec findValueBoundary (expr: Expression) (above: string list) =
                    match expr with
                    | :? MemberExpression as me when isDBRefValueBoundary me ->
                        ValueSome struct(me, above)
                    | :? MemberExpression as me when not (isNull me.Expression) ->
                        findValueBoundary me.Expression (me.Member.Name :: above)
                    | :? MethodCallExpression as mc when mc.Method.Name = "Invoke" && mc.Arguments.Count = 1 ->
                        match tryMakeValueMemberFromInvokeArg mc.Arguments.[0] with
                        | ValueSome valueME -> ValueSome struct(valueME, above)
                        | ValueNone when not (isNull mc.Object) -> findValueBoundary mc.Object above
                        | ValueNone -> ValueNone
                    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert ->
                        findValueBoundary ue.Operand above
                    | _ -> ValueNone

                match findValueBoundary topMe.Expression [topMe.Member.Name] with
                | ValueSome struct(valueME, propParts) ->
                    let alias = ensureDBRefJoin qb valueME
                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.JsonExtractExpr(Some alias, "Value", JsonPathOps.ofList propParts))
                    true
                | ValueNone -> false
        | _ -> false

    /// Compute the link table name for a DBRefMany property.
    /// Convention: SoloDBRelLink_{SourceTable}_{PropertyName}
    // Strict metadata resolution — no silent fallback for relation-backed paths.
    let private dbRefManyLinkTable (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationLink(ownerTable, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ ->
            raise (InvalidOperationException(
                $"Error: relation metadata missing for '{ownerTable}.{propName}'.\nReason: link table cannot be resolved without relation metadata (phase=translation).\nFix: repair/rebuild relation metadata before executing this query."))

    let private dbRefManyOwnerUsesSource (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationOwnerUsesSource(ownerTable, propName) with
        | Some value -> value
        | None ->
            raise (InvalidOperationException(
                $"Error: relation metadata missing for '{ownerTable}.{propName}'.\nReason: owner-source direction cannot be resolved without relation metadata (phase=translation).\nFix: repair/rebuild relation metadata before executing this query."))

    type private DBRefManyOwnerRef = {
        OwnerCollection: string
        OwnerAliasSql: string
        PropertyExpr: MemberExpression
    }

    /// Synthesize a MemberExpression for DBRef<T>.Value from an Invoke arg that is a DBRef property.
    /// F# expression trees emit Invoke(closure, dbrefPropExpr) instead of MemberAccess(Value, dbrefPropExpr).
    let private tryMakeValueMemberFromInvoke (invokeExpr: MethodCallExpression) : MemberExpression voption =
        if invokeExpr.Arguments.Count <> 1 then ValueNone
        else
            match unwrapConvert invokeExpr.Arguments.[0] with
            | :? MemberExpression as dbrefPropExpr when isDBRefType dbrefPropExpr.Type ->
                let valueProp = dbrefPropExpr.Type.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
                if isNull valueProp then ValueNone
                else ValueSome (Expression.MakeMemberAccess(dbrefPropExpr, valueProp))
            | _ -> ValueNone

    /// Extract DBRefMany source with owner resolution for both root and nested (through DBRef.Value) paths.
    /// Handles both C# MemberExpression chains and F# MethodCallExpression(Invoke) wrappers.
    let private tryGetDBRefManyOwnerRef (qb: QueryBuilder) (arg: Expression) : DBRefManyOwnerRef voption =
        let arg = unwrapConvert arg
        match arg with
        | :? MemberExpression as me when not (isNull me.Expression) && isDBRefManyType me.Type ->
            match unwrapConvert me.Expression with
            | :? ParameterExpression as pe ->
                let sourceAlias =
                    if String.IsNullOrEmpty qb.TableNameDot then "\"" + qb.SourceContext.RootTable + "\""
                    else qb.TableNameDot.TrimEnd('.')
                // Detect subquery context: if sourceAlias differs from root alias,
                // resolve owner collection from parameter type (e.g. ChainMid in inner Any).
                let rootAlias = "\"" + qb.SourceContext.RootTable + "\""
                let ownerCollection =
                    if StringComparer.Ordinal.Equals(sourceAlias, rootAlias) then
                        qb.SourceContext.RootTable
                    else
                        qb.SourceContext.ResolveCollectionForType(Utils.typeIdentityKey pe.Type, formatName pe.Type.Name)
                ValueSome {
                    OwnerCollection = ownerCollection
                    OwnerAliasSql = sourceAlias
                    PropertyExpr = me
                }
            | :? MemberExpression as valueMe when isDBRefValueBoundary valueMe ->
                // Nested C#: o.Ref.Value.Items — resolve via DBRef JOIN chain.
                let alias = ensureDBRefJoin qb valueMe
                let parentDbRefExpr = unwrapConvert valueMe.Expression :?> MemberExpression
                let struct(_, _) = resolveDBRefOwnerCollectionAndProperty qb parentDbRefExpr
                let joinedOwnerCollection =
                    match qb.SourceContext.TryFindJoinByAlias(alias) with
                    | Some join -> join.TargetTable
                    | None -> qb.SourceContext.RootTable
                ValueSome { OwnerCollection = joinedOwnerCollection; OwnerAliasSql = alias; PropertyExpr = me }
            // F# expression tree: Items member on Invoke(closure, Ref) — synthesize .Value access.
            | :? MethodCallExpression as mc when mc.Method.Name = "Invoke" ->
                match tryMakeValueMemberFromInvoke mc with
                | ValueSome valueMe ->
                    let alias = ensureDBRefJoin qb valueMe
                    let parentDbRefExpr = unwrapConvert valueMe.Expression :?> MemberExpression
                    let struct(_, _) = resolveDBRefOwnerCollectionAndProperty qb parentDbRefExpr
                    let joinedOwnerCollection =
                        match qb.SourceContext.TryFindJoinByAlias(alias) with
                        | Some join -> join.TargetTable
                        | None -> qb.SourceContext.RootTable
                    ValueSome { OwnerCollection = joinedOwnerCollection; OwnerAliasSql = alias; PropertyExpr = me }
                | ValueNone -> ValueNone
            | _ -> ValueNone
        | _ -> ValueNone

    /// Extract source expression and optional predicate from Any/All MethodCallExpression,
    /// handling both instance-call (mce.Object.Any(pred)) and extension-call (Enumerable.Any(source, pred)) forms.
    let private extractSourceAndPredicate (mce: MethodCallExpression) =
        if not (isNull mce.Object) then
            let pred = if mce.Arguments.Count >= 1 then ValueSome mce.Arguments.[0] else ValueNone
            ValueSome mce.Object, pred
        elif mce.Arguments.Count >= 1 then
            let pred = if mce.Arguments.Count >= 2 then ValueSome mce.Arguments.[1] else ValueNone
            ValueSome mce.Arguments.[0], pred
        else
            ValueNone, ValueNone

    /// Helper to build a simple SelectCore for link-table subqueries.
    let private mkSubCore projections source where =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = where; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    /// Helper: build a simple link-table subquery (no join to target).
    /// Pattern: SELECT <proj> FROM "linkTable" WHERE ownerColumn = ownerAlias.Id
    let private linkSubquery (linkTable: string) (ownerColumn: string) (ownerAlias: string) (proj: Projection list) =
        let where =
            SqlExpr.Binary(
                SqlExpr.Column(None, ownerColumn),
                BinaryOperator.Eq,
                SqlExpr.Column(Some ownerAlias, "Id"))
        { Ctes = []; Body = SingleSelect(mkSubCore proj (Some(BaseTable(linkTable, None))) (Some where)) }

    /// Build a correlated EXISTS or NOT EXISTS subquery for a DBRefMany quantifier with a predicate.
    /// When isAll=false: EXISTS(SELECT 1 FROM link INNER JOIN target ... WHERE owner AND predicate)
    /// When isAll=true:  NOT EXISTS(SELECT 1 FROM link INNER JOIN target ... WHERE owner AND NOT(predicate))
    let private buildQuantifierWithPredicate (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (predicateExpr: Expression) (isAll: bool) =
        let methodName = if isAll then "All" else "Any"
        match tryExtractLambdaExpression predicateExpr with
        | ValueSome predExpr ->
            if countDbRefManyDepth predExpr.Body > 1 then
                raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))

            let propName = ownerRef.PropertyExpr.Member.Name
            let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
            let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
            let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
            let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
            let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
            let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
            let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
            let tgtAlias = sprintf "_tgt%d" aliasId
            let lnkAlias = sprintf "_lnk%d" aliasId

            // Visit the predicate body to get a DU expression.
            let subQb = qb.ForSubquery(tgtAlias, predExpr)
            let predicateDu = visitDu predExpr.Body subQb

            // Build: SELECT 1 FROM "linkTable" AS _lnk INNER JOIN "targetTable" AS _tgt
            //   ON _tgt.Id = _lnk.targetColumn
            //   WHERE _lnk.ownerColumn = ownerAlias.Id AND [NOT] predicate
            let joinOn =
                SqlExpr.Binary(
                    SqlExpr.Column(Some tgtAlias, "Id"),
                    BinaryOperator.Eq,
                    SqlExpr.Column(Some lnkAlias, targetColumn))
            let ownerWhere =
                SqlExpr.Binary(
                    SqlExpr.Column(Some lnkAlias, ownerColumn),
                    BinaryOperator.Eq,
                    SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
            let fullPredicate =
                if isAll then SqlExpr.Unary(UnaryOperator.Not, predicateDu)
                else predicateDu
            let fullWhere = SqlExpr.Binary(ownerWhere, BinaryOperator.And, fullPredicate)
            let core =
                { mkSubCore
                    [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                    (Some(BaseTable(linkTable, Some lnkAlias)))
                    (Some fullWhere) with
                    Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] }
            let subSelect = { Ctes = []; Body = SingleSelect core }
            let existsExpr = SqlExpr.Exists subSelect
            if isAll then SqlExpr.Unary(UnaryOperator.Not, existsExpr)
            else existsExpr
        | ValueNone ->
            raise (NotSupportedException(
                sprintf "Error: Cannot extract predicate lambda for relation-backed DBRefMany.%s.\nReason: The predicate is not a simple lambda expression.\nFix: Use a simple lambda (e.g., x => ...) or move the operation after AsEnumerable()." methodName))

    let private buildFilteredPredicateDus (qb: QueryBuilder) (tgtAlias: string) (predicateExprs: Expression list) =
        predicateExprs
        |> List.map (fun predExpr ->
            match tryExtractLambdaExpression predExpr with
            | ValueSome predLambda ->
                if countDbRefManyDepth predLambda.Body > 0 then
                    raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))
                if containsRelationNavigation predLambda.Body then
                    raise (NotSupportedException(filteredWhereRelationNavigationMessage))
                if containsOuterCapture predLambda then
                    raise (NotSupportedException(filteredWhereOuterCaptureMessage))
                let subQb = qb.ForSubquery(tgtAlias, predLambda)
                visitDu predLambda.Body subQb
            | ValueNone ->
                raise (NotSupportedException(
                    "Error: Cannot extract predicate lambda for DBRefMany.Where.\nReason: The predicate is not a simple lambda expression.\nFix: Use a simple lambda or move the operation after AsEnumerable().")))

    /// L1: Peel .Where(pred) calls from a DBRefMany source expression.
    /// Returns (innerDBRefManyExpr, predicateExprs) where predicateExprs is a list of
    /// Where predicates in application order (chained .Where().Where() produces multiple).
    /// Returns ValueNone if the expression is not a .Where() on a DBRefMany source.
    let rec private tryPeelWhereFromDBRefMany (expr: Expression) : (Expression * Expression list) voption =
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "Where" ->
            let sourceExpr, predExpr =
                if not (isNull mce.Object) then
                    mce.Object, (if mce.Arguments.Count >= 1 then ValueSome mce.Arguments.[0] else ValueNone)
                elif mce.Arguments.Count >= 2 then
                    mce.Arguments.[0], ValueSome mce.Arguments.[1]
                else
                    null, ValueNone
            if isNull sourceExpr then ValueNone
            else
                let unwrappedSource = unwrapConvert sourceExpr
                if isDBRefManyType unwrappedSource.Type then
                    // Direct DBRefMany.Where(pred) — single predicate.
                    match predExpr with
                    | ValueSome pred -> ValueSome (sourceExpr, [pred])
                    | ValueNone -> ValueNone
                else
                    // Chained: source.Where(pred) where source is itself a .Where() call.
                    match tryPeelWhereFromDBRefMany sourceExpr with
                    | ValueSome (innerSource, preds) ->
                        match predExpr with
                        | ValueSome pred -> ValueSome (innerSource, preds @ [pred])
                        | ValueNone -> ValueSome (innerSource, preds)
                    | ValueNone -> ValueNone
        | _ -> ValueNone

    /// L1: Build a filtered COUNT subquery for DBRefMany.Where(pred).Count().
    /// Emits: ScalarSubquery(SELECT COUNT(*) FROM link JOIN target WHERE ownerLink AND pred1 AND pred2 ...)
    let private buildFilteredCountSubquery (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (predicateExprs: Expression list) : SqlExpr =
        let propName = ownerRef.PropertyExpr.Member.Name
        let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
        let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
        let tgtAlias = sprintf "_tgt%d" aliasId
        let lnkAlias = sprintf "_lnk%d" aliasId

        let predicateDus = buildFilteredPredicateDus qb tgtAlias predicateExprs

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"),
                BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn),
                BinaryOperator.Eq,
                SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
        let fullWhere =
            predicateDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere
        let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
        let core =
            { mkSubCore countProj (Some(BaseTable(linkTable, Some lnkAlias))) (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] }
        let subSelect = { Ctes = []; Body = SingleSelect core }
        SqlExpr.ScalarSubquery subSelect

    let private buildFilteredExistsSubquery (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (predicateExprs: Expression list) : SqlExpr =
        let propName = ownerRef.PropertyExpr.Member.Name
        let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
        let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
        let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
        let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
        let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
        let tgtAlias = sprintf "_tgt%d" aliasId
        let lnkAlias = sprintf "_lnk%d" aliasId
        let predicateDus = buildFilteredPredicateDus qb tgtAlias predicateExprs

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"),
                BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn),
                BinaryOperator.Eq,
                SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
        let fullWhere =
            predicateDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere
        let core =
            { mkSubCore
                [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                (Some(BaseTable(linkTable, Some lnkAlias)))
                (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] }
        let subSelect = { Ctes = []; Body = SingleSelect core }
        SqlExpr.Exists subSelect

    /// preExpressionHandler for DBRefMany.Count, Any(), Any(pred) as correlated subqueries.
    let private handleDBRefManyExpression (qb: QueryBuilder) (exp: Expression) : bool =
        match exp with
        // Case 1: x.Tags.Count or x.Ref.Value.Tags.Count → correlated COUNT subquery
        | :? MemberExpression as topMe when topMe.Member.Name = "Count" && not (isNull topMe.Expression) ->
            let inner = unwrapConvert topMe.Expression
            if isDBRefManyType inner.Type then
                match tryGetDBRefManyOwnerRef qb inner with
                | ValueSome ownerRef ->
                    let propName = ownerRef.PropertyExpr.Member.Name
                    let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
                    let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
                    let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
                    let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery(linkSubquery linkTable ownerColumn ownerRef.OwnerAliasSql countProj))
                    true
                | ValueNone -> false
            else false

        // L1: .Where(pred).Count() / .LongCount() method forms only.
        | :? MethodCallExpression as mce when mce.Method.Name = "Count" || mce.Method.Name = "LongCount" ->
            let sourceArg, predArg = extractSourceAndPredicate mce
            match sourceArg with
            | ValueSome sourceExpr ->
                match tryPeelWhereFromDBRefMany sourceExpr with
                | ValueSome (dbRefManySource, predicateExprs) ->
                    match predArg with
                    | ValueSome _ ->
                        raise (NotSupportedException(filteredWhereOnlyAnyCountLongCountMessage))
                    | ValueNone ->
                        match tryGetDBRefManyOwnerRef qb dbRefManySource with
                        | ValueSome ownerRef ->
                            qb.DuHandlerResult.Value <- ValueSome(buildFilteredCountSubquery qb ownerRef predicateExprs)
                            true
                        | ValueNone -> false
                | ValueNone -> false
            | ValueNone -> false

        // Case 2/3: Any() / Any(pred) over DBRefMany in either extension-call or instance-call form.
        // L1: also handles .Where(pred).Any() → normalized to .Any(pred) with AND composition.
        | :? MethodCallExpression as mce when mce.Method.Name = "Any" ->
            let sourceArg, predArg = extractSourceAndPredicate mce

            match sourceArg with
            | ValueSome sourceExpr ->
                match tryGetDBRefManyOwnerRef qb sourceExpr with
                | ValueSome ownerRef ->
                    match predArg with
                    | ValueNone ->
                        // Any() without predicate.
                        let propName = ownerRef.PropertyExpr.Member.Name
                        let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
                        let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
                        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
                        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists(linkSubquery linkTable ownerColumn ownerRef.OwnerAliasSql oneProj))
                        true
                    | ValueSome predicateExpr ->
                        qb.DuHandlerResult.Value <- ValueSome(buildQuantifierWithPredicate qb ownerRef predicateExpr false)
                        true
                | ValueNone ->
                    // L1: .Where(pred).Any() or .Where(pred).Any(pred2) — peel Where, compose predicates.
                    match tryPeelWhereFromDBRefMany sourceExpr with
                    | ValueSome (dbRefManySource, wherePredicates) ->
                        match tryGetDBRefManyOwnerRef qb dbRefManySource with
                        | ValueSome ownerRef ->
                            match predArg with
                            | ValueSome _ ->
                                raise (NotSupportedException(filteredWhereOnlyAnyCountLongCountMessage))
                            | ValueNone ->
                                qb.DuHandlerResult.Value <- ValueSome(buildFilteredExistsSubquery qb ownerRef wherePredicates)
                                true
                        | ValueNone -> false
                    | ValueNone -> false
            | ValueNone -> false

        // Case 4: All(pred) over DBRefMany — relation-aware NOT EXISTS with negated predicate.
        | :? MethodCallExpression as mce when mce.Method.Name = "All" ->
            let sourceArg, predArg = extractSourceAndPredicate mce

            match sourceArg with
            | ValueSome sourceExpr ->
                match tryGetDBRefManyOwnerRef qb sourceExpr with
                | ValueSome ownerRef ->
                    match predArg with
                    | ValueSome predicateExpr ->
                        qb.DuHandlerResult.Value <- ValueSome(buildQuantifierWithPredicate qb ownerRef predicateExpr true)
                        true
                    | ValueNone ->
                        // All() without predicate is trivially true; emit literal 1.
                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Literal(SqlLiteral.Integer 1L))
                        true
                | ValueNone ->
                    match tryPeelWhereFromDBRefMany sourceExpr with
                    | ValueSome _ ->
                        raise (NotSupportedException(filteredWhereOnlyAnyCountLongCountMessage))
                    | ValueNone -> false
            | ValueNone -> false

        // Case 5: Select(proj) over DBRefMany — correlated subquery with jsonb_group_array (CP-01).
        | :? MethodCallExpression as mce when mce.Method.Name = "Select" ->
            let sourceExpr, projExpr =
                if not (isNull mce.Object) then
                    mce.Object, (if mce.Arguments.Count >= 1 then ValueSome mce.Arguments.[0] else ValueNone)
                elif mce.Arguments.Count >= 2 then
                    mce.Arguments.[0], ValueSome mce.Arguments.[1]
                else
                    null, ValueNone

            if isNull sourceExpr then false
            else
                let unwrappedSource = unwrapConvert sourceExpr
                if isDBRefManyType unwrappedSource.Type then
                    // Direct DBRefMany source — CP-01 admitted
                    match tryGetDBRefManyOwnerRef qb sourceExpr with
                    | ValueSome ownerRef ->
                        match projExpr with
                        | ValueSome projectionExprRaw ->
                            match tryExtractLambdaExpression projectionExprRaw with
                            | ValueSome projLambda ->
                                if countDbRefManyDepth projLambda.Body > 0 then
                                    raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))

                                let propName = ownerRef.PropertyExpr.Member.Name
                                let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
                                let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
                                let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
                                let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
                                let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
                                let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
                                let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
                                let tgtAlias = sprintf "_tgt%d" aliasId
                                let lnkAlias = sprintf "_lnk%d" aliasId

                                // Translate projection body in subquery context
                                let subQb = qb.ForSubquery(tgtAlias, projLambda)
                                let projectedDu = visitDu projLambda.Body subQb

                                // Build: ScalarSubquery(SELECT jsonb_group_array(projectedExpr)
                                //   FROM link AS _lnk INNER JOIN target AS _tgt ON _tgt.Id = _lnk.targetCol
                                //   WHERE _lnk.ownerCol = outerAlias.Id)
                                let joinOn =
                                    SqlExpr.Binary(
                                        SqlExpr.Column(Some tgtAlias, "Id"),
                                        BinaryOperator.Eq,
                                        SqlExpr.Column(Some lnkAlias, targetColumn))
                                let ownerWhere =
                                    SqlExpr.Binary(
                                        SqlExpr.Column(Some lnkAlias, ownerColumn),
                                        BinaryOperator.Eq,
                                        SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
                                let groupArrayExpr = SqlExpr.FunctionCall("json_group_array", [projectedDu])
                                let core =
                                    { mkSubCore
                                        [{ Alias = None; Expr = groupArrayExpr }]
                                        (Some(BaseTable(linkTable, Some lnkAlias)))
                                        (Some ownerWhere) with
                                        Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] }
                                let subSelect = { Ctes = []; Body = SingleSelect core }
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery subSelect)
                                true
                            | ValueNone ->
                                raise (NotSupportedException(
                                    "Error: Cannot extract projection lambda for relation-backed DBRefMany.Select.\nReason: The projection is not a simple lambda expression.\nFix: Use a simple lambda (e.g., x => x.Name) or move the operation after AsEnumerable()."))
                        | ValueNone -> false
                    | ValueNone -> false
                // L1 reject: .Where(pred).Select(proj) on DBRefMany — deferred to L2+.
                elif unwrappedSource :? MethodCallExpression then
                    let innerMce = unwrappedSource :?> MethodCallExpression
                    let innerSource =
                        if not (isNull innerMce.Object) then innerMce.Object
                        elif innerMce.Arguments.Count > 0 then innerMce.Arguments.[0]
                        else null
                    if not (isNull innerSource) then
                        let deepSource = unwrapConvert innerSource
                        if isDBRefManyType deepSource.Type then
                            raise (NotSupportedException(
                                "Error: DBRefMany.Where().Select() is not supported in this cycle.\n" +
                                "Reason: Only .Where().Any(), .Where().Count(), and .Where().LongCount() are admitted.\n" +
                                "Fix: Use .Where().Any() or .Where().Count() for filtering, or move the query after AsEnumerable()."))
                        else
                            // Check if the source is a chained Where on DBRefMany.
                            match tryPeelWhereFromDBRefMany unwrappedSource with
                            | ValueSome _ ->
                                raise (NotSupportedException(
                                    "Error: DBRefMany.Where().Select() is not supported in this cycle.\n" +
                                    "Reason: Only .Where().Any(), .Where().Count(), and .Where().LongCount() are admitted.\n" +
                                    "Fix: Use .Where().Any() or .Where().Count() for filtering, or move the query after AsEnumerable()."))
                            | ValueNone -> false
                    else false
                else false

        // L1 reject: bare DBRefMany.Where(pred) materialization.
        | :? MethodCallExpression as mce when mce.Method.Name = "Where" ->
            match tryPeelWhereFromDBRefMany exp with
            | ValueSome _ ->
                raise (NotSupportedException(filteredWhereOnlyAnyCountLongCountMessage))
            | ValueNone -> false

        // L1 reject: ordering/paging/set operators after DBRefMany.Where(pred).
        | :? MethodCallExpression as mce
            when mce.Method.Name = "OrderBy"
              || mce.Method.Name = "OrderByDescending"
              || mce.Method.Name = "ThenBy"
              || mce.Method.Name = "ThenByDescending"
              || mce.Method.Name = "Take"
              || mce.Method.Name = "Skip"
              || mce.Method.Name = "SelectMany" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            if isNull sourceExpr then false
            else
                match tryPeelWhereFromDBRefMany sourceExpr with
                | ValueSome _ ->
                    raise (NotSupportedException(filteredWhereOnlyAnyCountLongCountMessage))
                | ValueNone -> false

        | _ -> false

    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefExpression))
    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefManyExpression))

    /// Module initialization sentinel — accessing this value forces execution of module do-bindings.
    let internal handlerCount = preExpressionHandler.Count
