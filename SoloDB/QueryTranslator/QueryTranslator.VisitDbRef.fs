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
    let private filteredWhereUnsupportedTerminalMessage =
        "Error: DBRefMany operator chain is not supported with this terminal operator.\nReason: Only .Any(), .Count(), .LongCount(), .Select(), .All(), and ordering operators (.OrderBy/.ThenBy) are admitted as composable prefixes.\nFix: Use one of the admitted operators, or move the query after AsEnumerable()."

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
                // L10: Also accept OfType(DBRefMany) as a valid source for Where peeling.
                let isValidSource =
                    isDBRefManyType unwrappedSource.Type ||
                    (match unwrappedSource with
                     | :? MethodCallExpression as mc when mc.Method.Name = "OfType" ->
                         let innerSrc = if not (isNull mc.Object) then mc.Object elif mc.Arguments.Count > 0 then mc.Arguments.[0] else null
                         not (isNull innerSrc) && isDBRefManyType (unwrapConvert innerSrc).Type
                     | _ -> false)
                if isValidSource then
                    // Direct DBRefMany.Where(pred) or OfType(DBRefMany).Where(pred) — single predicate.
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

    /// L3: Peel .OrderBy/.OrderByDescending/.ThenBy/.ThenByDescending calls from a source expression.
    /// Returns (innerExpr, sortKeys) where sortKeys is a list of (keySelector, SortDirection).
    /// ThenBy/ThenByDescending APPEND to existing keys; OrderBy/OrderByDescending REPLACE all previous.
    /// Returns ValueNone if the expression has no ordering calls.
    let rec private tryPeelOrderByFromSource (expr: Expression) : (Expression * (Expression * SortDirection) list) voption =
        match expr with
        | :? MethodCallExpression as mce
            when mce.Method.Name = "OrderBy"
              || mce.Method.Name = "OrderByDescending"
              || mce.Method.Name = "ThenBy"
              || mce.Method.Name = "ThenByDescending" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let keyExpr =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then ValueSome mce.Arguments.[0] else ValueNone
                elif mce.Arguments.Count >= 2 then ValueSome mce.Arguments.[1]
                else ValueNone
            if isNull sourceExpr then ValueNone
            else
                let dir =
                    match mce.Method.Name with
                    | "OrderBy" | "ThenBy" -> SortDirection.Asc
                    | _ -> SortDirection.Desc
                let isThen = mce.Method.Name = "ThenBy" || mce.Method.Name = "ThenByDescending"
                match keyExpr with
                | ValueSome key ->
                    match tryPeelOrderByFromSource sourceExpr with
                    | ValueSome (innerSource, existingKeys) ->
                        if isThen then
                            // ThenBy appends to existing ordering.
                            ValueSome (innerSource, existingKeys @ [(key, dir)])
                        else
                            // OrderBy replaces all previous ordering.
                            ValueSome (innerSource, [(key, dir)])
                    | ValueNone ->
                        if isThen then
                            // ThenBy without preceding OrderBy — reject.
                            raise (NotSupportedException(
                                "Error: ThenBy/ThenByDescending requires a preceding OrderBy on DBRefMany.\nReason: Composite ordering must start with OrderBy.\nFix: Add .OrderBy(key) before .ThenBy(key)."))
                        else
                            ValueSome (sourceExpr, [(key, dir)])
                | ValueNone -> ValueNone
        | _ -> ValueNone

    /// L3: Build sort key DU expressions from peeled OrderBy key selectors.
    let private buildSortKeyDus (qb: QueryBuilder) (tgtAlias: string) (sortKeys: (Expression * SortDirection) list) : OrderBy list =
        sortKeys
        |> List.map (fun (keyExpr, dir) ->
            match tryExtractLambdaExpression keyExpr with
            | ValueSome keyLambda ->
                let subQb = qb.ForSubquery(tgtAlias, keyLambda)
                let keyDu = visitDu keyLambda.Body subQb
                { Expr = keyDu; Direction = dir }
            | ValueNone ->
                raise (NotSupportedException(
                    "Error: Cannot extract key selector lambda for OrderBy on DBRefMany.\nFix: Use a simple lambda (e.g., x => x.Name).")))

    /// L4: Peel .Take(n) and .Skip(n) calls from a source expression.
    /// Returns (innerExpr, limit: Expression option, offset: Expression option).
    /// Admits only: Take(n), Skip(n), Skip(m).Take(n) (canonical pagination).
    /// Rejects chained: Take.Take, Skip.Skip, Take.Skip (complex composition deferred).
    let private tryPeelTakeSkipFromSource (expr: Expression) : (Expression * Expression option * Expression option) voption =
        let extractSourceAndArg (mce: MethodCallExpression) =
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let argExpr =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
                elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
                else None
            sourceExpr, argExpr
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "Take" ->
            let sourceExpr, takeArg = extractSourceAndArg mce
            if isNull sourceExpr then ValueNone
            else
                // Check if source is Skip (canonical Skip.Take pagination).
                match sourceExpr with
                | :? MethodCallExpression as innerMce when innerMce.Method.Name = "Skip" ->
                    let innerSource, skipArg = extractSourceAndArg innerMce
                    if isNull innerSource then ValueNone
                    else ValueSome (innerSource, takeArg, skipArg)
                | _ ->
                    // Bare Take(n) — no Skip.
                    ValueSome (sourceExpr, takeArg, None)
        | :? MethodCallExpression as mce when mce.Method.Name = "Skip" ->
            let sourceExpr, skipArg = extractSourceAndArg mce
            if isNull sourceExpr then ValueNone
            else
                // Bare Skip(n) — no Take.
                ValueSome (sourceExpr, None, skipArg)
        | _ -> ValueNone

    /// L4: Build LIMIT/OFFSET DU expressions from peeled Take/Skip arguments.
    let private buildLimitOffset (qb: QueryBuilder) (limitExpr: Expression option) (offsetExpr: Expression option) : SqlExpr option * SqlExpr option =
        let visitArg (e: Expression) =
            match e with
            | :? ConstantExpression as ce -> SqlExpr.Literal(SqlLiteral.Integer(System.Convert.ToInt64(ce.Value)))
            | _ -> visitDu e qb
        let limit =
            match limitExpr with
            | Some e -> Some (visitArg e)
            | None ->
                // SQLite requires LIMIT with OFFSET. If offset present but no limit, use LIMIT -1.
                match offsetExpr with
                | Some _ -> Some (SqlExpr.Literal(SqlLiteral.Integer -1L))
                | None -> None
        let offset =
            match offsetExpr with
            | Some e -> Some (visitArg e)
            | None -> None
        limit, offset

    /// L9: Build correlated subquery parts from a Select-on-DBRefMany expression.
    /// Handles Select(DBRefMany, proj) and Select(Where(DBRefMany, pred), proj).
    /// Returns the subquery core for use in set operations.
    let private tryBuildProjectedSetOperand (qb: QueryBuilder) (selectExpr: Expression) : (SqlExpr * SelectCore * string) voption =
        match selectExpr with
        | :? MethodCallExpression as selectMce when selectMce.Method.Name = "Select" ->
            let selectSource, projArg =
                if not (isNull selectMce.Object) then
                    selectMce.Object, (if selectMce.Arguments.Count >= 1 then Some selectMce.Arguments.[0] else None)
                elif selectMce.Arguments.Count >= 2 then
                    selectMce.Arguments.[0], Some selectMce.Arguments.[1]
                else null, None
            if isNull selectSource || projArg.IsNone then ValueNone
            else
                let unwrappedSrc = unwrapConvert selectSource
                // Peel Where from inside Select.
                let innerSrc, wherePreds =
                    if isDBRefManyType unwrappedSrc.Type then unwrappedSrc, []
                    else
                        match tryPeelWhereFromDBRefMany unwrappedSrc with
                        | ValueSome (s, p) -> s, p
                        | ValueNone -> unwrappedSrc, []
                match tryGetDBRefManyOwnerRef qb innerSrc with
                | ValueSome ownerRef ->
                    match projArg.Value |> tryExtractLambdaExpression with
                    | ValueSome projLambda ->
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

                        let wherePredDus =
                            if wherePreds.IsEmpty then []
                            else buildFilteredPredicateDus qb tgtAlias wherePreds

                        let subQb = qb.ForSubquery(tgtAlias, projLambda)
                        let projectedDu = visitDu projLambda.Body subQb

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
                            wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

                        let core =
                            { mkSubCore
                                [{ Alias = Some "v"; Expr = projectedDu }]
                                (Some(BaseTable(linkTable, Some lnkAlias)))
                                (Some fullWhere) with
                                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] }
                        ValueSome (projectedDu, core, tgtAlias)
                    | ValueNone -> ValueNone
                | ValueNone -> ValueNone
        | _ -> ValueNone

    /// L10: Peel .OfType<T>() from a source expression.
    /// Returns (innerExpr, typeName) if the source is OfType on a DBRefMany chain.
    let private tryPeelOfTypeFromSource (expr: Expression) : (Expression * string) voption =
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "OfType" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            if isNull sourceExpr then ValueNone
            else
                let genericArgs = mce.Method.GetGenericArguments()
                if genericArgs.Length = 1 then
                    match Utils.typeToName genericArgs.[0] with
                    | Some typeName -> ValueSome (sourceExpr, typeName)
                    | None -> ValueNone
                else ValueNone
        | _ -> ValueNone

    /// L10: Try to get DBRefMany owner ref, peeling through OfType if present.
    /// Returns (ownerRef, ofTypeName option).
    let private tryGetDBRefManyOwnerRefWithOfType (qb: QueryBuilder) (expr: Expression) : (DBRefManyOwnerRef * string option) voption =
        match tryGetDBRefManyOwnerRef qb expr with
        | ValueSome ref -> ValueSome (ref, None)
        | ValueNone ->
            match tryPeelOfTypeFromSource expr with
            | ValueSome (innerExpr, typeName) ->
                match tryGetDBRefManyOwnerRef qb innerExpr with
                | ValueSome ref -> ValueSome (ref, Some typeName)
                | ValueNone -> ValueNone
            | ValueNone -> ValueNone

    /// L10: Build a type discriminator WHERE predicate for OfType<T>.
    let private buildOfTypePredicate (tgtAlias: string) (typeName: string) : SqlExpr =
        SqlExpr.Binary(
            SqlExpr.FunctionCall("jsonb_extract", [
                SqlExpr.Column(Some tgtAlias, "Value")
                SqlExpr.Literal(SqlLiteral.String "$.$type")]),
            BinaryOperator.Eq,
            SqlExpr.Literal(SqlLiteral.String typeName))

    /// L12: Build a TakeWhile/SkipWhile windowed DerivedTable from a DBRefMany source.
    /// Returns the inner SELECT with _cf window column and the appropriate outer WHERE filter.
    /// The caller wraps this in the terminal (EXISTS, COUNT, json_group_array).
    let private buildTakeWhileCore
        (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (wherePredicates: Expression list)
        (sortKeys: (Expression * SortDirection) list) (twPredLambda: LambdaExpression) (isTakeWhile: bool)
        (ofTypeName: string option) : SqlSelect =
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

        let wherePredDus =
            let basePreds = if wherePredicates.IsEmpty then [] else buildFilteredPredicateDus qb tgtAlias wherePredicates
            match ofTypeName with
            | Some tn -> basePreds @ [buildOfTypePredicate tgtAlias tn]
            | None -> basePreds
        let sortKeyDus = buildSortKeyDus qb tgtAlias sortKeys

        let twSubQb = qb.ForSubquery(tgtAlias, twPredLambda)
        let twPredDu = visitDu twPredLambda.Body twSubQb
        let caseExpr = SqlExpr.CaseExpr(
            (SqlExpr.Unary(UnaryOperator.Not, twPredDu), SqlExpr.Literal(SqlLiteral.Integer 1L)),
            [],
            Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
        let windowSpec = {
            Kind = NamedWindowFunction "SUM"
            Arguments = [caseExpr]
            PartitionBy = []
            OrderBy = sortKeyDus |> List.map (fun ob -> (ob.Expr, ob.Direction))
        }
        let cfExpr = SqlExpr.WindowCall windowSpec

        let joinOn =
            SqlExpr.Binary(
                SqlExpr.Column(Some tgtAlias, "Id"), BinaryOperator.Eq,
                SqlExpr.Column(Some lnkAlias, targetColumn))
        let ownerWhere =
            SqlExpr.Binary(
                SqlExpr.Column(Some lnkAlias, ownerColumn), BinaryOperator.Eq,
                SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
        let fullWhere =
            wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

        // Inner: SELECT 1 AS v, _cf FROM link JOIN target WHERE ... ORDER BY key
        let innerCore =
            { mkSubCore
                [{ Alias = Some "v"; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }
                 { Alias = Some "_cf"; Expr = cfExpr }]
                (Some(BaseTable(linkTable, Some lnkAlias)))
                (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                OrderBy = sortKeyDus }
        let innerSelect = { Ctes = []; Body = SingleSelect innerCore }
        let ordAlias = sprintf "_tw%d" aliasId

        // Outer: SELECT v FROM (inner) WHERE _cf = 0 (TakeWhile) or _cf > 0 (SkipWhile)
        let cfFilter =
            if isTakeWhile then
                SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
            else
                SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
        let outerCore =
            mkSubCore
                [{ Alias = Some "v"; Expr = SqlExpr.Column(Some ordAlias, "v") }]
                (Some(DerivedTable(innerSelect, ordAlias)))
                (Some cfFilter)
        { Ctes = []; Body = SingleSelect outerCore }

    /// L12: Peel TakeWhile/SkipWhile from expression tree.
    /// Returns (innerExpr, predicateLambda, isTakeWhile).
    let private tryPeelTakeWhileSkipWhile (expr: Expression) : (Expression * LambdaExpression * bool) voption =
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "TakeWhile" || mce.Method.Name = "SkipWhile" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let predExpr =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
                elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
                else None
            if isNull sourceExpr then ValueNone
            else
                match predExpr with
                | Some pe ->
                    match tryExtractLambdaExpression pe with
                    | ValueSome lambda -> ValueSome (sourceExpr, lambda, mce.Method.Name = "TakeWhile")
                    | ValueNone -> ValueNone
                | None -> ValueNone
        | _ -> ValueNone

    /// L9: Null-safe equality using SQLite IS operator (treats NULL IS NULL as true).
    let private nullSafeEq (left: SqlExpr) (right: SqlExpr) : SqlExpr =
        SqlExpr.Binary(left, BinaryOperator.Is, right)

    /// L9: Check if a source expression is a set operation (Intersect/Except/Union/Concat)
    /// on two Select-projected DBRefMany operands.
    let private tryMatchSetOperation (expr: Expression) : (string * Expression * Expression) voption =
        match expr with
        | :? MethodCallExpression as mce
            when mce.Method.Name = "Intersect"
              || mce.Method.Name = "Except"
              || mce.Method.Name = "Union"
              || mce.Method.Name = "Concat" ->
            let leftSource =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let rightSource =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then mce.Arguments.[0] else null
                elif mce.Arguments.Count >= 2 then mce.Arguments.[1]
                else null
            if isNull leftSource || isNull rightSource then ValueNone
            else ValueSome (mce.Method.Name, leftSource, rightSource)
        | _ -> ValueNone

    /// L8: Peel .GroupBy(keySelector) from a source expression.
    /// Returns (innerExpr, keySelector lambda) if the source is a GroupBy on a DBRefMany chain.
    let private tryPeelGroupByFromSource (expr: Expression) : (Expression * LambdaExpression) voption =
        match expr with
        | :? MethodCallExpression as mce when mce.Method.Name = "GroupBy" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            let keyExpr =
                if not (isNull mce.Object) then
                    if mce.Arguments.Count >= 1 then Some mce.Arguments.[0] else None
                elif mce.Arguments.Count >= 2 then Some mce.Arguments.[1]
                else None
            if isNull sourceExpr then ValueNone
            else
                match keyExpr with
                | Some ke ->
                    match tryExtractLambdaExpression ke with
                    | ValueSome keyLambda -> ValueSome (sourceExpr, keyLambda)
                    | ValueNone -> ValueNone
                | None -> ValueNone
        | _ -> ValueNone

    /// L8: Translate an IGrouping predicate body (g => g.Count() > N) to a HAVING DU expression.
    /// Recognizes g.Count(), g.Sum(sel), g.Min(sel), g.Max(sel), g.Average(sel), g.Key,
    /// and binary comparisons/logic.
    let private translateGroupingPredicate (qb: QueryBuilder) (tgtAlias: string) (groupKeyDu: SqlExpr) (groupParam: ParameterExpression) (body: Expression) : SqlExpr =
        let rec visit (e: Expression) : SqlExpr =
            let e = unwrapConvert e
            match e with
            | :? BinaryExpression as be ->
                let op =
                    match be.NodeType with
                    | ExpressionType.GreaterThan -> BinaryOperator.Gt
                    | ExpressionType.GreaterThanOrEqual -> BinaryOperator.Ge
                    | ExpressionType.LessThan -> BinaryOperator.Lt
                    | ExpressionType.LessThanOrEqual -> BinaryOperator.Le
                    | ExpressionType.Equal -> BinaryOperator.Eq
                    | ExpressionType.NotEqual -> BinaryOperator.Ne
                    | ExpressionType.AndAlso -> BinaryOperator.And
                    | ExpressionType.OrElse -> BinaryOperator.Or
                    | _ -> raise (NotSupportedException(sprintf "Unsupported binary operator in GroupBy predicate: %A" be.NodeType))
                SqlExpr.Binary(visit be.Left, op, visit be.Right)
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? int as v -> SqlExpr.Literal(SqlLiteral.Integer(int64 v))
                | :? int64 as v -> SqlExpr.Literal(SqlLiteral.Integer v)
                | :? double as v -> SqlExpr.Literal(SqlLiteral.Float v)
                | :? string as v -> SqlExpr.Literal(SqlLiteral.String v)
                | :? bool as v -> SqlExpr.Literal(SqlLiteral.Integer(if v then 1L else 0L))
                | _ -> visitDu e qb
            | :? MemberExpression as me ->
                // g.Key → the GROUP BY key expression
                if me.Member.Name = "Key" && me.Expression :? ParameterExpression then
                    let pe = me.Expression :?> ParameterExpression
                    if System.Object.ReferenceEquals(pe, groupParam) then
                        groupKeyDu
                    else visitDu e qb
                else visitDu e qb
            | :? MethodCallExpression as mc ->
                // Group aggregate methods: g.Count(), g.Sum(sel), etc.
                let isGroupMethod =
                    let source =
                        if not (isNull mc.Object) then mc.Object
                        elif mc.Arguments.Count > 0 then mc.Arguments.[0]
                        else null
                    if isNull source then false
                    else
                        match unwrapConvert source with
                        | :? ParameterExpression as pe -> System.Object.ReferenceEquals(pe, groupParam)
                        | _ -> false
                if isGroupMethod then
                    match mc.Method.Name with
                    | "Count" ->
                        // g.Count() → COUNT(*)
                        SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
                    | "Sum" | "Min" | "Max" | "Average" ->
                        let aggKind =
                            match mc.Method.Name with
                            | "Sum" -> AggregateKind.Sum
                            | "Min" -> AggregateKind.Min
                            | "Max" -> AggregateKind.Max
                            | "Average" -> AggregateKind.Avg
                            | _ -> failwith "unreachable"
                        // Extract selector: g.Sum(x => x.Field)
                        let selectorExpr =
                            if not (isNull mc.Object) then
                                if mc.Arguments.Count >= 1 then mc.Arguments.[0] else null
                            elif mc.Arguments.Count >= 2 then mc.Arguments.[1]
                            else null
                        if isNull selectorExpr then
                            raise (NotSupportedException(sprintf "GroupBy aggregate %s requires a selector lambda." mc.Method.Name))
                        match tryExtractLambdaExpression selectorExpr with
                        | ValueSome selectorLambda ->
                            let subQb = qb.ForSubquery(tgtAlias, selectorLambda)
                            let selectorDu = visitDu selectorLambda.Body subQb
                            let aggExpr = SqlExpr.AggregateCall(aggKind, Some selectorDu, false, None)
                            if mc.Method.Name = "Sum" then
                                SqlExpr.Coalesce(aggExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                            else aggExpr
                        | ValueNone ->
                            raise (NotSupportedException("Cannot extract selector for GroupBy aggregate."))
                    | "LongCount" ->
                        SqlExpr.AggregateCall(AggregateKind.Count, None, false, None)
                    | other ->
                        raise (NotSupportedException(sprintf "Unsupported group method: %s. Only Count, Sum, Min, Max, Average are admitted." other))
                else visitDu e qb
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
                SqlExpr.Unary(UnaryOperator.Not, visit ue.Operand)
            | _ -> visitDu e qb
        visit body

    /// L1/L3: Build a filtered COUNT subquery for DBRefMany.Where(pred).Count().
    /// Emits: ScalarSubquery(SELECT COUNT(*) FROM link JOIN target WHERE ownerLink AND pred1 AND pred2 ... [ORDER BY key])
    /// L3: sortKeys emitted faithfully (dead SQL, order-vacuous for COUNT).
    let private buildFilteredCountSubquery (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (predicateExprs: Expression list) (sortKeys: (Expression * SortDirection) list) (ofTypeName: string option) : SqlExpr =
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
        let sortKeyDus = if sortKeys.IsEmpty then [] else buildSortKeyDus qb tgtAlias sortKeys

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
            let withPreds = predicateDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere
            match ofTypeName with
            | Some tn -> SqlExpr.Binary(withPreds, BinaryOperator.And, buildOfTypePredicate tgtAlias tn)
            | None -> withPreds
        let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
        let core =
            { mkSubCore countProj (Some(BaseTable(linkTable, Some lnkAlias))) (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                OrderBy = sortKeyDus }
        let subSelect = { Ctes = []; Body = SingleSelect core }
        SqlExpr.ScalarSubquery subSelect

    /// L1/L3: Build a filtered EXISTS subquery for DBRefMany.Where(pred).Any().
    /// L3: sortKeys emitted faithfully (dead SQL, order-vacuous for EXISTS).
    let private buildFilteredExistsSubquery (qb: QueryBuilder) (ownerRef: DBRefManyOwnerRef) (predicateExprs: Expression list) (sortKeys: (Expression * SortDirection) list) (ofTypeName: string option) : SqlExpr =
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
        let sortKeyDus = if sortKeys.IsEmpty then [] else buildSortKeyDus qb tgtAlias sortKeys

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
            let withPreds = predicateDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere
            match ofTypeName with
            | Some tn -> SqlExpr.Binary(withPreds, BinaryOperator.And, buildOfTypePredicate tgtAlias tn)
            | None -> withPreds
        let core =
            { mkSubCore
                [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                (Some(BaseTable(linkTable, Some lnkAlias)))
                (Some fullWhere) with
                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                OrderBy = sortKeyDus }
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

        // L1/L8: .Where(pred).Count() / .LongCount() / GroupBy(key).Count()
        | :? MethodCallExpression as mce when mce.Method.Name = "Count" || mce.Method.Name = "LongCount" ->
            let sourceArg, predArg = extractSourceAndPredicate mce
            match sourceArg with
            | ValueSome sourceExpr ->
                // L7a: Check for Distinct.Count() — emit COUNT(DISTINCT proj).
                match sourceExpr with
                | :? MethodCallExpression as distinctMce when distinctMce.Method.Name = "Distinct" ->
                    let distinctSource =
                        if not (isNull distinctMce.Object) then distinctMce.Object
                        elif distinctMce.Arguments.Count > 0 then distinctMce.Arguments.[0]
                        else null
                    if not (isNull distinctSource) then
                        match tryBuildProjectedSetOperand qb distinctSource with
                        | ValueSome (_projectedDu, innerCore, _tgtAlias) ->
                            // Two-layer: inner SELECT DISTINCT proj, outer COUNT(*).
                            // Using COUNT(DISTINCT proj) would drop NULLs — LINQ counts null as distinct.
                            let distinctCore = { innerCore with Distinct = true }
                            let distinctSelect = { Ctes = []; Body = SingleSelect distinctCore }
                            let dtAlias = sprintf "_dc%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                            let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                            let outerCore = mkSubCore countProj (Some(DerivedTable(distinctSelect, dtAlias))) None
                            let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                            qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery outerSelect)
                            true
                        | ValueNone -> false
                    else false
                | _ ->
                // L8: Check for GroupBy.Count() first.
                match tryPeelGroupByFromSource sourceExpr with
                | ValueSome (groupByInner, keyLambda) ->
                    // GroupBy(key).Count() → SELECT COUNT(*) FROM (SELECT 1 FROM link JOIN target WHERE owner GROUP BY key)
                    let innerGBSource, wherePredsGBC =
                        match tryPeelWhereFromDBRefMany groupByInner with
                        | ValueSome (dbRefSrc, preds) -> dbRefSrc, preds
                        | ValueNone ->
                            if isDBRefManyType (unwrapConvert groupByInner).Type then groupByInner, []
                            else groupByInner, []
                    match tryGetDBRefManyOwnerRef qb innerGBSource with
                    | ValueSome ownerRef ->
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

                        let wherePredDus =
                            if wherePredsGBC.IsEmpty then []
                            else buildFilteredPredicateDus qb tgtAlias wherePredsGBC
                        let subQbKey = qb.ForSubquery(tgtAlias, keyLambda)
                        let groupKeyDu = visitDu keyLambda.Body subQbKey

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
                            wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

                        // Inner: SELECT 1 FROM link JOIN target WHERE owner GROUP BY key
                        let innerCore =
                            { mkSubCore
                                [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                                (Some(BaseTable(linkTable, Some lnkAlias)))
                                (Some fullWhere) with
                                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                GroupBy = [groupKeyDu] }
                        let innerSelect = { Ctes = []; Body = SingleSelect innerCore }
                        let gbAlias = sprintf "_gb%d" aliasId
                        // Outer: SELECT COUNT(*) FROM (inner) AS _gb
                        let outerCountProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                        let outerCore = mkSubCore outerCountProj (Some(DerivedTable(innerSelect, gbAlias))) None
                        let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery outerSelect)
                        true
                    | ValueNone -> false
                | ValueNone ->
                // L12: Check for TakeWhile/SkipWhile on Count.
                match tryPeelTakeWhileSkipWhile sourceExpr with
                | ValueSome (twInnerC, twPredLambdaC, isTWC) ->
                    match tryPeelOrderByFromSource twInnerC with
                    | ValueSome (orderInnerC, sortKeysC) ->
                        let whereInnerC, wherePredsC =
                            match tryPeelWhereFromDBRefMany orderInnerC with
                            | ValueSome (s, p) -> s, p
                            | ValueNone -> orderInnerC, []
                        match tryGetDBRefManyOwnerRefWithOfType qb whereInnerC with
                        | ValueSome (ownerRef, ofTypeNameC) ->
                            let twSelect = buildTakeWhileCore qb ownerRef wherePredsC sortKeysC twPredLambdaC isTWC ofTypeNameC
                            let dtAlias = sprintf "_twc%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                            let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                            let outerCore = mkSubCore countProj (Some(DerivedTable(twSelect, dtAlias))) None
                            let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                            qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery outerSelect)
                            true
                        | ValueNone -> false
                    | ValueNone ->
                        raise (NotSupportedException(
                            "Error: TakeWhile/SkipWhile requires an explicit OrderBy.\nFix: Add .OrderBy(key) before .TakeWhile(pred)."))
                | ValueNone ->
                // L9: Check for set operations on Count.
                match tryMatchSetOperation sourceExpr with
                | ValueSome (setOpName, leftExpr, rightExpr) ->
                    match tryBuildProjectedSetOperand qb leftExpr, tryBuildProjectedSetOperand qb rightExpr with
                    | ValueSome (leftProjDu, leftCore, _), ValueSome (_rightProjDu, rightCore, _rightTgtAlias) ->
                        match setOpName with
                        | "Intersect" | "Except" ->
                            // Set semantics: deduplicate left side before counting.
                            let addExistsCheck (isIntersect: bool) =
                                let rightCoreWithEq =
                                    { rightCore with
                                        Where =
                                            match rightCore.Where with
                                            | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, nullSafeEq _rightProjDu leftProjDu))
                                            | None -> Some(nullSafeEq _rightProjDu leftProjDu) }
                                let rightExistsSel = { Ctes = []; Body = SingleSelect rightCoreWithEq }
                                let existsExpr = if isIntersect then SqlExpr.Exists rightExistsSel else SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists rightExistsSel)
                                { leftCore with
                                    Distinct = true  // SET semantics: deduplicate projected values
                                    Where =
                                        match leftCore.Where with
                                        | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, existsExpr))
                                        | None -> Some existsExpr }
                            let innerCore = addExistsCheck (setOpName = "Intersect")
                            let innerSel = { Ctes = []; Body = SingleSelect innerCore }
                            let dtAlias = sprintf "_sc%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                            let countProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                            let outerCore = mkSubCore countProj (Some(DerivedTable(innerSel, dtAlias))) None
                            let outerSel = { Ctes = []; Body = SingleSelect outerCore }
                            qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery outerSel)
                            true
                        | "Concat" ->
                            // Concat.Count = left.Count + right.Count (sum of both sides).
                            let leftSel = { Ctes = []; Body = SingleSelect leftCore }
                            let rightSel = { Ctes = []; Body = SingleSelect rightCore }
                            let leftDtAlias = sprintf "_cl%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                            let rightDtAlias = sprintf "_cr%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                            let leftCountCore = mkSubCore [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }] (Some(DerivedTable(leftSel, leftDtAlias))) None
                            let rightCountCore = mkSubCore [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }] (Some(DerivedTable(rightSel, rightDtAlias))) None
                            let leftCountSel = { Ctes = []; Body = SingleSelect leftCountCore }
                            let rightCountSel = { Ctes = []; Body = SingleSelect rightCountCore }
                            qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Binary(SqlExpr.ScalarSubquery leftCountSel, BinaryOperator.Add, SqlExpr.ScalarSubquery rightCountSel))
                            true
                        | _ ->
                            // Union.Count via inclusion-exclusion: |A ∪ B| = |A| + |B| - |A ∩ B|.
                            // Left distinct count.
                            let leftSel = { Ctes = []; Body = SingleSelect { leftCore with Distinct = true } }
                            let leftDtAlias = sprintf "_ul%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                            let leftCountCore = mkSubCore [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }] (Some(DerivedTable(leftSel, leftDtAlias))) None
                            let leftCountExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect leftCountCore }
                            // Right distinct count.
                            let rightSel = { Ctes = []; Body = SingleSelect { rightCore with Distinct = true } }
                            let rightDtAlias = sprintf "_ur%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                            let rightCountCore = mkSubCore [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }] (Some(DerivedTable(rightSel, rightDtAlias))) None
                            let rightCountExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect rightCountCore }
                            // Intersect count (for subtraction).
                            // Need fresh copies of left/right cores for the intersect count.
                            match tryBuildProjectedSetOperand qb leftExpr, tryBuildProjectedSetOperand qb rightExpr with
                            | ValueSome (leftProjDu2, leftCore2, _), ValueSome (_rightProjDu2, rightCore2, _) ->
                                let rightCoreWithEq2 =
                                    { rightCore2 with
                                        Where =
                                            match rightCore2.Where with
                                            | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, nullSafeEq _rightProjDu2 leftProjDu2))
                                            | None -> Some(nullSafeEq _rightProjDu2 leftProjDu2) }
                                let rightExistsSel2 = { Ctes = []; Body = SingleSelect rightCoreWithEq2 }
                                let intersectCore =
                                    { leftCore2 with
                                        Distinct = true  // SET semantics
                                        Where =
                                            match leftCore2.Where with
                                            | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, SqlExpr.Exists rightExistsSel2))
                                            | None -> Some(SqlExpr.Exists rightExistsSel2) }
                                let intersectSel = { Ctes = []; Body = SingleSelect intersectCore }
                                let intersectDtAlias = sprintf "_ui%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                let intersectCountCore = mkSubCore [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }] (Some(DerivedTable(intersectSel, intersectDtAlias))) None
                                let intersectCountExpr = SqlExpr.ScalarSubquery { Ctes = []; Body = SingleSelect intersectCountCore }
                                // |A ∪ B| = |A| + |B| - |A ∩ B|
                                let unionCountExpr = SqlExpr.Binary(SqlExpr.Binary(leftCountExpr, BinaryOperator.Add, rightCountExpr), BinaryOperator.Sub, intersectCountExpr)
                                qb.DuHandlerResult.Value <- ValueSome unionCountExpr
                                true
                            | _ -> false
                    | _ -> false
                | ValueNone ->
                // L4: Peel Take/Skip first (outermost).
                let sourceAfterTakeSkip, limitExprC, offsetExprC =
                    match tryPeelTakeSkipFromSource sourceExpr with
                    | ValueSome (inner, limit, offset) -> inner, limit, offset
                    | ValueNone -> sourceExpr, None, None
                // L3: Peel OrderBy (dead SQL for Count, emitted faithfully per Hard Rule 7).
                let sourceAfterOrder, sortKeys =
                    match tryPeelOrderByFromSource sourceAfterTakeSkip with
                    | ValueSome (inner, keys) -> inner, keys
                    | ValueNone -> sourceAfterTakeSkip, []
                match tryPeelWhereFromDBRefMany sourceAfterOrder with
                | ValueSome (dbRefManySource, predicateExprs) ->
                    match predArg with
                    | ValueSome _ ->
                        raise (NotSupportedException(filteredWhereUnsupportedTerminalMessage))
                    | ValueNone ->
                        // L10: Get owner ref with OfType support.
                        match tryGetDBRefManyOwnerRefWithOfType qb dbRefManySource with
                        | ValueSome (ownerRef, _ofTypeNameCount) ->
                            let countExpr = buildFilteredCountSubquery qb ownerRef predicateExprs sortKeys _ofTypeNameCount
                            // L4: If Take/Skip present, wrap COUNT in derived table:
                            // SELECT COUNT(*) FROM (SELECT 1 FROM ... WHERE ... [ORDER BY] LIMIT n OFFSET m)
                            if limitExprC.IsSome || offsetExprC.IsSome then
                                match countExpr with
                                | SqlExpr.ScalarSubquery subSel ->
                                    match subSel.Body with
                                    | SingleSelect core ->
                                        let limitDu, offsetDu = buildLimitOffset qb limitExprC offsetExprC
                                        // Replace COUNT projection with SELECT 1, add LIMIT/OFFSET
                                        let innerCore =
                                            { core with
                                                Projections = ProjectionSetOps.ofList [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                                                Limit = limitDu
                                                Offset = offsetDu }
                                        let innerSelect = { Ctes = []; Body = SingleSelect innerCore }
                                        let aliasId = System.Threading.Interlocked.Increment(&subqueryAliasCounter)
                                        let dtAlias = sprintf "_pg%d" aliasId
                                        let outerCountProj = [{ Alias = None; Expr = SqlExpr.AggregateCall(AggregateKind.Count, None, false, None) }]
                                        let outerCore = mkSubCore outerCountProj (Some(DerivedTable(innerSelect, dtAlias))) None
                                        let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery outerSelect)
                                        true
                                    | _ ->
                                        qb.DuHandlerResult.Value <- ValueSome countExpr
                                        true
                                | _ ->
                                    qb.DuHandlerResult.Value <- ValueSome countExpr
                                    true
                            else
                                qb.DuHandlerResult.Value <- ValueSome countExpr
                                true
                        | ValueNone -> false
                // L10: If Where peeling found nothing, try OfType(DBRefMany) directly.
                | ValueNone ->
                    match tryGetDBRefManyOwnerRefWithOfType qb sourceAfterOrder with
                    | ValueSome (ownerRef, ofTypeNameDirect) ->
                        let countExpr = buildFilteredCountSubquery qb ownerRef [] [] ofTypeNameDirect
                        qb.DuHandlerResult.Value <- ValueSome countExpr
                        true
                    | ValueNone -> false
            | ValueNone -> false

        // Case 2/3: Any() / Any(pred) over DBRefMany in either extension-call or instance-call form.
        // L1: also handles .Where(pred).Any() → normalized to .Any(pred) with AND composition.
        // L3: also handles .OrderBy(key).Any() — OrderBy is dead SQL, peeled and passed through.
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
                    // L12: Check for TakeWhile/SkipWhile first.
                    match tryPeelTakeWhileSkipWhile sourceExpr with
                    | ValueSome (twInner, twPredLambda, isTW) ->
                        // Peel OrderBy from inside TakeWhile.
                        match tryPeelOrderByFromSource twInner with
                        | ValueSome (orderInner, sortKeysAny) ->
                            let whereInner, wherePredsAny =
                                match tryPeelWhereFromDBRefMany orderInner with
                                | ValueSome (s, p) -> s, p
                                | ValueNone -> orderInner, []
                            match tryGetDBRefManyOwnerRefWithOfType qb whereInner with
                            | ValueSome (ownerRef, ofTypeNameAny) ->
                                let twSelect = buildTakeWhileCore qb ownerRef wherePredsAny sortKeysAny twPredLambda isTW ofTypeNameAny
                                let dtAlias = sprintf "_twa%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists { Ctes = []; Body = twSelect.Body })
                                true
                            | ValueNone -> false
                        | ValueNone ->
                            raise (NotSupportedException(
                                "Error: TakeWhile/SkipWhile requires an explicit OrderBy.\nFix: Add .OrderBy(key) before .TakeWhile(pred)."))
                    | ValueNone ->
                    // L9: Check for set operations (Intersect/Except/Union/Concat) first.
                    match tryMatchSetOperation sourceExpr with
                    | ValueSome (setOpName, leftExpr, rightExpr) ->
                        match tryBuildProjectedSetOperand qb leftExpr, tryBuildProjectedSetOperand qb rightExpr with
                        | ValueSome (leftProjDu, leftCore, _leftTgtAlias), ValueSome (_rightProjDu, rightCore, rightTgtAlias) ->
                            match setOpName with
                            | "Intersect" ->
                                // Intersect.Any() → EXISTS(SELECT 1 FROM leftCore WHERE EXISTS(SELECT 1 FROM rightCore WHERE rightProj = leftProj))
                                let rightSubSelect = { Ctes = []; Body = SingleSelect rightCore }
                                let rightAlias = sprintf "_r%d" (System.Threading.Interlocked.Increment(&subqueryAliasCounter))
                                // Build equality check: rightCore.v = leftCore.v
                                let eqCheck =
                                    SqlExpr.Binary(
                                        SqlExpr.Column(Some rightTgtAlias, "v"),  // reuse rightCore's projection alias
                                        BinaryOperator.Eq,
                                        leftProjDu) // leftCore's projected expression
                                // Wait - rightCore.v is projected as alias "v" but the actual expression is the projected DU.
                                // The rightCore WHERE needs to compare the right projected value with the left projected value.
                                // Simplest: add the equality to rightCore's WHERE clause.
                                let rightCoreWithEq =
                                    { rightCore with
                                        Where =
                                            match rightCore.Where with
                                            | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, nullSafeEq _rightProjDu leftProjDu))
                                            | None -> Some(nullSafeEq _rightProjDu leftProjDu) }
                                let rightExistsSubSelect = { Ctes = []; Body = SingleSelect rightCoreWithEq }
                                let leftCoreWithExists =
                                    { leftCore with
                                        Where =
                                            match leftCore.Where with
                                            | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, SqlExpr.Exists rightExistsSubSelect))
                                            | None -> Some(SqlExpr.Exists rightExistsSubSelect) }
                                let leftSubSelect = { Ctes = []; Body = SingleSelect leftCoreWithExists }
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists leftSubSelect)
                                true
                            | "Except" ->
                                // Except.Any() → EXISTS(SELECT 1 FROM leftCore WHERE NOT EXISTS(SELECT 1 FROM rightCore WHERE rightProj = leftProj))
                                let rightCoreWithEq =
                                    { rightCore with
                                        Where =
                                            match rightCore.Where with
                                            | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, nullSafeEq _rightProjDu leftProjDu))
                                            | None -> Some(nullSafeEq _rightProjDu leftProjDu) }
                                let rightExistsSubSelect = { Ctes = []; Body = SingleSelect rightCoreWithEq }
                                let leftCoreWithNotExists =
                                    { leftCore with
                                        Where =
                                            match leftCore.Where with
                                            | Some w -> Some(SqlExpr.Binary(w, BinaryOperator.And, SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists rightExistsSubSelect)))
                                            | None -> Some(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists rightExistsSubSelect)) }
                                let leftSubSelect = { Ctes = []; Body = SingleSelect leftCoreWithNotExists }
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists leftSubSelect)
                                true
                            | "Union" | "Concat" ->
                                // Union/Concat.Any() → EXISTS(left) OR EXISTS(right)
                                let leftSubSelect = { Ctes = []; Body = SingleSelect leftCore }
                                let rightSubSelect = { Ctes = []; Body = SingleSelect rightCore }
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Binary(SqlExpr.Exists leftSubSelect, BinaryOperator.Or, SqlExpr.Exists rightSubSelect))
                                true
                            | _ -> false
                        | _ -> false
                    | ValueNone ->
                    // L8: Check for GroupBy first (outermost before Where for GroupBy scenarios).
                    match tryPeelGroupByFromSource sourceExpr with
                    | ValueSome (groupByInner, keyLambda) ->
                        // L8: GroupBy(key).Any(g => groupPred) — EXISTS with GROUP BY + HAVING.
                        // Peel Where from inside GroupBy.
                        let innerGBSource, wherePredsGB =
                            match tryPeelWhereFromDBRefMany groupByInner with
                            | ValueSome (dbRefSrc, preds) -> dbRefSrc, preds
                            | ValueNone ->
                                if isDBRefManyType (unwrapConvert groupByInner).Type then groupByInner, []
                                else groupByInner, []
                        match tryGetDBRefManyOwnerRef qb innerGBSource with
                        | ValueSome ownerRef ->
                            match predArg with
                            | ValueSome havingPredicateExpr ->
                                match tryExtractLambdaExpression havingPredicateExpr with
                                | ValueSome havingLambda ->
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

                                    let wherePredDus =
                                        if wherePredsGB.IsEmpty then []
                                        else buildFilteredPredicateDus qb tgtAlias wherePredsGB

                                    // Build GROUP BY key expression.
                                    let subQbKey = qb.ForSubquery(tgtAlias, keyLambda)
                                    let groupKeyDu = visitDu keyLambda.Body subQbKey

                                    // Build HAVING predicate from IGrouping predicate.
                                    let groupParam = havingLambda.Parameters.[0]
                                    let havingDu = translateGroupingPredicate qb tgtAlias groupKeyDu groupParam havingLambda.Body

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
                                        wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

                                    let core =
                                        { mkSubCore
                                            [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                                            (Some(BaseTable(linkTable, Some lnkAlias)))
                                            (Some fullWhere) with
                                            Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                            GroupBy = [groupKeyDu]
                                            Having = Some havingDu }
                                    let subSelect = { Ctes = []; Body = SingleSelect core }
                                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists subSelect)
                                    true
                                | ValueNone -> false
                            | ValueNone -> false
                        | ValueNone -> false
                    | ValueNone ->
                    // L4: Peel Take/Skip first (outermost).
                    let sourceAfterTakeSkip, limitExprA, offsetExprA =
                        match tryPeelTakeSkipFromSource sourceExpr with
                        | ValueSome (inner, limit, offset) -> inner, limit, offset
                        | ValueNone -> sourceExpr, None, None
                    // L3: Peel OrderBy (dead SQL for Any, emitted faithfully per Hard Rule 7).
                    let sourceAfterOrder, sortKeys =
                        match tryPeelOrderByFromSource sourceAfterTakeSkip with
                        | ValueSome (inner, keys) -> inner, keys
                        | ValueNone -> sourceAfterTakeSkip, []
                    // Peel Where (may or may not be present after peeling Take/Skip + OrderBy).
                    let innerSource, wherePredicates =
                        match tryPeelWhereFromDBRefMany sourceAfterOrder with
                        | ValueSome (dbRefSrc, preds) -> dbRefSrc, preds
                        | ValueNone -> sourceAfterOrder, []
                    // L10: Get owner ref with OfType support.
                    match tryGetDBRefManyOwnerRefWithOfType qb innerSource with
                    | ValueSome (ownerRef, ofTypeNameAny) ->
                        match predArg with
                        | ValueSome _ when not wherePredicates.IsEmpty ->
                            raise (NotSupportedException(filteredWhereUnsupportedTerminalMessage))
                        | ValueSome predicateExpr when wherePredicates.IsEmpty && sortKeys.IsEmpty && not (limitExprA.IsSome || offsetExprA.IsSome) && ofTypeNameAny.IsNone ->
                            // Bare Any(pred) after peeling — shouldn't normally reach here (direct case above), but handle gracefully.
                            qb.DuHandlerResult.Value <- ValueSome(buildQuantifierWithPredicate qb ownerRef predicateExpr false)
                            true
                        | _ ->
                            let existsExpr = buildFilteredExistsSubquery qb ownerRef wherePredicates sortKeys ofTypeNameAny
                            // L4: If Take/Skip present, add LIMIT/OFFSET to the EXISTS subquery.
                            if limitExprA.IsSome || offsetExprA.IsSome then
                                match existsExpr with
                                | SqlExpr.Exists subSel ->
                                    match subSel.Body with
                                    | SingleSelect core ->
                                        let limitDu, offsetDu = buildLimitOffset qb limitExprA offsetExprA
                                        let boundedCore = { core with Limit = limitDu; Offset = offsetDu }
                                        let boundedSelect = { Ctes = []; Body = SingleSelect boundedCore }
                                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists boundedSelect)
                                        true
                                    | _ ->
                                        qb.DuHandlerResult.Value <- ValueSome existsExpr
                                        true
                                | _ ->
                                    qb.DuHandlerResult.Value <- ValueSome existsExpr
                                    true
                            else
                                qb.DuHandlerResult.Value <- ValueSome existsExpr
                                true
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
                    // L8: Check for GroupBy.All first.
                    match tryPeelGroupByFromSource sourceExpr with
                    | ValueSome (groupByInnerAll, keyLambdaAll) ->
                        // GroupBy(key).All(g => groupPred) → NOT EXISTS with GROUP BY + HAVING NOT(pred).
                        let innerGBSourceAll, wherePredsGBAll =
                            match tryPeelWhereFromDBRefMany groupByInnerAll with
                            | ValueSome (dbRefSrc, preds) -> dbRefSrc, preds
                            | ValueNone ->
                                if isDBRefManyType (unwrapConvert groupByInnerAll).Type then groupByInnerAll, []
                                else groupByInnerAll, []
                        match tryGetDBRefManyOwnerRef qb innerGBSourceAll with
                        | ValueSome ownerRef ->
                            match predArg with
                            | ValueSome havingPredicateExprAll ->
                                match tryExtractLambdaExpression havingPredicateExprAll with
                                | ValueSome havingLambdaAll ->
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

                                    let wherePredDus =
                                        if wherePredsGBAll.IsEmpty then []
                                        else buildFilteredPredicateDus qb tgtAlias wherePredsGBAll
                                    let subQbKey = qb.ForSubquery(tgtAlias, keyLambdaAll)
                                    let groupKeyDu = visitDu keyLambdaAll.Body subQbKey
                                    let groupParam = havingLambdaAll.Parameters.[0]
                                    // Negate the HAVING predicate for NOT EXISTS pattern.
                                    let havingDu = translateGroupingPredicate qb tgtAlias groupKeyDu groupParam havingLambdaAll.Body
                                    let negatedHaving = SqlExpr.Unary(UnaryOperator.Not, havingDu)

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
                                        wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

                                    let core =
                                        { mkSubCore
                                            [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                                            (Some(BaseTable(linkTable, Some lnkAlias)))
                                            (Some fullWhere) with
                                            Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                            GroupBy = [groupKeyDu]
                                            Having = Some negatedHaving }
                                    let subSelect = { Ctes = []; Body = SingleSelect core }
                                    // NOT EXISTS → all groups satisfy the predicate
                                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists subSelect))
                                    true
                                | ValueNone -> false
                            | ValueNone ->
                                // GroupBy.All() without predicate → vacuous truth.
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Literal(SqlLiteral.Integer 1L))
                                true
                        | ValueNone -> false
                    | ValueNone ->
                    // L4: Peel Take/Skip first (outermost).
                    let sourceAfterTakeSkip, limitExprAll, offsetExprAll =
                        match tryPeelTakeSkipFromSource sourceExpr with
                        | ValueSome (inner, limit, offset) -> inner, limit, offset
                        | ValueNone -> sourceExpr, None, None
                    // L3: Peel OrderBy (dead SQL for All, emitted faithfully per Hard Rule 7).
                    let sourceAfterOrder, sortKeys =
                        match tryPeelOrderByFromSource sourceAfterTakeSkip with
                        | ValueSome (inner, keys) -> inner, keys
                        | ValueNone -> sourceAfterTakeSkip, []
                    // Peel Where (may or may not be present).
                    let innerSourceAll, wherePredicatesAll =
                        match tryPeelWhereFromDBRefMany sourceAfterOrder with
                        | ValueSome (dbRefSrc, preds) -> dbRefSrc, preds
                        | ValueNone -> sourceAfterOrder, []
                    match tryGetDBRefManyOwnerRef qb innerSourceAll with
                    | ValueSome ownerRef ->
                        match predArg with
                        | ValueSome allPredicateExpr ->
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

                            let wherePredDus =
                                if wherePredicatesAll.IsEmpty then []
                                else buildFilteredPredicateDus qb tgtAlias wherePredicatesAll
                            let sortKeyDus = if sortKeys.IsEmpty then [] else buildSortKeyDus qb tgtAlias sortKeys

                            match tryExtractLambdaExpression allPredicateExpr with
                            | ValueSome allPredLambda ->
                                if countDbRefManyDepth allPredLambda.Body > 1 then
                                    raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))
                                let subQb = qb.ForSubquery(tgtAlias, allPredLambda)
                                let allPredDu = visitDu allPredLambda.Body subQb

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
                                let withWheres =
                                    wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

                                let hasTakeSkipAll = limitExprAll.IsSome || offsetExprAll.IsSome
                                if not hasTakeSkipAll then
                                    // No pagination — single-layer NOT EXISTS (existing L2 shape).
                                    let fullWhere =
                                        SqlExpr.Binary(withWheres, BinaryOperator.And, SqlExpr.Unary(UnaryOperator.Not, allPredDu))
                                    let core =
                                        { mkSubCore
                                            [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                                            (Some(BaseTable(linkTable, Some lnkAlias)))
                                            (Some fullWhere) with
                                            Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                            OrderBy = sortKeyDus }
                                    let subSelect = { Ctes = []; Body = SingleSelect core }
                                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists subSelect))
                                    true
                                else
                                    // L4: Pagination present — two-layer: inner bounded rowset, outer NOT EXISTS for violations.
                                    // Inner: SELECT target columns FROM link JOIN target WHERE ownerLink [AND pred] ORDER BY key LIMIT n OFFSET m
                                    // Outer: NOT EXISTS(SELECT 1 FROM (inner) AS _bnd WHERE NOT allPred)
                                    let limitDuAll, offsetDuAll = buildLimitOffset qb limitExprAll offsetExprAll
                                    let innerCore =
                                        { Distinct = false
                                          Projections = AllColumns
                                          Source = Some(BaseTable(linkTable, Some lnkAlias))
                                          Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                          Where = Some withWheres
                                          GroupBy = []; Having = None
                                          OrderBy = sortKeyDus
                                          Limit = limitDuAll
                                          Offset = offsetDuAll }
                                    let innerSelect = { Ctes = []; Body = SingleSelect innerCore }
                                    let bndAlias = sprintf "_bnd%d" aliasId
                                    // Re-visit the All predicate in the bounded alias context.
                                    let bndQb = qb.ForSubquery(bndAlias, allPredLambda)
                                    let bndAllPredDu = visitDu allPredLambda.Body bndQb
                                    let outerWhere = SqlExpr.Unary(UnaryOperator.Not, bndAllPredDu)
                                    let outerCore =
                                        mkSubCore
                                            [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                                            (Some(DerivedTable(innerSelect, bndAlias)))
                                            (Some outerWhere)
                                    let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists outerSelect))
                                    true
                            | ValueNone ->
                                raise (NotSupportedException(
                                    "Error: Cannot extract predicate lambda for relation-backed DBRefMany.All.\nReason: The predicate is not a simple lambda expression.\nFix: Use a simple lambda (e.g., x => ...) or move the operation after AsEnumerable()."))
                        | ValueNone ->
                            // All() without terminal predicate → vacuous truth.
                            qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Literal(SqlLiteral.Integer 1L))
                            true
                    | ValueNone -> false
            | ValueNone -> false

        // L5: Sum/Min/Max/Average over DBRefMany — scalar aggregate correlated subquery.
        | :? MethodCallExpression as mce
            when mce.Method.Name = "Sum"
              || mce.Method.Name = "Min"
              || mce.Method.Name = "Max"
              || mce.Method.Name = "Average" ->
            let sourceArg, selectorArg = extractSourceAndPredicate mce

            match sourceArg with
            | ValueSome sourceExpr ->
                // Unified peeling: Take/Skip → OrderBy → Where → owner ref.
                let sourceAfterTakeSkip, limitExprAgg, offsetExprAgg =
                    match tryPeelTakeSkipFromSource sourceExpr with
                    | ValueSome (inner, limit, offset) -> inner, limit, offset
                    | ValueNone -> sourceExpr, None, None
                let sourceAfterOrder, sortKeys =
                    match tryPeelOrderByFromSource sourceAfterTakeSkip with
                    | ValueSome (inner, keys) -> inner, keys
                    | ValueNone -> sourceAfterTakeSkip, []
                let innerSourceAgg, wherePredicatesAgg =
                    if isDBRefManyType (unwrapConvert sourceAfterOrder).Type then
                        sourceAfterOrder, []
                    else
                        match tryPeelWhereFromDBRefMany sourceAfterOrder with
                        | ValueSome (dbRefSrc, preds) -> dbRefSrc, preds
                        | ValueNone -> sourceAfterOrder, []
                match tryGetDBRefManyOwnerRef qb innerSourceAgg with
                | ValueSome ownerRef ->
                    match selectorArg with
                    | ValueSome selectorExprRaw ->
                        match tryExtractLambdaExpression selectorExprRaw with
                        | ValueSome selectorLambda ->
                            // Average decimal reject per existing contract.
                            if mce.Method.Name = "Average" then
                                let retType = mce.Method.ReturnType
                                if retType = typeof<decimal> || retType = typeof<System.Nullable<decimal>> then
                                    raise (NotSupportedException(
                                        "Decimal Average is not supported on the SQL route because SQLite AVG uses REAL arithmetic and loses decimal precision. Call AsEnumerable() before Average for exact decimal semantics."))

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

                            let wherePredDus =
                                if wherePredicatesAgg.IsEmpty then []
                                else buildFilteredPredicateDus qb tgtAlias wherePredicatesAgg
                            let sortKeyDus = if sortKeys.IsEmpty then [] else buildSortKeyDus qb tgtAlias sortKeys

                            // Visit selector body.
                            let subQb = qb.ForSubquery(tgtAlias, selectorLambda)
                            let selectorDu = visitDu selectorLambda.Body subQb

                            // Determine aggregate kind.
                            let aggKind =
                                match mce.Method.Name with
                                | "Sum" -> AggregateKind.Sum
                                | "Min" -> AggregateKind.Min
                                | "Max" -> AggregateKind.Max
                                | "Average" -> AggregateKind.Avg
                                | other -> failwithf "Unexpected aggregate method: %s" other

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
                                wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

                            let hasTakeSkipAgg = limitExprAgg.IsSome || offsetExprAgg.IsSome
                            let aggExpr = SqlExpr.AggregateCall(aggKind, Some selectorDu, false, None)

                            if not hasTakeSkipAgg then
                                // Single-layer aggregate subquery.
                                let aggProj = [{ Alias = None; Expr = aggExpr }]
                                let core =
                                    { mkSubCore aggProj (Some(BaseTable(linkTable, Some lnkAlias))) (Some fullWhere) with
                                        Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                        OrderBy = sortKeyDus }
                                let subSelect = { Ctes = []; Body = SingleSelect core }
                                let scalarExpr = SqlExpr.ScalarSubquery subSelect
                                // Sum: COALESCE to 0. Min/Max/Average: raw NULL on empty.
                                let finalExpr =
                                    if mce.Method.Name = "Sum" then
                                        SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                                    else scalarExpr
                                qb.DuHandlerResult.Value <- ValueSome finalExpr
                                true
                            else
                                // Two-layer: inner bounded rowset, outer aggregate.
                                let limitDu, offsetDu = buildLimitOffset qb limitExprAgg offsetExprAgg
                                let innerCore =
                                    { Distinct = false
                                      Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = selectorDu }]
                                      Source = Some(BaseTable(linkTable, Some lnkAlias))
                                      Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                      Where = Some fullWhere
                                      GroupBy = []; Having = None
                                      OrderBy = sortKeyDus
                                      Limit = limitDu
                                      Offset = offsetDu }
                                let innerSelect = { Ctes = []; Body = SingleSelect innerCore }
                                let pgAlias = sprintf "_pg%d" aliasId
                                let outerAggExpr = SqlExpr.AggregateCall(aggKind, Some(SqlExpr.Column(Some pgAlias, "v")), false, None)
                                let outerProj = [{ Alias = None; Expr = outerAggExpr }]
                                let outerCore = mkSubCore outerProj (Some(DerivedTable(innerSelect, pgAlias))) None
                                let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                                let scalarExpr = SqlExpr.ScalarSubquery outerSelect
                                let finalExpr =
                                    if mce.Method.Name = "Sum" then
                                        SqlExpr.Coalesce(scalarExpr, [SqlExpr.Literal(SqlLiteral.Integer 0L)])
                                    else scalarExpr
                                qb.DuHandlerResult.Value <- ValueSome finalExpr
                                true
                        | ValueNone ->
                            raise (NotSupportedException(
                                "Error: Cannot extract selector lambda for aggregate on DBRefMany.\nFix: Use a simple lambda (e.g., x => x.Amount)."))
                    | ValueNone -> false
                | ValueNone -> false
            | ValueNone -> false

        // Case 5: Select(proj) over DBRefMany — unified handler for CP-01/CP-02/L3/L4.
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

                // Step -1 (L12): Peel TakeWhile/SkipWhile (outermost).
                let sourceAfterTW, takeWhileInfo =
                    match tryPeelTakeWhileSkipWhile unwrappedSource with
                    | ValueSome (inner, predLambda, isTakeWhile) -> unwrapConvert inner, Some (predLambda, isTakeWhile)
                    | ValueNone -> unwrappedSource, None

                // Step 0 (L4): Peel Take/Skip.
                let sourceAfterTakeSkip, limitExpr, offsetExpr =
                    match tryPeelTakeSkipFromSource sourceAfterTW with
                    | ValueSome (inner, limit, offset) -> unwrapConvert inner, limit, offset
                    | ValueNone -> sourceAfterTW, None, None

                // Step 1: Peel OrderBy.
                let sourceAfterOrder, sortKeys =
                    match tryPeelOrderByFromSource sourceAfterTakeSkip with
                    | ValueSome (inner, keys) -> unwrapConvert inner, keys
                    | ValueNone -> sourceAfterTakeSkip, []

                // Step 2: Peel Where.
                let sourceAfterWhere, wherePredicates =
                    if isDBRefManyType sourceAfterOrder.Type then
                        sourceAfterOrder, []
                    else
                        match tryPeelWhereFromDBRefMany sourceAfterOrder with
                        | ValueSome (dbRefSrc, preds) -> dbRefSrc, preds
                        | ValueNone -> sourceAfterOrder, []

                // Step 3: Get owner ref from the innermost DBRefMany source (L10: with OfType support).
                match tryGetDBRefManyOwnerRefWithOfType qb sourceAfterWhere with
                | ValueSome (ownerRef, ofTypeName) ->
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

                            // Build Where predicate DUs (empty list if no Where).
                            // L10: include OfType discriminator predicate if present.
                            let wherePredDus =
                                let basePreds =
                                    if wherePredicates.IsEmpty then []
                                    else buildFilteredPredicateDus qb tgtAlias wherePredicates
                                match ofTypeName with
                                | Some typeName -> basePreds @ [buildOfTypePredicate tgtAlias typeName]
                                | None -> basePreds

                            // Translate projection body in subquery context.
                            let subQb = qb.ForSubquery(tgtAlias, projLambda)
                            let projectedDu = visitDu projLambda.Body subQb

                            // Common parts: JOIN and WHERE.
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
                                wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

                            let hasTakeSkip = limitExpr.IsSome || offsetExpr.IsSome
                            // L12: TakeWhile/SkipWhile requires OrderBy.
                            match takeWhileInfo with
                            | Some (twPredLambda, isTakeWhile) when sortKeys.IsEmpty ->
                                raise (NotSupportedException(
                                    "Error: TakeWhile/SkipWhile requires an explicit OrderBy before it.\nReason: Without ordering, TakeWhile/SkipWhile is non-deterministic.\nFix: Add .OrderBy(key) before .TakeWhile(pred)."))
                            | _ -> ()
                            if sortKeys.IsEmpty && not hasTakeSkip && takeWhileInfo.IsNone then
                                // No ordering, no pagination — single-layer json_group_array (CP-01/CP-02).
                                let groupArrayExpr = SqlExpr.FunctionCall("json_group_array", [projectedDu])
                                let core =
                                    { mkSubCore
                                        [{ Alias = None; Expr = groupArrayExpr }]
                                        (Some(BaseTable(linkTable, Some lnkAlias)))
                                        (Some fullWhere) with
                                        Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] }
                                let subSelect = { Ctes = []; Body = SingleSelect core }
                                qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery subSelect)
                                true
                            else
                                // L3/L4/L12: Ordering/pagination/TakeWhile present — multi-layer DerivedTable.
                                let sortKeyDus = if sortKeys.IsEmpty then [] else buildSortKeyDus qb tgtAlias sortKeys
                                let limitDu, offsetDu = buildLimitOffset qb limitExpr offsetExpr

                                // L12: If TakeWhile/SkipWhile, add cumulative-failure window function.
                                let innerProjs, hasTakeWhileLayer =
                                    match takeWhileInfo with
                                    | Some (twPredLambda, _isTW) ->
                                        // Build the window function: SUM(CASE WHEN NOT(pred) THEN 1 ELSE 0 END) OVER (ORDER BY key)
                                        let twSubQb = qb.ForSubquery(tgtAlias, twPredLambda)
                                        let twPredDu = visitDu twPredLambda.Body twSubQb
                                        let caseExpr = SqlExpr.CaseExpr(
                                            (SqlExpr.Unary(UnaryOperator.Not, twPredDu), SqlExpr.Literal(SqlLiteral.Integer 1L)),
                                            [],
                                            Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
                                        let windowSpec = {
                                            Kind = NamedWindowFunction "SUM"
                                            Arguments = [caseExpr]
                                            PartitionBy = []
                                            OrderBy = sortKeyDus |> List.map (fun ob -> (ob.Expr, ob.Direction))
                                        }
                                        let cfExpr = SqlExpr.WindowCall windowSpec
                                        [{ Alias = Some "v"; Expr = projectedDu }; { Alias = Some "_cf"; Expr = cfExpr }], true
                                    | None ->
                                        [{ Alias = Some "v"; Expr = projectedDu }], false

                                let innerCore =
                                    { mkSubCore innerProjs
                                        (Some(BaseTable(linkTable, Some lnkAlias)))
                                        (Some fullWhere) with
                                        Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                        OrderBy = sortKeyDus
                                        Limit = limitDu
                                        Offset = offsetDu }
                                let innerSelect = { Ctes = []; Body = SingleSelect innerCore }
                                let ordAlias = sprintf "_ord%d" aliasId

                                if hasTakeWhileLayer then
                                    // L12: Three-layer: inner (with _cf window), middle (WHERE _cf filter), outer (json_group_array).
                                    let (_, isTW) = takeWhileInfo.Value
                                    let cfFilter =
                                        if isTW then
                                            SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                                        else
                                            SqlExpr.Binary(SqlExpr.Column(Some ordAlias, "_cf"), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
                                    let middleCore =
                                        mkSubCore
                                            [{ Alias = Some "v"; Expr = SqlExpr.Column(Some ordAlias, "v") }]
                                            (Some(DerivedTable(innerSelect, ordAlias)))
                                            (Some cfFilter)
                                    let middleSelect = { Ctes = []; Body = SingleSelect middleCore }
                                    let midAlias = sprintf "_tw%d" aliasId
                                    let outerGroupArray = SqlExpr.FunctionCall("json_group_array", [SqlExpr.Column(Some midAlias, "v")])
                                    let outerCore =
                                        mkSubCore
                                            [{ Alias = None; Expr = outerGroupArray }]
                                            (Some(DerivedTable(middleSelect, midAlias)))
                                            None
                                    let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery outerSelect)
                                    true
                                else
                                    // L3/L4: Two-layer (no TakeWhile).
                                    let outerGroupArray = SqlExpr.FunctionCall("json_group_array", [SqlExpr.Column(Some ordAlias, "v")])
                                    let outerCore =
                                        mkSubCore
                                            [{ Alias = None; Expr = outerGroupArray }]
                                            (Some(DerivedTable(innerSelect, ordAlias)))
                                            None
                                    let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery outerSelect)
                                    true
                        | ValueNone ->
                            raise (NotSupportedException(
                                "Error: Cannot extract projection lambda for relation-backed DBRefMany.Select.\nReason: The projection is not a simple lambda expression.\nFix: Use a simple lambda (e.g., x => x.Name) or move the operation after AsEnumerable()."))
                    | ValueNone -> false
                | ValueNone -> false

        // L7: Distinct on DBRefMany — only meaningful after Select projection.
        | :? MethodCallExpression as mce when mce.Method.Name = "Distinct" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            if isNull sourceExpr then false
            else
                // Distinct must follow Select on DBRefMany. Check if source is a Select MCE.
                match sourceExpr with
                | :? MethodCallExpression as selectMce when selectMce.Method.Name = "Select" ->
                    // Delegate to the Select handler with distinct flag.
                    // Re-parse as if it were Select but with distinct output.
                    let selectSourceExpr, projExpr =
                        if not (isNull selectMce.Object) then
                            selectMce.Object, (if selectMce.Arguments.Count >= 1 then ValueSome selectMce.Arguments.[0] else ValueNone)
                        elif selectMce.Arguments.Count >= 2 then
                            selectMce.Arguments.[0], ValueSome selectMce.Arguments.[1]
                        else
                            null, ValueNone
                    if isNull selectSourceExpr then false
                    else
                        let unwrappedSelectSource = unwrapConvert selectSourceExpr
                        // Unified peeling: Take/Skip → OrderBy → Where → owner ref.
                        let sourceAfterTakeSkip, limitExprD, offsetExprD =
                            match tryPeelTakeSkipFromSource unwrappedSelectSource with
                            | ValueSome (inner, limit, offset) -> unwrapConvert inner, limit, offset
                            | ValueNone -> unwrappedSelectSource, None, None
                        let sourceAfterOrder, sortKeysD =
                            match tryPeelOrderByFromSource sourceAfterTakeSkip with
                            | ValueSome (inner, keys) -> unwrapConvert inner, keys
                            | ValueNone -> sourceAfterTakeSkip, []
                        let sourceAfterWhere, wherePredicatesD =
                            if isDBRefManyType sourceAfterOrder.Type then sourceAfterOrder, []
                            else
                                match tryPeelWhereFromDBRefMany sourceAfterOrder with
                                | ValueSome (dbRefSrc, preds) -> dbRefSrc, preds
                                | ValueNone -> sourceAfterOrder, []

                        match tryGetDBRefManyOwnerRef qb sourceAfterWhere with
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

                                    let wherePredDus =
                                        if wherePredicatesD.IsEmpty then []
                                        else buildFilteredPredicateDus qb tgtAlias wherePredicatesD

                                    let subQb = qb.ForSubquery(tgtAlias, projLambda)
                                    let projectedDu = visitDu projLambda.Body subQb

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
                                        wherePredDus |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) ownerWhere

                                    let hasTakeSkipD = limitExprD.IsSome || offsetExprD.IsSome
                                    if sortKeysD.IsEmpty && not hasTakeSkipD then
                                        // Single-layer: json_group_array(DISTINCT proj).
                                        let distinctGroupArray = SqlExpr.AggregateCall(AggregateKind.JsonGroupArray, Some projectedDu, true, None)
                                        let core =
                                            { mkSubCore
                                                [{ Alias = None; Expr = distinctGroupArray }]
                                                (Some(BaseTable(linkTable, Some lnkAlias)))
                                                (Some fullWhere) with
                                                Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)] }
                                        let subSelect = { Ctes = []; Body = SingleSelect core }
                                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery subSelect)
                                        true
                                    else
                                        // With ordering/pagination: two-layer inner SELECT DISTINCT, outer json_group_array.
                                        let sortKeyDus = if sortKeysD.IsEmpty then [] else buildSortKeyDus qb tgtAlias sortKeysD
                                        let limitDu, offsetDu = buildLimitOffset qb limitExprD offsetExprD
                                        let innerCore =
                                            { Distinct = true
                                              Projections = ProjectionSetOps.ofList [{ Alias = Some "v"; Expr = projectedDu }]
                                              Source = Some(BaseTable(linkTable, Some lnkAlias))
                                              Joins = [ConditionedJoin(Inner, BaseTable(targetTable, Some tgtAlias), joinOn)]
                                              Where = Some fullWhere
                                              GroupBy = []; Having = None
                                              OrderBy = sortKeyDus
                                              Limit = limitDu
                                              Offset = offsetDu }
                                        let innerSelect = { Ctes = []; Body = SingleSelect innerCore }
                                        let ordAlias = sprintf "_ord%d" aliasId
                                        let outerGroupArray = SqlExpr.FunctionCall("json_group_array", [SqlExpr.Column(Some ordAlias, "v")])
                                        let outerCore =
                                            mkSubCore
                                                [{ Alias = None; Expr = outerGroupArray }]
                                                (Some(DerivedTable(innerSelect, ordAlias)))
                                                None
                                        let outerSelect = { Ctes = []; Body = SingleSelect outerCore }
                                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.ScalarSubquery outerSelect)
                                        true
                                | ValueNone -> false
                            | ValueNone -> false
                        | ValueNone -> false
                | _ -> false

        // L7: Contains on DBRefMany — Id-based membership check.
        | :? MethodCallExpression as mce when mce.Method.Name = "Contains" ->
            let sourceArg, valueArg = extractSourceAndPredicate mce
            match sourceArg with
            | ValueSome sourceExpr ->
                match tryGetDBRefManyOwnerRef qb sourceExpr with
                | ValueSome ownerRef ->
                    match valueArg with
                    | ValueSome containsValueExpr ->
                        // Contains(entity) → EXISTS(SELECT 1 FROM link WHERE ownerId AND targetId = entity.Id)
                        let propName = ownerRef.PropertyExpr.Member.Name
                        let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
                        let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
                        let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
                        let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"

                        // Extract the entity's Id: visit the expression to get a DU that resolves to the Id.
                        let entityIdDu =
                            let idProp = containsValueExpr.Type.GetProperty("Id")
                            if isNull idProp then
                                raise (NotSupportedException(
                                    "Error: Contains on DBRefMany requires an entity with an Id property.\nFix: Pass an entity instance with a resolvable Id."))
                            let idAccess = System.Linq.Expressions.Expression.MakeMemberAccess(containsValueExpr, idProp)
                            visitDu idAccess qb

                        let ownerWhere =
                            SqlExpr.Binary(
                                SqlExpr.Column(None, ownerColumn),
                                BinaryOperator.Eq,
                                SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id"))
                        let targetWhere =
                            SqlExpr.Binary(
                                SqlExpr.Column(None, targetColumn),
                                BinaryOperator.Eq,
                                entityIdDu)
                        let fullWhere = SqlExpr.Binary(ownerWhere, BinaryOperator.And, targetWhere)
                        let oneProj = [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }]
                        let core = mkSubCore oneProj (Some(BaseTable(linkTable, None))) (Some fullWhere)
                        let subSelect = { Ctes = []; Body = SingleSelect core }
                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.Exists subSelect)
                        true
                    | ValueNone -> false
                | ValueNone -> false
            | ValueNone -> false

        // L11: ToList/ToArray inside expression trees — identity passthrough.
        // Strip ToList/ToArray and translate the inner source directly.
        | :? MethodCallExpression as mce when mce.Method.Name = "ToList" || mce.Method.Name = "ToArray" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            if isNull sourceExpr then false
            else
                // Visit the inner source — the pre-expression handler will catch it.
                let innerDu = visitDu sourceExpr qb
                qb.DuHandlerResult.Value <- ValueSome innerDu
                true

        // L1 reject: bare DBRefMany.Where(pred) materialization.
        | :? MethodCallExpression as mce when mce.Method.Name = "Where" ->
            match tryPeelWhereFromDBRefMany exp with
            | ValueSome _ ->
                raise (NotSupportedException(filteredWhereUnsupportedTerminalMessage))
            | ValueNone -> false

        // L4: reject set operators after DBRefMany chain — Take/Skip/OrderBy now admitted.
        | :? MethodCallExpression as mce
            when mce.Method.Name = "SelectMany" ->
            let sourceExpr =
                if not (isNull mce.Object) then mce.Object
                elif mce.Arguments.Count > 0 then mce.Arguments.[0]
                else null
            if isNull sourceExpr then false
            else
                // Peel OrderBy first (it may sit between Where and the current operator).
                let sourceAfterOrder =
                    match tryPeelOrderByFromSource sourceExpr with
                    | ValueSome (inner, _) -> inner
                    | ValueNone -> sourceExpr
                match tryPeelWhereFromDBRefMany sourceAfterOrder with
                | ValueSome _ ->
                    raise (NotSupportedException(filteredWhereUnsupportedTerminalMessage))
                | ValueNone -> false

        | _ -> false

    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefExpression))
    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefManyExpression))

    /// Module initialization sentinel — accessing this value forces execution of module do-bindings.
    let internal handlerCount = preExpressionHandler.Count
