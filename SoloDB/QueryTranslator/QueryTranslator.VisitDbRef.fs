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
        "Error: Nested DBRefMany query is not supported.\nReason: Relation-backed DBRefMany predicates cannot contain inner DBRefMany traversals.\nFix: Rewrite the predicate to a single DBRefMany level, or move nested traversal after AsEnumerable()."

    let private predicateContainsDbRefManyTraversal (expr: Expression) =
        let rec visitExpr (e: Expression) =
            let e = unwrapConvert e
            match e with
            | null -> false
            | :? MemberExpression as me ->
                isDBRefManyType me.Type
                || (not (isNull me.Expression) && visitExpr me.Expression)
            | :? MethodCallExpression as mc ->
                let sourceIsDbRefMany =
                    if not (isNull mc.Object) then
                        isDBRefManyType (unwrapConvert mc.Object).Type
                    elif mc.Arguments.Count > 0 then
                        isDBRefManyType (unwrapConvert mc.Arguments.[0]).Type
                    else
                        false
                sourceIsDbRefMany
                || (not (isNull mc.Object) && visitExpr mc.Object)
                || (mc.Arguments |> Seq.exists visitExpr)
            | :? BinaryExpression as be ->
                visitExpr be.Left || visitExpr be.Right
            | :? UnaryExpression as ue ->
                visitExpr ue.Operand
            | :? ConditionalExpression as ce ->
                visitExpr ce.Test || visitExpr ce.IfTrue || visitExpr ce.IfFalse
            | :? InvocationExpression as ie ->
                visitExpr ie.Expression || (ie.Arguments |> Seq.exists visitExpr)
            | :? LambdaExpression as le ->
                visitExpr le.Body
            | :? NewExpression as ne ->
                ne.Arguments |> Seq.exists visitExpr
            | :? NewArrayExpression as nae ->
                nae.Expressions |> Seq.exists visitExpr
            | _ -> false
        visitExpr expr

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
                        qb.DuHandlerResult.Value <- ValueSome(SqlExpr.JsonExtractExpr(prefixToAlias prefix, "Value", JsonPath [prop]))
                        true
                    | _ -> false
                | "HasValue" ->
                    match innerExpr with
                    | :? MemberExpression as dbrefPropExpr ->
                        let struct(prefix, prop) = resolveDBRefPropertyLocation qb dbrefPropExpr
                        let extract = SqlExpr.JsonExtractExpr(prefixToAlias prefix, "Value", JsonPath [prop])
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
                    qb.DuHandlerResult.Value <- ValueSome(SqlExpr.JsonExtractExpr(Some alias, "Value", JsonPath propParts))
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
        { Distinct = false; Projections = projections; Source = source
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
            if predicateContainsDbRefManyTraversal predExpr.Body then
                raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))

            let propName = ownerRef.PropertyExpr.Member.Name
            let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
            let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
            let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
            let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"
            let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
            let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
            let tgtAlias = "_tgt"
            let lnkAlias = "_lnk"

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
                    Joins = [{ Kind = Inner; Source = BaseTable(targetTable, Some tgtAlias); On = Some joinOn }] }
            let subSelect = { Ctes = []; Body = SingleSelect core }
            let existsExpr = SqlExpr.Exists subSelect
            if isAll then SqlExpr.Unary(UnaryOperator.Not, existsExpr)
            else existsExpr
        | ValueNone ->
            raise (NotSupportedException(
                sprintf "Error: Cannot extract predicate lambda for relation-backed DBRefMany.%s.\nReason: The predicate is not a simple lambda expression.\nFix: Use a simple lambda (e.g., x => ...) or move the operation after AsEnumerable()." methodName))

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

        // Case 2/3: Any() / Any(pred) over DBRefMany in either extension-call or instance-call form.
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
                    false
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
                    false
            | ValueNone -> false

        | _ -> false

    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefExpression))
    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefManyExpression))

    /// Module initialization sentinel — accessing this value forces execution of module do-bindings.
    let internal handlerCount = preExpressionHandler.Count
