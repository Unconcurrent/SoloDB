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

/// Shared peeler functions, builder helpers, and constants for DBRefMany query translation.
module internal QueryTranslatorVisitDbRefPeelers =
    let internal nestedDbRefManyNotSupportedMessage =
        sprintf "Error: Deeply nested DBRefMany query exceeds maximum depth (%d).\nReason: Queries with more than %d levels of nested DBRefMany relations are not supported.\nFix: Reduce nesting depth or move deeper traversal after AsEnumerable()." maxRelationDepth maxRelationDepth

    [<Literal>]
    let internal filteredWhereUnsupportedTerminalMessage =
        "Error: DBRefMany operator chain is not supported with this terminal operator.\nReason: Only .Any(), .Count(), .LongCount(), .Select(), .All(), and ordering operators (.OrderBy/.ThenBy) are admitted as composable prefixes.\nFix: Use one of the admitted operators, or move the query after AsEnumerable()."

    [<Literal>]
    let internal filteredWhereOuterCaptureMessage =
        "Error: DBRefMany.Where() predicate cannot capture the outer owner entity.\nReason: The inner predicate binds only to the relation target alias.\nFix: Rewrite the predicate to depend only on the relation target, or move it after AsEnumerable()."

    // Unique alias counter for nested EXISTS subqueries (prevents alias collision).
    let mutable internal subqueryAliasCounter = 0L

    /// Count the maximum DBRefMany nesting depth in an expression tree.
    /// depth 0 = no nested DBRefMany, depth 1 = one nested level (admitted), depth 2+ = rejected.
    let internal countDbRefManyDepth (expr: Expression) : int =
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

    let internal containsRelationNavigation (expr: Expression) =
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

    let internal containsOuterCapture (lambda: LambdaExpression) =
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
                // Nested lambdas are out of scope here.
                not (Object.ReferenceEquals(innerLambda, lambda)) || visitExpr innerLambda.Body
            | _ -> false
        visitExpr lambda.Body

    /// preExpressionHandler for DBRef member access translation.
    /// Compute the link table name for a DBRefMany property.
    /// Convention: SoloDBRelLink_{SourceTable}_{PropertyName}
    // Strict metadata resolution — no silent fallback for relation-backed paths.
    let internal dbRefManyLinkTable (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationLink(ownerTable, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ ->
            raise (InvalidOperationException(
                $"Error: Relation metadata not found for property {propName} on collection {ownerTable}.\nReason: The link table for this relation could not be resolved.\nFix: Ensure the collection is initialized with Insert or GetCollection before querying, or call AsEnumerable() before accessing this relation."))

    let internal dbRefManyOwnerUsesSource (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationOwnerUsesSource(ownerTable, propName) with
        | Some value -> value
        | None ->
            raise (InvalidOperationException(
                $"Error: Relation metadata not found for property {propName} on collection {ownerTable}.\nReason: The owner-source direction for this relation could not be determined.\nFix: Ensure the collection is initialized with Insert or GetCollection before querying, or call AsEnumerable() before accessing this relation."))

    type internal DBRefManyOwnerRef = {
        OwnerCollection: string
        OwnerAliasSql: string
        OwnerIdExpr: SqlExpr option
        PropertyExpr: MemberExpression
    }

    let internal ownerIdExpr (ownerRef: DBRefManyOwnerRef) =
        match ownerRef.OwnerIdExpr with
        | Some expr -> expr
        | None -> SqlExpr.Column(Some ownerRef.OwnerAliasSql, "Id")

    /// Synthesize a MemberExpression for DBRef<T>.Value from an Invoke arg that is a DBRef property.
    /// F# expression trees emit Invoke(closure, dbrefPropExpr) instead of MemberAccess(Value, dbrefPropExpr).
    let internal tryMakeValueMemberFromInvoke (invokeExpr: MethodCallExpression) : MemberExpression voption =
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
    let internal tryGetDBRefManyOwnerRef (qb: QueryBuilder) (arg: Expression) : DBRefManyOwnerRef voption =
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
                    OwnerIdExpr = None
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
                ValueSome { OwnerCollection = joinedOwnerCollection; OwnerAliasSql = alias; OwnerIdExpr = None; PropertyExpr = me }
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
                    ValueSome { OwnerCollection = joinedOwnerCollection; OwnerAliasSql = alias; OwnerIdExpr = None; PropertyExpr = me }
                | ValueNone -> ValueNone
            | :? MethodCallExpression as ownerCall ->
                let ownerCollection =
                    qb.SourceContext.ResolveCollectionForType(Utils.typeIdentityKey ownerCall.Type, formatName ownerCall.Type.Name)
                let ownerExprDu = visitDu ownerCall qb
                let ownerIdExprDu =
                    SqlExpr.FunctionCall("jsonb_extract", [
                        ownerExprDu
                        SqlExpr.Literal(SqlLiteral.String "$.Id")
                    ])
                ValueSome {
                    OwnerCollection = ownerCollection
                    OwnerAliasSql = ""
                    OwnerIdExpr = Some ownerIdExprDu
                    PropertyExpr = me
                }
            | _ -> ValueNone
        | _ -> ValueNone

    /// Extract source expression and optional predicate from Any/All MethodCallExpression,
    /// handling both instance-call (mce.Object.Any(pred)) and extension-call (Enumerable.Any(source, pred)) forms.
    let internal extractSourceAndPredicate (mce: MethodCallExpression) =
        if not (isNull mce.Object) then
            let pred = if mce.Arguments.Count >= 1 then ValueSome mce.Arguments.[0] else ValueNone
            ValueSome mce.Object, pred
        elif mce.Arguments.Count >= 1 then
            let pred = if mce.Arguments.Count >= 2 then ValueSome mce.Arguments.[1] else ValueNone
            ValueSome mce.Arguments.[0], pred
        else
            ValueNone, ValueNone

    /// Helper to build a simple SelectCore for link-table subqueries.
    let internal mkSubCore projections source where =
        { Distinct = false; Projections = ProjectionSetOps.ofList projections; Source = source
          Joins = []; Where = where; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    /// Helper: build a simple link-table subquery (no join to target).
    /// Pattern: SELECT <proj> FROM "linkTable" WHERE ownerColumn = ownerAlias.Id
    let internal linkSubquery (linkTable: string) (ownerColumn: string) (ownerAlias: string) (proj: Projection list) =
        let where =
            SqlExpr.Binary(
                SqlExpr.Column(None, ownerColumn),
                BinaryOperator.Eq,
                SqlExpr.Column(Some ownerAlias, "Id"))
        { Ctes = []; Body = SingleSelect(mkSubCore proj (Some(BaseTable(linkTable, None))) (Some where)) }

    /// Build a correlated EXISTS or NOT EXISTS subquery for a DBRefMany quantifier with a predicate.
    let internal buildFilteredPredicateDus (qb: QueryBuilder) (tgtAlias: string) (targetTable: string) (predicateExprs: Expression list) =
        predicateExprs
        |> List.map (fun predExpr ->
            match tryExtractLambdaExpression predExpr with
            | ValueSome predLambda ->
                if countDbRefManyDepth predLambda.Body >= maxRelationDepth then
                    raise (NotSupportedException(nestedDbRefManyNotSupportedMessage))
                if containsOuterCapture predLambda then
                    raise (NotSupportedException(filteredWhereOuterCaptureMessage))
                let subQb = qb.ForSubquery(tgtAlias, predLambda, subqueryRootTable = targetTable)
                visitDu predLambda.Body subQb
            | ValueNone ->
                raise (NotSupportedException(
                    "Error: Cannot extract predicate lambda for DBRefMany.Where.\nReason: The predicate is not a simple lambda expression.\nFix: Use a simple lambda or move the operation after AsEnumerable().")))

    /// Peel .Where(pred) calls from a DBRefMany source expression.
    /// Returns (innerDBRefManyExpr, predicateExprs) where predicateExprs is a list of
    /// Where predicates in application order (chained .Where().Where() produces multiple).
