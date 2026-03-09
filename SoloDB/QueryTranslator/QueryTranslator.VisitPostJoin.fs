namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Reflection
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitPost
open SqlDu.Engine.C1.Spec
open DBRefTypeHelpers

module internal QueryTranslatorVisitPostJoin =
    /// Compute a stable path key for a member expression chain from root parameter.
    let rec private computePathKey (expr: Expression) : string =
        match expr with
        | :? MemberExpression as me when not (isNull me.Expression) ->
            let parent = computePathKey me.Expression
            if parent = "" then me.Member.Name else parent + "." + me.Member.Name
        | :? ParameterExpression -> ""
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> computePathKey ue.Operand
        | _ ->
            raise (NotSupportedException(
                sprintf "Unsupported relation path expression node: %A. Rewrite the query to use direct member access from the query root." expr.NodeType))

    // Count DBRef chain depth by relation hops, not by total JOIN count.
    // This avoids false positives when a predicate touches many independent DBRefs.
    let rec private dbRefChainDepth (expr: Expression) =
        match unwrapConvert expr with
        | :? MemberExpression as me when isDBRefType me.Type ->
            1 + dbRefOwnerDepth me.Expression
        | _ ->
            0

    and private dbRefOwnerDepth (expr: Expression) =
        match unwrapConvert expr with
        | :? MemberExpression as me when isDBRefValueBoundary me ->
            dbRefChainDepth (unwrapConvert me.Expression)
        | _ ->
            0

    /// Given a MemberExpression for a DBRef property (e.g., o.Customer or o.Author.Value.Publisher),
    /// resolve the source prefix and property name for JSON extraction.
    let rec internal resolveDBRefPropertyLocation (qb: QueryBuilder) (dbrefPropExpr: MemberExpression) : struct(string * string) =
        match dbrefPropExpr.Expression with
        | :? ParameterExpression ->
            struct(qb.TableNameDot, dbrefPropExpr.Member.Name)
        | :? MemberExpression as parentMe when isDBRefValueBoundary parentMe ->
            let parentAlias = ensureDBRefJoin qb parentMe
            struct(parentAlias + ".", dbrefPropExpr.Member.Name)
        | _ ->
            let fullPath = computePathKey dbrefPropExpr
            struct(qb.TableNameDot, fullPath)

    and internal resolveDBRefOwnerCollectionAndProperty (qb: QueryBuilder) (dbrefPropExpr: MemberExpression) : struct(string * string) =
        match dbrefPropExpr.Expression with
        | :? ParameterExpression ->
            struct(qb.SourceContext.RootTable, dbrefPropExpr.Member.Name)
        | :? MemberExpression as parentMe when isDBRefValueBoundary parentMe ->
            let parentDbRefExpr = unwrapConvert parentMe.Expression
            let parentPath = computePathKey parentDbRefExpr
            ensureDBRefJoin qb parentMe |> ignore
            let ownerCollection =
                match qb.SourceContext.FindJoin(parentPath) with
                | Some join -> join.TargetTable
                | None -> qb.SourceContext.RootTable
            struct(ownerCollection, dbrefPropExpr.Member.Name)
        | _ ->
            struct(qb.SourceContext.RootTable, dbrefPropExpr.Member.Name)

    and internal resolveTargetCollectionForRelation (ctx: QueryContext) (ownerCollection: string) (propertyName: string) (targetType: Type) =
        let defaultTable = formatName targetType.Name
        match ctx.TryResolveRelationTarget(ownerCollection, propertyName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> ctx.ResolveCollectionForType(Utils.typeIdentityKey targetType, defaultTable)

    /// Ensure a LEFT JOIN exists for a DBRef<T>.Value access. Returns the alias.
    and internal ensureDBRefJoin (qb: QueryBuilder) (valueMemberExpr: MemberExpression) : string =
        let dbrefExpr = unwrapConvert valueMemberExpr.Expression
        let targetType = valueMemberExpr.Type
        let ctx = qb.SourceContext

        if dbRefChainDepth dbrefExpr > 10 then
            raise (NotSupportedException(
                "Error: Relation chain is too deep or circular (depth > 10).\nReason: The query exceeds the supported relation traversal depth.\nFix: Reduce relation depth or split the query into multiple steps."))

        let pathKey = computePathKey dbrefExpr

        if ctx.ExcludedPaths.Contains(pathKey) then
            raise (InvalidOperationException(
                sprintf "Error: Cannot access excluded relation property '%s.Value'.\nReason: The property was excluded from the query.\nFix: Remove the Exclude call or use .Id instead." pathKey))

        match ctx.FindJoin(pathKey) with
        | Some existing -> existing.TargetAlias
        | None ->
            let alias = ctx.NextAlias()

            let struct(sourcePrefix, propName) =
                match dbrefExpr with
                | :? MemberExpression as me -> resolveDBRefPropertyLocation qb me
                | _ -> struct(qb.TableNameDot, computePathKey dbrefExpr)

            let struct(ownerCollection, relationPropertyName) =
                match dbrefExpr with
                | :? MemberExpression as me -> resolveDBRefOwnerCollectionAndProperty qb me
                | _ -> struct(qb.SourceContext.RootTable, propName)

            let targetTable = resolveTargetCollectionForRelation ctx ownerCollection relationPropertyName targetType

            let sourceAlias =
                if String.IsNullOrEmpty sourcePrefix then None
                else Some(sourcePrefix.TrimEnd('.'))
            ctx.Joins.Add({
                TargetAlias = alias
                TargetTable = targetTable
                JoinKind = "LEFT JOIN"
                OnSourceAlias = sourceAlias
                OnPropertyName = propName
                PropertyPath = pathKey
            })
            alias
