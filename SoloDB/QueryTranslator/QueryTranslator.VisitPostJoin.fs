namespace SoloDatabase

open System
open System.Linq.Expressions
open System.Reflection
open Utils
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitPost

module internal QueryTranslatorVisitPostJoin =
    /// Compute a stable path key for a member expression chain from root parameter.
    let rec private computePathKey (expr: Expression) : string =
        match expr with
        | :? MemberExpression as me when not (isNull me.Expression) ->
            let parent = computePathKey me.Expression
            if parent = "" then me.Member.Name else parent + "." + me.Member.Name
        | :? ParameterExpression -> ""
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> computePathKey ue.Operand
        | _ -> ""

    /// Unwrap Convert nodes to get the actual expression.
    let internal unwrapConvert (expr: Expression) =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> ue.Operand
        | e -> e

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
        | :? MemberExpression as me when me.Member.Name = "Value" && isDBRefType (unwrapConvert me.Expression).Type ->
            dbRefChainDepth (unwrapConvert me.Expression)
        | _ ->
            0

    /// Given a MemberExpression for a DBRef property (e.g., o.Customer or o.Author.Value.Publisher),
    /// resolve the source prefix and property name for JSON extraction.
    let rec internal resolveDBRefPropertyLocation (qb: QueryBuilder) (dbrefPropExpr: MemberExpression) : struct(string * string) =
        match dbrefPropExpr.Expression with
        | :? ParameterExpression ->
            struct(qb.TableNameDot, dbrefPropExpr.Member.Name)
        | :? MemberExpression as parentMe when parentMe.Member.Name = "Value" && isDBRefType (unwrapConvert parentMe.Expression).Type ->
            let parentAlias = ensureDBRefJoin qb parentMe
            struct(parentAlias + ".", dbrefPropExpr.Member.Name)
        | _ ->
            let fullPath = computePathKey dbrefPropExpr
            struct(qb.TableNameDot, fullPath)

    and private resolveDBRefOwnerCollectionAndProperty (qb: QueryBuilder) (dbrefPropExpr: MemberExpression) : struct(string * string) =
        match dbrefPropExpr.Expression with
        | :? ParameterExpression ->
            struct(qb.SourceContext.RootTable, dbrefPropExpr.Member.Name)
        | :? MemberExpression as parentMe when parentMe.Member.Name = "Value" && isDBRefType (unwrapConvert parentMe.Expression).Type ->
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
            raise (NotSupportedException("Circular or excessively deep relation chain (depth > 10)"))

        let pathKey = computePathKey dbrefExpr

        if ctx.ExcludedPaths.Contains(pathKey) then
            raise (InvalidOperationException(
                sprintf "Cannot access excluded relation property '%s.Value'. Remove the Exclude call or use .Id instead." pathKey))

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

            let onCondition = sprintf "%s.Id = jsonb_extract(%sValue, '$.%s')" alias sourcePrefix propName
            ctx.Joins.Add({
                TargetAlias = alias
                TargetTable = targetTable
                JoinKind = "LEFT JOIN"
                OnCondition = onCondition
                PropertyPath = pathKey
            })
            alias
