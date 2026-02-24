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
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitPostJoin

module internal QueryTranslatorVisitDbRef =
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

            // Case 1: Direct member on DBRef<T> — o.Ref.Id, o.Ref.HasValue
            if isDBRefType innerExpr.Type then
                match topMe.Member.Name with
                | "Id" ->
                    match innerExpr with
                    | :? MemberExpression as dbrefPropExpr ->
                        let struct(prefix, prop) = resolveDBRefPropertyLocation qb dbrefPropExpr
                        qb.AppendRaw (sprintf "jsonb_extract(%sValue, '$.%s')" prefix prop)
                        true
                    | _ -> false
                | "HasValue" ->
                    match innerExpr with
                    | :? MemberExpression as dbrefPropExpr ->
                        let struct(prefix, prop) = resolveDBRefPropertyLocation qb dbrefPropExpr
                        let extract = sprintf "jsonb_extract(%sValue, '$.%s')" prefix prop
                        qb.AppendRaw (sprintf "(%s IS NOT NULL AND %s <> 0)" extract extract)
                        true
                    | _ -> false
                | _ -> false

            // Case 2: Property through DBRef<T>.Value — o.Ref.Value.Name, o.Ref.Value.Address.City
            else
                let rec findValueBoundary (expr: Expression) (above: string list) =
                    match expr with
                    | :? MemberExpression as me when not (isNull me.Expression) && isDBRefType (unwrapConvert me.Expression).Type && me.Member.Name = "Value" ->
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
                    let targetPath = String.concat "." propParts
                    qb.AppendRaw (sprintf "jsonb_extract(%s.Value, '$.%s')" alias targetPath)
                    true
                | ValueNone -> false
        | _ -> false

    /// Compute the link table name for a DBRefMany property.
    /// Convention: SoloDBRelLink_{SourceTable}_{PropertyName}
    let private dbRefManyLinkTable (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationLink(ownerTable, propName) with
        | Some mapped when not (String.IsNullOrWhiteSpace mapped) -> formatName mapped
        | _ -> sprintf "SoloDBRelLink_%s_%s" ownerTable propName

    let private dbRefManyOwnerUsesSource (ctx: QueryContext) (ownerTable: string) (propName: string) =
        match ctx.TryResolveRelationOwnerUsesSource(ownerTable, propName) with
        | Some value -> value
        | None -> true

    type private DBRefManyOwnerRef = {
        OwnerCollection: string
        OwnerAliasSql: string
        PropertyExpr: MemberExpression
    }

    /// Extract DBRefMany source with owner resolution for both root and nested (through DBRef.Value) paths.
    let private tryGetDBRefManyOwnerRef (qb: QueryBuilder) (arg: Expression) : DBRefManyOwnerRef voption =
        let arg = unwrapConvert arg
        match arg with
        | :? MemberExpression as me when not (isNull me.Expression) && isDBRefManyType me.Type ->
            match unwrapConvert me.Expression with
            | :? ParameterExpression ->
                let sourceAlias =
                    if String.IsNullOrEmpty qb.TableNameDot then "\"" + qb.SourceContext.RootTable + "\""
                    else qb.TableNameDot.TrimEnd('.')
                ValueSome {
                    OwnerCollection = qb.SourceContext.RootTable
                    OwnerAliasSql = sourceAlias
                    PropertyExpr = me
                }
            | :? MemberExpression as valueMe when valueMe.Member.Name = "Value" && isDBRefType (unwrapConvert valueMe.Expression).Type ->
                // Nested: o.Ref.Value.Items — resolve via DBRef JOIN chain.
                let alias = ensureDBRefJoin qb valueMe
                let parentDbRefExpr = unwrapConvert valueMe.Expression :?> MemberExpression
                let struct(_, _) = resolveDBRefOwnerCollectionAndProperty qb parentDbRefExpr
                let joinedOwnerCollection =
                    match qb.SourceContext.TryFindJoinByAlias(alias) with
                    | Some join -> join.TargetTable
                    | None -> qb.SourceContext.RootTable
                ValueSome { OwnerCollection = joinedOwnerCollection; OwnerAliasSql = alias; PropertyExpr = me }
            | _ -> ValueNone
        | _ -> ValueNone

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
                    qb.AppendRaw (sprintf "(SELECT COUNT(*) FROM \"%s\" WHERE %s = %s.Id)" linkTable ownerColumn ownerRef.OwnerAliasSql)
                    true
                | ValueNone -> false
            else false

        // Case 2/3: Any() / Any(pred) over DBRefMany in either extension-call or instance-call form.
        | :? MethodCallExpression as mce when mce.Method.Name = "Any" ->
            let sourceArg, predArg =
                if not (isNull mce.Object) then
                    let pred =
                        if mce.Arguments.Count >= 1 then ValueSome mce.Arguments.[0]
                        else ValueNone
                    ValueSome mce.Object, pred
                elif mce.Arguments.Count >= 1 then
                    let pred =
                        if mce.Arguments.Count >= 2 then ValueSome mce.Arguments.[1]
                        else ValueNone
                    ValueSome mce.Arguments.[0], pred
                else
                    ValueNone, ValueNone

            match sourceArg with
            | ValueSome sourceExpr ->
                match tryGetDBRefManyOwnerRef qb sourceExpr with
                | ValueSome ownerRef ->
                    let propName = ownerRef.PropertyExpr.Member.Name
                    let linkTable = dbRefManyLinkTable qb.SourceContext ownerRef.OwnerCollection propName
                    let ownerUsesSource = dbRefManyOwnerUsesSource qb.SourceContext ownerRef.OwnerCollection propName
                    let ownerColumn = if ownerUsesSource then "SourceId" else "TargetId"
                    let targetColumn = if ownerUsesSource then "TargetId" else "SourceId"

                    match predArg with
                    | ValueNone ->
                        // Any() without predicate.
                        qb.AppendRaw (sprintf "EXISTS(SELECT 1 FROM \"%s\" WHERE %s = %s.Id)" linkTable ownerColumn ownerRef.OwnerAliasSql)
                        true
                    | ValueSome predicateExpr ->
                        // Any(pred) with predicate — correlated EXISTS with INNER JOIN to target table.
                        match tryExtractLambdaExpression predicateExpr with
                        | ValueSome predExpr ->
                            let targetType = ownerRef.PropertyExpr.Type.GetGenericArguments().[0]
                            let targetTable = resolveTargetCollectionForRelation qb.SourceContext ownerRef.OwnerCollection propName targetType
                            let tgtAlias = "_tgt"
                            let lnkAlias = "_lnk"

                            qb.AppendRaw (sprintf "EXISTS(SELECT 1 FROM \"%s\" AS %s INNER JOIN \"%s\" AS %s ON %s.Id = %s.%s WHERE %s.%s = %s.Id AND "
                                linkTable lnkAlias targetTable tgtAlias tgtAlias lnkAlias targetColumn lnkAlias ownerColumn ownerRef.OwnerAliasSql)

                            // Translate predicate body with target table as context
                            let subQb = qb.ForSubquery(tgtAlias, predExpr)
                            visit predExpr.Body subQb

                            qb.AppendRaw ")"
                            true
                        | ValueNone ->
                            false
                | ValueNone ->
                    false
            | ValueNone -> false

        | _ -> false

    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefExpression))
    do preExpressionHandler.Add(Func<QueryBuilder, Expression, bool>(handleDBRefManyExpression))

    /// Module initialization sentinel — accessing this value forces execution of module do-bindings.
    let internal handlerCount = preExpressionHandler.Count
