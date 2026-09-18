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
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitPostJoin
open DBRefTypeHelpers
open SoloDatabase.DBRefManyDescriptor

/// Relation-source resolution and validation for DBRefMany query translation.
module internal QueryTranslatorVisitDbRefPeelers =
    let internal nestedDbRefManyNotSupportedMessage =
        sprintf "Error: Deeply nested DBRefMany query exceeds maximum depth (%d).\nReason: Queries with more than %d levels of nested DBRefMany relations are not supported.\nFix: Reduce nesting depth or move deeper traversal after AsEnumerable()." maxRelationDepth maxRelationDepth

    [<Literal>]
    let internal filteredWhereUnsupportedTerminalMessage =
        "Error: DBRefMany operator chain is not supported with this terminal operator.\nReason: Only .Any(), .Count(), .LongCount(), .Select(), .All(), and ordering operators (.OrderBy/.ThenBy) are admitted as composable prefixes.\nFix: Use one of the admitted operators, or move the query after AsEnumerable()."

    /// Count the maximum DBRefMany nesting depth in an expression tree.
    /// Each relation call contributes one level to the configured nesting limit.
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
                    match qb.SourceAlias with
                    | Some alias -> alias
                    | None -> "\"" + qb.SourceContext.RootTable + "\""
                // Detect subquery context: if sourceAlias differs from root alias,
                // resolve owner collection from parameter type (e.g. ChainMid in inner Any).
                let rootAlias = "\"" + qb.SourceContext.RootTable + "\""
                let ownerCollection =
                    if StringComparer.Ordinal.Equals(sourceAlias, rootAlias) then
                        qb.SourceContext.RootTable
                    else
                        qb.SourceContext.ResolveCollectionForType(UtilsReflection.typeIdentityKey pe.Type, formatName pe.Type.Name)
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
                    qb.SourceContext.ResolveCollectionForType(UtilsReflection.typeIdentityKey ownerCall.Type, formatName ownerCall.Type.Name)
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
