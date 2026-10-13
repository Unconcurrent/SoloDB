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
        let depth = maxRelationDepth.ToString(Globalization.CultureInfo.InvariantCulture)
        "Error: Deeply nested DBRefMany query exceeds maximum depth (" + depth + ").\nReason: Queries with more than " + depth + " levels of nested DBRefMany relations are not supported.\nFix: Reduce nesting depth or move deeper traversal after AsEnumerable()."

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
                        isManyReferenceType (unwrapConvert mc.Object).Type
                    elif mc.Arguments.Count > 0 then
                        isManyReferenceType (unwrapConvert mc.Arguments.[0]).Type
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

    /// Relation membership consumes the original authored reference occurrence.
    let internal tryGetDBRefManyOwnerRef (qb: QueryBuilder) (arg: Expression) : DBRefManyOwnerRef voption =
        let arg = normalizeStoredConstruction arg
        if not(isManyReferenceType arg.Type) then ValueNone else
            let produced =
                match arg with
                | :? MemberExpression as memberValue when not(isNull memberValue.Expression) ->
                    let struct(produced, _, _) = QueryTranslatorVisitCoreMemberAccess.referenceInput visitProduced memberValue qb
                    ValueSome produced
                | :? ParameterExpression -> ValueSome(visitProduced arg qb)
                | _ -> ValueNone
            produced |> ValueOption.bind (fun reference ->
                match reference.Input.ReferenceOwner with
                | ValueSome owner when owner.Sources |> Array.exists (fun source -> source.Owner.IsSome) ->
                    ValueSome { Reference = reference; ReferenceType = arg.Type }
                | _ -> ValueNone)
