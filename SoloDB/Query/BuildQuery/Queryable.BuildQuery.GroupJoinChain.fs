namespace SoloDatabase
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Utils
open SoloDatabase
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.SqlModel
open SoloDatabase.ChainExpr
open SoloDatabase.GroupJoinRuntimeTypes
open SoloDatabase.GroupJoinAliases
open SoloDatabase.GroupJoinChainParts
module internal QueryableBuildQueryGroupJoinChain =
    open QueryableHelperJoin
    open QueryableHelperState
    open QueryableHelperPreprocess
    open QueryableHelperBase
    open QueryableBuildQueryWindowHelpers


    let rec translateGroupChainExpression (rt: GroupJoinRuntime) (ctx: QueryContext) (alias: string) (currentParam: ParameterExpression) (expr: Expression) =
        if not (referencesParam rt.OuterParam expr) then
            rt.TranslateJoinExpr ctx alias rt.Vars (Some currentParam) expr
        elif not (referencesParam currentParam expr) then
            rt.TranslateOuterExpr expr
        else
            match expr with
            | :? BinaryExpression as be ->
                let left = translateGroupChainExpression rt ctx alias currentParam be.Left
                let right = translateGroupChainExpression rt ctx alias currentParam be.Right
                let op =
                    match be.NodeType with
                    | ExpressionType.Coalesce -> None
                    | ExpressionType.Add -> Some BinaryOperator.Add
                    | ExpressionType.Subtract -> Some BinaryOperator.Sub
                    | ExpressionType.Multiply -> Some BinaryOperator.Mul
                    | ExpressionType.Divide -> Some BinaryOperator.Div
                    | ExpressionType.Modulo -> Some BinaryOperator.Mod
                    | ExpressionType.Equal -> Some BinaryOperator.Eq
                    | ExpressionType.NotEqual -> Some BinaryOperator.Ne
                    | ExpressionType.GreaterThan -> Some BinaryOperator.Gt
                    | ExpressionType.GreaterThanOrEqual -> Some BinaryOperator.Ge
                    | ExpressionType.LessThan -> Some BinaryOperator.Lt
                    | ExpressionType.LessThanOrEqual -> Some BinaryOperator.Le
                    | ExpressionType.AndAlso -> Some BinaryOperator.And
                    | ExpressionType.OrElse -> Some BinaryOperator.Or
                    | _ -> None
                match op with
                | Some op -> SqlExpr.Binary(left, op, right)
                | None when be.NodeType = ExpressionType.Coalesce -> SqlExpr.Coalesce(left, [right])
                | None ->
                    raise (NotSupportedException(
                        $"Error: GroupJoin mixed outer-capture expression '{be.NodeType}' is not supported.\n" +
                        "Fix: Simplify the group predicate or move it after AsEnumerable()."))
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert || ue.NodeType = ExpressionType.ConvertChecked || ue.NodeType = ExpressionType.TypeAs ->
                translateGroupChainExpression rt ctx alias currentParam ue.Operand
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
                SqlExpr.Unary(UnaryOperator.Not, translateGroupChainExpression rt ctx alias currentParam ue.Operand)
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Negate || ue.NodeType = ExpressionType.NegateChecked ->
                SqlExpr.Unary(UnaryOperator.Neg, translateGroupChainExpression rt ctx alias currentParam ue.Operand)
            | _ ->
                raise (NotSupportedException(
                    "Error: GroupJoin mixed outer-capture expression is not supported.\n" +
                    "Reason: The predicate mixes group-item and outer-row access in an unsupported shape.\n" +
                    "Fix: Simplify the expression or move it after AsEnumerable()."))
