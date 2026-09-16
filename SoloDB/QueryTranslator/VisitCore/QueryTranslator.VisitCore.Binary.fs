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
open SoloDatabase.SqlModel

module internal QueryTranslatorVisitCoreBinary =
    let private isRelationalBinary (nodeType: ExpressionType) =
        nodeType = ExpressionType.Equal
        || nodeType = ExpressionType.NotEqual
        || nodeType = ExpressionType.LessThan
        || nodeType = ExpressionType.LessThanOrEqual
        || nodeType = ExpressionType.GreaterThan
        || nodeType = ExpressionType.GreaterThanOrEqual

    let internal visitBinaryDu (visitDu: Expression -> QueryBuilder -> SqlExpr) (visitComparisonDu: Expression -> QueryBuilder -> SqlExpr) (visitStoredValueDu: Expression -> QueryBuilder -> SqlExpr) (b: BinaryExpression) (qb: QueryBuilder) : SqlExpr =
        // In UpdateMode an `Assign` on a member access lowers to the same
        // assignment that the `Extensions.Set(member, value)` path records.
        // This accepts the F# canonical owner-side property-mutation form
        // `o.Field <- value` alongside `o.Field.Set value`.
        if b.NodeType = ExpressionType.Assign && qb.UpdateMode then
            let pathExpr = visitDu b.Left qb
            let valueExpr = visitStoredValueDu b.Right qb
            qb.UpdateAssignments.Add(pathExpr, valueExpr)
            SqlExpr.Literal(SqlLiteral.Null)
        elif b.NodeType = ExpressionType.Add && (b.Left.Type = typeof<string> || b.Right.Type = typeof<string>) then
            let expr = Expression.Call(typeof<String>.GetMethod("Concat", [|typeof<string seq>|]), Expression.NewArrayInit(typeof<string>, [|b.Left; b.Right|]))
            visitDu expr qb
        else
        let isLeftNull = match b.Left with :? ConstantExpression as c when c.Value = null -> true | _ -> false
        let isRightNull = match b.Right with :? ConstantExpression as c when c.Value = null -> true | _ -> false
        let isAnyNull = isLeftNull || isRightNull
        let struct(left, right) = if isLeftNull then struct(b.Right, b.Left) else struct(b.Left, b.Right)
        let complexTypes = not isAnyNull && not (isPrimitiveSQLiteType left.Type || isPrimitiveSQLiteType right.Type) && (left.Type <> typeof<obj> && right.Type <> typeof<obj>)
        let boundComparison =
            match qb.SourceContext.BindQueryValue with
            | ValueSome bind when complexTypes && (b.NodeType = ExpressionType.Equal || b.NodeType = ExpressionType.NotEqual) ->
                if bind.IsValue right then ValueSome(EqualityComparison.bound qb bind (visitDu left qb) right)
                elif bind.IsValue left then ValueSome(EqualityComparison.bound qb bind (visitDu right qb) left)
                else ValueNone
            | _ -> ValueNone
        match boundComparison with
        | ValueSome comparison ->
            if b.NodeType = ExpressionType.Equal then comparison else SqlExpr.Unary(UnaryOperator.Not, comparison)
        | ValueNone ->
        let shouldUseComplex = complexTypes && (isFullyConstant left || isFullyConstant right)
        match struct(shouldUseComplex, b.NodeType) with
        | struct(true, ExpressionType.Equal) ->
            let struct(constant, expression) = if isFullyConstant left then struct(left, right) else struct(right, left)
            let targetExpr = visitDu expression qb
            let knownObject = evaluateExpr<obj> constant
            EqualityComparison.known qb targetExpr expression.Type knownObject
        | struct(true, ExpressionType.NotEqual) ->
            let struct(constant, expression) = if isFullyConstant left then struct(left, right) else struct(right, left)
            let targetExpr = visitDu expression qb
            let knownObject = evaluateExpr<obj> constant
            SqlExpr.Unary(UnaryOperator.Not, EqualityComparison.known qb targetExpr expression.Type knownObject)
        | _ ->
        let visitOperand = if isRelationalBinary b.NodeType then visitComparisonDu else visitDu
        let leftDu = visitOperand left qb
        let rightDu = visitOperand right qb
        let leftType = DateTimeFunctions.unwrapNullable left.Type
        let rightType = DateTimeFunctions.unwrapNullable right.Type
        let struct(leftDu, rightDu) =
            if isRelationalBinary b.NodeType then
                if DateTimeFunctions.isDateTimeLikeType leftType || DateTimeFunctions.isDateTimeLikeType rightType then
                    DateTimeFunctions.ensureComparableDateTimeLikeTypes left.Type right.Type
                    struct(DateTimeFunctions.canonicalizeForCompareOrOrder left.Type leftDu, DateTimeFunctions.canonicalizeForCompareOrOrder right.Type rightDu)
                else
                    struct(leftDu, rightDu)
            else
                struct(leftDu, rightDu)
        let op =
            match b.NodeType with
            | ExpressionType.And | ExpressionType.AndAlso -> BinaryOperator.And
            | ExpressionType.Or | ExpressionType.OrElse -> BinaryOperator.Or
            | ExpressionType.Equal ->
                if isAnyNull || involvesDBRefValueAccess left || involvesDBRefValueAccess right
                then BinaryOperator.Is else BinaryOperator.Eq
            | ExpressionType.NotEqual ->
                if isAnyNull || involvesDBRefValueAccess left || involvesDBRefValueAccess right
                then BinaryOperator.IsNot else BinaryOperator.Ne
            | ExpressionType.LessThan -> BinaryOperator.Lt
            | ExpressionType.LessThanOrEqual -> BinaryOperator.Le
            | ExpressionType.GreaterThan -> BinaryOperator.Gt
            | ExpressionType.GreaterThanOrEqual -> BinaryOperator.Ge
            | ExpressionType.Add -> BinaryOperator.Add
            | ExpressionType.Subtract -> BinaryOperator.Sub
            | ExpressionType.Multiply -> BinaryOperator.Mul
            | ExpressionType.Divide -> BinaryOperator.Div
            | ExpressionType.Modulo -> BinaryOperator.Mod
            | _ -> raise (NotSupportedException(sprintf "Error: Binary operator '%O' is not supported.\nReason: The operator cannot be translated to SQL for this expression.\nFix: Simplify the expression or move it after AsEnumerable()." b.NodeType))
        SqlExpr.Binary(leftDu, op, rightDu)
