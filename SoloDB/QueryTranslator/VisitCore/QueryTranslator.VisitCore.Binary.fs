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
    let internal visitBinaryDu (visitDu: Expression -> QueryBuilder -> SqlExpr) (visitComparisonDu: Expression -> QueryBuilder -> SqlExpr) (visitStoredValueDu: Expression -> QueryBuilder -> SqlExpr) (visitPresenceDu: Expression -> QueryBuilder -> SqlExpr) (b: BinaryExpression) (qb: QueryBuilder) : SqlExpr =
        // In UpdateMode an `Assign` on a member access lowers to the same
        // assignment that the `Extensions.Set(member, value)` path records.
        // This accepts the F# canonical owner-side property-mutation form
        // `o.Field <- value` alongside `o.Field.Set value`.
        if b.NodeType = ExpressionType.Assign && qb.UpdateMode then
            let pathExpr = visitDu b.Left qb
            let valueExpr = visitStoredValueDu b.Right qb
            qb.UpdateAssignments.Add(pathExpr, valueExpr)
            SqlExpr.Literal(SqlLiteral.Null)
        elif (b.NodeType = ExpressionType.Equal || b.NodeType = ExpressionType.NotEqual)
             && DBRefTypeHelpers.isFileRefType b.Left.Type && b.Left.Type.Equals(b.Right.Type) then
            let left = fileSystemReferenceIdentity visitDu qb b.Left
            let right = fileSystemReferenceIdentity visitDu qb b.Right
            SqlExpr.Binary(left, (if b.NodeType = ExpressionType.Equal then BinaryOperator.Eq else BinaryOperator.Ne), right)
        else
        let isLeftNull = match b.Left with :? ConstantExpression as c when c.Value = null -> true | _ -> false
        let isRightNull = match b.Right with :? ConstantExpression as c when c.Value = null -> true | _ -> false
        let isAnyNull = isLeftNull || isRightNull
        let relationalOperator = QueryTranslatorNumericMethods.relationalOperator b.NodeType
        let struct(left, right) = if isLeftNull then struct(b.Right, b.Left) else struct(b.Left, b.Right)
        let complexTypes = not isAnyNull && (QueryTranslatorNumeric.domain left.Type).IsNone && (QueryTranslatorNumeric.domain right.Type).IsNone && not (isPrimitiveSQLiteType left.Type || isPrimitiveSQLiteType right.Type) && (not (typeof<obj>.Equals(left.Type)) && not (typeof<obj>.Equals(right.Type)))
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
            let knownObject = ExpressionEvaluation.evaluateExpr<obj> constant
            EqualityComparison.known qb targetExpr expression.Type knownObject
        | struct(true, ExpressionType.NotEqual) ->
            let struct(constant, expression) = if isFullyConstant left then struct(left, right) else struct(right, left)
            let targetExpr = visitDu expression qb
            let knownObject = ExpressionEvaluation.evaluateExpr<obj> constant
            SqlExpr.Unary(UnaryOperator.Not, EqualityComparison.known qb targetExpr expression.Type knownObject)
        | _ ->
        let visitOperand =
            if isAnyNull && (b.NodeType=ExpressionType.Equal || b.NodeType=ExpressionType.NotEqual) then visitPresenceDu
            elif relationalOperator.IsSome then visitComparisonDu
            else visitDu
        let numericalComparison =
            if relationalOperator.IsSome && not isAnyNull then
                QueryTranslatorNumericMethods.comparison b.NodeType b.Method b.Left.Type b.Right.Type
            else ValueNone
        let leftDu = visitOperand left qb
        let rightDu = visitOperand right qb
        let leftType = DateTimeFunctions.unwrapNullable left.Type
        let rightType = DateTimeFunctions.unwrapNullable right.Type
        let struct(leftDu, rightDu) =
            if relationalOperator.IsSome then
                if DateTimeFunctions.isDateTimeLikeType leftType || DateTimeFunctions.isDateTimeLikeType rightType then
                    DateTimeFunctions.ensureComparableDateTimeLikeTypes left.Type right.Type
                    struct(DateTimeFunctions.canonicalizeForCompareOrOrder left.Type leftDu, DateTimeFunctions.canonicalizeForCompareOrOrder right.Type rightDu)
                elif not isAnyNull then
                    match numericalComparison with
                    | ValueSome comparison -> QueryTranslatorNumericMethods.comparableOperands comparison left.Type right.Type leftDu rightDu
                    | ValueNone -> struct(leftDu, rightDu)
                else struct(leftDu, rightDu)
            else
                struct(leftDu, rightDu)
        match QueryTranslatorNumeric.binary b.Type b.NodeType leftDu rightDu with
        | ValueSome result -> result
        | ValueNone ->
        let op =
            match b.NodeType with
            | ExpressionType.And | ExpressionType.AndAlso -> BinaryOperator.And
            | ExpressionType.Or | ExpressionType.OrElse -> BinaryOperator.Or
            | ExpressionType.Equal when isAnyNull || isOptionType left.Type || isOptionType right.Type || involvesDBRefValueAccess left || involvesDBRefValueAccess right -> BinaryOperator.Is
            | ExpressionType.NotEqual when isAnyNull || isOptionType left.Type || isOptionType right.Type || involvesDBRefValueAccess left || involvesDBRefValueAccess right -> BinaryOperator.IsNot
            | _ when relationalOperator.IsSome -> relationalOperator.Value
            | ExpressionType.Add -> BinaryOperator.Add
            | ExpressionType.Subtract -> BinaryOperator.Sub
            | ExpressionType.Multiply -> BinaryOperator.Mul
            | ExpressionType.Divide -> BinaryOperator.Div
            | ExpressionType.Modulo -> BinaryOperator.Mod
            | _ -> raise (NotSupportedException(sprintf "Error: Binary operator '%O' is not supported.\nReason: The operator cannot be translated to SQL for this expression.\nFix: Simplify the expression or move it after AsEnumerable()." b.NodeType))
        SqlExpr.Binary(leftDu, op, rightDu)
