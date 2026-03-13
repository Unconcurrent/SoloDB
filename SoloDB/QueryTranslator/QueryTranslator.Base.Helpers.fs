namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection
open System.Runtime.InteropServices
open JsonFunctions
open Utils
open QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

module internal QueryTranslatorBaseHelpers =
    /// <summary>
    /// A private dictionary mapping .NET Math method names and arities to their corresponding SQLite function formats.
    /// $N, where N is 1-based, represents the arguments.
    /// </summary>
    let internal mathFunctionTransformation =
        let arr = [|
            (1, "Sin"), "SIN($1)"
            (1, "sin"), "SIN($1)"
            (1, "Cos"), "COS($1)"
            (1, "cos"), "COS($1)"
            (1, "Tan"), "TAN($1)"
            (1, "tan"), "TAN($1)"
            (1, "Asin"), "ASIN($1)"
            (1, "asin"), "ASIN($1)"
            (1, "Acos"), "ACOS($1)"
            (1, "acos"), "ACOS($1)"
            (1, "Atan"), "ATAN($1)"
            (1, "atan"), "ATAN($1)"
            (2, "Atan2"), "ATAN2($1, $2)"
            (2, "atan2"), "ATAN2($1, $2)"
            (1, "Sinh"), "SINH($1)"
            (1, "sinh"), "SINH($1)"
            (1, "Cosh"), "COSH($1)"
            (1, "cosh"), "COSH($1)"
            (1, "Tanh"), "TANH($1)"
            (1, "tanh"), "TANH($1)"
            (1, "Asinh"), "ASINH($1)"
            (1, "asinh"), "ASINH($1)"
            (1, "Acosh"), "ACOSH($1)"
            (1, "acosh"), "ACOSH($1)"
            (1, "Atanh"), "ATANH($1)"
            (1, "atanh"), "ATANH($1)"
            (1, "Log"), "LN($1)"
            (1, "log"), "LN($1)"
            (1, "Log2"), "LOG(2, $1)"
            (1, "Log10"), "LOG10($1)"
            (1, "log10"), "LOG10($1)"
            (1, "Exp"), "EXP($1)"
            (1, "exp"), "EXP($1)"
            (2, "Pow"), "POW($1, $2)"
            (1, "Sqrt"), "SQRT($1)"
            (1, "sqrt"), "SQRT($1)"
            (1, "Ceiling"), "CEIL($1)"
            (1, "ceil"), "CEIL($1)"
            (1, "Floor"), "FLOOR($1)"
            (1, "floor"), "FLOOR($1)"
            (1, "Round"), "ROUND($1, 0)"
            (2, "Round"), "ROUND($1, $2)"
            (1, "round"), "ROUND($1, 0)"
            (1, "Truncate"), "TRUNC($1)"
            (1, "truncate"), "TRUNC($1)"
            (1, "Abs"), "ABS($1)"
            (1, "abs"), "ABS($1)"
            (1, "Sign"), "SIGN($1)"
            (1, "sign"), "SIGN($1)"
            (2, "Min"), "MIN($1, $2)"
            (2, "min"), "MIN($1, $2)"
            (2, "Max"), "MAX($1, $2)"
            (2, "max"), "MAX($1, $2)"
            (2, "IEEERemainder"), "($1 - $2 * ROUND($1 / $2))"
            (2, "BigMul"), "($1 * $2)"
        |]

        // F# uses the function differently.
        let fSharpFormatedArray = arr |> Array.map(fun ((n, fnName), op) -> (n + 1, fnName + "$W" (* Adding the postfix to the function name. *)), (* And shifting the parameters by 1. *) op.Replace("$2", "$3").Replace("$1", "$2"))

        arr
        |> Array.append fSharpFormatedArray
        |> readOnlyDict

    /// <summary>
    /// Evaluates a LINQ expression that is expected to be constant, returning its value.
    /// It first tries a direct cast for ConstantExpression and falls back to compiling and invoking for others.
    /// </summary>
    /// <typeparam name="'O">The expected return type.</typeparam>
    /// <param name="e">The expression to evaluate.</param>
    /// <returns>The result of the expression evaluation.</returns>
    let internal evaluateExpr<'O> (e: Expression) =
        match e with
        | :? ConstantExpression as ce ->
            ce.Value :?> 'O
        | _other ->
        let exprFunc = Expression.Lambda<Func<'O>>(UnaryExpression.Convert(e, typeof<'O>)).Compile(true)
        exprFunc.Invoke()

    /// <summary>
    /// Recursively checks if an expression originates from a root ParameterExpression.
    /// </summary>
    /// <param name="expr">The expression to check.</param>
    /// <returns>True if the expression's root is a ParameterExpression.</returns>
    let rec internal isRootParameter (expr: Expression) : bool =
        match expr with
        | :? ParameterExpression -> true
        | :? MemberExpression as inner -> isRootParameter inner.Expression
        | _ -> false

    /// <summary>
    /// Recursively attempts to find the root ConstantExpression of a member access chain.
    /// </summary>
    /// <param name="expr">The expression to check.</param>
    /// <returns>Some ConstantExpression if found, otherwise None.</returns>
    let rec internal tryRootConstant (expr: Expression) : ConstantExpression option =
        match expr with
        | :? ConstantExpression as e -> Some e
        | :? MemberExpression as inner -> tryRootConstant inner.Expression
        | _ -> None


    /// <summary>
    /// Recursively checks if an entire expression tree is constant (i.e., contains no ParameterExpressions).
    /// Such expressions can be pre-evaluated locally.
    /// </summary>
    /// <param name="expr">The expression to check.</param>
    /// <returns>True if the expression is fully constant.</returns>
    let rec isFullyConstant (expr: Expression) : bool =
        match expr with
        | :? ParameterExpression -> false
        | :? BinaryExpression as binExpr ->
            isFullyConstant binExpr.Left && isFullyConstant binExpr.Right
        | :? UnaryExpression as unaryExpr ->
            isFullyConstant unaryExpr.Operand
        | :? MethodCallExpression as methodCallExpr ->
            methodCallExpr.Arguments |> Seq.forall isFullyConstant && isFullyConstant methodCallExpr.Object
        | :? MemberExpression as memberExpr ->
            isFullyConstant memberExpr.Expression
        | :? ConstantExpression -> true
        | :? InvocationExpression as invocationExpr ->
            invocationExpr.Arguments |> Seq.forall isFullyConstant && isFullyConstant invocationExpr.Expression
        | :? LambdaExpression as lambdaExpr ->
            lambdaExpr.Body |> isFullyConstant
        | :? NewExpression as ne ->
            ne.Arguments |> Seq.forall isFullyConstant
        | :? NewArrayExpression as na ->
            na.Expressions |> Seq.forall isFullyConstant
        | :? MemberInitExpression as mi ->
            mi.Bindings |> Seq.map(fun b -> b :?> MemberAssignment) |> Seq.map(fun b -> b.Expression) |> Seq.append [|mi.NewExpression|] |> Seq.forall isFullyConstant
        | null -> true
        | _ -> false

    /// <summary>
    /// Recursively checks if any part of an expression tree is a constant.
    /// </summary>
    /// <param name="expr">The expression to check.</param>
    /// <returns>True if any node in the expression is a ConstantExpression.</returns>
    let rec isAnyConstant (expr: Expression) : bool =
        match expr with
        | :? IndexExpression as ie -> isAnyConstant ie.Object
        | :? ParameterExpression -> false
        | :? BinaryExpression as binExpr ->
            isAnyConstant binExpr.Left || isAnyConstant binExpr.Right
        | :? UnaryExpression as unaryExpr ->
            isAnyConstant unaryExpr.Operand
        | :? MethodCallExpression as methodCallExpr when methodCallExpr.Method.Name = "op_Dynamic" || methodCallExpr.Method.Name = "Dyn" ->
            isAnyConstant methodCallExpr.Arguments.[0]
        | :? MethodCallExpression as methodCallExpr when methodCallExpr.Method.Name = "get_Item" ->
            isAnyConstant methodCallExpr.Object
        | :? MethodCallExpression as methodCallExpr ->
            methodCallExpr.Arguments |> Seq.exists isAnyConstant || isAnyConstant methodCallExpr.Object
        | :? MemberExpression as memberExpr ->
            isAnyConstant memberExpr.Expression
        | :? ConstantExpression -> true
        | :? InvocationExpression as invocationExpr ->
            invocationExpr.Arguments |> Seq.exists isAnyConstant || isAnyConstant invocationExpr.Expression
        | :? LambdaExpression as lambdaExpr ->
            lambdaExpr.Body |> isAnyConstant
        | :? NewExpression as ne when isTuple ne.Type ->
            ne.Arguments |> Seq.exists isAnyConstant
        | :? NewArrayExpression as na ->
            na.Expressions |> Seq.exists isFullyConstant
        | _ -> true

    /// Substitute lambda parameters with invocation arguments (beta-reduction helper).
    let internal inlineLambdaInvocation (lambda: LambdaExpression) (args: IReadOnlyList<Expression>) =
        if lambda.Parameters.Count <> args.Count then
            raise (NotSupportedException(
                $"Error: Lambda invocation arity mismatch.\nReason: Expected {lambda.Parameters.Count} argument(s) but got {args.Count}.\nFix: Ensure invoked lambda argument count matches parameter count, or simplify expression before translation."))
        else
            let mutable body = lambda.Body
            for i = 0 to lambda.Parameters.Count - 1 do
                let p = lambda.Parameters.[i]
                let a = args.[i]
                let visitor =
                    { new ExpressionVisitor() with
                        override _.VisitParameter(node: ParameterExpression) =
                            if Object.ReferenceEquals(node, p) then a
                            else base.VisitParameter(node) }
                body <- visitor.Visit(body)
            body


    /// Compares a database value with a known .NET object, returning an SqlExpr conjunction.
    let inline internal compareKnownJsonDu (qb: QueryBuilder) (targetExpr: SqlExpr) (targetType: Type) (knownObject: obj) : SqlExpr =
        if isPrimitiveSQLiteType targetType then
            SqlExpr.Binary(targetExpr, BinaryOperator.Eq, qb.AllocateParamExpr knownObject)
        else
            let json =
                match knownObject with
                | :? JsonSerializator.JsonValue as json -> json
                | _ -> JsonSerializator.JsonValue.Serialize knownObject

            let rec buildConjunction (comparisons: SqlExpr list) : SqlExpr =
                match comparisons with
                | [] -> SqlExpr.Literal(SqlLiteral.Boolean true)
                | [single] -> single
                | head :: tail -> SqlExpr.Binary(head, BinaryOperator.And, buildConjunction tail)

            let appendObjectPathSegment (path: string) (key: string) =
                let key = key.Replace("\000", "")
                let needsQuoted =
                    key |> Seq.exists (fun c -> not (Char.IsLetterOrDigit c || c = '_' || c = '$'))

                if needsQuoted then
                    if path = "$" then sprintf "$.\"%s\"" key else sprintf "%s.\"%s\"" path key
                else
                    if path = "$" then sprintf "$.%s" key else sprintf "%s.%s" path key

            let rec compareJsonDu (path: string) (json: JsonSerializator.JsonValue) : SqlExpr list =
                let extract = SqlExpr.FunctionCall("jsonb_extract", [targetExpr; SqlExpr.Literal(SqlLiteral.String path)])
                match json with
                | JsonSerializator.JsonValue.Null ->
                    [SqlExpr.Unary(UnaryOperator.IsNull, extract)]
                | JsonSerializator.JsonValue.Boolean b ->
                    [SqlExpr.Binary(extract, BinaryOperator.Eq, qb.AllocateParamExpr b)]
                | JsonSerializator.JsonValue.Number n ->
                    [SqlExpr.Binary(extract, BinaryOperator.Eq, qb.AllocateParamExpr n)]
                | JsonSerializator.JsonValue.String s ->
                    [SqlExpr.Binary(extract, BinaryOperator.Eq, qb.AllocateParamExpr s)]
                | JsonSerializator.JsonValue.Object dict ->
                    [for KeyValue(k, v) in dict do
                        let newPath = appendObjectPathSegment path k
                        yield! compareJsonDu newPath v]
                | JsonSerializator.JsonValue.List items ->
                    let lengthCheck =
                        SqlExpr.Binary(
                            SqlExpr.FunctionCall("json_array_length", [SqlExpr.FunctionCall("jsonb_extract", [targetExpr; SqlExpr.Literal(SqlLiteral.String path)])]),
                            BinaryOperator.Eq,
                            qb.AllocateParamExpr items.Count)
                    [yield lengthCheck
                     for i in 0 .. items.Count - 1 do
                        let newPath = if path = "$" then $"$[{i}]" else $"{path}[{i}]"
                        yield! compareJsonDu newPath items.[i]]

            let comparisons =
                match json with
                | JsonSerializator.JsonValue.Object d ->
                    [for KeyValue(k, v) in d do
                        yield! compareJsonDu (appendObjectPathSegment "$" k) v]
                | _ -> compareJsonDu "$" json

            buildConjunction comparisons

    /// Unwrap a single Convert node to get the actual expression.
    let internal unwrapConvert (expr: Expression) =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> ue.Operand
        | e -> e

    /// Returns true if the expression is a DBRef<T>.Value member access boundary.
    let internal isDBRefValueBoundary (expr: MemberExpression) =
        expr.Member.Name = "Value" && not (isNull expr.Expression) && DBRefTypeHelpers.isDBRefType (unwrapConvert expr.Expression).Type

    /// Returns true if the expression traverses through DBRef<T>.Value,
    /// indicating a LEFT JOIN that may produce NULL for None-ref rows.
    let rec internal involvesDBRefValueAccess (expr: Expression) : bool =
        match unwrapConvert expr with
        | :? MemberExpression as me when isDBRefValueBoundary me ->
            true
        | :? MemberExpression as me when not (isNull me.Expression) ->
            involvesDBRefValueAccess me.Expression
        | :? MethodCallExpression as mc when mc.Method.Name = "Invoke" && mc.Arguments.Count > 0 ->
            mc.Arguments |> Seq.exists involvesDBRefValueAccess
        | _ -> false
