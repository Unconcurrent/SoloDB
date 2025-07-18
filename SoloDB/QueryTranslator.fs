﻿namespace SoloDatabase

open System.Collections

/// <summary>
/// Contains functions to translate .NET LINQ expression trees into SQLite SQL queries.
/// </summary>
module QueryTranslator =
    open System.Collections.ObjectModel
    open System.Text
    open System.Linq.Expressions
    open System
    open System.Reflection
    open System.Collections.Generic
    open JsonFunctions
    open Utils

    /// <summary>
    /// Represents a member access expression in a more abstract way.
    /// This private type simplifies handling different forms of member access.
    /// </summary>
    type private MemberAccess = 
        {
        /// <summary>The expression on which the member is being accessed.</summary>
        Expression: Expression
        /// <summary>The name of the member being accessed.</summary>
        MemberName: string
        /// <summary>The type of the input expression.</summary>
        InputType: Type
        /// <summary>The return type of the member access.</summary>
        ReturnType: Type
        /// <summary>The original MemberExpression, if available.</summary>
        OriginalExpression: MemberExpression option
        }

        /// <summary>
        /// Creates a MemberAccess record from a System.Linq.Expressions.MemberExpression.
        /// </summary>
        /// <param name="expr">The MemberExpression to convert.</param>
        /// <returns>A new MemberAccess record.</returns>
        static member From(expr: MemberExpression) =
            {
                Expression = expr.Expression
                MemberName = expr.Member.Name
                InputType = expr.Expression.Type
                ReturnType = expr.Type
                OriginalExpression = expr |> Some
            }

    /// <summary>
    /// Appends a value to the query as a parameter or literal, handling various primitive types and JSON serialization.
    /// </summary>
    /// <param name="sb">The StringBuilder to append the SQL text to.</param>
    /// <param name="variables">The dictionary of query parameters to add the value to.</param>
    /// <param name="value">The object value to append.</param>
    let internal appendVariable (sb: StringBuilder) (variables: #IDictionary<string, obj>) (value: obj) =
        match value with
        | :? bool as b -> 
            sb.Append (sprintf "%i" (if b then 1 else 0)) |> ignore
        | _other ->

        match value with
        | :? int8 as i -> sprintf "%i" i |> sb.Append |> ignore
        | :? uint8 as i -> sprintf "%i" i |> sb.Append |> ignore
        | :? int16 as i -> sprintf "%i" i |> sb.Append |> ignore
        | :? uint16 as i -> sprintf "%i" i |> sb.Append |> ignore
        | :? int32 as i -> sprintf "%i" i |> sb.Append |> ignore
        | :? uint32 as i -> sprintf "%i" i |> sb.Append |> ignore
        | :? int64 as i -> sprintf "%i" i |> sb.Append |> ignore
        | :? uint64 as i -> sprintf "%i" i |> sb.Append |> ignore
        | :? nativeint as i -> sprintf "%i" i |> sb.Append |> ignore
        | _other ->

        let jsonValue, kind = toSQLJson value
        let name = getVarName sb.Length
        if kind then 
            sb.Append (sprintf "jsonb(@%s)" name) |> ignore
        else 
            sb.Append (sprintf "@%s" name) |> ignore
        variables.[name] <- jsonValue

    /// <summary>
    /// Checks if a given .NET Type is considered a primitive type in the context of SQLite storage.
    /// Primitive types are stored directly, while others are serialized as JSON.
    /// </summary>
    /// <param name="x">The Type to check.</param>
    /// <returns>True if the type is a primitive SQLite type, otherwise false.</returns>
    let internal isPrimitiveSQLiteType (x: Type) =
        Utils.isIntegerBasedType x || Utils.isFloatBasedType x || x = typedefof<string> || x = typedefof<char> || x = typedefof<bool> || x = typedefof<Guid>
        || x = typedefof<Type>
        || x = typedefof<DateTime> || x = typedefof<DateTimeOffset> || x = typedefof<DateOnly> || x = typedefof<TimeOnly> || x = typedefof<TimeSpan>
        || x = typeof<byte array> || x = typeof<System.Collections.Generic.List<byte>> || x = typeof<byte list> || x = typeof<byte seq> 
        || x.Name = "Nullable`1"
        || x.IsEnum

    /// <summary>
    /// Escapes single quotes in a string for safe inclusion in a SQLite query.
    /// Also removes null characters.
    /// </summary>
    /// <param name="input">The string to escape.</param>
    /// <returns>The escaped string.</returns>
    let internal escapeSQLiteString (input: string) : string =
        input.Replace("'", "''").Replace("\0", "")

    /// <summary>
    /// A stateful builder for constructing a SQL query from an expression tree.
    /// </summary>
    type QueryBuilder =
        private {
            /// <summary>The StringBuilder holding the query text.</summary>
            StringBuilder: StringBuilder
            /// <summary>A dictionary of parameters for the query.</summary>
            Variables: Dictionary<string, obj>
            /// <summary>A function to append a variable to the query.</summary>
            AppendVariable: obj -> unit
            /// <summary>A function to roll back the StringBuilder by N characters.</summary>
            RollBack: uint -> unit
            /// <summary>Indicates if the builder is in 'update' mode, changing translation logic.</summary>
            UpdateMode: bool
            /// <summary>The table name prefix (e.g., "MyTable.") for column access.</summary>
            TableNameDot: string
            /// <summary>Determines if a root parameter should be wrapped in json_extract.</summary>
            JsonExtractSelfValue: bool
            /// <summary>The parameters of the root lambda expression.</summary>
            Parameters: ReadOnlyCollection<ParameterExpression>
            /// <summary>The index of the parameter representing the document ID.</summary>
            IdParameterIndex: int
        }
        /// <summary>
        /// Appends a raw string to the query being built.
        /// </summary>
        /// <param name="s">The string to append.</param>
        member this.AppendRaw (s: string) =
            this.StringBuilder.Append s |> ignore

        /// <summary>
        /// Appends a raw character to the query being built.
        /// </summary>
        /// <param name="s">The character to append.</param>
        member this.AppendRaw (s: char) =
            this.StringBuilder.Append s |> ignore

        /// <summary>
        /// Returns the generated SQL query string.
        /// </summary>
        /// <returns>The SQL query as a string.</returns>
        override this.ToString() = this.StringBuilder.ToString()

        /// <summary>
        /// Internal factory method to create a new QueryBuilder instance.
        /// </summary>
        /// <param name="sb">The StringBuilder to use.</param>
        /// <param name="variables">The dictionary for query parameters.</param>
        /// <param name="updateMode">Whether to operate in update mode.</param>
        /// <param name="tableName">The name of the table being queried.</param>
        /// <param name="expression">The root expression being translated.</param>
        /// <param name="idIndex">The parameter index for the document ID.</param>
        /// <returns>A new QueryBuilder instance.</returns>
        static member internal New(sb: StringBuilder)(variables: Dictionary<string, obj>)(updateMode: bool)(tableName)(expression: Expression)(idIndex: int) =
            {
                StringBuilder = sb
                Variables = variables
                AppendVariable = appendVariable sb variables
                RollBack = fun N -> sb.Remove(sb.Length - (int)N, (int)N) |> ignore
                UpdateMode = updateMode
                TableNameDot = if String.IsNullOrEmpty tableName then String.Empty else "\"" + tableName + "\"."
                JsonExtractSelfValue = true
                Parameters = 
                    let expression = 
                        if expression.NodeType = ExpressionType.Quote 
                        then (expression :?> UnaryExpression).Operand 
                        else expression 
                    in (expression :?> LambdaExpression).Parameters
                IdParameterIndex = idIndex
            }

    /// <summary>
    /// A private dictionary mapping .NET Math method names and arities to their corresponding SQLite function formats.
    /// $N, where N is 1-based, represents the arguments.
    /// </summary>
    let private mathFunctionTransformation = 
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
    let rec private isRootParameter (expr: Expression) : bool =
        match expr with
        | :? ParameterExpression -> true
        | :? MemberExpression as inner -> isRootParameter inner.Expression
        | _ -> false

    /// <summary>
    /// Recursively attempts to find the root ConstantExpression of a member access chain.
    /// </summary>
    /// <param name="expr">The expression to check.</param>
    /// <returns>Some ConstantExpression if found, otherwise None.</returns>
    let rec private tryRootConstant (expr: Expression) : ConstantExpression option =
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


    /// <summary>
    /// Generates SQL to compare a database value with a known .NET object.
    /// For primitive types, it performs a direct comparison.
    /// For complex types, it serializes the object to JSON and generates a deep comparison using jsonb_extract.
    /// </summary>
    /// <param name="qb">The query builder.</param>
    /// <param name="writeTarget">A function that writes the SQL for the database-side value (e.g., a column name).</param>
    /// <param name="targetType">The .NET type of the target value.</param>
    /// <param name="knownObject">The .NET object to compare against.</param>
    let inline private compareKnownJson (qb: QueryBuilder) (writeTarget: QueryBuilder -> unit) (targetType: Type) (knownObject: obj) =
        if isPrimitiveSQLiteType targetType then
            writeTarget qb
            qb.AppendRaw " = "
            qb.AppendVariable knownObject
            qb.AppendRaw ""
        else
            let json = JsonSerializator.JsonValue.Serialize knownObject

            let mutable first = true
            let inline writeAnd (qb: QueryBuilder) =
                if first then
                    first <- false
                else
                    qb.AppendRaw " AND "

            let rec compareJson (qb: QueryBuilder) (path: string) (json: JsonSerializator.JsonValue) =
                
                match json with
                | JsonSerializator.JsonValue.Null ->
                    writeAnd qb
                    qb.AppendRaw "jsonb_extract("
                    writeTarget qb
                    qb.AppendRaw ", '"
                    qb.AppendRaw path
                    qb.AppendRaw "') IS NULL"
        
                | JsonSerializator.JsonValue.Boolean b ->
                    writeAnd qb
                    qb.AppendRaw "jsonb_extract("
                    writeTarget qb
                    qb.AppendRaw ", '"
                    qb.AppendRaw path
                    qb.AppendRaw "') = "
                    qb.AppendVariable b
        
                | JsonSerializator.JsonValue.Number n ->
                    writeAnd qb
                    qb.AppendRaw "jsonb_extract("
                    writeTarget qb
                    qb.AppendRaw ", '"
                    qb.AppendRaw path
                    qb.AppendRaw "') = "
                    qb.AppendVariable n
        
                | JsonSerializator.JsonValue.String s ->
                    writeAnd qb
                    qb.AppendRaw "jsonb_extract("
                    writeTarget qb
                    qb.AppendRaw ", '"
                    qb.AppendRaw path
                    qb.AppendRaw "') = "
                    qb.AppendVariable s
        
                | JsonSerializator.JsonValue.Object dict ->
                    for KeyValue(k, v) in dict do
                        let newPath = if path = "$" then $"$.{k}" else $"{path}.{k}"
                        compareJson qb newPath v
        
                | JsonSerializator.JsonValue.List items ->
                    writeAnd qb
                    // For arrays, check that both arrays have the same length
                    // and each element matches at the corresponding index
                    qb.AppendRaw "json_array_length(jsonb_extract("
                    writeTarget qb
                    qb.AppendRaw ", '"
                    qb.AppendRaw path
                    qb.AppendRaw "')) = "
                    qb.AppendVariable items.Count
            
                    for i, item in items |> Seq.indexed do
                        let newPath = if path = "$" then $"$[{i}]" else $"{path}[{i}]"
                        compareJson qb newPath item

            match json with
            | JsonSerializator.JsonValue.Object d ->
                for KeyValue(k, v) in d do
                    if k = "$type" then () else

                    compareJson qb $"$.{k}" v
            | JsonSerializator.JsonValue.List _items ->
                compareJson qb "$" json
            | JsonSerializator.JsonValue.Null
            | JsonSerializator.JsonValue.Boolean _
            | JsonSerializator.JsonValue.Number _
            | JsonSerializator.JsonValue.String _ ->
                compareJson qb "$" json

    /// <summary>
    /// A list of functions to handle unknown expression types.
    /// Handlers are called when the main translator encounters an expression it doesn't recognize.
    /// They must not modify the QueryBuilder or expression if they return false (indicating not handled).
    /// Handlers are evaluated in reverse order (last added, first called).
    /// </summary>
    let unknownExpressionHandler = List<Func<QueryBuilder, Expression, bool>>(seq {
        Func<QueryBuilder, Expression, bool>(fun _qb exp -> raise<bool> (ArgumentException (sprintf "Unhandled expression type: '%O'" exp.NodeType)))
    })

    /// <summary>
    /// A list of functions called before the main translator attempts to translate an expression.
    /// This allows for overriding default translation behavior.
    /// Handlers must not modify the QueryBuilder or expression if they return false (indicating not handled).
    /// Handlers are evaluated in reverse order (last added, first called).
    /// </summary>
    let preExpressionHandler = List<Func<QueryBuilder, Expression, bool>>(seq {
        // No operation example.
        Func<QueryBuilder, Expression, bool>(fun _qb _exp -> false)
    })

    /// <summary>
    /// Executes a list of handlers for a given expression until one of them returns true.
    /// </summary>
    /// <param name="handler">The list of handler functions.</param>
    /// <param name="qb">The query builder.</param>
    /// <param name="exp">The expression to handle.</param>
    /// <returns>True if a handler processed the expression, otherwise false.</returns>
    let private runHandler (handler: List<Func<QueryBuilder, Expression, bool>>) (qb: QueryBuilder) (exp: Expression) =
        let mutable index = handler.Count - 1
        let mutable handled = false
        while index >= 0 && not handled do
            handled <- handler.[index].Invoke(qb, exp)
            index <- index - 1
        handled

    /// <summary>
    /// The main recursive visitor function that traverses the expression tree.
    /// It dispatches to specific visit methods based on the expression's NodeType.
    /// </summary>
    /// <param name="exp">The expression to visit and translate.</param>
    /// <param name="qb">The query builder state.</param>
    let rec private visit (exp: Expression) (qb: QueryBuilder) : unit =
        if runHandler preExpressionHandler qb exp then () else

        // If the expression is fully constant, evaluate it and append as a variable.
        if exp.NodeType <> ExpressionType.Lambda && exp.NodeType <> ExpressionType.Quote && isFullyConstant exp && (match exp with | :? ConstantExpression as ce when ce.Value = null -> false | other -> true) then
            let value = evaluateExpr<obj> exp
            qb.AppendVariable value
        else

        match exp.NodeType with
        // Binary operators
        | ExpressionType.And
        | ExpressionType.AndAlso
        | ExpressionType.Or
        | ExpressionType.OrElse
        | ExpressionType.Equal
        | ExpressionType.NotEqual
        | ExpressionType.LessThan
        | ExpressionType.LessThanOrEqual
        | ExpressionType.GreaterThan
        | ExpressionType.GreaterThanOrEqual 

        | ExpressionType.Add
        | ExpressionType.Subtract
        | ExpressionType.Multiply
        | ExpressionType.Divide
        | ExpressionType.Modulo ->
            visitBinary (exp :?> BinaryExpression) qb
        // Unary operators
        | ExpressionType.Not ->
            visitNot (exp :?> UnaryExpression) qb
        | ExpressionType.Negate
        | ExpressionType.NegateChecked ->
            visitNegate (exp :?> UnaryExpression) qb
        // Core expression types
        | ExpressionType.Lambda ->
            visitLambda (exp :?> LambdaExpression) qb
        | ExpressionType.Call ->
            visitMethodCall (exp :?> MethodCallExpression) qb
        | ExpressionType.Constant ->
            visitConstant (exp :?> ConstantExpression) qb
        | ExpressionType.MemberAccess ->
            visitMemberAccess (exp :?> MemberExpression |> MemberAccess.From) qb
        | ExpressionType.Convert ->
            visitConvert (exp :?> UnaryExpression) qb
        | ExpressionType.New ->
            visitNew (exp :?> NewExpression) qb
        | ExpressionType.Parameter ->
            visitParameter (exp :?> ParameterExpression) qb
        | ExpressionType.ArrayIndex ->
            match exp with
            | :? ParameterExpression as pe -> visitParameter pe qb
            | :? BinaryExpression as be -> arrayIndex be.Left be.Right qb
            | other -> failwithf "Unknown array index expression of: %A" other
        | ExpressionType.Index ->
            let exp = exp :?> IndexExpression
            if exp.Arguments.Count <> 1 then failwithf "The SQL translator does not support multiple args indexes."
            let arge = exp.Arguments.[0]
            if not (isFullyConstant arge) then failwithf "The SQL translator does not support non constant index arg."
            

            if arge.Type = typeof<string> then
                let arg = evaluateExpr<string> arge
                match arg with
                | "Id" when isRootParameter exp.Object -> qb.AppendRaw $"{qb.TableNameDot}Id"
                | arg ->
                visitProperty exp.Object arg ({new Expression() with member this.Type = exp.Object.Type}) qb
            else arrayIndex exp.Object arge qb
        | ExpressionType.Quote ->
            visit (exp :?> UnaryExpression).Operand qb
        | ExpressionType.Conditional ->
            visitIfElse (exp :?> ConditionalExpression) qb
        | ExpressionType.TypeIs ->
            let exp = exp :?> TypeBinaryExpression
            visitTypeIs exp qb
        | ExpressionType.MemberInit ->
            visitMemberInit (exp :?> MemberInitExpression) qb
        | ExpressionType.ListInit ->
            visitListInit (exp :?> ListInitExpression) qb
        // Fallback for unhandled types
        | _ ->
            if not (runHandler unknownExpressionHandler qb exp) then
                raise (ArgumentOutOfRangeException $"QueryTranslator.{nameof unknownExpressionHandler} did not handle the expression of type: {exp.NodeType}")

    /// <summary>
    /// Translates a ListInitExpression into a jsonb_array(...) function call.
    /// </summary>
    and private visitListInit (exp: ListInitExpression) (qb: QueryBuilder) =
        // json_array(...)
        qb.AppendRaw "jsonb_array("
        for item in exp.Initializers do
            let element = item.Arguments |> Seq.exactlyOne
            visit element qb
            qb.AppendRaw ','
        qb.RollBack 1u
        qb.AppendRaw ')'

    /// <summary>
    /// Translates a TypeBinaryExpression (TypeIs) into a check on the '$type' property of a JSON object.
    /// </summary>
    and private visitTypeIs (exp: TypeBinaryExpression) (qb: QueryBuilder) =
        if not (mustIncludeTypeInformationInSerializationFn exp.Expression.Type) then
            failwithf "Cannot translate TypeIs expression, because the DB will not store its type information for %A" exp.Type

        qb.AppendRaw "json_extract("
        do  visit exp.Expression qb
            qb.AppendRaw ','
            qb.AppendRaw "'$.$type'"
        qb.AppendRaw ')'
        qb.AppendRaw '='
        match exp.TypeOperand |> typeToName with
        | None -> failwithf "Cannot translate TypeIs expression with the TypeOperand: %A" exp.TypeOperand
        | Some typeName ->
        qb.AppendVariable typeName

    /// <summary>
    /// Translates a ConditionalExpression (if/then/else) into a SQL CASE WHEN ... THEN ... ELSE ... END statement.
    /// </summary>
    and private visitIfElse (conditionalExp: ConditionalExpression) (qb: QueryBuilder) =
        qb.AppendRaw "CASE WHEN "
        visit conditionalExp.Test qb
        qb.AppendRaw " THEN "
        visit conditionalExp.IfTrue qb
        qb.AppendRaw " ELSE "
        visit conditionalExp.IfFalse qb
        qb.AppendRaw " END"

    /// <summary>
    /// Translates an array or list index access into a jsonb_extract call with a JSON path.
    /// </summary>
    and private arrayIndex (array: Expression) (index: Expression) (qb: QueryBuilder) : unit =
        qb.AppendRaw "jsonb_extract("
        visit array qb |> ignore
        qb.AppendRaw ", '$["
        visit index qb |> ignore
        qb.AppendRaw "]')"
        ()

    /// <summary>
    /// Translates a property access into a jsonb_extract call or a direct column access for 'Id'.
    /// </summary>
    and private visitProperty (o: Expression) (property: obj) (m: Expression) (qb: QueryBuilder) =
        match property with
        | :? string as property ->
            if property = "Id" && o.NodeType = ExpressionType.Parameter && (m.Type = typeof<int64> || m.Type = typeof<int32>) then
                qb.AppendRaw qb.TableNameDot
                qb.AppendRaw "Id "
            else

            let memberAccess = {
                Expression = o
                MemberName = property
                InputType = m.Type
                ReturnType = typeof<obj>
                OriginalExpression = None
            }
            visitMemberAccess memberAccess qb

        | :? int as index ->
            let memberAccess = {
                Expression = o
                MemberName = $"[{index}]"
                InputType = m.Type
                ReturnType = typeof<obj>
                OriginalExpression = None
            }
            visitMemberAccess memberAccess qb

        | _other -> failwithf "Unable to translate property access."

    /// <summary>
    /// Wraps an expression in a SQL CAST operation.
    /// </summary>
    and private castTo (qb: QueryBuilder) (castToType: Type) (o: Expression) =
        let typ =
            match castToType with
            | OfType float
            | OfType float32
            | OfType decimal
                -> "REAL"
            | OfType bool
            | OfType int8
            | OfType uint8
            | OfType int16
            | OfType uint16
            | OfType int32
            | OfType uint32
            | OfType int64
            | OfType uint64
            | OfType nativeint
            | OfType unativeint
                -> "INTEGER"
            | _other -> "NOOP"

        if typ = "NOOP" then
            visit o qb
        else


        qb.AppendRaw "CAST("
        let innerQb = {qb with UpdateMode = false}
        visit o innerQb
        qb.AppendRaw " AS "
        qb.AppendRaw typ
        qb.AppendRaw ")"

    /// <summary>
    /// Translates a call to a method in the System.Math class using the mathFunctionTransformation map.
    /// </summary>
    /// <returns>True if the method was a known math function and was translated, otherwise false.</returns>
    and private visitMathMethod (m: MethodCallExpression) (qb: QueryBuilder) =
        match mathFunctionTransformation.TryGetValue ((m.Arguments.Count, m.Method.Name)) with
        | false, _ -> false
        | true, format ->
            let mutable visitNextChar = false
            for c in format do
                if visitNextChar && Char.IsDigit c then
                    visitNextChar <- false
                    visit
                        (match int c - int '0' with 
                            | 0 -> m.Object
                            | c -> m.Arguments[c - 1])
                        qb
                elif c = '$' then
                    visitNextChar <- true
                else
                    qb.AppendRaw c
            true

    /// <summary>
    /// Translates a MethodCallExpression into its corresponding SQL function or operation.
    /// </summary>
    and private visitMethodCall (m: MethodCallExpression) (qb: QueryBuilder) =    
        match visitMathMethod m qb with
        | true -> ()
        | false ->
        match m.Method.Name with
        | "ToLower" ->
            let value = m.Object
            // User defined function to also support non ASCII.
            qb.AppendRaw "TO_LOWER("
            visit value qb
            qb.AppendRaw ")"

        | "ToUpper" ->
            let value = m.Object
            // User defined function to also support non ASCII.
            qb.AppendRaw "TO_UPPER("
            visit value qb
            qb.AppendRaw ")"

        | "GetArray" ->
            let array = m.Arguments.[0]
            let index = m.Arguments.[1]

            arrayIndex array index qb
            
        | "Like" ->
            let string = m.Arguments.[0]
            let likeWhat = m.Arguments.[1]
            visit string qb |> ignore
            qb.AppendRaw " LIKE "
            visit likeWhat qb |> ignore

        | "Set" when qb.UpdateMode ->
            let oldValue = m.Arguments.[0]
            let newValue = m.Arguments.[1]
            visit oldValue qb |> ignore
            qb.AppendRaw ","
            visit newValue qb |> ignore
            qb.AppendRaw ","
        
        | "Append" | "Add" when qb.UpdateMode ->
            let struct (array, newValue) =
                if m.Arguments.Count = 2 then
                    struct (m.Arguments.[0], m.Arguments.[1])
                elif m.Object <> null && m.Arguments.Count = 1 then
                    struct (m.Object, m.Arguments.[0])
                else
                    failwithf "Unknown Append or Add method signature."

            visit array qb |> ignore
            qb.RollBack 1u
            qb.AppendRaw "[#]',"
            visit newValue qb |> ignore
            qb.AppendRaw ","
        
        | "SetAt" when qb.UpdateMode ->
            let array = m.Arguments.[0]
            let index = m.Arguments.[1]
            let newValue = m.Arguments.[2]
            visit array qb |> ignore
            qb.RollBack 1u
            qb.AppendRaw $"[{index}]',"
            visit newValue qb |> ignore
            qb.AppendRaw ","
        
        | "RemoveAt" when qb.UpdateMode ->
            let struct (array, index) =
                if m.Arguments.Count = 2 then
                    struct (m.Arguments.[0], m.Arguments.[1])
                elif m.Object <> null && m.Arguments.Count = 1 then
                    struct (m.Object, m.Arguments.[0])
                else
                    failwithf "Unknown RemoveAt method signature."

            visit array qb |> ignore
            qb.AppendRaw $",jsonb_remove(jsonb_extract({qb.TableNameDot}Value,"
            visit array qb |> ignore
            qb.AppendRaw "),"
            qb.AppendRaw $"'$[{index}]'),"
        
        | "op_Dynamic" ->
            let o = m.Arguments.[0]
            let property = (m.Arguments.[1] :?> ConstantExpression).Value

            match property with
            | :? string as property ->
                let memberAccess = {
                    Expression = o
                    MemberName = property
                    InputType = m.Type
                    ReturnType = typeof<obj>
                    OriginalExpression = None
                }
                visitMemberAccess memberAccess qb

            | :? int as index ->
                let memberAccess = {
                    Expression = o
                    MemberName = $"[{index}]"
                    InputType = m.Type
                    ReturnType = typeof<obj>
                    OriginalExpression = None
                }
                visitMemberAccess memberAccess qb

            | other -> failwithf "Unable to translate property access."
        | "Any" ->
            let array = m.Arguments.[0]
            let whereFuncExpr = m.Arguments.[1]
            let exprFunc = Expression.Lambda<Func<Expression>>(whereFuncExpr).Compile(true)
            let expr = exprFunc.Invoke()
            qb.AppendRaw $"EXISTS (SELECT 1 FROM json_each("

            do
                let qb = if qb.TableNameDot = "" then {qb with TableNameDot = "o."} else qb
                visit array qb |> ignore

            qb.AppendRaw ") WHERE "
            let innerQb = {qb with TableNameDot = "json_each."; JsonExtractSelfValue = false}
            visit expr innerQb |> ignore
            qb.AppendRaw ")"

        
        | "Contains" when 
                (m.Arguments.Count = 2 && typeof<System.Collections.IEnumerable>.IsAssignableFrom(m.Arguments.[0].Type)) 
                || (m.Arguments.Count = 1 && not (isNull m.Object) && m.Object.Type <> typeof<string> && typeof<System.Collections.IEnumerable>.IsAssignableFrom(m.Object.Type))
                ->

            let struct (array, value) = 
                if m.Arguments.Count = 2 then
                    struct (m.Arguments.[0], m.Arguments.[1])
                else
                    struct (m.Object, m.Arguments.[0])

            qb.AppendRaw $"EXISTS (SELECT 1 FROM json_each("
    
            do
                let qb = if qb.TableNameDot = "" then {qb with TableNameDot = "o."} else qb
                visit array qb |> ignore

            if isPrimitiveSQLiteType value.Type then
                qb.AppendRaw ") WHERE json_each.Value = "
                visit value qb |> ignore
                qb.AppendRaw ")"
            else
                if not (isFullyConstant value) then
                    raise (ArgumentException $"Cannot compare contains with this type of expression: {value.Type}")

                let targetType = value.Type

                let value = evaluateExpr<obj> value

                qb.AppendRaw ") WHERE "

                compareKnownJson qb (fun qb -> qb.AppendRaw "json_each.Value") targetType value

                qb.AppendRaw ")"
        
        | "QuotationToLambdaExpression" ->
            let arg1 = (m.Arguments.[0] :?> MethodCallExpression)
            let arg2 = arg1.Arguments.[0]
            visit arg2 qb
        
        | "op_Implicit" ->
            let arg1 = m.Arguments.[0]
            visit arg1 qb
        
        | "Contains" when not (isNull m.Object) && m.Object.Type = typeof<string> ->
            let text = m.Object
            let what = m.Arguments.[0]
            qb.AppendRaw "INSTR("
            visit text qb |> ignore
            qb.AppendRaw ","
            visit what qb |> ignore
            qb.AppendRaw ") > 0"
        
        | "GetType" when not (isNull m.Object) && m.Object.NodeType = ExpressionType.Parameter ->
            let o = m.Object
            qb.AppendRaw "jsonb_extract("
            visit o qb |> ignore
            qb.AppendRaw $",'$.$type')"
        
        | "TypeOf" when m.Type = typeof<Type> ->
            let t = m.Method.Invoke(null, Array.empty) :?> Type
            let name = match t |> typeToName with Some x -> x | None -> ""
            qb.AppendVariable name
        
        | "NewSqlId" ->
            let arg1 = m.Arguments.[0]
            visit arg1 qb
        
        | "get_Item" when not (isNull m.Object) && typeof<System.Collections.ICollection>.IsAssignableFrom m.Object.Type || typeof<Array>.IsAssignableFrom m.Object.Type || typeof<JsonSerializator.JsonValue>.IsAssignableFrom m.Object.Type ->
            let o = m.Object
            let property = (m.Arguments.[0] :?> ConstantExpression).Value

            visitProperty o property m qb


        | "Dyn" when m.Arguments.[1].Type = typeof<PropertyInfo> ->
            let o = m.Arguments.[0]

            let property = evaluateExpr<PropertyInfo> m.Arguments[1]
            let propertyName = property.Name

            visitProperty o propertyName m qb

        | "Dyn" ->
            let o = m.Arguments.[0]

            let property =
                match m.Arguments[1].Type with
                | t when t = typeof<string> || isIntegerBasedType t ->
                    evaluateExpr<obj> m.Arguments.[1]
                | _other -> failwithf "Cannot access dynamic property of %A" o

            visitProperty o property m qb

        | "Concat" when m.Type = typeof<string> ->          
            let len = m.Arguments.Count

            let args =
                if len = 1 && typeof<IEnumerable<string>>.IsAssignableFrom m.Arguments.[0].Type && m.Arguments.[0].NodeType = ExpressionType.NewArrayInit then
                    let array = m.Arguments.[0] :?> NewArrayExpression
                    array.Expressions
                else if len > 1 then          
                    m.Arguments
                else failwithf "Unknown such concat function: %A" m.Method

            let len = args.Count
            qb.AppendRaw "CONCAT("
            for i, arg in args |> Seq.indexed do
                visit arg qb |> ignore
                if i + 1 < len then qb.AppendRaw ", "

            qb.AppendRaw ")"

        | "StartsWith" when not (isNull m.Object) && m.Object.Type = typeof<string> ->
            let arg = m.Object
            let v = m.Arguments.[0]

            if isFullyConstant v then
                let prefix = if v.Type = typeof<char> then (string << evaluateExpr<char>) v else evaluateExpr<string> v

                if prefix.Length = 0 then
                    // Always true for non-null strings
                    qb.AppendRaw "("
                    visit arg qb
                    qb.AppendRaw " IS NOT NULL)"
                else

                // Compute next lexicographic string
                let nextString (s: string) =
                    let chars = s.ToCharArray()
                    let rec bump i =
                        if i < 0 then s + "\u0000"
                        elif chars.[i] < System.Char.MaxValue then
                            chars.[i] <- char (int chars.[i] + 1)
                            new string(chars, 0, i + 1)
                        else bump (i - 1)
                    bump (chars.Length - 1)

                let next = nextString prefix

                // col >= 'prefix' AND col < 'next'
                qb.AppendRaw "("
                visit arg qb
                qb.AppendRaw " >= "
                qb.AppendVariable prefix
                qb.AppendRaw " AND "
                visit arg qb
                qb.AppendRaw " < "
                qb.AppendVariable next
                qb.AppendRaw ")"
            else
                qb.AppendRaw "SUBSTR("
                visit arg qb
                qb.AppendRaw ","
                qb.AppendRaw "1,"
                qb.AppendRaw "LENGTH("
                visit v qb
                qb.AppendRaw "))"
                qb.AppendRaw " = "
                visit v qb
                    
        | "EndsWith" when not (isNull m.Object) && m.Object.Type = typeof<string> ->
            let arg = m.Object
            let v = m.Arguments.[0]

            qb.AppendRaw "SUBSTR("
            visit arg qb
            qb.AppendRaw ","
            qb.AppendRaw "-LENGTH("
            visit v qb
            qb.AppendRaw "))"
            qb.AppendRaw " = "
            visit v qb

        | "ToObject" when typeof<JsonSerializator.JsonValue>.IsAssignableFrom m.Method.DeclaringType || m.Method.DeclaringType.FullName = "SoloDatabase.MongoDB.BsonDocument" ->
            let castToType = m.Method.GetGenericArguments().[0]
            castTo qb castToType m.Object

        | "CastTo" ->
            let castToType = m.Method.GetGenericArguments().[0]
            castTo qb castToType m.Arguments.[0]

        // Numeric and floating-point Parse methods
        | "Parse" when
            m.Method.DeclaringType = typeof<SByte> ||
            m.Method.DeclaringType = typeof<Byte> ||
            m.Method.DeclaringType = typeof<Int16> ||
            m.Method.DeclaringType = typeof<UInt16> ||
            m.Method.DeclaringType = typeof<Int32> ||
            m.Method.DeclaringType = typeof<UInt32> ||
            m.Method.DeclaringType = typeof<Int64> ||
            m.Method.DeclaringType = typeof<UInt64> ||
            m.Method.DeclaringType = typeof<Single> ||
            m.Method.DeclaringType = typeof<Double> ||
            m.Method.DeclaringType = typeof<float> ||
            m.Method.DeclaringType = typeof<float32> ->

            let arg = m.Arguments.[0]
            let targetType =
                match m.Method.DeclaringType with
                | t when t = typeof<SByte> || t = typeof<Byte> || t = typeof<Int16> || t = typeof<UInt16>
                    || t = typeof<Int32> || t = typeof<UInt32> || t = typeof<Int64> || t = typeof<UInt64> -> "INTEGER"
                | t when t = typeof<Single> || t = typeof<Double> -> "REAL"
                | _ -> failwithf "Unsupported Parse type: %A" m.Method.DeclaringType

            qb.AppendRaw "CAST("
            visit arg qb
            qb.AppendRaw $" AS {targetType})"

        // String.Substring support
        | "Substring" when not (isNull m.Object) && m.Object.Type = typeof<string> ->
            let str = m.Object
            let start = m.Arguments.[0]
            qb.AppendRaw "SUBSTR("
            visit str qb
            qb.AppendRaw ","
            // SQLite is 1-based, .NET is 0-based
            qb.AppendRaw "("
            visit start qb
            qb.AppendRaw " + 1)"
            if m.Arguments.Count = 2 then
                let length = m.Arguments.[1]
                qb.AppendRaw ","
                visit length qb
            qb.AppendRaw ")"

        | "get_Chars" when m.Arguments.Count = 1 && not (isNull m.Object) && m.Object.Type = typeof<string> ->
            let str = m.Object
            let index = m.Arguments.[0]
            qb.AppendRaw "SUBSTR("
            visit str qb
            qb.AppendRaw ",("
            visit index qb
            qb.AppendRaw " + 1),1)"

        | "GetString" when m.Arguments.Count = 2 && isNull m.Object && m.Arguments.[0].Type = typeof<string> ->
            let str = m.Arguments.[0]
            let index = m.Arguments.[1]
            qb.AppendRaw "SUBSTR("
            visit str qb
            qb.AppendRaw ",("
            visit index qb
            qb.AppendRaw " + 1),1)"

        | "Count" when m.Arguments.Count = 1 && isNull m.Object && let t = m.Arguments.[0].Type in t.IsConstructedGenericType && t.GetGenericTypeDefinition() = typedefof<System.Linq.IGrouping<_,_>> ->
            qb.AppendRaw $"json_array_length({qb.TableNameDot}Value, '$.Items')"

        | "Items" when m.Arguments.Count = 1 && isNull m.Object && let t = m.Arguments.[0].Type in t.IsConstructedGenericType && t.GetGenericTypeDefinition() = typedefof<System.Linq.IGrouping<_,_>> ->
            qb.AppendRaw $"jsonb_extract({qb.TableNameDot}Value, '$.Items')"

        | "Invoke" when FSharp.Reflection.FSharpType.IsRecord m.Type ->
            // This handles chained F# record construction via curried lambdas.
            // Example: Username => Auth => Banned => FirstSeen => LastSeen => new ...(...).Invoke(...)
            // We need to walk the chain and collect the arguments in reverse order.
            let rec collectArgs (expr: Expression) (args: ResizeArray<struct (ParameterExpression * Expression)>) =
                match expr with
                | :? MethodCallExpression as mc when mc.Method.Name = "Invoke" && mc.Arguments.Count = 1 ->
                    match mc.Object with
                    | :? LambdaExpression as le ->
                        args.Add(struct (le.Parameters.[0], mc.Arguments.[0]))
                    | _ -> ()
                    collectArgs mc.Object args
                | :? LambdaExpression as le ->
                    collectArgs le.Body args
                | _ -> struct (expr, args)

            let struct (recordCtorExpr, args) = collectArgs (m :> Expression) (ResizeArray())

            let recordType = m.Type
            let fields = FSharp.Reflection.FSharpType.GetRecordFields recordType

            let constrArgs = (recordCtorExpr :?> NewExpression).Arguments

            let args = 
                seq {
                    for field, arg in constrArgs |> Seq.zip fields do
                        match arg with
                        | :? ParameterExpression as pe ->
                            let struct(_, correspondingExpr) = args |> Seq.find(fun struct(p, _) -> p = pe)
                            struct (field.Name, correspondingExpr)
                        | other -> 
                            struct (field.Name, other)
                } |> Seq.map(fun struct(name, expr) -> struct (name, visit expr)) |> Seq.toArray

            newObject qb args

            ()
        | _ -> 
            raise (NotSupportedException(sprintf "The method %s is not supported" m.Method.Name))

    /// <summary>
    /// Translates a LambdaExpression by visiting its body.
    /// </summary>
    and private visitLambda (m: LambdaExpression) (qb: QueryBuilder) =
        visit(m.Body) qb

    /// <summary>
    /// Translates a ParameterExpression. This typically represents the root document or an ID.
    /// </summary>
    and private visitParameter (m: ParameterExpression) (qb: QueryBuilder) =
        if m.Type = typeof<int64> // Check if it is an id type.
            && ((qb.Parameters.IndexOf m = 0 // For the .SelectWithId function, it is always at index 0,
                && qb.Parameters.Count = 2 // and has 2 parameters the id and the item.
                )
                || qb.IdParameterIndex = qb.Parameters.IndexOf m // Or it is specified as an id.
                )
            then qb.AppendRaw $"{qb.TableNameDot}Id"

        elif qb.UpdateMode then
            qb.AppendRaw $"'$'"

        elif qb.JsonExtractSelfValue && ((not << isPrimitiveSQLiteType) m.Type || (not << String.IsNullOrWhiteSpace) qb.TableNameDot) then
            if qb.StringBuilder.Length = 0 
            then qb.AppendRaw $"json_extract(json({qb.TableNameDot}Value), '$')" // To avoid returning the jsonB format instead of normal json.
            else qb.AppendRaw $"jsonb_extract({qb.TableNameDot}Value, '$')"
        else
            qb.AppendRaw $"{qb.TableNameDot}Value"
        

    /// <summary>
    /// Translates a UnaryExpression with NodeType 'Not' into a SQL NOT operator.
    /// </summary>
    and private visitNot (m: UnaryExpression) (qb: QueryBuilder) =
        qb.AppendRaw "NOT "
        visit m.Operand qb

    /// <summary>
    /// Translates a UnaryExpression with NodeType 'Negate' or 'NegateChecked' into a SQL unary minus.
    /// </summary>
    and private visitNegate (m: UnaryExpression) (qb: QueryBuilder) =
        qb.AppendRaw "-"
        visit m.Operand qb

    /// <summary>
    /// Translates the creation of a new object into a jsonb_object(...) function call.
    /// </summary>
    /// <param name="qb">The query builder.</param>
    /// <param name="memberGenerator">An array of tuples containing member names and functions to write their values.</param>
    and private newObject (qb: QueryBuilder) (memberGenerator: struct (string * (QueryBuilder -> unit)) array) =
        qb.AppendRaw "jsonb_object("
        let mutable index = 0
        for struct (name, visitMember) in memberGenerator do
            // Write the property name as a JSON key
            qb.AppendRaw $"'{escapeSQLiteString name}',"
            // Write the value by visiting the expression
            visitMember qb
            if index < memberGenerator.Length - 1 then
                qb.AppendRaw ","
            index <- index + 1
        qb.AppendRaw ")"

    /// <summary>
    /// Translates a NewExpression, typically for creating an anonymous type, a tuple, or a simple class instance.
    /// </summary>
    and private visitNew (m: NewExpression) (qb: QueryBuilder) =
        let t = m.Type

        if isTuple t then
            qb.AppendRaw "json_array("
            for i, arg in m.Arguments |> Seq.indexed do
                visit(arg) qb |> ignore
                if m.Arguments.IndexOf arg <> m.Arguments.Count - 1 then
                    qb.AppendRaw ","
            qb.AppendRaw ")"
        // A simple class or struct.
        elif m.Members.Count = m.Arguments.Count then
            let pairs = m.Arguments |> Seq.zip m.Members
            let members = pairs |> Seq.map(fun (membr, expr) -> struct (membr.Name, fun qb -> visit expr qb)) |> Seq.toArray
            newObject qb members
        else
            failwithf "Cannot construct new in SQL query %A" t

    /// <summary>
    /// Translates a MemberInitExpression (object initializer syntax) into a jsonb_object call.
    /// </summary>
    and private visitMemberInit (m: MemberInitExpression) (qb: QueryBuilder) =
        newObject qb [|
            for binding in m.Bindings |> Seq.cast<MemberAssignment> do
                struct(binding.Member.Name, visit binding.Expression)
        |]

    /// <summary>
    /// Translates a Convert or Cast expression. If converting to/from obj, it's a no-op. Otherwise, it generates a CAST.
    /// </summary>
    and private visitConvert (m: UnaryExpression) (qb: QueryBuilder) =
        if m.Type = typeof<obj> || m.Operand.Type = typeof<obj> then
            visit(m.Operand) qb
        else 
        castTo qb m.Type m.Operand

    /// <summary>
    /// Translates a BinaryExpression into its corresponding SQL operator.
    /// Handles null comparisons (IS NULL / IS NOT NULL) and complex type equality.
    /// </summary>
    and private visitBinary (b: BinaryExpression) (qb: QueryBuilder) =
        // Handle string concatenation
        if b.NodeType = ExpressionType.Add && (b.Left.Type = typeof<string> || b.Right.Type = typeof<string>) then
            let expr = Expression.Call(typeof<String>.GetMethod("Concat", [|typeof<string seq>|]), Expression.NewArrayInit(typeof<string>, [|b.Left; b.Right|]))
            visit expr qb
        else

        let isLeftNull =
            match b.Left with
            | :? ConstantExpression as c when c.Value = null -> true
            | _other -> false

        let isRightNull =
            match b.Right with
            | :? ConstantExpression as c when c.Value = null -> true
            | _other -> false

        let isAnyNull = isLeftNull || isRightNull

        let struct (left, right) = if isLeftNull then struct (b.Right, b.Left) else struct (b.Left, b.Right)

        let shouldUseComplex = not isAnyNull && not (isPrimitiveSQLiteType left.Type || isPrimitiveSQLiteType right.Type) && (left.Type <> typeof<obj> && right.Type <> typeof<obj>) && (isFullyConstant left || isFullyConstant right)
        match struct (shouldUseComplex, b.NodeType) with
        | struct (true, ExpressionType.Equal) -> 
            qb.AppendRaw("(")

            let struct (constant, expression) = 
                if isFullyConstant left then
                    struct (left, right)
                else
                    struct (right, left)

            let value = evaluateExpr<obj> constant

            compareKnownJson qb (fun qb -> visit expression qb) expression.Type value

            qb.AppendRaw(")")
        | struct (true, ExpressionType.NotEqual) -> 
            qb.AppendRaw("(NOT (")

            let struct (constant, expression) = 
                if isFullyConstant left then
                    struct (left, right)
                else
                    struct (right, left)

            let value = evaluateExpr<obj> constant

            compareKnownJson qb (fun qb -> visit expression qb) expression.Type value

            qb.AppendRaw("))")
        | _ ->

        qb.AppendRaw("(") |> ignore
        visit(left) qb |> ignore
        match b.NodeType with
        | ExpressionType.And
        | ExpressionType.AndAlso -> qb.AppendRaw(" AND ") |> ignore
        | ExpressionType.OrElse
        | ExpressionType.Or -> qb.AppendRaw(" OR ") |> ignore
        | ExpressionType.Equal -> if isAnyNull then qb.AppendRaw(" IS ") else qb.AppendRaw(" = ") |> ignore
        | ExpressionType.NotEqual -> if isAnyNull then qb.AppendRaw(" IS NOT ") else qb.AppendRaw(" <> ") |> ignore
        | ExpressionType.LessThan -> qb.AppendRaw(" < ") |> ignore
        | ExpressionType.LessThanOrEqual -> qb.AppendRaw(" <= ") |> ignore
        | ExpressionType.GreaterThan -> qb.AppendRaw(" > ") |> ignore
        | ExpressionType.GreaterThanOrEqual -> qb.AppendRaw(" >= ") |> ignore

        | ExpressionType.Add -> qb.AppendRaw(" + ") |> ignore
        | ExpressionType.Subtract -> qb.AppendRaw(" - ") |> ignore
        | ExpressionType.Multiply -> qb.AppendRaw(" * ") |> ignore
        | ExpressionType.Divide -> qb.AppendRaw(" / ") |> ignore
        | ExpressionType.Modulo -> qb.AppendRaw(" % ") |> ignore
        | _ -> raise (NotSupportedException(sprintf "The binary operator %O is not supported" b.NodeType))
        visit(right) qb |> ignore
        qb.AppendRaw(")") |> ignore       

    /// <summary>
    /// Translates a ConstantExpression into a SQL literal (NULL) or a query parameter.
    /// </summary>
    and private visitConstant (c: ConstantExpression) (qb: QueryBuilder) =
        match c.Value with
        | null -> qb.AppendRaw("NULL") |> ignore
        | _ ->
            qb.AppendVariable(c.Value) |> ignore        

    /// <summary>
    /// Translates a MemberExpression, handling property and field access.
    /// It builds a JSON path for nested properties.
    /// </summary>
    and private visitMemberAccess (m: MemberAccess) (qb: QueryBuilder) =
        let rec buildJsonPath (expr: Expression) (accum: string list) : string list =
            match expr with
            | :? MemberExpression as inner ->
                let currentField = inner.Member.Name
                buildJsonPath inner.Expression (currentField :: accum)
            | _ -> accum

        let formatAccess (path: string) =
            let path = path.Replace(".[", "[") // Replace array access.
            if qb.UpdateMode then sprintf "'$.%s'" path
            else sprintf "jsonb_extract(%sValue, '$.%s')" qb.TableNameDot path


        if m.MemberName = "Length" && (* All cases below implement IEnumerable*) m.InputType.GetInterface (typeof<IEnumerable>.FullName) <> null then
            if m.InputType = typeof<string> then
                qb.AppendRaw "length("
                visit m.Expression qb
                qb.AppendRaw ")"
            elif m.InputType = typeof<byte array> then
                qb.AppendRaw "length(base64("
                visit m.Expression qb
                qb.AppendRaw "))"
            else
                // json len
                qb.AppendRaw "json_array_length("
                visit m.Expression qb
                qb.AppendRaw ")"

        elif m.MemberName = "Count" && m.InputType.GetInterface (typeof<IEnumerable>.FullName) <> null then
            qb.AppendRaw "json_array_length("
            visit m.Expression qb
            qb.AppendRaw ")"

        elif typedefof<System.Linq.IGrouping<_,_>>.IsAssignableFrom (m.InputType) then
            match m.MemberName with
            | "Key" -> qb.AppendRaw "jsonb_extract(Value, '$.Key')"
            | other ->
                qb.AppendRaw "jsonb_extract(Value, '$.Items."
                qb.AppendRaw (escapeSQLiteString other)
                qb.AppendRaw "')"

        elif (m.ReturnType = typeof<int64> && m.MemberName = "Id") || (m.MemberName = "Id" && m.Expression.NodeType = ExpressionType.Parameter && m.Expression.Type.FullName = "SoloDatabase.JsonSerializator.JsonValue") then
            qb.AppendRaw $"{qb.TableNameDot}Id " |> ignore

        elif m.Expression <> null && isRootParameter m.Expression then
            let jsonPath = buildJsonPath m.Expression [m.MemberName]
            match jsonPath with
            | [] -> ()
            | [single] ->
                qb.AppendRaw(formatAccess single) |> ignore
            | paths ->
                let pathStr = String.concat $"." (List.map (sprintf "%s") paths)
                qb.AppendRaw(formatAccess pathStr) |> ignore          
        else 

        match m.OriginalExpression with
        | None -> 
            qb.AppendRaw "jsonb_extract("
            visit m.Expression qb
            qb.AppendRaw $", '$.{m.MemberName}')"
        | Some m ->
        if m.Expression = null then
            let value = (m.Member :?> PropertyInfo).GetValue null
            qb.AppendVariable value
        else if isFullyConstant m then
            let value = evaluateExpr m
            qb.AppendVariable value        
        else
            raise (NotSupportedException(sprintf "The member access '%O' is not supported" m.Member.Name))
            

    /// <summary>
    /// Translates a LINQ expression into a SQL string and a dictionary of parameters.
    /// This is a primary public entry point for the translator.
    /// </summary>
    /// <param name="tableName">The name of the table to query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// <returns>A tuple containing the generated SQL string and a dictionary of parameters.</returns>
    let translate (tableName: string) (expression: Expression) =
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables false tableName expression -1
    
        visit expression builder
        sb.ToString(), variables

    /// <summary>
    /// Internal function to translate a part of a queryable expression.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="expression">The expression to translate.</param>
    /// <param name="sb">The StringBuilder to append to.</param>
    /// <param name="variables">The dictionary of parameters.</param>
    let internal translateQueryable (tableName: string) (expression: Expression) (sb: StringBuilder) (variables: Dictionary<string, obj>) =
        let builder = QueryBuilder.New sb variables false tableName expression -1
        visit expression builder
        sb.Append " " |> ignore

    /// <summary>
    /// Internal function similar to translateQueryable, but prevents wrapping the root parameter in json_extract.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="expression">The expression to translate.</param>
    /// <param name="sb">The StringBuilder to append to.</param>
    /// <param name="variables">The dictionary of parameters.</param>
    let internal translateQueryableNotExtractSelfJson (tableName: string) (expression: Expression) (sb: StringBuilder) (variables: Dictionary<string, obj>) =
        let builder = {(QueryBuilder.New sb variables false tableName expression -1) with JsonExtractSelfValue = false}
        visit expression builder
        sb.Append " " |> ignore

    /// <summary>
    /// Internal function to translate an expression that involves the document ID, using a specific parameter index for the ID.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="expression">The expression to translate.</param>
    /// <param name="idParameterIndex">The index of the parameter that represents the ID.</param>
    /// <returns>A tuple containing the generated SQL string and a dictionary of parameters.</returns>
    let internal translateWithId (tableName: string) (expression: Expression) idParameterIndex =
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables false tableName expression idParameterIndex
    
        visit expression builder
        sb.ToString(), variables

    /// <summary>
    /// Internal function to translate an expression in "update" mode, which generates SQL fragments for jsonb_set or jsonb_insert.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="expression">The expression to translate.</param>
    /// <param name="fullSQL">The StringBuilder for the SQL command.</param>
    /// <param name="variableDict">The dictionary for parameters.</param>
    let internal translateUpdateMode (tableName: string) (expression: Expression) (fullSQL: StringBuilder) (variableDict: Dictionary<string, obj>) =
        let builder = QueryBuilder.New fullSQL variableDict true tableName expression -1
    
        visit expression builder
