namespace SoloDatabase

open System.Collections
open System.Runtime.InteropServices

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
        let value =
            match value with
            | :? bool as b -> 
                box (if b then 1 else 0)
            | _other ->
                value

        let jsonValue, shouldEncode = toSQLJson value
        let name = getVarName sb.Length
        if shouldEncode then 
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

    [<return: Struct>]
    let internal (|OfShape0|_|) (_retType: ('any1 -> 'T) | null) (_objType: ('any2 -> 'O) | null) (name: string) (m: MethodCallExpression) =
        if m.Method.Name = name && (isNull _retType || typeof<'T>.IsAssignableFrom m.Type) then
            let ret =
                if not (isNull m.Object) then
                    // Instance method: target is m.Object
                    ValueSome struct (m.Object, m.Arguments, 0)
                elif m.Arguments.Count > 0 then
                    // Static/Extension method: target is first arg
                    ValueSome struct (m.Arguments.[0], m.Arguments, 1)
                else
                    ValueNone

            let ret = 
                match ret with
                | ValueSome struct (o, _, _) when isNull _objType || typeof<'O>.IsAssignableFrom o.Type -> ret
                | _ -> ValueNone

            match ret with
            | ValueSome struct (o, _, _) -> ValueSome o
            | _ -> ValueNone
        else
            ValueNone

    [<return: Struct>]
    let internal (|OfShape1|_|) (_retType: ('any1 -> 'T) | null) (_objType: ('any2 -> 'O) | null) (name: string) (_argType: (unit -> 'A) | null) (m: MethodCallExpression) =
        if m.Method.Name = name && (isNull _retType || typeof<'T>.IsAssignableFrom m.Type) then
            let ret =
                if not (isNull m.Object) then
                    // Instance method: target is m.Object, args are m.Arguments
                    ValueSome struct (m.Object, m.Arguments, 0)
                elif m.Arguments.Count > 0 then
                    // Static/Extension method: target is first arg, rest are args
                    ValueSome struct (m.Arguments.[0], m.Arguments, 1)
                else
                    ValueNone

            let ret = 
                match ret with
                | ValueSome struct (o, _, _) when isNull _objType || typeof<'O>.IsAssignableFrom o.Type -> ret
                | _ -> ValueNone

            let ret =
                match ret with
                | ValueSome struct (o, args, argsIndex) when args.Count > argsIndex && (isNull _argType || typeof<'A>.IsAssignableFrom args.[argsIndex].Type) ->
                        ValueSome struct (o, args.[argsIndex])
                | _ -> ValueNone

            ret
        else
            ValueNone

    [<return: Struct>]
    let internal (|OfShape2|_|) (_retType: ('any1 -> 'T) | null) (_objType: ('any2 -> 'O) | null) (name: string) (_arg1Type: (unit -> 'A) | null) (_arg2Type: (unit -> 'B) | null) (m: MethodCallExpression) =
        if m.Method.Name = name && (isNull _retType || typeof<'T>.IsAssignableFrom m.Type) then
            let ret =
                if not (isNull m.Object) then
                    ValueSome struct (m.Object, m.Arguments, 0)
                elif m.Arguments.Count > 0 then
                    ValueSome struct (m.Arguments.[0], m.Arguments, 1)
                else
                    ValueNone

            let ret = 
                match ret with
                | ValueSome struct (o, _, _) when isNull _objType || typeof<'O>.IsAssignableFrom o.Type -> ret
                | _ -> ValueNone

            match ret with
            | ValueSome struct (o, args, argsIndex) when args.Count > argsIndex + 1 
                && (isNull _arg1Type || typeof<'A>.IsAssignableFrom args.[argsIndex].Type) 
                && (isNull _arg2Type || typeof<'B>.IsAssignableFrom args.[argsIndex + 1].Type) ->
                    ValueSome struct (o, args.[argsIndex], args.[argsIndex + 1])
            | _ -> ValueNone
        else
            ValueNone

    [<return: Struct>]
    let internal (|OfShape3|_|) (_retType: ('any1 -> 'T) | null) (_objType: ('any2 -> 'O) | null) (name: string) (_arg1Type: (unit -> 'A) | null) (_arg2Type: (unit -> 'B) | null) (_arg3Type: (unit -> 'C) | null) (m: MethodCallExpression) =
        if m.Method.Name = name && (isNull _retType || typeof<'T>.IsAssignableFrom m.Type) then
            let ret =
                if not (isNull m.Object) then
                    ValueSome struct (m.Object, m.Arguments, 0)
                elif m.Arguments.Count > 0 then
                    ValueSome struct (m.Arguments.[0], m.Arguments, 1)
                else
                    ValueNone

            let ret = 
                match ret with
                | ValueSome struct (o, _, _) when isNull _objType || typeof<'O>.IsAssignableFrom o.Type -> ret
                | _ -> ValueNone

            match ret with
            | ValueSome struct (o, args, argsIndex) when args.Count > argsIndex + 2
                && (isNull _arg1Type || typeof<'A>.IsAssignableFrom args.[argsIndex].Type) 
                && (isNull _arg2Type || typeof<'B>.IsAssignableFrom args.[argsIndex + 1].Type)
                && (isNull _arg3Type || typeof<'C>.IsAssignableFrom args.[argsIndex + 2].Type) ->
                    ValueSome struct (o, args.[argsIndex], args.[argsIndex + 1], args.[argsIndex + 2])
            | _ -> ValueNone
        else
            ValueNone

    // You cannot use the generic's "<" or ">" chars inside match's case
    let inline private OfIEnum () = Unchecked.defaultof<IEnumerable>
    let inline private OfString () = Unchecked.defaultof<string>
    let inline private OfType () = Unchecked.defaultof<Type>
    let inline private OfPropInfo () = Unchecked.defaultof<PropertyInfo>
    let inline private OfValueType () = Unchecked.defaultof<ValueType>
    let inline private OfStringComparison () = Unchecked.defaultof<StringComparison>

    /// Returns true if the StringComparison is case-insensitive.
    let inline private isIgnoreCase (comparison: StringComparison) =
        match comparison with
        | StringComparison.OrdinalIgnoreCase
        | StringComparison.CurrentCultureIgnoreCase
        | StringComparison.InvariantCultureIgnoreCase -> true
        | _ -> false

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
        if exp.NodeType <> ExpressionType.Lambda && exp.NodeType <> ExpressionType.Quote && isFullyConstant exp && (match exp with | :? ConstantExpression as ce when ce.Value = null -> false | _other -> true) then
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
        match index with
        | :? ConstantExpression as ce when ce.Type.IsPrimitive ->
            qb.AppendRaw (string ce.Value)
        | _other -> failwithf "The index of the array must always be a constant value."
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
        
        let containsImpl (_m: MethodCallExpression) (qb: QueryBuilder) array (value: Expression) =
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
                    raise (ArgumentException $"Cannot translate contains with this type of expression: {value.Type}")

                let targetType = value.Type
                let value = evaluateExpr<obj> value

                qb.AppendRaw ") WHERE "
                compareKnownJson qb (fun qb -> qb.AppendRaw "json_each.Value") targetType value
                qb.AppendRaw ")"

        /// Helper for nested array predicate methods (Any, All).
        /// isAll=false: EXISTS (SELECT 1 FROM json_each(...) WHERE predicate)
        /// isAll=true:  NOT EXISTS (SELECT 1 FROM json_each(...) WHERE NOT (predicate))
        let visitNestedArrayPredicate (qb: QueryBuilder) (array: Expression) (whereFuncExpr: Expression) (isAll: bool) =
            // Extract the lambda expression from the argument
            // It may be: 1) A Quote containing a lambda, 2) A lambda directly, 3) A constant with delegate
            let expr =
                match whereFuncExpr with
                | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
                    // Quoted lambda (from Queryable methods)
                    ue.Operand
                | :? LambdaExpression as le ->
                    // Direct lambda
                    le :> Expression
                | _ ->
                    // Try the original approach for backwards compatibility
                    let exprFunc = Expression.Lambda<Func<Expression>>(whereFuncExpr).Compile(true)
                    exprFunc.Invoke()

            if isAll then qb.AppendRaw "NOT "
            qb.AppendRaw "EXISTS (SELECT 1 FROM json_each("

            do
                let qb = if qb.TableNameDot = "" then {qb with TableNameDot = "o."} else qb
                visit array qb |> ignore

            qb.AppendRaw ") WHERE "
            if isAll then qb.AppendRaw "NOT ("

            let innerQb = {qb with TableNameDot = "json_each."; JsonExtractSelfValue = false}
            visit expr innerQb |> ignore

            if isAll then qb.AppendRaw ")"
            qb.AppendRaw ")"

        match m with
        | OfShape0 null null "ToLowerInvariant" value
        | OfShape0 null null "ToLower" value ->
            // User defined function to also support non ASCII.
            qb.AppendRaw "TO_LOWER("
            visit value qb
            qb.AppendRaw ")"

        | OfShape0 null null "ToUpperInvariant" value
        | OfShape0 null null "ToUpper" value ->
            // User defined function to also support non ASCII.
            qb.AppendRaw "TO_UPPER("
            visit value qb
            qb.AppendRaw ")"

        | OfShape1 null null "GetArray" null (array, index) ->
            arrayIndex array index qb
        
        | OfShape1 null null "Like" null (string, likeWhat) ->
            visit string qb |> ignore
            qb.AppendRaw " LIKE "
            visit likeWhat qb |> ignore

        | OfShape1 null null "Set" null (oldValue, newValue) when qb.UpdateMode ->
            visit oldValue qb |> ignore
            qb.AppendRaw ","
            visit newValue qb |> ignore
            qb.AppendRaw ","
    
        | OfShape1 null null "Append" null (array, newValue)
        | OfShape1 null null "Add" null (array, newValue) when qb.UpdateMode ->
            visit array qb |> ignore
            qb.RollBack 1u
            qb.AppendRaw "[#]',"
            visit newValue qb |> ignore
            qb.AppendRaw ","
    
        | OfShape2 null null "SetAt" null null (array, index, newValue) when qb.UpdateMode ->
            visit array qb |> ignore
            qb.RollBack 1u
            qb.AppendRaw $"[{index}]',"
            visit newValue qb |> ignore
            qb.AppendRaw ","
    
        | OfShape1 null null "RemoveAt" null (array, index) when qb.UpdateMode ->
            visit array qb |> ignore
            qb.AppendRaw $",jsonb_remove(jsonb_extract({qb.TableNameDot}Value,"
            visit array qb |> ignore
            qb.AppendRaw "),"
            qb.AppendRaw $"'$[{index}]'),"
    
        | OfShape1 null null "op_Dynamic" null (o, propExpr) ->
            let property = (propExpr |> unbox<ConstantExpression>).Value
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

            | _ -> failwithf "Unable to translate property access."

        // Nested array predicate methods (Any, All) - common platform
        | OfShape1 null null "Any" null (array, whereFuncExpr) ->
            visitNestedArrayPredicate qb array whereFuncExpr false // Any = EXISTS

        | OfShape1 null null "All" null (array, whereFuncExpr) ->
            visitNestedArrayPredicate qb array whereFuncExpr true  // All = NOT EXISTS ... NOT
                
        // String.Contains(string, StringComparison) - case-insensitive support (must come before OfShape1)
        | OfShape2 null OfString "Contains" null OfStringComparison (text, what, comparisonExpr) ->
            let comparison = evaluateExpr<StringComparison> comparisonExpr
            if isIgnoreCase comparison then
                qb.AppendRaw "INSTR(TO_LOWER("
                visit text qb |> ignore
                qb.AppendRaw "),TO_LOWER("
                visit what qb |> ignore
                qb.AppendRaw ")) > 0"
            else
                qb.AppendRaw "INSTR("
                visit text qb |> ignore
                qb.AppendRaw ","
                visit what qb |> ignore
                qb.AppendRaw ") > 0"

        | OfShape1 null OfString "Contains" null (text, what) ->
            qb.AppendRaw "INSTR("
            visit text qb |> ignore
            qb.AppendRaw ","
            visit what qb |> ignore
            qb.AppendRaw ") > 0"

        | OfShape1 null OfIEnum "Contains" null (array, value) ->
            containsImpl m qb array value

        | OfShape1 bool OfValueType "Contains" null (ros, value) when ros.Type.Name = "ReadOnlySpan`1" ->
            containsImpl m qb ros value
    
        | OfShape0 null null "QuotationToLambdaExpression" arg1 ->
             // arg1 is technically the target "o" in OfShape0 logic for static methods
             let arg1 = (arg1 :?> MethodCallExpression)
             let arg2 = arg1.Arguments.[0]
             visit arg2 qb
    
        | OfShape0 null null "op_Implicit" arg1 ->
            visit arg1 qb
    
        | OfShape0 null null "GetType" o when o.NodeType = ExpressionType.Parameter ->
            qb.AppendRaw "jsonb_extract("
            visit o qb |> ignore
            qb.AppendRaw $",'$.$type')"
    
        | OfShape0 (OfType) null "TypeOf" _ ->
            let t = m.Method.Invoke(null, Array.empty) :?> Type
            let name = match t |> typeToName with Some x -> x | None -> ""
            qb.AppendVariable name
    
        | OfShape0 null null "NewSqlId" arg1 ->
            visit arg1 qb
    
        | OfShape1 null null "get_Item" null (o, propExpr) when typeof<System.Collections.ICollection>.IsAssignableFrom o.Type || typeof<Array>.IsAssignableFrom o.Type || typeof<JsonSerializator.JsonValue>.IsAssignableFrom o.Type ->
            let property = (propExpr |> unbox<ConstantExpression>).Value
            visitProperty o property m qb

        | OfShape1 null null "Dyn" (OfPropInfo) (o, propExpr) ->
            let property = evaluateExpr<PropertyInfo> propExpr
            let propertyName = property.Name
            visitProperty o propertyName m qb

        | OfShape1 null null "Dyn" null (o, propExpr) ->
            let property =
                match propExpr.Type with
                | t when t = typeof<string> || isIntegerBasedType t ->
                    evaluateExpr<obj> propExpr
                | _other -> failwithf "Cannot access dynamic property of %A" o

            visitProperty o property m qb

        | OfShape0 (OfString) null "Concat" _ ->          
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

        // String.StartsWith(string, StringComparison) - must come before OfShape1
        | OfShape2 null OfString "StartsWith" null OfStringComparison (arg, v, comparisonExpr) ->
            let comparison = evaluateExpr<StringComparison> comparisonExpr
            if isIgnoreCase comparison then
                qb.AppendRaw "SUBSTR(TO_LOWER("
                visit arg qb
                qb.AppendRaw "),1,LENGTH("
                visit v qb
                qb.AppendRaw ")) = TO_LOWER("
                visit v qb
                qb.AppendRaw ")"
            else
                qb.AppendRaw "SUBSTR("
                visit arg qb
                qb.AppendRaw ",1,LENGTH("
                visit v qb
                qb.AppendRaw ")) = "
                visit v qb

        | OfShape1 null OfString "StartsWith" null (arg, v) ->
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

        // String.EndsWith(string, StringComparison) - must come before OfShape1
        | OfShape2 null OfString "EndsWith" null OfStringComparison (arg, v, comparisonExpr) ->
            let comparison = evaluateExpr<StringComparison> comparisonExpr
            if isIgnoreCase comparison then
                qb.AppendRaw "SUBSTR(TO_LOWER("
                visit arg qb
                qb.AppendRaw "),-LENGTH("
                visit v qb
                qb.AppendRaw ")) = TO_LOWER("
                visit v qb
                qb.AppendRaw ")"
            else
                qb.AppendRaw "SUBSTR("
                visit arg qb
                qb.AppendRaw ",-LENGTH("
                visit v qb
                qb.AppendRaw ")) = "
                visit v qb

        | OfShape1 null OfString "EndsWith" null (arg, v) ->
            qb.AppendRaw "SUBSTR("
            visit arg qb
            qb.AppendRaw ","
            qb.AppendRaw "-LENGTH("
            visit v qb
            qb.AppendRaw "))"
            qb.AppendRaw " = "
            visit v qb

        // String.Equals(string, StringComparison) - instance method
        | OfShape2 null OfString "Equals" null OfStringComparison (arg, v, comparisonExpr) ->
            let comparison = evaluateExpr<StringComparison> comparisonExpr
            if isIgnoreCase comparison then
                qb.AppendRaw "TO_LOWER("
                visit arg qb
                qb.AppendRaw ") = TO_LOWER("
                visit v qb
                qb.AppendRaw ")"
            else
                visit arg qb
                qb.AppendRaw " = "
                visit v qb

        // String.Equals(string, string, StringComparison) - static method
        // For static methods with 3 args: o=args[0] (first string), arg1=args[1] (second string), arg2=args[2] (StringComparison)
        | OfShape2 null null "Equals" OfString OfStringComparison (arg1, arg2, comparisonExpr) when m.Method.DeclaringType = typeof<string> && m.Method.IsStatic ->
            let comparison = evaluateExpr<StringComparison> comparisonExpr
            if isIgnoreCase comparison then
                qb.AppendRaw "TO_LOWER("
                visit arg1 qb
                qb.AppendRaw ") = TO_LOWER("
                visit arg2 qb
                qb.AppendRaw ")"
            else
                visit arg1 qb
                qb.AppendRaw " = "
                visit arg2 qb

        // String.Compare(string, string, StringComparison) - static method
        // Returns: -1 if a < b, 0 if a = b, 1 if a > b
        // For static methods with 3 args: o=args[0] (first string), arg1=args[1] (second string), arg2=args[2] (StringComparison)
        | OfShape2 null null "Compare" OfString OfStringComparison (arg1, arg2, comparisonExpr) when m.Method.DeclaringType = typeof<string> && m.Method.IsStatic ->
            let comparison = evaluateExpr<StringComparison> comparisonExpr
            if isIgnoreCase comparison then
                qb.AppendRaw "(CASE WHEN TO_LOWER("
                visit arg1 qb
                qb.AppendRaw ") < TO_LOWER("
                visit arg2 qb
                qb.AppendRaw ") THEN -1 WHEN TO_LOWER("
                visit arg1 qb
                qb.AppendRaw ") > TO_LOWER("
                visit arg2 qb
                qb.AppendRaw ") THEN 1 ELSE 0 END)"
            else
                qb.AppendRaw "(CASE WHEN "
                visit arg1 qb
                qb.AppendRaw " < "
                visit arg2 qb
                qb.AppendRaw " THEN -1 WHEN "
                visit arg1 qb
                qb.AppendRaw " > "
                visit arg2 qb
                qb.AppendRaw " THEN 1 ELSE 0 END)"

        // String.IndexOf(string, StringComparison)
        | OfShape2 null OfString "IndexOf" null OfStringComparison (arg, v, comparisonExpr) ->
            let comparison = evaluateExpr<StringComparison> comparisonExpr
            if isIgnoreCase comparison then
                qb.AppendRaw "(INSTR(TO_LOWER("
                visit arg qb
                qb.AppendRaw "),TO_LOWER("
                visit v qb
                qb.AppendRaw ")) - 1)"
            else
                qb.AppendRaw "(INSTR("
                visit arg qb
                qb.AppendRaw ","
                visit v qb
                qb.AppendRaw ") - 1)"

        | OfShape0 null null "ToObject" o when typeof<JsonSerializator.JsonValue>.IsAssignableFrom m.Method.DeclaringType || m.Method.DeclaringType.FullName = "SoloDatabase.MongoDB.BsonDocument" ->
            let castToType = m.Method.GetGenericArguments().[0]
            castTo qb castToType o

        | OfShape0 null null "CastTo" arg ->
            let castToType = m.Method.GetGenericArguments().[0]
            castTo qb castToType arg

        // Numeric and floating-point Parse methods
        | OfShape0 null null "Parse" arg when
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
        | OfShape2 null OfString "Substring" null null (str, start, length) ->
            qb.AppendRaw "SUBSTR("
            visit str qb
            qb.AppendRaw ","
            // SQLite is 1-based, .NET is 0-based
            qb.AppendRaw "("
            visit start qb
            qb.AppendRaw " + 1)"
            qb.AppendRaw ","
            visit length qb
            qb.AppendRaw ")"

        | OfShape1 null OfString "Substring" null (str, start) ->
            qb.AppendRaw "SUBSTR("
            visit str qb
            qb.AppendRaw ","
            // SQLite is 1-based, .NET is 0-based
            qb.AppendRaw "("
            visit start qb
            qb.AppendRaw " + 1)"
            qb.AppendRaw ")"

        | OfShape1 null OfString "get_Chars" null (str, index) ->
            qb.AppendRaw "SUBSTR("
            visit str qb
            qb.AppendRaw ",("
            visit index qb
            qb.AppendRaw " + 1),1)"

        | OfShape1 null null "GetString" null (str, index) ->
            qb.AppendRaw "SUBSTR("
            visit str qb
            qb.AppendRaw ",("
            visit index qb
            qb.AppendRaw " + 1),1)"

        | OfShape0 null null "Count" _ when let t = m.Arguments.[0].Type in t.IsConstructedGenericType && t.GetGenericTypeDefinition() = typedefof<System.Linq.IGrouping<_,_>> ->
            qb.AppendRaw $"json_array_length({qb.TableNameDot}Value, '$.Items')"

        | OfShape0 null null "Items" _ when let t = m.Arguments.[0].Type in t.IsConstructedGenericType && t.GetGenericTypeDefinition() = typedefof<System.Linq.IGrouping<_,_>> ->
            qb.AppendRaw $"jsonb_extract({qb.TableNameDot}Value, '$.Items')"

        | OfShape0 null null "Invoke" _ when FSharp.Reflection.FSharpType.IsRecord m.Type ->
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

        // String.Trim() -> TRIM(x)
        | OfShape0 null OfString "Trim" str ->
            qb.AppendRaw "TRIM("
            visit str qb |> ignore
            qb.AppendRaw ")"

        // String.TrimStart() -> LTRIM(x)
        | OfShape0 null OfString "TrimStart" str ->
            qb.AppendRaw "LTRIM("
            visit str qb |> ignore
            qb.AppendRaw ")"

        // String.TrimEnd() -> RTRIM(x)
        | OfShape0 null OfString "TrimEnd" str ->
            qb.AppendRaw "RTRIM("
            visit str qb |> ignore
            qb.AppendRaw ")"

        // String.Replace(old, new) -> REPLACE(str, old, new)
        | OfShape2 null OfString "Replace" null null (str, oldValue, newValue) ->
            qb.AppendRaw "REPLACE("
            visit str qb |> ignore
            qb.AppendRaw ","
            visit oldValue qb |> ignore
            qb.AppendRaw ","
            visit newValue qb |> ignore
            qb.AppendRaw ")"

        // String.IsNullOrEmpty(str) -> (str IS NULL OR str = '')
        | OfShape0 null OfString "IsNullOrEmpty" str ->
            qb.AppendRaw "("
            visit str qb |> ignore
            qb.AppendRaw " IS NULL OR "
            visit str qb |> ignore
            qb.AppendRaw " = '')"


        // Regex.IsMatch(input, pattern) -> input REGEXP pattern
        | OfShape1 null OfString "IsMatch" (OfString) (input, pattern) when m.Method.DeclaringType = typeof<System.Text.RegularExpressions.Regex> ->
            visit input qb |> ignore
            qb.AppendRaw " REGEXP "
            visit pattern qb |> ignore

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
