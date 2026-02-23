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

module internal QueryTranslatorBase =
    /// <summary>
    /// Represents a member access expression in a more abstract way.
    /// This private type simplifies handling different forms of member access.
    /// </summary>
    type internal MemberAccess =
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

    type internal UpdateManyRelationTransform =
        | SetDBRefToId of PropertyPath: string * TargetType: Type * TargetId: int64
        | SetDBRefToNone of PropertyPath: string * TargetType: Type
        | AddDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
        | RemoveDBRefMany of PropertyPath: string * TargetType: Type * TargetId: int64
        | ClearDBRefMany of PropertyPath: string * TargetType: Type

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

    /// When set to ValueSome, QueryBuilder.New uses this shared context instead of constructing a fresh one.
    /// Set by Queryable.fs startTranslation at query start; cleared after query execution.
    let internal activeQueryContext = new System.Threading.ThreadLocal<QueryContext voption>(fun () -> ValueNone)

    /// <summary>
    /// A stateful builder for constructing a SQL query from an expression tree.
    /// </summary>
    type QueryBuilder =
        internal {
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
            /// <summary>Query source context for multi-source (JOIN) support. When Joins is empty, behavior is identical to pre-relation pipeline.</summary>
            SourceContext: QueryContext
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

        /// Create a scoped sub-builder for correlated subquery predicate translation.
        /// Shares StringBuilder + Variables (parameters go to the same query), but uses a different table name and lambda parameters.
        member internal this.ForSubquery(tableName: string, lambdaExpr: LambdaExpression) =
            { this with
                TableNameDot = if String.IsNullOrEmpty tableName then String.Empty else "\"" + tableName + "\"."
                Parameters = lambdaExpr.Parameters
                JsonExtractSelfValue = true
                UpdateMode = false
                IdParameterIndex = -1 }

        // Step1 allowlisted internal accessors for cross-file visitor split boundary.
        member internal this.GetSourceContext() = this.SourceContext
        member internal this.GetIdParameterIndex() = this.IdParameterIndex
        member internal this.IsUpdateMode() = this.UpdateMode
        member internal this.GetTableNameDot() = this.TableNameDot
        member internal this.AppendVariableBoxed(value: obj) = this.AppendVariable value
        member internal this.RollBackBy(n: uint) = this.RollBack n

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
            let sourceCtx =
                match activeQueryContext.Value with
                | ValueSome ctx -> ctx
                | ValueNone -> QueryContext.SingleSource(tableName)
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
                SourceContext = sourceCtx
            }

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

    /// Substitute lambda parameters with invocation arguments (beta-reduction helper).
    let internal inlineLambdaInvocation (lambda: LambdaExpression) (args: IReadOnlyList<Expression>) =
        if lambda.Parameters.Count <> args.Count then
            lambda.Body
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


    /// <summary>
    /// Generates SQL to compare a database value with a known .NET object.
    /// For primitive types, it performs a direct comparison.
    /// For complex types, it serializes the object to JSON and generates a deep comparison using jsonb_extract.
    /// </summary>
    /// <param name="qb">The query builder.</param>
    /// <param name="writeTarget">A function that writes the SQL for the database-side value (e.g., a column name).</param>
    /// <param name="targetType">The .NET type of the target value.</param>
    /// <param name="knownObject">The .NET object to compare against.</param>
    let inline internal compareKnownJson (qb: QueryBuilder) (writeTarget: QueryBuilder -> unit) (targetType: Type) (knownObject: obj) =
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
    let inline internal OfIEnum () = Unchecked.defaultof<IEnumerable>
    let inline internal OfString () = Unchecked.defaultof<string>
    let inline internal OfType () = Unchecked.defaultof<Type>
    let inline internal OfPropInfo () = Unchecked.defaultof<PropertyInfo>
    let inline internal OfValueType () = Unchecked.defaultof<ValueType>
    let inline internal OfStringComparison () = Unchecked.defaultof<StringComparison>

    /// Returns true if the StringComparison is case-insensitive.
    let inline internal isIgnoreCase (comparison: StringComparison) =
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
    let internal runHandler (handler: List<Func<QueryBuilder, Expression, bool>>) (qb: QueryBuilder) (exp: Expression) =
        let mutable index = handler.Count - 1
        let mutable handled = false
        while index >= 0 && not handled do
            handled <- handler.[index].Invoke(qb, exp)
            index <- index - 1
        handled

    /// Helper for nested array predicate methods (Any, All).
    /// isAll=false: EXISTS (SELECT 1 FROM json_each(...) WHERE predicate)
    /// isAll=true:  NOT EXISTS (SELECT 1 FROM json_each(...) WHERE NOT (predicate))
    let internal visitNestedArrayPredicateHelper
            (visitFn: Expression -> QueryBuilder -> unit)
            (qb: QueryBuilder)
            (array: Expression)
            (whereFuncExpr: Expression)
            (isAll: bool) =
        // Extract the lambda expression from the argument.
        // It may be: 1) A Quote containing a lambda, 2) A lambda directly, 3) A constant with delegate.
        let expr =
            match whereFuncExpr with
            | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
                // Quoted lambda (from Queryable methods).
                ue.Operand
            | :? LambdaExpression as le ->
                // Direct lambda.
                le :> Expression
            | _ ->
                // Try the original approach for backwards compatibility.
                let exprFunc = Expression.Lambda<Func<Expression>>(whereFuncExpr).Compile(true)
                exprFunc.Invoke()

        if isAll then qb.AppendRaw "NOT "
        qb.AppendRaw "EXISTS (SELECT 1 FROM json_each("

        do
            let qb = if qb.TableNameDot = "" then {qb with TableNameDot = "o."} else qb
            visitFn array qb

        qb.AppendRaw ") WHERE "
        if isAll then qb.AppendRaw "NOT ("

        let innerQb = {qb with TableNameDot = "json_each."; JsonExtractSelfValue = false}
        visitFn expr innerQb

        if isAll then qb.AppendRaw ")"
        qb.AppendRaw ")"

    let rec private buildJsonPathFromMemberExpression (expr: Expression) (accum: string list) : string list =
        match expr with
        | :? MemberExpression as inner ->
            let currentField = inner.Member.Name
            buildJsonPathFromMemberExpression inner.Expression (currentField :: accum)
        | _ -> accum

    let private formatMemberAccessPath (qb: QueryBuilder) (path: string) =
        let path = path.Replace(".[", "[") // Replace array access.
        if qb.UpdateMode then sprintf "'$.%s'" path
        else sprintf "jsonb_extract(%sValue, '$.%s')" qb.TableNameDot path

    let internal tryHandleCollectionOrGroupingMemberAccess
            (visitFn: Expression -> QueryBuilder -> unit)
            (m: MemberAccess)
            (qb: QueryBuilder) : bool =
        if m.MemberName = "Length" && (* All cases below implement IEnumerable*) m.InputType.GetInterface (typeof<IEnumerable>.FullName) <> null then
            if m.InputType = typeof<string> then
                qb.AppendRaw "length("
                visitFn m.Expression qb
                qb.AppendRaw ")"
            elif m.InputType = typeof<byte array> then
                qb.AppendRaw "length(base64("
                visitFn m.Expression qb
                qb.AppendRaw "))"
            else
                // json len
                qb.AppendRaw "json_array_length("
                visitFn m.Expression qb
                qb.AppendRaw ")"
            true
        elif m.MemberName = "Count" && m.InputType.GetInterface (typeof<IEnumerable>.FullName) <> null then
            qb.AppendRaw "json_array_length("
            visitFn m.Expression qb
            qb.AppendRaw ")"
            true
        elif typedefof<System.Linq.IGrouping<_,_>>.IsAssignableFrom (m.InputType) then
            match m.MemberName with
            | "Key" -> qb.AppendRaw "jsonb_extract(Value, '$.Key')"
            | other ->
                qb.AppendRaw "jsonb_extract(Value, '$.Items."
                qb.AppendRaw (escapeSQLiteString other)
                qb.AppendRaw "')"
            true
        elif (m.ReturnType = typeof<int64> && m.MemberName = "Id") || (m.MemberName = "Id" && m.Expression.NodeType = ExpressionType.Parameter && m.Expression.Type.FullName = "SoloDatabase.JsonSerializator.JsonValue") then
            qb.AppendRaw $"{qb.TableNameDot}Id " |> ignore
            true
        else
            false

    let internal tryHandleRootParameterMemberAccess (m: MemberAccess) (qb: QueryBuilder) : bool =
        if m.Expression <> null && isRootParameter m.Expression then
            let jsonPath = buildJsonPathFromMemberExpression m.Expression [m.MemberName]
            match jsonPath with
            | [] -> ()
            | [single] ->
                qb.AppendRaw(formatMemberAccessPath qb single) |> ignore
            | paths ->
                let pathStr = String.concat "." (List.map (sprintf "%s") paths)
                qb.AppendRaw(formatMemberAccessPath qb pathStr) |> ignore
            true
        else
            false

    let internal emitFallbackMemberAccess
            (visitFn: Expression -> QueryBuilder -> unit)
            (m: MemberAccess)
            (qb: QueryBuilder) : unit =
        match m.OriginalExpression with
        | None ->
            qb.AppendRaw "jsonb_extract("
            visitFn m.Expression qb
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
    /// The main recursive visitor function that traverses the expression tree.
    /// It dispatches to specific visit methods based on the expression's NodeType.
    /// </summary>
    /// <param name="exp">The expression to visit and translate.</param>
    /// <param name="qb">The query builder state.</param>
