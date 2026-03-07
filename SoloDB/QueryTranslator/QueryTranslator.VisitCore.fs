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

module internal QueryTranslatorVisitCore =
    let rec internal visit (exp: Expression) (qb: QueryBuilder) : unit =
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
            | other -> raise (NotSupportedException(sprintf "Unknown array index expression of: %A" other))
        | ExpressionType.Index ->
            let exp = exp :?> IndexExpression
            if exp.Arguments.Count <> 1 then raise (NotSupportedException("The SQL translator does not support multiple args indexes."))
            let arge = exp.Arguments.[0]
            if not (isFullyConstant arge) then raise (NotSupportedException("The SQL translator does not support non constant index arg."))
            

            if arge.Type = typeof<string> then
                let arg = evaluateExpr<string> arge
                match arg with
                | "Id" when isRootParameter exp.Object -> qb.AppendRaw $"{qb.TableNameDot}Id"
                | arg ->
                visitProperty exp.Object arg ({new Expression() with member this.Type = exp.Object.Type}) qb
            else arrayIndex exp.Object arge qb
        | ExpressionType.Quote ->
            visit (exp :?> UnaryExpression).Operand qb
        | ExpressionType.Invoke ->
            let invocation = exp :?> InvocationExpression
            match invocation.Expression with
            | :? LambdaExpression as le when le.Parameters.Count = invocation.Arguments.Count ->
                visit (inlineLambdaInvocation le invocation.Arguments) qb
            | _ when invocation.CanReduce ->
                visit (invocation.Reduce()) qb
            | _ when isFullyConstant exp ->
                qb.AppendVariable (evaluateExpr<obj> exp)
            | _ ->
                raise (NotSupportedException(
                    "Error: Invoke expression is not reducible.\nReason: The invoked expression cannot be inlined or treated as a constant for SQL translation.\nFix: Inline the lambda or move the invocation after AsEnumerable()."))
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
            raise (NotSupportedException(sprintf "Cannot translate TypeIs expression, because the DB will not store its type information for %A" exp.Type))

        qb.AppendRaw "json_extract("
        do  visit exp.Expression qb
            qb.AppendRaw ','
            qb.AppendRaw "'$.$type'"
        qb.AppendRaw ')'
        qb.AppendRaw '='
        match exp.TypeOperand |> typeToName with
        | None -> raise (NotSupportedException(sprintf "Cannot translate TypeIs expression with the TypeOperand: %A" exp.TypeOperand))
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
        | _other -> raise (NotSupportedException("The index of the array must always be a constant value."))
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

        | _other -> raise (NotSupportedException("Unable to translate property access."))

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

        match m with
        | _ when m.Method.Name = "Invoke" && not (FSharp.Reflection.FSharpType.IsRecord m.Type) ->
            let rec stripConvert (expr: Expression) =
                match expr with
                | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> stripConvert ue.Operand
                | _ -> expr

            let targetExpr =
                if isNull m.Object then null
                else stripConvert m.Object

            match targetExpr with
            | :? LambdaExpression as le when le.Parameters.Count = m.Arguments.Count ->
                visit (inlineLambdaInvocation le m.Arguments) qb
            | _ when isFullyConstant (m :> Expression) ->
                qb.AppendVariable (evaluateExpr<obj> (m :> Expression))
            | _ ->
                raise (NotSupportedException(
                    sprintf "Error: Method '%s' is not supported.\nReason: The method has no SQL translation.\nFix: Rewrite the query or call AsEnumerable() before using this method." m.Method.Name))

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

            | _ -> raise (NotSupportedException("Unable to translate property access."))

        // Nested array predicate methods (Any, All) - common platform
        | OfShape1 null null "Any" null (array, whereFuncExpr) ->
            visitNestedArrayPredicateHelper visit qb array whereFuncExpr false // Any = EXISTS

        | OfShape1 null null "All" null (array, whereFuncExpr) ->
            visitNestedArrayPredicateHelper visit qb array whereFuncExpr true  // All = NOT EXISTS ... NOT
                
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
                | _other -> raise (NotSupportedException(sprintf "Cannot access dynamic property of %A" o))

            visitProperty o property m qb

        | OfShape0 (OfString) null "Concat" _ ->          
            let len = m.Arguments.Count
            let args =
                if len = 1 && typeof<IEnumerable<string>>.IsAssignableFrom m.Arguments.[0].Type && m.Arguments.[0].NodeType = ExpressionType.NewArrayInit then
                    let array = m.Arguments.[0] :?> NewArrayExpression
                    array.Expressions
                else if len > 1 then          
                    m.Arguments
                else raise (NotSupportedException(sprintf "Unknown such concat function: %A" m.Method))

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
                | _ -> raise (NotSupportedException(sprintf "Unsupported Parse type: %A" m.Method.DeclaringType))

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
            raise (NotSupportedException(
                sprintf "Error: Method '%s' is not supported.\nReason: The method has no SQL translation.\nFix: Rewrite the query or call AsEnumerable() before using this method." m.Method.Name))

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
            raise (NotSupportedException(sprintf "Cannot construct new in SQL query %A" t))

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
        | ExpressionType.NotEqual ->
            if isAnyNull || involvesDBRefValueAccess left || involvesDBRefValueAccess right
            then qb.AppendRaw(" IS NOT ") else qb.AppendRaw(" <> ") |> ignore
        | ExpressionType.LessThan -> qb.AppendRaw(" < ") |> ignore
        | ExpressionType.LessThanOrEqual -> qb.AppendRaw(" <= ") |> ignore
        | ExpressionType.GreaterThan -> qb.AppendRaw(" > ") |> ignore
        | ExpressionType.GreaterThanOrEqual -> qb.AppendRaw(" >= ") |> ignore

        | ExpressionType.Add -> qb.AppendRaw(" + ") |> ignore
        | ExpressionType.Subtract -> qb.AppendRaw(" - ") |> ignore
        | ExpressionType.Multiply -> qb.AppendRaw(" * ") |> ignore
        | ExpressionType.Divide -> qb.AppendRaw(" / ") |> ignore
        | ExpressionType.Modulo -> qb.AppendRaw(" % ") |> ignore
        | _ ->
            raise (NotSupportedException(
                sprintf "Error: Binary operator '%O' is not supported.\nReason: The operator cannot be translated to SQL for this expression.\nFix: Simplify the expression or move it after AsEnumerable()." b.NodeType))
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
        if tryHandleCollectionOrGroupingMemberAccess visit m qb then ()
        elif tryHandleRootParameterMemberAccess m qb then ()
        else emitFallbackMemberAccess visit m qb
            

    // ─── DBRef relation query translation ────────────────────────────────────────
