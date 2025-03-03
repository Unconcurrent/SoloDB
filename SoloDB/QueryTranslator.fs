namespace SoloDatabase

module QueryTranslator =
    open System.Collections.ObjectModel
    open System.Text
    open System.Linq.Expressions
    open System
    open System.Reflection
    open System.Collections.Generic
    open JsonFunctions
    open Utils

    type private MemberAccess = 
        {
        Expression: Expression
        MemberName: string
        InputType: Type
        ReturnType: Type
        OriginalExpression: MemberExpression option
        }

        static member From(expr: MemberExpression) =
            {
                Expression = expr.Expression
                MemberName = expr.Member.Name
                InputType = expr.Expression.Type
                ReturnType = expr.Type
                OriginalExpression = expr |> Some
            }

    let internal appendVariable (sb: StringBuilder) (variables: #IDictionary<string, obj>) (value: obj) =
        let name = getRandomVarName ()
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
        if kind then 
            sb.Append (sprintf "jsonb(@%s)" name) |> ignore
        else 
            sb.Append (sprintf "@%s" name) |> ignore
        variables.[name] <- jsonValue

    type private QueryBuilder = 
        {
            StringBuilder: StringBuilder
            Variables: Dictionary<string, obj>
            AppendVariable: obj -> unit
            RollBack: uint -> unit
            UpdateMode: bool
            TableNameDot: string
            JsonExtractSelfValue: bool
            Parameters: ReadOnlyCollection<ParameterExpression>
            IdParameterIndex: int
        }
        member this.AppendRaw (s: string) =
            this.StringBuilder.Append s |> ignore

        member this.AppendRaw (s: char) =
            this.StringBuilder.Append s |> ignore

        override this.ToString() = this.StringBuilder.ToString()

        static member New(sb: StringBuilder)(variables: Dictionary<string, obj>)(updateMode: bool)(tableName)(expression: Expression)(idIndex: int) =
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
    /// The Math method name and its SQLite format. $N, where N in 1..9, are arguments.
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

    let internal evaluateExpr<'O> (e: Expression) =
        match e with
        | :? ConstantExpression as ce ->
            ce.Value :?> 'O
        | _other ->
        let exprFunc = Expression.Lambda<Func<'O>>(UnaryExpression.Convert(e, typeof<'O>)).Compile(true)
        exprFunc.Invoke()

    let rec private isRootParameter (expr: Expression) : bool =
        match expr with
        | :? ParameterExpression -> true
        | :? MemberExpression as inner -> isRootParameter inner.Expression
        | _ -> false

    let rec private tryRootConstant (expr: Expression) : ConstantExpression option =
        match expr with
        | :? ConstantExpression as e -> Some e
        | :? MemberExpression as inner -> tryRootConstant inner.Expression
        | _ -> None
            

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


    let rec private visit (exp: Expression) (qb: QueryBuilder) : unit =
        if exp.NodeType <> ExpressionType.Lambda && isFullyConstant exp && (match exp with | :? ConstantExpression as ce when ce.Value = null -> false | other -> true) then
            let value = evaluateExpr<obj> exp
            qb.AppendVariable value
        else

        match exp.NodeType with
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
        | ExpressionType.Not ->
            visitNot (exp :?> UnaryExpression) qb
        | ExpressionType.Negate
        | ExpressionType.NegateChecked ->
            visitNegate (exp :?> UnaryExpression) qb
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
        | _ ->
            raise (Exception(sprintf "Unhandled expression type: '%O'" exp.NodeType))

    and private arrayIndex (array: Expression) (index: Expression) (qb: QueryBuilder) : unit =
        qb.AppendRaw "jsonb_extract("
        visit array qb |> ignore
        qb.AppendRaw ", '$["
        visit index qb |> ignore
        qb.AppendRaw "]')"
        ()

    and private visitProperty (o: Expression) (property: obj)  (m: Expression) (qb: QueryBuilder) =
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

        | _other -> failwithf "Unable to translate property access."

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
            
        | "Set" ->
            let oldValue = m.Arguments.[0]
            let newValue = m.Arguments.[1]
            visit oldValue qb |> ignore
            qb.AppendRaw ","
            visit newValue qb |> ignore
            qb.AppendRaw ","
        
        | "Append" | "Add" ->
            let array = m.Arguments.[0]
            let newValue = m.Arguments.[1]
            visit array qb |> ignore
            qb.RollBack 1u
            qb.AppendRaw "[#]',"
            visit newValue qb |> ignore
            qb.AppendRaw ","
        
        | "SetAt" ->
            let array = m.Arguments.[0]
            let index = m.Arguments.[1]
            let newValue = m.Arguments.[2]
            visit array qb |> ignore
            qb.RollBack 1u
            qb.AppendRaw $"[{index}]',"
            visit newValue qb |> ignore
            qb.AppendRaw ","
        
        | "RemoveAt" ->
            let array = m.Arguments.[0]
            let index = m.Arguments.[1]
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
            visit array qb |> ignore
            qb.AppendRaw ") WHERE "
            let innerQb = {qb with TableNameDot = "json_each."; JsonExtractSelfValue = false}
            visit expr innerQb |> ignore
            qb.AppendRaw ")"
        
        | "QuotationToLambdaExpression" ->
            let arg1 = (m.Arguments.[0] :?> MethodCallExpression)
            let arg2 = arg1.Arguments.[0]
            visit arg2 qb
        
        | "op_Implicit" ->
            let arg1 = m.Arguments.[0]
            visit arg1 qb
        
        | "Contains" when m.Object.Type = typeof<string> ->
            let text = m.Object
            let what = m.Arguments.[0]
            qb.AppendRaw "instr("
            visit text qb |> ignore
            qb.AppendRaw ","
            visit what qb |> ignore
            qb.AppendRaw ") > 0"
        
        | "GetType" when m.Object.NodeType = ExpressionType.Parameter ->
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
        
        | "get_Item" when typeof<System.Collections.ICollection>.IsAssignableFrom m.Object.Type || typeof<Array>.IsAssignableFrom m.Object.Type || typeof<JsonSerializator.JsonValue>.IsAssignableFrom m.Object.Type ->
            let o = m.Object
            let property = (m.Arguments.[0] :?> ConstantExpression).Value

            visitProperty o property m qb

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
            qb.AppendRaw "concat("
            for i, arg in args |> Seq.indexed do
                visit arg qb |> ignore
                if i + 1 < len then qb.AppendRaw ", "

            qb.AppendRaw ")"

        | "StartsWith" when m.Object.Type = typeof<string> ->
            let arg = m.Object
            let v = m.Arguments.[0]

            qb.AppendRaw "SUBSTR("
            visit arg qb
            qb.AppendRaw ","
            qb.AppendRaw "1,"
            qb.AppendRaw "LENGTH("
            visit v qb
            qb.AppendRaw "))"
            qb.AppendRaw " = "
            visit v qb
                    
        | "EndsWith" when m.Object.Type = typeof<string> ->
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

        | _ -> 
            raise (NotSupportedException(sprintf "The method %s is not supported" m.Method.Name))

    and private visitLambda (m: LambdaExpression) (qb: QueryBuilder) =
        visit(m.Body) qb

    and private visitParameter (m: ParameterExpression) (qb: QueryBuilder) =
        if m.Type = typeof<int64> // Check if it is an id type.
            && ((qb.Parameters.IndexOf m = 0 // For the .SelectWithId function, it is always at index 0,
                && qb.Parameters.Count = 2 // and has 2 parameters the id and the item.
                )
                || qb.IdParameterIndex = qb.Parameters.IndexOf m // Or it is specified as an id.
                )
        then
            qb.AppendRaw $"{qb.TableNameDot}Id"
        else if qb.UpdateMode then
            qb.AppendRaw $"'$'"
        else
            if qb.JsonExtractSelfValue then
                if qb.StringBuilder.Length = 0 
                then qb.AppendRaw $"json_extract(json({qb.TableNameDot}Value), '$')" // To avoid returning the jsonB format instead of normal json.
                else qb.AppendRaw $"jsonb_extract({qb.TableNameDot}Value, '$')"
            else
                qb.AppendRaw $"{qb.TableNameDot}Value"
        

    and private visitNot (m: UnaryExpression) (qb: QueryBuilder) =
        qb.AppendRaw "NOT "
        visit m.Operand qb

    and private visitNegate (m: UnaryExpression) (qb: QueryBuilder) =
        qb.AppendRaw "-"
        visit m.Operand qb

    and private visitNew (m: NewExpression) (qb: QueryBuilder) =
        let t = m.Type

        if isTuple t then
            qb.AppendRaw "json_array("
            for i, arg in m.Arguments |> Seq.indexed do
                visit(arg) qb |> ignore
                if m.Arguments.IndexOf arg <> m.Arguments.Count - 1 then
                    qb.AppendRaw ","
            qb.AppendRaw ")"
        else
            failwithf "Cannot construct new in SQL query %A" t

    and private visitConvert (m: UnaryExpression) (qb: QueryBuilder) =
        if m.Type = typeof<obj> || m.Operand.Type = typeof<obj> then
            visit(m.Operand) qb
        else 
        castTo qb m.Type m.Operand

    and private visitBinary (b: BinaryExpression) (qb: QueryBuilder) =
        if b.NodeType = ExpressionType.Add && (b.Left.Type = typeof<string> || b.Right.Type = typeof<string>) then
            let expr = Expression.Call(typeof<String>.GetMethod("Concat", [|typeof<string seq>|]), Expression.NewArrayInit(typeof<string>, [|b.Left; b.Right|]))
            visit expr qb
        else

        let isLeftNull =
            match b.Left with
            | :? ConstantExpression as c when c.Value = null -> true
            | other -> false

        let isRightNull =
            match b.Right with
            | :? ConstantExpression as c when c.Value = null -> true
            | other -> false

        let isAnyNull = isLeftNull || isRightNull

        let left, right = if isLeftNull then (b.Right, b.Left) else (b.Left, b.Right)

        qb.AppendRaw("(") |> ignore
        visit(left) qb |> ignore
        match b.NodeType with
        | ExpressionType.And
        | ExpressionType.AndAlso -> qb.AppendRaw(" AND ") |> ignore
        | ExpressionType.OrElse
        | ExpressionType.Or -> qb.AppendRaw(" OR ")  |> ignore
        | ExpressionType.Equal -> if isAnyNull then qb.AppendRaw(" IS ") else qb.AppendRaw(" = ")  |> ignore
        | ExpressionType.NotEqual -> if isAnyNull then qb.AppendRaw(" IS NOT ") else  qb.AppendRaw(" <> ")  |> ignore
        | ExpressionType.LessThan -> qb.AppendRaw(" < ")  |> ignore
        | ExpressionType.LessThanOrEqual -> qb.AppendRaw(" <= ")  |> ignore
        | ExpressionType.GreaterThan -> qb.AppendRaw(" > ")  |> ignore
        | ExpressionType.GreaterThanOrEqual -> qb.AppendRaw(" >= ") |> ignore

        | ExpressionType.Add -> qb.AppendRaw(" + ") |> ignore
        | ExpressionType.Subtract -> qb.AppendRaw(" - ") |> ignore
        | ExpressionType.Multiply -> qb.AppendRaw(" * ") |> ignore
        | ExpressionType.Divide -> qb.AppendRaw(" / ") |> ignore
        | ExpressionType.Modulo -> qb.AppendRaw(" % ") |> ignore
        | _ -> raise (NotSupportedException(sprintf "The binary operator %O is not supported" b.NodeType))
        visit(right) qb |> ignore
        qb.AppendRaw(")")  |> ignore        

    and private visitConstant (c: ConstantExpression) (qb: QueryBuilder) =
        match c.Value with
        | null -> qb.AppendRaw("NULL")  |> ignore
        | _ ->
            qb.AppendVariable(c.Value) |> ignore        

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


        if m.MemberName = "Length" && (m.InputType = typeof<string> || m.InputType = typeof<byte array>) then
            qb.AppendRaw "length("
            visit m.Expression qb
            qb.AppendRaw ")"
        else if m.ReturnType = typeof<int64> && m.MemberName = "Id" then
            qb.AppendRaw $"{qb.TableNameDot}Id" |> ignore
        else if m.Expression <> null && isRootParameter m.Expression then
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
            

    let translate (tableName: string) (expression: Expression) =
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables false tableName expression -1
    
        visit expression builder
        sb.ToString(), variables

    let internal translateQueryable (tableName: string) (expression: Expression) (sb: StringBuilder) (variables: Dictionary<string, obj>) =
        let builder = QueryBuilder.New sb variables false tableName expression -1
        visit expression builder

    let internal translateWithId (tableName: string) (expression: Expression) idParameterIndex =
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables false tableName expression idParameterIndex
    
        visit expression builder
        sb.ToString(), variables

    let internal translateUpdateMode (tableName: string) (expression: Expression) =
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables true tableName expression -1
    
        visit expression builder
        sb.ToString(), variables