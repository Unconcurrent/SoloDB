namespace SoloDatabase
module QueryTranslator =
    open System.Text
    open System.Linq.Expressions
    open System
    open System.Reflection
    open System.Collections.Generic
    open JsonFunctions
    open Utils
    open SoloDatabase.Types

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

    type private QueryBuilder = 
        {
            StringBuilder: StringBuilder
            Variables: Dictionary<string, obj>
            AppendRaw: string -> unit
            AppendVariable: obj -> unit
            RollBack: uint -> unit
            UpdateMode: bool
            TableNameDot: string
            JsonExtractSelfValue: bool
        }
        override this.ToString() = this.StringBuilder.ToString()

        static member New(sb: StringBuilder, variables: Dictionary<string, obj>, updateMode: bool, tableName) =
            let appendVariable (value: obj) =
                let name = $"VAR{Random.Shared.Next():X}{Random.Shared.Next():X}{Random.Shared.Next():X}{Random.Shared.Next():X}"
                match value with
                | :? bool as b -> 
                     sb.Append (sprintf "%i" (if b then 1 else 0)) |> ignore
                | other ->

                if isIntegerBased value then
                    sb.Append (sprintf "%s" (value.ToString())) |> ignore
                else
                
                let jsonValue, kind = toSQLJson value
                if kind then 
                    sb.Append (sprintf "jsonb(@%s)" name) |> ignore
                else 
                    sb.Append (sprintf "@%s" name) |> ignore
                variables.[name] <- jsonValue

            let appendRaw (s: string) = sb.Append s |> ignore

            {
                StringBuilder = sb
                Variables = variables
                AppendVariable = appendVariable
                AppendRaw = appendRaw
                RollBack = fun N -> sb.Remove(sb.Length - (int)N, (int)N) |> ignore
                UpdateMode = updateMode
                TableNameDot = tableName + "."
                JsonExtractSelfValue = true
            }

    let private evaluateExpr<'O> e =
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
        | :? ParameterExpression -> false
        | :? BinaryExpression as binExpr ->
            isAnyConstant binExpr.Left || isAnyConstant binExpr.Right
        | :? UnaryExpression as unaryExpr ->
            isAnyConstant unaryExpr.Operand
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
            let value = evaluateExpr exp
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

        | _ ->
            raise (Exception(sprintf "Unhandled expression type: '%O'" exp.NodeType))

    and private arrayIndex (array: Expression) (index: Expression) (qb: QueryBuilder) : unit =
        qb.AppendRaw "jsonb_extract("
        visit array qb |> ignore
        qb.AppendRaw ", '$["
        visit index qb |> ignore
        qb.AppendRaw "]')"
        ()

    and private visitMethodCall (m: MethodCallExpression) (qb: QueryBuilder) =        
        match m.Method.Name with
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
        
        | name when name = "AddToEnd" || name = "Add" ->
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

            (*qb.AppendRaw "jsonb_extract("
            visit o qb |> ignore
            if isIntegerBased property then
                qb.AppendRaw $",'$[{property}]')"
            else
                qb.AppendRaw $",'$.{property}')"*)
        
        | "AnyInEach" ->
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
        
        | "Dyn" ->
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
            let arg = 
                if m.Arguments.[0].Type = typeof<string> then
                    m.Arguments.[0]
                else if m.Arguments.[0].Type = typeof<char> && isFullyConstant m.Arguments.[0] then
                    let value = evaluateExpr<obj> (m.Arguments.[0])
                    Expression.Constant(value.ToString(), typeof<string>)
                else failwithf "Unknown %s method" m.Method.Name

            let likeMeth = Expression.Call(typeof<SoloDatabase.Extensions>.GetMethod(nameof(SoloDatabase.Extensions.Like)), m.Object, Expression.Call(typeof<String>.GetMethod("Concat", [|typeof<string>; typeof<string>|]), arg, Expression.Constant("%")))
            visit likeMeth qb
        
        | "EndsWith" when m.Object.Type = typeof<string> ->
            let arg = 
                if m.Arguments.[0].Type = typeof<string> then
                    m.Arguments.[0]
                else if m.Arguments.[0].Type = typeof<char> && isFullyConstant m.Arguments.[0] then
                    let value = evaluateExpr<obj> (m.Arguments.[0])
                    Expression.Constant(value.ToString(), typeof<string>)
                else failwithf "Unknown %s method" m.Method.Name

            let likeMeth = Expression.Call(typeof<SoloDatabase.Extensions>.GetMethod(nameof(SoloDatabase.Extensions.Like)), m.Object, Expression.Call(typeof<String>.GetMethod("Concat", [|typeof<string>; typeof<string>|]), Expression.Constant("%"), arg))
            visit likeMeth qb


        | _ -> 
            raise (NotSupportedException(sprintf "The method %s is not supported" m.Method.Name))

    and private visitLambda (m: LambdaExpression) (qb: QueryBuilder) =
        visit(m.Body) qb

    and private visitParameter (m: ParameterExpression) (qb: QueryBuilder) =
        if m.Type = typeof<SqlId> then
            qb.AppendRaw $"{qb.TableNameDot}Id"
        else if qb.UpdateMode then
            qb.AppendRaw $"'$'"
        else
            if qb.JsonExtractSelfValue then
                if qb.StringBuilder.Length = 0 
                then qb.AppendRaw $"json_extract(json({qb.TableNameDot}Value), '$')" // To avoit returning the jsonB format instead of normal json.
                else qb.AppendRaw $"jsonb_extract({qb.TableNameDot}Value, '$')"
            else
                qb.AppendRaw $"{qb.TableNameDot}Value"
        

    and private visitNot (m: UnaryExpression) (qb: QueryBuilder) =
        qb.AppendRaw "NOT "
        visit m.Operand qb |> ignore        

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
        else if isIntegerBasedType m.Type then // ignore cast like (int64)1
            visit(m.Operand) qb
        else failwithf "Convert not yet implemented: %A" m.Type

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
        else if m.ReturnType = typeof<SqlId> && m.MemberName = "Id" then
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
        | None -> failwithf "Unable to parse the member access."
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
        let builder = QueryBuilder.New(sb, variables, false, tableName)
    
        let e = visit expression builder
        sb.ToString(), variables

    let translateUpdateMode (tableName: string) (expression: Expression) =
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New(sb, variables, true, tableName)
    
        let e = visit expression builder
        sb.ToString(), variables