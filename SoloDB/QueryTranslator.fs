module QueryTranslator

open System.Text
open System.Linq.Expressions
open System
open System.Reflection
open System.Collections.Generic
open System.Text.Json
open JsonUtils
open Utils
open SoloDbTypes

type private QueryBuilder = 
    {
        StringBuilder: StringBuilder
        Variables: Dictionary<string, obj>
        AppendRaw: string -> unit
        AppendVariable: obj -> unit
        RollBack: uint -> unit
        UpdateMode: bool
        TableNameDot: string
    }
    override this.ToString() = this.StringBuilder.ToString()

    static member New(sb: StringBuilder, variables: Dictionary<string, obj>, updateMode: bool, tableName) =
        let appendVariable (value: obj) =
            let name = $"VAR{Random.Shared.NextInt64():X}{Random.Shared.NextInt64():X}"
            match value with
            | :? bool as b -> 
                 sb.Append (sprintf "%i" (if b then 1 else 0)) |> ignore
            | other ->

            if isIntegerBased value then
                sb.Append (sprintf "%s" (value.ToString())) |> ignore
            else
                
            let jsonValue, kind = toSQLJsonAndKind value
            match kind with
            | JsonValueKind.Object
            | JsonValueKind.Array -> sb.Append (sprintf "jsonb(@%s)" name) |> ignore
            | other -> sb.Append (sprintf "@%s" name) |> ignore
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
        }

let rec stripQuotes (e: Expression) =
    let mutable expr = e
    while expr.NodeType = ExpressionType.Quote do
        expr <- (expr :?> UnaryExpression).Operand
    expr

let rec private visit (exp: Expression) (sb: QueryBuilder) : Expression =
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
    | ExpressionType.GreaterThanOrEqual ->
        visitBinary (exp :?> BinaryExpression) sb
    | ExpressionType.Not ->
        visitNot (exp :?> UnaryExpression) sb
    | ExpressionType.Lambda ->
        visitLambda (exp :?> LambdaExpression) sb
    | ExpressionType.Call ->
        visitMethodCall (exp :?> MethodCallExpression) sb
    | ExpressionType.Constant ->
        visitConstant (exp :?> ConstantExpression) sb
    | ExpressionType.MemberAccess ->
        visitMemberAccess (exp :?> MemberExpression) sb
    | ExpressionType.Convert ->
        visitConvert (exp :?> UnaryExpression) sb
    | ExpressionType.New ->
        visitNew (exp :?> NewExpression) sb
    | ExpressionType.Parameter ->
        visitParameter (exp :?> ParameterExpression) sb
    | _ ->
        raise (Exception(sprintf "Unhandled expression type: '%O'" exp.NodeType))

and private visitMethodCall (m: MethodCallExpression) (qb: QueryBuilder) =
    if m.Method.Name = "GetArray" then
        let array = m.Arguments[0]
        let index = m.Arguments[1]

        qb.AppendRaw "jsonb_extract("
        visit array qb |> ignore
        
        qb.AppendRaw ", '$["
        visit index qb |> ignore
        qb.AppendRaw "]')"
        m
    else if m.Method.Name = "Object.Like" then
        let string = m.Arguments[0]
        let likeWhat = m.Arguments[1]

        visit string qb |> ignore
        qb.AppendRaw " LIKE "
        visit likeWhat qb |> ignore
        m
    else if m.Method.Name = "Object.Set" then
        let oldValue = m.Arguments[0]
        let newValue = m.Arguments[1]

        visit oldValue qb |> ignore
        qb.AppendRaw ","
        visit newValue qb |> ignore
        qb.AppendRaw ","
        m
    else if m.Method.Name = "Array.Add" then
        let array = m.Arguments[0]
        let newValue = m.Arguments[1]

        // qb.AppendRaw "jsonb_extract("
        visit array qb |> ignore
        qb.RollBack 1u
        qb.AppendRaw "[#]',"
        
        visit newValue qb |> ignore
        qb.AppendRaw ","
        m
    else if m.Method.Name = "Array.SetAt" then        
        let array = m.Arguments[0]
        let index = m.Arguments[1]
        let newValue = m.Arguments[2]

        visit array qb |> ignore
        qb.RollBack 1u
        qb.AppendRaw $"[{index}]',"
        visit newValue qb |> ignore
        qb.AppendRaw ","
        m
    else if m.Method.Name = "Array.RemoveAt" then
        let array = m.Arguments[0]
        let index = m.Arguments[1]

        visit array qb |> ignore
        qb.AppendRaw $",jsonb_remove(jsonb_extract({qb.TableNameDot}Value,"
        visit array qb |> ignore
        qb.AppendRaw "),"
        
        qb.AppendRaw $"'$[{index}]'),"
        m
    else if m.Method.Name = "op_BarPlusBar" then
        let a = m.Arguments[0]
        let b = m.Arguments[1]


        visit a qb |> ignore
        visit b qb |> ignore
        m

    else if m.Method.Name = "op_Dynamic" then
        let o = m.Arguments[0]
        let property = (m.Arguments[1] :?> ConstantExpression).Value

        qb.AppendRaw "jsonb_extract("
        visit o qb |> ignore

        if isIntegerBased property then
            qb.AppendRaw $",'$[{property}]')"
        else
            qb.AppendRaw $",'$.{property}')"
        m
    else if m.Method.Name = "Array.AnyInEach" then
        let array = m.Arguments[0]
        let whereFuncExpr = (m.Arguments[1] :?> NewExpression)
        
        let exprFunc = Expression.Lambda<Func<InnerExpr>>(whereFuncExpr).Compile(true)
        let expr = exprFunc.Invoke()

        qb.AppendRaw $"EXISTS (SELECT 1 FROM json_each("
        visit array qb |> ignore
        qb.AppendRaw ") WHERE "
        let innerQb = {qb with TableNameDot = "json_each."}
        visit expr.Expression innerQb |> ignore
        qb.AppendRaw ")"
        m
    else if m.Method.Name = "QuotationToLambdaExpression" then
        let arg1 = (m.Arguments[0] :?> MethodCallExpression) // SubstHelper
        let arg2 = arg1.Arguments[0]
        visit arg2 qb
    else if m.Method.Name =  "op_Implicit" then
        let arg1 = (m.Arguments[0])
        visit arg1 qb
    else
        raise (NotSupportedException(sprintf "The method %s is not supported" m.Method.Name))

and private visitLambda (m: LambdaExpression) (qb: QueryBuilder) =
    visit(m.Body) qb

and private visitParameter (m: ParameterExpression) (qb: QueryBuilder) =
    if m.Type = typeof<SqlId> then
        qb.AppendRaw $"{qb.TableNameDot}Id"
    else if qb.UpdateMode then
        qb.AppendRaw $"'$'"
    else
        qb.AppendRaw $"{qb.TableNameDot}Value"
    m

and private visitNot (m: UnaryExpression) (qb: QueryBuilder) =
    qb.AppendRaw "NOT "
    visit m.Operand qb |> ignore
    m

and private visitNew (m: NewExpression) (qb: QueryBuilder) =
    let t = m.Type

    if t.FullName.StartsWith "System.Tuple" then
        qb.AppendRaw "json_object("
        for i, arg in m.Arguments |> Seq.indexed do
            qb.AppendRaw $"'Item{i + 1}',"
            visit(arg) qb |> ignore
            if m.Arguments.IndexOf arg <> m.Arguments.Count - 1 then
                qb.AppendRaw ","
        qb.AppendRaw ")"
        m
    else
        failwithf "Cannot construct new in SQL query %A" t

and private visitConvert (m: UnaryExpression) (qb: QueryBuilder) =
    if m.Type = typeof<obj> || m.Operand.Type = typeof<obj> then
        visit(m.Operand) qb
    else failwithf "Convert not yet implemented: %A" m.Type

and private visitBinary (b: BinaryExpression) (qb: QueryBuilder) =
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
    | ExpressionType.AndAlso -> qb.AppendRaw(" AND ")  |> ignore
    | ExpressionType.OrElse
    | ExpressionType.Or -> qb.AppendRaw(" OR ")  |> ignore
    | ExpressionType.Equal -> if isAnyNull then qb.AppendRaw(" IS ") else qb.AppendRaw(" = ")  |> ignore
    | ExpressionType.NotEqual -> if isAnyNull then qb.AppendRaw(" IS NOT ") else  qb.AppendRaw(" <> ")  |> ignore
    | ExpressionType.LessThan -> qb.AppendRaw(" < ")  |> ignore
    | ExpressionType.LessThanOrEqual -> qb.AppendRaw(" <= ")  |> ignore
    | ExpressionType.GreaterThan -> qb.AppendRaw(" > ")  |> ignore
    | ExpressionType.GreaterThanOrEqual -> qb.AppendRaw(" >= ") |> ignore
    | _ -> raise (NotSupportedException(sprintf "The binary operator %O is not supported" b.NodeType))
    visit(right) qb |> ignore
    qb.AppendRaw(")")  |> ignore
    b

and private visitConstant (c: ConstantExpression) (qb: QueryBuilder) =
    match c.Value with
    | null -> qb.AppendRaw("NULL")  |> ignore
    | _ ->
        qb.AppendVariable(c.Value) |> ignore
    c

and private visitMemberAccess (m: MemberExpression) (qb: QueryBuilder) =
    let rec buildJsonPath (expr: Expression) (accum: string list) : string list =
        match expr with
        | :? MemberExpression as inner ->
            let currentField = inner.Member.Name
            buildJsonPath inner.Expression (currentField :: accum)
        | _ -> accum

    let rec isRootParameter (expr: Expression) : bool =
        match expr with
        | :? ParameterExpression -> true
        | :? MemberExpression as inner -> isRootParameter inner.Expression
        | _ -> false

    let formatAccess (path) =
        if qb.UpdateMode then sprintf "'$.%s'" path
        else sprintf "jsonb_extract(%sValue, '$.%s')" qb.TableNameDot path
    if m.Expression <> null && isRootParameter m then
        let jsonPath = buildJsonPath m []
        match jsonPath with
        | [] -> ()
        | [single] ->
            qb.AppendRaw(formatAccess single) |> ignore
        | paths ->
            let pathStr = String.concat $"." (List.map (sprintf "%s") paths)
            qb.AppendRaw(formatAccess pathStr) |> ignore
    else if m.Expression = null then
        let value = (m.Member :?> PropertyInfo).GetValue null
        qb.AppendVariable value
    else
        raise (NotSupportedException(sprintf "The member access '%O' is not supported" m.Member.Name))

    m

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