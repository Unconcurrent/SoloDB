module QueryTranslator

open System.Text
open System.Linq.Expressions
open System.Linq
open System
open System.Reflection
open System.Collections.Generic
open System.Numerics

type private QueryBuilder = {
    StringBuilder: StringBuilder
    Variables: Dictionary<string, obj>
    AppendRaw: string -> unit
    AppendVariable: obj -> unit
    AppendJSONAccess: unit -> unit
    JSONAccess: string
    FavorJSON: bool
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
    | ExpressionType.Equal
    | ExpressionType.NotEqual
    | ExpressionType.LessThan
    | ExpressionType.LessThanOrEqual
    | ExpressionType.GreaterThan
    | ExpressionType.GreaterThanOrEqual ->
        visitBinary (exp :?> BinaryExpression) sb
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
    | _ ->
        raise (Exception(sprintf "Unhandled expression type: '%O'" exp.NodeType))

and private visitMethodCall (m: MethodCallExpression) (qb: QueryBuilder) =
    if m.Method.Name = "GetArray" then
        let array = m.Arguments[0]
        let index = m.Arguments[1]

        visit array qb |> ignore
        qb.AppendJSONAccess()
        visit index qb |> ignore
        m
    else if m.Method.Name = "String.Like" then
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


        visit array qb |> ignore
        qb.AppendJSONAccess()
        qb.AppendRaw "[#],"
        visit newValue qb |> ignore
        qb.AppendRaw ","
        m
    else
        raise (NotSupportedException(sprintf "The method %s is not supported" m.Method.Name))

and private visitLambda (m: LambdaExpression) (qb: QueryBuilder) =
    visit(m.Body) qb

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
    qb.AppendRaw("(") |> ignore
    visit(b.Left) qb |> ignore
    match b.NodeType with
    | ExpressionType.And
    | ExpressionType.AndAlso -> qb.AppendRaw(" AND ")  |> ignore
    | ExpressionType.Or -> qb.AppendRaw(" OR ")  |> ignore
    | ExpressionType.Equal -> qb.AppendRaw(" = ")  |> ignore
    | ExpressionType.NotEqual -> qb.AppendRaw(" <> ")  |> ignore
    | ExpressionType.LessThan -> qb.AppendRaw(" < ")  |> ignore
    | ExpressionType.LessThanOrEqual -> qb.AppendRaw(" <= ")  |> ignore
    | ExpressionType.GreaterThan -> qb.AppendRaw(" > ")  |> ignore
    | ExpressionType.GreaterThanOrEqual -> qb.AppendRaw(" >= ") |> ignore
    | _ -> raise (NotSupportedException(sprintf "The binary operator %O is not supported" b.NodeType))
    visit(b.Right) qb |> ignore
    qb.AppendRaw(")")  |> ignore
    b

and private visitConstant (c: ConstantExpression) (qb: QueryBuilder) =
    match c.Value with
    | :? IQueryable as q ->
        qb.AppendRaw("SELECT * FROM ")
        qb.AppendRaw(q.ElementType.Name)  |> ignore
    | null -> qb.AppendRaw("NULL")  |> ignore

    | :? uint32
    | :? uint64
    | :? int64
    | :? int32 ->
        qb.AppendRaw(sprintf "%A " c.Value)
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

    if m.Expression <> null && isRootParameter m then
        let jsonPath = buildJsonPath m []
        match jsonPath with
        | [] -> ()
        | [single] -> qb.AppendRaw(sprintf "Value %s '%s'" qb.JSONAccess single) |> ignore
        | paths ->
            let pathStr = String.concat $" {qb.JSONAccess} " (List.map (sprintf "'%s'") paths)
            qb.AppendRaw(sprintf "Value %s %s" qb.JSONAccess pathStr) |> ignore
    else if m.Expression = null then
        let value = (m.Member :?> PropertyInfo).GetValue null
        qb.AppendVariable value
    else
        raise (NotSupportedException(sprintf "The member access '%O' is not supported" m.Member.Name))

    m

let translate (expression: Expression) (favorJSON: bool) =
    let sb = StringBuilder()
    let variables = Dictionary<string, obj>()

    let appendVariable (value: obj) =
        let name = $"VAR{Random.Shared.NextInt64():X}{Random.Shared.NextInt64():X}"
        sb.Append ("@" + name) |> ignore
        variables.[name] <- value

    let appendRaw (s: string) = sb.Append s |> ignore

    let builder = {
        StringBuilder = sb
        Variables = variables
        AppendVariable = appendVariable
        AppendRaw = appendRaw
        AppendJSONAccess = fun () -> if favorJSON then appendRaw "->" else appendRaw "->>"
        JSONAccess = if favorJSON then "->" else "->>"
        FavorJSON = favorJSON
    }
    let e = visit expression builder
    sb.ToString(), variables
