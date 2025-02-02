module Queryable

open System.Linq
open System.Collections
open System.Collections.Generic
open Microsoft.Data.Sqlite
open System.Linq.Expressions
open System
open SoloDatabase
open System.Runtime.CompilerServices
open SQLiteTools
open Microsoft.FSharp.Reflection
open System.Data
open System.Reflection

let private concatVars(vars1: Dictionary<string, obj>) (vars2: Dictionary<string, obj>) =
    let d = Dictionary<string, obj>(vars1.Count + vars2.Count)
    for KeyValue(k, v) in vars1 do
        d.Add(k, v)
    for KeyValue(k, v) in vars2 do
        d.Add(k, v)
    d

let rec private translateCall (source: Collection<'T>) (expression: MethodCallExpression) (first: bool) =
    match expression.Method.Name with
    | "Sum" ->
        let selection, variables = 
            match expression.Arguments.Count with
            | 1 -> QueryTranslator.translate "" (ExpressionHelper.get(fun x -> x))
            | 2 -> QueryTranslator.translate "" expression.Arguments.[1]
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

        let sql, recVariables = translate source expression.Arguments.[0] false
        $"SELECT SUM({selection}) FROM ({sql})", concatVars variables recVariables
    | other -> failwithf "Queryable method not implemented: %s" other

and private translateConstant (source: Collection<'T>) (expression: ConstantExpression) (first: bool) =
    if expression.Type.Name.StartsWith "DummyQueryable" then
        if first then
            $"SELECT json(\"{source.Name}\".Value) as Value FROM \"{source.Name}\"", Dictionary()
        else
            $"SELECT \"{source.Name}\".Value as Value FROM \"{source.Name}\"", Dictionary()
    else
        QueryTranslator.translate "" expression

and private translate (source: Collection<'T>) (expression: Expression) (first: bool) =
    match expression.NodeType with
    | ExpressionType.Call ->
        translateCall source (expression :?> MethodCallExpression) (first)
    | ExpressionType.Constant ->
        translateConstant source (expression :?> ConstantExpression) (first)
    | other -> failwithf "Could not translate Queryable expression of type: %A" other

type internal SoloDBCollectionQueryProvider<'T>(source: Collection<'T>) =
    member internal this.ExecuteEnumetable<'Elem> (expression: Expression) (query: string) (par: obj) : IEnumerable<'Elem> =
        seq {
            use connection = source.Connection.Get()
            yield! SQLiteTools.query<'Elem> connection query par
        }

    interface IQueryProvider with
        member this.CreateQuery<'TResult>(expression: Expression) : IQueryable<'TResult> =
            SoloDbCollectionQueryable<'T, 'TResult>(this, expression)

        member this.CreateQuery(expression: Expression) : IQueryable =
            let elementType = expression.Type.GetGenericArguments().[0]
            let queryableType = typedefof<SoloDbCollectionQueryable<_,_>>.MakeGenericType(elementType)
            Activator.CreateInstance(queryableType, source, this, expression) :?> IQueryable

        member this.Execute(expression: Expression) : obj =
            (this :> IQueryProvider).Execute<IEnumerable<'T>>(expression)


        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let query, variables = translate source expression true
            let t = typeof<'TResult>
            match t with
            | _ when 
                    t.IsGenericType
                    && typedefof<IEnumerable<_>>.IsAssignableFrom (t.GetGenericTypeDefinition()) ->
                let elemType = t.GetGenericArguments().[0]
                let m = 
                    typeof<SoloDBCollectionQueryProvider<'T>>
                        .GetMethod(nameof(this.ExecuteEnumetable), BindingFlags.NonPublic ||| BindingFlags.Instance)
                        .MakeGenericMethod(elemType)
                m.Invoke(this, [|expression; query; variables|]) :?> 'TResult
            | _other ->
                use connection = source.Connection.Get()
                connection.QueryFirstOrDefault<'TResult>(query, variables)
            

and internal SoloDbCollectionQueryable<'I, 'T>(provider: IQueryProvider, expression: Expression) =
    interface IQueryable<'T> with
        member _.Provider = provider
        member _.Expression = expression
        member _.ElementType = typeof<'T>

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            let items = (provider.Execute<IEnumerable<'T>>(expression))
            items.GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

and [<Sealed>] private DummyQueryable<'T>() =
    static member internal Instance = DummyQueryable<'T>()

    interface IQueryable<'T> with
        member _.Provider = null
        member _.Expression = Expression.Constant(null)
        member _.ElementType = typeof<'T>

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            Enumerable.Empty<'T>().GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() =
            Enumerable.Empty<'T>().GetEnumerator()

[<Extension; AbstractClass; Sealed>]
type QueryableExtensions =
    [<Extension>]
    static member AsQueryable(collection: Collection<'A>) : IQueryable<'A> =
        SoloDbCollectionQueryable<'A, 'A>(SoloDBCollectionQueryProvider(collection), Expression.Constant(DummyQueryable<'A>.Instance))