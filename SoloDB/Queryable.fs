namespace SoloDatabase

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
open System.Text

type private QueryableBuilder<'T> = 
    {
        SQLiteCommand: StringBuilder
        Variables: Dictionary<string, obj>
        Source: Collection<'T>
    }
    member this.Append(text: string) =
        this.SQLiteCommand.Append text |> ignore


module private QueryHelper =
    let rec private aggregateTranslator (fnName: string) (builder: QueryableBuilder<'T>) (expression: MethodCallExpression) =
        builder.Append "SELECT "
        builder.Append fnName
        builder.Append "("

        match expression.Arguments.Count with
        | 1 -> QueryTranslator.translateQueryable "" (ExpressionHelper.get(fun x -> x)) builder.SQLiteCommand builder.Variables
        | 2 -> QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
        | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

        builder.Append ") FROM ("

        translate builder expression.Arguments.[0]
        builder.Append ")"

    and private translateCall (builder: QueryableBuilder<'T>) (expression: MethodCallExpression) =
        // todo: add test
        match expression.Method.Name with
        | "Sum" ->
            // SUM() return NULL if all elements are NULL, TOTAL() return 0.0.
            // TOTAL() always returns a float, therefore we will just check for NULL
            builder.Append "SELECT COALESCE(("
            aggregateTranslator "SUM" builder expression
            builder.Append "),0) as Value"

        | "Average" ->
            aggregateTranslator "AVG" builder expression

        | "Min" ->
            aggregateTranslator "MIN" builder expression

        | "Max" ->
            aggregateTranslator "MAX" builder expression

        | "DistinctBy"
        | "Distinct" ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") GROUP BY "

            match expression.Arguments.Count with
            | 1 -> QueryTranslator.translateQueryable "" (ExpressionHelper.get(fun x -> x)) builder.SQLiteCommand builder.Variables
            | 2 -> QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other
            builder.Append ", Id"

        | "Count"
        | "LongCount" ->
            builder.Append "SELECT COUNT(Id) FROM ("

            translate builder expression.Arguments.[0]
            builder.Append ")"
            match expression.Arguments.Count with
            | 1 -> () // Noop needed.
            | 2 -> 
                builder.Append "WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

        | "Where" ->
            match expression.Arguments.[0] with
            | :? ConstantExpression as ce when ce.Type.Name.StartsWith "RootQueryable" ->
                builder.Append "SELECT Id, \""
                builder.Append builder.Source.Name
                builder.Append "\".Value as Value FROM \""
                
                builder.Append builder.Source.Name
                builder.Append "\" WHERE "
                QueryTranslator.translateQueryable builder.Source.Name expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | _other ->
                builder.Append "SELECT Id, Value FROM ("

                translate builder expression.Arguments.[0]

                builder.Append ") WHERE "
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables

        | "Select" ->
            builder.Append "SELECT Id, "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            builder.Append " as Value FROM "
            match expression.Arguments.[0] with
            | :? ConstantExpression as ce when ce.Type.Name.StartsWith "RootQueryable" ->
                builder.Append "\""
                builder.Append builder.Source.Name
                builder.Append "\""
            | other ->
                builder.Append "("
                translate builder other
                builder.Append ")"

        | "OrderBy" | "OrderByDescending" ->
            translate builder expression.Arguments.[0]
            builder.Append " ORDER BY "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            if expression.Method.Name = "OrderByDescending" then
                builder.Append "DESC "

        | "Take" ->
            translate builder expression.Arguments.[0]
            builder.Append "LIMIT "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables

        | "Skip" ->
            translate builder expression.Arguments.[0]
            builder.Append "OFFSET "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables

        | "First" | "FirstOrDefault" ->
            translate builder expression.Arguments.[0]
            
            match expression.Arguments.Count with
            | 1 -> () // Noop needed.
            | 2 -> 
                builder.Append "WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

            builder.Append "LIMIT 1 "

        | "Last" | "LastOrDefault" ->
            translate builder expression.Arguments.[0]

            match expression.Arguments.Count with
            | 1 -> () // Noop needed.
            | 2 -> 
                builder.Append "WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

            builder.Append "ORDER BY Id DESC LIMIT 1 "

        | "All" ->
            builder.Append "SELECT COUNT(*) = (SELECT COUNT(*) FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")) FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") WHERE "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables

        | "Any" ->
            builder.Append "SELECT EXISTS(SELECT 1 FROM ("
            translate builder expression.Arguments.[0]
            builder.Append "))"

        | other -> failwithf "Queryable method not implemented: %s" other

    and private translateConstant (builder: QueryableBuilder<'T>) (expression: ConstantExpression) =
        if expression.Type.Name.StartsWith "RootQueryable" then
            builder.Append "SELECT Id, \""
            builder.Append builder.Source.Name
            builder.Append "\".Value as Value FROM \""
            builder.Append builder.Source.Name
            builder.Append "\""
        else
            QueryTranslator.translateQueryable "" expression builder.SQLiteCommand builder.Variables

    and private translate (builder: QueryableBuilder<'T>) (expression: Expression) =
        match expression.NodeType with
        | ExpressionType.Call ->
            translateCall builder (expression :?> MethodCallExpression)
        | ExpressionType.Constant ->
            translateConstant builder (expression :?> ConstantExpression)
        | other -> failwithf "Could not translate Queryable expression of type: %A" other

    let internal startTranslation (source: Collection<'T>) (expression: Expression) =
        let builder = {
            SQLiteCommand = StringBuilder()
            Variables = Dictionary<string, obj>()
            Source = source
        }

        if typeof<IEnumerable>.IsAssignableFrom expression.Type then
            builder.Append "SELECT CASE WHEN TYPEOF(Value) = 'blob' THEN json_extract(Value, '$') ELSE Value END FROM ("
            translate builder expression
            builder.Append ")"
        else
            translate builder expression


        builder.SQLiteCommand.ToString(), builder.Variables


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
            let query, variables = QueryHelper.startTranslation source expression
            let t = typeof<'TResult>
            match t with
            | _ when t.IsGenericType
                     && typedefof<IEnumerable<_>> = (typedefof<'TResult>) ->
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

and [<Sealed>] private RootQueryable<'T>() =
    static member internal Instance = RootQueryable<'T>()

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
        SoloDbCollectionQueryable<'A, 'A>(SoloDBCollectionQueryProvider(collection), Expression.Constant(RootQueryable<'A>.Instance))