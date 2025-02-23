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
open JsonFunctions

type private QueryableBuilder<'T> = 
    {
        SQLiteCommand: StringBuilder
        Variables: Dictionary<string, obj>
        Source: Collection<'T>
    }
    member this.Append(text: string) =
        this.SQLiteCommand.Append text |> ignore

// Translation validation helpers.
type private SoloDBQueryProvider = interface end
type private IRootQueryable =
    abstract member SourceTableName: string

module private QueryHelper =
    let private escapeSQLiteString (input: string) : string =
        input.Replace("'", "''")

    let private translateSource<'T> (builder: QueryableBuilder<'T>) (root: IRootQueryable) =
        let sourceName = root.SourceTableName
        builder.Append "SELECT Id, \""
        builder.Append sourceName
        builder.Append "\".Value as Value FROM \""
        builder.Append sourceName
        builder.Append "\""

    let private translateSourceExpr<'T> (builder: QueryableBuilder<'T>) (rootExpr: Expression) =
        let root = QueryTranslator.evaluateExpr<IRootQueryable> rootExpr
        translateSource builder root

    let rec private aggregateTranslator (fnName: string) (builder: QueryableBuilder<'T>) (expression: MethodCallExpression) =
        builder.Append "SELECT "
        builder.Append fnName
        builder.Append "("

        match expression.Arguments.Count with
        | 1 -> QueryTranslator.translateQueryable "" (ExpressionHelper.get(fun x -> x)) builder.SQLiteCommand builder.Variables
        | 2 -> QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
        | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

        builder.Append ") as Value FROM ("

        translate builder expression.Arguments.[0]
        builder.Append ")"

    and private serializeForCollection (value: 'T) =
        match typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T> with
        | true -> JsonSerializator.JsonValue.SerializeWithType value
        | false -> JsonSerializator.JsonValue.Serialize value
        |> _.ToJsonString(), HasTypeId<'T>.Value

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
        | "CountBy"
        | "LongCount" ->
            builder.Append "SELECT COUNT(Id) as Value FROM ("

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
            | :? ConstantExpression as ce when typeof<IRootQueryable>.IsAssignableFrom ce.Type ->
                let tableName = (ce.Value :?> IRootQueryable).SourceTableName
                builder.Append "SELECT Id, \""
                builder.Append tableName
                builder.Append "\".Value as Value FROM \""
                
                builder.Append tableName
                builder.Append "\" WHERE "
                QueryTranslator.translateQueryable tableName expression.Arguments.[1] builder.SQLiteCommand builder.Variables
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
            | :? ConstantExpression as ce when typeof<IRootQueryable>.IsAssignableFrom ce.Type ->
                let tableName = (ce.Value :?> IRootQueryable).SourceTableName
                builder.Append "\""
                builder.Append tableName
                builder.Append "\""
            | other ->
                builder.Append "("
                translate builder other
                builder.Append ")"

        | "ThenBy" | "ThenByDescending" ->
            translate builder expression.Arguments.[0]
            builder.Append ","
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            if expression.Method.Name = "ThenByDescending" then
                builder.Append "DESC "

        | "OrderBy" | "OrderByDescending" ->
            // Remove all previous order by's, they are noop.
            let innerExpression = 
                let mutable expr = expression.Arguments.[0]
                while match expr with
                        | :? MethodCallExpression as mce -> mce.Method.Name = "OrderBy" || mce.Method.Name = "OrderByDescending" || mce.Method.Name = "ThenBy" || mce.Method.Name = "ThenByDescending"
                        | _other -> false 
                    do expr <- (expr :?> MethodCallExpression).Arguments.[0]
                expr

            translate builder innerExpression
            builder.Append " ORDER BY "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            if expression.Method.Name = "OrderByDescending" then
                builder.Append "DESC "

        | "Take" ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") LIMIT "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables

        | "Skip" ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") OFFSET "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables

        | "First" | "FirstOrDefault" ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") "
            match expression.Arguments.Count with
            | 1 -> () // Noop needed.
            | 2 -> 
                builder.Append "WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

            builder.Append "LIMIT 1 "

        | "Last" | "LastOrDefault" ->
            // SQlite does not guarantee the order of elements without an ORDER BY,
            // ensure that it is the only one, or throw an explanatory exception.
            match expression.Arguments.[0] with
            | :? ConstantExpression as ce when typeof<IRootQueryable>.IsAssignableFrom ce.Type ->
                translate builder expression.Arguments.[0]
                match expression.Arguments.Count with
                | 1 -> () // Noop needed.
                | 2 -> 
                    builder.Append "WHERE " 
                    QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
                | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

                builder.Append "ORDER BY Id DESC LIMIT 1 "
            | _other ->
                failwithf "Because SQLite does not guarantee the order of elements without an ORDER BY, this function cannot be implemented if it is not the only one applied(in this case it sorts by Id). Use an OrderBy() with a First'OrDefault'()"

        | "Single" ->
            let countVar = Utils.getRandomVarName()

            builder.Append "SELECT jsonb_extract(Encoded, '$.Id') as Id, jsonb_extract(Encoded, '$.Value') as Value FROM ("

            builder.Append "SELECT CASE WHEN "
            builder.Append countVar
            builder.Append " = 0 THEN (SELECT jsonb_object('Id', NULL, 'Value', '\"Sequence contains no elements\"'))"
            builder.Append "WHEN "
            builder.Append countVar
            builder.Append " > 1 THEN (SELECT jsonb_object('Id', NULL, 'Value', '\"Sequence contains more than one matching element\"')) ELSE ("

            builder.Append "SELECT jsonb_object('Id', Id, 'Value', Value) FROM ("

            match expression.Arguments.Count with
            | 1 -> translate builder expression.Arguments.[0]
            | 2 -> 
                builder.Append "SELECT Id, Value FROM ("
                translate builder expression.Arguments.[0]
                builder.Append ") WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

            
            builder.Append ")"

            builder.Append ") END as Encoded FROM "
            builder.Append "(SELECT COUNT(*) as "
            builder.Append countVar
            builder.Append " FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")"
            match expression.Arguments.Count with
            | 1 -> () // Noop needed.
            | 2 -> 
                builder.Append "WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other
            builder.Append "LIMIT 2)) WHERE Id IS NOT NULL OR Value IS NOT NULL"

        | "SingleOrDefault" ->
            let countVar = Utils.getRandomVarName()

            builder.Append "SELECT jsonb_extract(Encoded, '$.Id') as Id, jsonb_extract(Encoded, '$.Value') as Value FROM ("

            builder.Append "SELECT CASE WHEN "
            builder.Append countVar
            builder.Append " = 0 THEN"
            builder.Append "(SELECT 0 WHERE 0)"
            builder.Append "WHEN "
            builder.Append countVar
            builder.Append " > 1 THEN "
            builder.Append "(SELECT jsonb_object('Id', NULL, 'Value', '\"Sequence contains more than one matching element\"')) "
            builder.Append "ELSE ("

            builder.Append "SELECT jsonb_object('Id', Id, 'Value', Value) FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")"

            builder.Append ") END as Encoded FROM "
            builder.Append "(SELECT COUNT(*) as "
            builder.Append countVar
            builder.Append " FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")"
            match expression.Arguments.Count with
            | 1 -> () // Noop needed.
            | 2 -> 
                builder.Append "WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other
            builder.Append "LIMIT 2)) WHERE Id IS NOT NULL OR Value IS NOT NULL"

        | "All" ->
            builder.Append "SELECT COUNT(*) = (SELECT COUNT(*) FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")) as Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") WHERE "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables

        | "Any" ->
            builder.Append "SELECT EXISTS(SELECT 1 FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")) as Value"

        | "Contains" ->
            builder.Append "SELECT EXISTS(SELECT 1 FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") WHERE "

            let value = 
                match expression.Arguments.[1] with
                | :? ConstantExpression as ce -> ce.Value
                | other -> failwithf "Invalid Contains(...) parameter: %A" other

            QueryTranslator.translateQueryable "" (ExpressionHelper.get(fun x -> x = value)) builder.SQLiteCommand builder.Variables
            builder.Append ") as Value"

        | "Append" ->
            translate builder expression.Arguments.[0]
            builder.Append " UNION ALL "
            builder.Append "SELECT "
            let appendingObj = QueryTranslator.evaluateExpr<'T> expression.Arguments.[1]
            let jsonStringElement, hasId = serializeForCollection appendingObj
            match hasId with
            | false ->
                builder.Append "-1 as Id,"
            | true ->
                let id = HasTypeId<'T>.Read appendingObj
                builder.Append (sprintf "%i" id)
                builder.Append " as Id,"

            let escapedString = escapeSQLiteString jsonStringElement
            builder.Append "jsonb('"
            builder.Append escapedString
            builder.Append "')"
            builder.Append " As Value"

        | "Concat" ->
            translate builder expression.Arguments.[0]
            builder.Append " UNION ALL "
            builder.Append "SELECT Id, Value FROM ("

            let failwithMsg = "Cannot concat with an IEnumerable other that another SoloDB IQueryable, on the same connection. Do do this anyway, use to AsEnumerable()."
            let appendingQE =
                match expression.Arguments.[1] with
                | :? ConstantExpression as ce -> 
                    match QueryTranslator.evaluateExpr<IEnumerable> ce with
                    | :? IQueryable<'T> as appendingQuery when (match appendingQuery.Provider with :? SoloDBQueryProvider -> true | _other -> false) ->
                        appendingQuery.Expression
                    | :? IRootQueryable as rq ->
                        Expression.Constant(rq, typeof<IRootQueryable>)
                    | _other -> failwith failwithMsg
                | :? MethodCallExpression as mcl ->
                    mcl
                | _other -> failwith failwithMsg

            translate builder appendingQE
            builder.Append ")"

        | other -> failwithf "Queryable method not implemented: %s" other

    and private translateConstant (builder: QueryableBuilder<'T>) (expression: ConstantExpression) =
        if typedefof<IRootQueryable>.IsAssignableFrom expression.Type then
            translateSourceExpr builder expression
        else
            QueryTranslator.translateQueryable "" expression builder.SQLiteCommand builder.Variables

    and private translate (builder: QueryableBuilder<'T>) (expression: Expression) =
        match expression.NodeType with
        | ExpressionType.Call ->
            translateCall builder (expression :?> MethodCallExpression)
        | ExpressionType.Constant ->
            translateConstant builder (expression :?> ConstantExpression)
        | other -> failwithf "Could not translate Queryable expression of type: %A" other

    let internal doesNotReturnIdFn (expression: Expression) =
        match expression with
        | :? MethodCallExpression as mce ->
            match mce.Method.Name with
            | "Sum"
            | "Average"
            | "Min"
            | "Max"
            | "Count"
            | "LongCount"
            | "All"
            | "Any"
            | "Contains"
                -> true
            | _other -> false
        | _other -> false

    let internal startTranslation (source: Collection<'T>) (expression: Expression) =
        let builder = {
            SQLiteCommand = StringBuilder(256)
            Variables = Dictionary<string, obj>(16)
            Source = source
        }

        if doesNotReturnIdFn expression then
            builder.Append "SELECT -1 as Id, json_quote(Value) as ValueJSON FROM ("
            translate builder expression
            builder.Append ")"
        else
            builder.Append "SELECT Id, json_quote(Value) as ValueJSON FROM ("
            translate builder expression
            builder.Append ")"


        builder.SQLiteCommand.ToString(), builder.Variables


type internal SoloDBCollectionQueryProvider<'T>(source: Collection<'T>) =
    interface SoloDBQueryProvider
    member internal this.ExecuteEnumetable<'Elem> (query: string) (par: obj) : IEnumerable<'Elem> =
        seq {
            use connection = source.Connection.Get()
            yield! Seq.map JsonFunctions.fromSQLite<'Elem> (connection.Query<Types.DbObjectRow>(query, par))
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
            match typeof<'TResult> with
            | t when t.IsGenericType
                     && typedefof<IEnumerable<_>> = (typedefof<'TResult>) ->
                let elemType = t.GetGenericArguments().[0]
                let m = 
                    typeof<SoloDBCollectionQueryProvider<'T>>
                        .GetMethod(nameof(this.ExecuteEnumetable), BindingFlags.NonPublic ||| BindingFlags.Instance)
                        .MakeGenericMethod(elemType)
                m.Invoke(this, [|query; variables|]) :?> 'TResult
            | _other ->
                use connection = source.Connection.Get()
                match connection.Query<Types.DbObjectRow>(query, variables) |> Seq.tryHead with
                | None ->
                    match expression with
                    | :? MethodCallExpression as mce when mce.Method.Name.EndsWith "OrDefault" ->
                        Unchecked.defaultof<'TResult>
                    | _other -> failwithf "Sequence contains no elements"
                | Some row -> JsonFunctions.fromSQLite<'TResult> row
            

and internal SoloDbCollectionQueryable<'I, 'T>(provider: IQueryProvider, expression: Expression) =
    interface IOrderedQueryable<'T>

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

and [<Sealed>] private RootQueryable<'T>(c: Collection<'T>) =
    member val Source = c

    interface IRootQueryable with
        override this.SourceTableName = this.Source.Name

    interface IQueryable<'T> with
        member _.Provider = null
        member _.Expression = Expression.Constant(c)
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
    static member AsQueryable<'A>(collection: Collection<'A>) : IQueryable<'A> =
        SoloDbCollectionQueryable<'A, 'A>(SoloDBCollectionQueryProvider(collection), Expression.Constant(RootQueryable<'A>(collection))) :> IQueryable<'A>