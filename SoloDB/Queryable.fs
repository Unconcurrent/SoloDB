namespace SoloDatabase

open System.Linq
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions
open System
open SoloDatabase
open System.Runtime.CompilerServices
open SQLiteTools
open System.Reflection
open System.Text
open JsonFunctions
open Utils

type internal SupportedLinqMethods =
| Sum
| Average
| Min
| Max
| Distinct
| DistinctBy
| Count
| CountBy
| LongCount
| Where
| Select
| ThenBy
| ThenByDescending
| OrderBy
| OrderByDescending
| Take
| Skip
| First
| FirstOrDefault
| DefaultIfEmpty
| Last
| LastOrDefault
| Single
| SingleOrDefault
| All
| Any
| Contains
| Append
| Concat
| GroupBy
| Except
| ExceptBy
| Intersect
| IntersectBy

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
    let private parseSupportedMethod (methodName: string) : SupportedLinqMethods option =
        match methodName with
        | "Sum" -> Some Sum
        | "Average" -> Some Average
        | "Min" -> Some Min
        | "Max" -> Some Max
        | "Distinct" -> Some Distinct
        | "DistinctBy" -> Some DistinctBy
        | "Count" -> Some Count
        | "CountBy" -> Some CountBy
        | "LongCount" -> Some LongCount
        | "Where" -> Some Where
        | "Select" -> Some Select
        | "ThenBy" -> Some ThenBy
        | "ThenByDescending" -> Some ThenByDescending
        | "OrderBy" -> Some OrderBy
        | "OrderByDescending" -> Some OrderByDescending
        | "Take" -> Some Take
        | "Skip" -> Some Skip
        | "First" -> Some First
        | "FirstOrDefault" -> Some FirstOrDefault
        | "DefaultIfEmpty" -> Some DefaultIfEmpty
        | "Last" -> Some Last
        | "LastOrDefault" -> Some LastOrDefault
        | "Single" -> Some Single
        | "SingleOrDefault" -> Some SingleOrDefault
        | "All" -> Some All
        | "Any" -> Some Any
        | "Contains" -> Some Contains
        | "Append" -> Some Append
        | "Concat" -> Some Concat
        | "GroupBy" -> Some GroupBy
        | "Except" -> Some Except
        | "ExceptBy" -> Some ExceptBy
        | "Intersect" -> Some Intersect
        | "IntersectBy" -> Some IntersectBy
        | _ -> None

    let private escapeSQLiteString (input: string) : string =
        input.Replace("'", "''")

    let private readSoloDBQueryable<'T> (methodArg: Expression) =
        let failwithMsg = "Cannot concat with an IEnumerable other that another SoloDB IQueryable, on the same connection. Do do this anyway, use to AsEnumerable()."
        match methodArg with
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

    and private raiseIfNullAggregateTranslator (fnName: string) (builder: QueryableBuilder<'T>) (expression: MethodCallExpression) (errorMsg: string) =
        builder.Append "SELECT "
        // In this case NULL is an invalid operation, therefore to emulate the .NET behavior of throwing an exception, we return the Id = NULL.
        // And downstream the pipeline it will be checked and throwed.
        builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN NULL ELSE -1 END AS Id, "
        builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN '"
        builder.Append (escapeSQLiteString errorMsg)
        builder.Append "' ELSE Value END AS Value "

        builder.Append "FROM ("
        aggregateTranslator fnName builder expression
        builder.Append ")"

    and private serializeForCollection (value: 'T) =
        match typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T> with
        | true -> JsonSerializator.JsonValue.SerializeWithType value
        | false -> JsonSerializator.JsonValue.Serialize value
        |> _.ToJsonString(), HasTypeId<'T>.Value

    and private translateCall (builder: QueryableBuilder<'T>) (expression: MethodCallExpression) =
        match parseSupportedMethod expression.Method.Name with
        | None -> failwithf "Queryable method not implemented: %s" expression.Method.Name
        | Some method ->
        match method with
        | Sum ->
            // SUM() return NULL if all elements are NULL, TOTAL() return 0.0.
            // TOTAL() always returns a float, therefore we will just check for NULL
            builder.Append "SELECT COALESCE(("
            aggregateTranslator "SUM" builder expression
            builder.Append "),0) as Value"

        | Average ->
            raiseIfNullAggregateTranslator "AVG" builder expression "Sequence contains no elements"

        | Min ->
            raiseIfNullAggregateTranslator "MIN" builder expression "Sequence contains no elements"

        | Max ->
            raiseIfNullAggregateTranslator "MAX" builder expression "Sequence contains no elements"

        | DistinctBy
        | Distinct ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") GROUP BY "

            match expression.Arguments.Count with
            | 1 -> QueryTranslator.translateQueryable "" (ExpressionHelper.get(fun x -> x)) builder.SQLiteCommand builder.Variables
            | 2 -> QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other
            builder.Append ", Id"

        // GroupBy<TSource,TKey>(IQueryable<TSource>, Expression<Func<TSource,TKey>>) implementation
        | GroupBy ->
            match expression.Arguments.Count with
            | 2 ->
                let keySelector = expression.Arguments.[1]
            
                builder.Append "SELECT -1 as Id, jsonb_object('Key', "
            
                QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
            
                // Create an array of all items with the same key
                builder.Append ", 'Items', jsonb_group_array(Value)) as Value FROM ("
            
                translate builder expression.Arguments.[0]
            
                // Group by the key selector
                builder.Append ") GROUP BY "
                QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables

            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

        | Count
        | CountBy
        | LongCount ->
            builder.Append "SELECT COUNT(Id) as Value FROM ("

            translate builder expression.Arguments.[0]
            builder.Append ")"
            match expression.Arguments.Count with
            | 1 -> () // Noop needed.
            | 2 -> 
                builder.Append "WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

        | Where ->
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

        | Select ->
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

        | ThenBy | ThenByDescending ->
            translate builder expression.Arguments.[0]
            builder.Append ","
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            if expression.Method.Name = "ThenByDescending" then
                builder.Append "DESC "

        | OrderBy | OrderByDescending ->
            // Remove all previous order by's, they are noop.
            let innerExpression = 
                let mutable expr = expression.Arguments.[0]
                while match expr with
                        | :? MethodCallExpression as mce -> mce.Method.Name = "OrderBy" || mce.Method.Name = "OrderByDescending" || mce.Method.Name = "ThenBy" || mce.Method.Name = "ThenByDescending"
                        | _other -> false 
                    do expr <- (expr :?> MethodCallExpression).Arguments.[0]
                expr

            builder.Append "SELECT Id, Value FROM ("
            translate builder innerExpression
            builder.Append ") ORDER BY "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            if expression.Method.Name = "OrderByDescending" then
                builder.Append "DESC "

        | Take ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") LIMIT "
            QueryTranslator.appendVariable builder.SQLiteCommand builder.Variables (QueryTranslator.evaluateExpr<obj> expression.Arguments.[1])

        | Skip ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") LIMIT -1 OFFSET "
            QueryTranslator.appendVariable builder.SQLiteCommand builder.Variables (QueryTranslator.evaluateExpr<obj> expression.Arguments.[1])

        | First | FirstOrDefault ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") "
            match expression.Arguments.Count with
            | 1 -> () // Noop needed.
            | 2 -> 
                builder.Append "WHERE " 
                QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

            builder.Append " LIMIT 1 "

        | DefaultIfEmpty ->
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") UNION ALL SELECT -1 as Id, "

            match expression.Arguments.Count with
            | 1 -> 
                // If no default value is provided, then provide .NET's default.

                let genericArg = (GenericMethodArgCache.Get expression.Method).[0]
                if genericArg.IsValueType then
                    let defaultValueType = Activator.CreateInstance(genericArg)
                    let jsonObj = JsonSerializator.JsonValue.Serialize defaultValueType
                    let jsonText = jsonObj.ToJsonString()
                    let escapedJsonText = escapeSQLiteString jsonText

                    builder.Append "jsonb('"
                    builder.Append escapedJsonText
                    builder.Append "')"
                else
                    builder.Append "jsonb(NULL)"

            | 2 -> 
                // If a default value is provided, return it when the result set is empty
                
                let o = QueryTranslator.evaluateExpr<obj> expression.Arguments.[1]
                let jsonObj = JsonSerializator.JsonValue.Serialize o
                let jsonText = jsonObj.ToJsonString()
                let escapedJsonText = escapeSQLiteString jsonText

                builder.Append "jsonb('"
                builder.Append escapedJsonText
                builder.Append "')"

            | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

            builder.Append " as Value WHERE NOT EXISTS (SELECT 1 FROM ("
            translate builder expression.Arguments.[0]
            builder.Append "))"

        | Last | LastOrDefault ->
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

        | Single ->
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

        | SingleOrDefault ->
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

        | All ->
            builder.Append "SELECT COUNT(*) = (SELECT COUNT(*) FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")) as Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") WHERE "
            QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables

        | Any ->
            builder.Append "SELECT EXISTS(SELECT 1 FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")) as Value"

        | Contains ->
            builder.Append "SELECT EXISTS(SELECT 1 FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ") WHERE "

            let value = 
                match expression.Arguments.[1] with
                | :? ConstantExpression as ce -> ce.Value
                | other -> failwithf "Invalid Contains(...) parameter: %A" other

            QueryTranslator.translateQueryable "" (ExpressionHelper.get(fun x -> x = value)) builder.SQLiteCommand builder.Variables
            builder.Append ") as Value"

        | Append ->
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

        | Concat ->
            translate builder expression.Arguments.[0]
            builder.Append " UNION ALL "
            builder.Append "SELECT Id, Value FROM ("

            let appendingQE = readSoloDBQueryable<'T> expression.Arguments.[1]

            translate builder appendingQE
            builder.Append ")"

        | Except ->
            if expression.Arguments.Count <> 2 then
                failwithf "Invalid number of arguments in %s: %A" expression.Method.Name expression.Arguments.Count

            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")"

            // Extract it, to convert it if it's binary encoded.
            builder.Append " WHERE jsonb_extract(Value, '$') NOT IN ("
                
            do  // Skip the Id selection here.
                builder.Append "SELECT jsonb_extract(Value, '$') FROM ("
                let exceptingQuery = readSoloDBQueryable<'T> expression.Arguments.[1]
                translate builder exceptingQuery
                builder.Append ")"

            builder.Append ")"

        | Intersect ->
            if expression.Arguments.Count <> 2 then
                failwithf "Invalid number of arguments in %s: %A" expression.Method.Name expression.Arguments.Count

            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")"

            // Extract it, to convert it if it's binary encoded.
            builder.Append " WHERE jsonb_extract(Value, '$') IN ("
                
            do  // Skip the Id selection here.
                builder.Append "SELECT jsonb_extract(Value, '$') FROM ("
                let exceptingQuery = readSoloDBQueryable<'T> expression.Arguments.[1]
                translate builder exceptingQuery
                builder.Append ")"

            builder.Append ")"

        | ExceptBy ->
            if expression.Arguments.Count <> 3 then
                failwithf "Invalid number of arguments in %s: %A" expression.Method.Name expression.Arguments.Count
            
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")"
            
            let keySelector = expression.Arguments.[2]
            
            builder.Append " WHERE "
            QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
            builder.Append " NOT IN ("
            
            do
                builder.Append "SELECT "
                QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
                builder.Append " FROM ("

                let exceptingQuery = readSoloDBQueryable<'T> expression.Arguments.[1]
                translate builder exceptingQuery
                builder.Append ")"
            builder.Append ")"
        
        | IntersectBy ->
            if expression.Arguments.Count <> 3 then
                failwithf "Invalid number of arguments in %s: %A" expression.Method.Name expression.Arguments.Count
            
            builder.Append "SELECT Id, Value FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")"
            
            let keySelector = expression.Arguments.[2]
            
            builder.Append " WHERE "
            QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
            builder.Append " IN ("
            
            do
                builder.Append "SELECT "
                QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
                builder.Append " FROM ("
                let intersectingQuery = readSoloDBQueryable<'T> expression.Arguments.[1]
                translate builder intersectingQuery
                builder.Append ")"
            builder.Append ")"


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
            match parseSupportedMethod mce.Method.Name with
            | None -> false
            | Some method ->
            match method with
            | Sum
            | Min
            | Max
            | Count
            | CountBy
            | LongCount
            | All
            | Any
            | Contains
                -> true
            | Average
            | Distinct
            | DistinctBy
            | Where
            | Select
            | ThenBy
            | ThenByDescending
            | OrderBy
            | OrderByDescending
            | Take
            | Skip
            | First
            | FirstOrDefault
            | DefaultIfEmpty
            | Last
            | LastOrDefault
            | Single
            | SingleOrDefault
            | Append
            | Concat
            | GroupBy
            | Except
            | ExceptBy
            | Intersect
            | IntersectBy
                -> false
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
                    | :? MethodCallExpression as mce when let name = mce.Method.Name in name.EndsWith "OrDefault" ->
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