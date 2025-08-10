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
open SoloDatabase.JsonSerializator

/// <summary>
/// An internal discriminated union that enumerates the LINQ methods supported by the query translator.
/// </summary>
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
| SelectMany
| ThenBy
| ThenByDescending
| Order
| OrderDescending
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
| Cast
| OfType
| Aggregate

// Translation validation helpers.
/// <summary>A marker interface for the SoloDB query provider.</summary>
type private SoloDBQueryProvider = interface end
/// <summary>A marker interface for the root of a queryable expression, identifying the source table.</summary>
type private IRootQueryable =
    /// <summary>Gets the name of the source database table for the query.</summary>
    abstract member SourceTableName: string

/// <summary>
/// Represents a preprocessed query where the expression tree has been flattened
/// into a simple chain of method calls starting at the root queryable.
/// </summary>
type private PreprocessedQuery =
| Method of {|
    Value: SupportedLinqMethods;
    OriginalMethod: MethodInfo;
    Expressions: Expression array;
    InnerQuery: PreprocessedQuery |}
| RootQuery of IRootQueryable

/// <summary>
/// Represents an ORDER BY statement captured during preprocessing.
/// </summary>
type private PreprocessedOrder = { 
    Selector: Expression;
    Descending: bool
}

type private SQLSelector =
| Expression of Expression
| Raw of (string -> StringBuilder -> Dictionary<string, obj> -> unit)

type private UsedSQLStatements = {
    /// May be more than 1, they will be concatinated together with (...) AND (...) SQL structure.
    Filters: Expression ResizeArray
    Orders: PreprocessedOrder ResizeArray
    mutable Selector: SQLSelector option
    // These will account for the fact that in SQLite, the OFFSET clause cannot be used independently without the LIMIT clause.
    mutable Skip: Expression option
    mutable Take: Expression option
    UnionAll: (string -> StringBuilder -> Dictionary<string, obj> -> unit) ResizeArray
    TableName: string
}

type private SQLSubquery =
    | Simple of UsedSQLStatements
    | Complex of (struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|} -> unit)


/// <summary>
/// A private, mutable builder used to construct an SQL query from a LINQ expression tree.
/// </summary>
/// <remarks>This type holds the state of the translation process, including the SQL command being built and any parameters.</remarks>
type private QueryableBuilder<'T> = 
    {
        /// The StringBuilder that accumulates the generated SQL string.
        SQLiteCommand: StringBuilder
        /// A dictionary of parameters to be passed to the SQLite command.
        Variables: Dictionary<string, obj>
        /// The source collection that the query operates on.
        Source: ISoloDBCollection<'T>
        /// This list will be using to know if it is necessary to create another SQLite subquery to finish the translation, by checking if all the available slots have been used.
        TypedSubqueries: ResizeArray<SQLSubquery>
    }
    /// <summary>Appends a string to the current SQL command.</summary>
    member this.Append(text: string) =
        ignore (this.SQLiteCommand.Append text)

    /// <summary>Appends a StringBuilder to the current SQL command.</summary>
    member this.Append(text: StringBuilder) =
        ignore (this.SQLiteCommand.Append text)

    /// <summary>Adds a variable to the parameters dictionary and appends its placeholder to the SQL command.</summary>
    member this.AppendVar(variable: obj) =
        ignore (QueryTranslator.appendVariable this.SQLiteCommand this.Variables variable)

/// <summary>
/// Contains private helper functions for translating LINQ expression trees to SQL.
/// </summary>
module private QueryHelper =
    /// <summary>
    /// Parses a method name string into a <c>SupportedLinqMethods</c> option.
    /// </summary>
    /// <param name="methodName">The name of the LINQ method.</param>
    /// <returns>A Some value if the method is supported, otherwise None.</returns>
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
        | "SelectMany" -> Some SelectMany
        | "ThenBy" -> Some ThenBy
        | "ThenByDescending" -> Some ThenByDescending
        | "OrderBy" -> Some OrderBy
        | "Order" -> Some Order
        | "OrderDescending" -> Some OrderDescending
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
        | "Cast" -> Some Cast
        | "OfType" -> Some OfType
        | "Aggregate" -> Some Aggregate
        | _ -> None

    /// <summary>
    /// Determines the appropriate SQL to select a value, extracting it from JSON if the type is not a primitive SQLite type.
    /// </summary>
    /// <param name="x">The .NET type of the value being selected.</param>
    /// <returns>An SQL string snippet for selecting the value.</returns>
    let private extractValueAsJsonIfNecesary (x: Type) =
        let isPrimitive = QueryTranslator.isPrimitiveSQLiteType x

        if isPrimitive then
            "Value "
        else
            if (*unknown type*)
               x = typeof<obj> || typeof<JsonValue>.IsAssignableFrom x then
                "CASE WHEN typeof(Value) = 'blob' THEN json_extract(Value, '$') ELSE Value END "
            else
                "json_extract(Value, '$') "

    let emptySQLStatement () =
        { Filters = ResizeArray(4); Orders = ResizeArray(1); Selector = None; Skip = None; Take = None; TableName = ""; UnionAll = ResizeArray(0) }

    let addFilter (statements: ResizeArray<SQLSubquery>) (filter: Expression) =
        let last = statements.Last()
        match last with
        | Simple last ->
            match last.Selector with
            | Some _ -> 
                (statements.Add << Simple) { emptySQLStatement () with Filters = ResizeArray [ filter ] }
            | None ->
                // If we don't have a selector, we can just add the filter to the current statement
                last.Filters.Add filter
        | Complex _ ->
            (statements.Add << Simple) { emptySQLStatement () with Filters = ResizeArray [ filter ] }

    let addSelector (statements: ResizeArray<SQLSubquery>) (selector: SQLSelector) =
        let last = statements.Last()
        match last with
        | Simple last ->
            match last.Selector with
            | Some _ -> 
                (statements.Add << Simple) { emptySQLStatement () with Selector = Some selector }
            | None ->
                last.Selector <- Some selector
        | Complex _ ->
            (statements.Add << Simple) { emptySQLStatement () with Selector = Some selector }

    let addComplex (statements: ResizeArray<SQLSubquery>) (writeFunc: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|} -> unit) =
        statements.Add (Complex writeFunc)

    /// <summary>
    /// Recursively translates a LINQ expression tree into an SQL query.
    /// This is the main dispatcher for the translation process.
    /// </summary>
    /// <param name="builder">The query builder instance that accumulates the SQL and parameters.</param>
    /// <param name="expression">The expression to translate.</param>
    let private aggregateTranslator (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (Raw (fun tableName builder vars ->
            builder.Append fnName |> ignore
            builder.Append "(" |> ignore

            match args.Length with
            | 0 -> QueryTranslator.translateQueryable tableName (method.ReturnType |> ExpressionHelper.id) builder vars
            | 1 -> QueryTranslator.translateQueryable tableName args.[0] builder vars
            | other -> failwithf "Invalid number of arguments in %s: %A" method.Name other

            builder.Append ") " |> ignore
        ))

    let private zeroIfNullAggregateTranslator (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (Raw (fun tableName builder vars ->
            builder.Append "COALESCE(" |> ignore
            builder.Append fnName |> ignore
            builder.Append "(" |> ignore

            match args.Length with
            | 0 -> QueryTranslator.translateQueryable tableName (method.ReturnType |> ExpressionHelper.id) builder vars
            | 1 -> QueryTranslator.translateQueryable tableName args.[0] builder vars
            | other -> failwithf "Invalid number of arguments in %s: %A" method.Name other

            builder.Append "),0) " |> ignore
        ))

    /// <summary>
    /// Extracts the expression for a collection that is an argument to a set-based method like Concat or Except.
    /// </summary>
    /// <param name="methodArg">The method argument expression.</param>
    /// <returns>The expression representing the queryable collection.</returns>
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

    let private addUnionAll (queries: SQLSubquery ResizeArray) (fn: string -> StringBuilder -> Dictionary<string, obj> -> unit) =
        match queries.Last() with
        | Simple last ->
            last.UnionAll.Add fn
        | Complex _ ->
            queries.Add (Simple { emptySQLStatement () with UnionAll = ResizeArray [ fn ] })

    /// <summary>
    /// Wraps an aggregate function translator to handle cases where the source sequence is empty, which would result in a NULL from SQL.
    /// This function generates SQL to return a specific error message that can be caught and thrown as an exception, mimicking .NET behavior.
    /// </summary>
    /// <param name="fnName">The name of the SQL aggregate function (e.g., "AVG", "MIN").</param>
    /// <param name="builder">The query builder instance.</param>
    /// <param name="expression">The LINQ method call expression for the aggregate.</param>
    /// <param name="errorMsg">The error message to return if the aggregation result is NULL.</param>
    let private raiseIfNullAggregateTranslator (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) (errorMsg: string) =
        aggregateTranslator fnName queries method args
        addSelector queries (Raw (fun tableName builder vars ->
            // In this case NULL is an invalid operation, therefore to emulate the .NET behavior 
            // of throwing an exception we return the Id = NULL, and Value = {exception message}
            // And downstream the pipeline it will be checked and throwed.
            builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN NULL ELSE -1 END AS Id, " |> ignore
            builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN '" |> ignore
            builder.Append (QueryTranslator.escapeSQLiteString errorMsg) |> ignore
            builder.Append "' ELSE Value END AS Value " |> ignore
        ))

    /// <summary>
    /// Preprocesses an expression tree into a <see cref="PreprocessedQuery"/> structure,
    /// flattening nested method calls so the translation stage can avoid generating
    /// redundant subqueries.
    /// </summary>
    let rec private preprocessQuery (expression: Expression) : PreprocessedQuery =
        match expression with
        | :? MethodCallExpression as mce ->
            match parseSupportedMethod mce.Method.Name with
            | Some value ->
                let inner = preprocessQuery mce.Arguments.[0]
                let exprs = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                Method {| Value = value; Expressions = exprs; InnerQuery = inner; OriginalMethod = mce.Method |}
            | None -> raise (NotSupportedException(sprintf "Queryable method not implemented: %s" mce.Method.Name))
        | :? ConstantExpression as ce when typeof<IRootQueryable>.IsAssignableFrom ce.Type ->
            RootQuery (ce.Value :?> IRootQueryable)
        | e -> raise (NotSupportedException(sprintf "Cannot preprocess expression of type %A: %A" e.NodeType e))

    let private serializeForCollection (value: 'T) =
        struct (
            match typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T> with
            | true -> JsonSerializator.JsonValue.SerializeWithType value
            | false -> JsonSerializator.JsonValue.Serialize value
            |> _.ToJsonString(), HasTypeId<'T>.Value
        )

    /// Appends WHERE, ORDER BY, LIMIT/OFFSET directly to the main builder.
    let private writeClauses
        (builder: QueryableBuilder<'T>)
        (statement: UsedSQLStatements)
        (contextTable: string) =
        
        // WHERE
        if statement.Filters.Count <> 0 then
            builder.Append(" WHERE ") |> ignore
            statement.Filters
            |> Seq.iteri (fun j f ->
                if j > 0 then builder.Append(" AND ") |> ignore
                // write predicates straight into the main command
                QueryTranslator.translateQueryable contextTable f builder.SQLiteCommand builder.Variables
            )

        // ORDER BY
        if statement.Orders.Count <> 0 then
            builder.Append(" ORDER BY ") |> ignore
            statement.Orders
            |> Seq.iteri (fun j o ->
                if j > 0 then builder.Append(", ") |> ignore
                QueryTranslator.translateQueryable contextTable o.Selector builder.SQLiteCommand builder.Variables
                if o.Descending then builder.Append(" DESC") |> ignore
            )

        // LIMIT / OFFSET
        match statement.Take, statement.Skip with
        | Some take, Some skip ->
            builder.Append(" LIMIT ") |> ignore
            builder.AppendVar(QueryTranslator.evaluateExpr<obj> take) |> ignore
            builder.Append(" OFFSET ") |> ignore
            builder.AppendVar(QueryTranslator.evaluateExpr<obj> skip) |> ignore
        | Some take, None ->
            builder.Append(" LIMIT ") |> ignore
            builder.AppendVar(QueryTranslator.evaluateExpr<obj> take) |> ignore
        | None, Some skip ->
            builder.Append(" LIMIT -1 OFFSET ") |> ignore
            builder.AppendVar(QueryTranslator.evaluateExpr<obj> skip) |> ignore
        | None, None -> ()

        if statement.UnionAll.Count <> 0 then
            // If there are UNION ALL statements, we need to append them to the main command.
            builder.Append(" UNION ALL ") |> ignore
            statement.UnionAll
            |> Seq.iteri (fun j unionAll ->
                if j > 0 then builder.Append(" UNION ALL ") |> ignore
                unionAll contextTable builder.SQLiteCommand builder.Variables
            )
            |> ignore

    let rec private collectPreprocessedQuery<'T> (builder: QueryableBuilder<'T>) (pq: PreprocessedQuery) =
        let statements = builder.TypedSubqueries
        statements.Add (emptySQLStatement () |> Simple)

        let rec collect (q: PreprocessedQuery) :string =
            match q with
            | RootQuery rq -> rq.SourceTableName
            | Method m ->
                let tableName = collect m.InnerQuery             

                let inline simpleCurrent() =
                    match statements.Last() with
                    | Simple s -> s
                    | Complex _ ->
                        let n = emptySQLStatement ()
                        statements.Add (Simple n)
                        n

                let inline ifSelectorNewStatement() =
                    match statements.Last() with
                    | Simple current ->
                        match current.Selector with
                        | Some _ ->
                            let n = emptySQLStatement ()
                            statements.Add (Simple n)
                            n
                        | None ->
                            current
                    | Complex _ ->
                        let n = emptySQLStatement ()
                        statements.Add (Simple n)
                        n

                match m.Value with
                | Where -> 
                    addFilter statements m.Expressions.[0]
                | Select ->
                    addSelector statements (Expression m.Expressions.[0])
                | OrderBy | Order ->
                    let current = ifSelectorNewStatement()
                    current.Orders.Clear()
                    current.Orders.Add({ Selector = m.Expressions.[0]; Descending = false })
                | OrderByDescending | OrderDescending ->
                    let current = ifSelectorNewStatement()
                    current.Orders.Clear()
                    current.Orders.Add({ Selector = m.Expressions.[0]; Descending = true })
                | ThenBy ->
                    let current = simpleCurrent()
                    current.Orders.Add({ Selector = m.Expressions.[0]; Descending = false })
                | ThenByDescending ->
                    let current = simpleCurrent()
                    current.Orders.Add({ Selector = m.Expressions.[0]; Descending = true })
                | Skip ->
                    let current = simpleCurrent()
                    match current.Skip with
                    | Some _ -> statements.Add(Simple { emptySQLStatement () with Skip = Some m.Expressions.[0] })
                    | None   -> current.Skip <- Some m.Expressions.[0]
                | Take ->
                    let current = simpleCurrent()
                    match current.Take with
                    | Some _ -> statements.Add(Simple { emptySQLStatement () with Take = Some m.Expressions.[0] })
                    | None   -> current.Take <- Some m.Expressions.[0]

                | Sum ->
                    // SUM() return NULL if all elements are NULL, TOTAL() return 0.0.
                    // TOTAL() always returns a float, therefore we will just check for NULL
                    zeroIfNullAggregateTranslator "SUM" statements m.OriginalMethod m.Expressions

                | Average ->
                    raiseIfNullAggregateTranslator "AVG" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | Min ->
                    raiseIfNullAggregateTranslator "MIN" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | Max ->
                    raiseIfNullAggregateTranslator "MAX" statements m.OriginalMethod m.Expressions "Sequence contains no elements"
                
                | Distinct 
                | DistinctBy ->
                    (*
                    Here is the old code:
                    builder.Append "SELECT Id, Value FROM ("
                    translate builder expression.Arguments.[0]
                    builder.Append ") o GROUP BY "

                    match expression.Arguments.Count with
                    | 1 -> QueryTranslator.translateQueryable "" (GenericMethodArgCache.Get expression.Method |> Array.head |> ExpressionHelper.id) builder.SQLiteCommand builder.Variables
                    | 2 -> QueryTranslator.translateQueryable "" expression.Arguments.[1] builder.SQLiteCommand builder.Variables
                    | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other
                    *)
                    addComplex statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT Id, Value FROM (" |> ignore
                        builder.WriteInnerWith()
                        builder.Command.Append ") o GROUP BY " |> ignore
                        match m.Expressions.Length with
                        | 0 -> QueryTranslator.translateQueryable builder.TableName (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) builder.Command builder.Vars
                        | 1 -> QueryTranslator.translateQueryable builder.TableName m.Expressions.[0] builder.Command builder.Vars
                        | other -> failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other
                    )

                | GroupBy ->
                    addComplex statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT -1 as Id, json_object('Key', " |> ignore
                        QueryTranslator.translateQueryable builder.TableName m.Expressions.[0] builder.Command builder.Vars
                        // Create an array of all items with the same key
                        builder.Command.Append ", 'Items', json_group_array(Value)) as Value FROM (" |> ignore
                        builder.WriteInnerWith()
                        // Group by the key selector
                        builder.Command.Append ") o GROUP BY " |> ignore
                        QueryTranslator.translateQueryable builder.TableName m.Expressions.[0] builder.Command builder.Vars
                    )

                | Count
                | CountBy
                | LongCount ->
                    (*
                    builder.Append "SELECT COUNT(Id) as Value FROM "

                    match expression.Arguments.Count with
                    | 1 -> 
                        // Only select Id from the table if possible
                        match expression.Arguments.[0] with
                        | :? ConstantExpression as ce when (ce.Value :? IRootQueryable) ->
                            let tableName = (ce.Value :?> IRootQueryable).SourceTableName
                            builder.Append "\""
                            builder.Append tableName
                            builder.Append "\""
                        | other ->
                            builder.Append "("
                            translate builder other
                            builder.Append ") o"
                    | 2 -> 
                        builder.Append "("
                        translateWhereStatement translate builder expression.Arguments.[0] expression.Arguments.[1]
                        builder.Append ") o"

                    | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other
                    *)

                    addComplex statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT COUNT(Id) as Value FROM " |> ignore
                        match m.Expressions.Length with
                        | 0 -> 
                            // Only select Id from the table if possible
                            match m.Expressions.[0] with
                            | :? ConstantExpression as ce when (ce.Value :? IRootQueryable) ->
                                let tableName = (ce.Value :?> IRootQueryable).SourceTableName
                                builder.Command.Append "\"" |> ignore
                                builder.Command.Append tableName |> ignore
                                builder.Command.Append "\"" |> ignore
                            | other ->
                                builder.Command.Append "(" |> ignore
                                builder.WriteInnerWith()
                                builder.Command.Append ") o" |> ignore
                        | 1 -> 
                            builder.Command.Append "(" |> ignore
                            QueryTranslator.translateQueryable builder.TableName m.Expressions.[0] builder.Command builder.Vars
                            builder.Command.Append ") o" |> ignore
                        | other -> failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other
                    )
                
                
                | SelectMany ->
                    (*
                    Old code:
                    match expression.Arguments.Count with
                    | 2 ->
                        let generics = GenericMethodArgCache.Get expression.Method

                        if generics.[1] (*output*) = typeof<byte> then
                            raise (InvalidOperationException "Cannot use SelectMany() on byte arrays, as they are stored as base64 strings in SQLite. To process the array anyway, first exit the SQLite context with .AsEnumerable().")

                        let innerSourceName = Utils.getVarName builder.SQLiteCommand.Length

                        builder.Append "SELECT "
                        builder.Append innerSourceName
                        builder.Append ".Id AS Id, json_each.Value as Value FROM ("

                        // Evaluate and emit the outer source query (produces Id, Value)
                        translate builder expression.Arguments.[0]

                        builder.Append ") AS "
                        builder.Append innerSourceName
                        builder.Append " "
            

                        // Extract the path to the collection selector (e.g., "$.Values")
                        match expression.Arguments.[1] with
                        | :? UnaryExpression as ue when (ue.Operand :? LambdaExpression) ->
                            let lambda = ue.Operand :?> LambdaExpression
                            match lambda.Body with
                            | :? MemberExpression as me ->
                                builder.Append "JOIN json_each(jsonb_extract("
                                builder.Append innerSourceName
                                builder.Append ".Value, '$."
                                builder.Append me.Member.Name
                                builder.Append "'))"

                            | :? ParameterExpression as _pe ->
                                builder.Append "JOIN json_each("
                                builder.Append innerSourceName
                                builder.Append ".Value)"

                            | _ -> failwith "Unsupported SelectMany selector structure"
                        | _ -> failwith "Invalid SelectMany structure"
                    
                    | other -> 
                        failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other
                                *)

                    addComplex statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|}) ->
                        match m.Expressions.Length with
                        | 1 ->
                            let generics = GenericMethodArgCache.Get m.OriginalMethod
                            if generics.[1] (*output*) = typeof<byte> then
                                raise (InvalidOperationException "Cannot use SelectMany() on byte arrays, as they are stored as base64 strings in SQLite. To process the array anyway, first exit the SQLite context with .AsEnumerable().")
                            let innerSourceName = Utils.getVarName builder.Command.Length
                            builder.Command.Append "SELECT " |> ignore
                            builder.Command.Append innerSourceName |> ignore
                            builder.Command.Append ".Id AS Id, json_each.Value as Value FROM (" |> ignore
                            // Evaluate and emit the outer source query (produces Id, Value)
                            QueryTranslator.translateQueryable builder.TableName m.Expressions.[0] builder.Command builder.Vars
                            builder.Command.Append ") AS " |> ignore
                            builder.Command.Append innerSourceName |> ignore
                            builder.Command.Append " " |> ignore
                            // Extract the path to the collection selector (e.g., "$.Values")
                            match m.Expressions.[1] with
                            | :? UnaryExpression as ue when (ue.Operand :? LambdaExpression) ->
                                let lambda = ue.Operand :?> LambdaExpression
                                match lambda.Body with
                                | :? MemberExpression as me ->
                                    builder.Command.Append "JOIN json_each(jsonb_extract(" |> ignore
                                    builder.Command.Append innerSourceName |> ignore
                                    builder.Command.Append ".Value, '$." |> ignore
                                    builder.Command.Append me.Member.Name |> ignore
                                    builder.Command.Append "'))" |> ignore
                                | :? ParameterExpression as _pe ->
                                    builder.Command.Append "JOIN json_each(" |> ignore
                                    builder.Command.Append innerSourceName |> ignore
                                    builder.Command.Append ".Value)" |> ignore
                                | _ -> failwith "Unsupported SelectMany selector structure"
                            | _ -> failwith "Invalid SelectMany structure"
                        | other -> failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other
                    )
                
                | Single | SingleOrDefault
                | First | FirstOrDefault ->
                    (*match expression.Arguments.Count with
                    | 1 -> 
                        builder.Append "SELECT Id, Value FROM ("
                        translate builder expression.Arguments.[0]
                        builder.Append ") o "
                    | 2 -> 
                        translateWhereStatement translate builder expression.Arguments.[0] expression.Arguments.[1]
                    | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

                    builder.Append " LIMIT 1 "*)
                    
                    let current = simpleCurrent()
                    let constant1 = Expression.Constant(1, typeof<int>)
                    match current.Take with
                    | Some _ -> statements.Add(Simple { emptySQLStatement () with Take = Some constant1 })
                    | None   -> current.Take <- Some constant1


                | DefaultIfEmpty ->
                    (*builder.Append "SELECT Id, Value FROM ("
                    translate builder expression.Arguments.[0]
                    builder.Append ") o UNION ALL SELECT -1 as Id, "

                    match expression.Arguments.Count with
                    | 1 -> 
                        // If no default value is provided, then provide .NET's default.

                        let genericArg = (GenericMethodArgCache.Get expression.Method).[0]
                        if genericArg.IsValueType then
                            let defaultValueType = Activator.CreateInstance(genericArg)
                            let jsonObj = JsonSerializator.JsonValue.Serialize defaultValueType
                            let jsonText = jsonObj.ToJsonString()
                            let escapedJsonText = QueryTranslator.escapeSQLiteString jsonText

                            builder.Append "jsonb_extract(jsonb('"
                            builder.Append escapedJsonText
                            builder.Append "'), '$')"
                        else
                            builder.Append "NULL"

                    | 2 -> 
                        // If a default value is provided, return it when the result set is empty
            
                        let o = QueryTranslator.evaluateExpr<obj> expression.Arguments.[1]
                        let jsonObj = JsonSerializator.JsonValue.Serialize o
                        let jsonText = jsonObj.ToJsonString()
                        let escapedJsonText = QueryTranslator.escapeSQLiteString jsonText

                        builder.Append "jsonb_extract(jsonb('"
                        builder.Append escapedJsonText
                        builder.Append "'), '$')"

                    | other -> failwithf "Invalid number of arguments in %s: %A" expression.Method.Name other

                    builder.Append " as Value WHERE NOT EXISTS (SELECT 1 FROM ("
                    translate builder expression.Arguments.[0]
                    builder.Append ") o)"*)

                    addComplex statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT Id, Value FROM (" |> ignore
                        builder.WriteInnerWith()
                        builder.Command.Append ") o UNION ALL SELECT -1 as Id, " |> ignore
                        match m.Expressions.Length with
                        | 0 -> 
                            // If no default value is provided, then provide .NET's default.
                            let genericArg = (GenericMethodArgCache.Get m.OriginalMethod).[0]
                            if genericArg.IsValueType then
                                let defaultValueType = Activator.CreateInstance(genericArg)
                                let jsonObj = JsonSerializator.JsonValue.Serialize defaultValueType
                                let jsonText = jsonObj.ToJsonString()
                                let escapedJsonText = QueryTranslator.escapeSQLiteString jsonText
                                builder.Command.Append "jsonb_extract(jsonb('" |> ignore
                                builder.Command.Append escapedJsonText |> ignore
                                builder.Command.Append "'), '$') " |> ignore
                            else
                                builder.Command.Append "NULL " |> ignore
                        | 1 -> 
                            // If a default value is provided, return it when the result set is empty
            
                            let o = QueryTranslator.evaluateExpr<obj> m.Expressions.[0]
                            let jsonObj = JsonSerializator.JsonValue.Serialize o
                            let jsonText = jsonObj.ToJsonString()
                            let escapedJsonText = QueryTranslator.escapeSQLiteString jsonText
                            builder.Command.Append "jsonb_extract(jsonb('" |> ignore
                            builder.Command.Append escapedJsonText |> ignore
                            builder.Command.Append "'), '$') " |> ignore
                        | other -> failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other
                        builder.Command.Append " WHERE NOT EXISTS (SELECT 1 FROM (" |> ignore
                        builder.WriteInnerWith()
                        builder.Command.Append ") o)" |> ignore
                    )
                
                | Last | LastOrDefault ->
                    (*// SQlite does not guarantee the order of elements without an ORDER BY,
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
                        *)
                    match m.Expressions.Length with
                    | 0 -> () // If no arguments are provided, we assume the last element is the one with the highest Id.
                    | 1 ->
                        addFilter statements m.Expressions.[0]
                    | other -> failwithf "Invalid number of arguments in %A: %A" m.Value other

                    let current = simpleCurrent()
                    let constant1 = Expression.Constant(1, typeof<int>)
                    match current.Take with
                    | Some _ -> statements.Add(Simple { emptySQLStatement () with Take = Some constant1 })
                    | None   -> current.Take <- Some constant1

                (*Old All and Any:
                | All ->
        if expression.Arguments.Count = 2 then
            builder.Append "SELECT COUNT(*) = (SELECT COUNT(*) FROM ("
            translate builder expression.Arguments.[0]
            builder.Append ")) as Value FROM ("
            translateWhereStatement translate builder expression.Arguments.[0] expression.Arguments.[1]
            builder.Append ") o "
        else
            failwithf "Invalid All method with %i arguments" expression.Arguments.Count

    | Any ->
        builder.Append "(SELECT EXISTS(SELECT 1 FROM ("
        if expression.Arguments.Count = 2 then
            translateWhereStatement translate builder expression.Arguments.[0] expression.Arguments.[1]
        else
            translate builder expression.Arguments.[0]
        builder.Append " LIMIT 1) o) as Value)"*)

                | All ->
                    addComplex statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT COUNT(*) = (SELECT COUNT(*) FROM (" |> ignore
                        builder.WriteInnerWith()
                        builder.Command.Append ")) as Value FROM (" |> ignore
                        match m.Expressions.Length with
                        | 0 -> QueryTranslator.translateQueryable builder.TableName (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) builder.Command builder.Vars
                        | 1 -> QueryTranslator.translateQueryable builder.TableName m.Expressions.[0] builder.Command builder.Vars
                        | other -> failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other
                        builder.Command.Append ") o " |> ignore
                    )

                | Any ->
                    addComplex statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "(SELECT EXISTS(SELECT 1 FROM (" |> ignore
                        match m.Expressions.Length with
                        | 0 -> QueryTranslator.translateQueryable builder.TableName (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) builder.Command builder.Vars
                        | 1 -> QueryTranslator.translateQueryable builder.TableName m.Expressions.[0] builder.Command builder.Vars
                        | other -> failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other
                        builder.Command.Append " LIMIT 1) o) as Value)" |> ignore
                    )


                | Contains ->
                    let struct (t, value) = 
                        match m.Expressions.[0] with
                        | :? ConstantExpression as ce -> struct (ce.Type, ce.Value)
                        | other -> failwithf "Invalid Contains(...) parameter: %A" other

                    let filter = (ExpressionHelper.eq t value)
                    addFilter statements filter

                    addComplex statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInnerWith: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "(SELECT EXISTS(SELECT 1 FROM (" |> ignore
                        match m.Expressions.Length with
                        | 0 -> QueryTranslator.translateQueryable builder.TableName (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) builder.Command builder.Vars
                        | 1 -> QueryTranslator.translateQueryable builder.TableName m.Expressions.[0] builder.Command builder.Vars
                        | other -> failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other
                        builder.Command.Append " LIMIT 1) o) as Value)" |> ignore
                    )
                
                | Append ->
                    (*Old code:
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

                        let escapedString = QueryTranslator.escapeSQLiteString jsonStringElement
                        builder.Append "jsonb('"
                        builder.Append escapedString
                        builder.Append "')"
                        builder.Append " As Value "
                    *)

                    addUnionAll statements (fun tableName builder vars ->
                        builder.Append "SELECT " |> ignore
                        let appendingObj = QueryTranslator.evaluateExpr<'T> m.Expressions.[0]
                        let struct (jsonStringElement, hasId) = serializeForCollection appendingObj
                        match hasId with
                        | false ->
                            builder.Append "-1 as Id," |> ignore
                        | true ->
                            let id = HasTypeId<'T>.Read appendingObj
                            builder.Append (sprintf "%i" id) |> ignore
                            builder.Append " as Id," |> ignore

                        let escapedString = QueryTranslator.escapeSQLiteString jsonStringElement
                        builder.Append "jsonb('" |> ignore
                        builder.Append escapedString |> ignore
                        builder.Append "')" |> ignore
                        builder.Append " As Value " |> ignore
                    )
                
                
                | Concat ->
                    // Left side is the current pipeline; append the right side as UNION ALL.
                    addUnionAll statements (fun _tableName sb vars ->
                        sb.Append "SELECT Id, Value FROM (" |> ignore
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        preprocessAndCollect rhs |> writeLayers builder
                        sb.Append ") o " |> ignore
                    )

                | Except ->
                    // Project left, remove rows whose JSON value appears in the right.
                    addComplex statements (fun ctx ->
                        ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                        ctx.WriteInnerWith()
                        ctx.Command.Append ") o WHERE jsonb_extract(Value, '$') NOT IN (" |> ignore

                        ctx.Command.Append "SELECT jsonb_extract(Value, '$') FROM (" |> ignore
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        preprocessAndCollect rhs |> writeLayers builder
                        ctx.Command.Append ") o )" |> ignore
                    )

                | Intersect ->
                    // Keep only rows whose JSON value appears in the right.
                    addComplex statements (fun ctx ->
                        ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                        ctx.WriteInnerWith()
                        ctx.Command.Append ") o WHERE jsonb_extract(Value, '$') IN (" |> ignore

                        ctx.Command.Append "SELECT jsonb_extract(Value, '$') FROM (" |> ignore
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        preprocessAndCollect rhs |> writeLayers builder
                        ctx.Command.Append ") o )" |> ignore
                    )

                | ExceptBy ->
                    // Arguments: other, keySelector
                    if m.Expressions.Length <> 2 then failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length
                    let keySelE = m.Expressions.[1]
                    addComplex statements (fun ctx ->
                        ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                        ctx.WriteInnerWith()
                        ctx.Command.Append ") o WHERE " |> ignore
                        QueryTranslator.translateQueryable ctx.TableName keySelE ctx.Command ctx.Vars
                        ctx.Command.Append " NOT IN (" |> ignore

                        ctx.Command.Append "SELECT " |> ignore
                        QueryTranslator.translateQueryable ctx.TableName keySelE ctx.Command ctx.Vars
                        ctx.Command.Append " FROM (" |> ignore
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        preprocessAndCollect rhs |> writeLayers builder
                        ctx.Command.Append ") o )" |> ignore
                    )

                | IntersectBy ->
                    // Arguments: other, keySelector
                    if m.Expressions.Length <> 2 then failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length
                    let keySelE = m.Expressions.[1]
                    addComplex statements (fun ctx ->
                        ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                        ctx.WriteInnerWith()
                        ctx.Command.Append ") o WHERE " |> ignore
                        QueryTranslator.translateQueryable ctx.TableName keySelE ctx.Command ctx.Vars
                        ctx.Command.Append " IN (" |> ignore

                        ctx.Command.Append "SELECT " |> ignore
                        QueryTranslator.translateQueryable ctx.TableName keySelE ctx.Command ctx.Vars
                        ctx.Command.Append " FROM (" |> ignore
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        preprocessAndCollect rhs |> writeLayers builder
                        ctx.Command.Append ") o )" |> ignore
                    )

                // --- Type filtering / casting over polymorphic payloads ---
                | Cast ->
                    // Cast<TTarget>() with polymorphic guard & diagnostic messages.
                    if m.Expressions.Length <> 0 then failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length
                    match GenericMethodArgCache.Get m.OriginalMethod |> Array.tryHead with
                    | None -> failwith "Invalid type from Cast<T> method."
                    | Some t when typeof<JsonSerializator.JsonValue>.IsAssignableFrom t ->
                        // No-op cast: just keep pipeline as-is (i.e., do nothing here).
                        ()
                    | Some t ->
                        match t |> typeToName with
                        | None -> failwith "Incompatible type from Cast<T> method."
                        | Some typeName ->
                            addComplex statements (fun ctx ->
                                ctx.Command.Append "SELECT " |> ignore
                                // Id: preserve only if type info present and matches; else NULL
                                ctx.Command.Append "CASE WHEN jsonb_extract(Value, '$.$type') IS NULL THEN NULL " |> ignore
                                ctx.Command.Append "WHEN jsonb_extract(Value, '$.$type') <> " |> ignore
                                QueryTranslator.appendVariable ctx.Command ctx.Vars typeName
                                ctx.Command.Append " THEN NULL ELSE Id END AS Id, " |> ignore

                                // Value: propagate or attach error string
                                ctx.Command.Append "CASE " |> ignore
                                ctx.Command.Append "WHEN jsonb_extract(Value, '$.$type') IS NULL THEN json_quote('The type of item is not stored in the database, if you want to include it, then add the Polimorphic attribute to the type and reinsert all elements.') " |> ignore
                                ctx.Command.Append "WHEN jsonb_extract(Value, '$.$type') <> " |> ignore
                                QueryTranslator.appendVariable ctx.Command ctx.Vars typeName
                                ctx.Command.Append " THEN json_quote('Unable to cast object to the specified type, because the types are different.') " |> ignore
                                ctx.Command.Append "ELSE Value END AS Value FROM (" |> ignore
                                ctx.WriteInnerWith()
                                ctx.Command.Append ") o " |> ignore
                            )

                | OfType ->
                    if m.Expressions.Length <> 0 then failwithf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length
                    match GenericMethodArgCache.Get m.OriginalMethod |> Array.tryHead with
                    | None -> failwith "Invalid type from OfType<T> method."
                    | Some t when typeof<JsonSerializator.JsonValue>.IsAssignableFrom t ->
                        // No-op filter to JsonValue
                        ()
                    | Some t ->
                        match t |> typeToName with
                        | None -> failwith "Incompatible type from OfType<T> method."
                        | Some typeName ->
                            addComplex statements (fun ctx ->
                                ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                                ctx.WriteInnerWith()
                                ctx.Command.Append ") o WHERE jsonb_extract(Value, '$.$type') = " |> ignore
                                QueryTranslator.appendVariable ctx.Command ctx.Vars typeName
                                ctx.Command.Append " " |> ignore
                            )

                | Aggregate ->
                    failwithf "Aggregate is not supported."

                tableName

        let tn = collect pq
        match statements.[0] with
        | Simple s ->
            statements.[0] <- Simple {s with TableName = tn}
        | Complex _ ->
            ()

    and private preprocessAndCollect expression =
        let statements = ResizeArray<SQLSubquery>()
        preprocessQuery expression
        |> collectPreprocessedQuery statements
        statements

    and private writeLayers
        (builder: QueryableBuilder<'T>)
        (layers: SQLSubquery ResizeArray)
        =

        let layerCount = layers.Count

        let rec writeLayer (i: int) =
            let layer = layers.[i]
            match layer with
            | Simple layer ->
                match layer.Selector with
                | Some (Expression selector) ->
                    builder.Append "SELECT Id, "
                    QueryTranslator.translateQueryable layer.TableName selector builder.SQLiteCommand builder.Variables
                    builder.Append "AS Value "
                | Some (Raw func) ->
                    builder.Append "SELECT Id, "
                    func layer.TableName builder.SQLiteCommand builder.Variables
                    builder.Append "AS Value "
                | None ->
                    builder.Append "SELECT Id, Value "
                builder.Append "FROM "
                if i = 0 then
                    builder.Append layer.TableName
                else
                    builder.Append "("
                    writeLayer (i - 1)
                    builder.Append ")"

                writeClauses builder layer layer.TableName
            | Complex writeFunc ->
                let tableName =
                    if i = 0 then
                        builder.Source.Name
                    else
                        ""

                writeFunc {|
                    Command = builder.SQLiteCommand
                    Vars = builder.Variables
                    WriteInnerWith = fun () -> writeLayer (i - 1)
                    TableName = tableName
                |}

        writeLayer (layerCount - 1)


    and private translateFlattenedQuery<'T> (builder: QueryableBuilder<'T>) (pq: PreprocessedQuery) =
        // Collect layers + possible terminal
        collectPreprocessedQuery<'T> builder.TypedSubqueries pq
        let layers = builder.TypedSubqueries
        writeLayers builder layers
    
    /// <summary>
    /// Determines if a given LINQ expression corresponds to a method that does not return the document ID (e.g., aggregate functions).
    /// </summary>
    /// <param name="expression">The expression to check.</param>
    /// <returns>True if the method is an aggregate that doesn't return an ID; otherwise, false.</returns>
    let internal doesNotReturnIdFn (expression: Expression) =
        match expression with
        | :? MethodCallExpression as mce ->
            match parseSupportedMethod mce.Method.Name with
            | None -> false
            | Some method ->
            match method with
            | Sum
            | Count
            | CountBy
            | LongCount
            | All
            | Any
            | Contains
            | Aggregate
                -> true
            | Min
            | Max
            | Average
            | Distinct
            | DistinctBy
            | Where
            | Select
            | SelectMany
            | ThenBy
            | ThenByDescending
            | OrderBy
            | Order
            | OrderDescending
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
            | Cast
            | OfType
                -> false
        | _other -> false

    /// <summary>
    /// Checks if an Aggregate expression is a special call to ExplainQueryPlan.
    /// </summary>
    /// <param name="expression">The expression to check.</param>
    /// <returns>True if it's an ExplainQueryPlan call, otherwise false.</returns>
    let internal isAggregateExplainQuery (expression: Expression) =
        match expression with
        | :? MethodCallExpression as expression ->
            expression.Arguments.Count > 2 
            && expression.Arguments.[1] :? ConstantExpression 
            && Object.ReferenceEquals((expression.Arguments.[1] :?> ConstantExpression).Value, (QueryPlan.ExplainQueryPlanReference :> obj)) 
            && expression.Arguments.[2].NodeType = ExpressionType.Quote
        | _ -> false

    /// <summary>
    /// Checks if an Aggregate expression is a special call to GetSQL.
    /// </summary>
    /// <param name="expression">The expression to check.</param>
    /// <returns>True if it's a GetSQL call, otherwise false.</returns>
    let internal isGetGeneratedSQLQuery (expression: Expression) =
        match expression with
        | :? MethodCallExpression as expression ->
            expression.Arguments.Count > 2 
            && expression.Arguments.[1] :? ConstantExpression 
            && Object.ReferenceEquals((expression.Arguments.[1] :?> ConstantExpression).Value, (QueryPlan.GetGeneratedSQLReference :> obj)) 
            && expression.Arguments.[2].NodeType = ExpressionType.Quote
        | _ -> false

    /// <summary>
    /// Initiates the translation of a LINQ expression tree to an SQL query string and a dictionary of parameters.
    /// </summary>
    /// <param name="source">The source collection of the query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// <returns>A tuple containing the generated SQL string and the dictionary of parameters.</returns>
    let internal startTranslation (source: ISoloDBCollection<'T>) (expression: Expression) =
        let builder = {
            SQLiteCommand = StringBuilder(256)
            Variables = Dictionary<string, obj>(16)
            Source = source
            TypedSubqueries = ResizeArray()
        }

        let valueDecoded = 
            if typedefof<IQueryable>.IsAssignableFrom expression.Type then
                extractValueAsJsonIfNecesary (GenericTypeArgCache.Get expression.Type |> Array.head)
            else 
                extractValueAsJsonIfNecesary expression.Type

        let struct (isExplainQueryPlan, expression) =
            match expression with
            | :? MethodCallExpression as expression when isAggregateExplainQuery expression -> 
                struct (true, expression.Arguments.[0])
            | :? MethodCallExpression as expression when isGetGeneratedSQLQuery expression -> 
                struct (false, expression.Arguments.[0])
            | _ -> struct (false, expression)

        if isExplainQueryPlan then
            builder.Append "EXPLAIN QUERY PLAN "


        let pq = preprocessQuery expression

        if doesNotReturnIdFn expression then
            builder.Append "SELECT -1 as Id, "
            builder.Append valueDecoded
            builder.Append "as ValueJSON FROM ("
            // translate builder expression
            translateFlattenedQuery builder pq
            builder.Append ")"
        else
            builder.Append "SELECT Id, "
            builder.Append valueDecoded
            builder.Append "as ValueJSON FROM ("
            // translate builder expression
            translateFlattenedQuery builder pq
            builder.Append ")"


        builder.SQLiteCommand.ToString(), builder.Variables


/// <summary>
/// An internal interface defining the contract for a SoloDB query provider.
/// </summary>
type internal ISoloDBCollectionQueryProvider =
    /// <summary>Gets the source collection object.</summary>
    abstract member Source: obj
    /// <summary>Gets additional data passed from the parent database instance.</summary>
    abstract member AdditionalData: obj

/// <summary>
/// The internal implementation of <c>IQueryProvider</c> for SoloDB collections.
/// </summary>
/// <param name="source">The source collection.</param>
/// <param name="data">Additional data, such as cache clearing functions.</param>
type internal SoloDBCollectionQueryProvider<'T>(source: ISoloDBCollection<'T>, data: obj) =
    interface ISoloDBCollectionQueryProvider with
        override this.Source = source
        override this.AdditionalData = data

    interface SoloDBQueryProvider
    member internal this.ExecuteEnumetable<'Elem> (query: string) (par: obj) : IEnumerable<'Elem> =
        seq {
            use connection = source.GetInternalConnection()
            for row in connection.Query<Types.DbObjectRow>(query, par) do
                JsonFunctions.fromSQLite<'Elem> row
            ()
        }

    interface IQueryProvider with
        member this.CreateQuery<'TResult>(expression: Expression) : IQueryable<'TResult> =
            SoloDBCollectionQueryable<'T, 'TResult>(this, expression)

        member this.CreateQuery(expression: Expression) : IQueryable =
            let elementType = expression.Type.GetGenericArguments().[0]
            let queryableType = typedefof<SoloDBCollectionQueryable<_,_>>.MakeGenericType(elementType)
            Activator.CreateInstance(queryableType, source, this, expression) :?> IQueryable

        member this.Execute(expression: Expression) : obj =
            (this :> IQueryProvider).Execute<IEnumerable<'T>>(expression)

        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let query, variables = QueryHelper.startTranslation source expression

            #if DEBUG
            if System.Diagnostics.Debugger.IsAttached then
                printfn "%s" query
            #endif

            try
                match typeof<'TResult> with
                | t when t.IsGenericType
                         && typedefof<IEnumerable<_>> = (typedefof<'TResult>) ->
                    let elemType = (GenericTypeArgCache.Get t).[0]
                    let m = 
                        typeof<SoloDBCollectionQueryProvider<'T>>
                            .GetMethod(nameof(this.ExecuteEnumetable), BindingFlags.NonPublic ||| BindingFlags.Instance)
                            .MakeGenericMethod(elemType)
                    m.Invoke(this, [|query; variables|]) :?> 'TResult
                    // When is explain query plan.
                | t when t = typeof<string> && QueryHelper.isAggregateExplainQuery expression ->
                    use connection = source.GetInternalConnection()
                    let result = connection.Query<{|detail: string|}>(query, variables) |> Seq.toList
                    let plan = result |> List.map(_.detail) |> String.concat ";\n"
                    plan :> obj :?> 'TResult
                | t when t = typeof<string> && QueryHelper.isGetGeneratedSQLQuery expression ->
                    query :> obj :?> 'TResult
                | _other ->
                    use connection = source.GetInternalConnection()
                    let methodName = 
                        match expression with
                        | :? MethodCallExpression as mce -> mce.Method.Name
                        | _ -> "Execute"

                    // Add Single, First, and the OrDefault Variant here.
                    let query = connection.Query<Types.DbObjectRow>(query, variables)

                    match methodName with
                    | "Single" ->
                        query.Single() |> JsonFunctions.fromSQLite<'TResult>
                    | "SingleOrDefault" ->
                        // Emulate query.SingleOrDefault()
                        use enumerator = query.GetEnumerator()
                        match enumerator.MoveNext() with
                        | false -> Unchecked.defaultof<'TResult>
                        | true -> 
                            let prevElement = enumerator.Current
                            match enumerator.MoveNext() with
                            | true -> failwithf "Sequence contains more than one element"
                            | false -> 
                                // Only one element was found, return it.
                                JsonFunctions.fromSQLite<'TResult> prevElement

                    | "First" ->
                        query.First() |> JsonFunctions.fromSQLite<'TResult>
                    | "FirstOrDefault" ->
                        match query |> Seq.tryHead with
                        | None -> Unchecked.defaultof<'TResult>
                        | Some row -> JsonFunctions.fromSQLite<'TResult> row

                    | methodName when methodName.EndsWith("OrDefault", StringComparison.Ordinal) ->
                        match query |> Seq.tryHead with
                        | None -> Unchecked.defaultof<'TResult>
                        | Some row -> JsonFunctions.fromSQLite<'TResult> row
                    | _ ->
                        match query |> Seq.tryHead with
                        | None -> failwithf "Sequence contains no elements"
                        | Some row -> JsonFunctions.fromSQLite<'TResult> row
                    
            with _ex ->
                reraise()
            
/// <summary>
/// The internal implementation of <c>IQueryable</c> and <c>IOrderedQueryable</c> for SoloDB collections.
/// </summary>
/// <param name="provider">The query provider that created this queryable.</param>
/// <param name="expression">The expression tree representing the query.</param>
and internal SoloDBCollectionQueryable<'I, 'T>(provider: IQueryProvider, expression: Expression) =
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

/// <summary>
/// A private, sealed class representing the root of a query, which is always a collection.
/// </summary>
/// <param name="c">The source collection.</param>
and [<Sealed>] private RootQueryable<'T>(c: ISoloDBCollection<'T>) =
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
