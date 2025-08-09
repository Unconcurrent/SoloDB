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

/// <summary>
/// Represents a terminal operation that consumes the query sequence.
/// </summary>
type private TerminalOperation = {
    Method: SupportedLinqMethods
    Expressions: Expression array
    OriginalMethod: MethodInfo
}

type private UsedSQLStatements = 
    {
        /// May be more than 1, they will be concatinated together with (...) AND (...) SQL structure.
        Filters: Expression ResizeArray
        Orders: PreprocessedOrder ResizeArray
        mutable Selector: Expression option
        // These will account for the fact that in SQLite, the OFFSET clause cannot be used independently without the LIMIT clause.
        mutable Skip: Expression option
        mutable Take: Expression option
        TableName: string
    }


/// <summary>
/// A private, mutable builder used to construct an SQL query from a LINQ expression tree.
/// </summary>
/// <remarks>This type holds the state of the translation process, including the SQL command being built and any parameters.</remarks>
type private QueryableBuilder<'T> = 
    {
        /// <summary>The StringBuilder that accumulates the generated SQL string.</summary>
        SQLiteCommand: StringBuilder
        /// <summary>A dictionary of parameters to be passed to the SQLite command.</summary>
        Variables: Dictionary<string, obj>
        /// <summary>The source collection that the query operates on.</summary>
        Source: ISoloDBCollection<'T>
        /// This list will be using to know if it is necessary to create another SQLite subquery to finish the translation, by checking if all the available slots have been used.
        UsedStatements: ResizeArray<UsedSQLStatements>
        /// <summary>Holds the terminal operation, if any, found at the end of the query chain.</summary>
        mutable TerminalOperation: TerminalOperation option
    }
    /// <summary>Appends a string to the current SQL command.</summary>
    member this.Append(text: string) =
        ignore (this.SQLiteCommand.Append text)
        this

    /// <summary>Appends a string to the current SQL command.</summary>
    member this.AppendF(text: string) =
        ignore (this.SQLiteCommand.Append text)

    /// <summary>Appends a StringBuilder to the current SQL command.</summary>
    member this.Append(text: StringBuilder) =
        ignore (this.SQLiteCommand.Append text)
        this

    /// <summary>Adds a variable to the parameters dictionary and appends its placeholder to the SQL command.</summary>
    member this.AppendVar(variable: obj) =
        ignore (QueryTranslator.appendVariable this.SQLiteCommand this.Variables variable)
        this

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
            { Filters = ResizeArray(); Orders = ResizeArray(); Selector = None; Skip = None; Take = None; TableName = "" }

    let addSelectionSensibleFilter (statements: ResizeArray<UsedSQLStatements>) (filter: Expression) =
        match statements.Last().Selector with
        | Some _ -> 
            statements.Add { emptySQLStatement () with Filters = ResizeArray [ filter ] }
        | None ->
            // If we don't have a selector, we can just add the filter to the current statement
            statements.Last().Filters.Add filter

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
            |> ignore

        // ORDER BY
        if statement.Orders.Count <> 0 then
            builder.Append(" ORDER BY ") |> ignore
            statement.Orders
            |> Seq.iteri (fun j o ->
                if j > 0 then builder.Append(", ") |> ignore
                QueryTranslator.translateQueryable contextTable o.Selector builder.SQLiteCommand builder.Variables
                if o.Descending then builder.Append(" DESC") |> ignore
            )
            |> ignore

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

    let private collectPreprocessedQuery (statements: UsedSQLStatements ResizeArray) (terminalOperation: TerminalOperation option outref) (pq: PreprocessedQuery) =
        // Walk and collect “layers” + detect a terminal op.
        statements.Add (emptySQLStatement ())

        let rec collect (q: PreprocessedQuery) : struct (string * TerminalOperation option) =
            match q with
            | RootQuery rq -> struct (rq.SourceTableName, None)
            | Method m ->
                let struct (tableName, op) = collect m.InnerQuery
                let mutable terminalOperation = op

                let current = statements.Last()
                match m.Value with
                | Where -> current.Filters.Add m.Expressions.[0]
                | Select ->
                    match current.Selector with
                    | Some _ ->
                        statements.Add({ emptySQLStatement () with Selector = Some m.Expressions.[0] })
                    | None ->
                        current.Selector <- Some m.Expressions.[0]
                | OrderBy | Order ->
                    current.Orders.Clear()
                    current.Orders.Add({ Selector = m.Expressions.[0]; Descending = false })
                | OrderByDescending | OrderDescending ->
                    current.Orders.Clear()
                    current.Orders.Add({ Selector = m.Expressions.[0]; Descending = true })
                | ThenBy ->
                    current.Orders.Add({ Selector = m.Expressions.[0]; Descending = false })
                | ThenByDescending ->
                    current.Orders.Add({ Selector = m.Expressions.[0]; Descending = true })
                | Skip ->
                    match current.Skip with
                    | Some _ -> statements.Add({ emptySQLStatement () with Skip = Some m.Expressions.[0] })
                    | None   -> current.Skip <- Some m.Expressions.[0]
                | Take ->
                    match current.Take with
                    | Some _ -> statements.Add({ emptySQLStatement () with Take = Some m.Expressions.[0] })
                    | None   -> current.Take <- Some m.Expressions.[0]
                // terminal set
                | Sum | Average | Min | Max | Distinct | DistinctBy | Count | CountBy | LongCount
                | SelectMany | First | FirstOrDefault | DefaultIfEmpty | Last | LastOrDefault
                | Single | SingleOrDefault | All | Any | Contains | Append | Concat | GroupBy
                | Except | ExceptBy | Intersect | IntersectBy | Cast | OfType | Aggregate ->
                    terminalOperation <- Some { Method = m.Value; Expressions = m.Expressions; OriginalMethod = m.OriginalMethod }

                struct (tableName, terminalOperation)

        let struct (tn, tOp) = collect pq
        statements.[0] <- {statements.[0] with TableName = tn}
        terminalOperation <- tOp

    let private writeLayers
        (builder: QueryableBuilder<'T>)
        (layers: UsedSQLStatements ResizeArray)
        =

        let layerCount = layers.Count

        let rec writeLayer (i: int) =
            let layer = layers.[i]
            match layer.Selector with
            | Some selector ->
                builder.AppendF "SELECT Id, "
                QueryTranslator.translateQueryable layer.TableName selector builder.SQLiteCommand builder.Variables
                builder.AppendF "AS Value "
            | None ->
                builder.AppendF "SELECT Id, Value "
            builder.AppendF "FROM "
            if i = 0 then
                builder.AppendF layer.TableName
            else
                builder.AppendF "("
                writeLayer (i - 1)
                builder.AppendF ")"

            writeClauses builder layer layer.TableName

        writeLayer (layerCount - 1)

    let private serializeForCollection (value: 'T) =
        match typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T> with
        | true -> JsonSerializator.JsonValue.SerializeWithType value
        | false -> JsonSerializator.JsonValue.Serialize value
        |> _.ToJsonString(), HasTypeId<'T>.Value

    let rec private translateFlattenedQuery (builder: QueryableBuilder<'T>) (pq: PreprocessedQuery) =
        // Collect layers + possible terminal
        collectPreprocessedQuery builder.UsedStatements &builder.TerminalOperation pq

        let layers = builder.UsedStatements

        // --- local helpers for terminal ops ---

        // Render current layers as SELECT ... FROM ... with all collected WHERE/ORDER/LIMIT/OFFSET
        let inline writeBase () = writeLayers builder layers
        let inline writeBaseWith f = 
            f layers
            writeLayers builder layers

        // Render current layers wrapped as "( ... ) o"
        let inline writeBaseAsSubquery () =
            builder.AppendF "(" |> ignore
            writeBase ()
            builder.AppendF ") o" |> ignore

        let inline writeBaseAsSubqueryWith f =
            f layers
            builder.AppendF "(" |> ignore
            writeBase ()
            builder.AppendF ") o" |> ignore

        // Render an external IQueryable (expression) as "( ... ) o"
        let writeExternalAsSubquery (expr: Expression) =
            let extLayers = ResizeArray<UsedSQLStatements>()
            let mutable _noop : TerminalOperation option = None
            collectPreprocessedQuery extLayers &_noop (preprocessQuery expr)
            builder.AppendF "(" |> ignore
            writeLayers builder extLayers
            builder.AppendF ") o" |> ignore

        // Aggregate helpers
        let emitAggregate (sqlFn: string) (selectorOpt: Expression option) (coalesceZero: bool) =
            if coalesceZero && sqlFn = "SUM" then
                builder.AppendF "SELECT COALESCE((" |> ignore
            else
                builder.AppendF "SELECT " |> ignore

            builder.AppendF sqlFn |> ignore
            builder.AppendF "(" |> ignore
            match selectorOpt with
            | Some sel -> QueryTranslator.translateQueryable "" sel builder.SQLiteCommand builder.Variables
            | None     -> QueryTranslator.translateQueryable "" (ExpressionHelper.id typeof<'T>) builder.SQLiteCommand builder.Variables
            builder.AppendF ")" |> ignore

            if coalesceZero && sqlFn = "SUM" then builder.AppendF "),0" |> ignore
            builder.AppendF " as Value FROM " |> ignore
            writeBaseAsSubquery ()

        let emitRaiseIfNullAggregate (sqlFn: string) (selectorOpt: Expression option) (errMsg: string) =
            builder.AppendF "SELECT CASE WHEN jsonb_extract(Value, '$') IS NULL THEN NULL ELSE -1 END AS Id, " |> ignore
            builder.AppendF "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN '" |> ignore
            builder.AppendF (QueryTranslator.escapeSQLiteString errMsg) |> ignore
            builder.AppendF "' ELSE Value END AS Value FROM (" |> ignore
            emitAggregate sqlFn selectorOpt false
            builder.AppendF ") o" |> ignore

        // For DISTINCT and DISTINCT BY
        let emitDistinct (keySelectorOpt: Expression option) =
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o GROUP BY " |> ignore
            match keySelectorOpt with
            | Some ks -> QueryTranslator.translateQueryable "" ks builder.SQLiteCommand builder.Variables
            | None    -> QueryTranslator.translateQueryable "" (ExpressionHelper.id typeof<'T>) builder.SQLiteCommand builder.Variables

        // COUNT family
        let emitCountWithOptionalPredicate (predicateOpt: Expression option) =
            builder.AppendF "SELECT COUNT(Id) as Value FROM " |> ignore
            match predicateOpt with
            | None ->
                // Fast path if the top is a bare table — the layers already contain the table+clauses;
                // we still keep the subquery form to be uniform.
                writeBaseAsSubquery ()
            | Some pred ->
                builder.AppendF "(" |> ignore
                writeBase ()
                builder.AppendF ") o WHERE " |> ignore
                QueryTranslator.translateQueryable "" pred builder.SQLiteCommand builder.Variables

        // WHERE wrapper for “predicate overloads” (First/Single/Any/All/Contains…)
        let emitSelectIdValueWithOptionalWhere (predicateOpt: Expression option) =
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o " |> ignore
            match predicateOpt with
            | None -> ()
            | Some p ->
                builder.AppendF "WHERE " |> ignore
                QueryTranslator.translateQueryable "" p builder.SQLiteCommand builder.Variables

        // Single/SingleOrDefault scaffolding
        let emitSingleLike (predicateOpt: Expression option) (emptyAsError: bool) =
            let countVar = Utils.getVarName builder.SQLiteCommand.Length
            builder.AppendF "SELECT jsonb_extract(Encoded, '$.Id') as Id, jsonb_extract(Encoded, '$.Value') as Value FROM (" |> ignore

            builder.AppendF "SELECT CASE WHEN " |> ignore
            builder.AppendF countVar |> ignore
            builder.AppendF " = 0 THEN " |> ignore
            if emptyAsError then
                builder.AppendF "(SELECT jsonb_object('Id', NULL, 'Value', '\"Sequence contains no elements\"')) " |> ignore
            else
                builder.AppendF "(SELECT 0 WHERE 0) " |> ignore

            builder.AppendF "WHEN " |> ignore
            builder.AppendF countVar |> ignore
            builder.AppendF " > 1 THEN (SELECT jsonb_object('Id', NULL, 'Value', '\"Sequence contains more than one matching element\"')) ELSE (" |> ignore

            builder.AppendF "SELECT jsonb_object('Id', Id, 'Value', Value) FROM (" |> ignore
            emitSelectIdValueWithOptionalWhere predicateOpt
            builder.AppendF ") o" |> ignore

            builder.AppendF ") END as Encoded FROM (" |> ignore
            builder.AppendF "SELECT COUNT(*) as " |> ignore
            builder.AppendF countVar |> ignore
            builder.AppendF " FROM (" |> ignore
            writeBase ()
            builder.AppendF ") " |> ignore
            match predicateOpt with
            | None -> ()
            | Some p ->
                builder.AppendF "WHERE " |> ignore
                QueryTranslator.translateQueryable "" p builder.SQLiteCommand builder.Variables
            builder.AppendF " LIMIT 2) ) WHERE Id IS NOT NULL OR Value IS NOT NULL" |> ignore

        match builder.TerminalOperation with
        | None ->
            // No terminal op — just write the chain
            writeLayers builder layers
        | Some terminalOperation ->
        match terminalOperation.Method with
        // --- Aggregates ---
        | Sum      -> emitAggregate "SUM" (terminalOperation.Expressions |> Array.tryItem 0) true
        | Average  -> emitRaiseIfNullAggregate "AVG" (terminalOperation.Expressions |> Array.tryItem 0) "Sequence contains no elements"
        | Min      -> emitRaiseIfNullAggregate "MIN" (terminalOperation.Expressions |> Array.tryItem 0) "Sequence contains no elements"
        | Max      -> emitRaiseIfNullAggregate "MAX" (terminalOperation.Expressions |> Array.tryItem 0) "Sequence contains no elements"

        // --- Distinct / DistinctBy ---
        | Distinct ->
            // no selector: group by element identity
            emitDistinct None
        | DistinctBy ->
            // selector is Expressions.[0]
            emitDistinct (terminalOperation.Expressions |> Array.tryItem 0)

        // --- Count family ---
        | Count | CountBy | LongCount ->
            // optional predicate in Expressions.[0]
            emitCountWithOptionalPredicate (terminalOperation.Expressions |> Array.tryItem 0)

        // --- First / FirstOrDefault ---
        | First
        | FirstOrDefault ->
            emitSelectIdValueWithOptionalWhere (terminalOperation.Expressions |> Array.tryItem 0)
            builder.AppendF " LIMIT 1 " |> ignore

        // --- DefaultIfEmpty ---
        | DefaultIfEmpty ->
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o UNION ALL SELECT -1 as Id, " |> ignore
            match terminalOperation.Expressions |> Array.tryItem 0 with
            | None ->
                // default(T)
                let t = typeof<'T>
                if t.IsValueType then
                    let dv = Activator.CreateInstance t
                    let jv = JsonSerializator.JsonValue.Serialize dv
                    let js = jv.ToJsonString() |> QueryTranslator.escapeSQLiteString
                    builder.AppendF "jsonb_extract(jsonb('" |> ignore
                    builder.AppendF js |> ignore
                    builder.AppendF "'), '$')" |> ignore
                else
                    builder.AppendF "NULL" |> ignore
            | Some providedDefault ->
                let o = QueryTranslator.evaluateExpr<obj> providedDefault
                let jv = JsonSerializator.JsonValue.Serialize o
                let js = jv.ToJsonString() |> QueryTranslator.escapeSQLiteString
                builder.AppendF "jsonb_extract(jsonb('" |> ignore
                builder.AppendF js |> ignore
                builder.AppendF "'), '$')" |> ignore
            builder.AppendF " as Value WHERE NOT EXISTS (" |> ignore
            builder.AppendF "SELECT 1 FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o)" |> ignore

        // --- Last / LastOrDefault (only if no prior ordering ops exist, same constraint as old code) ---
        | Last
        | LastOrDefault ->
            // Enforce: no composed ORDER BY unless it’s a bare root; mirror old behavior
            if layers.Count <> 1 then
                raise (InvalidOperationException "Because SQLite does not guarantee ordering without ORDER BY, Last*() can only be used directly on the root (it then sorts by Id). Use OrderBy(...).First'OrDefault'() instead.")
            // render base root
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o " |> ignore
            match terminalOperation.Expressions |> Array.tryItem 0 with
            | None -> ()
            | Some p ->
                builder.AppendF "WHERE " |> ignore
                QueryTranslator.translateQueryable "" p builder.SQLiteCommand builder.Variables
            builder.AppendF " ORDER BY Id DESC LIMIT 1 " |> ignore

        // --- Single / SingleOrDefault ---
        | Single ->
            emitSingleLike (terminalOperation.Expressions |> Array.tryItem 0) true
        | SingleOrDefault ->
            emitSingleLike (terminalOperation.Expressions |> Array.tryItem 0) false

        // --- All ---
        | All ->
            match terminalOperation.Expressions |> Array.tryItem 0 with
            | None -> raise (InvalidOperationException "All(...) requires a predicate.")
            | Some p ->
                builder.AppendF "SELECT COUNT(*) = (SELECT COUNT(*) FROM (" |> ignore
                writeBase ()
                builder.AppendF ")) as Value FROM (" |> ignore
                writeBaseWith (fun l -> addSelectionSensibleFilter l p)
                builder.AppendF ")"

        // --- Any ---
        | Any ->
            builder.AppendF "(SELECT EXISTS(SELECT 1 FROM (" |> ignore
            match terminalOperation.Expressions |> Array.tryItem 0 with
            | None ->
                writeBase ()
            | Some p ->
                builder.AppendF "SELECT Id, Value FROM (" |> ignore
                writeBase ()
                builder.AppendF ") o WHERE " |> ignore
                QueryTranslator.translateQueryable "" p builder.SQLiteCommand builder.Variables
            builder.AppendF " LIMIT 1) o) as Value)" |> ignore

        // --- Contains ---
        | Contains ->
            let struct (t, v) =
                match terminalOperation.Expressions |> Array.tryItem 0 with
                | Some (:? ConstantExpression as ce) -> struct (ce.Type, ce.Value)
                | other -> failwithf "Invalid Contains(...) parameter: %A" other
            builder.AppendF "SELECT EXISTS(SELECT 1 FROM ("
            writeBaseWith (fun l -> addSelectionSensibleFilter l (ExpressionHelper.eq t v))
            builder.AppendF ") o"
            builder.AppendF ") as Value "

        // --- Append ---
        | Append ->
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o UNION ALL SELECT " |> ignore
            let appendingObj = QueryTranslator.evaluateExpr<'T> (terminalOperation.Expressions.[0])
            let jsonStringElement, hasId = serializeForCollection appendingObj
            if not hasId then
                builder.AppendF "-1 as Id," |> ignore
            else
                let id = HasTypeId<'T>.Read appendingObj
                builder.AppendF (string id) |> ignore
                builder.AppendF " as Id," |> ignore
            let escaped = QueryTranslator.escapeSQLiteString jsonStringElement
            builder.AppendF "jsonb('" |> ignore
            builder.AppendF escaped |> ignore
            builder.AppendF "') as Value " |> ignore

        // --- Concat ---
        | Concat ->
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o UNION ALL SELECT Id, Value FROM " |> ignore
            let other = terminalOperation.Expressions.[0]
            writeExternalAsSubquery other
            builder.AppendF " " |> ignore

        // --- GroupBy<TSource,TKey>(..., keySelector) -> { Key, Items[] } ---
        | GroupBy ->
            let keySelector =
                match terminalOperation.Expressions |> Array.tryItem 0 with
                | Some k -> k
                | None -> failwith "GroupBy requires a key selector."
            builder.AppendF "SELECT -1 as Id, jsonb_object('Key', " |> ignore
            QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
            builder.AppendF ", 'Items', jsonb_group_array(Value)) as Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o GROUP BY " |> ignore
            QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables

        // --- Except / Intersect (value equality against jsonb_extract(Value,'$')) ---
        | Except ->
            let other = terminalOperation.Expressions.[0]
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o WHERE jsonb_extract(Value, '$') NOT IN (" |> ignore
            builder.AppendF "SELECT jsonb_extract(Value, '$') FROM (" |> ignore
            writeExternalAsSubquery other
            builder.AppendF ") o )" |> ignore

        | Intersect ->
            let other = terminalOperation.Expressions.[0]
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o WHERE jsonb_extract(Value, '$') IN (" |> ignore
            builder.AppendF "SELECT jsonb_extract(Value, '$') FROM (" |> ignore
            writeExternalAsSubquery other
            builder.AppendF ") o )" |> ignore

        // --- ExceptBy / IntersectBy (…, other, keySelector) ---
        | ExceptBy ->
            if terminalOperation.Expressions.Length <> 2 then failwith "ExceptBy expects (other, keySelector)."
            let other, keySelector = terminalOperation.Expressions.[0], terminalOperation.Expressions.[1]
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o WHERE " |> ignore
            QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
            builder.AppendF " NOT IN (" |> ignore
            builder.AppendF "SELECT " |> ignore
            QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
            builder.AppendF " FROM (" |> ignore
            writeExternalAsSubquery other
            builder.AppendF ") o )" |> ignore

        | IntersectBy ->
            if terminalOperation.Expressions.Length <> 2 then failwith "IntersectBy expects (other, keySelector)."
            let other, keySelector = terminalOperation.Expressions.[0], terminalOperation.Expressions.[1]
            builder.AppendF "SELECT Id, Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") o WHERE " |> ignore
            QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
            builder.AppendF " IN (" |> ignore
            builder.AppendF "SELECT " |> ignore
            QueryTranslator.translateQueryable "" keySelector builder.SQLiteCommand builder.Variables
            builder.AppendF " FROM (" |> ignore
            writeExternalAsSubquery other
            builder.AppendF ") o )" |> ignore

        // --- Cast / OfType ---
        | Cast ->
            match GenericMethodArgCache.Get terminalOperation.OriginalMethod |> Array.tryHead with
            | None -> failwith "Cast<T>: missing generic type."
            | Some t when typeof<JsonSerializator.JsonValue>.IsAssignableFrom t ->
                // trivial pass-through
                writeBase ()
            | Some t ->
                match t |> typeToName with
                | None -> failwith "Cast<T>: incompatible type."
                | Some typeName ->
                    builder.AppendF "SELECT " |> ignore
                    builder.AppendF "CASE WHEN jsonb_extract(Value, '$.$type') IS NULL THEN NULL " |> ignore
                    builder.AppendF "WHEN jsonb_extract(Value, '$.$type') <> " |> ignore
                    builder.AppendVar typeName |> ignore
                    builder.AppendF " THEN NULL ELSE Id END AS Id, " |> ignore

                    builder.AppendF "CASE WHEN jsonb_extract(Value, '$.$type') IS NULL THEN json_quote('The type of item is not stored in the database, if you want to include it, then add the Polimorphic attribute to the type and reinsert all elements.') " |> ignore
                    builder.AppendF "WHEN jsonb_extract(Value, '$.$type') <> " |> ignore
                    builder.AppendVar typeName |> ignore
                    builder.AppendF " THEN json_quote('Unable to cast object to the specified type, because the types are different.') ELSE Value END AS Value FROM (" |> ignore
                    writeBase ()
                    builder.AppendF ") o " |> ignore

        | OfType ->
            match GenericMethodArgCache.Get terminalOperation.OriginalMethod |> Array.tryHead with
            | None -> failwith "OfType<T>: missing generic type."
            | Some t when typeof<JsonSerializator.JsonValue>.IsAssignableFrom t ->
                writeBase ()
            | Some t ->
                match t |> typeToName with
                | None -> failwith "OfType<T>: incompatible type."
                | Some typeName ->
                    builder.AppendF "SELECT Id, Value FROM (" |> ignore
                    writeBase ()
                    builder.AppendF ") o WHERE jsonb_extract(Value, '$.$type') = " |> ignore
                    builder.AppendVar typeName |> ignore
                    builder.AppendF " " |> ignore

        // --- SelectMany (sequence-producing but treated here as terminal in your enum) ---
        | SelectMany ->
            // Expect one selector: x => x.Prop or x => x
            let innerName = Utils.getVarName builder.SQLiteCommand.Length
            builder.AppendF "SELECT " |> ignore
            builder.AppendF innerName |> ignore
            builder.AppendF ".Id AS Id, json_each.Value as Value FROM (" |> ignore
            writeBase ()
            builder.AppendF ") AS " |> ignore
            builder.AppendF innerName |> ignore
            builder.AppendF " " |> ignore
            match terminalOperation.Expressions |> Array.tryItem 0 with
            | Some (:? UnaryExpression as ue) when (ue.Operand :? LambdaExpression) ->
                let lambda = ue.Operand :?> LambdaExpression
                match lambda.Body with
                | :? MemberExpression as me ->
                    builder.AppendF "JOIN json_each(jsonb_extract(" |> ignore
                    builder.AppendF innerName |> ignore
                    builder.AppendF ".Value, '$." |> ignore
                    builder.AppendF me.Member.Name |> ignore
                    builder.AppendF "'))" |> ignore
                | :? ParameterExpression ->
                    builder.AppendF "JOIN json_each(" |> ignore
                    builder.AppendF innerName |> ignore
                    builder.AppendF ".Value)" |> ignore
                | _ -> failwith "Unsupported SelectMany selector structure"
            | _ -> failwith "Invalid SelectMany structure"

        // --- Aggregate (not supported) ---
        | Aggregate -> failwith "Aggregate is not supported."

        // These should never be considered terminals here
        | Where | Select | ThenBy | ThenByDescending | OrderBy | Order | OrderDescending
        | OrderByDescending | Take | Skip ->
            (raise << InvalidOperationException) "Terminal operations that are not aggregates should not be here."

    
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
            UsedStatements = ResizeArray()
            TerminalOperation = None
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
            builder.AppendF "EXPLAIN QUERY PLAN "


        let pq = preprocessQuery expression

        if doesNotReturnIdFn expression then
            builder.AppendF "SELECT -1 as Id, "
            builder.AppendF valueDecoded
            builder.AppendF "as ValueJSON FROM ("
            // translate builder expression
            translateFlattenedQuery builder pq
            builder.AppendF ")"
        else
            builder.AppendF "SELECT Id, "
            builder.AppendF valueDecoded
            builder.AppendF "as ValueJSON FROM ("
            // translate builder expression
            translateFlattenedQuery builder pq
            builder.AppendF ")"


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
                    match connection.Query<Types.DbObjectRow>(query, variables) |> Seq.tryHead with
                    | None ->
                        match expression with
                        | :? MethodCallExpression as mce when let name = mce.Method.Name in name.EndsWith "OrDefault" ->
                            Unchecked.defaultof<'TResult>
                        | _other -> failwithf "Sequence contains no elements"
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
