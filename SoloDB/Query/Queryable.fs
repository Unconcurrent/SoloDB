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
open Connections
open SoloDatabase.RelationsTypes
open SoloDatabase.JsonSerializator
open Microsoft.Data.Sqlite

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
| Include
| Exclude

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
    Expressions: Expression array |}
| RootQuery of IRootQueryable

/// <summary>
/// Represents an ORDER BY statement captured during preprocessing.
/// </summary>
type private PreprocessedOrder = {
    OrderingRule: Expression;
    mutable Descending: bool
    /// When set, emitted directly as raw SQL instead of translating OrderingRule.
    RawSQL: string option
}

type private SQLSelector =
| Expression of Expression
| KeyProjection of Expression
| Raw of (string -> StringBuilder -> Dictionary<string, obj> -> unit)

type private UsedSQLStatements = 
    {
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
    member this.IsEmptyWithTableName =
        this.Filters.Count = 0
        && this.Orders.Count = 0
        && this.Selector.IsNone
        && this.Skip.IsNone
        && this.Take.IsNone
        && this.UnionAll.Count = 0
        && this.TableName.Length > 0

type private SQLSubquery =
    | Simple of UsedSQLStatements
    | Complex of (struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|} -> unit)

type private PredicateRole =
| WherePredicate
| AnyPredicate
| AllPredicate
| CountPredicate
| LongCountPredicate

type private RelationAccessKind =
| NoRelationAccess
| HasRelationAccess

type private LoweredPredicate = {
    Role: PredicateRole
    Predicate: Expression
    LayerPosition: LayerPosition
    RelationAccess: RelationAccessKind
    MaterializedPathsSnapshot: string array
}

type private KeySelectorRole =
| DistinctByKey
| GroupByKey

type private LoweredKeySelector = {
    Role: KeySelectorRole
    KeyExpression: Expression
    RelationAccess: RelationAccessKind
    Fingerprint: string
}

/// <summary>
/// A private, mutable builder used to construct an SQL query from a LINQ expression tree.
/// </summary>
/// <remarks>This type holds the state of the translation process, including the SQL command being built and any parameters.</remarks>
type private QueryableBuilder = 
    {
        /// The StringBuilder that accumulates the generated SQL string.
        SQLiteCommand: StringBuilder
        /// A dictionary of parameters to be passed to the SQLite command.
        Variables: Dictionary<string, obj>
        /// This list will be using to know if it is necessary to create another SQLite subquery to finish the translation, by checking if all the available slots have been used.
        Subqueries: ResizeArray<SQLSubquery>
    }
    /// <summary>Appends a string to the current SQL command.</summary>
    member this.Append(text: string) =
        ignore (this.SQLiteCommand.Append text)

    /// <summary>Appends a StringBuilder to the current SQL command.</summary>
    member this.Append(text: StringBuilder) =
        ignore (this.SQLiteCommand.Append text)

    /// <summary>Adds a variable to the parameters dictionary and appends its placeholder to the SQL command.</summary>
    member this.AppendVar(variable: obj) =
        QueryTranslator.appendVariable this.SQLiteCommand this.Variables variable


/// <summary>
/// Contains private helper functions for translating LINQ expression trees to SQL.
/// </summary>
module private QueryHelper =
    let private unwrapQuotedLambda (expr: Expression) =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote -> ue.Operand
        | _ -> expr

    /// Try to extract a relation JSON path from a lambda expression.
    /// For `o => o.Customer.Value.Rating`, returns Some "$.Customer[1].Rating".
    /// Only handles DBRef<T>.Value.Property single-hop patterns.
    /// Caller is responsible for guarding that the relation will be materialized.
    let private tryExtractRelationJsonPath (expr: Expression) : string option =
        let unwrapped = unwrapQuotedLambda expr
        match unwrapped with
        | :? LambdaExpression as lambda ->
            // Pattern: param.RelProp.Value.ScalarProp
            match lambda.Body with
            | :? MemberExpression as outerMember ->
                let scalarProp = outerMember.Member.Name
                match outerMember.Expression with
                | :? MemberExpression as valueMember when valueMember.Member.Name = "Value" ->
                    match valueMember.Expression with
                    | :? MemberExpression as _relMember ->
                        let relProp = _relMember.Member.Name
                        Some (sprintf "$.%s[1].%s" relProp scalarProp)
                    | _ -> None
                | _ -> None
            | _ -> None
        | _ -> None

    let private expressionFingerprint (expr: Expression) =
        let unwrapped = unwrapQuotedLambda expr
        match unwrapped with
        | :? LambdaExpression as lambda -> lambda.Body.ToString()
        | _ -> unwrapped.ToString()

    let rec private detectRelationAccessNode (expr: Expression) =
        if isNull expr then false
        else
            match expr with
            | :? MemberExpression as m ->
                let declaring = m.Member.DeclaringType
                let hitsRelationMember =
                    not (isNull declaring)
                    && declaring.IsGenericType
                    && (
                        declaring.GetGenericTypeDefinition() = typedefof<DBRef<_>>
                        || declaring.GetGenericTypeDefinition() = typedefof<DBRefMany<_>>
                    )
                hitsRelationMember || detectRelationAccessNode m.Expression
            | :? MethodCallExpression as m ->
                let declaring = m.Method.DeclaringType
                let hitsRelationCall =
                    not (isNull declaring)
                    && declaring.IsGenericType
                    && declaring.GetGenericTypeDefinition() = typedefof<DBRefMany<_>>
                hitsRelationCall
                || (not (isNull m.Object) && detectRelationAccessNode m.Object)
                || (m.Arguments |> Seq.exists detectRelationAccessNode)
            | :? LambdaExpression as l ->
                detectRelationAccessNode l.Body
            | :? UnaryExpression as u ->
                detectRelationAccessNode u.Operand
            | :? BinaryExpression as b ->
                detectRelationAccessNode b.Left || detectRelationAccessNode b.Right
            | :? ConditionalExpression as c ->
                detectRelationAccessNode c.Test
                || detectRelationAccessNode c.IfTrue
                || detectRelationAccessNode c.IfFalse
            | :? InvocationExpression as i ->
                detectRelationAccessNode i.Expression || (i.Arguments |> Seq.exists detectRelationAccessNode)
            | :? NewArrayExpression as na ->
                na.Expressions |> Seq.exists detectRelationAccessNode
            | :? NewExpression as n ->
                n.Arguments |> Seq.exists detectRelationAccessNode
            | :? MemberInitExpression as mi ->
                detectRelationAccessNode mi.NewExpression
                || (mi.Bindings |> Seq.exists (function
                    | :? MemberAssignment as ma -> detectRelationAccessNode ma.Expression
                    | _ -> false))
            | :? ListInitExpression as li ->
                detectRelationAccessNode li.NewExpression
                || (li.Initializers |> Seq.exists (fun init -> init.Arguments |> Seq.exists detectRelationAccessNode))
            | _ -> false

    let private detectRelationAccess (expr: Expression) =
        if detectRelationAccessNode expr then HasRelationAccess else NoRelationAccess

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
        | "Include" -> Some Include
        | "Exclude" -> Some Exclude
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

    let rec private isIdentityLambda (expr: Expression) =
        match expr with
        // Unwrap the quote node if present
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
            isIdentityLambda ue.Operand
        // Check if it's a lambda
        | :? LambdaExpression as lambda ->
            match lambda.Body with
            | :? ParameterExpression as bodyParam ->
                Object.ReferenceEquals(bodyParam, lambda.Parameters.[0])
            | _ -> false
        | _ -> false

    let private negatePredicateExpression (expr: Expression) =
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
            match ue.Operand with
            | :? LambdaExpression as lambda ->
                let negated = Expression.Lambda(Expression.Not(lambda.Body), lambda.Parameters)
                Expression.Quote negated :> Expression
            | _ ->
                Expression.Not expr :> Expression
        | :? LambdaExpression as lambda ->
            Expression.Lambda(Expression.Not(lambda.Body), lambda.Parameters) :> Expression
        | _ ->
            Expression.Not expr :> Expression

    let private emptySQLStatement () =
        { Filters = ResizeArray(4); Orders = ResizeArray(1); Selector = None; Skip = None; Take = None; TableName = ""; UnionAll = ResizeArray(0) }

    let inline private simpleCurrent (statements: ResizeArray<SQLSubquery>) =
        match statements.Last() with
        | Simple s -> s
        | Complex _ ->
            let n = emptySQLStatement ()
            statements.Add (Simple n)
            n

    let inline private ifSelectorNewStatement (statements: ResizeArray<SQLSubquery>) =
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

    let addFilter (statements: ResizeArray<SQLSubquery>) (filter: Expression) =
        let last = statements.Last()
        let inline addNewQuery () =
            (statements.Add << Simple) { emptySQLStatement () with Filters = ResizeArray [ filter ] }

        match last with
        | Simple last when last.Selector.IsNone && last.UnionAll.Count = 0 && last.Skip.IsNone && last.Take.IsNone ->
            last.Filters.Add filter
        | _ ->
            addNewQuery ()

    let private lowerPredicateLambda (_sourceCtx: QueryContext) (_tableName: string) (expr: Expression) (role: PredicateRole) =
        let relationAccess = detectRelationAccess expr
        // First migration-family seam: centralize predicate lowering call sites
        // while preserving the current translator and payload behavior byte-for-byte.
        {
            Role = role
            Predicate = expr
            LayerPosition = _sourceCtx.LayerPosition
            RelationAccess = relationAccess
            MaterializedPathsSnapshot = _sourceCtx.MaterializedPaths |> Seq.toArray
        }

    let private addLoweredPredicate (statements: ResizeArray<SQLSubquery>) (lowered: LoweredPredicate) =
        addFilter statements lowered.Predicate

    let private lowerKeySelectorLambda (_sourceCtx: QueryContext) (_tableName: string) (expr: Expression) (role: KeySelectorRole) =
        // Second migration-family seam: centralize key-selector lowering call sites
        // while preserving the current KeyProjection behavior for the inner layer.
        {
            Role = role
            KeyExpression = expr
            RelationAccess = detectRelationAccess expr
            Fingerprint = expressionFingerprint expr
        }

    let addOrder (statements: ResizeArray<SQLSubquery>) (ordering: Expression) (descending: bool) =
        let current = ifSelectorNewStatement statements
        current.Orders.Clear()
        current.Orders.Add({ OrderingRule = ordering; Descending = descending; RawSQL = None })

    let addSelector (statements: ResizeArray<SQLSubquery>) (selector: SQLSelector) =
        let last = statements.Last()
        match last with
        | Simple last when last.UnionAll.Count = 0 ->
            match last.Selector with
            | Some _ -> 
                (statements.Add << Simple) { emptySQLStatement () with Selector = Some selector }
            | None ->
                last.Selector <- Some selector
        | _ ->
            (statements.Add << Simple) { emptySQLStatement () with Selector = Some selector }

    let private addLoweredKeySelector (statements: ResizeArray<SQLSubquery>) (lowered: LoweredKeySelector) =
        addSelector statements (KeyProjection lowered.KeyExpression)

    let addTake (statements: ResizeArray<SQLSubquery>) (e: Expression) =
        let current = simpleCurrent statements
        match current.Take with
        | Some e2 -> current.Take <- Some (ExpressionHelper.min e2 e)
        | None   -> current.Take <- Some e

    /// The Complex subquery will be the last subquery processes, it has the WriteInner function to place the inner subqueries where necesary.
    let addComplexFinal (statements: ResizeArray<SQLSubquery>) (writeFunc: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|} -> unit) =
        statements.Add (Complex writeFunc)

    /// <summary>
    /// Recursively translates a LINQ expression tree into an SQL query.
    /// This is the main dispatcher for the translation process.
    /// </summary>
    /// <param name="builder">The query builder instance that accumulates the SQL and parameters.</param>
    /// <param name="expression">The expression to translate.</param>
    let private aggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (Raw (fun tableName builder vars ->
            builder.Append "-1 AS Id, " |> ignore
            builder.Append fnName |> ignore
            builder.Append "(" |> ignore

            match args.Length with
            | 0 -> QueryTranslator.translateQueryableWithContext sourceCtx tableName (method.ReturnType |> ExpressionHelper.id) builder vars
            | 1 -> QueryTranslator.translateQueryableWithContext sourceCtx tableName args.[0] builder vars
            | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" method.Name other))

            builder.Append ") AS Value " |> ignore
        ))

    let private zeroIfNullAggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (Raw (fun tableName builder vars ->
            builder.Append "-1 AS Id, " |> ignore
            builder.Append "COALESCE(" |> ignore
            builder.Append fnName |> ignore
            builder.Append "(" |> ignore

            match args.Length with
            | 0 -> QueryTranslator.translateQueryableWithContext sourceCtx tableName (method.ReturnType |> ExpressionHelper.id) builder vars
            | 1 -> QueryTranslator.translateQueryableWithContext sourceCtx tableName args.[0] builder vars
            | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" method.Name other))

            builder.Append "),0) AS Value " |> ignore
        ))

    /// <summary>
    /// Extracts the expression for a collection that is an argument to a set-based method like Concat or Except.
    /// </summary>
    /// <param name="methodArg">The method argument expression.</param>
    /// <returns>The expression representing the queryable collection.</returns>
    let private readSoloDBQueryable<'T> (methodArg: Expression) =
        let unsupportedConcatMessage = "Cannot concat with an IEnumerable other than another SoloDB IQueryable on the same connection. To do this anyway, use AsEnumerable()."
        match methodArg with
        | :? ConstantExpression as ce -> 
            match QueryTranslator.evaluateExpr<IEnumerable> ce with
            | :? IQueryable<'T> as appendingQuery when (match appendingQuery.Provider with :? SoloDBQueryProvider -> true | _other -> false) ->
                appendingQuery.Expression
            | :? IRootQueryable as rq ->
                Expression.Constant(rq, typeof<IRootQueryable>)
            | _other -> raise (NotSupportedException(unsupportedConcatMessage))
        | :? MethodCallExpression as mcl ->
            mcl
        | _other -> raise (NotSupportedException(unsupportedConcatMessage))

    let private addUnionAll (queries: SQLSubquery ResizeArray) (fn: string -> StringBuilder -> Dictionary<string, obj> -> unit) =
        match queries.Last() with
        | Simple last when last.Orders.Count = 0 ->
            last.UnionAll.Add fn
        | _ ->
            queries.Add (Simple { emptySQLStatement () with UnionAll = ResizeArray [ fn ] })

    /// <summary>
    /// Wraps an aggregate function translator to handle cases where the source sequence is empty, which would result in a NULL from SQL.
    /// This function generates SQL to return a specific error message that can be caught and thrown as an exception, mimicking .NET behavior.
    /// </summary>
    /// <param name="fnName">The name of the SQL aggregate function (e.g., "AVG", "MIN").</param>
    /// <param name="builder">The query builder instance.</param>
    /// <param name="expression">The LINQ method call expression for the aggregate.</param>
    /// <param name="errorMsg">The error message to return if the aggregation result is NULL.</param>
    let private raiseIfNullAggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) (errorMsg: string) =
        aggregateTranslator sourceCtx fnName queries method args
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
    let private preprocessQuery (expression: Expression) : PreprocessedQuery seq = seq {
        let mutable expression = expression

        while expression <> null do
            match expression with
            | :? MethodCallExpression as mce ->
                match parseSupportedMethod mce.Method.Name with
                | Some value ->
                    expression <- mce.Arguments.[0]
                    let exprs = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                    Method {| Value = value; Expressions = exprs; OriginalMethod = mce.Method |}
                | None ->
                    raise (NotSupportedException(
                        sprintf "Error: Queryable method '%s' is not supported.\nReason: The expression cannot be translated to SQL.\nFix: Call AsEnumerable() before this method or rewrite the query to a supported shape." mce.Method.Name))
            | :? ConstantExpression as ce when typeof<IRootQueryable>.IsAssignableFrom ce.Type ->
                RootQuery (ce.Value :?> IRootQueryable)
                expression <- null
            | e ->
                raise (NotSupportedException(
                    sprintf "Error: Cannot preprocess expression of type %A.\nReason: The expression shape is not supported for SQL translation.\nFix: Simplify the expression or switch to AsEnumerable() before this operation." e.NodeType))
    }

    let private tryExtractExcludePath (expressions: Expression array) =
        if expressions.Length < 1 then
            None
        else
            let selectorExprOpt =
                try
                    let arg = expressions.[0]
                    let lambdaExpr =
                        if arg.NodeType = ExpressionType.Quote then (arg :?> UnaryExpression).Operand :?> LambdaExpression
                        else arg :?> LambdaExpression
                    Some lambdaExpr
                with _ ->
                    None

            match selectorExprOpt with
            | None -> None
            | Some selectorExpr ->
                let rec extractPath (e: Expression) =
                    match e with
                    | :? MemberExpression as me ->
                        let parent = extractPath me.Expression
                        if parent = "" then me.Member.Name else parent + "." + me.Member.Name
                    | :? ParameterExpression -> ""
                    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Convert -> extractPath ue.Operand
                    | _ -> ""

                match extractPath selectorExpr.Body with
                | "" -> None
                | path -> Some path

    let private extractRelationPathOrThrow (directiveName: string) (expressions: Expression array) =
        match tryExtractExcludePath expressions with
        | Some path -> path
        | None ->
            raise (NotSupportedException(
                $"Error: {directiveName} selector is not supported.\nReason: Only direct member-path selectors are supported for relation directives.\nFix: Use a selector like x => x.Ref or x => x.RefMany (member path only)."))

    let private registerExcludePath (sourceCtx: QueryContext) path =
        if not (String.IsNullOrWhiteSpace path) then
            sourceCtx.ExcludedPaths.Add(path) |> ignore

    let private registerIncludePath (sourceCtx: QueryContext) path =
        if not (String.IsNullOrWhiteSpace path) then
            sourceCtx.IncludedPaths.Add(path) |> ignore

    let private validateIncludeExcludeConflicts (ctx: QueryContext) =
        for path in ctx.IncludedPaths do
            if ctx.ExcludedPaths.Contains(path) then
                raise (InvalidOperationException(
                    $"Error: Path '{path}' is both included and excluded.\nReason: Include/Exclude conflict on same relation path.\nFix: Keep only one directive for this path."))

    let private shouldLoadRelationPath (sourceCtx: QueryContext) (path: string) =
        if sourceCtx.ExcludedPaths.Contains(path) then false
        elif sourceCtx.IncludedPaths.Count > 0 then sourceCtx.IncludedPaths.Contains(path)
        else true

    let private cloneQueryContext (sourceCtx: QueryContext) =
        let typeCollections = Dictionary(System.StringComparer.Ordinal)
        for kv in sourceCtx.TypeCollections do
            typeCollections.[kv.Key] <- HashSet<string>(kv.Value, System.StringComparer.Ordinal)
        {
            RootTable = sourceCtx.RootTable
            LayerPosition = OuterLayer
            RootGraph =
                {
                    Roots = ResizeArray(sourceCtx.RootGraph.Roots |> Seq.map id)
                    SourceAliasCounter = sourceCtx.RootGraph.SourceAliasCounter
                }
            Joins = ResizeArray()
            AliasCounter = 0
            ExcludedPaths = HashSet(sourceCtx.ExcludedPaths, System.StringComparer.Ordinal)
            IncludedPaths = HashSet(sourceCtx.IncludedPaths, System.StringComparer.Ordinal)
            MaterializedPaths = HashSet(sourceCtx.MaterializedPaths, System.StringComparer.Ordinal)
            RelationTargets = Dictionary(sourceCtx.RelationTargets, System.StringComparer.Ordinal)
            RelationLinks = Dictionary(sourceCtx.RelationLinks, System.StringComparer.Ordinal)
            RelationOwnerUsesSource = Dictionary(sourceCtx.RelationOwnerUsesSource, System.StringComparer.Ordinal)
            TypeCollections = typeCollections
        }

    let private serializeForCollection (value: 'T) =
        struct (
            match typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T> with
            | true -> JsonSerializator.JsonValue.SerializeWithType value
            | false -> JsonSerializator.JsonValue.Serialize value
            |> _.ToJsonString(), HasTypeId<'T>.Value
        )

    /// Appends WHERE, ORDER BY, LIMIT/OFFSET directly to the main builder.
    let private writeClauses
        (sourceCtx: QueryContext)
        (builder: QueryableBuilder)
        (statement: UsedSQLStatements)
        (contextTable: string) =
        
        if statement.Filters.Count <> 0 then
            builder.Append(" WHERE ") |> ignore
            statement.Filters
            |> Seq.iteri (fun j f ->
                if j > 0 then builder.Append(" AND ") |> ignore
                // write predicates straight into the main command
                QueryTranslator.translateQueryableWithContext sourceCtx contextTable f builder.SQLiteCommand builder.Variables
            )

        if statement.UnionAll.Count <> 0 then
            // If there are UNION ALL statements, we need to append them to the main command.
            builder.Append(" UNION ALL ") |> ignore
            statement.UnionAll
            |> Seq.iteri (fun j unionAll ->
                if j > 0 then builder.Append(" UNION ALL ") |> ignore
                unionAll contextTable builder.SQLiteCommand builder.Variables
            )
            |> ignore

        if statement.Orders.Count <> 0 then
            builder.Append(" ORDER BY ") |> ignore
            statement.Orders
            |> Seq.iteri (fun j o ->
                if j > 0 then builder.Append(", ") |> ignore
                match o.RawSQL with
                | Some raw -> builder.Append raw |> ignore
                | None -> QueryTranslator.translateQueryableWithContext sourceCtx contextTable o.OrderingRule builder.SQLiteCommand builder.Variables
                if o.Descending then builder.Append(" DESC") |> ignore
            )

        match statement.Take, statement.Skip with
        | Some take, Some skip ->
            builder.Append(" LIMIT ") |> ignore
            builder.AppendVar (QueryTranslator.evaluateExpr<obj> take) |> ignore
            builder.Append(" OFFSET ") |> ignore
            builder.AppendVar (QueryTranslator.evaluateExpr<obj> skip) |> ignore
        | Some take, None ->
            builder.Append(" LIMIT ") |> ignore
            builder.AppendVar (QueryTranslator.evaluateExpr<obj> take) |> ignore
        | None, Some skip ->
            builder.Append(" LIMIT -1 OFFSET ") |> ignore
            builder.AppendVar (QueryTranslator.evaluateExpr<obj> skip) |> ignore
        | None, None -> ()


    let private writeLayers<'T>
        (sourceCtx: QueryContext)
        (builder: QueryableBuilder)
        (layers: SQLSubquery ResizeArray)
        =

        let layerCount = layers.Count

        let tableName =
            if layerCount > 0 then
                match layers.[0] with
                | Simple layer ->
                    layer.TableName
                | Complex _ ->
                    ""
            else
                ""

        let rec writeLayer (i: int) =
            let layer = layers.[i]
            match layer with
            | Simple layer when layer.IsEmptyWithTableName ->
                let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                if isTypePrimitive then
                    builder.Append "SELECT Id, jsonb_extract(Value, '$') as Value FROM " |> ignore
                    builder.Append "\""
                    builder.Append layer.TableName
                    builder.Append "\""
                else
                    builder.Append "\""
                    builder.Append layer.TableName
                    builder.Append "\""
                
            | Simple layer ->
                let isLocalKeyProjection =
                    match layer.Selector with
                    | Some (KeyProjection _) -> true
                    | _ -> false
                let currentCtx =
                    if isLocalKeyProjection then cloneQueryContext sourceCtx
                    else sourceCtx
                let effectiveTableName = layer.TableName
                // Track Value column position for potential json_set materialization.
                // mutable because it's set inside the match below but used after JOIN discovery.
                let mutable valueColumnRange = ValueNone

                // Qualify column names with table name when present (needed for JOINed queries).
                let hasTableName = not (String.IsNullOrEmpty effectiveTableName)
                let idColumn = if hasTableName then sprintf "\"%s\".Id" effectiveTableName else "Id"
                let valueColumn = if hasTableName then sprintf "\"%s\".Value" effectiveTableName else "Value"

                match layer.Selector with
                | Some (Expression selector) ->
                    builder.Append "SELECT " |> ignore
                    builder.Append idColumn |> ignore
                    builder.Append ", " |> ignore
                    QueryTranslator.translateQueryableWithContext sourceCtx layer.TableName selector builder.SQLiteCommand builder.Variables
                    builder.Append "AS Value " |> ignore
                | Some (KeyProjection selector) ->
                    let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                    builder.Append "SELECT " |> ignore
                    builder.Append idColumn |> ignore
                    builder.Append ", " |> ignore
                    if isTypePrimitive then
                        builder.Append "jsonb_extract(" |> ignore
                        builder.Append valueColumn |> ignore
                        builder.Append ", '$') AS Value, " |> ignore
                    else
                        let vStart = builder.SQLiteCommand.Length
                        builder.Append valueColumn |> ignore
                        builder.Append " AS Value" |> ignore
                        let vEnd = builder.SQLiteCommand.Length
                        valueColumnRange <- ValueSome struct(vStart, vEnd)
                        builder.Append ", " |> ignore
                    QueryTranslator.translateQueryableWithContext currentCtx effectiveTableName selector builder.SQLiteCommand builder.Variables
                    builder.Append " AS __solodb_group_key " |> ignore
                | Some (Raw func) ->
                    builder.Append "SELECT " |> ignore
                    func layer.TableName builder.SQLiteCommand builder.Variables
                | None ->
                    let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                    if isTypePrimitive then
                        builder.Append "SELECT " |> ignore
                        builder.Append idColumn |> ignore
                        builder.Append ", jsonb_extract(" |> ignore
                        builder.Append valueColumn |> ignore
                        builder.Append ", '$') as Value " |> ignore
                    else
                        builder.Append "SELECT " |> ignore
                        builder.Append idColumn |> ignore
                        builder.Append ", " |> ignore
                        let vStart = builder.SQLiteCommand.Length
                        builder.Append valueColumn |> ignore
                        builder.Append " " |> ignore
                        let vEnd = builder.SQLiteCommand.Length
                        valueColumnRange <- ValueSome struct(vStart, vEnd)

                builder.Append "FROM " |> ignore
                let joinInsertPoint =
                    if i = 0 then
                        builder.Append "\""
                        builder.Append layer.TableName
                        builder.Append "\""
                        ValueSome builder.SQLiteCommand.Length
                    else
                        builder.Append "(" |> ignore
                        writeLayer (i - 1)
                        builder.Append ")" |> ignore
                        ValueNone

                writeClauses
                    (if isLocalKeyProjection then currentCtx else sourceCtx)
                    builder
                    layer
                    (if isLocalKeyProjection then effectiveTableName else layer.TableName)

                // After expression translation, emit LEFT JOINs + materialization SELECT rewriting.
                // JOINs are discovered during writeClauses but appear between FROM "Table" and WHERE.
                match joinInsertPoint with
                | ValueSome pos ->
                    match currentCtx.Joins.Count > 0 with
                    | true ->
                        // Step 1: Rewrite SELECT Value → json_set(Value, ...) for materialization.
                        // Do this FIRST because it's earlier in the string — adjusting joinInsertPoint afterward.
                        let mutable posAdjustment = 0
                        match valueColumnRange with
                        | ValueSome struct(vStart, vEnd) ->
                            let jsonSetSb = StringBuilder()
                            jsonSetSb.Append("jsonb_set(\"") |> ignore
                            jsonSetSb.Append(effectiveTableName) |> ignore
                            jsonSetSb.Append("\".Value") |> ignore
                            let mutable hasMaterialization = false
                            for j in currentCtx.Joins do
                                // Skip materialization for excluded paths — the DBRef stays as raw integer (Unloaded).
                                // The JOIN itself is still emitted (needed for WHERE/ORDER BY).
                                if shouldLoadRelationPath currentCtx j.PropertyPath then
                                    hasMaterialization <- true
                                    currentCtx.MaterializedPaths.Add(j.PropertyPath) |> ignore
                                    jsonSetSb.Append(", '$.") |> ignore
                                    jsonSetSb.Append(j.PropertyPath) |> ignore
                                    jsonSetSb.Append("', CASE WHEN ") |> ignore
                                    jsonSetSb.Append(j.TargetAlias) |> ignore
                                    jsonSetSb.Append(".Id IS NOT NULL THEN jsonb_array(") |> ignore
                                    jsonSetSb.Append(j.TargetAlias) |> ignore
                                    jsonSetSb.Append(".Id, ") |> ignore
                                    jsonSetSb.Append(j.TargetAlias) |> ignore
                                    jsonSetSb.Append(".Value) ELSE jsonb_extract(\"") |> ignore
                                    jsonSetSb.Append(effectiveTableName) |> ignore
                                    jsonSetSb.Append("\".Value, '$.") |> ignore
                                    jsonSetSb.Append(j.PropertyPath) |> ignore
                                    jsonSetSb.Append("') END") |> ignore
                            if hasMaterialization then
                                jsonSetSb.Append(") as Value ") |> ignore
                            else
                                // All joins are excluded — no materialization needed, keep original qualified Value
                                jsonSetSb.Clear() |> ignore
                                jsonSetSb.Append("\"") |> ignore
                                jsonSetSb.Append(effectiveTableName) |> ignore
                                jsonSetSb.Append("\".Value ") |> ignore
                            let replacement = jsonSetSb.ToString()
                            let originalLen = vEnd - vStart
                            builder.SQLiteCommand.Remove(vStart, originalLen) |> ignore
                            builder.SQLiteCommand.Insert(vStart, replacement) |> ignore
                            posAdjustment <- replacement.Length - originalLen
                        | ValueNone -> ()

                        // Step 2: Insert LEFT JOIN clauses after FROM "TableName".
                        let adjustedPos = pos + posAdjustment
                        let joinSql = StringBuilder()
                        for j in currentCtx.Joins do
                            joinSql.Append(' ') |> ignore
                            joinSql.Append(j.JoinKind) |> ignore
                            joinSql.Append(" \"") |> ignore
                            joinSql.Append(j.TargetTable) |> ignore
                            joinSql.Append("\" AS ") |> ignore
                            joinSql.Append(j.TargetAlias) |> ignore
                            joinSql.Append(" ON ") |> ignore
                            joinSql.Append(j.OnCondition) |> ignore
                        builder.SQLiteCommand.Insert(adjustedPos, joinSql.ToString()) |> ignore
                    | _ -> ()
                | ValueNone -> ()
            | Complex writeFunc ->
                let tableName =
                    if i = 0 then
                        tableName
                    else
                        ""

                writeFunc {|
                    Command = builder.SQLiteCommand 
                    Vars = builder.Variables
                    WriteInner = fun () -> writeLayer (i - 1)
                    TableName = tableName
                |}

        writeLayer (layerCount - 1)


    let rec private buildQuery<'T> (sourceCtx: QueryContext) (statements: SQLSubquery ResizeArray) (e: Expression) =
        statements.Add (emptySQLStatement () |> Simple)

        let mutable tableName = ""
        let mutable pendingDistinctByScalarReuse : LoweredKeySelector option = None
        // Set when Select consumes a carried scalar slot (DistinctBy → Select reuse path).
        // Terminal zero-arg aggregates use this to consume Value directly instead of retranslating.
        let mutable isPostScalarProjection = false
        let preprocessed = preprocessQuery e |> Seq.toArray

        // Register Include/Exclude paths up-front so behavior is deterministic regardless method-call order.
        // If a predicate accesses an excluded path via .Value, translator must fail loudly in both orders.
        for q in preprocessed do
            match q with
            | Method m when m.Value = Exclude || m.Value = Include ->
                let path =
                    if m.Value = Exclude then extractRelationPathOrThrow "Exclude" m.Expressions
                    else extractRelationPathOrThrow "Include" m.Expressions
                if m.Value = Exclude then registerExcludePath sourceCtx path
                else registerIncludePath sourceCtx path
            | _ -> ()

        validateIncludeExcludeConflicts sourceCtx

        for q in preprocessed |> Array.rev do
            match q with
            | RootQuery rq -> tableName <- rq.SourceTableName
            | Method m ->
                let inline simpleCurrent() = simpleCurrent statements

                if m.Value <> Select && m.Value <> DistinctBy
                   && m.Value <> OrderBy && m.Value <> OrderByDescending
                   && m.Value <> ThenBy && m.Value <> ThenByDescending then
                    pendingDistinctByScalarReuse <- None

                match m.Value with
                | Where -> 
                    addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName m.Expressions.[0] WherePredicate)
                | Select ->
                    let selectFingerprint = expressionFingerprint m.Expressions.[0]
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = selectFingerprint ->
                        addSelector statements (Raw (fun tableName builder _vars ->
                            let hasTableName = not (String.IsNullOrEmpty tableName)
                            let idColumn = if hasTableName then sprintf "\"%s\".Id" tableName else "Id"
                            builder.Append idColumn |> ignore
                            builder.Append " AS Id, " |> ignore
                            builder.Append "__solodb_scalar_slot0 AS Value " |> ignore
                        ))
                        pendingDistinctByScalarReuse <- None
                        isPostScalarProjection <- true
                    | Some lowered when lowered.RelationAccess = HasRelationAccess ->
                        // C12 path: post-DistinctBy Select on a DIFFERENT relation scalar than
                        // the DistinctBy key. The base layer will materialize the relation into
                        // Value via jsonb_set, so we extract from the materialized JSON payload.
                        match tryExtractRelationJsonPath m.Expressions.[0] with
                        | Some jsonPath ->
                            let capturedPath = jsonPath
                            addSelector statements (Raw (fun _tableName builder _vars ->
                                builder.Append "-1 AS Id, jsonb_extract(Value, '" |> ignore
                                builder.Append capturedPath |> ignore
                                builder.Append "') AS Value " |> ignore
                            ))
                            isPostScalarProjection <- true
                        | None ->
                            addSelector statements (Expression m.Expressions.[0])
                        pendingDistinctByScalarReuse <- None
                    | _ ->
                        addSelector statements (Expression m.Expressions.[0])
                        pendingDistinctByScalarReuse <- None
                | Order | OrderDescending ->
                    addOrder statements (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) (m.Value = OrderDescending)
                | OrderBy | OrderByDescending  ->
                    let descending = (m.Value = OrderByDescending)
                    let orderFingerprint = expressionFingerprint m.Expressions.[0]
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = orderFingerprint ->
                        let current = ifSelectorNewStatement statements
                        current.Orders.Clear()
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawSQL = Some "__solodb_scalar_slot0" })
                    | _ ->
                        addOrder statements m.Expressions.[0] descending
                | ThenBy | ThenByDescending ->
                    let descending = (m.Value = ThenByDescending)
                    let orderFingerprint = expressionFingerprint m.Expressions.[0]
                    let current = simpleCurrent()
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = orderFingerprint ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawSQL = Some "__solodb_scalar_slot0" })
                    | _ ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawSQL = None })
                | Skip ->
                    let current = simpleCurrent()
                    match current.Skip with
                    | Some _ -> statements.Add(Simple { emptySQLStatement () with Skip = Some m.Expressions.[0] })
                    | None   -> current.Skip <- Some m.Expressions.[0]
                | Take ->
                    addTake statements m.Expressions.[0]

                | Sum ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        addSelector statements (Raw (fun _tableName builder _vars ->
                            builder.Append "-1 AS Id, COALESCE(SUM(jsonb_extract(Value, '$')),0) AS Value " |> ignore
                        ))
                    else
                        // SUM() return NULL if all elements are NULL, TOTAL() return 0.0.
                        // TOTAL() always returns a float, therefore we will just check for NULL
                        zeroIfNullAggregateTranslator sourceCtx "SUM" statements m.OriginalMethod m.Expressions

                | Average ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        addSelector statements (Raw (fun _tableName builder _vars ->
                            builder.Append "-1 AS Id, AVG(jsonb_extract(Value, '$')) AS Value " |> ignore
                        ))
                        addSelector statements (Raw (fun _tableName builder _vars ->
                            builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN NULL ELSE -1 END AS Id, " |> ignore
                            builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN 'Sequence contains no elements' ELSE Value END AS Value " |> ignore
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "AVG" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | Min ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        addSelector statements (Raw (fun _tableName builder _vars ->
                            builder.Append "-1 AS Id, MIN(jsonb_extract(Value, '$')) AS Value " |> ignore
                        ))
                        addSelector statements (Raw (fun _tableName builder _vars ->
                            builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN NULL ELSE -1 END AS Id, " |> ignore
                            builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN 'Sequence contains no elements' ELSE Value END AS Value " |> ignore
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "MIN" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | Max ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        addSelector statements (Raw (fun _tableName builder _vars ->
                            builder.Append "-1 AS Id, MAX(jsonb_extract(Value, '$')) AS Value " |> ignore
                        ))
                        addSelector statements (Raw (fun _tableName builder _vars ->
                            builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN NULL ELSE -1 END AS Id, " |> ignore
                            builder.Append "CASE WHEN jsonb_extract(Value, '$') IS NULL THEN 'Sequence contains no elements' ELSE Value END AS Value " |> ignore
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "MAX" statements m.OriginalMethod m.Expressions "Sequence contains no elements"
                
                | Distinct ->
                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT -1 AS Id, Value FROM (" |> ignore
                        builder.WriteInner()
                        builder.Command.Append ") o GROUP BY " |> ignore
                        QueryTranslator.translateQueryableWithContext sourceCtx builder.TableName (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) builder.Command builder.Vars
                    )

                | DistinctBy ->
                    let lowered = lowerKeySelectorLambda sourceCtx tableName m.Expressions.[0] DistinctByKey
                    addLoweredKeySelector statements lowered
                    pendingDistinctByScalarReuse <-
                        match lowered.RelationAccess with
                        | HasRelationAccess -> Some lowered
                        | NoRelationAccess -> None
                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT o.Id AS Id, o.Value AS Value" |> ignore
                        if lowered.RelationAccess = HasRelationAccess then
                            builder.Command.Append ", o.__solodb_scalar_slot0 AS __solodb_scalar_slot0" |> ignore
                        builder.Command.Append " FROM (" |> ignore
                        builder.Command.Append "SELECT o.Id AS Id, o.Value AS Value" |> ignore
                        if lowered.RelationAccess = HasRelationAccess then
                            builder.Command.Append ", o.__solodb_group_key AS __solodb_scalar_slot0" |> ignore
                        builder.Command.Append ", ROW_NUMBER() OVER (PARTITION BY o.__solodb_group_key ORDER BY o.Id) AS __solodb_rn FROM (" |> ignore
                        builder.WriteInner()
                        builder.Command.Append ") o) o WHERE o.__solodb_rn = 1" |> ignore
                    )

                | GroupBy ->
                    addLoweredKeySelector statements (lowerKeySelectorLambda sourceCtx tableName m.Expressions.[0] GroupByKey)
                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT -1 as Id, json_object('Key', o.__solodb_group_key" |> ignore
                        // Create an array of all items with the same key
                        // Inject row Id into each grouped item payload so deserialization
                        // reconstructs correct entity identity (Id lives outside Value blob).
                        builder.Command.Append ", 'Items', json_group_array(jsonb_set(o.Value, '$.Id', o.Id))) as Value FROM (" |> ignore
                        builder.WriteInner()
                        // Group by the key selector
                        builder.Command.Append ") o GROUP BY o.__solodb_group_key" |> ignore
                    )

                | Count
                | CountBy
                | LongCount ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 ->
                        let role =
                            match m.Value with
                            | Count
                            | CountBy -> CountPredicate
                            | LongCount -> LongCountPredicate
                            | _ -> CountPredicate
                        addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName m.Expressions.[0] role)
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT COUNT(Id) as Value FROM " |> ignore
                        builder.Command.Append "(" |> ignore
                        builder.WriteInner()
                        builder.Command.Append ") o " |> ignore
                    )
                
                
                | SelectMany ->
                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
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
                            builder.WriteInner()
                            builder.Command.Append ") AS " |> ignore
                            builder.Command.Append innerSourceName |> ignore
                            builder.Command.Append " " |> ignore
                            // Extract the path to the collection selector (e.g., "$.Values")
                            match m.Expressions.[0] with
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
                                | _ ->
                                    raise (NotSupportedException(
                                        "Error: Unsupported SelectMany selector structure.\nReason: The selector cannot be translated to SQL.\nFix: Simplify the selector or move SelectMany after AsEnumerable()."))
                            | _ ->
                                raise (NotSupportedException(
                                    "Error: Invalid SelectMany structure.\nReason: The SelectMany arguments are not a supported query pattern.\nFix: Rewrite the query or move SelectMany after AsEnumerable()."))
                        | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                    )
                
                | Single | SingleOrDefault
                | First | FirstOrDefault ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 ->
                        match m.Expressions.[0] with
                        | :? LambdaExpression as expr -> addFilter statements expr
                        | expr when typeof<Expression>.IsAssignableFrom expr.Type -> addFilter statements expr
                        | _ -> raise (NotSupportedException("SingleOrDefault and FirstOrDefault are not supported with a default value argument."))
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    let limit =
                        match m.Value with
                        | Single | SingleOrDefault -> 2
                        | _ -> 1

                    addTake statements (ExpressionHelper.constant limit)

                | DefaultIfEmpty ->
                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT Id, Value FROM (" |> ignore
                        builder.WriteInner()
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
                        | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                        builder.Command.Append " WHERE NOT EXISTS (SELECT 1 FROM (" |> ignore
                        builder.WriteInner()
                        builder.Command.Append ") o)" |> ignore
                    )
                
                | Last | LastOrDefault ->
                    // SQlite does not guarantee the order of elements without an ORDER BY.
                    match m.Expressions.Length with
                    | 0 -> () // If no arguments are provided, we assume the last element is the one with the highest Id.
                    | 1 ->
                        addFilter statements m.Expressions.[0]
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %A: %A" m.Value other))

                    match statements.Last() with
                    | Simple x when x.Orders.Count <> 0 ->
                        for order in x.Orders do
                            order.Descending <- not order.Descending
                    | _ ->
                        addOrder statements (ExpressionHelper.get(fun (x: obj) -> x.Dyn<int64>("Id"))) true
                    addTake statements (ExpressionHelper.constant 1)

                | All ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 -> addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName (negatePredicateExpression m.Expressions.[0]) AllPredicate)
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addTake statements (ExpressionHelper.constant 1)

                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT -1 As Id, NOT EXISTS(SELECT 1 FROM (" |> ignore
                        builder.WriteInner()
                        builder.Command.Append ")) as Value" |> ignore
                    )

                | Any ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 -> addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName m.Expressions.[0] AnyPredicate)
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addTake statements (ExpressionHelper.constant 1)

                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT -1 As Id, EXISTS(SELECT 1 FROM (" |> ignore
                        builder.WriteInner()
                        builder.Command.Append ")) as Value" |> ignore
                    )


                | Contains ->
                    let struct (t, value) = 
                        match m.Expressions.[0] with
                        | :? ConstantExpression as ce -> struct (ce.Type, ce.Value)
                        | other -> raise (NotSupportedException(sprintf "Invalid Contains(...) parameter: %A" other))

                    let filter = (ExpressionHelper.eq t value)
                    addFilter statements filter
                    addTake statements (ExpressionHelper.constant 1)

                    addComplexFinal statements (fun (builder: struct {|Command: StringBuilder; Vars: Dictionary<string, obj>; WriteInner: unit -> unit; TableName: string|}) ->
                        builder.Command.Append "SELECT -1 As Id, EXISTS(SELECT 1 FROM (" |> ignore
                        builder.WriteInner()
                        builder.Command.Append ")) as Value" |> ignore
                    )
                
                | Append ->
                    addUnionAll statements (fun tableName builder vars ->
                        builder.Append "SELECT " |> ignore
                        let appendingObj = QueryTranslator.evaluateExpr<'T> m.Expressions.[0]

                        match QueryTranslator.isPrimitiveSQLiteType typeof<'T> with
                        | false ->
                            let struct (jsonStringElement, hasId) = serializeForCollection appendingObj
                            match hasId with
                            | false ->
                                builder.Append "-1 as Id," |> ignore
                            | true ->
                                let id = HasTypeId<'T>.Read appendingObj
                                builder.Append (sprintf "%i" id) |> ignore
                                builder.Append " as Id," |> ignore

                            let escapedString = QueryTranslator.escapeSQLiteString jsonStringElement
                            builder.Append "jsonb_extract('" |> ignore
                            builder.Append escapedString |> ignore
                            builder.Append "', '$')" |> ignore
                            builder.Append " As Value " |> ignore
                        | true ->
                            QueryTranslator.appendVariable builder vars appendingObj
                            builder.Append " As Value " |> ignore
                    )
                
                
                | Concat ->
                    // Left side is the current pipeline; append the right side as UNION ALL.
                    addUnionAll statements (fun _tableName sb vars ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        sb.Append "SELECT Id, " |> ignore
                        sb.Append (extractValueAsJsonIfNecesary (rhs.Type)) |> ignore
                        sb.Append "As Value FROM (" |> ignore

                        translateQuery sourceCtx {
                            Subqueries = ResizeArray<SQLSubquery>()
                            SQLiteCommand = sb
                            Variables = vars
                        } rhs

                        sb.Append ") o " |> ignore
                    )

                | Except ->
                    // Project left, remove rows whose JSON value appears in the right.
                    addComplexFinal statements (fun ctx ->
                        ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                        ctx.WriteInner()
                        ctx.Command.Append ") o WHERE jsonb_extract(Value, '$') NOT IN (" |> ignore

                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        ctx.Command.Append "SELECT " |> ignore
                        ctx.Command.Append "jsonb_extract(Value, '$') " |> ignore
                        ctx.Command.Append "As Value FROM (" |> ignore

                        translateQuery sourceCtx {
                            Subqueries = ResizeArray<SQLSubquery>()
                            SQLiteCommand = ctx.Command
                            Variables = ctx.Vars
                        } rhs

                        ctx.Command.Append ") o )" |> ignore
                    )

                | Intersect ->
                    // Keep only rows whose JSON value appears in the right.
                    addComplexFinal statements (fun ctx ->
                        ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                        ctx.WriteInner()
                        ctx.Command.Append ") o WHERE jsonb_extract(Value, '$') IN (" |> ignore

                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        ctx.Command.Append "SELECT " |> ignore
                        ctx.Command.Append "jsonb_extract(Value, '$') " |> ignore
                        ctx.Command.Append "As Value FROM (" |> ignore

                        translateQuery sourceCtx {
                            Subqueries = ResizeArray<SQLSubquery>()
                            SQLiteCommand = ctx.Command
                            Variables = ctx.Vars
                        } rhs

                        ctx.Command.Append ") o )" |> ignore
                    )

                | ExceptBy ->
                    // Arguments: other, keySelector
                    if m.Expressions.Length <> 2 then raise (NotSupportedException(sprintf "Invalid number of arguments, expected 2, in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    let keySelE =  m.Expressions.[1]
                    addComplexFinal statements (fun ctx ->
                        ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                        ctx.WriteInner()
                        ctx.Command.Append ") o WHERE " |> ignore
                        QueryTranslator.translateQueryableWithContext sourceCtx ctx.TableName keySelE ctx.Command ctx.Vars
                        ctx.Command.Append " NOT IN (" |> ignore

                        ctx.Command.Append "SELECT " |> ignore
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        match keySelE with
                        | keySelE when isIdentityLambda keySelE ->
                            ctx.Command.Append (extractValueAsJsonIfNecesary (rhs.Type)) |> ignore
                        | _ -> QueryTranslator.translateQueryableWithContext sourceCtx ctx.TableName keySelE ctx.Command ctx.Vars
                        ctx.Command.Append "As Value FROM (" |> ignore
                        
                        translateQuery sourceCtx {
                            Subqueries = ResizeArray<SQLSubquery>()
                            SQLiteCommand = ctx.Command
                            Variables = ctx.Vars
                        } rhs

                        ctx.Command.Append ") o )" |> ignore
                    )

                | IntersectBy ->
                    // Arguments: other, keySelector
                    if m.Expressions.Length <> 2 then raise (NotSupportedException(sprintf "Invalid number of arguments, expected 2, in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    let keySelE = m.Expressions.[1]
                    addComplexFinal statements (fun ctx ->
                        ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                        ctx.WriteInner()
                        ctx.Command.Append ") o WHERE " |> ignore
                        QueryTranslator.translateQueryableWithContext sourceCtx ctx.TableName keySelE ctx.Command ctx.Vars
                        ctx.Command.Append " IN (" |> ignore

                        ctx.Command.Append "SELECT " |> ignore
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        match keySelE with
                        | keySelE when isIdentityLambda keySelE ->
                            ctx.Command.Append (extractValueAsJsonIfNecesary (rhs.Type)) |> ignore
                        | _ -> QueryTranslator.translateQueryableWithContext sourceCtx ctx.TableName keySelE ctx.Command ctx.Vars
                        ctx.Command.Append "As Value FROM (" |> ignore
                        

                        translateQuery sourceCtx {
                            Subqueries = ResizeArray<SQLSubquery>()
                            SQLiteCommand = ctx.Command
                            Variables = ctx.Vars
                        } rhs

                        ctx.Command.Append ") o )" |> ignore
                    )

                // --- Type filtering / casting over polymorphic payloads ---
                | Cast ->
                    // Cast<TTarget>() with polymorphic guard & diagnostic messages.
                    if m.Expressions.Length <> 0 then raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    match GenericMethodArgCache.Get m.OriginalMethod |> Array.tryHead with
                    | None -> raise (NotSupportedException("Invalid type from Cast<T> method."))
                    | Some t when typeof<JsonSerializator.JsonValue>.IsAssignableFrom t ->
                        // No-op cast: just keep pipeline as-is (i.e., do nothing here).
                        ()
                    | Some t ->
                        match t |> typeToName with
                        | None -> raise (NotSupportedException("Incompatible type from Cast<T> method."))
                        | Some typeName ->
                            addComplexFinal statements (fun ctx ->
                                ctx.Command.Append "SELECT " |> ignore
                                // Id: preserve only if type info present and matches; else NULL
                                ctx.Command.Append "CASE WHEN jsonb_extract(Value, '$.$type') IS NULL THEN NULL " |> ignore
                                ctx.Command.Append "WHEN jsonb_extract(Value, '$.$type') <> " |> ignore
                                QueryTranslator.appendVariable ctx.Command ctx.Vars typeName
                                ctx.Command.Append " THEN NULL ELSE Id END AS Id, " |> ignore

                                // Value: propagate or attach error string
                                ctx.Command.Append "CASE " |> ignore
                                ctx.Command.Append "WHEN jsonb_extract(Value, '$.$type') IS NULL THEN json_quote('The type of item is not stored in the database, if you want to include it, then add the Polymorphic attribute to the type and reinsert all elements.') " |> ignore
                                ctx.Command.Append "WHEN jsonb_extract(Value, '$.$type') <> " |> ignore
                                QueryTranslator.appendVariable ctx.Command ctx.Vars typeName
                                ctx.Command.Append " THEN json_quote('Unable to cast object to the specified type, because the types are different.') " |> ignore
                                ctx.Command.Append "ELSE Value END AS Value FROM (" |> ignore
                                ctx.WriteInner()
                                ctx.Command.Append ") o " |> ignore
                            )

                | OfType ->
                    if m.Expressions.Length <> 0 then raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    match GenericMethodArgCache.Get m.OriginalMethod |> Array.tryHead with
                    | None -> raise (NotSupportedException("Invalid type from OfType<T> method."))
                    | Some t when typeof<JsonSerializator.JsonValue>.IsAssignableFrom t ->
                        // No-op filter to JsonValue
                        ()
                    | Some t ->
                        match t |> typeToName with
                        | None -> raise (NotSupportedException("Incompatible type from OfType<T> method."))
                        | Some typeName ->
                            addComplexFinal statements (fun ctx ->
                                ctx.Command.Append "SELECT Id, Value FROM (" |> ignore
                                ctx.WriteInner()
                                ctx.Command.Append ") o WHERE jsonb_extract(Value, '$.$type') = " |> ignore
                                QueryTranslator.appendVariable ctx.Command ctx.Vars typeName
                                ctx.Command.Append " " |> ignore
                            )

                | Exclude ->
                    // Extract property path from the selector lambda and add to ExcludedPaths.
                    // Exclude does not produce SQL — it only suppresses materialization for the specified path.
                    let path = extractRelationPathOrThrow "Exclude" m.Expressions
                    registerExcludePath sourceCtx path
                | Include ->
                    // Extract property path from the selector lambda and add to IncludedPaths.
                    // Include does not produce SQL — it only controls relation hydration whitelist.
                    let path = extractRelationPathOrThrow "Include" m.Expressions
                    registerIncludePath sourceCtx path

                | Aggregate ->
                    raise (NotSupportedException("Aggregate is not supported."))


        match statements.[0] with
        | Simple s ->
            statements.[0] <- Simple {s with TableName = tableName}
        | Complex _ ->
            ()

    and private translateQuery<'T> (sourceCtx: QueryContext) (builder: QueryableBuilder) (expression: Expression) =
        // Collect layers + possible terminal
        buildQuery<'T> sourceCtx builder.Subqueries expression
        writeLayers<'T> sourceCtx builder builder.Subqueries
    
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
            | Include
            | Exclude
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

    let private tableExists (connection: SqliteConnection) (tableName: string) =
        connection.QueryFirst<int64>(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name) THEN 1 ELSE 0 END",
            {| name = tableName |}) = 1L

    [<Struct>]
    type private RelationShapeInfo = {
        HasAny: bool
        HasSingle: bool
        HasMany: bool
    }

    let private relationShapeCache = System.Collections.Concurrent.ConcurrentDictionary<Type, RelationShapeInfo>()

    let private getRelationShape (t: Type) : RelationShapeInfo =
        relationShapeCache.GetOrAdd(t, Func<Type, RelationShapeInfo>(fun t ->
            let props = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            let hasSingle = props |> Array.exists (fun p -> DBRefTypeHelpers.isDBRefType p.PropertyType)
            let hasMany = props |> Array.exists (fun p -> DBRefTypeHelpers.isDBRefManyType p.PropertyType)
            { HasAny = hasSingle || hasMany; HasSingle = hasSingle; HasMany = hasMany }
        ))

    let private hasRelationProperties (t: Type) =
        (getRelationShape t).HasAny

    /// Context captured during query translation for post-query relation batch loading.
    [<Struct>]
    type internal BatchLoadContext = {
        OwnerTable: string
        OwnerType: Type
        ExcludedPaths: HashSet<string>
        IncludedPaths: HashSet<string>
        HasSingleRelations: bool
        HasManyRelations: bool
    }

    let private preloadQueryContextMetadata (ctx: QueryContext) (connection: SqliteConnection) =
        let canonicalManyName (a: string) (b: string) =
            if StringComparer.Ordinal.Compare(a, b) <= 0 then $"{a}_{b}" else $"{b}_{a}"

        if tableExists connection "SoloDBRelation" then
            for relation in connection.Query<{|
                Name: string
                OwnerCollection: string
                PropertyName: string
                TargetCollection: string
                RefKind: string
            |}>("SELECT Name, OwnerCollection, PropertyName, TargetCollection, RefKind FROM SoloDBRelation;") do
                let name = relation.Name
                let ownerCollection = relation.OwnerCollection
                let propertyName = relation.PropertyName
                let targetCollection = relation.TargetCollection
                let refKind = relation.RefKind
                if not (isNull name || isNull ownerCollection || isNull propertyName || isNull targetCollection || isNull refKind) then
                    let relationKind = stringToRelationKind refKind
                    let canonicalName = canonicalManyName ownerCollection targetCollection
                    let canonicalLinkTable = "SoloDBRelLink_" + canonicalName
                    let defaultLinkTable = "SoloDBRelLink_" + name
                    let useSharedMany =
                        match relationKind with
                        | RelationKind.Many -> tableExists connection canonicalLinkTable
                        | RelationKind.Single -> false
                    let ownerUsesSource =
                        if useSharedMany then
                            StringComparer.Ordinal.Compare(ownerCollection, targetCollection) <= 0
                        else
                            true
                    let linkTable = if useSharedMany then canonicalLinkTable else defaultLinkTable
                    ctx.RegisterRelation(ownerCollection, propertyName, targetCollection, linkTable, ownerUsesSource)

        if tableExists connection "SoloDBTypeCollectionMap" then
            for mapping in connection.Query<{|
                TypeKey: string
                CollectionName: string
            |}>("SELECT TypeKey, CollectionName FROM SoloDBTypeCollectionMap;") do
                let typeKey = mapping.TypeKey
                let collectionName = mapping.CollectionName
                if not (isNull typeKey || isNull collectionName) then
                    ctx.RegisterTypeCollection(typeKey, collectionName)

    /// <summary>
    /// Initiates the translation of a LINQ expression tree to an SQL query string and a dictionary of parameters.
    /// </summary>
    /// <param name="source">The source collection of the query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// <returns>A tuple containing the generated SQL string and the dictionary of parameters.</returns>
    let private startTranslationCore (metadataConnection: SqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        let builder = {
            SQLiteCommand = StringBuilder(256)
            Variables = Dictionary<string, obj>(16)
            Subqueries = ResizeArray()
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

        let ctx = QueryContext.SingleSource(source.Name)
        let hasRelations = hasRelationProperties typeof<'T>
        if hasRelations then
            let relationTx: Relations.RelationTxContext = {
                Connection = metadataConnection
                OwnerTable = source.Name
                OwnerType = typeof<'T>
                InTransaction =
                    match metadataConnection with
                    | :? TransactionalConnection -> true
                    | :? CachingDbConnection as cc -> cc.InsideTransaction
                    | _ -> false
            }
            Relations.withRelationSqliteWrap "build" "startTranslation.ensureSchemaForOwnerType" (fun () ->
                Relations.ensureSchemaForOwnerType relationTx typeof<'T>
            )

        preloadQueryContextMetadata ctx metadataConnection

        if isExplainQueryPlan then
            builder.Append "EXPLAIN QUERY PLAN "

        if doesNotReturnIdFn expression then
            builder.Append "SELECT -1 as Id, "
            builder.Append valueDecoded
            builder.Append "as ValueJSON FROM ("
            translateQuery<'T> ctx builder expression
            builder.Append ")"
        else
            builder.Append "SELECT Id, "
            builder.Append valueDecoded
            builder.Append "as ValueJSON FROM ("
            translateQuery<'T> ctx builder expression
            builder.Append ")"

        let shape = getRelationShape typeof<'T>
        let hasSingleRelations = hasRelations && shape.HasSingle
        let hasManyRelations = hasRelations && shape.HasMany

        let batchLoadContext =
            if hasSingleRelations || hasManyRelations then
                ValueSome {
                    OwnerTable = source.Name
                    OwnerType = typeof<'T>
                    ExcludedPaths = new HashSet<string>(ctx.ExcludedPaths, StringComparer.Ordinal)
                    IncludedPaths = new HashSet<string>(ctx.IncludedPaths, StringComparer.Ordinal)
                    HasSingleRelations = hasSingleRelations
                    HasManyRelations = hasManyRelations
                }
            else
                ValueNone

        builder.SQLiteCommand.ToString(), builder.Variables, batchLoadContext

    let internal startTranslation (source: ISoloDBCollection<'T>) (expression: Expression) =
        use metadataConnection = source.GetInternalConnection()
        startTranslationCore metadataConnection source expression

    let internal startTranslationWithConnection (metadataConnection: SqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        startTranslationCore metadataConnection source expression


/// <summary>
/// An internal interface defining the contract for a SoloDB query provider.
/// </summary>
type internal ISoloDBCollectionQueryProvider =
    /// <summary>Gets the source collection object.</summary>
    abstract member Source: obj
    /// <summary>Gets additional data passed from the parent database instance.</summary>
    abstract member AdditionalData: obj

module internal QueryableTranslation =
    let startFilterTranslationWithConnection (metadataConnection: SqliteConnection) (source: ISoloDBCollection<'T>) (expression: Expression) =
        let query, variables, _ = QueryHelper.startTranslationWithConnection metadataConnection source expression
        query, variables

/// <summary>
/// The internal implementation of <c>IQueryProvider</c> for SoloDB collections.
/// </summary>
/// <param name="source">The source collection.</param>
/// <param name="data">Additional data, such as cache clearing functions.</param>
type internal SoloDBCollectionQueryProvider<'T>(source: ISoloDBCollection<'T>, data: obj) =
    static let enumerableDispatchCache = System.Collections.Concurrent.ConcurrentDictionary<Type, MethodInfo>()

    interface ISoloDBCollectionQueryProvider with
        override this.Source = source
        override this.AdditionalData = data

    interface SoloDBQueryProvider
    member internal this.ExecuteEnumetable<'Elem> (query: string) (par: obj) (batchCtxObj: obj) : IEnumerable<'Elem> =
        let batchCtx = batchCtxObj :?> QueryHelper.BatchLoadContext voption
        seq {
            use connection = source.GetInternalConnection()
            match batchCtx with
            | ValueSome ctx when ctx.HasSingleRelations || ctx.HasManyRelations ->
                // Buffer all results, then batch-load relation properties.
                let buffer = ResizeArray<int64 * 'Elem>()
                for row in connection.Query<Types.DbObjectRow>(query, par) do
                    let entity = JsonFunctions.fromSQLite<'Elem> row
                    buffer.Add(row.Id.Value, entity)

                if buffer.Count > 0 then
                    // Select projections can change result element type (for example to string),
                    // so batch loading must run only for actual owner entity objects.
                    let ownerEntities =
                        buffer
                        |> Seq.choose (fun (id, e) ->
                            let boxed = box e
                            if isNull boxed then None
                            elif ctx.OwnerType.IsInstanceOfType boxed then Some(id, boxed)
                            else None)
                        |> Seq.toArray

                    if ownerEntities.Length > 0 then
                        if ctx.HasSingleRelations then
                            Relations.withRelationSqliteWrap "query-batch-load" "ExecuteEnumerable.batchLoadDBRefProperties" (fun () ->
                                Relations.batchLoadDBRefProperties connection ctx.OwnerTable ctx.OwnerType ctx.ExcludedPaths ctx.IncludedPaths ownerEntities
                            )

                        if ctx.HasManyRelations then
                            Relations.withRelationSqliteWrap "query-batch-load" "ExecuteEnumerable.batchLoadDBRefManyProperties" (fun () ->
                                Relations.batchLoadDBRefManyProperties connection ctx.OwnerTable ctx.OwnerType ctx.ExcludedPaths ctx.IncludedPaths ownerEntities source.InTransaction
                            )

                        Relations.captureRelationVersionForEntities connection ctx.OwnerTable ownerEntities

                for (_id, entity) in buffer do
                    yield entity
            | _ ->
                for row in connection.Query<Types.DbObjectRow>(query, par) do
                    yield JsonFunctions.fromSQLite<'Elem> row
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
            let query, variables, batchCtx = QueryHelper.startTranslation source expression

            #if DEBUG
            if System.Diagnostics.Debugger.IsAttached then
                printfn "%s" query
            #endif

            let inline batchLoadSingle (connection: SqliteConnection) (row: Types.DbObjectRow) (entity: 'TResult) =
                match batchCtx with
                | ValueSome ctx when (ctx.HasSingleRelations || ctx.HasManyRelations) && not (isNull (box entity)) && row.Id.HasValue && ctx.OwnerType.IsAssignableFrom(typeof<'TResult>) ->
                    if ctx.HasSingleRelations then
                        Relations.withRelationSqliteWrap "query-batch-load" "ExecuteScalar.batchLoadDBRefProperties" (fun () ->
                            Relations.batchLoadDBRefProperties connection ctx.OwnerTable ctx.OwnerType ctx.ExcludedPaths ctx.IncludedPaths [| (row.Id.Value, box entity) |]
                        )
                    if ctx.HasManyRelations then
                        Relations.withRelationSqliteWrap "query-batch-load" "ExecuteScalar.batchLoadDBRefManyProperties" (fun () ->
                            Relations.batchLoadDBRefManyProperties connection ctx.OwnerTable ctx.OwnerType ctx.ExcludedPaths ctx.IncludedPaths [| (row.Id.Value, box entity) |] source.InTransaction
                        )
                    Relations.captureRelationVersionForEntities connection ctx.OwnerTable [| (row.Id.Value, box entity) |]
                | _ -> ()
                entity

            try
                match typeof<'TResult> with
                | t when t.IsGenericType && typeof<IEnumerable<'T>>.Equals typeof<'TResult> ->
                    let result = this.ExecuteEnumetable<'T> query variables (box batchCtx)
                    result :> obj :?> 'TResult
                | t when t.IsGenericType && typedefof<IEnumerable<_>>.Equals typedefof<'TResult> ->
                    let elemType = (GenericTypeArgCache.Get t).[0]
                    let m : MethodInfo =
                        enumerableDispatchCache.GetOrAdd(elemType, Func<Type, MethodInfo>(fun et ->
                            typeof<SoloDBCollectionQueryProvider<'T>>
                                .GetMethod(nameof(this.ExecuteEnumetable), BindingFlags.NonPublic ||| BindingFlags.Instance)
                                .MakeGenericMethod(et)
                        ))
                    m.Invoke(this, [|query; variables; box batchCtx|]) :?> 'TResult
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
                        let row = query.Single()
                        let entity = JsonFunctions.fromSQLite<'TResult> row
                        batchLoadSingle connection row entity
                    | "SingleOrDefault" ->
                        // Emulate query.SingleOrDefault()
                        use enumerator = query.GetEnumerator()
                        match enumerator.MoveNext() with
                        | false -> Unchecked.defaultof<'TResult>
                        | true ->
                            let prevElement = enumerator.Current
                            match enumerator.MoveNext() with
                            | true -> raise (InvalidOperationException("Sequence contains more than one element"))
                            | false ->
                                // Only one element was found, return it.
                                let entity = JsonFunctions.fromSQLite<'TResult> prevElement
                                batchLoadSingle connection prevElement entity

                    | "First" ->
                        let row = query.First()
                        let entity = JsonFunctions.fromSQLite<'TResult> row
                        batchLoadSingle connection row entity
                    | "FirstOrDefault" ->
                        match query |> Seq.tryHead with
                        | None -> Unchecked.defaultof<'TResult>
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity

                    | methodName when methodName.EndsWith("OrDefault", StringComparison.Ordinal) ->
                        match query |> Seq.tryHead with
                        | None -> Unchecked.defaultof<'TResult>
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity
                    | _ ->
                        match query |> Seq.tryHead with
                        | None -> raise (InvalidOperationException("Sequence contains no elements"))
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity

            finally
                ()
            
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
