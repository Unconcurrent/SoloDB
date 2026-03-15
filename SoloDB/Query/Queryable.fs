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
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseTypes

/// <summary>
/// An internal discriminated union that enumerates the LINQ methods supported by the query translator.
/// </summary>
type internal SupportedLinqMethods =
| Sum
| Average
| Min
| Max
| MinBy
| MaxBy
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
| Join
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
    /// When set, used as DU expression instead of translating OrderingRule.
    RawExpr: SqlExpr option
}

type private SQLSelector =
| Expression of Expression
| KeyProjection of Expression
| DuSelector of (string -> Dictionary<string, obj> -> Projection list)

type private UsedSQLStatements = 
    {
        /// May be more than 1, they will be concatinated together with (...) AND (...) SQL structure.
        Filters: Expression ResizeArray
        Orders: PreprocessedOrder ResizeArray
        mutable Selector: SQLSelector option
        // These will account for the fact that in SQLite, the OFFSET clause cannot be used independently without the LIMIT clause.
        mutable Skip: Expression option
        mutable Take: Expression option
        UnionAll: (string -> Dictionary<string, obj> -> SelectCore) ResizeArray
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
    | ComplexDu of (struct {|Vars: Dictionary<string, obj>; Inner: SqlSelect; TableName: string|} -> SqlSelect)

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
        /// A dictionary of parameters to be passed to the SQLite command.
        Variables: Dictionary<string, obj>
        /// This list will be using to know if it is necessary to create another SQLite subquery to finish the translation, by checking if all the available slots have been used.
        Subqueries: ResizeArray<SQLSubquery>
    }


/// <summary>
/// Contains private helper functions for translating LINQ expression trees to SQL.
/// </summary>
module private QueryHelper =
    /// Allocate a parameter in the shared Variables dict and return a SqlExpr referencing it.
    let private allocateParam (variables: Dictionary<string, obj>) (value: obj) : SqlExpr =
        let value = match value with :? bool as b -> box (if b then 1 else 0) | _ -> value
        let jsonValue, shouldEncode = toSQLJson value
        let name = sprintf "dp%d" variables.Count
        variables.[name] <- jsonValue
        if shouldEncode then SqlExpr.FunctionCall("jsonb", [SqlExpr.Parameter name])
        else SqlExpr.Parameter name

    /// Translate a LINQ expression to SqlExpr DU via the QueryTranslator DU path.
    let private translateExprDu (sourceCtx: QueryContext) (tableName: string) (expr: Expression) (vars: Dictionary<string, obj>) : SqlExpr =
        QueryTranslator.translateToSqlExpr sourceCtx tableName expr vars

    /// Return the DU expression for extracting Value as JSON if the type is not a primitive SQLite type.
    let private extractValueAsJsonDu (x: Type) : SqlExpr =
        let isPrimitive = QueryTranslator.isPrimitiveSQLiteType x
        if isPrimitive then
            SqlExpr.Column(None, "Value")
        elif x = typeof<obj> || typeof<JsonValue>.IsAssignableFrom x then
            SqlExpr.CaseExpr(
                [(SqlExpr.Binary(
                    SqlExpr.FunctionCall("typeof", [SqlExpr.Column(None, "Value")]),
                    BinaryOperator.Eq,
                    SqlExpr.Literal(SqlLiteral.String "blob")),
                  SqlExpr.FunctionCall("json_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")]))],
                Some(SqlExpr.Column(None, "Value")))
        else
            SqlExpr.FunctionCall("json_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])

    /// Emit a SqlSelect to a StringBuilder via the minimal emitter.
    let private emitSelectToSb (sb: StringBuilder) (variables: Dictionary<string, obj>) (indexModel: SoloDatabase.IndexModel.IndexModel) (sel: SqlSelect) =
        let qb : QueryBuilder = {
            StringBuilder = sb
            Variables = variables
            AppendVariable = appendVariable sb variables
            RollBack = fun N -> sb.Remove(sb.Length - (int)N, (int)N) |> ignore
            UpdateMode = false
            TableNameDot = ""
            JsonExtractSelfValue = true
            Parameters = System.Collections.ObjectModel.ReadOnlyCollection(Array.empty)
            IdParameterIndex = -1
            SourceContext = QueryContext.SingleSource("")
            ParamCounter = ref 0
            DuHandlerResult = ref ValueNone
        }
        let pipelineResult =
            PassRunner.runPipeline [
                ConstantFoldPass.constantFold
                FlattenPass.subqueryFlatten
                PushdownPass.predicatePushdown
                ProjectionPass.projectionPushdown
                IndexPlanShapingPass.indexPlanShaping indexModel
                JsonbRewritePolicyPass.jsonbRewritePolicy indexModel
            ] (SelectStmt sel)
        match pipelineResult.Output with
        | SelectStmt outSel -> SqlDuMinimalEmit.emitSelect qb outSel
        | _ -> failwith "internal invariant violation: expected SelectStmt from optimizer pipeline"

    /// Helper to build a simple SelectCore with default empty fields.
    let private mkCore projections source =
        { Distinct = false; Projections = projections; Source = source
          Joins = []; Where = None; GroupBy = []; Having = None
          OrderBy = []; Limit = None; Offset = None }

    /// Wrap a SelectCore in a SqlSelect.
    let private wrapCore core = { Ctes = []; Body = SingleSelect core }

    /// Build a DerivedTable source from a SqlSelect with alias "o".
    let private derivedO sel = DerivedTable(sel, "o")

    let private collectIndexModelTableNames (sourceTableName: string) (select: SqlSelect) : string list =
        let tableNames = HashSet<string>(StringComparer.Ordinal)
        tableNames.Add(sourceTableName) |> ignore

        let addJoinSource = function
            | BaseTable(name, _) ->
                tableNames.Add(name) |> ignore
            | _ -> ()

        let addCoreJoinTables (core: SelectCore) =
            core.Joins |> List.iter (fun join -> addJoinSource join.Source)

        match select.Body with
        | SingleSelect core ->
            addCoreJoinTables core
        | UnionAllSelect(head, tail) ->
            addCoreJoinTables head
            tail |> List.iter addCoreJoinTables

        tableNames |> Seq.toList

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
        | "MinBy" -> Some MinBy
        | "MaxBy" -> Some MaxBy
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
        | "Join" -> Some Join
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

    /// Null-safe negation for All() violation subquery.
    /// Produces NOT (predicate) OR (predicate) IS NULL so that rows with
    /// missing/null boolean fields are counted as violations instead of being
    /// silently excluded by SQL three-valued NOT NULL → NULL semantics.
    let private negatePredicateForAllExpression (expr: Expression) =
        let wrapBody (body: Expression) (parameters: Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>) =
            let negated = Expression.Not(body)
            let boxed = Expression.Convert(body, typeof<obj>)
            let isNull = Expression.Equal(boxed, Expression.Constant(null, typeof<obj>))
            let combined = Expression.OrElse(negated, isNull)
            Expression.Lambda(combined, parameters)
        match expr with
        | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Quote ->
            match ue.Operand with
            | :? LambdaExpression as lambda ->
                Expression.Quote (wrapBody lambda.Body lambda.Parameters) :> Expression
            | _ ->
                Expression.Not expr :> Expression
        | :? LambdaExpression as lambda ->
            wrapBody lambda.Body lambda.Parameters :> Expression
        | _ ->
            Expression.Not expr :> Expression

    let private emptySQLStatement () =
        { Filters = ResizeArray(4); Orders = ResizeArray(1); Selector = None; Skip = None; Take = None; TableName = ""; UnionAll = ResizeArray<string -> Dictionary<string, obj> -> SelectCore>(0) }

    let inline private simpleCurrent (statements: ResizeArray<SQLSubquery>) =
        match statements.Last() with
        | Simple s -> s
        | ComplexDu _ ->
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
        | ComplexDu _ ->
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
        current.Orders.Add({ OrderingRule = ordering; Descending = descending; RawExpr = None })

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

    /// The ComplexDu subquery will be the last subquery processed. It receives the inner SqlSelect and returns a new SqlSelect.
    let addComplexFinal (statements: ResizeArray<SQLSubquery>) (buildFunc: struct {|Vars: Dictionary<string, obj>; Inner: SqlSelect; TableName: string|} -> SqlSelect) =
        statements.Add (ComplexDu buildFunc)

    /// <summary>
    /// Recursively translates a LINQ expression tree into an SQL query.
    /// This is the main dispatcher for the translation process.
    /// </summary>
    /// <param name="builder">The query builder instance that accumulates the SQL and parameters.</param>
    /// <param name="expression">The expression to translate.</param>
    let private aggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (DuSelector (fun tableName vars ->
            let innerExpr =
                match args.Length with
                | 0 -> translateExprDu sourceCtx tableName (method.ReturnType |> ExpressionHelper.id) vars
                | 1 -> translateExprDu sourceCtx tableName args.[0] vars
                | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" method.Name other))
            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall(fnName, [innerExpr]) }]
        ))

    let private zeroIfNullAggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (DuSelector (fun tableName vars ->
            let innerExpr =
                match args.Length with
                | 0 -> translateExprDu sourceCtx tableName (method.ReturnType |> ExpressionHelper.id) vars
                | 1 -> translateExprDu sourceCtx tableName args.[0] vars
                | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" method.Name other))
            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
             { Alias = Some "Value"; Expr = SqlExpr.Coalesce([SqlExpr.FunctionCall(fnName, [innerExpr]); SqlExpr.Literal(SqlLiteral.Integer 0L)]) }]
        ))

    let private isDecimalOrNullableDecimal (t: Type) =
        t = typeof<decimal> ||
        (t.IsGenericType &&
         t.GetGenericTypeDefinition() = typedefof<Nullable<_>> &&
         t.GetGenericArguments().[0] = typeof<decimal>)

    let private rejectDecimalAverageIfNeeded (method: MethodInfo) =
        if isDecimalOrNullableDecimal method.ReturnType then
            raise (NotSupportedException(
                "Decimal Average is not supported on the SQL route because SQLite AVG uses REAL arithmetic and loses decimal precision. Call AsEnumerable() before Average for exact decimal semantics."))

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

    let private readSoloDBQueryableUntyped (methodArg: Expression) =
        let unsupportedJoinMessage = "Join is supported only when the inner source is another SoloDB IQueryable. To do this anyway, use AsEnumerable() first."

        let rec isSoloDBQueryableExpression (expr: Expression) =
            match expr with
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? IQueryable as q -> q.Provider :? SoloDBQueryProvider
                | :? IRootQueryable -> true
                | _ -> false
            | :? MethodCallExpression as mce when mce.Arguments.Count > 0 ->
                isSoloDBQueryableExpression mce.Arguments.[0]
            | _ -> false

        match methodArg with
        | :? ConstantExpression as ce ->
            match ce.Value with
            | :? IQueryable as q when (match q.Provider with | :? SoloDBQueryProvider -> true | _ -> false) ->
                q.Expression
            | :? IRootQueryable as rq ->
                Expression.Constant(rq, typeof<IRootQueryable>)
            | _ ->
                raise (NotSupportedException(unsupportedJoinMessage))
        | :? MethodCallExpression as mcl when isSoloDBQueryableExpression mcl ->
            raise (NotSupportedException(
                "Error: Join inner source is not supported.
Reason: Composed inner SoloDB queries are deferred in this cycle.
Fix: Join directly against a root SoloDB collection or move the join after AsEnumerable()."))
        | _ ->
            raise (NotSupportedException(unsupportedJoinMessage))

    type private JoinSourceBinding =
    | NoJoinSource
    | OuterJoinSource
    | InnerJoinSource
    | MixedJoinSource

    let private combineJoinSourceBinding left right =
        match left, right with
        | MixedJoinSource, _
        | _, MixedJoinSource -> MixedJoinSource
        | NoJoinSource, other
        | other, NoJoinSource -> other
        | OuterJoinSource, OuterJoinSource -> OuterJoinSource
        | InnerJoinSource, InnerJoinSource -> InnerJoinSource
        | _ -> MixedJoinSource

    let private unwrapLambdaExpressionOrThrow (operationName: string) (expr: Expression) =
        match unwrapQuotedLambda expr with
        | :? LambdaExpression as lambda -> lambda
        | _ ->
            raise (NotSupportedException(
                $"Error: {operationName} is not supported.
Reason: Expected a quoted lambda expression.
Fix: Rewrite the query to use direct lambda arguments or move it after AsEnumerable()."))

    let private isCompositeJoinKeyBody (expr: Expression) =
        match expr with
        | :? NewExpression
        | :? MemberInitExpression
        | :? NewArrayExpression -> true
        | _ -> false

    let rec private classifyJoinResultExpression (outerParam: ParameterExpression) (innerParam: ParameterExpression) (expr: Expression) =
        if isNull expr then
            NoJoinSource
        else
            match expr with
            | :? ParameterExpression as p ->
                if Object.ReferenceEquals(p, outerParam) then OuterJoinSource
                elif Object.ReferenceEquals(p, innerParam) then InnerJoinSource
                else NoJoinSource
            | :? MemberExpression as m ->
                classifyJoinResultExpression outerParam innerParam m.Expression
            | :? UnaryExpression as u ->
                classifyJoinResultExpression outerParam innerParam u.Operand
            | :? BinaryExpression as b ->
                combineJoinSourceBinding
                    (classifyJoinResultExpression outerParam innerParam b.Left)
                    (classifyJoinResultExpression outerParam innerParam b.Right)
            | :? ConditionalExpression as c ->
                [ classifyJoinResultExpression outerParam innerParam c.Test
                  classifyJoinResultExpression outerParam innerParam c.IfTrue
                  classifyJoinResultExpression outerParam innerParam c.IfFalse ]
                |> List.reduce combineJoinSourceBinding
            | :? MethodCallExpression as m ->
                let objectBinding =
                    if isNull m.Object then NoJoinSource
                    else classifyJoinResultExpression outerParam innerParam m.Object
                m.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding objectBinding
            | :? NewExpression as n ->
                n.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding NoJoinSource
            | :? MemberInitExpression as mi ->
                let start = classifyJoinResultExpression outerParam innerParam mi.NewExpression
                mi.Bindings
                |> Seq.fold (fun acc binding ->
                    match binding with
                    | :? MemberAssignment as ma ->
                        combineJoinSourceBinding acc (classifyJoinResultExpression outerParam innerParam ma.Expression)
                    | _ -> MixedJoinSource) start
            | :? NewArrayExpression as na ->
                na.Expressions
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding NoJoinSource
            | :? InvocationExpression as i ->
                let exprBinding = classifyJoinResultExpression outerParam innerParam i.Expression
                i.Arguments
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding exprBinding
            | :? ListInitExpression as li ->
                let start = classifyJoinResultExpression outerParam innerParam li.NewExpression
                li.Initializers
                |> Seq.collect (fun init -> init.Arguments)
                |> Seq.map (classifyJoinResultExpression outerParam innerParam)
                |> Seq.fold combineJoinSourceBinding start
            | :? ConstantExpression -> NoJoinSource
            | _ -> MixedJoinSource

    let private translateJoinSingleSourceExpression (ctx: QueryContext) (tableAlias: string) (vars: Dictionary<string, obj>) (parameter: ParameterExpression option) (expr: Expression) =
        let lambdaExpr =
            match parameter with
            | Some p -> Expression.Lambda(expr, p) :> Expression
            | None -> Expression.Lambda(expr) :> Expression
        translateExprDu ctx tableAlias lambdaExpr vars

    let rec private translateJoinResultSelectorExpression
        (outerCtx: QueryContext)
        (innerCtx: QueryContext)
        (vars: Dictionary<string, obj>)
        (outerAlias: string)
        (innerAlias: string)
        (outerParam: ParameterExpression)
        (innerParam: ParameterExpression)
        (expr: Expression) =

        match classifyJoinResultExpression outerParam innerParam expr with
        | NoJoinSource ->
            translateJoinSingleSourceExpression outerCtx outerAlias vars None expr
        | OuterJoinSource ->
            translateJoinSingleSourceExpression outerCtx outerAlias vars (Some outerParam) expr
        | InnerJoinSource ->
            translateJoinSingleSourceExpression innerCtx innerAlias vars (Some innerParam) expr
        | MixedJoinSource ->
            match expr with
            | :? NewExpression as n ->
                let memberNames =
                    if isNull n.Members then
                        [| for i in 0 .. n.Arguments.Count - 1 -> $"Item{i + 1}" |]
                    else
                        n.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                SqlExpr.JsonObjectExpr(
                    [ for i in 0 .. n.Arguments.Count - 1 ->
                        memberNames.[i],
                        translateJoinResultSelectorExpression outerCtx innerCtx vars outerAlias innerAlias outerParam innerParam n.Arguments.[i] ])
            | :? MemberInitExpression as mi ->
                SqlExpr.JsonObjectExpr(
                    [ for binding in mi.Bindings do
                        match binding with
                        | :? MemberAssignment as ma ->
                            yield ma.Member.Name,
                                  translateJoinResultSelectorExpression outerCtx innerCtx vars outerAlias innerAlias outerParam innerParam ma.Expression
                        | _ ->
                            raise (NotSupportedException(
                                "Error: Join result selector is not supported.
Reason: Only direct member assignments are supported for mixed-source object initialization.
Fix: Use an anonymous object, tuple, or move the projection after AsEnumerable().")) ])
            | _ ->
                raise (NotSupportedException(
                    "Error: Join result selector is not supported.
Reason: Mixed-source selector expressions are limited to object and tuple construction in this cycle.
Fix: Project outer and inner members into an anonymous object or move the projection after AsEnumerable()."))

    let private tryGetJoinRootSourceTable (expression: Expression) =
        let rec loop (expr: Expression) =
            match expr with
            | :? ConstantExpression as ce ->
                match ce.Value with
                | :? IRootQueryable as rq -> Some rq.SourceTableName
                | _ -> None
            | :? MethodCallExpression as mce when mce.Arguments.Count > 0 ->
                loop mce.Arguments.[0]
            | _ -> None
        loop expression

    type private GroupJoinCarrierMembers = {
        OuterMember: MemberInfo
        GroupMember: MemberInfo
    }

    let private tryExtractGroupJoinCarrierMembers (lambda: LambdaExpression) =
        if lambda.Parameters.Count <> 2 then
            None
        else
            let outerParam = lambda.Parameters.[0]
            let groupParam = lambda.Parameters.[1]
            let mutable outerMember: MemberInfo option = None
            let mutable groupMember: MemberInfo option = None

            let recordBinding (memberInfo: MemberInfo) (expr: Expression) =
                if Object.ReferenceEquals(expr, outerParam) then
                    outerMember <- Some memberInfo
                    true
                elif Object.ReferenceEquals(expr, groupParam) then
                    groupMember <- Some memberInfo
                    true
                else
                    false

            let exactlyTwoBindings count ok =
                count = 2 && ok && outerMember.IsSome && groupMember.IsSome

            match lambda.Body with
            | :? NewExpression as n when not (isNull n.Members) ->
                let ok =
                    [ for i in 0 .. n.Arguments.Count - 1 -> recordBinding n.Members.[i] n.Arguments.[i] ]
                    |> List.forall id
                if exactlyTwoBindings n.Arguments.Count ok then
                    Some { OuterMember = outerMember.Value; GroupMember = groupMember.Value }
                else
                    None
            | :? MemberInitExpression as mi ->
                let ok =
                    mi.Bindings
                    |> Seq.map (function
                        | :? MemberAssignment as ma -> recordBinding ma.Member ma.Expression
                        | _ -> false)
                    |> Seq.forall id
                if exactlyTwoBindings mi.Bindings.Count ok then
                    Some { OuterMember = outerMember.Value; GroupMember = groupMember.Value }
                else
                    None
            | _ -> None

    let private tryMatchGroupCarrierDefaultIfEmpty (groupMember: MemberInfo) (lambda: LambdaExpression) =
        if lambda.Parameters.Count <> 1 then
            false
        else
            match lambda.Body with
            | :? MethodCallExpression as mce when mce.Method.Name = "DefaultIfEmpty" && mce.Arguments.Count = 1 ->
                match mce.Arguments.[0] with
                | :? MemberExpression as me ->
                    Object.ReferenceEquals(me.Expression, lambda.Parameters.[0]) && me.Member = groupMember
                | _ -> false
            | _ -> false

    type private LeftJoinCompositeResultRewriter
        (carrierParam: ParameterExpression, outerMember: MemberInfo, groupMember: MemberInfo, outerParam: ParameterExpression) =
        inherit ExpressionVisitor()

        let mutable touchesGroup = false
        let mutable touchesCarrier = false

        member _.TouchesGroup = touchesGroup
        member _.TouchesCarrier = touchesCarrier

        override _.VisitParameter(node: ParameterExpression) =
            if Object.ReferenceEquals(node, carrierParam) then
                touchesCarrier <- true
            base.VisitParameter(node)

        override _.VisitMember(node: MemberExpression) =
            if not (isNull node.Expression) && Object.ReferenceEquals(node.Expression, carrierParam) then
                if node.Member = outerMember then
                    outerParam :> Expression
                elif node.Member = groupMember then
                    touchesGroup <- true
                    node :> Expression
                else
                    touchesCarrier <- true
                    node :> Expression
            else
                base.VisitMember(node)

    type private CanonicalLeftJoinComposite = {
        InnerExpression: Expression
        OuterKeySelector: LambdaExpression
        InnerKeySelector: LambdaExpression
        ResultSelector: LambdaExpression
        CarrierOuterMember: MemberInfo
        CarrierGroupMember: MemberInfo
    }

    let private tryMatchCanonicalLeftJoinComposite (expressions: Expression array) =
        match expressions with
        | [| (:? MethodCallExpression as groupJoin); collectionSelectorExpr; resultSelectorExpr |] when groupJoin.Method.Name = "GroupJoin" ->
            if groupJoin.Arguments.Count <> 5 then
                None
            else
                let outerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin outer key selector" groupJoin.Arguments.[2]
                let innerKeySelector = unwrapLambdaExpressionOrThrow "GroupJoin inner key selector" groupJoin.Arguments.[3]
                let groupJoinResultSelector = unwrapLambdaExpressionOrThrow "GroupJoin result selector" groupJoin.Arguments.[4]
                let collectionSelector = unwrapLambdaExpressionOrThrow "GroupJoin SelectMany collection selector" collectionSelectorExpr
                let resultSelector = unwrapLambdaExpressionOrThrow "GroupJoin SelectMany result selector" resultSelectorExpr

                match tryExtractGroupJoinCarrierMembers groupJoinResultSelector with
                | Some carrierMembers when tryMatchGroupCarrierDefaultIfEmpty carrierMembers.GroupMember collectionSelector ->
                    Some {
                        InnerExpression = groupJoin.Arguments.[1]
                        OuterKeySelector = outerKeySelector
                        InnerKeySelector = innerKeySelector
                        ResultSelector = resultSelector
                        CarrierOuterMember = carrierMembers.OuterMember
                        CarrierGroupMember = carrierMembers.GroupMember
                    }
                | _ -> None
        | _ -> None

    let private addUnionAll (queries: SQLSubquery ResizeArray) (fn: string -> Dictionary<string, obj> -> SelectCore) =
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
        let jsonExtractValue = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
        let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, jsonExtractValue)
        addSelector queries (DuSelector (fun _tableName _vars ->
            // In this case NULL is an invalid operation, therefore to emulate the .NET behavior
            // of throwing an exception we return the Id = NULL, and Value = {exception message}
            // And downstream the pipeline it will be checked and throwed.
            [{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr([(isNullCheck, SqlExpr.Literal(SqlLiteral.Null))], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
             { Alias = Some "Value"; Expr = SqlExpr.CaseExpr([(isNullCheck, SqlExpr.Literal(SqlLiteral.String errorMsg))], Some(SqlExpr.Column(None, "Value"))) }]
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
                | Some SelectMany when mce.Arguments.Count = 3 ->
                    match mce.Arguments.[0] with
                    | :? MethodCallExpression as groupJoin when groupJoin.Method.Name = "GroupJoin" ->
                        expression <- groupJoin.Arguments.[0]
                        let exprs = [| groupJoin :> Expression; mce.Arguments.[1]; mce.Arguments.[2] |]
                        Method {| Value = SelectMany; Expressions = exprs; OriginalMethod = mce.Method |}
                    | _ ->
                        expression <- mce.Arguments.[0]
                        let exprs = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                        Method {| Value = SelectMany; Expressions = exprs; OriginalMethod = mce.Method |}
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

    let private isExpressionLikeArgument (expr: Expression) =
        match expr with
        | :? LambdaExpression -> true
        | _ -> typeof<Expression>.IsAssignableFrom expr.Type

    let private tryGetTerminalDefaultValueExpression (expression: Expression) =
        match expression with
        | :? MethodCallExpression as mce ->
            let args = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
            match mce.Method.Name, args with
            | ("FirstOrDefault" | "SingleOrDefault"), [| defaultValue |]
                when not (isExpressionLikeArgument defaultValue) ->
                Some defaultValue
            | ("FirstOrDefault" | "SingleOrDefault"), [| predicate; defaultValue |]
                when isExpressionLikeArgument predicate ->
                Some defaultValue
            | _ ->
                None
        | _ ->
            None

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

    /// Build WHERE, ORDER BY, LIMIT, OFFSET, UnionAll from a UsedSQLStatements layer as DU values.
    /// Returns (where, orderBy, limit, offset, unionAlls).
    let private buildClausesDu
        (sourceCtx: QueryContext)
        (vars: Dictionary<string, obj>)
        (statement: UsedSQLStatements)
        (contextTable: string) =

        let where =
            if statement.Filters.Count = 0 then None
            else
                let exprs =
                    statement.Filters
                    |> Seq.map (fun f -> translateExprDu sourceCtx contextTable f vars)
                    |> Seq.toList
                match exprs with
                | [single] -> Some single
                | multiple -> Some (multiple |> List.reduce (fun a b -> SqlExpr.Binary(a, BinaryOperator.And, b)))

        let unionAlls =
            statement.UnionAll |> Seq.map (fun buildFn -> buildFn contextTable vars) |> Seq.toList

        let orderBy =
            statement.Orders |> Seq.map (fun o ->
                let expr =
                    match o.RawExpr with
                    | Some duExpr -> duExpr
                    | None -> translateExprDu sourceCtx contextTable o.OrderingRule vars
                { Expr = expr; Direction = if o.Descending then SortDirection.Desc else SortDirection.Asc }
            ) |> Seq.toList

        let limit =
            match statement.Take with
            | Some take -> Some (allocateParam vars (QueryTranslator.evaluateExpr<obj> take))
            | None ->
                match statement.Skip with
                | Some _ -> Some (SqlExpr.Literal(SqlLiteral.Integer -1L))
                | None -> None

        let offset =
            match statement.Skip with
            | Some skip -> Some (allocateParam vars (QueryTranslator.evaluateExpr<obj> skip))
            | None -> None

        struct (where, orderBy, limit, offset, unionAlls)

    /// Parse a JoinKind string from the handler mechanism into a DU JoinKind.
    let private parseJoinKind (kind: string) =
        match kind.Trim().ToUpperInvariant() with
        | s when s.Contains("LEFT") -> JoinKind.Left
        | s when s.Contains("INNER") -> JoinKind.Inner
        | s when s.Contains("CROSS") -> JoinKind.Cross
        | _ -> JoinKind.Left

    /// Build materialized Value projection (jsonb_set with JOIN data) for relation materialization.
    let private buildMaterializedValueExpr (ctx: QueryContext) (effectiveTableName: string) (valueColumnExpr: SqlExpr) =
        let materializedJoins =
            ctx.Joins
            |> Seq.filter (fun j -> shouldLoadRelationPath ctx j.PropertyPath)
            |> Seq.toList
        if materializedJoins.IsEmpty then
            // All joins are excluded — no materialization needed, keep original qualified Value
            valueColumnExpr
        else
            // jsonb_set("Table".Value, '$.Prop1', CASE WHEN a1.Id IS NOT NULL THEN jsonb_array(a1.Id, a1.Value) ELSE jsonb_extract("Table".Value, '$.Prop1') END, ...)
            let args = ResizeArray<SqlExpr>()
            args.Add(SqlExpr.Column(Some effectiveTableName, "Value"))
            for j in materializedJoins do
                ctx.MaterializedPaths.Add(j.PropertyPath) |> ignore
                args.Add(SqlExpr.Literal(SqlLiteral.String ("$." + j.PropertyPath)))
                args.Add(SqlExpr.CaseExpr(
                    [(SqlExpr.Unary(UnaryOperator.IsNotNull, SqlExpr.Column(Some j.TargetAlias, "Id")),
                      SqlExpr.FunctionCall("jsonb_array", [SqlExpr.Column(Some j.TargetAlias, "Id"); SqlExpr.Column(Some j.TargetAlias, "Value")]))],
                    Some (SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(Some effectiveTableName, "Value"); SqlExpr.Literal(SqlLiteral.String ("$." + j.PropertyPath))]))))
            SqlExpr.FunctionCall("jsonb_set", args |> Seq.toList)

    /// Wrap a SelectBody with optional ORDER BY, LIMIT, OFFSET.
    /// For SingleSelect, these are already on the core (identity pass-through).
    /// For UnionAllSelect, wrapping is needed if ordering/limit is present.
    /// Strip source aliases from SqlExpr so column references become unqualified.
    /// Required when ORDER BY expressions translated against a base table (e.g. "Int32")
    /// are lifted to an outer derived-table wrapper where those aliases don't exist.
    let rec private stripSourceAlias (expr: SqlExpr) : SqlExpr =
        match expr with
        | SqlExpr.Column(Some _, col) -> SqlExpr.Column(None, col)
        | SqlExpr.Column(None, _) -> expr
        | SqlExpr.Literal _ -> expr
        | SqlExpr.Parameter _ -> expr
        | SqlExpr.JsonExtractExpr(Some _, col, path) -> SqlExpr.JsonExtractExpr(None, col, path)
        | SqlExpr.JsonExtractExpr(None, _, _) -> expr
        | SqlExpr.JsonSetExpr(target, assignments) ->
            SqlExpr.JsonSetExpr(
                stripSourceAlias target,
                assignments |> List.map (fun (path, value) -> path, stripSourceAlias value))
        | SqlExpr.JsonArrayExpr elements ->
            SqlExpr.JsonArrayExpr(elements |> List.map stripSourceAlias)
        | SqlExpr.JsonObjectExpr properties ->
            SqlExpr.JsonObjectExpr(properties |> List.map (fun (name, value) -> name, stripSourceAlias value))
        | SqlExpr.FunctionCall(name, args) ->
            SqlExpr.FunctionCall(name, args |> List.map stripSourceAlias)
        | SqlExpr.AggregateCall(kind, argument, distinct, separator) ->
            SqlExpr.AggregateCall(kind, argument |> Option.map stripSourceAlias, distinct, separator |> Option.map stripSourceAlias)
        | SqlExpr.WindowCall spec ->
            SqlExpr.WindowCall {
                spec with
                    Arguments = spec.Arguments |> List.map stripSourceAlias
                    PartitionBy = spec.PartitionBy |> List.map stripSourceAlias
                    OrderBy = spec.OrderBy |> List.map (fun (e, d) -> stripSourceAlias e, d)
            }
        | SqlExpr.Unary(op, inner) -> SqlExpr.Unary(op, stripSourceAlias inner)
        | SqlExpr.Binary(l, op, r) -> SqlExpr.Binary(stripSourceAlias l, op, stripSourceAlias r)
        | SqlExpr.Between(e, lower, upper) -> SqlExpr.Between(stripSourceAlias e, stripSourceAlias lower, stripSourceAlias upper)
        | SqlExpr.InList(e, values) -> SqlExpr.InList(stripSourceAlias e, values |> List.map stripSourceAlias)
        | SqlExpr.InSubquery(e, subquery) -> SqlExpr.InSubquery(stripSourceAlias e, stripSourceAliasInSelect subquery)
        | SqlExpr.Cast(inner, ty) -> SqlExpr.Cast(stripSourceAlias inner, ty)
        | SqlExpr.Coalesce exprs -> SqlExpr.Coalesce(exprs |> List.map stripSourceAlias)
        | SqlExpr.Exists subquery -> SqlExpr.Exists(stripSourceAliasInSelect subquery)
        | SqlExpr.ScalarSubquery subquery -> SqlExpr.ScalarSubquery(stripSourceAliasInSelect subquery)
        | SqlExpr.CaseExpr(branches, elseExpr) ->
            SqlExpr.CaseExpr(
                branches |> List.map (fun (w, t) -> stripSourceAlias w, stripSourceAlias t),
                elseExpr |> Option.map stripSourceAlias)
        | SqlExpr.UpdateFragment(path, value) -> SqlExpr.UpdateFragment(stripSourceAlias path, stripSourceAlias value)

    and private stripSourceAliasInTableSource (source: TableSource) : TableSource =
        match source with
        | BaseTable _ -> source
        | DerivedTable(query, alias) -> DerivedTable(stripSourceAliasInSelect query, alias)
        | FromJsonEach(valueExpr, alias) -> FromJsonEach(stripSourceAlias valueExpr, alias)

    and private stripSourceAliasInJoin (join: JoinShape) : JoinShape =
        {
            join with
                Source = stripSourceAliasInTableSource join.Source
                On = join.On |> Option.map stripSourceAlias
        }

    and private stripSourceAliasInProjection (projection: Projection) : Projection =
        { projection with Expr = stripSourceAlias projection.Expr }

    and private stripSourceAliasInOrderBy (orderBy: OrderBy) : OrderBy =
        { orderBy with Expr = stripSourceAlias orderBy.Expr }

    and private stripSourceAliasInCore (core: SelectCore) : SelectCore =
        {
            core with
                Source = core.Source |> Option.map stripSourceAliasInTableSource
                Joins = core.Joins |> List.map stripSourceAliasInJoin
                Projections = core.Projections |> List.map stripSourceAliasInProjection
                Where = core.Where |> Option.map stripSourceAlias
                GroupBy = core.GroupBy |> List.map stripSourceAlias
                Having = core.Having |> Option.map stripSourceAlias
                OrderBy = core.OrderBy |> List.map stripSourceAliasInOrderBy
                Limit = core.Limit |> Option.map stripSourceAlias
                Offset = core.Offset |> Option.map stripSourceAlias
        }

    and private stripSourceAliasInSelect (select: SqlSelect) : SqlSelect =
        {
            select with
                Ctes = select.Ctes |> List.map (fun cte -> { cte with Query = stripSourceAliasInSelect cte.Query })
                Body =
                    match select.Body with
                    | SingleSelect core -> SingleSelect(stripSourceAliasInCore core)
                    | UnionAllSelect(head, tail) -> UnionAllSelect(stripSourceAliasInCore head, tail |> List.map stripSourceAliasInCore)
        }

    let private wrapCoreBody (body: SelectBody) (orderBy: OrderBy list) (limit: SqlExpr option) (offset: SqlExpr option) : SqlSelect =
        match body with
        | SingleSelect core ->
            { Ctes = []; Body = SingleSelect core }
        | UnionAllSelect(head, tail) ->
            if orderBy.IsEmpty && limit.IsNone && offset.IsNone then
                { Ctes = []; Body = UnionAllSelect(head, tail) }
            else
                // Wrap the UNION ALL in a derived table to apply ORDER BY / LIMIT.
                // Strip source aliases from ORDER BY: expressions were translated against
                // the base table (e.g. "Int32") but the outer wrapper's columns are unqualified.
                let strippedOrderBy = orderBy |> List.map (fun o -> { o with Expr = stripSourceAlias o.Expr })
                let innerSelect = { Ctes = []; Body = UnionAllSelect(head, tail) }
                let outerCore =
                    { mkCore
                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                         { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                        (Some (DerivedTable(innerSelect, "o")))
                      with OrderBy = strippedOrderBy; Limit = limit; Offset = offset }
                { Ctes = []; Body = SingleSelect outerCore }

    /// Build SqlSelect from collected layers (DU construction path — no StringBuilder).
    let private buildLayersDu<'T>
        (sourceCtx: QueryContext)
        (vars: Dictionary<string, obj>)
        (layers: SQLSubquery ResizeArray)
        : SqlSelect =

        let layerCount = layers.Count

        let tableName =
            if layerCount > 0 then
                match layers.[0] with
                | Simple layer -> layer.TableName
                | ComplexDu _ -> ""
            else
                ""

        let rec buildLayer (i: int) : SqlSelect =
            let layer = layers.[i]
            match layer with
            // Edge case 1: Empty query (root table only)
            | Simple layer when layer.IsEmptyWithTableName ->
                let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                if isTypePrimitive then
                    // Edge case 2: Primitive type extraction — jsonb_extract(Value, '$')
                    wrapCore (mkCore
                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                         { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")]) }]
                        (Some (BaseTable(layer.TableName, None))))
                else
                    // Non-primitive: bare table reference (Id and Value are columns)
                    wrapCore (mkCore
                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                         { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                        (Some (BaseTable(layer.TableName, None))))

            | Simple layer ->
                let isLocalKeyProjection =
                    match layer.Selector with
                    | Some (KeyProjection _) -> true
                    | _ -> false
                let currentCtx =
                    if isLocalKeyProjection then cloneQueryContext sourceCtx
                    else sourceCtx
                let effectiveTableName = layer.TableName
                let hasTableName = not (String.IsNullOrEmpty effectiveTableName)
                let quotedTableName = "\"" + effectiveTableName + "\""
                let idColumnExpr = if hasTableName then SqlExpr.Column(Some quotedTableName, "Id") else SqlExpr.Column(None, "Id")
                let valueColumnExpr = if hasTableName then SqlExpr.Column(Some quotedTableName, "Value") else SqlExpr.Column(None, "Value")

                // Track whether the Value projection can be rewritten for JOIN materialization.
                let mutable needsValueMaterialization = false

                // Build projections based on selector.
                let projections =
                    match layer.Selector with
                    | Some (Expression selector) ->
                        let selectorExpr = translateExprDu sourceCtx layer.TableName selector vars
                        [{ Alias = None; Expr = idColumnExpr }
                         { Alias = Some "Value"; Expr = selectorExpr }]
                    | Some (KeyProjection selector) ->
                        let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                        let keyExpr = translateExprDu currentCtx effectiveTableName selector vars
                        if isTypePrimitive then
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [valueColumnExpr; SqlExpr.Literal(SqlLiteral.String "$")]) }
                             { Alias = Some "__solodb_group_key"; Expr = keyExpr }]
                        else
                            needsValueMaterialization <- true
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = valueColumnExpr }
                             { Alias = Some "__solodb_group_key"; Expr = keyExpr }]
                    | Some (DuSelector buildProjections) ->
                        buildProjections layer.TableName vars
                    | None ->
                        let isTypePrimitive = QueryTranslator.isPrimitiveSQLiteType typeof<'T>
                        if isTypePrimitive then
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [valueColumnExpr; SqlExpr.Literal(SqlLiteral.String "$")]) }]
                        else
                            needsValueMaterialization <- true
                            [{ Alias = None; Expr = idColumnExpr }
                             { Alias = None; Expr = valueColumnExpr }]

                // Build source.
                let isBaseTable = (i = 0)
                let source =
                    if isBaseTable then
                        Some (BaseTable(layer.TableName, None))
                    else
                        let innerSel = buildLayer (i - 1)
                        Some (DerivedTable(innerSel, "o"))

                // Build clauses (WHERE, ORDER BY, LIMIT, OFFSET, UNION ALL).
                // Side effect: expression translation discovers JOINs via QueryContext.
                let clauseCtx = if isLocalKeyProjection then currentCtx else sourceCtx
                let clauseTable = if isLocalKeyProjection then effectiveTableName else layer.TableName
                let struct (where, orderBy, limit, offset, unionAlls) =
                    buildClausesDu clauseCtx vars layer clauseTable

                // Edge case 8: JOIN materialization (DBRef) — discovered during clause translation.
                let mutable finalProjections = projections
                let mutable joins = []

                if isBaseTable && currentCtx.Joins.Count > 0 then
                    // Build JoinShape list from discovered JoinEdges.
                    joins <-
                        currentCtx.Joins
                        |> Seq.map (fun j ->
                            { Kind = parseJoinKind j.JoinKind
                              Source = BaseTable(j.TargetTable, Some j.TargetAlias)
                              On = Some(SqlExpr.Binary(
                                SqlExpr.Column(Some j.TargetAlias, "Id"),
                                BinaryOperator.Eq,
                                SqlExpr.JsonExtractExpr(j.OnSourceAlias, "Value", JsonPath [j.OnPropertyName]))) })
                        |> Seq.toList

                    // Rewrite Value projection for materialization (jsonb_set).
                    if needsValueMaterialization then
                        let materializedValueExpr = buildMaterializedValueExpr currentCtx quotedTableName valueColumnExpr
                        finalProjections <-
                            finalProjections |> List.map (fun p ->
                                // Replace bare Value column with materialized expression.
                                match p.Alias, p.Expr with
                                | None, SqlExpr.Column(_, "Value") ->
                                    { Alias = Some "Value"; Expr = materializedValueExpr }
                                | Some "Value", _ ->
                                    { Alias = Some "Value"; Expr = materializedValueExpr }
                                | _ -> p)

                // Assemble the SelectCore.
                let body =
                    match unionAlls with
                    | [] ->
                        let core =
                            { mkCore finalProjections source with
                                Joins = joins
                                Where = where
                                OrderBy = orderBy
                                Limit = limit
                                Offset = offset }
                        SingleSelect core
                    | _ ->
                        // Edge case 4: UNION ALL chains
                        let headCore =
                            { mkCore finalProjections source with
                                Joins = joins
                                Where = where }
                        UnionAllSelect(headCore, unionAlls)

                wrapCoreBody body orderBy limit offset

            | ComplexDu buildFunc ->
                let tn =
                    if i = 0 then tableName
                    else ""
                let innerSel =
                    if i > 0 then buildLayer (i - 1)
                    else
                        // No inner layers — provide an empty select (shouldn't happen in practice)
                        wrapCore (mkCore [] None)
                buildFunc {| Vars = vars; Inner = innerSel; TableName = tn |}

        buildLayer (layerCount - 1)

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
                let inline installTerminalOrdering (ordering: Expression) (descending: bool) (rawExpr: SqlExpr option) =
                    let current = ifSelectorNewStatement statements
                    let existingOrders = current.Orders |> Seq.toList
                    current.Orders.Clear()
                    current.Orders.Add({ OrderingRule = ordering; Descending = descending; RawExpr = rawExpr })
                    if List.isEmpty existingOrders then
                        current.Orders.Add({ OrderingRule = ExpressionHelper.get(fun (x: obj) -> x.Dyn<int64>("Id")); Descending = false; RawExpr = None })
                    else
                        for order in existingOrders do
                            current.Orders.Add(order)

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
                        // Edge case 13: PostScalarProjection path — DistinctBy → Select reuse consuming __solodb_scalar_slot0
                        addSelector statements (DuSelector (fun tableName _vars ->
                            let hasTableName = not (String.IsNullOrEmpty tableName)
                            let idExpr = if hasTableName then SqlExpr.Column(Some tableName, "Id") else SqlExpr.Column(None, "Id")
                            [{ Alias = Some "Id"; Expr = idExpr }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(None, "__solodb_scalar_slot0") }]
                        ))
                        pendingDistinctByScalarReuse <- None
                        isPostScalarProjection <- true
                    | Some lowered when lowered.RelationAccess = HasRelationAccess ->
                        // C12 path: post-DistinctBy Select on a DIFFERENT relation scalar than
                        // the DistinctBy key. The base layer will materialize the relation into
                        // Value via jsonb_set, so we extract from the materialized JSON payload.
                        // Edge case 20: Relation-backed DistinctBy scalar slot reuse
                        match tryExtractRelationJsonPath m.Expressions.[0] with
                        | Some jsonPath ->
                            let capturedPath = jsonPath
                            addSelector statements (DuSelector (fun _tableName _vars ->
                                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                                 { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String capturedPath)]) }]
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
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawExpr = Some (SqlExpr.Column(None, "__solodb_scalar_slot0")) })
                    | _ ->
                        addOrder statements m.Expressions.[0] descending
                | ThenBy | ThenByDescending ->
                    let descending = (m.Value = ThenByDescending)
                    let orderFingerprint = expressionFingerprint m.Expressions.[0]
                    let current = simpleCurrent()
                    match pendingDistinctByScalarReuse with
                    | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = orderFingerprint ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawExpr = Some (SqlExpr.Column(None, "__solodb_scalar_slot0")) })
                    | _ ->
                        current.Orders.Add({ OrderingRule = m.Expressions.[0]; Descending = descending; RawExpr = None })
                | Skip ->
                    let current = simpleCurrent()
                    match current.Skip with
                    | Some _ -> statements.Add(Simple { emptySQLStatement () with Skip = Some m.Expressions.[0] })
                    | None   -> current.Skip <- Some m.Expressions.[0]
                | Take ->
                    addTake statements m.Expressions.[0]

                | Sum ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                             { Alias = Some "Value"; Expr = SqlExpr.Coalesce([SqlExpr.FunctionCall("SUM", [extractVal]); SqlExpr.Literal(SqlLiteral.Integer 0L)]) }]
                        ))
                    else
                        // SUM() return NULL if all elements are NULL, TOTAL() return 0.0.
                        // TOTAL() always returns a float, therefore we will just check for NULL
                        zeroIfNullAggregateTranslator sourceCtx "SUM" statements m.OriginalMethod m.Expressions

                | Average ->
                    rejectDecimalAverageIfNeeded m.OriginalMethod
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, extractVal)
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("AVG", [extractVal]) }]
                        ))
                        // Edge case 14: Aggregate NULL handling — error message for empty sequences
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr([(isNullCheck, SqlExpr.Literal(SqlLiteral.Null))], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
                             { Alias = Some "Value"; Expr = SqlExpr.CaseExpr([(isNullCheck, SqlExpr.Literal(SqlLiteral.String "Sequence contains no elements"))], Some(SqlExpr.Column(None, "Value"))) }]
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "AVG" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | Min ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, extractVal)
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("MIN", [extractVal]) }]
                        ))
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr([(isNullCheck, SqlExpr.Literal(SqlLiteral.Null))], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
                             { Alias = Some "Value"; Expr = SqlExpr.CaseExpr([(isNullCheck, SqlExpr.Literal(SqlLiteral.String "Sequence contains no elements"))], Some(SqlExpr.Column(None, "Value"))) }]
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "MIN" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | Max ->
                    if isPostScalarProjection && m.Expressions.Length = 0 then
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        let isNullCheck = SqlExpr.Unary(UnaryOperator.IsNull, extractVal)
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall("MAX", [extractVal]) }]
                        ))
                        addSelector statements (DuSelector (fun _tableName _vars ->
                            [{ Alias = Some "Id"; Expr = SqlExpr.CaseExpr([(isNullCheck, SqlExpr.Literal(SqlLiteral.Null))], Some(SqlExpr.Literal(SqlLiteral.Integer -1L))) }
                             { Alias = Some "Value"; Expr = SqlExpr.CaseExpr([(isNullCheck, SqlExpr.Literal(SqlLiteral.String "Sequence contains no elements"))], Some(SqlExpr.Column(None, "Value"))) }]
                        ))
                    else
                        raiseIfNullAggregateTranslator sourceCtx "MAX" statements m.OriginalMethod m.Expressions "Sequence contains no elements"

                | MinBy | MaxBy ->
                    match m.Expressions.Length with
                    | 1 ->
                        let descending = (m.Value = MaxBy)
                        let orderFingerprint = expressionFingerprint m.Expressions.[0]
                        let rawExpr =
                            match pendingDistinctByScalarReuse with
                            | Some lowered when lowered.RelationAccess = HasRelationAccess && lowered.Fingerprint = orderFingerprint ->
                                Some (SqlExpr.Column(None, "__solodb_scalar_slot0"))
                            | _ ->
                                None
                        installTerminalOrdering m.Expressions.[0] descending rawExpr
                        addTake statements (ExpressionHelper.constant 1)
                    | 2 ->
                        raise (NotSupportedException("MinBy/MaxBy comparer overloads are not supported."))
                    | other ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                
                | Distinct ->
                    addComplexFinal statements (fun ctx ->
                        // SELECT -1 AS Id, Value FROM (inner) o GROUP BY {identity expr}
                        let groupByExpr = translateExprDu sourceCtx ctx.TableName (GenericMethodArgCache.Get m.OriginalMethod |> Array.head |> ExpressionHelper.id) ctx.Vars
                        let core =
                            { mkCore
                                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                                 { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                                (Some (DerivedTable(ctx.Inner, "o")))
                              with GroupBy = [groupByExpr] }
                        wrapCore core
                    )

                | DistinctBy ->
                    // Edge case 7: DistinctBy with ROW_NUMBER OVER — window function + derived table
                    let lowered = lowerKeySelectorLambda sourceCtx tableName m.Expressions.[0] DistinctByKey
                    addLoweredKeySelector statements lowered
                    pendingDistinctByScalarReuse <-
                        match lowered.RelationAccess with
                        | HasRelationAccess -> Some lowered
                        | NoRelationAccess -> None
                    addComplexFinal statements (fun ctx ->
                        // Inner: SELECT o.Id, o.Value, [o.__solodb_group_key AS __solodb_scalar_slot0,] ROW_NUMBER() OVER (PARTITION BY o.__solodb_group_key ORDER BY o.Id) AS __solodb_rn FROM (inner) o
                        let innerProjs =
                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }]
                            @ (if lowered.RelationAccess = HasRelationAccess then
                                   [{ Alias = Some "__solodb_scalar_slot0"; Expr = SqlExpr.Column(Some "o", "__solodb_group_key") }]
                               else [])
                            @ [{ Alias = Some "__solodb_rn"; Expr = SqlExpr.WindowCall({
                                    Kind = WindowFunctionKind.RowNumber
                                    Arguments = []
                                    PartitionBy = [SqlExpr.Column(Some "o", "__solodb_group_key")]
                                    OrderBy = [(SqlExpr.Column(Some "o", "Id"), SortDirection.Asc)]
                                }) }]
                        let innerCore = mkCore innerProjs (Some (DerivedTable(ctx.Inner, "o")))
                        let innerSel = wrapCore innerCore

                        // Outer: SELECT o.Id, o.Value [, o.__solodb_scalar_slot0] FROM (inner) o WHERE o.__solodb_rn = 1
                        let outerProjs =
                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }]
                            @ (if lowered.RelationAccess = HasRelationAccess then
                                   [{ Alias = Some "__solodb_scalar_slot0"; Expr = SqlExpr.Column(Some "o", "__solodb_scalar_slot0") }]
                               else [])
                        let outerCore =
                            { mkCore outerProjs (Some (DerivedTable(innerSel, "o")))
                              with Where = Some (SqlExpr.Binary(SqlExpr.Column(Some "o", "__solodb_rn"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L))) }
                        wrapCore outerCore
                    )

                | GroupBy ->
                    // Edge case 6: GroupBy with json_group_array — custom projection
                    addLoweredKeySelector statements (lowerKeySelectorLambda sourceCtx tableName m.Expressions.[0] GroupByKey)
                    addComplexFinal statements (fun ctx ->
                        // SELECT -1 as Id, json_object('Key', o.__solodb_group_key, 'Items', json_group_array(jsonb_set(o.Value, '$.Id', o.Id))) as Value FROM (inner) o GROUP BY o.__solodb_group_key
                        let core =
                            { mkCore
                                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                                 { Alias = Some "Value"; Expr =
                                    SqlExpr.FunctionCall("jsonb_object", [
                                        SqlExpr.Literal(SqlLiteral.String "Key")
                                        SqlExpr.Column(Some "o", "__solodb_group_key")
                                        SqlExpr.Literal(SqlLiteral.String "Items")
                                        SqlExpr.FunctionCall("jsonb_group_array", [
                                            SqlExpr.FunctionCall("jsonb_set", [
                                                SqlExpr.Column(Some "o", "Value")
                                                SqlExpr.Literal(SqlLiteral.String "$.Id")
                                                SqlExpr.Column(Some "o", "Id")
                                            ])
                                        ])
                                    ]) }]
                                (Some (DerivedTable(ctx.Inner, "o")))
                              with GroupBy = [SqlExpr.Column(Some "o", "__solodb_group_key")] }
                        wrapCore core
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

                    addComplexFinal statements (fun ctx ->
                        // SELECT COUNT(Id) as Value FROM (inner) o
                        let projs = [{ Alias = Some "Value"; Expr = SqlExpr.FunctionCall("COUNT", [SqlExpr.Column(None, "Id")]) }]
                        let core = mkCore projs (Some (DerivedTable(ctx.Inner, "o")))
                        wrapCore core
                    )


                | SelectMany ->
                    match tryMatchCanonicalLeftJoinComposite m.Expressions with
                    | Some composite ->
                        if composite.ResultSelector.Parameters.Count <> 2 then
                            raise (NotSupportedException(
                                "Error: GroupJoin SelectMany result selector is not supported.
Reason: The canonical left-join composite requires a two-parameter result selector.
Fix: Use SelectMany with the standard (carrier, inner) result selector or move the query after AsEnumerable()."))

                        if isCompositeJoinKeyBody composite.OuterKeySelector.Body || isCompositeJoinKeyBody composite.InnerKeySelector.Body then
                            raise (NotSupportedException(
                                "Error: Left-join composite key selectors are not supported.
Reason: Anonymous-type and composite key equality lowering is deferred in this cycle.
Fix: Join on a single scalar key or move the query after AsEnumerable()."))

                        let innerExpression = readSoloDBQueryableUntyped composite.InnerExpression
                        let innerRootTable =
                            match tryGetJoinRootSourceTable innerExpression with
                            | Some tableName -> tableName
                            | None ->
                                raise (NotSupportedException(
                                    "Error: Left-join inner source is not supported.
Reason: The inner query does not resolve to a SoloDB root collection.
Fix: Use another SoloDB IQueryable rooted in a collection or move the query after AsEnumerable()."))

                        let outerResultParam = composite.ResultSelector.Parameters.[0]
                        let innerResultParam = composite.ResultSelector.Parameters.[1]
                        let rewriter = LeftJoinCompositeResultRewriter(outerResultParam, composite.CarrierOuterMember, composite.CarrierGroupMember, composite.OuterKeySelector.Parameters.[0])
                        let rewrittenResultBody = rewriter.Visit(composite.ResultSelector.Body)

                        if rewriter.TouchesGroup || rewriter.TouchesCarrier then
                            raise (NotSupportedException(
                                "Error: Left-join result selector is not supported.
Reason: Only projections over the outer row and the matched inner row are supported; grouped-sequence and carrier-shaped projections are deferred in this cycle.
Fix: Project scalar members from the outer row and the DefaultIfEmpty inner row, or move the query after AsEnumerable()."))

                        addComplexFinal statements (fun ctx ->
                            let outerAlias = "o"
                            let innerAlias = "j"
                            let innerCtx = QueryContext.SingleSource(innerRootTable)
                            let outerKeyExpr =
                                translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some composite.OuterKeySelector.Parameters.[0]) composite.OuterKeySelector.Body
                            let innerKeyExpr =
                                translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some composite.InnerKeySelector.Parameters.[0]) composite.InnerKeySelector.Body
                            let resultExpr =
                                translateJoinResultSelectorExpression
                                    sourceCtx
                                    innerCtx
                                    ctx.Vars
                                    outerAlias
                                    innerAlias
                                    composite.OuterKeySelector.Parameters.[0]
                                    innerResultParam
                                    rewrittenResultBody

                            let core =
                                { mkCore
                                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                                     { Alias = Some "Value"; Expr = resultExpr }]
                                    (Some (DerivedTable(ctx.Inner, outerAlias)))
                                  with
                                      Joins =
                                          [{ Kind = JoinKind.Left
                                             Source = BaseTable(innerRootTable, Some innerAlias)
                                             On = Some (SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerKeyExpr)) }] }
                            wrapCore core
                        )
                    | None ->
                        // Edge case 15: SelectMany with json_each — JOIN json_each on inner source
                        addComplexFinal statements (fun ctx ->
                            match m.Expressions.Length with
                            | 1 ->
                                let generics = GenericMethodArgCache.Get m.OriginalMethod
                                if generics.[1] (*output*) = typeof<byte> then
                                    raise (InvalidOperationException "Cannot use SelectMany() on byte arrays, as they are stored as base64 strings in SQLite. To process the array anyway, first exit the SQLite context with .AsEnumerable().")
                                // Use a stable inner source alias based on the inner select structure
                                let innerSourceName = Utils.getVarName (hash ctx.Inner.Body % 10000 |> abs)
                                // Build the json_each join source expression
                                let jsonEachExpr =
                                    match m.Expressions.[0] with
                                    | :? UnaryExpression as ue when (ue.Operand :? LambdaExpression) ->
                                        let lambda = ue.Operand :?> LambdaExpression
                                        match lambda.Body with
                                        | :? MemberExpression as me ->
                                            SqlExpr.FunctionCall("jsonb_extract", [
                                                SqlExpr.Column(Some innerSourceName, "Value")
                                                SqlExpr.Literal(SqlLiteral.String ("$." + me.Member.Name))
                                            ])
                                        | :? ParameterExpression ->
                                            SqlExpr.Column(Some innerSourceName, "Value")
                                        | _ ->
                                            raise (NotSupportedException(
                                                "Error: Unsupported SelectMany selector structure.
Reason: The selector cannot be translated to SQL.
Fix: Simplify the selector or move SelectMany after AsEnumerable()."))
                                    | _ ->
                                        raise (NotSupportedException(
                                            "Error: Invalid SelectMany structure.
Reason: The SelectMany arguments are not a supported query pattern.
Fix: Rewrite the query or move SelectMany after AsEnumerable()."))
                                // SELECT innerSource.Id AS Id, json_each.Value as Value FROM (inner) AS innerSource JOIN json_each(expr)
                                let core =
                                    { mkCore
                                        [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some innerSourceName, "Id") }
                                         { Alias = Some "Value"; Expr = SqlExpr.Column(Some "json_each", "Value") }]
                                        (Some (DerivedTable(ctx.Inner, innerSourceName)))
                                      with Joins = [{ Kind = JoinKind.Inner; Source = FromJsonEach(jsonEachExpr, None); On = None }] }
                                wrapCore core
                            | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                        )

                | Join ->
                    match m.Expressions.Length with
                    | 5 ->
                        raise (NotSupportedException(
                            "Error: Join comparer overload is not supported.
Reason: SoloDB only supports SQL-translatable equality without custom comparers in this cycle.
Fix: Remove the comparer or move the join after AsEnumerable()."))
                    | 4 ->
                        let innerExpression = readSoloDBQueryableUntyped m.Expressions.[0]
                        let outerKeySelector = unwrapLambdaExpressionOrThrow "Join outer key selector" m.Expressions.[1]
                        let innerKeySelector = unwrapLambdaExpressionOrThrow "Join inner key selector" m.Expressions.[2]
                        let resultSelector = unwrapLambdaExpressionOrThrow "Join result selector" m.Expressions.[3]

                        if isCompositeJoinKeyBody outerKeySelector.Body || isCompositeJoinKeyBody innerKeySelector.Body then
                            raise (NotSupportedException(
                                "Error: Join composite key selectors are not supported.
Reason: Anonymous-type and composite key equality lowering is deferred in this cycle.
Fix: Join on a single scalar key or move the join after AsEnumerable()."))

                        let innerRootTable =
                            match tryGetJoinRootSourceTable innerExpression with
                            | Some tableName -> tableName
                            | None ->
                                raise (NotSupportedException(
                                    "Error: Join inner source is not supported.
Reason: The inner query does not resolve to a SoloDB root collection.
Fix: Use another SoloDB IQueryable rooted in a collection or move the join after AsEnumerable()."))

                        addComplexFinal statements (fun ctx ->
                            let outerAlias = "o"
                            let innerAlias = "j"
                            let innerCtx = QueryContext.SingleSource(innerRootTable)
                            let outerKeyExpr =
                                translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body
                            let innerKeyExpr =
                                translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body
                            let resultExpr =
                                translateJoinResultSelectorExpression
                                    sourceCtx
                                    innerCtx
                                    ctx.Vars
                                    outerAlias
                                    innerAlias
                                    resultSelector.Parameters.[0]
                                    resultSelector.Parameters.[1]
                                    resultSelector.Body

                            let core =
                                { mkCore
                                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                                     { Alias = Some "Value"; Expr = resultExpr }]
                                    (Some (DerivedTable(ctx.Inner, outerAlias)))
                                  with
                                      Joins =
                                          [{ Kind = JoinKind.Inner
                                             Source = BaseTable(innerRootTable, Some innerAlias)
                                             On = Some (SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerKeyExpr)) }] }
                            wrapCore core
                        )
                    | other ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                
                
                | Single | SingleOrDefault
                | First | FirstOrDefault ->
                    match m.Value, m.Expressions with
                    | (Single | First), [||] -> ()
                    | (Single | First), [| predicate |] when isExpressionLikeArgument predicate ->
                        addFilter statements predicate
                    | (SingleOrDefault | FirstOrDefault), [||] -> ()
                    | (SingleOrDefault | FirstOrDefault), [| predicateOrDefault |] ->
                        if isExpressionLikeArgument predicateOrDefault then
                            addFilter statements predicateOrDefault
                    | (SingleOrDefault | FirstOrDefault), [| predicate; _defaultValue |]
                        when isExpressionLikeArgument predicate ->
                        addFilter statements predicate
                    | _ ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))

                    let limit =
                        match m.Value with
                        | Single | SingleOrDefault -> 2
                        | _ -> 1

                    addTake statements (ExpressionHelper.constant limit)

                | DefaultIfEmpty ->
                    // Edge case 11: DefaultIfEmpty with UNION ALL — synthetic row when result set empty
                    addComplexFinal statements (fun ctx ->
                        let defaultValueExpr =
                            match m.Expressions.Length with
                            | 0 ->
                                let genericArg = (GenericMethodArgCache.Get m.OriginalMethod).[0]
                                if genericArg.IsValueType then
                                    let defaultValueType = Activator.CreateInstance(genericArg)
                                    let jsonObj = JsonSerializator.JsonValue.Serialize defaultValueType
                                    let jsonText = jsonObj.ToJsonString()
                                    SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
                                else
                                    SqlExpr.Literal(SqlLiteral.Null)
                            | 1 ->
                                let o = QueryTranslator.evaluateExpr<obj> m.Expressions.[0]
                                let jsonObj = JsonSerializator.JsonValue.Serialize o
                                let jsonText = jsonObj.ToJsonString()
                                SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String jsonText)]); SqlExpr.Literal(SqlLiteral.String "$")])
                            | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                        // SELECT Id, Value FROM (inner) o
                        let mainProjs = [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }; { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                        let mainCore = mkCore mainProjs (Some (DerivedTable(ctx.Inner, "o")))
                        // UNION ALL SELECT -1 as Id, defaultValue WHERE NOT EXISTS (SELECT 1 FROM (inner) o)
                        let defaultProjs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = None; Expr = defaultValueExpr }]
                        let defaultCore =
                            { mkCore defaultProjs None
                              with Where = Some (SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists(ctx.Inner))) }
                        { Ctes = []; Body = UnionAllSelect(mainCore, [defaultCore]) }
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
                    | 1 -> addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName (negatePredicateForAllExpression m.Expressions.[0]) AllPredicate)
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addTake statements (ExpressionHelper.constant 1)

                    addComplexFinal statements (fun ctx ->
                        // SELECT -1 As Id, NOT EXISTS(SELECT 1 FROM (inner)) as Value
                        let existsSubquery = wrapCore (mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some (DerivedTable(ctx.Inner, "o"))))
                        let projs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists(existsSubquery)) }]
                        wrapCore (mkCore projs None)
                    )

                | Any ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 -> addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName m.Expressions.[0] AnyPredicate)
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addTake statements (ExpressionHelper.constant 1)

                    addComplexFinal statements (fun ctx ->
                        let existsSubquery = wrapCore (mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some (DerivedTable(ctx.Inner, "o"))))
                        let projs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = SqlExpr.Exists(existsSubquery) }]
                        wrapCore (mkCore projs None)
                    )


                | Contains ->
                    let struct (t, value) =
                        match m.Expressions.[0] with
                        | :? ConstantExpression as ce -> struct (ce.Type, ce.Value)
                        | other -> raise (NotSupportedException(sprintf "Invalid Contains(...) parameter: %A" other))

                    let filter = (ExpressionHelper.eq t value)
                    addFilter statements filter
                    addTake statements (ExpressionHelper.constant 1)

                    addComplexFinal statements (fun ctx ->
                        let existsSubquery = wrapCore (mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some (DerivedTable(ctx.Inner, "o"))))
                        let projs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = SqlExpr.Exists(existsSubquery) }]
                        wrapCore (mkCore projs None)
                    )

                | Append ->
                    addUnionAll statements (fun _tableName vars ->
                        let appendingObj = QueryTranslator.evaluateExpr<'T> m.Expressions.[0]
                        match QueryTranslator.isPrimitiveSQLiteType typeof<'T> with
                        | false ->
                            let struct (jsonStringElement, hasId) = serializeForCollection appendingObj
                            let idExpr =
                                if hasId then
                                    let id = HasTypeId<'T>.Read appendingObj
                                    SqlExpr.Literal(SqlLiteral.Integer id)
                                else
                                    SqlExpr.Literal(SqlLiteral.Integer -1L)
                            let valueExpr = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Literal(SqlLiteral.String jsonStringElement); SqlExpr.Literal(SqlLiteral.String "$")])
                            mkCore [{ Alias = Some "Id"; Expr = idExpr }; { Alias = Some "Value"; Expr = valueExpr }] None
                        | true ->
                            let valueExpr = allocateParam vars (box appendingObj)
                            mkCore [{ Alias = Some "Value"; Expr = valueExpr }] None
                    )
                
                
                | Concat ->
                    // Left side is the current pipeline; append the right side as UNION ALL.
                    addUnionAll statements (fun _tableName vars ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQuery<'T> sourceCtx vars rhs
                        let valueExpr = extractValueAsJsonDu rhs.Type
                        mkCore
                            [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                             { Alias = Some "Value"; Expr = valueExpr }]
                            (Some (DerivedTable(rhsSelect, "o")))
                    )

                | Except ->
                    // Edge case 16: ExceptBy/IntersectBy key selector — NOT IN subquery
                    addComplexFinal statements (fun ctx ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQuery<'T> sourceCtx ctx.Vars rhs
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        let rhsSubquery = wrapCore (mkCore
                            [{ Alias = Some "Value"; Expr = extractVal }]
                            (Some (DerivedTable(rhsSelect, "o"))))
                        let core =
                            { mkCore
                                [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                                 { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                                (Some (DerivedTable(ctx.Inner, "o")))
                              with Where = Some (SqlExpr.Binary(extractVal, BinaryOperator.NotInOp, SqlExpr.ScalarSubquery(rhsSubquery))) }
                        wrapCore core
                    )

                | Intersect ->
                    addComplexFinal statements (fun ctx ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQuery<'T> sourceCtx ctx.Vars rhs
                        let extractVal = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$")])
                        let rhsSubquery = wrapCore (mkCore
                            [{ Alias = Some "Value"; Expr = extractVal }]
                            (Some (DerivedTable(rhsSelect, "o"))))
                        let core =
                            { mkCore
                                [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                                 { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                                (Some (DerivedTable(ctx.Inner, "o")))
                              with Where = Some (SqlExpr.InSubquery(extractVal, rhsSubquery)) }
                        wrapCore core
                    )

                | ExceptBy ->
                    // Arguments: other, keySelector
                    if m.Expressions.Length <> 2 then raise (NotSupportedException(sprintf "Invalid number of arguments, expected 2, in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    let keySelE = m.Expressions.[1]
                    addComplexFinal statements (fun ctx ->
                        let keyExpr = translateExprDu sourceCtx ctx.TableName keySelE ctx.Vars
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQuery<'T> sourceCtx ctx.Vars rhs
                        let rhsValueExpr =
                            if isIdentityLambda keySelE then extractValueAsJsonDu rhs.Type
                            else translateExprDu sourceCtx ctx.TableName keySelE ctx.Vars
                        let rhsSubquery = wrapCore (mkCore
                            [{ Alias = Some "Value"; Expr = rhsValueExpr }]
                            (Some (DerivedTable(rhsSelect, "o"))))
                        let core =
                            { mkCore
                                [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                                 { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                                (Some (DerivedTable(ctx.Inner, "o")))
                              with Where = Some (SqlExpr.Binary(keyExpr, BinaryOperator.NotInOp, SqlExpr.ScalarSubquery(rhsSubquery))) }
                        wrapCore core
                    )

                | IntersectBy ->
                    // Arguments: other, keySelector
                    if m.Expressions.Length <> 2 then raise (NotSupportedException(sprintf "Invalid number of arguments, expected 2, in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    let keySelE = m.Expressions.[1]
                    addComplexFinal statements (fun ctx ->
                        let keyExpr = translateExprDu sourceCtx ctx.TableName keySelE ctx.Vars
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQuery<'T> sourceCtx ctx.Vars rhs
                        let rhsValueExpr =
                            if isIdentityLambda keySelE then extractValueAsJsonDu rhs.Type
                            else translateExprDu sourceCtx ctx.TableName keySelE ctx.Vars
                        let rhsSubquery = wrapCore (mkCore
                            [{ Alias = Some "Value"; Expr = rhsValueExpr }]
                            (Some (DerivedTable(rhsSelect, "o"))))
                        let core =
                            { mkCore
                                [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                                 { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                                (Some (DerivedTable(ctx.Inner, "o")))
                              with Where = Some (SqlExpr.InSubquery(keyExpr, rhsSubquery)) }
                        wrapCore core
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
                            // Edge case 12: Cast/OfType with $type discrimination
                            addComplexFinal statements (fun ctx ->
                                let typeExtract = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$.$type")])
                                let typeIsNull = SqlExpr.Unary(UnaryOperator.IsNull, typeExtract)
                                let typeParam = allocateParam ctx.Vars typeName
                                let typeMismatch = SqlExpr.Binary(typeExtract, BinaryOperator.Ne, typeParam)
                                // Id: NULL when type missing or mismatched, else preserve Id
                                let idExpr = SqlExpr.CaseExpr(
                                    [(typeIsNull, SqlExpr.Literal(SqlLiteral.Null))
                                     (typeMismatch, SqlExpr.Literal(SqlLiteral.Null))],
                                    Some(SqlExpr.Column(None, "Id")))
                                // Value: error string when type missing/mismatched, else preserve Value
                                let valueExpr = SqlExpr.CaseExpr(
                                    [(typeIsNull, SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "The type of item is not stored in the database, if you want to include it, then add the Polymorphic attribute to the type and reinsert all elements.")]))
                                     (typeMismatch, SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "Unable to cast object to the specified type, because the types are different.")]))],
                                    Some(SqlExpr.Column(None, "Value")))
                                let projs = [{ Alias = Some "Id"; Expr = idExpr }; { Alias = Some "Value"; Expr = valueExpr }]
                                let core = mkCore projs (Some (DerivedTable(ctx.Inner, "o")))
                                wrapCore core
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
                                let typeExtract = SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.Column(None, "Value"); SqlExpr.Literal(SqlLiteral.String "$.$type")])
                                let typeParam = allocateParam ctx.Vars typeName
                                let core =
                                    { mkCore
                                        [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                                         { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                                        (Some (DerivedTable(ctx.Inner, "o")))
                                      with Where = Some (SqlExpr.Binary(typeExtract, BinaryOperator.Eq, typeParam)) }
                                wrapCore core
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
        | ComplexDu _ ->
            ()

    and private translateQuery<'T> (sourceCtx: QueryContext) (vars: Dictionary<string, obj>) (expression: Expression) : SqlSelect =
        let statements = ResizeArray<SQLSubquery>()
        buildQuery<'T> sourceCtx statements expression
        buildLayersDu<'T> sourceCtx vars statements
    
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
            | MinBy
            | MaxBy
            | Average
            | Distinct
            | DistinctBy
            | Where
            | Select
            | Join
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
        let variables = Dictionary<string, obj>(16)

        let valueDecodedType =
            if typedefof<IQueryable>.IsAssignableFrom expression.Type then
                GenericTypeArgCache.Get expression.Type |> Array.head
            else
                expression.Type

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

        // Build the inner query as a SqlSelect DU.
        let innerSelect = translateQuery<'T> ctx variables expression

        // Build the outer wrapper: SELECT Id/(-1), valueDecoded as ValueJSON FROM (inner)
        let valueDecodedExpr = extractValueAsJsonDu valueDecodedType
        let outerProjections =
            if doesNotReturnIdFn expression then
                [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                 { Alias = Some "ValueJSON"; Expr = valueDecodedExpr }]
            else
                [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                 { Alias = Some "ValueJSON"; Expr = valueDecodedExpr }]
        let outerCore = mkCore outerProjections (Some (DerivedTable(innerSelect, "o")))
        let outerSelect = wrapCore outerCore

        // Emit to string via the minimal emitter.
        let sb = StringBuilder(256)
        let modelTableNames = collectIndexModelTableNames source.Name outerSelect
        let indexModel = SoloDatabase.IndexModel.loadModelForTables metadataConnection modelTableNames
        // Edge case 18: ExplainQueryPlan prefix
        if isExplainQueryPlan then
            sb.Append "EXPLAIN QUERY PLAN " |> ignore
        emitSelectToSb sb variables indexModel outerSelect

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

        sb.ToString(), variables, batchLoadContext

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
                    let getTerminalDefaultValue() =
                        let isExpressionLikeArgument (expr: Expression) =
                            match expr with
                            | :? LambdaExpression -> true
                            | _ -> typeof<Expression>.IsAssignableFrom expr.Type
                        let defaultExprOpt =
                            match expression with
                            | :? MethodCallExpression as mce ->
                                let args = Array.init (mce.Arguments.Count - 1) (fun i -> mce.Arguments.[i + 1])
                                match mce.Method.Name, args with
                                | ("FirstOrDefault" | "SingleOrDefault"), [| defaultValue |]
                                    when not (isExpressionLikeArgument defaultValue) ->
                                    Some defaultValue
                                | ("FirstOrDefault" | "SingleOrDefault"), [| predicate; defaultValue |]
                                    when isExpressionLikeArgument predicate ->
                                    Some defaultValue
                                | _ ->
                                    None
                            | _ ->
                                None
                        match defaultExprOpt with
                        | Some defaultExpr -> QueryTranslator.evaluateExpr<'TResult> defaultExpr
                        | None -> Unchecked.defaultof<'TResult>

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
                        | false -> getTerminalDefaultValue()
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
                        | None -> getTerminalDefaultValue()
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity

                    | "MinBy"
                    | "MaxBy" ->
                        match query |> Seq.tryHead with
                        | Some row ->
                            let entity = JsonFunctions.fromSQLite<'TResult> row
                            batchLoadSingle connection row entity
                        | None when typeof<'TResult>.IsValueType && isNull (Nullable.GetUnderlyingType(typeof<'TResult>)) ->
                            raise (InvalidOperationException("Sequence contains no elements"))
                        | None ->
                            Unchecked.defaultof<'TResult>

                    | methodName when methodName.EndsWith("OrDefault", StringComparison.Ordinal) ->
                        match query |> Seq.tryHead with
                        | None -> getTerminalDefaultValue()
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
