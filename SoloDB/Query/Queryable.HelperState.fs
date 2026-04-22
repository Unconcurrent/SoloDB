namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite
open SQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SqlDu.Engine.C1.Spec

module internal QueryableHelperState =
    open QueryableHelperBase
    let internal emptySQLStatement () =
        { Filters = ResizeArray(4); Orders = ResizeArray(1); Selector = None; Skip = None; Take = None; TableName = ""; UnionAll = ResizeArray<string -> Dictionary<string, obj> -> SelectCore>(0) }

    let inline internal simpleCurrent (statements: ResizeArray<SQLSubquery>) =
        match statements.Last() with
        | Simple s -> s
        | ComplexDu _ ->
            let n = emptySQLStatement ()
            statements.Add (Simple n)
            n

    let inline internal ifSelectorNewStatement (statements: ResizeArray<SQLSubquery>) =
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

    let internal lowerPredicateLambda (_sourceCtx: QueryContext) (_tableName: string) (expr: Expression) (role: PredicateRole) =
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

    let internal addLoweredPredicate (statements: ResizeArray<SQLSubquery>) (lowered: LoweredPredicate) =
        addFilter statements lowered.Predicate

    let internal lowerKeySelectorLambda (_sourceCtx: QueryContext) (_tableName: string) (expr: Expression) (role: KeySelectorRole) =
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

    let internal addLoweredKeySelector (statements: ResizeArray<SQLSubquery>) (lowered: LoweredKeySelector) =
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
    let internal aggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (DuSelector (fun tableName vars ->
            let innerExpr =
                match args.Length with
                | 0 -> translateExprDu sourceCtx tableName (method.ReturnType |> ExpressionHelper.id) vars
                | 1 -> translateExprDu sourceCtx tableName args.[0] vars
                | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" method.Name other))
            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
             { Alias = Some "Value"; Expr = SqlExpr.FunctionCall(fnName, [innerExpr]) }]
        ))

    let internal zeroIfNullAggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (DuSelector (fun tableName vars ->
            let innerExpr =
                match args.Length with
                | 0 -> translateExprDu sourceCtx tableName (method.ReturnType |> ExpressionHelper.id) vars
                | 1 -> translateExprDu sourceCtx tableName args.[0] vars
                | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" method.Name other))
            [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
             { Alias = Some "Value"; Expr = SqlExpr.Coalesce(SqlExpr.FunctionCall(fnName, [innerExpr]), [SqlExpr.Literal(SqlLiteral.Integer 0L)]) }]
        ))

    let internal isDecimalOrNullableDecimal (t: Type) =
        t = typeof<decimal> ||
        (t.IsGenericType &&
         t.GetGenericTypeDefinition() = typedefof<Nullable<_>> &&
         t.GetGenericArguments().[0] = typeof<decimal>)

    /// <summary>
    /// Extracts the expression for a collection that is an argument to a set-based method like Concat or Except.
    /// </summary>
    /// <param name="methodArg">The method argument expression.</param>
    /// <returns>The expression representing the queryable collection.</returns>
