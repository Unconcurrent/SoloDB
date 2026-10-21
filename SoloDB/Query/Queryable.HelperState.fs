namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Text
open System.Runtime.CompilerServices
open NativeSQLiteTools
open Utils
open JsonFunctions
open Connections
open SoloDatabase
open SoloDatabase.JsonSerializator
open SoloDatabase.RelationsTypes
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.SqlModel

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

    let private hasBounds (statement: UsedSQLStatements) = statement.Skip.IsSome || statement.Take.IsSome

    let internal hasBoundedInput (statements: ResizeArray<SQLSubquery>) =
        statements |> Seq.exists (function Simple statement -> hasBounds statement | ComplexDu _ -> false)

    /// Later consumers operate on the membership already selected by this bound.
    let internal completeBounded (statements: ResizeArray<SQLSubquery>) =
        let current = simpleCurrent statements
        if hasBounds current then
            let n = emptySQLStatement ()
            statements.Add (Simple n)
            n
        else current

    let inline internal ifSelectorNewStatement (statements: ResizeArray<SQLSubquery>) =
        let current = completeBounded statements
        if current.Selector.IsSome then
            let n = emptySQLStatement ()
            statements.Add (Simple n)
            n
        else current

    let addFilter (statements: ResizeArray<SQLSubquery>) (filter: Expression) =
        let last = statements.Last()
        let inline addNewQuery () =
            (statements.Add << Simple) { emptySQLStatement () with Filters = ResizeArray [ filter ] }

        match last with
        | Simple last when last.Selector.IsNone && last.UnionAll.Count = 0 && last.Skip.IsNone && last.Take.IsNone ->
            last.Filters.Add filter
        | _ ->
            addNewQuery ()

    let internal lowerKeySelectorLambda (expr: Expression) =
        { KeyExpression = expr
          RelationAccess = detectRelationAccess expr
          Fingerprint = expressionFingerprint expr }

    let addOrder (statements: ResizeArray<SQLSubquery>) (ordering: Expression) (descending: bool) =
        let current = ifSelectorNewStatement statements
        current.Orders.Clear()
        current.Orders.Add({ OrderingRule = ordering; Direction = SortDirection.ofDescending descending; RawExpr = None })

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

    let addTake (statements: ResizeArray<SQLSubquery>) (e: Expression) =
        let current = simpleCurrent statements
        match current.Take with
        | Some e2 -> current.Take <- Some (TypeMetadata.ExpressionHelper.min e2 e)
        | None   -> current.Take <- Some e

    // The ComplexDu subquery will be the last subquery processed. It receives the inner SqlSelect and returns a new SqlSelect.
    let addComplexFinal (statements: ResizeArray<SQLSubquery>) (buildFunc: struct {|Vars: Dictionary<string, obj>; Inner: SqlSelect; Input: QuerySerializationInput; KeyInput: QuerySerializationInput voption; TableName: string|} -> TranslatedQuery) =
        statements.Add (ComplexDu buildFunc)

    /// <summary>
    /// Recursively translates a LINQ expression tree into an SQL query.
    /// This is the main dispatcher for the translation process.
    /// </summary>
    /// <param name="builder">The query builder instance that accumulates the SQL and parameters.</param>
    /// <param name="expression">The expression to translate.</param>
    let internal aggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (DuSelector (fun currentCtx _input tableName vars ->
            let innerExpr =
                match args.Length with
                | 0 -> translateExprDu currentCtx tableName (QueryTranslatorNumeric.aggregateInput method |> Expression.Parameter) vars
                | 1 -> translateExprDu currentCtx tableName args.[0] vars
                | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" method.Name other))
            let value = QueryTranslatorNumeric.aggregate method (QueryTranslatorNumeric.aggregateInput method) (QueryTranslatorNumeric.kind fnName) innerExpr false
            let produced = QueryProduced.typed method.ReturnType value
            struct([{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                    { Alias = Some "Value"; Expr = value }], produced.Input)
        ))

    let internal zeroIfNullAggregateTranslator (sourceCtx: QueryContext) (fnName: string) (queries: SQLSubquery ResizeArray) (method: MethodInfo) (args: Expression array) =
        addSelector queries (DuSelector (fun currentCtx _input tableName vars ->
            let innerExpr =
                match args.Length with
                | 0 -> translateExprDu currentCtx tableName (QueryTranslatorNumeric.aggregateInput method |> Expression.Parameter) vars
                | 1 -> translateExprDu currentCtx tableName args.[0] vars
                | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" method.Name other))
            let value = SqlExpr.Coalesce(QueryTranslatorNumeric.aggregate method (QueryTranslatorNumeric.aggregateInput method) (QueryTranslatorNumeric.kind fnName) innerExpr false, [QueryTranslatorNumeric.zero method.ReturnType])
            let produced = QueryProduced.typed method.ReturnType value
            struct([{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                    { Alias = Some "Value"; Expr = value }], produced.Input)
        ))

    let internal isDecimalOrNullableDecimal (t: Type) =
        typeof<decimal>.Equals(t) ||
        (t.IsGenericType &&
         typedefof<Nullable<_>>.Equals(t.GetGenericTypeDefinition()) &&
         typeof<decimal>.Equals(t.GetGenericArguments().[0]))

    /// <summary>
    /// Extracts the expression for a collection that is an argument to a set-based method like Concat or Except.
    /// </summary>
    /// <param name="methodArg">The method argument expression.</param>
    /// <returns>The expression representing the queryable collection.</returns>
