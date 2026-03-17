namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Text
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.QueryTranslatorVisitCore
open SoloDatabase.QueryTranslatorVisitPost
open SoloDatabase.QueryTranslatorVisitDbRef
open SqlDu.Engine.C1.Spec

/// <summary>
/// Contains functions to translate .NET LINQ expression trees into SQLite SQL queries.
/// All entry points route through the DU construction path (visitDu) + DU emission (SqlDuMinimalEmit).
/// </summary>
module QueryTranslator =
    let private ensureDbRefHandlersInitialized () =
        let count = QueryTranslatorVisitDbRef.handlerCount
        if count < 3 then
            raise (InvalidOperationException(
                $"DBRef handler registration incomplete: count={count}, expected=3"))

    // Re-export symbols from split modules used by Queryable.fs, Helper.Schema.fs, and other product code.
    let internal escapeSQLiteString input = QueryTranslatorBase.escapeSQLiteString input
    let inline internal evaluateExpr<'O> (e: Expression) = QueryTranslatorBase.evaluateExpr<'O> e
    let internal isAnyConstant expr = QueryTranslatorBase.isAnyConstant expr
    let internal isPrimitiveSQLiteType x = QueryTranslatorBase.isPrimitiveSQLiteType x
    let internal tryTranslateUpdateManyRelationTransform expr = QueryTranslatorVisitPost.tryTranslateUpdateManyRelationTransform expr

    /// <summary>
    /// Translates a LINQ expression into a SQL string and a dictionary of parameters.
    /// Routes through DU construction (visitDu) and DU emission (SqlDuMinimalEmit).
    /// </summary>
    /// <param name="tableName">The name of the table to query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// Canonical predicate path: translates a filter expression to SqlExpr DU + variables.
    /// Used by write-path call sites that compose UPDATE/DELETE templates with DU-emitted WHERE.
    /// This is the canonical predicate producer; `translate` is a thin wrapper over this.
    let translateWhereExpr (tableName: string) (expression: Expression) : SqlExpr * Dictionary<string, obj> =
        ensureDbRefHandlersInitialized()
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables false tableName expression -1 ValueNone
        let duExpr = visitDu expression builder
        duExpr, variables

    /// <returns>A tuple containing the generated SQL string and a dictionary of parameters.</returns>
    let translate (tableName: string) (expression: Expression) =
        // Thin wrapper over translateWhereExpr: get DU, emit via MinimalEmit (product path).
        let duExpr, variables = translateWhereExpr tableName expression
        let sb = StringBuilder()
        let builder = QueryBuilder.New sb variables false tableName expression -1 ValueNone
        SqlDuMinimalEmit.emitExpr builder duExpr
        sb.ToString(), variables

    /// Returns a SqlExpr DU node for an expression without emitting to any StringBuilder.
    /// Used by the Queryable DU construction path to build SqlSelect trees.
    /// Side effects: allocates parameters in the provided Variables dict; may populate sourceContext.Joins.
    let internal translateToSqlExpr (sourceContext: QueryContext) (tableName: string) (expression: Expression) (variables: Dictionary<string, obj>) : SqlExpr =
        ensureDbRefHandlersInitialized()
        let sb = StringBuilder()
        let builder = QueryBuilder.New sb variables false tableName expression -1 (ValueSome sourceContext)
        visitDu expression builder

    /// <summary>
    /// Translates an expression and appends the result to an existing StringBuilder.
    /// Routes through DU construction (visitDu) and DU emission (SqlDuMinimalEmit).
    /// </summary>
    let internal translateQueryable (tableName: string) (expression: Expression) (sb: StringBuilder) (variables: Dictionary<string, obj>) =
        ensureDbRefHandlersInitialized()
        let sbStart = sb.Length
        let builder = QueryBuilder.New sb variables false tableName expression -1 ValueNone
        let duExpr = visitDu expression builder
        sb.Length <- sbStart
        SqlDuMinimalEmit.emitExpr builder duExpr
        sb.Append " " |> ignore

    /// <summary>
    /// Translates an expression in "update" mode, generating SQL fragments for jsonb_set arguments.
    /// Routes through DU construction (visitDu with UpdateMode) and DU emission (SqlDuMinimalEmit).
    /// </summary>
    let internal translateUpdateMode (tableName: string) (expression: Expression) (fullSQL: StringBuilder) (variableDict: Dictionary<string, obj>) =
        ensureDbRefHandlersInitialized()
        match tryTranslateUpdateManyRelationTransform expression with
        | ValueSome _ ->
            raise (NotSupportedException updateManyRelationUnsupportedMessage)
        | ValueNone -> ()

        let sbStart = fullSQL.Length
        let builder = QueryBuilder.New fullSQL variableDict true tableName expression -1 ValueNone
        let duExpr = visitDu expression builder
        fullSQL.Length <- sbStart
        SqlDuMinimalEmit.emitExpr builder duExpr
