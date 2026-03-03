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

/// <summary>
/// Contains functions to translate .NET LINQ expression trees into SQLite SQL queries.
/// </summary>
module QueryTranslator =
    // Force module initialization to ensure DBRef handler registration.
    let private _dbrefInit = QueryTranslatorVisitDbRef.handlerCount
    let private ensureDbRefHandlersInitialized () = ignore _dbrefInit

    // Re-export symbols from split modules for backward compatibility.
    let internal appendVariable sb vars value = QueryTranslatorBase.appendVariable sb vars value
    let internal escapeSQLiteString input = QueryTranslatorBase.escapeSQLiteString input
    let inline internal evaluateExpr<'O> (e: Expression) = QueryTranslatorBase.evaluateExpr<'O> e
    let internal isAnyConstant expr = QueryTranslatorBase.isAnyConstant expr
    let internal isPrimitiveSQLiteType x = QueryTranslatorBase.isPrimitiveSQLiteType x
    let internal tryTranslateUpdateManyRelationTransform expr = QueryTranslatorVisitPost.tryTranslateUpdateManyRelationTransform expr

    /// <summary>
    /// Translates a LINQ expression into a SQL string and a dictionary of parameters.
    /// This is a primary public entry point for the translator.
    /// </summary>
    /// <param name="tableName">The name of the table to query.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// <returns>A tuple containing the generated SQL string and a dictionary of parameters.</returns>
    let translate (tableName: string) (expression: Expression) =
        ensureDbRefHandlersInitialized()
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables false tableName expression -1 ValueNone

        visit expression builder
        sb.ToString(), variables

    let internal translateWithContext (sourceContext: QueryContext) (tableName: string) (expression: Expression) =
        ensureDbRefHandlersInitialized()
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables false tableName expression -1 (ValueSome sourceContext)
        visit expression builder
        sb.ToString(), variables

    /// <summary>
    /// Internal function to translate a part of a queryable expression.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="expression">The expression to translate.</param>
    /// <param name="sb">The StringBuilder to append to.</param>
    /// <param name="variables">The dictionary of parameters.</param>
    let internal translateQueryable (tableName: string) (expression: Expression) (sb: StringBuilder) (variables: Dictionary<string, obj>) =
        ensureDbRefHandlersInitialized()
        let builder = QueryBuilder.New sb variables false tableName expression -1 ValueNone
        visit expression builder
        sb.Append " " |> ignore

    let internal translateQueryableWithContext (sourceContext: QueryContext) (tableName: string) (expression: Expression) (sb: StringBuilder) (variables: Dictionary<string, obj>) =
        ensureDbRefHandlersInitialized()
        let builder = QueryBuilder.New sb variables false tableName expression -1 (ValueSome sourceContext)
        visit expression builder
        sb.Append " " |> ignore

    /// <summary>
    /// Internal function similar to translateQueryable, but prevents wrapping the root parameter in json_extract.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="expression">The expression to translate.</param>
    /// <param name="sb">The StringBuilder to append to.</param>
    /// <param name="variables">The dictionary of parameters.</param>
    let internal translateQueryableNotExtractSelfJson (tableName: string) (expression: Expression) (sb: StringBuilder) (variables: Dictionary<string, obj>) =
        ensureDbRefHandlersInitialized()
        let builder = {(QueryBuilder.New sb variables false tableName expression -1 ValueNone) with JsonExtractSelfValue = false}
        visit expression builder
        sb.Append " " |> ignore

    /// <summary>
    /// Internal function to translate an expression that involves the document ID, using a specific parameter index for the ID.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="expression">The LINQ expression to translate.</param>
    /// <param name="idParameterIndex">The index of the parameter that represents the ID.</param>
    /// <returns>A tuple containing the generated SQL string and a dictionary of parameters.</returns>
    let internal translateWithId (tableName: string) (expression: Expression) idParameterIndex =
        ensureDbRefHandlersInitialized()
        let sb = StringBuilder()
        let variables = Dictionary<string, obj>()
        let builder = QueryBuilder.New sb variables false tableName expression idParameterIndex ValueNone

        visit expression builder
        sb.ToString(), variables

    /// <summary>
    /// Internal function to translate an expression in "update" mode, which generates SQL fragments for jsonb_set or jsonb_insert.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="expression">The expression to translate.</param>
    /// <param name="fullSQL">The StringBuilder for the SQL command.</param>
    /// <param name="variableDict">The dictionary for parameters.</param>
    let internal translateUpdateMode (tableName: string) (expression: Expression) (fullSQL: StringBuilder) (variableDict: Dictionary<string, obj>) =
        ensureDbRefHandlersInitialized()
        match tryTranslateUpdateManyRelationTransform expression with
        | ValueSome _ ->
            raise (NotSupportedException updateManyRelationUnsupportedMessage)
        | ValueNone -> ()

        let builder = QueryBuilder.New fullSQL variableDict true tableName expression -1 ValueNone

        visit expression builder
