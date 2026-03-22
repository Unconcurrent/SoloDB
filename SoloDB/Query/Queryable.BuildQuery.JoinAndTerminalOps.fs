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

module internal QueryableBuildQueryPartB =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase
    let internal apply<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> SqlSelect)
        (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
        match m.Value with
                | SupportedLinqMethods.Count
                | SupportedLinqMethods.LongCount ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 ->
                        let role =
                            match m.Value with
                            | SupportedLinqMethods.Count
                            | SupportedLinqMethods.LongCount -> LongCountPredicate
                            | _ -> CountPredicate
                        addLoweredPredicate statements (lowerPredicateLambda sourceCtx tableName m.Expressions.[0] role)
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addComplexFinal statements (fun ctx ->
                        // SELECT COUNT(Id) as Value FROM (inner) o
                        let projs = [{ Alias = Some "Value"; Expr = SqlExpr.FunctionCall("COUNT", [SqlExpr.Column(None, "Id")]) }]
                        let core = mkCore projs (Some (DerivedTable(ctx.Inner, "o")))
                        wrapCore core
                    )


                | SupportedLinqMethods.SelectMany ->
                    QueryableBuildQueryPartBSelectMany.applySelectMany<'T>
                        sourceCtx tableName statements translateQueryFn m.Expressions m.OriginalMethod

                | SupportedLinqMethods.Join ->
                    match m.Expressions.Length with
                    | 5 ->
                        raise (NotSupportedException(
                            "Error: Join comparer overload is not supported.
Reason: SoloDB only supports SQL-translatable equality without custom comparers here.
Fix: Remove the comparer or move the join after AsEnumerable()."))
                    | 4 ->
                        let innerSource =
                            match tryExtractInnerJoinSource m.Expressions.[0] with
                            | Some source -> source
                            | None ->
                                let innerExpression = readSoloDBQueryableUntyped m.Expressions.[0]
                                match tryGetJoinRootSourceTable innerExpression with
                                | Some tableName -> { TableName = tableName; WherePredicates = [] }
                                | None ->
                                    raise (NotSupportedException(
                                        "Error: Join inner source is not supported.
Reason: The inner query does not resolve to a SoloDB root collection.
Fix: Use another SoloDB IQueryable rooted in a collection or move the join after AsEnumerable()."))

                        let outerKeySelector = unwrapLambdaExpressionOrThrow "Join outer key selector" m.Expressions.[1]
                        let innerKeySelector = unwrapLambdaExpressionOrThrow "Join inner key selector" m.Expressions.[2]
                        let resultSelector = unwrapLambdaExpressionOrThrow "Join result selector" m.Expressions.[3]

                        if isCompositeJoinKeyBody outerKeySelector.Body || isCompositeJoinKeyBody innerKeySelector.Body then
                            raise (NotSupportedException(
                                "Error: Join composite key selectors are not supported.
Reason: Anonymous-type and composite key equality lowering is not supported here.
Fix: Join on a single scalar key or move the join after AsEnumerable()."))

                        let innerRootTable = innerSource.TableName

                        addComplexFinal statements (fun ctx ->
                            let outerAlias = "o"
                            let innerAlias = "j"
                            let innerCtx = QueryContext.SingleSource(innerRootTable)
                            let outerKeyExpr =
                                translateJoinSingleSourceExpression sourceCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body
                            let innerKeyExpr =
                                translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some innerKeySelector.Parameters.[0]) innerKeySelector.Body

                            let joinCondition =
                                let keyEq = SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, innerKeyExpr)
                                match innerSource.WherePredicates with
                                | [] -> keyEq
                                | predicates ->
                                    let translatedPredicates =
                                        predicates |> List.map (fun lambda ->
                                            translateJoinSingleSourceExpression innerCtx innerAlias ctx.Vars (Some lambda.Parameters.[0]) lambda.Body)
                                    translatedPredicates |> List.fold (fun acc pred -> SqlExpr.Binary(acc, BinaryOperator.And, pred)) keyEq

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
                                          [ConditionedJoin(
                                              JoinKind.Inner,
                                              BaseTable(innerRootTable, Some innerAlias),
                                              joinCondition)] }
                            wrapCore core
                        )
                    | other ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                | SupportedLinqMethods.GroupJoin ->
                    QueryableBuildQueryPartBGroupJoin.applyGroupJoin<'T>
                        sourceCtx tableName statements translateQueryFn m.Expressions

                | SupportedLinqMethods.Single | SupportedLinqMethods.SingleOrDefault
                | SupportedLinqMethods.First | SupportedLinqMethods.FirstOrDefault
                | SupportedLinqMethods.ElementAt | SupportedLinqMethods.ElementAtOrDefault ->
                    match m.Value, m.Expressions with
                    | (SupportedLinqMethods.Single | SupportedLinqMethods.First), [||] -> ()
                    | (SupportedLinqMethods.Single | SupportedLinqMethods.First), [| predicate |] when isExpressionLikeArgument predicate ->
                        addFilter statements predicate
                    | (SupportedLinqMethods.SingleOrDefault | SupportedLinqMethods.FirstOrDefault), [||] -> ()
                    | (SupportedLinqMethods.SingleOrDefault | SupportedLinqMethods.FirstOrDefault), [| predicateOrDefault |] ->
                        if isExpressionLikeArgument predicateOrDefault then
                            addFilter statements predicateOrDefault
                    | (SupportedLinqMethods.SingleOrDefault | SupportedLinqMethods.FirstOrDefault), [| predicate; _defaultValue |]
                        when isExpressionLikeArgument predicate ->
                        addFilter statements predicate
                    | (SupportedLinqMethods.ElementAt | SupportedLinqMethods.ElementAtOrDefault), [| index |] ->
                        statements.Add(Simple { emptySQLStatement () with Skip = Some index; Take = Some (ExpressionHelper.constant 1) })
                    | _ ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))

                    let limit =
                        match m.Value with
                        | SupportedLinqMethods.Single | SupportedLinqMethods.SingleOrDefault -> 2
                        | SupportedLinqMethods.ElementAt | SupportedLinqMethods.ElementAtOrDefault -> 1
                        | _ -> 1

                    match m.Value with
                    | SupportedLinqMethods.ElementAt
                    | SupportedLinqMethods.ElementAtOrDefault ->
                        ()
                    | _ ->
                        addTake statements (ExpressionHelper.constant limit)

                | SupportedLinqMethods.DefaultIfEmpty ->
                    // Edge case 11: SupportedLinqMethods.DefaultIfEmpty with UNION ALL — synthetic row when result set empty
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
                
                | SupportedLinqMethods.Last | SupportedLinqMethods.LastOrDefault ->
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

                | _ -> ()
