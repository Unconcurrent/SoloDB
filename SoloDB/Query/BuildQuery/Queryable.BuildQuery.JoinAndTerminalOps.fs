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

module internal QueryableBuildQueryJoinAndTerminalOps =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperJoinBindings
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase

    let internal apply<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> TranslatedQuery)
        (m: {| Value: SupportedLinqMethods; OriginalMethod: MethodInfo; Expressions: Expression array |}) =
        match m.Value with
                | SupportedLinqMethods.Count
                | SupportedLinqMethods.LongCount ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 ->
                        addFilter statements m.Expressions.[0]
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addComplexFinal statements (fun ctx ->
                        // SELECT COUNT(Id) as Value FROM (inner) o
                        let projs = [{ Alias = Some "Value"; Expr = SqlExpr.FunctionCall("COUNT", [SqlExpr.Column(None, "Id")]) }]
                        let core = mkCore projs (Some (DerivedTable(ctx.Inner, "o")))
                        { Query = wrapCore core; SerializationInput = (QueryProduced.typed m.OriginalMethod.ReturnType projs.Head.Expr).Input; KeyInput = ValueNone }
                    )


                | SupportedLinqMethods.SelectMany ->
                    QueryableBuildQuerySelectManyOps.applySelectMany<'T>
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
                                match tryGetJoinRootSource innerExpression with
                                | Some struct(tableName, originalType) -> { TableName = tableName; SerializationType = originalType; WherePredicates = [] }
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
                            let outerCtx =
                                { sourceCtx with
                                    LayerPosition = OuterLayer
                                    SerializationInput = ctx.Input
                                    Joins = ResizeArray() }
                            let innerCtx =
                                { sourceCtx with
                                    RootTable = innerRootTable
                                    SerializationInput = QueryTranslatorPhysicalSource.ownerInput sourceCtx innerSource.SerializationType innerRootTable (SqlExpr.Column(Some innerAlias, "Id"))
                                    LayerPosition = BaseLayer
                                    RootGraph = QueryRootGraph.Single(innerRootTable)
                                    Joins = ResizeArray() }
                            let outerKeyExpr =
                                translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars (Some outerKeySelector.Parameters.[0]) outerKeySelector.Body
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

                            let result =
                                translateJoinResultSelectorProduced
                                    outerCtx
                                    innerCtx
                                    ctx.Vars
                                    outerAlias
                                    innerAlias
                                    resultSelector.Parameters.[0]
                                    resultSelector.Parameters.[1]
                                    resultSelector.Body ValueNone

                            let outerDiscoveredJoins =
                                QueryTranslatorReferenceJoin.preparedJoinClauses outerCtx.Joins
                            let innerDiscoveredJoins =
                                QueryTranslatorReferenceJoin.preparedJoinClauses innerCtx.Joins
                            let core =
                                { mkCore
                                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                                     { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }]
                                    (Some (DerivedTable(ctx.Inner, outerAlias)))
                                  with
                                      Joins =
                                          outerDiscoveredJoins
                                          @ [ConditionedJoin(
                                                JoinKind.Inner,
                                                BaseTable(innerRootTable, Some innerAlias),
                                                joinCondition)]
                                          @ innerDiscoveredJoins }
                            let struct(query, input) = QueryProduced.publishCore sourceCtx.NextAlias core result false
                            { Query = query; SerializationInput = input; KeyInput = ValueNone }
                        )
                    | other ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                | SupportedLinqMethods.GroupJoin ->
                    QueryableBuildQueryGroupJoinOps.applyGroupJoin<'T>
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
                        statements.Add(Simple { emptySQLStatement () with Skip = Some index; Take = Some (TypeMetadata.ExpressionHelper.constant 1) })
                    | _ ->
                        raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))

                    let limit =
                        match m.Value with
                        | SupportedLinqMethods.Single | SupportedLinqMethods.SingleOrDefault -> 2
                        | SupportedLinqMethods.ElementAt | SupportedLinqMethods.ElementAtOrDefault
                        | SupportedLinqMethods.First | SupportedLinqMethods.FirstOrDefault
                        | SupportedLinqMethods.Last | SupportedLinqMethods.LastOrDefault -> 1
                        // Unreachable: outer dispatch only routes element terminals to this block.
                        | _ -> 1

                    match m.Value with
                    | SupportedLinqMethods.ElementAt
                    | SupportedLinqMethods.ElementAtOrDefault ->
                        ()
                    | _ ->
                        addTake statements (TypeMetadata.ExpressionHelper.constant limit)

                | SupportedLinqMethods.DefaultIfEmpty ->
                    // Edge case 11: SupportedLinqMethods.DefaultIfEmpty with UNION ALL — synthetic row when result set empty
                    addComplexFinal statements (fun ctx ->
                        let target = (TypeMetadata.GenericMethodArgCache.Get m.OriginalMethod)[0]
                        let fallback =
                            match m.Expressions.Length with
                            | 0 ->
                                let value = if target.IsValueType then RelationsAccessorCache.activateDefault target else null
                                QueryTranslator.translateProduced sourceCtx "" (Expression.Constant(value, target)) ctx.Vars
                            | 1 -> QueryTranslator.translateProduced sourceCtx "" m.Expressions[0] ctx.Vars
                            | other -> raise(NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))
                        let name = "_input" + sourceCtx.NextAlias()
                        let current = QueryTranslatorSerialization.scopeSerializationInput (Some "o") OuterLayer ctx.Input
                        let mainCore = mkCore [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                                               { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }
                                               { Alias = Some name; Expr = QuerySerializationSql.inputDescriptor current }]
                                              (Some(DerivedTable(ctx.Inner, "o")))
                        let output = QueryTranslatorSerialization.mergeInputs target [|ctx.Input; fallback.Input|]
                        let packet = QueryProduced.packet fallback
                        let alias = sourceCtx.NextAlias()
                        let column = SqlExpr.Column(Some alias, "value")
                        let token = SqlExpr.FunctionCall("jsonb_extract", [column; SqlExpr.Literal(SqlLiteral.String "$[0]")])
                        let value = if ctx.Input.Carrier = CanonicalToken then token else QueryProduced.scalarValue target token { fallback.Input with Carrier = CanonicalToken }
                        let defaultCore =
                            { mkCore [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }
                                      { Alias = Some "Value"; Expr = value }
                                      { Alias = Some name; Expr = SqlExpr.FunctionCall("jsonb_extract", [column; SqlExpr.Literal(SqlLiteral.String "$[1]")]) }]
                                     (Some(FromJsonEach(SqlExpr.JsonArrayExpr [packet], Some alias))) with
                                Where = Some(SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists ctx.Inner)) }
                        { Query = { Ctes = []; Body = UnionAllSelect(mainCore, [defaultCore]) }
                          SerializationInput = QueryTranslatorSerialization.fromDescriptor (SqlExpr.Column(None, name)) output
                          KeyInput = ValueNone }
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
                        addOrder statements (TypeMetadata.ExpressionHelper.get(fun (x: obj) -> x.Dyn<int64>("Id"))) true
                    addTake statements (TypeMetadata.ExpressionHelper.constant 1)

                | _ -> ()
        ()
