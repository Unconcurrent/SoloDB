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

module internal QueryableBuildQueryPartC =
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
                | SupportedLinqMethods.All ->
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

                | SupportedLinqMethods.Any ->
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


                | SupportedLinqMethods.Contains ->
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

                | SupportedLinqMethods.Append ->
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
                
                
                | SupportedLinqMethods.Concat ->
                    // Left side is the current pipeline; append the right side as UNION ALL.
                    addUnionAll statements (fun _tableName vars ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQueryFn sourceCtx vars rhs
                        let valueExpr = extractValueAsJsonDu rhs.Type
                        mkCore
                            [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                             { Alias = Some "Value"; Expr = valueExpr }]
                            (Some (DerivedTable(rhsSelect, "o")))
                    )

                | SupportedLinqMethods.Except ->
                    // Edge case 16: SupportedLinqMethods.ExceptBy/SupportedLinqMethods.IntersectBy key selector — NOT IN subquery
                    addComplexFinal statements (fun ctx ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQueryFn sourceCtx ctx.Vars rhs
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

                | SupportedLinqMethods.Intersect ->
                    addComplexFinal statements (fun ctx ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQueryFn sourceCtx ctx.Vars rhs
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

                | SupportedLinqMethods.ExceptBy ->
                    // Arguments: other, keySelector
                    if m.Expressions.Length <> 2 then raise (NotSupportedException(sprintf "Invalid number of arguments, expected 2, in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    let keySelE = m.Expressions.[1]
                    addComplexFinal statements (fun ctx ->
                        let keyExpr = translateExprDu sourceCtx ctx.TableName keySelE ctx.Vars
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQueryFn sourceCtx ctx.Vars rhs
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

                | SupportedLinqMethods.IntersectBy ->
                    // Arguments: other, keySelector
                    if m.Expressions.Length <> 2 then raise (NotSupportedException(sprintf "Invalid number of arguments, expected 2, in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    let keySelE = m.Expressions.[1]
                    addComplexFinal statements (fun ctx ->
                        let keyExpr = translateExprDu sourceCtx ctx.TableName keySelE ctx.Vars
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQueryFn sourceCtx ctx.Vars rhs
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

                | SupportedLinqMethods.UnionBy ->
                    // UnionBy(other, keySelector): UNION ALL + deduplicate by key (first occurrence wins).
                    // Emits: SELECT Id, Value FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY Id) AS _rn
                    //         FROM (left UNION ALL right)) WHERE _rn = 1
                    if m.Expressions.Length <> 2 then raise (NotSupportedException(sprintf "Invalid number of arguments, expected 2, in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    let keySelE = m.Expressions.[1]
                    addUnionAll statements (fun _tableName vars ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQueryFn sourceCtx vars rhs
                        let valueExpr = extractValueAsJsonDu rhs.Type
                        mkCore
                            [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                             { Alias = Some "Value"; Expr = valueExpr }]
                            (Some (DerivedTable(rhsSelect, "o")))
                    )
                    // After UNION ALL, apply DistinctBy-style deduplication via ROW_NUMBER window.
                    addLoweredKeySelector statements (lowerKeySelectorLambda sourceCtx tableName keySelE DistinctByKey)
                    addComplexFinal statements (fun ctx ->
                        let innerProjs =
                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }
                             { Alias = Some "__solodb_rn"; Expr = SqlExpr.WindowCall({
                                Kind = WindowFunctionKind.RowNumber
                                Arguments = []
                                PartitionBy = [SqlExpr.Column(Some "o", "__solodb_group_key")]
                                OrderBy = [(SqlExpr.Column(Some "o", "Id"), SortDirection.Asc)]
                             }) }]
                        let innerCore = mkCore innerProjs (Some (DerivedTable(ctx.Inner, "o")))
                        let innerSel = wrapCore innerCore
                        let outerProjs =
                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }]
                        let outerCore =
                            { mkCore outerProjs (Some (DerivedTable(innerSel, "o")))
                              with Where = Some (SqlExpr.Binary(SqlExpr.Column(Some "o", "__solodb_rn"), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L))) }
                        wrapCore outerCore
                    )

                // --- Type filtering / casting over polymorphic payloads ---
                | SupportedLinqMethods.Cast ->
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
                                    (typeIsNull, SqlExpr.Literal(SqlLiteral.Null)),
                                    [(typeMismatch, SqlExpr.Literal(SqlLiteral.Null))],
                                    Some(SqlExpr.Column(None, "Id")))
                                // Value: error string when type missing/mismatched, else preserve Value
                                let valueExpr = SqlExpr.CaseExpr(
                                    (typeIsNull, SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "The type of item is not stored in the database, if you want to include it, then add the Polymorphic attribute to the type and reinsert all elements.")])),
                                    [(typeMismatch, SqlExpr.FunctionCall("json_quote", [SqlExpr.Literal(SqlLiteral.String "Unable to cast object to the specified type, because the types are different.")]))],
                                    Some(SqlExpr.Column(None, "Value")))
                                let projs = [{ Alias = Some "Id"; Expr = idExpr }; { Alias = Some "Value"; Expr = valueExpr }]
                                let core = mkCore projs (Some (DerivedTable(ctx.Inner, "o")))
                                wrapCore core
                            )

                | SupportedLinqMethods.OfType ->
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

                | SupportedLinqMethods.Exclude ->
                    if m.Expressions.Length > 0 then
                        // Selector-based Exclude: extract property path and register as exclusion.
                        let path = extractRelationPathOrThrow "Exclude" m.Expressions
                        registerExcludePath sourceCtx path
                    else
                        // Parameterless Exclude(): whitelist mode — set up-front in Main.fs.
                        sourceCtx.WhitelistMode <- true
                | SupportedLinqMethods.Include ->
                    // Extract property path from the selector lambda and add to IncludedPaths.
                    // SupportedLinqMethods.Include does not produce SQL — it only controls relation hydration whitelist.
                    let path = extractRelationPathOrThrow "Include" m.Expressions
                    registerIncludePath sourceCtx path

                | SupportedLinqMethods.ThenInclude ->
                    // ThenInclude appends to the chain path from the carrier.
                    // The dotted path is composed in the extension method and registered here.
                    let path = extractRelationPathOrThrow "ThenInclude" m.Expressions
                    registerIncludePath sourceCtx path

                | SupportedLinqMethods.ThenExclude ->
                    // ThenExclude paths are registered up-front in Main.fs with full dotted path composition.
                    // No-op here — the up-front pass already handles the correct dotted path.
                    ()


                | SupportedLinqMethods.Aggregate ->
                    raise (NotSupportedException("Aggregate is not supported."))

                | _ -> ()
