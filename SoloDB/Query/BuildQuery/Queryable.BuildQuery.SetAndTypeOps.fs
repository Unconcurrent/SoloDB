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
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.SqlModel

module internal QueryableBuildQuerySetAndTypeOps =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase
    let private setValue elementType alias =
        let column = SqlExpr.Column(Some alias, "Value")
        let decoded = SqlExpr.FunctionCall("jsonb_extract", [column; SqlExpr.Literal(SqlLiteral.String "$")])
        if QueryTranslatorBaseTypes.isPrimitiveSQLiteType elementType then
            SqlExpr.CaseExpr(
                (SqlExpr.Binary(SqlExpr.FunctionCall("typeof", [column]), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String "blob")), decoded),
                [], Some column)
        else decoded
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
                    | 1 -> addFilter statements (negatePredicateForAllExpression m.Expressions.[0])
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addTake statements (UtilsReflection.ExpressionHelper.constant 1)

                    addComplexFinal statements (fun ctx ->
                        // SELECT -1 As Id, NOT EXISTS(SELECT 1 FROM (inner)) as Value
                        let existsSubquery = wrapCore (mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some (DerivedTable(ctx.Inner, "o"))))
                        let projs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists(existsSubquery)) }]
                        wrapCore (mkCore projs None)
                    )

                | SupportedLinqMethods.Any ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 -> addFilter statements m.Expressions.[0]
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addTake statements (UtilsReflection.ExpressionHelper.constant 1)

                    addComplexFinal statements (fun ctx ->
                        let existsSubquery = wrapCore (mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some (DerivedTable(ctx.Inner, "o"))))
                        let projs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = SqlExpr.Exists(existsSubquery) }]
                        wrapCore (mkCore projs None)
                    )


                | SupportedLinqMethods.Contains ->
                    if m.Expressions.Length <> 1 then
                        raise (NotSupportedException("Custom query comparers cannot be translated to SQL. Use the default comparer overload or call AsEnumerable() first."))
                    let value = m.Expressions.[0]
                    let parameter = Expression.Parameter value.Type
                    let filter = Expression.Lambda(Expression.Equal(parameter, value), [|parameter|])
                    addFilter statements filter
                    addTake statements (UtilsReflection.ExpressionHelper.constant 1)

                    addComplexFinal statements (fun ctx ->
                        let existsSubquery = wrapCore (mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some (DerivedTable(ctx.Inner, "o"))))
                        let projs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = SqlExpr.Exists(existsSubquery) }]
                        wrapCore (mkCore projs None)
                    )

                // todo: Append works at root level but is not yet supported at DBRefMany level (see Extract.fs)
                | SupportedLinqMethods.Append ->
                    addUnionAll statements (fun _tableName vars ->
                        match sourceCtx.BindQueryValue with
                        | ValueSome binder when binder.IsValue m.Expressions.[0] ->
                            let value = binder.Local m.Expressions.[0]
                            if QueryTranslatorBaseTypes.isPrimitiveSQLiteType value.Type then
                                mkCore [{ Alias = Some "Value"; Expr = binder.Parameter value }] None
                            else
                                let serializer = UtilsReflection.ExpressionHelper.get(fun (item: 'T) -> serializeForCollection item)
                                let idReader = UtilsReflection.ExpressionHelper.get(fun (item: 'T) ->
                                    if HasTypeId<'T>.Value then HasTypeId<'T>.Read item else -1L)
                                let json = binder.Scalar(Expression.Invoke(serializer, value))
                                let id = binder.Scalar(Expression.Invoke(idReader, value))
                                let payload = SqlExpr.FunctionCall("jsonb_extract", [json; SqlExpr.Literal(SqlLiteral.String "$")])
                                mkCore [{ Alias = Some "Id"; Expr = id }; { Alias = Some "Value"; Expr = payload }] None
                        | _ ->
                            let appendingObj = QueryTranslatorBaseHelpers.evaluateExpr<'T> m.Expressions.[0]
                            match QueryTranslatorBaseTypes.isPrimitiveSQLiteType typeof<'T> with
                            | false ->
                                let jsonStringElement = serializeForCollection appendingObj
                                let idExpr =
                                    if HasTypeId<'T>.Value then
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
                        let elementType = m.OriginalMethod.GetGenericArguments().[0]
                        let valueExpr =
                            if QueryTranslatorBaseTypes.isPrimitiveSQLiteType elementType then setValue elementType "o"
                            else SqlExpr.Column(Some "o", "Value")
                        mkCore
                            [{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                             { Alias = Some "Value"; Expr = valueExpr }]
                            (Some (DerivedTable(rhsSelect, "o")))
                    )

                | SupportedLinqMethods.Except
                | SupportedLinqMethods.Intersect
                | SupportedLinqMethods.ExceptBy
                | SupportedLinqMethods.IntersectBy ->
                    let isExcept = m.Value = SupportedLinqMethods.Except || m.Value = SupportedLinqMethods.ExceptBy
                    let byKey = m.Value = SupportedLinqMethods.ExceptBy || m.Value = SupportedLinqMethods.IntersectBy
                    let argumentCount = if byKey then 2 else 1
                    if m.Expressions.Length <> argumentCount then
                        raise (NotSupportedException("Custom set comparers cannot be translated to SQL. Use the default comparer or call AsEnumerable() first."))
                    let elementType = m.OriginalMethod.GetGenericArguments().[if byKey then 1 else 0]
                    addComplexFinal statements (fun ctx ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQueryFn sourceCtx ctx.Vars rhs
                        // Set membership includes NULL and emits each matching value once.
                        // Scalar projections are already SQL values, not JSON documents.
                        let leftValue =
                            if byKey && not (isIdentityLambda m.Expressions.[1]) then translateExprDu sourceCtx "set_left" m.Expressions.[1] ctx.Vars
                            else setValue elementType "set_left"
                        let rightValue = setValue elementType "set_right"
                        let rightSource = Some (DerivedTable(rhsSelect, "set_right"))
                        let nonNullValues =
                            { mkCore [{ Alias = Some "Value"; Expr = rightValue }] rightSource
                              with Where = Some (SqlExpr.Unary(UnaryOperator.IsNotNull, rightValue)) }
                            |> wrapCore
                        let hasNull =
                            { mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] rightSource
                              with Where = Some (SqlExpr.Unary(UnaryOperator.IsNull, rightValue)) }
                            |> wrapCore |> SqlExpr.Exists
                        // Keep the right-hand sets uncorrelated: SQLite can build membership
                        // once instead of scanning the right input for every left row.
                        let matches =
                            SqlExpr.Binary(
                                SqlExpr.Coalesce(SqlExpr.InSubquery(leftValue, nonNullValues), [SqlExpr.Literal(SqlLiteral.Integer 0L)]),
                                BinaryOperator.Or,
                                SqlExpr.Binary(SqlExpr.Unary(UnaryOperator.IsNull, leftValue), BinaryOperator.And, hasNull))
                        let predicate = if isExcept then SqlExpr.Unary(UnaryOperator.Not, matches) else matches
                        let input =
                            if byKey then
                                // A window over the completed input keeps its delivered order,
                                // including bounds, before choosing the first value for each key.
                                let ordinal = SqlExpr.WindowCall { Kind = WindowFunctionKind.RowNumber; Arguments = []; PartitionBy = []; OrderBy = [] }
                                wrapCore (mkCore
                                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "set_input", "Id") }
                                     { Alias = Some "Value"; Expr = SqlExpr.Column(Some "set_input", "Value") }
                                     { Alias = Some "__set_ordinal"; Expr = ordinal }]
                                    (Some(DerivedTable(ctx.Inner, "set_input"))))
                            else ctx.Inner
                        let first = SqlExpr.AggregateCall(AggregateKind.Min, Some(SqlExpr.Column(Some "set_left", "__set_ordinal")), false, None)
                        let id = SqlExpr.Column(Some "set_left", "Id")
                        let projections =
                            [{ Alias = Some "Id"; Expr = if byKey then id else SqlExpr.AggregateCall(AggregateKind.Min, Some id, false, None) }
                             { Alias = Some "Value"; Expr = SqlExpr.Column(Some "set_left", "Value") }]
                            @ (if byKey then [{ Alias = Some "__set_first"; Expr = first }] else [])
                        let core =
                            { mkCore projections (Some (DerivedTable(input, "set_left")))
                              with Where = Some predicate
                                   GroupBy = [leftValue]
                                   OrderBy = if byKey then [{ Expr = first; Direction = SortDirection.Asc }] else [] }
                        wrapCore core
                    )

                | SupportedLinqMethods.UnionBy ->
                    if m.Expressions.Length <> 2 then
                        raise (NotSupportedException("Custom set comparers cannot be translated to SQL. Use the default comparer overload or call AsEnumerable() first."))
                    addComplexFinal statements (fun ctx ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let rhsSelect = translateQueryFn sourceCtx ctx.Vars rhs
                        let arm input alias branch =
                            let col name = SqlExpr.Column(Some alias, name)
                            let elementType = m.OriginalMethod.GetGenericArguments().[0]
                            let value =
                                if QueryTranslatorBaseTypes.isPrimitiveSQLiteType elementType then setValue elementType alias
                                else col "Value"
                            let key =
                                if isIdentityLambda m.Expressions.[1] then
                                    setValue elementType alias
                                else translateExprDu sourceCtx alias m.Expressions.[1] ctx.Vars
                            let ordinal = SqlExpr.WindowCall {
                                Kind = WindowFunctionKind.RowNumber; Arguments = []; PartitionBy = []; OrderBy = [] }
                            mkCore
                                [{ Alias = Some "Id"; Expr = col "Id" }
                                 { Alias = Some "Value"; Expr = value }
                                 { Alias = Some "__set_key"; Expr = key }
                                 { Alias = Some "__set_branch"; Expr = SqlExpr.Literal(SqlLiteral.Integer branch) }
                                 { Alias = Some "__set_ordinal"; Expr = ordinal }]
                                (Some(DerivedTable(input, alias)))
                        // Rank each completed input before combining them. Row identity
                        // is not sequence position, and the entire left input precedes right.
                        let combined = { Ctes = []; Body = UnionAllSelect(arm ctx.Inner "set_left" 0L, [arm rhsSelect "set_right" 1L]) }
                        let col name = SqlExpr.Column(Some "set_union", name)
                        let position = [col "__set_branch", SortDirection.Asc; col "__set_ordinal", SortDirection.Asc]
                        let rank = SqlExpr.WindowCall {
                            Kind = WindowFunctionKind.RowNumber; Arguments = []
                            PartitionBy = [col "__set_key"]; OrderBy = position }
                        let ranked = mkCore
                                        (["Id"; "Value"; "__set_branch"; "__set_ordinal"] |> List.map (fun name -> { Alias = Some name; Expr = col name })
                                         |> fun fields -> fields @ [{ Alias = Some "__set_rank"; Expr = rank }])
                                        (Some(DerivedTable(combined, "set_union"))) |> wrapCore
                        let resultColumn name = SqlExpr.Column(Some "set_result", name)
                        { mkCore [{ Alias = Some "Id"; Expr = resultColumn "Id" }; { Alias = Some "Value"; Expr = resultColumn "Value" }]
                                 (Some(DerivedTable(ranked, "set_result"))) with
                            Where = Some(SqlExpr.Binary(resultColumn "__set_rank", BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                            OrderBy = ["__set_branch"; "__set_ordinal"] |> List.map (fun name -> { Expr = resultColumn name; Direction = SortDirection.Asc }) }
                        |> wrapCore)

                // --- Type filtering / casting over polymorphic payloads ---
                | SupportedLinqMethods.Cast ->
                    // Cast<TTarget>() with polymorphic guard & diagnostic messages.
                    if m.Expressions.Length <> 0 then raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    match UtilsReflection.GenericMethodArgCache.Get m.OriginalMethod |> Array.tryHead with
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
                                // Value: typed-payload JSON object when type missing/mismatched, else preserve Value.
                                // Paired with idExpr=NULL: JsonFunctions detects Id=NULL, parses payload kind, raises InvalidCastException.
                                let valueExpr = SqlExpr.CaseExpr(
                                    (typeIsNull, runtimeErrorPayload RuntimeErrorKind.CastError "The type of item is not stored in the database, if you want to include it, then add the Polymorphic attribute to the type and reinsert all elements."),
                                    [(typeMismatch, runtimeErrorPayload RuntimeErrorKind.CastError "Unable to cast object to the specified type, because the types are different.")],
                                    Some(SqlExpr.Column(None, "Value")))
                                let projs = [{ Alias = Some "Id"; Expr = idExpr }; { Alias = Some "Value"; Expr = valueExpr }]
                                let core = mkCore projs (Some (DerivedTable(ctx.Inner, "o")))
                                wrapCore core
                            )

                | SupportedLinqMethods.OfType ->
                    if m.Expressions.Length <> 0 then raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    match UtilsReflection.GenericMethodArgCache.Get m.OriginalMethod |> Array.tryHead with
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
                    raise (NotSupportedException(
                        "Error: Aggregate is not supported.\n" +
                        "Reason: LINQ Aggregate (seed/accumulator fold) has no direct SQL translation. SoloDB supports specific aggregates (Sum, Min, Max, Average, Count) natively.\n" +
                        "Fix: Use .Sum(), .Min(), .Max(), .Average(), or .Count() instead, or call .AsEnumerable() before .Aggregate()."))

                | _ -> ()
