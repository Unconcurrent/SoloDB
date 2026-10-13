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
open SoloDatabase.QueryableGroupByAliases
open SoloDatabase.SqlModel

module internal QueryableBuildQuerySetAndTypeOps =
    open QueryableHelperState
    open QueryableHelperJoin
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
                | SupportedLinqMethods.All ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 -> addFilter statements (negatePredicateForAllExpression m.Expressions.[0])
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addTake statements (TypeMetadata.ExpressionHelper.constant 1)

                    addComplexFinal statements (fun ctx ->
                        // SELECT -1 As Id, NOT EXISTS(SELECT 1 FROM (inner)) as Value
                        let existsSubquery = wrapCore (mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some (DerivedTable(ctx.Inner, "o"))))
                        let projs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = SqlExpr.Unary(UnaryOperator.Not, SqlExpr.Exists(existsSubquery)) }]
                        { Query = wrapCore (mkCore projs None); SerializationInput = (QueryProduced.typed typeof<bool> projs[1].Expr).Input; KeyInput = ValueNone }
                    )

                | SupportedLinqMethods.Any ->
                    match m.Expressions.Length with
                    | 0 -> ()
                    | 1 -> addFilter statements m.Expressions.[0]
                    | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name other))

                    addTake statements (TypeMetadata.ExpressionHelper.constant 1)

                    addComplexFinal statements (fun ctx ->
                        let existsSubquery = wrapCore (mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some (DerivedTable(ctx.Inner, "o"))))
                        let projs = [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = SqlExpr.Exists(existsSubquery) }]
                        { Query = wrapCore (mkCore projs None); SerializationInput = (QueryProduced.typed typeof<bool> projs[1].Expr).Input; KeyInput = ValueNone }
                    )


                | SupportedLinqMethods.Contains ->
                    if m.Expressions.Length <> 1 then
                        raise (NotSupportedException("Custom query comparers cannot be translated to SQL. Use the default comparer overload or call AsEnumerable() first."))
                    let value = m.Expressions[0]
                    let elementType = m.OriginalMethod.GetGenericArguments()[0]
                    addComplexFinal statements (fun ctx ->
                        let alias = "contains_input"
                        let current = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = ctx.Input }
                        let receiver = QueryTranslator.translateComparisonProduced current alias (Expression.Parameter(elementType)) ctx.Vars
                        let detached = QueryTranslatorBaseHelpers.isFullyConstant value ||
                                       (sourceCtx.BindQueryValue |> ValueOption.exists (fun binder -> binder.IsValue value))
                        let comparison = QueryDefaultEquality.contains sourceCtx.NextAlias elementType RootValueMembership detached receiver
                                             (fun () -> QueryTranslator.translateComparisonProduced current alias value ctx.Vars)
                                             (fun () -> invalidOp "Root membership does not infer a detached entity source.")
                                             (fun left -> QueryTranslator.translateValueMembership current alias elementType left value ctx.Vars)
                        let rows = { mkCore [{ Alias = None; Expr = SqlExpr.Literal(SqlLiteral.Integer 1L) }] (Some(DerivedTable(ctx.Inner, alias))) with
                                        Where = Some comparison; Limit = Some(SqlExpr.Literal(SqlLiteral.Integer 1L)) }
                        let result = QueryProduced.typed typeof<bool> (SqlExpr.Exists(wrapCore rows))
                        { Query = wrapCore(mkCore [{ Alias = Some "Id"; Expr = SqlExpr.Literal(SqlLiteral.Integer -1L) }; { Alias = Some "Value"; Expr = QueryProduced.value sourceCtx.NextAlias result }] None)
                          SerializationInput = result.Input; KeyInput = ValueNone })

                // todo: Append works at root level but is not yet supported at DBRefMany level (see Extract.fs)
                | SupportedLinqMethods.Append ->
                    let elementType = TypeMetadata.GenericMethodArgCache.Get(m.OriginalMethod).[0]
                    let readId value =
                        if QueryTranslatorBaseHelpers.hasPhysicalRowIdentity elementType then
                            (EntityIdentity.compiledInt64IdReader elementType).Invoke value
                        else -1L
                    addComplexFinal statements (fun ctx ->
                        let appended =
                            match sourceCtx.BindQueryValue with
                            | ValueSome binder when binder.IsValue m.Expressions[0] ->
                                let value = binder.Local m.Expressions[0]
                                let json = binder.Scalar(BoundValueCallExpression([|value|], typeof<string>,
                                                Func<obj array,obj>(fun inputs -> box (serializeForCollection elementType inputs[0]))))
                                let id = binder.Scalar(BoundValueCallExpression([|value|], typeof<int64>,
                                                Func<obj array,obj>(fun inputs -> box (readId inputs[0]))))
                                id, SqlExpr.FunctionCall("jsonb", [json])
                            | _ ->
                                let value = ExpressionEvaluation.evaluateExpr<obj> m.Expressions[0]
                                let json = serializeForCollection elementType value
                                SqlExpr.Literal(SqlLiteral.Integer(readId value)), SqlExpr.FunctionCall("jsonb", [SqlExpr.Literal(SqlLiteral.String json)])
                        let id, token = appended
                        let value = jsonTokenValue elementType token
                        let input = { QuerySerializationInput.ForType elementType with Source = ValueSome token; Carrier = (if isSqlScalarResult elementType then SqlScalar else CanonicalToken) }
                        let name = "_input" + sourceCtx.NextAlias()
                        let leftInput = QueryTranslatorSerialization.scopeSerializationInput (Some "o") OuterLayer ctx.Input
                        let head = mkCore [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some "o", "Id") }
                                           { Alias = Some "Value"; Expr = SqlExpr.Column(Some "o", "Value") }
                                           { Alias = Some name; Expr = QuerySerializationSql.inputDescriptor leftInput }]
                                          (Some(DerivedTable(ctx.Inner, "o")))
                        let tail = mkCore [{ Alias = Some "Id"; Expr = id }; { Alias = Some "Value"; Expr = value }
                                           { Alias = Some name; Expr = QuerySerializationSql.inputDescriptor input }] None
                        let output = QueryTranslatorSerialization.fromDescriptor (SqlExpr.Column(None, name)) (QueryTranslatorSerialization.mergeInputs ctx.Input.OriginalType [|ctx.Input; input|])
                        { Query = { Ctes = []; Body = UnionAllSelect(head, [tail]) }; SerializationInput = output; KeyInput = ValueNone })

                | SupportedLinqMethods.Concat
                | SupportedLinqMethods.Union ->
                    OrderedChainPlan.validateDefaultComparerArity m.OriginalMethod.Name (m.Expressions.Length + 1)
                    addComplexFinal statements (fun ctx ->
                        let rhs = readSoloDBQueryable<'T> m.Expressions[0]
                        let completed = translateQueryFn sourceCtx ctx.Vars rhs
                        let name = "_input" + sourceCtx.NextAlias()
                        let arm query input alias =
                            let input = QueryTranslatorSerialization.scopeSerializationInput (Some alias) OuterLayer input
                            mkCore [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some alias, "Id") }
                                    { Alias = Some "Value"; Expr = SqlExpr.Column(Some alias, "Value") }
                                    { Alias = Some name; Expr = QuerySerializationSql.inputDescriptor input }]
                                   (Some(DerivedTable(query, alias)))
                        let output = QueryTranslatorSerialization.fromDescriptor (SqlExpr.Column(None, name)) (QueryTranslatorSerialization.mergeInputs ctx.Input.OriginalType [|ctx.Input; completed.SerializationInput|])
                        { Query = { Ctes = []; Body = UnionAllSelect(arm ctx.Inner ctx.Input "set_left", [arm completed.Query completed.SerializationInput "set_right"]) }
                          SerializationInput = output; KeyInput = ValueNone })

                    if m.Value = SupportedLinqMethods.Union then
                        QueryableBuildQueryDistinct.apply sourceCtx statements {| m with Expressions = Array.empty |}

                | SupportedLinqMethods.Except
                | SupportedLinqMethods.Intersect
                | SupportedLinqMethods.ExceptBy
                | SupportedLinqMethods.IntersectBy ->
                    QueryableBuildQuerySetMembership.apply<'T> sourceCtx statements translateQueryFn m

                | SupportedLinqMethods.UnionBy ->
                    if m.Expressions.Length <> 2 then
                        raise (NotSupportedException("Custom set comparers cannot be translated to SQL. Use the default comparer overload or call AsEnumerable() first."))
                    addComplexFinal statements (fun ctx ->
                        let name = "_input" + sourceCtx.NextAlias()
                        let rhs = readSoloDBQueryable<'T> m.Expressions.[0]
                        let completed = translateQueryFn sourceCtx ctx.Vars rhs
                        let rhsSelect = completed.Query
                        let elementType = m.OriginalMethod.GetGenericArguments()[0]
                        let keyType = m.OriginalMethod.GetGenericArguments()[1]
                        let keyDescriptorName = "_key_input" + sourceCtx.NextAlias()
                        let arm query fact alias branch =
                            let col name = SqlExpr.Column(Some alias, name)
                            let scoped = QueryTranslatorSerialization.scopeSerializationInput (Some alias) OuterLayer fact
                            let value =
                                if QueryTranslatorBaseTypes.isPrimitiveSQLiteType elementType then nullValue elementType (rootValue elementType OuterLayer (Some alias) "Value")
                                else col "Value"
                            let core = mkCore
                                           [{ Alias = Some "Id"; Expr = QueryDefaultEquality.outputIdentity elementType scoped (col "Id") }
                                            { Alias = Some "Value"; Expr = value }
                                            { Alias = Some "__set_branch"; Expr = SqlExpr.Literal(SqlLiteral.Integer branch) }
                                            { Alias = Some name; Expr = QuerySerializationSql.inputDescriptor scoped }
                                            { Alias = Some "__set_key"; Expr = SqlExpr.Literal SqlLiteral.Null }]
                                           (Some(DerivedTable(query, alias)))
                                       |> QueryProduced.withPosition "__set_ordinal"
                            let struct(prepared, keyInput) = QueryTranslator.publishSelector
                                                                 { sourceCtx with SerializationInput = fact }
                                                                 alias m.Expressions[1] ctx.Vars "__set_key" core
                            let publishedAlias = "key_" + alias
                            let col name = SqlExpr.Column(Some publishedAlias, name)
                            let keyInput = QueryTranslatorSerialization.scopeSerializationInput (Some publishedAlias) OuterLayer keyInput
                            let fields = ["Id"; "Value"; "__set_branch"; "__set_ordinal"; name; "__set_key"]
                                         |> List.map (fun name -> { Alias = Some name; Expr = col name })
                            mkCore (fields @ [{ Alias = Some keyDescriptorName; Expr = QuerySerializationSql.inputDescriptor keyInput }])
                                   (Some(DerivedTable(prepared, publishedAlias))), keyInput
                        let left, leftKeyInput = arm ctx.Inner ctx.Input "set_left" 0L
                        let right, rightKeyInput = arm rhsSelect completed.SerializationInput "set_right" 1L
                        let prepared = { Ctes = []; Body = UnionAllSelect(left, [right]) }
                        let col name = SqlExpr.Column(Some "set_keyed", name)
                        let keyInput = QueryTranslatorSerialization.fromDescriptor (col keyDescriptorName)
                                           (QueryTranslatorSerialization.mergeInputs keyType [|leftKeyInput; rightKeyInput|])
                        let key = QueryDefaultEquality.key sourceCtx.NextAlias keyType (col "__set_key") keyInput
                        let position = [col "__set_branch", SortDirection.Asc; col "__set_ordinal", SortDirection.Asc]
                        let rank = QueryableBuildQueryWindowHelpers.rowNumberExpr (QueryDefaultEquality.terms key) position
                        let ranked = mkCore
                                        (["Id"; "Value"; "__set_branch"; "__set_ordinal"; name] |> List.map (fun name -> { Alias = Some name; Expr = col name })
                                         |> fun fields -> fields @ [{ Alias = Some "__set_rank"; Expr = rank }])
                                        (Some(DerivedTable(prepared, "set_keyed"))) |> wrapCore
                        let resultColumn name = SqlExpr.Column(Some "set_result", name)
                        { mkCore [{ Alias = Some "Id"; Expr = resultColumn "Id" }; { Alias = Some "Value"; Expr = resultColumn "Value" }; { Alias = Some name; Expr = resultColumn name }]
                                 (Some(DerivedTable(ranked, "set_result"))) with
                            Where = Some(SqlExpr.Binary(resultColumn "__set_rank", BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 1L)))
                            OrderBy = ["__set_branch"; "__set_ordinal"] |> List.map (fun name -> { Expr = resultColumn name; Direction = SortDirection.Asc }) }
                        |> fun core ->
                            { Query = wrapCore core
                              SerializationInput = QueryTranslatorSerialization.fromDescriptor (SqlExpr.Column(None, name)) (QueryTranslatorSerialization.mergeInputs ctx.Input.OriginalType [|ctx.Input; completed.SerializationInput|])
                              KeyInput = ValueNone })

                // --- Type filtering / casting over polymorphic payloads ---
                | SupportedLinqMethods.Cast ->
                    // Cast<TTarget>() with polymorphic guard & diagnostic messages.
                    if m.Expressions.Length <> 0 then raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    match TypeMetadata.GenericMethodArgCache.Get m.OriginalMethod |> Array.tryHead with
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
                                            @ QueryTranslatorSerialization.companionProjections (Some "o") ctx.Input
                                let core = mkCore projs (Some (DerivedTable(ctx.Inner, "o")))
                                { Query = wrapCore core; SerializationInput = ctx.Input; KeyInput = ValueNone }
                            )

                | SupportedLinqMethods.OfType ->
                    if m.Expressions.Length <> 0 then raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" m.OriginalMethod.Name m.Expressions.Length))
                    match TypeMetadata.GenericMethodArgCache.Get m.OriginalMethod |> Array.tryHead with
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
                                        ([{ Alias = None; Expr = SqlExpr.Column(None, "Id") }
                                          { Alias = None; Expr = SqlExpr.Column(None, "Value") }]
                                         @ QueryTranslatorSerialization.companionProjections (Some "o") ctx.Input)
                                        (Some (DerivedTable(ctx.Inner, "o")))
                                      with Where = Some (SqlExpr.Binary(typeExtract, BinaryOperator.Eq, typeParam)) }
                                { Query = wrapCore core; SerializationInput = ctx.Input; KeyInput = ValueNone }
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
        ()
