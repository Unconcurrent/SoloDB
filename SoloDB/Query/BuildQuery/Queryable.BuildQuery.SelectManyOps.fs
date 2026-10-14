namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Threading
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

module internal QueryableBuildQuerySelectManyOps =
    open QueryableHelperState
    open QueryableHelperJoin
    open QueryableHelperJoinBindings
    open QueryableHelperPreprocess
    open QueryableLayerBuild
    open QueryableHelperBase

    let internal applySelectMany<'T>
        (sourceCtx: QueryContext)
        (tableName: string)
        (statements: ResizeArray<SQLSubquery>)
        (translateQueryFn: QueryContext -> Dictionary<string, obj> -> Expression -> TranslatedQuery)
        (expressions: Expression array)
        (originalMethod: MethodInfo) =
                    let elementType = ModelMetadata.SequenceElement originalMethod.ReturnType
                    match tryMatchCanonicalLeftJoinComposite expressions with
                    | Some composite ->
                        if composite.ResultSelector.Parameters.Count <> 2 then
                            raise (NotSupportedException(
                                "Error: GroupJoin SelectMany result selector is not supported.
Reason: The canonical left-join composite requires a two-parameter result selector.
Fix: Use SelectMany with the standard (carrier, inner) result selector or move the query after AsEnumerable()."))

                        if isCompositeJoinKeyBody composite.OuterKeySelector.Body || isCompositeJoinKeyBody composite.InnerKeySelector.Body then
                            raise (NotSupportedException(
                                "Error: Left-join composite key selectors are not supported.
Reason: Anonymous-type and composite key equality lowering is not supported here.
Fix: Join on a single scalar key or move the query after AsEnumerable()."))

                        let prepareInner = prepareInnerJoinSource "Left-join" sourceCtx translateQueryFn composite.InnerExpression

                        let outerResultParam = composite.ResultSelector.Parameters.[0]
                        let innerResultParam = composite.ResultSelector.Parameters.[1]
                        let rewriter = LeftJoinCompositeResultRewriter(outerResultParam, composite.CarrierOuterMember, composite.CarrierGroupMember, composite.OuterKeySelector.Parameters.[0])
                        let rewrittenResultBody = rewriter.Visit(composite.ResultSelector.Body)

                        if rewriter.TouchesGroup || rewriter.TouchesCarrier then
                            raise (NotSupportedException(
                                "Error: Left-join result selector is not supported.
Reason: Only projections over the outer row and the matched inner row are supported; grouped-sequence and carrier-shaped projections are not supported here.
Fix: Project scalar members from the outer row and the SupportedLinqMethods.DefaultIfEmpty inner row, or move the query after AsEnumerable()."))

                        addComplexFinal statements (fun ctx ->
                            let outerAlias = "o"
                            let innerAlias = "j"
                            let outerCtx = { sourceCtx with LayerPosition = OuterLayer; SerializationInput = ctx.Input; Joins = ResizeArray() }
                            let struct(innerCtx, innerInput) = prepareInner ctx.Vars
                            let outerKeyExpr =
                                translateJoinSingleSourceExpression outerCtx outerAlias ctx.Vars (Some composite.OuterKeySelector.Parameters.[0]) composite.OuterKeySelector.Body
                            let struct(innerQuery, keyName) =
                                publishInnerJoinKey innerCtx innerInput innerAlias (fun context alias ->
                                    translateJoinSingleSourceProduced context alias ctx.Vars (Some composite.InnerKeySelector.Parameters.[0]) composite.InnerKeySelector.Body)
                            let joinCondition = SqlExpr.Binary(outerKeyExpr, BinaryOperator.Eq, SqlExpr.Column(Some innerAlias, keyName))

                            let outerKeyJoins = QueryTranslatorReferenceJoin.preparedJoinClauses outerCtx.Joins
                            let outerCtx = { outerCtx with Joins = ResizeArray() }
                            let result =
                                translateJoinResultSelectorProduced
                                    outerCtx
                                    innerCtx
                                    ctx.Vars
                                    outerAlias
                                    innerAlias
                                    composite.OuterKeySelector.Parameters.[0]
                                    innerResultParam
                                    rewrittenResultBody ValueNone

                            let core =
                                { mkCore
                                    [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some outerAlias, "Id") }
                                     { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }]
                                    (Some (DerivedTable(ctx.Inner, outerAlias)))
                                  with
                                      Joins =
                                          outerKeyJoins
                                          @ [ConditionedJoin(
                                              JoinKind.Left,
                                              DerivedTable(innerQuery, innerAlias),
                                              joinCondition)]
                                          @ QueryTranslatorReferenceJoin.preparedJoinClauses outerCtx.Joins
                                          @ QueryTranslatorReferenceJoin.preparedJoinClauses innerCtx.Joins }
                            let struct(query, input) = QueryProduced.publishCore sourceCtx.NextAlias core result false
                            { Query = query; SerializationInput = input; KeyInput = ValueNone }
                        )
                    | None ->
                        // Check for DBRefMany SelectMany using JOIN-based flattening.
                        let tryMatchDBRefManySelectMany () =
                            match expressions.Length with
                            | 1 | 2 ->
                                match expressions.[0] with
                                | :? UnaryExpression as ue when (ue.Operand :? LambdaExpression) ->
                                    let lambda = ue.Operand :?> LambdaExpression
                                    match QueryTranslatorBaseHelpers.normalizeStoredConstruction lambda.Body with
                                    | :? MemberExpression as me when DBRefTypeHelpers.isManyReferenceType me.Type ->
                                        // Restrict this path to direct owner-parameter member access.
                                        // Reject member chains (o.Ref.Value.Tags) and non-parameter bases.
                                        match me.Expression with
                                        | :? ParameterExpression as pe when
                                            lambda.Parameters.Count = 1 &&
                                            System.Object.ReferenceEquals(pe, lambda.Parameters.[0]) ->
                                            Some (lambda, me, expressions.Length)
                                        | _ -> None
                                    | _ -> None
                                | _ -> None
                            | _ -> None

                        match tryMatchDBRefManySelectMany () with
                        | Some (selectorLambda, memberExpr, argCount) ->
                            addComplexFinal statements (fun ctx ->
                                let ownerAlias = sourceCtx.NextAlias()
                                let ownerCtx = { sourceCtx.CloneForSubquery() with LayerPosition = OuterLayer; SerializationInput = ctx.Input }
                                let builder = QueryBuilder.New (StringBuilder()) ctx.Vars false ownerAlias selectorLambda -1 (ValueSome ownerCtx)
                                let ownerRef = QueryTranslatorVisitDbRefPeelers.tryGetDBRefManyOwnerRef builder memberExpr
                                               |> ValueOption.defaultWith (fun () -> invalidOp "A relation source requires its physical owner input.")
                                let struct(core, target) = DBRefManyBuilderCore.buildFlattenedRelationSource builder ownerRef (DerivedTable(ctx.Inner, ownerAlias))
                                let result =
                                    if argCount = 1 then target
                                    else
                                        let resultLambda = unwrapLambdaExpressionOrThrow "SelectMany result selector" expressions[1]
                                        if resultLambda.Parameters.Count <> 2 then
                                            raise(NotSupportedException("Error: SelectMany result selector must have exactly 2 parameters (owner, target).\nFix: Use (o, t) => new { ... } pattern."))
                                        let targetCtx = { ownerCtx with SerializationInput = target.Input; Joins = ResizeArray() }
                                        let owner = QueryTranslatorVisitCore.visitProduced selectorLambda.Parameters[0] builder
                                        translateJoinResultSelectorProduced ownerCtx targetCtx ctx.Vars ownerAlias ownerAlias
                                            resultLambda.Parameters[0] resultLambda.Parameters[1] resultLambda.Body (ValueSome(owner, target))
                                let edges = QueryTranslatorReferenceJoin.preparedJoinClauses ownerCtx.Joins
                                let core = { core with Joins = edges @ core.Joins }
                                let struct(query, input) = QueryProduced.publishCore sourceCtx.NextAlias core result false
                                { Query = query; SerializationInput = input; KeyInput = ValueNone })
                        | None ->
                            // Fallback: json_each SelectMany.
                            addComplexFinal statements (fun ctx ->
                                match expressions.Length with
                                | 1 ->
                                    let generics = TypeMetadata.GenericMethodArgCache.Get originalMethod
                                    if typeof<byte>.Equals(generics.[1]) then
                                        raise (InvalidOperationException "Cannot use SelectMany() on byte arrays, as they are stored as base64 strings in SQLite. To process the array anyway, first exit the SQLite context with .AsEnumerable().")
                                    // Use a stable inner source alias based on the inner select structure
                                    let innerSourceName = Utils.getVarName (Interlocked.Increment(sourceCtx.AliasCounter))
                                    let selectorCtx = { sourceCtx.CloneForSubquery() with LayerPosition = OuterLayer; SerializationInput = ctx.Input }
                                    // Build the json_each join source expression
                                    let collection =
                                        match expressions.[0] with
                                        | :? UnaryExpression as ue when (ue.Operand :? LambdaExpression) ->
                                            let lambda = ue.Operand :?> LambdaExpression
                                            let body = QueryTranslatorBaseHelpers.normalizeStoredConstruction lambda.Body
                                            let memberAccess =
                                                match body with
                                                | :? MemberExpression -> true
                                                | :? BoundValueAccessExpression as access -> not(String.IsNullOrEmpty access.MemberName)
                                                | _ -> false
                                            match body with
                                            | body when memberAccess ->
                                                // Reject DBRefMany-typed members here; they must go through
                                                // the link-table JOIN path, not json_each on embedded JSON.
                                                if DBRefTypeHelpers.isManyReferenceType body.Type || DBRefTypeHelpers.isSingleReferenceType body.Type then
                                                    raise (NotSupportedException(
                                                        "Error: SelectMany on a relation-backed property requires direct owner-parameter access.\nReason: Non-direct member access (projected alias, computed selector) is not supported.\nFix: Use .SelectMany(o => o.RelationProperty) directly on the owner collection."))
                                                QueryTranslator.translateProduced selectorCtx innerSourceName lambda ctx.Vars
                                            | :? ParameterExpression ->
                                                QueryTranslator.translateProduced selectorCtx innerSourceName lambda ctx.Vars
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
                                    let relationJoins = QueryTranslatorReferenceJoin.preparedJoinClauses selectorCtx.Joins
                                    let resultType = TypeMetadata.GenericMethodArgCache.Get(originalMethod).[1]
                                    let collectionAlias = sourceCtx.NextAlias()
                                    let collectionSource, collectionInput =
                                        match collection with
                                        | DirectValue(value, input) -> DerivedTable(ctx.Inner, innerSourceName), (value, input)
                                        | PackedValue _ ->
                                            let collectionCore = mkCore [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some innerSourceName, "Id") }
                                                                         { Alias = Some "Value"; Expr = SqlExpr.Literal SqlLiteral.Null }]
                                                                        (Some(DerivedTable(ctx.Inner, innerSourceName)))
                                            let collectionCore = { collectionCore with Joins = relationJoins }
                                            let struct(query, input) = QueryProduced.publishCore sourceCtx.NextAlias collectionCore collection false
                                            DerivedTable(query, collectionAlias), (SqlExpr.Column(Some collectionAlias, "Value"), QueryTranslatorSerialization.scopeSerializationInput (Some collectionAlias) OuterLayer input)
                                    let jsonEachExpr, collectionInput = collectionInput
                                    let elementAlias = sourceCtx.NextAlias()
                                    let fullkey = SqlExpr.Column(Some elementAlias, "fullkey")
                                    let token = SqlExpr.FunctionCall("jsonb", [SqlExpr.Binary(jsonEachExpr, BinaryOperator.JsonText, fullkey)])
                                    let produced = QueryTranslatorSerialization.elementInput collectionInput (SqlExpr.Column(Some elementAlias, "key")) token
                                    let produced = { produced with Carrier = if isSqlScalarResult resultType then SqlScalar else CanonicalToken }
                                    let value = QueryProduced.scalarValue resultType token { produced with Carrier = CanonicalToken }
                                    let packed = match collection with PackedValue _ -> true | _ -> false
                                    let ownerAlias = if packed then collectionAlias else innerSourceName
                                    let core =
                                        { mkCore
                                            [{ Alias = Some "Id"; Expr = SqlExpr.Column(Some ownerAlias, "Id") }
                                             { Alias = Some "Value"; Expr = value }]
                                            (Some collectionSource)
                                          with Joins = (if packed then [] else relationJoins) @ [CrossJoin(FromJsonEach(jsonEachExpr, Some elementAlias))] }
                                    let struct(query, input) = QueryProduced.publishCore sourceCtx.NextAlias core (DirectValue(value, produced)) false
                                    { Query = query; SerializationInput = input; KeyInput = ValueNone }
                                | other -> raise (NotSupportedException(sprintf "Invalid number of arguments in %s: %A" originalMethod.Name other))
                            )
                    ()
