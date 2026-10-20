namespace SoloDatabase

open System
open SoloDatabase.Metadata.Generation
open System.Collections
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq.Expressions
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open JsonFunctions
open Utils
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.QueryTranslatorBaseHelpers
open SoloDatabase.QueryTranslatorExpressionAdmission
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.SqlModel
open SoloDatabase.JsonSerializator.JsonEqualitySupport
open SoloDatabase.QueryTranslatorSerialization
open SoloDatabase.QuerySerializationSql

module internal QueryTranslatorVisitCore =
    type private ValueUse = Value | Comparison | Stored | Presence | PresenceComparison

    let rec private emitStringOperandDu (qb: QueryBuilder) (ignoreCase: bool) (expr: Expression) : SqlExpr =
        if ignoreCase then SqlExpr.FunctionCall("TO_LOWER", [visitDu expr qb])
        else visitDu expr qb

    and private castToDu (qb: QueryBuilder) (castToType: Type) (o: Expression) : QueryProducedValue =
        QueryTranslatorVisitCoreCastAggregate.castToDu visitProduced qb castToType o false

    and private visitMathMethodDu (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr voption =
        QueryTranslatorVisitCoreCastAggregate.visitMathMethodDu visitDu visitComparisonDu m qb

    and private arrayIndexDu (array: Expression) (index: Expression) (resultType: Type) (qb: QueryBuilder) : QueryProducedValue =
        match index with
        | :? ConstantExpression as ce when ce.Type.IsPrimitive ->
            visitAccessDu array (StoredLabel(box(Convert.ToInt32 ce.Value))) resultType qb
        | _ -> raise (NotSupportedException("The index of the array must always be a constant value."))

    /// Member paths and root consumption use the same original parameter binding.
    and private resolveAliasForParameter (paramExpr: ParameterExpression) (qb: QueryBuilder) : string option =
        (qb.ResolveParameterSource paramExpr).Alias

    and private propertyProduced (o: Expression) (property: obj) (m: Expression) (qb: QueryBuilder) (receiver: QueryProducedValue) =
        match property with
        | :? string as property when property = "Id" && isRootParameter o && (typeof<int64>.Equals(m.Type) || typeof<int32>.Equals(m.Type)) ->
            if not qb.UpdateMode && receiver.Input.PhysicalOwner.IsSome then
                QueryTranslatorVisitCoreMemberAccess.accessValue (QueryDefault.value qb.Variables) qb.SourceContext.NextAlias false (StoredLabel(box property)) m.Type receiver
            else QueryProduced.typed m.Type (SqlExpr.Column(resolveAliasForParameter (unwrapConvert o :?> ParameterExpression) qb, "Id"))
        | :? string as property -> QueryTranslatorVisitCoreMemberAccess.accessValue (QueryDefault.value qb.Variables) qb.SourceContext.NextAlias qb.UpdateMode (StoredLabel(box property)) m.Type receiver
        | :? int as index -> QueryTranslatorVisitCoreMemberAccess.accessValue (QueryDefault.value qb.Variables) qb.SourceContext.NextAlias qb.UpdateMode (StoredLabel(box index)) m.Type receiver
        | _ -> raise(NotSupportedException("Unable to translate property access."))

    and private visitPropertyDu o property m qb = propertyProduced o property m qb (visitProduced o qb)

    and private visitPreparedPropertyDu o (property: Expression) normalize (m: Expression) (qb: QueryBuilder) =
        let prepared =
            match qb.SourceContext.BindQueryValue with
            | ValueSome binder -> binder.PrepareConstant property m.Type
            | ValueNone -> PreparedConstant(ExpressionEvaluation.evaluateExpr<obj> property)
        let receiver = visitProduced o qb
        let produce value = propertyProduced o (normalize value) m qb receiver
        match prepared with PreparedConstant value -> produce value | DeferredConstant complete -> complete produce

    and private visitParameterDu valueUse (m: ParameterExpression) (qb: QueryBuilder) : QueryProducedValue =
        let source = qb.ResolveParameterSource m
        match source.Value with
        | ProducedParameter produced -> produced
        | DeferredParameter produced -> produced.Value
        | IdentityParameter -> QueryProduced.typed m.Type (SqlExpr.Column(source.Alias, "Id"))
        | SourceParameter ->
            let value =
                if qb.UpdateMode then SqlExpr.Literal(SqlLiteral.String "$")
                elif qb.JsonExtractSelfValue then
                    if valueUse = Presence || valueUse = PresenceComparison then rootInput source.Position source.Alias "Value"
                    else rootValue m.Type source.Position source.Alias "Value"
                else SqlExpr.Column(source.Alias, "Value")
            let input =
                let original = source.SerializationInput
                let original = if isNull original.OriginalType then QuerySerializationInput.ForType m.Type else original
                scopeSerializationInput source.Alias source.Position original
            let carrier =
                if qb.UpdateMode then SqlScalar
                elif not qb.JsonExtractSelfValue then input.Carrier
                elif source.Position = OuterLayer then input.Carrier
                elif valueUse = Presence || valueUse = PresenceComparison || isSqlScalarResult m.Type then SqlScalar else CanonicalToken
            DirectValue(value, { input with Carrier = carrier })

    and private visitAccessDu parent access resultType qb =
        QueryTranslatorVisitCoreMemberAccess.access visitProduced parent access resultType qb

    and private visitMemberAccessDu valueUse memberAccess qb =
        QueryTranslatorVisitCoreMemberAccess.memberAccess visitProduced visitDu visitPresenceDu (visitValueDu valueUse) memberAccess qb

    and private containsImplDu qb array value =
        QueryTranslatorVisitCoreArrayPredicates.contains visitProduced visitComparisonDu qb array value

    and private visitNestedArrayPredicateDu qb array predicate isAll =
        QueryTranslatorVisitCoreArrayPredicates.nested visitProduced visitDu qb array predicate isAll

    and private visitBinaryDu (b: BinaryExpression) (qb: QueryBuilder) : SqlExpr =
        QueryTranslatorVisitCoreBinary.visitBinaryDu visitProduced visitDu visitComparisonDu visitStoredValueDu visitPresenceDu (visitValueCoreDu PresenceComparison) b qb

    and private visitMethodCallDu valueUse (m: MethodCallExpression) (qb: QueryBuilder) : QueryProducedValue =
        if isIntrinsicBox m then visitValueDu valueUse m.Arguments[0] qb
        else QueryTranslatorVisitCoreMethodCall.visitMethodCallDu visitProduced visitTextProduced (visitValueDu valueUse) visitComparisonProduced visitDu visitStoredValueDu visitPresenceDu visitMathMethodDu arrayIndexDu visitPropertyDu visitPreparedPropertyDu containsImplDu visitNestedArrayPredicateDu castToDu membersProduced emitStringOperandDu m qb

    /// Scalar consumers project the value from the canonical paired lowerer.
    and internal visitDu (exp: Expression) (qb: QueryBuilder) : SqlExpr =
        visitProduced exp qb |> QueryProduced.value qb.SourceContext.NextAlias

    and internal visitProduced (exp: Expression) (qb: QueryBuilder) : QueryProducedValue =
        visitValueDu Value exp qb

    and private visitPresenceDu (exp: Expression) (qb: QueryBuilder) : SqlExpr =
        visitValueCoreDu Presence exp qb |> QueryProduced.presence qb.SourceContext.NextAlias

    and internal visitComparisonProduced (exp: Expression) (qb: QueryBuilder) : QueryProducedValue =
        visitValueDu Comparison exp qb

    and private visitTextProduced (exp: Expression) (qb: QueryBuilder) =
        let original =
            match exp with
            | :? MethodCallExpression as boxing when isIntrinsicBox boxing -> boxing.Arguments[0]
            | :? UnaryExpression as boxing when boxing.NodeType = ExpressionType.Convert && typeof<obj>.Equals boxing.Type -> boxing.Operand
            | _ -> exp
        if typeof<char>.Equals original.Type then visitComparisonProduced original qb else visitProduced exp qb

    and private visitComparisonDu (exp: Expression) (qb: QueryBuilder) : SqlExpr =
        visitComparisonProduced exp qb
        |> QueryProduced.consume qb.SourceContext.NextAlias (QueryProduced.comparisonValue exp.Type)

    and private visitStoredValueDu (destination: QuerySerializationInput) (exp: Expression) (qb: QueryBuilder) : SqlExpr =
        let rec withoutBoxing (value: Expression) =
            match value with
            | :? MethodCallExpression as boxing when isIntrinsicBox boxing && boxing.Arguments[0].Type.IsValueType ->
                withoutBoxing boxing.Arguments[0]
            | :? UnaryExpression as conversion when conversion.NodeType=ExpressionType.Convert &&
                                                    typeof<obj>.Equals(conversion.Type) && conversion.Operand.Type.IsValueType ->
                withoutBoxing conversion.Operand
            | _ -> value
        let exp=withoutBoxing exp
        let operation = JsonSerializator.JsonValue.WriterOperation(destination.OriginalType,true)
        let dynamicTarget =
            (operation = Metadata.CodecGeneration.RawRootOperation.Dynamic ||
             operation = Metadata.CodecGeneration.RawRootOperation.Abstract) &&
            JsonSerializator.JsonValue.WriterOperation(exp.Type,true) <> Metadata.CodecGeneration.RawRootOperation.Tree
        let targetPayload = Utils.optionalPayloadType destination.OriginalType
        let storedNumericTarget = (TypeSupport.StoredNumeric targetPayload.FullName).IsSome
        let valueUse = if dynamicTarget then Value else Stored
        let produced =
            match unwrapConvert exp with
            // Bare entity assignments retain their original update root path.
            | :? ParameterExpression -> visitValueDu valueUse exp qb
            | _ -> visitValueDu valueUse exp { qb with UpdateMode = false }
        QueryProduced.consume qb.SourceContext.NextAlias (fun value input ->
            if not dynamicTarget then
                let sourcePayload = Utils.optionalPayloadType input.OriginalType
                let sourceNumeric = if sourcePayload.IsEnum then TypeMetadata.enumUnderlying sourcePayload else sourcePayload
                let restoreExact =
                    storedNumericTarget && not(targetPayload.Equals sourcePayload) &&
                    ((TypeSupport.NumericCore sourceNumeric.FullName).IsSome ||
                     (let operation = JsonSerializator.JsonValue.WriterOperation(input.OriginalType,true)
                      operation = Metadata.CodecGeneration.RawRootOperation.Dynamic ||
                      operation = Metadata.CodecGeneration.RawRootOperation.Abstract))
                if restoreExact then
                    if typeof<char>.Equals sourcePayload then
                        QueryTranslatorNumeric.convertNativeJson targetPayload (jsonToken(QueryTranslatorCharacters.comparable exp.Type value input))
                        |> ValueOption.defaultWith (fun () -> invalidOp "The declared numerical writer requires a numerical target.")
                    else
                        let source = input.Source |> ValueOption.defaultWith (fun () -> jsonToken value)
                        QueryTranslatorNumeric.convertWriter targetPayload source (QuerySerializationSql.writerDescriptor input)
                        |> ValueOption.defaultWith (fun () -> invalidOp "The declared numerical writer requires a numerical target.")
                else QueryTranslatorCharacters.comparable exp.Type value input
            else
                match QueryTranslatorNumeric.domain input.OriginalType with
                | ValueSome domain -> SqlExpr.FunctionCall("jsonb", [QueryTranslatorNumeric.call domain "TEXT" [value]])
                | ValueNone ->
                    let sourceOperation = JsonSerializator.JsonValue.WriterOperation(input.OriginalType,true)
                    if sourceOperation = Metadata.CodecGeneration.RawRootOperation.Dynamic ||
                       sourceOperation = Metadata.CodecGeneration.RawRootOperation.Abstract then
                        let source = input.Source |> ValueOption.defaultWith (fun () ->
                            if input.Carrier = CanonicalToken then value else jsonToken value)
                        QueryTranslatorNumeric.nativeJsonOutput source (QuerySerializationSql.writerDescriptor input)
                    else QueryTranslatorCharacters.comparable exp.Type value input) produced

    // Comparison mode belongs to this operand only. Recursive expression visits
    // use visitDu, preserving the released kinds inside arithmetic and functions.
    and private visitValueDu valueUse (exp: Expression) (qb: QueryBuilder) : QueryProducedValue =
        let produced = visitValueCoreDu valueUse exp qb
        if qb.UpdateMode || valueUse = Presence || valueUse = PresenceComparison then produced
        else
            let produced =
                if produced.Input.Carrier = CanonicalToken && isSqlScalarResult exp.Type then
                    QueryProduced.bind qb.SourceContext.NextAlias (fun value input ->
                        DirectValue(QueryProduced.scalarValue exp.Type value input, { input with Carrier = SqlScalar })) produced
                else produced
            QueryProduced.map qb.SourceContext.NextAlias (fun value ->
                let value = QueryProduced.scalarValue exp.Type value produced.Input
                if valueUse = Stored && QueryTranslatorNumeric.unsigned64 exp.Type && not(isFullyConstant exp) then
                    QueryTranslatorNumeric.storedUnsigned exp.Type value
                else value) produced

    and private membersProduced target names expressions qb =
        QueryTranslatorVisitCoreConstruction.members visitProduced target names expressions qb

    and private tupleProduced expression qb =
        QueryTranslatorVisitCoreConstruction.tuple visitProduced expression qb

    and private visitValueCoreDu valueUse (exp: Expression) (qb: QueryBuilder) : QueryProducedValue =
        qb.StepTranslation()
        let exp = unwrapStructCopy exp
        match qb.ProducedHandler |> ValueOption.bind (fun handler -> handler exp) with
        | ValueSome produced -> produced
        | ValueNone -> visitUnhandledCoreDu valueUse exp qb

    and private visitUnhandledCoreDu valueUse (exp: Expression) (qb: QueryBuilder) : QueryProducedValue =
        // Pre-expression handlers (DBRef etc.) — return DU directly via DuHandlerResult
        qb.DuHandlerResult.Value <- ValueNone
        if runHandler preExpressionHandler qb exp then
            match qb.DuHandlerResult.Value with
            | ValueSome duResult -> duResult
            | ValueNone -> raise (InvalidOperationException "Pre-expression handler returned true but did not set DuHandlerResult")
        else

        match qb.SourceContext.BindQueryValue |> ValueOption.bind (fun bind -> bind.TryBind ((valueUse = Comparison || valueUse = PresenceComparison) || valueUse = Stored && typeof<char>.Equals(Utils.optionalPayloadType exp.Type)) exp) with
        | ValueSome value -> value
        | ValueNone ->

        // Fully-constant early-out
        if isConstantParameter exp then
            let value = ExpressionEvaluation.evaluateExpr<obj> exp
            let struct(value, writerType) = StoredValueParameter.allocateProduced ((valueUse = Comparison || valueUse = PresenceComparison) || valueUse = Stored && typeof<char>.Equals(Utils.optionalPayloadType exp.Type)) qb.Variables exp.Type value
            QueryProduced.typed writerType value
        else

        match exp.NodeType with
        | ExpressionType.Add when
            let binary = exp :?> BinaryExpression
            typeof<string>.Equals binary.Left.Type || typeof<string>.Equals binary.Right.Type ->
            QueryTranslatorVisitCoreStringFormatting.binaryProduced visitTextProduced (exp :?> BinaryExpression) qb
        | ExpressionType.And | ExpressionType.AndAlso | ExpressionType.Or | ExpressionType.OrElse
        | ExpressionType.Equal | ExpressionType.NotEqual | ExpressionType.LessThan | ExpressionType.LessThanOrEqual
        | ExpressionType.GreaterThan | ExpressionType.GreaterThanOrEqual
        | ExpressionType.Add | ExpressionType.Subtract | ExpressionType.Multiply | ExpressionType.Divide | ExpressionType.Modulo
        | ExpressionType.AddChecked | ExpressionType.SubtractChecked | ExpressionType.MultiplyChecked
        | ExpressionType.Assign ->
            visitBinaryDu (exp :?> BinaryExpression) qb |> QueryProduced.typed exp.Type
        | ExpressionType.Coalesce ->
            let b = exp :?> BinaryExpression
            let left = visitValueDu valueUse b.Left qb
            let right = visitValueDu valueUse b.Right qb
            let coalesce = if valueUse = Presence || valueUse = PresenceComparison then QueryProduced.coalesceRaw else QueryProduced.coalesce
            coalesce qb.SourceContext.NextAlias exp.Type left right
        | ExpressionType.Not ->
            // SARGABLE form: NOT(expr) → (expr = 0) for boolean operands.
            // LINQ only generates NOT on boolean expressions, so this is safe.
            // Enables index utilization on boolean fields.
            let operand = visitDu (exp :?> UnaryExpression).Operand qb
            SqlExpr.Binary(operand, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L)) |> QueryProduced.typed exp.Type
        | ExpressionType.Negate | ExpressionType.NegateChecked -> QueryTranslatorNumeric.negate exp.Type (visitDu (exp :?> UnaryExpression).Operand qb) |> QueryProduced.typed exp.Type
        | ExpressionType.Lambda -> visitValueDu valueUse (exp :?> LambdaExpression).Body qb
        | ExpressionType.Call -> visitMethodCallDu valueUse (exp :?> MethodCallExpression) qb
        | ExpressionType.Constant ->
            let c = exp :?> ConstantExpression
            (match c.Value with null -> SqlExpr.Literal(SqlLiteral.Null) | _ -> qb.AllocateParamExpr(c.Value)) |> QueryProduced.typed c.Type
        | ExpressionType.MemberAccess -> visitMemberAccessDu valueUse (exp :?> MemberExpression |> MemberAccess.From) qb
        | ExpressionType.Extension when (match exp with :? BoundValueAccessExpression as access -> not(isNull access.MemberName) | _ -> false) ->
            let access = exp :?> BoundValueAccessExpression
            visitMemberAccessDu valueUse { Expression = access.Parent; MemberName = access.MemberName; InputType = access.Parent.Type;
                                  ReturnType = access.Type; OriginalExpression = None } qb
        | ExpressionType.Extension when (match exp with :? BoundValueCallExpression as call -> isTuple call.Type | _ -> false) ->
            tupleProduced exp qb
        | ExpressionType.Extension when (match exp with :? BoundValueCallExpression as call -> not(isNull call.ProjectionNames) | _ -> false) ->
            let call = exp :?> BoundValueCallExpression
            membersProduced call.Type call.ProjectionNames call.Inputs qb
        | ExpressionType.Convert | ExpressionType.ConvertChecked | ExpressionType.TypeAs ->
            let m = exp :?> UnaryExpression
            if typeof<obj>.Equals(m.Type) then
                visitValueDu valueUse m.Operand qb
            elif isReferenceUpcast m then visitValueDu valueUse m.Operand qb
            else QueryTranslatorVisitCoreCastAggregate.castToDu visitProduced qb m.Type m.Operand (m.NodeType = ExpressionType.ConvertChecked)
        | ExpressionType.NewArrayInit ->
            let array = exp :?> NewArrayExpression
            let expressions = array.Expressions |> Seq.toArray
            let components = expressions |> Array.map (fun item -> visitProduced item qb)
            QueryProduced.construct qb.SourceContext.NextAlias (fun components ->
                let components = components |> Array.map QueryProduced.embedded
                DirectValue(SqlExpr.JsonArrayExpr(components |> Array.map fst |> Array.toList),
                            QuerySerializationInput.Positions(array.Type, components |> Array.map snd))) components
        | ExpressionType.New ->
            let m = exp :?> NewExpression
            match DateTimeFunctions.translateDateTimeLikeConstructor m (fun arg -> visitDu arg qb) with
            | Some translated -> QueryProduced.typed m.Type translated
            | None when isTuple m.Type -> tupleProduced m qb
            | None ->
                match QueryConstructionExpressions.memberNames m with
                | ValueSome fields -> membersProduced m.Type fields (m.Arguments |> Seq.toArray) qb
                | _ -> raise (NotSupportedException(sprintf "Cannot construct new in SQL query %A" m.Type))
        | ExpressionType.Parameter -> visitParameterDu valueUse (exp :?> ParameterExpression) qb
        | ExpressionType.ArrayIndex ->
            match exp with
            | :? ParameterExpression as pe -> visitParameterDu valueUse pe qb
            | :? BinaryExpression as be -> arrayIndexDu be.Left be.Right be.Type qb
            | other -> raise (NotSupportedException(sprintf "Unknown array index expression of: %A" other))
        | ExpressionType.Index ->
            let indexExp = exp :?> IndexExpression
            if indexExp.Arguments.Count <> 1 then raise (NotSupportedException("The SQL translator does not support multiple args indexes."))
            let arge = indexExp.Arguments.[0]
            if not (isFullyConstant arge) then raise (NotSupportedException("The SQL translator does not support non constant index arg."))
            if typeof<string>.Equals(arge.Type) then
                let prepared =
                    match qb.SourceContext.BindQueryValue with
                    | ValueSome binder -> binder.PrepareConstant arge indexExp.Type
                    | ValueNone -> PreparedConstant(ExpressionEvaluation.evaluateExpr<obj> arge)
                let receiver = visitProduced indexExp.Object qb
                let produce argument = propertyProduced indexExp.Object argument indexExp qb receiver
                match prepared with
                | PreparedConstant value -> produce value
                | DeferredConstant complete -> complete produce
            else arrayIndexDu indexExp.Object arge indexExp.Type qb
        | ExpressionType.Quote -> visitValueDu valueUse (exp :?> UnaryExpression).Operand qb
        | ExpressionType.Invoke ->
            let invocation = exp :?> InvocationExpression
            let body = normalizeStoredConstruction invocation
            match body with
            | _ when not(obj.ReferenceEquals(body, invocation)) -> visitValueDu valueUse body qb
            | _ when invocation.CanReduce ->
                let body = invocation.Reduce()
                visitValueDu valueUse body qb
            | _ when isFullyConstant exp -> qb.AllocateParamExpr(ExpressionEvaluation.evaluateExpr<obj> exp) |> QueryProduced.typed exp.Type
            | _ -> raise (NotSupportedException("Error: Invoke expression is not reducible.\nReason: The invoked expression cannot be inlined or treated as a constant for SQL translation.\nFix: Inline the lambda or move the invocation after AsEnumerable()."))
        | ExpressionType.Block ->
            let block = exp :?> BlockExpression
            match QueryConstructionExpressions.orderedMembers block with
            | ValueSome(struct(target, names, expressions, slots)) ->
                QueryTranslatorVisitCoreConstruction.orderedMembers visitProduced target names expressions slots qb
            | ValueNone ->
                // Quotation pattern matching can separately bind a stored receiver.
                match tryStoredLocal block with
                | Some body -> visitValueDu valueUse body qb
                | None -> raise(NotSupportedException("Only local bindings of stored values can be translated to SQL. Move executable bindings after AsEnumerable()."))
        | ExpressionType.Conditional ->
            let c = exp :?> ConditionalExpression
            let condition = visitDu c.Test qb
            let whenTrue = visitValueDu valueUse c.IfTrue qb
            let whenFalse = visitValueDu valueUse c.IfFalse qb
            let choose = if valueUse = Presence || valueUse = PresenceComparison then QueryProduced.chooseRaw else QueryProduced.choose
            choose c.Type condition whenTrue whenFalse
        | ExpressionType.TypeIs ->
            let typeIsExp = exp :?> TypeBinaryExpression
            if not (TypeMetadata.requiresTypeInformation typeIsExp.Expression.Type) then
                raise (NotSupportedException(sprintf "Cannot translate TypeIs expression, because the DB will not store its type information for %A" typeIsExp.Type))
            let typeExpr = SqlExpr.FunctionCall("jsonb_extract", [visitDu typeIsExp.Expression qb; SqlExpr.Literal(SqlLiteral.String "$.$type")])
            match typeIsExp.TypeOperand |> typeToName with
            | None -> raise (NotSupportedException(sprintf "Cannot translate TypeIs expression with the TypeOperand: %A" typeIsExp.TypeOperand))
            | Some typeName -> SqlExpr.Binary(typeExpr, BinaryOperator.Eq, qb.AllocateParamExpr typeName) |> QueryProduced.typed exp.Type
        | ExpressionType.MemberInit ->
            let m = exp :?> MemberInitExpression
            let bindings = m.Bindings |> Seq.cast<MemberAssignment> |> Seq.toArray
            membersProduced m.Type (bindings |> Array.map (fun binding -> binding.Member.Name))
                (bindings |> Array.map _.Expression) qb
        | ExpressionType.ListInit ->
            let listExp = exp :?> ListInitExpression
            let components = listExp.Initializers |> Seq.map (fun item -> visitProduced (item.Arguments |> Seq.exactlyOne) qb) |> Seq.toArray
            QueryProduced.construct qb.SourceContext.NextAlias (fun components ->
                let components = components |> Array.map QueryProduced.embedded
                let elementType = ModelMetadata.SequenceElement listExp.Type
                let input = { QuerySerializationInput.Positions(elementType.MakeArrayType(), components |> Array.map snd) with OriginalType = listExp.Type }
                DirectValue(SqlExpr.FunctionCall("jsonb_array", components |> Array.map fst |> Array.toList), input)) components
        // Unknown handler fallback — returns DU via DuHandlerResult
        | _ ->
            qb.DuHandlerResult.Value <- ValueNone
            if not (runHandler unknownExpressionHandler qb exp) then
                raise (ArgumentOutOfRangeException $"QueryTranslator.{nameof unknownExpressionHandler} did not handle the expression of type: {exp.NodeType}")
            match qb.DuHandlerResult.Value with
            | ValueSome duResult -> duResult
            | ValueNone -> raise (InvalidOperationException $"Unknown expression handler returned true but did not set DuHandlerResult for: {exp.NodeType}")

    // ─── DBRef relation query translation ────────────────────────────────────────
