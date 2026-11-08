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
open SoloDatabase.QueryTranslatorBase
open SoloDatabase.SqlModel

module internal QueryTranslatorVisitCoreMethodCall =
    let private rowCharacter (qb: QueryBuilder) (argument: Expression) =
        typeof<char>.Equals(argument.Type) &&
        (match qb.SourceContext.BindQueryValue with
         | ValueSome binder -> not(binder.IsValue argument)
         | _ -> not(isFullyConstant argument))

    let private visitScalarMethodCallDu (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) (visitDu: Expression -> QueryBuilder -> SqlExpr) (visitStoredValueDu: QuerySerializationInput -> Expression -> QueryBuilder -> SqlExpr) (visitPresenceDu: Expression -> QueryBuilder -> SqlExpr) (visitMathMethodDu: MethodCallExpression -> QueryBuilder -> SqlExpr voption) (containsImplDu: QueryBuilder -> Expression -> Expression -> SqlExpr) (visitNestedArrayPredicateDu: QueryBuilder -> Expression -> Expression -> bool -> SqlExpr) (emitStringOperandDu: QueryBuilder -> bool -> Expression -> SqlExpr) (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr =
        match visitMathMethodDu m qb with
        | ValueSome result -> result
        | ValueNone ->
        let nullableOperation =
            if m.Arguments.Count = 2 then
                QueryTranslatorNumericMethods.nullableDateTimeOperation m.Method m.Arguments[0].Type m.Arguments[1].Type
            else ValueNone
        match nullableOperation with
        | ValueSome operation -> visitDu (Expression.MakeBinary(operation, m.Arguments[0], m.Arguments[1], false, m.Method)) qb
        | ValueNone ->
        match QueryTranslatorCollectionMutation.tryCall visitProduced visitDu visitStoredValueDu m qb with
        | ValueSome mutation -> mutation
        | ValueNone ->
        match m with
        | _ when QuerySearchAuxiliary.isMethod m.Method -> QuerySearchAuxiliary.translate visitProduced visitDu m qb
        | _ when m.Method.DeclaringType = typeof<SearchSql> ->
            if m.Method.Name = "Identity" then SqlExpr.Column(qb.SourceAlias,"Id") else
            if m.Method.Name = "Rank" then
                QueryProduced.consume qb.SourceContext.NextAlias (fun value input ->
                    match input.SearchOwner |> ValueOption.bind _.Direct with
                    | ValueSome direct -> direct.Rank
                    | ValueNone ->
                        let score = QueryTranslatorVisitCoreMemberAccess.accessValue (QueryDefault.value qb.Variables) qb.SourceContext.NextAlias false
                                        (ClrMember("Score",typeof<double>)) typeof<double> (DirectValue(value,input))
                        QueryProduced.consume qb.SourceContext.NextAlias (fun value input ->
                            SqlExpr.Unary(UnaryOperator.Neg,QueryProduced.scalarValue typeof<double> value input)) score) (visitProduced m.Arguments.[0] qb)
            else
            let index = ExpressionEvaluation.evaluateExpr<string> m.Arguments.[1]
            if m.Method.Name = "Score" then SqlExpr.Unary(UnaryOperator.Neg,SqlExpr.Column(Some index,"rank"))
            else invalidOp "Unknown search SQL operation."
        | _ when QueryTranslatorCharacters.indexArguments m.Method ->
            let source, index = if m.Method.IsStatic then m.Arguments[0], m.Arguments[1] else m.Object, m.Arguments[0]
            let sourceValue = visitDu source qb
            let position = visitDu index qb
            QueryTranslatorCharacters.index index sourceValue position
        | _ when (QueryTranslatorCharacters.stringOperation m.Method).IsSome && (m.Arguments |> Seq.exists (rowCharacter qb)) ->
            let operation = (QueryTranslatorCharacters.stringOperation m.Method).Value
            let ignoreCase =
                operation <> QueryTranslatorCharacters.Operation.Replace && m.Arguments.Count = 2 &&
                isIgnoreCase(ExpressionEvaluation.evaluateExpr<StringComparison> m.Arguments[1])
            let source = visitDu m.Object qb
            let source = if ignoreCase then SqlExpr.FunctionCall("TO_LOWER", [source]) else source
            let first = visitDu m.Arguments[0] qb
            let second =
                if operation = QueryTranslatorCharacters.Operation.Replace then visitDu m.Arguments[1] qb
                else SqlExpr.Literal(SqlLiteral.Integer(if ignoreCase then 1L else 0L))
            QueryTranslatorCharacters.operation operation source first second
        | _ when m.Method.Name = "Equals" && not (isNull m.Object)
                 && DBRefTypeHelpers.isFileRefType m.Object.Type && m.Arguments.Count = 1
                 && (unwrapConvert m.Arguments.[0]).Type.Equals(m.Object.Type) ->
            SqlExpr.Binary(fileSystemReferenceIdentity visitDu qb m.Object, BinaryOperator.Eq,
                           fileSystemReferenceIdentity visitDu qb m.Arguments.[0])
        | OfShape0 null null "ToLowerInvariant" value | OfShape0 null null "ToLower" value ->
            SqlExpr.FunctionCall("TO_LOWER", [visitDu value qb])
        | OfShape0 null null "ToUpperInvariant" value | OfShape0 null null "ToUpper" value ->
            SqlExpr.FunctionCall("TO_UPPER", [visitDu value qb])
        | OfShape1 null null "Like" null (str, likeWhat) ->
            SqlExpr.Binary(visitDu str qb, BinaryOperator.Like, visitDu likeWhat qb)
        | OfShape1 null null "Any" null (array, whereFuncExpr) ->
            visitNestedArrayPredicateDu qb array whereFuncExpr false
        | OfShape1 null null "All" null (array, whereFuncExpr) ->
            visitNestedArrayPredicateDu qb array whereFuncExpr true
        | OfShape2 null OfString "Contains" null OfStringComparison (text, what, comparisonExpr) ->
            let ic = isIgnoreCase (ExpressionEvaluation.evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(SqlExpr.FunctionCall("INSTR", [emitStringOperandDu qb ic text; emitStringOperandDu qb ic what]), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
        | OfShape1 null OfString "Contains" null (text, what) ->
            SqlExpr.Binary(SqlExpr.FunctionCall("INSTR", [visitDu text qb; visitDu what qb]), BinaryOperator.Gt, SqlExpr.Literal(SqlLiteral.Integer 0L))
        | OfShape1 null OfIEnum "Contains" null (array, value) -> containsImplDu qb array value
        | OfShape1 bool OfValueType "Contains" null (ros, value) when ros.Type.Name = "ReadOnlySpan`1" -> containsImplDu qb ros value
        | OfShape0 null null "GetType" o when o.NodeType = ExpressionType.Parameter ->
            SqlExpr.FunctionCall("jsonb_extract", [visitDu o qb; SqlExpr.Literal(SqlLiteral.String "$.$type")])
        | OfShape0 (OfType) null "TypeOf" _ ->
            let t = m.Method.Invoke(null, Array.empty) :?> Type
            qb.AllocateParamExpr(match t |> typeToName with Some x -> x | None -> "")
        | OfShape2 null OfString "StartsWith" null OfStringComparison (arg, v, comparisonExpr) ->
            let ic = isIgnoreCase (ExpressionEvaluation.evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(
                SqlExpr.FunctionCall("SUBSTR", [emitStringOperandDu qb ic arg; SqlExpr.Literal(SqlLiteral.Integer 1L); SqlExpr.FunctionCall("LENGTH", [visitDu v qb])]),
                BinaryOperator.Eq, emitStringOperandDu qb ic v)
        | OfShape1 null OfString "StartsWith" null (arg, v) ->
            match qb.SourceContext.BindQueryValue with
            | ValueSome binder when binder.IsValue v && (typeof<string>.Equals(v.Type) || typeof<char>.Equals(v.Type)) ->
                let local = binder.Local v
                let prefix = binder.Prefix local
                let upper = visitDu (BoundValueAccessExpression(local, typeof<string>, QueryPrefixBounds.Reader)) qb
                let value = visitDu arg qb
                let range = SqlExpr.Binary(SqlExpr.Binary(value, BinaryOperator.Ge, prefix), BinaryOperator.And,
                                           SqlExpr.Binary(value, BinaryOperator.Lt, upper))
                if qb.SourceContext.GeneralPrefixMatch then
                    // Empty prefixes preserve the existing IS NOT NULL behavior,
                    // including documents whose stored value is not a JSON string.
                    let empty = SqlExpr.Unary(UnaryOperator.IsNotNull, value)
                    let isEmpty =
                        match binder.PrepareChoice with
                        | ValueSome prepare -> prepare local (fun sampled -> Object.Equals(sampled, ""))
                        | ValueNone -> SqlExpr.Binary(prefix, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String ""))
                    SqlExpr.CaseExpr((isEmpty, empty), [], Some range)
                else range
            | _ ->
            if isFullyConstant v then
                let prefix = if typeof<char>.Equals(v.Type) then (string << ExpressionEvaluation.evaluateExpr<char>) v else ExpressionEvaluation.evaluateExpr<string> v
                if prefix.Length = 0 then SqlExpr.Unary(UnaryOperator.IsNotNull, visitDu arg qb)
                else
                    SqlExpr.Binary(
                        SqlExpr.Binary(visitDu arg qb, BinaryOperator.Ge, qb.AllocateParamExpr prefix),
                        BinaryOperator.And,
                        SqlExpr.Binary(visitDu arg qb, BinaryOperator.Lt, qb.AllocateParamExpr(QueryPrefixBounds.Next prefix)))
            else
                SqlExpr.Binary(
                    SqlExpr.FunctionCall("SUBSTR", [visitDu arg qb; SqlExpr.Literal(SqlLiteral.Integer 1L); SqlExpr.FunctionCall("LENGTH", [visitDu v qb])]),
                    BinaryOperator.Eq, visitDu v qb)
        | OfShape2 null OfString "EndsWith" null OfStringComparison (arg, v, comparisonExpr) ->
            let ic = isIgnoreCase (ExpressionEvaluation.evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(
                SqlExpr.FunctionCall("SUBSTR", [emitStringOperandDu qb ic arg; SqlExpr.Unary(UnaryOperator.Neg, SqlExpr.FunctionCall("LENGTH", [visitDu v qb]))]),
                BinaryOperator.Eq, emitStringOperandDu qb ic v)
        | OfShape1 null OfString "EndsWith" null (arg, v) ->
            SqlExpr.Binary(
                SqlExpr.FunctionCall("SUBSTR", [visitDu arg qb; SqlExpr.Unary(UnaryOperator.Neg, SqlExpr.FunctionCall("LENGTH", [visitDu v qb]))]),
                BinaryOperator.Eq, visitDu v qb)
        | OfShape2 null OfString "Equals" null OfStringComparison (arg, v, comparisonExpr) ->
            let ic = isIgnoreCase (ExpressionEvaluation.evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(emitStringOperandDu qb ic arg, BinaryOperator.Eq, emitStringOperandDu qb ic v)
        | OfShape2 null null "Equals" OfString OfStringComparison (arg1, arg2, comparisonExpr) when typeof<string>.Equals(m.Method.DeclaringType) && m.Method.IsStatic ->
            let ic = isIgnoreCase (ExpressionEvaluation.evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(emitStringOperandDu qb ic arg1, BinaryOperator.Eq, emitStringOperandDu qb ic arg2)
        | OfShape2 null null "Compare" OfString OfStringComparison (arg1, arg2, comparisonExpr) when typeof<string>.Equals(m.Method.DeclaringType) && m.Method.IsStatic ->
            let ic = isIgnoreCase (ExpressionEvaluation.evaluateExpr<StringComparison> comparisonExpr)
            let a = emitStringOperandDu qb ic arg1
            let b = emitStringOperandDu qb ic arg2
            SqlExpr.CaseExpr((SqlExpr.Binary(a, BinaryOperator.Lt, b), SqlExpr.Literal(SqlLiteral.Integer -1L)), [(SqlExpr.Binary(a, BinaryOperator.Gt, b), SqlExpr.Literal(SqlLiteral.Integer 1L))], Some(SqlExpr.Literal(SqlLiteral.Integer 0L)))
        | OfShape2 null OfString "IndexOf" null OfStringComparison (arg, v, comparisonExpr) ->
            let ic = isIgnoreCase (ExpressionEvaluation.evaluateExpr<StringComparison> comparisonExpr)
            SqlExpr.Binary(SqlExpr.FunctionCall("INSTR", [emitStringOperandDu qb ic arg; emitStringOperandDu qb ic v]), BinaryOperator.Sub, SqlExpr.Literal(SqlLiteral.Integer 1L))
        | _ when m.Method.IsStatic && m.Method.Name = "Parse" && m.Arguments.Count = 1 &&
                 typeof<string>.Equals m.Arguments[0].Type &&
                 (DateTimeSql.isDateTimeLikeType m.Method.DeclaringType || typeof<decimal>.Equals m.Method.DeclaringType) ->
            SqlExpr.FunctionCall("TEMPORAL_PARSE",[SqlExpr.Literal(SqlLiteral.Integer(DateTimeSql.scalarKind m.Method.DeclaringType));visitDu m.Arguments[0] qb])
        | OfShape0 null null "Parse" arg when
            typeof<SByte>.Equals(m.Method.DeclaringType) || typeof<Byte>.Equals(m.Method.DeclaringType) || typeof<Int16>.Equals(m.Method.DeclaringType) || typeof<UInt16>.Equals(m.Method.DeclaringType) ||
            typeof<Int32>.Equals(m.Method.DeclaringType) || typeof<UInt32>.Equals(m.Method.DeclaringType) || typeof<Int64>.Equals(m.Method.DeclaringType) || typeof<UInt64>.Equals(m.Method.DeclaringType) ||
            typeof<Single>.Equals(m.Method.DeclaringType) || typeof<Double>.Equals(m.Method.DeclaringType) || typeof<float>.Equals(m.Method.DeclaringType) || typeof<float32>.Equals(m.Method.DeclaringType) ->
            let targetType = match m.Method.DeclaringType with
                             | t when typeof<SByte>.Equals(t) || typeof<Byte>.Equals(t) || typeof<Int16>.Equals(t) || typeof<UInt16>.Equals(t) || typeof<Int32>.Equals(t) || typeof<UInt32>.Equals(t) || typeof<Int64>.Equals(t) || typeof<UInt64>.Equals(t) -> "INTEGER"
                             | t when typeof<Single>.Equals(t) || typeof<Double>.Equals(t) -> "REAL"
                             | _ -> raise (NotSupportedException(sprintf "Unsupported Parse type: %A" m.Method.DeclaringType))
            SqlExpr.Cast(visitDu arg qb, targetType)
        | OfShape2 null OfString "Substring" null null (str, start, length) ->
            SqlExpr.FunctionCall("SUBSTR", [visitDu str qb; SqlExpr.Binary(visitDu start qb, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L)); visitDu length qb])
        | OfShape1 null OfString "Substring" null (str, start) ->
            SqlExpr.FunctionCall("SUBSTR", [visitDu str qb; SqlExpr.Binary(visitDu start qb, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L))])
        | OfShape1 null null "GetString" null (str, index) ->
            SqlExpr.FunctionCall("SUBSTR", [visitDu str qb; SqlExpr.Binary(visitDu index qb, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L)); SqlExpr.Literal(SqlLiteral.Integer 1L)])
        | OfShape0 null null "Count" _
        | OfShape0 null null "LongCount" _ when let t = m.Arguments.[0].Type in t.IsConstructedGenericType && typedefof<System.Linq.IGrouping<_,_>>.Equals(t.GetGenericTypeDefinition()) ->
            let alias = qb.SourceAlias
            SqlExpr.FunctionCall("json_array_length", [SqlExpr.Column(alias, "Value"); SqlExpr.Literal(SqlLiteral.String "$.Items")])
        | OfShape2 null OfString "Replace" null null (str, oldValue, newValue) ->
            SqlExpr.FunctionCall("REPLACE", [visitDu str qb; visitDu oldValue qb; visitDu newValue qb])
        | OfShape0 null OfString "IsNullOrEmpty" str ->
            SqlExpr.Binary(SqlExpr.Unary(UnaryOperator.IsNull, visitDu str qb), BinaryOperator.Or, SqlExpr.Binary(visitDu str qb, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String "")))
        | OfShape1 null OfString "IsMatch" (OfString) (input, pattern) when typeof<System.Text.RegularExpressions.Regex>.Equals(m.Method.DeclaringType) ->
            SqlExpr.Binary(visitDu input qb, BinaryOperator.Regexp, visitDu pattern qb)
        | _ when m.Method.Name = "ToString" && not (isNull m.Object) && m.Object.Type.IsEnum &&
                 not m.Method.IsStatic && m.Arguments.Count = 0 && typeof<string>.Equals m.Method.ReturnType ->
            TypeMetadata.registerType m.Object.Type
            visitProduced m.Object qb |> QueryProduced.consume qb.SourceContext.NextAlias (fun value input ->
                let token = input.Source |> ValueOption.defaultWith (fun () -> QueryProduced.treeToken qb.SourceContext.NextAlias value input)
                SqlExpr.FunctionCall("ENUM_TO_STRING", [qb.AllocateParamExpr(TypeMetadataCache.RegisterExactName m.Object.Type); SqlExpr.FunctionCall("json", [token])]))
        | _ when m.Method.Name = "ToString" && not (isNull m.Object)
                 && DateTimeSql.supportsTemporalToStringTranslationType m.Object.Type ->
            let format () =
                if m.Arguments.Count = 0 then SqlExpr.Literal SqlLiteral.Null
                elif m.Arguments.Count >= 1 && typeof<string>.Equals m.Arguments[0].Type then visitDu m.Arguments[0] qb
                else raise(NotSupportedException "This temporal formatting overload needs its actual format-provider operation.")
            let constructed =
                match m.Object with
                | :? NewExpression as created -> DateTimeConstruction.tryTranslateDateTimeOffsetFormat created format qb (fun value -> visitDu value qb)
                | :? MethodCallExpression as parsed when DateTimeSql.isOffsetParse parsed ->
                    Some(SqlExpr.FunctionCall("DTO_PARSE_FORMAT",[visitDu parsed.Arguments[0] qb;format()]))
                | _ -> None
            let scalarFormat =
                if typeof<TimeSpan>.Equals m.Object.Type then true
                elif typeof<DateOnly>.Equals m.Object.Type || typeof<TimeOnly>.Equals m.Object.Type then
                    if m.Arguments.Count = 0 then true else
                    match m.Arguments[0] with
                    | :? ConstantExpression as value when typeof<string>.Equals value.Type ->
                        let text = value.Value :?> string
                        String.IsNullOrEmpty text || text = "o" || text = "O" || text = "r" || text = "R"
                    | _ -> false
                else false
            match constructed with
            | Some formatted -> formatted
            | None when not scalarFormat &&
                        (m.Arguments.Count = 0 || m.Arguments[0] :? ConstantExpression) ->
                DateTimeFormatting.translateDateTimeToStringCall
                    (visitDu m.Object qb) m.Object m.Arguments.Count (if m.Arguments.Count = 0 then None else Some m.Arguments[0])
            | None ->
                let receiver = visitDu m.Object qb
                SqlExpr.FunctionCall("TEMPORAL_FORMAT",[SqlExpr.Literal(SqlLiteral.Integer(DateTimeSql.scalarKind m.Object.Type));receiver;format();SqlExpr.Literal SqlLiteral.Null])
        | _ when m.Method.Name = "ToUnixTimeMilliseconds" && not (isNull m.Object) && typeof<DateTimeOffset>.Equals(m.Object.Type) ->
            visitDu m.Object qb
        | _ ->
            raise (NotSupportedException(
                sprintf "Error: Method '%s' is not supported.\nReason: The method has no SQL translation.\nFix: Rewrite the query or call AsEnumerable() before using this method." m.Method.Name))

    let rec internal visitMethodCallDu
        (visitProduced: Expression -> QueryBuilder -> QueryProducedValue)
        (visitTextProduced: Expression -> QueryBuilder -> QueryProducedValue)
        (visitRequested: Expression -> QueryBuilder -> QueryProducedValue)
        (visitComparisonProduced: Expression -> QueryBuilder -> QueryProducedValue)
        (visitDu: Expression -> QueryBuilder -> SqlExpr)
        (visitStoredValueDu: QuerySerializationInput -> Expression -> QueryBuilder -> SqlExpr)
        (visitPresenceDu: Expression -> QueryBuilder -> SqlExpr)
        (visitMathMethodDu: MethodCallExpression -> QueryBuilder -> SqlExpr voption)
        (arrayIndexDu: Expression -> Expression -> Type -> QueryBuilder -> QueryProducedValue)
        (visitPropertyDu: Expression -> obj -> Expression -> QueryBuilder -> QueryProducedValue)
        (visitPreparedPropertyDu: Expression -> Expression -> (obj -> obj) -> Expression -> QueryBuilder -> QueryProducedValue)
        (containsImplDu: QueryBuilder -> Expression -> Expression -> SqlExpr)
        (visitNestedArrayPredicateDu: QueryBuilder -> Expression -> Expression -> bool -> SqlExpr)
        (castToDu: QueryBuilder -> Type -> Expression -> QueryProducedValue)
        (membersProduced: Type -> string array -> Expression array -> QueryBuilder -> QueryProducedValue)
        (emitStringOperandDu: QueryBuilder -> bool -> Expression -> SqlExpr)
        (m: MethodCallExpression) (qb: QueryBuilder) : QueryProducedValue =
        match QueryStructuralRelations.tryMinMax visitProduced m qb with
        | ValueSome result -> result
        | ValueNone ->
            let characterOperation =
                (QueryTranslatorCharacters.stringOperation m.Method).IsSome && (m.Arguments |> Seq.exists (rowCharacter qb))
            let visitOperation (expression: Expression) (builder: QueryBuilder) =
                if characterOperation && typeof<char>.Equals expression.Type then visitComparisonProduced expression builder
                else visitProduced expression builder
            let operationOperand expression builder = visitOperation expression builder |> QueryProduced.value builder.SourceContext.NextAlias
            let render operand stringOperand call =
                visitScalarMethodCallDu visitProduced operand visitStoredValueDu visitPresenceDu visitMathMethodDu containsImplDu visitNestedArrayPredicateDu stringOperand call qb
            match QueryTranslatorVisitCorePreparedCall.tryProduced visitOperation visitComparisonProduced m qb render with
            | Some result -> result
            | None -> visitFormattedMethodCallDu visitProduced visitTextProduced visitRequested (if characterOperation then operationOperand else visitDu) visitStoredValueDu visitPresenceDu visitMathMethodDu arrayIndexDu visitPropertyDu visitPreparedPropertyDu containsImplDu visitNestedArrayPredicateDu castToDu membersProduced emitStringOperandDu m qb
    and private visitFormattedMethodCallDu visitProduced visitTextProduced visitRequested visitDu visitStoredValueDu visitPresenceDu visitMathMethodDu arrayIndexDu visitPropertyDu visitPreparedPropertyDu containsImplDu visitNestedArrayPredicateDu castToDu membersProduced emitStringOperandDu (m: MethodCallExpression) (qb: QueryBuilder) =
        match QueryTranslatorVisitCoreStringFormatting.tryProduced visitTextProduced m qb with
        | ValueSome result -> result
        | ValueNone -> visitOtherMethodCallDu visitProduced visitRequested visitDu visitStoredValueDu visitPresenceDu visitMathMethodDu arrayIndexDu visitPropertyDu visitPreparedPropertyDu containsImplDu visitNestedArrayPredicateDu castToDu membersProduced emitStringOperandDu m qb

    and private visitOtherMethodCallDu visitProduced visitRequested visitDu visitStoredValueDu visitPresenceDu visitMathMethodDu arrayIndexDu visitPropertyDu visitPreparedPropertyDu containsImplDu visitNestedArrayPredicateDu castToDu membersProduced emitStringOperandDu (m: MethodCallExpression) (qb: QueryBuilder) =
        match QueryTranslatorIndex.tryCall visitProduced visitDu visitPropertyDu visitPreparedPropertyDu m qb with
        | ValueSome indexed -> indexed
        | ValueNone ->
        match m with
        | _ when m.Arguments.Count = 1 && TypeSupport.OptionalMember(m.Method.DeclaringType, m.Method.Name).IsSome ->
            match TypeSupport.OptionalMember(m.Method.DeclaringType, m.Method.Name).Value with
            | OptionalAccess.Present -> QueryProduced.typed m.Type (SqlExpr.Unary(UnaryOperator.IsNotNull, visitPresenceDu m.Arguments[0] qb))
            | OptionalAccess.Absent -> QueryProduced.typed m.Type (SqlExpr.Unary(UnaryOperator.IsNull, visitPresenceDu m.Arguments[0] qb))
            | OptionalAccess.Payload ->
                QueryProduced.bind qb.SourceContext.NextAlias
                    (fun value input -> DirectValue(value, QueryTranslatorSerialization.serializationPayload input))
                    (visitRequested m.Arguments[0] qb)
        | _ when m.Method.Name = "Invoke" ->
            let body = normalizeStoredConstruction m
            if not(obj.ReferenceEquals(body, m)) then visitRequested body qb
            elif isFullyConstant m then QueryProduced.typed m.Type (qb.AllocateParamExpr(ExpressionEvaluation.evaluateExpr<obj> m))
            else raise(NotSupportedException(sprintf "Error: Method '%s' is not supported.\nReason: The method has no SQL translation.\nFix: Rewrite the query or call AsEnumerable() before using this method." m.Method.Name))
        | OfShape1 null null "GetArray" null (array, index) -> arrayIndexDu array index m.Type qb
        | OfShape1 null null "op_Dynamic" null (o, propExpr) -> visitPropertyDu o (propExpr :?> ConstantExpression).Value m qb
        | OfShape0 null null "QuotationToLambdaExpression" arg -> visitRequested (arg :?> MethodCallExpression).Arguments[0] qb
        | OfShape0 null null "op_Implicit" arg ->
            let produced = visitProduced arg qb
            QueryProduced.bind qb.SourceContext.NextAlias (fun value input ->
                match QueryTranslatorNumeric.cast false m.Type arg.Type value with
                | ValueSome converted -> QueryProduced.typed m.Type converted
                | ValueNone -> DirectValue(value, input)) produced
        | OfShape0 null null "op_Explicit" arg -> castToDu qb m.Type arg
        | OfShape0 null null "NewSqlId" arg -> visitProduced arg qb
        | OfShape0 null null "ToObject" o when typeof<JsonSerializator.JsonValue>.IsAssignableFrom m.Method.DeclaringType || m.Method.DeclaringType.FullName = "SoloDatabase.MongoDB.BsonDocument" ->
            castToDu qb (m.Method.GetGenericArguments()[0]) o
        | OfShape0 null null "CastTo" arg -> castToDu qb (m.Method.GetGenericArguments()[0]) arg
        | OfShape0 null null "Items" source when source.Type.IsConstructedGenericType && typedefof<System.Linq.IGrouping<_,_>>.Equals(source.Type.GetGenericTypeDefinition()) ->
            visitPropertyDu source "Items" m qb
        | _ ->
            let value = visitScalarMethodCallDu visitProduced visitDu visitStoredValueDu visitPresenceDu visitMathMethodDu containsImplDu visitNestedArrayPredicateDu emitStringOperandDu m qb
            let produced = QueryProduced.typed m.Type value
            if QueryTranslatorCharacters.indexArguments m.Method then
                let index = if m.Method.IsStatic then m.Arguments[1] else m.Arguments[0]
                DirectValue(value, { produced.Input with LegacyCharacterText = QueryTranslatorCharacters.hasLegacyIndex index })
            else produced
