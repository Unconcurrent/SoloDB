namespace SoloDatabase

open System
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
open SoloDatabase.Metadata.Generation

module internal QueryTranslatorVisitCoreCastAggregate =
    let internal castToDu (visitProduced: Expression -> QueryBuilder -> QueryProducedValue) (qb: QueryBuilder) (castToType: Type) (o: Expression) checkedConversion : QueryProducedValue =
        let sqlType=QueryTranslatorNumeric.nativeCastType castToType
        let numeric = (QueryTranslatorNumeric.domain castToType).IsSome || (QueryTranslatorNumeric.domain o.Type).IsSome
        let argument = visitProduced o (if numeric || sqlType.IsSome then {qb with UpdateMode = false} else qb)
        let framed input argument =
          if input.Carrier = CanonicalToken then argument else
            match argument with
            | SqlExpr.JsonExtractExpr _ | SqlExpr.JsonRootExtract _ | SqlExpr.FunctionCall("jsonb_extract", _) -> argument
            | _ -> jsonToken argument
        let convert argument input =
          let framed = framed input
          let originalInput =
              if typeof<JsonSerializator.JsonValue>.IsAssignableFrom o.Type || o.Type.FullName = "SoloDatabase.MongoDB.BsonDocument" then ValueSome input
              else ValueNone
          let converted =
            match originalInput with
            | ValueSome original when original.Operand.IsSome &&
                                      (original.Operand.Value.Accesses.Length > 0 ||
                                       not(QuerySerializationSql.isConstantDescriptor
                                               (QuerySerializationSql.serializationOperand original.Operand.Value))) ->
                let descriptor = QuerySerializationSql.serializationOperand original.Operand.Value
                QueryTranslatorNumeric.convertWriter castToType (framed argument) descriptor
                |> ValueOption.map (fun result ->
                    match sqlType, QueryTranslatorNumeric.domain castToType with
                    | ValueSome sqlType, ValueNone ->
                        // Native tree inputs retain SQLite scalar extraction and CAST.
                        // Stored inputs have already completed their checked target conversion.
                        SqlExpr.Cast(SqlExpr.FunctionCall("jsonb_extract", [result; SqlExpr.Literal(SqlLiteral.String "$")]), sqlType)
                    | _ -> result)
            | ValueSome original when QueryTranslatorSerialization.isStoredNumericalInput original.OriginalType ->
                QueryTranslatorNumeric.convertStored castToType (framed argument)
            | ValueSome _ ->
                match QueryTranslatorNumeric.domain castToType with
                | ValueSome _ -> QueryTranslatorNumeric.convertNativeJson castToType (framed argument)
                | ValueNone when sqlType.IsSome ->
                    ValueSome(QueryTranslatorNumeric.conversion checkedConversion castToType o.Type (QueryTranslatorNumeric.nativeScalar (framed argument)))
                | _ -> ValueNone
            | ValueNone -> ValueNone
          match converted with
          | ValueSome converted -> QueryProduced.typed castToType converted
          | ValueNone ->
              let nativeArgument =
                  if originalInput.IsSome && isSqlScalarResult castToType then QueryTranslatorNumeric.nativeScalar (framed argument)
                  elif typeof<char>.Equals(Utils.optionalPayloadType o.Type) && not input.LegacyCharacterText then
                      QueryTranslatorCharacters.comparable o.Type argument input
                  else argument
              let converted = QueryTranslatorNumeric.conversion checkedConversion castToType o.Type nativeArgument
              if obj.ReferenceEquals(converted, argument) then DirectValue(argument, input)
              else QueryProduced.typed castToType converted
        QueryProduced.bind qb.SourceContext.NextAlias convert argument

    let private parseMathFormatDu (format: string) (resolveArg: int -> SqlExpr) : SqlExpr =
        if format = "($1 - $2 * ROUND($1 / $2))" then
            SqlExpr.Binary(resolveArg 1, BinaryOperator.Sub, SqlExpr.Binary(resolveArg 2, BinaryOperator.Mul, SqlExpr.FunctionCall("ROUND", [SqlExpr.Binary(resolveArg 1, BinaryOperator.Div, resolveArg 2)])))
        elif format = "($1 * $2)" then
            SqlExpr.Binary(resolveArg 1, BinaryOperator.Mul, resolveArg 2)
        elif format = "($2 - $3 * ROUND($2 / $3))" then
            SqlExpr.Binary(resolveArg 2, BinaryOperator.Sub, SqlExpr.Binary(resolveArg 3, BinaryOperator.Mul, SqlExpr.FunctionCall("ROUND", [SqlExpr.Binary(resolveArg 2, BinaryOperator.Div, resolveArg 3)])))
        elif format = "($2 * $3)" then
            SqlExpr.Binary(resolveArg 2, BinaryOperator.Mul, resolveArg 3)
        else
            let parenIdx = format.IndexOf('(')
            let funcName = format.Substring(0, parenIdx)
            let argsStr = format.Substring(parenIdx + 1, format.Length - parenIdx - 2)
            let args =
                argsStr.Split(',')
                |> Array.map (fun s ->
                    let s = s.Trim()
                    if s.Length > 0 && s.[0] = '$' then resolveArg (int (s.Substring(1)))
                    else SqlExpr.Literal(SqlLiteral.Integer(int64 s)))
                |> Array.toList
            SqlExpr.FunctionCall(funcName, args)

    let internal visitMathMethodDu (visitDu: Expression -> QueryBuilder -> SqlExpr) (visitComparisonDu: Expression -> QueryBuilder -> SqlExpr) (m: MethodCallExpression) (qb: QueryBuilder) : SqlExpr voption =
        match QueryTranslatorNumericMethods.tryComparisonCall m.Method m.Arguments.Count (fun index -> m.Arguments[index]) (fun index -> visitComparisonDu m.Arguments[index] qb) with
        | ValueSome result -> ValueSome result
        | ValueNone ->
        match QueryTranslatorNumericMethods.tryMathCall m.Method m.Arguments.Count (fun index -> m.Arguments[index]) (fun index -> visitDu m.Arguments[index] qb) with
        | QueryTranslatorNumericMethods.Translated result -> ValueSome result
        | QueryTranslatorNumericMethods.Unsupported -> ValueNone
        | QueryTranslatorNumericMethods.NotApplicable ->
        match mathFunctionTransformation.TryGetValue((m.Arguments.Count, m.Method.Name)) with
        | false, _ -> ValueNone
        | true, format ->
            let resolveArg (n: int) =
                visitDu (match n with | 0 -> m.Object | d -> m.Arguments.[d - 1]) qb
            ValueSome(parseMathFormatDu format resolveArg)
