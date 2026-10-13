namespace SoloDatabase

open System
open System.Collections.Generic
open JsonFunctions
open SoloDatabase.JsonSerializator
open SoloDatabase.SqlModel

/// Canonical allocation boundary for values compared with SQLite-stored document values.
module internal StoredValueParameter =

    /// Structural or dynamic serialization can invoke user accessors or enumerators.
    let canSerializeWithoutEntityCode declaredType =
        JsonValue.HasScalarCarrier declaredType

    let private normalizeBoolean (value: obj) =
        match value with
        | :? bool as booleanValue -> box (if booleanValue then 1 else 0)
        | _ -> value

    let private allocateNamedForComparison comparison
        (variables: #IDictionary<string, obj>)
        (name: string)
        (value: obj)
        : SqlExpr =
        let struct (jsonValue, shouldEncode) = value |> normalizeBoolean |> toSQLParameterForComparison comparison
        variables.[name] <- jsonValue

        let parameter = SqlExpr.Parameter name
        if shouldEncode then
            SqlExpr.FunctionCall("jsonb", [parameter])
        else
            parameter

    /// Retain the writer selected by this allocation without sampling the value again.
    let allocateProduced comparison (variables: #IDictionary<string, obj>) declaredType value =
        let struct(stored, encoded, writer) = JsonFunctions.prepareQueryParameter comparison declaredType value
        let name = "dp" + variables.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)
        variables[name] <- stored
        let parameter = SqlExpr.Parameter name
        let expression = if encoded then SqlExpr.FunctionCall("jsonb", [parameter]) else parameter
        struct(expression, writer)

    let allocateNamed variables name value = allocateNamedForComparison false variables name value

    let private parameterName (count: int) =
        "dp" + count.ToString(System.Globalization.CultureInfo.InvariantCulture)

    let allocateComparison variables value =
        allocateNamedForComparison true variables (parameterName variables.Count) value

    let allocateNext
        (variables: #IDictionary<string, obj>)
        (value: obj)
        : SqlExpr =
        allocateNamed variables (parameterName variables.Count) value

    /// Bind the exact comparable value as a JSON string leaf.
    let allocateExactDecimal
        (variables: #IDictionary<string, obj>)
        (value: decimal)
        : SqlExpr =
        allocateNext variables (box value)

    /// Wide unsigned values consume their original JSON digits before comparison.
    let declaredExpression declaredType expression =
        if QueryTranslatorNumeric.unsigned64 declaredType then
            QueryTranslatorNumeric.storedValue declaredType expression
        else expression

    let allocateNamedForDeclaredType
        (variables: #IDictionary<string, obj>)
        (name: string)
        (declaredType: Type)
        (value: obj)
        : SqlExpr =
        let exactUnsigned = QueryTranslatorNumeric.unsigned64 declaredType
        let storedValue, shouldEncode =
            if exactUnsigned then JsonValue.StoredComparisonParameter(declaredType,value)
            else JsonValue.StoredParameter(declaredType,value)
        let struct(storedValue, shouldEncode) =
            if exactUnsigned && not shouldEncode then toSQLParameterForComparison true storedValue
            else struct(storedValue,shouldEncode)
        variables.[name] <- storedValue

        let parameter = SqlExpr.Parameter name
        if shouldEncode then
            SqlExpr.FunctionCall("jsonb", [parameter])
        else
            parameter
