namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection
open JsonFunctions
open SoloDatabase.JsonSerializator
open SoloDatabase.SqlModel

/// Canonical allocation boundary for values compared with SQLite-stored document values.
module internal StoredValueParameter =
    let private declaredSerializerCache =
        ConcurrentDictionary<Type, Func<obj, JsonValue>>()

    let private serializeAsDeclaredType (declaredType: Type) (value: obj) =
        let serializer =
            declaredSerializerCache.GetOrAdd(
                declaredType,
                Func<Type, Func<obj, JsonValue>>(fun valueType ->
                    let valueParameter = Expression.Parameter(typeof<obj>, "value")
                    let serializeMethod =
                        typeof<JsonValue>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                        |> Array.find (fun methodInfo ->
                            methodInfo.Name = "Serialize"
                            && methodInfo.IsGenericMethodDefinition
                            && methodInfo.GetParameters().Length = 1)
                        |> fun methodInfo -> methodInfo.MakeGenericMethod([| valueType |])
                    Expression.Lambda<Func<obj, JsonValue>>(
                        Expression.Call(serializeMethod, Expression.Convert(valueParameter, valueType)),
                        [| valueParameter |]).Compile(false)))
        serializer.Invoke(value)

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

    let allocateNamed variables name value = allocateNamedForComparison false variables name value

    let allocateComparison variables value =
        allocateNamedForComparison true variables (sprintf "dp%d" variables.Count) value

    let allocateNext
        (variables: #IDictionary<string, obj>)
        (value: obj)
        : SqlExpr =
        allocateNamed variables (sprintf "dp%d" variables.Count) value

    /// Allocate a decimal so that the digits written are the digits stored.
    ///
    /// The ordinary path renders a JSON number as a double, which is lossy for any decimal that
    /// binary64 cannot represent: the row updates, the query returns, and the value quietly is not
    /// the one the caller wrote. SoloDB keeps decimal precision as a public guarantee, so the
    /// value is bound as its exact invariant text and parsed back into a JSON number by SQLite,
    /// which preserves the digits and leaves the document a JSON number rather than a string.
    let allocateExactDecimal
        (variables: #IDictionary<string, obj>)
        (value: decimal)
        : SqlExpr =
        let name = sprintf "dp%d" variables.Count
        variables.[name] <- (value.ToString(System.Globalization.CultureInfo.InvariantCulture) :> obj)
        SqlExpr.FunctionCall("jsonb", [ SqlExpr.Parameter name ])

    let allocateNamedForDeclaredType
        (variables: #IDictionary<string, obj>)
        (name: string)
        (declaredType: Type)
        (value: obj)
        : SqlExpr =
        let storedValue, shouldEncode =
            (serializeAsDeclaredType declaredType value).ToSQLValue()
        variables.[name] <- storedValue

        let parameter = SqlExpr.Parameter name
        if shouldEncode then
            SqlExpr.FunctionCall("jsonb", [parameter])
        else
            parameter
