namespace SoloDatabase

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection
open JsonFunctions
open SoloDatabase.JsonSerializator
open SqlDu.Engine.C1.Spec

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

    let allocateNamed
        (variables: #IDictionary<string, obj>)
        (name: string)
        (value: obj)
        : SqlExpr =
        let jsonValue, shouldEncode = value |> normalizeBoolean |> toSQLJson
        variables.[name] <- jsonValue

        let parameter = SqlExpr.Parameter name
        if shouldEncode then
            SqlExpr.FunctionCall("jsonb", [parameter])
        else
            parameter

    let allocateNext
        (variables: #IDictionary<string, obj>)
        (value: obj)
        : SqlExpr =
        allocateNamed variables (sprintf "dp%d" variables.Count) value

    let allocateNamedForDeclaredType
        (variables: #IDictionary<string, obj>)
        (name: string)
        (declaredType: Type)
        (value: obj)
        : SqlExpr =
        let storedValue, shouldEncode =
            serializeAsDeclaredType declaredType value
            |> jsonValueToSQLValue
        variables.[name] <- storedValue

        let parameter = SqlExpr.Parameter name
        if shouldEncode then
            SqlExpr.FunctionCall("jsonb", [parameter])
        else
            parameter
