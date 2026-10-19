namespace SoloDatabase

open System
open SoloDatabase.RawSqliteFunctions
open SoloDatabase.SqlModel

/// Span range validation uses the existing preserved managed-failure channel.
module internal QueryTranslatorSpanSource =
    let name = "SPAN_VALUE"
    let private integer value = SqlExpr.Literal(SqlLiteral.Integer value)
    let private absent = SqlExpr.Literal SqlLiteral.Null
    let call stringSource source start length sought =
        SqlExpr.FunctionCall(name, [integer (if stringSource then 1L else 0L); source; start; length; sought])
    let array source start length =
        let available = SqlExpr.FunctionCall("COALESCE", [SqlExpr.FunctionCall("json_array_length", [source]); integer 0L])
        let valid = call false available start length absent
        SqlExpr.CaseExpr((valid,source), [], Some absent)
    let register (connection: NativeSqliteConnection) =
        connection.RegisterManagedScalar(name, 5, RawScalarFunc(fun context arguments ->
            let isString = arguments[0].GetInt32() <> 0
            let text = if isString then arguments[1].CopyToString() else null
            let available = if isString then (if isNull text then 0L else int64 text.Length) else arguments[1].GetInt64()
            let length = if arguments[3].IsNull then None else Some(arguments[3].GetInt64())
            let struct(start,count) = QuerySpanSource.bounds isString available (arguments[2].GetInt64()) length
            if not isString then context.SetInt32(1)
            else
                let matches =
                    match QueryTranslatorCharacters.unitValue arguments[4] with
                    | ValueSome unit when not(isNull text) -> text.AsSpan(start,count).IndexOf(unit) >= 0
                    | _ -> false
                context.SetInt32(if matches then 1 else 0)), SqliteFunctionFlags.Deterministic)
