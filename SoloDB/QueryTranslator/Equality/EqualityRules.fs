namespace SoloDatabase

open System
open System.Collections.Generic
open System.Globalization
open SoloDatabase.JsonSerializator
open SoloDatabase.SqlModel

open JsonEqualitySupport

module internal EqualityRules =
    let conjunction comparisons =
        let rec combine = function
            | [] -> SqlExpr.Literal(SqlLiteral.Boolean true)
            | [single] -> single
            | head :: tail -> SqlExpr.Binary(head, BinaryOperator.And, combine tail)
        combine comparisons

    // A prior extraction may yield SQL text, not a JSON document. Preserve that
    // distinction for strings that happen to contain valid JSON as well.
    let extract target path =
        SqlExpr.FunctionCall("jsonb_extract", [SqlExpr.FunctionCall("json_quote", [target]); path])
    let pathLiteral path = SqlExpr.Literal(SqlLiteral.String path)

    let scalar target value discriminator =
        let equal =
            if value = SqlExpr.Literal(SqlLiteral.Null) then SqlExpr.Unary(UnaryOperator.IsNull, target)
            else SqlExpr.Binary(target, BinaryOperator.Is, value)
        match discriminator with
        | SqlExpr.Literal(SqlLiteral.Boolean false) -> equal
        | SqlExpr.Literal(SqlLiteral.Boolean true) -> SqlExpr.Binary(equal, BinaryOperator.Or, SqlExpr.Unary(UnaryOperator.IsNull, target))
        | _ -> SqlExpr.Binary(equal, BinaryOperator.Or,
                   SqlExpr.Binary(discriminator, BinaryOperator.And, SqlExpr.Unary(UnaryOperator.IsNull, target)))

    let length target value =
        SqlExpr.Binary(SqlExpr.Unary(UnaryOperator.IsNotNull, target), BinaryOperator.And,
            SqlExpr.Binary(SqlExpr.FunctionCall("json_array_length", [SqlExpr.FunctionCall("json_quote", [target])]), BinaryOperator.Is, value))

    // Keep serialized leaves in their declared JSON form until canonical parameter binding.
    let scalarValue = function
        | JsonValue.Null -> null
        | JsonValue.Boolean value -> box value
        | JsonValue.Number value -> numberToSQLValue value
        | JsonValue.String value -> box value
        | _ -> invalidOp "Expected a scalar serialization for a comparison field."

    type Arguments =
        static member Scalar(value: obj, valueType: Type) = JsonValue.StoredParameter(valueType,value)

        static member StoredJson(value: obj, valueType: Type, root: bool) =
            match value with
            | :? JsonValue as json -> json
            | _ ->
                let target = if root then typeof<obj> else valueType
                let text = JsonValue.WriteRuntimeBuffer(target,value,false,false)
                JsonValue.Parse(ReadOnlySpan(text.Array,text.Offset,text.Count))

        static member Rows(value: obj, valueType: Type, missing: bool, ignoreDiscriminators: bool, knownOperand: bool) =
            let rows = ResizeArray<JsonValue>()
            let append path comparison =
                let kind, value =
                    match comparison with
                    | Length count -> 1M, JsonValue.Number(decimal count)
                    | Scalar (JsonValue.Number number) ->
                        match numberToSQLValue number with
                        | :? double as real -> 2M, JsonValue.String(real.ToString("R", CultureInfo.InvariantCulture))
                        | integer -> 0M, JsonValue.Serialize integer
                    | Scalar value -> 0M, JsonValue.Serialize(scalarValue value)
                rows.Add(JsonValue.List [|JsonValue.String(renderPath path); JsonValue.Number kind; value|])
            let json =
                if missing then JsonValue.Null
                else Arguments.StoredJson(value,valueType,knownOperand)
            JsonEquality.Walk(ignoreDiscriminators, [], json, append)
            JsonValue.List(rows).ToJsonString()
