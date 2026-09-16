namespace SoloDatabase

open System
open System.Collections.Generic
open System.Globalization
open SoloDatabase.JsonSerializator
open SoloDatabase.SqlModel

module internal EqualityRules =
    [<Struct>]
    type Comparison =
        | Scalar of value: obj
        | Length of count: int

    [<Struct>]
    type PathSegment =
        | Member of name: string
        | Index of index: int

    let appendMember (path: string) (key: string) =
        if key.Length = 0 || key |> Seq.exists (fun c -> not (Char.IsLetterOrDigit c || c = '_' || c = '$')) then
            let escaped = key.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\000", "\\u0000")
            sprintf "%s.\"%s\"" path escaped
        else sprintf "%s.%s" path key

    // Traversal prepends segments; rendering applies them from the root outwards.
    let renderPath segments =
        List.foldBack (fun segment path ->
            match segment with
            | Member key -> appendMember path key
            | Index index -> sprintf "%s[%d]" path index) segments "$"

    let rec walk ignoreDiscriminators path json emit =
        match json with
        | JsonValue.Null -> emit path (Scalar null)
        | JsonValue.Boolean value -> emit path (Scalar(box value))
        | JsonValue.Number value -> emit path (Scalar(box value))
        | JsonValue.String value -> emit path (Scalar(box value))
        | JsonValue.Object fields ->
            for KeyValue(key, value) in fields do
                if not (ignoreDiscriminators && key = "$type") then
                    walk ignoreDiscriminators (Member key :: path) value emit
        | JsonValue.List items ->
            emit path (Length items.Count)
            for i = 0 to items.Count - 1 do walk ignoreDiscriminators (Index i :: path) items.[i] emit

    // SQLite's JSON label comparison terminates at NUL, even in escaped labels.
    let private sameMember (left: string) (right: string) =
        let leftEnd = left.IndexOf '\000'
        let rightEnd = right.IndexOf '\000'
        let leftLength = if leftEnd < 0 then left.Length else leftEnd
        let rightLength = if rightEnd < 0 then right.Length else rightEnd
        leftLength = rightLength && String.CompareOrdinal(left, 0, right, 0, leftLength) = 0

    let rec private readPath stored = function
        | [] -> stored
        | segment :: parent ->
            match segment, readPath stored parent with
            | Member key, JsonValue.Object fields ->
                let name = key
                use entries = fields.GetEnumerator()
                let mutable found = false
                let mutable value = JsonValue.Null
                while not found && entries.MoveNext() do
                    if sameMember entries.Current.Key name then
                        found <- true
                        value <- entries.Current.Value
                value
            | Index index, JsonValue.List items when index < items.Count -> items.[index]
            | _ -> JsonValue.Null

    // SQLite compares INTEGER and REAL without rounding the integer to a double.
    let private integerEqualsReal integer real =
        real >= -9223372036854775808.0 && real < 9223372036854775808.0
        && Math.Truncate(real) = real && int64 real = integer

    let private sqlIs (left: obj) (right: obj) =
        match left, right with
        | null, null -> true
        | (:? int64 as a), (:? double as b) -> integerEqualsReal a b
        | (:? double as a), (:? int64 as b) -> integerEqualsReal b a
        | _ -> Object.Equals(left, right)

    let matches stored (argument: obj) =
        let expected = match argument with :? JsonValue as json -> json | _ -> JsonValue.Serialize argument
        let mutable matches = true
        let compare path comparison =
            if matches then
                let actual = readPath stored path
                matches <-
                    match comparison with
                    | Length count ->
                        match actual with
                        | JsonValue.Null -> false
                        | JsonValue.List items -> items.Count = count
                        | _ -> count = 0
                    | Scalar value ->
                        let discriminator = match path with Member "$type" :: _ -> true | _ -> false
                        let storedValue, isJson = JsonFunctions.jsonValueToSQLValue actual
                        let struct (expectedValue, _) = JsonFunctions.toSQLParameterForComparison true value
                        (discriminator && isNull storedValue)
                        || (not isJson && sqlIs storedValue expectedValue)
        walk false [] expected compare
        matches

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
        | JsonValue.Number value -> box value
        | JsonValue.String value -> box value
        | _ -> invalidOp "Expected a scalar serialization for a comparison field."

    type Arguments =
        static member Scalar<'T>(value: 'T) = JsonValue.Serialize value |> scalarValue

        static member Rows<'T>(value: 'T, missing: bool, ignoreDiscriminators: bool, knownOperand: bool) =
            let rows = ResizeArray<JsonValue>()
            let append path comparison =
                let kind, value =
                    match comparison with
                    | Length count -> 1M, JsonValue.Number(decimal count)
                    | Scalar (:? decimal as number) ->
                        match JsonFunctions.numberToSQLValue number with
                        | :? double as real -> 2M, JsonValue.String(real.ToString("R", CultureInfo.InvariantCulture))
                        | integer -> 0M, JsonValue.Serialize integer
                    | Scalar value -> 0M, JsonValue.Serialize value
                rows.Add(JsonValue.List [|JsonValue.String(renderPath path); JsonValue.Number kind; value|])
            let json =
                if missing then JsonValue.Null
                elif knownOperand && not ignoreDiscriminators then
                    match box value with :? JsonValue as json -> json | _ -> JsonValue.Serialize value
                else JsonValue.Serialize value
            walk ignoreDiscriminators [] json append
            JsonValue.List(rows).ToJsonString()
