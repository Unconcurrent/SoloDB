namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Text
open SoloDatabase.QueryTranslatorBaseTypes
open SoloDatabase.SqlModel

/// Formats scalar components after their actual expression producers have run.
module internal QueryTranslatorVisitCoreStringFormatting =
    let private parts (format: string) =
        let pieces = ResizeArray<Choice<string, int>>()
        let literal = StringBuilder()
        let flush () =
            if literal.Length > 0 then
                pieces.Add(Choice1Of2(literal.ToString()))
                literal.Clear() |> ignore
        let mutable position = 0
        while position < format.Length do
            match format[position] with
            | '{' when position + 1 < format.Length && format[position + 1] = '{' ->
                literal.Append('{') |> ignore
                position <- position + 2
            | '}' when position + 1 < format.Length && format[position + 1] = '}' ->
                literal.Append('}') |> ignore
                position <- position + 2
            | '{' ->
                let closing = format.IndexOf('}', position + 1)
                if closing < 0 then raise(NotSupportedException("The composite format string is malformed."))
                flush()
                let placeholder = format.Substring(position + 1, closing - position - 1)
                let endings = [placeholder.IndexOf(','); placeholder.IndexOf(':')] |> List.filter (fun index -> index >= 0)
                let ending = if endings.IsEmpty then placeholder.Length else List.min endings
                match Int32.TryParse(placeholder.Substring(0, ending).Trim()) with
                | true, index -> pieces.Add(Choice2Of2 index)
                | _ -> raise(NotSupportedException("Only numeric composite format placeholders are supported."))
                position <- closing + 1
            | character ->
                literal.Append(character) |> ignore
                position <- position + 1
        flush()
        pieces.ToArray()

    let private stringValue value input =
        match QueryTranslatorNumeric.domain input.OriginalType with
        | ValueSome kind -> QueryTranslatorNumeric.call kind "TEXT" [value]
        | ValueNone when input.Carrier = CanonicalToken -> QueryTranslatorNumeric.nativeScalar value
        | ValueNone -> value

    let private construct builder format (components: QueryProducedValue array) =
        QueryProduced.construct builder.SourceContext.NextAlias (fun components ->
            let values = components |> Array.map (fun (value, input) ->
                let role =
                    if typeof<char>.Equals(Utils.optionalPayloadType input.OriginalType) then SqlExpr.Literal(SqlLiteral.Integer 1L)
                    elif typeof<obj>.Equals input.OriginalType && input.Operand.IsSome then
                        SqlExpr.FunctionCall("json", [QuerySerializationSql.writerDescriptor input])
                    else SqlExpr.Literal(SqlLiteral.Integer 0L)
                stringValue value input, role)
            let values =
                match format with
                | ValueNone -> values
                | ValueSome pieces ->
                    pieces |> Array.map (function
                        | Choice1Of2 literal -> SqlExpr.Literal(SqlLiteral.String literal), SqlExpr.Literal(SqlLiteral.Integer 0L)
                        | Choice2Of2 index when index >= 0 && index < values.Length -> values[index]
                        | _ -> raise(NotSupportedException("The composite format index is out of range.")))
            let text =
                if values |> Array.exists (fun (_, role) -> role <> SqlExpr.Literal(SqlLiteral.Integer 0L)) then QueryTranslatorCharacters.concat values
                else SqlExpr.FunctionCall("CONCAT", values |> Array.map fst |> Array.toList)
            QueryProduced.typed typeof<string> text) components

    let internal binaryProduced visit (binary: BinaryExpression) builder =
        let left = visit binary.Left builder
        let right = visit binary.Right builder
        construct builder ValueNone [|left; right|]

    let internal tryProduced (visit: Expression -> QueryBuilder -> QueryProducedValue) (call: MethodCallExpression) (builder: QueryBuilder) =
        if not(typeof<string>.Equals call.Method.DeclaringType) then ValueNone else
        let components, format =
            match call.Method.Name with
            | "Concat" ->
                let arguments =
                    if call.Arguments.Count = 1 && call.Arguments[0] :? NewArrayExpression then
                        (call.Arguments[0] :?> NewArrayExpression).Expressions |> Seq.toArray
                    elif call.Arguments.Count > 1 then call.Arguments |> Seq.toArray
                    else Array.empty
                arguments, ValueNone
            | "Format" when builder.AllowStringFormat && call.Arguments.Count >= 2 && (call.Arguments[0] :? ConstantExpression) ->
                let arguments =
                    if call.Arguments.Count = 2 && call.Arguments[1] :? NewArrayExpression then
                        (call.Arguments[1] :?> NewArrayExpression).Expressions |> Seq.toArray
                    else call.Arguments |> Seq.skip 1 |> Seq.toArray
                arguments, ValueSome(parts ((call.Arguments[0] :?> ConstantExpression).Value :?> string))
            | _ -> Array.empty, ValueNone
        if components.Length = 0 then ValueNone else
        ValueSome(construct builder format (components |> Array.map (fun expression -> visit expression builder)))
