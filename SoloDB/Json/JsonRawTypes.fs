namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Runtime.CompilerServices

/// Native parameter facts; encoded numeric output retains its existing owned text carrier.
[<Struct>]
type internal JsonSqlNumber =
    val Value: obj
    val Encoded: bool
    new(value,encoded) = { Value = value; Encoded = encoded }

/// Input remains borrowed for one synchronous conversion. Only offsets enter member maps.
[<Struct; IsByRefLike>]
type internal JsonReadContext<'R when 'R : struct and 'R :> IJsonReader> =
    val mutable Cursor: ParserContext<'R>
    val mutable Range: JsonSourceRange
    val mutable Properties: Dictionary<string,JsonSourceRange>
    val mutable Consumed: bool
    val mutable TypeSelected: bool
    val mutable Elements: List<JsonSourceRange>
    new(cursor, range) = { Cursor = cursor; Range = range; Properties = null; Elements = null; Consumed = false; TypeSelected = false }

type internal JsonDirectReader<'T,'R when 'R : struct and 'R :> IJsonReader> =
    delegate of byref<JsonReadContext<'R>> -> 'T

type internal JsonDirectRuntimeReader<'R when 'R : struct and 'R :> IJsonReader> =
    delegate of byref<JsonReadContext<'R>> -> obj

type internal JsonDirectWriter<'T,'Element> =
    delegate of 'T * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit

type internal JsonDirectRuntimeWriter<'Element> =
    delegate of obj * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit

exception internal JsonDifferentRuntimeType

/// Fixed wire contracts distinguish native JSON from declared document storage.
[<Sealed>]
type internal JsonNativeContract private () = class end
[<Sealed>]
type internal JsonStoredContract private () = class end

type internal JsonOperationPolicy<'Contract> =
    static member IsStored = typeof<'Contract>.Equals(typeof<JsonStoredContract>)
    static member Name(name: string) =
        if JsonOperationPolicy<'Contract>.IsStored then SoloDatabase.Metadata.Generation.TypeSupport.StoredRole name else name

module internal JsonReadCursor =
    let numericStart<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        context.Cursor.codeUnitIndex <- context.Range.Start
        let point = JsonLexer.nextSignificantCodePoint &context.Cursor
        let number = uint32(point - int '0') <= 9u || point = int '+' || point = int '-' || point = int '.'
        if number then
            let skipped = context.Cursor.codeUnitIndex - context.Range.Start
            context.Range <- { Start = context.Cursor.codeUnitIndex; Length = context.Range.Length - skipped }
        number

    let scan<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        context.Cursor.codeUnitIndex <- context.Range.Start
        let mutable sink = JsonRangeSink<'R>(context.Range.Start)
        let range = JsonGrammar.parse<JsonSourceRange,Dictionary<string,JsonSourceRange>,List<JsonSourceRange>,'R,JsonRangeSink<'R>> &sink &context.Cursor
        context.Range <- range
        context.Properties <- sink.properties
        context.Elements <- sink.elements
        context.Consumed <- true

    let token<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        context.Cursor.codeUnitIndex <- context.Range.Start
        context.Cursor.decodeStrings <- true
        let token = JsonLexer.readNext &context.Cursor
        context.Consumed <- (match token with StringToken _ | NumberToken _ | BooleanToken _ | NullToken -> true | _ -> false)
        match token with NumberToken literal -> JsonGrammar.validateNumber &context.Cursor literal | _ -> ()
        token

    let child<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) range =
        JsonReadContext(context.Cursor,range)

    let tryProperty<'R when 'R : struct and 'R :> IJsonReader>
        (context: byref<JsonReadContext<'R>>) name (result: byref<JsonReadContext<'R>>) =
        if isNull context.Properties then scan &context
        if isNull context.Properties then invalidOp "Expected a JSON object."
        match context.Properties.TryGetValue name with
        | true, range -> result <- child &context range; true
        | false, _ -> false

    let element<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) index =
        if isNull context.Elements then scan &context
        if isNull context.Elements then invalidOp "Expected a JSON array."
        child &context context.Elements[index]

    let skip<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        let mutable sink = JsonDiscardSink<'R>()
        context.Cursor.codeUnitIndex <- context.Range.Start
        JsonGrammar.parse<int,int,int,'R,JsonDiscardSink<'R>> &sink &context.Cursor |> ignore
        context.Consumed <- true

    let finish<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        context.Cursor.decodeStrings <- false
        match JsonLexer.readNext &context.Cursor with EndOfInput -> () | _ -> failwith "Malformed json."
