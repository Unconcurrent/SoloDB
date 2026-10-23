namespace SoloDatabase.JsonSerializator

open System
open System.Numerics

[<AbstractClass; Sealed>]
type internal JsonRawScalarReader =
    static member Create<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'T,'R> voption =
        let convert (reader: Delegate) = ValueSome(reader :?> JsonDirectReader<'T,'R>)
        if (SoloDatabase.Metadata.Generation.TypeSupport.RawNumericCore typeof<'T>.FullName).IsSome then
            ValueSome(JsonNumericRuntime.reader<'T,'R>())
        else
        match SoloDatabase.Metadata.Generation.TypeSupport.RawPrimitive typeof<'T>.FullName with
        | "Boolean" -> convert(JsonDirectReader<bool,'R>(fun context -> JsonRawPrimitive.ReadBoolean<'R> &context))
        | "Char" -> convert(JsonDirectReader<char,'R>(fun context -> JsonRawPrimitive.ReadChar<'R> &context))
        | "String" -> convert(JsonDirectReader<string,'R>(fun context -> JsonRawPrimitive.ReadString<'R> &context))
        | "Bytes" | "Guid" -> ValueSome(JsonFormattedRuntime.reader<'T,'R>())
        | "Type" -> convert(JsonDirectReader<Type,'R>(fun context -> JsonRawPrimitive.ReadType<'R> &context))
        | "DateTime" -> convert(JsonDirectReader<DateTime,'R>(fun context -> JsonRawPrimitive.ReadDateTime<'R> &context))
        | "DateTimeOffset" -> convert(JsonDirectReader<DateTimeOffset,'R>(fun context -> JsonRawPrimitive.ReadDateTimeOffset<'R> &context))
        | "DateOnly" -> convert(JsonDirectReader<DateOnly,'R>(fun context -> JsonRawPrimitive.ReadDateOnly<'R> &context))
        | "TimeOnly" -> convert(JsonDirectReader<TimeOnly,'R>(fun context -> JsonRawPrimitive.ReadTimeOnly<'R> &context))
        | "TimeSpan" -> convert(JsonDirectReader<TimeSpan,'R>(fun context -> JsonRawPrimitive.ReadTimeSpan<'R> &context))
        | _ -> ValueNone

[<AbstractClass; Sealed>]
type internal JsonRawScalarWriter =
    static member Create<'T,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<'T,'E> voption =
        let convert (writer: Delegate) = ValueSome(writer :?> JsonDirectWriter<'T,'E>)
        if (SoloDatabase.Metadata.Generation.TypeSupport.NumericWriterCore typeof<'T>.FullName).IsSome then
            ValueSome(JsonWriterRuntime.writer<'T,'E,'O>())
        else
        match SoloDatabase.Metadata.Generation.TypeSupport.RawPrimitive typeof<'T>.FullName with
        | "Boolean" -> convert((JsonWriterRuntime.boolean typeof<'E> typeof<'O>).CreateDelegate(typeof<JsonDirectWriter<bool,'E>>))
        | "String" -> convert((JsonWriterRuntime.text typeof<'E> typeof<'O>).CreateDelegate(typeof<JsonDirectWriter<string,'E>>))
        | "Bytes" -> convert((JsonWriterRuntime.bytes typeof<'E> typeof<'O>).CreateDelegate(typeof<JsonDirectWriter<byte[],'E>>))
        | "Guid" -> convert((JsonWriterRuntime.guid typeof<'E> typeof<'O>).CreateDelegate(typeof<JsonDirectWriter<Guid,'E>>))
        | _ when SoloDatabase.Metadata.Generation.TypeSupport.SequenceBytes(typeof<'T>.IsArray,
                     (if typeof<System.Collections.Generic.IEnumerable<byte>>.IsAssignableFrom typeof<'T> then "System.Byte" else "")) ->
            ValueSome(JsonWriterRuntime.sequenceBytes<'T,'E,'O>())
        | _ -> ValueNone
