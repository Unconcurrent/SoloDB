namespace SoloDatabase.JsonSerializator

open System
open System.Runtime.InteropServices
open System.Text

/// Concrete owning conversion for already-framed input; no reader selection or metadata lookup.
[<AbstractClass; Sealed>]
type internal JsonStringInput =
    static member internal EscapedUtf8(context: byref<JsonReadContext<Utf8Reader>>, start: int, count: int, quote: int): string =
        context.Cursor.decodeStrings <- true
        JsonStringReader.readRemainder &context.Cursor (byte quote) start count

    static member internal EscapedUtf16(context: byref<JsonReadContext<Utf16Reader>>, start: int, count: int, quote: int): string =
        context.Cursor.decodeStrings <- true
        JsonStringReader.readRemainder &context.Cursor (byte quote) start count

    static member internal Utf8(context: byref<JsonReadContext<Utf8Reader>>, start: int, count: int): string =
        context.Cursor.decodeStrings <- true
        try JsonTextEncoding.utf8(context.Cursor.input.Slice(start,count),true)
        with :? DecoderFallbackException ->
            JsonStringReader.readRemainder &context.Cursor context.Cursor.input[start-1] start 0

    static member internal Utf16(context: byref<JsonReadContext<Utf16Reader>>, start: int, count: int): string =
        context.Cursor.decodeStrings <- true
        MemoryMarshal.Cast<byte,char>(context.Cursor.input).Slice(start,count).ToString()
