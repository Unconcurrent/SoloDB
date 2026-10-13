namespace SoloDatabase.JsonSerializator

open System

/// Alternate formatted carriers preserve complete lexical conversion before format errors.
[<AbstractClass; Sealed>]
type internal JsonFormattedInput =
    static member internal GuidUtf8(context: byref<JsonReadContext<Utf8Reader>>, start: int, count: int): Guid =
        Guid.Parse(JsonStringInput.Utf8(&context,start,count))
    static member internal GuidUtf16(context: byref<JsonReadContext<Utf16Reader>>, start: int, count: int): Guid =
        Guid.Parse(JsonStringInput.Utf16(&context,start,count))
    static member internal GuidEscapedUtf8(context: byref<JsonReadContext<Utf8Reader>>, start: int, count: int, quote: int): Guid =
        Guid.Parse(JsonStringInput.EscapedUtf8(&context,start,count,quote))
    static member internal GuidEscapedUtf16(context: byref<JsonReadContext<Utf16Reader>>, start: int, count: int, quote: int): Guid =
        Guid.Parse(JsonStringInput.EscapedUtf16(&context,start,count,quote))
    static member internal BytesUtf8(context: byref<JsonReadContext<Utf8Reader>>, start: int, count: int): byte[] =
        JsonHelper.JSONStringToByteArray(JsonStringInput.Utf8(&context,start,count))
    static member internal BytesUtf16(context: byref<JsonReadContext<Utf16Reader>>, start: int, count: int): byte[] =
        JsonHelper.JSONStringToByteArray(JsonStringInput.Utf16(&context,start,count))
    static member internal BytesEscapedUtf8(context: byref<JsonReadContext<Utf8Reader>>, start: int, count: int, quote: int): byte[] =
        JsonHelper.JSONStringToByteArray(JsonStringInput.EscapedUtf8(&context,start,count,quote))
    static member internal BytesEscapedUtf16(context: byref<JsonReadContext<Utf16Reader>>, start: int, count: int, quote: int): byte[] =
        JsonHelper.JSONStringToByteArray(JsonStringInput.EscapedUtf16(&context,start,count,quote))
