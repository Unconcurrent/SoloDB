namespace SoloDatabase.JsonSerializator

open System

/// Concrete product operations are expanded before implementation publication.
module internal JsonProductOperations =
    [<Literal>]
    let CompletionVersion = 0

    [<NoCompilerInlining>]
    let Int8DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): sbyte =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int8HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): sbyte =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int8DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): sbyte =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int8HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): sbyte =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt8DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): byte =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt8HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): byte =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt8DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): byte =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt8HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): byte =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int16DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): int16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int16HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): int16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int16DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): int16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int16HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): int16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt16DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): uint16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt16HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): uint16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt16DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): uint16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt16HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): uint16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int32DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): int =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int32HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): int =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int32DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): int =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int32HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): int =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt32DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): uint32 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt32HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): uint32 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt32DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): uint32 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt32HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): uint32 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int64DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): int64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int64HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): int64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int64DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): int64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Int64HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): int64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt64DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): uint64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt64HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): uint64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt64DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): uint64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let UInt64HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): uint64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeInt32DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): nativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeInt32HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): nativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeInt32DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): nativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeInt32HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): nativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeUInt32DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): unativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeUInt32HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): unativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeUInt32DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): unativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeUInt32HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): unativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeInt64DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): nativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeInt64HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): nativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeInt64DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): nativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeInt64HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): nativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeUInt64DecimalUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): unativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeUInt64HexUtf8 (_source: byref<byte>, _length: int, _position: byref<int>): unativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeUInt64DecimalUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): unativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let NativeUInt64HexUtf16 (_source: byref<byte>, _length: int, _position: byref<int>): unativeint =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Round16 (_significand: uint64, _exponent: int64, _negative: bool): uint16 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Round32 (_significand: uint64, _exponent: int64, _negative: bool): uint32 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let Round64 (_significand: uint64, _exponent: int64, _negative: bool): uint64 =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let CaptureMarkerUtf8 (_context: byref<JsonReadContext<Utf8Reader>>): JsonMarkerRange =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let CaptureMarkerUtf16 (_context: byref<JsonReadContext<Utf16Reader>>): JsonMarkerRange =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let CaptureEnvelopeUtf8 (_context: byref<JsonReadContext<Utf8Reader>>): JsonEnvelopeRanges =
        invalidOp "The product operation implementation was not delivered."

    [<NoCompilerInlining>]
    let CaptureEnvelopeUtf16 (_context: byref<JsonReadContext<Utf16Reader>>): JsonEnvelopeRanges =
        invalidOp "The product operation implementation was not delivered."
