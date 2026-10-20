namespace SoloDatabase.JsonSerializator

open System
open System.Numerics
open System.Runtime.CompilerServices
open SoloDatabase
open SoloDatabase.Numerics
open SoloDatabase.Metadata.CodecGeneration

/// Stored numerical leaves consume only the admitted comparable representation.
[<AbstractClass; Sealed>]
type internal JsonStoredNumeric private () =
    static member internal PrepareDecimal(value: decimal) = NumericKey.prepare(ExactNumber.ofDecimal value)
    static member internal PrepareBigInteger(value: BigInteger) = NumericKey.prepareInteger value
    static member internal Length(value: NumericKeyDigits) = NumericKey.length value
    static member internal Utf8Length(value: NumericKeyDigits) = NumericKey.utf8Length value
    static member internal Unit(value: NumericKeyDigits, index: int) = NumericKey.unitAt value index
    static member internal InvalidFormat(inner: Exception) =
        FormatException("A stored Decimal or BigInteger value must use the encoded string format. Missing data migration from SoloDB 1.3.0 to 1.4.0 in an edge case may be the cause. See https://github.com/Unconcurrent/SoloDB/blob/master/docs/numeric-migration.md.", inner)
    static member internal Decode(key: string) =
        try NumericKey.decode key
        with :? FormatException as error -> raise(JsonStoredNumeric.InvalidFormat(error))
    static member private Read<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | StringToken key -> ValueSome(JsonStoredNumeric.Decode key)
        | NullToken -> ValueNone
        | _ -> raise(JsonStoredNumeric.InvalidFormat(null))
    static member private Key<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        match JsonStoredNumeric.Read<'R> &context with
        | ValueSome value -> value
        | ValueNone -> raise(FormatException("A stored numerical key is required."))
    static member internal ReadKeySByte<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toSByte(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyByte<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toByte(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyInt16<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toInt16(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyUInt16<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toUInt16(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyInt32<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toInt32(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyUInt32<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toUInt32(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyInt64<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toInt64(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyUInt64<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toUInt64(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyIntPtr<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        if IntPtr.Size=4 then nativeint(JsonStoredNumeric.ReadKeyInt32<'R> &context)
        else nativeint(JsonStoredNumeric.ReadKeyInt64<'R> &context)
    static member internal ReadKeyUIntPtr<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        if UIntPtr.Size=4 then unativeint(JsonStoredNumeric.ReadKeyUInt32<'R> &context)
        else unativeint(JsonStoredNumeric.ReadKeyUInt64<'R> &context)
    static member internal ReadKeyDouble<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toDouble(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadKeyHalf<'T,'R when 'T : struct and 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): 'T =
        let mutable bits = ExactNumber.toHalfBits(JsonStoredNumeric.Key<'R> &context)
        Unsafe.As<uint16,'T>(&bits)
    static member internal ReadKeySingle<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        ExactNumber.toSingle(JsonStoredNumeric.Key<'R> &context)
    static member internal ReadDecimal<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        match JsonStoredNumeric.Read<'R> &context with
        | ValueSome value -> ExactNumber.toDecimal value
        | ValueNone -> failwithf "Cannot convert Null into %s" typeof<decimal>.FullName
    static member internal ReadBigInteger<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>) =
        match JsonStoredNumeric.Read<'R> &context with
        | ValueSome value -> ExactNumber.toBigInteger value
        | ValueNone -> failwithf "Cannot convert Null into %s" typeof<BigInteger>.FullName
