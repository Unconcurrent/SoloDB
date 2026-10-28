namespace SoloDatabase

open System
open System.Numerics
open System.Runtime.InteropServices
open SoloDatabase.Numerics

/// Converts unquoted canonical stored numerical keys to their requested native values.
[<AbstractClass; Sealed>]
type StoredNumeric private () =
    /// Decodes a stored key using Decimal rounding and range semantics.
    static member DecodeDecimal(key: ReadOnlySpan<char>): decimal =
        ExactNumber.toDecimal(NumericKey.decodeSpan key)

    /// Decodes a stored key using Decimal rounding and range semantics.
    static member DecodeDecimal(key: string): decimal =
        if isNull key then nullArg (nameof key)
        StoredNumeric.DecodeDecimal(key.AsSpan())

    /// Decodes a stored key, truncating any fractional part toward zero.
    static member DecodeBigInteger(key: ReadOnlySpan<char>): BigInteger =
        ExactNumber.toBigInteger(NumericKey.decodeSpan key)

    /// Decodes a stored key, truncating any fractional part toward zero.
    static member DecodeBigInteger(key: string): BigInteger =
        if isNull key then nullArg (nameof key)
        StoredNumeric.DecodeBigInteger(key.AsSpan())

    /// Returns false for malformed keys or values outside the requested Decimal range.
    static member TryDecodeDecimal(key: ReadOnlySpan<char>, [<Out>] value: byref<decimal>): bool =
        value <- Decimal.Zero
        try
            value <- StoredNumeric.DecodeDecimal(key)
            true
        with
        | :? FormatException | :? OverflowException -> false

    /// Returns false for null, malformed keys or values outside the requested Decimal range.
    static member TryDecodeDecimal(key: string, [<Out>] value: byref<decimal>): bool =
        if isNull key then value <- Decimal.Zero; false
        else StoredNumeric.TryDecodeDecimal(key.AsSpan(),&value)

    /// Returns false for malformed keys or an unrepresentable encoded exponent.
    static member TryDecodeBigInteger(key: ReadOnlySpan<char>, [<Out>] value: byref<BigInteger>): bool =
        value <- BigInteger.Zero
        try
            value <- StoredNumeric.DecodeBigInteger(key)
            true
        with
        | :? FormatException | :? OverflowException -> false

    /// Returns false for null, malformed keys or an unrepresentable encoded exponent.
    static member TryDecodeBigInteger(key: string, [<Out>] value: byref<BigInteger>): bool =
        if isNull key then value <- BigInteger.Zero; false
        else StoredNumeric.TryDecodeBigInteger(key.AsSpan(),&value)
