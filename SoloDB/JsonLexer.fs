namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Globalization
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text

/// <summary>
/// Represents the different types of tokens that can be found in a JSON string.
/// </summary>
type internal Token =
    /// <summary>Represents a JSON null literal.</summary>
    | NullToken
    /// <summary>Represents an opening brace '{'.</summary>
    | OpenBrace
    /// <summary>Represents a closing brace '}'.</summary>
    | CloseBrace
    /// <summary>Represents an opening bracket '['.</summary>
    | OpenBracket
    /// <summary>Represents a closing bracket ']'.</summary>
    | CloseBracket
    /// <summary>Represents a comma ','.</summary>
    | Comma
    /// <summary>Represents a colon ':'.</summary>
    | Colon
    /// <summary>Represents a string literal.</summary>
    | StringToken of string
    /// <summary>Represents a numeric value.</summary>
    | NumberToken of decimal
    /// <summary>Represents a boolean literal.</summary>
    | BooleanToken of bool
    /// <summary>Represents the end of the input string.</summary>
    | EndOfInput

/// The "constructor surface" the tokenizer needs from whatever JSON value representation you use.
type internal JsonValueParserApi<'T> =
    {
        Null   : unit -> 'T
        Bool   : bool -> 'T
        Str    : string -> 'T
        Num    : decimal -> 'T
        Arr    : IList<'T> -> 'T
        Obj    : IDictionary<string, 'T> -> 'T
    }

[<Struct>]
type internal CommonLiteral =
    | Null
    | NullUpper
    | True
    | False
    | Infinity
    | NaN

type internal IJsonReader =
    abstract member TryPeekAsciiByte : ReadOnlySpan<byte> * int -> ValueOption<byte>
    abstract member TryMatchCommonLiterals : ReadOnlySpan<byte> * int -> ValueOption<struct (CommonLiteral * int)>
    abstract member DecodeCodePointAt : ReadOnlySpan<byte> * int -> struct (int * int)
    abstract member TryReadHex4 : ReadOnlySpan<byte> * int -> ValueOption<int>
    abstract member ParseNumber : ReadOnlySpan<byte> * int * int -> decimal

module internal JsonLexerConstants =
    let packedNull = MemoryMarshal.Read<uint32>("null"B.AsSpan())
    let packedNULL = MemoryMarshal.Read<uint32>("NULL"B.AsSpan())
    let packedTrue = MemoryMarshal.Read<uint32>("true"B.AsSpan())
    let packedFals = MemoryMarshal.Read<uint32>("fals"B.AsSpan())
    let packedNaN = MemoryMarshal.Read<uint32>(ReadOnlySpan<byte>([| 'N'B; 'a'B; 'N'B; 0uy |]))
    let packedInfinity = MemoryMarshal.Read<uint64>("Infinity"B.AsSpan())
    let packedNullUtf16 = MemoryMarshal.Read<uint64>(MemoryMarshal.AsBytes("null".AsSpan()))
    let packedNullUpperUtf16 = MemoryMarshal.Read<uint64>(MemoryMarshal.AsBytes("NULL".AsSpan()))
    let packedTrueUtf16 = MemoryMarshal.Read<uint64>(MemoryMarshal.AsBytes("true".AsSpan()))
    let packedFalsUtf16 = MemoryMarshal.Read<uint64>(MemoryMarshal.AsBytes("fals".AsSpan()))
    let packedNaNUtf16 =
        MemoryMarshal.Read<uint64>(ReadOnlySpan<byte>([| byte 'N'; 0uy; byte 'a'; 0uy; byte 'N'; 0uy; 0uy; 0uy |]))
    let packedInfiUtf16 = MemoryMarshal.Read<uint64>(MemoryMarshal.AsBytes("Infi".AsSpan()))
    let packedNityUtf16 = MemoryMarshal.Read<uint64>(MemoryMarshal.AsBytes("nity".AsSpan()))

module internal Utf16Helpers =
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline utf16CharLength (input: ReadOnlySpan<byte>) =
        input.Length >>> 1

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline readUtf16Char (input: ReadOnlySpan<byte>) (charIndex: int) =
        let byteIndex = charIndex <<< 1
        MemoryMarshal.Read<char>(input.Slice(byteIndex, 2))

module internal NumberParsing =
    let pow10 =
        [|
            1M
            10M
            100M
            1000M
            10000M
            100000M
            1000000M
            10000000M
            100000000M
            1000000000M
            10000000000M
            100000000000M
            1000000000000M
            10000000000000M
            100000000000000M
            1000000000000000M
            10000000000000000M
            100000000000000000M
            1000000000000000000M
            10000000000000000000M
            100000000000000000000M
            1000000000000000000000M
            10000000000000000000000M
            100000000000000000000000M
            1000000000000000000000000M
            10000000000000000000000000M
            100000000000000000000000000M
            1000000000000000000000000000M
            10000000000000000000000000000M
        |]

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline isDigitByte (b: byte) =
        b >= '0'B && b <= '9'B

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline isDigitChar (c: char) =
        c >= '0' && c <= '9'

    let inline scaleUp (value: decimal) (exp: int) =
        let mutable v = value
        let mutable e = exp
        while e > 0 do
            let step = if e > 28 then 28 else e
            v <- v * pow10.[step]
            e <- e - step
        v

    let inline scaleDown (value: decimal) (exp: int) =
        let mutable v = value
        let mutable e = exp
        while e > 0 do
            let step = if e > 28 then 28 else e
            v <- v / pow10.[step]
            e <- e - step
        v

    let parseDecimalAsciiBytes (span: ReadOnlySpan<byte>) =
        let len = span.Length
        if len = 0 then raise (FormatException "Invalid number")

        let mutable i = 0
        let mutable sign = 1
        let b0 = span.[0]
        if b0 = '-'B then
            sign <- -1
            i <- 1
        elif b0 = '+'B then
            i <- 1

        let mutable value = 0M
        let mutable intDigits = 0
        while i < len && isDigitByte span.[i] do
            value <- value * 10M + decimal (int span.[i] - int '0'B)
            intDigits <- intDigits + 1
            i <- i + 1

        let mutable fracDigits = 0
        if i < len && span.[i] = '.'B then
            i <- i + 1
            while i < len && isDigitByte span.[i] do
                value <- value * 10M + decimal (int span.[i] - int '0'B)
                fracDigits <- fracDigits + 1
                i <- i + 1

        if intDigits = 0 && fracDigits = 0 then
            raise (FormatException "Invalid number")

        let mutable exp = 0
        let mutable expSign = 1
        if i < len && (span.[i] = 'e'B || span.[i] = 'E'B) then
            i <- i + 1
            if i >= len then raise (FormatException "Invalid number")
            if span.[i] = '+'B || span.[i] = '-'B then
                expSign <- if span.[i] = '-'B then -1 else 1
                i <- i + 1
            if i >= len then raise (FormatException "Invalid number")
            let mutable expDigits = 0
            while i < len && isDigitByte span.[i] do
                exp <- (exp * 10) + int (span.[i] - '0'B)
                expDigits <- expDigits + 1
                i <- i + 1
            if expDigits = 0 then raise (FormatException "Invalid number")
            exp <- exp * expSign

        if i <> len then raise (FormatException "Invalid number")

        let mutable scale = fracDigits - exp
        let mutable result = value
        if scale > 0 then
            result <- scaleDown result scale
        elif scale < 0 then
            result <- scaleUp result (-scale)

        if sign < 0 then -result else result

    let parseDecimalAsciiUtf16 (input: ReadOnlySpan<byte>) (startIndex: int) (length: int) =
        let endIndex = startIndex + length
        if length <= 0 then raise (FormatException "Invalid number")

        let mutable i = startIndex
        let mutable sign = 1
        let c0 = Utf16Helpers.readUtf16Char input i
        if c0 = '-' then
            sign <- -1
            i <- i + 1
        elif c0 = '+' then
            i <- i + 1

        let mutable value = 0M
        let mutable intDigits = 0
        while i < endIndex && isDigitChar (Utf16Helpers.readUtf16Char input i) do
            value <- value * 10M + decimal (int (Utf16Helpers.readUtf16Char input i) - int '0')
            intDigits <- intDigits + 1
            i <- i + 1

        let mutable fracDigits = 0
        if i < endIndex && Utf16Helpers.readUtf16Char input i = '.' then
            i <- i + 1
            while i < endIndex && isDigitChar (Utf16Helpers.readUtf16Char input i) do
                value <- value * 10M + decimal (int (Utf16Helpers.readUtf16Char input i) - int '0')
                fracDigits <- fracDigits + 1
                i <- i + 1

        if intDigits = 0 && fracDigits = 0 then
            raise (FormatException "Invalid number")

        let mutable exp = 0
        let mutable expSign = 1
        if i < endIndex then
            let eChar = Utf16Helpers.readUtf16Char input i
            if eChar = 'e' || eChar = 'E' then
                i <- i + 1
                if i >= endIndex then raise (FormatException "Invalid number")
                let signChar = Utf16Helpers.readUtf16Char input i
                if signChar = '+' || signChar = '-' then
                    expSign <- if signChar = '-' then -1 else 1
                    i <- i + 1
                if i >= endIndex then raise (FormatException "Invalid number")
                let mutable expDigits = 0
                while i < endIndex && isDigitChar (Utf16Helpers.readUtf16Char input i) do
                    exp <- (exp * 10) + int (Utf16Helpers.readUtf16Char input i) - int '0'
                    expDigits <- expDigits + 1
                    i <- i + 1
                if expDigits = 0 then raise (FormatException "Invalid number")
                exp <- exp * expSign

        if i <> endIndex then raise (FormatException "Invalid number")

        let mutable scale = fracDigits - exp
        let mutable result = value
        if scale > 0 then
            result <- scaleDown result scale
        elif scale < 0 then
            result <- scaleUp result (-scale)

        if sign < 0 then -result else result

[<Struct>]
type internal Utf8Reader =
    interface IJsonReader with
        member _.TryPeekAsciiByte(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            if codeUnitIndex < input.Length then
                let b = input.[codeUnitIndex]
                if b < 0x80uy then ValueSome b else ValueNone
            else
                ValueNone

        member _.TryMatchCommonLiterals(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            if codeUnitIndex >= input.Length then
                ValueNone
            else
                let b0 = input.[codeUnitIndex]
                if b0 = 'n'B || b0 = 't'B || b0 = 'f'B || b0 = 'N'B || b0 = 'I'B then
                    if b0 = 'I'B then
                        if codeUnitIndex + 8 <= input.Length then
                            let word = MemoryMarshal.Read<uint64>(input.Slice(codeUnitIndex, 8))
                            if word = JsonLexerConstants.packedInfinity then ValueSome (struct (CommonLiteral.Infinity, 8)) else ValueNone
                        else
                            ValueNone
                    elif b0 = 'N'B then
                        if codeUnitIndex + 4 <= input.Length then
                            let word = MemoryMarshal.Read<uint32>(input.Slice(codeUnitIndex, 4))
                            if word = JsonLexerConstants.packedNULL then ValueSome (struct (CommonLiteral.NullUpper, 4))
                            elif word &&& 0x00FFFFFFu = JsonLexerConstants.packedNaN then ValueSome (struct (CommonLiteral.NaN, 3))
                            else ValueNone
                        elif codeUnitIndex + 3 <= input.Length then
                            if input.[codeUnitIndex] = 'N'B && input.[codeUnitIndex + 1] = 'a'B && input.[codeUnitIndex + 2] = 'N'B then
                                ValueSome (struct (CommonLiteral.NaN, 3))
                            else
                                ValueNone
                        else
                            ValueNone
                    else
                        if codeUnitIndex + 4 <= input.Length then
                            let word = MemoryMarshal.Read<uint32>(input.Slice(codeUnitIndex, 4))
                            if (word &&& 0x80808080u) = 0u then
                                if word = JsonLexerConstants.packedNull then ValueSome (struct (CommonLiteral.Null, 4))
                                elif word = JsonLexerConstants.packedTrue then ValueSome (struct (CommonLiteral.True, 4))
                                elif word = JsonLexerConstants.packedFals then
                                    if codeUnitIndex + 5 <= input.Length && input.[codeUnitIndex + 4] = 'e'B then
                                        ValueSome (struct (CommonLiteral.False, 5))
                                    else
                                        ValueNone
                                else
                                    ValueNone
                            else
                                ValueNone
                        else
                            ValueNone
                else
                    ValueNone

        member _.DecodeCodePointAt(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            let len = input.Length
            let b0 = input.[codeUnitIndex]
            if b0 < 0x80uy then
                struct (int b0, 1)
            elif (b0 &&& 0xE0uy) = 0xC0uy then
                if codeUnitIndex + 1 >= len then failwith "Invalid UTF-8 sequence"
                let b1 = input.[codeUnitIndex + 1]
                if (b1 &&& 0xC0uy) <> 0x80uy then failwith "Invalid UTF-8 continuation byte"
                let codePoint = (((int b0) &&& 0x1F) <<< 6) ||| ((int b1) &&& 0x3F)
                if codePoint < 0x80 then failwith "Overlong UTF-8 sequence"
                struct (codePoint, 2)
            elif (b0 &&& 0xF0uy) = 0xE0uy then
                if codeUnitIndex + 2 >= len then failwith "Invalid UTF-8 sequence"
                let b1 = input.[codeUnitIndex + 1]
                let b2 = input.[codeUnitIndex + 2]
                if (b1 &&& 0xC0uy) <> 0x80uy || (b2 &&& 0xC0uy) <> 0x80uy then
                    failwith "Invalid UTF-8 continuation byte"
                let codePoint =
                    (((int b0) &&& 0x0F) <<< 12) |||
                    (((int b1) &&& 0x3F) <<< 6) |||
                    ((int b2) &&& 0x3F)
                if codePoint < 0x800 then failwith "Overlong UTF-8 sequence"
                if codePoint >= 0xD800 && codePoint <= 0xDFFF then
                    failwith "Invalid UTF-8 surrogate code point"
                struct (codePoint, 3)
            elif (b0 &&& 0xF8uy) = 0xF0uy then
                if codeUnitIndex + 3 >= len then failwith "Invalid UTF-8 sequence"
                let b1 = input.[codeUnitIndex + 1]
                let b2 = input.[codeUnitIndex + 2]
                let b3 = input.[codeUnitIndex + 3]
                if (b1 &&& 0xC0uy) <> 0x80uy || (b2 &&& 0xC0uy) <> 0x80uy || (b3 &&& 0xC0uy) <> 0x80uy then
                    failwith "Invalid UTF-8 continuation byte"
                let codePoint =
                    (((int b0) &&& 0x07) <<< 18) |||
                    (((int b1) &&& 0x3F) <<< 12) |||
                    (((int b2) &&& 0x3F) <<< 6) |||
                    ((int b3) &&& 0x3F)
                if codePoint < 0x10000 || codePoint > 0x10FFFF then
                    failwith "Invalid UTF-8 code point"
                struct (codePoint, 4)
            else
                failwith "Invalid UTF-8 leading byte"

        member _.TryReadHex4(input: ReadOnlySpan<byte>, hexStartIndex: int) =
            let inline hexValue (b: byte) =
                if b >= '0'B && b <= '9'B then int b - int '0'B
                elif b >= 'a'B && b <= 'f'B then 10 + int b - int 'a'B
                elif b >= 'A'B && b <= 'F'B then 10 + int b - int 'A'B
                else -1

            if hexStartIndex + 4 <= input.Length then
                let d0 = hexValue input.[hexStartIndex]
                let d1 = hexValue input.[hexStartIndex + 1]
                let d2 = hexValue input.[hexStartIndex + 2]
                let d3 = hexValue input.[hexStartIndex + 3]
                if d0 < 0 || d1 < 0 || d2 < 0 || d3 < 0 then
                    ValueNone
                else
                    ValueSome ((d0 <<< 12) ||| (d1 <<< 8) ||| (d2 <<< 4) ||| d3)
            else
                ValueNone

        member _.ParseNumber(input: ReadOnlySpan<byte>, startIndex: int, length: int) =
            let slice = input.Slice(startIndex, length)

            if slice.IsEmpty then raise (FormatException "Invalid number")

            let mutable output = 0.0m
            let mutable bytesRead = 0
            let mutable parsed = false

            let spanToParse =
                if slice.[0] = '+'B then
                    slice.Slice(1)
                else
                    slice

            if not spanToParse.IsEmpty then
                let ok = System.Buffers.Text.Utf8Parser.TryParse(spanToParse, &output, &bytesRead)
                parsed <- ok && bytesRead = spanToParse.Length

            if parsed then output else NumberParsing.parseDecimalAsciiBytes slice

[<Struct>]
type internal Utf16Reader =
    interface IJsonReader with
        member _.TryPeekAsciiByte(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            let byteIndex = codeUnitIndex <<< 1
            if byteIndex + 1 < input.Length then
                let ch = Utf16Helpers.readUtf16Char input codeUnitIndex
                if ch <= '\u007F' then ValueSome (byte ch) else ValueNone
            else
                ValueNone

        member _.TryMatchCommonLiterals(inputUtf16: ReadOnlySpan<byte>, charIndex: int) =
            // charIndex is in UTF-16 code units (chars)
            let byteIndex = charIndex <<< 1

            if byteIndex >= inputUtf16.Length then
                ValueNone
            elif BitConverter.IsLittleEndian then
                if byteIndex + 8 <= inputUtf16.Length then
                    let x = MemoryMarshal.Read<uint64>(inputUtf16.Slice(byteIndex, 8))
                    if (x &&& 0xFF80FF80FF80FF80UL) <> 0UL then
                        ValueNone
                    else
                        if x = JsonLexerConstants.packedNullUtf16 then ValueSome (struct (CommonLiteral.Null, 4))
                        elif x = JsonLexerConstants.packedNullUpperUtf16 then ValueSome (struct (CommonLiteral.NullUpper, 4))
                        elif x = JsonLexerConstants.packedTrueUtf16 then ValueSome (struct (CommonLiteral.True, 4))
                        elif x = JsonLexerConstants.packedFalsUtf16 then
                            if byteIndex + 10 <= inputUtf16.Length then
                                let eWord = MemoryMarshal.Read<uint16>(inputUtf16.Slice(byteIndex + 8, 2))
                                if eWord = uint16 'e' then ValueSome (struct (CommonLiteral.False, 5)) else ValueNone
                            else
                                ValueNone
                        elif byteIndex + 16 <= inputUtf16.Length then
                            let x2 = MemoryMarshal.Read<uint64>(inputUtf16.Slice(byteIndex + 8, 8))
                            if x = JsonLexerConstants.packedInfiUtf16 && x2 = JsonLexerConstants.packedNityUtf16 then
                                ValueSome (struct (CommonLiteral.Infinity, 8))
                            else
                                if x &&& 0x0000FFFFFFFFFFFFUL = JsonLexerConstants.packedNaNUtf16 then
                                    ValueSome (struct (CommonLiteral.NaN, 3))
                                else
                                    ValueNone
                        else
                            if x &&& 0x0000FFFFFFFFFFFFUL = JsonLexerConstants.packedNaNUtf16 then
                                ValueSome (struct (CommonLiteral.NaN, 3))
                            else
                                ValueNone
                else
                    ValueNone
            else
                let charsLength = Utf16Helpers.utf16CharLength inputUtf16
                if charIndex >= charsLength then
                    ValueNone
                else
                    let c0 = Utf16Helpers.readUtf16Char inputUtf16 charIndex
                    if c0 = 'n' || c0 = 't' || c0 = 'f' || c0 = 'N' || c0 = 'I' then
                        if c0 = 'I' then
                            if charIndex + 7 < charsLength then
                                let ok =
                                    Utf16Helpers.readUtf16Char inputUtf16 (charIndex) = 'I' &&
                                    Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 1) = 'n' &&
                                    Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 2) = 'f' &&
                                    Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 3) = 'i' &&
                                    Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 4) = 'n' &&
                                    Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 5) = 'i' &&
                                    Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 6) = 't' &&
                                    Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 7) = 'y'
                                if ok then ValueSome (struct (CommonLiteral.Infinity, 8)) else ValueNone
                            else
                                ValueNone
                        elif c0 = 'N' then
                            if charIndex + 3 <= charsLength then
                                if Utf16Helpers.readUtf16Char inputUtf16 (charIndex) = 'N' &&
                                   Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 1) = 'U' &&
                                   Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 2) = 'L' &&
                                   Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 3) = 'L' then
                                    ValueSome (struct (CommonLiteral.NullUpper, 4))
                                elif charIndex + 2 < charsLength &&
                                     Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 1) = 'a' &&
                                     Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 2) = 'N' then
                                    ValueSome (struct (CommonLiteral.NaN, 3))
                                else
                                    ValueNone
                            elif charIndex + 2 < charsLength &&
                                 Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 1) = 'a' &&
                                 Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 2) = 'N' then
                                ValueSome (struct (CommonLiteral.NaN, 3))
                            else
                                ValueNone
                        else
                            if charIndex + 3 < charsLength then
                                if Utf16Helpers.readUtf16Char inputUtf16 (charIndex) = 'n' &&
                                   Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 1) = 'u' &&
                                   Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 2) = 'l' &&
                                   Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 3) = 'l' then
                                    ValueSome (struct (CommonLiteral.Null, 4))
                                elif Utf16Helpers.readUtf16Char inputUtf16 (charIndex) = 't' &&
                                     Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 1) = 'r' &&
                                     Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 2) = 'u' &&
                                     Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 3) = 'e' then
                                    ValueSome (struct (CommonLiteral.True, 4))
                                elif Utf16Helpers.readUtf16Char inputUtf16 (charIndex) = 'f' &&
                                     Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 1) = 'a' &&
                                     Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 2) = 'l' &&
                                     Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 3) = 's' then
                                    if charIndex + 4 < charsLength && Utf16Helpers.readUtf16Char inputUtf16 (charIndex + 4) = 'e' then
                                        ValueSome (struct (CommonLiteral.False, 5))
                                    else
                                        ValueNone
                                else
                                    ValueNone
                            else
                                ValueNone
                    else
                        ValueNone

        member _.DecodeCodePointAt(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            let ch = Utf16Helpers.readUtf16Char input codeUnitIndex
            if Char.IsHighSurrogate(ch) then
                let charsLength = Utf16Helpers.utf16CharLength input
                if codeUnitIndex + 1 >= charsLength then
                    failwith "Invalid high surrogate at end of input"
                let ch2 = Utf16Helpers.readUtf16Char input (codeUnitIndex + 1)
                if not (Char.IsLowSurrogate ch2) then
                    failwith "Invalid low surrogate after high surrogate"
                let codePoint =
                    0x10000 +
                    (((int ch) - 0xD800) <<< 10) +
                    ((int ch2) - 0xDC00)
                struct (codePoint, 2)
            elif Char.IsLowSurrogate(ch) then
                failwith "Unexpected low surrogate"
            else
                struct (int ch, 1)

        member _.TryReadHex4(input: ReadOnlySpan<byte>, hexStartIndex: int) =
            let inline hexValue (ch: char) =
                if ch >= '0' && ch <= '9' then int ch - int '0'
                elif ch >= 'a' && ch <= 'f' then 10 + int ch - int 'a'
                elif ch >= 'A' && ch <= 'F' then 10 + int ch - int 'A'
                else -1

            let charsLength = Utf16Helpers.utf16CharLength input
            if hexStartIndex + 4 <= charsLength then
                let d0 = hexValue (Utf16Helpers.readUtf16Char input hexStartIndex)
                let d1 = hexValue (Utf16Helpers.readUtf16Char input (hexStartIndex + 1))
                let d2 = hexValue (Utf16Helpers.readUtf16Char input (hexStartIndex + 2))
                let d3 = hexValue (Utf16Helpers.readUtf16Char input (hexStartIndex + 3))
                if d0 < 0 || d1 < 0 || d2 < 0 || d3 < 0 then
                    ValueNone
                else
                    ValueSome ((d0 <<< 12) ||| (d1 <<< 8) ||| (d2 <<< 4) ||| d3)
            else
                ValueNone

        member _.ParseNumber(input: ReadOnlySpan<byte>, startIndex: int, length: int) =
            NumberParsing.parseDecimalAsciiUtf16 input startIndex length

/// Tokenizer state + the value factory API
[<Struct; IsByRefLike>]
type internal ParserContext<'T, 'R when 'R : struct and 'R :> IJsonReader> =
    {
        input : ReadOnlySpan<byte>
        codeUnitLength : int
        mutable codeUnitIndex : int
        sb : StringBuilder
        api : JsonValueParserApi<'T>
        reader : 'R
    }

type internal ParserContextFactoryStr<'T> = delegate of string -> ParserContext<'T, Utf16Reader>
type internal ParserContextFactoryUtf16<'T> = delegate of ReadOnlySpan<char> -> ParserContext<'T, Utf16Reader>
type internal ParserContextFactoryUtf8<'T> = delegate of ReadOnlySpan<byte> -> ParserContext<'T, Utf8Reader>

module internal JsonParser =

    let inline fromString (api: JsonValueParserApi<'T>) : ParserContextFactoryStr<'T> =
        ParserContextFactoryStr<'T>(fun (source: string) ->
            let span = source.AsSpan()
            { input = MemoryMarshal.AsBytes span
              codeUnitLength = span.Length
              codeUnitIndex = 0
              sb = StringBuilder()
              api = api
              reader = Utf16Reader() })

    let inline fromUtf16Span (api: JsonValueParserApi<'T>) : ParserContextFactoryUtf16<'T> =
        ParserContextFactoryUtf16<'T>(fun (source: ReadOnlySpan<char>) ->
            { input = MemoryMarshal.AsBytes source
              codeUnitLength = source.Length
              codeUnitIndex = 0
              sb = StringBuilder()
              api = api
              reader = Utf16Reader() })

    let inline fromUtf8Span (api: JsonValueParserApi<'T>) : ParserContextFactoryUtf8<'T> =
        ParserContextFactoryUtf8<'T>(fun (source: ReadOnlySpan<byte>) ->
            { input = source
              codeUnitLength = source.Length
              codeUnitIndex = 0
              sb = StringBuilder()
              api = api
              reader = Utf8Reader() })

    let inline appendCodePoint (sb: StringBuilder) (codePoint: int) =
        if codePoint <= 0xFFFF then
            sb.Append(char codePoint) |> ignore
        else
            let cp = codePoint - 0x10000
            let high = 0xD800 + (cp >>> 10)
            let low = 0xDC00 + (cp &&& 0x3FF)
            sb.Append(char high) |> ignore
            sb.Append(char low) |> ignore

    let inline isJsonWhitespaceByte (b: byte) =
        b = ' 'B || b = '\t'B || b = '\n'B || b = '\r'B || b = '\u000B'B || b = '\u000C'B

    let inline isJsonWhitespaceCodePoint (codePoint: int) =
        match codePoint with
        | 0x0009 | 0x000A | 0x000B | 0x000C | 0x000D
        | 0x0020 | 0x00A0 | 0x1680 | 0x180E | 0x2028 | 0x2029 | 0x202F | 0x205F | 0x3000 | 0xFEFF -> true
        | cp when cp >= 0x2000 && cp <= 0x200A -> true
        | _ -> false

    let inline isDigitOrMinusByte (b: byte) =
        b = '-'B || (b >= '0'B && b <= '9'B)

    let inline isNumberCharByte (b: byte) =
        isDigitOrMinusByte b || b = '.'B || b = 'e'B || b = 'E'B || b = '+'B

    let inline isAsciiIdentifierStart (b: byte) =
        (b >= 'A'B && b <= 'Z'B) || (b >= 'a'B && b <= 'z'B) || b = '_'B || b = '$'B

    let inline getUnicodeCategory (codePoint: int) =
        #if NETSTANDARD2_1
        CharUnicodeInfo.GetUnicodeCategory(codePoint)
        #else
        if codePoint <= 0xFFFF then
            CharUnicodeInfo.GetUnicodeCategory(char codePoint)
        else
            let s = Char.ConvertFromUtf32 codePoint
            CharUnicodeInfo.GetUnicodeCategory(s, 0)
        #endif


    let inline isIdentifierStartCodePoint (codePoint: int) =
        if codePoint = int '_' || codePoint = int '$' then true else
        match getUnicodeCategory codePoint with
        | UnicodeCategory.UppercaseLetter
        | UnicodeCategory.LowercaseLetter
        | UnicodeCategory.TitlecaseLetter
        | UnicodeCategory.ModifierLetter
        | UnicodeCategory.OtherLetter
        | UnicodeCategory.LetterNumber -> true
        | _ -> false

    let inline isIdentifierCharCodePoint (codePoint: int) =
        if codePoint = int '_' || codePoint = int '$' || codePoint = 0x200C || codePoint = 0x200D then true else
        match getUnicodeCategory codePoint with
        | UnicodeCategory.UppercaseLetter
        | UnicodeCategory.LowercaseLetter
        | UnicodeCategory.TitlecaseLetter
        | UnicodeCategory.ModifierLetter
        | UnicodeCategory.OtherLetter
        | UnicodeCategory.LetterNumber
        | UnicodeCategory.DecimalDigitNumber
        | UnicodeCategory.ConnectorPunctuation
        | UnicodeCategory.NonSpacingMark
        | UnicodeCategory.SpacingCombiningMark -> true
        | _ -> false

    let inline private isIdentifierCharAt<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) (probeCodeUnitIndex: int) =
        if probeCodeUnitIndex >= ctx.codeUnitLength then
            false
        else
            match ctx.reader.TryPeekAsciiByte(ctx.input, probeCodeUnitIndex) with
            | ValueSome b -> isIdentifierCharCodePoint (int b)
            | ValueNone ->
                let struct (cp, _consumed) = ctx.reader.DecodeCodePointAt(ctx.input, probeCodeUnitIndex)
                isIdentifierCharCodePoint cp

    let private parseIdentifierFallback<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) (startIndex: int) : struct (Token * int) =
        let sb = ctx.sb.Clear()
        let mutable i = startIndex
        let mutable first = true
        let mutable finished = false

        while i < ctx.codeUnitLength && not finished do
            match ctx.reader.TryPeekAsciiByte(ctx.input, i) with
            | ValueSome '\\'B ->
                if i + 1 >= ctx.codeUnitLength then
                    failwith "Invalid identifier escape sequence"
                match ctx.reader.TryPeekAsciiByte(ctx.input, i + 1) with
                | ValueSome 'u'B ->
                    let mutable codePoint = 0
                    let mutable consumed = 0
                    if i + 2 < ctx.codeUnitLength && ctx.reader.TryPeekAsciiByte(ctx.input, i + 2) = ValueSome '{'B then
                        let mutable j = i + 3
                        let mutable hasDigit = false
                        let mutable closed = false
                        while j < ctx.codeUnitLength && not closed do
                            match ctx.reader.TryPeekAsciiByte(ctx.input, j) with
                            | ValueSome '}'B ->
                                closed <- true
                                j <- j + 1
                            | ValueSome h ->
                                let digit =
                                    if h >= '0'B && h <= '9'B then int h - int '0'B
                                    elif h >= 'a'B && h <= 'f'B then 10 + int h - int 'a'B
                                    elif h >= 'A'B && h <= 'F'B then 10 + int h - int 'A'B
                                    else -1
                                if digit < 0 then
                                    failwith "Invalid identifier escape sequence"
                                hasDigit <- true
                                let next = (codePoint <<< 4) ||| digit
                                if next > 0x10FFFF then failwith "Invalid Unicode escape sequence"
                                codePoint <- next
                                j <- j + 1
                            | ValueNone ->
                                failwith "Invalid identifier escape sequence"

                        if not closed || not hasDigit then
                            failwith "Invalid identifier escape sequence"
                        consumed <- j - i
                    else
                        match ctx.reader.TryReadHex4(ctx.input, i + 2) with
                        | ValueSome cp ->
                            codePoint <- cp
                            consumed <- 6
                        | ValueNone ->
                            failwith "Invalid identifier escape sequence"

                    if codePoint >= 0xD800 && codePoint <= 0xDFFF then
                        failwith "Invalid Unicode escape sequence"

                    let ok = if first then isIdentifierStartCodePoint codePoint else isIdentifierCharCodePoint codePoint
                    if not ok then
                        failwith "Invalid identifier escape sequence"

                    appendCodePoint sb codePoint
                    i <- i + consumed
                    first <- false
                | _ ->
                    failwith "Invalid identifier escape sequence"
            | _ ->
                let struct (cp, consumed) = ctx.reader.DecodeCodePointAt(ctx.input, i)
                let ok = if first then isIdentifierStartCodePoint cp else isIdentifierCharCodePoint cp
                if not ok then
                    finished <- true
                else
                    appendCodePoint sb cp
                    i <- i + consumed
                    first <- false

        let txt = sb.ToString()

        struct (
            match txt with
            | "null" | "NULL" -> NullToken
            | "true" -> BooleanToken true
            | "false" -> BooleanToken false
            | "Infinity" -> NumberToken Decimal.MaxValue
            | "NaN" -> NumberToken Decimal.Zero
            | other -> StringToken other
            , i
        )

    let private readString<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) (quote: byte) : Token =
        let sb = ctx.sb.Clear()
        let mutable i = ctx.codeUnitIndex + 1
        let mutable finished = false

        while i < ctx.codeUnitLength && not finished do
            match ctx.reader.TryPeekAsciiByte(ctx.input, i) with
            | ValueSome b when b = quote ->
                finished <- true
            | ValueSome b when b = '\\'B ->
                if i + 1 >= ctx.codeUnitLength then
                    failwith "Invalid escape sequence"
                match ctx.reader.TryPeekAsciiByte(ctx.input, i + 1) with
                | ValueSome esc ->
                    match esc with
                    | '\\'B -> sb.Append('\\') |> ignore; i <- i + 2
                    | '"'B -> sb.Append('"') |> ignore; i <- i + 2
                    | '\''B -> sb.Append('\'') |> ignore; i <- i + 2
                    | '/'B  -> sb.Append('/') |> ignore; i <- i + 2
                    | 'b'B  -> sb.Append('\b') |> ignore; i <- i + 2
                    | 'f'B  -> sb.Append('\f') |> ignore; i <- i + 2
                    | 'n'B  -> sb.Append('\n') |> ignore; i <- i + 2
                    | 'r'B  -> sb.Append('\r') |> ignore; i <- i + 2
                    | 't'B  -> sb.Append('\t') |> ignore; i <- i + 2
                    | 'v'B  -> sb.Append('\u000B') |> ignore; i <- i + 2
                    | '0'B  ->
                        match ctx.reader.TryPeekAsciiByte(ctx.input, i + 2) with
                        | ValueSome d when d >= '0'B && d <= '9'B ->
                            failwith "Invalid \\0 escape sequence"
                        | _ ->
                            sb.Append('\u0000') |> ignore
                            i <- i + 2
                    | 'u'B  ->
                        if i + 2 < ctx.codeUnitLength && ctx.reader.TryPeekAsciiByte(ctx.input, i + 2) = ValueSome '{'B then
                            let mutable j = i + 3
                            let mutable codePoint = 0
                            let mutable hasDigit = false
                            let mutable closed = false
                            while j < ctx.codeUnitLength && not closed do
                                match ctx.reader.TryPeekAsciiByte(ctx.input, j) with
                                | ValueSome '}'B ->
                                    closed <- true
                                    j <- j + 1
                                | ValueSome h ->
                                    let digit =
                                        if h >= '0'B && h <= '9'B then int h - int '0'B
                                        elif h >= 'a'B && h <= 'f'B then 10 + int h - int 'a'B
                                        elif h >= 'A'B && h <= 'F'B then 10 + int h - int 'A'B
                                        else -1
                                    if digit < 0 then
                                        failwith "Invalid Unicode escape sequence"
                                    hasDigit <- true
                                    let next = (codePoint <<< 4) ||| digit
                                    if next > 0x10FFFF then failwith "Invalid Unicode escape sequence"
                                    codePoint <- next
                                    j <- j + 1
                                | ValueNone ->
                                    failwith "Invalid Unicode escape sequence"

                            if not closed || not hasDigit then
                                failwith "Invalid Unicode escape sequence"
                            if codePoint >= 0xD800 && codePoint <= 0xDFFF then
                                failwith "Invalid Unicode escape sequence"
                            appendCodePoint sb codePoint
                            i <- j
                        else
                            match ctx.reader.TryReadHex4(ctx.input, i + 2) with
                            | ValueSome code1 ->
                                i <- i + 6
                                if code1 >= 0xD800 && code1 <= 0xDBFF then
                                    if i + 1 < ctx.codeUnitLength then
                                        match ctx.reader.TryPeekAsciiByte(ctx.input, i) with
                                        | ValueSome '\\'B when (ctx.reader.TryPeekAsciiByte(ctx.input, i + 1) = ValueSome 'u'B) ->
                                            match ctx.reader.TryReadHex4(ctx.input, i + 2) with
                                            | ValueSome code2 when code2 >= 0xDC00 && code2 <= 0xDFFF ->
                                                let codePoint =
                                                    0x10000 +
                                                    ((code1 - 0xD800) <<< 10) +
                                                    (code2 - 0xDC00)
                                                appendCodePoint sb codePoint
                                                i <- i + 6
                                            | _ ->
                                                failwith "Invalid low surrogate"
                                        | _ ->
                                            failwith "Expected low surrogate after high surrogate"
                                    else
                                        failwith "Expected low surrogate after high surrogate"
                                elif code1 >= 0xDC00 && code1 <= 0xDFFF then
                                    failwith "Unexpected low surrogate"
                                else
                                    appendCodePoint sb code1
                            | ValueNone ->
                                failwith "Invalid Unicode escape sequence"
                    | 'x'B ->
                        let inline hexValue (b: byte) =
                            if b >= '0'B && b <= '9'B then int b - int '0'B
                            elif b >= 'a'B && b <= 'f'B then 10 + int b - int 'a'B
                            elif b >= 'A'B && b <= 'F'B then 10 + int b - int 'A'B
                            else -1
                        if i + 3 >= ctx.codeUnitLength then
                            failwith "Invalid hex escape sequence"
                        match ctx.reader.TryPeekAsciiByte(ctx.input, i + 2), ctx.reader.TryPeekAsciiByte(ctx.input, i + 3) with
                        | ValueSome h1, ValueSome h2 ->
                            let d1 = hexValue h1
                            let d2 = hexValue h2
                            if d1 < 0 || d2 < 0 then
                                failwith "Invalid hex escape sequence"
                            let code = (d1 <<< 4) ||| d2
                            sb.Append(char code) |> ignore
                            i <- i + 4
                        | _ ->
                            failwith "Invalid hex escape sequence"
                    | '\n'B ->
                        i <- i + 2
                    | '\r'B ->
                        if i + 2 < ctx.codeUnitLength && ctx.reader.TryPeekAsciiByte(ctx.input, i + 2) = ValueSome '\n'B then
                            i <- i + 3
                        else
                            i <- i + 2
                    | other ->
                        failwithf "Invalid escape sequence: '%c'" (char other)
                | ValueNone ->
                    if i + 1 >= ctx.codeUnitLength then
                        failwith "Invalid escape sequence"
                    let struct (cp, consumed) = ctx.reader.DecodeCodePointAt(ctx.input, i + 1)
                    if cp = 0x2028 || cp = 0x2029 then
                        i <- i + 1 + consumed
                    else
                        failwith "Invalid escape sequence"
            | ValueSome b ->
                if b = '\n'B || b = '\r'B then
                    failwith "Unterminated string"
                sb.Append(char b) |> ignore
                i <- i + 1
            | ValueNone ->
                let struct (cp, consumed) = ctx.reader.DecodeCodePointAt(ctx.input, i)
                if cp = 0x2028 || cp = 0x2029 then
                    failwith "Unterminated string"
                appendCodePoint sb cp
                i <- i + consumed

        if not finished then failwith "Unterminated string"
        ctx.codeUnitIndex <- i + 1
        StringToken(sb.ToString())

    let rec private readNext<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) : Token =

        RuntimeHelpers.EnsureSufficientExecutionStack()

        if ctx.codeUnitIndex < ctx.codeUnitLength then
            match ctx.reader.TryPeekAsciiByte(ctx.input, ctx.codeUnitIndex) with
            | ValueSome b ->
                match b with
                | '{'B -> ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; OpenBrace
                | '}'B -> ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; CloseBrace
                | '['B -> ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; OpenBracket
                | ']'B -> ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; CloseBracket
                | ','B -> ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; Comma
                | ':'B -> ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; Colon
                | '"'B | '\''B as quote ->
                    readString &ctx quote
                | '/'B ->
                    match ctx.reader.TryPeekAsciiByte(ctx.input, ctx.codeUnitIndex + 1) with
                    | ValueSome '/'B ->
                        ctx.codeUnitIndex <- ctx.codeUnitIndex + 2
                        let mutable i = ctx.codeUnitIndex
                        let mutable finished = false
                        while i < ctx.codeUnitLength && not finished do
                            match ctx.reader.TryPeekAsciiByte(ctx.input, i) with
                            | ValueSome '\n'B
                            | ValueSome '\r'B ->
                                finished <- true
                            | ValueSome _ ->
                                i <- i + 1
                            | ValueNone ->
                                let struct (cp, consumed) = ctx.reader.DecodeCodePointAt(ctx.input, i)
                                if cp = 0x2028 || cp = 0x2029 then
                                    finished <- true
                                else
                                    i <- i + consumed
                        ctx.codeUnitIndex <- i
                        readNext &ctx
                    | ValueSome '*'B ->
                        ctx.codeUnitIndex <- ctx.codeUnitIndex + 2
                        let mutable i = ctx.codeUnitIndex
                        let mutable closed = false
                        while i + 1 < ctx.codeUnitLength && not closed do
                            match ctx.reader.TryPeekAsciiByte(ctx.input, i), ctx.reader.TryPeekAsciiByte(ctx.input, i + 1) with
                            | ValueSome '*'B, ValueSome '/'B ->
                                closed <- true
                                i <- i + 2
                            | _ -> i <- i + 1
                        if not closed then failwith "Unterminated comment"
                        ctx.codeUnitIndex <- i
                        readNext &ctx
                    | _ ->
                        failwithf "Unexpected JSON character: %c, index = %i" (char b) ctx.codeUnitIndex
                | '+'B | '-'B | '.'B | '0'B | '1'B | '2'B | '3'B | '4'B | '5'B | '6'B | '7'B | '8'B | '9'B ->
                    let startIndex = ctx.codeUnitIndex
                    let mutable i = startIndex
                    let negative = b = '-'B
                    let mutable tokenOpt = ValueNone

                    if b = '+'B || b = '-'B then
                        match ctx.reader.TryMatchCommonLiterals(ctx.input, ctx.codeUnitIndex + 1) with
                        | ValueSome struct (lit, len) when not (isIdentifierCharAt &ctx (ctx.codeUnitIndex + 1 + len)) ->
                            ctx.codeUnitIndex <- ctx.codeUnitIndex + 1 + len
                            tokenOpt <-
                                ValueSome (
                                    match lit with
                                    | CommonLiteral.Infinity -> NumberToken (if negative then Decimal.MinValue else Decimal.MaxValue)
                                    | CommonLiteral.NaN -> NumberToken Decimal.Zero
                                    | _ -> failwith "Invalid signed literal"
                                )
                        | _ ->
                            let struct (tok, nextIndex) = parseIdentifierFallback &ctx (ctx.codeUnitIndex + 1)
                            match tok with
                            | NumberToken num when nextIndex > ctx.codeUnitIndex + 1 && not (isIdentifierCharAt &ctx nextIndex) ->
                                ctx.codeUnitIndex <- nextIndex
                                let signed =
                                    if num = Decimal.MaxValue then (if negative then Decimal.MinValue else Decimal.MaxValue)
                                    else num
                                tokenOpt <- ValueSome (NumberToken signed)
                            | _ -> ()

                    match tokenOpt with
                    | ValueSome tok -> tok
                    | ValueNone ->
                        if ctx.codeUnitIndex = startIndex then
                            let mutable hexStartIndex = -1
                            let mutable hexNegative = false

                            if b = '+'B || b = '-'B then
                                match ctx.reader.TryPeekAsciiByte(ctx.input, startIndex + 1) with
                                | ValueSome '0'B ->
                                    match ctx.reader.TryPeekAsciiByte(ctx.input, startIndex + 2) with
                                    | ValueSome 'x'B
                                    | ValueSome 'X'B ->
                                        hexStartIndex <- startIndex + 1
                                        hexNegative <- b = '-'B
                                    | _ -> ()
                                | _ -> ()
                            elif b = '0'B then
                                match ctx.reader.TryPeekAsciiByte(ctx.input, startIndex + 1) with
                                | ValueSome 'x'B
                                | ValueSome 'X'B ->
                                    hexStartIndex <- startIndex
                                    hexNegative <- false
                                | _ -> ()

                            if hexStartIndex >= 0 then
                                let mutable idx = hexStartIndex + 2
                                if idx >= ctx.codeUnitLength then failwith "Invalid hex literal"
                                let mutable acc = 0UL
                                let mutable hasDigit = false
                                let mutable overflow = false
                                let mutable finished = false
                                while idx < ctx.codeUnitLength && not finished && not overflow do
                                    match ctx.reader.TryPeekAsciiByte(ctx.input, idx) with
                                    | ValueSome ch ->
                                        let digit =
                                            if ch >= '0'B && ch <= '9'B then int ch - int '0'B
                                            elif ch >= 'a'B && ch <= 'f'B then 10 + int ch - int 'a'B
                                            elif ch >= 'A'B && ch <= 'F'B then 10 + int ch - int 'A'B
                                            else -1
                                        if digit < 0 then
                                            finished <- true
                                        else
                                            hasDigit <- true
                                            let next = acc * 16UL + uint64 digit
                                            if next < acc then overflow <- true else acc <- next
                                            idx <- idx + 1
                                    | _ ->
                                        finished <- true

                                if not hasDigit then failwith "Invalid hex literal"
                                ctx.codeUnitIndex <- idx
                                let value = if overflow then Decimal.MaxValue else decimal acc
                                tokenOpt <- ValueSome (if hexNegative then NumberToken(-value) else NumberToken(value))

                        match tokenOpt with
                        | ValueSome tok -> tok
                        | ValueNone ->
                            let mutable finished = false
                            while i < ctx.codeUnitLength && not finished do
                                match ctx.reader.TryPeekAsciiByte(ctx.input, i) with
                                | ValueSome nb when isNumberCharByte nb -> i <- i + 1
                                | _ -> finished <- true

                            let length = i - startIndex
                            ctx.codeUnitIndex <- i

                            NumberToken(
                                try ctx.reader.ParseNumber(ctx.input, startIndex, length)
                                with :? OverflowException ->
                                    if negative then Decimal.MinValue else Decimal.MaxValue
                            )

                | '\\'B ->
                    match parseIdentifierFallback &ctx ctx.codeUnitIndex with
                    | struct (tok, i) -> ctx.codeUnitIndex <- i; tok

                | _ when isAsciiIdentifierStart b ->
                    match ctx.reader.TryMatchCommonLiterals(ctx.input, ctx.codeUnitIndex) with
                    | ValueSome struct (lit, len) when not (isIdentifierCharAt &ctx (ctx.codeUnitIndex + len)) ->
                        ctx.codeUnitIndex <- ctx.codeUnitIndex + len
                        match lit with
                        | CommonLiteral.Null
                        | CommonLiteral.NullUpper -> NullToken
                        | CommonLiteral.True -> BooleanToken true
                        | CommonLiteral.False -> BooleanToken false
                        | CommonLiteral.Infinity -> NumberToken Decimal.MaxValue
                        | CommonLiteral.NaN -> NumberToken Decimal.Zero
                    | _ ->
                        match parseIdentifierFallback &ctx ctx.codeUnitIndex with
                        | struct (tok, i) -> ctx.codeUnitIndex <- i; tok

                | _ when isJsonWhitespaceByte b ->
                    ctx.codeUnitIndex <- ctx.codeUnitIndex + 1
                    readNext &ctx

                | _ ->
                    failwithf "Unexpected JSON character: %c, index = %i" (char b) ctx.codeUnitIndex

            | ValueNone ->
                let struct (cp, consumed) = ctx.reader.DecodeCodePointAt(ctx.input, ctx.codeUnitIndex)
                if isJsonWhitespaceCodePoint cp then
                    ctx.codeUnitIndex <- ctx.codeUnitIndex + consumed
                    readNext &ctx
                elif isIdentifierStartCodePoint cp then
                    match parseIdentifierFallback &ctx ctx.codeUnitIndex with
                    | struct (tok, i) -> ctx.codeUnitIndex <- i; tok
                else
                    failwithf "Unexpected JSON character: U+%04X, index = %i" cp ctx.codeUnitIndex
        else
            EndOfInput

    let inline peek<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) : Token =
        let savedIndex = ctx.codeUnitIndex
        try readNext &ctx
        finally ctx.codeUnitIndex <- savedIndex

    let rec private parseMembers<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) (dict: IDictionary<string, 'T>) =
        match readNext &ctx with
        | CloseBrace -> dict
        | token ->
            let keyOpt =
                match token with
                | StringToken s -> ValueSome s
                | NullToken -> ValueSome "null"
                | BooleanToken true -> ValueSome "true"
                | BooleanToken false -> ValueSome "false"
                | _ -> ValueNone

            match keyOpt with
            | ValueNone ->
                failwith "Invalid object syntax"
            | ValueSome key ->
                let colon = readNext &ctx
                if colon <> Colon then failwith "Malformed json."

                let value = parse &ctx
                dict.[key] <- value

                match readNext &ctx with
                | CloseBrace -> dict
                | Comma ->
                    match peek &ctx with
                    | CloseBrace ->
                        ignore (readNext &ctx)
                        dict
                    | _ -> parseMembers &ctx dict
                | _ -> failwith "Malformed json."

    and private parseObject<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) : IDictionary<string, 'T> =
        let dict = JsonHelper.newDefaultDict<'T>()
        parseMembers &ctx dict

    and private parseElements<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) (items: List<'T>) : IList<'T> =
        let itemOpt =
            parseTokensOr &ctx (fun t ->
                match t with
                | CloseBracket -> None
                | other -> failwithf "Invalid list token: %A" other
            )

        match itemOpt with
        | None -> items :> IList<'T>
        | Some item ->
            items.Add item
            match readNext &ctx with
            | CloseBracket -> items :> IList<'T>
            | Comma -> parseElements &ctx items
            | _ -> failwith "Malformed json."

    and private parseArray<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) : IList<'T> =
        let items = List<'T>()
        parseElements &ctx items

    and private parseTokensOr<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) (fallback: Token -> 'T option) : 'T option =
        match readNext &ctx with
        | OpenBrace ->
            let obj = parseObject &ctx
            ctx.api.Obj obj |> Some
        | OpenBracket ->
            let arr = parseArray &ctx
            ctx.api.Arr arr |> Some
        | StringToken str ->
            ctx.api.Str str |> Some
        | NumberToken num ->
            ctx.api.Num num |> Some
        | BooleanToken b ->
            ctx.api.Bool b |> Some
        | NullToken ->
            ctx.api.Null() |> Some
        | other ->
            fallback other

    and private parseTokens<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) : 'T =
        match parseTokensOr &ctx (fun _ -> None) with
        | Some x -> x
        | None -> failwithf "Could not parse the token at index = %i." ctx.codeUnitIndex

    and parse<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) : 'T =
        parseTokens &ctx
