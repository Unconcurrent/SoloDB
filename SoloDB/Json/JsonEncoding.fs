namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Globalization
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text
open Microsoft.FSharp.NativeInterop

#nowarn "9"

/// Numeric syntax retains the source digits until the consumer selects a numeric type.
[<NoEquality; NoComparison>]
type internal NumberLiteral =
    | DecimalLiteral of startIndex: int * length: int
    | HexLiteral of magnitude: uint64 * negative: bool * overflow: bool
    | InfinityLiteral of infinityNegative: bool
    | NaNLiteral

/// <summary>
/// Represents the different types of tokens that can be found in a JSON string.
/// </summary>
[<Struct; NoEquality; NoComparison>]
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
    | StringToken of stringValue: string
    /// <summary>Represents a numeric value.</summary>
    | NumberToken of numberValue: NumberLiteral
    /// <summary>Represents a boolean literal.</summary>
    | BooleanToken of booleanValue: bool
    /// <summary>Represents the end of the input string.</summary>
    | EndOfInput

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
    abstract member AsciiStringRunLength : ReadOnlySpan<byte> * int * byte -> int
    abstract member AsciiString : ReadOnlySpan<byte> * int * int -> string
    abstract member UnescapedStringRunLength : ReadOnlySpan<byte> * int * byte -> int
    abstract member StringRun : ReadOnlySpan<byte> * int * int * bool -> string
    abstract member AppendAsciiStringRun : StringBuilder * ReadOnlySpan<byte> * int * int -> unit
    abstract member DecodeCodePointAt : ReadOnlySpan<byte> * int -> struct (int * int)
    abstract member DecodeCodePointFrom : ReadOnlySpan<byte> * int * int -> struct (int * int)
    abstract member TryReadHex4 : ReadOnlySpan<byte> * int -> ValueOption<int>
    abstract member ParseNumber : ReadOnlySpan<byte> * int * int -> decimal
    abstract member ParseDouble : ReadOnlySpan<byte> * int * int -> double
    abstract member ParseSingle : ReadOnlySpan<byte> * int * int -> single

module internal JsonTextEncoding =
    let private strictUtf8 = UTF8Encoding(false,true)

    let utf8 (source: ReadOnlySpan<byte>, decode: bool) =
        if source.IsEmpty then String.Empty else
        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        if decode then strictUtf8.GetString(source)
        else
            strictUtf8.GetCharCount(source) |> ignore
            String.Empty
        #else
        use pointer = fixed source
        if decode then strictUtf8.GetString(pointer,source.Length)
        else
            strictUtf8.GetCharCount(pointer,source.Length) |> ignore
            String.Empty
        #endif

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

module internal Utf8CodePoints =
    let decodeFrom (input: ReadOnlySpan<byte>) codeUnitIndex (b0: byte) =
        let len = input.Length
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

        member _.AsciiStringRunLength(input, start, quote) =
            let mutable finish = start
            while finish < input.Length && input[finish] < 0x80uy && input[finish] <> quote
                  && input[finish] <> '\\'B && input[finish] <> '\r'B && input[finish] <> '\n'B do
                finish <- finish + 1
            finish - start

        member _.AsciiString(input,start,count) =
            if count = 0 then String.Empty else
            let source = input.Slice(start,count)
            #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
            Encoding.UTF8.GetString source
            #else
            use pointer = fixed source
            Encoding.UTF8.GetString(pointer,count)
            #endif

        member _.UnescapedStringRunLength(input,start,quote) =
            let mutable finish = start
            let mutable stopped = false
            while finish < input.Length && not stopped do
                let point = input[finish]
                if point = quote || point = '\\'B || point = '\r'B || point = '\n'B ||
                   (point = 0xE2uy && input.Length - finish >= 3 && input[finish+1] = 0x80uy &&
                    uint32(input[finish+2]) - 0xA8u <= 1u) then stopped <- true
                else finish <- finish + 1
            finish - start

        member _.StringRun(input,start,count,decode) = JsonTextEncoding.utf8(input.Slice(start,count),decode)

        member _.AppendAsciiStringRun(builder, input, start, count) =
            for index = start to start + count - 1 do builder.Append(char input[index]) |> ignore

        member _.DecodeCodePointAt(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            Utf8CodePoints.decodeFrom input codeUnitIndex input[codeUnitIndex]

        member _.DecodeCodePointFrom(input: ReadOnlySpan<byte>, codeUnitIndex: int, first: int) =
            Utf8CodePoints.decodeFrom input codeUnitIndex (byte first)

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

        member _.ParseDouble(input,start,length) =
            let mutable slice = input.Slice(start,length)
            if not slice.IsEmpty && slice[0] = '+'B then slice <- slice.Slice(1)
            let mutable value = 0.0
            let mutable consumed = 0
            if System.Buffers.Text.Utf8Parser.TryParse(slice,&value,&consumed) && consumed = slice.Length then value
            else raise(FormatException "Invalid number")

        member _.ParseSingle(input,start,length) =
            let mutable slice = input.Slice(start,length)
            if not slice.IsEmpty && slice[0] = '+'B then slice <- slice.Slice(1)
            let mutable value = 0.0f
            let mutable consumed = 0
            if System.Buffers.Text.Utf8Parser.TryParse(slice,&value,&consumed) && consumed = slice.Length then value
            else raise(FormatException "Invalid number")

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

        member _.AsciiStringRunLength(input, start, quote) =
            let chars = MemoryMarshal.Cast<byte,char>(input)
            let mutable finish = start
            while finish < chars.Length && chars[finish] < '\u0080' && chars[finish] <> char quote
                  && chars[finish] <> '\\' && chars[finish] <> '\r' && chars[finish] <> '\n' do
                finish <- finish + 1
            finish - start

        member _.AsciiString(input,start,count) =
            MemoryMarshal.Cast<byte,char>(input).Slice(start,count).ToString()

        member _.UnescapedStringRunLength(input,start,quote) =
            let chars = MemoryMarshal.Cast<byte,char>(input)
            let mutable finish = start
            let mutable stopped = false
            while finish < chars.Length && not stopped do
                let point = chars[finish]
                if point = char quote || point = '\\' || point = '\r' || point = '\n' ||
                   point = '\u2028' || point = '\u2029' then stopped <- true
                elif Char.IsSurrogate(point) then
                    let struct(_,consumed) = Utf16Helpers.decodeCodePoint input finish
                    finish <- finish + consumed
                else finish <- finish + 1
            finish - start

        member _.StringRun(input,start,count,decode) =
            if decode then MemoryMarshal.Cast<byte,char>(input).Slice(start,count).ToString() else String.Empty

        member _.AppendAsciiStringRun(builder, input, start, count) =
            let chars = MemoryMarshal.Cast<byte,char>(input).Slice(start,count)
            #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
            builder.Append(chars) |> ignore
            #else
            use pointer = fixed chars
            builder.Append(pointer,count) |> ignore
            #endif

        member _.DecodeCodePointAt(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            Utf16Helpers.decodeCodePoint input codeUnitIndex

        member _.DecodeCodePointFrom(input: ReadOnlySpan<byte>, codeUnitIndex: int, first: int) =
            Utf16Helpers.decodeCodePointFrom input codeUnitIndex (char first)

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


        member _.ParseDouble(input,start,length) =
            let chars = MemoryMarshal.Cast<byte,char>(input.Slice(start <<< 1,length <<< 1))
            #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
            Double.Parse(chars,NumberStyles.Float,CultureInfo.InvariantCulture)
            #else
            Double.Parse(chars.ToString(),NumberStyles.Float,CultureInfo.InvariantCulture)
            #endif

        member _.ParseSingle(input,start,length) =
            let chars = MemoryMarshal.Cast<byte,char>(input.Slice(start <<< 1,length <<< 1))
            #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
            Single.Parse(chars,NumberStyles.Float,CultureInfo.InvariantCulture)
            #else
            Single.Parse(chars.ToString(),NumberStyles.Float,CultureInfo.InvariantCulture)
            #endif

/// Borrowed input and encoding-specific token position.
[<Struct; IsByRefLike>]
type internal ParserContext<'R when 'R : struct and 'R :> IJsonReader> =
    val input: ReadOnlySpan<byte>
    val codeUnitLength: int
    val mutable codeUnitIndex: int
    val mutable sb: StringBuilder
    val mutable decodeStrings: bool
    val reader: 'R
    new(input, length, reader) =
        { input = input; codeUnitLength = length; codeUnitIndex = 0
          sb = null; decodeStrings = true; reader = reader }
