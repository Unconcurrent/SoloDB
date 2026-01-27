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

type internal IJsonReader =
    abstract member TryPeekAsciiByte : ReadOnlySpan<byte> * int -> ValueOption<byte>
    abstract member TryReadingPackedAsciiWord4 : ReadOnlySpan<byte> * int -> ValueOption<uint32>
    abstract member DecodeCodePointAt : ReadOnlySpan<byte> * int -> struct (int * int)
    abstract member TryReadHex4 : ReadOnlySpan<byte> * int -> ValueOption<int>
    abstract member ParseNumber : ReadOnlySpan<byte> * int * int -> decimal

[<Struct>]
type internal Utf8Reader =
    interface IJsonReader with
        member _.TryPeekAsciiByte(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            if codeUnitIndex < input.Length then
                let b = input.[codeUnitIndex]
                if b < 0x80uy then ValueSome b else ValueNone
            else
                ValueNone

        member _.TryReadingPackedAsciiWord4(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            if codeUnitIndex + 4 <= input.Length then
                let word = MemoryMarshal.Read<uint32>(input.Slice(codeUnitIndex, 4))
                if (word &&& 0x80808080u) = 0u then ValueSome word else ValueNone
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
            let numberText =
                #if NETSTANDARD2_1
                Encoding.UTF8.GetString(input.Slice(startIndex, length))
                #else
                Encoding.UTF8.GetString(input.Slice(startIndex, length).ToArray())
                #endif
            Decimal.Parse(numberText, NumberStyles.Any, CultureInfo.InvariantCulture)

[<Struct>]
type internal Utf16Reader =
    interface IJsonReader with
        member _.TryPeekAsciiByte(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            let chars = MemoryMarshal.Cast<byte, char>(input)
            if codeUnitIndex < chars.Length then
                let ch = chars.[codeUnitIndex]
                if ch <= '\u007F' then ValueSome (byte ch) else ValueNone
            else
                ValueNone

        member _.TryReadingPackedAsciiWord4(inputUtf16: ReadOnlySpan<byte>, charIndex: int) =
            // charIndex is in UTF-16 code units (chars)
            let byteIndex = charIndex <<< 1

            if byteIndex + 8 <= inputUtf16.Length then
                if BitConverter.IsLittleEndian then
                    // Read 8 bytes = 4 UTF-16LE code units
                    let x = MemoryMarshal.Read<uint64>(inputUtf16.Slice(byteIndex, 8))

                    // ASCII in UTF-16: each 16-bit unit must have bits 7..15 cleared (<= 0x007F)
                    if (x &&& 0xFF80FF80FF80FF80UL) = 0UL then
                        let b0 = byte x
                        let b1 = byte (x >>> 16)
                        let b2 = byte (x >>> 32)
                        let b3 = byte (x >>> 48)

                        let word =
                            (uint32 b0) |||
                            ((uint32 b1) <<< 8) |||
                            ((uint32 b2) <<< 16) |||
                            ((uint32 b3) <<< 24)

                        ValueSome word
                    else
                        ValueNone
                else
                    let chars = MemoryMarshal.Cast<byte, char>(inputUtf16)
                    if charIndex + 4 <= chars.Length then
                        let c0 = chars.[charIndex]
                        let c1 = chars.[charIndex + 1]
                        let c2 = chars.[charIndex + 2]
                        let c3 = chars.[charIndex + 3]
                        if c0 <= '\u007F' && c1 <= '\u007F' && c2 <= '\u007F' && c3 <= '\u007F' then
                            let word =
                                (uint32 (byte c0)) |||
                                ((uint32 (byte c1)) <<< 8) |||
                                ((uint32 (byte c2)) <<< 16) |||
                                ((uint32 (byte c3)) <<< 24)
                            ValueSome word
                        else
                            ValueNone
                    else
                        ValueNone
            else
                ValueNone

        member _.DecodeCodePointAt(input: ReadOnlySpan<byte>, codeUnitIndex: int) =
            let chars = MemoryMarshal.Cast<byte, char>(input)
            let ch = chars.[codeUnitIndex]
            if Char.IsHighSurrogate(ch) then
                if codeUnitIndex + 1 >= chars.Length then
                    failwith "Invalid high surrogate at end of input"
                let ch2 = chars.[codeUnitIndex + 1]
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

            let chars = MemoryMarshal.Cast<byte, char>(input)
            if hexStartIndex + 4 <= chars.Length then
                let d0 = hexValue chars.[hexStartIndex]
                let d1 = hexValue chars.[hexStartIndex + 1]
                let d2 = hexValue chars.[hexStartIndex + 2]
                let d3 = hexValue chars.[hexStartIndex + 3]
                if d0 < 0 || d1 < 0 || d2 < 0 || d3 < 0 then
                    ValueNone
                else
                    ValueSome ((d0 <<< 12) ||| (d1 <<< 8) ||| (d2 <<< 4) ||| d3)
            else
                ValueNone

        member _.ParseNumber(input: ReadOnlySpan<byte>, startIndex: int, length: int) =
            let chars = MemoryMarshal.Cast<byte, char>(input)
            #if NETSTANDARD2_1
            let numberSpan = chars.Slice(startIndex, length)
            Decimal.Parse(numberSpan, NumberStyles.Any, CultureInfo.InvariantCulture)
            #else
            let numberText = chars.Slice(startIndex, length).ToString()
            Decimal.Parse(numberText, NumberStyles.Any, CultureInfo.InvariantCulture)
            #endif

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
        b = ' 'B || b = '\t'B || b = '\n'B || b = '\r'B

    let inline isDigitOrMinusByte (b: byte) =
        b = '-'B || (b >= '0'B && b <= '9'B)

    let inline isNumberCharByte (b: byte) =
        isDigitOrMinusByte b || b = '.'B || b = 'e'B || b = 'E'B || b = '+'B

    let inline isAsciiIdentifierStart (b: byte) =
        (b >= 'A'B && b <= 'Z'B) || (b >= 'a'B && b <= 'z'B) || b = '_'B

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
        if codePoint = int '_' then true else
        match getUnicodeCategory codePoint with
        | UnicodeCategory.UppercaseLetter
        | UnicodeCategory.LowercaseLetter
        | UnicodeCategory.TitlecaseLetter
        | UnicodeCategory.ModifierLetter
        | UnicodeCategory.OtherLetter
        | UnicodeCategory.LetterNumber -> true
        | _ -> false

    let inline isIdentifierCharCodePoint (codePoint: int) =
        if codePoint = int '_' then true else
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

    let private packedNull = MemoryMarshal.Read<uint32>("null"B.AsSpan())
    let private packedNULL = MemoryMarshal.Read<uint32>("NULL"B.AsSpan())
    let private packedTrue = MemoryMarshal.Read<uint32>("true"B.AsSpan())
    let private packedFals = MemoryMarshal.Read<uint32>("fals"B.AsSpan())

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
            // Non-standard: treat as bare identifier string token
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
                    | 'u'B  ->
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
                    | other ->
                        failwithf "Invalid escape sequence: '%c'" (char other)
                | ValueNone ->
                    failwith "Invalid escape sequence"
            | ValueSome b ->
                sb.Append(char b) |> ignore
                i <- i + 1
            | ValueNone ->
                let struct (cp, consumed) = ctx.reader.DecodeCodePointAt(ctx.input, i)
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
                | _ when isDigitOrMinusByte b ->
                    let startIndex = ctx.codeUnitIndex
                    let negative = b = '-'B
                    let mutable i = startIndex

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

                | _ when isAsciiIdentifierStart b ->
                    match ctx.reader.TryReadingPackedAsciiWord4(ctx.input, ctx.codeUnitIndex) with
                    | ValueSome word when word = packedNull && not (isIdentifierCharAt &ctx (ctx.codeUnitIndex + 4)) ->
                        ctx.codeUnitIndex <- ctx.codeUnitIndex + 4
                        NullToken
                    | ValueSome word when word = packedNULL && not (isIdentifierCharAt &ctx (ctx.codeUnitIndex + 4)) ->
                        ctx.codeUnitIndex <- ctx.codeUnitIndex + 4
                        NullToken
                    | ValueSome word when word = packedTrue && not (isIdentifierCharAt &ctx (ctx.codeUnitIndex + 4)) ->
                        ctx.codeUnitIndex <- ctx.codeUnitIndex + 4
                        BooleanToken true
                    | ValueSome word when word = packedFals &&
                                          ctx.codeUnitIndex + 4 < ctx.codeUnitLength &&
                                          (ctx.reader.TryPeekAsciiByte(ctx.input, ctx.codeUnitIndex + 4) = ValueSome 'e'B) &&
                                          not (isIdentifierCharAt &ctx (ctx.codeUnitIndex + 5)) ->
                        ctx.codeUnitIndex <- ctx.codeUnitIndex + 5
                        BooleanToken false
                    | _ ->
                        match parseIdentifierFallback &ctx ctx.codeUnitIndex with
                        | struct (tok, i) -> ctx.codeUnitIndex <- i; tok

                | _ when isJsonWhitespaceByte b ->
                    ctx.codeUnitIndex <- ctx.codeUnitIndex + 1
                    readNext &ctx

                | _ ->
                    failwithf "Unexpected JSON character: %c, index = %i" (char b) ctx.codeUnitIndex

            | ValueNone ->
                let struct (cp, _consumed) = ctx.reader.DecodeCodePointAt(ctx.input, ctx.codeUnitIndex)
                if isIdentifierStartCodePoint cp then
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
        (ctx: byref<ParserContext<'T, 'R>>) (dict: Dictionary<string, 'T>) =
        match readNext &ctx with
        | CloseBrace -> dict :> IDictionary<string, 'T>
        | StringToken key ->
            let colon = readNext &ctx
            if colon <> Colon then failwith "Malformed json."

            let value = parse &ctx
            dict.[key] <- value
            parseMembers &ctx dict
        | Comma ->
            parseMembers &ctx dict
        | _ ->
            failwith "Invalid object syntax"

    and private parseObject<'T, 'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'T, 'R>>) : IDictionary<string, 'T> =
        let dict = Dictionary<string, 'T>()
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
