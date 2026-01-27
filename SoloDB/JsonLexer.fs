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

/// Tokenizer state + the value factory API; no dependency on JsonValue anymore.
[<Struct; IsByRefLike>]
type internal ParserContext<'T> =
    {
        input : ReadOnlySpan<char>
        mutable index : int
        sb : StringBuilder
        api : JsonValueParserApi<'T>
    }

[<Struct; IsByRefLike>]
type internal ParserContextUtf8<'T> =
    {
        input : ReadOnlySpan<byte>
        mutable index : int
        sb : StringBuilder
        api : JsonValueParserApi<'T>
    }

type internal ParserContextFactoryStr<'T> = delegate of string -> ParserContext<'T>
type internal ParserContextFactoryUtf16<'T> = delegate of ReadOnlySpan<char> -> ParserContext<'T>
type internal ParserContextFactoryUtf8<'T> = delegate of ReadOnlySpan<byte> -> ParserContextUtf8<'T>

module internal JsonParser =

    let inline fromString (api: JsonValueParserApi<'T>) : ParserContextFactoryStr<'T> =
        ParserContextFactoryStr<'T>(fun (source: string) ->
            { input = source.AsSpan(); index = 0; sb = StringBuilder(); api = api })

    let inline fromUtf16Span (api: JsonValueParserApi<'T>) : ParserContextFactoryUtf16<'T> =
        ParserContextFactoryUtf16<'T>(fun (source: ReadOnlySpan<char>) ->
            { input = source; index = 0; sb = StringBuilder(); api = api })

    let inline fromUtf8Span (api: JsonValueParserApi<'T>) : ParserContextFactoryUtf8<'T> =
        ParserContextFactoryUtf8<'T>(fun (source: ReadOnlySpan<byte>) ->
            { input = source; index = 0; sb = StringBuilder(); api = api })

    let inline appendCodePoint (sb: StringBuilder) (codePoint: int) =
        if codePoint <= 0xFFFF then
            sb.Append(char codePoint) |> ignore
        else
            let cp = codePoint - 0x10000
            let high = 0xD800 + (cp >>> 10)
            let low = 0xDC00 + (cp &&& 0x3FF)
            sb.Append(char high) |> ignore
            sb.Append(char low) |> ignore

    let inline decodeUtf16At (input: ReadOnlySpan<char>) (index: int) : struct (int * int) =
        let ch = input.[index]
        if Char.IsHighSurrogate(ch) then
            if index + 1 >= input.Length then
                failwith "Invalid high surrogate at end of input"
            let ch2 = input.[index + 1]
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

    let inline decodeUtf8At (input: ReadOnlySpan<byte>) (index: int) : struct (int * int) =
        let len = input.Length
        let b0 = input.[index]
        if b0 < 0x80uy then
            struct (int b0, 1)
        elif (b0 &&& 0xE0uy) = 0xC0uy then
            if index + 1 >= len then failwith "Invalid UTF-8 sequence"
            let b1 = input.[index + 1]
            if (b1 &&& 0xC0uy) <> 0x80uy then failwith "Invalid UTF-8 continuation byte"
            let codePoint = (((int b0) &&& 0x1F) <<< 6) ||| ((int b1) &&& 0x3F)
            if codePoint < 0x80 then failwith "Overlong UTF-8 sequence"
            struct (codePoint, 2)
        elif (b0 &&& 0xF0uy) = 0xE0uy then
            if index + 2 >= len then failwith "Invalid UTF-8 sequence"
            let b1 = input.[index + 1]
            let b2 = input.[index + 2]
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
            if index + 3 >= len then failwith "Invalid UTF-8 sequence"
            let b1 = input.[index + 1]
            let b2 = input.[index + 2]
            let b3 = input.[index + 3]
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

    let private pack4 (bytes: byte array) =
        MemoryMarshal.Read<uint32>(bytes.AsSpan())

    let private packedNull = pack4 "null"B
    let private packedNULL = pack4 "NULL"B
    let private packedTrue = pack4 "true"B
    let private packedFals = pack4 "fals"B

    let inline private readUInt32Packed (input: ReadOnlySpan<byte>, index: int) =
        MemoryMarshal.Read<uint32>(input.Slice(index, 4))

    module internal Utf16Chars =
        let inline isDigitOrMinus (c: char) = Char.IsDigit c || c = '-'
        let inline isInitialIdentifierChar (c: char) = Char.IsLetter c || c = '_'
        let inline isIdentifierChar (c: char) = isInitialIdentifierChar c || Char.IsDigit c
        let inline isWhitespace (c: char) = Char.IsWhiteSpace c

    module internal Utf8Bytes =
        let inline isDigitOrMinus (b: byte) = b = '-'B || (b >= '0'B && b <= '9'B)
        let inline isInitialIdentifierChar (b: byte) =
            (b >= 'A'B && b <= 'Z'B) || (b >= 'a'B && b <= 'z'B) || b = '_'B
        let inline isIdentifierChar (b: byte) =
            isInitialIdentifierChar b || (b >= '0'B && b <= '9'B)
        let inline isWhitespace (b: byte) = b = ' 'B || b = '\t'B || b = '\n'B || b = '\r'B

    let private parseIdentifierFallbackUtf16 (input: ReadOnlySpan<char>, index: int, sb: StringBuilder) : struct (Token * int) =

        let sb = sb.Clear()
        let mutable i = index
        while i < input.Length && Utf16Chars.isIdentifierChar input.[i] do
            sb.Append(input.[i]) |> ignore
            i <- i + 1

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

    let private hexValueByte (b: byte) =
        if b >= '0'B && b <= '9'B then int b - int '0'B
        elif b >= 'a'B && b <= 'f'B then 10 + int b - int 'a'B
        elif b >= 'A'B && b <= 'F'B then 10 + int b - int 'A'B
        else -1

    let private parseHex4Utf8 (input: ReadOnlySpan<byte>, offset: int) =
        if offset + 4 > input.Length then
            failwith "Invalid Unicode escape sequence"
        let d0 = hexValueByte input.[offset]
        let d1 = hexValueByte input.[offset + 1]
        let d2 = hexValueByte input.[offset + 2]
        let d3 = hexValueByte input.[offset + 3]
        if d0 < 0 || d1 < 0 || d2 < 0 || d3 < 0 then
            failwith "Invalid Unicode escape sequence"
        (d0 <<< 12) ||| (d1 <<< 8) ||| (d2 <<< 4) ||| d3

    let private parseIdentifierFallbackUtf8 (input: ReadOnlySpan<byte>, index: int, sb: StringBuilder) : struct (Token * int) =
        let sb = sb.Clear()
        let mutable i = index
        while i < input.Length && Utf8Bytes.isIdentifierChar input.[i] do
            sb.Append(input.[i]) |> ignore
            i <- i + 1

        let txt = sb.ToString()

        struct (
            match txt with
            | "null" | "NULL" -> NullToken
            | "true" -> BooleanToken true
            | "false" -> BooleanToken false
            | other -> StringToken other
            , i
        )

    let rec private readNext (ctx: byref<ParserContext<'T>>) : Token =
        RuntimeHelpers.EnsureSufficientExecutionStack()

        let input = ctx.input

        if ctx.index < input.Length then
            let currentChar = input.[ctx.index]
            match currentChar with
            | '{' ->
                ctx.index <- ctx.index + 1
                OpenBrace
            | '}' ->
                ctx.index <- ctx.index + 1
                CloseBrace
            | '[' ->
                ctx.index <- ctx.index + 1
                OpenBracket
            | ']' ->
                ctx.index <- ctx.index + 1
                CloseBracket
            | ',' ->
                ctx.index <- ctx.index + 1
                Comma
            | ':' ->
                ctx.index <- ctx.index + 1
                Colon
            | '"' | '\'' as quote ->
                let sb = ctx.sb.Clear()
                let mutable i = ctx.index + 1
                while i < input.Length && input.[i] <> quote do
                    if input.[i] = '\\' then
                        if i + 1 >= input.Length then
                            failwith "Invalid escape sequence"
                        i <- i + 1
                        match input.[i] with
                        | '\\' -> sb.Append('\\') |> ignore; i <- i + 1
                        | '\"' -> sb.Append('"') |> ignore; i <- i + 1
                        | '\'' -> sb.Append('\'') |> ignore; i <- i + 1
                        | '/'  -> sb.Append('/') |> ignore; i <- i + 1
                        | 'b'  -> sb.Append('\b') |> ignore; i <- i + 1
                        | 'f'  -> sb.Append('\f') |> ignore; i <- i + 1
                        | 'n'  -> sb.Append('\n') |> ignore; i <- i + 1
                        | 'r'  -> sb.Append('\r') |> ignore; i <- i + 1
                        | 't'  -> sb.Append('\t') |> ignore; i <- i + 1
                        | 'u'  ->
                            if i + 4 < input.Length then
                                let code1 = JsonHelper.parseHex (input.Slice(i + 1, 4))
                                i <- i + 5
                                if code1 >= 0xD800 && code1 <= 0xDBFF then
                                    if i + 1 < input.Length && input.[i] = '\\' && input.[i + 1] = 'u' then
                                        let code2 = JsonHelper.parseHex (input.Slice(i + 2, 4))
                                        if code2 >= 0xDC00 && code2 <= 0xDFFF then
                                            let codePoint =
                                                0x10000 +
                                                ((code1 - 0xD800) <<< 10) +
                                                (code2 - 0xDC00)
                                            appendCodePoint sb codePoint
                                            i <- i + 6
                                        else
                                            failwith "Invalid low surrogate"
                                    else
                                        failwith "Expected low surrogate after high surrogate"
                                elif code1 >= 0xDC00 && code1 <= 0xDFFF then
                                    failwith "Unexpected low surrogate"
                                else
                                    appendCodePoint sb code1
                            else
                                failwith "Invalid Unicode escape sequence"
                        | other -> failwithf "Invalid escape sequence: '%c'" other
                    else
                        let struct (codePoint, consumed) = decodeUtf16At input i
                        appendCodePoint sb codePoint
                        i <- i + consumed

                if i >= input.Length then failwith "Unterminated string"
                ctx.index <- i + 1
                StringToken(sb.ToString())

            | c when Utf16Chars.isDigitOrMinus c ->
                let mutable i = ctx.index
                let mutable length = 0

                while i < input.Length &&
                      (Utf16Chars.isDigitOrMinus input.[i] ||
                       input.[i] = '.' ||
                       input.[i] = 'e' ||
                       input.[i] = 'E' ||
                       input.[i] = '+') do
                    length <- length + 1
                    i <- i + 1

                #if NETSTANDARD2_1
                let s = input.Slice(ctx.index, length)
                #else
                let s = input.Slice(ctx.index, length).ToString()
                #endif

                ctx.index <- i

                NumberToken(
                    try Decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)
                    with :? OverflowException ->
                        if s.[0] = '-' then Decimal.MinValue else Decimal.MaxValue
                )

            | c when Utf16Chars.isInitialIdentifierChar c ->
                if ctx.index + 4 <= input.Length then
                    match input.Slice(ctx.index, 4) with
                    | x when x.Equals("null".AsSpan(), StringComparison.Ordinal)
                             && (ctx.index + 4 = input.Length || (not << Utf16Chars.isIdentifierChar) input.[ctx.index + 4]) ->
                        ctx.index <- ctx.index + 4
                        NullToken
                    | x when x.Equals("NULL".AsSpan(), StringComparison.Ordinal)
                             && (ctx.index + 4 = input.Length || (not << Utf16Chars.isIdentifierChar) input.[ctx.index + 4]) ->
                        ctx.index <- ctx.index + 4
                        NullToken
                    | x when x.Equals("true".AsSpan(), StringComparison.Ordinal)
                             && (ctx.index + 4 = input.Length || (not << Utf16Chars.isIdentifierChar) input.[ctx.index + 4]) ->
                        ctx.index <- ctx.index + 4
                        BooleanToken true
                    | x when x.Equals("fals".AsSpan(), StringComparison.Ordinal)
                             && ctx.index + 4 < input.Length
                             && input.[ctx.index + 4] = 'e'
                             && (ctx.index + 5 = input.Length || (not << Utf16Chars.isIdentifierChar) input.[ctx.index + 5]) ->
                        ctx.index <- ctx.index + 5
                        BooleanToken false
                    | _ ->
                        match parseIdentifierFallbackUtf16 (input, ctx.index, ctx.sb) with
                        | struct (tok, i) -> ctx.index <- i; tok
                else
                    match parseIdentifierFallbackUtf16 (input, ctx.index, ctx.sb) with
                    | struct (tok, i) -> ctx.index <- i; tok

            | c when Utf16Chars.isWhitespace c ->
                ctx.index <- ctx.index + 1
                readNext &ctx

            | c ->
                failwithf "Unexpected JSON character: %c, index = %i" c ctx.index
        else
            EndOfInput

    let inline peek (ctx: byref<ParserContext<'T>>) : Token =
        let savedIndex = ctx.index
        try readNext &ctx
        finally ctx.index <- savedIndex

    let rec private parseMembers (ctx: byref<ParserContext<'T>>) (dict: Dictionary<string, 'T>) =
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

    and private parseObject (ctx: byref<ParserContext<'T>>) : IDictionary<string, 'T> =
        let dict = Dictionary<string, 'T>()
        parseMembers &ctx dict

    and private parseElements (ctx: byref<ParserContext<'T>>) (items: List<'T>) : IList<'T> =
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

    and private parseArray (ctx: byref<ParserContext<'T>>) : IList<'T> =
        let items = List<'T>()
        parseElements &ctx items

    and private parseTokensOr (ctx: byref<ParserContext<'T>>) (fallback: Token -> 'T option) : 'T option =
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

    and private parseTokens (ctx: byref<ParserContext<'T>>) : 'T =
        match parseTokensOr &ctx (fun _ -> None) with
        | Some x -> x
        | None -> failwithf "Could not parse the token at index = %i." ctx.index

    and parse (ctx: byref<ParserContext<'T>>) : 'T =
        parseTokens &ctx

    let rec private readNextUtf8 (ctx: byref<ParserContextUtf8<'T>>) : Token =
        RuntimeHelpers.EnsureSufficientExecutionStack()

        let input = ctx.input

        if ctx.index < input.Length then
            let current = input.[ctx.index]
            match current with
            | '{'B -> ctx.index <- ctx.index + 1; OpenBrace
            | '}'B -> ctx.index <- ctx.index + 1; CloseBrace
            | '['B -> ctx.index <- ctx.index + 1; OpenBracket
            | ']'B -> ctx.index <- ctx.index + 1; CloseBracket
            | ','B -> ctx.index <- ctx.index + 1; Comma
            | ':'B -> ctx.index <- ctx.index + 1; Colon
            | '"'B | '\''B as quote ->
                let sb = ctx.sb.Clear()
                let mutable i = ctx.index + 1
                while i < input.Length && input.[i] <> quote do
                    if input.[i] = '\\'B then
                        if i + 1 >= input.Length then
                            failwith "Invalid escape sequence"
                        i <- i + 1
                        match input.[i] with
                        | '\\'B -> sb.Append('\\') |> ignore; i <- i + 1
                        | '"'B -> sb.Append('"') |> ignore; i <- i + 1
                        | '\''B -> sb.Append('\'') |> ignore; i <- i + 1
                        | '/'B -> sb.Append('/') |> ignore; i <- i + 1
                        | 'b'B -> sb.Append('\b') |> ignore; i <- i + 1
                        | 'f'B -> sb.Append('\f') |> ignore; i <- i + 1
                        | 'n'B -> sb.Append('\n') |> ignore; i <- i + 1
                        | 'r'B -> sb.Append('\r') |> ignore; i <- i + 1
                        | 't'B -> sb.Append('\t') |> ignore; i <- i + 1
                        | 'u'B ->
                            if i + 4 < input.Length then
                                let code1 = parseHex4Utf8 (input, i + 1)
                                i <- i + 5
                                if code1 >= 0xD800 && code1 <= 0xDBFF then
                                    if i + 1 < input.Length && input.[i] = '\\'B && input.[i + 1] = 'u'B then
                                        let code2 = parseHex4Utf8 (input, i + 2)
                                        if code2 >= 0xDC00 && code2 <= 0xDFFF then
                                            let codePoint =
                                                0x10000 +
                                                ((code1 - 0xD800) <<< 10) +
                                                (code2 - 0xDC00)
                                            appendCodePoint sb codePoint
                                            i <- i + 6
                                        else
                                            failwith "Invalid low surrogate"
                                    else
                                        failwith "Expected low surrogate after high surrogate"
                                elif code1 >= 0xDC00 && code1 <= 0xDFFF then
                                    failwith "Unexpected low surrogate"
                                else
                                    appendCodePoint sb code1
                            else
                                failwith "Invalid Unicode escape sequence"
                        | other -> failwithf "Invalid escape sequence: '%c'" (char other)
                    else
                        let struct (codePoint, consumed) = decodeUtf8At input i
                        appendCodePoint sb codePoint
                        i <- i + consumed

                if i >= input.Length then failwith "Unterminated string"
                ctx.index <- i + 1
                StringToken(sb.ToString())

            | c when Utf8Bytes.isDigitOrMinus c ->
                let mutable i = ctx.index
                let mutable length = 0

                while i < input.Length &&
                      (Utf8Bytes.isDigitOrMinus input.[i] ||
                       input.[i] = '.'B ||
                       input.[i] = 'e'B ||
                       input.[i] = 'E'B ||
                       input.[i] = '+'B) do
                    length <- length + 1
                    i <- i + 1

                let numberText =
                    #if NETSTANDARD2_1
                    Encoding.UTF8.GetString(input.Slice(ctx.index, length))
                    #else
                    Encoding.UTF8.GetString(input.Slice(ctx.index, length).ToArray())
                    #endif

                ctx.index <- i

                NumberToken(
                    try Decimal.Parse(numberText, NumberStyles.Any, CultureInfo.InvariantCulture)
                    with :? OverflowException ->
                        if numberText.[0] = '-' then Decimal.MinValue else Decimal.MaxValue
                )

            | c when Utf8Bytes.isInitialIdentifierChar c ->
                if ctx.index + 4 <= input.Length then
                    let word = readUInt32Packed (input, ctx.index)
                    if word = packedNull &&
                       (ctx.index + 4 = input.Length || (not << Utf8Bytes.isIdentifierChar) input.[ctx.index + 4]) then
                        ctx.index <- ctx.index + 4
                        NullToken
                    elif word = packedNULL &&
                         (ctx.index + 4 = input.Length || (not << Utf8Bytes.isIdentifierChar) input.[ctx.index + 4]) then
                        ctx.index <- ctx.index + 4
                        NullToken
                    elif word = packedTrue &&
                         (ctx.index + 4 = input.Length || (not << Utf8Bytes.isIdentifierChar) input.[ctx.index + 4]) then
                        ctx.index <- ctx.index + 4
                        BooleanToken true
                    elif word = packedFals &&
                         ctx.index + 4 < input.Length && input.[ctx.index + 4] = 'e'B &&
                         (ctx.index + 5 = input.Length || (not << Utf8Bytes.isIdentifierChar) input.[ctx.index + 5]) then
                        ctx.index <- ctx.index + 5
                        BooleanToken false
                    else
                        match parseIdentifierFallbackUtf8 (input, ctx.index, ctx.sb) with
                        | struct (tok, i) -> ctx.index <- i; tok
                else
                    match parseIdentifierFallbackUtf8 (input, ctx.index, ctx.sb) with
                    | struct (tok, i) -> ctx.index <- i; tok

            | c when Utf8Bytes.isWhitespace c ->
                ctx.index <- ctx.index + 1
                readNextUtf8 &ctx

            | c when c < 0x80uy ->
                failwithf "Unexpected JSON character: %c, index = %i" (char c) ctx.index
            | _ ->
                failwithf "Unexpected JSON character: byte 0x%02X, index = %i" current ctx.index
        else
            EndOfInput

    let inline peekUtf8 (ctx: byref<ParserContextUtf8<'T>>) : Token =
        let savedIndex = ctx.index
        try readNextUtf8 &ctx
        finally ctx.index <- savedIndex

    let rec private parseMembersUtf8 (ctx: byref<ParserContextUtf8<'T>>) (dict: Dictionary<string, 'T>) =
        match readNextUtf8 &ctx with
        | CloseBrace -> dict :> IDictionary<string, 'T>
        | StringToken key ->
            let colon = readNextUtf8 &ctx
            if colon <> Colon then failwith "Malformed json."

            let value = parseUtf8 &ctx
            dict.[key] <- value
            parseMembersUtf8 &ctx dict
        | Comma ->
            parseMembersUtf8 &ctx dict
        | _ ->
            failwith "Invalid object syntax"

    and private parseObjectUtf8 (ctx: byref<ParserContextUtf8<'T>>) : IDictionary<string, 'T> =
        let dict = Dictionary<string, 'T>()
        parseMembersUtf8 &ctx dict

    and private parseElementsUtf8 (ctx: byref<ParserContextUtf8<'T>>) (items: List<'T>) : IList<'T> =
        let itemOpt =
            parseTokensOrUtf8 &ctx (fun t ->
                match t with
                | CloseBracket -> None
                | other -> failwithf "Invalid list token: %A" other
            )

        match itemOpt with
        | None -> items :> IList<'T>
        | Some item ->
            items.Add item
            match readNextUtf8 &ctx with
            | CloseBracket -> items :> IList<'T>
            | Comma -> parseElementsUtf8 &ctx items
            | _ -> failwith "Malformed json."

    and private parseArrayUtf8 (ctx: byref<ParserContextUtf8<'T>>) : IList<'T> =
        let items = List<'T>()
        parseElementsUtf8 &ctx items

    and private parseTokensOrUtf8 (ctx: byref<ParserContextUtf8<'T>>) (fallback: Token -> 'T option) : 'T option =
        match readNextUtf8 &ctx with
        | OpenBrace ->
            let obj = parseObjectUtf8 &ctx
            ctx.api.Obj obj |> Some
        | OpenBracket ->
            let arr = parseArrayUtf8 &ctx
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

    and private parseTokensUtf8 (ctx: byref<ParserContextUtf8<'T>>) : 'T =
        match parseTokensOrUtf8 &ctx (fun _ -> None) with
        | Some x -> x
        | None -> failwithf "Could not parse the token at index = %i." ctx.index

    and parseUtf8 (ctx: byref<ParserContextUtf8<'T>>) : 'T =
        parseTokensUtf8 &ctx
