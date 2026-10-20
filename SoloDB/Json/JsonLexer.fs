namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Globalization
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text

type internal JsonNestingException(limit: int) =
    inherit FormatException("JSON contains more than " + limit.ToString(CultureInfo.InvariantCulture) + " nested containers.")

module internal JsonLexer =
    open JsonStringReader
    open JsonIdentifiers

    let enterContainer<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) =
        let limit = 1000
        if ctx.containerDepth >= limit then raise (JsonNestingException limit)
        ctx.containerDepth <- ctx.containerDepth + 1

    let leaveContainer<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) =
        if ctx.containerDepth > 0 then ctx.containerDepth <- ctx.containerDepth - 1

    let fromString (source: string) =
        let span = source.AsSpan()
        ParserContext(MemoryMarshal.AsBytes span, span.Length, Utf16Reader())

    let fromUtf16Span (source: ReadOnlySpan<char>) =
        ParserContext(MemoryMarshal.AsBytes source, source.Length, Utf16Reader())

    let fromUtf8Span (source: ReadOnlySpan<byte>) =
        ParserContext(source, source.Length, Utf8Reader())

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

    let nextSignificantCodePoint<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) =
        let mutable scanning = true
        let mutable next = -1
        while scanning && ctx.codeUnitIndex < ctx.codeUnitLength do
            match ctx.reader.TryPeekAsciiByte(ctx.input, ctx.codeUnitIndex) with
            | ValueSome b when isJsonWhitespaceByte b ->
                next <- int b
                ctx.codeUnitIndex <- ctx.codeUnitIndex + 1
            | ValueSome '/'B ->
                next <- int '/'
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
                | _ ->
                    failwithf "Unexpected JSON character: %c, index = %i" '/' ctx.codeUnitIndex
            | ValueSome b -> next <- int b; scanning <- false
            | ValueNone ->
                let struct (cp, consumed) = ctx.reader.DecodeCodePointAt(ctx.input, ctx.codeUnitIndex)
                next <- cp
                if isJsonWhitespaceCodePoint cp then ctx.codeUnitIndex <- ctx.codeUnitIndex + consumed
                else scanning <- false
        if ctx.codeUnitIndex < ctx.codeUnitLength then next else -1

    let readNext<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) : Token =

        RuntimeHelpers.EnsureSufficientExecutionStack()
        let next = nextSignificantCodePoint &ctx

        if ctx.codeUnitIndex < ctx.codeUnitLength then
            match (if next <= 0x7F then ValueSome(byte next) else ValueNone) with
            | ValueSome b ->
                match b with
                | '{'B -> enterContainer &ctx; ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; OpenBrace
                | '}'B -> leaveContainer &ctx; ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; CloseBrace
                | '['B -> enterContainer &ctx; ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; OpenBracket
                | ']'B -> leaveContainer &ctx; ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; CloseBracket
                | ','B -> ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; Comma
                | ':'B -> ctx.codeUnitIndex <- ctx.codeUnitIndex + 1; Colon
                | '"'B | '\''B as quote ->
                    readString &ctx quote
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
                                    | CommonLiteral.Infinity -> NumberToken(InfinityLiteral negative)
                                    | CommonLiteral.NaN -> NumberToken NaNLiteral
                                    | _ -> failwith "Invalid signed literal"
                                )
                        | _ ->
                            let struct (tok, nextIndex) = parseIdentifierFallback &ctx (ctx.codeUnitIndex + 1)
                            match tok with
                            | NumberToken literal when nextIndex > ctx.codeUnitIndex + 1 && not (isIdentifierCharAt &ctx nextIndex) ->
                                ctx.codeUnitIndex <- nextIndex
                                tokenOpt <- ValueSome(NumberToken(match literal with InfinityLiteral _ -> InfinityLiteral negative | other -> other))
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
                                tokenOpt <- ValueSome(NumberToken(HexLiteral(acc,hexNegative,overflow)))

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

                            NumberToken(DecimalLiteral(startIndex,length))

                | '\\'B ->
                    match parseIdentifierFallback &ctx ctx.codeUnitIndex with
                    | struct (tok, i) -> ctx.codeUnitIndex <- i; tok

                | _ when isAsciiIdentifierStart b -> readIdentifierFrom &ctx 0

                | _ ->
                    failwithf "Unexpected JSON character: %c, index = %i" (char b) ctx.codeUnitIndex

            | ValueNone ->
                let cp = next
                if isIdentifierStartCodePoint cp then
                    match parseIdentifierFallback &ctx ctx.codeUnitIndex with
                    | struct (tok, i) -> ctx.codeUnitIndex <- i; tok
                else
                    failwithf "Unexpected JSON character: U+%04X, index = %i" cp ctx.codeUnitIndex
        else
            EndOfInput

    // The caller has already admitted trivia and the significant point.
    let tryReadNullAtPoint<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) point =
        let start = ctx.codeUnitIndex
        let found =
            if point = int 'n' || point = int 'N' || point = int '\\' then
                match ctx.reader.TryMatchCommonLiterals(ctx.input,ctx.codeUnitIndex) with
                | ValueSome struct (literal,length) when not (isIdentifierCharAt &ctx (ctx.codeUnitIndex + length)) ->
                    match literal with
                    | CommonLiteral.Null | CommonLiteral.NullUpper ->
                        ctx.codeUnitIndex <- ctx.codeUnitIndex + length
                        true
                    | CommonLiteral.True | CommonLiteral.False | CommonLiteral.NaN | CommonLiteral.Infinity -> false
                | ValueSome struct ((CommonLiteral.Null | CommonLiteral.NullUpper),_) -> false
                | _ ->
                    // Only rewriting within the four-letter keyword can turn an unmatched spelling into null.
                    let mutable i = start
                    let limit = start + min 4 (ctx.codeUnitLength - start)
                    let mutable escaped = false
                    while i < limit && not escaped do
                        escaped <-
                            match ctx.reader.TryPeekAsciiByte(ctx.input,i) with
                            | ValueSome '\\'B -> true
                            | _ -> false
                        i <- i + 1
                    if escaped then
                        match parseIdentifierFallback &ctx start with
                        | struct (NullToken,index) -> ctx.codeUnitIndex <- index; true
                        | _ -> false
                    else false
            else false
        if not found then ctx.codeUnitIndex <- start
        found

    // Stored values and optional readers use the same keyword and boundary policy.
    let tryReadNull<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) =
        let start = ctx.codeUnitIndex
        let point = nextSignificantCodePoint &ctx
        let found = tryReadNullAtPoint &ctx point
        if not found then ctx.codeUnitIndex <- start
        found
