namespace SoloDatabase.JsonSerializator

open System
open System.Globalization

/// Shared identifier admission, rewriting and token identity for lexical and borrowed-name readers.
module internal JsonIdentifiers =
    open JsonStringReader
    let inline isAsciiIdentifierStart (b: byte) =
        JsonIdentifierSyntax.isStart (int b)

    let inline getUnicodeCategory (codePoint: int) =
        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
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

    let inline internal isIdentifierCharAt<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) (probeCodeUnitIndex: int) =
        if probeCodeUnitIndex >= ctx.codeUnitLength then
            false
        else
            match ctx.reader.TryPeekAsciiByte(ctx.input, probeCodeUnitIndex) with
            | ValueSome b -> isIdentifierCharCodePoint (int b)
            | ValueNone ->
                let struct (cp, _consumed) = ctx.reader.DecodeCodePointAt(ctx.input, probeCodeUnitIndex)
                isIdentifierCharCodePoint cp

    let internal readEscape<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) index first =
        let i = index
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

            struct(codePoint, i + consumed)
        | _ ->
            failwith "Invalid identifier escape sequence"

    let private parseIdentifierRemainder<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) (startIndex: int) prefix : struct (Token * int) =
        let sb = builder &ctx
        if prefix <> 0 then ctx.reader.AppendAsciiStringRun(sb,ctx.input,startIndex,prefix)
        let mutable i = startIndex + prefix
        let mutable first = prefix = 0
        let mutable finished = false

        while i < ctx.codeUnitLength && not finished do
            match ctx.reader.TryPeekAsciiByte(ctx.input, i) with
            | ValueSome '\\'B ->
                let struct(point,next) = readEscape &ctx i first
                appendCodePoint sb point
                i <- next
                first <- false
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
            match JsonIdentifierSyntax.classify txt with
            | 0 -> NullToken
            | 1 -> BooleanToken true
            | 2 -> BooleanToken false
            | 3 -> NumberToken(InfinityLiteral false)
            | 4 -> NumberToken NaNLiteral
            | _ -> StringToken txt
            , i
        )

    let internal parseIdentifierFallback<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) start = parseIdentifierRemainder &ctx start 0

    let readIdentifierFrom<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) prefix =
        match ctx.reader.TryMatchCommonLiterals(ctx.input,ctx.codeUnitIndex) with
        | ValueSome struct(literal,length) when not(isIdentifierCharAt &ctx (ctx.codeUnitIndex + length)) ->
            ctx.codeUnitIndex <- ctx.codeUnitIndex + length
            match literal with
            | CommonLiteral.Null | CommonLiteral.NullUpper -> NullToken
            | CommonLiteral.True -> BooleanToken true
            | CommonLiteral.False -> BooleanToken false
            | CommonLiteral.Infinity -> NumberToken(InfinityLiteral false)
            | CommonLiteral.NaN -> NumberToken NaNLiteral
        | _ ->
            let struct(token,index) = parseIdentifierRemainder &ctx ctx.codeUnitIndex prefix
            ctx.codeUnitIndex <- index
            token
