namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Globalization
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text

module internal JsonStringReader =
    let builder<'R when 'R : struct and 'R :> IJsonReader> (ctx: byref<ParserContext<'R>>) =
        if isNull ctx.sb then ctx.sb <- StringBuilder()
        ctx.sb.Clear()

    let inline appendCodePoint (sb: StringBuilder) (codePoint: int) =
        if not(isNull sb) then
            if codePoint <= 0xFFFF then
                sb.Append(char codePoint) |> ignore
            else
                let cp = codePoint - 0x10000
                let high = 0xD800 + (cp >>> 10)
                let low = 0xDC00 + (cp &&& 0x3FF)
                sb.Append(char high) |> ignore
                sb.Append(char low) |> ignore


    let internal readEscape<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) index =
        let mutable i = index
        let mutable point = -1
        if i + 1 >= ctx.codeUnitLength then
            failwith "Invalid escape sequence"
        match ctx.reader.TryPeekAsciiByte(ctx.input, i + 1) with
        | ValueSome esc ->
            match esc with
            | '\\'B -> point <- int ('\\'); i <- i + 2
            | '"'B -> point <- int ('"'); i <- i + 2
            | '\''B -> point <- int ('\''); i <- i + 2
            | '/'B  -> point <- int ('/'); i <- i + 2
            | 'b'B  -> point <- int ('\b'); i <- i + 2
            | 'f'B  -> point <- int ('\f'); i <- i + 2
            | 'n'B  -> point <- int ('\n'); i <- i + 2
            | 'r'B  -> point <- int ('\r'); i <- i + 2
            | 't'B  -> point <- int ('\t'); i <- i + 2
            | 'v'B  -> point <- int ('\u000B'); i <- i + 2
            | '0'B  ->
                match ctx.reader.TryPeekAsciiByte(ctx.input, i + 2) with
                | ValueSome d when d >= '0'B && d <= '9'B ->
                    failwith "Invalid \\0 escape sequence"
                | _ ->
                    point <- int ('\u0000')
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
                    point <- codePoint
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
                                        point <- codePoint
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
                            point <- code1
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
                    point <- code
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
        struct(point,i)

    let internal readRemainder<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) (quote: byte) start count : string =
        let sb = if ctx.decodeStrings then builder &ctx else null
        let mutable i = start
        let prefixEnd = start + count
        while i < prefixEnd do
            match ctx.reader.TryPeekAsciiByte(ctx.input,i) with
            | ValueSome _ ->
                let run = ctx.reader.AsciiStringRunLength(ctx.input,i,quote)
                if not(isNull sb) then ctx.reader.AppendAsciiStringRun(sb,ctx.input,i,run)
                i <- i + run
            | ValueNone ->
                let struct(cp,consumed) = ctx.reader.DecodeCodePointAt(ctx.input,i)
                appendCodePoint sb cp
                i <- i + consumed
        let mutable finished = false

        while i < ctx.codeUnitLength && not finished do
            match ctx.reader.TryPeekAsciiByte(ctx.input, i) with
            | ValueSome b when b = quote ->
                finished <- true
            | ValueSome b when b = '\\'B ->
                let struct(point,next) = readEscape &ctx i
                if point >= 0 then appendCodePoint sb point
                i <- next
            | ValueSome b ->
                if b = '\n'B || b = '\r'B then
                    failwith "Unterminated string"
                let count = ctx.reader.AsciiStringRunLength(ctx.input,i,quote)
                if not(isNull sb) then ctx.reader.AppendAsciiStringRun(sb,ctx.input,i,count)
                i <- i + count
            | ValueNone ->
                let struct (cp, consumed) = ctx.reader.DecodeCodePointAt(ctx.input, i)
                if cp = 0x2028 || cp = 0x2029 then
                    failwith "Unterminated string"
                appendCodePoint sb cp
                i <- i + consumed

        if not finished then failwith "Unterminated string"
        ctx.codeUnitIndex <- i + 1
        if isNull sb then String.Empty else sb.ToString()

    let readString<'R when 'R : struct and 'R :> IJsonReader>
        (ctx: byref<ParserContext<'R>>) (quote: byte) : Token =
        let start = ctx.codeUnitIndex + 1
        let ascii = ctx.reader.AsciiStringRunLength(ctx.input,start,quote)
        if ctx.reader.TryPeekAsciiByte(ctx.input,start+ascii) = ValueSome quote then
            ctx.codeUnitIndex <- start+ascii+1
            StringToken(if ctx.decodeStrings then ctx.reader.AsciiString(ctx.input,start,ascii) else String.Empty)
        else
            let count = ascii + ctx.reader.UnescapedStringRunLength(ctx.input,start+ascii,quote)
            if ctx.reader.TryPeekAsciiByte(ctx.input,start+count) = ValueSome quote then
                try
                    let value = ctx.reader.StringRun(ctx.input,start,count,ctx.decodeStrings)
                    ctx.codeUnitIndex <- start+count+1
                    StringToken value
                with :? DecoderFallbackException -> StringToken(readRemainder &ctx quote start 0)
            else StringToken(readRemainder &ctx quote start count)
