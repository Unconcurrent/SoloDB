namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Globalization
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text

module internal Utf16Helpers =
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline utf16CharLength (input: ReadOnlySpan<byte>) =
        input.Length >>> 1

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline readUtf16Char (input: ReadOnlySpan<byte>) (charIndex: int) =
        let byteIndex = charIndex <<< 1
        MemoryMarshal.Read<char>(input.Slice(byteIndex, 2))

    let inline decodeCodePointFrom (input: ReadOnlySpan<byte>) codeUnitIndex (ch: char) =
        if Char.IsHighSurrogate(ch) then
            let charsLength = utf16CharLength input
            if codeUnitIndex + 1 >= charsLength then
                failwith "Invalid high surrogate at end of input"
            let ch2 = readUtf16Char input (codeUnitIndex + 1)
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

    let inline decodeCodePoint (input: ReadOnlySpan<byte>) codeUnitIndex =
        decodeCodePointFrom input codeUnitIndex (readUtf16Char input codeUnitIndex)

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
