namespace SoloDatabase.JsonSerializator

open System

module internal JsonRawNumbers =
    let inline private ascii<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) index =
        match context.Cursor.reader.TryPeekAsciiByte(context.Cursor.input,index) with
        | ValueSome value -> value
        | ValueNone -> raise(FormatException "Invalid number")

    // Integer conversion truncates fractional JSON numbers as the existing numeric cast does.
    // Plain integers accumulate during the digit scan; scaled forms use their resulting digits.
    let private magnitude<'R when 'R : struct and 'R :> IJsonReader>
        (context: byref<JsonReadContext<'R>>) literal signed =
        match literal with
        | NaNLiteral -> struct(0UL,false)
        | InfinityLiteral _ -> raise(OverflowException())
        | HexLiteral(value,negative,overflow) ->
            if overflow then raise(OverflowException())
            struct(value,negative)
        | DecimalLiteral(start,length) ->
            let finish = start + length
            let mutable index = start
            let negative = ascii &context index = '-'B
            if negative || ascii &context index = '+'B then index <- index + 1
            let digitsStart = index
            let limit = if signed then (if negative then 9223372036854775808UL else uint64 Int64.MaxValue) else UInt64.MaxValue
            let mutable integerValue = 0UL
            let mutable integerOverflow = false
            let mutable integerEnd = false
            while index < finish && not integerEnd do
                let digit = ascii &context index
                if NumberParsing.isDigitByte digit then
                    let digit = uint64(digit - '0'B)
                    if not integerOverflow then
                        if integerValue > (limit - digit) / 10UL then integerOverflow <- true
                        else integerValue <- integerValue * 10UL + digit
                    index <- index + 1
                else integerEnd <- true
            if index = finish then
                if integerOverflow then raise(OverflowException())
                struct(integerValue,negative)
            else
                let integerDigits = index - digitsStart
                let mutable allDigits = integerDigits
                if index < finish && ascii &context index = '.'B then
                    index <- index + 1
                    let fractionStart = index
                    while index < finish && NumberParsing.isDigitByte(ascii &context index) do index <- index + 1
                    allDigits <- allDigits + index - fractionStart
                let exponentLimit = int64 allDigits + 20L
                let mutable exponent = 0L
                if index < finish then
                    index <- index + 1
                    let exponentNegative = ascii &context index = '-'B
                    if exponentNegative || ascii &context index = '+'B then index <- index + 1
                    while index < finish do
                        exponent <- min exponentLimit (exponent * 10L + int64(ascii &context index - '0'B))
                        index <- index + 1
                    if exponentNegative then exponent <- -exponent
                let needed = int64 integerDigits + exponent
                let mutable value = 0UL
                if needed > 0L then
                    index <- digitsStart
                    let mutable consumed = 0L
                    while consumed < min needed (int64 allDigits) do
                        let digit = ascii &context index
                        index <- index + 1
                        if digit <> '.'B then
                            let digit = uint64(digit - '0'B)
                            if value > (limit - digit) / 10UL then raise(OverflowException())
                            value <- value * 10UL + digit
                            consumed <- consumed + 1L
                    if value <> 0UL then
                        if needed - consumed > 19L then raise(OverflowException())
                        while consumed < needed do
                            if value > limit / 10UL then raise(OverflowException())
                            value <- value * 10UL
                            consumed <- consumed + 1L
                struct(value,negative)

    let signed<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) literal =
        let struct(value,negative) = magnitude &context literal true
        if negative then
            if value > 9223372036854775808UL then raise(OverflowException())
            elif value = 9223372036854775808UL then Int64.MinValue
            else -int64 value
        elif value > uint64 Int64.MaxValue then raise(OverflowException())
        else int64 value

    let unsigned<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) literal =
        let struct(value,negative) = magnitude &context literal false
        if negative && value <> 0UL then raise(OverflowException())
        value

    let decimal<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) literal =
        JsonGrammar.treeNumber &context.Cursor literal

    let double<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) literal =
        match literal with
        | DecimalLiteral(start,length) -> context.Cursor.reader.ParseDouble(context.Cursor.input,start,length)
        | HexLiteral(value,negative,overflow) ->
            if overflow then raise(OverflowException())
            let number = float value
            if negative then -number else number
        | InfinityLiteral negative -> if negative then Double.NegativeInfinity else Double.PositiveInfinity
        | NaNLiteral -> Double.NaN

    let single<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) literal =
        match literal with
        | DecimalLiteral(start,length) -> context.Cursor.reader.ParseSingle(context.Cursor.input,start,length)
        | HexLiteral(value,negative,overflow) ->
            if overflow then raise(OverflowException())
            let number = float32 value
            if negative then -number else number
        | InfinityLiteral negative -> if negative then Single.NegativeInfinity else Single.PositiveInfinity
        | NaNLiteral -> Single.NaN
