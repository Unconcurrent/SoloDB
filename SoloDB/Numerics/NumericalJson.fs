namespace SoloDatabase.Numerics

open System
open System.Numerics
open SoloDatabase.JsonSerializator

/// Conversion consumes the original, grammar-admitted numerical text, without a tree value.
module internal NumericalJson =
    type private InputRole = NativeScalar | NativeJson | StoredJson

    [<Struct>]
    type Input = private {
        Text: string
        Start: int
        Finish: int
        Token: Token
    }

    /// The one original token is retained until its fixed input role is selected.
    let readInput (text: string) =
        let mutable cursor = JsonLexer.fromString text
        JsonLexer.nextSignificantCodePoint &cursor |> ignore
        let start = cursor.codeUnitIndex
        let token = JsonLexer.readNext &cursor
        { Text = text; Start = start; Finish = cursor.codeUnitIndex; Token = token }

    let private complete (input: Input) =
        let mutable cursor = JsonLexer.fromString input.Text
        cursor.codeUnitIndex <- input.Finish
        match JsonLexer.readNext &cursor with EndOfInput -> () | _ -> failwith "Malformed json."

    let isStringInput input =
        match input.Token with StringToken _ -> true | _ -> false

    let isNullInput input =
        match input.Token with
        | NullToken -> complete input; true
        | _ -> false

    let private number (text: string) start finish literal =
        match literal with
        | NaNLiteral | InfinityLiteral _ -> raise(OverflowException())
        | HexLiteral(_,negative,_) ->
            let mutable index = start
            if text[index] = '+' || text[index] = '-' then index <- index + 1
            index <- index + 2
            let mutable value = BigInteger.Zero
            let mutable chunk = 0u
            let mutable count = 0
            while index < finish do
                let unit = text[index]
                let digit = if unit <= '9' then int unit - 48 elif unit <= 'F' then int unit - 55 else int unit - 87
                chunk <- (chunk <<< 4) + uint32 digit
                count <- count + 1
                if count = 7 then
                    value <- (value <<< 28) + BigInteger chunk
                    chunk <- 0u
                    count <- 0
                index <- index + 1
            if count > 0 then value <- (value <<< (count * 4)) + BigInteger chunk
            ExactNumber.ofBigInteger(if negative then -value else value)
        | DecimalLiteral(start,length) ->
            let finish = start + length
            let mutable index = start
            let negative = text[index] = '-'
            if negative || text[index] = '+' then index <- index + 1
            let mutable coefficient = BigInteger.Zero
            let mutable chunk = 0u
            let mutable count = 0
            let mutable fractional = 0
            let mutable afterPoint = false
            while index < finish && text[index] <> 'e' && text[index] <> 'E' do
                if text[index] = '.' then afterPoint <- true
                else
                    chunk <- chunk * 10u + uint32(int text[index] - 48)
                    count <- count + 1
                    if afterPoint then fractional <- fractional + 1
                    if count = 8 then
                        coefficient <- coefficient * 100000000I + BigInteger chunk
                        chunk <- 0u
                        count <- 0
                index <- index + 1
            if count > 0 then coefficient <- coefficient * BigInteger(pown 10u count) + BigInteger chunk
            let mutable exponent = BigInteger.Zero
            if index < finish then
                index <- index + 1
                let negativeExponent = text[index] = '-'
                if negativeExponent || text[index] = '+' then index <- index + 1
                while index < finish do
                    exponent <- exponent * 10I + BigInteger(int text[index] - 48)
                    index <- index + 1
                if negativeExponent then exponent <- -exponent
            ExactNumber.create (if negative then -coefficient else coefficient) (exponent - BigInteger fractional)

    let rec private consume role (input: Input) =
        match input.Token with
        | NumberToken literal ->
            let mutable cursor = JsonLexer.fromString input.Text
            cursor.codeUnitIndex <- input.Finish
            JsonGrammar.validateNumber &cursor literal
            complete input
            ValueSome(number input.Text input.Start input.Finish literal)
        | StringToken key when role = StoredJson ->
            complete input
            ValueSome(NumericKey.decode key)
        | StringToken text when role = NativeJson -> consume NativeScalar (readInput text)
        | BooleanToken value when role = NativeJson -> ValueSome(ExactNumber.ofBigInteger(if value then BigInteger.One else BigInteger.Zero))
        | NullToken when role <> NativeScalar ->
            complete input
            ValueNone
        | _ -> raise(FormatException("A numerical JSON value is required."))

    let parse text = (consume NativeScalar (readInput text)).Value
    let nativeInput input = consume NativeJson input
    let storedInput input = consume StoredJson input
    let nativeValue text = nativeInput (readInput text)
    let storedValue text = storedInput (readInput text)
