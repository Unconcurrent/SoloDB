namespace SoloDatabase.JsonSerializator

/// Logical name units reuse the quoted and identifier escape authorities.
[<AbstractClass; Sealed>]
type internal JsonNameInput =
    static member internal Begin<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, first: bool): int =
        JsonGrammar.beginPropertyName &context.Cursor first

    static member internal Quoted<'R when 'R : struct and 'R :> IJsonReader>
        (context: byref<JsonReadContext<'R>>, quote: int, point: int): int =
        let index = context.Cursor.codeUnitIndex
        if point = quote then
            context.Cursor.codeUnitIndex <- index + 1
            -1
        elif point = 10 || point = 13 then failwith "Unterminated string"
        elif point = int '\\' then
            let struct(value,finish) = JsonStringReader.readEscape &context.Cursor index
            context.Cursor.codeUnitIndex <- finish
            if value < 0 then -2 else value
        else
            let struct(value,count) =
                if point < 128 then struct(point,1)
                else context.Cursor.reader.DecodeCodePointFrom(context.Cursor.input,index,point)
            if value = 0x2028 || value = 0x2029 then failwith "Unterminated string"
            context.Cursor.codeUnitIndex <- index + count
            value

    static member internal Identifier<'R when 'R : struct and 'R :> IJsonReader>
        (context: byref<JsonReadContext<'R>>, first: bool, point: int, decoded: bool, width: int, escaped: byref<bool>): int =
        let index = context.Cursor.codeUnitIndex
        if point = int '\\' then
            let struct(value,finish) = JsonIdentifiers.readEscape &context.Cursor index first
            context.Cursor.codeUnitIndex <- finish
            escaped <- true
            value
        else
            let struct(value,count) =
                if decoded then struct(point,width)
                else context.Cursor.reader.DecodeCodePointFrom(context.Cursor.input,index,point)
            let valid =
                if value < 128 then JsonIdentifierSyntax.isStart value || (not first && uint32(value - JsonIdentifierSyntax.Digit) <= uint32 JsonIdentifierSyntax.DigitRange)
                elif first then JsonIdentifiers.isIdentifierStartCodePoint value
                else JsonIdentifiers.isIdentifierCharCodePoint value
            if valid then context.Cursor.codeUnitIndex <- index + count; value else -1

    static member internal Invalid(): unit = failwith "Invalid object syntax"
    static member internal Unterminated(): unit = failwith "Unterminated string"

    static member internal Unsupported<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit =
        context.Cursor.decodeStrings <- false
        JsonLexer.readNext &context.Cursor |> JsonGrammar.propertyTokenName |> ignore

    static member internal Colon<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit =
        JsonGrammar.completePropertyName &context.Cursor

    static member internal Value<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit =
        context.Range <- { Start = context.Cursor.codeUnitIndex; Length = context.Cursor.codeUnitLength - context.Cursor.codeUnitIndex; Depth = context.Cursor.containerDepth }
        context.Properties <- null
        context.Elements <- null
        context.Consumed <- false
