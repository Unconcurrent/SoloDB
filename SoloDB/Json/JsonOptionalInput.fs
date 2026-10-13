namespace SoloDatabase.JsonSerializator

/// Optional compatibility retains only its two source ranges, never a property map.
[<AbstractClass; Sealed>]
type internal JsonOptionalInput =
    static member internal Begin<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int =
        context.Consumed <- false
        context.Cursor.codeUnitIndex <- context.Range.Start
        let point = JsonLexer.nextSignificantCodePoint &context.Cursor
        let start = context.Cursor.codeUnitIndex
        context.Range <- { Start = start; Length = context.Range.Start + context.Range.Length - start }
        if JsonLexer.tryReadNullAtPoint &context.Cursor point then
            context.Consumed <- true
            -1
        else point

    static member internal Open<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit =
        context.Cursor.codeUnitIndex <- context.Cursor.codeUnitIndex + 1

    static member internal Capture<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, value: byref<JsonReadContext<'R>>): unit =
        value <- JsonReadContext(context.Cursor,{ Start = context.Range.Start; Length = context.Cursor.codeUnitIndex - context.Range.Start })

    static member internal End<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, original: byref<JsonReadContext<'R>>): unit =
        original.Range <- { Start = original.Range.Start; Length = context.Cursor.codeUnitIndex - original.Range.Start }
        original.Cursor.codeUnitIndex <- context.Cursor.codeUnitIndex
        original.Consumed <- true
        original.Properties <- null
        original.Elements <- null
        context <- original

    static member internal Flat<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit =
        context.Cursor.codeUnitIndex <- context.Range.Start
        context.Consumed <- false

    static member internal Restore<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, original: byref<JsonReadContext<'R>>): unit =
        context <- original

    static member internal Missing(): unit = failwith "Property not found"
