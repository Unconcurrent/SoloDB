namespace SoloDatabase.JsonSerializator

open System

/// Tuple capture uses the existing grammar at the current admitted cursor.
[<AbstractClass; Sealed>]
type internal JsonTupleInput =
    static member Begin<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int =
        match JsonReadCursor.token &context with
        | NullToken -> -1
        | OpenBracket -> 0
        | OpenBrace -> 1
        | _ -> invalidOp "A tuple requires a JSON list or object."

    static member NextArray<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, first: bool): bool =
        JsonGrammar.nextElement &context.Cursor first

    static member Capture<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, arrayElement: bool): JsonSourceRange =
        let start = context.Cursor.codeUnitIndex
        context.Cursor.decodeStrings <- false
        let token = JsonLexer.readNext &context.Cursor
        let mutable sink = JsonDiscardSink<'R>()
        JsonGrammar.fromToken<int,int,int,'R,JsonDiscardSink<'R>> &sink &context.Cursor start token arrayElement |> ignore
        { Start = start; Length = context.Cursor.codeUnitIndex - start; Depth = context.Cursor.containerDepth }

    static member Complete<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit =
        context.Range <- { context.Range with Length = context.Cursor.codeUnitIndex - context.Range.Start; Depth = context.Cursor.containerDepth }
        context.Consumed <- true

    static member Missing(): unit = failwith "Property not found"
