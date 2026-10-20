namespace SoloDatabase.JsonSerializator

/// Union capture retains only known ranges and the already admitted predicate token.
[<AbstractClass; Sealed>]
type internal JsonUnionInput =
    static member Begin<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): bool =
        JsonReadCursor.reset &context
        context.Cursor.decodeStrings <- false
        let start = context.Cursor.codeUnitIndex
        let token = JsonLexer.readNext &context.Cursor
        match token with
        | NullToken ->
            context.Consumed <- true
            false
        | OpenBrace -> true
        | _ ->
            let mutable sink = JsonDiscardSink<'R>()
            JsonGrammar.fromToken<int,int,int,'R,JsonDiscardSink<'R>> &sink &context.Cursor start token false |> ignore
            JsonTupleInput.Missing()
            false

    static member Capture<'R when 'R : struct and 'R :> IJsonReader>
        (context: byref<JsonReadContext<'R>>, truth: byref<bool>): JsonSourceRange =
        let start = context.Cursor.codeUnitIndex
        context.Cursor.decodeStrings <- false
        let token = JsonLexer.readNext &context.Cursor
        let mutable sink = JsonDiscardSink<'R>()
        JsonGrammar.fromToken<int,int,int,'R,JsonDiscardSink<'R>> &sink &context.Cursor start token false |> ignore
        truth <- (match token with BooleanToken true -> true | _ -> false)
        { Start = start; Length = context.Cursor.codeUnitIndex - start; Depth = context.Cursor.containerDepth }
