namespace SoloDatabase.JsonSerializator

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices

/// Mechanical access to one borrowed numeric input; conversion is emitted by the codec generator.
[<AbstractClass; Sealed>]
type internal JsonNumericInput =
    static member internal Begin<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int =
        context.Consumed <- false
        JsonReadCursor.reset &context
        JsonLexer.nextSignificantCodePoint &context.Cursor

    static member internal Reference<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): byref<byte> =
        &MemoryMarshal.GetReference(context.Cursor.input)

    static member internal Length<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int =
        context.Cursor.codeUnitLength

    static member internal Position<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): byref<int> =
        &context.Cursor.codeUnitIndex

    static member internal Complete<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit =
        context.Consumed <- true

    static member internal Consumed<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): bool =
        context.Consumed

    /// Only newly allocated, unpublished numeric text is supplied by the shared emitter.
    static member internal StringReference(value: string): byref<byte> =
        &Unsafe.As<char,byte>(&MemoryMarshal.GetReference(value.AsSpan()))

    static member internal Culture() = System.Globalization.CultureInfo.InvariantCulture
