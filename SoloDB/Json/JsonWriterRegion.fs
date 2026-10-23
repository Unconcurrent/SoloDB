namespace SoloDatabase.JsonSerializator

open System
open System.Runtime.InteropServices
open System.Runtime.CompilerServices

/// Encoded persisted identity is prepared once for the original closed CLR type.
type private JsonTypeNameLiteral<'T,'E>() =
    [<DefaultValue>]
    static val mutable private encoded: 'E[]
    static do
        let name = SoloDatabase.Metadata.Generation.TypeSupport.StoredTypeName typeof<'T>.FullName
        let text =
            match name with
            | Some name ->
                let property = JsonStringLiteral.property name
                property.Substring(0,property.Length-1)
            | None -> ""
        JsonTypeNameLiteral<'T,'E>.encoded <-
            if typeof<'E>.Equals(typeof<char>) then box(text.ToCharArray()) :?> 'E[]
            else box(System.Text.Encoding.UTF8.GetBytes(text)) :?> 'E[]
    static member Value = JsonTypeNameLiteral<'T,'E>.encoded

/// One native encoded image, shared by every concrete numeric writer of this encoding.
type private JsonDigitPairs<'E>() =
    [<DefaultValue>]
    static val mutable private encoded: 'E[]
    static do
        if typeof<'E>.Equals(typeof<char>) then
            let image = Array.init 200 (fun index -> char(int '0' + (if index % 2 = 0 then index / 20 else index / 2 % 10)))
            JsonDigitPairs<'E>.encoded <- box image :?> 'E[]
        else
            let image = Array.init 200 (fun index -> byte(int '0' + (if index % 2 = 0 then index / 20 else index / 2 % 10)))
            JsonDigitPairs<'E>.encoded <- box image :?> 'E[]
    static member Value = JsonDigitPairs<'E>.encoded

type private JsonBase64Alphabet() =
    static let encoded = System.Text.Encoding.ASCII.GetBytes("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
    static member Value = encoded

[<AbstractClass; Sealed>]
type internal JsonWriterRegion =
    static member internal Base64LengthOverflow(): unit = raise(OutOfMemoryException())

    static member internal Base64Reference(): byref<byte> =
        &MemoryMarshal.GetReference(Span<byte>(JsonBase64Alphabet.Value))

    static member internal DigitPairReference<'E>(): byref<byte> =
        &Unsafe.As<'E,byte>(&MemoryMarshal.GetReference(Span<'E>(JsonDigitPairs<'E>.Value)))

    static member internal TypeNameLiteral<'T,'E>(): 'E[] = JsonTypeNameLiteral<'T,'E>.Value

    static member internal LiteralReference<'E>(value: 'E[]): byref<byte> =
        &Unsafe.As<'E,byte>(&MemoryMarshal.GetReference(Span<'E>(value)))

    static member internal ReserveSpan<'E>(destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>, minimum: int): unit =
        if minimum > destination.Length then JsonWriteBuffer.ensureSpan &destination &state minimum

    static member internal AddList<'E>(state: byref<JsonWriteState<'E>>, value: 'E): unit =
        JsonWriteBuffer.addToList &state value

    static member internal FitsSpan<'E>(destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>, count: int): bool =
        count <= destination.Length - state.Written

    static member internal TryReserveKnownSpan<'E>(destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>, count: int): bool =
        if JsonWriterRegion.FitsSpan(&destination,&state,count) then true
        elif isNull state.Grow && state.Written = destination.Length && count <= Int32.MaxValue - state.Written then
            JsonWriteBuffer.ensureSpan &destination &state (state.Written + count)
            true
        else false

    static member internal Start<'E>(destination: byref<Span<'E>>): byref<byte> =
        &Unsafe.As<'E,byte>(&MemoryMarshal.GetReference(destination))

    static member internal Position<'E>(state: byref<JsonWriteState<'E>>): int = state.Written

    static member internal Advance<'E>(state: byref<JsonWriteState<'E>>, count: int): unit =
        state.Written <- state.Written + count

    static member internal DecimalSource(value: byref<decimal>): byref<byte> =
        &Unsafe.As<decimal,byte>(&value)

    static member internal Source<'T>(value: byref<'T>): byref<byte> =
        &Unsafe.As<'T,byte>(&value)

    static member private Capture<'E>(value: byte[], overlaps: bool, available: int, state: byref<JsonWriteState<'E>>, count: int): byte[] =
        if (overlaps || (not(isNull state.List) && isNull state.ConcreteList) ||
                                 (not(isNull state.Grow) && count > available)) then
            Array.copy value
        else value

    static member internal CaptureBytesUtf8(value: byte[], destination: byref<Span<byte>>, state: byref<JsonWriteState<byte>>, count: int): byte[] =
        let overlaps = Span<byte>(value).Overlaps(destination)
        JsonWriterRegion.Capture(value,overlaps,destination.Length - state.Written,&state,count)

    static member internal CaptureBytesUtf16(value: byte[], destination: byref<Span<char>>, state: byref<JsonWriteState<char>>, count: int): byte[] =
        // A valid char view of this byte array can overlap only its complete char units.
        let overlaps = MemoryMarshal.Cast<byte,char>(Span<byte>(value)).Overlaps(destination)
        JsonWriterRegion.Capture(value,overlaps,destination.Length - state.Written,&state,count)

    static member internal BytesReference(value: byte[]): byref<byte> =
        &MemoryMarshal.GetReference(Span<byte>(value))

    static member internal StringReference(value: string): byref<byte> =
        &Unsafe.As<char,byte>(&MemoryMarshal.GetReference(value.AsSpan()))
