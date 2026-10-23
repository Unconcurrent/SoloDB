namespace SoloDatabase.JsonSerializator

open System
open System.Collections.Generic
open System.Text
open System.Numerics
open System.Runtime.InteropServices
open Microsoft.FSharp.NativeInterop

#nowarn "9"

/// Supplies replacement storage with at least the requested capacity.
type JsonBufferGrow<'Element> = delegate of minimumCapacity: int -> Span<'Element>

/// Releases superseded storage after its written prefix has been copied to its replacement.
type JsonBufferRelease<'Element> = delegate of storage: Span<'Element> -> unit

[<Struct>]
type internal JsonWriteState<'Element> =
    val mutable Written: int
    val mutable Storage: 'Element array
    val mutable IncludeType: bool
    val mutable OmitId: bool
    val List: IList<'Element>
    val ConcreteList: List<'Element>
    val Grow: JsonBufferGrow<'Element>
    val Release: JsonBufferRelease<'Element>
    new(list: IList<'Element>, grow: JsonBufferGrow<'Element>, release: JsonBufferRelease<'Element>) =
        if isNull grow && not(isNull release) then
            invalidArg "release" "A release callback requires a custom growth callback."
        if not(isNull list) && list.IsReadOnly then
            invalidArg "destination" "The destination list is read-only."
        { Written = 0; Storage = Array.empty; IncludeType = false; OmitId = false; List = list; ConcreteList = (match list with :? List<'Element> as value -> value | _ -> null); Grow = grow; Release = release }

module internal JsonWriteBuffer =
    let private capacity current minimum =
        let mutable result = max 128 current
        while result < minimum && result <= Int32.MaxValue / 2 do result <- result * 2
        if result < minimum then minimum else result

    let ensureSpan (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) minimum =
        if minimum > destination.Length then
            let requested = capacity destination.Length minimum
            if isNull state.Grow then
                let storage = Array.zeroCreate<'E> requested
                state.Storage <- storage
                let replacement = Span<'E>(storage)
                destination.Slice(0,state.Written).CopyTo replacement
                destination <- replacement
            else
                let replacement = state.Grow.Invoke requested
                let overlaps = replacement.Overlaps destination
                if replacement.Length < requested || (not(isNull state.Release) && overlaps) then
                    // Never release live overlapping storage when rejecting a replacement.
                    if not(isNull state.Release) && not overlaps && not replacement.IsEmpty then
                        try state.Release.Invoke replacement with _ -> ()
                    invalidOp "The growth callback must return independent storage with the requested capacity."
                let old = destination
                old.Slice(0,state.Written).CopyTo replacement
                destination <- replacement
                if not(isNull state.Release) then state.Release.Invoke old

    let ensureList (list: List<'E>) count =
        let total = Checked.(+) list.Count count
        if total > list.Capacity then list.Capacity <- capacity list.Capacity total

    let ensure (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) count =
        ensureSpan &destination &state (Checked.(+) state.Written count)

    let append (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) value =
        ensure &destination &state 1
        destination[state.Written] <- value
        state.Written <- state.Written + 1

    let appendSpan (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) (source: ReadOnlySpan<'E>) =
        ensure &destination &state source.Length
        source.CopyTo(destination.Slice(state.Written))
        state.Written <- state.Written + source.Length

    let addToList (state: byref<JsonWriteState<'E>>) value =
        if isNull state.ConcreteList then state.List.Add(value)
        else
            ensureList state.ConcreteList 1
            state.ConcreteList.Add(value)

    let appendIList (state: byref<JsonWriteState<'E>>) value =
        Checked.(+) state.Written 1 |> ignore
        addToList &state value
        state.Written <- state.Written + 1

    let appendIListSpan (state: byref<JsonWriteState<'E>>) (source: ReadOnlySpan<'E>) =
        Checked.(+) state.Written source.Length |> ignore
        for index = 0 to source.Length - 1 do
            addToList &state source[index]
            state.Written <- state.Written + 1

    let private listUnit (state: byref<JsonWriteState<byte>>) value =
        addToList &state value
        state.Written <- state.Written + 1

    let private listRegion units (state: byref<JsonWriteState<byte>>) =
        Checked.(+) state.Written units |> ignore

    let utf8ListText (value: string) start count (state: byref<JsonWriteState<byte>>) =
        let finish = start + count
        let mutable index = start
        while index < finish do
            let c = int value[index]
            if c < 0x80 then
                listRegion 1 &state
                listUnit &state (byte c)
            elif c < 0x800 then
                listRegion 2 &state
                listUnit &state (byte(0xC0 ||| (c >>> 6)))
                listUnit &state (byte(0x80 ||| (c &&& 63)))
            else
                let mutable code = c
                if c >= 0xD800 && c <= 0xDFFF then
                    if c <= 0xDBFF && index + 1 < finish && Char.IsLowSurrogate value[index+1] then
                        code <- Char.ConvertToUtf32(value[index],value[index+1])
                        index <- index + 1
                    else code <- 0xFFFD
                if code > 0xFFFF then
                    listRegion 4 &state
                    listUnit &state (byte(0xF0 ||| (code >>> 18)))
                    listUnit &state (byte(0x80 ||| ((code >>> 12) &&& 63)))
                else
                    listRegion 3 &state
                    listUnit &state (byte(0xE0 ||| (code >>> 12)))
                listUnit &state (byte(0x80 ||| ((code >>> 6) &&& 63)))
                listUnit &state (byte(0x80 ||| (code &&& 63)))
            index <- index + 1

type internal IJsonTextOutput<'Element> =
    abstract AsciiCode: 'Element -> byte
    abstract Ascii: byte * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit
    abstract Text: string * int * int * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit
    abstract Raw: ReadOnlySpan<'Element> * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit

[<Struct>]
type internal JsonUtf16Output =
    interface IJsonTextOutput<char> with
        member _.AsciiCode(value) = byte value
        member _.Ascii(value,destination,state) = JsonWriteBuffer.append &destination &state (char value)
        member _.Raw(value,destination,state) = JsonWriteBuffer.appendSpan &destination &state value
        member _.Text(value,start,count,destination,state) =
            JsonWriteBuffer.appendSpan &destination &state (value.AsSpan(start,count))

[<Struct>]
type internal JsonUtf8Output =
    interface IJsonTextOutput<byte> with
        member _.AsciiCode(value) = value
        member _.Ascii(value,destination,state) = JsonWriteBuffer.append &destination &state value
        member _.Raw(value,destination,state) = JsonWriteBuffer.appendSpan &destination &state value
        member _.Text(value,start,count,destination,state) =
            if count > 0 then
                #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
                let source = value.AsSpan(start,count)
                let bytes = Encoding.UTF8.GetByteCount(source)
                JsonWriteBuffer.ensure &destination &state bytes
                let written = Encoding.UTF8.GetBytes(source,destination.Slice(state.Written))
                #else
                use pinnedSource = fixed value
                let source = NativePtr.add pinnedSource start
                let bytes = Encoding.UTF8.GetByteCount(source,count)
                JsonWriteBuffer.ensure &destination &state bytes
                use target = fixed destination
                let written = Encoding.UTF8.GetBytes(source,count,NativePtr.add target state.Written,destination.Length-state.Written)
                #endif
                state.Written <- state.Written + written
[<Struct>]
type internal JsonUtf8IListOutput =
    interface IJsonTextOutput<byte> with
        member _.AsciiCode(value) = value
        member _.Ascii(value,_,state) = JsonWriteBuffer.appendIList &state value
        member _.Raw(value,_,state) = JsonWriteBuffer.appendIListSpan &state value
        member _.Text(value,start,count,_,state) =
            JsonWriteBuffer.utf8ListText value start count &state

[<Struct>]
type internal JsonUtf16IListOutput =
    interface IJsonTextOutput<char> with
        member _.AsciiCode(value) = byte value
        member _.Ascii(value,_,state) = JsonWriteBuffer.appendIList &state (char value)
        member _.Raw(value,_,state) = JsonWriteBuffer.appendIListSpan &state value
        member _.Text(value,start,count,_,state) =
            JsonWriteBuffer.appendIListSpan &state (value.AsSpan(start,count))

module internal JsonStringEscaping =
    // Skip safe ASCII in vectors; every exceptional or Unicode unit still uses escape.
    let private lower = Vector<uint16>(32us)
    let private upper = Vector<uint16>(126us)
    let private quote = Vector<uint16>(uint16 '"')
    let private slash = Vector<uint16>(uint16 '\\')
    let private apostrophe = Vector<uint16>(uint16 '\'')
    let private less = Vector<uint16>(uint16 '<')
    let private greater = Vector<uint16>(uint16 '>')
    let private ampersand = Vector<uint16>(uint16 '&')

    let nextEscape (text: string) start =
        let chars = MemoryMarshal.Cast<char,uint16>(text.AsSpan())
        let width = Vector<uint16>.Count
        let mutable index = start
        let mutable scanning = Vector.IsHardwareAccelerated
        while scanning && index <= chars.Length-width do
            let values = MemoryMarshal.Read<Vector<uint16>>(MemoryMarshal.AsBytes(chars.Slice(index,width)))
            let outside = Vector.BitwiseOr(Vector.LessThan(values,lower),Vector.GreaterThan(values,upper))
            let quotes = Vector.BitwiseOr(Vector.Equals(values,quote),Vector.Equals(values,apostrophe))
            let markup = Vector.BitwiseOr(Vector.Equals(values,less),Vector.Equals(values,greater))
            let punctuation = Vector.BitwiseOr(Vector.Equals(values,slash),Vector.Equals(values,ampersand))
            let special = Vector.BitwiseOr(Vector.BitwiseOr(outside,quotes),Vector.BitwiseOr(markup,punctuation))
            if Vector.EqualsAll(special,Vector<uint16>.Zero) then index <- index+width
            else scanning <- false
        while index < text.Length && JsonStringLiteral.escape text[index] = 0 do index <- index+1
        index


    let write<'E,'O when 'O : struct and 'O :> IJsonTextOutput<'E>>
        (output: 'O) (text: string) (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) =
        output.Ascii('"'B,&destination,&state)
        if not(String.IsNullOrEmpty text) then
            let mutable run = 0
            let mutable index = nextEscape text 0
            while index < text.Length do
                let code = JsonStringLiteral.escape text[index]
                if code <> 0 then
                    if index > run then output.Text(text,run,index-run,&destination,&state)
                    output.Ascii('\\'B,&destination,&state)
                    if code < 0 then output.Ascii(byte -code,&destination,&state)
                    else
                        let c = code - 1
                        output.Ascii('u'B,&destination,&state)
                        output.Ascii(byte JsonStringLiteral.hex[c >>> 12],&destination,&state)
                        output.Ascii(byte JsonStringLiteral.hex[(c >>> 8) &&& 15],&destination,&state)
                        output.Ascii(byte JsonStringLiteral.hex[(c >>> 4) &&& 15],&destination,&state)
                        output.Ascii(byte JsonStringLiteral.hex[c &&& 15],&destination,&state)
                    run <- index + 1
                index <- nextEscape text (index+1)
            if run < text.Length then output.Text(text,run,text.Length-run,&destination,&state)
        output.Ascii('"'B,&destination,&state)
