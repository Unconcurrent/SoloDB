namespace SoloDatabase.JsonSerializator

open System
open System.Buffers
open System.Buffers.Text
open System.Globalization
open Microsoft.FSharp.NativeInterop

#nowarn "9"

module internal JsonNumberSpelling =
    let private zeroes<'E,'O when 'O : struct and 'O :> IJsonTextOutput<'E>>
        (output: 'O) count (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) =
        for _ = 1 to count do output.Ascii('0'B,&destination,&state)

    let decimal<'E,'O when 'O : struct and 'O :> IJsonTextOutput<'E>>
        (output: 'O) (source: ReadOnlySpan<'E>) (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) =
        let mutable dot = -1
        for index = 0 to source.Length - 1 do
            if output.AsciiCode source[index] = '.'B then dot <- index
        let mutable finish = source.Length
        if dot >= 0 then
            while finish > dot + 1 && output.AsciiCode source[finish-1] = '0'B do finish <- finish - 1
            if finish = dot + 1 then finish <- dot
        JsonWriteBuffer.appendSpan &destination &state (source.Slice(0,finish))

    let floating<'E,'O when 'O : struct and 'O :> IJsonTextOutput<'E>>
        (output: 'O) (source: ReadOnlySpan<'E>) fixedSpelling (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) =
        let mutable exponentStart = -1
        let mutable dot = -1
        for index = 0 to source.Length - 1 do
            match output.AsciiCode source[index] with
            | 'e'B | 'E'B -> exponentStart <- index
            | '.'B -> dot <- index
            | _ -> ()
        if exponentStart < 0 || not fixedSpelling then
            JsonWriteBuffer.appendSpan &destination &state source
        else
            let sign = if output.AsciiCode source[0] = '-'B then 1 else 0
            let mutable index = exponentStart + 1
            let negativeExponent = output.AsciiCode source[index] = '-'B
            if negativeExponent || output.AsciiCode source[index] = '+'B then index <- index + 1
            let mutable exponent = 0
            while index < source.Length do
                exponent <- exponent * 10 + int(output.AsciiCode source[index] - '0'B)
                index <- index + 1
            if negativeExponent then exponent <- -exponent
            let digits = exponentStart - sign - (if dot >= 0 then 1 else 0)
            let point = (if dot >= 0 then dot else exponentStart) - sign + exponent
            if sign <> 0 then output.Ascii('-'B,&destination,&state)
            if point <= 0 then
                output.Ascii('0'B,&destination,&state)
                output.Ascii('.'B,&destination,&state)
                zeroes output (-point) &destination &state
            let mutable consumed = 0
            for index = sign to exponentStart - 1 do
                let value = output.AsciiCode source[index]
                if value <> '.'B then
                    if point > 0 && point < digits && consumed = point then output.Ascii('.'B,&destination,&state)
                    output.Ascii(value,&destination,&state)
                    consumed <- consumed + 1
            if point > digits then zeroes output (point-digits) &destination &state

module internal JsonRawNumericWriter =
    let private faithfulFloatingRange value =
        let magnitude = abs value
        magnitude >= 1e-28 && magnitude <= 79228162514264337593543950335.0

    let int64Utf8 (value: int64) (destination: byref<Span<byte>>) (state: byref<JsonWriteState<byte>>) =
        let pointer = NativePtr.stackalloc<byte> 128
        let buffer = Span<byte>(NativePtr.toVoidPtr pointer,128)
        let mutable written = 0
        if not(Utf8Formatter.TryFormat(value,buffer,&written,StandardFormat())) then invalidOp "Cannot format the JSON number."
        let source = ReadOnlySpan<byte>(NativePtr.toVoidPtr pointer,written)
        JsonWriteBuffer.appendSpan &destination &state source

    let uint64Utf8 (value: uint64) (destination: byref<Span<byte>>) (state: byref<JsonWriteState<byte>>) =
        let pointer = NativePtr.stackalloc<byte> 128
        let buffer = Span<byte>(NativePtr.toVoidPtr pointer,128)
        let mutable written = 0
        if not(Utf8Formatter.TryFormat(value,buffer,&written,StandardFormat())) then invalidOp "Cannot format the JSON number."
        let source = ReadOnlySpan<byte>(NativePtr.toVoidPtr pointer,written)
        JsonWriteBuffer.appendSpan &destination &state source

    let decimalUtf8 (value: decimal) (destination: byref<Span<byte>>) (state: byref<JsonWriteState<byte>>) =
        if value = 0M then JsonWriteBuffer.append &destination &state ('0'B)
        else
            let pointer = NativePtr.stackalloc<byte> 128
            let buffer = Span<byte>(NativePtr.toVoidPtr pointer,128)
            let mutable written = 0
            if not(Utf8Formatter.TryFormat(value,buffer,&written,StandardFormat())) then invalidOp "Cannot format the JSON number."
            let source = ReadOnlySpan<byte>(NativePtr.toVoidPtr pointer,written)
            JsonNumberSpelling.decimal (JsonUtf8Output()) source &destination &state

    let doubleUtf8 (value: double) (destination: byref<Span<byte>>) (state: byref<JsonWriteState<byte>>) =
        if value = 0.0 then
            if BitConverter.DoubleToInt64Bits(float value) < 0L then JsonWriteBuffer.append &destination &state ('-'B)
            JsonWriteBuffer.append &destination &state ('0'B)
        else
            let pointer = NativePtr.stackalloc<byte> 128
            let buffer = Span<byte>(NativePtr.toVoidPtr pointer,128)
            let mutable written = 0
            if not(Utf8Formatter.TryFormat(value,buffer,&written,StandardFormat())) then invalidOp "Cannot format the JSON number."
            let source = ReadOnlySpan<byte>(NativePtr.toVoidPtr pointer,written)
            JsonNumberSpelling.floating (JsonUtf8Output()) source (faithfulFloatingRange (float value)) &destination &state

    let singleUtf8 (value: single) (destination: byref<Span<byte>>) (state: byref<JsonWriteState<byte>>) =
        if value = 0.0f then
            if BitConverter.DoubleToInt64Bits(float value) < 0L then JsonWriteBuffer.append &destination &state ('-'B)
            JsonWriteBuffer.append &destination &state ('0'B)
        else
            let pointer = NativePtr.stackalloc<byte> 128
            let buffer = Span<byte>(NativePtr.toVoidPtr pointer,128)
            let mutable written = 0
            if not(Utf8Formatter.TryFormat(value,buffer,&written,StandardFormat())) then invalidOp "Cannot format the JSON number."
            let source = ReadOnlySpan<byte>(NativePtr.toVoidPtr pointer,written)
            JsonNumberSpelling.floating (JsonUtf8Output()) source (faithfulFloatingRange (float value)) &destination &state

    let int64Utf16 (value: int64) (destination: byref<Span<char>>) (state: byref<JsonWriteState<char>>) =
        let pointer = NativePtr.stackalloc<char> 128
        let buffer = Span<char>(NativePtr.toVoidPtr pointer,128)
        let mutable written = 0
        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        if not(value.TryFormat(buffer,&written,"G".AsSpan(),CultureInfo.InvariantCulture)) then invalidOp "Cannot format the JSON number."
        #else
        let text = value.ToString("G",CultureInfo.InvariantCulture)
        text.AsSpan().CopyTo buffer
        written <- text.Length
        #endif
        let source = ReadOnlySpan<char>(NativePtr.toVoidPtr pointer,written)
        JsonWriteBuffer.appendSpan &destination &state source

    let uint64Utf16 (value: uint64) (destination: byref<Span<char>>) (state: byref<JsonWriteState<char>>) =
        let pointer = NativePtr.stackalloc<char> 128
        let buffer = Span<char>(NativePtr.toVoidPtr pointer,128)
        let mutable written = 0
        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        if not(value.TryFormat(buffer,&written,"G".AsSpan(),CultureInfo.InvariantCulture)) then invalidOp "Cannot format the JSON number."
        #else
        let text = value.ToString("G",CultureInfo.InvariantCulture)
        text.AsSpan().CopyTo buffer
        written <- text.Length
        #endif
        let source = ReadOnlySpan<char>(NativePtr.toVoidPtr pointer,written)
        JsonWriteBuffer.appendSpan &destination &state source

    let decimalUtf16 (value: decimal) (destination: byref<Span<char>>) (state: byref<JsonWriteState<char>>) =
        if value = 0M then JsonWriteBuffer.append &destination &state '0'
        else
            let pointer = NativePtr.stackalloc<char> 128
            let buffer = Span<char>(NativePtr.toVoidPtr pointer,128)
            let mutable written = 0
            #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
            if not(value.TryFormat(buffer,&written,"G".AsSpan(),CultureInfo.InvariantCulture)) then invalidOp "Cannot format the JSON number."
            #else
            let text = value.ToString("G",CultureInfo.InvariantCulture)
            text.AsSpan().CopyTo buffer
            written <- text.Length
            #endif
            let source = ReadOnlySpan<char>(NativePtr.toVoidPtr pointer,written)
            JsonNumberSpelling.decimal (JsonUtf16Output()) source &destination &state

    let doubleUtf16 (value: double) (destination: byref<Span<char>>) (state: byref<JsonWriteState<char>>) =
        if value = 0.0 then
            if BitConverter.DoubleToInt64Bits(float value) < 0L then JsonWriteBuffer.append &destination &state '-'
            JsonWriteBuffer.append &destination &state '0'
        else
            let pointer = NativePtr.stackalloc<char> 128
            let buffer = Span<char>(NativePtr.toVoidPtr pointer,128)
            let mutable written = 0
            #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
            if not(value.TryFormat(buffer,&written,"R".AsSpan(),CultureInfo.InvariantCulture)) then invalidOp "Cannot format the JSON number."
            #else
            let text = value.ToString("R",CultureInfo.InvariantCulture)
            text.AsSpan().CopyTo buffer
            written <- text.Length
            #endif
            let source = ReadOnlySpan<char>(NativePtr.toVoidPtr pointer,written)
            JsonNumberSpelling.floating (JsonUtf16Output()) source (faithfulFloatingRange (float value)) &destination &state

    let singleUtf16 (value: single) (destination: byref<Span<char>>) (state: byref<JsonWriteState<char>>) =
        if value = 0.0f then
            if BitConverter.DoubleToInt64Bits(float value) < 0L then JsonWriteBuffer.append &destination &state '-'
            JsonWriteBuffer.append &destination &state '0'
        else
            let pointer = NativePtr.stackalloc<char> 128
            let buffer = Span<char>(NativePtr.toVoidPtr pointer,128)
            let mutable written = 0
            #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
            if not(value.TryFormat(buffer,&written,"R".AsSpan(),CultureInfo.InvariantCulture)) then invalidOp "Cannot format the JSON number."
            #else
            let text = value.ToString("R",CultureInfo.InvariantCulture)
            text.AsSpan().CopyTo buffer
            written <- text.Length
            #endif
            let source = ReadOnlySpan<char>(NativePtr.toVoidPtr pointer,written)
            JsonNumberSpelling.floating (JsonUtf16Output()) source (faithfulFloatingRange (float value)) &destination &state
