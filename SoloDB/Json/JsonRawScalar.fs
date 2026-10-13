namespace SoloDatabase.JsonSerializator

open System
open System.Globalization
open System.Numerics
open SoloDatabase
open SoloDatabase.Utils

type internal JsonImplNumeric =
    static member isIntegerString(value: string) =
        if value.Length = 0 then false
        else
            let mutable index = if value[0] = '+' || value[0] = '-' then 1 else 0
            let mutable valid = index < value.Length
            while valid && index < value.Length do
                valid <- Char.IsDigit value[index]
                index <- index + 1
            valid

exception internal JsonScalarShapeMismatch
exception internal JsonPrimitiveShapeMismatch of Type

module internal JsonRawScalar =
    let unexpected (_target: Type) = raise JsonScalarShapeMismatch

    let signed<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | NumberToken literal -> JsonRawNumbers.signed &context literal
        | StringToken value -> int64 value
        | _ -> unexpected typeof<int64>

    let unsigned<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | NumberToken literal -> JsonRawNumbers.unsigned &context literal
        | StringToken value -> uint64 value
        | _ -> unexpected typeof<uint64>

    let decimal<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | NumberToken(DecimalLiteral(start,length)) ->
            Decimal.Parse(context.Cursor.reader.AsciiString(context.Cursor.input,start,length),NumberStyles.Float,CultureInfo.InvariantCulture)
        | NumberToken(HexLiteral(magnitude,negative,overflow)) ->
            if overflow then raise(OverflowException())
            let value = decimal magnitude
            if negative then -value else value
        | NumberToken NaNLiteral | NumberToken(InfinityLiteral _) -> raise(OverflowException())
        | StringToken value -> decimal value
        | _ -> unexpected typeof<decimal>

    let double<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | NumberToken literal -> JsonRawNumbers.double &context literal
        | StringToken value -> float value
        | NullToken -> Double.NaN
        | _ -> unexpected typeof<double>

    let single<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | NumberToken literal -> JsonRawNumbers.single &context literal
        | StringToken value -> float32 value
        | NullToken -> Single.NaN
        | _ -> unexpected typeof<single>

    let string<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | StringToken value -> value
        | NumberToken NaNLiteral -> "NaN"
        | NumberToken(InfinityLiteral negative) -> if negative then "-Infinity" else "Infinity"
        | NullToken -> null
        | _ -> unexpected typeof<string>

    let boolean<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | BooleanToken value -> value
        | NumberToken NaNLiteral -> false
        | NumberToken(InfinityLiteral _) -> true
        | StringToken value -> value = "true" || (JsonImplNumeric.isIntegerString value && int64 value <> 0L)
        | NullToken -> false
        | _ -> unexpected typeof<bool>

    let character<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | StringToken value when value.Length = 1 && not(Char.IsDigit value[0]) -> value[0]
        | StringToken value -> char value
        | NumberToken literal -> Checked.char (JsonRawNumbers.unsigned &context literal)
        | _ -> unexpected typeof<char>

    let bytes<'R when 'R : struct and 'R :> IJsonReader> (context: byref<JsonReadContext<'R>>) =
        match JsonReadCursor.token &context with
        | StringToken value -> JsonHelper.JSONStringToByteArray value
        | NullToken -> null
        | _ -> unexpected typeof<byte[]>


module internal JsonOutputOperations =
    let text<'E,'O when 'O : struct and 'O :> IJsonTextOutput<'E>> (output: 'O) value start count (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) =
        output.Text(value,start,count,&destination,&state)
    let literal<'E,'O when 'O : struct and 'O :> IJsonTextOutput<'E>> (output: 'O) (value: string) (destination: byref<Span<'E>>) (state: byref<JsonWriteState<'E>>) =
        output.Text(value,0,value.Length,&destination,&state)

type internal IJsonValueOutput<'Element> =
    inherit IJsonTextOutput<'Element>
    abstract Signed: int64 * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit
    abstract Unsigned: uint64 * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit
    abstract Decimal: decimal * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit
    abstract Double: double * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit
    abstract Single: single * byref<Span<'Element>> * byref<JsonWriteState<'Element>> -> unit

[<Struct>]
type internal JsonUtf8Writer =
    interface IJsonValueOutput<byte> with
        member _.AsciiCode(value) = value
        member _.Ascii(value,destination,state) = JsonWriteBuffer.append &destination &state value
        member _.Text(value,start,count,destination,state) =
            let output = JsonUtf8Output()
            JsonOutputOperations.text output value start count &destination &state
        member _.Signed(value,destination,state) = JsonRawNumericWriter.int64Utf8(value) &destination &state
        member _.Unsigned(value,destination,state) = JsonRawNumericWriter.uint64Utf8(value) &destination &state
        member _.Decimal(value,destination,state) = JsonRawNumericWriter.decimalUtf8(value) &destination &state
        member _.Double(value,destination,state) = JsonRawNumericWriter.doubleUtf8(value) &destination &state
        member _.Single(value,destination,state) = JsonRawNumericWriter.singleUtf8(value) &destination &state

[<Struct>]
type internal JsonUtf16Writer =
    interface IJsonValueOutput<char> with
        member _.AsciiCode(value) = byte value
        member _.Ascii(value,destination,state) = JsonWriteBuffer.append &destination &state (char value)
        member _.Text(value,start,count,destination,state) =
            let output = JsonUtf16Output()
            JsonOutputOperations.text output value start count &destination &state
        member _.Signed(value,destination,state) = JsonRawNumericWriter.int64Utf16(value) &destination &state
        member _.Unsigned(value,destination,state) = JsonRawNumericWriter.uint64Utf16(value) &destination &state
        member _.Decimal(value,destination,state) = JsonRawNumericWriter.decimalUtf16(value) &destination &state
        member _.Double(value,destination,state) = JsonRawNumericWriter.doubleUtf16(value) &destination &state
        member _.Single(value,destination,state) = JsonRawNumericWriter.singleUtf16(value) &destination &state


[<AbstractClass; Sealed>]
type internal JsonRawPrimitive =
    static member internal ReadNumericText<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): string =
        match JsonReadCursor.token &context with
        | StringToken value -> value
        | NumberToken(DecimalLiteral(start,length)) -> context.Cursor.reader.AsciiString(context.Cursor.input,start,length)
        | NumberToken(HexLiteral(magnitude,negative,overflow)) ->
            if overflow then raise(OverflowException())
            (if negative then "-" else "") + magnitude.ToString(CultureInfo.InvariantCulture)
        | NumberToken(InfinityLiteral negative) -> if negative then "-Infinity" else "Infinity"
        | NumberToken NaNLiteral | NullToken -> "NaN"
        | _ -> raise(JsonPrimitiveShapeMismatch typeof<'T>)

    static member internal ReadSByte<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): sbyte =
        try
            Checked.sbyte (JsonRawScalar.signed &context)
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<sbyte>)
    static member internal WriteSByte<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: sbyte, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(int64 value,&destination,&state)
    static member internal ReadInt16<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int16 =
        try
            Checked.int16 (JsonRawScalar.signed &context)
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<int16>)
    static member internal WriteInt16<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: int16, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(int64 value,&destination,&state)
    static member internal ReadInt32<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int =
        try
            Checked.int (JsonRawScalar.signed &context)
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<int>)
    static member internal WriteInt32<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: int, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(int64 value,&destination,&state)
    static member internal ReadInt64<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int64 =
        try
            JsonRawScalar.signed &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<int64>)
    static member internal WriteInt64<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: int64, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(value,&destination,&state)
    static member internal ReadByte<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): byte =
        try
            Checked.byte (JsonRawScalar.unsigned &context)
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<byte>)
    static member internal WriteByte<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: byte, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Unsigned(uint64 value,&destination,&state)
    static member internal ReadUInt16<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): uint16 =
        try
            Checked.uint16 (JsonRawScalar.unsigned &context)
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<uint16>)
    static member internal WriteUInt16<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: uint16, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Unsigned(uint64 value,&destination,&state)
    static member internal ReadUInt32<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): uint32 =
        try
            Checked.uint32 (JsonRawScalar.unsigned &context)
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<uint32>)
    static member internal WriteUInt32<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: uint32, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Unsigned(uint64 value,&destination,&state)
    static member internal ReadUInt64<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): uint64 =
        try
            JsonRawScalar.unsigned &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<uint64>)
    static member internal WriteUInt64<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: uint64, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Unsigned(value,&destination,&state)
    static member internal ReadDouble<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): double =
        try
            JsonRawScalar.double &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<double>)
    static member internal WriteDouble<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: double, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Double(value,&destination,&state)
    static member internal ReadSingle<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): single =
        try
            JsonRawScalar.single &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<single>)
    static member internal WriteSingle<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: single, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Single(value,&destination,&state)
    static member internal ReadDecimal<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): decimal =
        try
            JsonRawScalar.decimal &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<decimal>)
    static member internal WriteDecimal<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: decimal, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Decimal(value,&destination,&state)
    static member internal ReadBoolean<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): bool =
        try
            JsonRawScalar.boolean &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<bool>)
    static member internal WriteBoolean<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: bool, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        JsonOutputOperations.literal output (if value then "true" else "false") &destination &state
    static member internal ReadChar<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): char =
        try
            JsonRawScalar.character &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<char>)
    static member internal WriteChar<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: char, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Unsigned(uint64 value,&destination,&state)
    static member internal ReadString<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): string =
        try
            JsonRawScalar.string &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<string>)
    static member internal WriteString<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: string, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        if isNull value then JsonOutputOperations.literal output "null" &destination &state
                    else JsonStringEscaping.write output value &destination &state
    static member internal ReadBytes<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): byte[] =
        try
            JsonRawScalar.bytes &context
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<byte[]>)
    static member internal ReadGuid<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): Guid =
        try
            match JsonReadCursor.token &context with
            | StringToken value -> Guid.Parse value
            | _ -> JsonRawScalar.unexpected typeof<Guid>
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<Guid>)
    static member internal WriteGuid<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: Guid, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        JsonStringEscaping.write output (value.ToString("D",CultureInfo.InvariantCulture)) &destination &state
    static member internal ReadIntPtr<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): IntPtr =
        try
            IntPtr(JsonRawScalar.signed &context)
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<IntPtr>)
    static member internal WriteIntPtr<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: IntPtr, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(value.ToInt64(),&destination,&state)
    static member internal ReadUIntPtr<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): UIntPtr =
        try
            UIntPtr(JsonRawScalar.unsigned &context)
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<UIntPtr>)
    static member internal WriteUIntPtr<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: UIntPtr, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Unsigned(value.ToUInt64(),&destination,&state)
    static member internal ReadType<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): Type =
        try
            let name = JsonRawScalar.string &context in if isNull name then null else TypeMetadata.nameToType name
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<Type>)
    static member internal ReadDateTime<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): DateTime =
        try
            match JsonReadCursor.token &context with
            | StringToken value -> DateTime.Parse(value,CultureInfo.InvariantCulture)
            | NumberToken literal -> DateTime.FromBinary(JsonRawNumbers.signed &context literal)
            | _ -> JsonRawScalar.unexpected typeof<DateTime>
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<DateTime>)
    static member internal WriteDateTime<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: DateTime, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(value.ToBinary(),&destination,&state)
    static member internal ReadDateTimeOffset<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): DateTimeOffset =
        try
            match JsonReadCursor.token &context with
            | StringToken value -> DateTimeOffset.Parse(value,CultureInfo.InvariantCulture)
            | NumberToken literal -> DateTimeOffset.FromUnixTimeMilliseconds(JsonRawNumbers.signed &context literal)
            | _ -> JsonRawScalar.unexpected typeof<DateTimeOffset>
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<DateTimeOffset>)
    static member internal WriteDateTimeOffset<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: DateTimeOffset, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(value.ToUnixTimeMilliseconds(),&destination,&state)
    static member internal ReadDateOnly<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): DateOnly =
        try
            match JsonReadCursor.token &context with
            | StringToken value -> DateOnly.Parse(value,CultureInfo.InvariantCulture)
            | NumberToken literal -> DateOnly.FromDayNumber(Checked.int(JsonRawNumbers.signed &context literal))
            | _ -> JsonRawScalar.unexpected typeof<DateOnly>
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<DateOnly>)
    static member internal WriteDateOnly<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: DateOnly, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(int64 value.DayNumber,&destination,&state)
    static member internal ReadTimeOnly<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): TimeOnly =
        try
            match JsonReadCursor.token &context with
            | StringToken value -> TimeOnly.Parse(value,CultureInfo.InvariantCulture)
            | NumberToken literal -> TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(JsonRawNumbers.double &context literal))
            | _ -> JsonRawScalar.unexpected typeof<TimeOnly>
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<TimeOnly>)
    static member internal WriteTimeOnly<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: TimeOnly, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(int64(value.ToTimeSpan().TotalMilliseconds),&destination,&state)
    static member internal ReadTimeSpan<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): TimeSpan =
        try
            match JsonReadCursor.token &context with
            | StringToken value -> TimeSpan.Parse(value,CultureInfo.InvariantCulture)
            | NumberToken literal -> TimeSpan.FromMilliseconds(JsonRawNumbers.double &context literal)
            | _ -> JsonRawScalar.unexpected typeof<TimeSpan>
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<TimeSpan>)
    static member internal WriteTimeSpan<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(value: TimeSpan, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        output.Signed(int64 value.TotalMilliseconds,&destination,&state)
    static member internal ReadSqlNumber<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): JsonSqlNumber =
        match JsonReadCursor.token &context with
        | NumberToken NaNLiteral | NumberToken(InfinityLiteral _) -> JsonSqlNumber(null,true)
        | _ -> raise(JsonPrimitiveShapeMismatch typeof<JsonSqlNumber>)

    static member internal ReadObjectNumber<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): obj =
        match JsonReadCursor.token &context with
        | NumberToken NaNLiteral -> box Double.NaN
        | NumberToken(InfinityLiteral negative) -> box(if negative then Double.NegativeInfinity else Double.PositiveInfinity)
        | _ -> raise(JsonPrimitiveShapeMismatch typeof<obj>)

    static member internal ReadBigInteger<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): BigInteger =
        try
            match JsonReadCursor.token &context with
            | StringToken value -> BigInteger.Parse(value,NumberStyles.Any,CultureInfo.InvariantCulture)
            | NumberToken NaNLiteral | NumberToken(InfinityLiteral _) -> raise(OverflowException())
            | _ -> JsonRawScalar.unexpected typeof<BigInteger>
        with JsonScalarShapeMismatch -> raise(JsonPrimitiveShapeMismatch typeof<BigInteger>)
