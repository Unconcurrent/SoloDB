namespace SoloDatabase

open System
open System.Globalization
open System.Numerics
open System.Text
open System.Runtime.CompilerServices
open SoloDatabase.RawSqliteFunctions

module internal NativeScalarFunctions =
    // Use the shared text owner and preserve explicit lengths and embedded NUL.
    let private copyText (value: SqliteRawValue) = value.CopyToString()
    let private bigInteger (value: SqliteRawValue) =
        NativeValueConversion.bigInteger value.SqliteType value.GetInt64 value.GetDouble value.CopyToString

    let private readDynamic<'T> (name: string) (ordinal: int) (value: SqliteRawValue) : 'T =
        if value.IsNull then
            if typeof<'T>.IsValueType && isNull (TypeMetadata.nullableUnderlying(typeof<'T>)) then
                raise (InvalidOperationException($"The SQL function '{name}' was called with a NULL argument at ordinal {ordinal}. Create the function using a Nullable parameter or rewrite your query to avoid passing NULL."))
            Unchecked.defaultof<'T>
        else
        let requested = typeof<'T>
        let nullable = TypeMetadata.nullableUnderlying(requested)
        let target = if isNull nullable then requested else nullable
        let result: obj =
            if typeof<string>.Equals(target) then box (copyText value)
            elif typeof<byte[]>.Equals(target) then box (value.CopyBlobToArray())
            elif typeof<obj>.Equals(target) then
                match value.SqliteType with
                | 1 -> box (value.GetInt64())
                | 2 -> box (value.GetDouble())
                | 4 -> box (value.CopyBlobToArray())
                | _ -> box (copyText value)
            elif typeof<int64>.Equals(target) then box (value.GetInt64())
            elif typeof<double>.Equals(target) then box (value.GetDouble())
            elif typeof<single>.Equals(target) then box (single (value.GetDouble()))
            elif typeof<bool>.Equals(target) then box (value.GetInt64() <> 0L)
            elif typeof<char>.Equals(target) then
                let text = copyText value
                if value.IsText && text.Length = 1 then box text.[0] else box (Checked.char (value.GetInt64()))
            elif typeof<Guid>.Equals(target) then
                if value.IsBlob then
                    let bytes = value.CopyBlobToArray()
                    box (if bytes.Length = 16 then Guid(bytes) else Guid(Encoding.UTF8.GetString bytes))
                else box (Guid(copyText value))
            elif typeof<DateTime>.Equals(target) then
                box (if value.IsInteger || value.IsFloat then NativeValueConversion.fromJulianDate (value.GetDouble()) else DateTime.Parse(copyText value, NativeValueConversion.invariant))
            elif typeof<DateTimeOffset>.Equals(target) then
                box (if value.IsInteger || value.IsFloat then DateTimeOffset(NativeValueConversion.fromJulianDate (value.GetDouble())) else DateTimeOffset.Parse(copyText value, NativeValueConversion.invariant))
            elif typeof<DateOnly>.Equals(target) then
                box (if value.IsInteger || value.IsFloat then DateOnly.FromDateTime(NativeValueConversion.fromJulianDate (value.GetDouble())) else DateOnly.Parse(copyText value, NativeValueConversion.invariant))
            elif typeof<TimeOnly>.Equals(target) then box (TimeOnly.Parse(copyText value, NativeValueConversion.invariant))
            elif typeof<TimeSpan>.Equals(target) then box (if value.IsInteger || value.IsFloat then TimeSpan.FromDays(value.GetDouble()) else TimeSpan.Parse(copyText value))
            elif typeof<uint64>.Equals(target) then box (uint64 (value.GetInt64()))
            elif typeof<decimal>.Equals(target) then box (Decimal.Parse(copyText value, NumberStyles.Number ||| NumberStyles.AllowExponent, CultureInfo.InvariantCulture))
            elif typeof<BigInteger>.Equals(target) then box (bigInteger value)
            elif requested.IsEnum then TypeMetadata.enumValue requested (value.GetInt64())
            else Convert.ChangeType(value.GetInt64(), target, CultureInfo.InvariantCulture)
        unbox<'T> result

    let private writeDynamic (context: inref<SqliteRawContext>) (result: obj) =
        match result with
        | null | :? DBNull -> context.SetNull()
        | :? string as text -> context.SetText text
        | :? (byte array) as bytes -> context.SetBlob bytes
        | :? int64 as value -> context.SetInt64 value
        | :? double as value -> if Double.IsNaN(value) then raise (InvalidOperationException("Cannot store 'NaN' values.")) else context.SetDouble value
        | :? single as value -> if Single.IsNaN(value) then raise (InvalidOperationException("Cannot store 'NaN' values.")) else context.SetDouble(double value)
        | :? bool as value -> context.SetInt32(if value then 1 else 0)
        | :? decimal as value -> context.SetText(value.ToString("0.0###########################", CultureInfo.InvariantCulture))
        | :? BigInteger as value -> context.SetText(NativeValueConversion.bigIntegerText value)
        | :? Guid as value -> context.SetText(value.ToString().ToUpperInvariant())
        | :? DateTime as value -> context.SetText(NativeValueConversion.dateTimeText value)
        | :? DateTimeOffset as value -> context.SetText(NativeValueConversion.offsetText value)
        | :? DateOnly as value -> context.SetText(NativeValueConversion.dateText value)
        | :? TimeOnly as value -> context.SetText(NativeValueConversion.timeText value)
        | :? uint64 as value -> context.SetInt64(int64 value)
        | :? TimeSpan as value -> context.SetText(value.ToString("c", CultureInfo.InvariantCulture))
        | :? char as value -> context.SetText(string value)
        | value when value.GetType().IsEnum && typeof<uint64>.Equals(TypeMetadata.enumUnderlying(value.GetType())) ->
            context.SetInt64(int64 (Convert.ToUInt64(value, CultureInfo.InvariantCulture)))
        | value -> context.SetInt64(Convert.ToInt64(value, CultureInfo.InvariantCulture))

    // Type equality is specialized by the JIT. Reinterpret only equal CLR types,
    // keeping primitive values in their typed representation throughout the callback.
    let inline private cast<'A, 'B> (value: 'A) : 'B =
        let mutable value = value
        Unsafe.As<'A, 'B>(&value)

    let read<'T> (name: string) (ordinal: int) (value: SqliteRawValue) : 'T =
        if value.IsNull then
            if typeof<'T>.IsValueType && isNull (TypeMetadata.nullableUnderlying(typeof<'T>)) then
                raise (InvalidOperationException($"The SQL function '{name}' was called with a NULL argument at ordinal {ordinal}. Create the function using a Nullable parameter or rewrite your query to avoid passing NULL."))
            Unchecked.defaultof<'T>
        elif typeof<'T>.Equals(typeof<int64>) then cast<int64, 'T> (value.GetInt64())
        elif typeof<'T>.Equals(typeof<int>) then cast<int, 'T> (Checked.int (value.GetInt64()))
        elif typeof<'T>.Equals(typeof<int16>) then cast<int16, 'T> (Checked.int16 (value.GetInt64()))
        elif typeof<'T>.Equals(typeof<sbyte>) then cast<sbyte, 'T> (Checked.sbyte (value.GetInt64()))
        elif typeof<'T>.Equals(typeof<byte>) then cast<byte, 'T> (Checked.byte (value.GetInt64()))
        elif typeof<'T>.Equals(typeof<uint16>) then cast<uint16, 'T> (Checked.uint16 (value.GetInt64()))
        elif typeof<'T>.Equals(typeof<uint32>) then cast<uint32, 'T> (Checked.uint32 (value.GetInt64()))
        elif typeof<'T>.Equals(typeof<uint64>) then cast<uint64, 'T> (uint64 (value.GetInt64()))
        elif typeof<'T>.Equals(typeof<double>) then cast<double, 'T> (value.GetDouble())
        elif typeof<'T>.Equals(typeof<BigInteger>) then cast<BigInteger,'T> (bigInteger value)
        elif typeof<'T>.Equals(typeof<single>) then cast<single, 'T> (single (value.GetDouble()))
        elif typeof<'T>.Equals(typeof<bool>) then cast<bool, 'T> (value.GetInt64() <> 0L)
        elif typeof<'T>.Equals(typeof<string>) then cast<string, 'T> (copyText value)
        elif typeof<'T>.Equals(typeof<byte[]>) then cast<byte[], 'T> (value.CopyBlobToArray())
        else readDynamic<'T> name ordinal value

    let write<'T> (context: inref<SqliteRawContext>) (result: 'T) =
        if typeof<'T>.Equals(typeof<int64>) then context.SetInt64(cast<'T, int64> result)
        elif typeof<'T>.Equals(typeof<int>) then context.SetInt32(cast<'T, int> result)
        elif typeof<'T>.Equals(typeof<int16>) then context.SetInt32(int (cast<'T, int16> result))
        elif typeof<'T>.Equals(typeof<sbyte>) then context.SetInt32(int (cast<'T, sbyte> result))
        elif typeof<'T>.Equals(typeof<byte>) then context.SetInt32(int (cast<'T, byte> result))
        elif typeof<'T>.Equals(typeof<uint16>) then context.SetInt32(int (cast<'T, uint16> result))
        elif typeof<'T>.Equals(typeof<uint32>) then context.SetInt64(int64 (cast<'T, uint32> result))
        elif typeof<'T>.Equals(typeof<uint64>) then context.SetInt64(int64 (cast<'T, uint64> result))
        elif typeof<'T>.Equals(typeof<bool>) then context.SetInt32(if cast<'T, bool> result then 1 else 0)
        elif typeof<'T>.Equals(typeof<double>) then
            let value = cast<'T, double> result
            if Double.IsNaN value then raise (InvalidOperationException("Cannot store 'NaN' values."))
            context.SetDouble value
        elif typeof<'T>.Equals(typeof<single>) then
            let value = cast<'T, single> result
            if Single.IsNaN value then raise (InvalidOperationException("Cannot store 'NaN' values."))
            context.SetDouble(double value)
        elif typeof<'T>.Equals(typeof<string>) then
            let text = cast<'T, string> result
            if isNull text then context.SetNull() else context.SetText text
        elif typeof<'T>.Equals(typeof<BigInteger>) then
            context.SetText(NativeValueConversion.bigIntegerText (cast<'T,BigInteger> result))
        elif typeof<'T>.Equals(typeof<byte[]>) then
            let bytes = cast<'T, byte[]> result
            if isNull bytes then context.SetNull() else context.SetBlob bytes
        else writeDynamic &context (box result)
