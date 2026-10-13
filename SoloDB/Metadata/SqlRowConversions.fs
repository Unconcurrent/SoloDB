namespace SoloDatabase

open System
open System.Reflection
open System.Numerics
open System.Collections
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Runtime.CompilerServices
open SoloDatabase.Metadata.Generation
open SoloDatabase.JsonSerializator

module internal SqlReadArguments =
    let operation (target: Type) =
        let decision = TypeSupport.Runtime target
        let children = if target.IsGenericType then TypeMetadata.GenericTypeArgCache.Get target else [||]
        let child = if children.Length = 0 then TypeShape.Object else (TypeSupport.Runtime children[0]).Shape
        TypeSupport.SqlReader decision.Shape child target.IsArray

    let resolve (target: Type) arguments =
        let element () = (TypeMetadata.GenericTypeArgCache.Get target)[0]
        arguments |> Array.map (function
            | SqlTypeArgument.Target -> target
            | SqlTypeArgument.Element -> element()
            | SqlTypeArgument.EnumUnderlying -> TypeMetadata.enumUnderlying target
            | SqlTypeArgument.ElementEnumUnderlying -> TypeMetadata.enumUnderlying (element())
            | other -> invalidArg "arguments" (string other))

/// Typed scalar conversions shared by direct callers and dynamic model mapping.
[<AbstractClass; Sealed>]
type SqlRowValue private () =
    static member internal Document(row: SqlRow) : Types.DbObjectRow =
        { Id = if row.IsDBNull(0) then Nullable() else Nullable(row.GetInt64(0))
          ValueJSON = if row.IsDBNull(1) then null else row.GetString(1)
          HydrationJSON = if row.FieldCount > 2 && not (row.IsDBNull(2)) then row.GetString(2) else null }

    static member private Cast<'A, 'B>(value: 'A) : 'B =
        let mutable value = value
        Unsafe.As<'A, 'B>(&value)

    /// Preserves the existing raw-value cast for a compiler-established nonscalar leaf.
    static member internal ReadObject<'T>(row: SqlRow, ordinal: int) : 'T =
        unbox<'T>(row.GetValue ordinal)

    static member Read<'T>(row: SqlRow, ordinal: int) : 'T =
        let t = typeof<'T>
        if typeof<int64>.Equals(t) then SqlRowValue.Cast<int64, 'T>(row.GetInt64 ordinal)
        elif typeof<int>.Equals(t) then SqlRowValue.Cast<int, 'T>(row.GetInt32 ordinal)
        elif typeof<int16>.Equals(t) then SqlRowValue.Cast<int16, 'T>(row.GetInt16 ordinal)
        elif typeof<byte>.Equals(t) then SqlRowValue.Cast<byte, 'T>(row.GetByte ordinal)
        elif typeof<sbyte>.Equals(t) then SqlRowValue.Cast<sbyte, 'T>(Checked.sbyte (row.GetInt16 ordinal))
        elif typeof<uint16>.Equals(t) then SqlRowValue.Cast<uint16, 'T>(uint16 (row.GetInt32 ordinal))
        elif typeof<uint32>.Equals(t) then SqlRowValue.Cast<uint32, 'T>(uint32 (row.GetInt64 ordinal))
        elif typeof<uint64>.Equals(t) then SqlRowValue.Cast<uint64, 'T>(uint64 (row.GetInt64 ordinal))
        elif typeof<double>.Equals(t) then SqlRowValue.Cast<double, 'T>(row.GetDouble ordinal)
        elif typeof<single>.Equals(t) then SqlRowValue.Cast<single, 'T>(row.GetFloat ordinal)
        elif typeof<bool>.Equals(t) then SqlRowValue.Cast<bool, 'T>(row.GetBoolean ordinal)
        elif typeof<char>.Equals(t) then SqlRowValue.Cast<char, 'T>(row.GetChar ordinal)
        elif typeof<string>.Equals(t) then SqlRowValue.Cast<string, 'T>(row.GetString ordinal)
        elif typeof<decimal>.Equals(t) then SqlRowValue.Cast<decimal, 'T>(row.GetDecimal ordinal)
        elif typeof<BigInteger>.Equals(t) then SqlRowValue.Cast<BigInteger,'T>(row.GetBigInteger ordinal)
        elif typeof<Guid>.Equals(t) then SqlRowValue.Cast<Guid, 'T>(row.GetGuid ordinal)
        elif typeof<DateTime>.Equals(t) then
            let value = if row.StorageClass ordinal = SQLitePCL.raw.SQLITE_TEXT then row.GetDateTime ordinal else DateTime.FromBinary(row.GetInt64 ordinal)
            SqlRowValue.Cast<DateTime, 'T>(value)
        elif typeof<DateTimeOffset>.Equals(t) then
            let value = if row.StorageClass ordinal = SQLitePCL.raw.SQLITE_TEXT then DateTimeOffset.Parse(row.GetString ordinal, Globalization.CultureInfo.InvariantCulture) else DateTimeOffset.FromUnixTimeMilliseconds(row.GetInt64 ordinal)
            SqlRowValue.Cast<DateTimeOffset, 'T>(value)
        elif typeof<DateOnly>.Equals(t) then
            let value = if row.StorageClass ordinal = SQLitePCL.raw.SQLITE_TEXT then DateOnly.Parse(row.GetString ordinal, Globalization.CultureInfo.InvariantCulture) else DateOnly.FromDayNumber(int (row.GetInt64 ordinal))
            SqlRowValue.Cast<DateOnly, 'T>(value)
        elif typeof<TimeOnly>.Equals(t) then
            let value = if row.StorageClass ordinal = SQLitePCL.raw.SQLITE_TEXT then TimeOnly.Parse(row.GetString ordinal, Globalization.CultureInfo.InvariantCulture) else TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(double (row.GetInt64 ordinal)))
            SqlRowValue.Cast<TimeOnly, 'T>(value)
        elif typeof<TimeSpan>.Equals(t) then
            let value = if row.StorageClass ordinal = SQLitePCL.raw.SQLITE_TEXT then TimeSpan.Parse(row.GetString ordinal, Globalization.CultureInfo.InvariantCulture) else TimeSpan.FromMilliseconds(double (row.GetInt64 ordinal))
            SqlRowValue.Cast<TimeSpan, 'T>(value)
        elif t.IsEnum then
            let read =
                match MetadataValue.tryValue (TypeMetadataCache.Get(t).EnumReader) with
                | ValueSome read -> read
                | ValueNone ->
                    TypeMetadataCache.Discover(t, (fun metadata -> metadata.EnumReader),
                        (fun operation metadata -> { metadata with EnumReader = ValueSome operation }), Func<Delegate>(fun () ->
                            if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "SQL enum conversion" t
                            let methodInfo = typeof<SqlRowValue>.GetMethod("ReadEnum",BindingFlags.NonPublic ||| BindingFlags.Static)
                            methodInfo.MakeGenericMethod(t,TypeMetadata.enumUnderlying(t)).CreateDelegate(typeof<Func<SqlRow,int,'T>>)))
            (read :?> Func<SqlRow,int,'T>).Invoke(row,ordinal)
        else unbox<'T> (row.GetValue ordinal)

    /// Converts a compiler-established enum width through the canonical scalar reader.
    static member internal ReadEnum<'Enum, 'Underlying>(row: SqlRow, ordinal: int) : 'Enum =
        if typeof<'Underlying>.Equals(typeof<int64>) && row.StorageClass ordinal <> SQLitePCL.raw.SQLITE_INTEGER then
            raise (InvalidCastException())
        SqlRowValue.Cast<'Underlying, 'Enum>(SqlRowValue.Read<'Underlying>(row, ordinal))

    static member internal RegisterRawScalar<'T>() =
        TypeMetadataCache.Register(typeof<'T>,fun metadata -> { metadata with RawNullable = MetadataValue.generated (null: Delegate) })

    static member internal RegisterRawEnum<'Enum,'Underlying>() =
        TypeMetadata.registerEnumUnderlying typeof<'Enum> typeof<'Underlying>
        let read = Func<SqlRow,int,'Enum>(fun row ordinal -> SqlRowValue.ReadEnum<'Enum,'Underlying>(row,ordinal))
        TypeMetadataCache.Register(typeof<'Enum>,fun metadata -> { metadata with EnumReader = MetadataValue.generated (read :> Delegate) })
        SqlRowValue.RegisterRawScalar<'Enum>()

    /// Retains nullable enum conversion without rediscovering its underlying width.
    static member internal ReadNullableEnum<'Enum,'Underlying when 'Enum : struct and 'Enum : (new: unit -> 'Enum) and 'Enum :> ValueType>(row: SqlRow, ordinal: int) : Nullable<'Enum> =
        if row.IsDBNull ordinal then Nullable()
        else Nullable(SqlRowValue.ReadEnum<'Enum,'Underlying>(row, ordinal))

    static member internal ReadRaw<'T>(row: SqlRow, ordinal: int) : 'T =
        if row.IsDBNull ordinal then raise (InvalidOperationException($"SQL NULL at ordinal {ordinal}."))
        let t = typeof<'T>
        if typeof<DateTime>.Equals(t) then SqlRowValue.Cast<DateTime, 'T>(row.GetDateTime ordinal)
        elif typeof<DateTimeOffset>.Equals(t) then SqlRowValue.Cast<DateTimeOffset, 'T>(row.GetDateTimeOffset ordinal)
        elif typeof<DateOnly>.Equals(t) then SqlRowValue.Cast<DateOnly, 'T>(row.GetDateOnly ordinal)
        elif typeof<TimeOnly>.Equals(t) then SqlRowValue.Cast<TimeOnly, 'T>(row.GetTimeOnly ordinal)
        elif typeof<TimeSpan>.Equals(t) then SqlRowValue.Cast<TimeSpan, 'T>(row.GetTimeSpan ordinal)
        else
            let nullable: Func<SqlRow, int, 'T> = SqlRawNullableReader<'T>.Reader
            if isNull nullable then SqlRowValue.Read<'T>(row, ordinal)
            else nullable.Invoke(row, ordinal)

    static member internal ReadRawNullable<'T when 'T : struct and 'T : (new: unit -> 'T) and 'T :> ValueType>(row: SqlRow, ordinal: int) =
        Nullable(SqlRowValue.ReadRaw<'T>(row, ordinal))

    static member internal RegisterRawNullable(owner: Type, read: Delegate) =
        TypeMetadataCache.Register(owner,fun metadata -> { metadata with RawNullable = MetadataValue.generated read })

    static member ReadNullable<'T when 'T : struct and 'T : (new: unit -> 'T) and 'T :> ValueType>(row: SqlRow, ordinal: int) : Nullable<'T> =
        let kind = row.StorageClass ordinal
        if kind = SQLitePCL.raw.SQLITE_NULL then Nullable()
        else
            if (typeof<'T>.Equals(typeof<int64>) && kind <> SQLitePCL.raw.SQLITE_INTEGER)
               || (typeof<'T>.Equals(typeof<double>) && kind <> SQLitePCL.raw.SQLITE_FLOAT) then
                raise (InvalidCastException())
            Nullable(SqlRowValue.Read<'T>(row, ordinal))

    static member private StoredScalar(row: SqlRow, ordinal: int) =
        let mutable destination = Span<char>.Empty
        let mutable state = JsonWriteState<char>(null,null,null)
        match row.StorageClass ordinal with
        | SQLitePCL.raw.SQLITE_INTEGER -> JsonRawNumericWriter.int64Utf16 (row.GetInt64 ordinal) &destination &state
        | SQLitePCL.raw.SQLITE_FLOAT -> JsonRawNumericWriter.doubleUtf16 (row.GetDouble ordinal) &destination &state
        | SQLitePCL.raw.SQLITE_TEXT -> JsonStringEscaping.write (JsonUtf16Output()) (row.GetString ordinal) &destination &state
        | _ -> raise(InvalidCastException("The SQL value is not a JSON scalar."))
        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        System.String(destination.Slice(0,state.Written))
        #else
        System.String(destination.Slice(0,state.Written).ToArray())
        #endif

    static member internal ReadOption<'T>(row: SqlRow, ordinal: int) : option<'T> =
        if row.IsDBNull ordinal then None
        else
            if typeof<'T>.Equals(typeof<byte[]>) && row.StorageClass ordinal = SQLitePCL.raw.SQLITE_BLOB then
                Some(SqlRowValue.ReadObject<'T>(row,ordinal))
            else
                let scalarPayload =
                    TypeSupport.IsScalar((Utils.optionalPayloadType typeof<'T>).FullName) ||
                    JsonValue.HasScalarCarrier typeof<'T>
                let text = if scalarPayload then SqlRowValue.StoredScalar(row,ordinal) else row.GetString ordinal
                Some(JsonValue.ParseInto<'T>(text))

    static member internal ReadRawOption<'T>(row: SqlRow, ordinal: int) : option<'T> =
        SqlRowValue.ReadOption<'T>(row,ordinal)

    static member internal RegisterRawOption(owner: Type, read: Delegate) =
        TypeMetadataCache.Register(owner,fun metadata -> { metadata with RawNullable = MetadataValue.generated read })

/// Closed generic construction is resolved once; rows keep the canonical raw conversion policy.
and [<AbstractClass; Sealed>] internal SqlRawNullableReader<'T> private () =
    static member Reader: Func<SqlRow,int,'T> =
        let reader =
            match MetadataValue.tryValue (TypeMetadataCache.Get(typeof<'T>).RawNullable) with
            | ValueSome reader -> reader
            | ValueNone ->
                TypeMetadataCache.Discover(typeof<'T>, (fun metadata -> metadata.RawNullable),
                    (fun operation metadata -> { metadata with RawNullable = ValueSome operation }), Func<Delegate>(fun () ->
                        let target = typeof<'T>
                        let decision = TypeSupport.Runtime target
                        let arguments = if target.IsGenericType then TypeMetadata.GenericTypeArgCache.Get target else [||]
                        let childShape = if arguments.Length = 0 then TypeShape.Object else (TypeSupport.Runtime arguments[0]).Shape
                        let raw = TypeSupport.SqlRawRegistration decision.Shape childShape
                        match raw.Read with
                        | ValueNone -> null
                        | ValueSome read ->
                            if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "raw optional SQL conversion" target
                            let methodInfo = typeof<SqlRowValue>.GetMethod(read.Method,BindingFlags.Static ||| BindingFlags.NonPublic)
                            methodInfo.MakeGenericMethod(SqlReadArguments.resolve target read.Arguments).CreateDelegate(typeof<Func<SqlRow,int,'T>>)))
        reader :?> Func<SqlRow,int,'T>

/// Conversion expressions call the same typed reader used by direct callers.
module internal SQLiteToolsRowConversions =
    let internal tryBindScalar<'T> ordinal =
        let t = typeof<'T>
        let readOperation = SqlReadArguments.operation t
        if readOperation.NeedsClosedBinding then
            if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "optional SQL row mapping" t
            let arguments = SqlReadArguments.resolve t readOperation.Arguments
            let read = typeof<SqlRowValue>.GetMethod(readOperation.Method,BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic)
                           .MakeGenericMethod(arguments).CreateDelegate(typeof<Func<SqlRow,int,'T>>) :?> Func<SqlRow,int,'T>
            ValueSome(Func<SqlRow,'T>(fun row -> read.Invoke(row,ordinal)))
        elif t.IsEnum || TypeSupport.SqlScalar t.FullName then
            ValueSome(Func<SqlRow,'T>(fun row -> SqlRowValue.Read<'T>(row,ordinal)))
        else ValueNone
