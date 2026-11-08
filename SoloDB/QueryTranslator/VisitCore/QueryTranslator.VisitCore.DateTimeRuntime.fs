namespace SoloDatabase

open System
open System.Globalization
open SoloDatabase.RawSqliteFunctions
open SoloDatabase.Numerics

/// Portable scalar operations supplement the existing indexed temporal SQL.
module internal DateTimeRuntime =
    let private datePart name (value: DateTime) =
        match name with
        | "Ticks" -> value.Ticks
        | "Kind" -> int64 value.Kind
        | "Date" -> value.Date.ToBinary()
        | "TimeOfDay" -> int64 value.TimeOfDay.TotalMilliseconds
        | "Microsecond" -> value.Ticks % 10000L / 10L
        | "Nanosecond" -> value.Ticks % 10L * 100L
        | _ -> invalidArg "name" "The DateTime scalar member is unknown."

    let private offsetPart name (value: DateTimeOffset) =
        match name with
        | "Value" -> value.ToUnixTimeMilliseconds()
        | "Year" -> int64 value.Year
        | "Month" -> int64 value.Month
        | "Day" -> int64 value.Day
        | "Hour" -> int64 value.Hour
        | "Minute" -> int64 value.Minute
        | "Second" -> int64 value.Second
        | "Millisecond" -> int64 value.Millisecond
        | "DayOfWeek" -> int64 value.DayOfWeek
        | "DayOfYear" -> int64 value.DayOfYear
        | "Ticks" -> value.Ticks
        | "UtcTicks" -> value.UtcDateTime.Ticks
        | "Offset" -> int64 value.Offset.TotalMilliseconds
        | "Date" -> value.Date.ToBinary()
        | "DateTime" -> value.DateTime.ToBinary()
        | "UtcDateTime" -> value.UtcDateTime.ToBinary()
        | "LocalDateTime" -> value.LocalDateTime.ToBinary()
        | "TimeOfDay" -> int64 value.TimeOfDay.TotalMilliseconds
        | "Microsecond" -> value.Ticks % 10000L / 10L
        | "Nanosecond" -> value.Ticks % 10L * 100L
        | _ -> invalidArg "name" "The DateTimeOffset scalar member is unknown."

    let private fromClock mode raw offset =
        if mode = 0 then DateTimeOffset(DateTime.FromBinary raw)
        elif mode = 1 then DateTimeOffset(DateTime.FromBinary raw,TimeSpan.FromTicks offset)
        else DateTimeOffset(raw,TimeSpan.FromTicks offset)

    let private format kind raw (format: string) offset =
        let invariant = CultureInfo.InvariantCulture
        match kind with
        | 0 ->
            let ticks = raw &&& 0x3FFFFFFFFFFFFFFFL
            let value = DateTime(ticks / 10000L * 10000L,DateTimeKind.Utc)
            value.ToString(DateTimeFormatting.runtimeFormat (FromEpoch typeof<DateTime>) format false,invariant)
        | 1 ->
            let value = DateTimeOffset.FromUnixTimeMilliseconds raw
            let value = match offset with ValueSome ms -> value.ToOffset(TimeSpan.FromMilliseconds ms) | ValueNone -> value
            value.ToString(DateTimeFormatting.runtimeFormat (FromEpoch typeof<DateTimeOffset>) format offset.IsSome,invariant)
        | 2 -> DateOnly.FromDayNumber(int raw).ToString(format,invariant)
        | 3 -> TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(float raw)).ToString(format,invariant)
        | 4 -> TimeSpan.FromMilliseconds(float raw).ToString(format,invariant)
        | _ -> invalidArg "kind" "The temporal scalar kind is unknown."

    let internal register (connection: NativeSqliteConnection) =
        connection.RegisterManagedScalar("TEMPORAL_PART",3,RawScalarFunc(fun result args ->
            if args[2].IsNull then result.SetNull() else
            let value =
                if args[0].GetInt32() = 0 then datePart (args[1].CopyToString()) (DateTime.FromBinary(args[2].GetInt64()))
                else offsetPart (args[1].CopyToString()) (DateTimeOffset.FromUnixTimeMilliseconds(args[2].GetInt64()))
            result.SetInt64 value),SqliteFunctionFlags.None)
        connection.RegisterManagedScalar("TEMPORAL_FORMAT",4,RawScalarFunc(fun result args ->
            if args[1].IsNull then result.SetNull() else
            let offset = if args[3].IsNull then ValueNone else ValueSome(args[3].GetDouble())
            result.SetText(format (args[0].GetInt32()) (args[1].GetInt64()) (args[2].CopyToString()) offset)),SqliteFunctionFlags.None)
        connection.RegisterManagedScalar("TEMPORAL_PARSE",2,RawScalarFunc(fun result args ->
            let text = if args[1].IsNull then null else args[1].CopyToString()
            match args[0].GetInt32() with
            | 0 -> result.SetInt64(DateTime.Parse(text,CultureInfo.CurrentCulture).ToBinary())
            | 1 -> result.SetInt64(DateTimeOffset.Parse(text,CultureInfo.CurrentCulture).ToUnixTimeMilliseconds())
            | 2 -> result.SetInt64(int64(DateOnly.Parse(text,CultureInfo.CurrentCulture).DayNumber))
            | 3 -> result.SetInt64(int64(TimeOnly.Parse(text,CultureInfo.CurrentCulture).ToTimeSpan().TotalMilliseconds))
            | 4 -> result.SetInt64(int64(TimeSpan.Parse(text,CultureInfo.CurrentCulture).TotalMilliseconds))
            | 5 -> result.SetText(NumericKey.encode(ExactNumber.ofDecimal(Decimal.Parse(text,CultureInfo.CurrentCulture))))
            | _ -> invalidArg "kind" "The parsed scalar kind is unknown."),SqliteFunctionFlags.None)
        connection.RegisterManagedScalar("DTO_CLOCK_PART",4,RawScalarFunc(fun result args ->
            if args[1].IsNull || args[2].IsNull then result.SetNull() else
            result.SetInt64(offsetPart (args[3].CopyToString()) (fromClock (args[0].GetInt32()) (args[1].GetInt64()) (args[2].GetInt64())))),SqliteFunctionFlags.None)
        connection.RegisterManagedScalar("DTO_CLOCK_FORMAT",4,RawScalarFunc(fun result args ->
            if args[1].IsNull || args[2].IsNull then result.SetNull() else
            let value = fromClock (args[0].GetInt32()) (args[1].GetInt64()) (args[2].GetInt64())
            result.SetText(value.ToString(args[3].CopyToString(),CultureInfo.InvariantCulture))),SqliteFunctionFlags.None)
        connection.RegisterManagedScalar("DTO_PARSE_PART",2,RawScalarFunc(fun result args ->
            let text = if args[1].IsNull then null else args[1].CopyToString()
            result.SetInt64(offsetPart (args[0].CopyToString()) (DateTimeOffset.Parse(text,CultureInfo.CurrentCulture)))),SqliteFunctionFlags.None)
        connection.RegisterManagedScalar("DTO_PARSE_FORMAT",2,RawScalarFunc(fun result args ->
            let text = if args[0].IsNull then null else args[0].CopyToString()
            let value = DateTimeOffset.Parse(text,CultureInfo.CurrentCulture)
            result.SetText(value.ToString(args[1].CopyToString(),CultureInfo.InvariantCulture))),SqliteFunctionFlags.None)
