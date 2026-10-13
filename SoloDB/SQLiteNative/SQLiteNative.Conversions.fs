namespace SoloDatabase

open System
open System.Globalization
open System.Numerics

/// Raw SQL values retain their native representation, independent of document codecs.
module internal NativeValueConversion =
    let invariant = CultureInfo.InvariantCulture
    let bigIntegerText (value: BigInteger) = value.ToString(invariant)
    let inline bigInteger storageClass (readInteger: unit -> int64) (readReal: unit -> double) (readText: unit -> string) =
        match storageClass with
        | 1 -> BigInteger(readInteger())
        | 2 -> BigInteger(readReal())
        | _ -> BigInteger.Parse(readText(),NumberStyles.Number ||| NumberStyles.AllowExponent,invariant)
    let julianDate (value: DateTime) =
        let midnight = value.Date.Ticks / TimeSpan.TicksPerMillisecond + 148731163200000L
        let seconds = int64 ((float value.Second + float value.Millisecond / 1000.0) * 1000.0)
        float (midnight + int64 value.Hour * 3600000L + int64 value.Minute * 60000L + seconds) / 86400000.0
    let fromJulianDate (value: double) =
        let milliseconds = int64 (Math.Floor(value * 86400000.0 + 0.5)) - 148731163200000L
        DateTime(Checked.(*) milliseconds TimeSpan.TicksPerMillisecond, DateTimeKind.Unspecified)
    let dateTimeText (value: DateTime) = value.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF", invariant)
    let offsetText (value: DateTimeOffset) = value.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", invariant)
    let dateText (value: DateOnly) = value.ToString("yyyy-MM-dd", invariant)
    let timeText (value: TimeOnly) = value.ToString((if value.Ticks % TimeSpan.TicksPerSecond = 0L then "HH:mm:ss" else "HH:mm:ss.fffffff"), invariant)
