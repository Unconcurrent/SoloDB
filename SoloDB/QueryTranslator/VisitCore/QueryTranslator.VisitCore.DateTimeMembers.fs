namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers
open Utils
open DateTimeSql

/// Stored temporal members consume the shared scalar arithmetic.
module internal DateTimeMembers =

    // ─── Member access ────────────────────────────────────────────────────────────

    /// Translate a DateTime/DateTimeOffset member access to a SqlExpr.
    /// rawExpr is the raw stored field SqlExpr, clrType is DateTime or DateTimeOffset.
    let internal translateDateTimeMember (memberName: string) (rawExpr: SqlExpr) (clrType: Type) : SqlExpr =
        let ms  = toUnixMs rawExpr clrType
        let sec = unixSec ms
        let strftimeInt fmt = castInt (SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); sec; SqlExpr.Literal(SqlLiteral.String "unixepoch")]))
        match memberName with
        | "Year"        -> strftimeInt "%Y"
        | "Month"       -> strftimeInt "%m"
        | "Day"         -> strftimeInt "%d"
        | "Hour"        -> strftimeInt "%H"
        | "Minute"      -> strftimeInt "%M"
        | "Second"      -> strftimeInt "%S"
        | "Millisecond" -> castInt (SqlExpr.Binary(ms, BinaryOperator.Mod, ms1000))
        | "DayOfWeek"   -> strftimeInt "%w"
        | "DayOfYear"   -> strftimeInt "%j"
        | "Ticks" | "Kind" | "Date" | "TimeOfDay" when typeof<DateTime>.Equals clrType ->
            SqlExpr.FunctionCall("TEMPORAL_PART",[intLit 0L;SqlExpr.Literal(SqlLiteral.String memberName);rawExpr])
        | "UtcTicks" | "UtcDateTime" | "LocalDateTime" when typeof<DateTimeOffset>.Equals clrType ->
            SqlExpr.FunctionCall("TEMPORAL_PART",[intLit 1L;SqlExpr.Literal(SqlLiteral.String memberName);rawExpr])
        | "Microsecond" when typeof<DateTime>.Equals clrType ->
            div (modExpr (SqlExpr.Binary(rawExpr,BinaryOperator.BitwiseAnd,mask)) ticksMs) (intLit 10L)
        | "Nanosecond" when typeof<DateTime>.Equals clrType ->
            mul (modExpr (SqlExpr.Binary(rawExpr,BinaryOperator.BitwiseAnd,mask)) (intLit 10L)) (intLit 100L)
        | other ->
            raise (NotSupportedException(
                $"DateTime/DateTimeOffset member access '.{other}' is not supported in SQL translation. " +
                "Supported members: Year, Month, Day, Hour, Minute, Second, Millisecond, DayOfWeek, DayOfYear."))

    // ─── Shared arithmetic primitives (DateOnly / TimeOnly / TimeSpan translators) ──

    let private divInt (a: SqlExpr) (b: SqlExpr) = SqlExpr.Cast(SqlExpr.Binary(a, BinaryOperator.Div, b), "INTEGER")
    let private modInt (a: SqlExpr) (b: SqlExpr) = SqlExpr.Cast(SqlExpr.Binary(a, BinaryOperator.Mod, b), "INTEGER")
    let private castReal (e: SqlExpr) = SqlExpr.Cast(e, "REAL")
    let private divReal (a: SqlExpr) (b: SqlExpr) = SqlExpr.Binary(castReal a, BinaryOperator.Div, b)

    // ─── DateOnly / TimeOnly / TimeSpan member translators ───────────────────────

    let internal translateDateOnlyMember (memberName: string) (rawExpr: SqlExpr) : SqlExpr =
        // rawExpr is DayNumber (int). Add Julian Day epoch to get SQLite Julian Day at midnight.
        let julianDay = SqlExpr.Binary(rawExpr, BinaryOperator.Add, dayNumberEpoch)
        let strftimeInt fmt =
            SqlExpr.Cast(
                SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); julianDay; SqlExpr.Literal(SqlLiteral.String "julianday")]),
                "INTEGER")
        match memberName with
        | "Year"      -> strftimeInt "%Y"
        | "Month"     -> strftimeInt "%m"
        | "Day"       -> strftimeInt "%d"
        | "DayOfWeek" -> strftimeInt "%w"
        | "DayOfYear" -> strftimeInt "%j"
        | "DayNumber" -> rawExpr
        | _ ->
            raise (NotSupportedException(
                $"DateOnly member access '.{memberName}' is not supported in SQL translation. " +
                "Supported: Year, Month, Day, DayOfWeek, DayOfYear, DayNumber."))

    let internal translateTimeOnlyMember (memberName: string) (rawExpr: SqlExpr) : SqlExpr =
        // rawExpr is milliseconds since midnight (int64).
        match memberName with
        | "Hour"        -> divInt rawExpr ms3600000
        | "Minute"      -> modInt (divInt rawExpr ms60000) ms60
        | "Second"      -> modInt (divInt rawExpr ms1000) ms60
        | "Millisecond" -> modInt rawExpr ms1000
        | "Ticks" -> mul rawExpr ticksMs
        | "Microsecond" | "Nanosecond" -> zero
        | _ ->
            raise (NotSupportedException(
                $"TimeOnly member access '.{memberName}' is not supported in SQL translation. " +
                "Supported: Hour, Minute, Second, Millisecond."))

    let internal translateTimeSpanMember (memberName: string) (rawExpr: SqlExpr) : SqlExpr =
        // rawExpr is TotalMilliseconds as int64 (signed).
        match memberName with
        | "TotalMilliseconds" -> castReal rawExpr
        | "TotalSeconds"      -> divReal rawExpr ms1000R
        | "TotalMinutes"      -> divReal rawExpr ms60000R
        | "TotalHours"        -> divReal rawExpr ms3600000R
        | "TotalDays"         -> divReal rawExpr ms86400000R
        | "Days"              -> divInt rawExpr ms86400000
        | "Hours"             -> modInt (divInt rawExpr ms3600000) ms24
        | "Minutes"           -> modInt (divInt rawExpr ms60000) ms60
        | "Seconds"           -> modInt (divInt rawExpr ms1000) ms60
        | "Milliseconds"      -> modInt rawExpr ms1000
        | "Ticks"             -> mul rawExpr ticksMs
        | "Microseconds" | "Nanoseconds" -> zero
        | "TotalMicroseconds" -> mul (castReal rawExpr) (intLit 1000L)
        | "TotalNanoseconds" -> mul (castReal rawExpr) (intLit 1000000L)
        | _ ->
            raise (NotSupportedException(
                $"TimeSpan member access '.{memberName}' is not supported in SQL translation. " +
                "Supported: TotalMilliseconds, TotalSeconds, TotalMinutes, TotalHours, TotalDays, Days, Hours, Minutes, Seconds, Milliseconds."))

    /// Route group-key member access to the correct type-specific translator.
    /// For Nullable<T> keys, ".Value" is a pass-through (null rows pre-filtered by caller).
    let internal translateGroupKeyMemberAccess (keyExpr: SqlExpr) (keyType: Type) (memberName: string) : SqlExpr =
        let isNullable = keyType.IsGenericType && typedefof<Nullable<_>>.Equals(keyType.GetGenericTypeDefinition())
        if isNullable && memberName = "Value" then
            keyExpr
        else
            let underlying = unwrapNullable keyType
            if typeof<DateTime>.Equals(underlying) || typeof<DateTimeOffset>.Equals(underlying) then
                translateDateTimeMember memberName keyExpr underlying
            elif typeof<DateOnly>.Equals(underlying) then
                translateDateOnlyMember memberName keyExpr
            elif typeof<TimeOnly>.Equals(underlying) then
                translateTimeOnlyMember memberName keyExpr
            elif typeof<TimeSpan>.Equals(underlying) then
                translateTimeSpanMember memberName keyExpr
            else
                SqlExpr.FunctionCall("json_extract", [keyExpr; SqlExpr.Literal(SqlLiteral.String ("$." + memberName))])

