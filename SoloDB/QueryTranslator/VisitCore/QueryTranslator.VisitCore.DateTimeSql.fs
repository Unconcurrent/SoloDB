namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers
open Utils

/// Translation mode: from canonical persisted integer value.
type internal DateTimeTranslationMode =
    | FromEpoch of clrType: Type
    | FromDayNumber
    | FromMillisecondsSinceMidnight

module internal DateTimeSql =

    // ─── Constants ────────────────────────────────────────────────────────────────

    // ToBinary mask: strips Kind bits 63:62 to recover UTC-equivalent ticks
    let internal mask    = SqlExpr.Literal(SqlLiteral.Integer 4611686018427387903L)  // 0x3FFFFFFFFFFFFFFF
    let internal epochTk = SqlExpr.Literal(SqlLiteral.Integer 621355968000000000L)   // ticks 0001-01-01 → 1970-01-01 UTC
    let internal ticksMs = SqlExpr.Literal(SqlLiteral.Integer 10000L)               // 100-ns ticks per millisecond
    let internal ticksSecond = SqlExpr.Literal(SqlLiteral.Integer 10000000L)
    let internal ticksMinute = SqlExpr.Literal(SqlLiteral.Integer 600000000L)
    let internal ticksHour = SqlExpr.Literal(SqlLiteral.Integer 36000000000L)
    let internal ticksDay = SqlExpr.Literal(SqlLiteral.Integer 864000000000L)
    let internal ms24        = SqlExpr.Literal(SqlLiteral.Integer 24L)
    let internal ms60        = SqlExpr.Literal(SqlLiteral.Integer 60L)
    let internal ms1000      = SqlExpr.Literal(SqlLiteral.Integer 1000L)
    let internal ms60000     = SqlExpr.Literal(SqlLiteral.Integer 60000L)
    let internal ms3600000   = SqlExpr.Literal(SqlLiteral.Integer 3600000L)
    let internal ms86400000  = SqlExpr.Literal(SqlLiteral.Integer 86400000L)
    let internal ms1000R     = SqlExpr.Literal(SqlLiteral.Float 1000.0)
    let internal ms60000R    = SqlExpr.Literal(SqlLiteral.Float 60000.0)
    let internal ms3600000R  = SqlExpr.Literal(SqlLiteral.Float 3600000.0)
    let internal ms86400000R = SqlExpr.Literal(SqlLiteral.Float 86400000.0)
    let internal dayNumberEpoch = SqlExpr.Literal(SqlLiteral.Float 1721425.5)

    // ─── Preamble ─────────────────────────────────────────────────────────────────

    /// Convert raw stored SqlExpr to canonical unix-milliseconds.
    let internal toUnixMs (raw: SqlExpr) (clrType: Type) : SqlExpr =
        if typeof<DateTime>.Equals(clrType) then
            // ToBinary: ((raw & 0x3FFFFFFFFFFFFFFF) - 621355968000000000) / 10000
            SqlExpr.Binary(
                SqlExpr.Binary(
                    SqlExpr.Binary(raw, BinaryOperator.BitwiseAnd, mask),
                    BinaryOperator.Sub, epochTk),
                BinaryOperator.Div, ticksMs)
        elif typeof<DateTimeOffset>.Equals(clrType) then
            raw  // identity: ToUnixTimeMilliseconds() is the stored value
        else
            raise (NotSupportedException $"toUnixMs: unsupported type {clrType.FullName}")

    /// Unix seconds from unix milliseconds.
    let internal unixSec (ms: SqlExpr) : SqlExpr =
        SqlExpr.Binary(ms, BinaryOperator.Div, ms1000)

    /// Milliseconds part from unix milliseconds (epoch mode only).
    let internal millisPart (ms: SqlExpr) : SqlExpr =
        SqlExpr.Binary(ms, BinaryOperator.Mod, ms1000)

    let internal strftimeFromEpoch (fmt: string) (ms: SqlExpr) : SqlExpr =
        SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); unixSec ms; SqlExpr.Literal(SqlLiteral.String "unixepoch")])

    let internal strftime (fmt: string) (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        match mode with
        | FromEpoch clrType -> strftimeFromEpoch fmt (toUnixMs rawExpr clrType)
        | FromDayNumber ->
            let julianDay = SqlExpr.Binary(rawExpr, BinaryOperator.Add, dayNumberEpoch)
            SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); julianDay; SqlExpr.Literal(SqlLiteral.String "julianday")])
        | FromMillisecondsSinceMidnight ->
            SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); unixSec rawExpr; SqlExpr.Literal(SqlLiteral.String "unixepoch")])

    let internal castInt (expr: SqlExpr) : SqlExpr = SqlExpr.Cast(expr, "INTEGER")

    let internal intLit (n: int64) = SqlExpr.Literal(SqlLiteral.Integer n)
    let internal nullLit = SqlExpr.Literal(SqlLiteral.Null)
    let internal add a b = SqlExpr.Binary(a, BinaryOperator.Add, b)
    let internal sub a b = SqlExpr.Binary(a, BinaryOperator.Sub, b)
    let internal mul a b = SqlExpr.Binary(a, BinaryOperator.Mul, b)
    let internal div a b = SqlExpr.Binary(a, BinaryOperator.Div, b)
    let internal modExpr a b = SqlExpr.Binary(a, BinaryOperator.Mod, b)
    let internal eq a b = SqlExpr.Binary(a, BinaryOperator.Eq, b)
    let internal ne a b = SqlExpr.Binary(a, BinaryOperator.Ne, b)
    let internal lt a b = SqlExpr.Binary(a, BinaryOperator.Lt, b)
    let internal le a b = SqlExpr.Binary(a, BinaryOperator.Le, b)
    let internal ge a b = SqlExpr.Binary(a, BinaryOperator.Ge, b)
    let internal andAlso a b = SqlExpr.Binary(a, BinaryOperator.And, b)
    let internal orElse a b = SqlExpr.Binary(a, BinaryOperator.Or, b)
    let internal between e lo hi = andAlso (ge e lo) (le e hi)

    let internal all conditions =
        match conditions with
        | [] -> SqlExpr.Literal(SqlLiteral.Boolean true)
        | head :: tail -> tail |> List.fold andAlso head

    let internal any conditions =
        match conditions with
        | [] -> SqlExpr.Literal(SqlLiteral.Boolean false)
        | head :: tail -> tail |> List.fold orElse head

    let internal whenValid valid value =
        SqlExpr.CaseExpr((valid, value), [], Some nullLit)

    let internal unwrapNullable (t: Type) : Type =
        unwrapNullableType t

    let internal supportsTemporalToStringTranslationType (t: Type) =
        let u = unwrapNullable t
        typeof<DateTime>.Equals(u) || typeof<DateTimeOffset>.Equals(u) || typeof<DateOnly>.Equals(u) || typeof<TimeOnly>.Equals(u)

    let internal daysBeforeMonth (month: SqlExpr) =
        let branch m days = eq month (intLit m), intLit days
        SqlExpr.CaseExpr(
            branch 1L 0L,
            [ branch 2L 31L; branch 3L 59L; branch 4L 90L; branch 5L 120L; branch 6L 151L
              branch 7L 181L; branch 8L 212L; branch 9L 243L; branch 10L 273L; branch 11L 304L; branch 12L 334L ],
            Some nullLit)

    let internal isLeapYear (year: SqlExpr) =
        orElse
            (eq (modExpr year (intLit 400L)) (intLit 0L))
            (andAlso (eq (modExpr year (intLit 4L)) (intLit 0L)) (ne (modExpr year (intLit 100L)) (intLit 0L)))

    let internal daysInMonth (year: SqlExpr) (month: SqlExpr) =
        let branch m days = eq month (intLit m), intLit days
        SqlExpr.CaseExpr(
            branch 1L 31L,
            [ (eq month (intLit 2L), SqlExpr.CaseExpr((isLeapYear year, intLit 29L), [], Some(intLit 28L)))
              branch 3L 31L; branch 4L 30L; branch 5L 31L; branch 6L 30L
              branch 7L 31L; branch 8L 31L; branch 9L 30L; branch 10L 31L; branch 11L 30L; branch 12L 31L ],
            Some nullLit)

    let internal gregorianDayNumberUnchecked (year: SqlExpr) (month: SqlExpr) (day: SqlExpr) =
        let y0 = sub year (intLit 1L)
        let priorYears =
            add (add (sub (mul y0 (intLit 365L)) (div y0 (intLit 100L))) (div y0 (intLit 4L))) (div y0 (intLit 400L))
        let leapAdj =
            SqlExpr.CaseExpr((andAlso (ge month (intLit 3L)) (isLeapYear year), intLit 1L), [], Some(intLit 0L))
        add (add (add priorYears (daysBeforeMonth month)) leapAdj) (sub day (intLit 1L))

    let internal validDate year month day =
        all [ between year (intLit 1L) (intLit 9999L)
              between month (intLit 1L) (intLit 12L)
              between day (intLit 1L) (daysInMonth year month) ]

    let internal dateTicksUnchecked year month day =
        mul (gregorianDayNumberUnchecked year month day) ticksDay

    let internal timeTicks hour minute second millisecond =
        add (add (add (mul hour ticksHour) (mul minute ticksMinute)) (mul second ticksSecond)) (mul millisecond ticksMs)

    let internal validTimeOfDay hour minute second millisecond =
        all [ between hour (intLit 0L) (intLit 23L)
              between minute (intLit 0L) (intLit 59L)
              between second (intLit 0L) (intLit 59L)
              between millisecond (intLit 0L) (intLit 999L) ]

    let internal validOffsetMs offsetMs =
        between offsetMs (intLit -50400000L) (intLit 50400000L)

    let internal zero = intLit 0L

    let internal isDateTimeLikeType (t: Type) =
        Utils.isDateTimeLikeType t

    let internal canonicalizeForCompareOrOrder (clrType: Type) (expr: SqlExpr) =
        let u = unwrapNullable clrType
        if typeof<DateTime>.Equals(u) then
            SqlExpr.Binary(expr, BinaryOperator.BitwiseAnd, mask)
        elif isDateTimeLikeType u then
            expr
        else
            expr

    let internal ensureComparableDateTimeLikeTypes (leftType: Type) (rightType: Type) =
        let l = unwrapNullable leftType
        let r = unwrapNullable rightType
        if isDateTimeLikeType l || isDateTimeLikeType r then
            if not (l.Equals(r)) then
                raise (NotSupportedException($"Cannot compare date/time values of different CLR types: {l.FullName} and {r.FullName}."))

