namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseHelpers
open Utils

/// Translation mode: from canonical persisted integer value.
type internal DateTimeTranslationMode =
    | FromEpoch of clrType: Type
    | FromDayNumber
    | FromMillisecondsSinceMidnight

module internal DateTimeFunctions =

    // ─── Constants ────────────────────────────────────────────────────────────────

    // ToBinary mask: strips Kind bits 63:62 to recover UTC-equivalent ticks
    let private mask    = SqlExpr.Literal(SqlLiteral.Integer 4611686018427387903L)  // 0x3FFFFFFFFFFFFFFF
    let private epochTk = SqlExpr.Literal(SqlLiteral.Integer 621355968000000000L)   // ticks 0001-01-01 → 1970-01-01 UTC
    let private ticksMs = SqlExpr.Literal(SqlLiteral.Integer 10000L)               // 100-ns ticks per millisecond
    let private ticksSecond = SqlExpr.Literal(SqlLiteral.Integer 10000000L)
    let private ticksMinute = SqlExpr.Literal(SqlLiteral.Integer 600000000L)
    let private ticksHour = SqlExpr.Literal(SqlLiteral.Integer 36000000000L)
    let private ticksDay = SqlExpr.Literal(SqlLiteral.Integer 864000000000L)
    let private ms24        = SqlExpr.Literal(SqlLiteral.Integer 24L)
    let private ms60        = SqlExpr.Literal(SqlLiteral.Integer 60L)
    let private ms1000      = SqlExpr.Literal(SqlLiteral.Integer 1000L)
    let private ms60000     = SqlExpr.Literal(SqlLiteral.Integer 60000L)
    let private ms3600000   = SqlExpr.Literal(SqlLiteral.Integer 3600000L)
    let private ms86400000  = SqlExpr.Literal(SqlLiteral.Integer 86400000L)
    let private ms1000R     = SqlExpr.Literal(SqlLiteral.Float 1000.0)
    let private ms60000R    = SqlExpr.Literal(SqlLiteral.Float 60000.0)
    let private ms3600000R  = SqlExpr.Literal(SqlLiteral.Float 3600000.0)
    let private ms86400000R = SqlExpr.Literal(SqlLiteral.Float 86400000.0)
    let private dayNumberEpoch = SqlExpr.Literal(SqlLiteral.Float 1721425.5)

    // ─── Preamble ─────────────────────────────────────────────────────────────────

    /// Convert raw stored SqlExpr to canonical unix-milliseconds.
    let internal toUnixMs (raw: SqlExpr) (clrType: Type) : SqlExpr =
        if clrType = typeof<DateTime> then
            // ToBinary: ((raw & 0x3FFFFFFFFFFFFFFF) - 621355968000000000) / 10000
            SqlExpr.Binary(
                SqlExpr.Binary(
                    SqlExpr.Binary(raw, BinaryOperator.BitwiseAnd, mask),
                    BinaryOperator.Sub, epochTk),
                BinaryOperator.Div, ticksMs)
        elif clrType = typeof<DateTimeOffset> then
            raw  // identity: ToUnixTimeMilliseconds() is the stored value
        else
            raise (NotSupportedException $"toUnixMs: unsupported type {clrType.FullName}")

    /// Unix seconds from unix milliseconds.
    let private unixSec (ms: SqlExpr) : SqlExpr =
        SqlExpr.Binary(ms, BinaryOperator.Div, SqlExpr.Literal(SqlLiteral.Integer 1000L))

    /// Milliseconds part from unix milliseconds (epoch mode only).
    let private millisPart (ms: SqlExpr) : SqlExpr =
        SqlExpr.Binary(ms, BinaryOperator.Mod, SqlExpr.Literal(SqlLiteral.Integer 1000L))

    let private strftimeFromEpoch (fmt: string) (ms: SqlExpr) : SqlExpr =
        SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); unixSec ms; SqlExpr.Literal(SqlLiteral.String "unixepoch")])

    let private strftime (fmt: string) (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        match mode with
        | FromEpoch clrType -> strftimeFromEpoch fmt (toUnixMs rawExpr clrType)
        | FromDayNumber ->
            let julianDay = SqlExpr.Binary(rawExpr, BinaryOperator.Add, dayNumberEpoch)
            SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); julianDay; SqlExpr.Literal(SqlLiteral.String "julianday")])
        | FromMillisecondsSinceMidnight ->
            SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); unixSec rawExpr; SqlExpr.Literal(SqlLiteral.String "unixepoch")])

    let private castInt (expr: SqlExpr) : SqlExpr = SqlExpr.Cast(expr, "INTEGER")

    let private intLit (n: int64) = SqlExpr.Literal(SqlLiteral.Integer n)
    let private nullLit = SqlExpr.Literal(SqlLiteral.Null)
    let private add a b = SqlExpr.Binary(a, BinaryOperator.Add, b)
    let private sub a b = SqlExpr.Binary(a, BinaryOperator.Sub, b)
    let private mul a b = SqlExpr.Binary(a, BinaryOperator.Mul, b)
    let private div a b = SqlExpr.Binary(a, BinaryOperator.Div, b)
    let private modExpr a b = SqlExpr.Binary(a, BinaryOperator.Mod, b)
    let private eq a b = SqlExpr.Binary(a, BinaryOperator.Eq, b)
    let private ne a b = SqlExpr.Binary(a, BinaryOperator.Ne, b)
    let private lt a b = SqlExpr.Binary(a, BinaryOperator.Lt, b)
    let private le a b = SqlExpr.Binary(a, BinaryOperator.Le, b)
    let private ge a b = SqlExpr.Binary(a, BinaryOperator.Ge, b)
    let private andAlso a b = SqlExpr.Binary(a, BinaryOperator.And, b)
    let private orElse a b = SqlExpr.Binary(a, BinaryOperator.Or, b)
    let private between e lo hi = andAlso (ge e lo) (le e hi)

    let private all conditions =
        match conditions with
        | [] -> SqlExpr.Literal(SqlLiteral.Boolean true)
        | head :: tail -> tail |> List.fold andAlso head

    let private any conditions =
        match conditions with
        | [] -> SqlExpr.Literal(SqlLiteral.Boolean false)
        | head :: tail -> tail |> List.fold orElse head

    let private whenValid valid value =
        SqlExpr.CaseExpr((valid, value), [], Some nullLit)

    let internal unwrapNullable (t: Type) : Type =
        unwrapNullableType t

    let internal supportsTemporalToStringTranslationType (t: Type) =
        let u = unwrapNullable t
        u = typeof<DateTime> || u = typeof<DateTimeOffset> || u = typeof<DateOnly> || u = typeof<TimeOnly>

    let private daysBeforeMonth (month: SqlExpr) =
        let branch m days = eq month (intLit m), intLit days
        SqlExpr.CaseExpr(
            branch 1L 0L,
            [ branch 2L 31L; branch 3L 59L; branch 4L 90L; branch 5L 120L; branch 6L 151L
              branch 7L 181L; branch 8L 212L; branch 9L 243L; branch 10L 273L; branch 11L 304L; branch 12L 334L ],
            Some nullLit)

    let private isLeapYear (year: SqlExpr) =
        orElse
            (eq (modExpr year (intLit 400L)) (intLit 0L))
            (andAlso (eq (modExpr year (intLit 4L)) (intLit 0L)) (ne (modExpr year (intLit 100L)) (intLit 0L)))

    let private daysInMonth (year: SqlExpr) (month: SqlExpr) =
        let branch m days = eq month (intLit m), intLit days
        SqlExpr.CaseExpr(
            branch 1L 31L,
            [ (eq month (intLit 2L), SqlExpr.CaseExpr((isLeapYear year, intLit 29L), [], Some(intLit 28L)))
              branch 3L 31L; branch 4L 30L; branch 5L 31L; branch 6L 30L
              branch 7L 31L; branch 8L 31L; branch 9L 30L; branch 10L 31L; branch 11L 30L; branch 12L 31L ],
            Some nullLit)

    let private gregorianDayNumberUnchecked (year: SqlExpr) (month: SqlExpr) (day: SqlExpr) =
        let y0 = sub year (intLit 1L)
        let priorYears =
            add (add (sub (mul y0 (intLit 365L)) (div y0 (intLit 100L))) (div y0 (intLit 4L))) (div y0 (intLit 400L))
        let leapAdj =
            SqlExpr.CaseExpr((andAlso (ge month (intLit 3L)) (isLeapYear year), intLit 1L), [], Some(intLit 0L))
        add (add (add priorYears (daysBeforeMonth month)) leapAdj) (sub day (intLit 1L))

    let private validDate year month day =
        all [ between year (intLit 1L) (intLit 9999L)
              between month (intLit 1L) (intLit 12L)
              between day (intLit 1L) (daysInMonth year month) ]

    let private dateTicksUnchecked year month day =
        mul (gregorianDayNumberUnchecked year month day) ticksDay

    let private timeTicks hour minute second millisecond =
        add (add (add (mul hour ticksHour) (mul minute ticksMinute)) (mul second ticksSecond)) (mul millisecond ticksMs)

    let private validTimeOfDay hour minute second millisecond =
        all [ between hour (intLit 0L) (intLit 23L)
              between minute (intLit 0L) (intLit 59L)
              between second (intLit 0L) (intLit 59L)
              between millisecond (intLit 0L) (intLit 999L) ]

    let private validOffsetMs offsetMs =
        between offsetMs (intLit -50400000L) (intLit 50400000L)

    let private zero = intLit 0L

    let internal isDateTimeLikeType (t: Type) =
        Utils.isDateTimeLikeType t

    let internal canonicalizeForCompareOrOrder (clrType: Type) (expr: SqlExpr) =
        let u = unwrapNullable clrType
        if u = typeof<DateTime> then
            SqlExpr.Binary(expr, BinaryOperator.BitwiseAnd, mask)
        elif isDateTimeLikeType u then
            expr
        else
            expr

    let internal ensureComparableDateTimeLikeTypes (leftType: Type) (rightType: Type) =
        let l = unwrapNullable leftType
        let r = unwrapNullable rightType
        if isDateTimeLikeType l || isDateTimeLikeType r then
            if l <> r then
                raise (NotSupportedException($"Cannot compare date/time values of different CLR types: {l.FullName} and {r.FullName}."))

    let private tryEval<'T> (expr: Expression) =
        if isFullyConstant expr then Some(evaluateExpr<'T> expr) else None

    let private unsupported (typeName: string) (argCount: int) =
        raise (NotSupportedException($"new {typeName} with signature of {argCount} argument(s) is not supported in SQL translation."))

    let private constKind (expr: Expression) =
        match tryEval<DateTimeKind> expr with
        | Some k -> k
        | None -> raise (NotSupportedException("DateTime constructor DateTimeKind argument must be a compile-time constant."))

    let private kindFlag (kind: DateTimeKind) =
        match kind with
        | DateTimeKind.Unspecified -> 0L
        | DateTimeKind.Utc -> 4611686018427387904L
        | DateTimeKind.Local -> Int64.MinValue
        | _ -> 0L

    let private dtoDateTimeMessage =
        "DateTimeOffset(DateTime[, offset]) supported only for UTC DateTime with zero offset, or constant Unspecified DateTime with TimeSpan.Zero."

    let private requireZeroOffset (offsetExpr: Expression option) =
        match offsetExpr with
        | None -> ()
        | Some e ->
            match tryEval<TimeSpan> e with
            | Some ts when ts = TimeSpan.Zero -> ()
            | _ -> raise (NotSupportedException(dtoDateTimeMessage))

    let private translateDateTimeConstructorToTicks (ne: NewExpression) (visitArg: Expression -> SqlExpr) =
        let arg i = visitArg ne.Arguments.[i]
        let ymdTime y m d h mi s ms kind =
            let ticks = add (dateTicksUnchecked y m d) (timeTicks h mi s ms)
            let withKind = add ticks (intLit (kindFlag kind))
            whenValid (andAlso (validDate y m d) (validTimeOfDay h mi s ms)) withKind

        match ne.Arguments.Count with
        | 1 ->
            arg 0
        | 2 when ne.Arguments.[0].Type = typeof<DateOnly> && ne.Arguments.[1].Type = typeof<TimeOnly> ->
            add (mul (arg 0) ticksDay) (mul (arg 1) ticksMs)
        | 2 when ne.Arguments.[0].Type = typeof<int64> || ne.Arguments.[0].Type = typeof<int64> ->
            add (arg 0) (intLit (kindFlag (constKind ne.Arguments.[1])))
        | 3 when ne.Arguments.[0].Type = typeof<DateOnly> && ne.Arguments.[1].Type = typeof<TimeOnly> && ne.Arguments.[2].Type = typeof<DateTimeKind> ->
            add (add (mul (arg 0) ticksDay) (mul (arg 1) ticksMs)) (intLit (kindFlag (constKind ne.Arguments.[2])))
        | 3 ->
            // new DateTime(y,m,d) has Kind=Unspecified, so the high Kind bits are zero and the mask is identity.
            ymdTime (arg 0) (arg 1) (arg 2) zero zero zero zero DateTimeKind.Unspecified
        | 6 ->
            ymdTime (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) zero DateTimeKind.Unspecified
        | 7 when ne.Arguments.[6].Type = typeof<DateTimeKind> ->
            ymdTime (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) zero (constKind ne.Arguments.[6])
        | 7 ->
            ymdTime (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) (arg 6) DateTimeKind.Unspecified
        | 8 when ne.Arguments.[7].Type = typeof<DateTimeKind> ->
            ymdTime (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) (arg 6) (constKind ne.Arguments.[7])
        | n -> unsupported "DateTime" n

    let private offsetMsFromTimeSpanExpr (expr: Expression) (visitArg: Expression -> SqlExpr) =
        visitArg expr

    let private unixMsFromDateTimeTicks (ticks: SqlExpr) =
        div (sub (SqlExpr.Binary(ticks, BinaryOperator.BitwiseAnd, mask)) epochTk) ticksMs

    let private translateDateTimeOffsetConstructor (ne: NewExpression) (visitArg: Expression -> SqlExpr) =
        let arg i = visitArg ne.Arguments.[i]
        let dtoFromParts y m d h mi s ms offsetExpr =
            let dtTicks = add (dateTicksUnchecked y m d) (timeTicks h mi s ms)
            let offsetMs = offsetMsFromTimeSpanExpr offsetExpr visitArg
            whenValid (all [ validDate y m d; validTimeOfDay h mi s ms; validOffsetMs offsetMs ]) (sub (unixMsFromDateTimeTicks dtTicks) offsetMs)
        let dtoFromDateTimeArg (dtArg: Expression) (offsetArg: Expression option) =
            match dtArg with
            | :? NewExpression as dtNe when dtNe.Type = typeof<DateTime> ->
                let explicitKind =
                    if dtNe.Arguments.Count >= 2 && dtNe.Arguments.[dtNe.Arguments.Count - 1].Type = typeof<DateTimeKind> then
                        Some (constKind dtNe.Arguments.[dtNe.Arguments.Count - 1])
                    else None
                match explicitKind with
                | Some DateTimeKind.Utc ->
                    requireZeroOffset offsetArg
                    unixMsFromDateTimeTicks (translateDateTimeConstructorToTicks dtNe visitArg)
                | _ ->
                    raise (NotSupportedException(dtoDateTimeMessage))
            | _ ->
                match tryEval<DateTime> dtArg with
                | Some dt when dt.Kind = DateTimeKind.Utc ->
                    requireZeroOffset offsetArg
                    intLit (DateTimeOffset(dt).ToUnixTimeMilliseconds())
                | Some dt when dt.Kind = DateTimeKind.Unspecified ->
                    requireZeroOffset offsetArg
                    intLit (DateTimeOffset(dt, TimeSpan.Zero).ToUnixTimeMilliseconds())
                | _ ->
                    raise (NotSupportedException(dtoDateTimeMessage))

        match ne.Arguments.Count with
        | 1 when ne.Arguments.[0].Type = typeof<DateTime> -> dtoFromDateTimeArg ne.Arguments.[0] None
        | 2 when ne.Arguments.[0].Type = typeof<DateTime> -> dtoFromDateTimeArg ne.Arguments.[0] (Some ne.Arguments.[1])
        | 2 when ne.Arguments.[0].Type = typeof<int64> ->
            let offsetMs = offsetMsFromTimeSpanExpr ne.Arguments.[1] visitArg
            whenValid (validOffsetMs offsetMs) (sub (unixMsFromDateTimeTicks (arg 0)) offsetMs)
        | 7 -> dtoFromParts (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) zero ne.Arguments.[6]
        | 8 -> dtoFromParts (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) (arg 6) ne.Arguments.[7]
        | n -> unsupported "DateTimeOffset" n

    let internal tryTranslateDateTimeOffsetConstructorMember (memberName: string) (ne: NewExpression) (visitArg: Expression -> SqlExpr) : SqlExpr option =
        let arg i = visitArg ne.Arguments.[i]
        let translateOffsetMemberFromUnixMs unixMs =
            let sec = unixSec unixMs
            let strftimeInt fmt = castInt (SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); sec; SqlExpr.Literal(SqlLiteral.String "unixepoch")]))
            match memberName with
            | "Year" -> strftimeInt "%Y"
            | "Month" -> strftimeInt "%m"
            | "Day" -> strftimeInt "%d"
            | "Hour" -> strftimeInt "%H"
            | "Minute" -> strftimeInt "%M"
            | "Second" -> strftimeInt "%S"
            | "Millisecond" -> castInt (SqlExpr.Binary(unixMs, BinaryOperator.Mod, SqlExpr.Literal(SqlLiteral.Integer 1000L)))
            | "DayOfWeek" -> strftimeInt "%w"
            | "DayOfYear" -> strftimeInt "%j"
            | _ -> raise (NotSupportedException($"DateTimeOffset member access '.{memberName}' is not supported in SQL translation."))
        let fromLocalTicks localTicks = translateOffsetMemberFromUnixMs (unixMsFromDateTimeTicks localTicks)
        let fromDateParts y m d h mi s ms offsetExpr =
            let localTicks = add (dateTicksUnchecked y m d) (timeTicks h mi s ms)
            let offsetMs = offsetMsFromTimeSpanExpr offsetExpr visitArg
            whenValid (all [ validDate y m d; validTimeOfDay h mi s ms; validOffsetMs offsetMs ]) (fromLocalTicks localTicks)
        let fromDateTimeArg (dtArg: Expression) (offsetArg: Expression option) =
            match dtArg with
            | :? NewExpression as dtNe when dtNe.Type = typeof<DateTime> ->
                let explicitKind =
                    if dtNe.Arguments.Count >= 2 && dtNe.Arguments.[dtNe.Arguments.Count - 1].Type = typeof<DateTimeKind> then
                        Some (constKind dtNe.Arguments.[dtNe.Arguments.Count - 1])
                    else None
                match explicitKind with
                | Some DateTimeKind.Utc ->
                    requireZeroOffset offsetArg
                    fromLocalTicks (translateDateTimeConstructorToTicks dtNe visitArg)
                | _ -> raise (NotSupportedException(dtoDateTimeMessage))
            | _ ->
                match tryEval<DateTime> dtArg with
                | Some dt when dt.Kind = DateTimeKind.Utc ->
                    requireZeroOffset offsetArg
                    translateOffsetMemberFromUnixMs (intLit (DateTimeOffset(dt).ToUnixTimeMilliseconds()))
                | Some dt when dt.Kind = DateTimeKind.Unspecified ->
                    requireZeroOffset offsetArg
                    translateOffsetMemberFromUnixMs (intLit (DateTimeOffset(dt, TimeSpan.Zero).ToUnixTimeMilliseconds()))
                | _ -> raise (NotSupportedException(dtoDateTimeMessage))

        match ne.Arguments.Count with
        | 1 when ne.Arguments.[0].Type = typeof<DateTime> -> Some (fromDateTimeArg ne.Arguments.[0] None)
        | 2 when ne.Arguments.[0].Type = typeof<DateTime> -> Some (fromDateTimeArg ne.Arguments.[0] (Some ne.Arguments.[1]))
        | 2 when ne.Arguments.[0].Type = typeof<int64> ->
            let offsetMs = offsetMsFromTimeSpanExpr ne.Arguments.[1] visitArg
            Some (whenValid (validOffsetMs offsetMs) (fromLocalTicks (arg 0)))
        | 7 -> Some (fromDateParts (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) zero ne.Arguments.[6])
        | 8 ->
            Some (fromDateParts (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) (arg 6) ne.Arguments.[7])
        | _ -> None

    let private translateDateOnlyConstructor (ne: NewExpression) (visitArg: Expression -> SqlExpr) =
        match ne.Arguments.Count with
        | 3 ->
            let y = visitArg ne.Arguments.[0]
            let m = visitArg ne.Arguments.[1]
            let d = visitArg ne.Arguments.[2]
            whenValid (validDate y m d) (gregorianDayNumberUnchecked y m d)
        | n -> unsupported "DateOnly" n

    let private translateTimeOnlyConstructor (ne: NewExpression) (visitArg: Expression -> SqlExpr) =
        let arg i = visitArg ne.Arguments.[i]
        let hms h m s ms =
            whenValid (validTimeOfDay h m s ms) (add (add (add (mul h ms3600000) (mul m ms60000)) (mul s ms1000)) ms)
        match ne.Arguments.Count with
        | 1 when ne.Arguments.[0].Type = typeof<int64> -> div (arg 0) ticksMs
        | 2 -> hms (arg 0) (arg 1) zero zero
        | 3 -> hms (arg 0) (arg 1) (arg 2) zero
        | 4 -> hms (arg 0) (arg 1) (arg 2) (arg 3)
        | n -> unsupported "TimeOnly" n

    let private translateTimeSpanConstructor (ne: NewExpression) (visitArg: Expression -> SqlExpr) =
        let arg i = visitArg ne.Arguments.[i]
        // TimeSpan has no row-dependent invalid component domain under supported overloads; storage is total milliseconds.
        match ne.Arguments.Count with
        | 1 when ne.Arguments.[0].Type = typeof<int64> -> div (arg 0) ticksMs
        | 3 -> add (add (mul (arg 0) ms3600000) (mul (arg 1) ms60000)) (mul (arg 2) ms1000)
        | 4 -> add (mul (arg 0) ms86400000) (add (add (mul (arg 1) ms3600000) (mul (arg 2) ms60000)) (mul (arg 3) ms1000))
        | 5 -> add (add (mul (arg 0) ms86400000) (add (add (mul (arg 1) ms3600000) (mul (arg 2) ms60000)) (mul (arg 3) ms1000))) (arg 4)
        | n -> unsupported "TimeSpan" n

    let internal translateDateTimeLikeConstructor (ne: NewExpression) (visitArg: Expression -> SqlExpr) : SqlExpr option =
        if ne.Type = typeof<DateTime> then Some (translateDateTimeConstructorToTicks ne visitArg)
        elif ne.Type = typeof<DateTimeOffset> then Some (translateDateTimeOffsetConstructor ne visitArg)
        elif ne.Type = typeof<DateOnly> then Some (translateDateOnlyConstructor ne visitArg)
        elif ne.Type = typeof<TimeOnly> then Some (translateTimeOnlyConstructor ne visitArg)
        elif ne.Type = typeof<TimeSpan> then Some (translateTimeSpanConstructor ne visitArg)
        else None

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
        | "Millisecond" -> castInt (SqlExpr.Binary(ms, BinaryOperator.Mod, SqlExpr.Literal(SqlLiteral.Integer 1000L)))
        | "DayOfWeek"   -> strftimeInt "%w"
        | "DayOfYear"   -> strftimeInt "%j"
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
        | _ ->
            raise (NotSupportedException(
                $"TimeSpan member access '.{memberName}' is not supported in SQL translation. " +
                "Supported: TotalMilliseconds, TotalSeconds, TotalMinutes, TotalHours, TotalDays, Days, Hours, Minutes, Seconds, Milliseconds."))

    /// Route group-key member access to the correct type-specific translator.
    /// For Nullable<T> keys, ".Value" is a pass-through (null rows pre-filtered by caller).
    let internal translateGroupKeyMemberAccess (keyExpr: SqlExpr) (keyType: Type) (memberName: string) : SqlExpr =
        let isNullable = keyType.IsGenericType && keyType.GetGenericTypeDefinition() = typedefof<Nullable<_>>
        if isNullable && memberName = "Value" then
            keyExpr
        else
            let underlying = unwrapNullable keyType
            if underlying = typeof<DateTime> || underlying = typeof<DateTimeOffset> then
                translateDateTimeMember memberName keyExpr underlying
            elif underlying = typeof<DateOnly> then
                translateDateOnlyMember memberName keyExpr
            elif underlying = typeof<TimeOnly> then
                translateTimeOnlyMember memberName keyExpr
            elif underlying = typeof<TimeSpan> then
                translateTimeSpanMember memberName keyExpr
            else
                SqlExpr.FunctionCall("json_extract", [keyExpr; SqlExpr.Literal(SqlLiteral.String ("$." + memberName))])

    // ─── Format tokenizer ─────────────────────────────────────────────────────────

    // Standard specifier char set: single-char formats dispatched as standard
    // 'm' = "MMMM dd" (month-day), 'y' = "yyyy MMMM" (year-month) — both are standard on .NET
    let private standardSpecifierSet =
        System.Collections.Generic.HashSet<char>(['d';'D';'f';'F';'g';'G';'m';'M';'y';'Y';'t';'T';'s';'u';'o';'O';'U';'R';'r'])

    // Custom specifier chars: the ONLY chars that produce Specifier tokens
    let private specifierChars =
        System.Collections.Generic.HashSet<char>(['y';'M';'d';'H';'h';'m';'s';'f';'F';'t';'z';'K';'g'])

    let private isSpecifierChar (c: char) = specifierChars.Contains(c)

    type private FormatToken =
        | Specifier of char * int    // char * run-count
        | Literal   of string

    let private tokenize (fmt: string) : FormatToken list =
        let tokens = ResizeArray<FormatToken>()
        let mutable pos = 0
        while pos < fmt.Length do
            let ch = fmt.[pos]
            if ch = '\\' && pos + 1 < fmt.Length then
                // 1. Backslash escape: next char is literal
                tokens.Add(Literal(string fmt.[pos + 1]))
                pos <- pos + 2
            elif ch = '\'' then
                // 2. Quoted literal block
                let close =
                    let idx = fmt.IndexOf('\'', pos + 1)
                    if idx < 0 then fmt.Length else idx
                tokens.Add(Literal(fmt.Substring(pos + 1, close - (pos + 1))))
                pos <- close + 1
            elif ch = '/' || ch = ':' then
                // 3. Culture separators → literal
                tokens.Add(Literal(string ch))
                pos <- pos + 1
            elif ch = '%' && pos + 1 < fmt.Length && isSpecifierChar fmt.[pos + 1] then
                // 4. % prefix: forces next char as single-count specifier
                tokens.Add(Specifier(fmt.[pos + 1], 1))
                pos <- pos + 2
            elif isSpecifierChar ch then
                // 5. Specifier run: count consecutive identical chars
                let mutable count = 1
                while pos + count < fmt.Length && fmt.[pos + count] = ch do
                    count <- count + 1
                tokens.Add(Specifier(ch, count))
                pos <- pos + count
            else
                // 6. All other chars (T, Z, G, %, etc.) → literal
                tokens.Add(Literal(string ch))
                pos <- pos + 1
        tokens |> Seq.toList

    // ─── Standard specifier expansion ────────────────────────────────────────────

    let private expandStandardSpecifier (spec: char) : string =
        match spec with
        | 'd' -> "MM/dd/yyyy"
        | 'D' -> "dddd, dd MMMM yyyy"
        | 'f' -> "dddd, dd MMMM yyyy HH:mm"
        | 'F' -> "dddd, dd MMMM yyyy HH:mm:ss"
        | 'g' -> "MM/dd/yyyy HH:mm"
        | 'G' -> "MM/dd/yyyy HH:mm:ss"
        | 'M' | 'm' -> "MMMM dd"
        | 'Y' | 'y' -> "yyyy MMMM"
        | 't' -> "HH:mm"
        | 'T' -> "HH:mm:ss"
        | 's' -> "yyyy-MM-ddTHH:mm:ss"
        | 'u' -> "yyyy-MM-dd HH:mm:ssZ"
        | 'o' | 'O' -> "yyyy-MM-ddTHH:mm:ss.fffffffK"
        | 'R' | 'r' -> "ddd, dd MMM yyyy HH:mm:ss 'GMT'"
        | 'U' -> "dddd, dd MMMM yyyy HH:mm:ss"
        | _ -> raise (ArgumentException $"Not a standard specifier: {spec}")

    // ─── CASE expression builders ─────────────────────────────────────────────────

    let private strfWeekday (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        strftime "%w" rawExpr mode

    let private strfMonth (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        strftime "%m" rawExpr mode

    let private buildMonthCase (full: bool) (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        let monthExpr = strfMonth rawExpr mode
        let pair (mm: string) (name: string) =
            SqlExpr.Binary(monthExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String mm)),
            SqlExpr.Literal(SqlLiteral.String name)
        if full then
            let first = pair "01" "January"
            let rest  = [pair "02" "February"; pair "03" "March"; pair "04" "April"; pair "05" "May"; pair "06" "June";
                         pair "07" "July";     pair "08" "August"; pair "09" "September";
                         pair "10" "October";  pair "11" "November"; pair "12" "December"]
            SqlExpr.CaseExpr(first, rest, None)
        else
            let first = pair "01" "Jan"
            let rest  = [pair "02" "Feb"; pair "03" "Mar"; pair "04" "Apr"; pair "05" "May"; pair "06" "Jun";
                         pair "07" "Jul"; pair "08" "Aug"; pair "09" "Sep";
                         pair "10" "Oct"; pair "11" "Nov"; pair "12" "Dec"]
            SqlExpr.CaseExpr(first, rest, None)

    let private buildDayOfWeekCase (full: bool) (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        let wExpr = strfWeekday rawExpr mode
        let pair (w: string) (name: string) =
            SqlExpr.Binary(wExpr, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.String w)),
            SqlExpr.Literal(SqlLiteral.String name)
        if full then
            let first = pair "0" "Sunday"
            let rest  = [pair "1" "Monday"; pair "2" "Tuesday"; pair "3" "Wednesday";
                         pair "4" "Thursday"; pair "5" "Friday"; pair "6" "Saturday"]
            SqlExpr.CaseExpr(first, rest, None)
        else
            let first = pair "0" "Sun"
            let rest  = [pair "1" "Mon"; pair "2" "Tue"; pair "3" "Wed"; pair "4" "Thu"; pair "5" "Fri"; pair "6" "Sat"]
            SqlExpr.CaseExpr(first, rest, None)

    // ─── Token dispatch ───────────────────────────────────────────────────────────

    let private tokenText = function
        | Literal s -> s
        | Specifier(ch, count) -> new String(ch, count)

    let private receiverTypeName = function
        | FromEpoch clrType when clrType = typeof<DateTimeOffset> -> "DateTimeOffset"
        | FromEpoch _ -> "DateTime"
        | FromDayNumber -> "DateOnly"
        | FromMillisecondsSinceMidnight -> "TimeOnly"

    let private validateTemporalFormatTokens (fmt: string) (tokens: FormatToken list) (mode: DateTimeTranslationMode) =
        let isUnsupported =
            match mode with
            | FromEpoch _ -> fun _ -> false
            | FromDayNumber ->
                function
                | Specifier(ch, _) when ch = 'H' || ch = 'h' || ch = 'm' || ch = 's' || ch = 'f' || ch = 'F' || ch = 't' || ch = 'K' || ch = 'z' -> true
                | _ -> false
            | FromMillisecondsSinceMidnight ->
                function
                | Specifier(ch, _) when ch = 'y' || ch = 'M' || ch = 'd' || ch = 'g' || ch = 'K' || ch = 'z' -> true
                | _ -> false

        match tokens |> List.tryFind isUnsupported with
        | Some bad ->
            let receiver = receiverTypeName mode
            let category =
                match mode with
                | FromDayNumber -> "date-only"
                | FromMillisecondsSinceMidnight -> "time-only"
                | FromEpoch _ -> "date/time"
            raise (NotSupportedException($"{receiver}.ToString: format specifier '{tokenText bad}' is not supported for {category} values."))
        | None -> ()

    let rec private dispatchToken (token: FormatToken) (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        let sec, msPart =
            match mode with
            | FromEpoch clrType ->
                let ms = toUnixMs rawExpr clrType
                unixSec ms, millisPart ms
            | FromDayNumber ->
                SqlExpr.Literal(SqlLiteral.Integer 0L), SqlExpr.Literal(SqlLiteral.Integer 0L)
            | FromMillisecondsSinceMidnight ->
                unixSec rawExpr, SqlExpr.Binary(rawExpr, BinaryOperator.Mod, SqlExpr.Literal(SqlLiteral.Integer 1000L))

        let sf fmt =
            match mode with
            | FromEpoch _ ->
                SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); sec; SqlExpr.Literal(SqlLiteral.String "unixepoch")])
            | FromDayNumber ->
                let julianDay = SqlExpr.Binary(rawExpr, BinaryOperator.Add, dayNumberEpoch)
                SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); julianDay; SqlExpr.Literal(SqlLiteral.String "julianday")])
            | FromMillisecondsSinceMidnight ->
                SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); sec; SqlExpr.Literal(SqlLiteral.String "unixepoch")])

        let sfInt fmt = castInt (sf fmt)

        let hHour = sfInt "%H"

        match token with
        | Literal s -> SqlExpr.Literal(SqlLiteral.String s)

        | Specifier('y', 1) ->
            // year mod 100, no zero-pad
            SqlExpr.Binary(castInt (sf "%Y"), BinaryOperator.Mod, SqlExpr.Literal(SqlLiteral.Integer 100L))
        | Specifier('y', 2) ->
            let y1 = SqlExpr.Binary(castInt (sf "%Y"), BinaryOperator.Mod, SqlExpr.Literal(SqlLiteral.Integer 100L))
            SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%02d"); y1])
        | Specifier('y', _) ->  // count 3 or 4+: full year
            sf "%Y"

        | Specifier('M', 1) -> sfInt "%m"
        | Specifier('M', 2) -> sf "%m"
        | Specifier('M', 3) -> buildMonthCase false rawExpr mode
        | Specifier('M', _) -> buildMonthCase true  rawExpr mode

        | Specifier('d', 1) -> sfInt "%d"
        | Specifier('d', 2) -> sf "%d"
        | Specifier('d', 3) -> buildDayOfWeekCase false rawExpr mode
        | Specifier('d', _) -> buildDayOfWeekCase true  rawExpr mode

        | Specifier('H', 1) -> sfInt "%H"
        | Specifier('H', _) -> sf "%H"

        | Specifier('h', 1) ->
            // 12-hour: (hour + 11) % 12 + 1. Handles hour=0→12, hour=12→12, hour=13→1
            SqlExpr.Binary(
                SqlExpr.Binary(
                    SqlExpr.Binary(hHour, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 11L)),
                    BinaryOperator.Mod, SqlExpr.Literal(SqlLiteral.Integer 12L)),
                BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L))
        | Specifier('h', _) ->
            let h1 = SqlExpr.Binary(
                         SqlExpr.Binary(hHour, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 11L)),
                         BinaryOperator.Mod, SqlExpr.Literal(SqlLiteral.Integer 12L))
            let h1_plus1 = SqlExpr.Binary(h1, BinaryOperator.Add, SqlExpr.Literal(SqlLiteral.Integer 1L))
            SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%02d"); h1_plus1])

        | Specifier('m', 1) -> sfInt "%M"
        | Specifier('m', _) -> sf "%M"

        | Specifier('s', 1) -> sfInt "%S"
        | Specifier('s', _) -> sf "%S"

        | Specifier('f', 1) -> SqlExpr.Binary(msPart, BinaryOperator.Div, SqlExpr.Literal(SqlLiteral.Integer 100L))
        | Specifier('f', 2) -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%02d"); SqlExpr.Binary(msPart, BinaryOperator.Div, SqlExpr.Literal(SqlLiteral.Integer 10L))])
        | Specifier('f', 3) -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%03d"); msPart])
        | Specifier('f', 4) -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%04d"); SqlExpr.Binary(msPart, BinaryOperator.Mul, SqlExpr.Literal(SqlLiteral.Integer 10L))])
        | Specifier('f', 5) -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%05d"); SqlExpr.Binary(msPart, BinaryOperator.Mul, SqlExpr.Literal(SqlLiteral.Integer 100L))])
        | Specifier('f', 6) -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%06d"); SqlExpr.Binary(msPart, BinaryOperator.Mul, SqlExpr.Literal(SqlLiteral.Integer 1000L))])
        | Specifier('f', _) -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%07d"); SqlExpr.Binary(msPart, BinaryOperator.Mul, SqlExpr.Literal(SqlLiteral.Integer 10000L))])

        | Specifier('F', count) ->
            // F-family: same as f-family but RTRIM trailing zeros; empty string if all-zero
            let fExpr = dispatchToken (Specifier('f', count)) rawExpr mode
            let isAllZero =
                if count = 1 then SqlExpr.Binary(SqlExpr.Binary(msPart, BinaryOperator.Div, SqlExpr.Literal(SqlLiteral.Integer 100L)), BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
                else              SqlExpr.Binary(msPart, BinaryOperator.Eq, SqlExpr.Literal(SqlLiteral.Integer 0L))
            let trimmed =
                if count = 1 then SqlExpr.Cast(fExpr, "TEXT")
                else              SqlExpr.FunctionCall("RTRIM", [fExpr; SqlExpr.Literal(SqlLiteral.String "0")])
            SqlExpr.CaseExpr((isAllZero, SqlExpr.Literal(SqlLiteral.String "")), [], Some trimmed)

        | Specifier('t', 1) ->
            SqlExpr.CaseExpr((SqlExpr.Binary(hHour, BinaryOperator.Lt, SqlExpr.Literal(SqlLiteral.Integer 12L)), SqlExpr.Literal(SqlLiteral.String "A")), [], Some(SqlExpr.Literal(SqlLiteral.String "P")))
        | Specifier('t', _) ->
            SqlExpr.CaseExpr((SqlExpr.Binary(hHour, BinaryOperator.Lt, SqlExpr.Literal(SqlLiteral.Integer 12L)), SqlExpr.Literal(SqlLiteral.String "AM")), [], Some(SqlExpr.Literal(SqlLiteral.String "PM")))

        | Specifier('K', _) ->
            // DateTime → "Z" (DateTimeKind.Utc stored); DateTimeOffset → "+00:00" (UTC+0 offset notation)
            match mode with
            | FromEpoch clrType when clrType = typeof<DateTimeOffset> -> SqlExpr.Literal(SqlLiteral.String "+00:00")
            | _ -> SqlExpr.Literal(SqlLiteral.String "Z")

        | Specifier('g', _) -> SqlExpr.Literal(SqlLiteral.String "A.D.")  // all stored dates are CE

        | Specifier('z', count) ->
            let zStr = String.replicate count "z"
            raise (NotSupportedException(
                $"DateTime.ToString: format specifier '{zStr}' is not supported — UTC offset data is not stored " +
                "(DateTimeOffset.ToUnixTimeMilliseconds discards the offset; DateTime.ToBinary does not store timezone)."))

        | Specifier(ch, _) ->
            // Unknown specifier chars should not reach here (tokenizer only emits known specifier chars)
            raise (NotSupportedException($"DateTime.ToString: unknown specifier char '{ch}'."))

    // ─── String translation entry ─────────────────────────────────────────────────

    /// Translate DateTime/DateTimeOffset .ToString(fmt) to a SQL string expression.
    /// rawExpr: the raw stored SqlExpr for the DateTime/DateTimeOffset column.
    /// mode: FromEpoch (with CLR type) or FromIsoString (for new DateTime(y,m,d) receiver).
    /// fmt: the resolved format string (null/empty treated as "G").
    let internal translateDateTimeToString (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) (fmt: string) : SqlExpr =
        // null/empty format → treat as "G" (matches .NET .ToString("", InvariantCulture) = general format)
        let fmt = if String.IsNullOrEmpty fmt then "G" else fmt

        // Standard vs custom dispatch: single-char format in standard set → expand and retokenize
        let tokens =
            if fmt.Length = 1 && standardSpecifierSet.Contains(fmt.[0]) then
                tokenize (expandStandardSpecifier fmt.[0])
            else
                tokenize fmt

        validateTemporalFormatTokens fmt tokens mode

        let parts = tokens |> List.map (fun tok -> dispatchToken tok rawExpr mode)

        match parts with
        | []     -> SqlExpr.Literal(SqlLiteral.String "")
        | [single] -> single
        | many   -> SqlExpr.FunctionCall("CONCAT", many)

    /// Extract a constant format string from a LINQ expression argument.
    /// Returns Some(fmt) if the expression is constant (no row parameter references).
    /// Returns None if the expression depends on a row parameter (non-constant) — caller throws Error #4.
    let internal tryExtractConstantFormat (fmtExpr: Expression) : string option =
        if isFullyConstant fmtExpr then
            Some(evaluateExpr<string> fmtExpr)
        else
            None

    /// Unified DateTime.ToString→SQL translation shared by MethodCall.fs and GroupByOps.fs.
    let internal translateDateTimeToStringCall
        (rawExpr: SqlExpr)
        (objExpr: Expression)
        (argCount: int)
        (firstArg: Expression option) : SqlExpr =
        let fmtStr =
            if argCount = 0 then "G"
            else
                match firstArg |> Option.bind tryExtractConstantFormat with
                | Some fmt -> fmt
                | None ->
                    raise (NotSupportedException(
                        "DateTime.ToString(format): the format argument must be a compile-time constant for SQL translation. " +
                        "Use a string literal, or call AsEnumerable() before ToString to evaluate client-side."))
        let mode =
            match unwrapNullable objExpr.Type with
            | t when t = typeof<DateOnly> -> DateTimeTranslationMode.FromDayNumber
            | t when t = typeof<TimeOnly> -> DateTimeTranslationMode.FromMillisecondsSinceMidnight
            | _ -> DateTimeTranslationMode.FromEpoch objExpr.Type
        translateDateTimeToString rawExpr mode fmtStr
