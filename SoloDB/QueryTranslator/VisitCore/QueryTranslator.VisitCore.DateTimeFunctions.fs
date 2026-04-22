namespace SoloDatabase

open System
open System.Linq.Expressions
open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryTranslatorBaseHelpers

/// Translation mode: from unix-ms epoch field, or from ISO string (new DateTime(y,m,d)).
type internal DateTimeTranslationMode =
    | FromEpoch of clrType: Type
    | FromIsoString

module internal DateTimeFunctions =

    // ─── Constants ────────────────────────────────────────────────────────────────

    // ToBinary mask: strips Kind bits 63:62 to recover UTC-equivalent ticks
    let private mask    = SqlExpr.Literal(SqlLiteral.Integer 4611686018427387903L)  // 0x3FFFFFFFFFFFFFFF
    let private epochTk = SqlExpr.Literal(SqlLiteral.Integer 621355968000000000L)   // ticks 0001-01-01 → 1970-01-01 UTC
    let private ticksMs = SqlExpr.Literal(SqlLiteral.Integer 10000L)               // 100-ns ticks per millisecond

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

    let private strftimeFromIso (fmt: string) (isoExpr: SqlExpr) : SqlExpr =
        SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); isoExpr])

    let private strftime (fmt: string) (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        match mode with
        | FromEpoch clrType -> strftimeFromEpoch fmt (toUnixMs rawExpr clrType)
        | FromIsoString     -> strftimeFromIso fmt rawExpr

    let private castInt (expr: SqlExpr) : SqlExpr = SqlExpr.Cast(expr, "INTEGER")

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

    let internal unwrapNullable (t: Type) : Type =
        if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Nullable<_>>
        then t.GetGenericArguments().[0]
        else t

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

    let rec private dispatchToken (token: FormatToken) (rawExpr: SqlExpr) (mode: DateTimeTranslationMode) : SqlExpr =
        let ms =
            match mode with
            | FromEpoch clrType -> toUnixMs rawExpr clrType
            | FromIsoString     -> rawExpr  // ISO string: arithmetic on TEXT unsupported for epoch ops

        let sec =
            match mode with
            | FromEpoch _ -> unixSec ms
            | FromIsoString -> rawExpr  // strftimeFromIso uses rawExpr directly

        let msPart =
            match mode with
            | FromEpoch _ -> millisPart ms  // integer arithmetic on int64
            | FromIsoString -> SqlExpr.Literal(SqlLiteral.Integer 0L)  // midnight, always zero

        let sf fmt =
            match mode with
            | FromEpoch _   -> SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); sec; SqlExpr.Literal(SqlLiteral.String "unixepoch")])
            | FromIsoString -> SqlExpr.FunctionCall("strftime", [SqlExpr.Literal(SqlLiteral.String fmt); sec])

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
