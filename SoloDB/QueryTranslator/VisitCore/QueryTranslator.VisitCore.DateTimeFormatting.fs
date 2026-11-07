namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers
open Utils
open DateTimeSql

/// Temporal format translation retains its established invariant SQL spelling.
module internal DateTimeFormatting =

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
        | FromEpoch clrType when typeof<DateTimeOffset>.Equals(clrType) -> "DateTimeOffset"
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
                unixSec rawExpr, SqlExpr.Binary(rawExpr, BinaryOperator.Mod, ms1000)

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
        | Specifier('f', 6) -> SqlExpr.FunctionCall("printf", [SqlExpr.Literal(SqlLiteral.String "%06d"); SqlExpr.Binary(msPart, BinaryOperator.Mul, ms1000)])
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
            | FromEpoch clrType when typeof<DateTimeOffset>.Equals(clrType) -> SqlExpr.Literal(SqlLiteral.String "+00:00")
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
            Some(ExpressionEvaluation.evaluateExpr<string> fmtExpr)
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
            | t when typeof<DateOnly>.Equals(t) -> DateTimeTranslationMode.FromDayNumber
            | t when typeof<TimeOnly>.Equals(t) -> DateTimeTranslationMode.FromMillisecondsSinceMidnight
            | _ -> DateTimeTranslationMode.FromEpoch objExpr.Type
        translateDateTimeToString rawExpr mode fmtStr
