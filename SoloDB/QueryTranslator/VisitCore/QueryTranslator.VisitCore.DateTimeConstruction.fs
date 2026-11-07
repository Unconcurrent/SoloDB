namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers
open Utils
open DateTimeSql

/// Temporal construction preserves the established stored scalar representation.
module internal DateTimeConstruction =

    let private tryEval<'T> (expr: Expression) =
        if isFullyConstant expr then Some(ExpressionEvaluation.evaluateExpr<'T> expr) else None

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

    let private constructorKind (expr: Expression) visitArg =
        match expr with
        | :? ConstantExpression as constant ->
            let kind = unbox<DateTimeKind> constant.Value
            if kind >= DateTimeKind.Unspecified && kind <= DateTimeKind.Local then intLit(kindFlag kind) else nullLit
        | _ ->
            let kind = visitArg expr
            SqlExpr.CaseExpr((eq kind zero,zero),
                [eq kind (intLit 1L),intLit(kindFlag DateTimeKind.Utc)
                 eq kind (intLit 2L),intLit(kindFlag DateTimeKind.Local)],Some nullLit)

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
            let withKind = add ticks kind
            whenValid (andAlso (validDate y m d) (validTimeOfDay h mi s ms)) withKind

        match ne.Arguments.Count with
        | 1 ->
            arg 0
        | 2 when typeof<DateOnly>.Equals(ne.Arguments.[0].Type) && typeof<TimeOnly>.Equals(ne.Arguments.[1].Type) ->
            add (mul (arg 0) ticksDay) (mul (arg 1) ticksMs)
        | 2 when typeof<int64>.Equals(ne.Arguments.[0].Type) && typeof<DateTimeKind>.Equals(ne.Arguments.[1].Type) ->
            add (arg 0) (constructorKind ne.Arguments.[1] visitArg)
        | 3 when typeof<DateOnly>.Equals(ne.Arguments.[0].Type) && typeof<TimeOnly>.Equals(ne.Arguments.[1].Type) && typeof<DateTimeKind>.Equals(ne.Arguments.[2].Type) ->
            add (add (mul (arg 0) ticksDay) (mul (arg 1) ticksMs)) (constructorKind ne.Arguments.[2] visitArg)
        | 3 ->
            // new DateTime(y,m,d) has Kind=Unspecified, so the high Kind bits are zero and the mask is identity.
            ymdTime (arg 0) (arg 1) (arg 2) zero zero zero zero zero
        | 6 ->
            ymdTime (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) zero zero
        | 7 when typeof<DateTimeKind>.Equals(ne.Arguments.[6].Type) ->
            ymdTime (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) zero (constructorKind ne.Arguments.[6] visitArg)
        | 7 ->
            ymdTime (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) (arg 6) zero
        | 8 when typeof<DateTimeKind>.Equals(ne.Arguments.[7].Type) ->
            ymdTime (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) (arg 6) (constructorKind ne.Arguments.[7] visitArg)
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
            | :? NewExpression as dtNe when typeof<DateTime>.Equals(dtNe.Type) ->
                let explicitKind =
                    if dtNe.Arguments.Count >= 2 && typeof<DateTimeKind>.Equals(dtNe.Arguments.[dtNe.Arguments.Count - 1].Type) then
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
        | 1 when typeof<DateTime>.Equals(ne.Arguments.[0].Type) -> dtoFromDateTimeArg ne.Arguments.[0] None
        | 2 when typeof<DateTime>.Equals(ne.Arguments.[0].Type) -> dtoFromDateTimeArg ne.Arguments.[0] (Some ne.Arguments.[1])
        | 2 when typeof<int64>.Equals(ne.Arguments.[0].Type) ->
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
            | "Millisecond" -> castInt (SqlExpr.Binary(unixMs, BinaryOperator.Mod, ms1000))
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
            | :? NewExpression as dtNe when typeof<DateTime>.Equals(dtNe.Type) ->
                let explicitKind =
                    if dtNe.Arguments.Count >= 2 && typeof<DateTimeKind>.Equals(dtNe.Arguments.[dtNe.Arguments.Count - 1].Type) then
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
        | 1 when typeof<DateTime>.Equals(ne.Arguments.[0].Type) -> Some (fromDateTimeArg ne.Arguments.[0] None)
        | 2 when typeof<DateTime>.Equals(ne.Arguments.[0].Type) -> Some (fromDateTimeArg ne.Arguments.[0] (Some ne.Arguments.[1]))
        | 2 when typeof<int64>.Equals(ne.Arguments.[0].Type) ->
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
        | 1 when typeof<int64>.Equals(ne.Arguments.[0].Type) -> div (arg 0) ticksMs
        | 2 -> hms (arg 0) (arg 1) zero zero
        | 3 -> hms (arg 0) (arg 1) (arg 2) zero
        | 4 -> hms (arg 0) (arg 1) (arg 2) (arg 3)
        | n -> unsupported "TimeOnly" n

    let private translateTimeSpanConstructor (ne: NewExpression) (visitArg: Expression -> SqlExpr) =
        let arg i = visitArg ne.Arguments.[i]
        // TimeSpan has no row-dependent invalid component domain under supported overloads; storage is total milliseconds.
        match ne.Arguments.Count with
        | 1 when typeof<int64>.Equals(ne.Arguments.[0].Type) -> div (arg 0) ticksMs
        | 3 -> add (add (mul (arg 0) ms3600000) (mul (arg 1) ms60000)) (mul (arg 2) ms1000)
        | 4 -> add (mul (arg 0) ms86400000) (add (add (mul (arg 1) ms3600000) (mul (arg 2) ms60000)) (mul (arg 3) ms1000))
        | 5 -> add (add (mul (arg 0) ms86400000) (add (add (mul (arg 1) ms3600000) (mul (arg 2) ms60000)) (mul (arg 3) ms1000))) (arg 4)
        | n -> unsupported "TimeSpan" n

    let internal translateDateTimeLikeConstructor (ne: NewExpression) (visitArg: Expression -> SqlExpr) : SqlExpr option =
        if typeof<DateTime>.Equals(ne.Type) then Some (translateDateTimeConstructorToTicks ne visitArg)
        elif typeof<DateTimeOffset>.Equals(ne.Type) then Some (translateDateTimeOffsetConstructor ne visitArg)
        elif typeof<DateOnly>.Equals(ne.Type) then Some (translateDateOnlyConstructor ne visitArg)
        elif typeof<TimeOnly>.Equals(ne.Type) then Some (translateTimeOnlyConstructor ne visitArg)
        elif typeof<TimeSpan>.Equals(ne.Type) then Some (translateTimeSpanConstructor ne visitArg)
        else None

