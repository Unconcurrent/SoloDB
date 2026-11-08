namespace SoloDatabase

open System
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.QueryTranslatorBaseHelpers
open Utils
open DateTimeSql
open SoloDatabase.QueryTranslatorBaseTypes

/// Temporal construction preserves the established stored scalar representation.
module internal DateTimeConstruction =

    let private unsupported (typeName: string) (argCount: int) =
        raise (NotSupportedException($"new {typeName} with signature of {argCount} argument(s) is not supported in SQL translation."))

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

    let private literalZeroOffset (value: Expression option) =
        match value with
        | None -> true
        | Some(:? ConstantExpression as value) -> unbox<TimeSpan> value.Value = TimeSpan.Zero
        | Some(:? MemberExpression as value) -> isNull value.Expression && value.Member.DeclaringType = typeof<TimeSpan> && value.Member.Name = "Zero"
        | _ -> false

    let private literalUtc (value: Expression) =
        match value with
        | :? NewExpression as created when typeof<DateTime>.Equals created.Type && created.Arguments.Count > 1 ->
            match created.Arguments[created.Arguments.Count-1] with
            | :? ConstantExpression as kind when typeof<DateTimeKind>.Equals kind.Type -> unbox<DateTimeKind> kind.Value = DateTimeKind.Utc
            | _ -> false
        | :? ConstantExpression as value when typeof<DateTime>.Equals value.Type -> (unbox<DateTime> value.Value).Kind = DateTimeKind.Utc
        | _ -> false

    let internal usesStoredOffsetFormat (created: NewExpression) =
        typeof<DateTimeOffset>.Equals created.Type &&
        (created.Arguments.Count = 1 || created.Arguments.Count = 2 && literalZeroOffset(Some created.Arguments[1])) &&
        literalUtc created.Arguments[0]

    let private utcDateArgument (value: Expression) visitArg =
        match value with
        | :? NewExpression as created -> unixMsFromDateTimeTicks(translateDateTimeConstructorToTicks created visitArg)
        | :? ConstantExpression as value -> intLit(DateTimeOffset(unbox<DateTime> value.Value).ToUnixTimeMilliseconds())
        | _ -> invalidOp "A literal UTC constructor argument is required."

    let private originalOffset (qb: QueryBuilder) (visitArg: Expression -> SqlExpr) (expression: Expression) =
        match expression with
        | :? NewExpression as created when typeof<TimeSpan>.Equals created.Type && created.Arguments.Count = 1 && typeof<int64>.Equals created.Arguments[0].Type ->
            visitArg created.Arguments[0]
        | _ when isFullyConstant expression || (qb.SourceContext.BindQueryValue |> ValueOption.exists(fun binder -> binder.IsValue expression)) ->
            let ticks = BoundValueCallExpression([|expression|],typeof<int64>,Func<obj array,obj>(fun values -> box((unbox<TimeSpan> values[0]).Ticks)))
            visitArg ticks
        | _ -> mul (visitArg expression) ticksMs

    let private translateDateTimeOffsetConstructor (ne: NewExpression) (qb: QueryBuilder) (visitArg: Expression -> SqlExpr) =
        let arg i = visitArg ne.Arguments.[i]
        let dtoFromParts y m d h mi s ms offsetExpr =
            let dtTicks = add (dateTicksUnchecked y m d) (timeTicks h mi s ms)
            let offsetMs = offsetMsFromTimeSpanExpr offsetExpr visitArg
            whenValid (all [ validDate y m d; validTimeOfDay h mi s ms; validOffsetMs offsetMs ]) (sub (unixMsFromDateTimeTicks dtTicks) offsetMs)
        let dtoFromDateTimeArg dtArg offsetArg =
            if literalUtc dtArg && literalZeroOffset offsetArg then utcDateArgument dtArg visitArg else
            let raw = visitArg dtArg
            let offset = offsetArg |> Option.map (originalOffset qb visitArg) |> Option.defaultValue zero
            let mode = if offsetArg.IsSome then 1L else 0L
            SqlExpr.FunctionCall("DTO_CLOCK_PART",[intLit mode;raw;offset;SqlExpr.Literal(SqlLiteral.String "Value")])

        match ne.Arguments.Count with
        | 1 when typeof<DateTime>.Equals(ne.Arguments.[0].Type) -> dtoFromDateTimeArg ne.Arguments.[0] None
        | 2 when typeof<DateTime>.Equals(ne.Arguments.[0].Type) -> dtoFromDateTimeArg ne.Arguments.[0] (Some ne.Arguments.[1])
        | 2 when typeof<int64>.Equals(ne.Arguments.[0].Type) ->
            let offsetMs = offsetMsFromTimeSpanExpr ne.Arguments.[1] visitArg
            whenValid (validOffsetMs offsetMs) (sub (unixMsFromDateTimeTicks (arg 0)) offsetMs)
        | 7 -> dtoFromParts (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) zero ne.Arguments.[6]
        | 8 -> dtoFromParts (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) (arg 6) ne.Arguments.[7]
        | n -> unsupported "DateTimeOffset" n

    let internal tryTranslateDateTimeOffsetConstructorMember (memberName: string) (ne: NewExpression) (qb: QueryBuilder) (visitArg: Expression -> SqlExpr) : SqlExpr option =
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
            if List.contains memberName ["Year";"Month";"Day";"Hour";"Minute";"Second";"Millisecond";"DayOfWeek";"DayOfYear"] then
                let offsetMs = offsetMsFromTimeSpanExpr offsetExpr visitArg
                whenValid (all [ validDate y m d; validTimeOfDay h mi s ms; validOffsetMs offsetMs ]) (fromLocalTicks localTicks)
            else
                let offset = originalOffset qb visitArg offsetExpr
                let clock = whenValid (andAlso (validDate y m d) (validTimeOfDay h mi s ms)) localTicks
                SqlExpr.FunctionCall("DTO_CLOCK_PART",[intLit 2L;clock;offset;SqlExpr.Literal(SqlLiteral.String memberName)])
        let fromDateTimeArg dtArg offsetArg =
            if literalUtc dtArg && literalZeroOffset offsetArg &&
               List.contains memberName ["Year";"Month";"Day";"Hour";"Minute";"Second";"Millisecond";"DayOfWeek";"DayOfYear"] then
                translateOffsetMemberFromUnixMs(utcDateArgument dtArg visitArg)
            else
            let raw = visitArg dtArg
            let offset = offsetArg |> Option.map (originalOffset qb visitArg) |> Option.defaultValue zero
            SqlExpr.FunctionCall("DTO_CLOCK_PART",[intLit(if offsetArg.IsSome then 1L else 0L);raw;offset;SqlExpr.Literal(SqlLiteral.String memberName)])

        match ne.Arguments.Count with
        | 1 when typeof<DateTime>.Equals(ne.Arguments.[0].Type) -> Some (fromDateTimeArg ne.Arguments.[0] None)
        | 2 when typeof<DateTime>.Equals(ne.Arguments.[0].Type) -> Some (fromDateTimeArg ne.Arguments.[0] (Some ne.Arguments.[1]))
        | 2 when typeof<int64>.Equals(ne.Arguments.[0].Type) ->
            if List.contains memberName ["Year";"Month";"Day";"Hour";"Minute";"Second";"Millisecond";"DayOfWeek";"DayOfYear"] then
                let ticks = arg 0
                let offset = arg 1
                Some(whenValid (validOffsetMs offset) (fromLocalTicks ticks))
            else Some(SqlExpr.FunctionCall("DTO_CLOCK_PART",[intLit 2L;arg 0;originalOffset qb visitArg ne.Arguments[1];SqlExpr.Literal(SqlLiteral.String memberName)]))
        | 7 -> Some (fromDateParts (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) zero ne.Arguments.[6])
        | 8 ->
            Some (fromDateParts (arg 0) (arg 1) (arg 2) (arg 3) (arg 4) (arg 5) (arg 6) ne.Arguments.[7])
        | _ -> None

    let internal tryTranslateDateTimeOffsetFormat (ne: NewExpression) (format: unit -> SqlExpr) (qb: QueryBuilder) visitArg =
        if not(typeof<DateTimeOffset>.Equals ne.Type) || usesStoredOffsetFormat ne then None else
        match ne.Arguments.Count with
        | 1 when typeof<DateTime>.Equals ne.Arguments[0].Type ->
            Some(SqlExpr.FunctionCall("DTO_CLOCK_FORMAT",[zero;visitArg ne.Arguments[0];zero;format()]))
        | 2 when typeof<DateTime>.Equals ne.Arguments[0].Type ->
            Some(SqlExpr.FunctionCall("DTO_CLOCK_FORMAT",[intLit 1L;visitArg ne.Arguments[0];originalOffset qb visitArg ne.Arguments[1];format()]))
        | 2 when typeof<int64>.Equals ne.Arguments[0].Type ->
            Some(SqlExpr.FunctionCall("DTO_CLOCK_FORMAT",[intLit 2L;visitArg ne.Arguments[0];originalOffset qb visitArg ne.Arguments[1];format()]))
        | 7 | 8 ->
            let args = ne.Arguments |> Seq.mapi(fun index expression ->
                if index = ne.Arguments.Count-1 then originalOffset qb visitArg expression else visitArg expression) |> Seq.toArray
            let ms = if args.Length = 8 then args[6] else zero
            let clock = add (dateTicksUnchecked args[0] args[1] args[2]) (timeTicks args[3] args[4] args[5] ms)
            let clock = whenValid (andAlso (validDate args[0] args[1] args[2]) (validTimeOfDay args[3] args[4] args[5] ms)) clock
            Some(SqlExpr.FunctionCall("DTO_CLOCK_FORMAT",[intLit 2L;clock;args[args.Length-1];format()]))
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

    let internal translateDateTimeLikeConstructor (ne: NewExpression) (qb: QueryBuilder) (visitArg: Expression -> SqlExpr) : SqlExpr option =
        if typeof<DateTime>.Equals(ne.Type) then Some (translateDateTimeConstructorToTicks ne visitArg)
        elif typeof<DateTimeOffset>.Equals(ne.Type) then Some (translateDateTimeOffsetConstructor ne qb visitArg)
        elif typeof<DateOnly>.Equals(ne.Type) then Some (translateDateOnlyConstructor ne visitArg)
        elif typeof<TimeOnly>.Equals(ne.Type) then Some (translateTimeOnlyConstructor ne visitArg)
        elif typeof<TimeSpan>.Equals(ne.Type) then Some (translateTimeSpanConstructor ne visitArg)
        else None

