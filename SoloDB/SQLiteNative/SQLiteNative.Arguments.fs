namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Globalization
open System.Numerics
open System.Reflection
open System.Linq.Expressions
open SQLitePCL

/// Named SQL arguments supplied as dynamically typed values.
/// Values are captured when execution begins, before the first statement takes effect.
[<Sealed>]
type SqlArguments(names: string[], values: obj[]) =
    do
        if isNull names then nullArg "names"
        if isNull values then nullArg "values"
        if names.Length <> values.Length then invalidArg "values" "Names and values must have equal lengths."
    member internal _.Names = names
    member internal _.Values = values

[<Struct>]
type internal TrimmedArray = { Array: Array; TrimmedLen: int }

type internal ParameterValues = {
    Constants: KeyValuePair<string, obj>[]
    Names: string[]
    Values: obj[]
    Offset: int
}

module internal NativeParameterBinding =
    let private invariant = CultureInfo.InvariantCulture
    let textSpan (statement: sqlite3_stmt) index (span: ReadOnlySpan<char>) (original: string) =
        if span.IsEmpty then raw.sqlite3_bind_text(statement, index, utf8z.FromSpan(ReadOnlySpan(NativeTextEncoding.emptyText)))
        elif NativeTextEncoding.requiresUtf8(span) then
            let text =
                if isNull original then String(span.ToArray())
                elif span.Length = original.Length then original
                else original.Substring(0, span.Length)
            raw.sqlite3_bind_text(statement, index, text)
        else raw.sqlite3_bind_text16(statement, index, span)
    let text (statement: sqlite3_stmt) index size (value: string) =
        let length = if size >= 0 then min size value.Length else value.Length
        textSpan statement index (value.AsSpan(0,length)) value
    let textBuffer statement index size (value: ArraySegment<char>) =
        let length = if size >= 0 then min size value.Count else value.Count
        let span = ReadOnlySpan(value.Array,value.Offset,length)
        if span.IsEmpty then textSpan statement index span null
        elif NativeTextEncoding.requiresUtf8 span then
            raw.sqlite3_bind_text(statement,index,String(value.Array,value.Offset,length))
        else raw.sqlite3_bind_text16(statement,index,span)
    let blob (statement: sqlite3_stmt) index size (value: byte[]) =
        let length = if size >= 0 then min size value.Length else value.Length
        if length = 0 then raw.sqlite3_bind_zeroblob(statement, index, 0)
        else raw.sqlite3_bind_blob(statement, index, ReadOnlySpan(value, 0, length))
    let real (statement: sqlite3_stmt) index value =
        if Double.IsNaN value then raise (InvalidOperationException("Cannot store 'NaN' values."))
        raw.sqlite3_bind_double(statement, index, value)
    let normalize (value: obj) =
        match value with
        | :? TrimmedArray as value -> struct (value.Array :> obj, value.TrimmedLen)
        | :? DateTimeOffset as value -> struct (box (value.ToUnixTimeMilliseconds()), -1)
        | value -> struct (value, -1)
    let private storageValue (value: obj) =
        match value with
        | :? decimal as value -> box (value.ToString("0.0###########################", invariant))
        | :? BigInteger as value -> box (NativeValueConversion.bigIntegerText value)
        | :? Guid as value -> box (value.ToString().ToUpperInvariant())
        | :? char as value -> box (string value)
        | :? DateTime as value -> box (NativeValueConversion.dateTimeText value)
        | :? DateTimeOffset as value -> box (NativeValueConversion.offsetText value)
        | :? DateOnly as value -> box (NativeValueConversion.dateText value)
        | :? TimeOnly as value -> box (NativeValueConversion.timeText value)
        | :? TimeSpan as value -> box (value.ToString("c", invariant))
        | _ -> value
    let inferRawType (value: obj) =
        match value with
        | null | :? DBNull -> raw.SQLITE_TEXT
        | :? (byte array) -> raw.SQLITE_BLOB
        | :? double | :? single -> raw.SQLITE_FLOAT
        | :? bool | :? byte | :? sbyte | :? int16 | :? uint16 | :? int | :? uint32 | :? int64 | :? uint64 -> raw.SQLITE_INTEGER
        | :? ArraySegment<char> | :? string | :? char | :? decimal | :? BigInteger | :? Guid | :? DateTime | :? DateTimeOffset | :? DateOnly | :? TimeOnly | :? TimeSpan -> raw.SQLITE_TEXT
        | value when value.GetType().IsEnum -> raw.SQLITE_INTEGER
        | _ -> raise (InvalidOperationException($"The member of type {value.GetType().Name} cannot be used as a parameter value."))
    let private julianDate (value: DateTime) =
        let year = if value.Month <= 2 then value.Year - 1 else value.Year
        let month = if value.Month <= 2 then value.Month + 12 else value.Month
        let century = year / 100
        let days = (36525 * (year + 4716) / 100) + (306001 * (month + 1) / 10000) + value.Day + 2 - century + century / 4
        let milliseconds = int64 ((float days - 1524.5) * 86400000.0)
        float (milliseconds + int64 value.Hour * 3600000L + int64 value.Minute * 60000L + int64 value.Second * 1000L + int64 value.Millisecond) / 86400000.0
    let rawStorageValue (value: obj) sqliteType =
        match value with
        | :? char as value when sqliteType = raw.SQLITE_INTEGER -> box (int64 value)
        | :? Guid as value when sqliteType = raw.SQLITE_BLOB -> box (value.ToByteArray())
        | :? DateTime as value when sqliteType = raw.SQLITE_FLOAT -> box (julianDate value)
        | :? DateTimeOffset as value when sqliteType = raw.SQLITE_FLOAT -> box (julianDate value.DateTime)
        | :? DateOnly as value when sqliteType = raw.SQLITE_FLOAT -> box (julianDate (DateTime(value.Year, value.Month, value.Day)))
        | :? TimeOnly as value when sqliteType = raw.SQLITE_FLOAT -> box (float (value.Hour * 3600000 + value.Minute * 60000 + value.Second * 1000 + value.Millisecond) / 86400000.0)
        | :? TimeSpan as value when sqliteType = raw.SQLITE_FLOAT -> box value.TotalDays
        | _ -> storageValue value
    let rec private bindValue (statement: sqlite3_stmt) index size (value: obj) =
        let text value = text statement index size value
        let blob value = blob statement index size value
        let real value = real statement index value
        match value with
        | null | :? DBNull -> raw.sqlite3_bind_null(statement, index)
        | :? ArraySegment<char> as value -> textBuffer statement index size value
        | :? string as value -> text value
        | :? (byte array) as value -> blob value
        | :? int64 as value -> raw.sqlite3_bind_int64(statement, index, value)
        | :? int as value -> raw.sqlite3_bind_int64(statement, index, int64 value)
        | :? bool as value -> raw.sqlite3_bind_int(statement, index, if value then 1 else 0)
        | :? double as value -> real value
        | :? single as value -> real (double value)
        | :? byte as value -> raw.sqlite3_bind_int(statement, index, int value)
        | :? sbyte as value -> raw.sqlite3_bind_int(statement, index, int value)
        | :? int16 as value -> raw.sqlite3_bind_int(statement, index, int value)
        | :? uint16 as value -> raw.sqlite3_bind_int(statement, index, int value)
        | :? uint32 as value -> raw.sqlite3_bind_int64(statement, index, int64 value)
        | :? uint64 as value -> raw.sqlite3_bind_int64(statement, index, int64 value)
        | value when value.GetType().IsEnum ->
            if typeof<uint64>.Equals(TypeMetadata.enumUnderlying(value.GetType())) then raw.sqlite3_bind_int64(statement, index, int64 (Convert.ToUInt64(value, invariant)))
            else raw.sqlite3_bind_int64(statement, index, Convert.ToInt64(value, invariant))
        | _ ->
            let encoded = storageValue value
            if Object.ReferenceEquals(encoded, value) then
                raise (InvalidOperationException($"The member of type {value.GetType().Name} cannot be used as a parameter value."))
            bindValue statement index size encoded

    let bind statement index value =
        let struct (value, size) = normalize value
        bindValue statement index size value
    let bindRaw statement index size value =
        if isNull value then raise (InvalidOperationException("Value must be set."))
        bindValue statement index size value

[<Sealed>]
type internal NativeBindingTarget(database: NativeDatabase, statement: sqlite3_stmt, count: int) =
    let written = Array.zeroCreate<bool> count
    let mutable ordinals: int[] = Array.empty
    let mutable bound = 0
    let generation = database.Generation
    let mutable session = 0L
    let mutable active = false
    let mutable thread = 0
    member _.Session = session
    member _.Statement = statement
    member _.Check(result) = database.Check(result)
    member _.Begin(layout: int[]) =
        database.EnsureGeneration(generation)
        session <- session + 1L
        active <- true
        thread <- Environment.CurrentManagedThreadId
        ordinals <- layout
        Array.Clear(written, 0, written.Length)
        bound <- 0
    member _.End() = active <- false
    member _.Ordinal(argument: int, expectedSession: int64) =
        if not active || expectedSession <> session || thread <> Environment.CurrentManagedThreadId then
            raise (InvalidOperationException("The SQL parameter writer is no longer active on its binding callback."))
        database.EnsureGeneration(generation)
        if statement.IsClosed || statement.IsInvalid then raise (ObjectDisposedException("SQL statement"))
        let ordinal = ordinals.[argument]
        if ordinal > 0 && not written.[ordinal - 1] then
            written.[ordinal - 1] <- true
            bound <- bound + 1
        ordinal
    member _.Finish() =
        if bound <> count && raw.sqlite3_stmt_isexplain(statement) = 0 then
            raise (InvalidOperationException("The typed SQL binder omitted a required value."))

/// Direct primitive binding into the active statement. It does not own or retain the input state.
[<Struct>]
type SqlParameterWriter =
    val private target: NativeBindingTarget
    val private session: int64
    internal new(target: NativeBindingTarget) = { target = target; session = target.Session }
    [<System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)>]
    member private this.Ordinal(argument) =
        if isNull (box this.target) then raise (InvalidOperationException("There is no active SQL parameter writer."))
        this.target.Ordinal(argument, this.session)
    [<System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)>]
    member this.Int64(argument: int, value: int64) =
        let ordinal = this.Ordinal(argument)
        if ordinal > 0 then this.target.Check(raw.sqlite3_bind_int64(this.target.Statement, ordinal, value))
    member this.Int32(argument: int, value: int) = this.Int64(argument, int64 value)
    member this.Boolean(argument: int, value: bool) = this.Int64(argument, if value then 1L else 0L)
    member this.Double(argument: int, value: double) =
        let ordinal = this.Ordinal(argument)
        if ordinal > 0 then this.target.Check(NativeParameterBinding.real this.target.Statement ordinal value)
    member this.Text(argument: int, value: string) =
        let ordinal = this.Ordinal(argument)
        if ordinal > 0 then
            this.target.Check(if isNull value then raw.sqlite3_bind_null(this.target.Statement, ordinal)
                              else NativeParameterBinding.text this.target.Statement ordinal -1 value)
    member this.Blob(argument: int, value: byte[], length: int) =
        let ordinal = this.Ordinal(argument)
        if ordinal > 0 then
            this.target.Check(if isNull value then raw.sqlite3_bind_null(this.target.Statement, ordinal)
                              else NativeParameterBinding.blob this.target.Statement ordinal length value)
    member this.Blob(argument: int, value: byte[]) = this.Blob(argument, value, if isNull value then 0 else value.Length)
    member internal this.Raw(argument: int, value: obj, size: int, sqliteType: int) =
        let value = NativeParameterBinding.rawStorageValue value sqliteType
        let ordinal = this.Ordinal(argument)
        if ordinal > 0 then this.target.Check(NativeParameterBinding.bindRaw this.target.Statement ordinal size value)
    member internal this.Raw(argument: int, value: obj, size: int) = this.Raw(argument, value, size, 0)
    member this.Null(argument: int) =
        let ordinal = this.Ordinal(argument)
        if ordinal > 0 then this.target.Check(raw.sqlite3_bind_null(this.target.Statement, ordinal))
    member this.DateTimeOffset(argument: int, value: DateTimeOffset) = this.Int64(argument, value.ToUnixTimeMilliseconds())
    member this.DateTime(argument: int, value: DateTime) = this.Text(argument, NativeValueConversion.dateTimeText value)
    member this.Guid(argument: int, value: Guid) = this.Text(argument, value.ToString().ToUpperInvariant())
    member this.Decimal(argument: int, value: decimal) = this.Text(argument, value.ToString("0.0###########################", CultureInfo.InvariantCulture))

/// A stable binding shape with typed acquisition and typed captured state.
/// Capture runs once when execution starts; every statement binds from that same state.
[<Sealed>]
type SqlParameterPlan<'Input, 'State> private (names: string[], capture: Func<'Input, 'State>, reusableCapture: Func<'Input, 'State, 'State>, bind: Action<SqlParameterWriter, 'State>, clear: Action<'State>, operationErrors: bool) =
    do
        if isNull names then nullArg "names"
        if isNull capture && isNull reusableCapture then nullArg "capture"
        if isNull bind then nullArg "bind"
    let layout =
        if operationErrors then NativeOperationArgumentLayout(Array.copy names) :> NativeArgumentLayout
        else NativeArgumentLayout(Array.copy names)
    let validNames = not operationErrors || not (Array.exists String.IsNullOrEmpty layout.Names)
    new(names: string[], capture: Func<'Input, 'State>, bind: Action<SqlParameterWriter, 'State>) =
        SqlParameterPlan(names, capture, null, bind, null, false)
    internal new(names: string[], capture: Func<'Input, 'State, 'State>, bind: Action<SqlParameterWriter, 'State>, clear: Action<'State>) =
        SqlParameterPlan(names, null, capture, bind, clear, false)
    internal new(names: string[], capture: Func<'Input, 'State, 'State>, bind: Action<SqlParameterWriter, 'State>, clear: Action<'State>, operationErrors: bool) =
        SqlParameterPlan(names, null, capture, bind, clear, operationErrors)
    member internal _.Layout =
        if not validNames then raise (InvalidOperationException("ParameterName must be set."))
        layout
    member internal _.Capture(input: 'Input, previous: 'State) =
        if isNull reusableCapture then capture.Invoke(input) else reusableCapture.Invoke(input, previous)
    member internal _.Clear(state: 'State) =
        if isNull clear then Unchecked.defaultof<'State>
        else clear.Invoke(state); state
    member internal _.Bind(writer, state: 'State) = bind.Invoke(writer, state)

[<AllowNullLiteral>]
type internal INativeArgumentBinding =
    abstract Layout: NativeArgumentLayout
    abstract Bind: NativeBindingTarget -> unit
    abstract Clear: unit -> unit

/// Compiled invocations already own their argument arrays. Borrow them until the
/// statement finishes instead of copying each value through dynamic capture.
[<Sealed>]
type internal NativeCompiledArguments() =
    let mutable constants: KeyValuePair<string, obj>[] = Array.empty
    let mutable values: obj[] = Array.empty
    let mutable offset = 0
    let mutable count = 0
    let mutable layout = NativeArgumentLayout(Array.empty)
    member _.Capture(packet: ParameterValues) =
        let previous = layout.Names
        let mutable same = previous.Length = packet.Constants.Length + packet.Names.Length
        let mutable i = 0
        while same && i < packet.Constants.Length do
            same <- previous.[i] = packet.Constants.[i].Key
            i <- i + 1
        let mutable j = 0
        while same && j < packet.Names.Length do
            same <- previous.[packet.Constants.Length + j] = packet.Names.[j]
            j <- j + 1
        if not same then layout <- NativeArgumentLayout(Array.append (packet.Constants |> Array.map _.Key) packet.Names)
        constants <- packet.Constants
        values <- packet.Values
        offset <- packet.Offset
        count <- packet.Names.Length
    interface INativeArgumentBinding with
        member _.Layout = layout
        member _.Bind(target) =
            for i = 0 to constants.Length - 1 do
                let ordinal = target.Ordinal(i, target.Session)
                if ordinal > 0 then target.Check(NativeParameterBinding.bind target.Statement ordinal constants.[i].Value)
            for i = 0 to count - 1 do
                let ordinal = target.Ordinal(constants.Length + i, target.Session)
                if ordinal > 0 then target.Check(NativeParameterBinding.bind target.Statement ordinal values.[offset + i])
        member _.Clear() =
            constants <- Array.empty
            values <- Array.empty
            offset <- 0
            count <- 0

/// The borrowed statement owns typed captured state; value-type states are never boxed.
[<Sealed>]
type internal SqlParameterCapture<'Input, 'State>(plan: SqlParameterPlan<'Input, 'State>) =
    let mutable state = Unchecked.defaultof<'State>
    member _.Plan = plan
    member _.Capture(input) = state <- plan.Capture(input, state)
    interface INativeArgumentBinding with
        member _.Layout = plan.Layout
        member _.Bind(target) =
            plan.Bind(SqlParameterWriter(target), state)
        member _.Clear() =
            state <- plan.Clear(state)
