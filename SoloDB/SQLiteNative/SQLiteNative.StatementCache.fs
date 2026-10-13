#nowarn "9"
namespace SoloDatabase

open System
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.CompilerServices
open System.Threading
open Microsoft.FSharp.NativeInterop
open SQLitePCL

/// Compiled plans retain weak references; the physical connection owns all statements.
[<Sealed>]
type internal RetainedPreparedHandle(sql: string) =
    let cacheEligible = not (isNull sql) && not (sql.Contains "@VAR")
    let entries = ConditionalWeakTable<NativeDatabase, WeakReference<NativePreparedStatement>>()
    member _.Sql = sql
    member _.CacheEligible = cacheEligible
    member _.TryGet(database: NativeDatabase) =
        let mutable weak = Unchecked.defaultof<WeakReference<NativePreparedStatement>>
        let mutable statement = Unchecked.defaultof<NativePreparedStatement>
        if entries.TryGetValue(database, &weak) && weak.TryGetTarget(&statement) then ValueSome statement else ValueNone
    member _.Remember(database, statement) =
        entries.GetValue(database, fun _ -> WeakReference<NativePreparedStatement>(statement)).SetTarget(statement)

/// One SQL origin supplies text, prepared bytes and immutable cache eligibility.
[<Struct>]
type internal NativeStatementSource =
    | Text of text: string
    | Retained of retained: RetainedPreparedHandle
    | Prepared of prepared: NativePreparedStatement
    member this.Sql =
        match this with
        | Text text -> text
        | Retained retained -> retained.Sql
        | Prepared prepared -> prepared.Sql
    member this.Bytes =
        match this with
        | Prepared prepared -> prepared.Bytes
        | _ -> null
    member this.CacheEligible =
        match this with
        | Text text -> not (text.Contains "@VAR")
        | Retained retained -> retained.CacheEligible
        | Prepared prepared -> prepared.CacheEligible
    member this.RetainedHandle =
        match this with Retained retained -> ValueSome retained | _ -> ValueNone

/// Fixed statements and script segments share one cache and one native preparation path.
[<Sealed>]
type internal NativeStatementCache(database: NativeDatabase) =
    let entries = Dictionary<struct (string * int), NativePreparedStatement>()
    let gate = obj()
    let mutable created = 0
    let mutable disposed = 0
    let mutable configuration: Types.SoloDBConfiguration = { CachingEnabled = true }
    let mutable activeBorrows = 0
    let mutable schemaProbeCount = 0
    let mutable schemaProbeBorrows = 0L
    let dispose (statement: NativePreparedStatement) =
        try statement.Dispose()
        finally if statement.Cached then disposed <- disposed + 1
    let retire (statement: NativePreparedStatement) =
        statement.Retired <- true
        if not statement.InUse then dispose statement
    member _.Database = database
    member _.BeginSchemaProbe() = lock gate (fun () -> schemaProbeCount <- schemaProbeCount + 1; schemaProbeBorrows)
    member _.EndSchemaProbe() = lock gate (fun () -> schemaProbeCount <- schemaProbeCount - 1)
    member _.SchemaProbeBorrows = lock gate (fun () -> schemaProbeBorrows)
    member _.HasActiveBorrows = lock gate (fun () -> activeBorrows > 0)
    member _.UseConfiguration(value: Types.SoloDBConfiguration) = configuration <- value
    member _.Enabled
        with get() = Volatile.Read(&configuration.CachingEnabled)
        and set value = Volatile.Write(&configuration.CachingEnabled, value)
    member _.Snapshot = lock gate (fun () -> struct (entries.Count, created, disposed))
    member _.Acquire(source: NativeStatementSource, offset: int, budget: byref<NativeExecutionBudget>) =
        let sql = source.Sql
        if isNull sql then nullArg "sql"
        let retained = source.RetainedHandle
        database.EnsureOpen()
        // The timeout budget is passed by reference through preparation, so it cannot be captured by lock's closure.
        Monitor.Enter(gate)
        try
            let key = struct (sql, offset)
            let enabled = Volatile.Read(&configuration.CachingEnabled)
            let eligible = source.CacheEligible
            let cacheable = enabled && eligible
            let remembered =
                match retained with
                | ValueSome handle when cacheable && offset = 0 ->
                    match handle.TryGet(database) with
                    | ValueSome statement when not statement.Retired && statement.Generation = database.Generation -> ValueSome statement
                    | _ -> ValueNone
                | _ -> ValueNone
            let available =
                match remembered with
                | ValueSome statement when not statement.InUse -> ValueSome statement
                | _ ->
                    let cached =
                        match entries.TryGetValue(key) with
                        | true, old when old.Generation <> database.Generation ->
                            entries.Remove(key) |> ignore
                            retire old
                            ValueNone
                        | true, statement -> ValueSome statement
                        | _ -> ValueNone
                    match cached with
                    | ValueSome statement when cacheable && not statement.InUse -> ValueSome statement
                    | _ -> ValueNone
            let statement =
                match available with
                | ValueSome statement -> statement
                | _ ->
                    let sourceBytes = source.Bytes
                    let bytes = if isNull sourceBytes then utf8z.GetZeroTerminatedUTF8Bytes(sql) else sourceBytes
                    let length = Array.IndexOf(bytes, 0uy)
                    let mutable position = offset
                    let mutable handle: sqlite3_stmt = null
                    try
                        while position < length && (isNull handle || handle.IsInvalid) do
                            if not (isNull handle) then handle.Dispose()
                            use pinned = fixed bytes
                            let remaining = utf8z.FromSpan(ReadOnlySpan(bytes, position, length - position + 1))
                            let mutable tail = utf8z.FromSpan(ReadOnlySpan<byte>.Empty)
                            let started = Stopwatch.GetTimestamp()
                            let mutable result = raw.sqlite3_prepare_v2(database.Handle, remaining, &handle, &tail)
                            while NativeExecution.isLocked result && budget.CanRetry(started) do
                                if not (isNull handle) then handle.Dispose()
                                NativeExecution.wait()
                                result <- raw.sqlite3_prepare_v2(database.Handle, remaining, &handle, &tail)
                            budget.Record(started)
                            if result <> raw.SQLITE_OK then raise (NativeSqliteException.FromResult(database.Handle, result))
                            let pointer = Unsafe.AsPointer(&Unsafe.AsRef(&tail.GetPinnableReference())) |> NativePtr.ofVoidPtr<byte> |> NativePtr.toNativeInt
                            let consumed = int (pointer - NativePtr.toNativeInt pinned) - position
                            if consumed <= 0 then raise (InvalidOperationException("SQLite preparation did not consume input."))
                            position <- position + consumed
                        let entry = new NativePreparedStatement(database, handle, sql, bytes, offset, position, eligible)
                        if cacheable && not (entries.ContainsKey(key)) then
                            if entries.Count >= 1000 then
                                let oldest = entries |> Seq.toArray
                                Array.sortInPlaceBy (fun (pair: KeyValuePair<struct (string * int), NativePreparedStatement>) -> pair.Value.Calls) oldest
                                for i = 0 to 249 do
                                    let pair = oldest.[i]
                                    entries.Remove(pair.Key) |> ignore
                                    retire pair.Value
                            entry.Cached <- true
                            entries.Add(key, entry)
                            created <- created + 1
                        else entry.Retired <- true
                        entry
                    with error ->
                        if not (isNull handle) then handle.Dispose()
                        error.Data.["Statement"] <- System.Text.Encoding.UTF8.GetString(bytes, offset, min 200 (length - offset)).Trim()
                        reraise()
            if schemaProbeCount > 0 then schemaProbeBorrows <- schemaProbeBorrows + 1L
            activeBorrows <- activeBorrows + 1
            statement.InUse <- true
            statement.Borrow <- statement.Borrow + 1L
            statement.Calls <- statement.Calls + 1L
            match retained, remembered with
            | ValueSome _, ValueSome previous when Object.ReferenceEquals(previous, statement) -> ()
            | ValueSome handle, _ when statement.Cached && offset = 0 -> handle.Remember(database, statement)
            | _ -> ()
            statement
        finally Monitor.Exit(gate)
    member _.Release(statement: NativePreparedStatement, borrow: int64) =
        lock gate (fun () ->
            if statement.InUse && statement.Borrow = borrow then
                activeBorrows <- activeBorrows - 1
                statement.InUse <- false
                statement.ClearCapture()
                if statement.Retired then dispose statement)
    member _.Clear() =
        lock gate (fun () ->
            let previous = entries.Values |> Seq.toArray
            entries.Clear()
            let mutable failure: exn = null
            for statement in previous do
                try retire statement with error -> if isNull failure then failure <- error
            if not (isNull failure) then Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw())
