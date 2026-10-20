#nowarn "9"
namespace SoloDatabase

open System
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
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

/// Unpublished preparation owns only its original input and current native attempt.
[<Struct>]
type private NativePreparation =
    val Source: NativeStatementSource
    val Offset: int
    val Generation: int64
    val mutable Sql: string
    val mutable Bytes: byte[]
    val mutable Length: int
    val mutable Position: int
    val mutable Handle: sqlite3_stmt
    val mutable Initialized: bool
    val mutable Cacheable: bool
    val mutable Eligible: bool
    val mutable Remembered: NativePreparedStatement voption
    val mutable Retained: RetainedPreparedHandle voption
    val mutable Preparing: bool
    val mutable Started: int64
    new(source, offset, generation) =
        { Source = source; Offset = offset; Generation = generation; Sql = null; Bytes = null
          Length = 0; Position = offset; Handle = null; Initialized = false
          Cacheable = false; Eligible = false; Remembered = ValueNone; Retained = ValueNone
          Preparing = false; Started = 0L }
    member this.Dispose() =
        if not (isNull this.Handle) then
            this.Handle.Dispose()
            this.Handle <- null

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
    member private _.Borrow(statement: NativePreparedStatement, preparation: byref<NativePreparation>) =
        if schemaProbeCount > 0 then schemaProbeBorrows <- schemaProbeBorrows + 1L
        activeBorrows <- activeBorrows + 1
        statement.InUse <- true
        statement.Borrow <- statement.Borrow + 1L
        statement.Calls <- statement.Calls + 1L
        match preparation.Retained, preparation.Remembered with
        | ValueSome _, ValueSome previous when Object.ReferenceEquals(previous, statement) -> ()
        | ValueSome retained, _ when statement.Cached && preparation.Offset = 0 -> retained.Remember(database, statement)
        | _ -> ()
        statement
    member private this.PrepareLocked(preparation: byref<NativePreparation>, budget: byref<NativeExecutionBudget>) =
        let mutable blocked = false
        while not blocked && preparation.Position < preparation.Length && (isNull preparation.Handle || preparation.Handle.IsInvalid) do
            preparation.Dispose()
            let position = preparation.Position
            use pinned = fixed preparation.Bytes
            let remaining = utf8z.FromSpan(ReadOnlySpan(preparation.Bytes, position, preparation.Length - position + 1))
            let mutable tail = utf8z.FromSpan(ReadOnlySpan<byte>.Empty)
            if not preparation.Preparing then
                preparation.Started <- Stopwatch.GetTimestamp()
                preparation.Preparing <- true
            let result = raw.sqlite3_prepare_v2(database.Handle, remaining, &preparation.Handle, &tail)
            database.EnsureGeneration(preparation.Generation)
            if NativeExecution.isLocked result then
                if not (budget.CanRetry(preparation.Started)) then
                    raise (NativeSqliteException.FromResult(database.Handle, result))
                preparation.Dispose()
                blocked <- true
            elif result <> raw.SQLITE_OK then
                raise (NativeSqliteException.FromResult(database.Handle, result))
            else
                let pointer = Unsafe.AsPointer(&Unsafe.AsRef(&tail.GetPinnableReference())) |> NativePtr.ofVoidPtr<byte> |> NativePtr.toNativeInt
                let consumed = int (pointer - NativePtr.toNativeInt pinned) - position
                if consumed <= 0 then raise (InvalidOperationException("SQLite preparation did not consume input."))
                preparation.Position <- position + consumed
        if blocked then ValueNone else
        let entry = new NativePreparedStatement(database, preparation.Handle, preparation.Sql, preparation.Bytes, preparation.Offset, preparation.Position, preparation.Eligible)
        let key = struct (preparation.Sql, preparation.Offset)
        if preparation.Cacheable && not (entries.ContainsKey(key)) then
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
        let owned = this.Borrow(entry, &preparation)
        preparation.Handle <- null
        ValueSome owned
    member private this.TryAcquireLocked(preparation: byref<NativePreparation>, budget: byref<NativeExecutionBudget>) =
        let sql = if preparation.Initialized then preparation.Sql else preparation.Source.Sql
        if isNull sql then nullArg "sql"
        database.EnsureGeneration(preparation.Generation)
        if preparation.Initialized then this.PrepareLocked(&preparation, &budget) else
        preparation.Sql <- sql
        preparation.Retained <- preparation.Source.RetainedHandle
        preparation.Eligible <- preparation.Source.CacheEligible
        preparation.Cacheable <- Volatile.Read(&configuration.CachingEnabled) && preparation.Eligible
        let key = struct (sql, preparation.Offset)
        preparation.Remembered <-
            match preparation.Retained with
            | ValueSome retained when preparation.Cacheable && preparation.Offset = 0 ->
                match retained.TryGet(database) with
                | ValueSome statement when not statement.Retired && statement.Generation = preparation.Generation -> ValueSome statement
                | _ -> ValueNone
            | _ -> ValueNone
        let available =
            match preparation.Remembered with
            | ValueSome statement when not statement.InUse -> ValueSome statement
            | _ ->
                let cached =
                    match entries.TryGetValue(key) with
                    | true, old when old.Generation <> preparation.Generation ->
                        entries.Remove(key) |> ignore
                        retire old
                        ValueNone
                    | true, statement -> ValueSome statement
                    | _ -> ValueNone
                match cached with
                | ValueSome statement when preparation.Cacheable && not statement.InUse -> ValueSome statement
                | _ -> ValueNone
        match available with
        | ValueSome statement -> ValueSome(this.Borrow(statement, &preparation))
        | ValueNone ->
            let sourceBytes = preparation.Source.Bytes
            preparation.Bytes <- if isNull sourceBytes then utf8z.GetZeroTerminatedUTF8Bytes(sql) else sourceBytes
            preparation.Length <- Array.IndexOf(preparation.Bytes, 0uy)
            preparation.Initialized <- true
            this.PrepareLocked(&preparation, &budget)
    member private _.PreparationFailed(preparation: byref<NativePreparation>, budget: byref<NativeExecutionBudget>, error: exn) =
        if preparation.Preparing then budget.Record(preparation.Started)
        preparation.Dispose()
        if not (isNull preparation.Bytes) then
            error.Data.["Statement"] <- System.Text.Encoding.UTF8.GetString(preparation.Bytes, preparation.Offset, min 200 (preparation.Length - preparation.Offset)).Trim()
    member private _.CompletePreparation(preparation: byref<NativePreparation>, budget: byref<NativeExecutionBudget>, statement: NativePreparedStatement) =
        if preparation.Preparing then budget.Record(preparation.Started)
        statement
    member this.Acquire(source: NativeStatementSource, offset: int, budget: byref<NativeExecutionBudget>) =
        let mutable preparation = NativePreparation(source, offset, database.Generation)
        Monitor.Enter(gate)
        try
            try
                let mutable result = this.TryAcquireLocked(&preparation, &budget)
                while result.IsNone do
                    NativeExecution.wait()
                    result <- this.TryAcquireLocked(&preparation, &budget)
                this.CompletePreparation(&preparation, &budget, result.Value)
            with error -> this.PreparationFailed(&preparation, &budget, error); reraise()
        finally Monitor.Exit(gate)
    member private this.TryAcquire(preparation: byref<NativePreparation>, budget: byref<NativeExecutionBudget>) =
        Monitor.Enter(gate)
        try this.TryAcquireLocked(&preparation, &budget)
        finally Monitor.Exit(gate)
    member private this.WaitAcquireAsync(pending: NativePreparation, initialBudget: NativeExecutionBudget) = task {
        let mutable preparation = pending
        let mutable budget = initialBudget
        try
            let mutable result = ValueNone
            while result.IsNone do
                do! NativeExecution.waitAsync()
                result <- this.TryAcquire(&preparation, &budget)
            let statement = this.CompletePreparation(&preparation, &budget, result.Value)
            return struct (statement, budget)
        with error ->
            this.PreparationFailed(&preparation, &budget, error)
            return (System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw(); Unchecked.defaultof<_>)
    }
    member this.AcquireAsync(source: NativeStatementSource, offset: int, budget: NativeExecutionBudget, generation: int64) =
        let mutable preparation = NativePreparation(source, offset, generation)
        let mutable budget = budget
        try
            let result = this.TryAcquire(&preparation, &budget)
            match result with
            | ValueSome _ ->
                let statement = this.CompletePreparation(&preparation, &budget, result.Value)
                ValueTask<struct (NativePreparedStatement * NativeExecutionBudget)>(struct (statement, budget))
            | ValueNone -> ValueTask<struct (NativePreparedStatement * NativeExecutionBudget)>(this.WaitAcquireAsync(preparation, budget))
        with error ->
            this.PreparationFailed(&preparation, &budget, error)
            ValueTask<struct (NativePreparedStatement * NativeExecutionBudget)>(Task.FromException<struct (NativePreparedStatement * NativeExecutionBudget)>(error))
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
