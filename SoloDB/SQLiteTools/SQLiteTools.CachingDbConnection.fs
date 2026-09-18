namespace SoloDatabase

open System
open System.Collections.Generic
open System.Data
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Microsoft.Data.Sqlite
open System.Data.Common
open SQLiteToolsParams
open SQLiteToolsMapper

module SQLiteTools =
    open SQLiteToolsHandlerFaultState

    type internal PreparedCacheEntry = {
        Command: SqliteCommand
        ColumnDict: Dictionary<string, int>
        CallCount: int64 ref
        InUse: bool ref
        HasParameters: bool ref
        mutable Retired: bool
        mutable BindingOwner: obj
        mutable BindingSlots: SqliteParameter array
    }

    /// A compiled query remembers entries weakly; the connection cache owns the commands.
    type internal RetainedPreparedHandle(sql: string) =
        let entries = ConditionalWeakTable<SqliteConnection, WeakReference<PreparedCacheEntry>>()
        member _.Sql = sql
        member _.TryGet(connection: SqliteConnection) =
            let mutable weak = Unchecked.defaultof<WeakReference<PreparedCacheEntry>>
            let mutable entry = Unchecked.defaultof<PreparedCacheEntry>
            if entries.TryGetValue(connection, &weak) && weak.TryGetTarget(&entry) then ValueSome entry else ValueNone
        member _.Remember(connection: SqliteConnection, entry: PreparedCacheEntry) =
            let weak = entries.GetValue(connection, fun _ -> WeakReference<PreparedCacheEntry>(entry))
            weak.SetTarget(entry)

    /// <summary>
    /// A sealed wrapper around SqliteConnection that adds command caching capabilities.
    /// </summary>
    /// <param name="connectionStr">The connection string.</param>
    /// <param name="onDispose">A callback function to execute on disposal.</param>
    /// <param name="config">The database configuration.</param>
    type [<Sealed>] CachingDbConnection internal (connectionStr: string, onDispose, config: Types.SoloDBConfiguration, onEnterEventHandlerScope: unit -> unit, onExitEventHandlerScope: unit -> unit) =
        inherit SqliteConnection(connectionStr)
        let mutable preparedCache = Dictionary<string, PreparedCacheEntry>()
        let mutable preparedCreated = 0
        let mutable preparedDisposed = 0
        let maxCacheSize = 1000
        // Connection-level reader-active guard to prevent indefinite hang from overlapping readers.
        let mutable readerActive = false
        // Serializes mutations of preparedCache and of the SqliteConnection-owned internal command list
        // (which CreateCommand appends to and SqliteCommand.Dispose removes from). Without this lock,
        // concurrent QueryFirst/CreateCommand on one thread races with ClearCache/Command.Dispose on
        // another, causing an NRE inside SqliteConnection.RemoveCommand under Release-tightened timing.
        let cacheLock = obj()

        // SQLITE_SCHEMA compensation: Microsoft.Data.Sqlite throws ArgumentOutOfRangeException
        // from PrepareAndEnumerateStatements when concurrent DDL invalidates the schema cache.
        // SQLite re-prepares on SQLITE_SCHEMA internally; the .NET wrapper does not. Under
        // tight cross-connection DDL bursts on the same DB file a single retry is
        // insufficient — bounded loop of up to 8 attempts with geometric backoff so a
        // competing DDL can complete its schema flush between attempts.
        let prepareWithSchemaRetry (command: SqliteCommand) =
            // Microsoft.Data.Sqlite's PrepareAndEnumerateStatements surfaces transient
            // ArgumentOutOfRangeException when concurrent DDL invalidates the schema
            // cache mid-prepare. SQLite itself recovers via SQLITE_SCHEMA, but the
            // managed wrapper does not retry. We do — 8 attempts with geometric backoff
            // (1, 2, 4, 8, 16, 32 ms tail) so the wait outlasts a typical DDL burst.
            let mutable attempt = 0
            let mutable lastExn : ArgumentOutOfRangeException = null
            let mutable ok = false
            let maxAttempts = 8
            while not ok && attempt < maxAttempts do
                try
                    command.Prepare()
                    ok <- true
                with :? ArgumentOutOfRangeException as ex ->
                    lastExn <- ex
                    attempt <- attempt + 1
                    if attempt < maxAttempts then
                        // First retry: yield only. Subsequent: true geometric backoff
                        // (1, 2, 4, 8, 16, 32 ms; total tail ~63 ms across 7 retries)
                        // so the wait outlasts a typical DDL burst that triggered the
                        // SQLITE_SCHEMA cross-connection invalidation.
                        if attempt = 1 then Threading.Thread.Sleep(0)
                        else Threading.Thread.Sleep(1 <<< (attempt - 2))
            if not ok then raise lastExn

        let retireEntry (entry: PreparedCacheEntry) =
            entry.Retired <- true
            if not !entry.InUse then
                entry.Command.Dispose()
                preparedDisposed <- preparedDisposed + 1

        let releaseEntry (entry: PreparedCacheEntry) =
            lock cacheLock (fun () ->
                if !entry.InUse then
                    entry.InUse := false
                    if entry.Retired then
                        entry.Command.Dispose()
                        preparedDisposed <- preparedDisposed + 1)

        let bindRetained (entry: PreparedCacheEntry) (handle: RetainedPreparedHandle) (packet: ParameterValues) =
            let count = packet.Constants.Length + packet.Names.Length
            if not (Object.ReferenceEquals(entry.BindingOwner, handle)) then
                // Another caller may have changed or removed named parameters. Rebuild this
                // handle's references once, then use the same objects on subsequent calls.
                if entry.HasParameters.Value then
                    for i = 0 to entry.Command.Parameters.Count - 1 do
                        entry.Command.Parameters.[i].Value <- null
                for pair in packet.Constants do
                    setOrAddParameter entry.Command pair.Key pair.Value
                for i = 0 to packet.Names.Length - 1 do
                    setOrAddParameter entry.Command packet.Names.[i] packet.Values.[i]
                for i = entry.Command.Parameters.Count - 1 downto 0 do
                    if isNull entry.Command.Parameters.[i].Value then
                        entry.Command.Parameters.RemoveAt i
                entry.BindingSlots <- Array.init count (fun i ->
                    let name = if i < packet.Constants.Length then packet.Constants.[i].Key
                               else packet.Names.[i - packet.Constants.Length]
                    entry.Command.Parameters.[name])
                entry.BindingOwner <- handle
            else
                for i = 0 to packet.Constants.Length - 1 do
                    setRetainedParameterValue entry.BindingSlots.[i] packet.Constants.[i].Value
                for i = 0 to packet.Names.Length - 1 do
                    setRetainedParameterValue entry.BindingSlots.[packet.Constants.Length + i] packet.Values.[i]
            entry.HasParameters.Value <- count > 0
            count

        let tryCachedCommand (this: CachingDbConnection) (sql: string) (parameters: obj) (retained: RetainedPreparedHandle voption) =
            match retained with
            | ValueSome handle when handle.Sql <> sql ->
                invalidArg "retained" "The retained prepared handle does not match the statement."
            | _ -> ()
            // @VAR variable names are randomly generated, so caching them is not possible.
            if sql.Contains "@VAR" then struct(ValueNone, parameters) else
            if not config.CachingEnabled then
                struct(ValueNone, parameters)
            else
            lock cacheLock (fun () ->

            // Delete from cache 1/4 of the least used commands.
            if preparedCache.Count >= maxCacheSize then
                let arr = preparedCache |> Seq.toArray
                arr |> Array.sortInPlaceBy (fun (KeyValue(_sql, item)) -> !item.CallCount)

                for i in 0..(maxCacheSize / 4 - 1) do
                    preparedCache.Remove (arr.[i].Key) |> ignore
                    retireEntry arr.[i].Value


            let struct(item, needsBinding, remembered) =
                let retainedEntry =
                    match retained with
                    | ValueSome handle ->
                        match handle.TryGet(this :> SqliteConnection) with
                        | ValueSome entry when not entry.Retired -> ValueSome entry
                        | _ -> ValueNone
                    | ValueNone -> ValueNone
                match retainedEntry with
                | ValueSome entry -> struct(entry, true, true)
                | ValueNone ->
                  match preparedCache.TryGetValue sql with
                  | true, x -> struct(x, true, false)
                  | false, _ ->
                    let command = this.CreateCommand()
                    try
                        command.CommandText <- sql
                        let count = processParameters addParameter command parameters
                        prepareWithSchemaRetry command

                        let item = {
                            Command = command; ColumnDict = Dictionary<string, int>()
                            CallCount = ref 0L; InUse = ref false; HasParameters = ref (count > 0)
                            Retired = false; BindingOwner = null; BindingSlots = [||]
                        }
                        preparedCache.[sql] <- item
                        preparedCreated <- preparedCreated + 1
                        struct(item, false, false)
                    with _ ->
                        command.Dispose()
                        reraise()

            if !item.InUse then struct(ValueNone, parameters) else

            item.CallCount := !item.CallCount + 1L
            item.InUse := true
            if not remembered then
                match retained with
                | ValueSome handle -> handle.Remember(this :> SqliteConnection, item)
                | ValueNone -> ()

            try
                let mutable emptyRebind = false
                if needsBinding then
                    match retained, parameters with
                    | ValueSome handle, (:? ParameterValues as packet) ->
                        if packet.Constants.Length + packet.Names.Length = 0 && item.HasParameters.Value then
                            item.BindingOwner <- null
                            emptyRebind <- true
                        else
                            bindRetained item handle packet |> ignore
                    | _ ->
                        item.BindingOwner <- null
                        // Do not initialize the provider's lazy parameter collection for
                        // parameterless calls: that also changes its missing-value errors.
                        if item.HasParameters.Value then
                            let parameters = item.Command.Parameters
                            for i = 0 to parameters.Count - 1 do
                                parameters.[i].Value <- null
                        let count = processParameters setOrAddParameter item.Command parameters
                        emptyRebind <- count = 0 && item.HasParameters.Value
                        if count > 0 then
                            item.HasParameters.Value <- true
                            // DBNull is a supplied SQL NULL. Remove only omitted values
                            // and leave missing-name reporting with the provider.
                            for i = item.Command.Parameters.Count - 1 downto 0 do
                                if isNull item.Command.Parameters.[i].Value then
                                    item.Command.Parameters.RemoveAt i
                if emptyRebind then
                    releaseEntry item
                    // The argument source was consumed once and supplied no values.
                    // Use a fresh parameterless command, without enumerating it again.
                    struct(ValueNone, null)
                else
                    match sqlTraceCallback with ValueSome cb -> cb.Invoke(sql) | ValueNone -> ()
                    // Reported after binding, so a caller can see what a cached statement actually ran with.
                    match sqlBoundTraceCallback with
                    | ValueSome cb ->
                        let bound = ResizeArray<KeyValuePair<string, obj>>(item.Command.Parameters.Count)
                        for i in 0 .. item.Command.Parameters.Count - 1 do
                            let p = item.Command.Parameters.[i]
                            bound.Add(KeyValuePair(p.ParameterName, p.Value))
                        cb.Invoke(sql, bound :> IReadOnlyList<KeyValuePair<string, obj>>)
                    | ValueNone -> ()
                    struct(ValueSome item, parameters)
            with _ ->
                // A throwing getter may have added a prefix of the arguments.
                if needsBinding then item.HasParameters.Value <- true
                item.BindingOwner <- null
                releaseEntry item
                reraise())

        // Uncached command lifecycle helpers. The SqliteConnection internal command list is
        // appended to by CreateCommand and removed from by SqliteCommand.Dispose. Both sides
        // must run under cacheLock to stay race-free with ClearCache and the cached path.
        let createUncachedLocked (this: CachingDbConnection) (sql: string) (parameters: obj) : SqliteCommand * IDisposable =
            let command =
                lock cacheLock (fun () ->
                    let c = createCommand this sql parameters
                    try
                        prepareWithSchemaRetry c
                        c
                    with _ ->
                        c.Dispose()
                        reraise())
            let guard =
                { new IDisposable with
                    member _.Dispose() = lock cacheLock (fun () -> command.Dispose()) }
            command, guard
    
        // Per-connection event-handler depth counter for savepoint suppression.
        // Tracks how many nested handler invocations are active on THIS connection.
        // Strict Enter/Exit balance — no negative clamping (negative depth = bug signal).
        let mutable eventHandlerDepth = 0
        let eventDispatchPendingRemovals = ResizeArray<obj * obj>()
        let mutable eventDispatchDepth = 0
    
        /// <summary>The underlying SqliteConnection.</summary>
        member internal this.Inner = this :> SqliteConnection
        member internal _.PreparedCacheSnapshot =
            lock cacheLock (fun () -> struct (preparedCache.Count, preparedCreated, preparedDisposed))
        member internal this.HasManagedTransaction = not (isNull base.Transaction)
        /// <summary>Indicates if the connection is currently part of a transaction.</summary>
        member val InsideTransaction = false with get, set

        /// <summary>
        /// Set when this connection is known to be unfit for reuse — for example a schema migration
        /// whose cleanup failed, leaving state the pool cannot reason about. The pool disposes such a
        /// connection instead of probing it, so returning it cannot raise a second, unrelated failure
        /// over the one that made it unusable.
        /// </summary>
        member val Unusable = false with get, set
    
        member internal this.EnterEventHandlerScope() =
            Threading.Interlocked.Increment(&eventHandlerDepth) |> ignore
            onEnterEventHandlerScope()
    
        member internal this.ExitEventHandlerScope() =
            let rec decrementOrFail () =
                let snapshot = Threading.Volatile.Read(&eventHandlerDepth)
                if snapshot <= 0 then
                    if Threading.Interlocked.CompareExchange(&eventHandlerDepth, 0, snapshot) = snapshot then
                        raise (InvalidOperationException("Event handler scope underflow detected. ExitEventHandlerScope was called without a matching EnterEventHandlerScope."))
                    else
                        decrementOrFail ()
                else if Threading.Interlocked.CompareExchange(&eventHandlerDepth, snapshot - 1, snapshot) <> snapshot then
                    decrementOrFail ()
    
            decrementOrFail ()
            onExitEventHandlerScope()
    
        /// <summary>
        /// Returns true when this connection is currently executing inside a SQLite trigger callback.
        /// Used by savepoint suppression to avoid SAVEPOINT on active-statement connections.
        /// </summary>
        member internal this.IsInEventHandlerScope =
            Threading.Volatile.Read(&eventHandlerDepth) > 0
    
        member internal this.EventHandlerDepth =
            Threading.Volatile.Read(&eventHandlerDepth)
    
        member internal this.EnterEventDispatchScope() =
            eventDispatchDepth <- eventDispatchDepth + 1
    
        member internal this.ExitEventDispatchScope() =
            if eventDispatchDepth <= 0 then
                eventDispatchDepth <- 0
                raise (InvalidOperationException("Event dispatch scope underflow detected. ExitEventDispatchScope was called without a matching EnterEventDispatchScope."))
    
            eventDispatchDepth <- eventDispatchDepth - 1
    
        member internal this.EventDispatchDepth = eventDispatchDepth
        member internal this.EventDispatchPendingRemovals = eventDispatchPendingRemovals
    
        member internal this.IsEventDispatchStateClean =
            eventDispatchDepth = 0 && eventDispatchPendingRemovals.Count = 0
    
        member internal this.ResetEventDispatchState() =
            eventDispatchDepth <- 0
            eventDispatchPendingRemovals.Clear()
    
        /// <summary>
        /// Retires cached commands. Active readers finish before their commands are disposed.
        /// </summary>
        member this.ClearCache() =
            lock cacheLock (fun () ->
                let oldCache = preparedCache
                preparedCache <- Dictionary<string, PreparedCacheEntry>()
                for KeyValue(_, entry) in oldCache do retireEntry entry)
    
        member internal this.ReaderActive
            with get() = lock cacheLock (fun () -> readerActive)
            and set(v) = lock cacheLock (fun () -> readerActive <- v)
        member internal this.CheckNoActiveReader() =
            if lock cacheLock (fun () -> readerActive) then
                raise (InvalidOperationException("A data reader is already active on this connection. Close the existing reader before executing another command."))

        /// <summary>Executes a non-query SQL command, utilizing the cache if possible.</summary>
        /// <param name="sql">The SQL command text.</param>
        /// <param name="parameters">The parameters for the command.</param>
        /// <returns>The number of rows affected.</returns>
        member this.Execute(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            withHandlerFaultWrap (this :> SqliteConnection) (fun () ->
                this.CheckNoActiveReader()
                let struct(cached, parameters) = tryCachedCommand this sql parameters ValueNone
                match cached with
                | ValueSome item ->
                    try
                        item.Command.ExecuteNonQuery()
                    finally
                        releaseEntry item
                | ValueNone ->
                    let command, cmdGuard = createUncachedLocked this sql parameters
                    use _guard = cmdGuard
                    command.ExecuteNonQuery())

        /// <summary>Opens a data reader, utilizing the cache if possible.</summary>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="outReader">The output SqliteDataReader.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>An IDisposable to manage the lifetime of the reader and command.</returns>
        member this.OpenReader(sql: string, outReader: outref<SqliteDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            this.CheckNoActiveReader()
            let struct(cached, parameters) = tryCachedCommand this sql parameters ValueNone
            match cached with
            | ValueSome item ->
                try
                    let reader = item.Command.ExecuteReader()
                    let conn = this
                    let lease =
                        { new IDisposable with
                            member _.Dispose() =
                                conn.ReaderActive <- false
                                try
                                    reader.Dispose()
                                finally
                                    releaseEntry item }
                    try
                        raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                        outReader <- reader
                        lock cacheLock (fun () -> readerActive <- true)
                        lease
                    with ex ->
                        try
                            lease.Dispose()
                        with _ -> ()
                        tryRecordHandlerFault (this :> SqliteConnection) ex
                        reraise()
                with ex ->
                    releaseEntry item
                    tryRecordHandlerFault (this :> SqliteConnection) ex
                    reraise()
            | ValueNone ->
                let command, cmdGuard = createUncachedLocked this sql parameters
                try
                    let reader = command.ExecuteReader()
                    let conn = this
                    let lease =
                        { new IDisposable with
                            member _.Dispose() =
                                conn.ReaderActive <- false
                                try
                                    reader.Dispose()
                                finally
                                    cmdGuard.Dispose() }
                    try
                        raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                        outReader <- reader
                        lock cacheLock (fun () -> readerActive <- true)
                        lease
                    with ex ->
                        try
                            lease.Dispose()
                        with _ -> ()
                        tryRecordHandlerFault (this :> SqliteConnection) ex
                        reraise()
                with ex ->
                    try
                        cmdGuard.Dispose()
                    with _ -> ()
                    tryRecordHandlerFault (this :> SqliteConnection) ex
                    reraise()
    
        /// <summary>
        /// Executes a query and maps the results to a sequence of 'T, utilizing the cache if possible.
        /// Handler faults are checked once before command acquisition and again after full successful enumeration.
        /// </summary>
        /// <typeparam name="'T">The type to map results to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>A sequence of 'T objects. Callers that stop early observe the pre-check but not the post-enumeration check.</returns>
        member private this.QueryCore<'T>(sql: string, parameters: obj, retained: RetainedPreparedHandle voption) = recordEnumerationFaults (this :> SqliteConnection) (seq {
            this.CheckNoActiveReader()
            raiseIfHandlerFaultRecorded (this :> SqliteConnection)
            let struct(cached, parameters) = tryCachedCommand this sql parameters retained
            match cached with
            | ValueSome item ->
                try
                    yield! queryCommand<'T> item.Command item.ColumnDict
                    raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                finally
                    releaseEntry item
            | ValueNone ->
                let command, cmdGuard = createUncachedLocked this sql parameters
                use _guard = cmdGuard
                yield! queryCommand<'T> command null
                raiseIfHandlerFaultRecorded (this :> SqliteConnection)
        })

        member this.Query<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            this.QueryCore<'T>(sql, parameters, ValueNone)

        member internal this.QueryRetained<'T>(handle: RetainedPreparedHandle, parameters: ParameterValues) =
            this.QueryCore<'T>(handle.Sql, box parameters, ValueSome handle)
    
        /// <summary>Executes a query and returns the first result, utilizing the cache if possible.</summary>
        /// <typeparam name="'T">The type to map the result to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>The first 'T object from the result set.</returns>
        member this.QueryFirst<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            withHandlerFaultWrap (this :> SqliteConnection) (fun () ->
                this.CheckNoActiveReader()
                let struct(cached, parameters) = tryCachedCommand this sql parameters ValueNone
                match cached with
                | ValueSome item ->
                    try
                        queryCommand<'T> item.Command item.ColumnDict |> Seq.head
                    finally
                        releaseEntry item
                | ValueNone ->
                    let command, cmdGuard = createUncachedLocked this sql parameters
                    use _guard = cmdGuard
                    queryCommand<'T> command null |> Seq.head)
    
        /// <summary>Executes a query and returns the first result, or a default value if the sequence is empty, utilizing the cache if possible.</summary>
        /// <typeparam name="'T">The type to map the result to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>The first 'T object from the result set, or default.</returns>
        member this.QueryFirstOrDefault<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            withHandlerFaultWrap (this :> SqliteConnection) (fun () ->
                this.CheckNoActiveReader()
                let struct(cached, parameters) = tryCachedCommand this sql parameters ValueNone
                match cached with
                | ValueSome item ->
                    try
                        match queryCommand<'T> item.Command item.ColumnDict |> Seq.tryHead with
                        | Some x -> x
                        | None -> defaultOf<'T>()
                    finally releaseEntry item
                | ValueNone ->
                    let command, cmdGuard = createUncachedLocked this sql parameters
                    use _guard = cmdGuard
                    match queryCommand<'T> command null |> Seq.tryHead with
                    | Some x -> x
                    | None -> defaultOf<'T>())
    
        /// <summary>
        /// Executes a multi-mapping query, utilizing the cache if possible.
        /// Handler faults are checked once before command acquisition and again after full successful enumeration.
        /// </summary>
        /// <typeparam name="'T1">The type of the first object.</typeparam>
        /// <typeparam name="'T2">The type of the second object.</typeparam>
        /// <typeparam name="'TReturn">The return type after mapping.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="map">The function to map the two objects to the return type.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <param name="splitOn">The column name to split the results on.</param>
        /// <returns>A sequence of 'TReturn objects. Callers that stop early observe the pre-check but not the post-enumeration check.</returns>
        member this.Query<'T1, 'T2, 'TReturn>(sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) = recordEnumerationFaults (this :> SqliteConnection) (seq {
            this.CheckNoActiveReader()
            raiseIfHandlerFaultRecorded (this :> SqliteConnection)
            let struct (command, dict, uncachedGuard, cachedEntry) =
                let struct(cached, parameters) = tryCachedCommand this sql parameters ValueNone
                match cached with
                | ValueSome item ->
                    struct (item.Command, item.ColumnDict, (null: IDisposable), Some item)
                | ValueNone ->
                    let c, g = createUncachedLocked this sql parameters
                    struct (c, Dictionary<string, int>(), g, None)
            try
                use reader = command.ExecuteReader()

                if dict.Count = 0 then
                    for i in 0..(reader.FieldCount - 1) do
                        dict.Add(reader.GetName(i), i)

                let splitIndex = reader.GetOrdinal(splitOn)

                while reader.Read() do
                    let t1 = TypeMapper<'T1>.Map reader 0 dict
                    let t2 =
                        if reader.IsDBNull(splitIndex) then Unchecked.defaultof<'T2>
                        else TypeMapper<'T2>.Map reader splitIndex dict

                    yield map.Invoke (t1, t2)

                raiseIfHandlerFaultRecorded (this :> SqliteConnection)
            finally
                match cachedEntry with
                | Some item -> releaseEntry item
                | _ -> ()
                if not (isNull uncachedGuard) then uncachedGuard.Dispose()
        })
    
        /// <summary>
        /// Performs the actual disposal of the base connection.
        /// </summary>
        member this.DisposeReal() =
            base.Dispose(true)
    
        // Override Dispose(bool) to ensure TakeBack is always called on disposal,
        // regardless of whether Dispose() is called via IDisposable or base class dispatch.
        // Dispose is suppressed when InsideTransaction is true (set by WithTransactionBorrowed
        // and event handler paths). This prevents premature pool return when connection is used
        // inside a transaction via Transactional wrapping or event callbacks.
        override this.Dispose(disposing: bool) =
            if disposing && not this.InsideTransaction then
                onDispose this
    
        interface SQLiteToolsExtensions.ICachingDbConnectionOps with
            member this.CheckNoActiveReader() = this.CheckNoActiveReader()
            member this.ReaderActive
                with get() = this.ReaderActive
                and set(v) = this.ReaderActive <- v
            member this.Execute(sql: string, parameters: obj) = this.Execute(sql, parameters)
            member this.Query<'T>(sql: string, parameters: obj) = this.Query<'T>(sql, parameters)
            member this.QueryFirst<'T>(sql: string, parameters: obj) = this.QueryFirst<'T>(sql, parameters)
            member this.QueryFirstOrDefault<'T>(sql: string, parameters: obj) = this.QueryFirstOrDefault<'T>(sql, parameters)
            member this.Query<'T1, 'T2, 'TReturn>(sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) =
                this.Query<'T1, 'T2, 'TReturn>(sql, map, parameters, splitOn)
        interface IDisposable with
            override this.Dispose (): unit =
                if not this.InsideTransaction then
                    GC.SuppressFinalize(this)
                    onDispose this
    [<Extension>]
    type IDbConnectionExtensions =
        [<Extension>]
        static member OpenReader<'R>(this: SqliteConnection, sql: string, outReader: outref<DbDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            SQLiteToolsExtensions.IDbConnectionExtensions.OpenReader<'R>(this, sql, &outReader, parameters)
        [<Extension>]
        static member Execute(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            SQLiteToolsExtensions.IDbConnectionExtensions.Execute(this, sql, parameters)
        [<Extension>]
        static member Query<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            SQLiteToolsExtensions.IDbConnectionExtensions.Query<'T>(this, sql, parameters)
        [<Extension>]
        static member QueryFirst<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            SQLiteToolsExtensions.IDbConnectionExtensions.QueryFirst<'T>(this, sql, parameters)
        [<Extension>]
        static member QueryFirstOrDefault<'T>(this: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            SQLiteToolsExtensions.IDbConnectionExtensions.QueryFirstOrDefault<'T>(this, sql, parameters)
        [<Extension>]
        static member Query<'T1, 'T2, 'TReturn>(this: SqliteConnection, sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) =
            SQLiteToolsExtensions.IDbConnectionExtensions.Query<'T1, 'T2, 'TReturn>(this, sql, map, parameters, splitOn)
