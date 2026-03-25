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
    /// <summary>
    /// A sealed wrapper around SqliteConnection that adds command caching capabilities.
    /// </summary>
    /// <param name="connectionStr">The connection string.</param>
    /// <param name="onDispose">A callback function to execute on disposal.</param>
    /// <param name="config">The database configuration.</param>
    type [<Sealed>] CachingDbConnection internal (connectionStr: string, onDispose, config: Types.SoloDBConfiguration, onEnterEventHandlerScope: unit -> unit, onExitEventHandlerScope: unit -> unit) =
        inherit SqliteConnection(connectionStr)
        let mutable preparedCache = Dictionary<string, {| Command: SqliteCommand; ColumnDict: Dictionary<string, int>; CallCount: int64 ref; InUse : bool ref |}>()
        let maxCacheSize = 1000
        // Connection-level reader-active guard to prevent indefinite hang from overlapping readers.
        let mutable readerActive = false
    
        let tryCachedCommand (this: CachingDbConnection) (sql: string) (parameters: obj) =
            // @VAR variable names are randomly generated, so caching them is not possible.
            if sql.Contains "@VAR" then ValueNone else
            if not config.CachingEnabled then
                ValueNone
            else
    
            // Delete from cache 1/4 of the least used commands.
            if preparedCache.Count >= maxCacheSize then
                let arr = preparedCache |> Seq.toArray
                arr |> Array.sortInPlaceBy (fun (KeyValue(_sql, item)) -> !item.CallCount)
    
                for i in 0..(maxCacheSize / 4 - 1) do
                    preparedCache.Remove (arr.[i].Key) |> ignore
                    arr.[i].Value.Command.Dispose()
    
    
            let item =
                match preparedCache.TryGetValue sql with
                | true, x -> x
                | false, _ ->
                    let command = this.CreateCommand()
                    command.CommandText <- sql
                    processParameters addParameter command parameters
                    // SQLITE_SCHEMA compensation: Microsoft.Data.Sqlite throws ArgumentOutOfRangeException
                    // from PrepareAndEnumerateStatements when concurrent DDL invalidates the schema cache.
                    // SQLite re-prepares on SQLITE_SCHEMA internally; the .NET wrapper does not. Single retry.
                    try command.Prepare()
                    with :? ArgumentOutOfRangeException -> command.Prepare()
    
                    let item = {| Command = command; ColumnDict = Dictionary<string, int>(); CallCount = ref 0L; InUse = ref false |}
                    preparedCache.[sql] <- item
                    item
    
            if !item.InUse then ValueNone else
    
            item.CallCount := !item.CallCount + 1L
            item.InUse := true
    
            processParameters setOrAddParameter item.Command parameters
            match sqlTraceCallback with ValueSome cb -> cb.Invoke(sql) | ValueNone -> ()
            struct (item.Command, item.ColumnDict, item.InUse) |> ValueSome
    
        // Per-connection event-handler depth counter for savepoint suppression.
        // Tracks how many nested handler invocations are active on THIS connection.
        // Strict Enter/Exit balance — no negative clamping (negative depth = bug signal).
        let mutable eventHandlerDepth = 0
        let eventDispatchPendingRemovals = ResizeArray<obj * obj>()
        let mutable eventDispatchDepth = 0
    
        /// <summary>The underlying SqliteConnection.</summary>
        member internal this.Inner = this :> SqliteConnection
        /// <summary>Indicates if the connection is currently part of a transaction.</summary>
        member val InsideTransaction = false with get, set
    
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
        /// Clears the prepared statement cache, waiting for any in-use commands to be released.
        /// </summary>
        member this.ClearCache() =
            if preparedCache.Count = 0 then () else
    
            let oldCache = preparedCache
            preparedCache <- Dictionary<string, {| Command: SqliteCommand; ColumnDict: Dictionary<string, int>; CallCount: int64 ref; InUse : bool ref |}>()
    
            // Bounded timeout to prevent livelock from permanently in-use commands.
            let deadline = System.Diagnostics.Stopwatch.StartNew()
            let maxWaitMs = 5000L
            while oldCache.Count > 0 && deadline.ElapsedMilliseconds < maxWaitMs do
                for KeyValue(k, v) in oldCache |> Seq.toArray do
                    if (not !v.InUse) then
                        v.Command.Dispose()
                        ignore (oldCache.Remove k)
                if oldCache.Count > 0 then
                    Threading.Thread.Sleep(1)
    
            // Force-dispose any commands still in-use after deadline.
            for KeyValue(_, v) in oldCache do
                v.Command.Dispose()
            oldCache.Clear()
    
        /// <summary>Executes a non-query SQL command, utilizing the cache if possible.</summary>
        /// <param name="sql">The SQL command text.</param>
        /// <param name="parameters">The parameters for the command.</param>
        /// <returns>The number of rows affected.</returns>
        member internal this.ReaderActive
            with get() = readerActive
            and set(v) = readerActive <- v
        member internal this.CheckNoActiveReader() =
            if readerActive then
                raise (InvalidOperationException("A data reader is already active on this connection. Close the existing reader before executing another command."))
    
        member this.Execute(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            try
                this.CheckNoActiveReader()
                match tryCachedCommand this sql parameters with
                | ValueSome struct (command, _columnDict, inUse) ->
                    try
                        let affected = command.ExecuteNonQuery()
                        raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                        affected
                    finally
                        inUse := false
                | ValueNone ->
                    use command = createCommand this sql parameters
                    try command.Prepare()
                    with :? ArgumentOutOfRangeException -> command.Prepare()
                    let affected = command.ExecuteNonQuery()
                    raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                    affected
            with ex ->
                tryRecordHandlerFault (this :> SqliteConnection) ex
                reraise()
    
        /// <summary>Opens a data reader, utilizing the cache if possible.</summary>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="outReader">The output SqliteDataReader.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>An IDisposable to manage the lifetime of the reader and command.</returns>
        member this.OpenReader(sql: string, outReader: outref<SqliteDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            this.CheckNoActiveReader()
            match tryCachedCommand this sql parameters with
            | ValueSome struct (command, _columnDict, inUse) ->
                try
                    let reader = command.ExecuteReader()
                    outReader <- reader
                    readerActive <- true
                    let conn = this
                    { new IDisposable with
                        member _.Dispose() =
                            conn.ReaderActive <- false
                            try
                                reader.Dispose()
                            finally
                                inUse := false }
                with _ ->
                    inUse := false
                    reraise()
            | ValueNone ->
                let command = createCommand this sql parameters
                try command.Prepare()
                with :? ArgumentOutOfRangeException -> command.Prepare()
                let reader = command.ExecuteReader()
                outReader <- reader
                readerActive <- true
                let conn = this
                { new IDisposable with
                    member _.Dispose() =
                        conn.ReaderActive <- false
                        reader.Dispose()
                        command.Dispose()
                }
    
        /// <summary>Executes a query and maps the results to a sequence of 'T, utilizing the cache if possible.</summary>
        /// <typeparam name="'T">The type to map results to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>A sequence of 'T objects.</returns>
        member this.Query<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = seq {
            try
                this.CheckNoActiveReader()
                match tryCachedCommand this sql parameters with
                | ValueSome struct (command, columnDict, inUse) ->
                    try yield! queryCommand<'T> command columnDict
                    finally inUse := false
                | ValueNone ->
                    use command = createCommand this sql parameters
                    yield! queryCommand<'T> command null
            with ex ->
                tryRecordHandlerFault (this :> SqliteConnection) ex
                raise ex
        }
    
        /// <summary>Executes a query and returns the first result, utilizing the cache if possible.</summary>
        /// <typeparam name="'T">The type to map the result to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>The first 'T object from the result set.</returns>
        member this.QueryFirst<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            try
                this.CheckNoActiveReader()
                match tryCachedCommand this sql parameters with
                | ValueSome struct (command, columnDict, inUse) ->
                    try
                        let result = queryCommand<'T> command columnDict |> Seq.head
                        raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                        result
                    finally
                        inUse := false
                | ValueNone ->
                    use command = createCommand this sql parameters
                    let result = queryCommand<'T> command null |> Seq.head
                    raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                    result
            with ex ->
                tryRecordHandlerFault (this :> SqliteConnection) ex
                reraise()
    
        /// <summary>Executes a query and returns the first result, or a default value if the sequence is empty, utilizing the cache if possible.</summary>
        /// <typeparam name="'T">The type to map the result to.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <returns>The first 'T object from the result set, or default.</returns>
        member this.QueryFirstOrDefault<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            try
                this.CheckNoActiveReader()
                match tryCachedCommand this sql parameters with
                | ValueSome struct (command, columnDict, inUse) ->
                    try
                        let result =
                            match queryCommand<'T> command columnDict |> Seq.tryHead with
                            | Some x -> x
                            | None -> defaultOf<'T>()
                        raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                        result
                    finally inUse := false
                | ValueNone ->
                    use command = createCommand this sql parameters
                    let result =
                        match queryCommand<'T> command null |> Seq.tryHead with
                        | Some x -> x
                        | None -> defaultOf<'T>()
                    raiseIfHandlerFaultRecorded (this :> SqliteConnection)
                    result
            with ex ->
                tryRecordHandlerFault (this :> SqliteConnection) ex
                reraise()
    
        /// <summary>Executes a multi-mapping query, utilizing the cache if possible.</summary>
        /// <typeparam name="'T1">The type of the first object.</typeparam>
        /// <typeparam name="'T2">The type of the second object.</typeparam>
        /// <typeparam name="'TReturn">The return type after mapping.</typeparam>
        /// <param name="sql">The SQL query text.</param>
        /// <param name="map">The function to map the two objects to the return type.</param>
        /// <param name="parameters">The parameters for the query.</param>
        /// <param name="splitOn">The column name to split the results on.</param>
        /// <returns>A sequence of 'TReturn objects.</returns>
        member this.Query<'T1, 'T2, 'TReturn>(sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) = seq {
            this.CheckNoActiveReader()
            let struct (command, dict, dispose, inUse) =
                match tryCachedCommand this sql parameters with
                | ValueSome struct (command, columnDict, inUse) ->
                    struct (command, columnDict, false, Some inUse)
                | ValueNone ->
                    struct (createCommand this sql parameters, Dictionary<string, int>(), true, None)
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
            finally
                match inUse with
                | Some inUse -> inUse := false
                | _ -> ()
                if dispose then command.Dispose()
        }
    
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
