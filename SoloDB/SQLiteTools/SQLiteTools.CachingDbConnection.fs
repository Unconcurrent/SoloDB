namespace SoloDatabase

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open SQLiteToolsMapper

module NativeSQLiteTools =
    open SQLiteToolsHandlerFaultState
    open SQLiteToolsExecution


    /// <summary>
    /// A pooled native connection with explicit transaction and event state.
    /// </summary>
    /// <param name="connectionStr">The connection string.</param>
    /// <param name="onDispose">A callback function to execute on disposal.</param>
    /// <param name="config">The database configuration.</param>
    type [<Sealed>] NativeCachingDbConnection internal (connectionStr: string, onDispose, config: Types.SoloDBConfiguration, onEnterEventHandlerScope: unit -> unit, onExitEventHandlerScope: unit -> unit) as this =
        inherit NativeSqliteConnection(connectionStr)
        let mutable callbackResources: IFileSystemCallbackResources = Unchecked.defaultof<_>
        let callbackResourceDepth () =
            if isNull (box callbackResources) then 0 else callbackResources.Depth
        let restoreCallbackResources depth =
            if not (isNull (box callbackResources)) then callbackResources.Restore(depth)

        do base.StatementCache.UseConfiguration(config)

        // Per-connection event-handler depth counter for savepoint suppression.
        // Tracks how many nested handler invocations are active on THIS connection.
        // Strict Enter/Exit balance — no negative clamping (negative depth = bug signal).
        let mutable acquisition = 0L
        let mutable eventHandlerDepth = 0
        let mutable eventHandlerInvocationVersion = 0L
        let eventDispatchState = EventDispatch.EventDispatchState()
        do
            base.DatabaseOwner.Observer <-
                { new INativeExecutionObserver with
                    member _.After() = raiseIfHandlerFaultRecorded (this :> NativeSqliteConnection)
                    member _.Failed(error) = tryRecordHandlerFault (this :> NativeSqliteConnection) error
                    member _.Checkpoint() = callbackResourceDepth()
                    member _.Restore(depth) = restoreCallbackResources depth }
    
        /// <summary>The underlying NativeSqliteConnection.</summary>
        member internal _.Acquisition = acquisition
        member internal _.RenewAcquisition() = acquisition <- acquisition + 1L
        member internal this.Inner = this :> NativeSqliteConnection
        member internal _.PreparedCacheSnapshot =
            base.StatementCache.Snapshot
        member internal this.HasManagedTransaction = base.HasOwnedTransaction
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
            eventHandlerInvocationVersion <- eventHandlerInvocationVersion + 1L
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
    
        member internal _.EventHandlerInvocationVersion = eventHandlerInvocationVersion

        member internal this.EventHandlerDepth =
            Threading.Volatile.Read(&eventHandlerDepth)

        interface IEventHandlerScopeState with
            member _.EventHandlerDepth = Threading.Volatile.Read(&eventHandlerDepth)
    
        member internal this.EnterEventDispatchScope() =
            eventDispatchState.Enter()
    
        member internal this.ExitEventDispatchScope() =
            eventDispatchState.Exit()
    
        member internal this.EventDispatchState = eventDispatchState
        member internal this.EventDispatchDepth = eventDispatchState.Depth
        member internal this.EventDispatchPendingRemovals = eventDispatchState.PendingRemovals
    
        member internal this.IsEventDispatchStateClean =
            eventDispatchState.IsClean
    
        member internal _.FileSystemCallbackResources
            with get() = callbackResources
            and set(value) = callbackResources <- value

        member val internal FileSystemEventDispatch: IFileSystemEventDispatch = Unchecked.defaultof<_> with get, set

        member internal this.ResetEventDispatchState() =
            eventDispatchState.Reset()
            if not (isNull (box this.FileSystemCallbackResources)) then this.FileSystemCallbackResources.Reset()
    
        /// Retire cached statements without waiting for an active execution to finish.
        member _.ClearCache() = base.StatementCache.Clear()
        member this.Execute(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = execute this sql parameters
        member this.OpenReader(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = openReader this sql parameters
        member this.Query<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = query<'T> this (NativeStatementSource.Text sql) parameters
        member this.QueryFirst<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = queryFirst<'T> this sql parameters
        member this.QueryFirstOrDefault<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = queryFirstOrDefault<'T> this sql parameters
        member this.Query<'T1, 'T2, 'TReturn>(sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) = multiQuery<'T1, 'T2, 'TReturn> this sql map parameters splitOn
        member internal this.QueryRetained<'T>(handle: RetainedPreparedHandle, parameters: ParameterValues) =
            SQLiteToolsExecution.query<'T> this (NativeStatementSource.Retained handle) parameters
        /// <summary>
        /// Performs the actual disposal of the base connection.
        /// </summary>
        member this.DisposeReal() =
            let mutable failure: exn = null
            try this.ResetEventDispatchState() with error -> failure <- error
            try base.Dispose(true) with error -> if isNull failure then failure <- error
            if not (isNull failure) then System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw()
    
        // Override Dispose(bool) to ensure TakeBack is always called on disposal,
        // regardless of whether Dispose() is called via IDisposable or base class dispatch.
        // Dispose is suppressed when InsideTransaction is true (set by WithTransactionBorrowed
        // and event handler paths). This prevents premature pool return when connection is used
        // inside a transaction via NativeTransactional wrapping or event callbacks.
        override this.Dispose(disposing: bool) =
            if disposing && not this.InsideTransaction then
                onDispose this
    
        interface IDisposable with
            override this.Dispose() =
                if not this.InsideTransaction then
                    GC.SuppressFinalize(this)
                    onDispose this
    /// Named SQL execution and typed results share one native execution core.
    [<Extension>]
    type IDbConnectionExtensions =
        [<Extension>]
        static member OpenReader(this: NativeSqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            openReader this sql parameters
        [<Extension>]
        static member Execute(this: NativeSqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            execute this sql parameters
        [<Extension>]
        static member Query<'T>(this: NativeSqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            query<'T> this (NativeStatementSource.Text sql) parameters
        [<Extension>]
        static member QueryFirst<'T>(this: NativeSqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            queryFirst<'T> this sql parameters
        [<Extension>]
        static member QueryFirstOrDefault<'T>(this: NativeSqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            queryFirstOrDefault<'T> this sql parameters
        /// Direct typed mapping avoids dynamic mapper discovery and expression compilation.
        [<Extension>]
        static member internal Query<'T>(this: NativeSqliteConnection, sql: string, map: Func<SqlRow, 'T>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            if isNull map then nullArg "map"
            seq {
                raiseIfHandlerFaultRecorded this
                let mutable rows = this.AcquireScript(NativeStatementSource.Text sql, parameters)
                try
                    while readRow &rows do yield mapRow &rows map
                    completeRows this &rows
                finally rows.Dispose()
            }
        [<Extension>]
        static member Query<'T1, 'T2, 'TReturn>(this: NativeSqliteConnection, sql: string, map: Func<'T1, 'T2, 'TReturn>, parameters: obj, splitOn: string) =
            multiQuery<'T1, 'T2, 'TReturn> this sql map parameters splitOn
        [<Extension>]
        static member internal Execute<'Input, 'State>(this: NativeSqliteConnection, sql: string, plan: SqlParameterPlan<'Input, 'State>, input: 'Input) =
            withHandlerFaultWrap this (fun () -> this.ExecuteSql(sql, plan, input))
        [<Extension>]
        static member internal Query<'T, 'Input, 'State>(this: NativeSqliteConnection, sql: string, plan: SqlParameterPlan<'Input, 'State>, input: 'Input) =
            seq {
                raiseIfHandlerFaultRecorded this
                let mutable rows = this.AcquireScript(sql, plan, input)
                try
                    let mutable map: Func<SqlRow, 'T> = null
                    while readRow &rows do
                        if isNull map then map <- bindLease<'T> &rows 0 rows.FieldCount
                        yield mapRow &rows map
                    completeRows this &rows
                finally rows.Dispose()
            }
        [<Extension>]
        static member internal QueryFirst<'T, 'Input, 'State>(this: NativeSqliteConnection, sql: string, plan: SqlParameterPlan<'Input, 'State>, input: 'Input) =
            withHandlerFaultWrap this (fun () ->
                let mutable statement = this.OpenStatement(sql)
                let mutable result = Unchecked.defaultof<'T>
                try
                    let entry = statement.Statement
                    let arguments = entry.Capture(plan, input)
                    if entry.HasStatement then statement.Bind(arguments)
                    if statement.Step() then result <- (bind<'T> statement.Statement.Columns 0).Invoke(statement.Row)
                    elif entry.HasStatement && entry.ColumnCount = 0 && entry.HasTail then
                        match firstFromTail<'T> true this statement arguments with
                        | ValueSome value -> result <- value
                        | ValueNone -> Seq.head (Seq.empty<'T>) |> ignore
                    else Seq.head (Seq.empty<'T>) |> ignore
                    if statement.IsCurrent && entry.HasTail then
                        NativeSqlScript.CompleteTail(this.StatementCache, statement, arguments)
                    else statement.Dispose()
                with error -> statement.Abort(error); rethrow error
                result)
