namespace SoloDatabase.RawSqliteFunctions

open System.Runtime.CompilerServices
open System
open System.Data
open System.Runtime.InteropServices
open System.Collections.Concurrent
open Microsoft.Data.Sqlite
open SQLitePCL
/// <summary>
/// Holds references to registered function delegates to prevent GC collection.
/// Automatically cleans up when connections are closed.
/// </summary>
module private DelegateRegistry =
    // Maps (connection handle, function name) -> delegate
    // This prevents the delegates from being garbage collected
    let private registry = ConcurrentDictionary<struct(nativeint * string), obj>()

    // Track state change handlers per connection to avoid duplicate subscriptions
    let private connectionHandlers = ConcurrentDictionary<nativeint, StateChangeEventHandler>()

    let register (db: sqlite3) (name: string) (del: obj) (connection: SqliteConnection) =
        let handle = db.DangerousGetHandle()
        let key = struct(handle, name)
        registry.[key] <- del

        // Subscribe to connection close event (only once per connection)
        if not (connectionHandlers.ContainsKey(handle)) then
            let handler = StateChangeEventHandler(fun _ args ->
                if args.CurrentState = ConnectionState.Closed then
                    // Remove all delegates for this connection
                    for kvp in registry.Keys |> Seq.toArray do
                        let struct(h, _) = kvp
                        if h = handle then
                            registry.TryRemove(kvp) |> ignore
                    connectionHandlers.TryRemove(handle) |> ignore
            )
            if connectionHandlers.TryAdd(handle, handler) then
                connection.StateChange.AddHandler(handler)

    let unregister (db: sqlite3) (name: string) =
        let key = struct(db.DangerousGetHandle(), name)
        registry.TryRemove(key) |> ignore
/// <summary>
/// Extension methods for SqliteConnection to create raw zero-copy functions.
/// </summary>
[<Extension>]
type internal SqliteConnectionRawExtensions =
    /// <summary>
    /// Creates a raw scalar function with variable arguments and zero-copy value access.
    /// </summary>
    /// <param name="connection">The SQLite connection.</param>
    /// <param name="name">The name of the SQL function.</param>
    /// <param name="argCount">Number of arguments (-1 for variable).</param>
    /// <param name="func">The function implementation.</param>
    /// <param name="flags">Optional function flags (deterministic, etc.).</param>
    [<Extension>]
    static member CreateRawFunction(
            connection: SqliteConnection,
            name: string,
            argCount: int,
            func: RawScalarFunc,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =

        if isNull name then nullArg (nameof name)
        if isNull func then nullArg (nameof func)
        if argCount < -1 || argCount > 127 then
            invalidArg (nameof argCount) "argCount must be -1 (variable) or 0-127"

        let db = connection.Handle

        // Create the native callback
        let nativeFunc: delegate_function_scalar =
            if argCount >= 0 then
                // Fixed arg count: use cached array with lock for thread safety
                let cachedArgs = Array.zeroCreate<SqliteRawValue> argCount
                let lockObj = obj()
                delegate_function_scalar(fun ctx _userData args ->
                    lock lockObj (fun () ->
                        try
                            try
                                let mutable wrappedCtx = { Context = ctx }
                                for i = 0 to args.Length - 1 do
                                    cachedArgs.[i] <- { Value = args.[i] }
                                func.Invoke(&wrappedCtx, ReadOnlySpan(cachedArgs))
                            with ex ->
                                raw.sqlite3_result_error(ctx, ex.Message)
                                match ex with
                                | :? SqliteException as sqlEx ->
                                    raw.sqlite3_result_error_code(ctx, sqlEx.SqliteErrorCode)
                                | _ -> ()
                        finally
                            Array.Clear(cachedArgs, 0, cachedArgs.Length)
                    )
                )
            else
                // Variable arg count (-1): use mutable array with lock, resize only when needed
                let mutable cachedArgs = Array.zeroCreate<SqliteRawValue> 4
                let lockObj = obj()
                delegate_function_scalar(fun ctx _userData args ->
                    lock lockObj (fun () ->
                        try
                            try
                                let mutable wrappedCtx = { Context = ctx }
                                // Resize array if needed
                                if cachedArgs.Length < args.Length then
                                    cachedArgs <- Array.zeroCreate<SqliteRawValue> args.Length
                                for i = 0 to args.Length - 1 do
                                    cachedArgs.[i] <- { Value = args.[i] }
                                func.Invoke(&wrappedCtx, ReadOnlySpan(cachedArgs, 0, args.Length))
                            with ex ->
                                raw.sqlite3_result_error(ctx, ex.Message)
                                match ex with
                                | :? SqliteException as sqlEx ->
                                    raw.sqlite3_result_error_code(ctx, sqlEx.SqliteErrorCode)
                                | _ -> ()
                        finally
                            Array.Clear(cachedArgs, 0, min args.Length cachedArgs.Length)
                    )
                )

        // Root the delegate to prevent GC
        DelegateRegistry.register db name (box (func, nativeFunc)) connection

        // Register with SQLite
        let rc = raw.sqlite3_create_function(
            db,
            name,
            argCount,
            raw.SQLITE_UTF8 ||| int flags,
            null,
            nativeFunc)

        if rc <> raw.SQLITE_OK then
            DelegateRegistry.unregister db name
            failwithf "Failed to create function '%s': error code %d" name rc

    /// <summary>
    /// Creates a raw scalar function with 0 arguments.
    /// </summary>
    [<Extension>]
    static member CreateRawFunction(
            connection: SqliteConnection,
            name: string,
            func: RawScalarFunc0,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =

        if isNull name then nullArg (nameof name)
        if isNull func then nullArg (nameof func)

        let db = connection.Handle

        let nativeFunc: delegate_function_scalar =
            delegate_function_scalar(fun ctx _userData _args ->
                try
                    let mutable wrappedCtx = { Context = ctx }
                    func.Invoke(&wrappedCtx)
                with ex ->
                    raw.sqlite3_result_error(ctx, ex.Message)
                    match ex with
                    | :? SqliteException as sqlEx ->
                        raw.sqlite3_result_error_code(ctx, sqlEx.SqliteErrorCode)
                    | _ -> ()
            )

        DelegateRegistry.register db name (box (func, nativeFunc)) connection

        let rc = raw.sqlite3_create_function(
            db, name, 0,
            raw.SQLITE_UTF8 ||| int flags,
            null, nativeFunc)

        if rc <> raw.SQLITE_OK then
            DelegateRegistry.unregister db name
            failwithf "Failed to create function '%s': error code %d" name rc

    /// <summary>
    /// Creates a raw scalar function with 1 argument.
    /// </summary>
    [<Extension>]
    static member CreateRawFunction(
            connection: SqliteConnection,
            name: string,
            func: RawScalarFunc1,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =

        if isNull name then nullArg (nameof name)
        if isNull func then nullArg (nameof func)

        let db = connection.Handle

        let nativeFunc: delegate_function_scalar =
            delegate_function_scalar(fun ctx _userData args ->
                try
                    let mutable wrappedCtx = { Context = ctx }
                    let arg0 = { Value = args.[0] }
                    func.Invoke(&wrappedCtx, &arg0)
                with ex ->
                    raw.sqlite3_result_error(ctx, ex.Message)
                    match ex with
                    | :? SqliteException as sqlEx ->
                        raw.sqlite3_result_error_code(ctx, sqlEx.SqliteErrorCode)
                    | _ -> ()
            )

        DelegateRegistry.register db name (box (func, nativeFunc)) connection

        let rc = raw.sqlite3_create_function(
            db, name, 1,
            raw.SQLITE_UTF8 ||| int flags,
            null, nativeFunc)

        if rc <> raw.SQLITE_OK then
            DelegateRegistry.unregister db name
            failwithf "Failed to create function '%s': error code %d" name rc

    /// <summary>
    /// Creates a raw scalar function with 2 arguments.
    /// </summary>
    [<Extension>]
    static member CreateRawFunction(
            connection: SqliteConnection,
            name: string,
            func: RawScalarFunc2,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =

        if isNull name then nullArg (nameof name)
        if isNull func then nullArg (nameof func)

        let db = connection.Handle

        let nativeFunc: delegate_function_scalar =
            delegate_function_scalar(fun ctx _userData args ->
                try
                    let mutable wrappedCtx = { Context = ctx }
                    let arg0 = { Value = args.[0] }
                    let arg1 = { Value = args.[1] }
                    func.Invoke(&wrappedCtx, &arg0, &arg1)
                with ex ->
                    raw.sqlite3_result_error(ctx, ex.Message)
                    match ex with
                    | :? SqliteException as sqlEx ->
                        raw.sqlite3_result_error_code(ctx, sqlEx.SqliteErrorCode)
                    | _ -> ()
            )

        DelegateRegistry.register db name (box (func, nativeFunc)) connection

        let rc = raw.sqlite3_create_function(
            db, name, 2,
            raw.SQLITE_UTF8 ||| int flags,
            null, nativeFunc)

        if rc <> raw.SQLITE_OK then
            DelegateRegistry.unregister db name
            failwithf "Failed to create function '%s': error code %d" name rc

    /// <summary>
    /// Creates a raw scalar function with 3 arguments.
    /// </summary>
    [<Extension>]
    static member CreateRawFunction(
            connection: SqliteConnection,
            name: string,
            func: RawScalarFunc3,
            [<Optional; DefaultParameterValue(SqliteFunctionFlags.None)>] flags: SqliteFunctionFlags) =

        if isNull name then nullArg (nameof name)
        if isNull func then nullArg (nameof func)

        let db = connection.Handle

        let nativeFunc: delegate_function_scalar =
            delegate_function_scalar(fun ctx _userData args ->
                try
                    let mutable wrappedCtx = { Context = ctx }
                    let arg0 = { Value = args.[0] }
                    let arg1 = { Value = args.[1] }
                    let arg2 = { Value = args.[2] }
                    func.Invoke(&wrappedCtx, &arg0, &arg1, &arg2)
                with ex ->
                    raw.sqlite3_result_error(ctx, ex.Message)
                    match ex with
                    | :? SqliteException as sqlEx ->
                        raw.sqlite3_result_error_code(ctx, sqlEx.SqliteErrorCode)
                    | _ -> ()
            )

        DelegateRegistry.register db name (box (func, nativeFunc)) connection

        let rc = raw.sqlite3_create_function(
            db, name, 3,
            raw.SQLITE_UTF8 ||| int flags,
            null, nativeFunc)

        if rc <> raw.SQLITE_OK then
            DelegateRegistry.unregister db name
            failwithf "Failed to create function '%s': error code %d" name rc

    /// <summary>
    /// Removes a previously registered raw function.
    /// </summary>
    [<Extension>]
    static member RemoveRawFunction(connection: SqliteConnection, name: string, argCount: int) =
        let db = connection.Handle

        // Pass null function to remove
        let rc = raw.sqlite3_create_function(
            db, name, argCount,
            raw.SQLITE_UTF8, null,
            Unchecked.defaultof<delegate_function_scalar>)

        DelegateRegistry.unregister db name

        if rc <> raw.SQLITE_OK then
            failwithf "Failed to remove function '%s': error code %d" name rc

