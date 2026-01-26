namespace SoloDatabase.RawSqliteFunctions

open System.Runtime.CompilerServices

[<assembly: InternalsVisibleTo("Tests")>]
[<assembly: InternalsVisibleTo("CSharpTests")>]
do ()

open System
open System.Data
open System.Runtime.InteropServices
open System.Collections.Concurrent
open Microsoft.Data.Sqlite
open SQLitePCL

#nowarn "9"

// ============================================================================
// Raw SQLite Value Types (Zero-Copy Wrappers)
// ============================================================================

/// <summary>
/// Represents a raw SQLite value with zero-copy access.
/// Valid only during the UDF callback - do not store or use after callback returns.
/// </summary>
[<Struct; IsReadOnly>]
type SqliteRawValue =
    internal { Value: sqlite3_value }

    /// <summary>Returns the SQLite type of this value.</summary>
    member this.SqliteType: int = raw.sqlite3_value_type(this.Value)

    /// <summary>Returns true if the value is NULL.</summary>
    member this.IsNull = this.SqliteType = raw.SQLITE_NULL

    /// <summary>Returns true if the value is an INTEGER.</summary>
    member this.IsInteger = this.SqliteType = raw.SQLITE_INTEGER

    /// <summary>Returns true if the value is a FLOAT.</summary>
    member this.IsFloat = this.SqliteType = raw.SQLITE_FLOAT

    /// <summary>Returns true if the value is TEXT.</summary>
    member this.IsText = this.SqliteType = raw.SQLITE_TEXT

    /// <summary>Returns true if the value is a BLOB.</summary>
    member this.IsBlob = this.SqliteType = raw.SQLITE_BLOB

    /// <summary>
    /// Gets the value as a 64-bit integer.
    /// </summary>
    member this.GetInt64() : int64 =
        raw.sqlite3_value_int64(this.Value)

    /// <summary>
    /// Gets the value as a 32-bit integer.
    /// </summary>
    member this.GetInt32() : int32 =
        raw.sqlite3_value_int(this.Value)

    /// <summary>
    /// Gets the value as a double-precision float.
    /// </summary>
    member this.GetDouble() : float =
        raw.sqlite3_value_double(this.Value)

    /// <summary>
    /// Gets the value as a zero-copy ReadOnlySpan of bytes.
    /// WARNING: Only valid during the callback! Do not store the span.
    /// </summary>
    member this.GetBlobSpan() : ReadOnlySpan<byte> =
        raw.sqlite3_value_blob(this.Value)

    /// <summary>
    /// Gets the byte length of a BLOB or TEXT value.
    /// </summary>
    member this.GetByteCount() : int =
        raw.sqlite3_value_bytes(this.Value)

    /// <summary>
    /// Gets the value as a UTF-8 string (allocates a new string).
    /// </summary>
    member this.CopyToString() : string =
        let utf8z = raw.sqlite3_value_text(this.Value)
        utf8z.utf8_to_string()

    /// <summary>
    /// Gets the value as a zero-copy UTF-8 ReadOnlySpan.
    /// WARNING: Only valid during the callback! Do not store the span.
    /// </summary>
    member this.GetTextSpan() : ReadOnlySpan<byte> =
        // sqlite3_value_text returns utf8z which wraps a pointer
        // We can get the span via blob access for text too
        raw.sqlite3_value_blob(this.Value)

    /// <summary>
    /// Gets the value as a byte array (copies the data).
    /// Use GetBlobSpan() for zero-copy access.
    /// </summary>
    member this.CopyBlobToArray() : byte[] =
        this.GetBlobSpan().ToArray()


/// <summary>
/// Context for setting the result of a raw SQLite function.
/// </summary>
[<Struct; IsReadOnly>]
type SqliteRawContext =
    internal { Context: sqlite3_context }

    /// <summary>Sets the result to NULL.</summary>
    member this.SetNull() =
        raw.sqlite3_result_null(this.Context)

    /// <summary>Sets the result to a 64-bit integer.</summary>
    member this.SetInt64(value: int64) =
        raw.sqlite3_result_int64(this.Context, value)

    /// <summary>Sets the result to a 32-bit integer.</summary>
    member this.SetInt32(value: int32) =
        raw.sqlite3_result_int(this.Context, value)

    /// <summary>Sets the result to a double-precision float.</summary>
    member this.SetDouble(value: float) =
        raw.sqlite3_result_double(this.Context, value)

    /// <summary>
    /// Sets the result to a BLOB (copies the data).
    /// </summary>
    member this.SetBlob(value: ReadOnlySpan<byte>) =
        raw.sqlite3_result_blob(this.Context, value)

    /// <summary>
    /// Sets the result to a BLOB from a byte array.
    /// </summary>
    member this.SetBlob(value: byte[]) =
        raw.sqlite3_result_blob(this.Context, ReadOnlySpan(value))

    /// <summary>
    /// Sets the result to a UTF-8 text string.
    /// </summary>
    member this.SetText(value: string) =
        raw.sqlite3_result_text(this.Context, value)

    /// <summary>
    /// Sets the result to an error message.
    /// </summary>
    member this.SetError(message: string) =
        raw.sqlite3_result_error(this.Context, message)

    /// <summary>
    /// Sets a "too big" error result.
    /// </summary>
    member this.SetErrorTooBig() =
        raw.sqlite3_result_error_toobig(this.Context)

    /// <summary>
    /// Sets a "no memory" error result.
    /// </summary>
    member this.SetErrorNoMem() =
        raw.sqlite3_result_error_nomem(this.Context)

    /// <summary>
    /// Allocates a zero-filled blob of the specified size.
    /// </summary>
    member this.SetZeroBlob(size: int) =
        raw.sqlite3_result_zeroblob(this.Context, size)


// ============================================================================
// Function Registration
// ============================================================================

/// <summary>
/// Delegate for a raw scalar function with zero-copy value access.
/// </summary>
type RawScalarFunc = delegate of ctx: byref<SqliteRawContext> * args: ReadOnlySpan<SqliteRawValue> -> unit

/// <summary>
/// Delegate for a raw scalar function with 0 arguments.
/// </summary>
type RawScalarFunc0 = delegate of ctx: byref<SqliteRawContext> -> unit

/// <summary>
/// Delegate for a raw scalar function with 1 argument.
/// </summary>
type RawScalarFunc1 = delegate of ctx: byref<SqliteRawContext> * arg0: inref<SqliteRawValue> -> unit

/// <summary>
/// Delegate for a raw scalar function with 2 arguments.
/// </summary>
type RawScalarFunc2 = delegate of ctx: byref<SqliteRawContext> * arg0: inref<SqliteRawValue> * arg1: inref<SqliteRawValue> -> unit

/// <summary>
/// Delegate for a raw scalar function with 3 arguments.
/// </summary>
type RawScalarFunc3 = delegate of ctx: byref<SqliteRawContext> * arg0: inref<SqliteRawValue> * arg1: inref<SqliteRawValue> * arg2: inref<SqliteRawValue> -> unit


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
/// SQLite function flags that control optimization and security behavior.
/// These flags can be combined using bitwise OR (|||).
/// </summary>
/// <remarks>
/// Performance: Using Deterministic allows SQLite to cache results and use the function in indexes.
/// Security: DirectOnly prevents indirect invocation from triggers/views (for security-sensitive functions).
/// Innocuous marks a function as safe for all contexts (no side effects, no information leaks).
/// </remarks>
[<Flags>]
type SqliteFunctionFlags =
    /// <summary>
    /// No special flags. The function may return different results for same inputs,
    /// can be called from triggers/views/generated columns, and results won't be cached.
    /// Use this for functions with side effects or that read mutable state.
    /// </summary>
    | None = 0

    /// <summary>
    /// SQLITE_DETERMINISTIC: The function always returns the same result given the same inputs.
    /// Benefits: SQLite can cache results, function can be used in CHECK constraints and indexes,
    /// and query optimizer can apply additional optimizations.
    /// Examples: UPPER(), LENGTH(), pure mathematical functions.
    /// Do NOT use for: RANDOM(), CURRENT_TIMESTAMP, functions reading external/mutable state.
    /// </summary>
    | Deterministic = 0x000000800

    /// <summary>
    /// SQLITE_DIRECTONLY: Function can only be invoked from top-level SQL, NOT from
    /// triggers, views, CHECK constraints, DEFAULT clauses, or generated columns.
    /// Use for: Functions with I/O, network operations, or security-sensitive side effects.
    /// Security: Prevents SQL injection attacks where malicious data could trigger
    /// function execution through a trigger or view definition.
    /// </summary>
    | DirectOnly = 0x000080000

    /// <summary>
    /// SQLITE_INNOCUOUS: Function has no side effects and is safe to call from any context.
    /// Requirements: No side effects, doesn't reveal sensitive info, safe with any input.
    /// This is the opposite of DirectOnly - explicitly marks function as safe everywhere.
    /// Typical use: Combine with Deterministic for pure functions (Deterministic ||| Innocuous).
    /// </summary>
    | Innocuous = 0x000200000


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

