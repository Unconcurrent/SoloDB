namespace SoloDatabase.RawSqliteFunctions

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text
open System.Threading
open SQLitePCL
open SoloDatabase

/// Argument buffers are borrowed by one invocation, including reentrant invocations.
type private RawArgumentBuffer() =
    member val Values: SqliteRawValue array = Array.empty with get, set

type internal RawCallbackState(database: NativeDatabase, func: RawScalarFunc, preserveManagedFailure: bool) =
    let gate = obj()
    let buffers = CallbackSlots(fun () -> RawArgumentBuffer())
    let generation = database.Generation

    new(database, func) = RawCallbackState(database, func, false)

    member _.InvokeNative(context: nativeint, count: int, arguments: nativeint) =
        Monitor.Enter(gate)
        try
            database.ObserveCallback()
            let mutable wrapped = { Context = Unchecked.defaultof<sqlite3_context>; NativeContext = context }
            try
                if count = 0 then func.Invoke(&wrapped, ReadOnlySpan<SqliteRawValue>.Empty)
                else
                    let buffer = buffers.Borrow()
                    try
                        if buffer.Values.Length < count then buffer.Values <- Array.zeroCreate count
                        for i = 0 to count - 1 do
                            buffer.Values.[i] <-
                                { Value = Unchecked.defaultof<sqlite3_value>
                                  NativeValue = Marshal.ReadIntPtr(arguments, i * IntPtr.Size) }
                        func.Invoke(&wrapped, ReadOnlySpan(buffer.Values, 0, count))
                    finally
                        Array.Clear(buffer.Values, 0, count)
                        buffers.Return()
            with ex ->
                if preserveManagedFailure then database.RecordCallbackFailure(generation, ex)
                wrapped.SetError(ex.Message)
                match box ex with
                | :? ISqliteError as sql -> NativeSqlite.sqlite3_result_error_code(context, sql.ResultCode)
                | _ -> ()
        finally
            Monitor.Exit(gate)

    member _.InvokeManaged(context: sqlite3_context, arguments: sqlite3_value array) =
        Monitor.Enter(gate)
        try
            database.ObserveCallback()
            let mutable wrapped = { Context = context; NativeContext = 0n }
            try
                if arguments.Length = 0 then func.Invoke(&wrapped, ReadOnlySpan<SqliteRawValue>.Empty)
                else
                    let buffer = buffers.Borrow()
                    try
                        if buffer.Values.Length < arguments.Length then buffer.Values <- Array.zeroCreate arguments.Length
                        for i = 0 to arguments.Length - 1 do
                            buffer.Values.[i] <- { Value = arguments.[i]; NativeValue = 0n }
                        func.Invoke(&wrapped, ReadOnlySpan(buffer.Values, 0, arguments.Length))
                    finally
                        Array.Clear(buffer.Values, 0, arguments.Length)
                        buffers.Return()
            with ex ->
                if preserveManagedFailure then database.RecordCallbackFailure(generation, ex)
                wrapped.SetError(ex.Message)
                match box ex with
                | :? ISqliteError as sql -> raw.sqlite3_result_error_code(context, sql.ResultCode)
                | _ -> ()
        finally
            Monitor.Exit(gate)

module internal RawFunctionRegistration =
    let private scalar = NativeScalarCallback(fun context count arguments ->
        let handle = GCHandle.FromIntPtr(NativeSqlite.sqlite3_user_data(context))
        (handle.Target :?> RawCallbackState).InvokeNative(context, count, arguments))

    let private destroy = NativeFunctionDestructor(fun data ->
        GCHandle.FromIntPtr(data).Free())

    let private usesBundledEngine () =
        String.Equals(raw.GetNativeLibraryName(), "e_sqlite3", StringComparison.Ordinal)

    let private createWithFailureTransport (database: NativeDatabase) name count (func: RawScalarFunc) (flags: SqliteFunctionFlags) preserveManagedFailure =
        let db = database.Handle
        if isNull name then nullArg (nameof name)
        if isNull func then nullArg (nameof func)
        if count < -1 || count > 127 then invalidArg "argCount" "argCount must be -1 (variable) or 0-127"
        let state = RawCallbackState(database, func, preserveManagedFailure)
        let result =
            if usesBundledEngine() then
                let bytes = Encoding.UTF8.GetBytes(name + "\000")
                let handle = GCHandle.Alloc(state)
                // SQLite owns this handle after the call, including registration failure.
                // Its destructor also runs on replacement, removal and actual connection close.
                try
                    NativeSqlite.sqlite3_create_function_v2(db, bytes, count,
                        raw.SQLITE_UTF8 ||| int flags, GCHandle.ToIntPtr(handle), scalar, 0n, 0n, destroy)
                with _ ->
                    handle.Free()
                    reraise()
            else
                let callback = delegate_function_scalar(fun context _ arguments -> state.InvokeManaged(context, arguments))
                raw.sqlite3_create_function(db, name, count, raw.SQLITE_UTF8 ||| int flags, null, callback)
        if result <> raw.SQLITE_OK then failwithf "Failed to create function '%s': error code %d" name result

    let create database name count func flags = createWithFailureTransport database name count func flags false
    let createManaged database name count func flags = createWithFailureTransport database name count func flags true

    let remove (db: sqlite3) name count =
        let result =
            if usesBundledEngine() then
                NativeSqlite.sqlite3_create_function_v2(db, Encoding.UTF8.GetBytes(name + "\000"),
                    count, raw.SQLITE_UTF8, 0n, Unchecked.defaultof<NativeScalarCallback>, 0n, 0n,
                    Unchecked.defaultof<NativeFunctionDestructor>)
            else
                raw.sqlite3_create_function(db, name, count, raw.SQLITE_UTF8, null, Unchecked.defaultof<delegate_function_scalar>)
        if result <> raw.SQLITE_OK then failwithf "Failed to remove function '%s': error code %d" name result
