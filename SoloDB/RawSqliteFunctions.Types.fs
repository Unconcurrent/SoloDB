namespace SoloDatabase.RawSqliteFunctions

open System.Runtime.CompilerServices
open System
open System.Runtime.InteropServices
open SQLitePCL

// NativePtr operations for zero-copy access to SQLite callback values
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
    /// Gets the value as a zero-copy UTF-8 ReadOnlySpan.
    /// WARNING: Only valid during the callback! Do not store the span.
    /// </summary>
    member this.GetTextSpan() : ReadOnlySpan<byte> =
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
    member this.GetBlobPointer() : nativeptr<byte> =
        // sqlite3_value_text returns utf8` which wraps a pointer
        Unsafe.AsPointer(&Unsafe.AsRef(&raw.sqlite3_value_blob(this.Value).GetPinnableReference())) |> NativeInterop.NativePtr.ofVoidPtr

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
