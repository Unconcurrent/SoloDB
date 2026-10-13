namespace SoloDatabase.RawSqliteFunctions

open System
open System.Runtime.InteropServices
open System.Text
open Microsoft.FSharp.NativeInterop

#nowarn "9"

[<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
type internal NativeScalarCallback = delegate of nativeint * int * nativeint -> unit

[<UnmanagedFunctionPointer(CallingConvention.Cdecl)>]
type internal NativeFunctionDestructor = delegate of nativeint -> unit

/// Direct callbacks into the bundled SQLite engine keep borrowed values as pointers.
/// Other configured providers retain their own registration and value adapters.
module internal NativeSqlite =
    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int sqlite3_create_function_v2(SQLitePCL.sqlite3 db, byte[] name, int argc, int flags, nativeint data, NativeScalarCallback scalar, nativeint step, nativeint final, NativeFunctionDestructor destroy)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern nativeint sqlite3_user_data(nativeint context)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int sqlite3_value_type(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int sqlite3_value_int(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int64 sqlite3_value_int64(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern double sqlite3_value_double(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern nativeint sqlite3_value_blob(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern nativeint sqlite3_value_text(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int sqlite3_value_bytes(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_null(nativeint context)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_int(nativeint context, int value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_int64(nativeint context, int64 value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_double(nativeint context, double value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_blob(nativeint context, nativeint value, int length, nativeint destroy)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_text(nativeint context, nativeint value, int length, nativeint destroy)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_error(nativeint context, nativeint value, int length)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_error_code(nativeint context, int code)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_error_toobig(nativeint context)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_error_nomem(nativeint context)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_zeroblob(nativeint context, int size)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern nativeint sqlite3_column_text16(SQLitePCL.sqlite3_stmt statement, int ordinal)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int sqlite3_column_bytes16(SQLitePCL.sqlite3_stmt statement, int ordinal)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern nativeint sqlite3_value_text16(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int sqlite3_value_bytes16(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_text16(nativeint context, nativeint value, int length, nativeint destroy)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern void sqlite3_result_error16(nativeint context, nativeint value, int length)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern nativeint sqlite3_column_value(SQLitePCL.sqlite3_stmt statement, int ordinal)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int sqlite3_value_encoding(nativeint value)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern nativeint sqlite3_column_text(SQLitePCL.sqlite3_stmt statement, int ordinal)

    [<DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)>]
    extern int sqlite3_column_bytes(SQLitePCL.sqlite3_stmt statement, int ordinal)

    let private copyUtf16 pointer bytes =
        if bytes = 0 then String.Empty else Marshal.PtrToStringUni(pointer, bytes / 2)

    let ColumnText(statement: SQLitePCL.sqlite3_stmt, ordinal: int) =
        // Read each native representation directly before copying the managed string.
        // This preserves stored text and avoids an extra native encoding conversion.
        if sqlite3_value_encoding(sqlite3_column_value(statement, ordinal)) = 1 then
            let pointer = sqlite3_column_text(statement, ordinal)
            SoloDatabase.NativeTextEncoding.copyUtf8(pointer, sqlite3_column_bytes(statement, ordinal))
        else
            let pointer = sqlite3_column_text16(statement, ordinal)
            copyUtf16 pointer (sqlite3_column_bytes16(statement, ordinal))

    let ValueText(value: nativeint) =
        if sqlite3_value_encoding(value) = 1 then
            let pointer = sqlite3_value_text(value)
            SoloDatabase.NativeTextEncoding.copyUtf8(pointer, sqlite3_value_bytes(value))
        else
            let pointer = sqlite3_value_text16(value)
            copyUtf16 pointer (sqlite3_value_bytes16(value))

    let SetBlob(context: nativeint, value: ReadOnlySpan<byte>) =
        if value.IsEmpty then sqlite3_result_zeroblob(context, 0)
        else
            use ptr = fixed value
            sqlite3_result_blob(context, NativePtr.toNativeInt ptr, value.Length, -1n)

    let SetText(context: nativeint, value: string) =
        if isNull value then sqlite3_result_null(context)
        elif SoloDatabase.NativeTextEncoding.requiresUtf8(value.AsSpan()) then
            let bytes = Encoding.UTF8.GetBytes(value)
            use ptr = fixed bytes
            sqlite3_result_text(context, NativePtr.toNativeInt ptr, bytes.Length, -1n)
        else
            use ptr = fixed value
            sqlite3_result_text16(context, NativePtr.toNativeInt ptr, Checked.(*) value.Length 2, -1n)

    let SetError(context: nativeint, value: string) =
        if SoloDatabase.NativeTextEncoding.requiresUtf8(value.AsSpan()) then
            let bytes = Encoding.UTF8.GetBytes(value)
            use ptr = fixed bytes
            sqlite3_result_error(context, NativePtr.toNativeInt ptr, bytes.Length)
        else
            use ptr = fixed value
            sqlite3_result_error16(context, NativePtr.toNativeInt ptr, Checked.(*) value.Length 2)
