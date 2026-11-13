namespace SoloDatabase

open System
open System.Data.Common
open SQLitePCL

/// Shared numeric error contract for native and public callback failures.
type internal ISqliteError =
    abstract ResultCode: int

/// A SQLite failure with its primary and extended engine result codes.
type NativeSqliteException internal (message: string, errorCode: int, extendedErrorCode: int, cause: exn) =
    inherit DbException(message, cause)
    new(message: string, errorCode: int, extendedErrorCode: int) = NativeSqliteException(message, errorCode, extendedErrorCode, null)
    new(message: string, errorCode: int) = NativeSqliteException(message, errorCode, errorCode)
    member _.SqliteErrorCode = errorCode
    member _.SqliteExtendedErrorCode = extendedErrorCode
    interface ISqliteError with member _.ResultCode = extendedErrorCode
    static member internal FromStorageResult(code: int, cause: exn) =
        let message = raw.sqlite3_errstr(code).utf8_to_string()
        NativeSqliteException($"SQLite Error {code &&& 255}: '{message}'.", code &&& 255, code, cause)
    static member internal FromResult(handle: sqlite3, result: int) =
        let primary = result &&& 255
        let extended = if isNull handle || handle.IsInvalid then result else raw.sqlite3_extended_errcode(handle)
        let text = if isNull handle || handle.IsInvalid then raw.sqlite3_errstr(result).utf8_to_string() else raw.sqlite3_errmsg(handle).utf8_to_string()
        NativeSqliteException($"SQLite Error {primary}: '{text}'.", primary, extended)
