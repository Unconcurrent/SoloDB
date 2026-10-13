namespace SoloDatabase.Data.Sqlite

open System.Data.Common
open SoloDatabase

/// A SQLite error with the primary and extended result codes supplied by its native owner.
[<Sealed>]
type SqliteException private (message: string, errorCode: int, extendedErrorCode: int, inner: System.Exception) =
    inherit DbException(message, inner)
    new(message: string, errorCode: int, extendedErrorCode: int) = SqliteException(message, errorCode, extendedErrorCode, null)
    new(message: string, errorCode: int) = SqliteException(message, errorCode, errorCode, null)
    internal new(error: NativeSqliteException) = SqliteException(error.Message, error.SqliteErrorCode, error.SqliteExtendedErrorCode, error)
    member _.SqliteErrorCode = errorCode
    member _.SqliteExtendedErrorCode = extendedErrorCode
    interface ISqliteError with member _.ResultCode = extendedErrorCode
    static member ThrowExceptionForRC(result: int, handle: SQLitePCL.sqlite3) =
        if result <> SQLitePCL.raw.SQLITE_OK && result <> SQLitePCL.raw.SQLITE_ROW && result <> SQLitePCL.raw.SQLITE_DONE then
            raise (SqliteException(NativeSqliteException.FromResult(handle, result)))

module internal AdoErrors =
    let inline convert action =
        try action()
        with :? NativeSqliteException as error -> raise (SqliteException(error))
