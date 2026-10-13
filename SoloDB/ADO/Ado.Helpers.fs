namespace SoloDatabase

open System
open System.Data.Common
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open SoloDatabase.Data.Sqlite

module internal PublicSqlExecution =
    let guard (connection: SqliteConnection) action =
        connection.Session.Check()
        connection.Session.Guard(action)
    let sequence (connection: SqliteConnection) (values: seq<'T>) =
        let session = connection.Session
        let version = session.Version
        seq {
            session.Check(version)
            use iterator = values.GetEnumerator()
            let mutable more = true
            while more do
                session.Check(version)
                more <- session.Guard(iterator.MoveNext)
                if more then yield iterator.Current
        }
    let execute connection sql parameters = guard connection (fun () -> SQLiteToolsExecution.execute connection.NativeConnection sql parameters)
    let query<'T> connection sql parameters =
        guard connection (fun () -> SQLiteToolsExecution.query<'T> connection.NativeConnection (NativeStatementSource.Text sql) parameters)
        |> sequence connection
    let first<'T> connection sql parameters = guard connection (fun () -> SQLiteToolsExecution.queryFirst<'T> connection.NativeConnection sql parameters)
    let firstOrDefault<'T> connection sql parameters = guard connection (fun () -> SQLiteToolsExecution.queryFirstOrDefault<'T> connection.NativeConnection sql parameters)
    let multi<'A, 'B, 'R> connection sql map parameters splitOn =
        guard connection (fun () -> SQLiteToolsExecution.multiQuery<'A, 'B, 'R> connection.NativeConnection sql map parameters splitOn)
        |> sequence connection
    let openReader (connection: SqliteConnection) sql parameters =
        guard connection (fun () ->
            let admission = NativeBlobResult(NativeBlobSyntax())
            let rows = connection.NativeConnection.AcquireRows(sql, parameters, false, admission)
            try
                new SqliteDataReader(connection.Session, rows, admission, false, Action(ignore))
            with error -> admission.Dispose(); rows.Abort(error); reraise())
    let acquisitionGuard (native: NativeSqliteConnection) guard =
        match native with
        | :? NativeSQLiteTools.NativeCachingDbConnection as pooled ->
            let identity = pooled.Acquisition
            fun () ->
                guard()
                if pooled.Acquisition <> identity then raise (InvalidOperationException("The pooled connection acquisition has expired."))
        | _ -> guard

module SQLiteTools =
    /// The public caching connection shares its acquired native execution and fault owners.
    type CachingDbConnection internal (native: NativeSqliteConnection, leaveOpen: bool, guard: unit -> unit) as this =
        inherit SqliteConnection(native, leaveOpen, PublicSqlExecution.acquisitionGuard native guard)
        member _.InsideTransaction
            with get() = PublicSqlExecution.guard this (fun () -> (native :?> NativeSQLiteTools.NativeCachingDbConnection).InsideTransaction)
            and set value = PublicSqlExecution.guard this (fun () -> (native :?> NativeSQLiteTools.NativeCachingDbConnection).InsideTransaction <- value)
        member _.Unusable
            with get() = PublicSqlExecution.guard this (fun () -> (native :?> NativeSQLiteTools.NativeCachingDbConnection).Unusable)
            and set value = PublicSqlExecution.guard this (fun () -> (native :?> NativeSQLiteTools.NativeCachingDbConnection).Unusable <- value)
        member _.ClearCache() = PublicSqlExecution.guard this (fun () -> native.StatementCache.Clear())
        member _.DisposeReal() = PublicSqlExecution.guard this (fun () -> (native :?> NativeSQLiteTools.NativeCachingDbConnection).DisposeReal())
        member _.Execute(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.execute this sql parameters
        member _.Query<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.query<'T> this sql parameters
        member _.QueryFirst<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.first<'T> this sql parameters
        member _.QueryFirstOrDefault<'T>(sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.firstOrDefault<'T> this sql parameters
        member _.Query<'A, 'B, 'R>(sql: string, map: Func<'A, 'B, 'R>, parameters: obj, splitOn: string) = PublicSqlExecution.multi<'A, 'B, 'R> this sql map parameters splitOn
        member _.OpenReader(sql: string, [<Out>] reader: byref<SqliteDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            let result = PublicSqlExecution.openReader this sql parameters
            reader <- result
            result :> IDisposable

    [<Extension>]
    type IDbConnectionExtensions =
        [<Extension>]
        static member Execute(connection: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.execute connection sql parameters
        [<Extension>]
        static member Query<'T>(connection: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.query<'T> connection sql parameters
        [<Extension>]
        static member QueryFirst<'T>(connection: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.first<'T> connection sql parameters
        [<Extension>]
        static member QueryFirstOrDefault<'T>(connection: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.firstOrDefault<'T> connection sql parameters
        [<Extension>]
        static member Query<'A, 'B, 'R>(connection: SqliteConnection, sql: string, map: Func<'A, 'B, 'R>, parameters: obj, splitOn: string) = PublicSqlExecution.multi<'A, 'B, 'R> connection sql map parameters splitOn
        [<Extension>]
        static member OpenReader<'R>(connection: SqliteConnection, sql: string, [<Out>] reader: byref<DbDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            let result = PublicSqlExecution.openReader connection sql parameters
            reader <- result
            result :> IDisposable

/// Retains the original explicit helper container for recompiling existing source.
module SQLiteToolsExtensions =
    [<Extension>]
    type IDbConnectionExtensions =
        [<Extension>]
        static member Execute(connection: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.execute connection sql parameters
        [<Extension>]
        static member Query<'T>(connection: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.query<'T> connection sql parameters
        [<Extension>]
        static member QueryFirst<'T>(connection: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.first<'T> connection sql parameters
        [<Extension>]
        static member QueryFirstOrDefault<'T>(connection: SqliteConnection, sql: string, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) = PublicSqlExecution.firstOrDefault<'T> connection sql parameters
        [<Extension>]
        static member Query<'A, 'B, 'R>(connection: SqliteConnection, sql: string, map: Func<'A, 'B, 'R>, parameters: obj, splitOn: string) = PublicSqlExecution.multi<'A, 'B, 'R> connection sql map parameters splitOn
        [<Extension>]
        static member OpenReader<'R>(connection: SqliteConnection, sql: string, [<Out>] reader: byref<DbDataReader>, [<Optional; DefaultParameterValue(null: obj)>] parameters: obj) =
            let result = PublicSqlExecution.openReader connection sql parameters
            reader <- result
            result :> IDisposable

module internal ConnectionPresentation =
    let create (native: NativeSqliteConnection) leaveOpen guard : SqliteConnection =
        match native with
        | :? NativeSQLiteTools.NativeCachingDbConnection -> new SQLiteTools.CachingDbConnection(native, leaveOpen, guard)
        | _ -> new SqliteConnection(native, leaveOpen, guard)
