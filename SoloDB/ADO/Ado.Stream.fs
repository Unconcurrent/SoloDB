namespace SoloDatabase.Data.Sqlite

open System
open System.IO
open System.Data.Common
open SoloDatabase
open System.Threading
open System.Threading.Tasks

/// A public BLOB stream presents the shared native incremental handle.
type SqliteBlob private (session: AdoSession, inner: NativeBlobStream) as this =
    inherit Stream()
    let version = session.Version
    let mutable closed = false
    let check () =
        if closed then raise (ObjectDisposedException("SqliteBlob"))
        session.Check(version)
    let guard action =
        check()
        let invoke () = action()
        if session.IsCurrent(version) then session.Guard(invoke)
        else AdoErrors.convert invoke
    do session.Register(this)
    new(connection: DbConnection, databaseName: string, tableName: string, columnName: string, rowid: int64, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] readOnly: bool) =
        let session: AdoSession =
            match box connection with
            | :? IAdoSessionHost as host -> host.Session
            | null -> nullArg "connection"
            | _ -> invalidArg "connection" "A SQLite connection is required."
        session.Check()
        let stream = session.Guard(fun () ->
            let state = NativeBlobAccess.openBlob session.Native.DatabaseOwner databaseName tableName columnName rowid readOnly
            new NativeBlobStream(state))
        new SqliteBlob(session, stream)
    new(connection: DbConnection, tableName: string, columnName: string, rowid: int64, [<System.Runtime.InteropServices.Optional; System.Runtime.InteropServices.DefaultParameterValue(false)>] readOnly: bool) =
        new SqliteBlob(connection, "main", tableName, columnName, rowid, readOnly)
    static member internal FromSelected(session: AdoSession, stream: Stream) =
        match stream with
        | :? NativeBlobStream as native -> new SqliteBlob(session, native) :> Stream
        | _ -> stream
    override _.CanRead = not closed && session.IsCurrent(version) && inner.CanRead
    override _.CanSeek = not closed && session.IsCurrent(version) && inner.CanSeek
    override _.CanWrite = not closed && session.IsCurrent(version) && inner.CanWrite
    override _.Length = guard (fun () -> inner.Length)
    override _.Position
        with get() = guard (fun () -> inner.Position)
        and set value = guard (fun () -> inner.Position <- value)
    override _.Read(buffer, offset, count) = guard (fun () -> inner.Read(buffer, offset, count))
    override _.ReadByte() = guard inner.ReadByte
    override _.ReadAsync(buffer, offset, count, cancellationToken: CancellationToken) =
        if cancellationToken.IsCancellationRequested then Task.FromCanceled<int>(cancellationToken)
        else
            try Task.FromResult(guard (fun () -> inner.Read(buffer, offset, count)))
            with error -> Task.FromException<int>(error)
#if NETSTANDARD2_1 || NET6_0_OR_GREATER
    override _.Read(buffer: Span<byte>) =
#else
    member _.Read(buffer: Span<byte>) =
#endif
        // A span cannot be captured. The physical stream validates its own generation.
        check()
        try
            let count = inner.Read(buffer)
            if session.IsCurrent(version) then SoloDatabase.SQLiteToolsHandlerFaultState.raiseIfHandlerFaultRecorded session.Native
            count
        with error ->
            let failure = match error with :? SoloDatabase.NativeSqliteException as native -> SqliteException(native) :> exn | _ -> error
            if session.IsCurrent(version) then SoloDatabase.SQLiteToolsHandlerFaultState.tryRecordHandlerFault session.Native failure
            raise failure
#if NETSTANDARD2_1 || NET6_0_OR_GREATER
    override _.Write(buffer: ReadOnlySpan<byte>) =
#else
    member _.Write(buffer: ReadOnlySpan<byte>) =
#endif
        check()
        try
            inner.Write(buffer)
            if session.IsCurrent(version) then SoloDatabase.SQLiteToolsHandlerFaultState.raiseIfHandlerFaultRecorded session.Native
        with error -> session.Guard(fun () -> raise error)
#if NETSTANDARD2_1 || NET6_0_OR_GREATER
    override this.WriteAsync(buffer: ReadOnlyMemory<byte>, [<System.Runtime.InteropServices.Optional>] cancellationToken: CancellationToken) =
        if cancellationToken.IsCancellationRequested then ValueTask(Task.FromCanceled(cancellationToken))
        else try this.Write(buffer.Span); ValueTask() with error -> ValueTask(Task.FromException(error))
    override this.ReadAsync(buffer: Memory<byte>, [<System.Runtime.InteropServices.Optional>] cancellationToken: CancellationToken) =
        if cancellationToken.IsCancellationRequested then ValueTask<int>(Task.FromCanceled<int>(cancellationToken))
        else
            try ValueTask<int>(this.Read(buffer.Span))
            with error -> ValueTask<int>(Task.FromException<int>(error))
#endif
    override _.Seek(offset, origin) = guard (fun () -> inner.Seek(offset, origin))
    override _.Flush() = guard inner.Flush
    override _.SetLength(value) = guard (fun () -> inner.SetLength(value))
    override _.Write(buffer, offset, count) = guard (fun () -> inner.Write(buffer, offset, count))
    override _.WriteAsync(buffer, offset, count, cancellationToken: CancellationToken) =
        if cancellationToken.IsCancellationRequested then Task.FromCanceled(cancellationToken)
        else try this.Write(buffer, offset, count); Task.CompletedTask with error -> Task.FromException(error)
    override _.Dispose(disposing) =
        try
            if disposing && not closed then
                closed <- true
                session.Unregister(this)
                try AdoErrors.convert inner.Dispose
                with error ->
                    if session.IsCurrent(version) then SoloDatabase.SQLiteToolsHandlerFaultState.tryRecordHandlerFault session.Native error
                    reraise()
        finally base.Dispose(disposing)
