namespace SoloDatabase

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open SQLitePCL

/// The incremental blob lease owns one native handle and its physical connection generation.
[<Struct>]
type internal NativeBlobState =
    { Database: NativeDatabase
      Generation: int64
      Handle: sqlite3_blob
      Length: int64
      ReadOnly: bool
      mutable Position: int64
      mutable Disposed: bool }

module internal NativeBlobAccess =
    let openBlob (database: NativeDatabase) (databaseName: string) (tableName: string) (columnName: string) (rowid: int64) (readOnly: bool) =
        let mutable handle: sqlite3_blob = null
        let result = raw.sqlite3_blob_open(database.Handle, databaseName, tableName, columnName, rowid, (if readOnly then 0 else 1), &handle)
        if result <> raw.SQLITE_OK then
            let failure = NativeSqliteException.FromResult(database.Handle, result)
            if not (isNull handle) then handle.Dispose()
            raise failure
        { Database = database; Generation = database.Generation; Handle = handle
          Length = int64 (raw.sqlite3_blob_bytes handle); ReadOnly = readOnly
          Position = 0L; Disposed = false }
    let check (state: inref<NativeBlobState>) =
        if state.Disposed then raise (ObjectDisposedException("NativeSqliteBlob"))
        state.Database.EnsureGeneration(state.Generation)
    let read (state: byref<NativeBlobState>, buffer: Span<byte>) =
        check &state
        let count = int (max 0L (min (int64 buffer.Length) (state.Length - state.Position)))
        if count > 0 then
            state.Database.Check(raw.sqlite3_blob_read(state.Handle, buffer.Slice(0, count), int state.Position))
            state.Position <- state.Position + int64 count
        count
    let write (state: byref<NativeBlobState>, buffer: ReadOnlySpan<byte>) =
        check &state
        if state.ReadOnly then raise (NotSupportedException("The blob is read-only."))
        if int64 buffer.Length > state.Length - state.Position then raise (NotSupportedException("SQLite blobs cannot be resized."))
        state.Database.Check(raw.sqlite3_blob_write(state.Handle, buffer, int state.Position))
        state.Position <- state.Position + int64 buffer.Length
    let position (state: byref<NativeBlobState>, value: int64) =
        check &state
        if value < 0L then raise (ArgumentOutOfRangeException("value"))
        state.Position <- value
    let seek (state: byref<NativeBlobState>, offset: int64, origin: SeekOrigin) =
        check &state
        let target =
            match origin with
            | SeekOrigin.Begin -> offset
            | SeekOrigin.Current -> Checked.(+) state.Position offset
            | SeekOrigin.End -> Checked.(+) state.Length offset
            | _ -> raise (ArgumentOutOfRangeException("origin"))
        if target < 0L then raise (IOException("Attempted to seek outside the blob."))
        state.Position <- target
        target
    let close (state: byref<NativeBlobState>, disposing: bool) =
        if not state.Disposed then
            state.Disposed <- true
            let result = raw.sqlite3_blob_close state.Handle
            if state.Database.IsOpen && state.Database.Generation = state.Generation then state.Database.ObserveStatementCompletion()
            if disposing && result <> raw.SQLITE_OK then
                if state.Database.IsOpen && state.Database.Generation = state.Generation then state.Database.Check result
                else raise (NativeSqliteException.FromResult(null, result))


/// The physical stream owns one incremental handle and its generation.
type NativeBlobStream internal (initial: NativeBlobState) =
    inherit Stream()
    let mutable state = initial
    override _.CanRead = not state.Disposed && state.Database.IsOpen && state.Database.Generation = state.Generation
    override _.CanSeek = not state.Disposed && state.Database.IsOpen && state.Database.Generation = state.Generation
    override _.CanWrite = not state.ReadOnly && not state.Disposed && state.Database.IsOpen && state.Database.Generation = state.Generation
    override _.Length = NativeBlobAccess.check &state; state.Length
    override _.Position
        with get() = NativeBlobAccess.check &state; state.Position
        and set value = NativeBlobAccess.position(&state, value)
    override _.Read(buffer, offset, count) =
        if isNull buffer then nullArg "buffer"
        if offset < 0 || offset > buffer.Length then raise (ArgumentOutOfRangeException("offset"))
        if count < 0 || count > buffer.Length - offset then raise (ArgumentOutOfRangeException("count"))
        NativeBlobAccess.read(&state, Span(buffer, offset, count))
#if NETSTANDARD2_1 || NET6_0_OR_GREATER
    override _.Read(buffer: Span<byte>) = NativeBlobAccess.read(&state, buffer)
#else
    member _.Read(buffer: Span<byte>) = NativeBlobAccess.read(&state, buffer)
#endif
    override this.ReadAsync(buffer, offset, count, cancellationToken: CancellationToken) =
        if cancellationToken.IsCancellationRequested then Task.FromCanceled<int>(cancellationToken)
        else
            try Task.FromResult(this.Read(buffer, offset, count))
            with error -> Task.FromException<int>(error)
#if NETSTANDARD2_1 || NET6_0_OR_GREATER
    override this.ReadAsync(buffer: Memory<byte>, [<System.Runtime.InteropServices.Optional>] cancellationToken: CancellationToken) =
        if cancellationToken.IsCancellationRequested then ValueTask<int>(Task.FromCanceled<int>(cancellationToken))
        else
            try ValueTask<int>(this.Read(buffer.Span))
            with error -> ValueTask<int>(Task.FromException<int>(error))
#endif
    override _.Seek(offset, origin) = NativeBlobAccess.seek(&state, offset, origin)
    override _.Flush() = NativeBlobAccess.check &state
    override _.SetLength(_) = raise (NotSupportedException("The length of a SQLite BLOB cannot be changed by its stream."))
    override _.Write(buffer, offset, count) =
        if isNull buffer then nullArg "buffer"
        if offset < 0 || offset > buffer.Length then raise (ArgumentOutOfRangeException("offset"))
        if count < 0 || count > buffer.Length-offset then raise (ArgumentOutOfRangeException("count"))
        NativeBlobAccess.write(&state, ReadOnlySpan(buffer, offset, count))
#if NETSTANDARD2_1 || NET6_0_OR_GREATER
    override _.Write(buffer: ReadOnlySpan<byte>) = NativeBlobAccess.write(&state, buffer)
#else
    member _.Write(buffer: ReadOnlySpan<byte>) = NativeBlobAccess.write(&state, buffer)
#endif
    override this.WriteAsync(buffer, offset, count, cancellationToken: CancellationToken) =
        if cancellationToken.IsCancellationRequested then Task.FromCanceled(cancellationToken)
        else
            try this.Write(buffer, offset, count); Task.CompletedTask
            with error -> Task.FromException(error)
#if NETSTANDARD2_1 || NET6_0_OR_GREATER
    override this.WriteAsync(buffer: ReadOnlyMemory<byte>, [<System.Runtime.InteropServices.Optional>] cancellationToken: CancellationToken) =
        if cancellationToken.IsCancellationRequested then ValueTask(Task.FromCanceled(cancellationToken))
        else
            try this.Write(buffer.Span); ValueTask()
            with error -> ValueTask(Task.FromException(error))
#endif
    override _.Dispose(disposing) =
        try NativeBlobAccess.close(&state, disposing)
        finally base.Dispose(disposing)

module internal NativeColumnStreams =
    let openStream (database: NativeDatabase, handle: sqlite3_stmt, ordinal: int, readBlob: int -> byte[], readId: int -> int64, source: NativeBlobSource voption) : Stream =
        let mutable identity = ValueNone
        match source with
        | ValueSome source when source.Columns.Length = raw.sqlite3_column_count handle ->
            let selected = source.Columns.[ordinal]
            let sameOrigin index (column: NativeBlobColumn) =
                NativeBlobIdentity.equalIdentifier source.Schema (raw.sqlite3_column_database_name(handle,index).utf8_to_string())
                && NativeBlobIdentity.equalIdentifier source.Table (raw.sqlite3_column_table_name(handle,index).utf8_to_string())
                && NativeBlobIdentity.equalIdentifier column.OriginName (raw.sqlite3_column_origin_name(handle,index).utf8_to_string())
            if selected.Stored && sameOrigin ordinal selected then
                let mutable i = 0
                while identity.IsNone && i<source.Columns.Length do
                    let candidate=source.Columns.[i]
                    if i<>ordinal && candidate.Rowid && sameOrigin i candidate && raw.sqlite3_column_type(handle,i)=raw.SQLITE_INTEGER then
                        identity <- ValueSome(struct(source.Schema,source.Table,selected.PhysicalName,readId i))
                    i<-i+1
        | _ -> ()
        match identity with
        | ValueNone -> new MemoryStream(readBlob ordinal,false) :> Stream
        | ValueSome(struct(schema,table,column,id)) ->
            let state = NativeBlobAccess.openBlob database schema table column id true
            new NativeBlobStream(state) :> Stream
