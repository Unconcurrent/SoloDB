namespace SoloDatabase

open System
open System.IO
open NativeSQLiteTools
open SoloDatabase.Types
open System.Runtime.InteropServices
open SoloDatabase.Connections
open FileStorageCore
open FileStorageCoreChunks

// NativePtr operations for efficient chunk-based file I/O
#nowarn "9"

module FileStorageCoreStream =
    let private validateBufferArguments (buffer: byte[]) offset count =
        if isNull buffer then raise (ArgumentNullException(nameof buffer))
        if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
        if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
        if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))

    /// <summary>
    /// Provides a stream-based interface for reading from and writing to a file stored inside the SQLite database.
    /// File data is broken into fixed-size chunks compressed with Snappy. All write operations are transactional.
    /// </summary>
    type DbFileStream internal (db: NativeConnection, fileId: int64, fullPath: string) =
        inherit Stream()

        let mutable position = 0L
        let mutable disposed = false
        let mutable dirty = false
        let mutable mutationVersion = 0L

        let checkDisposed() =
            db.CheckAccess()
            if disposed then raise (ObjectDisposedException(nameof(DbFileStream)))

        member internal this.UpdateModified() =
            if dirty then
                let version = mutationVersion
                db.WithTransaction(fun conn ->
                    updateModifiedTimestamp conn fileId
                )
                // A metadata callback may have written through this same stream.
                if mutationVersion = version then dirty <- false

        override _.CanRead = db.CheckAccess(); not disposed
        override _.CanSeek = db.CheckAccess(); not disposed
        override _.CanWrite = db.CheckAccess(); not disposed

        override _.Length =
            checkDisposed()
            use db = db.Get()
            getFileLengthById db fileId

        override this.Position
            with get() =
                checkDisposed()
                position
            and set(value) =
                checkDisposed()
                this.Seek(value, SeekOrigin.Begin) |> ignore

        /// <summary>
        /// Returns the FullPath of the file at the moment of opening of the stream.
        /// </summary>
        member this.FullPath = db.CheckAccess(); fullPath

        override this.Flush() =
            checkDisposed()
            this.UpdateModified()

        member private this.ReadFrom(source: NativeConnection, requestedPosition: int64, buffer: Span<byte>) =
            checkDisposed()
            if buffer.IsEmpty then 0 else

            use connection = source.Get()
            let len = getFileLengthById connection fileId
            if len < 0L then raise(InvalidDataException("The Stream returned a negative length."))
            let currentPosition = requestedPosition
            let remaining = len - currentPosition
            if remaining <= 0L then 0 else
            let count = int(min (int64 buffer.Length) remaining)
            position <- currentPosition
            let mutable processed = 0
            try
                readChunkedData connection fileId currentPosition (buffer.Slice(0, count)) &processed
                processed
            finally
                this.Position <- currentPosition + int64 processed

        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        override this.Read(buffer: Span<byte>) = this.ReadFrom(db, position, buffer)
        #else
        member this.Read(buffer: Span<byte>) = this.ReadFrom(db, position, buffer)
        #endif

        override this.Read(buffer: byte[], offset: int, count: int) : int =
            validateBufferArguments buffer offset count
            this.Read(Span<byte>(buffer, offset, count))

        // Native statements and callback dispatch remain owned by the invoking thread.
        // Outside handlers the standard asynchronous stream behavior is preserved.
        override this.ReadAsync(buffer: byte[], offset: int, count: int, cancellationToken) =
            if not db.IsEventHandlerScope then base.ReadAsync(buffer, offset, count, cancellationToken)
            else
                validateBufferArguments buffer offset count
                if not this.CanRead then raise (NotSupportedException("Stream does not support reading."))
                if cancellationToken.IsCancellationRequested then Threading.Tasks.Task.FromCanceled<int>(cancellationToken)
                else
                    try Threading.Tasks.Task.FromResult(this.Read(buffer, offset, count))
                    with ex -> Threading.Tasks.Task.FromException<int>(ex)

        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        override this.ReadAsync(buffer: Memory<byte>, [<Optional>] cancellationToken: Threading.CancellationToken) =
            if not db.IsEventHandlerScope then base.ReadAsync(buffer, cancellationToken)
            else
                if not this.CanRead then raise (NotSupportedException("Stream does not support reading."))
                if cancellationToken.IsCancellationRequested then
                    Threading.Tasks.ValueTask<int>(Threading.Tasks.Task.FromCanceled<int>(cancellationToken))
                else
                    try Threading.Tasks.ValueTask<int>(this.Read(buffer.Span))
                    with ex -> Threading.Tasks.ValueTask<int>(Threading.Tasks.Task.FromException<int>(ex))
        #endif

        member private this.WriteFrom(source: NativeConnection, buffer: ReadOnlySpan<byte>, scratch: ChunkWriteScratch voption) =
            db.CheckAccess()
            if buffer.IsEmpty then () else
            checkDisposed()
            let startPosition = position
            let bufferLen = buffer.Length
            if startPosition > Int64.MaxValue - int64 bufferLen then
                raise (IOException("The write exceeds the supported file range."))
            let newPosition = startPosition + int64 bufferLen
            use buffer = fixed buffer
            let perform db =
                let oldLength = getFileLengthById db fileId
                if oldLength < 0L then raise(InvalidDataException("The Stream returned a negative length."))
                let facts = FileStorageContentEvents.prepare db {
                    FileId = fileId; Offset = startPosition; Length = int64 bufferLen
                    OldLength = oldLength; NewLength = max oldLength newPosition
                    Kind = FileContentChangeKind.Write }
                checkDisposed()
                let data = ReadOnlySpan<byte>(NativeInterop.NativePtr.toVoidPtr buffer, bufferLen)
                match scratch with
                | ValueSome scratch -> writeChunkedDataWithScratch db fileId startPosition data facts.OldLength scratch |> ignore
                | ValueNone -> writeChunkedData db fileId startPosition data facts.OldLength |> ignore
                if facts.NewLength > facts.OldLength then updateLenById db fileId facts.NewLength
                FileStorageContentEvents.completed db facts
            match scratch, source with
            | ValueSome _, NativeTransactional connection -> perform connection
            | _ -> source.WithTransaction perform
            position <- newPosition
            dirty <- true
            mutationVersion <- mutationVersion + 1L

        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        override this.Write(buffer: ReadOnlySpan<byte>) = this.WriteFrom(db, buffer, ValueNone)
        #else
        member this.Write(buffer: ReadOnlySpan<byte>) = this.WriteFrom(db, buffer, ValueNone)
        #endif

        override this.Write(buffer: byte[], offset: int, count: int) =
            validateBufferArguments buffer offset count
            this.Write(buffer.AsSpan(offset, count))

        override this.WriteAsync(buffer: byte[], offset: int, count: int, ct) =
            ct.ThrowIfCancellationRequested()
            this.Write(buffer, offset, count)
            Threading.Tasks.Task.CompletedTask

        member private this.SetLengthFrom(source: NativeConnection, value: int64) =
            checkDisposed()
            if value < 0 then
                raise (ArgumentOutOfRangeException("Length"))

            let mutable shouldMarkDirty = false
            let mutable shouldClampPosition = false

            source.WithTransaction(fun db ->
                let originalLength = getFileLengthById db fileId
                if originalLength < 0L then raise(InvalidDataException("The Stream returned a negative length."))
                if originalLength <> value then
                    let facts = FileStorageContentEvents.prepare db {
                        FileId = fileId; Offset = min originalLength value
                        Length = abs (originalLength - value); OldLength = originalLength
                        NewLength = value; Kind = FileContentChangeKind.Resize }
                    checkDisposed()
                    let len = facts.OldLength
                    if len < value then
                        clearChunkTail db fileId len
                        updateLenById db fileId value
                    elif len > value then downsetFileLength db fileId value
                    shouldMarkDirty <- len <> value
                    shouldClampPosition <- position > value
                    FileStorageContentEvents.completed db facts
            )

            if shouldClampPosition then position <- value
            if shouldMarkDirty then
                dirty <- true
                mutationVersion <- mutationVersion + 1L

        override this.SetLength(value: int64) = this.SetLengthFrom(db, value)

        member private this.WriteAtFrom(source, offset, buffer: ReadOnlySpan<byte>, scratch) =
            checkDisposed()
            StorageValidation.bounds offset buffer.Length
            if not buffer.IsEmpty then
                position <- offset
                this.WriteFrom(source, buffer, scratch)

        member private _.Source = db
        member private _.FileId = fileId

        interface IStorageParentStream with
            member this.ByteFile =
                { new ISqliteStorageFile with
                    member _.ReadAt(offset, buffer) =
                        StorageValidation.bounds offset buffer.Length
                        StorageReadResult(this.ReadFrom(db, offset, buffer), null)
                    member _.WriteAt(offset, buffer) =
                        this.WriteAtFrom(db, offset, buffer, ValueNone)
                        null
                    member _.GetLength() =
                        let length = this.Length
                        if length < 0L then raise(InvalidDataException("The Stream returned a negative length."))
                        StorageLengthResult(length, null)
                    member _.SetLength(length) = this.SetLengthFrom(db, length); null
                    member _.Flush() = this.Flush(); null }
            member this.PairWith(auxiliary) =
                match db, auxiliary with
                | NativePooled pool, (:? DbFileStream as other) when fileId <> other.FileId && not pool.IsReadOnly ->
                    match other.Source with
                    | NativePooled otherPool when Object.ReferenceEquals(pool, otherPool) ->
                        { new IStorageParentBatchFactory with
                            member _.Begin() =
                                let transaction = pool.BeginStorageTransaction()
                                let scratch =
                                    try new ChunkWriteScratch()
                                    with error -> transaction.Fail(error); reraise()
                                let source = NativeTransactional transaction.Connection
                                let mutable active = true
                                { new IStorageParentBatch with
                                    member _.Write(auxiliary, offset, buffer) =
                                        if not active then invalidOp "The storage batch has completed."
                                        let target = if auxiliary then other else this
                                        target.WriteAtFrom(source, offset, buffer, ValueSome scratch)
                                    member _.Complete() =
                                        if active then
                                            active <- false
                                            try transaction.Complete()
                                            finally (scratch :> IDisposable).Dispose()
                                    member _.Fail(error) =
                                        if active then
                                            active <- false
                                            try transaction.Fail(error)
                                            finally (scratch :> IDisposable).Dispose() } }
                    | _ -> null
                | _ -> null

        override this.Seek(offset: int64, origin: SeekOrigin) =
            checkDisposed()
            let relativePosition basis =
                if offset > 0L && basis > Int64.MaxValue - offset then
                    raise (IOException("The seek position exceeds the supported file range."))
                let target = basis + offset
                if target < 0L then
                    raise (IOException("Attempted to seek before the beginning of the file."))
                target
            let newPosition =
                match origin with
                | SeekOrigin.Begin ->
                    if offset < 0L then raise (ArgumentOutOfRangeException(nameof offset, offset, "Seek offset from Begin must not be negative."))
                    offset
                | SeekOrigin.Current -> relativePosition position
                | SeekOrigin.End -> relativePosition this.Length
                | other -> failwithf "Invalid SeekOrigin: %A" other
            position <- newPosition
            position

        override this.Dispose(disposing) =
            if not disposed then
                try
                    this.Flush()
                finally
                    disposed <- true
            ()
