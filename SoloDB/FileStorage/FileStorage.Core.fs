namespace SoloDatabase

open System
open System.IO
open NativeSQLiteTools
open SoloDatabase.Types
open NativeArray
open Snappier
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

        let checkDisposed() =
            db.CheckAccess()
            if disposed then raise (ObjectDisposedException(nameof(DbFileStream)))

        member internal this.UpdateModified() =
            if dirty then
                db.WithTransaction(fun conn ->
                    updateModifiedTimestamp conn fileId
                )

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

        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        override this.Read(buffer: Span<byte>) =
        #else
        member this.Read(buffer: Span<byte>) =
        #endif
            checkDisposed()
            if buffer.IsEmpty then 0 else

            let len               = this.Length
            let currentPosition   = position
            let remainingBytes    = len - currentPosition
            if remainingBytes <= 0L then 0 else

            let bytesToReadTotal  = int (min (int64 buffer.Length) remainingBytes)
            let buffer            = buffer.Slice(0, bytesToReadTotal)

            use db = db.Get()
            let startChunk        = currentPosition / chunkSize
            let endChunk          = (currentPosition + int64 bytesToReadTotal - 1L) / chunkSize

            let mutable bytesWrittenToBuffer    = 0
            let mutable chunkDecompressionBuffer = NativeArray.NativeArray.Empty

            try
                for chunk in getAllCompressedChunksWithinRange db fileId startChunk endChunk do
                    let chunkStartPos = chunk.Number * chunkSize
                    let copyFrom      = max currentPosition chunkStartPos
                    let copyTo        = min (currentPosition + int64 bytesToReadTotal) (chunkStartPos + chunkSize)
                    let bytesInChunk  = int (copyTo - copyFrom)

                    if bytesInChunk > 0 then
                        let bufferOffset    = int (copyFrom - currentPosition)
                        let destinationSpan = buffer.Slice(bufferOffset, bytesInChunk)

                        if chunk.Data.Length = 0 then
                            destinationSpan.Fill(0uy)
                            bytesWrittenToBuffer <- bytesWrittenToBuffer + bytesInChunk
                        else
                            let offsetInChunk = int (copyFrom - chunkStartPos)
                            let written =
                                if offsetInChunk = 0 then
                                    match Snappy.TryDecompress(chunk.Data.Span, destinationSpan) with
                                    | _, 0 ->
                                        failwithf "Failed to decompress chunk #%d." chunk.Number
                                    | _, decompressed when decompressed <> destinationSpan.Length ->
                                        failwithf "Decompressed %d bytes but expected %d." decompressed destinationSpan.Length
                                    | _, decompressed ->
                                        decompressed
                                else
                                    if chunkDecompressionBuffer.Length = 0 then
                                        chunkDecompressionBuffer <- NativeArray.NativeArray.Alloc (int maxChunkStoreSize)
                                    let decompBuf = chunkDecompressionBuffer.Span
                                    let _ = Snappy.Decompress(chunk.Data.Span, decompBuf)
                                    decompBuf.Slice(offsetInChunk, bytesInChunk).CopyTo(destinationSpan)
                                    bytesInChunk
                            bytesWrittenToBuffer <- bytesWrittenToBuffer + written
            finally
                chunkDecompressionBuffer.Dispose()
                this.Position <- currentPosition + int64 bytesWrittenToBuffer

            bytesWrittenToBuffer

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

        #if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
        override
        #else
        member
        #endif
            this.Write(buffer: ReadOnlySpan<byte>) =
            db.CheckAccess()
            if buffer.IsEmpty then () else
            checkDisposed()
            let startPosition = position
            let bufferLen = buffer.Length
            if startPosition > Int64.MaxValue - int64 bufferLen then
                raise (IOException("The write exceeds the supported file range."))
            let newPosition = startPosition + int64 bufferLen
            use buffer = fixed buffer
            db.WithTransaction(fun db ->
                let oldLength = getFileLengthById db fileId
                let facts = FileStorageContentEvents.prepare db {
                    FileId = fileId; Offset = startPosition; Length = int64 bufferLen
                    OldLength = oldLength; NewLength = max oldLength newPosition
                    Kind = FileContentChangeKind.Write }
                checkDisposed()
                let data = ReadOnlySpan<byte>(NativeInterop.NativePtr.toVoidPtr buffer, bufferLen)
                writeChunkedData db fileId startPosition data |> ignore
                if facts.NewLength > facts.OldLength then updateLenById db fileId facts.NewLength
                FileStorageContentEvents.completed db facts)
            position <- newPosition
            dirty <- true

        override this.Write(buffer: byte[], offset: int, count: int) =
            validateBufferArguments buffer offset count
            this.Write(buffer.AsSpan(offset, count))

        override this.WriteAsync(buffer: byte[], offset: int, count: int, ct) =
            ct.ThrowIfCancellationRequested()
            this.Write(buffer, offset, count)
            Threading.Tasks.Task.CompletedTask

        override this.SetLength(value: int64) =
            checkDisposed()
            if value < 0 then
                raise (ArgumentOutOfRangeException("Length"))

            let mutable shouldMarkDirty = false
            let mutable shouldClampPosition = false

            db.WithTransaction(fun db ->
                let originalLength = getFileLengthById db fileId
                if originalLength <> value then
                    let facts = FileStorageContentEvents.prepare db {
                        FileId = fileId; Offset = min originalLength value
                        Length = abs (originalLength - value); OldLength = originalLength
                        NewLength = value; Kind = FileContentChangeKind.Resize }
                    checkDisposed()
                    let len = facts.OldLength
                    if len < value then
                        let chunkOffset = len % chunkSize
                        let fillSize = int (chunkSize - chunkOffset)
                        use mem = NativeArray.NativeArray.Alloc fillSize
                        let mem = mem.Span
                        mem.Clear()
                        writeChunkedData db fileId len (Span.op_Implicit mem) |> ignore
                        updateLenById db fileId value
                    elif len > value then downsetFileLength db fileId value
                    shouldMarkDirty <- len <> value
                    shouldClampPosition <- position > value
                    FileStorageContentEvents.completed db facts
            )

            if shouldClampPosition then position <- value
            if shouldMarkDirty then dirty <- true

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
