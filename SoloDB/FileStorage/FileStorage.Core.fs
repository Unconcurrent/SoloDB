namespace SoloDatabase

open System
open System.IO
open Microsoft.Data.Sqlite
open SQLiteTools
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
    /// <summary>
    /// Provides a stream-based interface for reading from and writing to a file stored inside the SQLite database.
    /// File data is broken into fixed-size chunks compressed with Snappy. All write operations are transactional.
    /// </summary>
    type DbFileStream internal (db: Connection, fileId: int64, directoryId: int64, fullPath: string) =
        inherit Stream()

        let mutable position = 0L
        let mutable disposed = false
        let mutable dirty = false

        let checkDisposed() = if disposed then raise (ObjectDisposedException(nameof(DbFileStream)))

        member internal this.UpdateModified() =
            if dirty then
                db.WithTransaction(fun conn ->
                    ignore (conn.Execute (updateModifiedTimestampSQL, {|FileId = fileId; DirId = directoryId|}))
                )

        override _.CanRead = not disposed
        override _.CanSeek = not disposed
        override _.CanWrite = not disposed

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
        member this.FullPath = fullPath

        override this.Flush() =
            checkDisposed()
            this.UpdateModified()

        #if NETSTANDARD2_1_OR_GREATER
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
            if buffer = null then raise (ArgumentNullException(nameof buffer))
            if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
            if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
            if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))
            this.Read(Span<byte>(buffer, offset, count))

        #if NETSTANDARD2_1_OR_GREATER
        override
        #else
        member
        #endif
            this.Write(buffer: ReadOnlySpan<byte>) =
            if buffer.IsEmpty then () else
            let bufferLen = buffer.Length
            use buffer = fixed buffer

            db.WithTransaction(fun db ->
                let buffer = ReadOnlySpan<byte>(NativeInterop.NativePtr.toVoidPtr buffer, bufferLen)
                let newPosition = writeChunkedData db fileId position buffer

                if newPosition > getFileLengthById db fileId then
                    updateLenById db fileId newPosition

                this.Position <- newPosition
            )
            dirty <- true

        override this.Write(buffer: byte[], offset: int, count: int) =
            if buffer = null then raise (ArgumentNullException(nameof buffer))
            if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
            if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
            if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))
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
                let len = getFileLengthById db fileId
                if len = value then
                    ()
                elif len < value then
                    shouldMarkDirty <- true
                    let chunkOffset = len % chunkSize
                    let fillSize = int (chunkSize - chunkOffset)
                    use mem = NativeArray.NativeArray.Alloc fillSize
                    let mem = mem.Span
                    mem.Clear()
                    ignore (writeChunkedData db fileId len (Span.op_Implicit mem))
                    updateLenById db fileId value
                else
                    shouldMarkDirty <- true
                    downsetFileLength db fileId value
                    shouldClampPosition <- position > value
            )

            if shouldClampPosition then position <- value
            if shouldMarkDirty then dirty <- true

        override this.Seek(offset: int64, origin: SeekOrigin) =
            checkDisposed()
            let newPosition =
                match origin with
                | SeekOrigin.Begin ->
                    if offset < 0L then raise (ArgumentOutOfRangeException(nameof offset, offset, "Seek offset from Begin must not be negative."))
                    offset
                | SeekOrigin.Current -> position + offset
                | SeekOrigin.End ->
                    let len = this.Length
                    len + offset
                | other -> failwithf "Invalid SeekOrigin: %A" other
            position <- newPosition
            position

        override this.Dispose(disposing) =
            if not disposed then
                this.Flush()
            disposed <- true
            ()
