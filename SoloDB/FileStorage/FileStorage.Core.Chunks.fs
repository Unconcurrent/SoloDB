namespace SoloDatabase

open System
open System.Buffers
open NativeSQLiteTools
open Snappier
open SoloDatabase.Types
open FileStorageCore

// NativePtr operations for efficient chunk-based file I/O
#nowarn "9"

module internal FileStorageCoreChunks =
    /// One operation owns both rentals through its final native consumer.
    [<Sealed>]
    type internal ChunkWriteScratch internal () =
        let plain = ArrayPool<byte>.Shared.Rent(int chunkSize)
        let compressed =
            try ArrayPool<byte>.Shared.Rent(int maxChunkStoreSize)
            with _ -> ArrayPool<byte>.Shared.Return(plain); reraise()
        let mutable disposed = false
        member internal _.Plain = plain.AsSpan(0, int chunkSize)
        member internal _.Compressed = compressed.AsSpan(0, int maxChunkStoreSize)
        member internal _.CompressedArray = compressed
        interface IDisposable with
            member _.Dispose() =
                if not disposed then
                    disposed <- true
                    ArrayPool<byte>.Shared.Return(compressed)
                    ArrayPool<byte>.Shared.Return(plain)

    let private readTreeEntry (statement: byref<NativeStatementScope>) =
        try
            let row = statement.Row
            let path = row.GetString(3)
            let metadata = System.Collections.Generic.Dictionary<string, string>()
            let entry =
                match row.GetString(2) with
                | "File" -> SoloDBEntryHeader.File {
                    Id = row.GetInt64(1); Name = row.GetString(4); FullPath = path
                    DirectoryId = row.GetInt64(8); Length = row.GetInt64(5)
                    Created = SqlRowValue.Read<DateTimeOffset>(row, 6)
                    Modified = SqlRowValue.Read<DateTimeOffset>(row, 7); Metadata = metadata }
                | "Directory" -> SoloDBEntryHeader.Directory {
                    Id = row.GetInt64(1); Name = row.GetString(4); FullPath = path
                    ParentId = if row.IsDBNull(8) then Nullable() else Nullable(row.GetInt64(8))
                    Created = SqlRowValue.Read<DateTimeOffset>(row, 6)
                    Modified = SqlRowValue.Read<DateTimeOffset>(row, 7); Metadata = metadata }
                | other -> failwithf "Invalid entry type: %s" other
            let mutable sameEntry = true
            while sameEntry do
                let row = statement.Row
                if not (row.IsDBNull(9)) then metadata.[row.GetString(9)] <- row.GetString(10)
                sameEntry <- statement.Step() && statement.Row.GetString(3) = path
            entry
        with error -> statement.Abort(error); reraise()

    let private openTree (db: NativeSqliteConnection) sql path =
        let mutable statement = db.OpenStatement(sql)
        try
            statement.BindText(1, path)
            statement.Step() |> ignore
            statement
        with error -> statement.Abort(error); reraise()

    let internal recursiveListAllEntriesInDirectory (db: NativeSqliteConnection) (directoryFullPath: string) =
        let directoryFullPath = [|directoryFullPath|] |> combinePathArr
        let queryCommand = """
            WITH RECURSIVE DirectoryTree AS (
                SELECT Id, Name, FullPath, ParentId, Created, Modified, 0 AS Level
                FROM SoloDBDirectoryHeader WHERE FullPath = @FullPath
                UNION ALL
                SELECT d.Id, d.Name, d.FullPath, d.ParentId, d.Created, d.Modified, dt.Level + 1 AS Level
                FROM SoloDBDirectoryHeader d
                INNER JOIN DirectoryTree dt ON d.ParentId = dt.Id
            )
            SELECT dt.Level, dt.Id as Id, 'Directory' AS Type, dt.FullPath AS Path, dt.Name AS Name,
               	0 AS Size, dt.Created as Created, dt.Modified as Modified,
                dt.ParentId as DirectoryId, dm.Key AS MetadataKey, dm.Value AS MetadataValue
            FROM DirectoryTree dt
            LEFT JOIN SoloDBDirectoryMetadata dm ON dt.Id = dm.DirectoryId
            UNION ALL
            SELECT dt.Level + 1 AS Level, fh.Id as Id, 'File' AS Type, fh.FullPath AS Path, fh.Name AS Name,
               	fh.Length AS Size, fh.Created as Created, fh.Modified as Modified,
                fh.DirectoryId as DirectoryId, fm.Key AS MetadataKey, fm.Value AS MetadataValue
            FROM DirectoryTree dt
            INNER JOIN SoloDBFileHeader fh ON dt.Id = fh.DirectoryId
            LEFT JOIN SoloDBFileMetadata fm ON fh.Id = fm.FileId"""

        seq {
            let mutable statement = openTree db queryCommand directoryFullPath
            try
                while statement.HasRow do yield readTreeEntry &statement
            finally statement.Dispose()
        }

    let internal getFileLengthById (db: NativeSqliteConnection) (fileId: int64) =
        let mutable statement = db.OpenStatement("SELECT Length FROM SoloDBFileHeader WHERE Id = @FileId")
        try
            statement.BindInt64(1, fileId)
            if not (statement.Step()) then Seq.head (Seq.empty<unit>)
            let length = statement.Row.GetInt64(0)
            statement.Dispose()
            length
        with error -> statement.Abort(error); reraise()

    let internal tryGetChunkData (db: NativeSqliteConnection) (fileId: int64) (chunkNumber: int64) (buffer: Span<byte>) =
        let mutable statement = db.OpenStatement("SELECT Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number = @ChunkNumber")
        let length =
            try
                statement.BindInt64(1, fileId)
                statement.BindInt64(2, chunkNumber)
                let length =
                    if statement.Step() && not (statement.Row.IsDBNull(0)) then
                        let data = SQLitePCL.raw.sqlite3_column_blob(statement.Handle, 0)
                        Snappy.Decompress(data, buffer)
                    else 0
                statement.Dispose()
                length
            with error -> statement.Abort(error); reraise()
        buffer.Slice(0, length)

    let private storeChunk (db: NativeSqliteConnection) fileId chunkNumber (scratch: ChunkWriteScratch) length =
        let mutable statement = db.OpenStatement("INSERT INTO SoloDBFileChunk(FileId, Number, Data) VALUES (@FileId, @Number, @Data) ON CONFLICT(FileId, Number) DO UPDATE SET Data = excluded.Data")
        try
            statement.BindInt64(1, fileId)
            statement.BindInt64(2, chunkNumber)
            statement.BindBlob(3, scratch.CompressedArray, length)
            statement.Step() |> ignore
            let result = statement.RecordsAffected
            statement.Dispose()
            if result <> 1 then failwith "writeChunkData failed."
        with error -> statement.Abort(error); reraise()

    let private writeChunkData db fileId chunkNumber (data: Span<byte>) (scratch: ChunkWriteScratch) =
        let length = Snappy.Compress(data, scratch.Compressed)
        storeChunk db fileId chunkNumber scratch length

    /// Clear the stored boundary suffix without forming an absolute chunk end.
    let private clearChunkTailWithScratch (db: NativeSqliteConnection) (fileId: int64) (position: int64) (scratch: ChunkWriteScratch) =
        let chunkNumber = position / chunkSize
        let retainedBytes = int (position % chunkSize)
        let bytes = scratch.Plain
        bytes.Clear()
        let stored = tryGetChunkData db fileId chunkNumber bytes
        if not stored.IsEmpty then
            bytes.Slice(retainedBytes).Clear()
            writeChunkData db fileId chunkNumber bytes scratch

    let internal clearChunkTail db fileId position =
        use scratch = new ChunkWriteScratch()
        clearChunkTailWithScratch db fileId position scratch

    /// Truncate within the caller's transaction, including the discarded tail of
    /// a retained chunk so a subsequent sparse write cannot reveal old bytes.
    let internal downsetFileLength (db: NativeSqliteConnection) (fileId: int64) (newFileLength: int64) =
        let lastChunk = if newFileLength = 0L then -1L else (newFileLength - 1L) / chunkSize
        if newFileLength % chunkSize <> 0L then clearChunkTail db fileId newFileLength
        let mutable statement = db.OpenStatement("DELETE FROM SoloDBFileChunk WHERE FileId = @FileId AND Number > @LastChunkNumber")
        try
            statement.BindInt64(1, fileId)
            statement.BindInt64(2, lastChunk)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
        updateLenById db fileId newFileLength

    /// Consume each borrowed SQLite value before advancing the private statement.
    let internal readChunkedData (db: NativeSqliteConnection) fileId position (destination: Span<byte>) (processed: byref<int>) =
        let firstChunk = position / chunkSize
        let ending = position + int64 destination.Length
        let lastChunk = (ending - 1L) / chunkSize
        let mutable partial = Array.empty<byte>
        let mutable statement = db.OpenStatement("SELECT Number, Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number >= @StartChunk AND Number <= @EndChunk ORDER BY Number")
        try
            try
                statement.BindInt64(1, fileId)
                statement.BindInt64(2, firstChunk)
                statement.BindInt64(3, lastChunk)
                let mutable stored = statement.Step()
                let mutable storedNumber = if stored then statement.Row.GetInt64(0) else -1L
                let mutable number = firstChunk
                while number <= lastChunk do
                    let offset = if number = firstChunk then int(position % chunkSize) else 0
                    let count = min (int chunkSize - offset) (destination.Length - processed)
                    let target = destination.Slice(processed, count)
                    if stored && storedNumber = number then
                        let compressed = SQLitePCL.raw.sqlite3_column_blob(statement.Handle, 1)
                        if compressed.IsEmpty then target.Clear()
                        elif offset = 0 then
                            match Snappy.TryDecompress(compressed, target) with
                            | _, 0 -> failwithf "Failed to decompress chunk #%d." number
                            | _, decoded when decoded <> target.Length ->
                                failwithf "Decompressed %d bytes but expected %d." decoded target.Length
                            | _ -> ()
                        else
                            if partial.Length = 0 then partial <- ArrayPool<byte>.Shared.Rent(int chunkSize)
                            let bytes = partial.AsSpan(0, int chunkSize)
                            let decoded = Snappy.Decompress(compressed, bytes)
                            if decoded < bytes.Length then bytes.Slice(decoded).Clear()
                            bytes.Slice(offset, count).CopyTo(target)
                        processed <- processed + count
                        stored <- statement.Step()
                        storedNumber <- if stored then statement.Row.GetInt64(0) else -1L
                    else
                        target.Clear()
                        processed <- processed + count
                    number <- number + 1L
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        finally
            if partial.Length <> 0 then ArrayPool<byte>.Shared.Return(partial)

    let internal setFileModifiedById (db: NativeSqliteConnection) (fileId: int64) (modified: DateTimeOffset) =
        let mutable statement = db.OpenStatement("UPDATE SoloDBFileHeader SET Modified = @Timestamp WHERE Id = @FileId")
        try
            statement.BindInt64(1, modified.ToUnixTimeMilliseconds())
            statement.BindInt64(2, fileId)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let internal setFileCreatedById (db: NativeSqliteConnection) (fileId: int64) (created: DateTimeOffset) =
        let mutable statement = db.OpenStatement("UPDATE SoloDBFileHeader SET Created = @Timestamp WHERE Id = @FileId")
        try
            statement.BindInt64(1, created.ToUnixTimeMilliseconds())
            statement.BindInt64(2, fileId)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let internal updateModifiedTimestamp (db: NativeSqliteConnection) fileId =
        let mutable file = db.OpenStatement("UPDATE SoloDBFileHeader SET Modified = UNIXTIMESTAMP() WHERE Id = @FileId AND Modified < UNIXTIMESTAMP()")
        try
            file.BindInt64(1, fileId)
            file.Step() |> ignore
            file.Dispose()
        with error -> file.Abort(error); reraise()
        let mutable directory = db.OpenStatement("UPDATE SoloDBDirectoryHeader SET Modified = UNIXTIMESTAMP() WHERE Id = (SELECT DirectoryId FROM SoloDBFileHeader WHERE Id = @FileId) AND Modified < UNIXTIMESTAMP()")
        try
            directory.BindInt64(1, fileId)
            directory.Step() |> ignore
            directory.Dispose()
        with error -> directory.Abort(error); reraise()

    let internal writeChunkedDataWithScratch (db: NativeSqliteConnection) (fileId: int64) (position: int64)
                                            (buffer: ReadOnlySpan<byte>) (originalLength: int64) (scratch: ChunkWriteScratch) =
        if buffer.IsEmpty then position else
        let ending = position + int64 buffer.Length
        let firstChunk = position / chunkSize
        let lastChunk = (ending - 1L) / chunkSize
        let oldBoundary = originalLength / chunkSize
        // Older truncation could leave a stored boundary suffix past logical EOF.
        // Sparse growth need only repair that boundary, never materialize the gap.
        if position > originalLength && originalLength % chunkSize <> 0L && oldBoundary < firstChunk then
            clearChunkTailWithScratch db fileId originalLength scratch
        let mutable consumed = 0
        let mutable number = firstChunk
        while number <= lastChunk do
            let offset = if number = firstChunk then int(position % chunkSize) else 0
            let count = min (int chunkSize - offset) (buffer.Length - consumed)
            let data = buffer.Slice(consumed, count)
            let bytes = scratch.Plain
            let partial = offset <> 0 || count <> int chunkSize
            let mutable unchanged = false
            if partial then
                let stored = tryGetChunkData db fileId number bytes
                if stored.Length < bytes.Length then bytes.Slice(stored.Length).Clear()
                // The logical effects and any length change remain with the stream.
                unchanged <- ending <= originalLength && not stored.IsEmpty && data.SequenceEqual(bytes.Slice(offset, count))
                if ending > originalLength && number = oldBoundary then
                    bytes.Slice(int(originalLength % chunkSize)).Clear()
            if not unchanged then
                data.CopyTo(bytes.Slice(offset, count))
                writeChunkData db fileId number bytes scratch
            consumed <- consumed + count
            number <- number + 1L
        ending

    let internal writeChunkedData db fileId position (buffer: ReadOnlySpan<byte>) originalLength =
        use scratch = new ChunkWriteScratch()
        writeChunkedDataWithScratch db fileId position buffer originalLength scratch
