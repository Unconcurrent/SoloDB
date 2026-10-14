namespace SoloDatabase

open System
open NativeSQLiteTools
open SQLiteToolsRowConversions
open NativeArray
open Snappier
open SoloDatabase.Types
open FileStorageCore

// NativePtr operations for efficient chunk-based file I/O
#nowarn "9"

module internal FileStorageCoreChunks =
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

    let internal writeChunkData (db: NativeSqliteConnection) (fileId: int64) (chunkNumber: int64) (data: Span<byte>) =
        let arrayBuffer = Array.zeroCreate (int maxChunkStoreSize)
        let len = Snappy.Compress(data, arrayBuffer.AsSpan())
        let mutable statement = db.OpenStatement("INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) VALUES (@FileId, @Number, @Data)")
        try
            statement.BindInt64(1, fileId)
            statement.BindInt64(2, chunkNumber)
            statement.BindBlob(3, arrayBuffer, len)
            statement.Step() |> ignore
            let result = statement.RecordsAffected
            statement.Dispose()
            if result <> 1 then failwith "writeChunkData failed."
        with error -> statement.Abort(error); reraise()

    let private allocateChunk (db: NativeSqliteConnection) (fileId: int64) (number: int64) (size: int) =
        let mutable statement = db.OpenStatement("INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid")
        try
            statement.BindInt64(1, fileId)
            statement.BindInt64(2, number)
            statement.BindInt64(3, int64 size)
            if not (statement.Step()) then Seq.head (Seq.empty<unit>)
            let rowid = statement.Row.GetInt64(0)
            statement.Dispose()
            rowid
        with error -> statement.Abort(error); reraise()

    /// Clear the stored boundary suffix without forming an absolute chunk end.
    let internal clearChunkTail (db: NativeSqliteConnection) (fileId: int64) (position: int64) =
        let chunkNumber = position / chunkSize
        let retainedBytes = int (position % chunkSize)
        use buffer = NativeArray.NativeArray.Alloc (int chunkSize)
        let bytes = buffer.Span
        bytes.Clear()
        let stored = tryGetChunkData db fileId chunkNumber bytes
        if not stored.IsEmpty then
            bytes.Slice(retainedBytes).Clear()
            writeChunkData db fileId chunkNumber bytes

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

    [<Struct; CLIMutable>]
    type internal ChunkDTO = { Number: int64; Data: NativeArray.NativeArray }

    let private openStoredChunks (db: NativeSqliteConnection) fileId startChunk lastChunk =
        let mutable statement = db.OpenStatement("SELECT Number, Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number >= @StartChunk AND Number <= @EndChunk ORDER BY Number")
        try
            statement.BindInt64(1, fileId)
            statement.BindInt64(2, startChunk)
            statement.BindInt64(3, lastChunk)
            statement
        with error -> statement.Abort(error); reraise()

    let private readStoredChunk (statement: byref<NativeStatementScope>) =
        try
            if statement.Step() then
                ValueSome { Number = statement.Row.GetInt64(0); Data = statement.Row.GetNativeArray(1) }
            else ValueNone
        with error -> statement.Abort(error); reraise()

    let internal getStoredChunks (db: NativeSqliteConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) = seq {
        let mutable statement = openStoredChunks db fileId startChunk lastChunk
        try
            let mutable reading = true
            while reading do
                match readStoredChunk &statement with
                | ValueSome chunk -> yield chunk
                | ValueNone -> reading <- false
        finally statement.Dispose()
    }

    let internal getAllCompressedChunksWithinRange (db: NativeSqliteConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) = seq {
        let mutable previousNumber = startChunk - 1L

        for chunk in getStoredChunks db fileId startChunk lastChunk do
            try
                while chunk.Number - 1L > previousNumber do
                    yield { Number = previousNumber + 1L; Data = NativeArray.NativeArray.Empty }
                    previousNumber <- previousNumber + 1L

                yield chunk
            finally
                chunk.Data.Dispose()

            previousNumber <- chunk.Number

        while previousNumber < lastChunk do
            yield { Number = previousNumber + 1L; Data = NativeArray.NativeArray.Empty }
            previousNumber <- previousNumber + 1L
    }

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

    let internal writeChunkedData (db: NativeSqliteConnection) (fileId: int64) (position: int64) (buffer: ReadOnlySpan<byte>) =
        let bufferLen = buffer.Length
        use uncompressedChunkBuffer = NativeArray.Alloc (int chunkSize)
        use compressedChunkBuffer = NativeArray.Alloc (int maxChunkStoreSize)
        let uncompressedChunkBufferSpan = uncompressedChunkBuffer.Span
        let compressedChunkBufferSpan = compressedChunkBuffer.Span

        let startChunkNumber = position / int64 chunkSize
        let startChunkOffset = position % int64 chunkSize

        let endChunkNumber = (position + int64 bufferLen) / int64 chunkSize
        let endChunkOffset = (position + int64 bufferLen) % int64 chunkSize

        // Capture the bounded affected range before writes can change chunk membership.
        let chunks = ResizeArray()
        let mutable statement = db.OpenStatement("SELECT rowid, Number FROM SoloDBFileChunk WHERE FileId = @FileId AND Number >= @StartChunk AND Number <= @EndChunk ORDER BY Number")
        try
            statement.BindInt64(1, fileId)
            statement.BindInt64(2, startChunkNumber)
            statement.BindInt64(3, endChunkNumber)
            let mutable currentChunkNr = startChunkNumber
            while statement.Step() do
                let number = statement.Row.GetInt64(1)
                while currentChunkNr < number do
                    chunks.Add {| rowid = -1L; Number = currentChunkNr |}
                    currentChunkNr <- currentChunkNr + 1L
                chunks.Add {| rowid = statement.Row.GetInt64(0); Number = number |}
                currentChunkNr <- number + 1L
            while currentChunkNr <= endChunkNumber do
                chunks.Add {| rowid = -1L; Number = currentChunkNr |}
                currentChunkNr <- currentChunkNr + 1L
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

        let mutable bufferConsumed = 0

        for chunk in chunks do
            let exists = chunk.rowid <> -1

            if startChunkNumber = endChunkNumber then
                if exists then
                    use blob = new NativeSqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
                    let compressedSize = blob.Read compressedChunkBufferSpan
                    let _chunkSizeExpected = Snappy.Decompress(compressedChunkBufferSpan.Slice(0, compressedSize), uncompressedChunkBufferSpan)
                    ()
                else
                    uncompressedChunkBufferSpan.Slice(0, int startChunkOffset).Clear()
                    let endOfWrite = int startChunkOffset + buffer.Length
                    if endOfWrite < int chunkSize then
                        uncompressedChunkBufferSpan.Slice(endOfWrite).Clear()
                buffer.CopyTo(uncompressedChunkBufferSpan.Slice(int startChunkOffset))
                let compressedSize = Snappy.Compress(uncompressedChunkBufferSpan, compressedChunkBufferSpan)
                let rowid =
                    allocateChunk db fileId chunk.Number compressedSize
                use blob = new NativeSqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

            elif chunk.Number = startChunkNumber then
                if exists && startChunkOffset > 0 then
                    use blob = new NativeSqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
                    let compressedSize = blob.Read compressedChunkBufferSpan
                    let _chunkSizeExpected = Snappy.Decompress(compressedChunkBufferSpan.Slice(0, compressedSize), uncompressedChunkBufferSpan)
                    ()
                else
                    uncompressedChunkBufferSpan.Slice(0, int startChunkOffset).Clear()
                let writeLen = int (chunkSize - startChunkOffset)
                buffer.Slice(0, writeLen).CopyTo(uncompressedChunkBufferSpan.Slice(int startChunkOffset))
                bufferConsumed <- bufferConsumed + writeLen
                let compressedSize = Snappy.Compress(uncompressedChunkBufferSpan, compressedChunkBufferSpan)
                let rowid =
                    allocateChunk db fileId chunk.Number compressedSize
                use blob = new NativeSqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

            elif chunk.Number = endChunkNumber then
                if exists then
                    use blob = new NativeSqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
                    let compressedSize = blob.Read compressedChunkBufferSpan
                    let _chunkSizeExpected = Snappy.Decompress(compressedChunkBufferSpan.Slice(0, compressedSize), uncompressedChunkBufferSpan)
                    ()
                else
                    uncompressedChunkBufferSpan.Slice(buffer.Length - bufferConsumed).Clear()
                let dataToWrite = buffer.Slice(bufferConsumed)
                if not dataToWrite.IsEmpty then
                    dataToWrite.CopyTo(uncompressedChunkBufferSpan)
                    let compressedSize = Snappy.Compress(uncompressedChunkBufferSpan, compressedChunkBufferSpan)
                    let rowid =
                        allocateChunk db fileId chunk.Number compressedSize
                    use blob = new NativeSqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                    blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

            else
                let writeLen = min (int chunkSize) (bufferLen - bufferConsumed)
                let dataToWrite = buffer.Slice(bufferConsumed, writeLen)
                dataToWrite.CopyTo(uncompressedChunkBufferSpan)
                if dataToWrite.Length < int chunkSize then
                    uncompressedChunkBufferSpan.Slice(dataToWrite.Length).Clear()
                bufferConsumed <- bufferConsumed + writeLen
                let compressedSize = Snappy.Compress(uncompressedChunkBufferSpan, compressedChunkBufferSpan)
                let rowid =
                    allocateChunk db fileId chunk.Number compressedSize
                use blob = new NativeSqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

        position + int64 bufferLen
