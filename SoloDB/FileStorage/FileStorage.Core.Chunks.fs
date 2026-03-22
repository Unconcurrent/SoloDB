namespace SoloDatabase

open System
open Microsoft.Data.Sqlite
open SQLiteTools
open SQLiteToolsParams
open NativeArray
open Snappier
open SoloDatabase.Types
open FileStorageCore

// NativePtr operations for efficient chunk-based file I/O
#nowarn "9"

module internal FileStorageCoreChunks =
    let internal recursiveListAllEntriesInDirectory (db: SqliteConnection) (directoryFullPath: string) =
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

        let entries =
            db.Query<SQLEntry> (queryCommand, {|FullPath = directoryFullPath|})
            |> Utils.SeqExt.sequentialGroupBy(fun e -> e.Path)
            |> Seq.map(fun entry ->
                let metadata = entry |> Seq.filter(fun x -> x.MetadataKey <> null) |> Seq.map(fun x -> (x.MetadataKey, x.MetadataValue)) |> readOnlyDict
                let entryData = entry.[0]
                match entryData.Type with
                | "File" ->
                    let file = {
                        Id = entryData.Id;
                        Name = entryData.Name;
                        FullPath = entryData.Path;
                        DirectoryId = entryData.DirectoryId.Value;
                        Length = entryData.Size;
                        Created = entryData.Created;
                        Modified = entryData.Modified;
                        Metadata = metadata;
                    }
                    SoloDBEntryHeader.File file
                | "Directory" ->
                    let dir = {
                        Id = entryData.Id;
                        Name = entryData.Name;
                        FullPath = entryData.Path;
                        ParentId = entryData.DirectoryId;
                        Created = entryData.Created;
                        Modified = entryData.Modified;
                        Metadata = metadata;
                    }
                    SoloDBEntryHeader.Directory dir
                | other -> failwithf "Invalid entry type: %s" other
            )

        entries

    let internal getFileLengthById (db: SqliteConnection) (fileId: int64) =
        db.QueryFirst<int64>(@"SELECT Length FROM SoloDBFileHeader WHERE Id = @FileId",
                       {| FileId = fileId |})

    let internal tryGetChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (buffer: Span<byte>) =
        let sql = "SELECT Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number = @ChunkNumber"
        match db.QueryFirstOrDefault<byte array>(sql, {| FileId = fileId; ChunkNumber = chunkNumber |}) with
        | data when Object.ReferenceEquals(data, null) -> Span.Empty
        | data ->
            let len = Snappy.Decompress(Span<byte>(data), buffer)
            buffer.Slice(0, len)

    let internal writeChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (data: Span<byte>) =
        let arrayBuffer = Array.zeroCreate (int maxChunkStoreSize)
        let memoryBuffer = arrayBuffer.AsSpan()
        let len = Snappy.Compress(data, memoryBuffer)

        let result = db.Execute("INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) VALUES (@FileId, @Number, @Data)", {|
            FileId = fileId
            Number = chunkNumber
            Data = { Array = arrayBuffer; TrimmedLen = len }
        |})

        if result <> 1 then failwithf "writeChunkData failed."

    [<Struct; CLIMutable>]
    type internal ChunkDTO = { Number: int64; Data: NativeArray.NativeArray }

    let inline internal getStoredChunks (db: SqliteConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) =
        db.Query<ChunkDTO>("""
        SELECT
          rowid,
          Number,
          Data
        FROM
          SoloDBFileChunk
        WHERE
          FileId = @FileId AND Number >= @StartChunk AND Number <= @EndChunk
        ORDER BY
          Number;
    """, {|FileId = fileId; StartChunk = startChunk; EndChunk = lastChunk|})

    let internal getAllCompressedChunksWithinRange (db: SqliteConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) = seq {
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

    let internal setFileModifiedById (db: SqliteConnection) (fileId: int64) (modified: DateTimeOffset) =
        db.Execute("UPDATE SoloDBFileHeader
        SET Modified = @Modified
        WHERE Id = @FileId", {|Modified = modified; FileId = fileId|})
        |> ignore

    let internal setFileCreatedById (db: SqliteConnection) (fileId: int64) (created: DateTimeOffset) =
        db.Execute("UPDATE SoloDBFileHeader
        SET Created = @Created
        WHERE Id = @FileId", {|Created = created; FileId = fileId|})
        |> ignore

    [<Literal>]
    let internal updateModifiedTimestampSQL = @"
        UPDATE SoloDBFileHeader
        SET Modified = UNIXTIMESTAMP()
        WHERE Id = @FileId AND Modified < UNIXTIMESTAMP();

        -- Update parent directory's Modified timestamp
        UPDATE SoloDBDirectoryHeader
        SET Modified = UNIXTIMESTAMP()
        WHERE Id = @DirId AND Modified < UNIXTIMESTAMP();
    "

    let internal writeChunkedData (db: SqliteConnection) (fileId: int64) (position: int64) (buffer: ReadOnlySpan<byte>) =
        let bufferLen = buffer.Length
        use uncompressedChunkBuffer = NativeArray.Alloc (int chunkSize)
        use compressedChunkBuffer = NativeArray.Alloc (int maxChunkStoreSize)
        let uncompressedChunkBufferSpan = uncompressedChunkBuffer.Span
        let compressedChunkBufferSpan = compressedChunkBuffer.Span

        let startChunkNumber = position / int64 chunkSize
        let startChunkOffset = position % int64 chunkSize

        let endChunkNumber = (position + int64 bufferLen) / int64 chunkSize
        let endChunkOffset = (position + int64 bufferLen) % int64 chunkSize

        let chunks = ResizeArray(
            seq {
                let mutable currentChunkNr = startChunkNumber
                let existingChunks =
                    db.Query<{|rowid: int64; Number: int64|}>(
                        """SELECT rowid, Number FROM SoloDBFileChunk
                           WHERE FileId = @FileId AND Number >= @StartChunk AND Number <= @EndChunk
                           ORDER BY Number;""",
                        {| FileId = fileId; StartChunk = startChunkNumber; EndChunk = endChunkNumber |}
                    )
                for chunk in existingChunks do
                    while currentChunkNr < chunk.Number do
                        yield {| rowid = -1L; Number = currentChunkNr |}
                        currentChunkNr <- currentChunkNr + 1L
                    yield chunk
                    currentChunkNr <- currentChunkNr + 1L
                while currentChunkNr <= endChunkNumber do
                    yield {| rowid = -1L; Number = currentChunkNr |}
                    currentChunkNr <- currentChunkNr + 1L
            }
        )

        let mutable bufferConsumed = 0

        for chunk in chunks do
            let exists = chunk.rowid <> -1

            if startChunkNumber = endChunkNumber then
                if exists then
                    use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
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
                    db.QueryFirst<int64>(
                        """INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data)
                           VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid""",
                        {| FileId = fileId; Number = chunk.Number; Size = compressedSize |}
                    )
                use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

            elif chunk.Number = startChunkNumber then
                if exists && startChunkOffset > 0 then
                    use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
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
                    db.QueryFirst<int64>(
                        """INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data)
                           VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid""",
                        {| FileId = fileId; Number = chunk.Number; Size = compressedSize |}
                    )
                use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

            elif chunk.Number = endChunkNumber then
                if exists then
                    use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
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
                        db.QueryFirst<int64>(
                            """INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data)
                               VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid""",
                            {| FileId = fileId; Number = chunk.Number; Size = compressedSize |}
                        )
                    use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
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
                    db.QueryFirst<int64>(
                        """INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data)
                           VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid""",
                        {| FileId = fileId; Number = chunk.Number; Size = compressedSize |}
                    )
                use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

        position + int64 bufferLen
