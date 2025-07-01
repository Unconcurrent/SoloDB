namespace SoloDatabase

open System.Collections.Generic
open System
open System.IO
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.Types
open NativeArray
open SoloDatabase.Connections
open Snappier
open System.Runtime.InteropServices
open System.Data

#nowarn "9"

module FileStorage =
    let chunkSize = 
        16384L // 16KB, aprox. a SQLite page size.

    let maxChunkStoreSize = 
        chunkSize + 100L // If the compression fails.

    let private combinePath a b = Path.Combine(a, b) |> _.Replace('\\', '/')
    let private combinePathArr arr = arr |> Path.Combine |> _.Replace('\\', '/')

    let private noopDisposer = { 
         new System.IDisposable with
             member this.Dispose() =
                 ()
    }

    let private lockPathIfNotInTransaction (db: SqliteConnection) (path: string) =
        if db.IsWithinTransaction() then
            noopDisposer
        else
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(db.ConnectionString)}-Path-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(path)}")
        mutex :> IDisposable

    let private lockFileIdIfNotInTransaction (db: SqliteConnection) (id: int64) =
        if db.IsWithinTransaction() then
            noopDisposer
        else
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(db.ConnectionString)}-FileId-{id}")
        mutex :> IDisposable

    let private fillDirectoryMetadata (db: SqliteConnection) (directory: SoloDBDirectoryHeader) =
        let allMetadata = db.Query<Metadata>("SELECT Key, Value FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId", {|DirectoryId = directory.Id|})
        let dict = Dictionary<string, string>()

        for meta in allMetadata do
            dict[meta.Key] <- meta.Value

        {directory with Metadata = dict}


    let private tryGetDirectoriesWhere (connection: SqliteConnection) (where: string) (parameters: obj) =
        let query = $"""
            SELECT dh.*, dm.Key, dm.Value
            FROM SoloDBDirectoryHeader dh
            LEFT JOIN SoloDBDirectoryMetadata dm ON dh.Id = dm.DirectoryId
            WHERE {where};
        """
    
        let directoryDictionary = new System.Collections.Generic.Dictionary<int64, SoloDBDirectoryHeader>()
        let isNull x = Object.ReferenceEquals(x, null)

        
        connection.Query<SoloDBDirectoryHeader, Metadata, unit>(
            query,
            (fun directory metadata ->
                let dir = 
                    match directoryDictionary.TryGetValue(directory.Id) with
                    | true, dir ->
                        dir
                    | false, _ ->
                        let dir = 
                            if isNull directory.Metadata then
                                {directory with Metadata = Dictionary()}
                            else directory
                            
                        directoryDictionary.Add(dir.Id, dir)
                        dir

                if not (isNull metadata) then
                    (dir.Metadata :?> IDictionary<string, string>).Add(metadata.Key, metadata.Value)
                ()
            ),
            parameters,
            splitOn = "Key"
        ) |> Seq.iter ignore
    
        directoryDictionary.Values :> SoloDBDirectoryHeader seq

    let private tryGetDir (db: SqliteConnection) (path: string) =
        tryGetDirectoriesWhere db "dh.FullPath = @Path" {|Path = path|} |> Seq.tryHead

    let rec private getOrCreateDir (db: SqliteConnection) (path: string) =
        use _l = lockPathIfNotInTransaction db path

        match tryGetDir db path with
        | Some d -> d
        | None ->
        match path with
        | "/" -> db.QueryFirst<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE Name = @RootName", {|RootName = ""|}) |> fillDirectoryMetadata db
        | path ->



        let names = path.Split ([|'/'|], StringSplitOptions.RemoveEmptyEntries)
        let previousNames = names |> Array.take (names.Length - 1)
        let currentName = names |> Array.last
        let previousPath = "/" + (previousNames |> String.concat "/")
        let previousDir = getOrCreateDir db previousPath
        let fullPath = combinePath previousPath currentName
        if fullPath <> path then failwithf "Inconsistent paths!"

        let newDir = {|
            Name = currentName
            ParentId = previousDir.Id
            FullPath = path
        |}

        let _result = db.Execute("INSERT INTO SoloDBDirectoryHeader(Name, ParentId, FullPath) VALUES(@Name, @ParentId, @FullPath)", newDir)

        match db.QueryFirstOrDefault<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE FullPath = @FullPath", {|FullPath = path|}) with
        | dir when Utils.isNull dir -> failwithf "Normally you cannot end up here, cannot find a directory that has just created: %s" path
        | dir -> dir |> fillDirectoryMetadata db
    
    let private updateLenById (db: SqliteConnection) (fileId: int64) (len: int64) =
        let result = db.Execute ("UPDATE SoloDBFileHeader SET Length = @Length WHERE Id = @Id", {|Id = fileId; Length=len|})
        if result <> 1 then failwithf "updateLen failed."

    let private downsetFileLength (db: SqliteConnection) (fileId: int64) (newFileLength: int64) =
        let lastChunkNumberKeep = ((float(newFileLength) / float(chunkSize)) |> Math.Ceiling |> int64) - 1L

        let _resultDelete = db.Execute(@"DELETE FROM SoloDBFileChunk WHERE FileId = @FileId AND Number > @LastChunkNumber",
                       {| FileId = fileId; LastChunkNumber = lastChunkNumberKeep |})

        let _resultUpdate = db.Execute(@"UPDATE SoloDBFileHeader 
                        SET Length = @NewFileLength
                        WHERE Id = @FileId",
                       {| FileId = fileId; NewFileLength = newFileLength |})
        ()

    let private deleteFile (db: SqliteConnection) (file: SoloDBFileHeader) =
        let _result = db.Execute(@"DELETE FROM SoloDBFileHeader WHERE Id = @FileId",
                    {| FileId = file.Id; |})

        ()

    let private deleteDirectory (db: SqliteConnection) (dir: SoloDBDirectoryHeader) = 
        let _result = db.Execute(@"DELETE FROM SoloDBDirectoryHeader WHERE Id = @DirId",
                        {| DirId = dir.Id; |})
        ()

    [<CLIMutable>]
    type SQLEntry = {
        Level: int64
        Id: int64
        Type: string
        Path: string
        Name: string
        Size: int64
        Created: DateTimeOffset
        Modified: DateTimeOffset
        DirectoryId: Nullable<int64>
        MetadataKey: string
        MetadataValue: string
    }

    let private recursiveListAllEntriesInDirectory (db: SqliteConnection) (directoryFullPath: string) =
        let directoryFullPath = [|directoryFullPath|] |> combinePathArr
        let queryCommand = 
            """
            -- EXPLAIN QUERY PLAN 
            WITH RECURSIVE DirectoryTree AS (
                -- Base case: start with the specified directory
                SELECT
                    Id,
                    Name,
                    FullPath,
                    ParentId,
               		Created,
               		Modified,
                    0 AS Level -- starting level
                FROM
                    SoloDBDirectoryHeader
                WHERE
                    FullPath = @FullPath
            
                UNION ALL
            
                -- Recursive case: get the subdirectories
                SELECT
                    d.Id,
                    d.Name,
                    d.FullPath,
                    d.ParentId,
               		d.Created,
               		d.Modified,
                    dt.Level + 1 AS Level
                FROM
                    SoloDBDirectoryHeader d
                INNER JOIN
                    DirectoryTree dt ON d.ParentId = dt.Id
            )
            
            SELECT
                dt.Level,
            	dt.Id as Id,
                'Directory' AS Type,
                dt.FullPath AS Path,
                dt.Name AS Name,
               	0 AS Size,
               	dt.Created as Created,
               	dt.Modified as Modified,
                dt.ParentId as DirectoryId,
                dm.Key AS MetadataKey,
                dm.Value AS MetadataValue
            FROM
                DirectoryTree dt
            LEFT JOIN
                SoloDBDirectoryMetadata dm ON dt.Id = dm.DirectoryId
            
            UNION ALL
            
            SELECT
                dt.Level + 1 AS Level,
            	fh.Id as Id,
                'File' AS Type,
                fh.FullPath AS Path,
                fh.Name AS Name,
               	fh.Length AS Size,
               	fh.Created as Created,
               	fh.Modified as Modified,
                fh.DirectoryId as DirectoryId,
                fm.Key AS MetadataKey,
                fm.Value AS MetadataValue
            FROM
                DirectoryTree dt
            INNER JOIN
                SoloDBFileHeader fh ON dt.Id = fh.DirectoryId
            LEFT JOIN
                SoloDBFileMetadata fm ON fh.Id = fm.FileId
            """

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


    let private getFileLengthById (db: SqliteConnection) (fileId: int64) =
        db.QueryFirst<int64>(@"SELECT Length FROM SoloDBFileHeader WHERE Id = @FileId",
                       {| FileId = fileId |})

    let private tryGetChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (buffer: Span<byte>) =
        let sql = "SELECT Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number = @ChunkNumber"
        match db.QueryFirstOrDefault<byte array>(sql, {| FileId = fileId; ChunkNumber = chunkNumber |}) with
        | data when Object.ReferenceEquals(data, null) -> Span.Empty
        | data -> 
            let len = Snappy.Decompress(Span<byte>(data), buffer)
            buffer.Slice(0, len)


    let private writeChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (data: Span<byte>) =
        let arrayBuffer = Array.zeroCreate (int maxChunkStoreSize)
        let memoryBuffer = arrayBuffer.AsSpan()
        let len = Snappy.Compress(data, memoryBuffer)

        let result = db.Execute("INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) VALUES (@FileId, @Number, @Data)", {|
            FileId = fileId
            Number = chunkNumber
            Data = { Array = arrayBuffer; TrimmedLen = len } // Crop to size.
        |})


        if result <> 1 then failwithf "writeChunkData failed."


    [<Struct; CLIMutable>]
    type internal ChunkDTO = { Number: int64; Data: NativeArray.NativeArray }

    let inline private getStoredChunks (db: SqliteConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) =
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
        

    /// The chunks are automatically freed after iterating.
    let private getAllCompressedChunksWithinRange (db: SqliteConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) = seq {
        let mutable previousNumber = startChunk - 1L // Initialize to one less than the starting chunk number.

        // Iterate through the chunks that are physically stored in the database.
        for chunk in getStoredChunks db fileId startChunk lastChunk do
            try
                // This loop identifies and fills any gaps with empty chunks
                // that exist before the current chunk from the database.
                while chunk.Number - 1L > previousNumber do
                    yield { Number = previousNumber + 1L; Data = NativeArray.NativeArray.Empty }
                    previousNumber <- previousNumber + 1L

                // Yield the actual chunk retrieved from storage.
                yield chunk
            finally
                // Ensure the native memory is disposed of after the chunk is yielded.
                chunk.Data.Dispose()
        
            // Update the tracker to the number of the chunk just processed.
            previousNumber <- chunk.Number

        // This new loop rectifies the original bug.
        // It handles all cases where the last chunks in the range are missing from the database.
        // It continues yielding empty chunks until the requested 'lastChunk' is reached.
        while previousNumber < lastChunk do
            yield { Number = previousNumber + 1L; Data = NativeArray.NativeArray.Empty }
            previousNumber <- previousNumber + 1L
    }

    let private setFileModifiedById (db: SqliteConnection) (fileId: int64) (modified: DateTimeOffset) =
        db.Execute("UPDATE SoloDBFileHeader 
        SET Modified = @Modified
        WHERE Id = @FileId", {|Modified = modified; FileId = fileId|})
        |> ignore

    let private setFileCreatedById (db: SqliteConnection) (fileId: int64) (created: DateTimeOffset) =
        db.Execute("UPDATE SoloDBFileHeader 
        SET Created = @Created
        WHERE Id = @FileId", {|Created = created; FileId = fileId|})
        |> ignore

    [<Literal>]
    let private updateModifiedTimestampSQL = @"
        UPDATE SoloDBFileHeader
        SET Modified = UNIXTIMESTAMP()
        WHERE Id = @FileId AND Modified < UNIXTIMESTAMP();
                    
        -- Update parent directory's Modified timestamp
        UPDATE SoloDBDirectoryHeader
        SET Modified = UNIXTIMESTAMP()
        WHERE Id = @DirId AND Modified < UNIXTIMESTAMP();
    "

    [<Literal>]
    let private updateModifiedTimestampTrSQL = "BEGIN;" + updateModifiedTimestampSQL + "COMMIT TRANSACTION;"
    
    type DbFileStream internal (db: Connection, fileId: int64, directoryId: int64, fullPath: string) =
        inherit Stream()

        let mutable position = 0L
        let mutable disposed = false
        let mutable dirty = false

        let checkDisposed() = if disposed then raise (ObjectDisposedException(nameof(DbFileStream)))

        member private this.UpdateModified() =
            if dirty then
                use db = db.Get()


                match db.IsWithinTransaction() with
                | true ->
                    ignore (db.Execute (updateModifiedTimestampSQL, {|FileId = fileId; DirId = directoryId|}))
                | false ->
                    ignore (db.Execute (updateModifiedTimestampTrSQL, {|FileId = fileId; DirId = directoryId|}))

                ()
            
            ()


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
        /// This returns the FullPath of the file at the moment of opening of the stream,
        /// if the file moved while the stream is still open then this will be wrong.
        /// </summary>
        member this.FullPath =
            fullPath

        override this.Flush() =         
            checkDisposed()
            this.UpdateModified()

        #if NETSTANDARD2_1_OR_GREATER
        override 
        #else
        member
        #endif
            this.Read(buffer: Span<byte>) =
            checkDisposed()

            if buffer.IsEmpty then 0 else

            use db = db.Get()
            let len = this.Length
            let currentPosition = position
            let bytesToReadTotal = min (int64 buffer.Length) (len - currentPosition)

            if bytesToReadTotal <= 0L then 0 else

            let startChunk = currentPosition / chunkSize
            let endChunk = (currentPosition + bytesToReadTotal - 1L) / chunkSize

            let mutable bytesWrittenToBuffer = 0
            let mutable chunkDecompressionBuffer = NativeArray.NativeArray.Empty

            try
                for chunk in getAllCompressedChunksWithinRange db fileId startChunk endChunk do
                    let chunkStartPos = chunk.Number * chunkSize
                    let copyFrom = max currentPosition chunkStartPos
                    let copyTo = min (currentPosition + bytesToReadTotal) (chunkStartPos + chunkSize)
                    let bytesToCopyInChunk = int (copyTo - copyFrom)

                    if bytesToCopyInChunk > 0 then
                        let bufferOffset = int (copyFrom - currentPosition)
                        let destinationSpan = buffer.Slice(bufferOffset, bytesToCopyInChunk)

                        // Check for sparse chunk by comparing with the static Empty property.
                        if chunk.Data.Length = 0 then
                            destinationSpan.Fill(0uy)
                            bytesWrittenToBuffer <- bytesWrittenToBuffer + bytesToCopyInChunk
                        else
                            let offsetInChunk = int (copyFrom % chunkSize)
                            let writtenBytes = 
                                if offsetInChunk = 0 then
                                    match Snappy.TryDecompress(chunk.Data.Span, destinationSpan) with
                                    | _, 0 -> failwithf "Failed to decompress chunk."
                                    | _, decompressedBytes when decompressedBytes <> destinationSpan.Length -> failwithf "Failed to fully decompress chunk."
                                    | _, decompressedBytes -> decompressedBytes
                                else
                                    if chunkDecompressionBuffer.Length = 0 then
                                        chunkDecompressionBuffer <- NativeArray.NativeArray.Alloc (int maxChunkStoreSize)

                                    let chunkDecompressionBuffer = chunkDecompressionBuffer.Span
                                    let decompressedBytes = Snappy.Decompress(chunk.Data.Span, chunkDecompressionBuffer)
                                    chunkDecompressionBuffer.Slice(offsetInChunk, bytesToCopyInChunk).CopyTo(destinationSpan)

                                    bytesToCopyInChunk

                            bytesWrittenToBuffer <- bytesWrittenToBuffer + writtenBytes
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
            dirty <- true

            let bufferLen = buffer.Length
            use buffer = fixed buffer
            
            db.WithTransaction(fun db ->
                let buffer = ReadOnlySpan<byte>(NativeInterop.NativePtr.toVoidPtr buffer, bufferLen)
                use uncompressedChunkBuffer = NativeArray.Alloc (int chunkSize)
                use compressedChunkBuffer = NativeArray.Alloc (int maxChunkStoreSize)
                let uncompressedChunkBufferSpan = uncompressedChunkBuffer.Span
                let compressedChunkBufferSpan = compressedChunkBuffer.Span

                let position = position

                let startChunkNumber = position / int64 chunkSize
                let startChunkOffset = position % int64 chunkSize

                let endChunkNumber = (position + int64 bufferLen) / int64 chunkSize
                let endChunkOffset = (position + int64 bufferLen) % int64 chunkSize
                

                let chunks = ResizeArray(
                    seq {
                        let mutable currentChunkNr = startChunkNumber

                        // Fetch all existing chunks within the write range
                        let existingChunks = 
                            db.Query<{|rowid: int64; Number: int64|}>(
                                """SELECT rowid, Number FROM SoloDBFileChunk 
                                   WHERE FileId = @FileId AND Number >= @StartChunk AND Number <= @EndChunk 
                                   ORDER BY Number;""", 
                                {| FileId = fileId; StartChunk = startChunkNumber; EndChunk = endChunkNumber |}
                            )

                        // Iterate through the chunks that actually exist
                        for chunk in existingChunks do
                            // Fill in any gaps before the current existing chunk
                            while currentChunkNr < chunk.Number do
                                yield {| rowid = -1L; Number = currentChunkNr |}
                                currentChunkNr <- currentChunkNr + 1L
            
                            // Yield the existing chunk itself
                            yield chunk
                            currentChunkNr <- currentChunkNr + 1L

                        // This handles cases where no chunks exist or where the last chunks are missing.
                        while currentChunkNr <= endChunkNumber do
                            yield {| rowid = -1L; Number = currentChunkNr |}
                            currentChunkNr <- currentChunkNr + 1L
                    }
                )
                    
                let mutable bufferConsumed = 0

                for chunk in chunks do
                    let exists = chunk.rowid <> -1

                    // CASE 1: The entire write operation occurs within this single chunk.
                    if startChunkNumber = endChunkNumber then
                        // This is a read-modify-write on a single chunk.
                        if exists then
                            // Read the existing compressed data.
                            use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
                            let compressedSize = blob.Read compressedChunkBufferSpan
                            // Decompress it to get the original chunk content.
                            let _chunkSizeExpected = Snappy.Decompress(compressedChunkBufferSpan.Slice(0, compressedSize), uncompressedChunkBufferSpan)
                            ()
                        else
                            // Clear the part before the data is copied.
                            uncompressedChunkBufferSpan.Slice(0, int startChunkOffset).Clear()
                            // Clear the part after the data is copied.
                            let endOfWrite = int startChunkOffset + buffer.Length
                            if endOfWrite < int chunkSize then
                                uncompressedChunkBufferSpan.Slice(endOfWrite).Clear()

                        // Overwrite the relevant portion of the uncompressed data with the input buffer.
                        // The entire input buffer is used here.
                        buffer.CopyTo(uncompressedChunkBufferSpan.Slice(int startChunkOffset))

                        // Now, compress the modified chunk and write it back.
                        let compressedSize = Snappy.Compress(uncompressedChunkBufferSpan, compressedChunkBufferSpan)
                        let rowid = 
                            db.QueryFirst<int64>(
                                """INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) 
                                   VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid""", 
                                {| FileId = fileId; Number = chunk.Number; Size = compressedSize |}
                            )
                        use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                        blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

                    // CASE 2: This is the first chunk of a multi-chunk write.
                    elif chunk.Number = startChunkNumber then
                        // This is a partial write from startChunkOffset to the end of the chunk.
                        if exists && startChunkOffset > 0 then
                            use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
                            let compressedSize = blob.Read compressedChunkBufferSpan
                            let _chunkSizeExpected = Snappy.Decompress(compressedChunkBufferSpan.Slice(0, compressedSize), uncompressedChunkBufferSpan)
                            ()
                        else
                            // The rest of the chunk is guaranteed to be filled by the CopyTo operation below.
                            uncompressedChunkBufferSpan.Slice(0, int startChunkOffset).Clear()

                        // Determine how much data to write into this first chunk.
                        let writeLen = int (chunkSize - startChunkOffset)
        
                        // Copy the initial part of the input buffer.
                        buffer.Slice(0, writeLen).CopyTo(uncompressedChunkBufferSpan.Slice(int startChunkOffset))
        
                        // Mark this portion of the buffer as consumed.
                        bufferConsumed <- bufferConsumed + writeLen

                        // Compress the modified chunk and write it back.
                        let compressedSize = Snappy.Compress(uncompressedChunkBufferSpan, compressedChunkBufferSpan)
                        let rowid = 
                            db.QueryFirst<int64>(
                                """INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) 
                                   VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid""", 
                                {| FileId = fileId; Number = chunk.Number; Size = compressedSize |}
                            )
                        use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                        blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

                    // CASE 3: This is the last chunk of a multi-chunk write.
                    elif chunk.Number = endChunkNumber then
                        // This is a partial write from the beginning of the chunk up to endChunkOffset.
                        if exists then
                            use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", chunk.rowid, true)
                            let compressedSize = blob.Read compressedChunkBufferSpan
                            let _chunkSizeExpected = Snappy.Decompress(compressedChunkBufferSpan.Slice(0, compressedSize), uncompressedChunkBufferSpan)
                            ()
                        else
                            uncompressedChunkBufferSpan.Slice(buffer.Length - bufferConsumed).Clear()

                        // The data to write is the remainder of the input buffer.
                        let dataToWrite = buffer.Slice(bufferConsumed)

                        // Return early is there is no data left.
                        if dataToWrite.IsEmpty then () 
                        else
                            // Copy the final part of the input buffer to the start of the chunk buffer.
                            dataToWrite.CopyTo(uncompressedChunkBufferSpan)
        
                            // Compress and write back.
                            let compressedSize = Snappy.Compress(uncompressedChunkBufferSpan, compressedChunkBufferSpan)
                            let rowid = 
                                db.QueryFirst<int64>(
                                    """INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) 
                                       VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid""", 
                                    {| FileId = fileId; Number = chunk.Number; Size = compressedSize |}
                                )
                            use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                            blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

                    // CASE 4: This is a middle chunk that is being fully overwritten.
                    else
                        // This is the most efficient path: a write-only operation. No read is needed.
                        let writeLen = min (int chunkSize) (bufferLen - bufferConsumed)
                        let dataToWrite = buffer.Slice(bufferConsumed, writeLen)
        
                        // Copy the data and clear the remainder of the uncompressed buffer.
                        dataToWrite.CopyTo(uncompressedChunkBufferSpan)
                        if dataToWrite.Length < int chunkSize then
                            uncompressedChunkBufferSpan.Slice(dataToWrite.Length).Clear()

                        // Update the consumed counter for the next iteration.
                        bufferConsumed <- bufferConsumed + writeLen

                        // Compress and write.
                        let compressedSize = Snappy.Compress(uncompressedChunkBufferSpan, compressedChunkBufferSpan)
                        let rowid = 
                            db.QueryFirst<int64>(
                                """INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) 
                                   VALUES (@FileId, @Number, ZEROBLOB(@Size)) RETURNING rowid""", 
                                {| FileId = fileId; Number = chunk.Number; Size = compressedSize |}
                            )
                        use blob = new SqliteBlob(db, "SoloDBFileChunk", "Data", rowid, false)
                        blob.Write(compressedChunkBufferSpan.Slice(0, compressedSize))

                let newPosition = position + int64 bufferLen

                if newPosition > this.Length then
                    updateLenById db fileId newPosition

                this.Position <- newPosition
            )

        override this.Write(buffer: byte[], offset: int, count: int) =
            if buffer = null then raise (ArgumentNullException(nameof buffer))
            if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
            if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
            if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))
            this.Write(buffer.AsSpan(offset, count))

        override this.WriteAsync(buffer: byte[], offset: int, count: int, ct) =
            this.Write(buffer, offset, count)
            Threading.Tasks.Task.CompletedTask

        override _.SetLength(value: int64) =
            checkDisposed()

            if value < 0 then 
                raise (ArgumentOutOfRangeException("Length"))

            dirty <- true
            use db = db.Get()
            use l = lockFileIdIfNotInTransaction db fileId
            downsetFileLength db fileId value
            if position > value then position <- value

        override this.Seek(offset: int64, origin: SeekOrigin) =
            checkDisposed()
            use db = db.Get()
            use l = lockFileIdIfNotInTransaction db fileId
            match origin with
            | SeekOrigin.Begin -> position <- offset
            | SeekOrigin.Current -> position <- position + offset
            | SeekOrigin.End -> 
                let len = this.Length
                position <- len + offset
            | other -> failwithf "Invalid SeekOrigin: %A" other
            position

        override this.Dispose(disposing) =
            if not disposed then 
                this.Flush()
            disposed <- true
            ()


    let private getPathAndName (path: string) =
        let normalizedCompletePath = path.Split('\\', '/') |> Array.filter (fun x -> x |> String.IsNullOrWhiteSpace |> not)
        let sep = "/"
        let dirPath = $"/{normalizedCompletePath |> Array.take (Math.Max(normalizedCompletePath.Length - 1, 0l)) |> String.concat sep}"
        let name = match normalizedCompletePath |> Array.tryLast with Some x -> x | None -> ""
        struct (dirPath, name)

    let private formatPath path =
        let struct (dir, name) = getPathAndName path
        let dirPath = combinePath dir name
        dirPath

    let private createFileAt (db: SqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let directory = getOrCreateDir db dirPath
        let newFile = {            
            Name = name
            FullPath = combinePath dirPath name
            DirectoryId = directory.Id
            Length = 0L
            Created = DateTimeOffset.Now
            Modified = DateTimeOffset.Now

            Id = 0
            Metadata = null
        }
        let result = db.QueryFirst<SoloDBFileHeader>("INSERT INTO SoloDBFileHeader(Name, FullPath, DirectoryId, Length, Created, Modified) VALUES (@Name, @FullPath, @DirectoryId, @Length, @Created, @Modified) RETURNING *", newFile)

        {result with Metadata = readOnlyDict []}

    let private listDirectoriesAt (db: SqliteConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> Seq.empty
        | Some dir ->
        let childres = tryGetDirectoriesWhere db "ParentId = @Id" {|Id = dir.Id|}
        childres


    let private getFilesWhere (connection: SqliteConnection) (where: string) (parameters: obj) =
        let query = sprintf """
                    SELECT fh.*, fm.Key as MetaKey, fm.Value as MetaValue
                    FROM SoloDBFileHeader fh
                    LEFT JOIN SoloDBFileMetadata fm ON fh.Id = fm.FileId
                    WHERE %s;
                    """ where
    
        connection.Query<{|
            Id: int64
            Name: string
            FullPath: string
            DirectoryId: int64
            Length: int64
            Created: DateTimeOffset
            Modified: DateTimeOffset

            MetaKey: string
            MetaValue: string
            |}>(
            query,
            parameters
        ) 
        |> Utils.SeqExt.sequentialGroupBy(fun e -> e.Id)
        |> Seq.map(fun fileAndDatas ->
            let allMetadata = 
                fileAndDatas 
                |> Seq.filter(fun fileAndData -> fileAndData.MetaKey <> null) 
                |> Seq.map(fun fileAndData -> (fileAndData.MetaKey, fileAndData.MetaValue))
                |> readOnlyDict

            let file = fileAndDatas.[0]

            {
                Id = file.Id
                Name = file.Name
                FullPath = file.FullPath
                DirectoryId = file.DirectoryId
                Length = file.Length
                Created = file.Created
                Modified = file.Modified
                Metadata = allMetadata
            }
        )

    let private tryGetFileAt (db: SqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let fullPath = combinePath dirPath name
        getFilesWhere db "fh.FullPath = @FullPath" {|FullPath = fullPath|} |> Seq.tryHead

    let private getOrCreateFileAt (db: SqliteConnection) (path: string) =
        use l = lockPathIfNotInTransaction db path

        match tryGetFileAt db path with
        | Some f -> f
        | None ->
        createFileAt db path

    let private deleteDirectoryAt (db: SqliteConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> false
        | Some dir ->

        deleteDirectory db dir
        true

    let private getOrCreateDirectoryAt (db: SqliteConnection) (path: string) = 
        let dirPath = formatPath path
        getOrCreateDir db dirPath


    let private listFilesAt (db: SqliteConnection) (path: string) : SoloDBFileHeader seq = 
        let dirPath = formatPath path
        match tryGetDir db dirPath with 
        | None -> Seq.empty
        | Some dir ->
        getFilesWhere db "DirectoryId = @DirectoryId" {|DirectoryId = dir.Id|} |> ResizeArray :> SoloDBFileHeader seq

    let private openFile (db: Connection) (file: SoloDBFileHeader) =
        new DbFileStream(db, file.Id, file.DirectoryId, file.FullPath)

    let private openOrCreateFile (db: Connection) (path: string) =
        let file = 
            use conn = db.Get()
            match tryGetFileAt conn path with
            | Some x -> x 
            | None -> createFileAt conn path

        new DbFileStream(db, file.Id, file.DirectoryId, file.FullPath)

    let private setSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBFileMetadata(FileId, Key, Value) VALUES(@FileId, @Key, @Value)", {|FileId = file.Id; Key = key; Value = value|}) |> ignore

    let private deleteSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBFileMetadata WHERE FileId = @FileId AND Key = @Key", {|FileId = file.Id; Key = key|}) |> ignore


    let private setDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) VALUES(@DirectoryId, @Key, @Value)", {|DirectoryId = dir.Id; Key = key; Value = value|}) |> ignore

    let private deleteDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId AND Key = @Key", {|DirectoryId = dir.Id; Key = key|}) |> ignore
   
    let private moveFile (db: SqliteConnection) (file: SoloDBFileHeader) (toDir: SoloDBDirectoryHeader) (newName: string) =
        let newFileFullPath = combinePath toDir.FullPath newName
        try
            db.Execute("UPDATE SoloDBFileHeader 
            SET FullPath = @NewFullPath,
            DirectoryId = @DestDirId,
            Name = @NewName
            WHERE Id = @FileId", {|NewFullPath = newFileFullPath; DestDirId = toDir.Id; FileId = file.Id; NewName = newName|})
            |> ignore
        with
        | :? SqliteException as ex when ex.SqliteErrorCode = 19(*SQLITE_CONSTRAINT*) && ex.Message.Contains "SoloDBFileHeader.FullPath" ->
            raise (IOException("File already exists.", ex))

    let rec private moveDirectoryMustBeWithinTransaction (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (newParentDir: SoloDBDirectoryHeader) (newName: string) =
        // Step 1: Calculate the new path for the directory being moved
        let newDirFullPath = combinePath newParentDir.FullPath newName
    
        try
            // Step 2: Update the directory path
            db.Execute("UPDATE SoloDBDirectoryHeader 
                        SET FullPath = @NewFullPath, 
                            ParentId = @NewParentId, 
                            Name = @NewName 
                        WHERE Id = @DirId",
                        {| NewFullPath = newDirFullPath; NewParentId = newParentDir.Id; DirId = dir.Id; NewName = newName |})
            |> ignore
    
            let oldDirPath = dir.FullPath
            let dir = {dir with FullPath = newDirFullPath; ParentId = Nullable newParentDir.Id; Name = newName}

            // Step 3: Update paths of all subdirectories
            let subDirs = db.Query<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE ParentId = @DirId", {| DirId = dir.Id |}) |> Seq.toList
            for subDir in subDirs do
                let subDirNewName = subDir.Name
                moveDirectoryMustBeWithinTransaction db subDir dir subDirNewName
    
            // Step 4: Update paths of all files in the directory
            db.Execute("UPDATE SoloDBFileHeader 
                        SET FullPath = REPLACE(FullPath, @OldFullPath, @NewFullPath)
                        WHERE DirectoryId = @DirId",
                        {| OldFullPath = oldDirPath; NewFullPath = newDirFullPath; DirId = dir.Id |})
            |> ignore
        with
        | :? SqliteException as ex when ex.SqliteErrorCode = 19 (* SQLITE_CONSTRAINT *) && ex.Message.Contains "FullPath" ->
            raise (IOException("Directory or file already exists in the destination.", ex))
    
    type BulkFileData = {
        FullPath: string
        Data: byte array
        Created: Nullable<DateTimeOffset>
        Modified: Nullable<DateTimeOffset>
    }

    type FileSystem(connection: Connection) =
        member this.Upload(path, stream: Stream) =
            use file = openOrCreateFile connection path
            stream.CopyTo(file, int chunkSize)
            file.SetLength file.Position

        member this.UploadAsync(path, stream: Stream) = task {
            use file = openOrCreateFile connection path
            do! stream.CopyToAsync(file, int chunkSize)
            file.SetLength file.Position
        }

        member this.UploadBulk(files: BulkFileData seq) =
            connection.WithTransaction(fun tx -> 
                use _disabled = (unbox<IDisableDispose> tx).DisableDispose()
                let innerConnection = Transitive tx
                for file in files do
                    let fileHeader = getOrCreateFileAt tx file.FullPath

                    do
                        use fileStream = openFile innerConnection fileHeader
                        fileStream.Write(file.Data, 0, file.Data.Length)
                        fileStream.SetLength file.Data.Length
                    
                    if file.Modified.HasValue then
                        setFileModifiedById tx fileHeader.Id file.Modified.Value

                    if file.Created.HasValue then
                        setFileCreatedById tx fileHeader.Id file.Created.Value

                    ()
            )

        member this.ReplaceAsyncWithinTransaction(path, stream: Stream) = task {
            return! connection.WithAsyncTransaction(fun tx -> task {
                use _disabled = (unbox<IDisableDispose> tx).DisableDispose()
                let innerConnection = Transitive tx
                use file = openOrCreateFile innerConnection path
                do! stream.CopyToAsync(file, int chunkSize)
                file.SetLength file.Position
            })
        }

        member this.Download(path, stream: Stream) =
            
            let fileHeader = 
                use db = connection.Get()
                match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

            use fileStream = openFile connection fileHeader
            fileStream.CopyTo(stream, int chunkSize * 10)

        member this.DownloadAsync(path, stream: Stream) = task {
            
            let fileHeader = 
                use db = connection.Get()
                match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

            use fileStream = openFile connection fileHeader
            do! fileStream.CopyToAsync(stream, int chunkSize * 10)
        }

        member this.GetAt path =
            use db = connection.Get()
            match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

        member this.TryGetAt path =
            use db = connection.Get()
            tryGetFileAt db path

        member this.GetOrCreateAt path =
            use db = connection.Get()
            getOrCreateFileAt db path

        member this.GetDirAt path =
            use db = connection.Get()
            match tryGetDir db path with | Some f -> f | None -> raise (DirectoryNotFoundException("Directory not found at: " + path))

        member this.TryGetDirAt path =
            use db = connection.Get()
            tryGetDir db path

        member this.GetOrCreateDirAt path =
            use db = connection.Get()
            getOrCreateDirectoryAt db path

        member this.Open file =
            openFile connection file

        member this.OpenAt path =
            let file = this.GetAt path
            openFile connection file

        member this.TryOpenAt path =
            match this.TryGetAt path with
            | None -> None
            | Some file ->
            openFile connection file |> Some

        member this.OpenOrCreateAt path =
            openOrCreateFile connection path

        /// <summary>
        /// Writes data to a file at a specific offset.
        /// </summary>
        /// <param name="path">The path to the file.</param>
        /// <param name="offset">The zero-based byte offset in the file at which to begin writing.</param>
        /// <param name="data">The byte array to write to the file.</param>
        /// <param name="createIfInexistent">Specifies whether to create the file if it does not exist. Defaults to true.</param>
        member this.WriteAt(path: string, offset: int64, data: byte[], [<Optional; DefaultParameterValue(true)>] createIfInexistent: bool) =
             connection.WithTransaction(fun tx -> 
                let file = if createIfInexistent then getOrCreateFileAt tx path else match tryGetFileAt tx path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
                use _disabled = (unbox<IDisableDispose> tx).DisableDispose()
                let innerConnection = Transitive tx

                use fileStream = openFile innerConnection file
                fileStream.Position <- offset
                fileStream.Write(data, 0, data.Length)
                ()
            )


        /// <summary>
        /// Writes data to a file at a specific offset.
        /// </summary>
        /// <param name="path">The path to the file.</param>
        /// <param name="offset">The zero-based byte offset in the file at which to begin writing.</param>
        /// <param name="data">The Stream to copy to the file.</param>
        /// <param name="createIfInexistent">Specifies whether to create the file if it does not exist. Defaults to true.</param>
        member this.WriteAt(
            path: string,
            offset: int64,
            data: Stream,
            [<Optional; DefaultParameterValue(true)>] createIfInexistent: bool
        ) =
            connection.WithTransaction(fun tx -> 
                let file = if createIfInexistent then getOrCreateFileAt tx path else match tryGetFileAt tx path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
                use _disabled = (unbox<IDisableDispose> tx).DisableDispose()
                let innerConnection = Transitive tx

                use fileStream = openFile innerConnection file
                fileStream.Position <- offset
                data.CopyTo (fileStream, int chunkSize * 10)
            )

        member this.ReadAt(path: string, offset: int64, len) =
            let file = this.GetAt path

            use fileStream = new BinaryReader(openFile connection file)
            fileStream.BaseStream.Position <- offset
            fileStream.ReadBytes len

        member this.SetFileModificationDate(path, date) =
            let file = this.GetAt path
            use db = connection.Get()
            setFileModifiedById db file.Id date

        member this.SetFileCreationDate(path, date) =
            let file = this.GetAt path
            use db = connection.Get()
            setFileCreatedById db file.Id date

        member this.SetMetadata(file, key, value) =
            use db = connection.Get()
            setSoloDBFileMetadata db file key value

        member this.SetMetadata(path, key, value) =
            let file = this.GetAt path
            this.SetMetadata(file, key, value)

        member this.DeleteMetadata(file, key) =
            use db = connection.Get()
            deleteSoloDBFileMetadata db file key

        member this.DeleteMetadata(path, key) =
            let file = this.GetAt path
            this.DeleteMetadata(file, key)

        member this.SetDirectoryMetadata(dir, key, value) =
            use db = connection.Get()
            setDirMetadata db dir key value

        member this.SetDirectoryMetadata(path, key, value) =
            let dir = this.GetDirAt path
            this.SetDirectoryMetadata(dir, key, value)

        member this.DeleteDirectoryMetadata(dir, key) =
            use db = connection.Get()
            deleteDirMetadata db dir key

        member this.DeleteDirectoryMetadata(path, key) =
            let dir = this.GetDirAt path
            this.DeleteDirectoryMetadata(dir, key)

        member this.Delete(file) =
            use db = connection.Get()
            deleteFile db file

        member this.Delete(dir) =
            use db = connection.Get()
            deleteDirectory db dir

        member this.DeleteFileAt(path) =
            match this.TryGetAt path with
            | None -> false
            | Some file ->
            this.Delete file
            true

        member this.DeleteDirAt(path) =
            use db = connection.Get()
            deleteDirectoryAt db path

        member this.ListFilesAt(path) =
            use db = connection.Get()
            listFilesAt db path

        member this.ListDirectoriesAt(path) =
            use db = connection.Get()
            listDirectoriesAt db path

        member this.RecursiveListEntriesAt(path) =
            use db = connection.Get()
            recursiveListAllEntriesInDirectory db path   
            // If the downstream processing is slow, it can lock the DB and timeout on other's access,
            // therefore we will cache them in a List.
            |> ResizeArray 
            :> IList<SoloDBEntryHeader>

        // This method is not recommended, as its misuse can lock the database.
        member this.RecursiveListEntriesAtLazy(path) = seq {
            use db = connection.Get()
            yield! recursiveListAllEntriesInDirectory db path
        }

        member this.MoveFile(from, toPath) =
            let file = this.GetAt from
            let struct (toDirPath, fileName) = getPathAndName toPath
            let dir = this.GetOrCreateDirAt toDirPath
            use db = connection.Get()
            moveFile db file dir fileName

        member this.MoveReplaceFile(from, toPath) =
            let struct (toDirPath, fileName) = getPathAndName toPath
            connection.WithTransaction(fun db ->
                let file = match tryGetFileAt db from with | Some f -> f | None -> raise (FileNotFoundException("File not found.", from))
                let dir = getOrCreateDirectoryAt db toDirPath
                match tryGetFileAt db toPath with
                | Some replacedFile ->
                    if replacedFile.FullPath <> file.FullPath then
                        deleteFile db replacedFile
                | None -> ()
                moveFile db file dir fileName
            )

        member this.MoveDirectory(from, toPath) =
            let struct (toDirPath, fileName) = getPathAndName toPath
            connection.WithTransaction(fun db ->
                let dirToMove = match tryGetDir db from with | Some f -> f | None -> raise (DirectoryNotFoundException("From directory not found at: " + from))
                let toPathParent = match tryGetDir db toDirPath with | Some f -> f | None -> raise (DirectoryNotFoundException("To directory not found at: " + toDirPath))

                moveDirectoryMustBeWithinTransaction db dirToMove toPathParent fileName
            )
            