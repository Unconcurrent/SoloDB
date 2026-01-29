namespace SoloDatabase

open System.Collections.Generic
open System
open System.IO
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.Types
open NativeArray
open Snappier
open System.Runtime.InteropServices
open System.Data
open SoloDatabase.Connections

#nowarn "9"


module FileStorageCore =
    /// <summary>
    /// Defines the chunk size for file storage, approximating a SQLite page size.
    /// </summary>
    let chunkSize = 
        16384L // 16KB, aprox. a SQLite page size.

    /// <summary>
    /// Defines the maximum size allocated for storing compressed chunks.  Slightly larger than the chunk size to accommodate potential compression failures.
    /// </summary>
    let maxChunkStoreSize = 
        chunkSize + 100L // If the compression fails.

    /// <summary>
    /// Combines two path segments into a single path, replacing backslashes with forward slashes for consistency.
    /// </summary>
    /// <param name="a">The first path segment.</param>
    /// <param name="b">The second path segment.</param>
    /// <returns>A combined path string.</returns>
    let internal combinePath a b = Path.Combine(a, b) |> _.Replace('\\', '/')

    /// <summary>
    /// Combines an array of path segments into a single path, replacing backslashes with forward slashes for consistency.
    /// </summary>
    /// <param name="arr">An array of path segments.</param>
    /// <returns>A combined path string.</returns>
    let internal combinePathArr arr = arr |> Path.Combine |> _.Replace('\\', '/')

    /// <summary>
    /// Represents a disposable object that performs no action when disposed.  Used as a placeholder for scenarios where a resource does not require explicit disposal.
    /// </summary>
    let internal noopDisposer = { 
         new System.IDisposable with
             member this.Dispose() =
                 ()
    }

    /// <summary>
    /// Acquires a mutex to lock the specified path if a transaction is not already active.  This prevents concurrent access to files within the database.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The path to be locked.</param>
    /// <returns>An IDisposable object representing the mutex lock.</returns>
    let internal lockPathIfNotInTransaction (db: SqliteConnection) (path: string) =
        if db.IsWithinTransaction() then
            noopDisposer
        else
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(db.ConnectionString)}-Path-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(path)}")
        mutex :> IDisposable

    /// <summary>
    /// Acquires a mutex to lock the specified file ID if a transaction is not already active.  This prevents concurrent access to files within the database.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="id">The file ID to be locked.</param>
    /// <returns>An IDisposable object representing the mutex lock.</returns>
    let internal lockFileIdIfNotInTransaction (db: SqliteConnection) (id: int64) =
        if db.IsWithinTransaction() then
            noopDisposer
        else
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(db.ConnectionString)}-FileId-{id}")
        mutex :> IDisposable

    /// <summary>
    /// Fills the directory metadata for a given directory header by querying the database and populating a dictionary with key-value pairs.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="directory">The directory header to populate.</param>
    /// <returns>The updated directory header with metadata populated.</returns>
    let internal fillDirectoryMetadata (db: SqliteConnection) (directory: SoloDBDirectoryHeader) =
        let allMetadata = db.Query<Metadata>("SELECT Key, Value FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId", {|DirectoryId = directory.Id|})
        let dict = Dictionary<string, string>()

        for meta in allMetadata do
            dict[meta.Key] <- meta.Value

        {directory with Metadata = dict}


    /// <summary>
    /// Attempts to retrieve directories from the database based on a specified WHERE clause and parameters.
    /// </summary>
    /// <param name="connection">The SQLite connection.</param>
    /// <param name="where">The WHERE clause for the query.</param>
    /// <param name="parameters">The parameters for the query.</param>
    /// <returns>A sequence of SoloDBDirectoryHeader objects matching the criteria.</returns>
    let internal tryGetDirectoriesWhere (connection: SqliteConnection) (where: string) (parameters: obj) =
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

    /// <summary>
    /// Attempts to retrieve a directory from the database based on its full path.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The full path of the directory to retrieve.</param>
    /// <returns>An Option containing the SoloDBDirectoryHeader if found, otherwise None.</returns>
    let internal tryGetDir (db: SqliteConnection) (path: string) =
        tryGetDirectoriesWhere db "dh.FullPath = @Path" {|Path = path|} |> Seq.tryHead

    /// <summary>
    /// Recursively retrieves or creates a directory in the database, starting from the root if necessary.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The full path of the directory to retrieve or create.</param>
    /// <returns>The SoloDBDirectoryHeader representing the directory.</returns>
    let rec internal getOrCreateDir (db: SqliteConnection) (path: string) =
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
    
    /// <summary>
    /// Updates the length of a file in the database based on its ID.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to update.</param>
    /// <param name="len">The new length of the file.</param>
    let internal updateLenById (db: SqliteConnection) (fileId: int64) (len: int64) =
        let result = db.Execute ("UPDATE SoloDBFileHeader SET Length = @Length WHERE Id = @Id", {|Id = fileId; Length=len|})
        if result <> 1 then failwithf "updateLen failed."

    /// <summary>
    /// Reduces the length of a file in the database and deletes any chunks beyond the new length.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to downsize.</param>
    /// <param name="newFileLength">The new desired length of the file.</param>
    let internal downsetFileLength (db: SqliteConnection) (fileId: int64) (newFileLength: int64) =
        let lastChunkNumberKeep = ((float(newFileLength) / float(chunkSize)) |> Math.Ceiling |> int64) - 1L

        let _resultDelete = db.Execute(@"DELETE FROM SoloDBFileChunk WHERE FileId = @FileId AND Number > @LastChunkNumber",
                       {| FileId = fileId; LastChunkNumber = lastChunkNumberKeep |})

        let _resultUpdate = db.Execute(@"UPDATE SoloDBFileHeader 
                        SET Length = @NewFileLength
                        WHERE Id = @FileId",
                       {| FileId = fileId; NewFileLength = newFileLength |})
        ()

    /// <summary>
    /// Deletes a file from the database based on its header information.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="file">The SoloDBFileHeader representing the file to delete.</param>
    let internal deleteFile (db: SqliteConnection) (file: SoloDBFileHeader) =
        let _result = db.Execute(@"DELETE FROM SoloDBFileHeader WHERE Id = @FileId",
                    {| FileId = file.Id; |})

        ()

    /// <summary>
    /// Deletes a directory from the database based on its header information.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="dir">The SoloDBDirectoryHeader representing the directory to delete.</param>
    let internal deleteDirectory (db: SqliteConnection) (dir: SoloDBDirectoryHeader) = 
        let _result = db.Execute(@"DELETE FROM SoloDBDirectoryHeader WHERE Id = @DirId",
                        {| DirId = dir.Id; |})
        ()

    /// <summary>
    /// Internal only, do not touch. Represents an entry in the file system, either a file or directory.  Used for recursive listing operations.
    /// </summary>
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

    /// <summary>
    /// Recursively lists all entries (files and directories) within a specified directory.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="directoryFullPath">The full path of the directory to list recursively.</param>
    /// <returns>A sequence of SoloDBEntryHeader objects representing all entries within the directory and its subdirectories.</returns>
    let internal recursiveListAllEntriesInDirectory (db: SqliteConnection) (directoryFullPath: string) =
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


    /// <summary>
    /// Retrieves the length of a file from the database based on its ID.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to retrieve the length for.</param>
    /// <returns>The length of the file in bytes.</returns>
    let internal getFileLengthById (db: SqliteConnection) (fileId: int64) =
        db.QueryFirst<int64>(@"SELECT Length FROM SoloDBFileHeader WHERE Id = @FileId",
                       {| FileId = fileId |})

    /// <summary>
    /// Attempts to retrieve chunk data from the database based on file ID and chunk number.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file containing the chunk.</param>
    /// <param name="chunkNumber">The number of the chunk to retrieve.</param>
    /// <param name="buffer">A span to store the decompressed chunk data.</param>
    /// <returns>A Span representing the decompressed chunk data, or an empty span if not found.</returns>
    let internal tryGetChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (buffer: Span<byte>) =
        let sql = "SELECT Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number = @ChunkNumber"
        match db.QueryFirstOrDefault<byte array>(sql, {| FileId = fileId; ChunkNumber = chunkNumber |}) with
        | data when Object.ReferenceEquals(data, null) -> Span.Empty
        | data -> 
            let len = Snappy.Decompress(Span<byte>(data), buffer)
            buffer.Slice(0, len)


    /// <summary>
    /// Writes compressed chunk data to the database for a given file ID and chunk number.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to store the chunk in.</param>
    /// <param name="chunkNumber">The number of the chunk to store.</param>
    /// <param name="data">A span containing the compressed chunk data.</param>
    let internal writeChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (data: Span<byte>) =
        let arrayBuffer = Array.zeroCreate (int maxChunkStoreSize)
        let memoryBuffer = arrayBuffer.AsSpan()
        let len = Snappy.Compress(data, memoryBuffer)

        let result = db.Execute("INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) VALUES (@FileId, @Number, @Data)", {|
            FileId = fileId
            Number = chunkNumber
            Data = { Array = arrayBuffer; TrimmedLen = len } // Crop to size.
        |})


        if result <> 1 then failwithf "writeChunkData failed."


    /// <summary>
    /// Represents a chunk of data stored in the database, including its number and associated data array.
    /// </summary>
    [<Struct; CLIMutable>]
    type internal ChunkDTO = { Number: int64; Data: NativeArray.NativeArray }

    /// <summary>
    /// Retrieves all compressed chunks within a specified range for a given file ID.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to retrieve chunks from.</param>
    /// <param name="startChunk">The starting chunk number (inclusive).</param>
    /// <param name="lastChunk">The ending chunk number (inclusive).</param>
    /// <returns>A sequence of ChunkDTO objects representing the compressed chunks within the range.</returns>
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
        

    /// <summary>
    /// Retrieves all compressed chunks within a specified range for a given file ID, filling in any gaps with empty chunks.  The native memory associated with each chunk is automatically freed after iteration.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to retrieve chunks from.</param>
    /// <param name="startChunk">The starting chunk number (inclusive).</param>
    /// <param name="lastChunk">The ending chunk number (inclusive).</param>
    /// <returns>A sequence of ChunkDTO objects representing the compressed chunks within the range, including empty chunks for any gaps.</returns>
    let internal getAllCompressedChunksWithinRange (db: SqliteConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) = seq {
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

    /// <summary>
    /// Updates the modification timestamp of a file in the database based on its ID, only if the current timestamp is older than the existing one.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to update.</param>
    /// <param name="modified">The new modification timestamp.</param>
    let internal setFileModifiedById (db: SqliteConnection) (fileId: int64) (modified: DateTimeOffset) =
        db.Execute("UPDATE SoloDBFileHeader 
        SET Modified = @Modified
        WHERE Id = @FileId", {|Modified = modified; FileId = fileId|})
        |> ignore

    /// <summary>
    /// Updates the creation timestamp of a file in the database based on its ID.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to update.</param>
    /// <param name="created">The new creation timestamp.</param>
    let internal setFileCreatedById (db: SqliteConnection) (fileId: int64) (created: DateTimeOffset) =
        db.Execute("UPDATE SoloDBFileHeader 
        SET Created = @Created
        WHERE Id = @FileId", {|Created = created; FileId = fileId|})
        |> ignore

    /// <summary>
    /// SQL query to update the modification timestamp of a file and its parent directory, only if their current timestamps are older than the new one.
    /// </summary>
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

    /// <summary>
    /// SQL query to wrap the update modification timestamp operation in a transaction.
    /// </summary>
    [<Literal>]
    let internal updateModifiedTimestampTrSQL = "BEGIN;" + updateModifiedTimestampSQL + "COMMIT TRANSACTION;"
    
    /// <summary>
    /// Provides a stream-based interface for reading from and writing to a file that is stored inside the SQLite database. 
    /// Allows to treat a collection of database records as a single, continuous file.
    /// The file data is broken down into fixed-size chunks (16 KB, as defined by chunkSize).
    /// Each chunk is then compressed using the Snappy compression algorithm before being written to the SoloDBFileChunk table.
    /// When reading, it retrieves these chunks and decompresses them on the fly. This approach is efficient for handling large 
    /// files and sparse data (areas of the file that are empty).
    /// All write operations are performed within a database transaction. This ensures that modifications to the file's
    /// chunks and its metadata (like its length) are atomic. If any part of the write fails, the entire operation is
    /// rolled back, preventing data corruption.
    /// When the stream is written to and then flushed or disposed, it automatically updates the Modified timestamp for both the
    /// file and its parent directory in the database.
    /// </summary>
    /// <param name="db">The database connection provider used to interact with the underlying SQLite database.</param>
    /// <param name="fileId">The unique identifier (Id) for the file in the SoloDBFileHeader table.</param>
    /// <param name="directoryId">The unique identifier for the file's parent directory in the SoloDBDirectoryHeader table.</param>
    /// <param name="fullPath">The full, absolute path of the file within the virtual file system.</param>
    type DbFileStream internal (db: Connection, fileId: int64, directoryId: int64, fullPath: string) =
        inherit Stream()

        let mutable position = 0L
        let mutable disposed = false
        let mutable dirty = false

        let checkDisposed() = if disposed then raise (ObjectDisposedException(nameof(DbFileStream)))

        member internal this.UpdateModified() =
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
        override this.Read(buffer: Span<byte>) =
        #else
        member this.Read(buffer: Span<byte>) =
        #endif
            checkDisposed()

            // nothing to do on empty buffer
            if buffer.IsEmpty then 0 else

            let len               = this.Length
            let currentPosition   = position
            let remainingBytes    = len - currentPosition

            // if we're at or past the end, return 0
            if remainingBytes <= 0L then 0 else

            // compute how many bytes we'll actually read, and trim the buffer accordingly
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
                            // sparse chunk: fill with zeroes
                            destinationSpan.Fill(0uy)
                            bytesWrittenToBuffer <- bytesWrittenToBuffer + bytesInChunk
                        else
                            let offsetInChunk =
                                int (copyFrom - chunkStartPos)

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
                                    // partial-chunk decompress
                                    if chunkDecompressionBuffer.Length = 0 then
                                        chunkDecompressionBuffer <- NativeArray.NativeArray.Alloc (int maxChunkStoreSize)
                                    let decompBuf = chunkDecompressionBuffer.Span
                                    let _ = Snappy.Decompress(chunk.Data.Span, decompBuf)
                                    decompBuf.Slice(offsetInChunk, bytesInChunk).CopyTo(destinationSpan)
                                    bytesInChunk

                            bytesWrittenToBuffer <- bytesWrittenToBuffer + written
            finally
                chunkDecompressionBuffer.Dispose()
                // advance position by the bytes we've actually written
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

        override this.SetLength(value: int64) =
            checkDisposed()

            if value < 0 then 
                raise (ArgumentOutOfRangeException("Length"))

            let len = this.Length

            if len = value then
                ()
            elif len < value then
                dirty <- true
                let oldPos = position
                position <- len
                // Clear garbage data, if any.
                let chunkOffset = position % chunkSize
                use mem = NativeArray.NativeArray.Alloc (int chunkSize - int chunkOffset)
                let mem = mem.Span
                mem.Clear()
                this.Write mem

                position <- oldPos
                use db = db.Get()
                updateLenById db fileId value
            else
                dirty <- true
                use db = db.Get()

                downsetFileLength db fileId value
                if position > value then position <- value

        override this.Seek(offset: int64, origin: SeekOrigin) =
            checkDisposed()

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
