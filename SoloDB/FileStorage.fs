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
    let private combinePath a b = Path.Combine(a, b) |> _.Replace('\\', '/')

    /// <summary>
    /// Combines an array of path segments into a single path, replacing backslashes with forward slashes for consistency.
    /// </summary>
    /// <param name="arr">An array of path segments.</param>
    /// <returns>A combined path string.</returns>
    let private combinePathArr arr = arr |> Path.Combine |> _.Replace('\\', '/')

    /// <summary>
    /// Represents a disposable object that performs no action when disposed.  Used as a placeholder for scenarios where a resource does not require explicit disposal.
    /// </summary>
    let private noopDisposer = { 
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
    let private lockPathIfNotInTransaction (db: SqliteConnection) (path: string) =
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
    let private lockFileIdIfNotInTransaction (db: SqliteConnection) (id: int64) =
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
    let private fillDirectoryMetadata (db: SqliteConnection) (directory: SoloDBDirectoryHeader) =
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

    /// <summary>
    /// Attempts to retrieve a directory from the database based on its full path.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The full path of the directory to retrieve.</param>
    /// <returns>An Option containing the SoloDBDirectoryHeader if found, otherwise None.</returns>
    let private tryGetDir (db: SqliteConnection) (path: string) =
        tryGetDirectoriesWhere db "dh.FullPath = @Path" {|Path = path|} |> Seq.tryHead

    /// <summary>
    /// Recursively retrieves or creates a directory in the database, starting from the root if necessary.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The full path of the directory to retrieve or create.</param>
    /// <returns>The SoloDBDirectoryHeader representing the directory.</returns>
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
    
    /// <summary>
    /// Updates the length of a file in the database based on its ID.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to update.</param>
    /// <param name="len">The new length of the file.</param>
    let private updateLenById (db: SqliteConnection) (fileId: int64) (len: int64) =
        let result = db.Execute ("UPDATE SoloDBFileHeader SET Length = @Length WHERE Id = @Id", {|Id = fileId; Length=len|})
        if result <> 1 then failwithf "updateLen failed."

    /// <summary>
    /// Reduces the length of a file in the database and deletes any chunks beyond the new length.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to downsize.</param>
    /// <param name="newFileLength">The new desired length of the file.</param>
    let private downsetFileLength (db: SqliteConnection) (fileId: int64) (newFileLength: int64) =
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
    let private deleteFile (db: SqliteConnection) (file: SoloDBFileHeader) =
        let _result = db.Execute(@"DELETE FROM SoloDBFileHeader WHERE Id = @FileId",
                    {| FileId = file.Id; |})

        ()

    /// <summary>
    /// Deletes a directory from the database based on its header information.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="dir">The SoloDBDirectoryHeader representing the directory to delete.</param>
    let private deleteDirectory (db: SqliteConnection) (dir: SoloDBDirectoryHeader) = 
        let _result = db.Execute(@"DELETE FROM SoloDBDirectoryHeader WHERE Id = @DirId",
                        {| DirId = dir.Id; |})
        ()

    /// <summary>
    /// Represents an entry in the file system, either a file or directory.  Used for recursive listing operations.
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


    /// <summary>
    /// Retrieves the length of a file from the database based on its ID.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to retrieve the length for.</param>
    /// <returns>The length of the file in bytes.</returns>
    let private getFileLengthById (db: SqliteConnection) (fileId: int64) =
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
    let private tryGetChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (buffer: Span<byte>) =
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
        

    /// <summary>
    /// Retrieves all compressed chunks within a specified range for a given file ID, filling in any gaps with empty chunks.  The native memory associated with each chunk is automatically freed after iteration.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to retrieve chunks from.</param>
    /// <param name="startChunk">The starting chunk number (inclusive).</param>
    /// <param name="lastChunk">The ending chunk number (inclusive).</param>
    /// <returns>A sequence of ChunkDTO objects representing the compressed chunks within the range, including empty chunks for any gaps.</returns>
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

    /// <summary>
    /// Updates the modification timestamp of a file in the database based on its ID, only if the current timestamp is older than the existing one.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="fileId">The ID of the file to update.</param>
    /// <param name="modified">The new modification timestamp.</param>
    let private setFileModifiedById (db: SqliteConnection) (fileId: int64) (modified: DateTimeOffset) =
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
    let private setFileCreatedById (db: SqliteConnection) (fileId: int64) (created: DateTimeOffset) =
        db.Execute("UPDATE SoloDBFileHeader 
        SET Created = @Created
        WHERE Id = @FileId", {|Created = created; FileId = fileId|})
        |> ignore

    /// <summary>
    /// SQL query to update the modification timestamp of a file and its parent directory, only if their current timestamps are older than the new one.
    /// </summary>
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

    /// <summary>
    /// SQL query to wrap the update modification timestamp operation in a transaction.
    /// </summary>
    [<Literal>]
    let private updateModifiedTimestampTrSQL = "BEGIN;" + updateModifiedTimestampSQL + "COMMIT TRANSACTION;"
    
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


    /// <summary>
    /// Splits a file path into its directory path and file name. Normalizes separators to '/'.
    /// </summary>
    /// <param name="path">The input path string.</param>
    /// <returns>A struct tuple containing the directory path and the name.</returns>
    let private getPathAndName (path: string) =
        let normalizedCompletePath = path.Split('\\', '/') |> Array.filter (fun x -> x |> String.IsNullOrWhiteSpace |> not)
        let sep = "/"
        let dirPath = $"/{normalizedCompletePath |> Array.take (Math.Max(normalizedCompletePath.Length - 1, 0l)) |> String.concat sep}"
        let name = match normalizedCompletePath |> Array.tryLast with Some x -> x | None -> ""
        struct (dirPath, name)

    /// <summary>
    /// Normalizes a given path by cleaning up separators and combining directory and name parts.
    /// </summary>
    /// <param name="path">The input path string.</param>
    /// <returns>A normalized full path string.</returns>
    let private formatPath path =
        let struct (dir, name) = getPathAndName path
        let dirPath = combinePath dir name
        dirPath

    /// <summary>
    /// Creates a new file entry in the database at the specified path, including creating parent directories if they don't exist.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The full path for the new file.</param>
    /// <returns>The header of the newly created file.</returns>
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

    /// <summary>
    /// Lists all subdirectories directly within a given path.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The path of the parent directory.</param>
    /// <returns>A sequence of directory headers for the subdirectories.</returns>
    let private listDirectoriesAt (db: SqliteConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> Seq.empty
        | Some dir ->
        let childres = tryGetDirectoriesWhere db "ParentId = @Id" {|Id = dir.Id|}
        childres

    /// <summary>
    /// Retrieves file headers that match a given WHERE clause.
    /// </summary>
    /// <param name="connection">The SQLite connection.</param>
    /// <param name="where">The SQL WHERE clause (without the 'WHERE' keyword).</param>
    /// <param name="parameters">An object containing parameters for the query.</param>
    /// <returns>A sequence of file headers matching the criteria.</returns>
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

    /// <summary>
    /// Tries to get the header for a file at a specific path.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The full path of the file.</param>
    /// <returns>An option containing the file header if found, otherwise None.</returns>
    let private tryGetFileAt (db: SqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let fullPath = combinePath dirPath name
        getFilesWhere db "fh.FullPath = @FullPath" {|FullPath = fullPath|} |> Seq.tryHead

    /// <summary>
    /// Gets the header for a file at a specific path, creating it if it doesn't exist.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The full path of the file.</param>
    /// <returns>The file header.</returns>
    let private getOrCreateFileAt (db: SqliteConnection) (path: string) =
        use l = lockPathIfNotInTransaction db path

        match tryGetFileAt db path with
        | Some f -> f
        | None ->
        createFileAt db path

    /// <summary>
    /// Deletes a directory at the specified path if it's empty.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The path of the directory to delete.</param>
    /// <returns>True if the directory was deleted, false if it did not exist.</returns>
    let private deleteDirectoryAt (db: SqliteConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> false
        | Some dir ->

        deleteDirectory db dir
        true

    /// <summary>
    /// Gets the header for a directory at a specific path, creating it and any parent directories if they don't exist.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The full path of the directory.</param>
    /// <returns>The directory header.</returns>
    let private getOrCreateDirectoryAt (db: SqliteConnection) (path: string) = 
        let dirPath = formatPath path
        getOrCreateDir db dirPath

    /// <summary>
    /// Lists all files directly within a given path.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="path">The path of the directory.</param>
    /// <returns>A sequence of file headers.</returns>
    let private listFilesAt (db: SqliteConnection) (path: string) : SoloDBFileHeader seq = 
        let dirPath = formatPath path
        match tryGetDir db dirPath with 
        | None -> Seq.empty
        | Some dir ->
        getFilesWhere db "DirectoryId = @DirectoryId" {|DirectoryId = dir.Id|} |> ResizeArray :> SoloDBFileHeader seq

    /// <summary>
    /// Creates a DbFileStream for a given file header.
    /// </summary>
    /// <param name="db">The database connection provider.</param>
    /// <param name="file">The header of the file to open.</param>
    /// <returns>A new DbFileStream instance.</returns>
    let private openFile (db: Connection) (file: SoloDBFileHeader) =
        new DbFileStream(db, file.Id, file.DirectoryId, file.FullPath)

    /// <summary>
    /// Opens or creates a file at the specified path and returns a stream.
    /// </summary>
    /// <param name="db">The database connection provider.</param>
    /// <param name="path">The full path of the file.</param>
    /// <returns>A new DbFileStream instance for the file.</returns>
    let private openOrCreateFile (db: Connection) (path: string) =
        let file = 
            use conn = db.Get()
            match tryGetFileAt conn path with
            | Some x -> x 
            | None -> createFileAt conn path

        new DbFileStream(db, file.Id, file.DirectoryId, file.FullPath)

    /// <summary>
    /// Sets a metadata key-value pair for a file.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="file">The file header.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    let private setSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBFileMetadata(FileId, Key, Value) VALUES(@FileId, @Key, @Value)", {|FileId = file.Id; Key = key; Value = value|}) |> ignore

    /// <summary>
    /// Deletes a metadata key-value pair from a file.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="file">The file header.</param>
    /// <param name="key">The metadata key to delete.</param>
    let private deleteSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBFileMetadata WHERE FileId = @FileId AND Key = @Key", {|FileId = file.Id; Key = key|}) |> ignore

    /// <summary>
    /// Sets a metadata key-value pair for a directory.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="dir">The directory header.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    let private setDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) VALUES(@DirectoryId, @Key, @Value)", {|DirectoryId = dir.Id; Key = key; Value = value|}) |> ignore

    /// <summary>
    /// Deletes a metadata key-value pair from a directory.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="dir">The directory header.</param>
    /// <param name="key">The metadata key to delete.</param>
    let private deleteDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId AND Key = @Key", {|DirectoryId = dir.Id; Key = key|}) |> ignore
    
    /// <summary>
    /// Moves a file to a new directory and/or renames it.
    /// </summary>
    /// <param name="db">The SQLite connection.</param>
    /// <param name="file">The header of the file to move.</param>
    /// <param name="toDir">The header of the destination directory.</param>
    /// <param name="newName">The new name for the file.</param>
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

    /// <summary>
    /// Recursively moves a directory and all its contents. Must be called within a transaction.
    /// </summary>
    /// <param name="db">The SQLite connection (must be in a transaction).</param>
    /// <param name="dir">The directory to move.</param>
    /// <param name="newParentDir">The destination parent directory.</param>
    /// <param name="newName">The new name for the directory.</param>
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
    
    /// <summary>
    /// Represents the data for a single file in a bulk upload operation.
    /// </summary>
    type BulkFileData = {
        /// <summary>The full path where the file should be stored.</summary>
        FullPath: string
        /// <summary>The binary content of the file.</summary>
        Data: byte array
        /// <summary>Optional. The creation timestamp to set for the file.</summary>
        Created: Nullable<DateTimeOffset>
        /// <summary>Optional. The modification timestamp to set for the file.</summary>
        Modified: Nullable<DateTimeOffset>
    }

    /// <summary>
    /// Provides an API for interacting with a virtual file system within the SQLite database.
    /// </summary>
    /// <param name="connection">The database connection provider.</param>
    type FileSystem(connection: Connection) =
        /// <summary>
        /// Uploads a stream to a file at the specified path. If the file exists, it is overwritten. If it does not exist, it is created.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <param name="stream">The stream containing the file data to upload.</param>
        member this.Upload(path, stream: Stream) =
            use file = openOrCreateFile connection path
            stream.CopyTo(file, int chunkSize)
            file.SetLength file.Position

        /// <summary>
        /// Asynchronously uploads a stream to a file at the specified path. If the file exists, it is overwritten. If it does not exist, it is created.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <param name="stream">The stream containing the file data to upload.</param>
        member this.UploadAsync(path, stream: Stream) = task {
            use file = openOrCreateFile connection path
            do! stream.CopyToAsync(file, int chunkSize)
            file.SetLength file.Position
        }

        /// <summary>
        /// Uploads a sequence of files in a single transaction. This is more efficient for uploading many small files.
        /// </summary>
        /// <param name="files">A sequence of <c>BulkFileData</c> records representing the files to upload.</param>
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

        /// <summary>
        /// Asynchronously replaces a file's content from a stream within a single transaction.
        /// </summary>
        /// <param name="path">The path of the file to replace.</param>
        /// <param name="stream">The stream with the new content.</param>
        member this.ReplaceAsyncWithinTransaction(path, stream: Stream) = task {
            return! connection.WithAsyncTransaction(fun tx -> task {
                use _disabled = (unbox<IDisableDispose> tx).DisableDispose()
                let innerConnection = Transitive tx
                use file = openOrCreateFile innerConnection path
                do! stream.CopyToAsync(file, int chunkSize)
                file.SetLength file.Position
            })
        }

        /// <summary>
        /// Downloads a file from the specified path and writes its content to the provided stream.
        /// </summary>
        /// <param name="path">The full path of the file to download.</param>
        /// <param name="stream">The stream to which the file content will be written.</param>
        /// <exception cref="FileNotFoundException">Thrown if the file does not exist at the specified path.</exception>
        member this.Download(path, stream: Stream) =
            
            let fileHeader = 
                use db = connection.Get()
                match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

            use fileStream = openFile connection fileHeader
            fileStream.CopyTo(stream, int chunkSize * 10)

        /// <summary>
        /// Asynchronously downloads a file from the specified path and writes its content to the provided stream.
        /// </summary>
        /// <param name="path">The full path of the file to download.</param>
        /// <param name="stream">The stream to which the file content will be written.</param>
        /// <exception cref="FileNotFoundException">Thrown if the file does not exist at the specified path.</exception>
        member this.DownloadAsync(path, stream: Stream) = task {
            
            let fileHeader = 
                use db = connection.Get()
                match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

            use fileStream = openFile connection fileHeader
            do! fileStream.CopyToAsync(stream, int chunkSize * 10)
        }

        /// <summary>
        /// Gets the header information for a file at the specified path.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>The <c>SoloDBFileHeader</c> for the file.</returns>
        /// <exception cref="FileNotFoundException">Thrown if the file does not exist at the specified path.</exception>
        member this.GetAt path =
            use db = connection.Get()
            match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

        /// <summary>
        /// Tries to get the header information for a file at the specified path.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>An option containing the <c>SoloDBFileHeader</c> if the file exists, otherwise None.</returns>
        member this.TryGetAt path =
            use db = connection.Get()
            tryGetFileAt db path

        /// <summary>
        /// Gets the header information for a file at the specified path, creating it if it does not exist.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>The <c>SoloDBFileHeader</c> for the file.</returns>
        member this.GetOrCreateAt path =
            use db = connection.Get()
            getOrCreateFileAt db path

        /// <summary>
        /// Gets the header information for a directory at the specified path.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>The <c>SoloDBDirectoryHeader</c> for the directory.</returns>
        /// <exception cref="DirectoryNotFoundException">Thrown if the directory does not exist at the specified path.</exception>
        member this.GetDirAt path =
            use db = connection.Get()
            match tryGetDir db path with | Some f -> f | None -> raise (DirectoryNotFoundException("Directory not found at: " + path))

        /// <summary>
        /// Tries to get the header information for a directory at the specified path.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>An option containing the <c>SoloDBDirectoryHeader</c> if the directory exists, otherwise None.</returns>
        member this.TryGetDirAt path =
            use db = connection.Get()
            tryGetDir db path

        /// <summary>
        /// Gets the header information for a directory at the specified path, creating it if it does not exist.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>The <c>SoloDBDirectoryHeader</c> for the directory.</returns>
        member this.GetOrCreateDirAt path =
            use db = connection.Get()
            getOrCreateDirectoryAt db path

        /// <summary>
        /// Opens a file stream for a given file header.
        /// </summary>
        /// <param name="file">The file header of the file to open.</param>
        /// <returns>A readable and writable <c>Stream</c> for the file.</returns>
        member this.Open file =
            openFile connection file

        /// <summary>
        /// Opens a file stream for a file at the specified path.
        /// </summary>
        /// <param name="path">The full path of the file to open.</param>
        /// <returns>A readable and writable <c>Stream</c> for the file.</returns>
        /// <exception cref="FileNotFoundException">Thrown if the file does not exist at the specified path.</exception>
        member this.OpenAt path =
            let file = this.GetAt path
            openFile connection file

        /// <summary>
        /// Tries to open a file stream for a file at the specified path.
        /// </summary>
        /// <param name="path">The full path of the file to open.</param>
        /// <returns>An option containing the <c>Stream</c> if the file exists, otherwise None.</returns>
        member this.TryOpenAt path =
            match this.TryGetAt path with
            | None -> None
            | Some file ->
            openFile connection file |> Some

        /// <summary>
        /// Opens a file stream for a file at the specified path, creating the file if it does not exist.
        /// </summary>
        /// <param name="path">The full path of the file to open or create.</param>
        /// <returns>A readable and writable <c>Stream</c> for the file.</returns>
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

        /// <summary>
        /// Reads a specified number of bytes from a file at a given offset.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <param name="offset">The zero-based byte offset in the file at which to begin reading.</param>
        /// <param name="len">The number of bytes to read.</param>
        /// <returns>A byte array containing the data read from the file.</returns>
        member this.ReadAt(path: string, offset: int64, len) =
            let file = this.GetAt path

            use fileStream = new BinaryReader(openFile connection file)
            fileStream.BaseStream.Position <- offset
            fileStream.ReadBytes len

        /// <summary>
        /// Sets the modification date of a file.
        /// </summary>
        /// <param name="path">The path of the file.</param>
        /// <param name="date">The new modification date.</param>
        member this.SetFileModificationDate(path, date) =
            let file = this.GetAt path
            use db = connection.Get()
            setFileModifiedById db file.Id date

        /// <summary>
        /// Sets the creation date of a file.
        /// </summary>
        /// <param name="path">The path of the file.</param>
        /// <param name="date">The new creation date.</param>
        member this.SetFileCreationDate(path, date) =
            let file = this.GetAt path
            use db = connection.Get()
            setFileCreatedById db file.Id date

        /// <summary>
        /// Sets a metadata key-value pair for a file.
        /// </summary>
        /// <param name="file">The file header.</param>
        /// <param name="key">The metadata key.</param>
        /// <param name="value">The metadata value.</param>
        member this.SetMetadata(file, key, value) =
            use db = connection.Get()
            setSoloDBFileMetadata db file key value

        /// <summary>
        /// Sets a metadata key-value pair for a file at a given path.
        /// </summary>
        /// <param name="path">The path of the file.</param>
        /// <param name="key">The metadata key.</param>
        /// <param name="value">The metadata value.</param>
        member this.SetMetadata(path, key, value) =
            let file = this.GetAt path
            this.SetMetadata(file, key, value)

        /// <summary>
        /// Deletes a metadata key from a file.
        /// </summary>
        /// <param name="file">The file header.</param>
        /// <param name="key">The metadata key to delete.</param>
        member this.DeleteMetadata(file, key) =
            use db = connection.Get()
            deleteSoloDBFileMetadata db file key

        /// <summary>
        /// Deletes a metadata key from a file at a given path.
        /// </summary>
        /// <param name="path">The path of the file.</param>
        /// <param name="key">The metadata key to delete.</param>
        member this.DeleteMetadata(path, key) =
            let file = this.GetAt path
            this.DeleteMetadata(file, key)

        /// <summary>
        /// Sets a metadata key-value pair for a directory.
        /// </summary>
        /// <param name="dir">The directory header.</param>
        /// <param name="key">The metadata key.</param>
        /// <param name="value">The metadata value.</param>
        member this.SetDirectoryMetadata(dir, key, value) =
            use db = connection.Get()
            setDirMetadata db dir key value

        /// <summary>
        /// Sets a metadata key-value pair for a directory at a given path.
        /// </summary>
        /// <param name="path">The path of the directory.</param>
        /// <param name="key">The metadata key.</param>
        /// <param name="value">The metadata value.</param>
        member this.SetDirectoryMetadata(path, key, value) =
            let dir = this.GetDirAt path
            this.SetDirectoryMetadata(dir, key, value)

        /// <summary>
        /// Deletes a metadata key from a directory.
        /// </summary>
        /// <param name="dir">The directory header.</param>
        /// <param name="key">The metadata key to delete.</param>
        member this.DeleteDirectoryMetadata(dir, key) =
            use db = connection.Get()
            deleteDirMetadata db dir key

        /// <summary>
        /// Deletes a metadata key from a directory at a given path.
        /// </summary>
        /// <param name="path">The path of the directory.</param>
        /// <param name="key">The metadata key to delete.</param>
        member this.DeleteDirectoryMetadata(path, key) =
            let dir = this.GetDirAt path
            this.DeleteDirectoryMetadata(dir, key)

        /// <summary>
        /// Deletes a file.
        /// </summary>
        /// <param name="file">The header of the file to delete.</param>
        member this.Delete(file) =
            use db = connection.Get()
            deleteFile db file

        /// <summary>
        /// Deletes a directory. This will fail if the directory is not empty.
        /// </summary>
        /// <param name="dir">The header of the directory to delete.</param>
        member this.Delete(dir) =
            use db = connection.Get()
            deleteDirectory db dir

        /// <summary>
        /// Deletes a file at the specified path.
        /// </summary>
        /// <param name="path">The path of the file to delete.</param>
        /// <returns>True if the file was deleted, false if it did not exist.</returns>
        member this.DeleteFileAt(path) =
            match this.TryGetAt path with
            | None -> false
            | Some file ->
            this.Delete file
            true

        /// <summary>
        /// Deletes a directory at the specified path. This will fail if the directory is not empty.
        /// </summary>
        /// <param name="path">The path of the directory to delete.</param>
        /// <returns>True if the directory was deleted, false if it did not exist.</returns>
        member this.DeleteDirAt(path) =
            use db = connection.Get()
            deleteDirectoryAt db path

        /// <summary>
        /// Lists all files directly within the specified directory path.
        /// </summary>
        /// <param name="path">The path of the directory.</param>
        /// <returns>A sequence of file headers.</returns>
        member this.ListFilesAt(path) =
            use db = connection.Get()
            listFilesAt db path

        /// <summary>
        /// Lists all subdirectories directly within the specified directory path.
        /// </summary>
        /// <param name="path">The path of the directory.</param>
        /// <returns>A sequence of directory headers.</returns>
        member this.ListDirectoriesAt(path) =
            use db = connection.Get()
            listDirectoriesAt db path

        /// <summary>
        /// Recursively lists all entries (files and directories) starting from the specified path. The result is buffered into a list.
        /// </summary>
        /// <param name="path">The starting path.</param>
        /// <returns>A list of all entries.</returns>
        member this.RecursiveListEntriesAt(path) =
            use db = connection.Get()
            recursiveListAllEntriesInDirectory db path    
            // If the downstream processing is slow, it can lock the DB and timeout on other's access,
            // therefore we will cache them in a List.
            |> ResizeArray 
            :> IList<SoloDBEntryHeader>

        /// <summary>
        /// Lazily and recursively lists all entries (files and directories) starting from the specified path.
        /// This method is not recommended for long-running operations as it can hold the database connection open.
        /// </summary>
        /// <param name="path">The starting path.</param>
        /// <returns>A lazy sequence of all entries.</returns>
        member this.RecursiveListEntriesAtLazy(path) = seq {
            use db = connection.Get()
            yield! recursiveListAllEntriesInDirectory db path
        }

        /// <summary>
        /// Moves a file from a source path to a destination path.
        /// </summary>
        /// <param name="from">The source path of the file.</param>
        /// <param name="toPath">The destination path for the file.</param>
        /// <exception cref="IOException">Thrown if a file already exists at the destination.</exception>
        member this.MoveFile(from, toPath) =
            let file = this.GetAt from
            let struct (toDirPath, fileName) = getPathAndName toPath
            let dir = this.GetOrCreateDirAt toDirPath
            use db = connection.Get()
            moveFile db file dir fileName

        /// <summary>
        /// Moves a file from a source path to a destination path, replacing the destination file if it exists.
        /// </summary>
        /// <param name="from">The source path of the file.</param>
        /// <param name="toPath">The destination path for the file.</param>
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

        /// <summary>
        /// Moves a directory from a source path to a destination path. All contents are moved recursively.
        /// </summary>
        /// <param name="from">The source path of the directory.</param>
        /// <param name="toPath">The destination path for the directory.</param>
        /// <exception cref="IOException">Thrown if a file or directory already exists at the destination.</exception>
        member this.MoveDirectory(from, toPath) =
            let struct (toDirPath, fileName) = getPathAndName toPath
            connection.WithTransaction(fun db ->
                let dirToMove = match tryGetDir db from with | Some f -> f | None -> raise (DirectoryNotFoundException("From directory not found at: " + from))
                let toPathParent = match tryGetDir db toDirPath with | Some f -> f | None -> raise (DirectoryNotFoundException("To directory not found at: " + toDirPath))

                moveDirectoryMustBeWithinTransaction db dirToMove toPathParent fileName
            )
