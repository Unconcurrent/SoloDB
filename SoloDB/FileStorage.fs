namespace SoloDatabase

open System.Collections.Generic
open System
open System.IO
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.Types
open System.Security.Cryptography
open SoloDatabase.Connections
open Snappier
open System.Runtime.InteropServices
open System.Data

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

    let private lockPathIfNotInTransaction (db: IDbConnection) (path: string) =
        if db.IsWithinTransaction() then
            noopDisposer
        else
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(db.ConnectionString)}-Path-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(path)}")
        mutex :> IDisposable

    let private lockFileIdIfNotInTransaction (db: IDbConnection) (id: int64) =
        if db.IsWithinTransaction() then
            noopDisposer
        else
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(db.ConnectionString)}-FileId-{id}")
        mutex :> IDisposable

    let private fillDirectoryMetadata (db: IDbConnection) (directory: SoloDBDirectoryHeader) =
        let allMetadata = db.Query<Metadata>("SELECT Key, Value FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId", {|DirectoryId = directory.Id|})
        let dict = Dictionary<string, string>()

        for meta in allMetadata do
            dict[meta.Key] <- meta.Value

        {directory with Metadata = dict}


    let private tryGetDirectoriesWhere (connection: IDbConnection) (where: string) (parameters: obj) =
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

    let private tryGetDir (db: IDbConnection) (path: string) =
        tryGetDirectoriesWhere db "dh.FullPath = @Path" {|Path = path|} |> Seq.tryHead

    let rec private getOrCreateDir (db: IDbConnection) (path: string) =
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
    
    let private updateLenById (db: IDbConnection) (fileId: int64) (len: int64) =
        let result = db.Execute ("UPDATE SoloDBFileHeader SET Length = @Length WHERE Id = @Id", {|Id = fileId; Length=len|})
        if result <> 1 then failwithf "updateLen failed."

    let private downsetFileLength (db: IDbConnection) (fileId: int64) (newFileLength: int64) =
        let lastChunkNumberKeep = ((float(newFileLength) / float(chunkSize)) |> Math.Ceiling |> int64) - 1L

        let _resultDelete = db.Execute(@"DELETE FROM SoloDBFileChunk WHERE FileId = @FileId AND Number > @LastChunkNumber",
                       {| FileId = fileId; LastChunkNumber = lastChunkNumberKeep |})

        let _resultUpdate = db.Execute(@"UPDATE SoloDBFileHeader 
                        SET Length = @NewFileLength
                        WHERE Id = @FileId",
                       {| FileId = fileId; NewFileLength = newFileLength |})
        ()

    let private deleteFile (db: IDbConnection) (file: SoloDBFileHeader) =
        let _result = db.Execute(@"DELETE FROM SoloDBFileHeader WHERE Id = @FileId",
                    {| FileId = file.Id; |})

        ()

    let private deleteDirectory (db: IDbConnection) (dir: SoloDBDirectoryHeader) = 
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

    let private recursiveListAllEntriesInDirectory (db: IDbConnection) (directoryFullPath: string) =
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


    let private getFileLengthById (db: IDbConnection) (fileId: int64) =
        db.QueryFirst<int64>(@"SELECT Length FROM SoloDBFileHeader WHERE Id = @FileId",
                       {| FileId = fileId |})

    let private tryGetChunkData (db: IDbConnection) (fileId: int64) (chunkNumber: int64) (buffer: Span<byte>) =
        let sql = "SELECT Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number = @ChunkNumber"
        match db.QueryFirstOrDefault<byte array>(sql, {| FileId = fileId; ChunkNumber = chunkNumber |}) with
        | data when Object.ReferenceEquals(data, null) -> Span.Empty
        | data -> 
            let len = Snappy.Decompress(Span<byte>(data), buffer)
            buffer.Slice(0, len)


    let private writeChunkData (db: IDbConnection) (fileId: int64) (chunkNumber: int64) (data: Span<byte>) =
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

    let inline private getStoredChunks (db: IDbConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) =
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
    let private getAllCompressedChunksWithinRange (db: IDbConnection) (fileId: int64) (startChunk: int64) (lastChunk: int64) = seq {
        let mutable previousNumber = startChunk - 1L // Chunks start from 0.
        for chunk in getStoredChunks db fileId startChunk lastChunk do
            try
                while chunk.Number - 1L > previousNumber do
                    yield {Number = previousNumber + 1L; Data = NativeArray.NativeArray.Empty }
                    previousNumber <- previousNumber + 1L

                yield chunk
            finally chunk.Data.Dispose()
            previousNumber <- chunk.Number
    }

    let private setFileModifiedById (db: IDbConnection) (fileId: int64) (modified: DateTimeOffset) =
        db.Execute("UPDATE SoloDBFileHeader 
        SET Modified = @Modified
        WHERE Id = @FileId", {|Modified = modified; FileId = fileId|})
        |> ignore

    let private setFileCreatedById (db: IDbConnection) (fileId: int64) (created: DateTimeOffset) =
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
            dirty <- true
            let mutable writing = true
            let mutable offset = int64 0
            let mutable count = int64 buffer.Length

            use db = db.Get()
            use l = lockFileIdIfNotInTransaction db fileId

            while writing do
                writing <- false

                let position = position
                let mutable remainingBytes = 0L
                let mutable bytesToWrite = 0L

                let bufferArray = Array.zeroCreate (int maxChunkStoreSize)
                
                let chunkNumber = position / int64 chunkSize
                let chunkOffset = position % int64 chunkSize

                let dataBuffer = Span(bufferArray)

                let tryGetChunkDataResult = tryGetChunkData db fileId chunkNumber dataBuffer
                let existingChunk = match tryGetChunkDataResult with data when data.IsEmpty -> dataBuffer.Slice(0, int chunkSize) | data -> data

                let spaceInChunk = chunkSize - chunkOffset
                bytesToWrite <- min count spaceInChunk

                buffer.Slice(int offset, int bytesToWrite).CopyTo(existingChunk.Slice(int chunkOffset))

                writeChunkData db fileId chunkNumber existingChunk

                let newPosition = position + int64 bytesToWrite

                remainingBytes <- count - bytesToWrite
                if remainingBytes > 0 then
                    writing <- true
                    offset <- offset + bytesToWrite
                    count <- remainingBytes
                else 
                    if newPosition > this.Length then
                        updateLenById db fileId newPosition

                this.Position <- newPosition

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

    let private createFileAt (db: IDbConnection) (path: string) =
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

    let private listDirectoriesAt (db: IDbConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> Seq.empty
        | Some dir ->
        let childres = tryGetDirectoriesWhere db "ParentId = @Id" {|Id = dir.Id|}
        childres


    let private getFilesWhere (connection: IDbConnection) (where: string) (parameters: obj) =
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

    let private tryGetFileAt (db: IDbConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let fullPath = combinePath dirPath name
        getFilesWhere db "fh.FullPath = @FullPath" {|FullPath = fullPath|} |> Seq.tryHead

    let private getOrCreateFileAt (db: IDbConnection) (path: string) =
        use l = lockPathIfNotInTransaction db path

        match tryGetFileAt db path with
        | Some f -> f
        | None ->
        createFileAt db path

    let private deleteDirectoryAt (db: IDbConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> false
        | Some dir ->

        deleteDirectory db dir
        true

    let private getOrCreateDirectoryAt (db: IDbConnection) (path: string) = 
        let dirPath = formatPath path
        getOrCreateDir db dirPath


    let private listFilesAt (db: IDbConnection) (path: string) : SoloDBFileHeader seq = 
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

    let private setSoloDBFileMetadata (db: IDbConnection) (file: SoloDBFileHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBFileMetadata(FileId, Key, Value) VALUES(@FileId, @Key, @Value)", {|FileId = file.Id; Key = key; Value = value|}) |> ignore

    let private deleteSoloDBFileMetadata (db: IDbConnection) (file: SoloDBFileHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBFileMetadata WHERE FileId = @FileId AND Key = @Key", {|FileId = file.Id; Key = key|}) |> ignore


    let private setDirMetadata (db: IDbConnection) (dir: SoloDBDirectoryHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) VALUES(@DirectoryId, @Key, @Value)", {|DirectoryId = dir.Id; Key = key; Value = value|}) |> ignore

    let private deleteDirMetadata (db: IDbConnection) (dir: SoloDBDirectoryHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId AND Key = @Key", {|DirectoryId = dir.Id; Key = key|}) |> ignore
   
    let private moveFile (db: IDbConnection) (file: SoloDBFileHeader) (toDir: SoloDBDirectoryHeader) (newName: string) =
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

    let rec private moveDirectoryMustBeWithinTransaction (db: IDbConnection) (dir: SoloDBDirectoryHeader) (newParentDir: SoloDBDirectoryHeader) (newName: string) =
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
    
    let private createInnerConnectionInTransation tx =
        new DirectConnection(tx, true) :> IDbConnection |> Transitive

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
                let innerConnection = createInnerConnectionInTransation tx
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
                let innerConnection = createInnerConnectionInTransation tx
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
            fileStream.CopyTo(stream, int chunkSize)

        member this.DownloadAsync(path, stream: Stream) = task {
            
            let fileHeader = 
                use db = connection.Get()
                match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

            use fileStream = openFile connection fileHeader
            do! fileStream.CopyToAsync(stream, int chunkSize)
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
                let innerConnection = createInnerConnectionInTransation tx

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
                let innerConnection = createInnerConnectionInTransation tx

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
            