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

module FileStorage =
    let chunkSize = 
        16384L // 16KB, aprox. a SQLite page size.

    let maxChunkStoreSize = 
        chunkSize + 100L // If the compression fails.

    let disableHash = false
    let disableCompression = false
    let ensureFullBufferReadInDbFileStream = true // There are some libraries that wrongly assume so.

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

    let private getOrCreateDir (db: SqliteConnection) (path: string) =
        use l = lockPathIfNotInTransaction db path

        match tryGetDir db path with
        | Some d -> d
        | None ->


        let names = path.Split ([|'/'|], StringSplitOptions.RemoveEmptyEntries) |> Array.toList
        let root = db.QueryFirst<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE Name = @RootName", {|RootName = ""|})


        let rec innerLoop (prev: SoloDBDirectoryHeader) (remNames: string list) (prevName: string list) = 
            match remNames with
            | [] -> prev
            | head :: tail ->
                match db.QueryFirstOrDefault<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE Name = @Name AND ParentId = @ParentId", {|Name = head; ParentId = prev.Id|}) with
                | dir when Object.ReferenceEquals(dir, null) -> 
                    let sep = "/"
                    let newDir = {|
                        Name = head
                        ParentId = prev.Id
                        FullPath = $"/{(prevName @ [head] |> String.concat sep)}"
                    |}
                    let result = db.Execute("INSERT INTO SoloDBDirectoryHeader(Name, ParentId, FullPath) VALUES(@Name, @ParentId, @FullPath)", newDir)
                    innerLoop prev (head :: tail) prevName
                | dir -> innerLoop dir tail (prevName @ [head])
    
        innerLoop root names [] |> fillDirectoryMetadata db
    
    let private updateLenById (db: SqliteConnection) (fileId: int64) (len: int64) =
        let result = db.Execute ("UPDATE SoloDBFileHeader SET Length = @Length WHERE Id = @Id", {|Id = fileId; Length=len|})
        if result <> 1 then failwithf "updateLen failed."

    let private downsetFileLength (db: SqliteConnection) (fileId: int64) (newFileLength: int64) =
        let lastChunkNumberKeep = ((float(newFileLength) / float(chunkSize)) |> Math.Ceiling |> int64) - 1L

        let resultDelete = db.Execute(@"DELETE FROM SoloDBFileChunk WHERE FileId = @FileId AND Number > @LastChunkNumber",
                       {| FileId = fileId; LastChunkNumber = lastChunkNumberKeep |})

        let resultUpdate = db.Execute(@"UPDATE SoloDBFileHeader 
                        SET Length = @NewFileLength
                        WHERE Id = @FileId",
                       {| FileId = fileId; NewFileLength = newFileLength |})
        ()

    let private deleteFile (db: SqliteConnection) (file: SoloDBFileHeader) =
        let result = db.Execute(@"DELETE FROM SoloDBFileHeader WHERE Id = @FileId",
                    {| FileId = file.Id; |})

        ()

    let rec private deleteDirectory (db: SqliteConnection) (dir: SoloDBDirectoryHeader) = 
        let result = db.Execute(@"DELETE FROM SoloDBDirectoryHeader WHERE Id = @DirId",
                        {| DirId = dir.Id; |})
        ()

    [<CLIMutable>]
    type SQLEntry = {
        Level: int64; 
        Id: int64; 
        Type: string; 
        Path: string; 
        Name: string;
        Size: int64;
        Created: DateTimeOffset;
        Modified: DateTimeOffset;
        DirectoryId: Nullable<int64>;
        Hash: byte array; // Nullable
        MetadataKey: string;
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
            	NULL as Hash,
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
            	fh.Hash as Hash,
                fm.Key AS MetadataKey,
                fm.Value AS MetadataValue
            FROM
                DirectoryTree dt
            INNER JOIN
                SoloDBFileHeader fh ON dt.Id = fh.DirectoryId
            LEFT JOIN
                SoloDBFileMetadata fm ON fh.Id = fm.FileId
            """

        let query = 
            db.Query<SQLEntry> (queryCommand, {|FullPath = directoryFullPath|}) 
            |> Utils.SeqExt.sequentialGroupBy(fun e -> e.Path) 
            |> Seq.map(fun entry -> 
                let metadata = entry |> Seq.filter(fun x -> x.MetadataKey <> null) |> Seq.map(fun x -> (x.MetadataKey, x.MetadataValue)) |> readOnlyDict
                let entryData = entry |> Seq.head
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
                        Hash = entryData.Hash;
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

        query


    let private getFileLengthById (db: SqliteConnection) (fileId: int64) =
        db.QueryFirst<int64>(@"SELECT Length FROM SoloDBFileHeader WHERE Id = @FileId",
                       {| FileId = fileId |})

    let private tryGetChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (buffer: Span<byte>) =
        let sql = "SELECT Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number = @ChunkNumber"
        match db.QueryFirstOrDefault<byte array>(sql, {| FileId = fileId; ChunkNumber = chunkNumber |}) with
        | data when Object.ReferenceEquals(data, null) -> Span.Empty
        | data -> 
            if disableCompression then
                Span<byte>(data).CopyTo(buffer)
                buffer.Slice(0, data.Length)
            else
                let len = Snappy.Decompress(Span<byte>(data), buffer)
                buffer.Slice(0, len)


    let private writeChunkData (db: SqliteConnection) (fileId: int64) (chunkNumber: int64) (data: Span<byte>) =
        use command = db.CreateCommand()
        command.CommandText <- "INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) VALUES (@FileId, @Number, @Data)"

        let p = command.CreateParameter()
        p.ParameterName <- "FileId"
        p.Value <- fileId
        command.Parameters.Add p |> ignore

        let p = command.CreateParameter()
        p.ParameterName <- "Number"
        p.Value <- chunkNumber
        command.Parameters.Add p |> ignore

        let arrayBuffer = Array.zeroCreate (int maxChunkStoreSize)
        let memoryBuffer = arrayBuffer.AsSpan()
        let len = 
            if disableCompression then
                data.CopyTo(memoryBuffer)
                data.Length
            else
                Snappy.Compress(data, memoryBuffer)

        let p = command.CreateParameter()
        p.ParameterName <- "Data"
        p.Value <- arrayBuffer
        p.Size <- len // Crop to size.
        command.Parameters.Add p |> ignore

        let result = command.ExecuteNonQuery()

        if result <> 1 then failwithf "writeChunkData failed."


    let private getStoredChunks (db: SqliteConnection) (fileId: int64) =
        use command = db.CreateCommand()
        command.CommandText <- "SELECT Number, Data FROM SoloDBFileChunk WHERE FileId = @FileId ORDER BY Number ASC"

        let p = command.CreateParameter()
        p.ParameterName <- "FileId"
        p.Value <- fileId
        command.Parameters.Add p |> ignore

        seq {
            use reader = command.ExecuteReader()
            while reader.Read() do
                let memoryBuffer = Array.zeroCreate (int maxChunkStoreSize)
                let number = reader.GetInt64(0)
                let data = reader.GetValue(1)
                let data = data :?> byte array
                let count = 
                    if disableCompression then
                        Array.Copy(data, memoryBuffer, 0)
                        data.Length
                    else
                        Snappy.Decompress(data, Span<byte>(memoryBuffer))
                struct {|Number = number; Data = memoryBuffer; Len = count|}
                
        }
        

    let private emptyChunk = Array.zeroCreate<byte> (int chunkSize)
    let private getAllChunks (db: SqliteConnection) (fileId: int64) = seq {
        let mutable previousNumber = -1L // Chunks start from 0.
        for chunk in getStoredChunks db fileId do
            while chunk.Number - 1L > previousNumber do
                yield struct {|Number = previousNumber + 1L; Data = emptyChunk; Len = (int chunkSize)|}
                previousNumber <- previousNumber + 1L

            yield chunk
            previousNumber <- chunk.Number
    }

    let private updateHashById (db: SqliteConnection) (fileId: int64) (hash: byte array) =
        db.Execute("UPDATE SoloDBFileHeader 
        SET Hash = @Hash
        WHERE Id = @FileId", {|Hash = hash; FileId = fileId|})
        |> ignore

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

    let private calculateHash (db: SqliteConnection) (fileId: int64) (len: int64) =
        use sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1)
        
        for chunk in getAllChunks db fileId do
            let data = chunk.Data
            let struct (block, len) =
                let currentChunkDataOffset = chunk.Number * chunkSize
                if currentChunkDataOffset + chunkSize <= len then 
                    struct (data, chunk.Len)
                else 
                    let blockReadCount = int (len - currentChunkDataOffset) // Calculate how many bytes should actually be read to not exceed the file length
                    struct (data, blockReadCount) // Trim the chunk data to match the file's actual length

            sha1.AppendData(block, 0, len)

        sha1.GetHashAndReset()

    
    type DbFileStream(db: SqliteConnection, fileId: int64, fullPath: string, previousHash: byte array) =
        inherit Stream()

        let mutable position = 0L
        let mutable disposed = false
        let mutable dirty = false

        let checkDisposed() = if disposed then raise (ObjectDisposedException(nameof(DbFileStream)))

        member private this.UpdateHash() =
            if not disableHash && dirty then
                let hash = calculateHash db fileId this.Length
                if hash <> previousHash then
                    updateHashById db fileId hash


        override _.CanRead = not disposed
        override _.CanSeek = not disposed
        override _.CanWrite = not disposed

        override _.Length = 
            checkDisposed()
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
            this.UpdateHash()

        override this.Read(buffer: byte[], offset: int, count: int) : int =
            checkDisposed()
            if buffer = null then raise (ArgumentNullException(nameof buffer))
            if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
            if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
            if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))

            let position = this.Position
            let count = int64 count

            let chunkNumber = position / int64 chunkSize
            let chunkOffset = position % int64 chunkSize

            let len = this.Length
            let bytesToRead = min count (chunkSize - chunkOffset) |> min (len - position)

            if bytesToRead = 0 then
                0
            else

            let chunkBuffer = Array.zeroCreate (int maxChunkStoreSize)

            match tryGetChunkData db fileId chunkNumber (Span chunkBuffer) with
            | data when data.IsEmpty ->
                let maxChunkNumber = (min (position + count) len) / int64 chunkSize
                if maxChunkNumber > chunkNumber then // Try to load the other chunk if the file was sparsely allocated.
                    let offsetToNextChunk = chunkSize - chunkOffset

                    let bytesToRead = int (min (len - position) count) - int offsetToNextChunk
                    buffer.AsSpan(offset, int offsetToNextChunk).Fill(0uy)

                    this.Position <- position + offsetToNextChunk
                    this.Read(buffer, offset + int offsetToNextChunk, bytesToRead) + int offsetToNextChunk
                else
                0
            | chunkData ->

            chunkData.Slice(int chunkOffset, int bytesToRead).CopyTo(buffer.AsSpan(offset))
        
            this.Position <- position + bytesToRead
            let remainingBytes = count - bytesToRead

            if ensureFullBufferReadInDbFileStream && remainingBytes > 0 then
                this.Read(buffer, offset + int bytesToRead, int remainingBytes) + int bytesToRead
            else int bytesToRead
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

            while writing do
                writing <- false

                let position = position
                let mutable remainingBytes = 0L
                let mutable bytesToWrite = 0L

                let bufferArray = Array.zeroCreate (int maxChunkStoreSize)
                use l = lockFileIdIfNotInTransaction db fileId

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

        override _.SetLength(value: int64) =
            checkDisposed()

            if value < 0 then 
                raise (ArgumentOutOfRangeException("Length"))

            dirty <- true
            use l = lockFileIdIfNotInTransaction db fileId
            downsetFileLength db fileId value
            if position > value then position <- value

        override this.Seek(offset: int64, origin: SeekOrigin) =
            checkDisposed()
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
            Hash = null
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
                    SELECT fh.*, fm.Key, fm.Value
                    FROM SoloDBFileHeader fh
                    LEFT JOIN SoloDBFileMetadata fm ON fh.Id = fm.FileId
                    WHERE %s;
                    """ where
    
        let fileDictionary = new System.Collections.Generic.Dictionary<int64, SoloDBFileHeader>()
        let isNull x = Object.ReferenceEquals(x, null)

        
        connection.Query<SoloDBFileHeader, Metadata, unit>(
            query,
            (fun fileHeader metadata ->
                let file = 
                    match fileDictionary.TryGetValue(fileHeader.Id) with
                    | true, f ->
                        f
                    | false, _ ->
                        let f = 
                            if isNull fileHeader.Metadata then
                                {fileHeader with Metadata = Dictionary()}
                            else fileHeader
                            
                        fileDictionary.Add(f.Id, f)
                        f

                if not (isNull metadata) then
                    (file.Metadata :?> IDictionary<string, string>).Add(metadata.Key, metadata.Value)
                ()
            ),
            parameters,
            splitOn = "Key"
        ) |> Seq.iter ignore
    
        fileDictionary.Values :> SoloDBFileHeader seq

    let private tryGetFileAt (db: SqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let fullPath = combinePath dirPath name
        getFilesWhere db "fh.FullPath = @FullPath" {|FullPath = fullPath|} |> Seq.tryHead

    let private getFilesByHash (db: SqliteConnection) (hash: byte array) =
        getFilesWhere db "fh.Hash = @Hash" {|Hash = hash|}

    let private getOrCreateFileAt (db: SqliteConnection) (path: string) =
        use l = lockPathIfNotInTransaction db path

        match tryGetFileAt db path with
        | Some f -> f
        | None ->

        let struct (dirPath, name) = getPathAndName path
        let dir = getOrCreateDir db dirPath 

        match getFilesWhere db "DirectoryId = @DirectoryId AND Name = @Name" {|DirectoryId = dir.Id; Name = name|} |> Seq.tryHead with
        | None -> createFileAt db path
        | Some file -> file

    let private deleteDirectoryAt (db: SqliteConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> false
        | Some dir ->
        try
            deleteDirectory db dir
            true
        with ex ->
            printfn "%s" ex.Message
            false

    let private getOrCreateDirectoryAt (db: SqliteConnection) (path: string) = 
        let dirPath = formatPath path
        getOrCreateDir db dirPath


    let private listFilesAt (db: SqliteConnection) (path: string) : SoloDBFileHeader seq = 
        let dirPath = formatPath path
        match tryGetDir db dirPath with 
        | None -> Seq.empty
        | Some dir ->
        getFilesWhere db "DirectoryId = @DirectoryId" {|DirectoryId = dir.Id|}

    let private openFile (db: SqliteConnection) (file: SoloDBFileHeader) =
        new DbFileStream(db, file.Id, file.FullPath, file.Hash)

    let private openOrCreateFile (db: SqliteConnection) (path: string) =
        let file = tryGetFileAt db path
        let file = match file with
                    | Some x -> x 
                    | None -> createFileAt db path

        new DbFileStream(db, file.Id, file.FullPath, file.Hash)

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
            let subDirs = db.Query<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE ParentId = @DirId", {| DirId = dir.Id |})
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

    type FileSystem(manager: ConnectionManager) =
        member this.Upload(path, stream: Stream) =
            use db = manager.Borrow()
            use file = openOrCreateFile db path
            stream.CopyTo(file, int chunkSize)
            file.SetLength file.Position

        member this.UploadAsync(path, stream: Stream) = task {
            use db = manager.Borrow()
            use file = openOrCreateFile db path
            do! stream.CopyToAsync(file, int chunkSize)
            file.SetLength file.Position
        }

        member this.UploadBulk(files: BulkFileData seq) =
            manager.WithTransaction(fun tx -> 
                for file in files do
                    let fileHeader = getOrCreateFileAt tx file.FullPath

                    do
                        use fileStream = openFile tx fileHeader
                        fileStream.Write(file.Data, 0, file.Data.Length)
                        fileStream.SetLength file.Data.Length
                    
                    if file.Modified.HasValue then
                        setFileModifiedById tx fileHeader.Id file.Modified.Value

                    if file.Created.HasValue then
                        setFileCreatedById tx fileHeader.Id file.Created.Value

                    ()
            )

        member this.ReplaceAsyncWithinTransaction(path, stream: Stream) = task {
            return! manager.WithAsyncTransaction(fun db -> task {
                use file = openOrCreateFile db path
                do! stream.CopyToAsync(file, int chunkSize)
                file.SetLength file.Position
            })
        }

        member this.Download(path, stream: Stream) =
            use db = manager.Borrow()
            let fileHeader = match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
            use fileStream = openFile db fileHeader
            fileStream.CopyTo(stream, int chunkSize)

        member this.DownloadAsync(path, stream: Stream) = task {
            use db = manager.Borrow()
            let fileHeader = match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
            use fileStream = openFile db fileHeader
            do! fileStream.CopyToAsync(stream, int chunkSize)
        }

        member this.GetAt path =
            use db = manager.Borrow()
            match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

        member this.TryGetAt path =
            use db = manager.Borrow()
            tryGetFileAt db path

        member this.GetOrCreateAt path =
            use db = manager.Borrow()
            getOrCreateFileAt db path

        member this.GetDirAt path =
            use db = manager.Borrow()
            match tryGetDir db path with | Some f -> f | None -> raise (DirectoryNotFoundException("Directory not found at: " + path))

        member this.TryGetDirAt path =
            use db = manager.Borrow()
            tryGetDir db path

        member this.GetOrCreateDirAt path =
            use db = manager.Borrow()
            getOrCreateDirectoryAt db path

        member this.Open file =
            use db = manager.Borrow()
            openFile db file

        member this.OpenAt path =            
            let file = this.GetAt path
            use db = manager.Borrow()
            openFile db file

        member this.TryOpenAt path =            
            match this.TryGetAt path with
            | None -> None
            | Some file ->
            use db = manager.Borrow()
            openFile db file |> Some

        member this.OpenOrCreateAt path =
            use db = manager.Borrow()        
            openOrCreateFile db path

        member this.WriteAt(path, offset, data, ?createIfInexistent: bool) =
            let createIfInexistent = match createIfInexistent with Some x -> x | None -> true

            manager.WithTransaction(fun tx -> 
                let file = if createIfInexistent then getOrCreateFileAt tx path else match tryGetFileAt tx path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

                use fileStream = openFile tx file
                fileStream.Position <- offset
                fileStream.Write(data, 0, data.Length)
                ()
            )

        member this.WriteAt(path, offset, data: Stream, ?createIfInexistent: bool) =
            let createIfInexistent = match createIfInexistent with Some x -> x | None -> true

            manager.WithTransaction(fun tx -> 
                let file = if createIfInexistent then getOrCreateFileAt tx path else match tryGetFileAt tx path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

                use fileStream = openFile tx file
                fileStream.Position <- offset
                data.CopyTo (fileStream, int chunkSize)
                ()
            )

        member this.ReadAt(path, offset, len) =
            let file = this.GetAt path

            use db = manager.Borrow()
            use fileStream = new BinaryReader(openFile db file)
            fileStream.BaseStream.Position <- offset
            fileStream.ReadBytes len

        member this.SetFileModificationDate(path, date) =
            let file = this.GetAt path
            use db = manager.Borrow()
            setFileModifiedById db file.Id date

        member this.SetFileCreationDate(path, date) =
            let file = this.GetAt path
            use db = manager.Borrow()
            setFileCreatedById db file.Id date

        member this.SetMetadata(file, key, value) =
            use db = manager.Borrow()
            setSoloDBFileMetadata db file key value

        member this.SetMetadata(path, key, value) =
            let file = this.GetAt path
            this.SetMetadata(file, key, value)

        member this.DeleteMetadata(file, key) =
            use db = manager.Borrow()
            deleteSoloDBFileMetadata db file key

        member this.DeleteMetadata(path, key) =
            let file = this.GetAt path
            this.DeleteMetadata(file, key)

        member this.SetDirectoryMetadata(dir, key, value) =
            use db = manager.Borrow()
            setDirMetadata db dir key value

        member this.SetDirectoryMetadata(path, key, value) =
            let dir = this.GetDirAt path
            this.SetDirectoryMetadata(dir, key, value)

        member this.DeleteDirectoryMetadata(dir, key) =
            use db = manager.Borrow()
            deleteDirMetadata db dir key

        member this.DeleteDirectoryMetadata(path, key) =
            let dir = this.GetDirAt path
            this.DeleteDirectoryMetadata(dir, key)

        member this.GetFilesByHash(hash) =
            use db = manager.Borrow()
            getFilesByHash db hash

        member this.GetFileByHash(hash) =
            match this.GetFilesByHash hash |> Seq.tryHead with
            | None -> raise (FileNotFoundException("No file with such hash.", Utils.bytesToHex hash))
            | Some f -> f

        member this.Delete(file) =
            use db = manager.Borrow()
            deleteFile db file

        member this.Delete(dir) =
            use db = manager.Borrow()
            deleteDirectory db dir

        member this.DeleteFileAt(path) =
            match this.TryGetAt path with
            | None -> false
            | Some file ->
            this.Delete file
            true

        member this.DeleteDirAt(path) =
            use db = manager.Borrow()
            deleteDirectoryAt db path

        member this.ListFilesAt(path) =
            use db = manager.Borrow()
            listFilesAt db path

        member this.ListDirectoriesAt(path) =
            use db = manager.Borrow()
            listDirectoriesAt db path

        member this.RecursiveListEntriesAt(path) =
            seq {
                use db = manager.Borrow()
                yield! recursiveListAllEntriesInDirectory db path
            }

        member this.MoveFile(from, toPath) =
            let file = this.GetAt from
            let struct (toDirPath, fileName) = getPathAndName toPath
            let dir = this.GetOrCreateDirAt toDirPath
            use db = manager.Borrow()
            moveFile db file dir fileName

        member this.MoveReplaceFile(from, toPath) =
            let struct (toDirPath, fileName) = getPathAndName toPath
            manager.WithTransaction(fun db ->
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
            manager.WithTransaction(fun db ->
                let dirToMove = match tryGetDir db from with | Some f -> f | None -> raise (DirectoryNotFoundException("From directory not found at: " + from))
                let toPathParent = match tryGetDir db toDirPath with | Some f -> f | None -> raise (DirectoryNotFoundException("To directory not found at: " + toDirPath))

                moveDirectoryMustBeWithinTransaction db dirToMove toPathParent fileName
            )
            