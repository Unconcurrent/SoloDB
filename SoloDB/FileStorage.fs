namespace SoloDatabase
module FileStorage =
    open System
    open System.IO
    open Microsoft.Data.Sqlite
    open Dapper
    open SoloDatabase.Types
    open System.Collections.Generic
    open System.Security.Cryptography
    open SoloDatabase.Connections

    let chunkSize = 
        4096L // 4KiB

    let private combinePath (a, b) = (a, b) |> Path.Combine |> _.Replace('\\', '/')
    let private combinePathArr arr = arr |> Path.Combine |> _.Replace('\\', '/')

    let private lockPath (db: SqliteConnection) (path: string) =
        let mutex = new DisposableMutex($"SoloDB-{db.ConnectionString.GetHashCode(StringComparison.InvariantCultureIgnoreCase)}-Path-{path.GetHashCode(StringComparison.InvariantCultureIgnoreCase)}")
        mutex

    let private lockFileId (db: SqliteConnection) (id: SqlId) =
        let mutex = new DisposableMutex($"SoloDB-{db.ConnectionString.GetHashCode(StringComparison.InvariantCultureIgnoreCase)}-FileId-{id.Value}")
        mutex

    let fillDirectoryMetadata (db: SqliteConnection) (directory: SoloDBDirectoryHeader) =
        let allMetadata = db.Query<Metadata>("SELECT Key, Value FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId", {|DirectoryId = directory.Id|}) |> Seq.toList
        let dict = Dictionary<string, string>()

        for meta in allMetadata do
            dict[meta.Key] <- meta.Value

        {directory with Metadata = dict}

    let private tryGetDir (db: SqliteConnection) (path: string) =
        let dir = db.QueryFirstOrDefault<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE FullPath = @Path", {|Path = path|})
        if Object.ReferenceEquals(dir, null) then
            None
        else 
            dir |> fillDirectoryMetadata db |> Some

    let private getOrCreateDir (db: SqliteConnection) (path: string) =
        use l = lockPath db path

        match tryGetDir db path with
        | Some d -> d
        | None ->


        let names = path.Split ('/', StringSplitOptions.RemoveEmptyEntries) |> Array.toList
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
    
    let private tryGetChunkData (db: SqliteConnection) (fileId: SqlId) (chunkNumber: int64) transaction =
        let sql = "SELECT Data FROM SoloDBFileChunk WHERE FileId = @FileId AND Number = @ChunkNumber"
        match db.QueryFirstOrDefault<byte array>(sql, {| FileId = fileId; ChunkNumber = chunkNumber |}, transaction) with
        | data when Object.ReferenceEquals(data, null) -> None
        | data -> data |> Some

    let private writeChunkData (db: SqliteConnection) (fileId: SqlId) (chunkNumber: int64) (data: byte array) transaction =
        let result = db.Execute("INSERT OR REPLACE INTO SoloDBFileChunk(FileId, Number, Data) VALUES (@FileId, @Number, @Data)", {|FileId = fileId; Number = chunkNumber; Data = data|}, transaction)
        if result <> 1 then failwithf "writeChunkData failed."

    let private updateLenById (db: SqliteConnection) (fileId: SqlId) (len: int64) transaction =
        let result = db.Execute ("UPDATE SoloDBFileHeader SET Length = @Length WHERE Id = @Id", {|Id = fileId.Value; Length=len|}, transaction)
        if result <> 1 then failwithf "updateLen failed."

    let private downsetFileLength (db: SqliteConnection) (fileId: SqlId) (newFileLength: int64) transaction =
        let lastChunkNumberKeep = ((float(newFileLength) / float(chunkSize)) |> Math.Ceiling |> int64) - 1L

        let resultDelete = db.Execute(@"DELETE FROM SoloDBFileChunk WHERE FileId = @FileId AND Number > @LastChunkNumber",
                       {| FileId = fileId; LastChunkNumber = lastChunkNumberKeep |},
                       transaction = transaction)

        let resultUpdate = db.Execute(@"UPDATE SoloDBFileHeader 
                        SET Length = @NewFileLength
                        WHERE Id = @FileId",
                       {| FileId = fileId; NewFileLength = newFileLength |},
                       transaction = transaction)
        ()

    let getDirectoryById (db: SqliteConnection) (dirId: SqlId) =
        let directory = db.QueryFirst<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE Id = @DirId", {|DirId = dirId|})
        directory


    let getDirectoryPath (db: SqliteConnection) (dir: SoloDBDirectoryHeader) =
        let rec innerLoop currentPath (dirId: Nullable<SqlId>) =
            match dirId with
            | dirId when not dirId.HasValue -> currentPath |> List.rev |> String.concat "/"
            | dirId -> 
                let dirId = dirId.Value
                let directory = db.QueryFirst<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE Id = @DirId", {|DirId = dirId|})
                innerLoop (directory.Name :: currentPath) directory.ParentId
    

        let path = innerLoop [] (Nullable dir.Id)

        "/" + path


    let private deleteFile (db: SqliteConnection) (file: SoloDBFileHeader) (transaction) =
        let result = db.Execute(@"DELETE FROM SoloDBFileHeader WHERE Id = @FileId",
                    {| FileId = file.Id; |},
                    transaction = transaction)

        ()


    let private listDirectoryFiles (db: SqliteConnection) (dir: SoloDBDirectoryHeader) transaction =
        db.Query<SoloDBFileHeader>("SELECT * FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId", {|DirectoryId = dir.Id|}, transaction = transaction)


    let private listDirectoryChildren (db: SqliteConnection) (dir: SoloDBDirectoryHeader) transaction =
        db.Query<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE ParentId = @Id", {|Id = dir.Id|}, transaction = transaction)


    let rec private deleteDirectory (db: SqliteConnection) (dir: SoloDBDirectoryHeader) transaction = 
        let result = db.Execute(@"DELETE FROM SoloDBDirectoryHeader WHERE Id = @DirId",
                        {| DirId = dir.Id; |},
                        transaction = transaction)
        ()


    let private getFileLength (db: SqliteConnection) (file: SoloDBFileHeader) =
        db.QueryFirst<int64>(@"SELECT Length FROM SoloDBFileHeader WHERE Name = @Name AND DirectoryId = @DirectoryId",
                       {| Name = file.Name; DirectoryId = file.DirectoryId; |})

    let private getFileLengthById (db: SqliteConnection) (fileId: SqlId) transaction =
        db.QueryFirst<int64>(@"SELECT Length FROM SoloDBFileHeader WHERE Id = @FileId",
                       {| FileId = fileId.Value |}, transaction)

    let private getStoredChunks (db: SqliteConnection) (fileId: SqlId) =
        db.Query<SoloDBFileChunk>("SELECT * FROM SoloDBFileChunk WHERE FileId = @FileId ORDER BY Number ASC", {|FileId = fileId|})


    let private emptyChunk = Array.zeroCreate<byte> (int chunkSize)
    let private getAllChunks (db: SqliteConnection) (fileId: SqlId) = seq {
        let mutable previousNumber = -1L // Chunks start from 0.
        for chunk in getStoredChunks db fileId do
            while chunk.Number - 1L > previousNumber do
                yield {Id = SqlId -1L; FileId = fileId; Number = previousNumber + 1L; Data = emptyChunk}
                previousNumber <- previousNumber + 1L

            yield chunk
            previousNumber <- chunk.Number
    }

    let private updateHashById (db: SqliteConnection) (fileId: SqlId) (hash: byte array) =
        db.Execute("UPDATE SoloDBFileHeader 
        SET Hash = @Hash
        WHERE Id = @FileId", {|Hash = hash; FileId = fileId|})
        |> ignore

    [<Sealed>]
    type DbFileStream(db: SqliteConnection, fileId: SqlId) =
        inherit Stream()

        let mutable position = 0L
        let mutable disposed = false

        let checkDisposed() = if disposed then raise (ObjectDisposedException(nameof(DbFileStream)))

        member private this.UpdateHash() =
            use sha1 = SHA1.Create()
            let allChunks = getAllChunks db fileId
            let len = this.Length

            allChunks 
            |> Seq.map (fun b -> 
                let currentChunkDataOffset = b.Number * chunkSize
                if currentChunkDataOffset + chunkSize <= len then 
                    ValueTuple<byte array, int, int>(b.Data, 0, b.Data.Length) 
                else 
                    let blockReadCount = int (len - currentChunkDataOffset) // Calculate how many bytes should actually be read to not exceed the file length
                    ValueTuple<byte array, int, int>(b.Data, 0, blockReadCount)) // Trim the chunk data to match the file's actual length
            |> Seq.iter (fun struct (data, start, len) -> sha1.TransformBlock (data, start, len, null, 0) |> ignore)

            sha1.TransformFinalBlock([||], 0, 0) |> ignore

            let hash = sha1.Hash
            updateHashById db fileId hash


        override _.CanRead = true
        override _.CanSeek = true
        override _.CanWrite = true

        override _.Length = 
            checkDisposed()
            getFileLengthById db fileId null

        override this.Position 
            with get() = 
                checkDisposed()
                position 
            and set(value) = 
                checkDisposed()
                this.Seek(value, SeekOrigin.Begin) |> ignore

        override this.Flush() =         
            checkDisposed()
            this.UpdateHash()

        override this.Read(buffer: byte[], offset: int, count: int) : int =
            checkDisposed()
            if buffer = null then raise (ArgumentNullException(nameof buffer))
            if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
            if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
            if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))

            let position = position
            let count = int64 count

            let chunkNumber = position / int64 chunkSize
            let chunkOffset = position % int64 chunkSize

            let len = this.Length
            let bytesToRead = min count (chunkSize - chunkOffset) |> min (len - position)

            if bytesToRead = 0 then
                0
            else

            match tryGetChunkData db fileId chunkNumber null with
            | None -> 
                let maxChunkNumber = (min (position + count) len) / int64 chunkSize
                if maxChunkNumber > chunkNumber then // Try to load the other chunk if the file was sparsely allocated.
                    let offsetToNextChunk = chunkSize - chunkOffset

                    let bytesToRead = int (min (len - position) count) - int offsetToNextChunk
                    buffer.AsSpan(offset, int offsetToNextChunk).Fill(0uy)

                    this.Position <- position + offsetToNextChunk
                    this.Read(buffer, offset + int offsetToNextChunk, bytesToRead) + int offsetToNextChunk
                else
                0
            | Some chunkData ->

            Array.Copy(chunkData, chunkOffset, buffer, offset, bytesToRead)
        
            this.Position <- position + bytesToRead

            int bytesToRead
        

        override this.Write(buffer: byte[], offset: int, count: int) =
            if buffer = null then raise (ArgumentNullException(nameof buffer))
            if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
            if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
            if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))

            let position = position
            let count = int64 count
            let mutable remainingBytes = 0L
            let mutable bytesToWrite = 0L

            try
                use l = lockFileId db fileId

                let chunkNumber = position / int64 chunkSize
                let chunkOffset = position % int64 chunkSize

                let tryGetChunkDataResult = tryGetChunkData db fileId chunkNumber null
                let existingChunk = match tryGetChunkDataResult with Some data -> data | None -> Array.zeroCreate (int chunkSize)

                let spaceInChunk = chunkSize - chunkOffset
                bytesToWrite <- min count spaceInChunk

                Array.Copy(buffer, offset, existingChunk, chunkOffset, bytesToWrite)

                writeChunkData db fileId chunkNumber existingChunk null

                this.Position <- position + int64 bytesToWrite
                if this.Position > this.Length then
                    updateLenById db fileId this.Position null            

                remainingBytes <- count - bytesToWrite
            finally ()

            if remainingBytes > 0 then
                this.Write(buffer, offset + int bytesToWrite, int remainingBytes)


        override _.SetLength(value: int64) =
            checkDisposed()
            use l = lockFileId db fileId
            downsetFileLength db fileId value null

        override this.Seek(offset: int64, origin: SeekOrigin) =
            checkDisposed()
            use l = lockFileId db fileId
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


    let getPathAndName (path: string) =
        let normalizedCompletePath = path.Split('\\', '/') |> Array.filter (fun x -> x |> String.IsNullOrWhiteSpace |> not)
        let sep = "/"
        let dirPath = $"/{normalizedCompletePath |> Array.take (Math.Max(normalizedCompletePath.Length - 1, 0l)) |> String.concat sep}"
        let name = match normalizedCompletePath |> Array.tryLast with Some x -> x | None -> ""
        dirPath, name

    let createFileAt (db: SqliteConnection) (path: string) =
        let dirPath, name = getPathAndName path
        let directory = getOrCreateDir db dirPath
        let newFile = {|
            Name = name
            DirectoryId = directory.Id
            Length = 0L
            Created = DateTimeOffset.Now
            Modified = DateTimeOffset.Now
        |}
        let result = db.QueryFirst<SoloDBFileHeader>("INSERT INTO SoloDBFileHeader(Name, DirectoryId, Length, Created, Modified) VALUES (@Name, @DirectoryId, @Length, @Created, @Modified) RETURNING *", newFile)

        result


    let tryCreateFileAt (db: SqliteConnection) (path: string) =
        let dirPath, name = getPathAndName path
        let directory = getOrCreateDir db dirPath
        let newFile = {|
            Name = name
            DirectoryId = directory.Id
            Length = 0
            Created = DateTimeOffset.Now
            Modified = DateTimeOffset.Now
        |}
        try
            let result = db.QueryFirst<SoloDBFileHeader> ("INSERT INTO SoloDBFileHeader(Name, DirectoryId, Length, Created, Modified) VALUES (@Name, @DirectoryId, @Length, @Created, @Modified) RETURNING *", newFile)
            result |> Some
        with ex -> 
            None


    let listDirectoriesAt (db: SqliteConnection) (path: string) =
        let dirPath = getPathAndName path |> combinePath
        match tryGetDir db dirPath with
        | None -> Seq.empty
        | Some dir ->
        let childres = listDirectoryChildren db dir null
        childres


    let private metadataFiller (header: SoloDBFileHeader) (metadata: Metadata) =
        let header = {header with Metadata = (if Object.ReferenceEquals(header.Metadata, null) then Dictionary<string, string>() :> IDictionary<string, string> else header.Metadata)}
        if not (Object.ReferenceEquals(metadata, null)) then 
            header.Metadata[metadata.Key] <- metadata.Value
        header

    let private tryGetFileWhere (connection: SqliteConnection) (where: string) (parameters) =
        let query = sprintf """
                    SELECT fh.*, fm.Key, fm.Value
                    FROM SoloDBFileHeader fh
                    LEFT JOIN SoloDBFileMetadata fm ON fh.Id = fm.FileId
                    WHERE %s;
                    """ where
    
        connection.Query<SoloDBFileHeader, Metadata, SoloDBFileHeader>(
            query, 
            metadataFiller,
            parameters,
            splitOn = "Key"
        )
        |> Seq.tryHead

    let tryGetFile (connection: SqliteConnection) (fileId: SqlId) =
        tryGetFileWhere connection "fh.Id = @FileId" {| FileId = fileId |}

    let tryGetFileAt (db: SqliteConnection) (path: string) =
        let dirPath, name = getPathAndName path
        match tryGetDir db dirPath with
        | None -> None
        | Some dir -> 
        tryGetFileWhere db "fh.DirectoryId = @DirectoryId AND fh.Name = @Name" {|DirectoryId = dir.Id; Name = name|}

    let tryGetFileByHash (db: SqliteConnection) (hash: byte array) =
        tryGetFileWhere db "fh.Hash = @Hash" {|Hash = hash|}

    let getOrCreateFileAt (db: SqliteConnection) (path: string) =
        use l = lockPath db path

        let dirPath, name = getPathAndName path
        let dir = getOrCreateDir db dirPath 

        match db.QueryFirstOrDefault<SoloDBFileHeader>("SELECT * FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId AND Name = @Name", {|DirectoryId = dir.Id; Name = name|}) with
        | file when Object.ReferenceEquals(file, null) -> createFileAt db path
        | file -> file


    let getDirectoryAt (db: SqliteConnection) (path: string) =
        let dirPath = getPathAndName path |> combinePath
        tryGetDir db dirPath


    let deleteDirectoryAt (db: SqliteConnection) (path: string) =
        let dirPath = getPathAndName path |> combinePath
        match tryGetDir db dirPath with
        | None -> false
        | Some dir ->
        try
            deleteDirectory db dir null
            true
        with ex ->
            printfn "%s" ex.Message
            false

    let getOrCreateDirectoryAt (db: SqliteConnection) (path: string) = 
        let dirPath = getPathAndName path |> combinePath
        getOrCreateDir db dirPath


    let listFilesAt (db: SqliteConnection) (path: string) : SoloDBFileHeader seq = 
        let dirPath = getPathAndName path |> combinePath
        match tryGetDir db dirPath with 
        | None -> Seq.empty
        | Some dir ->
        listDirectoryFiles db dir null

    let openFile (db: SqliteConnection) (file: SoloDBFileHeader) =
        new DbFileStream(db, file.Id)

    let openOrCreateFile (db: SqliteConnection) (path: string) =
        let file = tryGetFileAt db path
        let file = match file with
                    | Some x -> x 
                    | None -> createFileAt db path

        new DbFileStream(db, file.Id)


    let getFileLen db file =
        getFileLength db file

    let getFilePath db (file: SoloDBFileHeader) = 
        let directory = getDirectoryById db file.DirectoryId
        let directoryPath = getDirectoryPath db directory
        [|directoryPath; file.Name|] |> combinePathArr

    let setSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) (value: string) =
        db.Execute("INSERT INTO SoloDBFileMetadata(FileId, Key, Value) VALUES(@FileId, @Key, @Value)", {|FileId = file.Id; Key = key; Value = value|}) |> ignore

    let deleteSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBFileMetadata WHERE FileId = @FileId AND Key = @Key", {|FileId = file.Id; Key = key|}) |> ignore


    let setDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) (value: string) =
        db.Execute("INSERT INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) VALUES(@DirectoryId, @Key, @Value)", {|DirectoryId = dir.Id; Key = key; Value = value|}) |> ignore

    let deleteDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId AND Key = @Key", {|DirectoryId = dir.Id; Key = key|}) |> ignore
   
    type FileSystem(manager: ConnectionManager) =
        member this.Upload(path, stream: Stream) =
            use db = manager.Borrow()
            use file = openOrCreateFile db path
            stream.CopyTo file
            file.SetLength file.Position

        member this.UploadAsync(path, stream: Stream) = task {
            use db = manager.Borrow()
            use file = openOrCreateFile db path
            do! stream.CopyToAsync file
            file.SetLength file.Position
        }

        member this.Download(path, stream: Stream) =
            use db = manager.Borrow()
            let fileHeader = match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
            use fileStream = openFile db fileHeader
            fileStream.CopyTo stream

        member this.DownloadAsync(path, stream: Stream) = task {
            use db = manager.Borrow()
            let fileHeader = match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
            use fileStream = openFile db fileHeader
            do! fileStream.CopyToAsync stream
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

        member this.TryDirGetAt path =
            use db = manager.Borrow()
            tryGetDir db path

        member this.GetOrCreateDirAt path =
            use db = manager.Borrow()
            getOrCreateDirectoryAt db path

        member this.Open file =
            use db = manager.Borrow()
            openFile db file

        member this.OpenAt path =
            use db = manager.Borrow()
            let file = this.GetAt path
            openFile db file

        member this.TryOpenAt path =
            use db = manager.Borrow()
            match this.TryGetAt path with
            | None -> None
            | Some file ->
            openFile db file |> Some

        member this.OpenOrCreateAt path =
            use db = manager.Borrow()        
            openOrCreateFile db path

        member this.WriteAt(path, offset, data, ?createIfInexistent: bool) =
            let createIfInexistent = match createIfInexistent with Some x -> x | None -> true
            let file = if createIfInexistent then this.GetOrCreateAt path else this.GetAt path

            use db = manager.Borrow()
            use fileStream = openFile db file
            fileStream.Position <- offset
            fileStream.Write(data, 0, data.Length)
            ()

        member this.ReadAt(path, offset, len) =
            let file = this.GetAt path
            let array = Array.zeroCreate<byte> len

            use db = manager.Borrow()
            use fileStream = openFile db file
            fileStream.Position <- offset
            fileStream.ReadExactly(array, 0, len)
            array

        member this.SetMetadata(file, key, value) =
            use db = manager.Borrow()
            setSoloDBFileMetadata db file key value

        member this.SetMetadata(path, key, value) =
            let file = this.GetAt path
            use db = manager.Borrow()
            setSoloDBFileMetadata db file key value

        member this.DeleteMetadata(file, key) =
            use db = manager.Borrow()
            deleteSoloDBFileMetadata db file key

        member this.DeleteMetadata(path, key) =
            let file = this.GetAt path
            use db = manager.Borrow()
            deleteSoloDBFileMetadata db file key


        member this.SetDirectoryMetadata(dir, key, value) =
            use db = manager.Borrow()
            setDirMetadata db dir key value

        member this.SetDirectoryMetadata(path, key, value) =
            let dir = this.GetDirAt path
            use db = manager.Borrow()
            setDirMetadata db dir key value

        member this.DeleteDirectoryMetadata(dir, key) =
            use db = manager.Borrow()
            deleteDirMetadata db dir key

        member this.DeleteDirectoryMetadata(path, key) =
            let dir = this.GetDirAt path
            use db = manager.Borrow()
            deleteDirMetadata db dir key

        member this.TryGetFileByHash(hash) =
            use db = manager.Borrow()
            tryGetFileByHash db hash

        member this.GetFileByHash(hash) =
            match this.TryGetFileByHash hash with
            | None -> raise (FileNotFoundException("No file with such hash.", Utils.hashBytesToStr hash))
            | Some f -> f

        member this.Delete(file) =
            use db = manager.Borrow()
            deleteFile db file null

        member this.Delete(dir) =
            use db = manager.Borrow()
            deleteDirectory db dir null

        member this.DeleteFileAt(path) =
            match this.TryGetAt path with
            | None -> false
            | Some file ->
            this.Delete file
            true

        member this.DeleteDirAt(path) =
            use db = manager.Borrow()
            deleteDirectoryAt db path