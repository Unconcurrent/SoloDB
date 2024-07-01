module FileStorage

open System
open System.IO
open Microsoft.Data.Sqlite
open Dapper
open SoloDBTypes

let chunkSize = 
    1L // MB
    * 1024L * 1024L

let private combinePath (a, b) = (a, b) |> Path.Combine |> _.Replace('\\', '/')
let private combinePathArr arr = arr |> Path.Combine |> _.Replace('\\', '/')

let private lockPath (db: SqliteConnection) (path: string) =
    let mutex = new DisposableMutex($"SoloDB-{db.ConnectionString.GetHashCode(StringComparison.InvariantCultureIgnoreCase)}-Path-{path.GetHashCode(StringComparison.InvariantCultureIgnoreCase)}")
    mutex

let private lockFileId (db: SqliteConnection) (id: SqlId) =
    let mutex = new DisposableMutex($"SoloDB-{db.ConnectionString.GetHashCode(StringComparison.InvariantCultureIgnoreCase)}-FileId-{id.Value}")
    mutex

let private getOrCreateDir (db: SqliteConnection) (path: string) =
    use l = lockPath db path

    let names = path.Split ('/', StringSplitOptions.RemoveEmptyEntries) |> Array.toList
    let root = db.QueryFirst<DirectoryHeader>("SELECT * FROM DirectoryHeader WHERE Name = @RootName", {|RootName = ""|})
    let root = {root with ParentId = Nullable()}

    let rec innerLoop (prev: DirectoryHeader) (namesRem: string list) = 
        match namesRem with
        | [] -> prev
        | head :: tail ->
            match db.QueryFirstOrDefault<DirectoryHeader>("SELECT * FROM DirectoryHeader WHERE Name = @Name AND ParentId = @ParentId", {|Name = head; ParentId = prev.Id|}) with
            | dir when Object.ReferenceEquals(dir, null) -> 
                let newDir = {|
                    Name = head
                    ParentId = prev.Id
                |}
                let result = db.Execute("INSERT INTO DirectoryHeader(Name, ParentId) VALUES(@Name, @ParentId)", newDir)
                innerLoop prev (head :: tail) // todo: Check if prev should be newDir.
            | dir -> innerLoop dir tail 
    
    innerLoop root names

let private getDir (db: SqliteConnection) (path: string) =
    use l = lockPath db path

    let names = path.Split ('/', StringSplitOptions.RemoveEmptyEntries) |> Array.toList
    let root = db.QueryFirst<DirectoryHeader>("SELECT * FROM DirectoryHeader WHERE Name = @RootName", {|RootName = ""|})
    let root = {root with ParentId = Nullable()}

    let rec innerLoop (prev: DirectoryHeader) (namesRem: string list) =
        match namesRem with
        | [] -> prev |> Some
        | head :: tail ->
            match db.QueryFirstOrDefault<DirectoryHeader>("SELECT * FROM DirectoryHeader WHERE Name = @Name AND ParentId = @ParentId", {|Name = head; ParentId = prev.Id|}) with
            | dir when Object.ReferenceEquals(dir, null) ->
                None
            | dir -> innerLoop dir tail 
        
    innerLoop root names
    
let private tryGetChunkData (db: SqliteConnection) (fileId: SqlId) (chunkNumber: int64) transaction =
    let sql = "SELECT Data FROM FileChunk WHERE FileId = @FileId AND Number = @ChunkNumber"
    match db.QueryFirstOrDefault<byte array>(sql, {| FileId = fileId; ChunkNumber = chunkNumber |}, transaction) with
    | data when Object.ReferenceEquals(data, null) -> None
    | data -> data |> Some

let private writeChunkData (db: SqliteConnection) (fileId: SqlId) (chunkNumber: int64) (data: byte array) transaction =
    let result = db.Execute("INSERT OR REPLACE INTO FileChunk(FileId, Number, Data) VALUES (@FileId, @Number, @Data)", {|FileId = fileId; Number = chunkNumber; Data = data|}, transaction)
    if result <> 1 then failwithf "writeChunkData failed."

let private updateLenById (db: SqliteConnection) (fileId: SqlId) (len: int64) transaction =
    let result = db.Execute ("UPDATE FileHeader SET Length = @Length WHERE Id = @Id", {|Id = fileId.Value; Length=len|}, transaction)
    if result <> 1 then failwithf "updateLen failed."

let private updateLen (db: SqliteConnection) (file: FileHeader) (len: int64) transaction =
    updateLenById db file.Id len transaction

let private downsetFileLength (db: SqliteConnection) (fileId: SqlId) (newFileLength: int64) transaction =
    let lastChunkNumberKeep = ((float(newFileLength) / float(chunkSize)) |> Math.Ceiling |> int64) - 1L

    let resultDelete = db.Execute(@"DELETE FROM FileChunk WHERE FileId = @FileId AND Number > @LastChunkNumber",
                   {| FileId = fileId; LastChunkNumber = lastChunkNumberKeep |},
                   transaction = transaction)

    let resultUpdate = db.Execute(@"UPDATE FileHeader 
                    SET Length = @NewFileLength
                    WHERE Id = @FileId",
                   {| FileId = fileId; NewFileLength = newFileLength |},
                   transaction = transaction)
    ()

let getDirectoryById (db: SqliteConnection) (dirId: SqlId) =
    let directory = db.QueryFirst<DirectoryHeader>("SELECT * FROM DirectoryHeader WHERE Id = @DirId", {|DirId = dirId|})
    directory


let getDirectoryPath (db: SqliteConnection) (dir: DirectoryHeader) =
    let rec innerLoop currentPath (dirId: Nullable<SqlId>) =
        match dirId with
        | dirId when not dirId.HasValue -> currentPath |> List.rev |> String.concat "/"
        | dirId -> 
            let dirId = dirId.Value
            let directory = db.QueryFirst<DirectoryHeader>("SELECT * FROM DirectoryHeader WHERE Id = @DirId", {|DirId = dirId|})
            innerLoop (directory.Name :: currentPath) directory.ParentId
    

    let path = innerLoop [] (Nullable dir.Id)

    "/" + path


let private deleteFile (db: SqliteConnection) (file: FileHeader) (transaction) =
    let result = db.Execute(@"DELETE FROM FileHeader WHERE Id = @FileId",
                {| FileId = file.Id; |},
                transaction = transaction)

    ()


let private listDirectoryFiles (db: SqliteConnection) (dir: DirectoryHeader) transaction =
    db.Query<FileHeader>("SELECT * FROM FileHeader WHERE DirectoryId = @DirectoryId", {|DirectoryId = dir.Id|}, transaction = transaction)


let private listDirectoryChildren (db: SqliteConnection) (dir: DirectoryHeader) transaction =
    db.Query<DirectoryHeader>("SELECT * FROM DirectoryHeader WHERE ParentId = @Id", {|Id = dir.Id|}, transaction = transaction)


let rec private deleteDirectory (db: SqliteConnection) (dir: DirectoryHeader) transaction = 
    let result = db.Execute(@"DELETE FROM DirectoryHeader WHERE Id = @DirId",
                    {| DirId = dir.Id; |},
                    transaction = transaction)
    ()


let private getFileLength (db: SqliteConnection) (file: FileHeader) =
    db.QueryFirst<int64>(@"SELECT Length FROM FileHeader WHERE Name = @Name AND DirectoryId = @DirectoryId",
                   {| Name = file.Name; DirectoryId = file.DirectoryId; |})

let private getFileLengthById (db: SqliteConnection) (fileId: SqlId) transaction =
    db.QueryFirst<int64>(@"SELECT Length FROM FileHeader WHERE Id = @FileId",
                   {| FileId = fileId.Value |}, transaction)

let private getFileById (db: SqliteConnection) (fileId: SqlId) (transaction) =
    db.QueryFirst<FileHeader>(@"SELECT * FROM FileHeader WHERE Id = @FileId",
                   {| FileId = fileId |}, transaction)

[<Sealed>]
type DbFileStream(db: SqliteConnection, fileId: SqlId) =
    inherit Stream()

    let mutable position = 0L
    let mutable disposed = false

    let checkDisposed() = if disposed then raise (ObjectDisposedException(nameof(DbFileStream)))

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
            use l = lockFileId db fileId

            if value < 0L || value > this.Length then
                raise (ArgumentOutOfRangeException(nameof(this.Position), "Position is out of range."))
            position <- value

    override _.Flush() = 
        checkDisposed()

    override this.Read(buffer: byte[], offset: int, count: int) : int =
        checkDisposed()
        if buffer = null then raise (ArgumentNullException(nameof buffer))
        if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
        if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
        if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))


        let count = int64 count

        let chunkNumber = position / int64 chunkSize
        let chunkOffset = position % int64 chunkSize

        let bytesToRead = min count (chunkSize - chunkOffset) |> min (this.Length - position)

        if bytesToRead = 0 then
            0
        else

        use l = lockFileId db fileId

        match tryGetChunkData db fileId chunkNumber null with
        | None -> 0
        | Some chunkData ->

        Array.Copy(chunkData, chunkOffset, buffer, offset, bytesToRead)
        
        position <- position + bytesToRead

        int bytesToRead
        

    override this.Write(buffer: byte[], offset: int, count: int) =
        if buffer = null then raise (ArgumentNullException(nameof buffer))
        if offset < 0 then raise (ArgumentOutOfRangeException(nameof offset))
        if count < 0 then raise (ArgumentOutOfRangeException(nameof count))
        if buffer.Length - offset < count then raise (ArgumentException("Invalid offset and length."))

        use l = lockFileId db fileId

        let count = int64 count
        let mutable remainingBytes = 0L
        let mutable bytesToWrite = 0L

        let chunkNumber = position / int64 chunkSize
        let chunkOffset = position % int64 chunkSize

        let tryGetChunkDataResult = tryGetChunkData db fileId chunkNumber null
        let existingChunk = match tryGetChunkDataResult with Some data -> data | None -> Array.zeroCreate (int chunkSize)

        let spaceInChunk = chunkSize - chunkOffset
        bytesToWrite <- min count spaceInChunk

        Array.Copy(buffer, offset, existingChunk, chunkOffset, bytesToWrite)

        writeChunkData db fileId chunkNumber existingChunk null

        position <- position + int64 bytesToWrite
        if position > this.Length then
            updateLenById db fileId position null            

        remainingBytes <- count - bytesToWrite

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
        disposed <- true
        ()


let getPathAndName (path: string) =
    let normalizedCompletePath = path.Split('\\', '/') |> Array.filter (fun x -> x |> String.IsNullOrWhiteSpace |> not)
    let sep = "/"
    let dirPath = $"/{normalizedCompletePath |> Array.take (Math.Max(normalizedCompletePath.Length - 1, 0l)) |> String.concat sep}"
    let name = match normalizedCompletePath |> Array.tryLast with Some x -> x | None -> ""
    dirPath, name

let private createFileAtTransactionally (db: SqliteConnection) (path: string) (transaction) =
    let dirPath, name = getPathAndName path
    let directory = getOrCreateDir db dirPath
    let newFile = {|
        Name = name
        DirectoryId = directory.Id
        Length = 0L
        Created = DateTimeOffset.Now
        Modified = DateTimeOffset.Now
    |}
    let result = db.QueryFirst<FileHeader>("INSERT INTO FileHeader(Name, DirectoryId, Length, Created, Modified) VALUES (@Name, @DirectoryId, @Length, @Created, @Modified) RETURNING *", newFile, transaction)

    result


let createFileAt (db: SqliteConnection) (path: string) =
    createFileAtTransactionally db path null


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
        let result = db.QueryFirst<FileHeader> ("INSERT INTO FileHeader(Name, DirectoryId, Length, Created, Modified) VALUES (@Name, @DirectoryId, @Length, @Created, @Modified) RETURNING *", newFile)
        result |> Some
    with ex -> 
        None


let listDirectoriesAt (db: SqliteConnection) (path: string) =
    let dirPath = getPathAndName path |> combinePath
    match getDir db dirPath with
    | None -> Seq.empty
    | Some dir ->
    let childres = listDirectoryChildren db dir null
    childres


let getFileAt (db: SqliteConnection) (path: string) =
    let dirPath, name = getPathAndName path
    match getDir db dirPath with
    | None -> None
    | Some dir -> 
    match db.QueryFirstOrDefault<FileHeader>("SELECT * FROM FileHeader WHERE DirectoryId = @DirectoryId AND Name = @Name", {|DirectoryId = dir.Id; Name = name|}) with
    | file when Object.ReferenceEquals(file, null) -> None
    | file -> file |> Some


let getDirectoryAt (db: SqliteConnection) (path: string) =
    let dirPath = getPathAndName path |> combinePath
    getDir db dirPath


let deleteDirectoryAt (db: SqliteConnection) (path: string) =
    let dirPath = getPathAndName path |> combinePath
    match getDir db dirPath with
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


let listFilesAt (db: SqliteConnection) (path: string) : FileHeader seq = 
    let dirPath = getPathAndName path |> combinePath
    match getDir db dirPath with 
    | None -> Seq.empty
    | Some dir ->
    listDirectoryFiles db dir null

let openFile (db: SqliteConnection) (file: FileHeader) =
    new DbFileStream(db, file.Id)

let openOrCreateFile (db: SqliteConnection) (path: string) =
    let file = getFileAt db path
    let file = match file with
                | Some x -> x 
                | None -> createFileAt db path

    new DbFileStream(db, file.Id)


let getFileLen db file =
    getFileLength db file

let getFilePath db (file: FileHeader) = 
    let directory = getDirectoryById db file.DirectoryId
    let directoryPath = getDirectoryPath db directory
    [|directoryPath; file.Name|] |> combinePathArr


type FileSystem(db: SqliteConnection) =
    member this.Upload(path, stream: Stream) =
        use file = openOrCreateFile db path
        stream.CopyTo file
        file.SetLength file.Position

    member this.UploadAsync(path, stream: Stream) = task {
        use file = openOrCreateFile db path
        do! stream.CopyToAsync file
        file.SetLength file.Position
    }

    member this.Download(path, stream: Stream) =
        let fileHeader = match getFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found for download", path))
        use fileStream = openFile db fileHeader
        fileStream.CopyTo stream

    member this.DownloadAsync(path, stream: Stream) = task {
        let fileHeader = match getFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found for download", path))
        use fileStream = openFile db fileHeader
        do! fileStream.CopyToAsync stream
    }

    member this.GetAt path =
        match getFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found for download", path))

    member this.TryGetAt path =
        getFileAt db path

