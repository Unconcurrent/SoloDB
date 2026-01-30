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
open FileStorageCore

#nowarn "9"

module FileStorage =
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
    /// Deletes a directory recursively at the specified path
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
    /// Gets the SQL ORDER BY column name for a sort field when applied to files.
    /// </summary>
    let private getFileSortColumn (sortBy: SortField) =
        match sortBy with
        | SortField.Name -> "fh.Name COLLATE NOCASE"
        | SortField.Size -> "fh.Length"
        | SortField.Created -> "fh.Created"
        | SortField.Modified -> "fh.Modified"
        | _ -> "fh.Name COLLATE NOCASE"

    /// <summary>
    /// Gets the SQL ORDER BY column name for a sort field when applied to directories.
    /// </summary>
    let private getDirSortColumn (sortBy: SortField) =
        match sortBy with
        | SortField.Name -> "dh.Name COLLATE NOCASE"
        | SortField.Size -> "0" // Directories don't have size
        | SortField.Created -> "dh.Created"
        | SortField.Modified -> "dh.Modified"
        | _ -> "dh.Name COLLATE NOCASE"

    /// <summary>
    /// Gets the SQL sort direction string.
    /// </summary>
    let private getSortDirection (sortDir: SortDirection) =
        match sortDir with
        | SortDirection.Ascending -> "ASC"
        | SortDirection.Descending -> "DESC"
        | _ -> "ASC"

    /// <summary>
    /// Lists files in a directory with pagination and sorting.
    /// </summary>
    let private listFilesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> (ResizeArray<SoloDBFileHeader>() :> IList<SoloDBFileHeader>, 0L)
        | Some dir ->

        let sortCol = getFileSortColumn sortBy
        let sortDirStr = getSortDirection sortDir

        // Get total count
        let count = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId", {|DirectoryId = dir.Id|})

        // Get paginated results with metadata
        let query = sprintf "SELECT fh.*, fm.Key as MetaKey, fm.Value as MetaValue FROM SoloDBFileHeader fh LEFT JOIN SoloDBFileMetadata fm ON fh.Id = fm.FileId WHERE fh.DirectoryId = @DirectoryId ORDER BY %s %s LIMIT @Limit OFFSET @Offset" sortCol sortDirStr

        let files =
            db.Query<{|
                Id: int64
                Name: string
                FullPath: string
                DirectoryId: int64
                Length: int64
                Created: DateTimeOffset
                Modified: DateTimeOffset
                MetaKey: string
                MetaValue: string
            |}>(query, {|DirectoryId = dir.Id; Limit = limit; Offset = offset|})
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
            |> ResizeArray

        (files :> IList<SoloDBFileHeader>, count)

    /// <summary>
    /// Lists directories with pagination and sorting.
    /// </summary>
    let private listDirectoriesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> (ResizeArray<SoloDBDirectoryHeader>() :> IList<SoloDBDirectoryHeader>, 0L)
        | Some dir ->

        let sortCol = getDirSortColumn sortBy
        let sortDirStr = getSortDirection sortDir

        // Get total count
        let count = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId", {|ParentId = dir.Id|})

        // Get paginated results with metadata
        let query = sprintf "SELECT dh.*, dm.Key, dm.Value FROM SoloDBDirectoryHeader dh LEFT JOIN SoloDBDirectoryMetadata dm ON dh.Id = dm.DirectoryId WHERE dh.ParentId = @ParentId ORDER BY %s %s LIMIT @Limit OFFSET @Offset" sortCol sortDirStr

        let directoryDictionary = new System.Collections.Generic.Dictionary<int64, SoloDBDirectoryHeader>()
        let isNull x = Object.ReferenceEquals(x, null)

        db.Query<SoloDBDirectoryHeader, Metadata, unit>(
            query,
            (fun directory metadata ->
                let dir =
                    match directoryDictionary.TryGetValue(directory.Id) with
                    | true, dir -> dir
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
            {|ParentId = dir.Id; Limit = limit; Offset = offset|},
            splitOn = "Key"
        ) |> Seq.iter ignore

        (ResizeArray(directoryDictionary.Values) :> IList<SoloDBDirectoryHeader>, count)

    /// <summary>
    /// Lists directory entries (directories first, then files) with pagination and sorting.
    /// </summary>
    let private listEntriesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> (ResizeArray<SoloDBEntryHeader>() :> IList<SoloDBEntryHeader>, 0L, 0L)
        | Some dir ->

        // Get counts
        let dirCount = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId", {|ParentId = dir.Id|})
        let fileCount = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId", {|DirectoryId = dir.Id|})

        let result = ResizeArray<SoloDBEntryHeader>(min limit (int (dirCount + fileCount)))

        // Determine how many directories and files to fetch based on offset
        if int64 offset < dirCount then
            // We need some directories
            let dirsToFetch = min limit (int dirCount - offset)
            let dirs, _ = listDirectoriesAtPaginated db path sortBy sortDir dirsToFetch offset
            for d in dirs do
                result.Add(SoloDBEntryHeader.Directory d)

            // If we have room for files
            let remaining = limit - dirsToFetch
            if remaining > 0 then
                let files, _ = listFilesAtPaginated db path sortBy sortDir remaining 0
                for f in files do
                    result.Add(SoloDBEntryHeader.File f)
        else
            // Offset is past all directories, only fetch files
            let fileOffset = offset - int dirCount
            let files, _ = listFilesAtPaginated db path sortBy sortDir limit fileOffset
            for f in files do
                result.Add(SoloDBEntryHeader.File f)

        (result :> IList<SoloDBEntryHeader>, dirCount, fileCount)

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
    /// Provides an API for interacting with a virtual file system within the SQLite database.
    /// </summary>
    /// <param name="connection">The database connection provider.</param>
    type internal FileSystem(connection: Connection) =
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
        /// Checks if a file or directory exists at the specified path.
        /// </summary>
        /// <param name="path">The full path of the file or directory.</param>
        /// <returns>True if an entry exists at the path, otherwise false.</returns>
        member this.Exists(path: string) =
            use db = connection.Get()
            let sql = """
                SELECT EXISTS (
                    SELECT 1 FROM SoloDBFileHeader WHERE FullPath = @Path
                    UNION ALL
                    SELECT 1 FROM SoloDBDirectoryHeader WHERE FullPath = @Path
                )
                """
            let formattedPath = formatPath path
            db.QueryFirst<bool>(sql, {| Path = formattedPath |})

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
        /// Lists files in a directory with pagination and sorting.
        /// </summary>
        /// <param name="path">The path of the directory.</param>
        /// <param name="sortBy">The field to sort by.</param>
        /// <param name="sortDir">The sort direction.</param>
        /// <param name="limit">Maximum number of files to return.</param>
        /// <param name="offset">Number of files to skip.</param>
        /// <returns>A tuple of (files list, total count).</returns>
        member this.ListFilesAtPaginated(path, sortBy, sortDir, limit, offset) =
            use db = connection.Get()
            listFilesAtPaginated db path sortBy sortDir limit offset

        /// <summary>
        /// Lists subdirectories with pagination and sorting.
        /// </summary>
        /// <param name="path">The path of the parent directory.</param>
        /// <param name="sortBy">The field to sort by.</param>
        /// <param name="sortDir">The sort direction.</param>
        /// <param name="limit">Maximum number of directories to return.</param>
        /// <param name="offset">Number of directories to skip.</param>
        /// <returns>A tuple of (directories list, total count).</returns>
        member this.ListDirectoriesAtPaginated(path, sortBy, sortDir, limit, offset) =
            use db = connection.Get()
            listDirectoriesAtPaginated db path sortBy sortDir limit offset

        /// <summary>
        /// Lists directory entries (directories first, then files) with pagination and sorting.
        /// </summary>
        /// <param name="path">The path of the directory.</param>
        /// <param name="sortBy">The field to sort by.</param>
        /// <param name="sortDir">The sort direction.</param>
        /// <param name="limit">Maximum number of entries to return.</param>
        /// <param name="offset">Number of entries to skip.</param>
        /// <returns>A tuple of (entries list, directory count, file count).</returns>
        member this.ListEntriesAtPaginated(path, sortBy, sortDir, limit, offset) =
            use db = connection.Get()
            listEntriesAtPaginated db path sortBy sortDir limit offset

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

        interface IFileSystem with
            member this.Upload(path, stream) = this.Upload(path, stream)
            member this.UploadAsync(path, stream) = this.UploadAsync(path, stream)
            member this.UploadBulk(files) = this.UploadBulk(files)
            member this.ReplaceAsyncWithinTransaction(path, stream) = this.ReplaceAsyncWithinTransaction(path, stream)
            member this.Download(path, stream) = this.Download(path, stream)
            member this.DownloadAsync(path, stream) = this.DownloadAsync(path, stream)
            member this.GetAt(path) = this.GetAt(path)
            member this.TryGetAt(path) = this.TryGetAt(path)
            member this.Exists(path) = this.Exists(path)
            member this.GetOrCreateAt(path) = this.GetOrCreateAt(path)
            member this.GetDirAt(path) = this.GetDirAt(path)
            member this.TryGetDirAt(path) = this.TryGetDirAt(path)
            member this.GetOrCreateDirAt(path) = this.GetOrCreateDirAt(path)
            member this.Open(file: SoloDBFileHeader) = this.Open(file)
            member this.OpenAt(path: string) = this.OpenAt(path)
            member this.TryOpenAt(path: string) = this.TryOpenAt(path)
            member this.OpenOrCreateAt(path: string) = this.OpenOrCreateAt(path)
            member this.WriteAt(path, offset, data: byte[], createIfInexistent) = this.WriteAt(path, offset, data, createIfInexistent)
            member this.WriteAt(path, offset, data: Stream, createIfInexistent) = this.WriteAt(path, offset, data, createIfInexistent)
            member this.ReadAt(path, offset, len) = this.ReadAt(path, offset, len)
            member this.SetFileModificationDate(path, date) = this.SetFileModificationDate(path, date)
            member this.SetFileCreationDate(path, date) = this.SetFileCreationDate(path, date)
            member this.SetMetadata(file: SoloDBFileHeader, key: string, value: string) = this.SetMetadata(file, key, value)
            member this.SetMetadata(path: string, key: string, value: string) = this.SetMetadata(path, key, value)
            member this.DeleteMetadata(file: SoloDBFileHeader, key: string) = this.DeleteMetadata(file, key)
            member this.DeleteMetadata(path: string, key: string) = this.DeleteMetadata(path, key)
            member this.SetDirectoryMetadata(dir: SoloDBDirectoryHeader, key: string, value: string) = this.SetDirectoryMetadata(dir, key, value)
            member this.SetDirectoryMetadata(path: string, key: string, value: string) = this.SetDirectoryMetadata(path, key, value)
            member this.DeleteDirectoryMetadata(dir: SoloDBDirectoryHeader, key: string) = this.DeleteDirectoryMetadata(dir, key)
            member this.DeleteDirectoryMetadata(path: string, key: string) = this.DeleteDirectoryMetadata(path, key)
            member this.Delete(file: SoloDBFileHeader) = this.Delete(file)
            member this.Delete(dir: SoloDBDirectoryHeader) = this.Delete(dir)
            member this.DeleteFileAt(path) = this.DeleteFileAt(path)
            member this.DeleteDirAt(path) = this.DeleteDirAt(path)
            member this.ListFilesAt(path) = this.ListFilesAt(path)
            member this.ListDirectoriesAt(path) = this.ListDirectoriesAt(path)
            member this.ListFilesAtPaginated(path, sortBy, sortDir, limit, offset) = this.ListFilesAtPaginated(path, sortBy, sortDir, limit, offset)
            member this.ListDirectoriesAtPaginated(path, sortBy, sortDir, limit, offset) = this.ListDirectoriesAtPaginated(path, sortBy, sortDir, limit, offset)
            member this.ListEntriesAtPaginated(path, sortBy, sortDir, limit, offset) = this.ListEntriesAtPaginated(path, sortBy, sortDir, limit, offset)
            member this.RecursiveListEntriesAt(path) = this.RecursiveListEntriesAt(path)
            member this.RecursiveListEntriesAtLazy(path) = this.RecursiveListEntriesAtLazy(path)
            member this.MoveFile(from, toPath) = this.MoveFile(from, toPath)
            member this.MoveReplaceFile(from, toPath) = this.MoveReplaceFile(from, toPath)
            member this.MoveDirectory(from, toPath) = this.MoveDirectory(from, toPath)
