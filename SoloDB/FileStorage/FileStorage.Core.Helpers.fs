namespace SoloDatabase

open System.Collections.Generic
open System
open System.IO
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.Types
open SoloDatabase.Connections

// NativePtr operations for efficient chunk-based file I/O
#nowarn "9"

module FileStorageCore =
    /// <summary>
    /// Defines the chunk size for file storage, approximating a SQLite page size.
    /// </summary>
    [<Literal>]
    let chunkSize =
        16384L // 16KB, aprox. a SQLite page size.

    /// <summary>
    /// Defines the maximum size allocated for storing compressed chunks.
    /// </summary>
    [<Literal>]
    let maxChunkStoreSize =
        chunkSize + 100L // If the compression fails.

    /// <summary>
    /// Combines two path segments into a single path, replacing backslashes with forward slashes.
    /// </summary>
    let internal combinePath a b = Path.Combine(a, b) |> _.Replace('\\', '/')

    /// <summary>
    /// Combines an array of path segments into a single path, replacing backslashes with forward slashes.
    /// </summary>
    let internal combinePathArr arr = arr |> Path.Combine |> _.Replace('\\', '/')

    /// <summary>
    /// Represents a disposable object that performs no action when disposed.
    /// </summary>
    let internal noopDisposer = {
         new System.IDisposable with
             member this.Dispose() =
                 ()
    }

    /// <summary>
    /// Acquires a mutex to lock the specified path if a transaction is not already active.
    /// </summary>
    let private isInTransaction (db: SqliteConnection) =
        match db with
        | :? TransactionalConnection -> true
        | :? CachingDbConnection as cc -> cc.InsideTransaction
        | _ -> false

    let internal lockPathIfNotInTransaction (db: SqliteConnection) (path: string) =
        if isInTransaction db then
            noopDisposer
        else
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(db.ConnectionString)}-Path-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(path)}")
        mutex :> IDisposable

    /// <summary>
    /// Fills the directory metadata for a given directory header by querying the database.
    /// </summary>
    let internal fillDirectoryMetadata (db: SqliteConnection) (directory: SoloDBDirectoryHeader) =
        let allMetadata = db.Query<Metadata>("SELECT Key, Value FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId", {|DirectoryId = directory.Id|})
        let dict = Dictionary<string, string>()

        for meta in allMetadata do
            dict[meta.Key] <- meta.Value

        {directory with Metadata = dict}

    /// <summary>
    /// Attempts to retrieve directories from the database based on a WHERE clause.
    /// </summary>
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
    let internal tryGetDir (db: SqliteConnection) (path: string) =
        tryGetDirectoriesWhere db "dh.FullPath = @Path" {|Path = path|} |> Seq.tryHead

    /// <summary>
    /// Recursively retrieves or creates a directory in the database.
    /// </summary>
    let rec internal getOrCreateDir (db: SqliteConnection) (path: string) =
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

        db.Execute("INSERT INTO SoloDBDirectoryHeader(Name, ParentId, FullPath) VALUES(@Name, @ParentId, @FullPath) ON CONFLICT(FullPath) DO NOTHING", newDir) |> ignore

        match db.QueryFirstOrDefault<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE FullPath = @FullPath", {|FullPath = path|}) with
        | dir when Utils.isNull dir -> failwithf "Normally you cannot end up here, cannot find a directory that has just created: %s" path
        | dir -> dir |> fillDirectoryMetadata db

    /// <summary>
    /// Updates the length of a file in the database based on its ID.
    /// </summary>
    let internal updateLenById (db: SqliteConnection) (fileId: int64) (len: int64) =
        let result = db.Execute ("UPDATE SoloDBFileHeader SET Length = @Length WHERE Id = @Id", {|Id = fileId; Length=len|})
        if result <> 1 then failwithf "updateLen failed."

    /// <summary>
    /// Reduces the length of a file and deletes chunks beyond the new length.
    /// </summary>
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
    let internal deleteFile (db: SqliteConnection) (file: SoloDBFileHeader) =
        let _result = db.Execute(@"DELETE FROM SoloDBFileHeader WHERE Id = @FileId",
                    {| FileId = file.Id; |})
        ()

    /// <summary>
    /// Deletes a directory from the database based on its header information.
    /// </summary>
    let internal deleteDirectory (db: SqliteConnection) (dir: SoloDBDirectoryHeader) =
        let _result = db.Execute(@"DELETE FROM SoloDBDirectoryHeader WHERE Id = @DirId",
                        {| DirId = dir.Id; |})
        ()

    /// <summary>
    /// Represents an entry in the file system for recursive listing operations.
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
