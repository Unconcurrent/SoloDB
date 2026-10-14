namespace SoloDatabase

open System.Collections.Generic
open System
open System.IO
open NativeSQLiteTools
open SoloDatabase.Types

// NativePtr operations for efficient chunk-based file I/O
#nowarn "9"

module FileStorageCore =
    /// Library-owned header projection names in declaration order.
    let internal fileHeaderProjectionFields =
        [| nameof Unchecked.defaultof<SoloDBFileHeader>.Id; nameof Unchecked.defaultof<SoloDBFileHeader>.Name
           nameof Unchecked.defaultof<SoloDBFileHeader>.FullPath; nameof Unchecked.defaultof<SoloDBFileHeader>.DirectoryId
           nameof Unchecked.defaultof<SoloDBFileHeader>.Length; nameof Unchecked.defaultof<SoloDBFileHeader>.Created
           nameof Unchecked.defaultof<SoloDBFileHeader>.Modified |]
    let internal directoryHeaderProjectionFields =
        [| nameof Unchecked.defaultof<SoloDBDirectoryHeader>.Id; nameof Unchecked.defaultof<SoloDBDirectoryHeader>.Name
           nameof Unchecked.defaultof<SoloDBDirectoryHeader>.FullPath; nameof Unchecked.defaultof<SoloDBDirectoryHeader>.ParentId
           nameof Unchecked.defaultof<SoloDBDirectoryHeader>.Created; nameof Unchecked.defaultof<SoloDBDirectoryHeader>.Modified |]

    [<Literal>]
    let internal fileHeaderColumns = "Id, Name, FullPath, DirectoryId, Length, Created, Modified"
    [<Literal>]
    let internal directoryHeaderColumns = "Id, Name, FullPath, ParentId, Created, Modified"

    let internal readFileHeader (row: SqlRow) metadata : SoloDBFileHeader =
        { Id = row.GetInt64(0); Name = row.GetString(1); FullPath = row.GetString(2)
          DirectoryId = row.GetInt64(3); Length = row.GetInt64(4)
          Created = SqlRowValue.Read<DateTimeOffset>(row, 5)
          Modified = SqlRowValue.Read<DateTimeOffset>(row, 6); Metadata = metadata }

    let internal readDirectoryHeader (row: SqlRow) metadata : SoloDBDirectoryHeader =
        { Id = row.GetInt64(0); Name = row.GetString(1); FullPath = row.GetString(2)
          ParentId = if row.IsDBNull(3) then Nullable() else Nullable(row.GetInt64(3))
          Created = SqlRowValue.Read<DateTimeOffset>(row, 4)
          Modified = SqlRowValue.Read<DateTimeOffset>(row, 5); Metadata = metadata }

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
    /// Fills the directory metadata for a given directory header by querying the database.
    /// </summary>
    let internal fillDirectoryMetadata (db: NativeSqliteConnection) (directory: SoloDBDirectoryHeader) =
        let mutable statement = db.OpenStatement("SELECT Key, Value FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId")
        try
            statement.BindInt64(1, directory.Id)
            let metadata = Dictionary<string, string>()
            while statement.Step() do
                metadata.[statement.Row.GetString(0)] <- statement.Row.GetString(1)
            statement.Dispose()
            {directory with Metadata = metadata}
        with error -> statement.Abort(error); reraise()

    /// <summary>
    /// Attempts to retrieve directories from the database based on a WHERE clause.
    /// </summary>
    let internal tryGetDirectoriesWhere (connection: NativeSqliteConnection) (where: string) (bind: NativeStatementScope -> unit) =
        let query = $"SELECT dh.Id, dh.Name, dh.FullPath, dh.ParentId, dh.Created, dh.Modified, dm.Key, dm.Value FROM SoloDBDirectoryHeader dh LEFT JOIN SoloDBDirectoryMetadata dm ON dh.Id = dm.DirectoryId WHERE {where}"
        let mutable statement = connection.OpenStatement(query)
        try
            bind statement
            let directories = Dictionary<int64, SoloDBDirectoryHeader>()
            while statement.Step() do
                let row = statement.Row
                let directory =
                    match directories.TryGetValue(row.GetInt64(0)) with
                    | true, directory -> directory
                    | _ ->
                        let directory = readDirectoryHeader row (Dictionary<string, string>())
                        directories.Add(directory.Id, directory)
                        directory
                if not (row.IsDBNull(6)) then
                    (directory.Metadata :?> Dictionary<string, string>).Add(row.GetString(6), row.GetString(7))
            statement.Dispose()
            directories.Values :> seq<_>
        with error -> statement.Abort(error); reraise()

    /// <summary>
    /// Attempts to retrieve a directory from the database based on its full path.
    /// </summary>
    /// Id-only directory lookup, for callers that resolve a directory in order to key further
    /// work on it rather than to return it.
    let internal tryGetDirIdAt (db: NativeSqliteConnection) (path: string) : int64 voption =
        let mutable statement = db.OpenStatement("SELECT Id FROM SoloDBDirectoryHeader WHERE FullPath = @Path LIMIT 1")
        try
            statement.BindText(1, path)
            let result = if statement.Step() then ValueSome(statement.Row.GetInt64(0)) else ValueNone
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()

    let internal tryGetDir (db: NativeSqliteConnection) (path: string) =
        tryGetDirectoriesWhere db "dh.FullPath = @Path" (fun statement -> statement.BindText(1, path)) |> Seq.tryHead

    /// <summary>
    /// Recursively retrieves or creates a directory, resolving ancestors by id only.
    /// Ancestors are walked purely to obtain a ParentId, so loading their headers and metadata
    /// at every level would be discarded work that grows with path depth.
    /// </summary>
    let internal getOrCreateDirId (db: NativeSqliteConnection) (path: string) : int64 =
        match tryGetDirIdAt db path with
        | ValueSome id -> id
        | ValueNone ->
            let missing = ResizeArray<int>()
            let mutable length = path.Length
            let mutable first = true
            let mutable parentId = 0L
            while parentId = 0L do
                let current = if length = path.Length then path else path.Substring(0, length)
                let existing = if first then first <- false; ValueNone else tryGetDirIdAt db current
                match existing with
                | ValueSome id -> parentId <- id
                | ValueNone when length = 1 ->
                    let mutable statement = db.OpenStatement("SELECT Id FROM SoloDBDirectoryHeader WHERE Name = ''")
                    try
                        if not (statement.Step()) then Seq.head (Seq.empty<unit>)
                        parentId <- statement.Row.GetInt64(0)
                        statement.Dispose()
                    with error -> statement.Abort(error); reraise()
                | ValueNone ->
                    missing.Add(length)
                    let boundary = path.LastIndexOf('/', length - 1)
                    if boundary < 0 then failwith "Inconsistent paths!"
                    length <- max 1 boundary
            for i = missing.Count - 1 downto 0 do
                let finish = missing.[i]
                let boundary = path.LastIndexOf('/', finish - 1)
                let name = path.Substring(boundary + 1, finish - boundary - 1)
                let parentPath = if boundary = 0 then "/" else path.Substring(0, boundary)
                let current = if finish = path.Length then path else path.Substring(0, finish)
                if combinePath parentPath name <> current then failwith "Inconsistent paths!"
                let mutable statement = db.OpenStatement("INSERT INTO SoloDBDirectoryHeader(Name, ParentId, FullPath) VALUES(@Name, @ParentId, @FullPath) ON CONFLICT(FullPath) DO NOTHING")
                try
                    statement.BindText(1, name)
                    statement.BindInt64(2, parentId)
                    statement.BindText(3, current)
                    statement.Step() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
                match tryGetDirIdAt db current with
                | ValueSome id -> parentId <- id
                | ValueNone -> failwithf "Cannot find a directory that has just been created: %s" current
            parentId

    /// <summary>
    /// Retrieves or creates a directory and returns its full public header, metadata included.
    /// Only the returned leaf is materialized as a header; ancestors are resolved by id.
    /// </summary>
    let internal getOrCreateDir (db: NativeSqliteConnection) (path: string) =
        match tryGetDir db path with
        | Some d -> d
        | None ->
        getOrCreateDirId db path |> ignore
        let mutable statement = db.OpenStatement("SELECT " + directoryHeaderColumns + " FROM SoloDBDirectoryHeader WHERE FullPath = @FullPath")
        let directory =
            try
                statement.BindText(1, path)
                if not (statement.Step()) then failwithf "Cannot find a directory that has just been created: %s" path
                let directory = readDirectoryHeader statement.Row null
                statement.Dispose()
                directory
            with error -> statement.Abort(error); reraise()
        fillDirectoryMetadata db directory

    /// <summary>
    /// Updates the length of a file in the database based on its ID.
    /// </summary>
    let internal updateLenById (db: NativeSqliteConnection) (fileId: int64) (len: int64) =
        let mutable statement = db.OpenStatement("UPDATE SoloDBFileHeader SET Length = @Length WHERE Id = @Id")
        try
            statement.BindInt64(1, len)
            statement.BindInt64(2, fileId)
            statement.Step() |> ignore
            let result = statement.RecordsAffected
            statement.Dispose()
            if result <> 1 then failwith "updateLen failed."
        with error -> statement.Abort(error); reraise()

    /// <summary>
    /// Deletes a file from the database based on its header information.
    /// </summary>
    let internal deleteFileById (db: NativeSqliteConnection) (fileId: int64) =
        let mutable statement = db.OpenStatement("DELETE FROM SoloDBFileHeader WHERE Id = @FileId")
        try
            statement.BindInt64(1, fileId)
            statement.Step() |> ignore
            let deleted = statement.RecordsAffected > 0
            statement.Dispose()
            deleted
        with error -> statement.Abort(error); reraise()

    let internal deleteFile (db: NativeSqliteConnection) (file: SoloDBFileHeader) =
        deleteFileById db file.Id

    /// <summary>
    /// Deletes a directory from the database based on its header information.
    /// </summary>
    let internal deleteDirectoryById (db: NativeSqliteConnection) (dirId: int64) =
        let mutable statement = db.OpenStatement("DELETE FROM SoloDBDirectoryHeader WHERE Id = @DirId")
        try
            statement.BindInt64(1, dirId)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let internal deleteDirectory (db: NativeSqliteConnection) (dir: SoloDBDirectoryHeader) =
        deleteDirectoryById db dir.Id
