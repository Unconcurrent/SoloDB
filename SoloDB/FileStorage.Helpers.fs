namespace SoloDatabase

open System.Collections.Generic
open System
open System.IO
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.Types
open SoloDatabase.Connections
open FileStorageCore
open FileStorageCoreChunks

module internal FileStorageHelpers =
    let internal getPathAndName (path: string) =
        let normalizedCompletePath = path.Split('\\', '/') |> Array.filter (fun x -> x |> String.IsNullOrWhiteSpace |> not)
        let sep = "/"
        let dirPath = $"/{normalizedCompletePath |> Array.take (Math.Max(normalizedCompletePath.Length - 1, 0l)) |> String.concat sep}"
        let name = match normalizedCompletePath |> Array.tryLast with Some x -> x | None -> ""
        struct (dirPath, name)

    let internal formatPath path =
        let struct (dir, name) = getPathAndName path
        combinePath dir name

    let internal createFileAt (db: SqliteConnection) (path: string) =
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

    let internal listDirectoriesAt (db: SqliteConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> Seq.empty
        | Some dir ->
        tryGetDirectoriesWhere db "ParentId = @Id" {|Id = dir.Id|}

    let internal getFilesWhere (connection: SqliteConnection) (where: string) (parameters: obj) =
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

    let internal tryGetFileAt (db: SqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let fullPath = combinePath dirPath name
        getFilesWhere db "fh.FullPath = @FullPath" {|FullPath = fullPath|} |> Seq.tryHead

    let internal getOrCreateFileAt (db: SqliteConnection) (path: string) =
        use l = lockPathIfNotInTransaction db path
        match tryGetFileAt db path with
        | Some f -> f
        | None ->
        createFileAt db path

    let internal deleteDirectoryAt (db: SqliteConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> false
        | Some dir ->
        deleteDirectory db dir
        true

    let internal getOrCreateDirectoryAt (db: SqliteConnection) (path: string) =
        let dirPath = formatPath path
        getOrCreateDir db dirPath

    let internal listFilesAt (db: SqliteConnection) (path: string) : SoloDBFileHeader seq =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> Seq.empty
        | Some dir ->
        getFilesWhere db "DirectoryId = @DirectoryId" {|DirectoryId = dir.Id|} |> ResizeArray :> SoloDBFileHeader seq

    let internal getFileSortColumn (sortBy: SortField) =
        match sortBy with
        | SortField.Name -> "fh.Name COLLATE NOCASE"
        | SortField.Size -> "fh.Length"
        | SortField.Created -> "fh.Created"
        | SortField.Modified -> "fh.Modified"
        | _ -> "fh.Name COLLATE NOCASE"

    let internal getDirSortColumn (sortBy: SortField) =
        match sortBy with
        | SortField.Name -> "dh.Name COLLATE NOCASE"
        | SortField.Size -> "0"
        | SortField.Created -> "dh.Created"
        | SortField.Modified -> "dh.Modified"
        | _ -> "dh.Name COLLATE NOCASE"

    let internal getSortDirection (sortDir: SortDirection) =
        match sortDir with
        | SortDirection.Ascending -> "ASC"
        | SortDirection.Descending -> "DESC"
        | _ -> "ASC"

    let internal openFile (db: Connection) (file: SoloDBFileHeader) =
        new FileStorageCoreStream.DbFileStream(db, file.Id, file.DirectoryId, file.FullPath)

    let internal openOrCreateFile (db: Connection) (path: string) =
        let file =
            use conn = db.Get()
            match tryGetFileAt conn path with
            | Some x -> x
            | None -> createFileAt conn path
        new FileStorageCoreStream.DbFileStream(db, file.Id, file.DirectoryId, file.FullPath)

    let internal setSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBFileMetadata(FileId, Key, Value) VALUES(@FileId, @Key, @Value)", {|FileId = file.Id; Key = key; Value = value|}) |> ignore

    let internal deleteSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBFileMetadata WHERE FileId = @FileId AND Key = @Key", {|FileId = file.Id; Key = key|}) |> ignore

    let internal setDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) VALUES(@DirectoryId, @Key, @Value)", {|DirectoryId = dir.Id; Key = key; Value = value|}) |> ignore

    let internal deleteDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId AND Key = @Key", {|DirectoryId = dir.Id; Key = key|}) |> ignore

    let internal moveFile (db: SqliteConnection) (file: SoloDBFileHeader) (toDir: SoloDBDirectoryHeader) (newName: string) =
        let newFileFullPath = combinePath toDir.FullPath newName
        try
            db.Execute("UPDATE SoloDBFileHeader
            SET FullPath = @NewFullPath,
            DirectoryId = @DestDirId,
            Name = @NewName
            WHERE Id = @FileId", {|NewFullPath = newFileFullPath; DestDirId = toDir.Id; FileId = file.Id; NewName = newName|})
            |> ignore
        with
        | :? SqliteException as ex when ex.SqliteErrorCode = 19 && ex.Message.Contains "SoloDBFileHeader.FullPath" ->
            raise (IOException("File already exists.", ex))

    let rec internal moveDirectoryMustBeWithinTransaction (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (newParentDir: SoloDBDirectoryHeader) (newName: string) =
        let newDirFullPath = combinePath newParentDir.FullPath newName
        try
            db.Execute("UPDATE SoloDBDirectoryHeader
                         SET FullPath = @NewFullPath,
                             ParentId = @NewParentId,
                             Name = @NewName
                         WHERE Id = @DirId",
                         {| NewFullPath = newDirFullPath; NewParentId = newParentDir.Id; DirId = dir.Id; NewName = newName |})
            |> ignore
            let oldDirPath = dir.FullPath
            let dir = {dir with FullPath = newDirFullPath; ParentId = Nullable newParentDir.Id; Name = newName}
            let subDirs = db.Query<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE ParentId = @DirId", {| DirId = dir.Id |}) |> Seq.toList
            for subDir in subDirs do
                moveDirectoryMustBeWithinTransaction db subDir dir subDir.Name
            db.Execute("UPDATE SoloDBFileHeader
                         SET FullPath = REPLACE(FullPath, @OldFullPath, @NewFullPath)
                         WHERE DirectoryId = @DirId",
                         {| OldFullPath = oldDirPath; NewFullPath = newDirFullPath; DirId = dir.Id |})
            |> ignore
        with
        | :? SqliteException as ex when ex.SqliteErrorCode = 19 && ex.Message.Contains "FullPath" ->
            raise (IOException("Directory or file already exists in the destination.", ex))
