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
        let fullPath = combinePath dirPath name
        let now = DateTimeOffset.Now
        db.Execute(
            "INSERT INTO SoloDBFileHeader(Name, FullPath, DirectoryId, Length, Created, Modified) VALUES (@Name, @FullPath, @DirectoryId, 0, @Created, @Modified) ON CONFLICT(FullPath) DO NOTHING",
            {| Name = name; FullPath = fullPath; DirectoryId = directory.Id; Created = now; Modified = now |}) |> ignore
        let result = db.QueryFirst<SoloDBFileHeader>("SELECT * FROM SoloDBFileHeader WHERE FullPath = @FullPath", {| FullPath = fullPath |})
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

    // ── Narrow path lookups ───────────────────────────────────────────────
    // getFilesWhere joins metadata and builds a full public header. Callers that only need an
    // identifier, or the three fields a stream is built from, use these instead so the cost does
    // not scale with a file's unrelated metadata rows. The full-header owner is unchanged and
    // still serves every API that returns metadata to the caller.

    /// Id, DirectoryId and FullPath: exactly what DbFileStream is constructed from.
    [<Struct>]
    type internal FileStreamIdentity =
        { Id: int64; DirectoryId: int64; FullPath: string }

    let internal tryGetFileIdAt (db: SqliteConnection) (path: string) : int64 voption =
        let struct (dirPath, name) = getPathAndName path
        match db.Query<int64>(
                "SELECT Id FROM SoloDBFileHeader WHERE FullPath = @FullPath LIMIT 1",
                {| FullPath = combinePath dirPath name |}) |> Seq.tryHead with
        | Some id -> ValueSome id
        | None -> ValueNone

    let internal tryGetFileStreamIdentityAt (db: SqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        match db.Query<FileStreamIdentity>(
                "SELECT Id, DirectoryId, FullPath FROM SoloDBFileHeader WHERE FullPath = @FullPath LIMIT 1",
                {| FullPath = combinePath dirPath name |}) |> Seq.tryHead with
        | Some identity -> ValueSome identity
        | None -> ValueNone

    let internal tryGetFileAt (db: SqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let fullPath = combinePath dirPath name
        getFilesWhere db "fh.FullPath = @FullPath" {|FullPath = fullPath|} |> Seq.tryHead

    let internal getOrCreateFileAt (db: SqliteConnection) (path: string) =
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

    let private getSortDirection (sortDir: SortDirection) =
        match sortDir with
        | SortDirection.Ascending -> "ASC"
        | SortDirection.Descending -> "DESC"
        | _ -> "ASC"

    let private buildOrderBy alias primaryColumns sortDir =
        let direction = getSortDirection sortDir
        Array.append primaryColumns [| $"{alias}.Name COLLATE NOCASE"; $"{alias}.Id" |]
        |> Array.map (fun column -> $"{column} {direction}")
        |> String.concat ", "

    let internal getFileOrderBy alias (sortBy: SortField) sortDir =
        let primaryColumns =
            match sortBy with
            | SortField.Name -> Array.empty
            | SortField.Size -> [| $"{alias}.Length" |]
            | SortField.Created -> [| $"{alias}.Created" |]
            | SortField.Modified -> [| $"{alias}.Modified" |]
            | _ -> Array.empty
        buildOrderBy alias primaryColumns sortDir

    let internal getDirectoryOrderBy alias (sortBy: SortField) sortDir =
        let primaryColumns =
            match sortBy with
            | SortField.Name
            | SortField.Size -> Array.empty
            | SortField.Created -> [| $"{alias}.Created" |]
            | SortField.Modified -> [| $"{alias}.Modified" |]
            | _ -> Array.empty
        buildOrderBy alias primaryColumns sortDir

    let internal openFile (db: Connection) (file: SoloDBFileHeader) =
        new FileStorageCoreStream.DbFileStream(db, file.Id, file.DirectoryId, file.FullPath)

    let internal openOrCreateFile (db: Connection) (path: string) =
        // Stream construction needs three columns; loading metadata here would be discarded.
        let identity =
            let existing =
                use conn = db.Get()
                tryGetFileStreamIdentityAt conn path
            match existing with
            | ValueSome x -> x
            | ValueNone ->
                db.WithTransaction(fun conn ->
                    let created = createFileAt conn path
                    { Id = created.Id; DirectoryId = created.DirectoryId; FullPath = created.FullPath })
        new FileStorageCoreStream.DbFileStream(db, identity.Id, identity.DirectoryId, identity.FullPath)

    let internal setSoloDBFileMetadataById (db: SqliteConnection) (fileId: int64) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBFileMetadata(FileId, Key, Value) VALUES(@FileId, @Key, @Value)", {|FileId = fileId; Key = key; Value = value|}) |> ignore

    let internal setSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) (value: string) =
        setSoloDBFileMetadataById db file.Id key value

    let internal deleteSoloDBFileMetadataById (db: SqliteConnection) (fileId: int64) (key: string) =
        db.Execute("DELETE FROM SoloDBFileMetadata WHERE FileId = @FileId AND Key = @Key", {|FileId = fileId; Key = key|}) |> ignore

    let internal deleteSoloDBFileMetadata (db: SqliteConnection) (file: SoloDBFileHeader) (key: string) =
        deleteSoloDBFileMetadataById db file.Id key

    let internal setDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) (value: string) =
        db.Execute("INSERT OR REPLACE INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) VALUES(@DirectoryId, @Key, @Value)", {|DirectoryId = dir.Id; Key = key; Value = value|}) |> ignore

    let internal deleteDirMetadata (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) =
        db.Execute("DELETE FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId AND Key = @Key", {|DirectoryId = dir.Id; Key = key|}) |> ignore

    /// SQLITE_CONSTRAINT_UNIQUE. Identifies a uniqueness violation without inspecting message text,
    /// so an unrelated constraint failure is never relabelled as a path collision.
    let [<Literal>] private SqliteConstraintUnique = 2067

    let internal pathExists (db: SqliteConnection) (fullPath: string) =
        db.QueryFirst<bool>(
            "SELECT EXISTS (
                SELECT 1 FROM SoloDBFileHeader WHERE FullPath = @FullPath
                UNION ALL
                SELECT 1 FROM SoloDBDirectoryHeader WHERE FullPath = @FullPath
             )",
            {| FullPath = fullPath |})

    let internal moveFile (db: SqliteConnection) (file: SoloDBFileHeader) (toDir: SoloDBDirectoryHeader) (newName: string) =
        let newFileFullPath = combinePath toDir.FullPath newName
        if newFileFullPath <> file.FullPath && pathExists db newFileFullPath then
            raise (IOException "File already exists.")
        try
            db.Execute("UPDATE SoloDBFileHeader
            SET FullPath = @NewFullPath,
            DirectoryId = @DestDirId,
            Name = @NewName
            WHERE Id = @FileId", {|NewFullPath = newFileFullPath; DestDirId = toDir.Id; FileId = file.Id; NewName = newName|})
            |> ignore
        with
        | :? SqliteException as ex when ex.SqliteExtendedErrorCode = SqliteConstraintUnique ->
            raise (IOException("File already exists.", ex))

    /// Rewrites a moved subtree in a bounded number of statements.
    ///
    /// Descendant paths are rebuilt by position: the segment of FullPath after the old root's
    /// length is appended to the new root. A textual REPLACE cannot be used here because it
    /// rewrites every occurrence rather than the leading prefix, which corrupts any path where
    /// the old root also appears later in the string.
    let internal moveDirectoryMustBeWithinTransaction (db: SqliteConnection) (dir: SoloDBDirectoryHeader) (newParentDir: SoloDBDirectoryHeader) (newName: string) =
        let oldDirPath = dir.FullPath
        let newDirFullPath = combinePath newParentDir.FullPath newName

        if newDirFullPath <> oldDirPath then
            // A directory cannot be moved inside itself. Compared as a path prefix, never as a
            // substring, so a sibling like /ab is not mistaken for a child of /a.
            if newDirFullPath.StartsWith(oldDirPath + "/", StringComparison.Ordinal) then
                raise (IOException "Cannot move a directory into itself.")
            if pathExists db newDirFullPath then
                raise (IOException "Directory or file already exists in the destination.")

        try
            // Subtree of the moved directory, including itself, by ParentId.
            let subtreeCte = """
                WITH RECURSIVE Subtree(Id) AS (
                    SELECT @DirId
                    UNION ALL
                    SELECT d.Id FROM SoloDBDirectoryHeader d JOIN Subtree s ON d.ParentId = s.Id
                )
                """

            // Descendants first: while the root still carries its old path, every descendant path
            // still shares the old prefix.
            db.Execute(subtreeCte + """
                UPDATE SoloDBDirectoryHeader
                SET FullPath = @NewFullPath || substr(FullPath, @OldPathLength + 1)
                WHERE Id IN (SELECT Id FROM Subtree) AND Id <> @DirId
                """,
                {| DirId = dir.Id; NewFullPath = newDirFullPath; OldPathLength = oldDirPath.Length |})
            |> ignore

            db.Execute(subtreeCte + """
                UPDATE SoloDBFileHeader
                SET FullPath = @NewFullPath || substr(FullPath, @OldPathLength + 1)
                WHERE DirectoryId IN (SELECT Id FROM Subtree)
                """,
                {| DirId = dir.Id; NewFullPath = newDirFullPath; OldPathLength = oldDirPath.Length |})
            |> ignore

            db.Execute("UPDATE SoloDBDirectoryHeader
                         SET FullPath = @NewFullPath,
                             ParentId = @NewParentId,
                             Name = @NewName
                         WHERE Id = @DirId",
                         {| NewFullPath = newDirFullPath; NewParentId = newParentDir.Id; DirId = dir.Id; NewName = newName |})
            |> ignore
        with
        | :? SqliteException as ex when ex.SqliteExtendedErrorCode = SqliteConstraintUnique ->
            raise (IOException("Directory or file already exists in the destination.", ex))

    // ── Copy helpers ──────────────────────────────────────────────────────

    /// Bulk-copies all chunk rows from source file to destination file via SQL-level INSERT...SELECT.
    /// No Snappy decompression/recompression — compressed blobs are copied as-is.
    let internal copyFileChunks (db: SqliteConnection) (srcFileId: int64) (dstFileId: int64) =
        db.Execute(
            "INSERT INTO SoloDBFileChunk(FileId, Number, Data) SELECT @DstFileId, Number, Data FROM SoloDBFileChunk WHERE FileId = @SrcFileId ORDER BY Number",
            {| SrcFileId = srcFileId; DstFileId = dstFileId |})
        |> ignore

    /// Bulk-copies all metadata key-value pairs from source file to destination file.
    let internal copyFileMetadata (db: SqliteConnection) (srcFileId: int64) (dstFileId: int64) =
        db.Execute(
            "INSERT INTO SoloDBFileMetadata(FileId, Key, Value) SELECT @DstFileId, Key, Value FROM SoloDBFileMetadata WHERE FileId = @SrcFileId",
            {| SrcFileId = srcFileId; DstFileId = dstFileId |})
        |> ignore

    /// Bulk-copies all metadata key-value pairs from source directory to destination directory.
    let internal copyDirectoryMetadata (db: SqliteConnection) (srcDirId: int64) (dstDirId: int64) =
        db.Execute(
            "INSERT INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) SELECT @DstDirId, Key, Value FROM SoloDBDirectoryMetadata WHERE DirectoryId = @SrcDirId",
            {| SrcDirId = srcDirId; DstDirId = dstDirId |})
        |> ignore

    /// Creates a copy of a file header at the destination path with NOW timestamps and source's Length.
    /// Returns the new SoloDBFileHeader.
    let internal createFileCopyAt (db: SqliteConnection) (src: SoloDBFileHeader) (dstDirId: int64) (dstFullPath: string) (dstName: string) =
        let now = DateTimeOffset.Now
        db.QueryFirst<SoloDBFileHeader>(
            "INSERT INTO SoloDBFileHeader(Name, FullPath, DirectoryId, Length, Created, Modified) VALUES (@Name, @FullPath, @DirectoryId, @Length, @Created, @Modified) RETURNING *",
            {| Name = dstName; FullPath = dstFullPath; DirectoryId = dstDirId; Length = src.Length; Created = now; Modified = now |})

    /// Core file-copy logic. Must be called within a transaction.
    /// If replace=true, deletes existing destination before copy. If replace=false, fails on collision.
    let internal copyFileMustBeWithinTransaction (db: SqliteConnection) (fromPath: string) (toPath: string) (replace: bool) (copyMetadata: bool) =
        // Edge case: self-copy
        let fromNorm = formatPath fromPath
        let toNorm = formatPath toPath
        if fromNorm = toNorm then raise (ArgumentException("Cannot copy a file to itself.", "toPath"))
        // Resolve source
        let src = match tryGetFileAt db fromNorm with | Some f -> f | None -> raise (FileNotFoundException("File not found.", fromPath))
        // Resolve destination directory and name
        let struct (toDirPath, toName) = getPathAndName toPath
        // File copy auto-creates destination parent.
        let dstDir = getOrCreateDirectoryAt db toDirPath
        let dstFullPath = combinePath dstDir.FullPath toName
        // Collision check
        match tryGetFileAt db dstFullPath with
        | Some existing when replace ->
            deleteFile db existing |> ignore // CASCADE deletes chunks + metadata
        | Some _ ->
            raise (IOException("File already exists."))
        | None -> ()
        // Create destination header with NOW timestamps and source Length
        let dstHeader = createFileCopyAt db src dstDir.Id dstFullPath toName
        // Clone chunks (SQL-level, zero Snappy work)
        copyFileChunks db src.Id dstHeader.Id
        // Clone metadata if requested
        if copyMetadata then
            copyFileMetadata db src.Id dstHeader.Id
        {dstHeader with Metadata = if copyMetadata then src.Metadata else readOnlyDict []}

    /// Recursive directory copy. Must be called within a transaction.
    /// If replace=true, deletes existing destination tree before copy. If replace=false, fails on collision.
    let rec internal copyDirectoryMustBeWithinTransaction (db: SqliteConnection) (fromPath: string) (toPath: string) (replace: bool) (recursive: bool) (copyMetadata: bool) =
        let fromNorm = formatPath fromPath
        let toNorm = formatPath toPath
        // Edge case: self-copy
        if fromNorm = toNorm then raise (ArgumentException("Cannot copy a directory to itself.", "toPath"))
        // Edge case: destination inside source subtree
        if toNorm.StartsWith(fromNorm + "/", StringComparison.Ordinal) then
            raise (ArgumentException("Cannot copy a directory into its own subtree.", "toPath"))
        // Resolve source
        let srcDir = match tryGetDir db fromNorm with | Some d -> d | None -> raise (DirectoryNotFoundException("Directory not found at: " + fromPath))
        // Directory copy requires destination parent to exist.
        let struct (toParentPath, toDirName) = getPathAndName toPath
        let toParentNorm = formatPath toParentPath
        let parentDir = match tryGetDir db toParentNorm with | Some d -> d | None -> raise (DirectoryNotFoundException("Destination parent directory not found at: " + toParentPath))
        let dstFullPath = combinePath parentDir.FullPath toDirName
        // Collision check
        match tryGetDir db dstFullPath with
        | Some existing when replace ->
            deleteDirectory db existing // CASCADE deletes entire subtree
        | Some _ ->
            raise (IOException("Directory already exists."))
        | None -> ()
        // Check non-recursive guard: source must be empty if recursive=false
        if not recursive then
            let hasChildren =
                db.QueryFirst<bool>(
                    "SELECT EXISTS (SELECT 1 FROM SoloDBDirectoryHeader WHERE ParentId = @Id UNION ALL SELECT 1 FROM SoloDBFileHeader WHERE DirectoryId = @Id)",
                    {| Id = srcDir.Id |})
            if hasChildren then raise (IOException("Directory is not empty and recursive=false."))
        // Create destination directory
        let now = DateTimeOffset.Now
        db.Execute(
            "INSERT INTO SoloDBDirectoryHeader(Name, ParentId, FullPath, Created, Modified) VALUES(@Name, @ParentId, @FullPath, @Created, @Modified)",
            {| Name = toDirName; ParentId = parentDir.Id; FullPath = dstFullPath; Created = now; Modified = now |})
        |> ignore
        let dstDir = match tryGetDir db dstFullPath with | Some d -> d | None -> failwithf "Cannot find directory just created: %s" dstFullPath
        // Copy directory metadata if requested
        if copyMetadata then
            copyDirectoryMetadata db srcDir.Id dstDir.Id
        // Copy files in this directory
        let files = getFilesWhere db "DirectoryId = @DirectoryId" {| DirectoryId = srcDir.Id |} |> Seq.toList
        for file in files do
            let fileDstPath = combinePath dstFullPath file.Name
            let dstFileHeader = createFileCopyAt db file dstDir.Id fileDstPath file.Name
            copyFileChunks db file.Id dstFileHeader.Id
            if copyMetadata then
                copyFileMetadata db file.Id dstFileHeader.Id
        // Recurse into subdirectories
        if recursive then
            let subDirs = db.Query<SoloDBDirectoryHeader>("SELECT * FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId", {| ParentId = srcDir.Id |}) |> Seq.toList
            for subDir in subDirs do
                let subDstPath = combinePath dstFullPath subDir.Name
                copyDirectoryMustBeWithinTransaction db subDir.FullPath subDstPath replace false copyMetadata |> ignore
        dstDir
