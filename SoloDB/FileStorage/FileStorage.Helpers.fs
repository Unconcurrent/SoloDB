namespace SoloDatabase

open System.Collections.Generic
open System
open System.IO
open NativeSQLiteTools
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

    let internal createFileAt (db: NativeSqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        // The parent is needed only for its id; its header and metadata would be discarded.
        let directoryId = getOrCreateDirId db dirPath
        let fullPath = combinePath dirPath name
        let now = DateTimeOffset.Now
        let mutable statement = db.OpenStatement("INSERT INTO SoloDBFileHeader(Name, FullPath, DirectoryId, Length, Created, Modified) VALUES (@Name, @FullPath, @DirectoryId, 0, @Created, @Modified) ON CONFLICT(FullPath) DO NOTHING")
        try
            statement.BindText(1, name)
            statement.BindText(2, fullPath)
            statement.BindInt64(3, directoryId)
            statement.BindInt64(4, now.ToUnixTimeMilliseconds())
            statement.BindInt64(5, now.ToUnixTimeMilliseconds())
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
        let mutable header = db.OpenStatement("SELECT " + fileHeaderColumns + " FROM SoloDBFileHeader WHERE FullPath = @FullPath")
        try
            header.BindText(1, fullPath)
            if not (header.Step()) then Seq.head (Seq.empty<unit>)
            let result = readFileHeader header.Row (readOnlyDict [])
            header.Dispose()
            result
        with error -> header.Abort(error); reraise()

    let internal listDirectoriesAt (db: NativeSqliteConnection) (path: string) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> Seq.empty
        | Some dir ->
        let id = dir.Id
        tryGetDirectoriesWhere db "ParentId = @Id" (fun statement -> statement.BindInt64(1, id))

    let internal readFileWithMetadata (statement: byref<NativeStatementScope>) =
        try
            let metadata = Dictionary<string, string>()
            let header = readFileHeader statement.Row metadata
            let mutable sameFile = true
            while sameFile do
                let row = statement.Row
                if not (row.IsDBNull(7)) then metadata.[row.GetString(7)] <- row.GetString(8)
                sameFile <- statement.Step() && statement.Row.GetInt64(0) = header.Id
            header
        with error -> statement.Abort(error); reraise()

    let private openFilesWhere (connection: NativeSqliteConnection) where (bind: NativeStatementScope -> unit) =
        let query = $"SELECT fh.Id, fh.Name, fh.FullPath, fh.DirectoryId, fh.Length, fh.Created, fh.Modified, fm.Key, fm.Value FROM SoloDBFileHeader fh LEFT JOIN SoloDBFileMetadata fm ON fh.Id = fm.FileId WHERE {where}"
        let mutable statement = connection.OpenStatement(query)
        try
            bind statement
            statement.Step() |> ignore
            statement
        with error -> statement.Abort(error); reraise()

    let internal getFilesWhere (connection: NativeSqliteConnection) (where: string) (bind: NativeStatementScope -> unit) = seq {
        let mutable statement = openFilesWhere connection where bind
        try
            while statement.HasRow do yield readFileWithMetadata &statement
        finally statement.Dispose()
    }

    // ── Narrow path lookups ───────────────────────────────────────────────
    // getFilesWhere joins metadata and builds a full public header. Callers that only need an
    // identifier, or the fields a stream is built from, use these instead so the cost does
    // not scale with a file's unrelated metadata rows. The full-header owner is unchanged and
    // still serves every API that returns metadata to the caller.

    /// Id and FullPath: exactly what DbFileStream is constructed from.
    [<Struct>]
    type internal FileStreamIdentity =
        { Id: int64; FullPath: string }

    let internal tryGetFileIdAt (db: NativeSqliteConnection) (path: string) : int64 voption =
        let struct (dirPath, name) = getPathAndName path
        let mutable statement = db.OpenStatement("SELECT Id FROM SoloDBFileHeader WHERE FullPath = @FullPath LIMIT 1")
        try
            statement.BindText(1, combinePath dirPath name)
            let result = if statement.Step() then ValueSome(statement.Row.GetInt64(0)) else ValueNone
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()

    let internal tryGetFileStreamIdentityAt (db: NativeSqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let mutable statement = db.OpenStatement("SELECT Id, FullPath FROM SoloDBFileHeader WHERE FullPath = @FullPath LIMIT 1")
        try
            statement.BindText(1, combinePath dirPath name)
            let result =
                if statement.Step() then ValueSome { Id = statement.Row.GetInt64(0); FullPath = statement.Row.GetString(1) }
                else ValueNone
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()

    let internal tryGetFileAt (db: NativeSqliteConnection) (path: string) =
        let struct (dirPath, name) = getPathAndName path
        let fullPath = combinePath dirPath name
        getFilesWhere db "fh.FullPath = @FullPath" (fun statement -> statement.BindText(1, fullPath)) |> Seq.tryHead

    let internal getOrCreateFileAt (db: NativeSqliteConnection) (path: string) =
        match tryGetFileAt db path with
        | Some f -> f
        | None ->
        createFileAt db path

    let internal deleteDirectoryAt (db: NativeSqliteConnection) (path: string) =
        let dirPath = formatPath path
        let mutable statement = db.OpenStatement("DELETE FROM SoloDBDirectoryHeader WHERE FullPath = @Path")
        try
            statement.BindText(1, dirPath)
            statement.Step() |> ignore
            let affected = statement.RecordsAffected
            statement.Dispose()
            affected > 0
        with error -> statement.Abort(error); reraise()

    let internal deleteFileAt (db: NativeSqliteConnection) (path: string) =
        let filePath = formatPath path
        let mutable statement = db.OpenStatement("DELETE FROM SoloDBFileHeader WHERE FullPath = @Path")
        try
            statement.BindText(1, filePath)
            statement.Step() |> ignore
            let affected = statement.RecordsAffected
            statement.Dispose()
            affected > 0
        with error -> statement.Abort(error); reraise()

    let internal getOrCreateDirectoryAt (db: NativeSqliteConnection) (path: string) =
        let dirPath = formatPath path
        getOrCreateDir db dirPath

    let internal listFilesAt (db: NativeSqliteConnection) (path: string) : SoloDBFileHeader seq =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> Seq.empty
        | Some dir ->
        let id = dir.Id
        getFilesWhere db "DirectoryId = @DirectoryId" (fun statement -> statement.BindInt64(1, id)) |> ResizeArray :> SoloDBFileHeader seq

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

    let internal openFile (db: NativeConnection) (file: SoloDBFileHeader) =
        new FileStorageCoreStream.DbFileStream(db, file.Id, file.FullPath)

    /// Stream identity for an existing file, creating it when absent. Used by the write paths,
    /// which need a stream and never the header.
    let internal getOrCreateFileStreamIdentity (db: NativeSqliteConnection) (path: string) =
        match tryGetFileStreamIdentityAt db path with
        | ValueSome x -> x
        | ValueNone ->
            let created = createFileAt db path
            { Id = created.Id; FullPath = created.FullPath }

    let internal openFileByIdentity (db: NativeConnection) (identity: FileStreamIdentity) =
        new FileStorageCoreStream.DbFileStream(db, identity.Id, identity.FullPath)

    let internal requireFileStreamIdentityAt (db: NativeSqliteConnection) (path: string) =
        match tryGetFileStreamIdentityAt db path with
        | ValueSome x -> x
        | ValueNone -> raise (FileNotFoundException("File not found.", path))

    let internal openOrCreateFile (db: NativeConnection) (path: string) =
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
                    { Id = created.Id; FullPath = created.FullPath })
        new FileStorageCoreStream.DbFileStream(db, identity.Id, identity.FullPath)

    let internal setSoloDBFileMetadataById (db: NativeSqliteConnection) (fileId: int64) (key: string) (value: string) =
        let mutable statement = db.OpenStatement("INSERT OR REPLACE INTO SoloDBFileMetadata(FileId, Key, Value) VALUES(@FileId, @Key, @Value)")
        try
            statement.BindInt64(1, fileId)
            statement.BindText(2, key)
            statement.BindText(3, value)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let internal setSoloDBFileMetadata (db: NativeSqliteConnection) (file: SoloDBFileHeader) (key: string) (value: string) =
        setSoloDBFileMetadataById db file.Id key value

    let internal deleteSoloDBFileMetadataById (db: NativeSqliteConnection) (fileId: int64) (key: string) =
        let mutable statement = db.OpenStatement("DELETE FROM SoloDBFileMetadata WHERE FileId = @FileId AND Key = @Key")
        try
            statement.BindInt64(1, fileId)
            statement.BindText(2, key)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let internal deleteSoloDBFileMetadata (db: NativeSqliteConnection) (file: SoloDBFileHeader) (key: string) =
        deleteSoloDBFileMetadataById db file.Id key

    let internal setDirMetadataById (db: NativeSqliteConnection) (dirId: int64) (key: string) (value: string) =
        let mutable statement = db.OpenStatement("INSERT OR REPLACE INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) VALUES(@DirectoryId, @Key, @Value)")
        try
            statement.BindInt64(1, dirId)
            statement.BindText(2, key)
            statement.BindText(3, value)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let internal setDirMetadata (db: NativeSqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) (value: string) =
        setDirMetadataById db dir.Id key value

    let internal deleteDirMetadataById (db: NativeSqliteConnection) (dirId: int64) (key: string) =
        let mutable statement = db.OpenStatement("DELETE FROM SoloDBDirectoryMetadata WHERE DirectoryId = @DirectoryId AND Key = @Key")
        try
            statement.BindInt64(1, dirId)
            statement.BindText(2, key)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    let internal deleteDirMetadata (db: NativeSqliteConnection) (dir: SoloDBDirectoryHeader) (key: string) =
        deleteDirMetadataById db dir.Id key

    /// SQLITE_CONSTRAINT_UNIQUE. Identifies a uniqueness violation without inspecting message text,
    /// so an unrelated constraint failure is never relabelled as a path collision.
    let [<Literal>] private SqliteConstraintUnique = 2067

    let internal pathExists (db: NativeSqliteConnection) (fullPath: string) =
        let mutable statement = db.OpenStatement("SELECT EXISTS (SELECT 1 FROM SoloDBFileHeader WHERE FullPath = @FullPath UNION ALL SELECT 1 FROM SoloDBDirectoryHeader WHERE FullPath = @FullPath)")
        try
            statement.BindText(1, fullPath)
            if not (statement.Step()) then Seq.head (Seq.empty<unit>)
            let exists = statement.Row.GetBoolean(0)
            statement.Dispose()
            exists
        with error -> statement.Abort(error); reraise()

    /// Identity a file move needs: which row, and its current path for the self-move check.
    [<Struct>]
    type internal FileMoveIdentity =
        { Id: int64; FullPath: string }

    let internal tryGetFileMoveIdentityAt (db: NativeSqliteConnection) (path: string) =
        match tryGetFileStreamIdentityAt db path with
        | ValueSome identity -> ValueSome { FileMoveIdentity.Id = identity.Id; FullPath = identity.FullPath }
        | ValueNone -> ValueNone

    let internal moveFileByIdentity (db: NativeSqliteConnection) (file: FileMoveIdentity) (toDirId: int64) (toDirFullPath: string) (newName: string) =
        let newFileFullPath = combinePath toDirFullPath newName
        if newFileFullPath <> file.FullPath && pathExists db newFileFullPath then
            raise (IOException "File already exists.")
        try
            let mutable statement = db.OpenStatement("UPDATE SoloDBFileHeader
            SET FullPath = @NewFullPath,
            DirectoryId = @DestDirId,
            Name = @NewName
            WHERE Id = @FileId")
            try
                statement.BindText(1, newFileFullPath)
                statement.BindInt64(2, toDirId)
                statement.BindText(3, newName)
                statement.BindInt64(4, file.Id)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        with
        | :? NativeSqliteException as ex when ex.SqliteExtendedErrorCode = SqliteConstraintUnique ->
            raise (IOException("File already exists.", ex))

    let internal moveFile (db: NativeSqliteConnection) (file: SoloDBFileHeader) (toDir: SoloDBDirectoryHeader) (newName: string) =
        let newFileFullPath = combinePath toDir.FullPath newName
        if newFileFullPath <> file.FullPath && pathExists db newFileFullPath then
            raise (IOException "File already exists.")
        try
            let mutable statement = db.OpenStatement("UPDATE SoloDBFileHeader
            SET FullPath = @NewFullPath,
            DirectoryId = @DestDirId,
            Name = @NewName
            WHERE Id = @FileId")
            try
                statement.BindText(1, newFileFullPath)
                statement.BindInt64(2, toDir.Id)
                statement.BindText(3, newName)
                statement.BindInt64(4, file.Id)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        with
        | :? NativeSqliteException as ex when ex.SqliteExtendedErrorCode = SqliteConstraintUnique ->
            raise (IOException("File already exists.", ex))

    /// Rewrites a moved subtree in a bounded number of statements.
    ///
    /// Descendant paths are rebuilt by position: the segment of FullPath after the old root's
    /// length is appended to the new root. A textual REPLACE cannot be used here because it
    /// rewrites every occurrence rather than the leading prefix, which corrupts any path where
    /// the old root also appears later in the string.
    let internal moveDirectoryMustBeWithinTransaction (db: NativeSqliteConnection) (dir: SoloDBDirectoryHeader) (newParentDir: SoloDBDirectoryHeader) (newName: string) =
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
            let mutable statement = db.OpenStatement(subtreeCte + """
                UPDATE SoloDBDirectoryHeader
                SET FullPath = @NewFullPath || substr(FullPath, @OldPathLength + 1)
                WHERE Id IN (SELECT Id FROM Subtree) AND Id <> @DirId
                """)
            try
                statement.BindInt64(1, dir.Id)
                statement.BindText(2, newDirFullPath)
                statement.BindInt64(3, int64 oldDirPath.Length)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()

            let mutable statement = db.OpenStatement(subtreeCte + """
                UPDATE SoloDBFileHeader
                SET FullPath = @NewFullPath || substr(FullPath, @OldPathLength + 1)
                WHERE DirectoryId IN (SELECT Id FROM Subtree)
                """)
            try
                statement.BindInt64(1, dir.Id)
                statement.BindText(2, newDirFullPath)
                statement.BindInt64(3, int64 oldDirPath.Length)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()

            let mutable statement = db.OpenStatement("UPDATE SoloDBDirectoryHeader
                     SET FullPath = @NewFullPath,
                         ParentId = @NewParentId,
                         Name = @NewName
                     WHERE Id = @DirId")
            try
                statement.BindText(1, newDirFullPath)
                statement.BindInt64(2, newParentDir.Id)
                statement.BindText(3, newName)
                statement.BindInt64(4, dir.Id)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        with
        | :? NativeSqliteException as ex when ex.SqliteExtendedErrorCode = SqliteConstraintUnique ->
            raise (IOException("Directory or file already exists in the destination.", ex))

    // ── Copy helpers ──────────────────────────────────────────────────────

    /// Immutable identity fields a file copy needs. Metadata is never loaded here; when metadata
    /// is copied it moves at SQL level, and is read back only for the header actually returned.
    [<Struct>]
    type internal FileCopyIdentity =
        { Id: int64; Name: string; Length: int64 }

    /// Immutable identity fields a directory copy traversal needs.
    [<Struct>]
    type internal DirectoryCopyIdentity =
        { Id: int64; Name: string; FullPath: string }

    let internal tryGetFileCopyIdentityAt (db: NativeSqliteConnection) (fullPath: string) =
        let mutable statement = db.OpenStatement("SELECT Id, Name, Length FROM SoloDBFileHeader WHERE FullPath = @FullPath LIMIT 1")
        try
            statement.BindText(1, fullPath)
            let result =
                if statement.Step() then
                    ValueSome { FileCopyIdentity.Id = statement.Row.GetInt64(0); Name = statement.Row.GetString(1); Length = statement.Row.GetInt64(2) }
                else ValueNone
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()

    /// Children of one source directory, captured before any destination row is inserted.
    /// The query is keyed on the source parent id, so rows created under the destination during
    /// the copy can never re-enter this traversal.
    let internal getFileCopyIdentitiesInDir (db: NativeSqliteConnection) (sourceDirId: int64) =
        let mutable statement = db.OpenStatement("SELECT Id, Name, Length FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId")
        try
            statement.BindInt64(1, sourceDirId)
            let files = ResizeArray<FileCopyIdentity>()
            while statement.Step() do
                files.Add { Id = statement.Row.GetInt64(0); Name = statement.Row.GetString(1); Length = statement.Row.GetInt64(2) }
            statement.Dispose()
            files.ToArray()
        with error -> statement.Abort(error); reraise()

    let internal getSubdirectoryCopyIdentities (db: NativeSqliteConnection) (sourceParentId: int64) =
        let mutable statement = db.OpenStatement("SELECT Id, Name, FullPath FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId")
        try
            statement.BindInt64(1, sourceParentId)
            let directories = ResizeArray<DirectoryCopyIdentity>()
            while statement.Step() do
                directories.Add { Id = statement.Row.GetInt64(0); Name = statement.Row.GetString(1); FullPath = statement.Row.GetString(2) }
            statement.Dispose()
            directories.ToArray()
        with error -> statement.Abort(error); reraise()

    let internal getFileMetadataMap (db: NativeSqliteConnection) (fileId: int64) =
        let mutable statement = db.OpenStatement("SELECT Key, Value FROM SoloDBFileMetadata WHERE FileId = @FileId")
        try
            statement.BindInt64(1, fileId)
            let metadata = Dictionary<string, string>()
            while statement.Step() do metadata.Add(statement.Row.GetString(0), statement.Row.GetString(1))
            statement.Dispose()
            metadata :> IReadOnlyDictionary<string, string>
        with error -> statement.Abort(error); reraise()

    /// Bulk-copies all chunk rows from source file to destination file via SQL-level INSERT...SELECT.
    /// No Snappy decompression/recompression — compressed blobs are copied as-is.
    let internal copyFileChunks (db: NativeSqliteConnection) (srcFileId: int64) (dstFileId: int64) knownSourceLength originalVersion =
        let headerMutated = FileStorageContentEvents.version db <> originalVersion
        let sourceLength = if headerMutated then getFileLengthById db srcFileId else knownSourceLength
        let oldLength = if headerMutated then getFileLengthById db dstFileId else knownSourceLength
        let version = FileStorageContentEvents.version db
        let facts = FileStorageContentEvents.prepare db {
            FileId = dstFileId; Offset = 0L; Length = sourceLength
            OldLength = oldLength; NewLength = sourceLength; Kind = FileContentChangeKind.Copy }
        // The before callback may have written destination chunks. A copy replaces
        // the destination's logical content, including holes absent from the source.
        let callbackMutated = headerMutated || FileStorageContentEvents.version db <> version
        if callbackMutated then
            let mutable statement = db.OpenStatement("DELETE FROM SoloDBFileChunk WHERE FileId = @Id")
            try
                statement.BindInt64(1, dstFileId)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        let currentSourceLength =
            if callbackMutated then getFileLengthById db srcFileId else sourceLength
        let mutable statement = db.OpenStatement("INSERT INTO SoloDBFileChunk(FileId, Number, Data) SELECT @DstFileId, Number, Data FROM SoloDBFileChunk WHERE FileId = @SrcFileId ORDER BY Number")
        try
            statement.BindInt64(1, dstFileId)
            statement.BindInt64(2, srcFileId)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
        let beforeLengthUpdate = FileStorageContentEvents.version db
        if facts.OldLength <> currentSourceLength then updateLenById db dstFileId currentSourceLength
        let copiedLength =
            if FileStorageContentEvents.version db <> beforeLengthUpdate then getFileLengthById db dstFileId
            else currentSourceLength
        if currentSourceLength <> 0L then
            FileStorageContentEvents.completed db { facts with Length = currentSourceLength; NewLength = copiedLength }
        copiedLength

    /// Bulk-copies all metadata key-value pairs from source file to destination file.
    let internal copyFileMetadata (db: NativeSqliteConnection) (srcFileId: int64) (dstFileId: int64) =
        let mutable statement = db.OpenStatement("INSERT INTO SoloDBFileMetadata(FileId, Key, Value) SELECT @DstFileId, Key, Value FROM SoloDBFileMetadata WHERE FileId = @SrcFileId")
        try
            statement.BindInt64(1, dstFileId)
            statement.BindInt64(2, srcFileId)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    /// Bulk-copies all metadata key-value pairs from source directory to destination directory.
    let internal copyDirectoryMetadata (db: NativeSqliteConnection) (srcDirId: int64) (dstDirId: int64) =
        let mutable statement = db.OpenStatement("INSERT INTO SoloDBDirectoryMetadata(DirectoryId, Key, Value) SELECT @DstDirId, Key, Value FROM SoloDBDirectoryMetadata WHERE DirectoryId = @SrcDirId")
        try
            statement.BindInt64(1, dstDirId)
            statement.BindInt64(2, srcDirId)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()

    /// Initializes the destination with its known logical length before copying chunks.
    /// Returns the new SoloDBFileHeader.
    let internal createFileCopyAt (db: NativeSqliteConnection) (srcLength: int64) (dstDirId: int64) (dstFullPath: string) (dstName: string) =
        let now = DateTimeOffset.Now
        let mutable statement = db.OpenStatement("INSERT INTO SoloDBFileHeader(Name, FullPath, DirectoryId, Length, Created, Modified) VALUES (@Name, @FullPath, @DirectoryId, @Length, @Created, @Modified) RETURNING " + fileHeaderColumns)
        try
            statement.BindText(1, dstName)
            statement.BindText(2, dstFullPath)
            statement.BindInt64(3, dstDirId)
            statement.BindInt64(4, srcLength)
            statement.BindInt64(5, now.ToUnixTimeMilliseconds())
            statement.BindInt64(6, now.ToUnixTimeMilliseconds())
            if not (statement.Step()) then Seq.head (Seq.empty<unit>)
            let header = readFileHeader statement.Row null
            statement.Dispose()
            header
        with error -> statement.Abort(error); reraise()

    /// Core file-copy logic. Must be called within a transaction.
    /// If replace=true, deletes existing destination before copy. If replace=false, fails on collision.
    let internal copyFileMustBeWithinTransaction (db: NativeSqliteConnection) (fromPath: string) (toPath: string) (replace: bool) (copyMetadata: bool) =
        // Edge case: self-copy
        let fromNorm = formatPath fromPath
        let toNorm = formatPath toPath
        if fromNorm = toNorm then raise (ArgumentException("Cannot copy a file to itself.", "toPath"))
        let originalVersion = FileStorageContentEvents.version db
        // Resolve source
        // Identity only: the source header and its metadata would be discarded when
        // copyMetadata is false, and metadata is copied at SQL level when it is true.
        let src =
            match tryGetFileCopyIdentityAt db fromNorm with
            | ValueSome f -> f
            | ValueNone -> raise (FileNotFoundException("File not found.", fromPath))
        let struct (toDirPath, toName) = getPathAndName toPath
        // File copy auto-creates destination parent; only its id is needed.
        let dstDirId = getOrCreateDirId db (formatPath toDirPath)
        let dstFullPath = combinePath (formatPath toDirPath) toName
        // Collision check needs existence, not a header.
        match tryGetFileIdAt db dstFullPath with
        | ValueSome existingId when replace ->
            deleteFileById db existingId |> ignore // CASCADE deletes chunks + metadata
        | ValueSome _ ->
            raise (IOException("File already exists."))
        | ValueNone -> ()
        let dstHeader = createFileCopyAt db src.Length dstDirId dstFullPath toName
        let copiedLength = copyFileChunks db src.Id dstHeader.Id src.Length originalVersion
        if copyMetadata then
            copyFileMetadata db src.Id dstHeader.Id
        // Metadata is read back only for the header this call returns.
        {dstHeader with Length = copiedLength; Metadata = if copyMetadata then getFileMetadataMap db dstHeader.Id else readOnlyDict []}

    /// Recursive directory copy. Must be called within a transaction.
    /// If replace=true, deletes existing destination tree before copy. If replace=false, fails on collision.
    let rec internal copyDirectoryMustBeWithinTransaction (db: NativeSqliteConnection) (fromPath: string) (toPath: string) (replace: bool) (recursive: bool) (copyMetadata: bool) =
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
                let mutable statement = db.OpenStatement("SELECT EXISTS (SELECT 1 FROM SoloDBDirectoryHeader WHERE ParentId = @Id UNION ALL SELECT 1 FROM SoloDBFileHeader WHERE DirectoryId = @Id)")
                try
                    statement.BindInt64(1, srcDir.Id)
                    if not (statement.Step()) then Seq.head (Seq.empty<unit>)
                    let exists = statement.Row.GetBoolean(0)
                    statement.Dispose()
                    exists
                with error -> statement.Abort(error); reraise()
            if hasChildren then raise (IOException("Directory is not empty and recursive=false."))
        // Create destination directory
        let now = DateTimeOffset.Now
        let mutable statement = db.OpenStatement("INSERT INTO SoloDBDirectoryHeader(Name, ParentId, FullPath, Created, Modified) VALUES(@Name, @ParentId, @FullPath, @Created, @Modified)")
        try
            statement.BindText(1, toDirName)
            statement.BindInt64(2, parentDir.Id)
            statement.BindText(3, dstFullPath)
            statement.BindInt64(4, now.ToUnixTimeMilliseconds())
            statement.BindInt64(5, now.ToUnixTimeMilliseconds())
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
        let dstDir = match tryGetDir db dstFullPath with | Some d -> d | None -> failwithf "Cannot find directory just created: %s" dstFullPath
        // Copy directory metadata if requested
        if copyMetadata then
            copyDirectoryMetadata db srcDir.Id dstDir.Id
        // Copy files in this directory
        // Snapshot the source children by source parent id before inserting any destination row,
        // holding only narrow immutable identity fields.
        let originalVersion = FileStorageContentEvents.version db
        let files = getFileCopyIdentitiesInDir db srcDir.Id
        for file in files do
            let fileDstPath = combinePath dstFullPath file.Name
            let dstFileHeader = createFileCopyAt db file.Length dstDir.Id fileDstPath file.Name
            copyFileChunks db file.Id dstFileHeader.Id file.Length originalVersion |> ignore
            if copyMetadata then
                copyFileMetadata db file.Id dstFileHeader.Id
        // Recurse into subdirectories
        if recursive then
            let subDirs = getSubdirectoryCopyIdentities db srcDir.Id
            for subDir in subDirs do
                let subDstPath = combinePath dstFullPath subDir.Name
                // Propagate the caller's recursive flag: a nested directory that itself has
                // children must still be copied, not rejected as non-empty.
                copyDirectoryMustBeWithinTransaction db subDir.FullPath subDstPath replace recursive copyMetadata |> ignore
        dstDir
