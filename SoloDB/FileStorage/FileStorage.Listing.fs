namespace SoloDatabase

open System.Collections.Generic
open System
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.Types
open FileStorageCore
open FileStorageHelpers

module internal FileStorageListing =
    /// Fetches one page of files for an already-resolved directory id. Split out so a caller that
    /// has already resolved the directory and counted its children does not repeat either.
    let internal listFilesPageByDirId (db: SqliteConnection) (directoryId: int64) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        let dir = {| Id = directoryId |}
        let orderBy = getFileOrderBy "fh" sortBy sortDir
        let query =
            $"""
            WITH PagedFiles AS (
                SELECT fh.*
                FROM SoloDBFileHeader fh
                WHERE fh.DirectoryId = @DirectoryId
                ORDER BY {orderBy}
                LIMIT @Limit OFFSET @Offset
            )
            SELECT fh.*, fm.Key as MetaKey, fm.Value as MetaValue
            FROM PagedFiles fh
            LEFT JOIN SoloDBFileMetadata fm ON fh.Id = fm.FileId
            ORDER BY {orderBy}
            """
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
            |> UtilsReflection.SeqExt.sequentialGroupBy(fun e -> e.Id)
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
        files :> IList<SoloDBFileHeader>

    /// Public paginated file listing: resolves the directory, counts, then pages.
    let internal listFilesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        match tryGetDirIdAt db (formatPath path) with
        | ValueNone -> (ResizeArray<SoloDBFileHeader>() :> IList<SoloDBFileHeader>, 0L)
        | ValueSome dirId ->
        let count = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId", {|DirectoryId = dirId|})
        (listFilesPageByDirId db dirId sortBy sortDir limit offset, count)

    /// Fetches one page of directories for an already-resolved parent id.
    let internal listDirectoriesPageByDirId (db: SqliteConnection) (directoryId: int64) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        let dir = {| Id = directoryId |}
        let orderBy = getDirectoryOrderBy "dh" sortBy sortDir
        let query =
            $"""
            WITH PagedDirectories AS (
                SELECT dh.*
                FROM SoloDBDirectoryHeader dh
                WHERE dh.ParentId = @ParentId
                ORDER BY {orderBy}
                LIMIT @Limit OFFSET @Offset
            )
            SELECT dh.*, dm.Key, dm.Value
            FROM PagedDirectories dh
            LEFT JOIN SoloDBDirectoryMetadata dm ON dh.Id = dm.DirectoryId
            ORDER BY {orderBy}
            """
        let directoryDictionary = new Dictionary<int64, SoloDBDirectoryHeader>()
        let directories = ResizeArray<SoloDBDirectoryHeader>()
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
                        directories.Add(dir)
                        dir
                if not (isNull metadata) then
                    (dir.Metadata :?> IDictionary<string, string>).Add(metadata.Key, metadata.Value)
                ()
            ),
            {|ParentId = dir.Id; Limit = limit; Offset = offset|},
            splitOn = "Key"
        ) |> Seq.iter ignore
        directories :> IList<SoloDBDirectoryHeader>

    /// Public paginated directory listing: resolves the directory, counts, then pages.
    let internal listDirectoriesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        match tryGetDirIdAt db (formatPath path) with
        | ValueNone -> (ResizeArray<SoloDBDirectoryHeader>() :> IList<SoloDBDirectoryHeader>, 0L)
        | ValueSome dirId ->
        let count = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId", {|ParentId = dirId|})
        (listDirectoriesPageByDirId db dirId sortBy sortDir limit offset, count)

    let internal listEntriesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        // Resolve the directory once and count each side once. The page primitives take the
        // resolved id, so a boundary-crossing page no longer repeats the lookup or the counts.
        match tryGetDirIdAt db (formatPath path) with
        | ValueNone -> (ResizeArray<SoloDBEntryHeader>() :> IList<SoloDBEntryHeader>, 0L, 0L)
        | ValueSome dirId ->
        let dirCount = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId", {|ParentId = dirId|})
        let fileCount = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId", {|DirectoryId = dirId|})
        let result = ResizeArray<SoloDBEntryHeader>(min limit (int (dirCount + fileCount)))
        if int64 offset < dirCount then
            let dirsToFetch = min limit (int dirCount - offset)
            for d in listDirectoriesPageByDirId db dirId sortBy sortDir dirsToFetch offset do
                result.Add(SoloDBEntryHeader.Directory d)
            let remaining = limit - dirsToFetch
            if remaining > 0 then
                for f in listFilesPageByDirId db dirId sortBy sortDir remaining 0 do
                    result.Add(SoloDBEntryHeader.File f)
        else
            let fileOffset = offset - int dirCount
            for f in listFilesPageByDirId db dirId sortBy sortDir limit fileOffset do
                result.Add(SoloDBEntryHeader.File f)
        (result :> IList<SoloDBEntryHeader>, dirCount, fileCount)
