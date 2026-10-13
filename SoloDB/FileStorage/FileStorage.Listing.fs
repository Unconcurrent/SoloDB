namespace SoloDatabase

open System.Collections.Generic
open System
open NativeSQLiteTools
open SoloDatabase.Types
open FileStorageCore
open FileStorageHelpers

module internal FileStorageListing =
    /// Fetches one page of files for an already-resolved directory id. Split out so a caller that
    /// has already resolved the directory and counted its children does not repeat either.
    let internal listFilesPageByDirId (db: NativeSqliteConnection) (directoryId: int64) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
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
            SELECT fh.Id, fh.Name, fh.FullPath, fh.DirectoryId, fh.Length, fh.Created, fh.Modified, fm.Key, fm.Value
            FROM PagedFiles fh
            LEFT JOIN SoloDBFileMetadata fm ON fh.Id = fm.FileId
            ORDER BY {orderBy}
            """
        let mutable statement = db.OpenStatement(query)
        try
            statement.BindInt64(1, directoryId)
            statement.BindInt64(2, int64 limit)
            statement.BindInt64(3, int64 offset)
            let files = ResizeArray<SoloDBFileHeader>()
            statement.Step() |> ignore
            while statement.HasRow do files.Add(readFileWithMetadata &statement)
            statement.Dispose()
            files :> IList<SoloDBFileHeader>
        with error -> statement.Abort(error); reraise()

    let private countFiles (db: NativeSqliteConnection) directoryId =
        let mutable statement = db.OpenStatement("SELECT COUNT(*) FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId")
        try
            statement.BindInt64(1, directoryId)
            if not (statement.Step()) then Seq.head (Seq.empty<unit>)
            let count = statement.Row.GetInt64(0)
            statement.Dispose()
            count
        with error -> statement.Abort(error); reraise()

    let private countDirectories (db: NativeSqliteConnection) parentId =
        let mutable statement = db.OpenStatement("SELECT COUNT(*) FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId")
        try
            statement.BindInt64(1, parentId)
            if not (statement.Step()) then Seq.head (Seq.empty<unit>)
            let count = statement.Row.GetInt64(0)
            statement.Dispose()
            count
        with error -> statement.Abort(error); reraise()

    /// Public paginated file listing: resolves the directory, counts, then pages.
    let internal listFilesAtPaginated (db: NativeSqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        match tryGetDirIdAt db (formatPath path) with
        | ValueNone -> (ResizeArray<SoloDBFileHeader>() :> IList<SoloDBFileHeader>, 0L)
        | ValueSome dirId ->
        let count = countFiles db dirId
        (listFilesPageByDirId db dirId sortBy sortDir limit offset, count)

    /// Fetches one page of directories for an already-resolved parent id.
    let internal listDirectoriesPageByDirId (db: NativeSqliteConnection) (directoryId: int64) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
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
            SELECT dh.Id, dh.Name, dh.FullPath, dh.ParentId, dh.Created, dh.Modified, dm.Key, dm.Value
            FROM PagedDirectories dh
            LEFT JOIN SoloDBDirectoryMetadata dm ON dh.Id = dm.DirectoryId
            ORDER BY {orderBy}
            """
        let mutable statement = db.OpenStatement(query)
        try
            statement.BindInt64(1, directoryId)
            statement.BindInt64(2, int64 limit)
            statement.BindInt64(3, int64 offset)
            let directoryDictionary = Dictionary<int64, SoloDBDirectoryHeader>()
            let directories = ResizeArray<SoloDBDirectoryHeader>()
            while statement.Step() do
                let row = statement.Row
                let directory =
                    match directoryDictionary.TryGetValue(row.GetInt64(0)) with
                    | true, directory -> directory
                    | _ ->
                        let directory = readDirectoryHeader row (Dictionary<string, string>())
                        directoryDictionary.Add(directory.Id, directory)
                        directories.Add(directory)
                        directory
                if not (row.IsDBNull(6)) then
                    (directory.Metadata :?> Dictionary<string, string>).Add(row.GetString(6), row.GetString(7))
            statement.Dispose()
            directories :> IList<SoloDBDirectoryHeader>
        with error -> statement.Abort(error); reraise()

    /// Public paginated directory listing: resolves the directory, counts, then pages.
    let internal listDirectoriesAtPaginated (db: NativeSqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        match tryGetDirIdAt db (formatPath path) with
        | ValueNone -> (ResizeArray<SoloDBDirectoryHeader>() :> IList<SoloDBDirectoryHeader>, 0L)
        | ValueSome dirId ->
        let count = countDirectories db dirId
        (listDirectoriesPageByDirId db dirId sortBy sortDir limit offset, count)

    let internal listEntriesAtPaginated (db: NativeSqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        // Resolve the directory once and count each side once. The page primitives take the
        // resolved id, so a boundary-crossing page no longer repeats the lookup or the counts.
        match tryGetDirIdAt db (formatPath path) with
        | ValueNone -> (ResizeArray<SoloDBEntryHeader>() :> IList<SoloDBEntryHeader>, 0L, 0L)
        | ValueSome dirId ->
        let dirCount = countDirectories db dirId
        let fileCount = countFiles db dirId
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
