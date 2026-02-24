namespace SoloDatabase

open System.Collections.Generic
open System
open Microsoft.Data.Sqlite
open SQLiteTools
open SoloDatabase.Types
open FileStorageCore
open FileStorageHelpers

module internal FileStorageListing =
    let internal listFilesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> (ResizeArray<SoloDBFileHeader>() :> IList<SoloDBFileHeader>, 0L)
        | Some dir ->
        let sortCol = getFileSortColumn sortBy
        let sortDirStr = getSortDirection sortDir
        let count = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId", {|DirectoryId = dir.Id|})
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

    let internal listDirectoriesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> (ResizeArray<SoloDBDirectoryHeader>() :> IList<SoloDBDirectoryHeader>, 0L)
        | Some dir ->
        let sortCol = getDirSortColumn sortBy
        let sortDirStr = getSortDirection sortDir
        let count = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId", {|ParentId = dir.Id|})
        let query = sprintf "SELECT dh.*, dm.Key, dm.Value FROM SoloDBDirectoryHeader dh LEFT JOIN SoloDBDirectoryMetadata dm ON dh.Id = dm.DirectoryId WHERE dh.ParentId = @ParentId ORDER BY %s %s LIMIT @Limit OFFSET @Offset" sortCol sortDirStr
        let directoryDictionary = new Dictionary<int64, SoloDBDirectoryHeader>()
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

    let internal listEntriesAtPaginated (db: SqliteConnection) (path: string) (sortBy: SortField) (sortDir: SortDirection) (limit: int) (offset: int) =
        let dirPath = formatPath path
        match tryGetDir db dirPath with
        | None -> (ResizeArray<SoloDBEntryHeader>() :> IList<SoloDBEntryHeader>, 0L, 0L)
        | Some dir ->
        let dirCount = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBDirectoryHeader WHERE ParentId = @ParentId", {|ParentId = dir.Id|})
        let fileCount = db.QueryFirst<int64>("SELECT COUNT(*) FROM SoloDBFileHeader WHERE DirectoryId = @DirectoryId", {|DirectoryId = dir.Id|})
        let result = ResizeArray<SoloDBEntryHeader>(min limit (int (dirCount + fileCount)))
        if int64 offset < dirCount then
            let dirsToFetch = min limit (int dirCount - offset)
            let dirs, _ = listDirectoriesAtPaginated db path sortBy sortDir dirsToFetch offset
            for d in dirs do
                result.Add(SoloDBEntryHeader.Directory d)
            let remaining = limit - dirsToFetch
            if remaining > 0 then
                let files, _ = listFilesAtPaginated db path sortBy sortDir remaining 0
                for f in files do
                    result.Add(SoloDBEntryHeader.File f)
        else
            let fileOffset = offset - int dirCount
            let files, _ = listFilesAtPaginated db path sortBy sortDir limit fileOffset
            for f in files do
                result.Add(SoloDBEntryHeader.File f)
        (result :> IList<SoloDBEntryHeader>, dirCount, fileCount)
