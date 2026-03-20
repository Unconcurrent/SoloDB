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
open FileStorageCoreChunks
open FileStorageCoreStream
open FileStorageHelpers
open FileStorageListing
open FileStorageCopySurface

// NativePtr operations for efficient stream handling
#nowarn "9"

module FileStorage =
    type DbFileStream = FileStorageCoreStream.DbFileStream

    /// <summary>
    /// Provides an API for interacting with a virtual file system within the SQLite database.
    /// </summary>
    type internal FileSystem(connection: Connection) =
        member this.Upload(path, stream: Stream) =
            connection.WithTransaction(fun tx ->
                let innerConnection = Transactional tx
                use file = openOrCreateFile innerConnection path
                stream.CopyTo(file, int chunkSize)
                file.SetLength file.Position
            )

        member this.UploadAsync(path, stream: Stream) = task {
            return! connection.WithAsyncTransaction(fun tx -> task {
                let innerConnection = Transactional tx
                use file = openOrCreateFile innerConnection path
                do! stream.CopyToAsync(file, int chunkSize)
                file.SetLength file.Position
            })
        }

        member this.UploadBulk(files: BulkFileData seq) =
            connection.WithTransaction(fun tx ->
                let innerConnection = Transactional tx
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

        member this.ReplaceAsyncWithinTransaction(path, stream: Stream) = task {
            return! connection.WithAsyncTransaction(fun tx -> task {
                let innerConnection = Transactional tx
                use file = openOrCreateFile innerConnection path
                do! stream.CopyToAsync(file, int chunkSize)
                file.SetLength file.Position
            })
        }

        member this.Download(path, stream: Stream) =
            let fileHeader =
                use db = connection.Get()
                match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
            use fileStream = openFile connection fileHeader
            fileStream.CopyTo(stream, int chunkSize * 10)

        member this.DownloadAsync(path, stream: Stream) = task {
            let fileHeader =
                use db = connection.Get()
                match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
            use fileStream = openFile connection fileHeader
            do! fileStream.CopyToAsync(stream, int chunkSize * 10)
        }

        member this.GetAt path =
            use db = connection.Get()
            match tryGetFileAt db path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))

        member this.TryGetAt path =
            use db = connection.Get()
            tryGetFileAt db path

        member this.Exists(path: string) =
            use db = connection.Get()
            let sql = """
                SELECT EXISTS (
                    SELECT 1 FROM SoloDBFileHeader WHERE FullPath = @Path
                    UNION ALL
                    SELECT 1 FROM SoloDBDirectoryHeader WHERE FullPath = @Path
                )
                """
            db.QueryFirst<bool>(sql, {| Path = formatPath path |})

        member this.GetOrCreateAt path =
            let existing =
                use db = connection.Get()
                tryGetFileAt db path
            match existing with
            | Some f -> f
            | None -> connection.WithTransaction(fun tx -> getOrCreateFileAt tx path)

        member this.GetDirAt path =
            use db = connection.Get()
            match tryGetDir db path with | Some f -> f | None -> raise (DirectoryNotFoundException("Directory not found at: " + path))

        member this.TryGetDirAt path =
            use db = connection.Get()
            tryGetDir db path

        member this.GetOrCreateDirAt path =
            let existing =
                use db = connection.Get()
                tryGetDir db (formatPath path)
            match existing with
            | Some d -> d
            | None -> connection.WithTransaction(fun tx -> getOrCreateDirectoryAt tx path)

        member this.Open file = openFile connection file
        member this.OpenAt path =
            let file = this.GetAt path
            openFile connection file

        member this.TryOpenAt path =
            match this.TryGetAt path with
            | None -> None
            | Some file -> openFile connection file |> Some

        member this.OpenOrCreateAt path = openOrCreateFile connection path

        member this.WriteAt(path: string, offset: int64, data: byte[], [<Optional; DefaultParameterValue(true)>] createIfInexistent: bool) =
             connection.WithTransaction(fun tx ->
                let file = if createIfInexistent then getOrCreateFileAt tx path else match tryGetFileAt tx path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
                let innerConnection = Transactional tx
                use fileStream = openFile innerConnection file
                fileStream.Position <- offset
                fileStream.Write(data, 0, data.Length)
                ()
            )

        member this.WriteAt(path: string, offset: int64, data: Stream, [<Optional; DefaultParameterValue(true)>] createIfInexistent: bool) =
            connection.WithTransaction(fun tx ->
                let file = if createIfInexistent then getOrCreateFileAt tx path else match tryGetFileAt tx path with | Some f -> f | None -> raise (FileNotFoundException("File not found.", path))
                let innerConnection = Transactional tx
                use fileStream = openFile innerConnection file
                fileStream.Position <- offset
                data.CopyTo (fileStream, int chunkSize * 10)
            )

        member this.ReadAt(path: string, offset: int64, len) =
            let file = this.GetAt path
            use fileStream = new BinaryReader(openFile connection file)
            fileStream.BaseStream.Position <- offset
            fileStream.ReadBytes len

        member private _.WithFileAt(path: string, f: SqliteConnection -> SoloDBFileHeader -> 'T) =
            connection.WithTransaction(fun tx ->
                let file = match tryGetFileAt tx path with | Some x -> x | None -> raise (FileNotFoundException("File not found.", path))
                f tx file
            )

        member private _.WithDirectoryAt(path: string, f: SqliteConnection -> SoloDBDirectoryHeader -> 'T) =
            connection.WithTransaction(fun tx ->
                let dir = match tryGetDir tx path with | Some x -> x | None -> raise (DirectoryNotFoundException("Directory not found at: " + path))
                f tx dir
            )

        member this.SetFileModificationDate(path, date) =
            this.WithFileAt(path, fun tx file ->
                setFileModifiedById tx file.Id date
            )

        member this.SetFileCreationDate(path, date) =
            this.WithFileAt(path, fun tx file ->
                setFileCreatedById tx file.Id date
            )

        member this.SetMetadata(file: SoloDBFileHeader, key, value) =
            connection.WithTransaction(fun tx ->
                setSoloDBFileMetadata tx file key value
            )

        member this.SetMetadata(path: string, key, value) =
            this.WithFileAt(path, fun tx file ->
                setSoloDBFileMetadata tx file key value
            )

        member this.DeleteMetadata(file: SoloDBFileHeader, key) =
            connection.WithTransaction(fun tx ->
                deleteSoloDBFileMetadata tx file key
            )

        member this.DeleteMetadata(path: string, key) =
            this.WithFileAt(path, fun tx file ->
                deleteSoloDBFileMetadata tx file key
            )

        member this.SetDirectoryMetadata(dir: SoloDBDirectoryHeader, key, value) =
            connection.WithTransaction(fun tx ->
                setDirMetadata tx dir key value
            )

        member this.SetDirectoryMetadata(path: string, key, value) =
            this.WithDirectoryAt(path, fun tx dir ->
                setDirMetadata tx dir key value
            )

        member this.DeleteDirectoryMetadata(dir: SoloDBDirectoryHeader, key) =
            connection.WithTransaction(fun tx ->
                deleteDirMetadata tx dir key
            )

        member this.DeleteDirectoryMetadata(path: string, key) =
            this.WithDirectoryAt(path, fun tx dir ->
                deleteDirMetadata tx dir key
            )

        member this.Delete(file: SoloDBFileHeader) =
            connection.WithTransaction(fun tx ->
                deleteFile tx file |> ignore
            )

        member this.Delete(dir: SoloDBDirectoryHeader) =
            connection.WithTransaction(fun tx ->
                deleteDirectory tx dir
            )

        member this.DeleteFileAt(path) =
            connection.WithTransaction(fun tx ->
                match tryGetFileAt tx path with
                | None -> false
                | Some file -> deleteFile tx file
            )

        member this.DeleteDirAt(path) =
            connection.WithTransaction(fun tx ->
                deleteDirectoryAt tx path
            )

        member this.ListFilesAt(path) =
            use db = connection.Get()
            listFilesAt db path

        member this.ListDirectoriesAt(path) =
            use db = connection.Get()
            listDirectoriesAt db path

        member this.ListFilesAtPaginated(path, sortBy, sortDir, limit, offset) =
            use db = connection.Get()
            listFilesAtPaginated db path sortBy sortDir limit offset

        member this.ListDirectoriesAtPaginated(path, sortBy, sortDir, limit, offset) =
            use db = connection.Get()
            listDirectoriesAtPaginated db path sortBy sortDir limit offset

        member this.ListEntriesAtPaginated(path, sortBy, sortDir, limit, offset) =
            use db = connection.Get()
            listEntriesAtPaginated db path sortBy sortDir limit offset

        member this.RecursiveListEntriesAt(path) =
            use db = connection.Get()
            recursiveListAllEntriesInDirectory db path
            |> ResizeArray
            :> IList<SoloDBEntryHeader>

        member this.RecursiveListEntriesAtLazy(path) = seq {
            use db = connection.Get()
            yield! recursiveListAllEntriesInDirectory db path
        }

        member this.MoveFile(from, toPath) =
            let struct (toDirPath, fileName) = getPathAndName toPath
            connection.WithTransaction(fun db ->
                let file = match tryGetFileAt db from with | Some f -> f | None -> raise (FileNotFoundException("File not found.", from))
                let dir = getOrCreateDirectoryAt db toDirPath
                moveFile db file dir fileName
            )

        member this.MoveReplaceFile(from, toPath) =
            let struct (toDirPath, fileName) = getPathAndName toPath
            connection.WithTransaction(fun db ->
                let file = match tryGetFileAt db from with | Some f -> f | None -> raise (FileNotFoundException("File not found.", from))
                let dir = getOrCreateDirectoryAt db toDirPath
                match tryGetFileAt db toPath with
                | Some replacedFile ->
                    if replacedFile.FullPath <> file.FullPath then
                        deleteFile db replacedFile |> ignore
                | None -> ()
                moveFile db file dir fileName
            )

        member this.MoveDirectory(from, toPath) =
            let struct (toDirPath, fileName) = getPathAndName toPath
            connection.WithTransaction(fun db ->
                let dirToMove = match tryGetDir db from with | Some f -> f | None -> raise (DirectoryNotFoundException("From directory not found at: " + from))
                let toPathParent = match tryGetDir db toDirPath with | Some f -> f | None -> raise (DirectoryNotFoundException("To directory not found at: " + toDirPath))
                moveDirectoryMustBeWithinTransaction db dirToMove toPathParent fileName
            )

        member _.CopyFile(fromPath, toPath, copyMetadata) = copyFile connection fromPath toPath copyMetadata
        member _.CopyFileAsync(fromPath, toPath, copyMetadata) = copyFileAsync connection fromPath toPath copyMetadata
        member _.CopyReplaceFile(fromPath, toPath, copyMetadata) = copyReplaceFile connection fromPath toPath copyMetadata
        member _.CopyReplaceFileAsync(fromPath, toPath, copyMetadata) = copyReplaceFileAsync connection fromPath toPath copyMetadata
        member _.CopyDirectory(fromPath, toPath, recursive, copyMetadata) = copyDirectory connection fromPath toPath recursive copyMetadata
        member _.CopyDirectoryAsync(fromPath, toPath, recursive, copyMetadata) = copyDirectoryAsync connection fromPath toPath recursive copyMetadata
        member _.CopyReplaceDirectory(fromPath, toPath, recursive, copyMetadata) = copyReplaceDirectory connection fromPath toPath recursive copyMetadata
        member _.CopyReplaceDirectoryAsync(fromPath, toPath, recursive, copyMetadata) = copyReplaceDirectoryAsync connection fromPath toPath recursive copyMetadata

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

        interface IFileSystemCopyInternal with
            member this.CopyFile(fromPath, toPath, copyMetadata) = this.CopyFile(fromPath, toPath, copyMetadata)
            member this.CopyFileAsync(fromPath, toPath, copyMetadata) = this.CopyFileAsync(fromPath, toPath, copyMetadata)
            member this.CopyReplaceFile(fromPath, toPath, copyMetadata) = this.CopyReplaceFile(fromPath, toPath, copyMetadata)
            member this.CopyReplaceFileAsync(fromPath, toPath, copyMetadata) = this.CopyReplaceFileAsync(fromPath, toPath, copyMetadata)
            member this.CopyDirectory(fromPath, toPath, recursive, copyMetadata) = this.CopyDirectory(fromPath, toPath, recursive, copyMetadata)
            member this.CopyDirectoryAsync(fromPath, toPath, recursive, copyMetadata) = this.CopyDirectoryAsync(fromPath, toPath, recursive, copyMetadata)
            member this.CopyReplaceDirectory(fromPath, toPath, recursive, copyMetadata) = this.CopyReplaceDirectory(fromPath, toPath, recursive, copyMetadata)
            member this.CopyReplaceDirectoryAsync(fromPath, toPath, recursive, copyMetadata) = this.CopyReplaceDirectoryAsync(fromPath, toPath, recursive, copyMetadata)
