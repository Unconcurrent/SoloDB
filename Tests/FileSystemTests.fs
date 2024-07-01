module FileSystemTests

open System
open System.Text
open System.IO
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDB
open JsonFunctions
open Types
open TestUtils
open FileStorage

let testFileBytes = "Hello this is some random data." |> Encoding.UTF8.GetBytes

[<TestClass>]
type FileSystemTests() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    let mutable fs: FileSystem = Unchecked.defaultof<FileSystem>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.instantiate dbSource
        fs <- db.FileSystem
    
    [<TestCleanup>]
    member this.Cleanup() =
        db.Dispose()

    [<TestMethod>]
    member this.FileUpload() =
        use ms = new MemoryStream(testFileBytes)
        fs.Upload("/abc.txt", ms)
        ()

    [<TestMethod>]
    member this.FileUploadDownload() =
        let path = "/abc.txt"
        use ms = new MemoryStream(testFileBytes)
        fs.Upload(path, ms)

        use tempMs = new MemoryStream()
        fs.Download(path, tempMs)
        assertEqual (tempMs.ToArray()) testFileBytes "File corrupted on storage."

    [<TestMethod>]
    member this.FileLen() =
        let path = "/abc.txt"
        use ms = new MemoryStream(testFileBytes)
        fs.Upload(path, ms)

        let fh = fs.GetAt path
        assertEqual fh.Length testFileBytes.Length "File len corrupted on storage."

    [<TestMethod>]
    member this.FileTryGetAtNone() =
        let path = "/abc.txt"
        assertEqual (fs.TryGetAt path) None "Expected None."

    [<TestMethod>]
    member this.FileTryGetAtSome() =
        let path = "/a/b/c/d/e/f/g/h/i/a/b/c/d/e/f/g/h/i/a/b/c/d/e/f/g/h/i/a/b/c/d/e/f/g/h/i/a/b/c/d/e/f/g/h/i/a/b/c/d/e/f/g/h/i/abc.txt"
        use ms = new MemoryStream(testFileBytes)
        fs.Upload(path, ms)

        assertEqual (fs.TryGetAt path).IsSome true "Expected None."

    [<TestMethod>]
    member this.FileReadWriteSparse() =
        let path = "/abc.txt"
        fs.WriteAt(path, FileStorage.chunkSize + 1L, testFileBytes)

        let bytes = fs.ReadAt(path, 0, FileStorage.chunkSize + 5L |> int)

        assertEqual bytes.LongLength (FileStorage.chunkSize + 5L) "Did not read as much as requested."

        let endOfArray = bytes[int FileStorage.chunkSize + 1..]
        let startOfData = testFileBytes[..3]

        assertEqual endOfArray startOfData "Sparse Write and Read failed."

    [<TestMethod>]
    member this.FileMetadata() =
        let path = "/xyz.txt"
        let file = fs.GetOrCreateAt path
        use ms = new MemoryStream(testFileBytes)
        fs.Upload(path, ms)
        fs.SetMetadata(file, "Owner", "John")
        fs.SetMetadata(file, "Tags", "One")

        let file2 = fs.GetAt path

        assertEqual file2.Metadata.["Owner"] "John" "Metadata not set."

        fs.DeleteMetadata(file, "Owner")

        let file3 = fs.GetAt path

        assertEqual file3.Metadata.["Tags"] "One" "Metadata not set."
        assertEqual file3.Metadata.Count 1 "Metadata not deleted."

    [<TestMethod>]
    member this.DirMetadata() =
        let path = "/alpha"
        let dir = fs.GetOrCreateDirAt path
        use ms = new MemoryStream(testFileBytes)

        fs.SetDirectoryMetadata(dir, "Owner", "John")
        fs.SetDirectoryMetadata(dir, "Tags", "One")

        let dir2 = fs.GetDirAt path

        assertEqual dir2.Metadata.["Owner"] "John" "Metadata not set."

        fs.DeleteDirectoryMetadata(dir, "Owner")

        let dir3 = fs.GetDirAt path

        assertEqual dir3.Metadata.["Tags"] "One" "Metadata not set."
        assertEqual dir3.Metadata.Count 1 "Metadata not deleted."
