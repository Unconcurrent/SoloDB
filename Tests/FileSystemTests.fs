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
type JsonTests() =
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
        let path = "/abc.txt"
        use ms = new MemoryStream(testFileBytes)
        fs.Upload(path, ms)

        assertEqual (fs.TryGetAt path).IsSome true "Expected None."

