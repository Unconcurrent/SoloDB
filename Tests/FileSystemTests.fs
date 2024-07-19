module FileSystemTests

open System
open System.Text
open System.IO
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDatabase
open SoloDatabase.Types
open SoloDatabase.FileStorage
open SoloDatabase.Utils
open TestUtils
open System.Security.Cryptography
open System.Globalization

let testFileBytes = "Hello this is some random data." |> Encoding.UTF8.GetBytes
let testFileBytes2 = "This data is different from the previous one." |> Encoding.UTF8.GetBytes

[<TestClass>]
type FileSystemTests() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    let mutable fs: FileSystem = Unchecked.defaultof<FileSystem>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- new SoloDB (dbSource)
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
    member this.FileUploadReplaceDownloadTransation() =
        let path = "/abc.txt"
        use ms = new MemoryStream(testFileBytes)
        fs.Upload(path, ms)

        use ms = new MemoryStream(testFileBytes2)

        fs.ReplaceAsyncWithinTransaction(path, ms).GetAwaiter().GetResult()

        use tempMs = new MemoryStream()
        fs.Download(path, tempMs)
        assertEqual (tempMs.ToArray()) testFileBytes2 "File corrupted on storage."

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
    member this.ManyFiles() =
        // db.Dispose(); db <- new SoloDB("./temp.solodb"); fs <- db.FileSystem

        for d in 1..50 do
            for f in 1..30 do
                let path = $"/directory{d}/file{f}.txt"
                fs.WriteAt(path, 0, testFileBytes)
                for m in 1..5 do
                    fs.SetMetadata(path, $"Rand{Random.Shared.NextInt64()}", Random.Shared.NextInt64().ToString())

        ()

    [<TestMethod>]
    member this.FileMetadata() =
        let path = "/alpha2/xyz.txt"

        let file = fs.GetOrCreateAt path
        use ms = new MemoryStream(testFileBytes)
        fs.Upload(path, ms)

        fs.SetMetadata(file, "Owner", "Artur")
        fs.SetMetadata(file, "Owner", "John")
        fs.SetMetadata(file, "Tags", "One")

        let file2 = fs.GetAt path

        assertEqual file2.Metadata.["Owner"] "John" "Metadata not set."
        assertEqual file2.Metadata.["Tags"] "One" "Metadata not set."

        fs.DeleteMetadata(file, "Owner")

        let file3 = fs.GetAt path

        assertEqual file3.Metadata.["Tags"] "One" "Metadata not set."
        assertEqual file3.Metadata.Count 1 "Metadata not deleted."

    [<TestMethod>]
    member this.DirMetadata() =        
        let path = "/alpha"
        let dir = fs.GetOrCreateDirAt path
        use ms = new MemoryStream(testFileBytes)

        fs.SetDirectoryMetadata(dir, "Owner", "Artur")
        fs.SetDirectoryMetadata(dir, "Owner", "John")
        fs.SetDirectoryMetadata(dir, "Tags", "One")

        let dir2 = fs.GetDirAt path

        assertEqual dir2.Metadata.["Owner"] "John" "Metadata not set."
        assertEqual dir2.Metadata.["Tags"] "One" "Metadata not set."

        fs.DeleteDirectoryMetadata(dir, "Owner")

        let dir3 = fs.GetDirAt path

        assertEqual dir3.Metadata.["Tags"] "One" "Metadata not set."
        assertEqual dir3.Metadata.Count 1 "Metadata not deleted."

    [<TestMethod>]
    member this.FileOpenWriteRead() =
        let path = "/alpha/binary.file"

        use fileStream = fs.OpenOrCreateAt path
        fileStream.Write testFileBytes
        fileStream.Position <- 0
        let readArray = Span<byte>(Array.zeroCreate<byte> testFileBytes.Length)

        fileStream.ReadExactly readArray

        assertEqual (readArray.SequenceEqual testFileBytes) true "Write then Read not equal."

    [<TestMethod>]
    member this.FileOpenWriteReadMultipleChunks() =
        let path = "/alpha/binary.file"
        let testFileBytes = Array.zeroCreate<byte> (FileStorage.chunkSize * 10L |> int)

        Random.Shared.NextBytes testFileBytes

        use fileStream = fs.OpenOrCreateAt path
        fileStream.Write testFileBytes
        fileStream.Position <- 0
        let readArray = Span<byte>(Array.zeroCreate<byte> testFileBytes.Length)

        fileStream.ReadExactly readArray

        assertEqual (readArray.SequenceEqual testFileBytes) true "Write then Read not equal."

    [<TestMethod>]
    member this.FileHash() =
        let path = "/alpha/binary.file"
        use fileStream = fs.OpenOrCreateAt path
        fileStream.Write testFileBytes
        fileStream.Flush()

        let dataHash = SHA1.HashData testFileBytes
        let header = fs.GetAt path
        assertEqual dataHash header.Hash "Hash unequal."

    [<TestMethod>]
    member this.FileHashBig() =
        let path = "/alpha/binary.file"
        let testFileBytes = Array.zeroCreate<byte> (FileStorage.chunkSize * 10L |> int)

        use fileStream = fs.OpenOrCreateAt path
        fileStream.Write testFileBytes
        fileStream.Flush()

        let dataHash = SHA1.HashData testFileBytes
        let header = fs.GetAt path
        assertEqual dataHash header.Hash "Hash unequal."

    [<TestMethod>]
    member this.FileHashSparse() =
        let path = "/alpha/binary.file"

        fs.WriteAt(path, chunkSize * 10L, testFileBytes)

        let dataHash = SHA1.HashData ([|Array.zeroCreate<byte>(int chunkSize * 10); testFileBytes|] |> Array.concat)
        let header = fs.GetAt path
        assertEqual dataHash header.Hash "Hash unequal."

    [<TestMethod>]
    member this.GetFileByHash() =
        let path = "/alpha/binary.file"
        do
            use fileStream = fs.OpenOrCreateAt path
            fileStream.Write testFileBytes


        let header = fs.GetAt path
        let header2 = fs.GetFileByHash header.Hash
        assertEqual header.Name header2.Name "Incorrect file header."
        assertEqual header.Length header2.Length "Incorrect file header."
        assertEqual header.Hash header2.Hash "Incorrect file header."
        assertEqual header.DirectoryId header2.DirectoryId "Incorrect file header."

        let inexistentHash = shaHash "ABCBSH"
        let inexistentHashStr = Convert.ToHexString inexistentHash |> _.ToLower(CultureInfo.InvariantCulture)
        try
            let inexistentHeader = fs.GetFileByHash inexistentHash 
            failwithf "Got inexistent file from hash."
        with
        | :? FileNotFoundException as fnf ->
            assertEqual fnf.FileName inexistentHashStr "Different hash."
            ()

    [<TestMethod>]
    member this.FileDelete() =
        let path = "/abc.txt"
        use ms = new MemoryStream(testFileBytes)
        fs.Upload(path, ms)

        assertTrue (fs.DeleteFileAt path) 
        assertTrue ((fs.DeleteFileAt path) = false)

    [<TestMethod>]
    member this.DirectoryDelete() =
        let path = "/abc/xyz"
        let dir = fs.GetOrCreateDirAt path

        assertTrue (fs.DeleteDirAt path) 
        assertTrue ((fs.DeleteDirAt path) = false)

    [<TestMethod>]
    member this.ListFiles() =
        let path = "/abc/"
        let dir = fs.GetOrCreateDirAt path

        for i in 1..10 do
            fs.GetOrCreateAt $"{path}{i}.txt" |> ignore

        for i in 1..20 do
            fs.GetOrCreateDirAt $"{path}{i}" |> ignore

        let files = fs.ListFilesAt path |> Seq.toList
        assertTrue (files.Length = [|1..10|].Length)

        let uniqueFileNames = files |> List.distinctBy(fun f -> f.Name)

        assertTrue (uniqueFileNames.Length = files.Length)

    [<TestMethod>]
    member this.ListDirs() =
        let path = "/abc/"
        let dir = fs.GetOrCreateDirAt path

        for i in 1..10 do
            fs.GetOrCreateAt $"{path}{i}.txt" |> ignore

        for i in 1..20 do
            fs.GetOrCreateDirAt $"{path}{i}" |> ignore

        let directories = fs.ListDirectoriesAt path |> Seq.toList
        assertTrue (directories.Length = [|1..20|].Length)

        let uniqueDirectoriesNames = directories |> List.distinctBy(fun d -> d.Name)

        assertTrue (uniqueDirectoriesNames.Length = directories.Length)



