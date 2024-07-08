module ReadMeTests

open System
open System.Text
open System.IO
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDatabase
open SoloDatabase.FileStorage
open Types
open TestUtils

[<CLIMutable>]
type MyType = { Id: SqlId; Name: string; Data: string }

[<TestClass>]
type ReadMeTests() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    let mutable fs: FileSystem = Unchecked.defaultof<FileSystem>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.Instantiate dbSource
        fs <- db.FileSystem
    
    [<TestCleanup>]
    member this.Cleanup() =
        db.Dispose()

    [<TestMethod>]
    member this.Instantiate() =
        let onDiskDB = SoloDB.Instantiate("./database.db");
        let inMemoryDB = SoloDB.Instantiate("memory:database-name");
        ()

    [<TestMethod>]
    member this.GetCollections() =
        let myCollection = db.GetCollection<User>();
        let untypedCollection = db.GetUntypedCollection("User");
        ()

    [<TestMethod>]
    member this.ExistsCollections() =
        let exists = db.CollectionExists<User>();
        assertTrue (not exists)

    [<TestMethod>]
    member this.DropCollections() =
        let myCollection = db.GetCollection<User>(); // Create it.

        db.DropCollection<User>();
        let exists = db.DropCollectionIfExists<User>();
        let ex = Assert.ThrowsException(fun () -> db.DropCollection("User"))
        db.DropCollectionIfExists("User") |> not |> assertTrue
        ()

    [<TestMethod>]
    member this.Transaction() =
        let ex = Assert.ThrowsException(fun () -> 
            db.WithTransaction(fun tx -> 
                let collection = tx.GetCollection<uint64>();
                // Perform operations within the transaction.
                let id = collection.Insert(420UL)
                failwithf "Simulate a fail."
            )
        )

        db.CollectionExists<uint64>() |> not |> assertTrue
        ()

    [<TestMethod>]
    member this.Backup() =
        // To check if it compiles.
        if false then
            db.BackupTo(db);
            let exec = db.VacuumTo("./path/to/backup.db")
            ()

        ()

    [<TestMethod>]
    member this.Optimize() =
        let exec = db.Optimize()
        ()
                

    [<TestMethod>]
    member this.Example() =
        let collection = db.GetCollection<MyType>()
        
        // Insert a document
        let docId = collection.Insert({ Id = SqlId(0); Name = "Document 1"; Data = "Some data" })
        
        // Or
        
        let data = { Id = SqlId(0); Name = "Document 1"; Data = "Some data" }
        collection.Insert(data) |> ignore
        printfn "%A" data.Id // 2
        
        // Query all documents into a F# list
        let documents = collection.Select().OnAll().ToList()
        
        // Query the Data property, where Name starts with 'Document'
        let documentsData = collection.Select(fun d -> d.Data).Where(fun d -> d.Name.StartsWith "Document").ToList()
        
        let data = {data with  Data = "Updated data"}
        
        // Update a document
        collection.Update(data)
        
        // Delete a document
        let count = collection.DeleteById(data.Id)
        ()