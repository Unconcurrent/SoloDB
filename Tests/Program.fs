module Tests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDB

type UserData = {
    Tags: string array
}

[<CLIMutable>]
type User = {
    Username: string
    Auth: bool
    Banned: bool
    FirstSeen: DateTimeOffset
    LastSeen: DateTimeOffset
    Data: UserData
}


let assertEqual<'T> (a: 'T) (b: 'T) (message: string) = Assert.AreEqual(a, b, message) // F# cannot decide the overload.

[<TestClass>]
type SoloDBTesting() =
    let mutable dbPath = "./test.db"
    [<TestInitialize>]
    member this.SetDBPath() =
        dbPath <- $"./test{Random.Shared.NextInt64()}.db"

    [<TestCleanup>]
    member this.ClearTemp() =
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools()
        let dbPath = dbPath
        Task.Run<unit>(fun () -> task {
            while File.Exists dbPath do // The file is not released instantly.
                try
                    File.Delete dbPath
                with ex -> do! Task.Delay 150
        }) |> ignore
        

    [<TestMethod>]
    member this.InsertAndGetByIdEqual() =    
        let testUser = {
            Username = "Bob"
            Auth = true
            Banned = false
            FirstSeen = DateTimeOffset.MinValue
            // The DB stores only the miliseconds, but the DateTimeOffset can have more precision.
            LastSeen = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
            Data = {
                Tags = [|"test1234"|]
            }
        }
    
    
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()
        let id = users.Insert testUser
        let userGetById = users.GetById id
        printfn "%A" testUser
        printfn "%A" userGetById
        assertEqual userGetById testUser "The inserted user is different."

    [<TestMethod>]
    member this.InsertAndGetByIdUnequal() =    
        let user1 = {
            Username = "John"
            Auth = true
            Banned = false
            FirstSeen = DateTimeOffset.MinValue
            LastSeen = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
            Data = {
                Tags = [|"tag1"|]
            }
        }

        let user2 = {
            Username = "James"
            Auth = true
            Banned = true
            FirstSeen = DateTimeOffset.MinValue
            LastSeen = DateTimeOffset.Parse("10/10/2020")
            Data = {
                Tags = [||]
            }
        }


        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let id1 = users.Insert user1
        let id2 = users.Insert user2

        let user1 = users.GetById id1
        let user2 = users.GetById id2

        printfn "%A" user1
        printfn "%A" user2
        Assert.AreNotEqual(user1, user2, "The inserted user are equal.")

    [<TestMethod>]
    member this.QueryTest() =    
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let usersToInsert = [|
            {
                Username = "John"
                Auth = true
                Banned = false
                FirstSeen = DateTimeOffset.UtcNow.AddYears -15
                LastSeen = DateTimeOffset.UtcNow.AddMinutes -10
                Data = {
                    Tags = [|"tag1-A"|]
                }
            };
            {
                Username = "Mihail"
                Auth = true
                Banned = false
                FirstSeen = DateTimeOffset.UtcNow.AddYears -10
                LastSeen = DateTimeOffset.UtcNow.AddMinutes -8
                Data = {
                    Tags = [|"tag2"|]
                }
            };
            {
                Username = "Vanya"
                Auth = true
                Banned = true
                FirstSeen = DateTimeOffset.UtcNow.AddYears -10
                LastSeen = DateTimeOffset.UtcNow.AddMinutes -5
                Data = {
                    Tags = [|"tag1-B"|]
                }
            };
            {
                Username = "Givany"
                Auth = false
                Banned = false
                FirstSeen = DateTimeOffset.UtcNow.AddYears -10
                LastSeen = DateTimeOffset.UtcNow.AddMinutes -2
                Data = {
                    Tags = [|"tag1-C"|]
                }
            }
        |]

        let ids = users.InsertBatch(usersToInsert)
        let mutable prevId = 0L
        for id in ids do
            Assert.IsTrue (id > prevId)
            prevId <- id

        let selectedData = users
                            .Select(fun u -> (u.Username, u.Data.Tags[0]))
                            .Where(fun u -> u.Data.Tags[0].Like "tag1%")
                            .OrderByDesc(fun u -> u.LastSeen)
                            .Offset(1UL)
                            .Limit(2UL)
                            .ToList()

        let selectedDataText = sprintf "%A" selectedData
        let expected = "[(\"Vanya\", \"tag1-B\"); (\"John\", \"tag1-A\")]" 
        printfn "%s" selectedDataText
        printfn "%s" expected
        assertEqual selectedDataText expected "The query is wrong."


        let selectedData = users
                            .Select(fun u -> (u.Data.Tags, u.Auth, true, u.Username))
                            .Where(fun u -> u.Data.Tags[0].Like "tag2%")
                            .OrderByDesc(fun u -> u.LastSeen)
                            .Limit(3UL)
                            .ToList()

        let selectedDataText = sprintf "%A" selectedData
        let expected = "[([|\"tag2\"|], true, true, \"Mihail\")]"
        printfn "%s" selectedDataText
        printfn "%s" expected
        assertEqual selectedDataText expected "The query is wrong."

        
        (*let newArray = [|"abc"|]
        let newValue = $"{Random.Shared.NextInt64()}"
        let update = users.Update(fun u -> u.Data.Tags.Add newValue).OnAll().Execute()
        let update2 = users.Update(userGetById).WhereId(id).Execute()*)
        

