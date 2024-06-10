﻿module Tests

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

let randomUsersToInsert = [|
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
        while File.Exists dbPath do // The file is not released instantly.
            try File.Delete dbPath
            with ex -> Thread.Sleep 150
        

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

        let ids = users.InsertBatch(randomUsersToInsert)

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

    [<TestMethod>]
    member this.InsertBatchTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        users.InsertBatch(randomUsersToInsert) |> ignore

        let id1, getFirstUser = users.SelectWithId().OnAll().Limit(1UL).Offset(0UL).First()
        let id2, getSecondUser = users.SelectWithId().OnAll().Limit(1UL).Offset(1UL).First()

        assertEqual getFirstUser.Username "John" "Name unequal."
        assertEqual getSecondUser.Username "Mihail" "Name unequal."

    [<TestMethod>]
    member this.UpdateLambdaAddSetAtTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let ids = users.InsertBatch(randomUsersToInsert)


        let replaceValue = $"{Random.Shared.NextInt64()}"
        let addTagValue = $"{Random.Shared.NextInt64()}"
        assertEqual (users.Update(
                            fun u -> (u.Data.Tags.Add addTagValue) |+| (u.Data.Tags.SetAt(0, replaceValue))
                     ).WhereId(ids.[0]).Execute()) 1 "No rows affected."

        let getFirstUser = users.GetById ids.[0]
        assertEqual getFirstUser.Data.Tags.[0] replaceValue "Value Tags unchanged."
        assertEqual getFirstUser.Data.Tags.[1] addTagValue "Value Tags unchanged."

    [<TestMethod>]
    member this.UpdateLambdaRemoveAtTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let ids = users.InsertBatch(randomUsersToInsert)


        let replaceValue = $"{Random.Shared.NextInt64()}"
        let addTagValue = $"{Random.Shared.NextInt64()}"
        assertEqual (users.Update(
                            fun u -> (u.Data.Tags.Add addTagValue) |+|(u.Data.Tags.RemoveAt 0)
                     ).WhereId(ids.[0]).Execute()) 1 "No rows affected."

        let firstUser = users.GetById ids.[0]
        assertEqual firstUser.Data.Tags.[0] replaceValue "Value Tags unchanged."
        assertEqual firstUser.Data.Tags.[1] addTagValue "Value Tags unchanged."

    [<TestMethod>]
    member this.UpdateObjTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let ids = users.InsertBatch(randomUsersToInsert)

        let secondUser = users.GetById ids.[1]
        let secondUserMod = {secondUser with LastSeen = DateTimeOffset.Now.Date |> DateTimeOffset}
        let update2 = users.Update(secondUserMod).WhereId(ids.[1]).Execute()
        let secondUserModDB = users.GetById ids.[1]

        assertEqual secondUserMod secondUserModDB "Update(User) failed."

        let newArray = [|"abc"|] in ()
        

[<EntryPoint>]
let main argv =
    let test = SoloDBTesting()
    test.SetDBPath()
    test.UpdateLambdaRemoveAtTest()
    test.ClearTemp()
    0