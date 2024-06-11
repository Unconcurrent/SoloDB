module Tests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDB
open FSharp.Interop.Dynamic

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
let assertEqual<'T when 'T : equality> (a: 'T) (b: 'T) (message: string) = Assert.IsTrue((b = a), message) // F# cannot decide the overload, and also switched the order.

[<TestClass>]
type SoloDBTesting() =
    let dbDir = "./temp/"
    let mutable dbPath = "./temp/test.db"
    [<TestInitialize>]
    member this.SetDBPath() =
        Directory.CreateDirectory dbDir |> ignore
        dbPath <- $"./temp/test{Random.Shared.NextInt64()}.db"

    [<TestCleanup>]
    member this.ClearTemp() =
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools()
        while Directory.EnumerateFiles dbDir |> Seq.isEmpty |> not do // The file is not released instantly.
            try Directory.EnumerateFiles dbDir |> Seq.iter File.Delete
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
    member this.IncrementalIdsTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let ids = users.InsertBatch(randomUsersToInsert)

        let mutable prevId = 0L
        for id in ids do
            Assert.IsTrue (id > prevId)
            prevId <- id

    [<TestMethod>]
    member this.QueryTest1() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let ids = users.InsertBatch(randomUsersToInsert)

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

    [<TestMethod>]
    member this.QueryTest2() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        users.InsertBatch(randomUsersToInsert) |> ignore

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
    member this.InsertBatchSelectTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        users.InsertBatch(randomUsersToInsert) |> ignore

        let id1, getFirstUser = users.SelectWithId().OnAll().Limit(1UL).Offset(0UL).First()
        let id2, getSecondUser = users.SelectWithId().OnAll().Limit(1UL).Offset(1UL).First()

        assertEqual getFirstUser.Username "John" "Name unequal."
        assertEqual getSecondUser.Username "Mihail" "Name unequal."

    [<TestMethod>]
    member this.InsertBatchCountTest() =
        use db = SoloDB.instantiate dbPath
        db.GetCollection<User>().InsertBatch(randomUsersToInsert) |> ignore
        assertEqual (db.GetCollection<User>().Select(fun u -> 1).OnAll().ToList() |> Seq.length) randomUsersToInsert.Length "Length unequal."

    [<TestMethod>]
    member this.UpdateLambdaAddTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let ids = users.InsertBatch(randomUsersToInsert)


        let addTagValue = $"{Random.Shared.NextInt64()}"
        assertEqual (users.Update(
                            fun u -> (u.Data.Tags.Add addTagValue)
                     ).WhereId(ids.[0]).Execute()) 1 "No rows affected."

        let getFirstUser = users.GetById ids.[0]
        assertEqual getFirstUser.Data.Tags.[1] addTagValue "Value Tags unchanged."

    [<TestMethod>]
    member this.UpdateLambdaSetAtTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let ids = users.InsertBatch(randomUsersToInsert)


        let replaceValue = $"{Random.Shared.NextInt64()}"
        assertEqual (users.Update(
                            fun u -> u.Data.Tags.SetAt(0, replaceValue)
                     ).WhereId(ids.[0]).Execute()) 1 "No rows affected."

        let getFirstUser = users.GetById ids.[0]
        assertEqual getFirstUser.Data.Tags.[0] replaceValue "Value Tags unchanged."

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

        let id = users.Insert {
            Username = "John"
            Auth = true
            Banned = false
            FirstSeen = DateTimeOffset.UtcNow.AddYears -15
            LastSeen = DateTimeOffset.UtcNow.AddMinutes -10
            Data = {
                Tags = [|"tag1"; "tag2"; "tag3"|]
            }
        }

        assertEqual (users.Update(
                            fun u -> (u.Data.Tags.RemoveAt (1))
                     ).WhereId(id).Execute()) 1 "No rows affected."

        let firstUser = users.GetById id
        assertEqual firstUser.Data.Tags.[0] "tag1" "Value Tags unchanged."
        assertEqual firstUser.Data.Tags.[1] "tag3" "Value Tags unchanged."

    [<TestMethod>]
    member this.UpdateLambdaReplaceArrayTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let id = users.Insert {
            Username = "John"
            Auth = true
            Banned = false
            FirstSeen = DateTimeOffset.UtcNow.AddYears -15
            LastSeen = DateTimeOffset.UtcNow.AddMinutes -10
            Data = {
                Tags = [|"tag1"; "tag2"; "tag3"|]
            }
        }

        let newTags = [|"abc"; "xyz"|]

        assertEqual (users.Update(fun u -> u.Data.Tags.Set newTags).WhereId(id).Execute()) 1 "Execute with no modification."

        let user = users.GetById id
        assertEqual<string array> user.Data.Tags newTags "Array set failed."

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

    [<TestMethod>]
    member this.SelectUntypedTest() =
        use db = SoloDB.instantiate dbPath
        let users = db.GetCollection<User>()

        let ids = users.InsertBatch(randomUsersToInsert)
        let id = ids[0]

        let users = users.SelectUntyped(fun u -> (u?Username, true)).Where(fun u -> u?Username > "A").ToList()
        let usersStr = sprintf "%A" users
        let expected = "[[\"John\"; 1L]; [\"Mihail\"; 1L]; [\"Vanya\"; 1L]; [\"Givany\"; 1L]]"
        printfn "%s" usersStr
        assertEqual usersStr expected "SelectUntypedTest failed."

[<EntryPoint>]
let main argv =
    let test = SoloDBTesting()
    test.SetDBPath()
    test.SelectUntypedTest()
    test.ClearTemp()
    0