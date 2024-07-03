module StandardTests2

#nowarn "3391" // Implicit on SqlId

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Microsoft.VisualStudio.TestTools.UnitTesting
open FSharp.Interop.Dynamic
open SoloDB
open SoloDBTypes
open Types
open TestUtils
open SoloDBExtensions

[<TestClass>]
type SoloDBStandard2Testing() =         
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>

    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.Instantiate dbSource

    [<TestCleanup>]
    member this.Cleanup() =
        db.Dispose()

    [<TestMethod>]
    member this.AnyTrue() =
        let testUser = {
            Username = "Alice"
            Auth = true
            Banned = false
            FirstSeen = DateTimeOffset.MinValue
            LastSeen = DateTimeOffset.UtcNow
            Data = { Tags = [|"example"|] }
        }
    
        let users = db.GetCollection<User>()
        users.Insert testUser |> ignore
        let exists = users.Any(fun u -> u.Username = "Alice")
        assertEqual exists true "The Any function should return true for username Alice."

    [<TestMethod>]
    member this.AnyFalse() =        
        let users = db.GetCollection<User>()
        let exists = users.Any(fun u -> u.Username = "NonExistent")
        assertEqual exists false "The Any function should return false for a non-existent username."

    [<TestMethod>]
    member this.CountAllEmpty() =        
        let users = db.GetCollection<User>()
        let count = users.CountAll()
        assertEqual count 0L "The count should be zero for an empty table."

    [<TestMethod>]
    member this.CountAllWithMultipleInserts() =
        let testUsers = [
            { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
            { Username = "Bob"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
        ]
    
        let users = db.GetCollection<User>()
        users.InsertBatch testUsers |> ignore
        let count = users.CountAll()
        assertEqual count 2L "The count should be two after inserting two users."

    [<TestMethod>]
    member this.CountWhereMultipleConditions() =
        let testUsers = [
            { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
            { Username = "Mark"; Auth = false; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example5"|] } }
            { Username = "Mark2"; Auth = false; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example5"|] } }
            { Username = "Mark3"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example5"|] } }
            { Username = "Bob"; Auth = true; Banned = true; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
        ]
    
        let users = db.GetCollection<User>()
        users.InsertBatch testUsers |> ignore
        let count = users.CountWhere(fun u -> u.Auth && not u.Banned)
        assertEqual count 2L "The count should be one where Auth is true and Banned is false."

    [<TestMethod>]
    member this.CountWithZeroConditionMatch() =
        let testUsers = [
            { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
            { Username = "Bob"; Auth = true; Banned = true; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
        ]
    
        let users = db.GetCollection<User>()
        users.InsertBatch testUsers |> ignore
        let count = users.CountWhere(fun u -> u.Auth && u.Banned && u.Username = "Charlie")
        assertEqual count 0L "The count should be zero where no user matches the condition."

    [<TestMethod>]
    member this.CountWhereMultipleUsersMatch() =
        let testUsers = [
            { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
            { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
        ]
    
        let users = db.GetCollection<User>()
        users.InsertBatch testUsers |> ignore
        let count = users.CountWhere(fun u -> u.Username = "Alice")
        assertEqual count 2L "The count should be two where username is Alice."

    [<TestMethod>]
    member this.AnyWithMultipleConditionsTrue() =
        let testUsers = [
            { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
            { Username = "Bob"; Auth = true; Banned = true; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
        ]
    
        let users = db.GetCollection<User>()
        users.InsertBatch testUsers |> ignore
        let exists = users.Any(fun u -> u.Auth && not u.Banned)
        assertEqual exists true "The Any function should return true where Auth is true and Banned is false."

    [<TestMethod>]
    member this.AnyWithMultipleConditionsFalse() =
        let testUsers = [
            { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
            { Username = "Bob"; Auth = true; Banned = true; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
        ]
    
        let users = db.GetCollection<User>()
        users.InsertBatch testUsers |> ignore
        let exists = users.Any(fun u -> u.Auth && u.Banned && u.Username = "Charlie")
        assertEqual exists false "The Any function should return false where no user matches the condition."

    [<TestMethod>]
    member this.CountWhereWithLimitZero() =        
        let users = db.GetCollection<User>()
        let count = users.CountWhere((fun u -> u.Username = "NonExistent"), 1UL)
        assertEqual count 0L "The count where username is NonExistent with limit 1 should be zero."
        
    [<TestMethod>]
    member this.UpdateUser() =        
        let users = db.GetCollection<User>()
        let testUser = randomUsersToInsert.[0]
        let id = users.Insert testUser
        let updatedUser = { testUser with Username = "UpdatedName" }
        users.Replace(updatedUser).WhereId(id).Execute() |> ignore
        let userGetById = users.GetById id
        assertEqual userGetById updatedUser "The updated user is different."

    [<TestMethod>]
    member this.DeleteUser() =        
        let users = db.GetCollection<User>()
        let testUser = randomUsersToInsert.[0]
        let id = users.Insert testUser
        assertEqual (users.DeleteById id) 1 "No row deleted."
        let userGetById = users.TryGetById id
        assertEqual userGetById None "The user should be deleted."

    [<TestMethod>]
    member this.CountUsers() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let count = users.CountAll()
        assertEqual count (int64 randomUsersToInsert.Length) "The count of users is incorrect."

    [<TestMethod>]
    member this.LimitUsers() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let limitedUsers = users.Select().OnAll().Limit(2UL).ToList()
        assertEqual (limitedUsers.Length) 2 "The limit query returned incorrect number of users."

    [<TestMethod>]
    member this.OffsetUsers() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let offsetUsers = users.Select().OnAll().Offset(2UL).ToList()
        assertEqual (offsetUsers.Length) (randomUsersToInsert.Length - 2) "The offset query returned incorrect number of users."

    [<TestMethod>]
    member this.OrderByAscUsers() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let orderedUsers = users.Select(fun u -> u.Username).OnAll().OrderByAsc(fun u -> u.Username).ToList()
        let sortedUsernames = randomUsersToInsert |> Array.map (fun u -> u.Username) |> Array.sort
        assertEqual (orderedUsers) (Array.toList sortedUsernames) "The ascending order query returned incorrect order of users."

    [<TestMethod>]
    member this.OrderByDescUsers() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let orderedUsers = users.Select(fun u -> u.Username).OnAll().OrderByDesc(fun u -> u.Username).ToList()
        let sortedUsernames = randomUsersToInsert |> Array.map (fun u -> u.Username) |> Array.sortDescending
        assertEqual (orderedUsers) (Array.toList sortedUsernames) "The descending order query returned incorrect order of users."
    
    [<TestMethod>]
    member this.CountWhereUsers() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let countAuthUsers = users.CountWhere(fun u -> u.Auth)
        let expectedCount = randomUsersToInsert |> Array.filter (fun u -> u.Auth) |> Array.length |> int64
        assertEqual countAuthUsers expectedCount "The count where query returned incorrect count."

    [<TestMethod>]
    member this.AnyUsers() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let anyBannedUsers = users.Any(fun u -> u.Banned)
        let expectedAny = randomUsersToInsert |> Array.exists (fun u -> u.Banned)
        assertEqual anyBannedUsers expectedAny "The any query returned incorrect result."

    [<TestMethod>]
    member this.UpdateWhere() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        users.Update(fun u -> u.Banned.Set true).Where(fun u -> u.Username = "John").Execute() |> ignore
        let bannedJohn = users.Select().Where(fun u -> u.Username = "John").First()
        assertEqual bannedJohn.Banned true "The update where query did not update the correct user."

    [<TestMethod>]
    member this.SelectWithId() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let usersWithId = users.SelectWithId().OnAll().ToList()
        assertEqual (usersWithId.Length) (randomUsersToInsert.Length) "The select with Id query returned incorrect number of users."

    [<TestMethod>]
    member this.GetNonExistentUser() =        
        let users = db.GetCollection<User>()
        let user = users.TryGetById 99999L
        assertEqual user None "The query for a non-existent user returned a result."

    [<TestMethod>]
    member this.InsertDuplicateUser() =        
        let users = db.GetCollection<User>()
        let testUser = randomUsersToInsert.[0]
        let id1 = users.Insert testUser
        let id2 = users.Insert testUser
        assertEqual (id1 <> id2) true "The insertion of a duplicate user did not create a new ID."


    [<TestMethod>]
    member this.InsertLargeNumberOfUsers() =        
        let users = db.GetCollection<User>()
        let largeBatch = [| for i in 1 .. 100000 -> { Username = $"User{i}"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.UtcNow.AddDays(-i); LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [| $"tag{i}" |] } } |]
        let ids = users.InsertBatch largeBatch
        assertEqual (ids.Count) (largeBatch.Length) "Not all users were inserted in the large batch."

    [<TestMethod>]
    member this.UpdateNonExistentUser() =        
        let users = db.GetCollection<User>()
        try
            users.Update(fun u -> u.Banned.Set true).Where(fun u -> u.Username = "NonExistentUser").Execute() |> ignore
            assertEqual true false "Updating a non-existent user should have failed."
        with ex -> ()

    [<TestMethod>]
    member this.DeleteWhileReading() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let task1 = Task.Run(fun () -> users.Select().OnAll().ToList())
        let task2 = Task.Run(fun () -> users.Update(fun u -> u.Banned.Set true).Where(fun u -> u.Username = "John").Execute() |> ignore)
        Task.WaitAll(task1, task2)
        assertEqual (task1.Status) TaskStatus.RanToCompletion "Reading users should complete successfully even while updating."
        assertEqual (task2.Status) TaskStatus.RanToCompletion "Updating users should complete successfully even while reading."

    [<TestMethod>]
    member this.ConcurrentInserts() =        
        let users = db.GetCollection<User>()
        let insertBatch () =
            let batch = [| for i in 1 .. 100 -> { Username = $"User{i}"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.UtcNow.AddDays(-i); LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [| $"tag{i}" |] } } |]
            users.InsertBatch batch |> ignore
        let tasks = [| for _ in 1 .. 100 -> Task.Run insertBatch |]
        Task.WaitAll(tasks)
        let count = users.CountAll()
        assertEqual (count) 10000L "Concurrent inserts did not result in the correct number of users."
   
    [<TestMethod>]
    member this.UpdateLargeNumberOfUsers() =        
        let users = db.GetCollection<User>()
        let largeBatch = [| for i in 1 .. 1000 -> { Username = $"User{i}"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.UtcNow.AddDays(-i); LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [| $"tag{i}" |] } } |]
        let ids = users.InsertBatch largeBatch
        let idMin = ids |> Seq.min
        let idMax = ids |> Seq.max
        let updateCount = users.Update(fun u -> u.Banned.Set true).WhereId(fun id -> id >= idMin && id <= idMax).Execute()
        for id in ids do
            let userGetById = users.GetById id
            assertEqual userGetById.Banned true "The updated user in the large batch is different."

    [<TestMethod>]
    member this.UpdateLargeNumberOfUsers2() =        
        let users = db.GetCollection<User>()
        let largeBatch = [| for i in 1 .. 1000 -> { Username = $"User{i}"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.UtcNow.AddDays(-i); LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [| $"tag{i}" |] } } |]
        let ids = users.InsertBatch largeBatch
        let idMin = ids |> Seq.min
        let idMax = ids |> Seq.max
        let updateCount = users.Update(fun u -> u.Banned.Set true).Where(fun id u -> id >= idMin && id <= idMax && not u.Banned).Execute()
        for id in ids do
            let userGetById = users.GetById id
            assertEqual userGetById.Banned true "The updated user in the large batch is different."

    [<TestMethod>]
    member this.MultiThreadedReads() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        users.InsertBatch randomUsersToInsert |> ignore

        let readUsers () = 
            users.Select().OnAll().ToList()

        let tasks = [| for _ in 1 .. 100 -> (Task.Run readUsers) :> Task |]
        Task.WaitAll(tasks)
        for task in tasks do
            assertEqual (task.Status) TaskStatus.RanToCompletion "Multi-threaded reads did not complete successfully."

    [<TestMethod>]
    member this.SelectWithAnyInEachConditions() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let complexQueryUsers = users.Select(fun u -> u.Username, u.Auth).Where(fun u -> u.Banned = false && u.Auth = true && u.Data.Tags.AnyInEach(InnerExpr(fun item -> item = "tag2"))).ToList()
        let resultName, resultAuth = complexQueryUsers.[0]
        let expectedUsers = randomUsersToInsert |> Array.filter (fun u -> u.Banned = false && u.Auth = true && u.Data.Tags |> Array.contains "tag2") |> Array.toList
        assertEqual resultName (expectedUsers.[0].Username) "The complex condition query returned incorrect number of users."

    [<TestMethod>]
    member this.EnsureIndex() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let ex = users.EnsureIndex(fun u -> u.Username)
        let plan = users.Select(fun u -> u.Username, u.Auth).Where(fun u -> u.Username > "AABB" && u.Auth = true).ExplainQueryPlan()
        printfn "%s" plan
        if plan.Contains "USING INDEX" |> not then failwithf "Does not use index"        
        ()

    [<TestMethod>]
    member this.EnsureIndexTuple() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let ex = Assert.ThrowsException<exn>(fun () -> users.EnsureIndex(fun u -> (u.Username, u.Auth)) |> ignore)

        users.EnsureIndex(fun u -> u.Username) |> ignore
        users.EnsureIndex(fun u -> u.Auth) |> ignore

        let plan = users.Select(fun u -> u.Username, u.Auth).Where(fun u -> u.Username > "AABB").ExplainQueryPlan()
        printfn "%s" plan
        if plan.Contains "USING INDEX" |> not then failwithf "Does not use index"

        let plan = users.Select(fun u -> u.Username, u.Auth).Where(fun u -> u.Auth = true).ExplainQueryPlan()
        printfn "%s" plan
        if plan.Contains "USING INDEX" |> not then failwithf "Does not use index"
        ()

    [<TestMethod>]
    member this.DropIndexIfExists() =        
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let ex = users.EnsureIndex(fun u -> u.Username)
        let plan = users.Select(fun u -> u.Username, u.Auth).Where(fun u -> u.Username > "AABB" && u.Auth = true).ExplainQueryPlan()
        printfn "%s" plan

        if plan.Contains "USING INDEX" |> not then failwithf "Does not use index"
    

        let ex2 = users.DropIndexIfExists(fun item -> item.Username)
        let plan = users.Select(fun u -> u.Username, u.Auth).Where(fun u -> u.Username > "AABB" && u.Auth = true).ExplainQueryPlan()
        printfn "%s" plan

        if plan.Contains "USING INDEX" then failwithf "Did not drop index."
        ()

    [<TestMethod>]
    member this.InitializeCollectionCreatesTable() =
        db.GetCollection<User>() |> ignore
        Assert.IsTrue(db.ExistCollection<User>(), "Table for User was not created")

    [<TestMethod>]
    member this.GetCollectionReturnsConsistentInstance() =
        let collection1 = db.GetCollection<User>()
        let collection2 = db.GetCollection<User>()
        Assert.IsTrue((collection1 = collection2), "GetCollection should return the same instance")

    [<TestMethod>]
    member this.DropCollectionIfExistsWorksCorrectly() =
        db.GetCollection<User>()  |> ignore
        let result = db.DropCollectionIfExists<User>()
        Assert.IsTrue(result && not (db.ExistCollection<User>()), "DropCollectionIfExists failed to drop the table")

    [<TestMethod>]
    member this.DropCollectionThatDoesNotExistReturnsFalse() =
        let result = db.DropCollectionIfExists "NonExistent"
        Assert.IsFalse(result, "DropCollectionIfExists should return false for non-existent tables")

    [<TestMethod>]
    member this.GetUntypedCollectionInitializesCorrectly() =
        let collection = db.GetUntypedCollection "Dynamic"
        Assert.IsNotNull(collection, "GetUntypedCollection failed to initialize a collection")

    [<TestMethod>]
    member this.DropNonExistentCollectionThrows() =
        Assert.ThrowsException<exn>((fun () -> db.DropCollection<User>()), "Expected exception was not thrown for dropping non-existent collection")

    [<TestMethod>]
    member this.LockHandlingDuringCollectionInitialization() =
        let initAction = async {
            db.DropCollectionIfExists<User>() |> ignore
            let _ = db.GetCollection<User>() in ()
        }
        let tasks = [| for _ in 1 .. 5 -> Async.StartAsTask initAction :> Task |]
        Task.WaitAll(tasks)
        Assert.IsTrue(db.ExistCollection<User>(), "Lock handling during collection initialization failed")

    [<TestMethod>]
    member this.EnsureOldUserToNewUserDeserialization() =
        db.GetCollection<TypesOld.User>().InsertBatch TypesOld.randomOldUsersToInsert |> ignore
        let newUsers = db.GetCollection<User>().Select().OnAll().ToList()
        printfn "%A" newUsers
        for user in newUsers do
            Assert.IsNotNull(user, "OldUser to User desealization failed.")

    [<TestMethod>]
    member this.BackupDBFullLock() =
        let ids = db.GetCollection<User>().InsertBatch randomUsersToInsert
        let ids = db.GetCollection<User>().InsertBatch randomUsersToInsert
        let ids = db.GetCollection<User>().InsertBatch randomUsersToInsert
        let ids = db.GetCollection<User>().InsertBatch randomUsersToInsert

        let randomData: obj list = [
            "test";
            1L;
            2L;
            seq { "A" :> obj; "B"; "C"; 99L }
        ]

        db.GetUntypedCollection("RandomData").InsertBatch randomData |> ignore

        use backup = SoloDB.Instantiate "memory:backup1"
        db.BackupTo backup

        db.DropCollection "RandomData" |> ignore
        db.Dispose()

        assertEqual (backup.ExistCollection "RandomData") true "Backup incomplete: not equal."
        let backupRandomData = backup.GetUntypedCollection "RandomData"
        let backupRandomDataValues = backupRandomData.Select().OnAll().ToList()
        let backupRandomDataValuesString = sprintf "%A" backupRandomDataValues
        let randomDataString = sprintf "%A" randomData

        assertEqual backupRandomDataValuesString randomDataString "Backup incomplete: not equal."

        let usersCount = backup.GetCollection<User>().CountAll()
        assertEqual usersCount (randomUsersToInsert.LongLength * 4L) "Backup incomplete: not everything."

    [<TestMethod>]
    member this.BackupVacuumFromAndToDisk() =
        db.Dispose()
        if File.Exists "./temp/temp_from.db" then File.Delete "./temp/temp_from.db"
        db <- SoloDB.Instantiate "./temp/temp_from.db"

        for i in 1..10 do db.GetUntypedCollection("Data").Insert {|abc = "1010"|} |> ignore
        
        try
            printfn "[%s] Start backup." (DateTime.Now.ToShortTimeString())

            let rez = db.BackupVacuumTo "./temp/temp.db"
            use backup = SoloDB.Instantiate "./temp/temp.db"

            let backupCount = backup.GetUntypedCollection("Data").Count().Where(fun x -> x?abc = "1010").First() |> int
            printfn "Backup count: %i" backupCount
            assertEqual backupCount 10 "Backup count mismatch."

            assertEqual (backup.GetUntypedCollection("Data2").Count().Where(fun x -> x?abc <> "xyz" && x?abc <> "1010").First()) 0L "Backup data mismatch."
        finally
            db.Dispose()

            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools()

            GC.Collect();
            GC.WaitForPendingFinalizers();

            while File.Exists "./temp/temp.db" do
                try                    
                    File.Delete "./temp/temp.db"
                with e -> Thread.Sleep 20

            while File.Exists "./temp/temp_from.db" do
                try                    
                    File.Delete "./temp/temp_from.db"
                with e -> Thread.Sleep 20

    [<TestMethod>]
    member this.ReplaceDocumentBySet() =
        let user = db.GetCollection<User>()
        let id = user.Insert randomUsersToInsert.[0]
        let userToUpdate = randomUsersToInsert.[1]
        assertEqual (user.Update(fun u -> u.Set userToUpdate).WhereId(id).Execute()) 1 "Did not update."
        let dbUser = user.GetById id
        assertEqual dbUser userToUpdate "Did not update."

    [<TestMethod>]
    member this.ReplaceDocument() =
        let user = db.GetCollection<User>()
        let id = user.Insert randomUsersToInsert.[0]
        let userToUpdate = randomUsersToInsert.[1]
        assertEqual (user.Replace(userToUpdate).WhereId(id).Execute()) 1 "Did not update."
        let dbUser = user.GetById id
        assertEqual dbUser userToUpdate "Did not update."

    [<TestMethod>]
    member this.UserWithIdInsert() =
        let users = db.GetCollection<UserWithId>()
        let user1 = randomUsersWithIdToInsert.[0].Clone()

        users.Insert user1 |> ignore
        Assert.AreNotEqual(user1.Id, 0L, "User id = 0, did not init its id.")

    [<TestMethod>]
    member this.UserWithIdReplace() =
        let users = db.GetCollection<UserWithId>()
        let user1 = randomUsersWithIdToInsert.[0].Clone()

        users.Insert user1 |> ignore
        Assert.AreNotEqual(user1.Id, 0L, "User id = 0, did not init its id.")

        user1.Username <- "FSJFHDJI"
        user1.Banned <- true

        users.Update(user1)

        let dbUser = users.GetById user1.Id
        assertEqual (dbUser.ToString()) (user1.ToString()) "User did not replace in DB."

    [<TestMethod>]
    member this.UserWithIdSelectTheId() =
        let users = db.GetCollection<UserWithId>()
        let user1 = randomUsersWithIdToInsert.[0].Clone()

        let ids = users.InsertBatch (randomUsersWithIdToInsert |> Seq.map _.Clone())
    
        let selected = users.Select(fun u -> (u.Id, u.Username)).Where(fun u -> u.Id < SqlId(3)).ToList()

        assertEqual (selected.Length) (2) "The select with SoloDBEntry.Id does not work."

    [<TestMethod>]
    member this.DeleteWithLimit() =
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore

        assertEqual (users.Delete().Where(fun u -> true).Limit(2UL).Execute()) 2 "More rows affected than the limit."

        assertEqual (users.CountAll()) (randomUsersToInsert.LongLength - 2L) "The delete with limit does not work."
    
    [<TestMethod>]
    member this.ListCollectionNames() =
        db.GetCollection<User>() |> ignore
        db.GetCollection<UserWithId>() |> ignore
        db.GetUntypedCollection "Abracadabra" |> ignore

        assertEqual (db.ListCollectionNames() |> Seq.toList) [nameof(User); nameof(UserWithId); "Abracadabra"] "ListCollectionNames did not list all the names."

    [<TestMethod>]
    member this.UsernameStringContains() =
        db.GetCollection<User>().InsertBatch randomUsersToInsert |> ignore

        let allUsersWithAInName = db.GetCollection<User>().Select(fun u -> u.Username).Where(fun u -> u.Username.Contains "a").ToList()

        assertEqual allUsersWithAInName.Length 3 "Could not find all users with 'a' in Username."

    [<TestMethod>]
    member this.AnyTrue2() =
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let exists = users.Any()
        assertEqual exists true "The Any function should return true."

        let exists = db.GetUntypedCollection("FHSAIFHIFUD").Any()
        assertEqual exists false "The Any function should return false for a empty collection."

    [<TestMethod>]
    member this.UpdateSet() =
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        let first = users.TryFirst(fun u -> true).Value
        let dateTimeNow = DateTimeOffset.Now
        assertEqual (users.Update(fun u -> u.LastSeen.Set dateTimeNow).OnAll().Execute()  |> int) randomUsersToInsert.Length "Update (Set) did not work."
        let allUsers = users.Select().OnAll().ToList()
        assertEqual allUsers.Length randomUsersToInsert.Length "Update (Set) did not work."

    [<TestMethod>]
    member this.SelectUnique() =
        let users = db.GetCollection<User>()
        users.InsertBatch randomUsersToInsert |> ignore
        users.InsertBatch randomUsersToInsert |> ignore
        users.InsertBatch randomUsersToInsert |> ignore
        users.InsertBatch randomUsersToInsert |> ignore

        let uniqueUsernames = users.SelectUnique(fun u -> u.Username).OnAll().ToList().Length 

        assertEqual uniqueUsernames 4 "SelectUnique Username failed."

    [<TestMethod>]
    member this.StructCast() =
        let dates = db.GetCollection<DateOnly>()
        dates.Insert (DateOnly.FromDateTime DateTime.Now) |> ignore
        dates.Insert (DateOnly.FromDateTime DateTime.Now) |> ignore
        dates.Insert (DateOnly.FromDateTime DateTime.Now) |> ignore
        dates.Insert (DateOnly.FromDateTime DateTime.Now) |> ignore
        dates.Insert (DateOnly.FromDateTime DateTime.Now) |> ignore

        let allDatesOlder = dates.Select().Where(fun d -> d <= (DateOnly.FromDateTime DateTime.Now)).ToList()
        assertEqual allDatesOlder.Length 5 "Incorrect count."


    [<TestMethod>]
    member this.BackupVacuum() =
        for i in 1..10 do 
            db.GetUntypedCollection("Data").Insert {|abc = "1010"|} |> ignore

        let ex = Assert.ThrowsException<exn>(fun () -> let rez = db.BackupVacuumTo "./temp/temp.db" in ())
        assertEqual ex.Message "Cannot vaccuum backuo from or to memory."

    [<TestMethod>]
    member this.BackupVacuum2() =
        for i in 1..10 do 
            db.GetUntypedCollection("Data").Insert {|abc = "5435"; asd=4|} |> ignore

        let ex = Assert.ThrowsException<exn>(fun () -> let rez = db.BackupVacuumTo "./temp/temp2.db" in ())
        assertEqual ex.Message "Cannot vaccuum backup from or to memory." 