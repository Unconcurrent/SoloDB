﻿module TransactionalTests

#nowarn "3391" // Implicit on SqlId

open System
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDB
open SoloDBTypes
open Types
open TestUtils

[<TestClass>]
type SoloDBTransactionalTesting() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.instantiate dbSource
    
    [<TestCleanup>]
    member this.Cleanup() =
        db.Dispose()

    [<TestMethod>]
    member this.TransactionRollbackDropCollection() =
        db.GetCollection<User>().InsertBatch randomUsersToInsert |> ignore

        Assert.ThrowsException<exn>(fun () ->
            db.Transactionally(
                fun db -> 
                    db.DropCollection<User>()
                    printfn "Now will fail."
                    db.DropCollection<User>()
                    ())
                    ) |> ignore
        assertEqual (db.GetCollection<User>().CountAll()) (randomUsersToInsert.Length) "DropCollection<User>() in transaction rollback failed."

    [<TestMethod>]
    member this.TransactionQuery() =
        db.Transactionally(
            fun db -> 
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
                let complexQueryUsers = 
                    users
                        .Select(fun u -> u.Username, u.Auth)
                        .Where(fun u -> u.Banned = false && u.Auth = true && u.Data.Tags.AnyInEach(InnerExpr(fun item -> item.Contains "tag2")))
                        .OrderByAsc(fun u -> u.LastSeen)
                        .Limit(2UL)
                        .ToList()

                let resultName, resultAuth = complexQueryUsers.[0]
                let expectedUsers = randomUsersToInsert |> Array.filter (fun u -> u.Banned = false && u.Auth = true && u.Data.Tags |> Array.contains "tag2") |> Array.toList
                assertEqual resultName (expectedUsers.[0].Username) "The complex condition query returned incorrect number of users."
                assertEqual resultAuth (expectedUsers.[0].Auth) "The complex condition query returned incorrect number of users."

                assertEqual (users.Delete().Where(fun u -> not (u.Username = resultName && u.Auth = resultAuth)).Execute()) 3 "Delete did not return correctly."
                ())
                

        assertEqual (db.GetCollection<User>().CountAll()) (1) "Transaction did not execute correctly."
        assertEqual (db.GetCollection<User>().Select().OnAll().First().Data.Tags.[0]) "tag2" "Transaction corrupted data."

    [<TestMethod>]
    member this.TransactionCountWhereWithLimitEqual() =
        db.Transactionally(
            fun db ->
                let testUsers = [
                    { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
                    { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
                ]
    
                let users = db.GetCollection<User>()
                users.InsertBatch testUsers |> ignore
                let count = users.CountWhere((fun u -> u.Username = "Alice"), 1UL)
                assertEqual count 1L "The count where username is Alice with limit 1 should be one."
        )

        let count = db.GetCollection<User>().CountWhere((fun u -> u.Username = "Alice"), 1UL)
        assertEqual count 1L "The count where username is Alice with limit 1 should be one."
    
    [<TestMethod>]
    member this.TransactionAnyTrue() =
        db.Transactionally(
            fun db ->
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
        )

        let users = db.GetCollection<User>()
        let exists = users.Any(fun u -> u.Username = "Alice")
        assertEqual exists true "The Any function should return true for username Alice."
    
    [<TestMethod>]
    member this.TransactionAnyFalse() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
                let exists = users.Any(fun u -> u.Username = "NonExistentAAAAA")
                assertEqual exists false "The Any function should return false for a non-existent username."
        )

        let users = db.GetCollection<User>()
        let exists = users.Any(fun u -> u.Username = "NonExistentAAAAA")
        assertEqual exists false "The Any function should return false for a non-existent username."

    [<TestMethod>]
    member this.TransactionRollBackAnyFalse() =
        Assert.ThrowsException(fun () ->
            db.Transactionally(
                fun db ->
                    let users = db.GetCollection<User>()
                    users.InsertBatch randomUsersToInsert |> ignore
                    let exists = users.Any()
                    assertEqual exists true "The Any function should return true."
                    failwithf "Simulated fail."
            )) |> ignore

        let users = db.GetCollection<User>()
        let exists = users.Any()
        assertEqual exists false "The Any function should return false for a empty collection."
    
    [<TestMethod>]
    member this.TransactionCountAllEmpty() =
        let users = db.GetCollection<User>()

        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                let count = users.CountAll()
                assertEqual count 0L "The count should be zero for an empty table."
        )

        
        let count = users.CountAll()
        assertEqual count 0L "The count should be zero for an empty table."
    
    [<TestMethod>]
    member this.TransactionCountAllWithMultipleInserts() =
        let users = db.GetCollection<User>()
        db.Transactionally(
            fun db ->
                let testUsers = [
                    { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
                    { Username = "Bob"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
                ]
    
                let users = db.GetCollection<User>()
                users.InsertBatch testUsers |> ignore
                let count = users.CountAll()
                assertEqual count 2L "The count should be two after inserting two users."
        )

        let count = users.CountAll()
        assertEqual count 2L "The count should be two after inserting two users."
    
    [<TestMethod>]
    member this.TransactionCountWhereMultipleConditions() =
        db.Transactionally(
            fun db ->
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
        )

        let users = db.GetCollection<User>()
        let count = users.CountWhere(fun u -> u.Auth && not u.Banned)
        assertEqual count 2L "The count should be one where Auth is true and Banned is false."
    
    [<TestMethod>]
    member this.TransactionCountWithZeroConditionMatch() =
        db.Transactionally(
            fun db ->
                let testUsers = [
                    { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
                    { Username = "Bob"; Auth = true; Banned = true; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
                ]
    
                let users = db.GetCollection<User>()
                users.InsertBatch testUsers |> ignore
                let count = users.CountWhere(fun u -> u.Auth && u.Banned && u.Username = "Charlie")
                assertEqual count 0L "The count should be zero where no user matches the condition."
        )
    
    [<TestMethod>]
    member this.TransactionCountWhereMultipleUsersMatch() =
        db.Transactionally(
            fun db ->
                let testUsers = [
                    { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
                    { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
                ]
    
                let users = db.GetCollection<User>()
                users.InsertBatch testUsers |> ignore
                let count = users.CountWhere(fun u -> u.Username = "Alice")
                assertEqual count 2L "The count should be two where username is Alice."
        )

        let users = db.GetCollection<User>()
        let count = users.CountWhere(fun u -> u.Username = "Alice")
        assertEqual count 2L "The count should be two where username is Alice."
    
    [<TestMethod>]
    member this.TransactionAnyWithMultipleConditionsTrue() =
        db.Transactionally(
            fun db ->
                let testUsers = [
                    { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
                    { Username = "Bob"; Auth = true; Banned = true; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
                ]
    
                let users = db.GetCollection<User>()
                users.InsertBatch testUsers |> ignore
                let exists = users.Any(fun u -> u.Auth && not u.Banned)
                assertEqual exists true "The Any function should return true where Auth is true and Banned is false."
        )
    
    [<TestMethod>]
    member this.TransactionAnyWithMultipleConditionsFalse() =
        db.Transactionally(
            fun db ->
                let testUsers = [
                    { Username = "Alice"; Auth = true; Banned = false; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example"|] } }
                    { Username = "Bob"; Auth = true; Banned = true; FirstSeen = DateTimeOffset.MinValue; LastSeen = DateTimeOffset.UtcNow; Data = { Tags = [|"example2"|] } }
                ]
    
                let users = db.GetCollection<User>()
                users.InsertBatch testUsers |> ignore
                let exists = users.Any(fun u -> u.Auth && u.Banned && u.Username = "Charlie")
                assertEqual exists false "The Any function should return false where no user matches the condition."
        )

        let users = db.GetCollection<User>()
        let exists = users.Any(fun u -> u.Auth && u.Banned && u.Username = "Charlie")
        assertEqual exists false "The Any function should return false where no user matches the condition."
    
    [<TestMethod>]
    member this.TransactionCountWhereWithLimitZero() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                let count = users.CountWhere((fun u -> u.Username = "NonExistent"), 1UL)
                assertEqual count 0L "The count where username is NonExistent with limit 1 should be zero."
        )
    
    [<TestMethod>]
    member this.TransactionUpdateUser() =
        let testUser = randomUsersToInsert.[0]
        let updatedUser = { testUser with Username = "UpdatedName" }
        let id = db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()                
                let id = users.Insert testUser                
                users.Replace(updatedUser).WhereId(id).Execute() |> ignore     
                id
        )

        let userGetById = db.GetCollection<User>().GetById id
        assertEqual userGetById updatedUser "The updated user is different."
    
    [<TestMethod>]
    member this.TransactionDeleteUser() =
        let id = db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                let testUser = randomUsersToInsert.[0]
                let id = users.Insert testUser
                assertEqual (users.DeleteById id) 1 "No row deleted."
                id
        )

        let userGetById = db.GetCollection<User>().TryGetById id
        assertEqual userGetById None "The user should be deleted."
    
    [<TestMethod>]
    member this.TransactionCountUsers() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
        )
        let users = db.GetCollection<User>()
        let count = users.CountAll()
        assertEqual count (int64 randomUsersToInsert.Length) "The count of users is incorrect."
    
    [<TestMethod>]
    member this.TransactionLimitUsers() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
                let limitedUsers = users.Select().OnAll().Limit(2UL).ToList()
                assertEqual (limitedUsers.Length) 2 "The limit query returned incorrect number of users."
        )

        let users = db.GetCollection<User>()
        let limitedUsers = users.Select().OnAll().Limit(2UL).ToList()
        assertEqual (limitedUsers.Length) 2 "The limit query returned incorrect number of users."
    
    [<TestMethod>]
    member this.TransactionOffsetUsers() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore                
        )
        let users = db.GetCollection<User>()
        let offsetUsers = users.Select().OnAll().Offset(2UL).ToList()
        assertEqual (offsetUsers.Length) (randomUsersToInsert.Length - 2) "The offset query returned incorrect number of users."
    
    [<TestMethod>]
    member this.TransactionOrderByAscUsers() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
                let orderedUsers = users.Select(fun u -> u.Username).OnAll().OrderByAsc(fun u -> u.Username).ToList()
                let sortedUsernames = randomUsersToInsert |> Array.map (fun u -> u.Username) |> Array.sort
                assertEqual (orderedUsers) (Array.toList sortedUsernames) "The ascending order query returned incorrect order of users."
        )
    
    [<TestMethod>]
    member this.TransactionCountWhereUsers() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
                let countAuthUsers = users.CountWhere(fun u -> u.Auth)
                let expectedCount = randomUsersToInsert |> Array.filter (fun u -> u.Auth) |> Array.length |> int64
                assertEqual countAuthUsers expectedCount "The count where query returned incorrect count."
        )
    
        let users = db.GetCollection<User>()
        let countAuthUsers = users.CountWhere(fun u -> u.Auth)
        let expectedCount = randomUsersToInsert |> Array.filter (fun u -> u.Auth) |> Array.length |> int64
        assertEqual countAuthUsers expectedCount "The count where query returned incorrect count."
    
    [<TestMethod>]
    member this.TransactionAnyUsers() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
                let anyBannedUsers = users.Any(fun u -> u.Banned)
                let expectedAny = randomUsersToInsert |> Array.exists (fun u -> u.Banned)
                assertEqual anyBannedUsers expectedAny "The any query returned incorrect result."
        )
    
        let users = db.GetCollection<User>()
        let anyBannedUsers = users.Any(fun u -> u.Banned)
        let expectedAny = randomUsersToInsert |> Array.exists (fun u -> u.Banned)
        assertEqual anyBannedUsers expectedAny "The any query returned incorrect result."
    
    [<TestMethod>]
    member this.TransactionUpdateWhere() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
                users.Update(fun u -> u.Banned.Set true).Where(fun u -> u.Username = "John").Execute() |> ignore
                let bannedJohn = users.Select().Where(fun u -> u.Username = "John").First()
                assertEqual bannedJohn.Banned true "The update where query did not update the correct user."
        )
    
        let users = db.GetCollection<User>()
        let bannedJohn = users.Select().Where(fun u -> u.Username = "John").First()
        assertEqual bannedJohn.Banned true "The update where query did not update the correct user."
    
    [<TestMethod>]
    member this.TransactionSelectWithId() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                users.InsertBatch randomUsersToInsert |> ignore
                let usersWithId = users.SelectWithId().OnAll().ToList()
                assertEqual (usersWithId.Length) (randomUsersToInsert.Length) "The select with Id query returned incorrect number of users."
        )
    
        let users = db.GetCollection<User>()
        let usersWithId = users.SelectWithId().OnAll().ToList()
        assertEqual (usersWithId.Length) (randomUsersToInsert.Length) "The select with Id query returned incorrect number of users."
    
    [<TestMethod>]
    member this.TransactionGetNonExistentUser() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                let user = users.TryGetById 99999L
                assertEqual user None "The query for a non-existent user returned a result."
        )
    
        let users = db.GetCollection<User>()
        let user = users.TryGetById 99999L
        assertEqual user None "The query for a non-existent user returned a result."
    
    [<TestMethod>]
    member this.TransactionInsertDuplicateUser() =
        db.Transactionally(
            fun db ->
                let users = db.GetCollection<User>()
                let testUser = randomUsersToInsert.[0]
                let id1 = users.Insert testUser
                let id2 = users.Insert testUser
                assertEqual (id1 <> id2) true "The insertion of a duplicate user did not create a new ID."
                id1, id2
        )