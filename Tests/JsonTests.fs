module JsonTests

#nowarn "3391" // Implicit on SqlId

open System
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDB
open JsonFunctions
open Types
open TestUtils

[<TestClass>]
type JsonTests() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.Instantiate dbSource
    
    [<TestCleanup>]
    member this.Cleanup() =
        db.Dispose()

    [<TestMethod>]
    member this.JsonSerialize() =
        let json = JsonSerializator.JsonValue.Serialize {|First = 1; Name = "Alpha"; Likes=["Ice Scream"; "Sleep"]; Activities={|List=["Codes"]; Len=1|}|}
        ()

    [<TestMethod>]
    member this.JsonSerializeDeserialize() =
        let user1 = {
            Username = "Mihail"
            Auth = true
            Banned = false
            FirstSeen = DateTimeOffset.UtcNow.AddYears -10 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
            LastSeen = DateTimeOffset.UtcNow.AddMinutes -8 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
            Data = {
                Tags = [|"tag2"|]
            }
        }
        let json = JsonSerializator.JsonValue.Serialize user1
        let user2 = json.ToObject<User>()
        let eq = (user1 = user2)
        Assert.IsTrue(eq)
        ()

    [<TestMethod>]
    member this.JsonUints() =
        db.GetCollection<uint8 array>().Insert [|65uy; 66uy; 67uy|] |> ignore
        db.GetCollection<uint16 array>().Insert [|65us; 66us; 67us|] |> ignore
        db.GetCollection<uint32 array>().Insert [|65u; 66u; 67u|] |> ignore
        db.GetCollection<uint64 array>().Insert [|65uL; 66uL; 67uL|] |> ignore

    [<TestMethod>]
    member this.ToSQLJsonAndKindTest() =
        let now = DateTimeOffset.Now
        assertEqual (toSQLJson now) (now.ToUnixTimeMilliseconds(), false) "ToSQLJsonAndKindTest: Datetimeoffset"
        assertEqual (toSQLJson "Hello") ("Hello", false) "ToSQLJsonAndKindTest: string"
        assertEqual (toSQLJson 1) (1, false) "ToSQLJsonAndKindTest: string"
        assertEqual (toSQLJson [1L]) ("[1]", true) "ToSQLJsonAndKindTest: string"
        assertEqual (toSQLJson {|hello = "test"|}) ("{\"hello\": \"test\"}", true) "ToSQLJsonAndKindTest: string"

    [<TestMethod>]
    member this.JsonDates() =
        let dateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds 43757783
        let dateTime = DateTime.Now.ToBinary() |> DateTime.FromBinary
        let dateOnly = DateOnly.FromDateTime dateTime
        let timeSpan = DateTime.Now.TimeOfDay.TotalMilliseconds |> int64 (* To the DB storage precision. *) |> float |> TimeSpan.FromMilliseconds
        let timeOnly = timeSpan |> TimeOnly.FromTimeSpan

        assertEqual (toJson dateTimeOffset |> fromJsonOrSQL) (dateTimeOffset) "DateTimeOffset failed."
        assertEqual (toJson dateTime |> fromJsonOrSQL) (dateTime) "DateTime failed."
        assertEqual (toJson dateOnly |> fromJsonOrSQL) (dateOnly) "DateOnly failed."
        assertEqual (toJson timeSpan |> fromJsonOrSQL) (timeSpan) "TimeSpan failed."
        assertEqual (toJson timeOnly |> fromJsonOrSQL) (timeOnly) "TimeOnly failed."