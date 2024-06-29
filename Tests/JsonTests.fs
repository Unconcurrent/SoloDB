module JsonTests

#nowarn "3391" // Implicit on SqlId

open System
open Microsoft.VisualStudio.TestTools.UnitTesting
open SoloDB
open SoloDBTypes
open Types
open TestUtils

[<TestClass>]
type JsonTests() =
    let mutable db: SoloDB = Unchecked.defaultof<SoloDB>
    
    [<TestInitialize>]
    member this.Init() =
        let dbSource = $"memory:Test{Random.Shared.NextInt64()}"
        db <- SoloDB.instantiate dbSource
    
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