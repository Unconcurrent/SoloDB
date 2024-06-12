module Types

open System

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
        FirstSeen = DateTimeOffset.UtcNow.AddYears -15 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes -10 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
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