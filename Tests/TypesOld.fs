module TypesOld

open System

[<CLIMutable>]
type User = {
    Username: string
    Auth: bool
    Banned: bool
    FirstSeen: DateTimeOffset
}

let randomOldUsersToInsert = [|
    {
        Username = "David"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears -5 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
    };
    {
        Username = "Alex"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears -3 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
    };
    {
        Username = "Sam"
        Auth = true
        Banned = true
        FirstSeen = DateTimeOffset.UtcNow.AddYears -8 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
    };
    {
        Username = "Nicolay"
        Auth = false
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears -20 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
    };
    {
        Username = "Giorgio"
        Auth = false
        Banned = true
        FirstSeen = DateTimeOffset.UtcNow.AddYears -2 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
    };
    {
        Username = "Alfred"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears -1 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
    }
|]