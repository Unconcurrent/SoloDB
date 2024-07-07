module Types

open System
open System.Text
open SoloDatabase.Types

[<CLIMutable>]
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

type UserWithId() =
    member val Id: SqlId = SqlId(0) with get, set
    member val Username: string = "" with get, set
    member val Auth: bool = false with get, set
    member val Banned: bool = false with get, set
    member val FirstSeen: DateTimeOffset = DateTimeOffset.MinValue with get, set
    member val LastSeen: DateTimeOffset = DateTimeOffset.MinValue with get, set
    member val Data: UserData = {Tags = [||]} with get, set

    member this.Clone() =
        let cloned = UserWithId()

        cloned.Username <- this.Username
        cloned.Auth <- this.Auth
        cloned.Banned <- this.Banned
        cloned.FirstSeen <- this.FirstSeen
        cloned.LastSeen <- this.LastSeen
        cloned.Data <- { Tags = Array.copy this.Data.Tags }
        cloned

    override this.ToString() =
        let sb = StringBuilder()
        sb.AppendLine("{") |> ignore
        sb.AppendLine(sprintf "  \"Username\": \"%s\"," this.Username) |> ignore
        sb.AppendLine(sprintf "  \"Auth\": %b," this.Auth) |> ignore
        sb.AppendLine(sprintf "  \"Banned\": %b," this.Banned) |> ignore
        sb.AppendLine(sprintf "  \"FirstSeen\": \"%s\"," (this.FirstSeen.ToString("o"))) |> ignore
        sb.AppendLine(sprintf "  \"LastSeen\": \"%s\"," (this.LastSeen.ToString("o"))) |> ignore
        sb.AppendLine("  \"Data\": {") |> ignore
        sb.AppendLine("    \"Tags\": [") |> ignore
        for tag in this.Data.Tags do
            sb.AppendLine(sprintf "      \"%s\"," tag) |> ignore
        sb.Remove(sb.Length - 3, 1) |> ignore  // Remove the trailing comma from the last element
        sb.AppendLine("    ]") |> ignore
        sb.AppendLine("  }") |> ignore
        sb.Append("}") |> ignore
        sb.ToString()

    static member FromUser id user =
        let userId = UserWithId()

        userId.Username <- user.Username
        userId.Auth <- user.Auth
        userId.Banned <- user.Banned
        userId.FirstSeen <- user.FirstSeen
        userId.LastSeen <- user.LastSeen
        userId.Data <- user.Data

        userId

[<AbstractClass>]
type Animal() =
    member val Id: SqlId = SqlId(0) with get, set

    member val Size: float = 1 with get, set
    member val Tammed: bool = false with get, set

    override this.ToString() =
        let sb = StringBuilder()
        sb.AppendLine("{") |> ignore
        sb.AppendLine(sprintf "  \"Size\": \"%f\"," this.Size) |> ignore
        sb.AppendLine(sprintf "  \"Tammed\": %b," this.Tammed) |> ignore
        sb.Append("}") |> ignore
        sb.ToString()

type Cat() =
    inherit Animal()

    member val TailSize: float = 1 with get, set

type Tiger() =
    inherit Cat()

    member val TailSize: float = 2 with get, set

type Dog() =
    inherit Animal()

    member val Tammed: bool = true with get, set
    member val Bark: string = "Hau" with get, set


let randomUsersToInsert = [|
    {
        Username = "John"
        Auth = true
        Banned = false
        // The DB stores only the miliseconds, but the DateTimeOffset can have more precision.
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
        FirstSeen = DateTimeOffset.UtcNow.AddYears -10 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes -8 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"tag2"|]
        }
    };
    {
        Username = "Vanya"
        Auth = true
        Banned = true
        FirstSeen = DateTimeOffset.UtcNow.AddYears -10 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes -5 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"tag1-B"|]
        }
    };
    {
        Username = "Givany"
        Auth = false
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears -10 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes -2 |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"tag1-C"|]
        }
    }
|]

let randomUsersWithIdToInsert = [|
    UserWithId.FromUser 0 {
        Username = "Ion"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-15) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-10) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"abc"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Elena"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-10) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-5) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"xyz"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Maria"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-8) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-30) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"123"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Vasile"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-20) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddHours(-1) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"456"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Alexandru"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-5) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-15) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"789"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Sofia"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-12) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-7) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"ghi"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Mihai"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-7) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-3) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"jkl"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Anastasia"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-4) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-20) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"mno"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Victor"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-3) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-25) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"pqr"|]
        }
    }
    UserWithId.FromUser 0 {
        Username = "Natalia"
        Auth = true
        Banned = false
        FirstSeen = DateTimeOffset.UtcNow.AddYears(-6) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        LastSeen = DateTimeOffset.UtcNow.AddMinutes(-12) |> _.ToUnixTimeMilliseconds() |> DateTimeOffset.FromUnixTimeMilliseconds
        Data = {
            Tags = [|"stu"|]
        }
    }
|]