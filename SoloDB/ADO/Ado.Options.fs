namespace SoloDatabase.Data.Sqlite

open System
open System.Collections
open System.Data.Common
open SoloDatabase

/// Standard SQLite connection options are presented without a second execution owner.
[<Sealed>]
type SqliteConnectionStringBuilder() as this =
    inherit DbConnectionStringBuilder()
    let keys = [| "Data Source"; "Mode"; "Cache"; "Password"; "Foreign Keys"; "Recursive Triggers"; "Default Timeout"; "Pooling" |]
    let canonical (keyword: string) =
        if isNull keyword then nullArg "keyword"
        match keyword.ToUpperInvariant() with
        | "DATA SOURCE" | "DATASOURCE" | "FILENAME" -> "Data Source"
        | "MODE" -> "Mode"
        | "CACHE" -> "Cache"
        | "PASSWORD" -> "Password"
        | "FOREIGN KEYS" | "FOREIGNKEYS" -> "Foreign Keys"
        | "RECURSIVE TRIGGERS" | "RECURSIVETRIGGERS" -> "Recursive Triggers"
        | "DEFAULT TIMEOUT" | "DEFAULTTIMEOUT" | "COMMAND TIMEOUT" | "COMMANDTIMEOUT" -> "Default Timeout"
        | "POOLING" -> "Pooling"
        | _ -> raise (ArgumentException($"Keyword not supported: '{keyword}'.", "keyword"))
    let value (keyword: string) (value: obj) : obj =
        match keyword with
        | "Data Source" | "Password" -> box (Convert.ToString(value, Globalization.CultureInfo.InvariantCulture))
        | "Mode" ->
            let mode = Enum.Parse(typeof<SqliteOpenMode>, string value, true) :?> SqliteOpenMode
            if not (Enum.IsDefined(typeof<SqliteOpenMode>, mode)) then invalidArg "value" "Invalid SQLite open mode."
            box mode
        | "Cache" ->
            let cache = Enum.Parse(typeof<SqliteCacheMode>, string value, true) :?> SqliteCacheMode
            if not (Enum.IsDefined(typeof<SqliteCacheMode>, cache)) then invalidArg "value" "Invalid SQLite cache mode."
            box cache
        | "Default Timeout" -> box (Convert.ToInt32(value, Globalization.CultureInfo.InvariantCulture))
        | _ -> box (Convert.ToBoolean(value, Globalization.CultureInfo.InvariantCulture))
    let fallback keyword : obj =
        match keyword with
        | "Data Source" | "Password" -> box ""
        | "Mode" -> box SqliteOpenMode.ReadWriteCreate
        | "Cache" -> box SqliteCacheMode.Default
        | "Foreign Keys" -> null
        | "Recursive Triggers" -> box false
        | "Default Timeout" -> box NativeConnectionDefaults.commandTimeout
        | "Pooling" -> box true
        | _ -> null
    new(connectionString: string) as this = new SqliteConnectionStringBuilder() then this.ConnectionString <- connectionString
    override _.Item
        with get(keyword: string) =
            let key = canonical keyword
            if base.ContainsKey(key) then value key base.[key] else fallback key
        and set (keyword: string) (item: obj) =
            let key = canonical keyword
            if isNull item then base.Remove(key) |> ignore
            else base.[key] <- value key item
    override _.Keys = keys :> ICollection
    override _.Values = (keys |> Array.map (fun key -> this.[key])) :> ICollection
    override _.ContainsKey(keyword) = try canonical keyword |> ignore; true with :? ArgumentException -> false
    override _.TryGetValue(keyword, result: byref<obj>) =
        if this.ContainsKey(keyword) then result <- this.[keyword]; true else result <- null; false
    override _.Remove(keyword) = if this.ContainsKey(keyword) then base.Remove(canonical keyword) else false
    override _.ShouldSerialize(keyword) = this.ContainsKey(keyword) && base.ShouldSerialize(canonical keyword)
    member _.DataSource with get() = this.["Data Source"] :?> string and set (value: string) = this.["Data Source"] <- value
    member _.Mode with get() = this.["Mode"] :?> SqliteOpenMode and set (value: SqliteOpenMode) = this.["Mode"] <- value
    member _.Cache with get() = this.["Cache"] :?> SqliteCacheMode and set (value: SqliteCacheMode) = this.["Cache"] <- value
    member _.Password with get() = this.["Password"] :?> string and set (value: string) = this.["Password"] <- value
    member _.ForeignKeys
        with get() = if isNull this.["Foreign Keys"] then Nullable<bool>() else Nullable(this.["Foreign Keys"] :?> bool)
        and set (value: Nullable<bool>) = this.["Foreign Keys"] <- if value.HasValue then box value.Value else null
    member _.RecursiveTriggers with get() = this.["Recursive Triggers"] :?> bool and set (value: bool) = this.["Recursive Triggers"] <- value
    member _.DefaultTimeout with get() = this.["Default Timeout"] :?> int and set (value: int) = this.["Default Timeout"] <- value
    member _.Pooling with get() = this.["Pooling"] :?> bool and set (value: bool) = this.["Pooling"] <- value
