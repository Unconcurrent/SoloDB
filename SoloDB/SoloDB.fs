﻿module SoloDB

open Microsoft.Data.Sqlite
open Dapper
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Text.Json
open System.Text
open System.Text.RegularExpressions
open System.Threading

type DateTimeOffsetJsonConverter() =
    inherit Serialization.JsonConverter<DateTimeOffset>()

    override this.Read(reader, typeToConvert: Type, options: JsonSerializerOptions) : DateTimeOffset =
        DateTimeOffset.FromUnixTimeMilliseconds (reader.GetInt64())

    override this.Write(writer: Utf8JsonWriter, dateTimeValue: DateTimeOffset, options) : unit =
        writer.WriteNumberValue (dateTimeValue.ToUnixTimeMilliseconds())

type BooleanJsonConverter() =
    inherit Serialization.JsonConverter<bool>()

    override this.Read(reader, typeToConvert: Type, options: JsonSerializerOptions) : bool =
        reader.GetInt64() = 1

    override this.Write(writer: Utf8JsonWriter, booleanValue: bool, options) : unit =
        if booleanValue then writer.WriteNumberValue 1UL else writer.WriteNumberValue 0UL


let private jsonOptions = 
    let o = JsonSerializerOptions()
    o.Converters.Add (DateTimeOffsetJsonConverter())
    o.Converters.Add (BooleanJsonConverter())
    o

let private toSQLJson<'T> item =
    let element = System.Text.Json.JsonSerializer.SerializeToElement<'T>(item, jsonOptions)
    let text = element.ToString()
    match element.ValueKind with
    | JsonValueKind.Object
    | JsonValueKind.Array ->
        text
    | other -> text.Trim '"'

let private toJson<'T> item =
    System.Text.Json.JsonSerializer.Serialize<'T>(item, jsonOptions)

let private fromJson<'T> (text: string) =
    System.Text.Json.JsonSerializer.Deserialize<'T> (text, jsonOptions)

let private fromIdJson<'T> (idValueJSON: string) =
    let element = System.Text.Json.JsonSerializer.Deserialize<JsonElement>(idValueJSON, jsonOptions)
    let id = element.GetProperty("Id").GetInt64()
    let value = element.GetProperty("Value").Deserialize<'T>(jsonOptions)
    id, value

let private createTable<'T> (name: string) (conn: SqliteConnection) =
    use transaction = conn.BeginTransaction()
    try
        conn.Execute($"CREATE TABLE \"{name}\" (
    	    Id INTEGER NOT NULL PRIMARY KEY UNIQUE,
    	    Value JSONB NOT NULL
        );", {|name = name|}, transaction) |> ignore
        conn.Execute("INSERT INTO Types(Name) VALUES (@name)", {|name = name|}, transaction) |> ignore
        transaction.Commit()
    with ex ->
        transaction.Rollback()
        reraise ()

type FinalBuilder<'T, 'R>(connection: SqliteConnection, name: string, sql: string, variables: Dictionary<string, obj>, select: string -> 'R) =
    let mutable sql = sql
    let mutable limit = 0UL
    let mutable offset = 0UL
    let orderByList = List<string>()

    let getQueryParameters() =
        let variables = seq {for key in variables.Keys do KeyValuePair<string, obj>(key, variables.[key])} |> Seq.toList
        let parameters = new DynamicParameters(variables)

        let finalSQL = 
            sql 
            + (if orderByList.Count > 0 then sprintf "ORDER BY %s " (orderByList |> String.concat ",") else " ") 
            + (if limit > 0UL then $"LIMIT {limit} " else " ")
            + (if offset > 0UL then $"OFFSET {offset} " else " ")

        printfn "%s" finalSQL 

        finalSQL, parameters

    member this.Limit(count: uint64) =
        if count = 0UL then failwithf "Cannot LIMIT 0"

        limit <- count  
        this

    member this.Offset(index: uint64) =
        offset <- index  
        this

    member this.OrderByAsc(expression: Expression<System.Func<'T, obj>>) =
        let orderSelector, _ = QueryTranslator.translate expression false
        let orderSQL = sprintf "(%s) ASC" orderSelector

        orderByList.Add orderSQL

        this

    member this.OrderByDesc(expression: Expression<System.Func<'T, obj>>) =
        let orderSelector, _ = QueryTranslator.translate expression false
        let orderSQL = sprintf "(%s) DESC" orderSelector

        orderByList.Add orderSQL

        this

    member this.ToList() =
        let finalSQL, parameters = getQueryParameters()
        connection.Query<string>(finalSQL, parameters) |> Seq.map select |> Seq.toList

    member this.First() =
        let finalSQL, parameters = getQueryParameters()
        connection.QueryFirst<string>(finalSQL, parameters) |> select

    member this.Execute() =
        let finalSQL, parameters = getQueryParameters()
        connection.Execute(finalSQL, parameters)

type WhereBuilder<'T, 'R>(connection: SqliteConnection, name: string, sql: string, select: string -> 'R, vars: Dictionary<string, obj>) =
    member this.Where(expression: Expression<System.Func<'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate expression false
        let sql = sql + sprintf "WHERE %s " whereSQL

        for key in newVariables.Keys do
            newVariables.[key] <- toSQLJson newVariables.[key]

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder(connection, name, sql, newVariables, select)

    member this.WhereId(id: int64) =
        FinalBuilder(connection, name, sql + sprintf "WHERE Id = %i " id, vars, select)

    member this.OnAll() =
        FinalBuilder(connection, name, sql, vars, select)


type Collection<'T>(connection: SqliteConnection, name: string) =
    let insertInner (item: 'T) (transaction: SqliteTransaction) =
        let json = toSQLJson item
        connection.QueryFirst<int64>($"INSERT INTO \"{name}\"(Value) VALUES(jsonb(@jsonText)) RETURNING Id;", {|name = name; jsonText = json|}, transaction)

    member this.Insert (item: 'T) =
        insertInner item null

    member this.InsertBatch (items: 'T seq) =
        let transaction = connection.BeginTransaction()
        try
            let ids = List<int64>()
            for item in items do
                insertInner item transaction |> ids.Add
            ids
        with ex ->
            transaction.Rollback()
            reraise()

    member this.TryGetById(id: int64) =
        match connection.QueryFirstOrDefault<string>($"SELECT json(Value) FROM \"{name}\" WHERE Id = @id LIMIT 1", {|id = id|}) with
        | null -> None
        | json -> fromJson<'T> json |> Some

    member this.GetById(id: int64) =
        match this.TryGetById id with
        | None -> failwithf "There is no element with id %i" id
        | Some x -> x



    member this.Select<'R>(select: Expression<System.Func<'T, 'R>>) =
        let selectSQL, variables = QueryTranslator.translate select true

        WhereBuilder<'T, 'R>(connection, name, $"SELECT {selectSQL} FROM \"{name}\" ", fromJson<'R>, variables)

    member this.Select() =
        WhereBuilder(connection, name, $"SELECT json(Value) FROM \"{name}\" ", fromJson<'T>, Dictionary<string, obj>())

    member this.SelectWithId() =
        WhereBuilder(connection, name, $"SELECT json_object('Id', Id, 'Value', Value) FROM \"{name}\" ", fromIdJson<'T>, Dictionary<string, obj>())

    member this.Update(expression: Expression<System.Action<'T>>) =
        let replaceTagSyntax (input: string) : string =
            let pattern = "' -> (\d+)"
            let replacement = "[$1]'"
            Regex.Replace(input, pattern, replacement)

        let replaceTagSyntax2 (input: string) : string =
            let pattern = "' -> ([^,]+?),"
            let replacement = "$1',"
            Regex.Replace(input, pattern, replacement)

        let updateSQL, variables = QueryTranslator.translateForUpdate expression
        let updateSQL = updateSQL.Trim ','

        for key in variables.Keys do
            variables.[key] <- toSQLJson variables.[key]

        WhereBuilder(connection, name, $"UPDATE \"{name}\" SET Value = jsonb_set(Value, {updateSQL})", fromJson<int64>, variables)

    member this.Update(item: 'T) =
        WhereBuilder(connection, name, $"UPDATE \"{name}\" SET Value = jsonb(@item)", fromJson<int64>, Dictionary([|KeyValuePair("item", toJson item :> obj)|]))
        

and SoloDB(connection: SqliteConnection) =
    member this.GetCollection<'T>() =
        let name = typeof<'T>.Name.Replace("\"", "") // Anti SQL injection
        use mutex = new Mutex(true, name) // To prevent a race condition where the next if statment is true for 2 threads.

        if (connection.QueryFirstOrDefault<string>("SELECT Name FROM Types WHERE Name = @name LIMIT 1", {|name = name|}) = null) then 
            createTable<'T> name connection

        Collection<'T>(connection, name)

    interface IDisposable with
        member this.Dispose() =
            connection.Close()
            connection.Dispose()

let instantiate (path: string) =
    let connection = new SqliteConnection($"Data Source={path}")
    connection.Open()
    let version = connection.QueryFirst<string>("SELECT SQLITE_VERSION()")

    connection.Execute(
        "CREATE TABLE IF NOT EXISTS Types (Id INTEGER NOT NULL PRIMARY KEY UNIQUE, Name TEXT NOT NULL) STRICT;
        CREATE INDEX IF NOT EXISTS TypeNameIndex ON Types(Name);") |> ignore

    new SoloDB(connection)

type System.String with
    member this.Like(pattern: string) =
        let regexPattern = 
            "^" + Regex.Escape(pattern).Replace("\\%", ".*").Replace("\\_", ".") + "$"
        Regex.IsMatch(this, regexPattern, RegexOptions.IgnoreCase)

type System.Object with
    member this.Set(value: obj) =
        failwithf "This is a dummy function for the SQL builder."

type Array with
    member this.Add(value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.SetAt(index: int, value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.RemoveAt(index: int) =
        failwithf "This is a dummy function for the SQL builder."

// This operator allow for multiple operations in the Update method,
// else it will throw 'Could not convert the following F# Quotation to a LINQ Expression Tree',
// imagine it as a ';'.
let (|+|) a b = ()