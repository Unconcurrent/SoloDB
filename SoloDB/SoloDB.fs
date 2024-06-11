module SoloDB

open Microsoft.Data.Sqlite
open Dapper
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Text.Json
open System.Text
open System.Text.RegularExpressions
open System.Threading
open FSharp.Interop.Dynamic
open JsonUtils
open QueryTranslator

type DisposableMutex(name: string) =
    let mutex = new Mutex(false, name)
    do mutex.WaitOne() |> ignore

    interface IDisposable with
        member this.Dispose() =
            mutex.ReleaseMutex()
            mutex.Dispose()

let private lockTable(name: string) =
    let mutex = new DisposableMutex($"SoloDB-Table-{name}")
    mutex

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

type FinalBuilder<'T, 'Q, 'R>(connection: SqliteConnection, name: string, sql: string, variables: Dictionary<string, obj>, select: 'Q -> 'R) =
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
            + (if limit > 0UL then $"LIMIT {limit} " else if offset > 0UL then "LIMIT -1 " else "")
            + (if offset > 0UL then $"OFFSET {offset} " else "")

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
        let orderSelector, _ = QueryTranslator.translate name expression
        let orderSQL = sprintf "(%s) ASC" orderSelector

        orderByList.Add orderSQL

        this

    member this.OrderByDesc(expression: Expression<System.Func<'T, obj>>) =
        let orderSelector, _ = QueryTranslator.translate name expression
        let orderSQL = sprintf "(%s) DESC" orderSelector

        orderByList.Add orderSQL

        this

    member this.Enumerate() =
        let finalSQL, parameters = getQueryParameters()
        connection.Query<'Q>(finalSQL, parameters) 
            |> Seq.filter (fun i -> not (Object.ReferenceEquals(i, null)))
            |> Seq.map select 

    member this.ToList() =
        this.Enumerate() 
        |> Seq.toList

    member this.First() =
        let finalSQL, parameters = getQueryParameters()
        connection.QueryFirst<'Q>(finalSQL, parameters) |> select

    member this.Execute() =
        let finalSQL, parameters = getQueryParameters()
        connection.Execute(finalSQL, parameters)

type WhereBuilder<'T, 'Q, 'R>(connection: SqliteConnection, name: string, sql: string, select: 'Q -> 'R, vars: Dictionary<string, obj>) =
    member this.Where(expression: Expression<System.Func<'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, newVariables, select)

    member this.Where(expression: Expression<System.Func<sqlId, 'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, newVariables, select)

    member this.WhereId(id: int64) =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql + sprintf "WHERE Id = %i " id, vars, select)

    member this.WhereId(func: Expression<System.Func<sqlId, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name func
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, vars, select)

    member this.OnAll() =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, vars, select)


type Collection<'T>(connection: SqliteConnection, name: string) =
    let insertInner (item: 'T) (transaction: SqliteTransaction) =
        let json = toSQLJson item
        connection.QueryFirst<int64>($"INSERT INTO \"{name}\"(Value) VALUES(jsonb(@jsonText)) RETURNING Id;", {|name = name; jsonText = json|}, transaction)

    member this.Insert (item: 'T) =
        insertInner item null

    member this.InsertBatch (items: 'T seq) =
        use l = lockTable name // If not, there will be multiple transactions started on the same connection when used concurently.
        let transaction = connection.BeginTransaction()
        try
            let ids = List<int64>()
            for item in items do
                insertInner item transaction |> ids.Add

            transaction.Commit()
            ids
        with ex ->
            transaction.Rollback()
            reraise()

    member this.TryGetById(id: int64) =
        match connection.QueryFirstOrDefault<string>($"SELECT json(Value) FROM \"{name}\" WHERE Id = @id LIMIT 1", {|id = id|}) with
        | null -> None
        | json -> fromJsonOrSQL<'T> json |> Some

    member this.GetById(id: int64) =
        match this.TryGetById id with
        | None -> failwithf "There is no element with id %i" id
        | Some x -> x

    member this.Select<'R>(select: Expression<System.Func<'T, 'R>>) =
        let selectSQL, variables = QueryTranslator.translate name select

        WhereBuilder<'T, string, 'R>(connection, name, $"SELECT {selectSQL} FROM \"{name}\" ", fromJsonOrSQL<'R>, variables)

    member this.Select() =
        WhereBuilder<'T, string, 'T>(connection, name, $"SELECT json(Value) FROM \"{name}\" ", fromJsonOrSQL<'T>, Dictionary<string, obj>())

    member this.SelectWithId() =
        WhereBuilder(connection, name, $"SELECT json_object('Id', Id, 'Value', Value) FROM \"{name}\" ", fromIdJson<'T>, Dictionary<string, obj>())

    member this.Count() =
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>())

    member this.CountAll() =        
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>()).OnAll().First()

    member this.CountAllLimit(limit: uint64) =        
        FinalBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM (SELECT Id FROM \"{name}\" LIMIT @limit)", Dictionary<string, obj>([|KeyValuePair("limit", limit :> obj)|]), fromJsonOrSQL<int64>).First()

    member this.CountWhere(func: Expression<System.Func<'T, bool>>) =
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>()).Where(func).First()

    member this.CountWhere(func: Expression<System.Func<'T, bool>>, limit: uint64) =
        let whereSQL, variables = QueryTranslator.translate name func
        variables["limit"] <- limit :> obj

        FinalBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM (SELECT Id FROM \"{name}\" WHERE {whereSQL} LIMIT @limit)", variables, fromJsonOrSQL<int64>).First()

    member this.Any(func) = this.CountWhere(func, 1UL) > 0L

    member this.Update(expression: Expression<System.Action<'T>>) =
        let updateSQL, variables = QueryTranslator.translateUpdateMode name expression
        let updateSQL = updateSQL.Trim ','

        WhereBuilder<'T, string, int64>(connection, name, $"UPDATE \"{name}\" SET Value = jsonb_set(Value, {updateSQL})", fromJsonOrSQL<int64>, variables)

    member this.Update(item: 'T) =
        WhereBuilder<'T, string, int64>(connection, name, $"UPDATE \"{name}\" SET Value = jsonb(@item)", fromJsonOrSQL<int64>, Dictionary([|KeyValuePair("item", toSQLJson item :> obj)|]))

    member this.Delete() =
        WhereBuilder<'T, string, int64>(connection, name, $"DELETE FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>())

    member this.DeleteById(id: int64) : int =
        this.Delete().WhereId(id).Execute()
        

and SoloDB(connection: SqliteConnection) =
    member this.GetCollection<'T>() =
        let name = typeof<'T>.Name.Replace("\"", "") // Anti SQL injection
        use mutex = lockTable name // To prevent a race condition where the next if statment is true for 2 threads.

        if (connection.QueryFirstOrDefault<string>("SELECT Name FROM Types WHERE Name = @name LIMIT 1", {|name = name|}) = null) then 
            createTable<'T> name connection

        Collection<'T>(connection, name)

    member this.GetUntypedCollection(name: string) =
        let name = name.Replace("\"", "") // Anti SQL injection
        use mutex = new Mutex(true, $"SoloDB-Table-{name}") // To prevent a race condition where the next if statment is true for 2 threads.

        if (connection.QueryFirstOrDefault<string>("SELECT Name FROM Types WHERE Name = @name LIMIT 1", {|name = name|}) = null) then 
            createTable<obj> name connection

        Collection<obj>(connection, name)

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

  

type System.Object with
    member this.Set(value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.Like(pattern: string) =
        let regexPattern = 
            "^" + Regex.Escape(pattern).Replace("\\%", ".*").Replace("\\_", ".") + "$"
        Regex.IsMatch(this.ToString(), regexPattern, RegexOptions.IgnoreCase)

type Array with
    member this.Add(value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.SetAt(index: int, value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.RemoveAt(index: int) =
        failwithf "This is a dummy function for the SQL builder."

    member this.AnyInEach(condition: QueryTranslator.InnerExpr) = 
        failwithf "This is a dummy function for the SQL builder."
        bool()



// This operator allow for multiple operations in the Update method,
// else it will throw 'Could not convert the following F# Quotation to a LINQ Expression Tree',
// imagine it as a ';'.
let (|+|) a b = ()