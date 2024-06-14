module SoloDB

open Microsoft.Data.Sqlite
open Dapper
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open System.Text.RegularExpressions
open System.Threading
open FSharp.Interop.Dynamic
open JsonUtils
open System.Collections.Concurrent
open SoloDBTypes

type private DisposableMutex(name: string) =
    let mutex = new Mutex(false, name)
    do mutex.WaitOne() |> ignore

    interface IDisposable with
        member this.Dispose() =
            mutex.ReleaseMutex()
            mutex.Dispose()

let private lockTable(name: string) =
    let mutex = new DisposableMutex($"SoloDB-Table-{name}")
    mutex

let private createTableInner (name: string) (conn: SqliteConnection) (transaction: SqliteTransaction) =
    conn.Execute($"CREATE TABLE \"{name}\" (
    	    Id INTEGER NOT NULL PRIMARY KEY UNIQUE,
    	    Value JSONB NOT NULL
        );", {|name = name|}, transaction) |> ignore
    conn.Execute("INSERT INTO Collections(Name) VALUES (@name)", {|name = name|}, transaction) |> ignore

let private existsCollection (name: string) (connection: SqliteConnection)  =
    connection.QueryFirstOrDefault<string>("SELECT Name FROM Collections WHERE Name = @name LIMIT 1", {|name = name|}) <> null

let private dropCollection (name: string) (transaction: SqliteTransaction) (connection: SqliteConnection) =
    connection.Execute(sprintf "DROP TABLE IF EXISTS \"%s\"" name, transaction = transaction) |> ignore
    connection.Execute("DELETE FROM Collections Where Name = @name", {|name = name|}, transaction = transaction) |> ignore

let private insertInner (item: 'T) (connection: SqliteConnection) (transaction: SqliteTransaction) (name: string) =
    let json = toJson item

    // I know, its ugly.
    match item :> obj with
    | :? SoloDBEntry as entry when entry.Id <> 0 -> failwithf "Cannot insert a SoloDBEntry with a non zero Id, maybe you want to Update()?"
    | _ ->
    
    let id = connection.QueryFirst<int64>($"INSERT INTO \"{name}\"(Value) VALUES(jsonb(@jsonText)) RETURNING Id;", {|name = name; jsonText = json|}, transaction)

    match item :> obj with
    | :? SoloDBEntry as entry -> SoloDBEntry.InitId entry id
    | other -> ()
    id

let private insertInnerRaw (item: DbObjectRow) (connection: SqliteConnection) (transaction: SqliteTransaction) (name: string) =
    connection.Execute($"INSERT INTO \"{name}\"(Id, Value) VALUES(@id, jsonb(@jsonText))", {|name = name; id = item.Id; jsonText = item.ValueJSON|}, transaction)

type FinalBuilder<'T, 'Q, 'R>(connection: SqliteConnection, name: string, sql: string, variables: Dictionary<string, obj>, select: 'Q -> 'R, postModifySQL: string -> string) =
    let mutable sql = sql
    let mutable limit: uint64 Option = None
    let mutable offset = 0UL
    let orderByList = List<string>()

    let getQueryParameters() =
        let variables = seq {for key in variables.Keys do KeyValuePair<string, obj>(key, variables.[key])} |> Seq.toList
        let parameters = new DynamicParameters(variables)

        let finalSQL = 
            sql 
            + (if orderByList.Count > 0 then sprintf "ORDER BY %s " (orderByList |> String.concat ",") else " ") 
            + (if limit.IsSome then $"LIMIT {limit.Value} " else if offset > 0UL then "LIMIT -1 " else "")
            + (if offset > 0UL then $"OFFSET {offset} " else "")

        let finalSQL = postModifySQL finalSQL

        printfn "%s" finalSQL 

        finalSQL, parameters

    member this.Limit(?count: uint64) =
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
            |> Seq.filter (fun i -> not (Object.ReferenceEquals(i, null)))

    member this.ToList() =
        this.Enumerate() 
        |> Seq.toList

    member this.First() =
        let finalSQL, parameters = getQueryParameters()
        connection.QueryFirst<'Q>(finalSQL, parameters) |> select

    member this.Execute() =
        let finalSQL, parameters = getQueryParameters()
        connection.Execute(finalSQL, parameters)

    member this.ExplainQueryPlan() =
        // EXPLAIN QUERY PLAN 
        let finalSQL, parameters = getQueryParameters()
        let finalSQL = "EXPLAIN QUERY PLAN " + finalSQL
        connection.Query<obj>(finalSQL, parameters)
        |> Seq.map(fun arr -> seq { for i in (arr :?> IEnumerable) do i } |> Seq.last |> Dyn.get "Value")
        |> String.concat ";\n"

type WhereBuilder<'T, 'Q, 'R>(connection: SqliteConnection, name: string, sql: string, select: 'Q -> 'R, vars: Dictionary<string, obj>, postModifySQL: string -> string) =
    member this.Where(expression: Expression<System.Func<'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, newVariables, select, postModifySQL)

    member this.Where(expression: Expression<System.Func<SqlId, 'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, newVariables, select, postModifySQL)

    member this.WhereId(id: int64) =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql + sprintf "WHERE Id = %i " id, vars, select, postModifySQL)

    member this.WhereId(func: Expression<System.Func<SqlId, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name func
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, vars, select, postModifySQL)

    member this.OnAll() =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, vars, select, postModifySQL)

type Collection<'T>(connection: SqliteConnection, name: string, connectionString: string, inTransaction: bool) =
    member private this.ConnectionString = connectionString
    member this.Name = name
    member this.InTransaction = inTransaction

    member this.Insert (item: 'T) =
        insertInner item connection null name

    member this.InsertBatch (items: 'T seq) =
        use l = lockTable name  // If not, there will be multiple transactions started on the same connection when used concurently,
                                // and it is faster than creating new connections.

        if inTransaction then
            let ids = List<int64>()
            for item in items do
                insertInner item connection null name |> ids.Add
            ids
        else

        use transaction = connection.BeginTransaction()
        try
            let ids = List<int64>()
            for item in items do
                insertInner item connection transaction name |> ids.Add

            transaction.Commit()
            ids
        with ex ->
            transaction.Rollback()
            reraise()

    member this.TryGetById(id: int64) =
        match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {|id = id|}) with
        | json when Object.ReferenceEquals(json, null) -> None
        | json -> fromDapper<'T> json |> Some

    member this.GetById(id: int64) =
        match this.TryGetById id with
        | None -> failwithf "There is no element with id %i" id
        | Some x -> x

    member this.Select<'R>(select: Expression<System.Func<'T, 'R>>) =
        let selectSQL, variables = QueryTranslator.translate name select

        WhereBuilder<'T, DbObjectRow, 'R>(connection, name, $"SELECT Id, {selectSQL} as ValueJSON FROM \"{name}\" ", fromDapper<'R>, variables, id)

    member this.Select() =
        WhereBuilder<'T, DbObjectRow, 'T>(connection, name, $"SELECT Id, json(Value) as ValueJSON FROM \"{name}\" ", fromDapper<'T>, Dictionary<string, obj>(), id)

    member this.SelectWithId() =
        WhereBuilder(connection, name, $"SELECT json_object('Id', Id, 'Value', Value) FROM \"{name}\" ", fromIdJson<'T>, Dictionary<string, obj>(), id)

    member this.TryFirst(func: Expression<System.Func<'T, bool>>) =
        this.Select().Where(func).Enumerate() |> Seq.tryHead

    member this.Count() =
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), id)

    member this.CountAll() =        
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), id).OnAll().First()

    member this.CountAllLimit(limit: uint64) =        
        FinalBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM (SELECT Id FROM \"{name}\" LIMIT @limit)", Dictionary<string, obj>([|KeyValuePair("limit", limit :> obj)|]), fromJsonOrSQL<int64>, id).First()

    member this.CountWhere(func: Expression<System.Func<'T, bool>>) =
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), id).Where(func).First()

    member this.CountWhere(func: Expression<System.Func<'T, bool>>, limit: uint64) =
        let whereSQL, variables = QueryTranslator.translate name func
        variables["limit"] <- limit :> obj

        FinalBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM (SELECT Id FROM \"{name}\" WHERE {whereSQL} LIMIT @limit)", variables, fromJsonOrSQL<int64>, id).First()

    member this.Any(func) = this.CountWhere(func, 1UL) > 0L

    member this.Any() = this.CountWhere((fun u -> true), 1UL) > 0L

    member this.Update(expression: Expression<System.Action<'T>>) =
        let updateSQL, variables = QueryTranslator.translateUpdateMode name expression
        let updateSQL = updateSQL.Trim ','

        WhereBuilder<'T, string, int64>(connection, name, $"UPDATE \"{name}\" SET Value = jsonb_set(Value, {updateSQL})", fromJsonOrSQL<int64>, variables, id)

    member this.Replace(item: 'T) =
        WhereBuilder<'T, string, int64>(connection, name, $"UPDATE \"{name}\" SET Value = jsonb(@item)", fromJsonOrSQL<int64>, Dictionary([|KeyValuePair("item", toSQLJson item :> obj)|]), id)

    member this.Update(item: SoloDBEntry) =
        match item :> obj with
        | :? 'T as value -> this.Replace(value).WhereId(item.Id).Execute() |> ignore
        | other -> failwithf "Cannot insert a %s in a %s Collection, but if you want it use a UntypedCollection with the name '%s'." (other.GetType().FullName) (typeof<'T>.FullName) (name)

    member this.Delete() =
        // https://stackoverflow.com/questions/1824490/how-do-you-enable-limit-for-delete-in-sqlite
        // By default, SQLite does not support LIMIT in a DELETE statement, but there is this workaround.
        WhereBuilder<'T, string, int64>(connection, name, $"DELETE FROM \"{name}\" WHERE Id IN (SELECT Id FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), fun sql -> sql + ")")

    member this.DeleteById(id: int64) : int =
        this.Delete().WhereId(id).Execute()

    member this.EnsureIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let whereSQL = whereSQL.Replace($"{name}.Value", "Value") // {name}.Value is not allowed in an index.

        if newVariables.Count > 0 then failwithf "Cannot have variables in index."
        let expressionStr = whereSQL.ToCharArray() |> Seq.filter(fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"

        let indexSQL = $"CREATE INDEX IF NOT EXISTS {indexName} ON {name}({whereSQL})"

        connection.Execute(indexSQL)

    member this.DropIndexIfExists<'R>(expression: Expression<System.Func<'T, 'R>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let whereSQL = whereSQL.Replace($"{name}.Value", "Value") // {name}.Value is not allowed in an index.

        if newVariables.Count > 0 then failwithf "Cannot have variables in index."
        let expressionStr = whereSQL.ToCharArray() |> Seq.filter(fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"

        let indexSQL = $"DROP INDEX IF EXISTS {indexName}"

        connection.Execute(indexSQL)
        
    override this.Equals(other) = 
        match other with
        | :? Collection<'T> as other ->
            (this :> IEquatable<Collection<'T>>).Equals other
        | other -> false

    override this.GetHashCode() = hash (this)

    interface IEquatable<Collection<'T>> with
        member this.Equals (other) =
            this.ConnectionString = other.ConnectionString && this.Name = other.Name

and SoloDB private (connectionCreator: bool -> SqliteConnection, dbConnection: SqliteConnection, connectionString: string, transaction: bool) = 
    let mutable disposed = false
    let inTransaction = transaction
    let connectionPool = ConcurrentDictionary<string, SqliteConnection>()

    let connectionCreator() = connectionCreator disposed

    member this.ConnectionString = connectionString
    member this.GetDBConnection() = dbConnection
    member this.GetNewConnection() = connectionCreator()

    member private this.FormatName (name: string) =
        name.Replace("\"", "") // Anti SQL injection

    member private this.GetNameFrom<'T>() =
        typeof<'T>.Name |> this.FormatName

    member private this.InitializeCollection<'T> name =     
        if disposed then raise (ObjectDisposedException(nameof(SoloDB)))

        let connection = 
            if inTransaction then dbConnection
            else connectionPool.GetOrAdd(name, (fun name -> connectionCreator()))

        use mutex = lockTable name // To prevent a race condition where the next if statment is true for 2 threads.
        if not (existsCollection name connection) then 
            if inTransaction then
                createTableInner name connection null
            else
                use transaction = connection.BeginTransaction()
                try
                    createTableInner name connection transaction
                    transaction.Commit()
                with ex ->
                    transaction.Rollback()
                    reraise ()

        Collection<'T>(connection, name, connectionString, inTransaction)

    member this.GetCollection<'T>() =
        let name = this.GetNameFrom<'T>()
        
        this.InitializeCollection<'T>(name)

    member this.GetUntypedCollection(name: string) =
        let name = name  |> this.FormatName
        
        this.InitializeCollection<obj>(name)

    member this.ExistCollection name =
        existsCollection name dbConnection

    member this.ExistCollection<'T>() =
        let name = this.GetNameFrom<'T>()
        existsCollection name dbConnection

    member this.DropCollectionIfExists name =
        use mutex = lockTable name

        if inTransaction then
            if existsCollection name dbConnection then 
                dropCollection name null dbConnection
                true
            else false
        else

        if existsCollection name dbConnection then
            use transaction = dbConnection.BeginTransaction()
            try
                dropCollection name transaction dbConnection
                transaction.Commit()
                true
            with ex -> 
                transaction.Rollback()
                reraise()
        else false

    member this.DropCollectionIfExists<'T>() =
        let name = this.GetNameFrom<'T>()
        this.DropCollectionIfExists name

    member this.DropCollection<'T>() =
        if this.DropCollectionIfExists<'T>() = false then
            let name = this.GetNameFrom<'T>()
            failwithf "Collection %s does not exists." name

    member this.DropCollection name =
        if this.DropCollectionIfExists name = false then
            failwithf "Collection %s does not exists." name

    member this.ListCollectionNames() =
        dbConnection.Query<string>("SELECT Name FROM Collections")

    member this.BackupTo(otherDb: SoloDB) =
        if transaction then failwithf "Cannot backup in a transaction."

        use connection = connectionCreator()
        use otherConnection = otherDb.GetNewConnection()
        connection.BackupDatabase otherConnection

    member this.BackupCollections(otherDb: SoloDB) =
        if transaction then failwithf "Cannot backup in a transaction."

        printfn "A warning from BackupCollections: This will backup only the collections managed by SoloDB, without any custom added tables."

        use connection = connectionCreator()
        use otherConnection = otherDb.GetNewConnection()

        connection.Execute("BEGIN IMMEDIATE") |> ignore // Get a write lock
                
        let names = connection.Query<string>("SELECT Name FROM Collections") |> Seq.toList
        printfn "Tables count: %i" names.Length

        use otherTransaction = otherConnection.BeginTransaction()
        try
            let otherNames = otherConnection.Query<string>("SELECT Name FROM Collections")
            for otherName in otherNames do dropCollection otherName otherTransaction otherConnection 

            for name in names do
                createTableInner name otherConnection otherTransaction 
                let rows = connection.Query<DbObjectRow>(sprintf "SELECT Id, json(Value) as ValueJSON FROM \"%s\"" name, buffered = false, commandTimeout = Nullable<int>())
                for row in rows do
                    let rowsAffected = insertInnerRaw row otherConnection otherTransaction name
                    if rowsAffected <> 1 then failwithf "No rows affected in insert."


            otherTransaction.Commit()
        with ex -> 
            otherTransaction.Rollback() 
            reraise()


        connection.Execute("ROLLBACK") |> ignore

    member this.Transactionally<'R>(func: Func<SoloDB, 'R>) =
        use newConnection = connectionCreator()

        let innerConnectionCreator disposed = failwithf "This should not be called."

        newConnection.Execute("BEGIN;") |> ignore

        use transactionalDb = new SoloDB(innerConnectionCreator, newConnection, connectionString, true)

        try
            let ret = func.Invoke transactionalDb
            newConnection.Execute "COMMIT;" |> ignore
            ret
        with ex -> 
            newConnection.Execute "ROLLBACK;" |> ignore
            reraise()

    member this.Dispose() =
        disposed <- true
        for connections in connectionPool.Values do
            connections.Dispose()
        connectionPool.Clear()
        dbConnection.Dispose()

    interface IDisposable with
        member this.Dispose() = this.Dispose()

    static member Instantiate (source: string) =
        let connectionString =
            if source.StartsWith("memory:", StringComparison.InvariantCultureIgnoreCase) then
                let memoryName = source.Substring "memory:".Length
                let memoryName = memoryName.Trim()
                sprintf "Data Source=%s;Mode=Memory;Cache=Shared" memoryName
            else 
                $"Data Source={source}"

        let connectionCreator disposed =
            if disposed then failwithf "SoloDB was disposed."

            let connection = new SqliteConnection(connectionString)
            connection.Open()
            connection
    
        let dbConnection = connectionCreator false
        dbConnection.Execute(
                            "PRAGMA journal_mode=wal;
                            CREATE TABLE IF NOT EXISTS Collections (Name TEXT NOT NULL) STRICT;
                            CREATE INDEX IF NOT EXISTS CollectionsNameIndex ON Collections(Name);
                            ") |> ignore

        new SoloDB(connectionCreator, dbConnection, connectionString, false)

    // The pipe operators.
    // If you want to use Expression<System.Func<'T, bool>> fluently, 
    // by just defining a normal function (fun (...) -> ...) you need to use a static member
    // with C# like arguments.

    static member collection<'T> (db: SoloDB) = db.GetCollection<'T>()
    static member collectionUntyped name (db: SoloDB) = db.GetUntypedCollection name

    static member drop<'T> (db: SoloDB) = db.DropCollection<'T>()
    static member dropByName name (db: SoloDB) = db.DropCollection name

    static member tryDrop<'T> (db: SoloDB) = db.DropCollectionIfExists<'T>()
    static member tryDropByName name (db: SoloDB) = db.DropCollectionIfExists name

    static member transactionally func (db: SoloDB) = db.Transactionally func

    static member ensureIndex<'T, 'R> (func: Expression<System.Func<'T, 'R>>) (collection: Collection<'T>) = collection.EnsureIndex func
    static member tryDropIndex<'T, 'R> (func: Expression<System.Func<'T, 'R>>) (collection: Collection<'T>) = collection.DropIndexIfExists func

    static member countWhere<'T> (func: Expression<System.Func<'T, bool>>) = fun  (collection: Collection<'T>) -> collection.CountWhere func
    static member count<'T> (collection: Collection<'T>) = collection.Count()
    static member countAll<'T> (collection: Collection<'T>) = collection.CountAll()
    static member countAllLimit<'T> (limit: uint64) (collection: Collection<'T>) = collection.CountAllLimit limit

    static member insert<'T> (item: 'T) (collection: Collection<'T>) = collection.Insert item
    static member insertBatch<'T> (items: 'T seq) (collection: Collection<'T>) = collection.InsertBatch items

    static member updateF<'T> (func: Expression<Action<'T>>) = fun (collection: Collection<'T>) -> collection.Update func
    static member update<'T> (item: SoloDBEntry) (collection: Collection<'T>) = collection.Update item
    static member replace<'T> (item: 'T) (collection: Collection<'T>) = collection.Replace item

    static member delete<'T> (collection: Collection<'T>) = collection.Delete()
    static member deleteById<'T> id (collection: Collection<'T>) = collection.DeleteById id

    static member select<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: Collection<'T>) -> collection.Select func
    static member all<'T> (collection: Collection<'T>) = collection.Select()

    static member where (func: Expression<System.Func<'a, bool>>) = fun (builder: WhereBuilder<'a, 'b, 'c>) -> builder.Where func
    static member whereId (func: int64) (builder: WhereBuilder<'a, 'b, 'c>) = builder.WhereId func

    static member limit (count: uint64) (builder: FinalBuilder<'a, 'b, 'c>) = builder.Limit count
    static member offset (count: uint64) (builder: FinalBuilder<'a, 'b, 'c>) = builder.Offset count

    static member orderAsc (func: Expression<System.Func<'a, obj>>) = fun (builder: FinalBuilder<'a, 'b, 'c>) -> builder.OrderByAsc func
    static member orderDesc (func: Expression<System.Func<'a, obj>>) = fun (builder: FinalBuilder<'a, 'b, 'c>) -> builder.OrderByDesc func

    static member exec (builder: FinalBuilder<'a, 'b, 'c>) = builder.Execute()
    static member toSeq (builder: FinalBuilder<'a, 'b, 'c>) = builder.Enumerate()
    static member toList (builder: FinalBuilder<'a, 'b, 'c>) = builder.ToList()

    static member explain (builder: FinalBuilder<'a, 'b, 'c>) = builder.ExplainQueryPlan()

    static member getById (id: int64) (collection: Collection<'T>) = collection.GetById id
    static member tryGetById (id: int64) (collection: Collection<'T>) = collection.TryGetById id

    static member tryFirst<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: Collection<'T>) -> func |> collection.TryFirst

let instantiate = SoloDB.Instantiate


type System.Object with
    member this.Set(value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.Like(pattern: string) =
        let regexPattern = 
            "^" + Regex.Escape(pattern).Replace("\\%", ".*").Replace("\\_", ".") + "$"
        Regex.IsMatch(this.ToString(), regexPattern, RegexOptions.IgnoreCase)

    member this.Contains(pattern: string) =
        failwithf "This is a dummy function for the SQL builder."

type Array with
    member this.Add(value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.SetAt(index: int, value: obj) =
        failwithf "This is a dummy function for the SQL builder."

    member this.RemoveAt(index: int) =
        failwithf "This is a dummy function for the SQL builder."

    member this.AnyInEach(condition: InnerExpr) = 
        failwithf "This is a dummy function for the SQL builder."
        bool()

// This operator allow for multiple operations in the Update method,
// else it will throw 'Could not convert the following F# Quotation to a LINQ Expression Tree',
// imagine it as a ';'.
let (|+|) a b = ()