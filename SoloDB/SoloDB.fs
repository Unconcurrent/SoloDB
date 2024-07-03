﻿namespace SoloDB

open Microsoft.Data.Sqlite
open Dapper
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open FSharp.Interop.Dynamic
open JsonFunctions
open System.Collections.Concurrent
open SoloDBTypes
open Dynamitey
open System.IO
open FileStorage
open Connections
open System.Transactions

module internal Helper =
    let internal lockTable (connectionStr: string) (name: string) =
        let mutex = new DisposableMutex($"SoloDB-{connectionStr.GetHashCode(StringComparison.InvariantCultureIgnoreCase)}-Table-{name}")
        mutex

    let internal createTableInner (name: string) (conn: SqliteConnection) (transaction: SqliteTransaction) =
        conn.Execute($"CREATE TABLE \"{name}\" (
    	        Id INTEGER NOT NULL PRIMARY KEY UNIQUE,
    	        Value JSONB NOT NULL
            );", {|name = name|}, transaction) |> ignore
        conn.Execute("INSERT INTO SoloDBCollections(Name) VALUES (@name)", {|name = name|}, transaction) |> ignore

    let internal existsCollection (name: string) (connection: SqliteConnection)  =
        connection.QueryFirstOrDefault<string>("SELECT Name FROM SoloDBCollections WHERE Name = @name LIMIT 1", {|name = name|}) <> null

    let internal dropCollection (name: string) (transaction: SqliteTransaction) (connection: SqliteConnection) =
        connection.Execute(sprintf "DROP TABLE IF EXISTS \"%s\"" name, transaction = transaction) |> ignore
        connection.Execute("DELETE FROM SoloDBCollections Where Name = @name", {|name = name|}, transaction = transaction) |> ignore

    let internal insertInner (typed: bool) (item: 'T) (connection: SqliteConnection) (transaction: SqliteTransaction) (name: string) =
        let json = if typed then toTypedJson item else toJson item

        let existsId = hasIdType typeof<'T>

        if existsId && SqlId(0) <> item?Id then 
            failwithf "Cannot insert a item with a non zero Id, maybe you meant Update?"

        let id = connection.QueryFirst<SqlId>($"INSERT INTO \"{name}\"(Value) VALUES(jsonb(@jsonText)) RETURNING Id;", {|name = name; jsonText = json|}, transaction)

        if existsId then 
            item?Id <- id

        id

    let internal insertInnerRaw (item: DbObjectRow) (connection: SqliteConnection) (transaction: SqliteTransaction) (name: string) =
        connection.Execute($"INSERT INTO \"{name}\"(Id, Value) VALUES(@id, jsonb(@jsonText))", {|name = name; id = item.Id; jsonText = item.ValueJSON|}, transaction)

    let internal formatName (name: string) =
        name.Replace("\"", "").Replace(" ", "") // Anti SQL injection

    let internal getNameFrom<'T>() =
        typeof<'T>.Name |> formatName

type FinalBuilder<'T, 'Q, 'R>(connection: Connection, name: string, sql: string, variables: Dictionary<string, obj>, select: 'Q -> 'R, postModifySQL: string -> string, limit: uint64 option, ?offset: uint64, ?orderByList) =
    let sql = sql
    let limit: uint64 Option = limit
    let offset = match offset with Some x -> x | None -> 0UL
    let orderByList: string list = match orderByList with | Some x -> x | None -> []

    let getQueryParameters() =
        let variables = seq {for key in variables.Keys do KeyValuePair<string, obj>(key, variables.[key])} |> Seq.toList
        let parameters = new DynamicParameters(variables)

        let finalSQL = 
            sql 
            + (if orderByList.Length > 0 then sprintf "ORDER BY %s " (orderByList |> String.concat ",") else " ") 
            + (if limit.IsSome then $"LIMIT {limit.Value} " else if offset > 0UL then "LIMIT -1 " else "")
            + (if offset > 0UL then $"OFFSET {offset} " else "")

        let finalSQL = postModifySQL finalSQL

        printfn "%s" finalSQL 

        finalSQL, parameters

    member this.Limit(?count: uint64) =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, variables, select, postModifySQL, count, offset)

    member this.Offset(index: uint64) =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, variables, select, postModifySQL, limit, index)

    member this.OrderByAsc(expression: Expression<System.Func<'T, obj>>) =
        let orderSelector, _ = QueryTranslator.translate name expression
        let orderSQL = sprintf "(%s) ASC" orderSelector

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, variables, select, postModifySQL, limit, offset, orderByList @ [orderSQL])

    member this.OrderByDesc(expression: Expression<System.Func<'T, obj>>) =
        let orderSelector, _ = QueryTranslator.translate name expression
        let orderSQL = sprintf "(%s) DESC" orderSelector

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, variables, select, postModifySQL, limit, offset, orderByList @ [orderSQL])

    member this.Enumerate() =
        let finalSQL, parameters = getQueryParameters()

        seq {
            use connection = connection.Get()
            try            
                for item in connection.Query<'Q>(finalSQL, parameters) 
                            |> Seq.filter (fun i -> not (Object.ReferenceEquals(i, null)))
                            |> Seq.map select 
                            |> Seq.filter (fun i -> not (Object.ReferenceEquals(i, null)))
                            do
                    yield item

            with ex -> 
                let ex = ex // For breakpoint.
                raise ex
        }
       

    member this.First() =        
        let finalSQL, parameters = getQueryParameters()
        use connection = connection.Get()
        connection.QueryFirst<'Q>(finalSQL, parameters) |> select

    member this.Execute() =
        let finalSQL, parameters = getQueryParameters()
        use connection = connection.Get()
        connection.Execute(finalSQL, parameters)

    member this.ExplainQueryPlan() =
        // EXPLAIN QUERY PLAN 
        let finalSQL, parameters = getQueryParameters()
        let finalSQL = "EXPLAIN QUERY PLAN " + finalSQL

        use connection = connection.Get()
        connection.Query<obj>(finalSQL, parameters)
        |> Seq.map(fun arr -> seq { for i in (arr :?> IEnumerable) do i } |> Seq.last |> Dyn.get "Value")
        |> String.concat ";\n"

    member this.ToList() =
        this.Enumerate() 
        |> Seq.toList

type WhereBuilder<'T, 'Q, 'R>(connection: Connection, name: string, sql: string, select: 'Q -> 'R, vars: Dictionary<string, obj>, postModifySQL: string -> string) =
    member this.Where(expression: Expression<System.Func<'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, newVariables, select, postModifySQL, None)

    member this.Where(expression: Expression<System.Func<SqlId, 'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, newVariables, select, postModifySQL, None)

    member this.WhereId(id: SqlId) =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql + sprintf "WHERE Id = %i " id.Value, vars, select, postModifySQL, None)

    member this.WhereId(func: Expression<System.Func<SqlId, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name func
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, vars, select, postModifySQL, None)

    member this.OnAll() =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, vars, select, postModifySQL, None)

type Collection<'T>(connection: Connection, name: string, connectionString: string) =
    member private this.ConnectionString = connectionString
    member this.Name = name
    member this.InTransaction = match connection with | Transactional _ -> true | Pooled _ -> false
    member this.IncludeType = typeof<'T>.IsAbstract

    member this.Insert (item: 'T) =
        use connection = connection.Get()

        Helper.insertInner this.IncludeType item connection null name

    member this.InsertBatch (items: 'T seq) =
        if this.InTransaction then
            use connection = connection.Get()
            let ids = List<SqlId>()
            for item in items do
                Helper.insertInner this.IncludeType item connection null name |> ids.Add
            ids
        else

        use connection = connection.Get()

        use transaction = connection.BeginTransaction()
        try
            let ids = List<SqlId>()
            for item in items do
                Helper.insertInner this.IncludeType item connection transaction name |> ids.Add

            transaction.Commit()
            ids
        with ex ->
            transaction.Rollback()
            reraise()

    member this.TryGetById(id: SqlId) =
        use connection = connection.Get()
        match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {|id = id.Value|}) with
        | json when Object.ReferenceEquals(json, null) -> None
        | json -> fromDapper<'T> json |> Some

    member this.GetById(id: SqlId) =
        match this.TryGetById id with
        | None -> failwithf "There is no element with id %i" id.Value
        | Some x -> x

    member this.Select<'R>(select: Expression<System.Func<'T, 'R>>) =
        let selectSQL, variables = QueryTranslator.translate name select

        WhereBuilder<'T, DbObjectRow, 'R>(connection, name, $"SELECT Id, {selectSQL} as ValueJSON FROM \"{name}\" ", fromDapper<'R>, variables, id)

    member this.SelectUnique<'R>(select: Expression<System.Func<'T, 'R>>) =
        let selectSQL, variables = QueryTranslator.translate name select

        WhereBuilder<'T, 'R, 'R>(connection, name, $"SELECT DISTINCT {selectSQL} FROM \"{name}\" ", fromDapper<'R>, variables, id)

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
        // https://stackoverflow.com/questions/1824490/how-do-you-enable-limit-for-delete-in-sqlite
        // By default, SQLite does not support LIMIT in a COUNT statement, but there is this workaround.
        FinalBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM (SELECT Id FROM \"{name}\" LIMIT @limit)", Dictionary<string, obj>([|KeyValuePair("limit", limit :> obj)|]), fromJsonOrSQL<int64>, id, None).First()

    member this.CountWhere(func: Expression<System.Func<'T, bool>>) =
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), id).Where(func).First()

    member this.CountWhere(func: Expression<System.Func<'T, bool>>, limit: uint64) =
        let whereSQL, variables = QueryTranslator.translate name func
        variables["limit"] <- limit :> obj

        FinalBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM (SELECT Id FROM \"{name}\" WHERE {whereSQL} LIMIT @limit)", variables, fromJsonOrSQL<int64>, id, None).First()

    member this.Any(func) = this.CountWhere(func, 1UL) > 0L

    member this.Any() = this.CountWhere((fun u -> true), 1UL) > 0L

    member this.Update(expression: Expression<System.Action<'T>>) =
        let updateSQL, variables = QueryTranslator.translateUpdateMode name expression
        let updateSQL = updateSQL.Trim ','

        WhereBuilder<'T, string, int64>(connection, name, $"UPDATE \"{name}\" SET Value = jsonb_set(Value, {updateSQL})", fromJsonOrSQL<int64>, variables, id)

    member this.Replace(item: 'T) =
        WhereBuilder<'T, string, int64>(connection, name, $"UPDATE \"{name}\" SET Value = jsonb(@item)", fromJsonOrSQL<int64>, Dictionary([|KeyValuePair("item", (if this.IncludeType then toTypedJson item else toJson item) |> box)|]), id)

    member this.Update(item: 'T) =
        match item :> obj with
        | :? 'T as value -> this.Replace(value).WhereId(Dynamic.InvokeConvert(item?Id, typeof<SqlId>, false) :?> SqlId).Execute() |> ignore
        | other -> failwithf "Cannot insert a %s in a %s Collection, but if you want it use a UntypedCollection with the name '%s'." (other.GetType().FullName) (typeof<'T>.FullName) (name)

    member this.Delete() =
        // https://stackoverflow.com/questions/1824490/how-do-you-enable-limit-for-delete-in-sqlite
        // By default, SQLite does not support LIMIT in a DELETE statement, but there is this workaround.
        WhereBuilder<'T, string, int64>(connection, name, $"DELETE FROM \"{name}\" WHERE Id IN (SELECT Id FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), fun sql -> sql + ")")

    member this.DeleteById(id: SqlId) : int =
        this.Delete().WhereId(id).Execute()

    member private this.GetIndexWhereAndName(expression: Expression<System.Func<'T, 'R>>)  =
        let whereSQL, variables = QueryTranslator.translate name expression
        let whereSQL = whereSQL.Replace($"{name}.Value", "Value") // {name}.Value is not allowed in an index.
        if variables.Count > 0 then failwithf "Cannot have variables in index."
        let expressionBody = expression.Body

        if QueryTranslator.isAnyConstant expressionBody then failwithf "Cannot index an outside expression."

        let whereSQL =
            match expressionBody with
            | :? NewExpression as ne when ne.Type.IsAssignableTo(typeof<System.Runtime.CompilerServices.ITuple>)
                -> failwithf "Cannot index an tuple expression, please use multiple indexes." 
            | :? MemberExpression as me ->                
                $"({whereSQL})"
            | other -> failwithf "Cannot index an expression with type: %A" (other.GetType())

        let expressionStr = whereSQL.ToCharArray() |> Seq.filter(fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"
        indexName, whereSQL

    member this.EnsureIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = this.GetIndexWhereAndName expression

        let indexSQL = $"CREATE INDEX IF NOT EXISTS {indexName} ON {name}{whereSQL}"

        use connection = connection.Get()
        connection.Execute(indexSQL)

    member this.DropIndexIfExists<'R>(expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = this.GetIndexWhereAndName expression

        let indexSQL = $"DROP INDEX IF EXISTS {indexName}"

        use connection = connection.Get()
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

type TransactionalSoloDB internal (connection: TransactionalConnection) =
    let connectionString = connection.ConnectionString

    member private this.InitializeCollection<'T> name =
        use mutex = Helper.lockTable connectionString name // To prevent a race condition where the next if statment is true for 2 threads.
        if not (Helper.existsCollection name connection) then 
            Helper.createTableInner name connection null

        Collection<'T>(Transactional connection, name, connectionString)

    member this.GetCollection<'T>() =
        let name = Helper.getNameFrom<'T>()
        
        this.InitializeCollection<'T>(name)

    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.formatName
        
        this.InitializeCollection<obj>(name)

    member this.ExistCollection name =
        Helper.existsCollection name connection

    member this.ExistCollection<'T>() =
        let name = Helper.getNameFrom<'T>()
        Helper.existsCollection name connection

    member this.DropCollectionIfExists name =
        use mutex = Helper.lockTable connectionString name

        if Helper.existsCollection name connection then
            Helper.dropCollection name null connection
            true
        else false

    member this.DropCollectionIfExists<'T>() =
        let name = Helper.getNameFrom<'T>()
        this.DropCollectionIfExists name

    member this.DropCollection<'T>() =
        if this.DropCollectionIfExists<'T>() = false then
            let name = Helper.getNameFrom<'T>()
            failwithf "Collection %s does not exists." name

    member this.DropCollection name =
        if this.DropCollectionIfExists name = false then
            failwithf "Collection %s does not exists." name

    member this.ListCollectionNames() =
        connection.Query<string>("SELECT Name FROM SoloDBCollections")

type internal SoloDBLocation =
| File of string
| Memory of string

type SoloDB private (connectionManager: ConnectionManager, connectionString: string, location: SoloDBLocation) = 
    static do
        // Dapper mapping.
        SqlMapper.AddTypeHandler(typeof<SqlId>, AccountTypeHandler())
        SqlMapper.RemoveTypeMap(typeof<DateTimeOffset>)
        SqlMapper.AddTypeHandler<DateTimeOffset> (DateTimeMapper())

    let mutable disposed = false

    member this.ConnectionString = connectionString
    member internal this.DataLocation = location
    member this.FileSystem = FileSystem(connectionManager)

    member private this.GetNewConnection() = connectionManager.Borrow()
        
    member private this.InitializeCollection<'T> name =     
        if disposed then raise (ObjectDisposedException(nameof(SoloDB)))

        use connection = connectionManager.Borrow()

        use mutex = Helper.lockTable connectionString name // To prevent a race condition where the next if statment is true for 2 threads.
        if not (Helper.existsCollection name connection) then 
            use transaction = connection.BeginTransaction()
            try
                Helper.createTableInner name connection transaction
                transaction.Commit()
            with ex ->
                transaction.Rollback()
                reraise ()

        Collection<'T>(Pooled connectionManager, name, connectionString)

    member this.GetCollection<'T>() =
        let name = Helper.getNameFrom<'T>()
        
        this.InitializeCollection<'T>(name)

    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.formatName
        
        this.InitializeCollection<obj>(name)

    member this.ExistCollection name =
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    member this.ExistCollection<'T>() =
        let name = Helper.getNameFrom<'T>()
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    member this.DropCollectionIfExists name =
        use mutex = Helper.lockTable connectionString name

        use dbConnection = connectionManager.Borrow()

        if Helper.existsCollection name dbConnection then
            use transaction = dbConnection.BeginTransaction()
            try
                Helper.dropCollection name transaction dbConnection
                transaction.Commit()
                true
            with ex -> 
                transaction.Rollback()
                reraise()
        else false

    member this.DropCollectionIfExists<'T>() =
        let name = Helper.getNameFrom<'T>()
        this.DropCollectionIfExists name

    member this.DropCollection<'T>() =
        if this.DropCollectionIfExists<'T>() = false then
            let name = Helper.getNameFrom<'T>()
            failwithf "Collection %s does not exists." name

    member this.DropCollection name =
        if this.DropCollectionIfExists name = false then
            failwithf "Collection %s does not exists." name

    member this.ListCollectionNames() =
        use dbConnection = connectionManager.Borrow()
        dbConnection.Query<string>("SELECT Name FROM SoloDBCollections")

    member this.BackupTo(otherDb: SoloDB) =
        use dbConnection = connectionManager.Borrow()
        use otherConnection = otherDb.GetNewConnection()
        dbConnection.BackupDatabase otherConnection

    member this.BackupVacuumTo(location: string) =
        match this.DataLocation with
        | Memory _ -> failwithf "Cannot vaccuum backuo from or to memory."
        | other ->

        let location = Path.GetFullPath location
        if File.Exists location then File.Delete location

        use dbConnection = connectionManager.Borrow()
        dbConnection.Execute($"VACUUM INTO '{location}'")

    member this.Transactionally<'R>(func: Func<TransactionalSoloDB, 'R>) : 'R =        
        use connectionForTransaction = connectionManager.CreateForTransaction()
        try
            connectionForTransaction.Open()
            connectionForTransaction.Execute("BEGIN;") |> ignore

            let transactionalDb = new TransactionalSoloDB(connectionForTransaction)
            

            try
                let ret = func.Invoke transactionalDb
                connectionForTransaction.Execute "COMMIT;" |> ignore
                ret
            with ex -> 
                connectionForTransaction.Execute "ROLLBACK;" |> ignore
                reraise()
        finally connectionForTransaction.DisposeReal(true)

    member this.Dispose() =
        disposed <- true
        (connectionManager :> IDisposable).Dispose()

    interface IDisposable with
        member this.Dispose() = this.Dispose()

    static member Instantiate (source: string) =        
        let connectionString, location =
            if source.StartsWith("memory:", StringComparison.InvariantCultureIgnoreCase) then
                let memoryName = source.Substring "memory:".Length
                let memoryName = memoryName.Trim()
                sprintf "Data Source=%s;Mode=Memory;Cache=Shared" memoryName, Memory memoryName
            else 
                let source = Path.GetFullPath source
                $"Data Source={source}", File source

        let setup (connection: SqliteConnection) =
            connection.CreateFunction("UNIXTIMESTAMP", Func<int64>(fun () -> DateTimeOffset.Now.ToUnixTimeMilliseconds()), false)
            connection.CreateFunction("SHA_HASH", Func<byte array, obj>(fun o -> Utils.shaHash o), true)

        let manager = new ConnectionManager(connectionString, setup)

        use dbConnection = manager.Borrow()

        dbConnection.Execute("
                            PRAGMA journal_mode=wal;
                            CREATE TABLE IF NOT EXISTS SoloDBCollections (Name TEXT NOT NULL) STRICT;
                            CREATE INDEX IF NOT EXISTS SoloDBCollectionsNameIndex ON SoloDB(Name);
                            ") |> ignore

        dbConnection.Execute("
                            CREATE TABLE IF NOT EXISTS DirectoryHeader (
                                Id INTEGER PRIMARY KEY,
                                Name TEXT NOT NULL CHECK ((length(Name) != 0 OR ParentId IS NULL) AND (Name != \".\") AND (Name != \"..\") AND NOT Name GLOB \"*/*\"),
                                FullPath TEXT NOT NULL CHECK (FullPath != \"\" AND NOT FullPath GLOB \"*/./*\" AND NOT FullPath GLOB \"*/../*\"),
                                ParentId INTEGER,
                                Created INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                                Modified INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                                FOREIGN KEY (ParentId) REFERENCES DirectoryHeader(Id) ON DELETE CASCADE,
                                UNIQUE(ParentId, Name),
                                UNIQUE(FullPath)
                            ) STRICT;

                            CREATE TABLE IF NOT EXISTS FileHeader (
                                Id INTEGER PRIMARY KEY,
                                Name TEXT NOT NULL CHECK (length(Name) != 0 AND Name != \".\" AND Name != \"..\"),
                                DirectoryId INTEGER NOT NULL,
                                Created INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                                Modified INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                                Length INTEGER NOT NULL DEFAULT 0,
                                Hash BLOB NOT NULL DEFAULT (SHA_HASH('')),
                                FOREIGN KEY (DirectoryId) REFERENCES DirectoryHeader(Id) ON DELETE CASCADE,
                                UNIQUE(DirectoryId, Name)
                            ) STRICT;

                            CREATE TABLE IF NOT EXISTS FileChunk (
                                FileId INTEGER NOT NULL,
                                Number INTEGER NOT NULL,
                                Data BLOB NOT NULL,
                                FOREIGN KEY (FileId) REFERENCES FileHeader(Id) ON DELETE CASCADE,
                                UNIQUE(FileId, Number) ON CONFLICT REPLACE
                            ) STRICT;

                            CREATE TABLE IF NOT EXISTS FileMetadata (
                                Id INTEGER PRIMARY KEY,
                                FileId INTEGER NOT NULL,
                                Key TEXT NOT NULL,
                                Value TEXT NOT NULL,
                                UNIQUE(FileId, Key) ON CONFLICT REPLACE,
                                FOREIGN KEY (FileId) REFERENCES FileHeader(Id) ON DELETE CASCADE
                            ) STRICT;

                            CREATE TABLE IF NOT EXISTS DirectoryMetadata (
                                Id INTEGER PRIMARY KEY,
                                DirectoryId INTEGER NOT NULL,
                                Key TEXT NOT NULL,
                                Value TEXT NOT NULL,
                                UNIQUE(DirectoryId, Key) ON CONFLICT REPLACE,
                                FOREIGN KEY (DirectoryId) REFERENCES DirectoryHeader(Id) ON DELETE CASCADE
                            ) STRICT;

                            CREATE INDEX IF NOT EXISTS SoloDBDirectoryHeaderNameAndParentIdIndex ON DirectoryHeader(Name, ParentId);
                            CREATE UNIQUE INDEX IF NOT EXISTS SoloDBDirectoryHeaderFullPathIndex ON DirectoryHeader(FullPath);
                            CREATE UNIQUE INDEX IF NOT EXISTS SoloDBDirectoryMetadataDirectoryIdAndKey ON DirectoryMetadata(DirectoryId, Key);

                            CREATE INDEX IF NOT EXISTS SoloDBFileHeaderNameAndDirectoryIdIndex ON FileHeader(Name, DirectoryId);
                            CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileChunkFileIdAndNumberIndex ON FileChunk(FileId, Number);
                            CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileMetadataFileIdAndKey ON FileMetadata(FileId, Key);
                            CREATE INDEX IF NOT EXISTS SoloDBFileHashIndex ON FileHeader(Hash);

                            ")|> ignore

        use t = dbConnection.BeginTransaction(false)
        try
            let rootDir = dbConnection.Query<SqlId>("SELECT Id FROM DirectoryHeader WHERE ParentId IS NULL AND length(Name) = 0") |> Seq.tryHead
            if rootDir.IsNone then
                dbConnection.Execute("INSERT INTO DirectoryHeader(Name, ParentId, FullPath) VALUES (\"\", NULL, \"/\");", transaction = t) |> ignore
            t.Commit()
        with e -> 
            t.Rollback()
            reraise()
        
        new SoloDB(manager, connectionString, location)

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
    static member update<'T> (item: 'T) (collection: Collection<'T>) = collection.Update item
    static member replace<'T> (item: 'T) (collection: Collection<'T>) = collection.Replace item

    static member delete<'T> (collection: Collection<'T>) = collection.Delete()
    static member deleteById<'T> id (collection: Collection<'T>) = collection.DeleteById id

    static member select<'T, 'R> (func: Expression<System.Func<'T, 'R>>) = fun (collection: Collection<'T>) -> collection.Select func
    static member select<'T> () = fun (collection: Collection<'T>) -> collection.Select()
    static member selectUnique<'T, 'R> (func: Expression<System.Func<'T, 'R>>) = fun (collection: Collection<'T>) -> collection.SelectUnique func

    static member where<'a, 'b, 'c> (func: Expression<System.Func<'a, bool>>) = fun (builder: WhereBuilder<'a, 'b, 'c>) -> builder.Where func
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

    static member any<'T> (func: Expression<System.Func<'T, bool>>) = fun (collection: Collection<'T>) -> func |> collection.Any