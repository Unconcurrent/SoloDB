namespace SoloDatabase

open Microsoft.Data.Sqlite
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open FSharp.Interop.Dynamic
open SoloDatabase.Types
open System.IO
open System.Text
open SQLiteTools
open JsonFunctions
open FileStorage
open Connections
open Utils

module internal Helper =
    let internal lockTable (connectionStr: string) (name: string) =
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(connectionStr)}-Table-{name}")
        mutex

    let internal createTableInner (name: string) (conn: SqliteConnection) =
        conn.Execute($"CREATE TABLE \"{name}\" (
    	        Id INTEGER NOT NULL PRIMARY KEY UNIQUE,
    	        Value JSONB NOT NULL
            );", {|name = name|}) |> ignore
        conn.Execute("INSERT INTO SoloDBCollections(Name) VALUES (@name)", {|name = name|}) |> ignore

    let internal existsCollection (name: string) (connection: SqliteConnection)  =
        connection.QueryFirstOrDefault<string>("SELECT Name FROM SoloDBCollections WHERE Name = @name LIMIT 1", {|name = name|}) <> null

    let internal dropCollection (name: string) (connection: SqliteConnection) =
        connection.Execute(sprintf "DROP TABLE IF EXISTS \"%s\"" name) |> ignore
        connection.Execute("DELETE FROM SoloDBCollections Where Name = @name", {|name = name|}) |> ignore

    let private insertImpl (typed: bool) (item: 'T) (connection: SqliteConnection) (name: string) (orReplace: bool) =
        let json = if typed then toTypedJson item else toJson item

        let existsId = hasIdType typeof<'T>

        if existsId && 0L <> item?Id then 
            failwithf "Cannot insert a item with a non zero Id, maybe you meant Update?"

        let orReplaceText = "OR REPLACE"
        let id = connection.QueryFirst<int64>($"INSERT {if orReplace then orReplaceText else String.Empty} INTO \"{name}\"(Value) VALUES(jsonb(@jsonText)) RETURNING Id;", {|name = name; jsonText = json|})

        if existsId then 
            item?Id <- id

        id

    let internal insertInner (typed: bool) (item: 'T) (connection: SqliteConnection) (name: string) =
        insertImpl typed item connection name false

    let internal insertOrReplaceInner (typed: bool) (item: 'T) (connection: SqliteConnection) (name: string) =
        insertImpl typed item connection name true

    let internal formatName (name: string) =
        String(name.ToCharArray() |> Array.filter(fun c -> Char.IsLetterOrDigit c || c = '_')) // Anti SQL injection

    let internal getNameFrom<'T>() =
        typeof<'T>.Name |> formatName

    let internal createDict(items: KeyValuePair<'a, 'b> seq) =
        let dic = new Dictionary<'a, 'b>()
        
        for kvp in items do
            dic.Add(kvp.Key, kvp.Value)

        dic

[<Struct>]
type FinalBuilder<'T, 'Q, 'R>(connection: Connection, name: string, sqlP: string, variablesP: Dictionary<string, obj>, select: 'Q -> 'R, postModifySQL: string -> string, limitP: uint64 option, ?offsetP: uint64, ?orderByListP: string list) =
    member private this.SQLText = sqlP
    member private this.SQLLimit: uint64 Option = limitP
    member private this.SQLOffset = match offsetP with Some x -> x | None -> 0UL
    member private this.OrderByList: string list = match orderByListP with | Some x -> x | None -> []
    member private this.Variables = variablesP

    member private this.getQueryParameters() =
        let variables = this.Variables
        let variables = seq {for key in variables.Keys do KeyValuePair<string, obj>(key, variables.[key])} |> Seq.toList
        let parameters = new Dictionary<string, obj>()

        for kvp in variables do
            parameters.Add(kvp.Key, kvp.Value)

        let finalSQL = 
            this.SQLText 
            + (if this.OrderByList.Length > 0 then sprintf "ORDER BY %s " (this.OrderByList |> String.concat ",") else " ") 
            + (if this.SQLLimit.IsSome then $"LIMIT {this.SQLLimit.Value} " else if this.SQLOffset > 0UL then "LIMIT -1 " else "")
            + (if this.SQLOffset > 0UL then $"OFFSET {this.SQLOffset} " else "")

        let finalSQL = postModifySQL finalSQL

        finalSQL, parameters

    member this.Limit(?count: uint64) =
        FinalBuilder<'T, 'Q, 'R>(connection, name, this.SQLText, this.Variables, select, postModifySQL, count, this.SQLOffset, this.OrderByList)

    member this.Offset(index: uint64) =
        FinalBuilder<'T, 'Q, 'R>(connection, name, this.SQLText, this.Variables, select, postModifySQL, this.SQLLimit, index, this.OrderByList)

    member this.OrderByAsc(expression: Expression<System.Func<'T, obj>>) =
        let orderSelector, _ = QueryTranslator.translate name expression
        let orderSQL = sprintf "(%s) ASC" orderSelector

        FinalBuilder<'T, 'Q, 'R>(connection, name, this.SQLText, this.Variables, select, postModifySQL, this.SQLLimit, this.SQLOffset, this.OrderByList @ [orderSQL])

    member this.OrderByDesc(expression: Expression<System.Func<'T, obj>>) =
        let orderSelector, _ = QueryTranslator.translate name expression
        let orderSQL = sprintf "(%s) DESC" orderSelector

        FinalBuilder<'T, 'Q, 'R>(connection, name, this.SQLText, this.Variables, select, postModifySQL, this.SQLLimit, this.SQLOffset, this.OrderByList @ [orderSQL])

    member this.Enumerate() =
        let finalSQL, parameters = this.getQueryParameters()
        let connection = connection // Cannot access 'this.' in the following builder, because 'this.' is a struct.
        let select = select

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
        let finalSQL, parameters = this.getQueryParameters()
        use connection = connection.Get()
        connection.QueryFirst<'Q>(finalSQL, parameters) |> select

    member this.Execute() =
        let finalSQL, parameters = this.getQueryParameters()
        use connection = connection.Get()
        connection.Execute(finalSQL, parameters)

    member this.ExplainQueryPlan() =
        // EXPLAIN QUERY PLAN 
        let finalSQL, parameters = this.getQueryParameters()
        let finalSQL = "EXPLAIN QUERY PLAN " + finalSQL

        use connection = connection.Get()
        connection.Query<obj>(finalSQL, parameters)
        |> Seq.map(fun arr -> (arr :?> IDictionary<string, obj>).["detail"].ToString())
        |> String.concat ";\n"

    member this.ToList() =
        Utils.CompatilibilityList<'R>(this.Enumerate())

[<Struct>]
type WhereBuilder<'T, 'Q, 'R>(connection: Connection, name: string, sql: string, select: 'Q -> 'R, vars: Dictionary<string, obj>, postModifySQL: string -> string) =
    member this.Where(expression: Expression<System.Func<'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, newVariables, select, postModifySQL, None)

    member this.Where(expression: Expression<System.Func<int64, 'T, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translate name expression
        let sql = sql + sprintf "WHERE %s " whereSQL

        for var in vars do
            newVariables.Add(var.Key, var.Value)

        FinalBuilder<'T, 'Q, 'R>(connection, name, sql, newVariables, select, postModifySQL, None)

    member this.WhereId(id: int64) =
        FinalBuilder<'T, 'Q, 'R>(connection, name, sql + sprintf "WHERE Id = %i " id, vars, select, postModifySQL, None)

    member this.WhereId(func: Expression<System.Func<int64, bool>>) =
        let whereSQL, newVariables = QueryTranslator.translateWithId name func 0
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

        Helper.insertInner this.IncludeType item connection name

    /// <summary>
    /// Will insert or replace the item in the DB based on its UNIQUE INDEXES, throwing if the Id is non zero.
    /// </summary>
    member this.InsertOrReplace (item: 'T) =
        use connection = connection.Get()

        Helper.insertOrReplaceInner this.IncludeType item connection name

    member this.InsertBatch (items: 'T seq) =
        if this.InTransaction then
            use connection = connection.Get()
            let ids = List<int64>()
            for item in items do
                Helper.insertInner this.IncludeType item connection name |> ids.Add
            ids
        else

        use connection = connection.Get()
        connection.Execute "BEGIN;" |> ignore
        try
            let ids = List<int64>()
            for item in items do
                Helper.insertInner this.IncludeType item connection name |> ids.Add

            connection.Execute "COMMIT;" |> ignore
            ids
        with ex ->
            connection.Execute "ROLLBACK;" |> ignore
            reraise()

    /// <summary>
    /// Will insert or replace the items in the DB based on its UNIQUE INDEXES, throwing if the Id is non zero.
    /// </summary>
    member this.InsertOrReplaceBatch (items: 'T seq) =
        if this.InTransaction then
            use connection = connection.Get()
            let ids = List<int64>()
            for item in items do
                Helper.insertOrReplaceInner this.IncludeType item connection name |> ids.Add
            ids
        else

        use connection = connection.Get()
        connection.Execute "BEGIN;" |> ignore
        try
            let ids = List<int64>()
            for item in items do
                Helper.insertOrReplaceInner this.IncludeType item connection name |> ids.Add

            connection.Execute "COMMIT;" |> ignore
            ids
        with ex ->
            connection.Execute "ROLLBACK;" |> ignore
            reraise()

    member this.TryGetById(id: int64) =
        use connection = connection.Get()
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

    member this.SelectUnique<'R>(select: Expression<System.Func<'T, 'R>>) =
        let selectSQL, variables = QueryTranslator.translate name select

        WhereBuilder<'T, 'R, 'R>(connection, name, $"SELECT DISTINCT {selectSQL} FROM \"{name}\" ", fromDapper<'R>, variables, id)

    member this.Select() =
        WhereBuilder<'T, DbObjectRow, 'T>(connection, name, $"SELECT Id, json(Value) as ValueJSON FROM \"{name}\" ", fromDapper<'T>, Dictionary<string, obj>(), id)

    member this.SelectWithId() =
        WhereBuilder<'T, string, (int64 * 'T)>(connection, name, $"SELECT json_object('Id', Id, 'Value', Value) FROM \"{name}\" ", fromIdJson<'T>, Dictionary<string, obj>(), id)

    member this.TryFirst(func: Expression<System.Func<'T, bool>>) =
        this.Select().Where(func).Limit(1UL).Enumerate() |> Seq.tryHead

    member this.Where(f: Expression<Func<'T, bool>>) = this.Select().Where(f)

    member this.ToList() = this.Select().OnAll().ToList()

    member this.Enumerate() = this.Select().OnAll().Enumerate()

    interface IEnumerable<'T> with
        override this.GetEnumerator() =
            this.Enumerate().GetEnumerator()

        override this.GetEnumerator() =
            this.Enumerate().GetEnumerator() :> IEnumerator

    member this.Count() =
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), id)

    member this.CountAll() =        
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), id).OnAll().First()

    member this.CountAllLimit(limit: uint64) =
        // https://stackoverflow.com/questions/1824490/how-do-you-enable-limit-for-delete-in-sqlite
        // By default, SQLite does not support LIMIT in a COUNT statement, but there is this workaround.
        FinalBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM (SELECT Id FROM \"{name}\" LIMIT @limit)", Helper.createDict([|KeyValuePair("limit", limit :> obj)|]), fromJsonOrSQL<int64>, id, None).First()

    member this.CountWhere(func: Expression<System.Func<'T, bool>>) =
        WhereBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), id).Where(func).First()

    member this.CountWhere(func: Expression<System.Func<'T, bool>>, limit: uint64) =
        let whereSQL, variables = QueryTranslator.translate name func
        variables["limit"] <- limit :> obj

        FinalBuilder<'T, string, int64>(connection, name, $"SELECT COUNT(*) FROM (SELECT Id FROM \"{name}\" WHERE {whereSQL} LIMIT @limit)", variables, fromJsonOrSQL<int64>, id, None).First()

    member this.Any(func) = this.CountWhere(func, 1UL) > 0L

    member this.Any() = this.CountWhere((fun u -> true), 1UL) > 0L

    member this.Update([<ParamArray>] expressions: Expression<System.Action<'T>> array) =
        let variables = Dictionary<string, obj>()
        let fullUpdateSQL = StringBuilder()
        for expression in expressions do
            let updateSQL, variablesToAdd = QueryTranslator.translateUpdateMode name expression
            fullUpdateSQL.Append updateSQL |> ignore
            for kvp in variablesToAdd do
                variables.Add(kvp.Key, kvp.Value)

        fullUpdateSQL.Remove(fullUpdateSQL.Length - 1, 1) |> ignore // Remove the ',' at the end.
        let fullUpdateSQL = fullUpdateSQL.ToString()

        WhereBuilder<'T, string, int64>(connection, name, $"UPDATE \"{name}\" SET Value = jsonb_set(Value, {fullUpdateSQL})", fromJsonOrSQL<int64>, variables, id)

    member this.Replace(item: 'T) =
        WhereBuilder<'T, string, int64>(connection, name, $"UPDATE \"{name}\" SET Value = jsonb(@item)", fromJsonOrSQL<int64>, Helper.createDict([|KeyValuePair("item", (if this.IncludeType then toTypedJson item else toJson item) |> box)|]), id)

    /// <summary>
    /// Will replace the item in the DB based on its Id, and if it does not have one then it will throw an InvalidOperationException.
    /// </summary>
    member this.Update(item: 'T) =
        if not (hasIdType typeof<'T>) then
            raise (InvalidOperationException $"The item's type {typeof<'T>.Name} does not have a int64 Id property to use in the update process.")

        this.Replace(item).WhereId(item?Id |> box :?> int64).Execute() |> ignore

    member this.Delete() =
        // https://stackoverflow.com/questions/1824490/how-do-you-enable-limit-for-delete-in-sqlite
        // By default, SQLite does not support LIMIT in a DELETE statement, but there is this workaround.
        WhereBuilder<'T, string, int64>(connection, name, $"DELETE FROM \"{name}\" WHERE Id IN (SELECT Id FROM \"{name}\" ", fromJsonOrSQL<int64>, Dictionary<string, obj>(), fun sql -> sql + ")")

    member this.DeleteById(id: int64) : int =
        this.Delete().WhereId(id).Execute()

    member private this.GetIndexWhereAndName(expression: Expression<System.Func<'T, 'R>>)  =
        let whereSQL, variables = QueryTranslator.translate name expression
        let whereSQL = whereSQL.Replace($"{name}.Value", "Value") // {name}.Value is not allowed in an index.
        if whereSQL.Contains $"{name}.Id" then failwithf "The Id of a collection is always stored in an index."
        if variables.Count > 0 then failwithf "Cannot have variables in index."
        let expressionBody = expression.Body

        if QueryTranslator.isAnyConstant expressionBody then failwithf "Cannot index an outside or constant expression."

        let whereSQL =
            match expressionBody with
            | :? NewExpression as ne when isTuple ne.Type
                -> whereSQL.Substring("json_array".Length)
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

    member this.EnsureUniqueAndIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = this.GetIndexWhereAndName expression

        let indexSQL = $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON {name}{whereSQL}"

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

    member this.Connection = connection

    member private this.InitializeCollection<'T> name =
        if not (Helper.existsCollection name connection) then 
            Helper.createTableInner name connection

        Collection<'T>(Transactional connection, name, connectionString)

    member this.GetCollection<'T>() =
        let name = Helper.getNameFrom<'T>()
        
        this.InitializeCollection<'T>(name)

    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.formatName
        
        this.InitializeCollection<obj>(name)

    member this.CollectionExists name =
        Helper.existsCollection name connection

    member this.CollectionExists<'T>() =
        let name = Helper.getNameFrom<'T>()
        Helper.existsCollection name connection

    member this.DropCollectionIfExists name =
        use mutex = Helper.lockTable connectionString name

        if Helper.existsCollection name connection then
            Helper.dropCollection name connection
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

[<Struct>]
type internal SoloDBLocation =
| File of filePath: string
| Memory of name: string

type SoloDB private (connectionManager: ConnectionManager, connectionString: string, location: SoloDBLocation) = 
    let mutable disposed = false

    new(source: string) =
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

        do
            use dbConnection = manager.Borrow()

            let initCommands = [|"
                                PRAGMA journal_mode=wal;
                                PRAGMA page_size=16384;

                                BEGIN EXCLUSIVE;

                                CREATE TABLE IF NOT EXISTS SoloDBCollections (Name TEXT NOT NULL) STRICT;
                                

                                CREATE TABLE IF NOT EXISTS SoloDBDirectoryHeader (
                                    Id INTEGER PRIMARY KEY,
                                    Name TEXT NOT NULL CHECK ((length(Name) != 0 OR ParentId IS NULL) AND (Name != \".\") AND (Name != \"..\") AND NOT Name GLOB \"*/*\"),
                                    FullPath TEXT NOT NULL CHECK (FullPath != \"\" AND NOT FullPath GLOB \"*/./*\" AND NOT FullPath GLOB \"*/../*\"),
                                    ParentId INTEGER,
                                    Created INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                                    Modified INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                                    FOREIGN KEY (ParentId) REFERENCES SoloDBDirectoryHeader(Id) ON DELETE CASCADE,
                                    UNIQUE(ParentId, Name),
                                    UNIQUE(FullPath)
                                ) STRICT;

                                CREATE TABLE IF NOT EXISTS SoloDBFileHeader (
                                    Id INTEGER PRIMARY KEY,
                                    Name TEXT NOT NULL CHECK (length(Name) != 0 AND Name != \".\" AND Name != \"..\"),
                                    FullPath TEXT NOT NULL CHECK (FullPath != \"\" AND NOT FullPath GLOB \"*/./*\" AND NOT FullPath GLOB \"*/../*\"),
                                    DirectoryId INTEGER NOT NULL,
                                    Created INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                                    Modified INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                                    Length INTEGER NOT NULL DEFAULT 0,
                                    Hash BLOB NOT NULL DEFAULT (SHA_HASH('')),
                                    FOREIGN KEY (DirectoryId) REFERENCES SoloDBDirectoryHeader(Id) ON DELETE CASCADE,
                                    UNIQUE(DirectoryId, Name)
                                ) STRICT;

                                CREATE TABLE IF NOT EXISTS SoloDBFileChunk (
                                    FileId INTEGER NOT NULL,
                                    Number INTEGER NOT NULL,
                                    Data BLOB NOT NULL,
                                    FOREIGN KEY (FileId) REFERENCES SoloDBFileHeader(Id) ON DELETE CASCADE,
                                    UNIQUE(FileId, Number) ON CONFLICT REPLACE
                                ) STRICT;

                                CREATE TABLE IF NOT EXISTS SoloDBFileMetadata (
                                    Id INTEGER PRIMARY KEY,
                                    FileId INTEGER NOT NULL,
                                    Key TEXT NOT NULL,
                                    Value TEXT NOT NULL,
                                    UNIQUE(FileId, Key) ON CONFLICT REPLACE,
                                    FOREIGN KEY (FileId) REFERENCES SoloDBFileHeader(Id) ON DELETE CASCADE
                                ) STRICT;

                                CREATE TABLE IF NOT EXISTS SoloDBDirectoryMetadata (
                                    Id INTEGER PRIMARY KEY,
                                    DirectoryId INTEGER NOT NULL,
                                    Key TEXT NOT NULL,
                                    Value TEXT NOT NULL,
                                    UNIQUE(DirectoryId, Key) ON CONFLICT REPLACE,
                                    FOREIGN KEY (DirectoryId) REFERENCES SoloDBDirectoryHeader(Id) ON DELETE CASCADE
                                ) STRICT;

                                COMMIT TRANSACTION;
                                "
                                ;
                                "
                                BEGIN EXCLUSIVE;


                                -- Trigger to update the Modified column on insert for SoloDBDirectoryHeader
                                CREATE TRIGGER IF NOT EXISTS Insert_SoloDBDirectoryHeader
                                AFTER INSERT ON SoloDBDirectoryHeader
                                FOR EACH ROW
                                BEGIN
                                    -- Update parent directory's Modified timestamp, if ParentId is NULL then it will be a noop.
                                    UPDATE SoloDBDirectoryHeader
                                    SET Modified = UNIXTIMESTAMP()
                                    WHERE Id = NEW.ParentId;
                                END;
                                
                                -- Trigger to update the Modified column on update for SoloDBDirectoryHeader
                                CREATE TRIGGER IF NOT EXISTS Update_SoloDBDirectoryHeader
                                AFTER UPDATE ON SoloDBDirectoryHeader
                                FOR EACH ROW
                                WHEN NOT (NEW.Modified <> OLD.Modified OR NEW.Created <> OLD.Created)
                                BEGIN
                                    UPDATE SoloDBDirectoryHeader
                                    SET Modified = UNIXTIMESTAMP()
                                    WHERE Id = NEW.Id;
                                    
                                    -- Update parent directory's Modified timestamp, if ParentId is NULL then it will be a noop.
                                    UPDATE SoloDBDirectoryHeader
                                    SET Modified = UNIXTIMESTAMP()
                                    WHERE Id = NEW.ParentId;
                                END;
                                
                                -- Trigger to update the Modified column on insert for SoloDBFileHeader
                                CREATE TRIGGER IF NOT EXISTS Insert_SoloDBFileHeader
                                AFTER INSERT ON SoloDBFileHeader
                                FOR EACH ROW
                                BEGIN
                                    -- Update parent directory's Modified timestamp
                                    UPDATE SoloDBDirectoryHeader
                                    SET Modified = UNIXTIMESTAMP()
                                    WHERE Id = NEW.DirectoryId;
                                END;
                                
                                -- Trigger to update the Modified column on update for SoloDBFileHeader
                                CREATE TRIGGER IF NOT EXISTS Update_SoloDBFileHeader
                                AFTER UPDATE ON SoloDBFileHeader
                                FOR EACH ROW
                                WHEN (NEW.Hash <> OLD.Hash)
                                BEGIN
                                    UPDATE SoloDBFileHeader
                                    SET Modified = UNIXTIMESTAMP()
                                    WHERE Id = NEW.Id;
                                    
                                    -- Update parent directory's Modified timestamp
                                    UPDATE SoloDBDirectoryHeader
                                    SET Modified = UNIXTIMESTAMP()
                                    WHERE Id = NEW.DirectoryId;
                                END;                 

                                COMMIT TRANSACTION;
                                "
                                ;
                                "
                                BEGIN EXCLUSIVE;


                                CREATE INDEX IF NOT EXISTS SoloDBCollectionsNameIndex ON SoloDBCollections(Name);

                                CREATE INDEX IF NOT EXISTS SoloDBDirectoryHeaderParentIdAndNameIndex ON SoloDBDirectoryHeader(ParentId, Name);
                                CREATE INDEX IF NOT EXISTS SoloDBDirectoryHeaderParentIdIndex ON SoloDBDirectoryHeader(ParentId);
                                CREATE INDEX IF NOT EXISTS SoloDBDirectoryHeaderNameIndex ON SoloDBDirectoryHeader(Name);
                                CREATE UNIQUE INDEX IF NOT EXISTS SoloDBDirectoryHeaderFullPathIndex ON SoloDBDirectoryHeader(FullPath);
                                CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileHeaderFullPathIndex ON SoloDBFileHeader(FullPath);
                                CREATE UNIQUE INDEX IF NOT EXISTS SoloDBDirectoryMetadataDirectoryIdAndKey ON SoloDBDirectoryMetadata(DirectoryId, Key);

                                CREATE INDEX IF NOT EXISTS SoloDBFileHeaderDirectoryIdAndNameIndex ON SoloDBFileHeader(DirectoryId, Name);
                                CREATE INDEX IF NOT EXISTS SoloDBFileHeaderDirectoryIdIndex ON SoloDBFileHeader(DirectoryId);
                                CREATE INDEX IF NOT EXISTS SoloDBFileHeaderNameIndex ON SoloDBFileHeader(Name);
                                CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileChunkFileIdAndNumberIndex ON SoloDBFileChunk(FileId, Number);
                                CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileMetadataFileIdAndKey ON SoloDBFileMetadata(FileId, Key);
                                CREATE INDEX IF NOT EXISTS SoloDBFileHashIndex ON SoloDBFileHeader(Hash);

                                INSERT INTO SoloDBDirectoryHeader (Name, ParentId, FullPath)
                                SELECT '', NULL, '/'
                                WHERE NOT EXISTS (
                                    SELECT 1
                                    FROM SoloDBDirectoryHeader
                                    WHERE ParentId IS NULL AND Name = ''
                                );

                                COMMIT TRANSACTION;
                                "|]
            
            do
                for command in initCommands do
                    use command = new SqliteCommand(command, dbConnection)
                    command.Prepare()
                    let result = command.ExecuteNonQuery()
                    ()
                ()

            let rez = dbConnection.Execute("PRAGMA optimize;")
            ()
        
        new SoloDB(manager, connectionString, location)

    member this.Connection = connectionManager
    member this.ConnectionString = connectionString
    member internal this.DataLocation = location
    member this.FileSystem = FileSystem(connectionManager)

    member private this.GetNewConnection() = connectionManager.Borrow()
        
    member private this.InitializeCollection<'T> name =     
        if disposed then raise (ObjectDisposedException(nameof(SoloDB)))

        use connection = connectionManager.Borrow()

        use mutex = Helper.lockTable connectionString name // To prevent a race condition where the next if statment is true for 2 threads.
        if not (Helper.existsCollection name connection) then 
            let withinTransaction = connection.IsWithinTransaction()
            if not withinTransaction then connection.Execute "BEGIN;" |> ignore
            try
                Helper.createTableInner name connection

                if not withinTransaction then connection.Execute "COMMIT;" |> ignore
            with ex ->
                if not withinTransaction then connection.Execute "ROLLBACK;" |> ignore
                reraise()

        Collection<'T>(Pooled connectionManager, name, connectionString)

    member this.GetCollection<'T>() =
        let name = Helper.getNameFrom<'T>()
        
        this.InitializeCollection<'T>(name)

    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.formatName
        
        this.InitializeCollection<obj>(name)

    member this.CollectionExists name =
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    member this.CollectionExists<'T>() =
        let name = Helper.getNameFrom<'T>()
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    member this.DropCollectionIfExists name =
        use mutex = Helper.lockTable connectionString name

        use connection = connectionManager.Borrow()

        if Helper.existsCollection name connection then
            let withinTransaction = connection.IsWithinTransaction()
            if not withinTransaction then connection.Execute "BEGIN;" |> ignore
            try
                Helper.dropCollection name connection

                if not withinTransaction then connection.Execute "COMMIT;" |> ignore
                true
            with ex ->
                if not withinTransaction then connection.Execute "ROLLBACK;" |> ignore
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

    member this.VacuumTo(location: string) =
        match this.DataLocation with
        | Memory _ -> failwithf "Cannot vacuum backup from or to memory."
        | other ->

        let location = Path.GetFullPath location
        if File.Exists location then File.Delete location

        use dbConnection = connectionManager.Borrow()
        dbConnection.Execute($"VACUUM INTO '{location}'")

    member this.Vacuum() =
        match this.DataLocation with
        | Memory _ -> failwithf "Cannot vacuum memory databases."
        | other ->

        use dbConnection = connectionManager.Borrow()
        dbConnection.Execute($"VACUUM;")

    member this.WithTransaction<'R>(func: Func<TransactionalSoloDB, 'R>) : 'R =        
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

    member this.Optimize() =
        use dbConnection = connectionManager.Borrow()
        dbConnection.Execute "PRAGMA optimize;"

    member this.Dispose() =
        disposed <- true
        (connectionManager :> IDisposable).Dispose()

    interface IDisposable with
        member this.Dispose() = this.Dispose()