namespace SoloDatabase

open SoloDatabase.JsonSerializator
open Microsoft.Data.Sqlite
open System.Linq.Expressions
open System
open System.Collections.Generic
open System.Collections
open SoloDatabase.Types
open System.IO
open System.Text
open SQLiteTools
open JsonFunctions
open FileStorage
open Connections
open Utils
open System.Runtime.CompilerServices
open System.Reflection
open System.Data
open System.Globalization
open System.Linq
open SoloDatabase.Attributes

module internal Helper =
    let internal lockTable (connectionStr: string) (name: string) =
        let mutex = new DisposableMutex($"SoloDB-{StringComparer.InvariantCultureIgnoreCase.GetHashCode(connectionStr)}-Table-{name}")
        mutex

    let internal existsCollection (name: string) (connection: IDbConnection)  =
        connection.QueryFirstOrDefault<string>("SELECT Name FROM SoloDBCollections WHERE Name = @name LIMIT 1", {|name = name|}) <> null

    let internal dropCollection (name: string) (connection: IDbConnection) =
        connection.Execute(sprintf "DROP TABLE IF EXISTS \"%s\"" name) |> ignore
        connection.Execute("DELETE FROM SoloDBCollections Where Name = @name", {|name = name|}) |> ignore

    let private insertJson (orReplace: bool) (id: int64 option) (name: string) (json: string) (connection: IDbConnection) =
        let includeId = id.IsSome

        let queryStringBuilder = StringBuilder(64 + name.Length + (if orReplace then 11 else 0) + (if includeId then 7 else 0))

        let queryString =
            queryStringBuilder
                .Append("INSERT ")
                .Append(if orReplace then "OR REPLACE " else String.Empty)
                .Append(" INTO \"")
                .Append(name)
                .Append("\"(")
                .Append(if includeId then "Id," else String.Empty)
                .Append("Value) VALUES(")
                .Append(if includeId then "@id," else String.Empty)
                .Append("jsonb(@jsonText)) RETURNING Id;")
                .ToString()

        let parameters: obj = 
            if includeId then {|
                name = name
                jsonText = json
                id = id.Value
            |}
            else {|
                name = name
                jsonText = json
            |}


        connection.QueryFirst<int64>(queryString, parameters)

    let private insertImpl<'T when 'T :> obj> (typed: bool) (item: 'T) (connection: IDbConnection) (name: string) (orReplace: bool) (collection: ISoloDBCollection<'T>) =
        let customIdGen = CustomTypeId<'T>.Value
        let existsWritebleDirectId = HasTypeId<'T>.Value


        match customIdGen with
        | Some x ->
            let oldId = x.GetId item
            match x.Generator with
            | :? SoloDatabase.Attributes.IIdGenerator as generator ->
                if generator.IsEmpty oldId then
                    let id = generator.GenerateId collection item
                    x.SetId id item
            | :? SoloDatabase.Attributes.IIdGenerator<'T> as generator ->
                if generator.IsEmpty oldId then
                    let id = generator.GenerateId collection item
                    x.SetId id item
            | other -> raise (InvalidOperationException(sprintf "Invalid Id generator type: %s" (other.GetType().ToString())))
        | None -> ()
        
        let json = if typed then toTypedJson item else toJson item
        

        let id =
            if existsWritebleDirectId && -1L >= HasTypeId<'T>.Read item then 
                failwithf "The Id must be either be:\n a) equal to 0, for it to be replaced by SQLite.\n b) A value greater than 0, for a specific Id to be inserted."
            elif existsWritebleDirectId && 0L <> HasTypeId<'T>.Read item then 
                // Inserting with Id
                insertJson orReplace (Some (HasTypeId<'T>.Read item)) name json connection
            else
                // Inserting without Id
                insertJson orReplace None name json connection

        if existsWritebleDirectId then
            HasTypeId<'T>.Write item id

        id

    let inline internal insertInner (typed: bool) (item: 'T) (connection: IDbConnection) (name: string) (collection: ISoloDBCollection<'T>) =
        insertImpl typed item connection name false collection

    let inline internal insertOrReplaceInner (typed: bool) (item: 'T) (connection: IDbConnection) (name: string) (collection: ISoloDBCollection<'T>) =
        insertImpl typed item connection name true collection

    let internal formatName (name: string) =
        String(name.ToCharArray() |> Array.filter(fun c -> Char.IsLetterOrDigit c || c = '_')) // Anti SQL injection

    let internal createDict(items: KeyValuePair<'a, 'b> seq) =
        let dic = new Dictionary<'a, 'b>()
        
        for kvp in items do
            dic.Add(kvp.Key, kvp.Value)

        dic

    let internal getIndexesFields<'a>() =
        // Get all serializable properties
        typeof<'a>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
        |> Array.choose(
            fun p -> // That have the IndexedAttribute
                match p.GetCustomAttribute<SoloDatabase.Attributes.IndexedAttribute>(true) with
                | a when isNull a -> None
                | a -> Some(p, a)) // With its uniqueness information.

    let internal getIndexWhereAndName<'T, 'R> (name: string) (expression: Expression<System.Func<'T, 'R>>)  =
        let whereSQL, variables = QueryTranslator.translate name expression
        let whereSQL = whereSQL.Replace($"\"{name}\".Value", "Value") // {name}.Value is not allowed in an index.
        if whereSQL.Contains $"\"{name}\".Id" then failwithf "The Id of a collection is always stored in an index."
        if variables.Count > 0 then failwithf "Cannot have variables in index."
        let expressionBody = expression.Body

        if QueryTranslator.isAnyConstant expressionBody then failwithf "Cannot index an outside or constant expression."

        let whereSQL =
            match expressionBody with
            | :? NewExpression as ne when isTuple ne.Type
                -> whereSQL.Substring("json_array".Length)
            | :? MethodCallExpression
            | :? MemberExpression ->
                $"({whereSQL})"
            | other -> failwithf "Cannot index an expression with type: %A" (other.GetType())

        let expressionStr = whereSQL.ToCharArray() |> Seq.filter(fun c -> Char.IsAsciiLetterOrDigit c || c = '_') |> Seq.map string |> String.concat ""
        let indexName = $"{name}_index_{expressionStr}"
        indexName, whereSQL

    let internal ensureIndex<'T, 'R> (collectionName: string) (conn: IDbConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndName<'T,'R> collectionName expression

        let indexSQL = $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"

        conn.Execute(indexSQL)

    let internal ensureUniqueAndIndex<'T,'R> (collectionName: string) (conn: IDbConnection) (expression: Expression<System.Func<'T, 'R>>) =
        let indexName, whereSQL = getIndexWhereAndName<'T,'R> collectionName expression

        let indexSQL = $"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} ON \"{collectionName}\"{whereSQL}"

        conn.Execute(indexSQL)

    let internal ensureDeclaredIndexesFields<'T> (name: string) (conn: IDbConnection) =
        for (pi, indexed) in getIndexesFields<'T>() do
            let ensureIndexesFn = if indexed.Unique then ensureUniqueAndIndex else ensureIndex
            let _code = ensureIndexesFn name conn (ExpressionHelper.get<obj, obj>(fun row -> row.Dyn<obj>(pi.Name)))
            ()

    let internal createTableInner<'T> (name: string) (conn: IDbConnection) =
        conn.Execute($"CREATE TABLE \"{name}\" (
    	        Id INTEGER NOT NULL PRIMARY KEY UNIQUE,
    	        Value JSONB NOT NULL
            );", {|name = name|}) |> ignore
        conn.Execute("INSERT INTO SoloDBCollections(Name) VALUES (@name)", {|name = name|}) |> ignore

        // Ignore the untyped collections.
        if not (typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>) then
            ensureDeclaredIndexesFields<'T> name conn

    let internal collectionNameOf<'T> =
        typeof<'T>.Name |> formatName

type internal Collection<'T>(connection: Connection, name: string, connectionString: string) as this =
    member val private SoloDBQueryable = SoloDBCollectionQueryable<'T, 'T>(SoloDBCollectionQueryProvider(this), Expression.Constant(RootQueryable<'T>(this))) :> IOrderedQueryable<'T>
    member val private ConnectionString = connectionString
    member val Name = name
    member val InTransaction = match connection with | Transactional _ | Transitive _ -> true | Pooled _ -> false
    member val IncludeType = mustIncludeTypeInformationInSerialization<'T>
    member val internal Connection = connection

    member this.Insert (item: 'T) =
        use connection = connection.Get()

        Helper.insertInner this.IncludeType item connection name this

    member this.InsertOrReplace (item: 'T) =
        use connection = connection.Get()

        Helper.insertOrReplaceInner this.IncludeType item connection name this


    member this.InsertBatch (items: 'T seq) =
        if this.InTransaction then
            use connection = connection.Get()

            let ids = List<int64>()
            for item in items do
                Helper.insertInner this.IncludeType item connection name this |> ids.Add
            ids
        else

        use connection = connection.Get()
        use directConnection = new DirectConnection(connection)
        let transientConnection = Collection((Connection.Transitive directConnection), name, connectionString)
        connection.Execute "BEGIN;" |> ignore

        try
            let ids = List<int64>()
            for item in items do
                Helper.insertInner this.IncludeType item connection name transientConnection |> ids.Add

            connection.Execute "COMMIT;" |> ignore
            ids
        with ex ->
            connection.Execute "ROLLBACK;" |> ignore
            reraise()
            

    member this.InsertOrReplaceBatch (items: 'T seq) =
        if this.InTransaction then
            use connection = connection.Get()

            let ids = List<int64>()
            for item in items do
                Helper.insertOrReplaceInner this.IncludeType item connection name this |> ids.Add
            ids
        else

        use connection = connection.Get()
        use directConnection = new DirectConnection(connection)
        let transientConnection = Collection((Connection.Transitive directConnection), name, connectionString)
        connection.Execute "BEGIN;" |> ignore
        
        try
            let ids = List<int64>()
            for item in items do
                Helper.insertOrReplaceInner this.IncludeType item connection name transientConnection |> ids.Add

            connection.Execute "COMMIT;" |> ignore
            ids
        with ex ->
            connection.Execute "ROLLBACK;" |> ignore
            reraise()

    member this.TryGetById(id: int64) =
        use connection = connection.Get()
        match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE Id = @id LIMIT 1", {|id = id|}) with
        | json when Object.ReferenceEquals(json, null) -> None
        | json -> fromSQLite<'T> json |> Some

    member this.GetById(id: int64) =
        match this.TryGetById id with
        | None -> failwithf "There is no element with id %i" id
        | Some x -> x

    member this.DeleteById(id: int64) =
        use connection = connection.Get()
        connection.Execute ($"DELETE FROM \"{name}\" WHERE Id = @id", {|id = id|})

    
    member this.TryGetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T option =
        let idProp = CustomTypeId<'T>.Value.Value.Property
        let filter, variables = QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<'IdType>(idProp) = id))
        use connection = connection.Get()
        match connection.QueryFirstOrDefault<DbObjectRow>($"SELECT Id, json_quote(Value) as ValueJSON FROM \"{name}\" WHERE {filter} LIMIT 1", variables) with
        | json when Object.ReferenceEquals(json, null) -> None
        | json -> fromSQLite<'T> json |> Some

    member this.GetById<'IdType when 'IdType : equality>(id: 'IdType) : 'T =
        match this.TryGetById id with
        | None -> failwithf "There is no element with id %A" id
        | Some x -> x

    member this.DeleteById<'IdType when 'IdType : equality>(id: 'IdType) : int =
        let idProp = CustomTypeId<'T>.Value.Value.Property
        let filter, variables = QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<'IdType>(idProp) = id))
        use connection = connection.Get()
        connection.Execute ($"DELETE FROM \"{name}\" WHERE {filter}", variables)

    member this.Update(item: 'T) =
        let filter, variables = 
            if HasTypeId<'T>.Value then
                let id = HasTypeId<'T>.Read item
                QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<int64>("Id") = id))
            else match CustomTypeId<'T>.Value with
                 | Some customId ->
                    let id = customId.GetId (item |> box)
                    let idProp = CustomTypeId<'T>.Value.Value.Property
                    QueryTranslator.translate name (ExpressionHelper.get(fun (x: 'T) -> x.Dyn<obj>(idProp) = id))
                 | None ->
                    raise (InvalidOperationException $"The item's type {typeof<'T>.Name} does not have a int64 Id property or a custom Id to use in the update process.")

        variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item
        use connection = connection.Get()
        let _count = connection.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE " + filter, variables)

        ()

    member this.DeleteMany(filter: Expression<Func<'T, bool>>) =
        let filter, variables = QueryTranslator.translate name filter

        use connection = connection.Get()
        connection.Execute ($"DELETE FROM \"{name}\" WHERE " + filter, variables)

    member this.DeleteOne(filter: Expression<Func<'T, bool>>) =
        let filter, variables = QueryTranslator.translate name filter

        use connection = connection.Get()
        connection.Execute ($"DELETE FROM \"{name}\" WHERE Id in (SELECT Id FROM \"{name}\" WHERE ({filter}) LIMIT 1)", variables)

    member this.ReplaceMany(item: 'T)(filter: Expression<Func<'T, bool>>) =
        let filter, variables = QueryTranslator.translate name filter
        variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item

        use connection = connection.Get()
        connection.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE " + filter, variables)

    member this.ReplaceOne(item: 'T)(filter: Expression<Func<'T, bool>>) =
        let filter, variables = QueryTranslator.translate name filter
        variables.["item"] <- if this.IncludeType then toTypedJson item else toJson item

        use connection = connection.Get()
        connection.Execute ($"UPDATE \"{name}\" SET Value = jsonb(@item) WHERE Id in (SELECT Id FROM \"{name}\" WHERE ({filter}) LIMIT 1)", variables)

    member this.EnsureIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        use connection = connection.Get()

        Helper.ensureIndex name connection expression

    member this.EnsureUniqueAndIndex<'R>(expression: Expression<System.Func<'T, 'R>>) =
        use connection = connection.Get()
        
        Helper.ensureUniqueAndIndex name connection expression

    member this.DropIndexIfExists<'R>(expression: Expression<System.Func<'T, 'R>>) =
        let indexName, _whereSQL = Helper.getIndexWhereAndName<'T, 'R> name expression

        let indexSQL = $"DROP INDEX IF EXISTS \"{indexName}\""

        use connection = connection.Get()
        connection.Execute(indexSQL)
        
    member this.EnsureAddedAttributeIndexes() =
        // Ignore the untyped collections.
        if not (typeof<JsonSerializator.JsonValue>.IsAssignableFrom typeof<'T>) then
            use conn = connection.Get()
            Helper.ensureDeclaredIndexesFields<'T> name conn

    override this.Equals(other) = 
        match other with
        | :? Collection<'T> as other ->
            (this :> IEquatable<Collection<'T>>).Equals other
        | other -> false

    override this.GetHashCode() = hash (this)

    interface IEquatable<Collection<'T>> with
        member this.Equals (other) =
            this.ConnectionString = other.ConnectionString && this.Name = other.Name


    interface IOrderedQueryable<'T>

    interface IQueryable<'T> with
        member this.Provider = this.SoloDBQueryable.Provider
        member this.Expression = this.SoloDBQueryable.Expression
        member this.ElementType = typeof<'T>

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            this.SoloDBQueryable.GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

    interface ISoloDBCollection<'T> with
        member this.InTransaction = this.InTransaction
        member this.IncludeType = this.IncludeType 
        member this.Name = this.Name

        member this.DropIndexIfExists(expression) = this.DropIndexIfExists(expression)
        member this.EnsureAddedAttributeIndexes() = this.EnsureAddedAttributeIndexes()
        member this.EnsureIndex(expression) = this.EnsureIndex(expression)
        member this.EnsureUniqueAndIndex(expression) = this.EnsureUniqueAndIndex(expression)
        member this.GetById(id) = this.GetById(id)
        member this.GetById(id: 'IdType) = this.GetById<'IdType>(id)
        member this.Insert(item) = this.Insert(item)
        member this.InsertBatch(items) = this.InsertBatch(items)
        member this.InsertOrReplace(item) = this.InsertOrReplace(item)
        member this.InsertOrReplaceBatch(items) = this.InsertOrReplaceBatch(items)
        member this.TryGetById(id) = this.TryGetById(id)
        member this.TryGetById(id: 'IdType) = this.TryGetById<'IdType>(id)
        member this.GetInternalConnection (): IDbConnection = this.Connection.Get()
        member this.Delete(id: 'IdType) = this.DeleteById<'IdType>(id)
        member this.Delete(id) = this.DeleteById(id)
        member this.Update(item) = this.Update(item)
        member this.DeleteMany(filter) = this.DeleteMany(filter)
        member this.DeleteOne(filter) = this.DeleteOne(filter)
        member this.ReplaceMany(item,filter) = this.ReplaceMany(filter)(item)
        member this.ReplaceOne(item,filter) = this.ReplaceOne(filter)(item)

type TransactionalSoloDB internal (connection: TransactionalConnection) =
    let connectionString = connection.ConnectionString

    member val Connection = connection

    member private this.InitializeCollection<'T> name =
        if not (Helper.existsCollection name connection) then 
            Helper.createTableInner<'T> name connection
            
        Collection<'T>(Transactional connection, name, connectionString) :> ISoloDBCollection<'T>

    member this.GetCollection<'T>() =
        let name = Helper.collectionNameOf<'T>
        
        this.InitializeCollection<'T>(name)

    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.formatName
        
        this.InitializeCollection<JsonSerializator.JsonValue>(name)

    member this.CollectionExists name =
        Helper.existsCollection name connection

    member this.CollectionExists<'T>() =
        let name = Helper.collectionNameOf<'T>
        Helper.existsCollection name connection

    member this.DropCollectionIfExists name =
        use _mutex = Helper.lockTable connectionString name

        if Helper.existsCollection name connection then
            Helper.dropCollection name connection
            true
        else false

    member this.DropCollectionIfExists<'T>() =
        let name = Helper.collectionNameOf<'T>
        this.DropCollectionIfExists name

    member this.DropCollection<'T>() =
        if this.DropCollectionIfExists<'T>() = false then
            let name = Helper.collectionNameOf<'T>
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

        let usCultureInfo = CultureInfo.GetCultureInfo("en-us")
        let setup (connection: SqliteConnection) =            
            connection.CreateFunction("UNIXTIMESTAMP", Func<int64>(fun () -> DateTimeOffset.Now.ToUnixTimeMilliseconds()), false)
            connection.CreateFunction("SHA_HASH", Func<byte array, obj>(fun o -> Utils.shaHash o), true)
            connection.CreateFunction("TO_LOWER", Func<string, string>(_.ToLower(usCultureInfo)), true)
            connection.CreateFunction("TO_UPPER", Func<string, string>(_.ToUpper(usCultureInfo)), true)
            connection.CreateFunction("base64", Func<obj, obj>(Utils.sqlBase64), true) // https://www.sqlite.org/base64.html
            connection.Execute "PRAGMA recursive_triggers = ON;" |> ignore // This must be enabled on every connection separately.

        let manager = new ConnectionManager(connectionString, setup)

        // todo: Put it into a separate file.
        do
            use dbConnection = manager.Borrow()

            let initTables = "
                PRAGMA journal_mode=wal;
                PRAGMA page_size=16384;
                PRAGMA recursive_triggers = ON;

                BEGIN EXCLUSIVE;

                CREATE TABLE IF NOT EXISTS SoloDBCollections (Name TEXT NOT NULL) STRICT;
                                

                CREATE TABLE IF NOT EXISTS SoloDBDirectoryHeader (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL 
                            CHECK ((length(Name) != 0 OR ParentId IS NULL) 
                                    AND (Name != \".\") 
                                    AND (Name != \"..\") 
                                    AND NOT Name GLOB \"*\\*\"
                                    AND NOT Name GLOB \"*/*\"),
                    FullPath TEXT NOT NULL 
                            CHECK (FullPath != \"\" 
                                    AND NOT FullPath GLOB \"*/./*\" 
                                    AND NOT FullPath GLOB \"*/../*\"
                                    AND NOT FullPath GLOB \"*\\*\"
                                    -- Recursion limit check, see https://www.sqlite.org/limits.html
                                    AND (LENGTH(FullPath) - LENGTH(REPLACE(FullPath, '/', '')) <= 900)),
                    ParentId INTEGER,
                    Created INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                    Modified INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                    FOREIGN KEY (ParentId) REFERENCES SoloDBDirectoryHeader(Id) ON DELETE CASCADE,
                    UNIQUE(ParentId, Name),
                    UNIQUE(FullPath)
                ) STRICT;

                CREATE TABLE IF NOT EXISTS SoloDBFileHeader (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL 
                            CHECK (length(Name) != 0 
                                    AND Name != \".\" 
                                    AND Name != \"..\"
                                    AND NOT Name GLOB \"*\\*\"
                                    AND NOT Name GLOB \"*/*\"
                                    ),
                    FullPath TEXT NOT NULL 
                            CHECK (FullPath != \"\" 
                                    AND NOT FullPath GLOB \"*/./*\"
                                    AND NOT FullPath GLOB \"*/../*\"
                                    AND NOT FullPath GLOB \"*\\*\"),
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

            let initTriggers = "
                BEGIN EXCLUSIVE;


                -- Trigger to update the Modified column on insert for SoloDBDirectoryHeader
                CREATE TRIGGER IF NOT EXISTS Insert_SoloDBDirectoryHeader
                AFTER INSERT ON SoloDBDirectoryHeader
                FOR EACH ROW
                BEGIN
                    -- Update parent directory's Modified timestamp, if ParentId is NULL then it will be a noop.
                    UPDATE SoloDBDirectoryHeader
                    SET Modified = NEW.Modified
                    WHERE Id = NEW.ParentId AND Modified < NEW.Modified;
                END;
                                
                -- Trigger to update the Modified column on update for SoloDBDirectoryHeader
                CREATE TRIGGER IF NOT EXISTS Update_SoloDBDirectoryHeader
                AFTER UPDATE ON SoloDBDirectoryHeader
                FOR EACH ROW
                BEGIN
                    -- Update parent directory's Modified timestamp, if ParentId is NULL then it will be a noop.
                    UPDATE SoloDBDirectoryHeader
                    SET Modified = NEW.Modified
                    WHERE Id = NEW.ParentId AND Modified < NEW.Modified;
                END;
                                
                -- Trigger to update the Modified column on insert for SoloDBFileHeader
                CREATE TRIGGER IF NOT EXISTS Insert_SoloDBFileHeader
                AFTER INSERT ON SoloDBFileHeader
                FOR EACH ROW
                BEGIN
                    -- Update parent directory's Modified timestamp
                    UPDATE SoloDBDirectoryHeader
                    SET Modified = NEW.Modified
                    WHERE Id = NEW.DirectoryId AND Modified < NEW.Modified;
                END;
                                
                -- Trigger to update the Modified column on update for SoloDBFileHeader
                CREATE TRIGGER IF NOT EXISTS Update_SoloDBFileHeader
                AFTER UPDATE OF Hash ON SoloDBFileHeader
                FOR EACH ROW
                BEGIN
                    UPDATE SoloDBFileHeader
                    SET Modified = UNIXTIMESTAMP()
                    WHERE Id = NEW.Id AND Modified < UNIXTIMESTAMP();
                                    
                    -- Update parent directory's Modified timestamp
                    UPDATE SoloDBDirectoryHeader
                    SET Modified = UNIXTIMESTAMP()
                    WHERE Id = NEW.DirectoryId AND Modified < UNIXTIMESTAMP();
                END;                 

                COMMIT TRANSACTION;
                "

            let initIndexes = "
                BEGIN EXCLUSIVE;


                CREATE INDEX IF NOT EXISTS SoloDBCollectionsNameIndex ON SoloDBCollections(Name);

                CREATE INDEX IF NOT EXISTS SoloDBDirectoryHeaderParentIdIndex ON SoloDBDirectoryHeader(ParentId);
                CREATE UNIQUE INDEX IF NOT EXISTS SoloDBDirectoryHeaderFullPathIndex ON SoloDBDirectoryHeader(FullPath);
                CREATE UNIQUE INDEX IF NOT EXISTS SoloDBFileHeaderFullPathIndex ON SoloDBFileHeader(FullPath);
                CREATE UNIQUE INDEX IF NOT EXISTS SoloDBDirectoryMetadataDirectoryIdAndKey ON SoloDBDirectoryMetadata(DirectoryId, Key);

                CREATE INDEX IF NOT EXISTS SoloDBFileHeaderDirectoryIdIndex ON SoloDBFileHeader(DirectoryId);
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
                "
            
            for command in [|initTables; initTriggers; initIndexes|] do
                use command = new SqliteCommand(command, dbConnection)
                command.Prepare()
                let result = command.ExecuteNonQuery()
                ()
            let mutable dbSchemaVersion = dbConnection.QueryFirst "PRAGMA user_version;";

            // Migrations.
            if dbSchemaVersion = 0 then
                // Modified the triggers, must recreate them.
                dbConnection.Execute "
                    BEGIN EXCLUSIVE;
                    DROP TRIGGER IF EXISTS Insert_SoloDBDirectoryHeader;
                    DROP TRIGGER IF EXISTS Update_SoloDBDirectoryHeader;
                    DROP TRIGGER IF EXISTS Insert_SoloDBFileHeader;
                    DROP TRIGGER IF EXISTS Update_SoloDBFileHeader;
                    COMMIT TRANSACTION;
                " 
                |> ignore
                dbConnection.Execute initTriggers
                |> ignore
                dbConnection.Execute "PRAGMA user_version = 1;"
                |> ignore
                dbSchemaVersion <- 1


            let _rez = dbConnection.Execute("PRAGMA optimize;")
            ()
        
        new SoloDB(manager, connectionString, location)

    member this.Connection = connectionManager
    member this.ConnectionString = connectionString
    member val internal DataLocation = location
    member val FileSystem = FileSystem connectionManager

    member private this.GetNewConnection() = connectionManager.Borrow()
        
    member private this.InitializeCollection<'T> (name: string) =     
        if disposed then raise (ObjectDisposedException(nameof(SoloDB)))
        if name.StartsWith "SoloDB" then raise (ArgumentException $"The SoloDB* prefix is forbidden in Collection names.")

        use _mutex = Helper.lockTable connectionString name // To prevent a race condition where the next if statment is true for 2 threads.

        use connection = connectionManager.Borrow()

        if not (Helper.existsCollection name connection) then 
            let withinTransaction = connection.IsWithinTransaction()
            if not withinTransaction then connection.Execute "BEGIN;" |> ignore
            try
                Helper.createTableInner<'T> name connection

                if not withinTransaction then connection.Execute "COMMIT;" |> ignore
            with ex ->
                if not withinTransaction then connection.Execute "ROLLBACK;" |> ignore
                reraise()

        Collection<'T>(Pooled connectionManager, name, connectionString) :> ISoloDBCollection<'T>

    member this.GetCollection<'T>() =
        let name = Helper.collectionNameOf<'T>
        
        this.InitializeCollection<'T>(name)

    member this.GetCollection<'T>(name) =        
        this.InitializeCollection<'T>(name)

    member this.GetUntypedCollection(name: string) =
        let name = name |> Helper.formatName
        
        this.InitializeCollection<JsonSerializator.JsonValue>(name)

    member this.CollectionExists name =
        use dbConnection = connectionManager.Borrow()
        Helper.existsCollection name dbConnection

    member this.CollectionExists<'T>() =
        let name = Helper.collectionNameOf<'T>
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
        let name = Helper.collectionNameOf<'T>
        this.DropCollectionIfExists name

    member this.DropCollection<'T>() =
        if this.DropCollectionIfExists<'T>() = false then
            let name = Helper.collectionNameOf<'T>
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


    /// Run the EXPLAIN QUERY PLAN on the generated SQL.
    static member ExplainQueryPlan(query: IQueryable<'T>) =
        // This is a hack, I do not think that it is possible to add new IQueryable methods directly.
        query.Aggregate(QueryPlan.ExplainQueryPlanReference, (fun _a _b -> ""))

    /// Returns the generated SQL.
    static member GetSQL(query: IQueryable<'T>) =
        query.Aggregate(QueryPlan.GetGeneratedSQLReference, (fun _a _b -> ""))