namespace SoloDatabase

open System
open SQLitePCL
open SoloDatabase.Connections
open SoloDatabase.NativeSQLiteTools

/// Catalog and native artifacts publish atomically in the existing transaction owner.
module internal SearchLifecycle =
    let private updateObjects (connection: NativeSqliteConnection) (index: SearchIndexDescriptor) =
        let objects = SearchCatalog.readObjects connection index
        let mutable statement = connection.OpenStatement($"UPDATE {SearchCatalog.table} SET Objects=@objects WHERE Id=@id")
        try
            statement.BindText(1,objects)
            statement.BindInt64(2,index.Id)
            statement.Step() |> ignore
            statement.Dispose()
            { index with Objects = objects }
        with error -> statement.Abort(error); reraise()
    let private insert (connection: NativeSqliteConnection) collection name version definition =
        let mutable statement = connection.OpenStatement($"INSERT INTO {SearchCatalog.table}(Collection,Name,Version,Definition,Objects) VALUES(@collection,@name,@version,@definition,'[]') RETURNING Id")
        let id =
            try
                statement.BindText(1,collection)
                statement.BindText(2,name)
                statement.BindInt64(3,int64 version)
                statement.BindText(4,SearchDefinition.encode definition)
                if not(statement.Step()) then invalidOp "Search identity was not returned."
                let id = statement.Row.GetInt64(0)
                while statement.Step() do ()
                statement.Dispose()
                id
            with error -> statement.Abort(error); reraise()
        let index = { Id = id; Collection = collection; Name = name; Version = version; Definition = definition; Objects = "[]" }
        SearchSchema.create connection index
        updateObjects connection index
    let private remove = SearchSchema.remove
    let private adopt connection =
        SchemaMigration.applyInTransaction connection
            { Label = "search metadata adoption"; Setup = ""; Body = SearchCatalog.schema
              FromVersion = 6; TargetVersion = 7; SupportedCeiling = 7; LegacyTextPolicy = SchemaMigration.Strict }
        // A missing catalog after adoption is damage, never a new identity sequence.
        if not(SearchCatalog.exists connection) then invalidOp "The adopted search catalog is missing."
    let private validateVersion name version =
        SearchCatalog.validateName name
        if version <= 0 then raise(ArgumentOutOfRangeException("version", "Search definition versions must be positive."))
    let private sameCurrent connection collection name version definition =
        match SearchCatalog.tryFind connection collection name with
        | ValueSome index when index.Version = version && index.Definition = definition ->
            SearchCatalog.validateObjects connection index
            true
        | _ -> false
    let private readSnapshot (connection: NativeSqliteConnection) read =
        if raw.sqlite3_get_autocommit connection.InternalHandle = 0 then read() else
        SearchSchema.execute connection "BEGIN DEFERRED"
        try
            let result = read()
            SearchSchema.execute connection "COMMIT"
            result
        with error ->
            try SearchSchema.execute connection "ROLLBACK"
            with cleanup ->
                match connection with
                | :? NativeCachingDbConnection as pooled -> pooled.Unusable <- true
                | _ -> ()
                error.Data.["SoloDB.TransactionCleanupException"] <- cleanup
            reraise()
    let private ensureLocked connection collection name version definition =
        adopt connection
        match SearchCatalog.tryFind connection collection name with
        | ValueNone -> insert connection collection name version definition |> ignore
        | ValueSome index when index.Version > version ->
            invalidOp $"Search index '{name}' requires declaration version {index.Version}; this program declared {version}."
        | ValueSome index when index.Version = version ->
            if index.Definition <> definition then
                invalidOp $"Search index '{name}' has a different definition at version {version}. Increase its version."
            SearchCatalog.validateObjects connection index
        | ValueSome index when index.Definition = definition ->
            SearchCatalog.validateObjects connection index
            let mutable statement = connection.OpenStatement($"UPDATE {SearchCatalog.table} SET Version=@version WHERE Id=@id")
            try
                statement.BindInt64(1,int64 version)
                statement.BindInt64(2,index.Id)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        | ValueSome index ->
            // Logical names cannot contain NUL. This unpublished name cannot collide with
            // a declaration and frees the unique key while the old corpus remains intact.
            let mutable statement = connection.OpenStatement($"UPDATE {SearchCatalog.table} SET Name=@name WHERE Id=@id")
            try
                statement.BindText(1,"\000"+string index.Id)
                statement.BindInt64(2,index.Id)
                statement.Step() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
            insert connection collection name version definition |> ignore
            remove connection index
    let ensure (source: NativeConnection) collection name version definition =
        validateVersion name version
        let current =
            use connection = source.Get()
            readSnapshot connection (fun () -> sameCurrent connection collection name version definition)
        if not current then
            source.CheckWritable()
            source.WithTransaction(fun connection -> ensureLocked connection collection name version definition)
    let drop (source: NativeConnection) collection name =
        source.CheckAccess()
        SearchCatalog.validateName name
        if source.IsReadOnly then
            use connection = source.Get()
            StorageValidation.writable (SearchCatalog.tryFind connection collection name).IsSome
            false
        else
            source.WithTransaction(fun connection ->
                match SearchCatalog.tryFind connection collection name with
                | ValueSome index -> remove connection index; true
                | ValueNone -> false)
    let dropCollection connection collection =
        SearchSchema.dropCollection connection collection
    let rebuild connection collection name =
        let index = SearchCatalog.find connection collection name
        SearchSchema.rebuild connection index
        updateObjects connection index |> ignore
    let list connection collection =
        SearchCatalog.load connection collection |> Array.map(fun index ->
            let definition = index.Definition
            let fields = definition.Fields |> Array.map(fun field -> SearchFieldInfo(field.Path,field.Alias,field.Weight))
            SearchIndexInfo(index.Name,index.Version,definition.Storage,definition.Tokenizer,fields,definition.Prefix))
    let verify connection collection name =
        let index = SearchCatalog.find connection collection name
        SearchVerification(name,Array.append (SearchSchema.verify connection index) (SearchCatalog.unowned connection))
    let optimize connection collection name =
        let index = SearchCatalog.find connection collection name
        SearchCatalog.validateObjects connection index
        SearchSchema.optimize connection index
