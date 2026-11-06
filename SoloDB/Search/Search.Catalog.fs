namespace SoloDatabase

open System
open System.Collections.Generic
open SoloDatabase.JsonSerializator

/// Persistent declarations are read on the executing connection, never in a process cache.
module internal SearchCatalog =
    let table = "\"SoloDB.SearchIndexes\""
    let schema = """
        CREATE TABLE "SoloDB.SearchIndexes"(
          Id INTEGER PRIMARY KEY AUTOINCREMENT,
          Collection TEXT NOT NULL, Name TEXT NOT NULL,
          Version INTEGER NOT NULL,
          Definition TEXT NOT NULL CHECK(json_valid(Definition)),
          Objects TEXT NOT NULL CHECK(json_valid(Objects)),
          UNIQUE(Collection,Name)) STRICT;
        """
    let validateName (name: string) =
        if isNull name then nullArg "name"
    let exists (connection: NativeSqliteConnection) =
        let mutable statement = connection.OpenStatement("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='SoloDB.SearchIndexes')")
        try
            if not(statement.Step()) then invalidOp "Search catalog existence was not returned."
            let found = statement.Row.GetBoolean(0)
            statement.Dispose()
            found
        with error -> statement.Abort(error); reraise()
    let version (connection: NativeSqliteConnection) =
        let mutable statement = connection.OpenStatement("PRAGMA user_version")
        try
            if not(statement.Step()) then invalidOp "The database schema version was not returned."
            let version = statement.Row.GetInt32(0)
            statement.Dispose()
            version
        with error -> statement.Abort(error); reraise()
    let private available connection =
        if exists connection then true
        elif version connection >= 7 then
            invalidOp "The adopted search catalog is missing. Restore the database catalog before administering search indexes."
        else false
    let private readRow collection (row: SqlRow) =
        { Id = row.GetInt64(0); Collection = collection; Name = row.GetString(1)
          Version = row.GetInt32(2); Definition = SearchDefinition.decode(row.GetString(3)); Objects = row.GetString(4) }
    let load (connection: NativeSqliteConnection) collection =
        if not(available connection) then Array.empty else
        let mutable statement = connection.OpenStatement($"SELECT Id,Name,Version,Definition,Objects FROM {table} WHERE Collection=@collection ORDER BY Name")
        try
            statement.BindText(1,collection)
            let found = ResizeArray<SearchIndexDescriptor>()
            while statement.Step() do found.Add(readRow collection statement.Row)
            statement.Dispose()
            found.ToArray()
        with error -> statement.Abort(error); reraise()
    let tryFind (connection: NativeSqliteConnection) collection name =
        validateName name
        if not(available connection) then ValueNone else
        let mutable statement = connection.OpenStatement($"SELECT Id,Name,Version,Definition,Objects FROM {table} WHERE Collection=@collection AND Name=@name")
        try
            statement.BindText(1,collection)
            statement.BindText(2,name)
            let found = if statement.Step() then ValueSome(readRow collection statement.Row) else ValueNone
            statement.Dispose()
            found
        with error -> statement.Abort(error); reraise()
    let find connection collection name =
        tryFind connection collection name |> ValueOption.defaultWith(fun () ->
            invalidOp $"Search index '{name}' is not defined on collection '{collection}'. Call EnsureSearchIndex explicitly.")
    let objects (index: SearchIndexDescriptor) =
        Array.append [|index.Table;index.View|]
            ([|"_insert";"_delete";"_update";"_config";"_data";"_idx";"_docsize"|]
             |> Array.map(fun suffix -> index.Table+suffix))
        |> fun names -> if index.Definition.Storage = SearchTextStorage.Copied then Array.append names [|index.Table+"_content"|] else names
    let private artifact (connection: NativeSqliteConnection) name =
        let mutable statement = connection.OpenStatement("SELECT type,sql FROM sqlite_schema WHERE name=@name")
        try
            statement.BindText(1,name)
            let found = if statement.Step() then ValueSome(struct(statement.Row.GetString(0),statement.Row.GetString(1))) else ValueNone
            statement.Dispose()
            found
        with error -> statement.Abort(error); reraise()
    let private rank (connection: NativeSqliteConnection) (index: SearchIndexDescriptor) =
        let config = SearchDefinition.quote(index.Table+"_config")
        let mutable statement = connection.OpenStatement($"SELECT v FROM {config} WHERE k='rank'")
        try
            let found = if statement.Step() then ValueSome(statement.Row.GetString(0)) else ValueNone
            statement.Dispose()
            found
        with error -> statement.Abort(error); reraise()
    let readObjects connection (index: SearchIndexDescriptor) =
        let result = ResizeArray<JsonValue>()
        // Construction version describes the published inventory, not current formatter output.
        result.Add(JsonValue.List [|JsonValue.String "format";JsonValue.Number 1M|])
        for name in objects index do
            let struct(kind,sql) = artifact connection name |> ValueOption.defaultWith(fun () -> invalidOp $"Missing search artifact '{name}'.")
            result.Add(JsonValue.List [|JsonValue.String name;JsonValue.String kind;JsonValue.String sql|])
        let configured = rank connection index |> ValueOption.defaultWith(fun () -> invalidOp "The search rank configuration is missing.")
        result.Add(JsonValue.List [|JsonValue.String "rank";JsonValue.String configured|])
        JsonValue.List(result).ToJsonString()
    let inspectObjects connection (index: SearchIndexDescriptor) =
        let findings = ResizeArray<SearchVerificationFinding>()
        let add kind name description = findings.Add(SearchVerificationFinding(kind,name,description))
        let entries =
            match JsonValue.Parse index.Objects with
            | JsonValue.List entries -> entries |> Seq.toArray
            | _ -> invalidOp "The search artifact inventory is invalid."
        for entry in entries do
            match entry with
            | JsonValue.List entry when entry.Count = 3 ->
                match entry.[0],entry.[1],entry.[2] with
                | JsonValue.String name,JsonValue.String kind,JsonValue.String sql ->
                    match artifact connection name with
                    | ValueNone -> add SearchVerificationKind.MissingArtifact name "The published search artifact is missing."
                    | ValueSome(struct(actualKind,actualSql)) when actualKind <> kind || actualSql <> sql ->
                        add SearchVerificationKind.AlteredArtifact name "The artifact differs from its published definition."
                    | _ -> ()
                | _ -> invalidOp "The search artifact inventory is invalid."
            | JsonValue.List entry when entry.Count = 2 ->
                match entry.[0],entry.[1] with
                | JsonValue.String "format",JsonValue.Number 1M -> ()
                | JsonValue.String "rank",JsonValue.String expected ->
                    if not(findings |> Seq.exists(fun finding -> finding.ObjectName = index.Table+"_config")) then
                      match artifact connection (index.Table+"_config") with
                      | ValueNone -> add SearchVerificationKind.MissingArtifact (index.Table+"_config") "The native search configuration is missing."
                      | ValueSome _ ->
                          match rank connection index with
                          | ValueSome actual when actual = expected -> ()
                          | _ -> add SearchVerificationKind.RankMismatch (index.Table+"_config") "The native rank configuration differs from its declaration."
                | _ -> invalidOp "The search artifact inventory is invalid."
            | _ -> invalidOp "The search artifact inventory is invalid."
        findings.ToArray()
    let validateObjects connection index =
        let findings = inspectObjects connection index
        if findings.Length <> 0 then
            invalidOp $"Search index '{index.Name}' is damaged. Run VerifySearchIndex and RebuildSearchIndex explicitly."

    let unowned (connection: NativeSqliteConnection) =
        let owned = HashSet<string>(StringComparer.Ordinal)
        if available connection then
            let mutable statement = connection.OpenStatement($"SELECT Id,Definition FROM {table}")
            try
                while statement.Step() do
                    let definition = SearchDefinition.decode(statement.Row.GetString(1))
                    let name = SearchArtifactNames.table (statement.Row.GetInt64(0)) definition.Fields
                    owned.Add(name) |> ignore
                    for suffix in [|"_source";"_insert";"_delete";"_update";"_config";"_data";"_idx";"_docsize"|] do
                        owned.Add(name+suffix) |> ignore
                    if definition.Storage = SearchTextStorage.Copied then owned.Add(name+"_content") |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
        let mutable statement = connection.OpenStatement("SELECT name FROM sqlite_schema WHERE name GLOB 'SoloDB.Search_*' ORDER BY name")
        try
            let findings = ResizeArray<SearchVerificationFinding>()
            while statement.Step() do
                let name = statement.Row.GetString(0)
                if not(owned.Contains name) then
                    findings.Add(SearchVerificationFinding(SearchVerificationKind.UnownedArtifact,name,"This database-wide reserved artifact has no search declaration; it does not establish damage to the selected index."))
            statement.Dispose()
            findings.ToArray()
        with error -> statement.Abort(error); reraise()

/// One catalog lookup per collection/name in one translation, shared by nested contexts.
type internal SearchCatalogSource(connection: NativeSqliteConnection) =
    let definitions = Dictionary<struct(string * string),SearchIndexDescriptor>()
    member _.Find(collection,name) =
        let key = struct(collection,name)
        match definitions.TryGetValue key with
        | true, index -> index
        | _ ->
            let index = SearchCatalog.find connection collection name
            definitions.Add(key,index)
            index
