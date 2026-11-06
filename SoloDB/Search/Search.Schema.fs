namespace SoloDatabase

open System
open SoloDatabase.NativeSQLiteTools
open SearchDefinition

module internal SearchSchema =
    let execute (connection: NativeSqliteConnection) sql =
        let mutable statement = connection.AcquireScript(NativeStatementSource.Text sql,null)
        try statement.Consume() |> ignore; statement.Dispose()
        with error -> statement.Abort(error); reraise()
    let create connection (index: SearchIndexDescriptor) =
        let definition = index.Definition
        let table, view = quote index.Table, quote index.View
        let fields = definition.Fields
        if fields |> Array.exists(fun field -> identifier field.Alias = identifier index.Table) then
            invalidArg "name" "A search alias cannot equal its physical FTS table name."
        let names = fields |> Array.map(fun field -> quote field.Alias) |> String.concat ","
        let values owner = fields |> Array.map(fun field -> projection owner field.Path) |> String.concat ","
        let columns = fields |> Array.map(fun field -> projection "" field.Path + " AS " + quote field.Alias) |> String.concat ","
        let content = if definition.Storage = SearchTextStorage.Shared then ",content=" + literal index.View + ",content_rowid='Id'" else ""
        let weights = fields |> Array.map(fun field -> field.Weight.ToString("R",Globalization.CultureInfo.InvariantCulture)) |> String.concat ","
        let configuredRank = literal("bm25(" + weights + ")")
        let prefix = if definition.Prefix.Length = 0 then "" else ",prefix=" + literal(String.Join(" ",definition.Prefix))
        let oldValues, newValues = values "old.", values "new."
        let remove =
            if definition.Storage = SearchTextStorage.Shared then $"INSERT INTO {table}({table},rowid,{names}) VALUES('delete',old.Id,{oldValues});"
            else $"DELETE FROM {table} WHERE rowid=old.Id;"
        let insert = $"INSERT INTO {table}(rowid,{names}) VALUES(new.Id,{newValues});"
        let changed = "old.Id IS NOT new.Id OR " + (fields |> Array.map(fun field ->
            let oldValue, newValue = projection "old." field.Path, projection "new." field.Path
            $"({oldValue}) IS NOT ({newValue})") |> String.concat " OR ")
        execute connection $"""
            CREATE VIEW {view} AS SELECT Id,{columns} FROM {quote index.Collection};
            CREATE VIRTUAL TABLE {table} USING fts5({names}{content},tokenize={literal definition.Tokenizer}{prefix});
            CREATE TRIGGER {quote(index.Table+"_insert")} AFTER INSERT ON {quote index.Collection} BEGIN {insert} END;
            CREATE TRIGGER {quote(index.Table+"_delete")} AFTER DELETE ON {quote index.Collection} BEGIN {remove} END;
            CREATE TRIGGER {quote(index.Table+"_update")} AFTER UPDATE OF Id,Value ON {quote index.Collection} WHEN {changed} BEGIN {remove} {insert} END;
            INSERT INTO {table}(rowid,{names}) SELECT Id,{names} FROM {view};
            INSERT INTO {table}({table},rank) VALUES('rank',{configuredRank});
            """
    let drop connection (index: SearchIndexDescriptor) =
        for suffix in ["_insert";"_delete";"_update"] do execute connection $"DROP TRIGGER IF EXISTS {quote(index.Table+suffix)}"
        execute connection $"DROP TABLE IF EXISTS {quote index.Table}; DROP VIEW IF EXISTS {quote index.View}"
    let remove (connection: NativeSqliteConnection) (index: SearchIndexDescriptor) =
        drop connection index
        let mutable statement = connection.OpenStatement($"DELETE FROM {SearchCatalog.table} WHERE Id=@id")
        try
            statement.BindInt64(1,index.Id)
            statement.Step() |> ignore
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
    let dropCollection connection collection =
        for index in SearchCatalog.load connection collection do remove connection index
    let rebuild (connection: NativeSqliteConnection) (index: SearchIndexDescriptor) =
        // A missing configuration table prevents the virtual-table constructor, hence DROP.
        // Reconstruct only its native bootstrap row, from this engine's fresh template.
        let missingConfig =
            let mutable statement = connection.OpenStatement("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=@table), EXISTS(SELECT 1 FROM sqlite_schema WHERE name=@config)")
            try
                statement.BindText(1,index.Table)
                statement.BindText(2,index.Table+"_config")
                if not(statement.Step()) then invalidOp "Search repair metadata was not returned."
                let missing = statement.Row.GetBoolean(0) && not(statement.Row.GetBoolean(1))
                statement.Dispose()
                missing
            with error -> statement.Abort(error); reraise()
        let unowned = SearchCatalog.unowned connection
        if unowned.Length <> 0 then
            invalidOp "Unowned search artifacts require explicit inspection before rebuilding."
        if missingConfig then
            let findings = SearchCatalog.inspectObjects connection index
            if findings |> Array.exists(fun finding -> finding.Kind = SearchVerificationKind.AlteredArtifact) then
                invalidOp "The damaged search table differs from its published definition; bootstrap repair is unsafe."
            let template = "SoloDB.SearchRepair_" + string index.Id
            let config = quote(index.Table+"_config")
            let templateConfig = "temp." + quote(template+"_config")
            for suffix in ["_insert";"_delete";"_update"] do
                execute connection $"DROP TRIGGER IF EXISTS {quote(index.Table+suffix)}"
            execute connection $"CREATE VIRTUAL TABLE temp.{quote template} USING fts5(text); CREATE TABLE {config}(k PRIMARY KEY,v) WITHOUT ROWID; INSERT INTO {config} SELECT k,v FROM {templateConfig} WHERE k='version';"
            drop connection index
            execute connection $"DROP TABLE temp.{quote template}"
        else drop connection index
        create connection index
    let optimize connection (index: SearchIndexDescriptor) =
        execute connection $"INSERT INTO {quote index.Table}({quote index.Table}) VALUES('optimize')"
    let verify (connection: NativeSqliteConnection) (index: SearchIndexDescriptor) =
        let findings = ResizeArray<SearchVerificationFinding>(SearchCatalog.inspectObjects connection index)
        if findings.Count = 0 then
            let table = quote index.Table
            let rank = if index.Definition.Storage = SearchTextStorage.Shared then ",rank" else ""
            let rankValue = if index.Definition.Storage = SearchTextStorage.Shared then ",1" else ""
            try execute connection $"INSERT INTO {table}({table}{rank}) VALUES('integrity-check'{rankValue})"
            with :? NativeSqliteException as error when error.SqliteExtendedErrorCode = SQLitePCL.raw.SQLITE_CORRUPT_VTAB ->
                findings.Add(SearchVerificationFinding(SearchVerificationKind.IntegrityFailure,index.Table,error.Message))
            if index.Definition.Storage = SearchTextStorage.Copied then
                let columns = index.Definition.Fields |> Array.map(fun field -> quote field.Alias) |> String.concat ","
                let expected = $"SELECT Id,{columns} FROM {quote index.View}"
                let observed = $"SELECT rowid,{columns} FROM {table}"
                let sql = $"SELECT EXISTS(SELECT * FROM ({expected} EXCEPT {observed})) OR EXISTS(SELECT * FROM ({observed} EXCEPT {expected}))"
                let mutable statement = connection.OpenStatement(sql)
                try
                    if not(statement.Step()) then invalidOp "Search content verification was not returned."
                    if statement.Row.GetBoolean(0) then
                        findings.Add(SearchVerificationFinding(SearchVerificationKind.ContentMismatch,index.Table,"Stored search text disagrees with its documents."))
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()
        findings.ToArray()
