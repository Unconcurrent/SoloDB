namespace SoloDatabase

open System
open SoloDatabase.NativeSQLiteTools
open SoloDatabase.SchemaMigration

/// Compatible evolution of the shared filesystem-reference table.
module internal FileSystemReferenceSchema =

    // Locate only the published column clause, not a matching SQL string or comment.
    // Keep the original body so added columns, constraints and table options survive.
    let private signedOwnerBody (sql: string) =
        let oldColumn = "OwnerId INTEGER NOT NULL CHECK(OwnerId > 0)"
        let mutable depth, start, body, change, i = 0, 0, -1, -1, 0
        let column stop =
            let mutable first = start
            while first < stop && Char.IsWhiteSpace sql[first] do first <- first + 1
            if stop - first >= oldColumn.Length &&
               String.CompareOrdinal(sql,first,oldColumn,0,oldColumn.Length) = 0 &&
               (first + oldColumn.Length = stop || Char.IsWhiteSpace sql[first + oldColumn.Length]) then
                change <- first + oldColumn.IndexOf("> 0",StringComparison.Ordinal)
        while i < sql.Length do
            let c = sql[i]
            if c = '\'' || c = '"' || c = '`' || c = '[' then
                let close = if c = '[' then ']' else c
                i <- i + 1
                let mutable quoted = true
                while quoted && i < sql.Length do
                    if sql[i] = close then
                        if i + 1 < sql.Length && sql[i+1] = close then i <- i + 2
                        else quoted <- false; i <- i + 1
                    else i <- i + 1
            elif c = '-' && i + 1 < sql.Length && sql[i+1] = '-' then
                while i < sql.Length && sql[i] <> '\n' do i <- i + 1
            elif c = '/' && i + 1 < sql.Length && sql[i+1] = '*' then
                let close = sql.IndexOf("*/",i+2,StringComparison.Ordinal)
                i <- if close < 0 then sql.Length else close + 2
            else
                if c = '(' then
                    if depth = 0 then body <- i; start <- i + 1
                    depth <- depth + 1
                elif depth = 1 && (c = ',' || c = ')') then
                    column i
                    if c = ')' then depth <- depth - 1 else start <- i + 1
                elif c = ')' then depth <- depth - 1
                i <- i + 1
        if body < 0 || change < 0 then ValueNone
        else ValueSome(sql.Substring(body,change-body) + "<> 0" + sql.Substring(change+3))

    let private fileReferenceBody (connection: NativeSqliteConnection) =
        let mutable statement = connection.OpenStatement("SELECT sql FROM main.sqlite_schema WHERE type='table' AND name='SoloDBFileSystemReference'")
        try
            let body = if statement.Step() then signedOwnerBody (statement.Row.GetString(0)) else ValueNone
            statement.Dispose()
            body
        with error -> statement.Abort(error); reraise()

    /// Broaden the published owner constraint once, before any user transaction.
    /// SQLite generalized ALTER TABLE section 8: create, copy, drop, then rename.
    let internal evolveFileSystemOwners (pooled: NativeCachingDbConnection) version supported =
        let connection = pooled.Inner
        let body = fileReferenceBody connection
        if body.IsSome && not connection.IsReadOnly then
            if SQLitePCL.raw.sqlite3_get_autocommit(connection.Handle) = 0 then invalidOp "Filesystem reference evolution requires an initialization connection outside a transaction."
            let scalar sql =
                let mutable statement = connection.OpenStatement(sql)
                try
                    statement.Step() |> ignore
                    let value = statement.Row.GetInt32(0)
                    statement.Dispose()
                    value
                with error -> statement.Abort(error); reraise()
            let label = "filesystem reference owner identity"
            let execute sql = executeStatements connection label sql
            let foreignKeys = scalar "PRAGMA foreign_keys"
            let legacyAlter = scalar "PRAGMA legacy_alter_table"
            let mutable primary = ValueNone
            try
                execute "PRAGMA foreign_keys=OFF; PRAGMA legacy_alter_table=ON"
                if scalar "PRAGMA foreign_keys" <> 0 then invalidOp "The initialization connection could not suspend foreign keys for table evolution."
                runWith pooled
                    { Label = label; Setup = ""; Body = ""; FromVersion = version; TargetVersion = version
                      SupportedCeiling = supported; LegacyTextPolicy = Strict }
                    (fun () ->
                        let current = readVersion connection
                        if current > supported then raise(unsupportedVersion current supported)
                        match fileReferenceBody connection with
                        | ValueNone -> () // A concurrent initializer already completed it.
                        | ValueSome body ->
                            let quote = RelationsTypes.quoteIdentifier
                            let table = "SoloDBFileSystemReference"
                            let temporary = table + "_" + Guid.NewGuid().ToString("N")
                            let definitions = ResizeArray<string>()
                            let mutable objects = connection.OpenStatement("SELECT sql FROM main.sqlite_schema WHERE tbl_name='SoloDBFileSystemReference' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY CASE type WHEN 'index' THEN 0 ELSE 1 END,name")
                            try
                                while objects.Step() do definitions.Add(objects.Row.GetString(0))
                                objects.Dispose()
                            with error -> objects.Abort(error); reraise()
                            let columns = ResizeArray<string>()
                            let names = Collections.Generic.HashSet<string>(StringComparer.OrdinalIgnoreCase)
                            let mutable info = connection.OpenStatement("SELECT name,hidden FROM pragma_table_xinfo('SoloDBFileSystemReference') ORDER BY cid")
                            try
                                while info.Step() do
                                    let name = info.Row.GetString(0)
                                    names.Add name |> ignore
                                    if info.Row.GetInt32(1) = 0 then columns.Add(quote name)
                                info.Dispose()
                            with error -> info.Abort(error); reraise()
                            if scalar "SELECT wr FROM pragma_table_list WHERE schema='main' AND name='SoloDBFileSystemReference'" = 0 then
                                match [|"rowid";"_rowid_";"oid"|] |> Array.tryFind(fun name -> not(names.Contains name)) with
                                | Some name -> columns.Insert(0,quote name)
                                | None -> ()
                            let columns = String.Join(",",columns)
                            execute ($"CREATE TABLE {quote temporary}" + body)
                            execute $"INSERT INTO {quote temporary}({columns}) SELECT {columns} FROM {quote table}"
                            execute $"DROP TABLE {quote table}; ALTER TABLE {quote temporary} RENAME TO {quote table}"
                            for sql in definitions do execute sql
                            if foreignKeys <> 0 then
                                let mutable check = connection.OpenStatement("PRAGMA foreign_key_check('SoloDBFileSystemReference')")
                                try
                                    if check.Step() then invalidOp "Foreign key validation failed after filesystem reference evolution."
                                    check.Dispose()
                                with error -> check.Abort(error); reraise())
            with error -> primary <- ValueSome error
            let restore name value =
                try
                    execute $"PRAGMA {name}={value}"
                    if scalar ($"PRAGMA {name}") <> value then invalidOp $"The prior {name} setting could not be restored."
                    ValueNone
                with error -> ValueSome($"restoring {name} failed: {error.Message}")
            let cleanup = [restore "legacy_alter_table" legacyAlter; restore "foreign_keys" foreignKeys]
            if cleanup |> List.exists ValueOption.isSome then pooled.Unusable <- true
            match composeOutcome label primary cleanup with
            | ValueNone -> ()
            | ValueSome error -> error.Throw()
