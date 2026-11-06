namespace SoloDatabase

open SoloDatabase.Connections
open SoloDatabase.Metadata.CodecGeneration

/// The collection row owns contract history and recorded alternative facts.
module internal CollectionTypeInformation =
    let private columns (connection: NativeSqliteConnection) =
        let mutable statement = connection.OpenStatement("SELECT sum(name='TypeInformation'),sum(name='TypeAlternatives') FROM pragma_table_info('SoloDBCollections')")
        try
            statement.StepMaintenance() |> ignore
            let result = struct(statement.Row.GetInt32(0) <> 0,statement.Row.GetInt32(1) <> 0)
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()

    let ensureColumn (source: NativeConnection) =
        let required =
            use connection = source.Get()
            not connection.IsReadOnly && columns connection <> struct(true,true)
        if required then
            source.WithTransaction(fun connection ->
                let struct(history,alternatives) = columns connection
                for exists,name in [|history,"TypeInformation";alternatives,"TypeAlternatives"|] do
                    if not exists then
                        let mutable statement = connection.OpenStatement("ALTER TABLE SoloDBCollections ADD COLUMN " + name + " BLOB")
                        try statement.StepMaintenance() |> ignore; statement.Dispose()
                        with error -> statement.Abort(error); reraise())

    let private historyPresent = "EXISTS(SELECT 1 FROM json_each(TypeInformation) AS saved WHERE jsonb_extract(TypeInformation,'$['||saved.key||'].Contract')=jsonb(@contract))"
    let private alternativesMissing = """EXISTS(SELECT 1 FROM json_each(@alternatives) AS proposed WHERE NOT EXISTS(
        SELECT 1 FROM json_each(TypeAlternatives) AS saved
        WHERE json_extract(TypeAlternatives,'$['||saved.key||'].For')=json_extract(@alternatives,'$['||proposed.key||'].For')
          AND jsonb_extract(TypeAlternatives,'$['||saved.key||'].Description.Contract')=jsonb_extract(@alternatives,'$['||proposed.key||'].Description.Contract')))"""

    let private lookupContract = "SELECT " + historyPresent + " FROM SoloDBCollections WHERE Name=@name LIMIT 1"
    let private lookupComplete = "SELECT " + historyPresent + " AND NOT " + alternativesMissing + " FROM SoloDBCollections WHERE Name=@name LIMIT 1"

    let lookup (connection: NativeSqliteConnection) name (description: CollectionTypeDescription) =
        let hasAlternatives = description.Alternatives <> "[]"
        let sql = if hasAlternatives then lookupComplete else lookupContract
        let mutable statement = connection.OpenStatement(sql)
        try
            statement.BindText(1,description.Contract)
            if hasAlternatives then
                statement.BindText(2,description.Alternatives)
                statement.BindText(3,name)
            else statement.BindText(2,name)
            let result = if statement.StepMaintenance() then struct(true,statement.Row.GetBoolean(0)) else struct(false,false)
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()

    let record (connection: NativeSqliteConnection) name (description: CollectionTypeDescription) =
        if not connection.IsReadOnly then
            let struct(_,recorded) = lookup connection name description
            if not recorded then
                // Recheck the current row under the caller's existing writer lock.
                // Alternative discovery cannot evict a historical contract, and a
                // narrower process cannot remove another writer's recorded facts.
                let mutable statement = connection.OpenStatement("""
                    UPDATE SoloDBCollections SET TypeInformation=CASE WHEN """ + historyPresent + """ THEN TypeInformation ELSE
                      jsonb_insert(CASE WHEN json_array_length(coalesce(TypeInformation,jsonb('[]')))>=10
                        THEN jsonb_remove(TypeInformation,'$[0]') ELSE coalesce(TypeInformation,jsonb('[]')) END,'$[#]',jsonb(@description)) END,
                    TypeAlternatives=CASE WHEN @alternatives='[]' THEN TypeAlternatives ELSE (
                      SELECT jsonb_group_array(json(value)) FROM (
                        SELECT value FROM json_each(TypeAlternatives)
                        UNION ALL SELECT proposed.value FROM json_each(@alternatives) AS proposed WHERE NOT EXISTS(
                          SELECT 1 FROM json_each(TypeAlternatives) AS saved
                          WHERE json_extract(TypeAlternatives,'$['||saved.key||'].For')=json_extract(@alternatives,'$['||proposed.key||'].For')
                            AND jsonb_extract(TypeAlternatives,'$['||saved.key||'].Description.Contract')=jsonb_extract(@alternatives,'$['||proposed.key||'].Description.Contract')))) END
                    WHERE Name=@name AND (NOT """ + historyPresent + " OR " + alternativesMissing + ")")
                try
                    statement.BindText(1,description.Contract)
                    statement.BindText(2,description.History)
                    statement.BindText(3,description.Alternatives)
                    statement.BindText(4,name)
                    statement.StepMaintenance() |> ignore
                    statement.Dispose()
                with error -> statement.Abort(error); reraise()

    let tryRead (connection: NativeSqliteConnection) name =
        let struct(history,_) = columns connection
        if not history then ValueNone
        else
            let mutable statement = connection.OpenStatement("SELECT json(TypeInformation) FROM SoloDBCollections WHERE Name=@name")
            try
                statement.BindText(1,name)
                let value = if statement.StepMaintenance() && not(statement.Row.IsDBNull(0)) then ValueSome(statement.Row.GetString(0)) else ValueNone
                statement.Dispose()
                value
            with error -> statement.Abort(error); reraise()
