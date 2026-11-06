namespace SoloDatabase

open SoloDatabase.Connections

/// The collection row owns descriptive history and its transactional lifetime.
module internal CollectionTypeInformation =
    let private hasColumn (connection: NativeSqliteConnection) =
        let mutable statement = connection.OpenStatement("SELECT EXISTS(SELECT 1 FROM pragma_table_info('SoloDBCollections') WHERE name='TypeInformation')")
        try
            statement.StepMaintenance() |> ignore
            let found = statement.Row.GetBoolean(0)
            statement.Dispose()
            found
        with error -> statement.Abort(error); reraise()

    let ensureColumn (source: NativeConnection) =
        let required =
            use connection = source.Get()
            not(connection.DatabaseOwner.IsStorageReadOnly) && not(hasColumn connection)
        if required then
            source.WithTransaction(fun connection ->
                if not(hasColumn connection) then
                    let mutable statement = connection.OpenStatement("ALTER TABLE SoloDBCollections ADD COLUMN TypeInformation BLOB")
                    try statement.StepMaintenance() |> ignore; statement.Dispose()
                    with error -> statement.Abort(error); reraise())

    let lookup (connection: NativeSqliteConnection) name meaning =
        let mutable statement = connection.OpenStatement("SELECT EXISTS(SELECT 1 FROM json_each(TypeInformation) WHERE json_extract(value,'$.ShapeKey')=@shape) FROM SoloDBCollections WHERE Name=@name LIMIT 1")
        try
            statement.BindText(1,meaning)
            statement.BindText(2,name)
            let result = if statement.StepMaintenance() then struct(true,statement.Row.GetBoolean(0)) else struct(false,false)
            statement.Dispose()
            result
        with error -> statement.Abort(error); reraise()

    let record (connection: NativeSqliteConnection) name struct(description,meaning) =
        let struct(_,recorded) = lookup connection name meaning
        if not recorded then
            // Evaluate the current bounded array under the actual writer lock. A
            // previously recorded shape is untouched, including when views alternate.
            let mutable statement = connection.OpenStatement("""
                UPDATE SoloDBCollections SET TypeInformation=jsonb_insert(
                  CASE WHEN json_array_length(coalesce(TypeInformation,jsonb('[]')))>=10
                       THEN jsonb_remove(TypeInformation,'$[0]')
                       ELSE coalesce(TypeInformation,jsonb('[]')) END,'$[#]',jsonb(@description))
                WHERE Name=@name AND NOT EXISTS(
                  SELECT 1 FROM json_each(TypeInformation) WHERE json_extract(value,'$.ShapeKey')=@shape)
                """)
            try
                statement.BindText(1,description)
                statement.BindText(2,name)
                statement.BindText(3,meaning)
                statement.StepMaintenance() |> ignore
                statement.Dispose()
            with error -> statement.Abort(error); reraise()
    let tryRead (connection: NativeSqliteConnection) name =
        if not(hasColumn connection) then ValueNone
        else
            let mutable statement = connection.OpenStatement("SELECT json(TypeInformation) FROM SoloDBCollections WHERE Name=@name")
            try
                statement.BindText(1,name)
                let value = if statement.StepMaintenance() && not(statement.Row.IsDBNull(0)) then ValueSome(statement.Row.GetString(0)) else ValueNone
                statement.Dispose()
                value
            with error -> statement.Abort(error); reraise()
