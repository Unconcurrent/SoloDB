namespace SoloDatabase

open System
open System.Globalization
open NativeSQLiteTools
open SoloDatabase.Types
open SoloDatabase.RelationsSharedSql

/// <summary>
/// Module containing lifted helpers for SoloDB constructor initialization.
/// These functions preserve the exact order and side effects of the original constructor code.
/// </summary>
module internal Bootstrap =
    let private parseVersionError (input: string) (detail: string) =
        FormatException($"Invalid sqlite_version value '{input}'. {detail}")

    let private migrationVerificationError (step: string) (expected: int) (actual: int) =
        InvalidOperationException($"Schema migration verification failed at {step}. Expected user_version={expected}, actual={actual}.")

    /// <summary>
    /// Parses a connection source string into a connection string and location.
    /// </summary>
    /// <param name="source">The database source (file path or "memory:name").</param>
    /// <returns>A tuple of (connectionString, location).</returns>
    let parseSource (source: string) =
        if source.StartsWith("memory:", StringComparison.InvariantCultureIgnoreCase) then
            let memoryName = source.Substring "memory:".Length
            let memoryName = memoryName.Trim()
            if String.IsNullOrWhiteSpace memoryName then
                raise (ArgumentException("Invalid memory source. Provide a non-empty name after 'memory:'."))
            sprintf "Data Source=%s;Mode=Memory;Cache=Shared;Pooling=False" memoryName, SoloDBLocation.Memory memoryName
        else
            let source = System.IO.Path.GetFullPath source
            $"Data Source={source};Pooling=False", SoloDBLocation.File source

    /// <summary>
    /// Creates the SQLite connection setup function that registers custom functions and pragmas.
    /// </summary>
    /// <param name="eventSystem">The event system to register functions on each connection.</param>
    /// <returns>A setup function for NativeSqliteConnection.</returns>
    [<Literal>]
    let private setupSql = "PRAGMA recursive_triggers = ON; PRAGMA foreign_keys = on; PRAGMA cache_size = -65536;"

    let createSetups (eventSystem: EventSystem) =
        let usCultureInfo = CultureInfo.GetCultureInfo("en-us")
        let lower (value: string) = value.ToLower(usCultureInfo)
        let regexpTimeout = TimeSpan.FromMilliseconds(100)
        let register (connection: NativeSqliteConnection) =
            NumericalFunctions.register connection
            QueryTranslatorCharacters.register lower connection
            QueryOrderingPrimitives.register connection
            QueryTranslatorSpanSource.register connection
            connection.RegisterManagedScalar(JsonFunctions.rowErrorFunction, 1,
                RawSqliteFunctions.RawScalarFunc(fun _ arguments ->
                    JsonFunctions.raiseRowError (arguments[0].CopyToString())),
                RawSqliteFunctions.SqliteFunctionFlags.Deterministic)
            connection.RegisterScalar(JsonFunctions.treeValueFunction, 3,
                RawSqliteFunctions.RawScalarFunc(fun context arguments ->
                    if arguments[0].IsNull then context.SetNull()
                    else
                        let text = arguments[0].CopyToString()
                        let identity = if arguments[1].IsNull then -1L else arguments[1].GetInt64()
                        match enum<TreeValueFormat>(arguments[2].GetInt32()) with
                        | TreeValueFormat.Subject -> context.SetText(JsonFunctions.physicalTreeSubject identity text)
                        | TreeValueFormat.EqualityKey ->
                            let subject = JsonFunctions.withPhysicalId identity (JsonSerializator.JsonValue.Parse text)
                            let key = subject.EqualityKey()
                            context.SetBlob(System.Runtime.InteropServices.MemoryMarshal.AsBytes(key.AsSpan()))
                        | _ -> invalidArg "format" "The native tree result format is not supported."),
                RawSqliteFunctions.SqliteFunctionFlags.Deterministic)
            connection.CreateFunction("UNIXTIMESTAMP", Func<int64>(fun () -> DateTimeOffset.Now.ToUnixTimeMilliseconds()), false)
            connection.CreateFunction("SHA_HASH", Func<byte array, obj>(fun o -> Utils.shaHash o), true)
            connection.CreateFunction("TO_LOWER", Func<string, string>(lower), true)
            connection.CreateFunction("TO_UPPER", Func<string, string>(_.ToUpper(usCultureInfo)), true)
            connection.CreateFunction("REGEXP", Func<string, string, bool>(fun pattern input ->
                if isNull pattern || isNull input then
                    false
                else
                    try
                        System.Text.RegularExpressions.Regex.IsMatch(input, pattern, System.Text.RegularExpressions.RegexOptions.None, regexpTimeout)
                    with :? System.Text.RegularExpressions.RegexMatchTimeoutException ->
                        false), true)
            eventSystem.CreateFunctions(connection)
            connection.CreateFunction("base64", Func<obj, obj>(Utils.sqlBase64), true) // https://www.sqlite.org/base64.html
        // Lock waiting belongs to each native command budget, including cold setup.
        let setup (connection: NativeSqliteConnection) =
            register connection
            let mutable execution = connection.AcquireScript(NativeStatementSource.Text setupSql, null)
            try
                execution.Consume() |> ignore
                execution.Dispose()
            with error -> execution.Abort(error); reraise()
        let setupAsync (connection: NativeSqliteConnection) = task {
            register connection
            let! _ = connection.ExecuteScriptAsync(setupSql)
            return ()
        }
        struct (setup, setupAsync)

    /// <summary>
    /// Parses the SQLite version string and returns a comparable Version value.
    /// </summary>
    let parseSqliteVersion (versionText: string) =
        let parts =
            try
                versionText.Split([| '.' |], StringSplitOptions.RemoveEmptyEntries)
                |> Seq.truncate 3
                |> Seq.map Int32.Parse
                |> ResizeArray
            with e ->
                raise (parseVersionError versionText e.Message)

        let major = if parts.Count > 0 then parts.[0] else raise (parseVersionError versionText "Missing major component.")
        let minor = if parts.Count > 1 then parts.[1] else raise (parseVersionError versionText "Missing minor component.")
        let patch = if parts.Count > 2 then parts.[2] else raise (parseVersionError versionText "Missing patch component.")
        Version(major, minor, patch)

    /// <summary>
    /// The initial database schema SQL, applied when user_version = 0.
    /// </summary>
    /// Connection settings that cannot be changed inside a transaction, so they are applied before
    /// the schema transaction opens.
    let [<Literal>] initialSchemaSetup = "
                PRAGMA journal_mode=wal;
                PRAGMA recursive_triggers = ON;
                PRAGMA foreign_keys = on;
                "

    /// The schema body. Transaction control and the version assignment belong to the migration
    /// owner, so they are not repeated here.
    let [<Literal>] initialSchema = "

                CREATE TABLE SoloDBCollections (Name TEXT NOT NULL) STRICT;


                CREATE TABLE SoloDBDirectoryHeader (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL
                        CHECK ((length(Name) != 0 OR ParentId IS NULL)
                                AND (Name != '.')
                                AND (Name != '..')
                                AND NOT Name GLOB '*\\*'
                                AND NOT Name GLOB '*/*'),
                    FullPath TEXT NOT NULL
                        CHECK (FullPath != ''
                                AND NOT FullPath GLOB '*/./*'
                                AND NOT FullPath GLOB '*/../*'
                                AND NOT FullPath GLOB '*\\*'
                                -- Recursion limit check, see https://www.sqlite.org/limits.html
                                AND (LENGTH(FullPath) - LENGTH(REPLACE(FullPath, '/', '')) <= 900)),
                    ParentId INTEGER,
                    Created INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                    Modified INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                    FOREIGN KEY (ParentId) REFERENCES SoloDBDirectoryHeader(Id) ON DELETE CASCADE,
                    UNIQUE(ParentId, Name),
                    UNIQUE(FullPath)
                ) STRICT;

                CREATE TABLE SoloDBFileHeader (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL
                        CHECK (length(Name) != 0
                                AND Name != '.'
                                AND Name != '..'
                                AND NOT Name GLOB '*\\*'
                                AND NOT Name GLOB '*/*'
                                ),
                    FullPath TEXT NOT NULL
                        CHECK (FullPath != ''
                                AND NOT FullPath GLOB '*/./*'
                                AND NOT FullPath GLOB '*/../*'
                                AND NOT FullPath GLOB '*\\*'),
                    DirectoryId INTEGER NOT NULL,
                    Created INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                    Modified INTEGER NOT NULL DEFAULT (UNIXTIMESTAMP()),
                    Length INTEGER NOT NULL DEFAULT 0,
                    Hash BLOB NOT NULL DEFAULT (SHA_HASH('')),
                    FOREIGN KEY (DirectoryId) REFERENCES SoloDBDirectoryHeader(Id) ON DELETE CASCADE,
                    UNIQUE(DirectoryId, Name)
                ) STRICT;

                CREATE TABLE SoloDBFileChunk (
                    FileId INTEGER NOT NULL,
                    Number INTEGER NOT NULL,
                    Data BLOB NOT NULL,
                    FOREIGN KEY (FileId) REFERENCES SoloDBFileHeader(Id) ON DELETE CASCADE,
                    UNIQUE(FileId, Number) ON CONFLICT REPLACE
                ) STRICT;

                CREATE TABLE SoloDBFileMetadata (
                    Id INTEGER PRIMARY KEY,
                    FileId INTEGER NOT NULL,
                    Key TEXT NOT NULL,
                    Value TEXT NOT NULL,
                    UNIQUE(FileId, Key) ON CONFLICT REPLACE,
                    FOREIGN KEY (FileId) REFERENCES SoloDBFileHeader(Id) ON DELETE CASCADE
                ) STRICT;

                CREATE TABLE SoloDBDirectoryMetadata (
                    Id INTEGER PRIMARY KEY,
                    DirectoryId INTEGER NOT NULL,
                    Key TEXT NOT NULL,
                    Value TEXT NOT NULL,
                    UNIQUE(DirectoryId, Key) ON CONFLICT REPLACE,
                    FOREIGN KEY (DirectoryId) REFERENCES SoloDBDirectoryHeader(Id) ON DELETE CASCADE
                ) STRICT;



                -- Trigger to update the Modified column on insert for SoloDBDirectoryHeader
                CREATE TRIGGER Insert_SoloDBDirectoryHeader
                AFTER INSERT ON SoloDBDirectoryHeader
                FOR EACH ROW
                BEGIN
                    -- Update parent directory's Modified timestamp, if ParentId is NULL then it will be a noop.
                    UPDATE SoloDBDirectoryHeader
                    SET Modified = NEW.Modified
                    WHERE Id = NEW.ParentId AND Modified < NEW.Modified;
                END;

                -- Trigger to update the Modified column on update for SoloDBDirectoryHeader
                CREATE TRIGGER Update_SoloDBDirectoryHeader
                AFTER UPDATE ON SoloDBDirectoryHeader
                FOR EACH ROW
                BEGIN
                    -- Update parent directory's Modified timestamp, if ParentId is NULL then it will be a noop.
                    UPDATE SoloDBDirectoryHeader
                    SET Modified = NEW.Modified
                    WHERE Id = NEW.ParentId AND Modified < NEW.Modified;
                END;

                -- Trigger to update the Modified column on insert for SoloDBFileHeader
                CREATE TRIGGER Insert_SoloDBFileHeader
                AFTER INSERT ON SoloDBFileHeader
                FOR EACH ROW
                BEGIN
                    -- Update parent directory's Modified timestamp
                    UPDATE SoloDBDirectoryHeader
                    SET Modified = NEW.Modified
                    WHERE Id = NEW.DirectoryId AND Modified < NEW.Modified;
                END;

                -- Trigger to update the Modified column on update for SoloDBFileHeader
                CREATE TRIGGER Update_SoloDBFileHeader
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

                CREATE UNIQUE INDEX SoloDBCollectionsNameIndex ON SoloDBCollections(Name);

                CREATE INDEX SoloDBDirectoryHeaderParentIdIndex ON SoloDBDirectoryHeader(ParentId);
                CREATE UNIQUE INDEX SoloDBDirectoryHeaderFullPathIndex ON SoloDBDirectoryHeader(FullPath);
                CREATE UNIQUE INDEX SoloDBFileHeaderFullPathIndex ON SoloDBFileHeader(FullPath);
                CREATE UNIQUE INDEX SoloDBDirectoryMetadataDirectoryIdAndKey ON SoloDBDirectoryMetadata(DirectoryId, Key);

                CREATE INDEX SoloDBFileHeaderDirectoryIdIndex ON SoloDBFileHeader(DirectoryId);
                CREATE UNIQUE INDEX SoloDBFileChunkFileIdAndNumberIndex ON SoloDBFileChunk(FileId, Number);
                CREATE UNIQUE INDEX SoloDBFileMetadataFileIdAndKey ON SoloDBFileMetadata(FileId, Key);
                CREATE INDEX SoloDBFileHashIndex ON SoloDBFileHeader(Hash);

                INSERT INTO SoloDBDirectoryHeader (Name, ParentId, FullPath)
                SELECT '', NULL, '/'
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM SoloDBDirectoryHeader
                    WHERE ParentId IS NULL AND Name = ''
                );


                "

    /// <summary>
    /// The current supported schema version. The database will refuse to open future versions.
    /// </summary>
    let [<Literal>] currentSupportedSchemaVersion = 6

    /// <summary>
    /// The minimum required SQLite version.
    /// </summary>
    let minimumSqliteVersion = Version(3, 47, 0)

    /// <summary>
    /// Validates the SQLite version and initializes/migrates the database schema.
    /// Must be called exactly once during SoloDB construction, with a borrowed connection.
    /// Preserves the exact order and side effects of the original constructor code.
    /// </summary>
    /// <param name="dbConnection">A borrowed connection from the connection manager.</param>
    let private collectionNames (connection: NativeSqliteConnection) =
        let mutable statement = connection.OpenStatement("SELECT Name FROM SoloDBCollections")
        try
            let names = ResizeArray<string>()
            while statement.Step() do names.Add(statement.Row.GetString(0))
            statement.Dispose()
            names.ToArray()
        with error -> statement.Abort(error); reraise()

    let initializeSchema (dbConnection: NativeCachingDbConnection) =
        // Edge case 1: SQLite version too old
        let sqliteVersionText =
            let mutable statement = dbConnection.OpenStatement("SELECT sqlite_version();")
            try
                if not (statement.Step()) then raise (InvalidOperationException("SQLite version was not returned."))
                let version = statement.Row.GetString(0)
                statement.Dispose()
                version
            with error -> statement.Abort(error); reraise()
        let sqliteVersion = parseSqliteVersion sqliteVersionText

        if sqliteVersion < minimumSqliteVersion then
            raise (NotSupportedException
                $"Error: SQLite version {sqliteVersionText} does not support the required features.\nReason: SoloDB requires JSONB and trigger RAISE() message expressions (SQLite 3.47.0+).\nFix: Use an engine that supports these features.")

        let mutable dbSchemaVersion = SchemaMigration.readVersion dbConnection;

        // Edge case 2: future schema version
        if dbSchemaVersion > currentSupportedSchemaVersion then
            raise (SchemaMigration.unsupportedVersion dbSchemaVersion currentSupportedSchemaVersion)

        // Schema creation: version 0 -> 1
        if dbSchemaVersion = 0 then
            SchemaMigration.run dbConnection
                { Label = "v0->v1"; Setup = initialSchemaSetup; Body = initialSchema; FromVersion = 0; TargetVersion = 1; SupportedCeiling = currentSupportedSchemaVersion
                  LegacyTextPolicy = SchemaMigration.Strict }
            dbSchemaVersion <- SchemaMigration.readVersion dbConnection
            match SchemaMigration.classifyVersion dbSchemaVersion 1 currentSupportedSchemaVersion with
                | SchemaMigration.Established -> ()
                | SchemaMigration.NotApplied -> raise (migrationVerificationError "v0->v1" 1 dbSchemaVersion)
                | SchemaMigration.BeyondSupported ->
                    raise (SchemaMigration.unsupportedVersion dbSchemaVersion currentSupportedSchemaVersion)

        // Migration: version 1 -> 2
        if dbSchemaVersion = 1 then
            SchemaMigration.run dbConnection
                { Label = "v1->v2"
                  Setup = ""
                  Body = "
                    DROP INDEX SoloDBFileHashIndex;

                    -- The update will be handled inside filestream's code.
                    DROP TRIGGER Update_SoloDBFileHeader;
                    ALTER TABLE SoloDBFileHeader DROP COLUMN \"Hash\";
                  "
                  FromVersion = 1; TargetVersion = 2; SupportedCeiling = currentSupportedSchemaVersion
                  // The only step that re-renders schema a previous release wrote, so the only one
                  // that can meet the historical double-quoted spelling.
                  LegacyTextPolicy = SchemaMigration.TolerateHistoricalText }
            dbSchemaVersion <- SchemaMigration.readVersion dbConnection
            match SchemaMigration.classifyVersion dbSchemaVersion 2 currentSupportedSchemaVersion with
                | SchemaMigration.Established -> ()
                | SchemaMigration.NotApplied -> raise (migrationVerificationError "v1->v2" 2 dbSchemaVersion)
                | SchemaMigration.BeyondSupported ->
                    raise (SchemaMigration.unsupportedVersion dbSchemaVersion currentSupportedSchemaVersion)

        // Migration: version 2 -> 3
        if dbSchemaVersion = 2 then
            let triggerSql =
                collectionNames dbConnection
                |> Seq.map (fun name ->
                    $"DROP TRIGGER IF EXISTS \"SoloDB_Update_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Insert_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Delete_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Updated_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Inserted_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Deleted_{name}\";\n{Helper.getSQLForTriggersForTable name}")
                |> String.concat "\n"

            SchemaMigration.run dbConnection
                { Label = "v2->v3"; Setup = ""; Body = triggerSql; FromVersion = 2; TargetVersion = 3; SupportedCeiling = currentSupportedSchemaVersion
                  LegacyTextPolicy = SchemaMigration.Strict }
            dbSchemaVersion <- SchemaMigration.readVersion dbConnection
            match SchemaMigration.classifyVersion dbSchemaVersion 3 currentSupportedSchemaVersion with
                | SchemaMigration.Established -> ()
                | SchemaMigration.NotApplied -> raise (migrationVerificationError "v2->v3" 3 dbSchemaVersion)
                | SchemaMigration.BeyondSupported ->
                    raise (SchemaMigration.unsupportedVersion dbSchemaVersion currentSupportedSchemaVersion)

        // Migration: version 3 -> 4
        // v3 = triggers/Event API. v4 = Relational API.
        // Ensure SoloDBTypeCollectionMap exists and add Metadata column to existing collections.
        if dbSchemaVersion = 3 then
            let collectionNames =
                collectionNames dbConnection

            let addMetadataColumnSql =
                collectionNames
                |> Array.choose (fun name ->
                    let normalized = Helper.normalizeCatalogNameOrThrow "v3->v4 catalog-name validation" name
                    let hasMetadata =
                        let mutable statement = dbConnection.OpenStatement("SELECT EXISTS (SELECT 1 FROM pragma_table_info(@table) WHERE name = 'Metadata')")
                        try
                            statement.BindText(1, normalized)
                            if not (statement.Step()) then raise (InvalidOperationException("Table metadata was not returned."))
                            let exists = statement.Row.GetBoolean(0)
                            statement.Dispose()
                            exists
                        with error -> statement.Abort(error); reraise()
                    if hasMetadata then None
                    else Some ($"ALTER TABLE \"{normalized}\" ADD COLUMN Metadata JSONB NOT NULL DEFAULT '{{}}';"))
                |> String.concat "\n"

            SchemaMigration.run dbConnection
                { Label = "v3->v4"
                  Setup = ""
                  Body = $"
                    {RelationsSharedSql.createTypeCollectionMapTableSql}

                    {addMetadataColumnSql}
                  "
                  FromVersion = 3; TargetVersion = 4; SupportedCeiling = currentSupportedSchemaVersion
                  LegacyTextPolicy = SchemaMigration.Strict }
            dbSchemaVersion <- SchemaMigration.readVersion dbConnection
            match SchemaMigration.classifyVersion dbSchemaVersion 4 currentSupportedSchemaVersion with
                | SchemaMigration.Established -> ()
                | SchemaMigration.NotApplied -> raise (migrationVerificationError "v3->v4" 4 dbSchemaVersion)
                | SchemaMigration.BeyondSupported ->
                    raise (SchemaMigration.unsupportedVersion dbSchemaVersion currentSupportedSchemaVersion)

        // Migration: version 4 -> 5
        // v5 enforces unique SoloDBCollections.Name for deterministic metadata behavior.
        if dbSchemaVersion = 4 then
            SchemaMigration.run dbConnection
                { Label = "v4->v5"
                  Setup = ""
                  Body = "
                    DELETE FROM SoloDBCollections
                    WHERE rowid NOT IN (
                        SELECT MIN(rowid) FROM SoloDBCollections GROUP BY Name
                    );

                    DROP INDEX IF EXISTS SoloDBCollectionsNameIndex;
                    CREATE UNIQUE INDEX IF NOT EXISTS SoloDBCollectionsNameIndex ON SoloDBCollections(Name);
                  "
                  FromVersion = 4; TargetVersion = 5; SupportedCeiling = currentSupportedSchemaVersion
                  LegacyTextPolicy = SchemaMigration.Strict }
            dbSchemaVersion <- SchemaMigration.readVersion dbConnection
            match SchemaMigration.classifyVersion dbSchemaVersion 5 currentSupportedSchemaVersion with
                | SchemaMigration.Established -> ()
                | SchemaMigration.NotApplied -> raise (migrationVerificationError "v4->v5" 5 dbSchemaVersion)
                | SchemaMigration.BeyondSupported ->
                    raise (SchemaMigration.unsupportedVersion dbSchemaVersion currentSupportedSchemaVersion)

        let fileSystemEvents = FileSystemEventSchema.forExistingTables dbConnection
        if dbSchemaVersion = 5 || fileSystemEvents.Length <> 0 then
            let label = if dbSchemaVersion = 5 then "v5->v6" else "filesystem event initialization"
            SchemaMigration.run dbConnection
                { Label = label
                  Setup = ""; Body = fileSystemEvents
                  FromVersion = dbSchemaVersion
                  TargetVersion = currentSupportedSchemaVersion
                  SupportedCeiling = currentSupportedSchemaVersion
                  LegacyTextPolicy = SchemaMigration.Strict }
            dbSchemaVersion <- SchemaMigration.readVersion dbConnection
            match SchemaMigration.classifyVersion dbSchemaVersion currentSupportedSchemaVersion currentSupportedSchemaVersion with
            | SchemaMigration.Established -> ()
            | SchemaMigration.NotApplied -> raise (migrationVerificationError label currentSupportedSchemaVersion dbSchemaVersion)
            | SchemaMigration.BeyondSupported -> raise (SchemaMigration.unsupportedVersion dbSchemaVersion currentSupportedSchemaVersion)

        // https://www.sqlite.org/pragma.html#pragma_optimize
        let mutable statement = dbConnection.OpenStatement("PRAGMA optimize=0x10002;")
        try
            while statement.Step() do ()
            statement.Dispose()
        with error -> statement.Abort(error); reraise()
