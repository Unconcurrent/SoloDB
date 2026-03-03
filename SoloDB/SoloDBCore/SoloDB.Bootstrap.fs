namespace SoloDatabase

open Microsoft.Data.Sqlite
open System
open System.Globalization
open SQLiteTools
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

    let private invalidCatalogNameError (step: string) (name: string) =
        InvalidOperationException($"Schema migration failed at {step}. Invalid collection name in SoloDBCollections: '{name}'.")

    let private sqlLiteral (value: string) =
        value.Replace("'", "''")

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
    /// <returns>A setup function for SqliteConnection.</returns>
    let createSetup (eventSystem: EventSystem) =
        let usCultureInfo = CultureInfo.GetCultureInfo("en-us")
        let regexpTimeout = TimeSpan.FromMilliseconds(100)
        fun (connection: SqliteConnection) ->
            connection.CreateFunction("UNIXTIMESTAMP", Func<int64>(fun () -> DateTimeOffset.Now.ToUnixTimeMilliseconds()), false)
            connection.CreateFunction("SHA_HASH", Func<byte array, obj>(fun o -> Utils.shaHash o), true)
            connection.CreateFunction("TO_LOWER", Func<string, string>(_.ToLower(usCultureInfo)), true)
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
            connection.Execute "PRAGMA recursive_triggers = ON; PRAGMA foreign_keys = on; PRAGMA busy_timeout = 5000;" |> ignore // This must be enabled on every connection separately.

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
    let [<Literal>] initialSchema = "
                PRAGMA journal_mode=wal;
                PRAGMA page_size=16384;
                PRAGMA recursive_triggers = ON;
                PRAGMA foreign_keys = on;

                BEGIN EXCLUSIVE;

                CREATE TABLE SoloDBCollections (Name TEXT NOT NULL) STRICT;


                CREATE TABLE SoloDBDirectoryHeader (
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

                CREATE TABLE SoloDBFileHeader (
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

                PRAGMA user_version = 1;
                COMMIT TRANSACTION;
                "

    /// <summary>
    /// The current supported schema version. The database will refuse to open future versions.
    /// </summary>
    let [<Literal>] currentSupportedSchemaVersion = 5

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
    let initializeSchema (dbConnection: CachingDbConnection) =
        // Edge case 1: SQLite version too old
        let sqliteVersionText = dbConnection.QueryFirst<string> "SELECT sqlite_version();"
        let sqliteVersion = parseSqliteVersion sqliteVersionText

        if sqliteVersion < minimumSqliteVersion then
            raise (NotSupportedException
                $"Error: SQLite version {sqliteVersionText} is too old.\nReason: SoloDB requires SQLite 3.47.0+ (JSONB + trigger RAISE() expressions).\nFix: Upgrade SQLite to 3.47.0 or newer.")

        let mutable dbSchemaVersion = dbConnection.QueryFirst<int> "PRAGMA user_version;";

        // Edge case 2: future schema version
        if dbSchemaVersion > currentSupportedSchemaVersion then
            raise (NotSupportedException
                $"Error: Schema version {dbSchemaVersion} is not supported.\nReason: Current supported version is {currentSupportedSchemaVersion}.\nFix: Migrate the database or update PRAGMA user_version to a compatible version.")

        // Schema creation: version 0 -> 1
        if dbSchemaVersion = 0 then
            use command = new SqliteCommand(initialSchema, dbConnection.Inner)
            // command.Prepare() // It does not work if the referenced tables are not created yet.
            ignore (command.ExecuteNonQuery())
            dbSchemaVersion <- dbConnection.QueryFirst<int> "PRAGMA user_version;"
            if dbSchemaVersion <> 1 then
                raise (migrationVerificationError "v0->v1" 1 dbSchemaVersion)

        // Migration: version 1 -> 2
        if dbSchemaVersion = 1 then
            use command = new SqliteCommand("
                    BEGIN EXCLUSIVE;

                    DROP INDEX SoloDBFileHashIndex;

                    -- The update will be handled inside filestream's code.
                    DROP TRIGGER Update_SoloDBFileHeader;
                    ALTER TABLE SoloDBFileHeader DROP COLUMN \"Hash\";

                    PRAGMA user_version = 2;
                    PRAGMA foreign_keys = on;

                    COMMIT TRANSACTION;
                ", dbConnection.Inner)
            command.Prepare()
            ignore (command.ExecuteNonQuery())
            dbSchemaVersion <- dbConnection.QueryFirst<int> "PRAGMA user_version;"
            if dbSchemaVersion <> 2 then
                raise (migrationVerificationError "v1->v2" 2 dbSchemaVersion)

        // Migration: version 2 -> 3
        if dbSchemaVersion = 2 then
            let triggerSql =
                dbConnection.Query<string>("SELECT Name FROM SoloDBCollections")
                |> Seq.map (fun name ->
                    $"DROP TRIGGER IF EXISTS \"SoloDB_Update_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Insert_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Delete_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Updated_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Inserted_{name}\";\nDROP TRIGGER IF EXISTS \"SoloDB_Deleted_{name}\";\n{Helper.getSQLForTriggersForTable name}")
                |> String.concat "\n"

            use command = new SqliteCommand($"
                    BEGIN EXCLUSIVE;

                    {triggerSql}

                    PRAGMA user_version = 3;

                    COMMIT TRANSACTION;
                ", dbConnection.Inner)
            command.Prepare()
            ignore (command.ExecuteNonQuery())
            dbSchemaVersion <- dbConnection.QueryFirst<int> "PRAGMA user_version;"
            if dbSchemaVersion <> 3 then
                raise (migrationVerificationError "v2->v3" 3 dbSchemaVersion)

        // Migration: version 3 -> 4
        // v3 = triggers/Event API. v4 = Relational API.
        // Ensure SoloDBTypeCollectionMap exists and add Metadata column to existing collections.
        if dbSchemaVersion = 3 then
            let collectionNames =
                dbConnection.Query<string>("SELECT Name FROM SoloDBCollections")
                |> Seq.toArray

            let addMetadataColumnSql =
                collectionNames
                |> Array.choose (fun name ->
                    let normalized = Utils.formatName name
                    if String.IsNullOrWhiteSpace name || normalized <> name then
                        raise (invalidCatalogNameError "v3->v4 catalog-name validation" name)
                    let tableLit = sqlLiteral normalized
                    let hasMetadata =
                        dbConnection.QueryFirst<int64>(
                            $"SELECT CASE WHEN EXISTS (SELECT 1 FROM pragma_table_info('{tableLit}') WHERE name = 'Metadata') THEN 1 ELSE 0 END") = 1L
                    if hasMetadata then None
                    else Some ($"ALTER TABLE \"{normalized}\" ADD COLUMN Metadata JSONB NOT NULL DEFAULT '{{}}';"))
                |> String.concat "\n"

            use command = new SqliteCommand($"
                    BEGIN EXCLUSIVE;

                    {RelationsSharedSql.createTypeCollectionMapTableSql}

                    {addMetadataColumnSql}

                    PRAGMA user_version = 4;
                    COMMIT TRANSACTION;
                ", dbConnection.Inner)
            // command.Prepare() // Cannot Prepare when tables are created in the same command batch.
            ignore (command.ExecuteNonQuery())
            dbSchemaVersion <- dbConnection.QueryFirst<int> "PRAGMA user_version;"
            if dbSchemaVersion <> 4 then
                raise (migrationVerificationError "v3->v4" 4 dbSchemaVersion)

        // Migration: version 4 -> 5
        // v5 enforces unique SoloDBCollections.Name for deterministic metadata behavior.
        if dbSchemaVersion = 4 then
            use command = new SqliteCommand("
                    BEGIN EXCLUSIVE;

                    DELETE FROM SoloDBCollections
                    WHERE rowid NOT IN (
                        SELECT MIN(rowid) FROM SoloDBCollections GROUP BY Name
                    );

                    DROP INDEX IF EXISTS SoloDBCollectionsNameIndex;
                    CREATE UNIQUE INDEX IF NOT EXISTS SoloDBCollectionsNameIndex ON SoloDBCollections(Name);

                    PRAGMA user_version = 5;
                    COMMIT TRANSACTION;
                ", dbConnection.Inner)
            command.Prepare()
            ignore (command.ExecuteNonQuery())
            dbSchemaVersion <- dbConnection.QueryFirst<int> "PRAGMA user_version;"
            if dbSchemaVersion <> 5 then
                raise (migrationVerificationError "v4->v5" 5 dbSchemaVersion)

        // https://www.sqlite.org/pragma.html#pragma_optimize
        let _rez = dbConnection.Execute("PRAGMA optimize=0x10002;")
        ()
