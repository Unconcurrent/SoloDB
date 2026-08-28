namespace SoloDatabase

open System
open Microsoft.Data.Sqlite
open SQLitePCL

/// <summary>
/// The single owner of transactional schema migration execution.
///
/// Every migration step used to be its own copy of the same shape: one command text carrying
/// <c>BEGIN EXCLUSIVE</c>, the work, a version assignment and <c>COMMIT</c>, submitted in one call.
/// That shape is silent about failure. A statement inside such a batch can be rejected while the
/// call returns normally, leaving the work half done, the version unchanged and the transaction
/// open, with nothing raised at the point of failure. The only thing that noticed was the version
/// check afterwards, which could say a migration did not happen but never which statement failed
/// or why.
///
/// This module executes the same SQL as its own statements, with boundaries decided by the SQLite
/// parser rather than by searching the text for separators. Each statement is prepared, stepped and
/// finalised in order, so a failure is raised where it happens and names the statement that caused
/// it. Trigger bodies and other compound statements stay whole, because the parser decides where
/// they end.
///
/// This is not the fix for any particular engine incompatibility. It is the structural owner, and
/// its contribution is that the next incompatibility reports itself precisely instead of appearing
/// later as an unexplained version mismatch.
/// </summary>
module internal SchemaMigration =

    /// <summary>
    /// Whether a step may re-parse schema text written under the historical double-quoted-string
    /// spelling.
    ///
    /// This is declared per step rather than inferred from whether a step happens to run. Only one
    /// step can encounter that spelling: the first altering step, which re-renders the stored schema
    /// of a database created before the spelling was corrected. Every other step either writes the
    /// schema itself or operates on text already normalised by that step. Leaving the tolerance on
    /// elsewhere would quietly accept newly authored double-quoted literals, which is the very check
    /// this work exists to keep.
    /// </summary>
    type internal LegacySchemaTextPolicy =
        /// The engine's own rules apply. Newly authored DDL is judged exactly as it will be in production.
        | Strict
        /// Historical stored schema text may re-parse for the duration of this step only.
        | TolerateHistoricalText

    /// <summary>One migration step: what it is called, what it does, and the version it establishes.</summary>
    type internal Plan =
        {
            /// Diagnostic label, for example "v0->v1". Used in raised errors.
            Label: string
            /// Statements executed before the transaction opens, for settings that cannot be
            /// changed inside one. Empty for most steps.
            Setup: string
            /// The body of the migration. May be empty when a step has nothing to do.
            Body: string
            /// The schema version this step establishes on success.
            TargetVersion: int
            /// Whether this step may re-parse historical double-quoted schema text.
            LegacyTextPolicy: LegacySchemaTextPolicy
        }

    /// <summary>
    /// Historical schema text used double-quoted string literals in CHECK constraints, which older
    /// engines accepted through a long-standing fallback. Newer builds disable that fallback, and a
    /// step that alters a table makes SQLite re-render the whole stored schema, re-parsing those
    /// constraints. A database written years ago therefore cannot be migrated on a current engine
    /// even though the migration itself is well formed.
    ///
    /// The bridge is connection-local and lasts exactly as long as one migration step: the DDL
    /// tolerance is switched on to let historical text re-parse, and the previous value is restored
    /// afterwards on every path. It is not switched on for data, and it is never left on, so newly
    /// submitted DDL is judged by the engine's own rules the moment the step ends.
    /// </summary>
    let private configureDqsForDdl (handle: sqlite3) (value: int) =
        let mutable current = 0
        let resultCode = raw.sqlite3_db_config (handle, raw.SQLITE_DBCONFIG_DQS_DDL, value, &current)
        if resultCode <> raw.SQLITE_OK then
            raise (InvalidOperationException $"Error: the schema migration compatibility setting could not be applied.\nReason: sqlite3_db_config returned {resultCode}.\nFix: use a SQLite build that supports the DQS_DDL configuration.")
        current

    /// <summary>Reads the current DDL tolerance without changing it.</summary>
    let private readDqsForDdl (handle: sqlite3) = configureDqsForDdl handle -1

    /// <summary>Is the connection free of an open transaction?</summary>
    let private inAutocommit (connection: SqliteConnection) =
        match connection.Handle with
        | null -> true
        | handle -> raw.sqlite3_get_autocommit handle <> 0

    /// <summary>
    /// Executes every statement in <paramref name="sql"/>, in order, as separate statements.
    /// Boundaries come from the parser: each prepare reports the text it consumed, and the
    /// remainder is prepared again. Text the parser consumes without producing a statement is
    /// whitespace or comment and is skipped.
    /// </summary>
    let private executeStatements (connection: SqliteConnection) (label: string) (sql: string) =
        if not (String.IsNullOrWhiteSpace sql) then
            let handle =
                match connection.Handle with
                | null -> raise (InvalidOperationException $"Error: the connection for migration {label} exposes no handle.\nReason: schema migration requires a live connection.\nFix: open the connection before migrating.")
                | h -> h

            let bytes = Text.Encoding.UTF8.GetBytes sql
            let mutable offset = 0
            let mutable finished = false

            while not finished && offset < bytes.Length do
                let remaining = ReadOnlySpan<byte>(bytes, offset, bytes.Length - offset)
                let mutable statement = Unchecked.defaultof<sqlite3_stmt>
                let mutable tail = ReadOnlySpan<byte>()
                let prepareResult = raw.sqlite3_prepare_v2 (handle, remaining, &statement, &tail)

                if prepareResult <> raw.SQLITE_OK then
                    // A statement that will not parse has no parser-defined end, so the excerpt is
                    // bounded text from the point of failure. Without it the report would name the
                    // step but not the SQL, which is the gap this owner exists to close.
                    let excerptLength = min 200 (bytes.Length - offset)
                    let excerpt = Text.Encoding.UTF8.GetString(bytes, offset, excerptLength).Trim()
                    let message = raw.sqlite3_errmsg(handle).utf8_to_string ()
                    raise (SqliteException($"Error: migration {label} could not prepare a statement.\nReason: {message}\nStatement: {excerpt}\nFix: correct the migration SQL for this schema step.", prepareResult))

                let consumed = remaining.Length - tail.Length
                if consumed <= 0 then
                    finished <- true
                else
                    if not (isNull (box statement)) && not statement.IsInvalid then
                        try
                            let mutable stepResult = raw.sqlite3_step statement
                            while stepResult = raw.SQLITE_ROW do
                                stepResult <- raw.sqlite3_step statement

                            if stepResult <> raw.SQLITE_DONE then
                                // The statement text is materialised only to describe a failure.
                                // Building it for every successful statement would put the whole
                                // migration script back on the allocation path of ordinary startup.
                                let statementText = Text.Encoding.UTF8.GetString(bytes, offset, consumed).Trim()
                                let message = raw.sqlite3_errmsg(handle).utf8_to_string ()
                                raise (SqliteException($"Error: migration {label} failed on a statement.\nReason: {message}\nStatement: {statementText}\nFix: correct this statement or the schema it depends on.", stepResult))
                        finally
                            statement.Dispose()

                    offset <- offset + consumed

    /// <summary>
    /// Runs one migration step inside an exclusive transaction and establishes its version.
    ///
    /// On any failure the transaction is rolled back and the original error is raised. The
    /// connection never leaves this function with a transaction still open, on any path, so a
    /// failed migration cannot be mistaken later for an unrelated pooling fault.
    /// </summary>
    let internal run (connection: SqliteConnection) (plan: Plan) =
        executeStatements connection plan.Label plan.Setup

        let handle =
            match connection.Handle with
            | null -> raise (InvalidOperationException $"Error: the connection for migration {plan.Label} exposes no handle.\nReason: schema migration requires a live connection.\nFix: open the connection before migrating.")
            | h -> h

        // Preserved exactly, and restored below whatever happens: success, a failing statement, a
        // failed rollback, or a failed verification afterwards. A strict step never touches the
        // setting at all, so it cannot silently widen what the engine accepts.
        let previousDqsForDdl =
            match plan.LegacyTextPolicy with
            | Strict -> ValueNone
            | TolerateHistoricalText ->
                let previous = readDqsForDdl handle
                configureDqsForDdl handle 1 |> ignore
                ValueSome previous

        let mutable committed = false
        try
            try
                executeStatements connection plan.Label "BEGIN EXCLUSIVE;"
                executeStatements connection plan.Label plan.Body
                executeStatements connection plan.Label $"PRAGMA user_version = {plan.TargetVersion};"
                executeStatements connection plan.Label "COMMIT TRANSACTION;"
                committed <- true
            with _ ->
                // Roll back before the exception leaves, so the failure is observed as a failed
                // migration rather than as a later symptom on a connection nobody expected to be
                // inside a transaction.
                if not (inAutocommit connection) then
                    try executeStatements connection plan.Label "ROLLBACK;" with _ -> ()
                reraise ()
        finally
            if not committed && not (inAutocommit connection) then
                try executeStatements connection plan.Label "ROLLBACK;" with _ -> ()
            match previousDqsForDdl with
            | ValueSome previous -> configureDqsForDdl handle previous |> ignore
            | ValueNone -> ()
