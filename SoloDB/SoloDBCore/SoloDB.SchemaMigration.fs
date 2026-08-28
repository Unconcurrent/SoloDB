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
        }

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
                    let message = raw.sqlite3_errmsg(handle).utf8_to_string ()
                    raise (SqliteException($"Error: migration {label} could not prepare a statement.\nReason: {message}\nFix: correct the migration SQL for this schema step.", prepareResult))

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
