namespace SoloDatabase

open System
open System.Runtime.ExceptionServices
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
    /// Declares a connection unfit for reuse. In production this is a field set on the pooled
    /// wrapper, which does not throw; the type says so rather than dressing a throwable callback up
    /// as an abstraction.
    ///
    /// Marking alone is not enough to be safe, because if it does not take effect the wrapper still
    /// looks reusable and the pool will probe it — raising a second exception over the failure that
    /// made it unfit. So a marking that fails is followed by closing the underlying connection,
    /// which makes reuse physically impossible and routes the pool to its dispose path instead of
    /// its raising one.
    /// </summary>
    type internal Quarantine =
        { MarkUnusable: unit -> unit }

    /// <summary>
    /// Combines the step's own failure with every cleanup failure into one outcome.
    ///
    /// The rule this exists to keep: a cleanup failure never replaces the failure that caused it.
    /// The original is always the inner cause, and cleanup failures are reported alongside it. It is
    /// separate from the statement execution so that every combination can be exercised directly,
    /// since an ordinary SQLite success is no evidence at all about the error branches.
    /// </summary>
    let internal composeOutcome (label: string) (primary: exn voption) (cleanups: string voption list) =
        let cleanupMessages = cleanups |> List.choose (function ValueSome m -> Some m | ValueNone -> None)
        let joined = String.Join("; ", cleanupMessages)
        match primary, cleanupMessages with
        | ValueNone, [] -> ValueNone
        | ValueSome failure, [] -> ValueSome(ExceptionDispatchInfo.Capture failure)
        | ValueNone, messages ->
            ValueSome(ExceptionDispatchInfo.Capture(InvalidOperationException
                $"Error: migration {label} left the connection unusable.\nReason: {joined}.\nFix: do not reuse this connection; investigate the migration step."))
        | ValueSome failure, messages ->
            ValueSome(ExceptionDispatchInfo.Capture(InvalidOperationException(
                $"Error: migration {label} failed and could not be cleaned up.\nReason: {failure.Message}\nCleanup: {joined}.\nFix: do not reuse this connection; investigate the migration step.",
                failure)))

    /// <summary>
    /// Runs one migration step inside an exclusive transaction and establishes its version.
    ///
    /// On failure the transaction is rolled back and the original error is raised. Cleanup is fail
    /// closed: if the rollback itself fails, or the connection is still inside a transaction
    /// afterwards, that is reported rather than swallowed, with the original failure carried as the
    /// inner cause. A connection must never return to the pool holding a transaction, and this
    /// function will not let one leave quietly.
    ///
    /// Version verification is not performed here. The caller re-reads the version after this
    /// returns, so a verification failure is outside this scope entirely.
    /// </summary>
    let internal run (connection: SqliteConnection) (quarantine: Quarantine) (plan: Plan) =
        let handle =
            match connection.Handle with
            | null -> raise (InvalidOperationException $"Error: the connection for migration {plan.Label} exposes no handle.\nReason: schema migration requires a live connection.\nFix: open the connection before migrating.")
            | h -> h

        // Quarantining is itself capable of failing, and swallowing that would put a connection the
        // owner could not mark back into service — recreating the masking defect from the other side.
        let mutable quarantineFailure : string voption = ValueNone
        let quarantineConnection () =
            try quarantine.MarkUnusable ()
            with quarantineError ->
                // Marking did not take effect, so the wrapper still looks reusable. Close the
                // underlying connection: reuse becomes impossible and the pool disposes it rather
                // than probing it and raising over the report we are about to make.
                let closed =
                    try connection.Close(); "the connection was closed so it cannot be reused"
                    with closeError -> $"and closing it also failed: {closeError.Message}"
                quarantineFailure <-
                    ValueSome $"the connection could not be quarantined: {quarantineError.Message}; {closed}"

        // Setup and acquisition run before there is anything to restore, and either can fail. They
        // are classified here rather than escaping as an unclassified constructor failure that leaves
        // a borrowed connection nobody has judged.
        let mutable previousDqsForDdl = ValueNone
        let mutable primary : exn voption = ValueNone
        try
            executeStatements connection plan.Label plan.Setup
            match plan.LegacyTextPolicy with
            | Strict -> ()
            | TolerateHistoricalText ->
                // Read first, then enable: the read establishes what restoration must put back, so a
                // failure to enable still has a known prior value.
                let previous = readDqsForDdl handle
                previousDqsForDdl <- ValueSome previous
                configureDqsForDdl handle 1 |> ignore
        with acquisitionError ->
            primary <- ValueSome acquisitionError
            quarantineConnection ()

        if primary.IsNone then
         try
            executeStatements connection plan.Label "BEGIN EXCLUSIVE;"
            executeStatements connection plan.Label plan.Body
            executeStatements connection plan.Label $"PRAGMA user_version = {plan.TargetVersion};"
            executeStatements connection plan.Label "COMMIT TRANSACTION;"
         with ex ->
            primary <- ValueSome ex

        // Cleanup is part of the contract, so each cleanup failure is a result rather than something
        // to discard — and, just as importantly, none of them may replace the failure that started
        // it. Restoring the compatibility setting is a cleanup step like the rollback: it is
        // capable of failing, so it is captured rather than executed as a bare statement between
        // collecting the outcome and reporting it.
        let transactionCleanup =
            if inAutocommit connection then ValueNone
            else
                try
                    executeStatements connection plan.Label "ROLLBACK;"
                    if inAutocommit connection then ValueNone
                    else ValueSome "the connection is still inside a transaction after rolling back"
                with rollbackError ->
                    ValueSome $"the rollback itself failed: {rollbackError.Message}"

        let restorationCleanup =
            match previousDqsForDdl with
            | ValueNone -> ValueNone
            | ValueSome previous ->
                try
                    configureDqsForDdl handle previous |> ignore
                    ValueNone
                with restoreError ->
                    ValueSome $"the compatibility setting could not be restored: {restoreError.Message}"

        // A cleanup failure means the connection's state is no longer something the pool can reason
        // about, so it is quarantined before this scope exits. A step that failed but rolled back
        // cleanly leaves a usable connection and is not quarantined.
        let cleanupFailed = [ transactionCleanup; restorationCleanup ] |> List.exists (fun c -> c.IsSome)
        if cleanupFailed then quarantineConnection ()

        match composeOutcome plan.Label primary [ transactionCleanup; restorationCleanup; quarantineFailure ] with
        | ValueNone -> ()
        | ValueSome report -> report.Throw()
