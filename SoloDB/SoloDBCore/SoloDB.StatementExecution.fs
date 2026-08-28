namespace SoloDatabase

open System.Collections.Generic
open Microsoft.Data.Sqlite
open SQLiteTools
open SqlDu.Engine.C1.Spec

/// Canonical execution of a typed SQL statement.
///
/// Statement building and statement execution used to be separated only inside one mutation branch,
/// where the executor was a local closure over the connections it happened to have. That left every
/// other corridor assembling SQL as text. This module is the single owner: it takes the connection
/// explicitly, emits canonically, reports the SQL, and executes.
///
/// This corridor used to run the optimizer pipeline as well, choosing between canonical emission and
/// an index-shaped pipeline, discovering the tables a statement touches and loading an index model
/// for them from a second connection. All of that is gone, and deliberately so: measured across the
/// whole Release suite, the pipeline changed **no** statement that reached here, and bypassing it
/// failed no test. It was cost and machinery on a write path with no observable effect, and it made
/// this module carry an index-model connection that the transactional callers did not want. If a
/// mutation shape is ever found that the optimizer genuinely improves, this is the place to
/// reintroduce it, with that shape as the test that justifies it.
module internal StatementExecution =

    /// Emit a statement to SQL, reporting it to the capture boundary.
    let emit (stmt: SqlStatement) : string =
        let emitted = EmitStatement.emitStatement (EmitContext(InlineLiterals = true)) stmt
        SqlCapture.OnSqlEmitted |> Option.iter (fun cb -> cb emitted.Sql)
        emitted.Sql

    /// Emit and execute, returning the affected row count.
    let execute
        (executionConnection: SqliteConnection)
        (stmt: SqlStatement)
        (variables: Dictionary<string, obj>) : int =
        executionConnection.Execute(emit stmt, variables)
