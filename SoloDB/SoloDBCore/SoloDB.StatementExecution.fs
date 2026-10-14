namespace SoloDatabase

open System.Collections.Generic
open NativeSQLiteTools
open SoloDatabase.SqlModel

/// Emits typed SQL through the canonical emitter and executes it on the supplied connection.
/// The owning native statement scope retains binding, fault and cleanup authority.
module internal StatementExecution =

    /// Emit a statement through the canonical SQL owner.
    let emit (stmt: SqlStatement) : string =
        let emitted = EmitStatement.emitStatement (EmitContext(InlineLiterals = true)) stmt
        emitted.Sql

    /// Emit and execute, returning the affected row count.
    let execute
        (executionConnection: NativeSqliteConnection)
        (stmt: SqlStatement)
        (variables: Dictionary<string, obj>) : int =
        let mutable statement = executionConnection.OpenStatement(emit stmt)
        try
            statement.BindValues(variables)
            while statement.Step() do ()
            let affected = statement.RecordsAffected
            statement.Dispose()
            affected
        with error -> statement.Abort(error); reraise()
