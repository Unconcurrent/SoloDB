module internal SoloDatabase.PassTypes

open SqlDu.Engine.C1.Spec

/// A single optimizer pass: a named pure function from statement to statement.
type Pass = {
    Name: string
    Transform: SqlStatement -> struct(SqlStatement * bool)
}

/// Audit row recorded for each pass execution, for diagnostic callers only.
///
/// This carried two fingerprint fields that were always assigned the empty string, and the hash
/// behind them is not a permitted verification mechanism. Both are gone: what a caller can learn
/// here is which pass ran and whether it changed the statement. Anything comparing statements
/// compares the statements, or their canonical SQL, exactly.
type PassAuditRow = {
    PassName: string
    Changed: bool
}

/// Result of a diagnostic pipeline run.
///
/// There is no `Input` field: nothing ever read it, and the runner only copied it forward.
/// Production does not use this type at all — it receives the optimized statement directly.
type PipelineResult = {
    Output: SqlStatement
    AuditTrail: PassAuditRow list
}
