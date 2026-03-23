module internal SoloDatabase.PassTypes

open SqlDu.Engine.C1.Spec

/// A single optimizer pass: a named pure function from statement to statement.
type Pass = {
    Name: string
    Transform: SqlStatement -> struct(SqlStatement * bool)
}

/// Audit row recorded for each pass execution in the pipeline.
type PassAuditRow = {
    PassName: string
    InputFingerprint: string
    OutputFingerprint: string
    Changed: bool
}

/// Result of running a complete pipeline.
type PipelineResult = {
    Input: SqlStatement
    Output: SqlStatement
    AuditTrail: PassAuditRow list
}
