module SoloDatabase.PassRunner

open SqlDu.Engine.C1.Spec
open SoloDatabase.PassTypes

/// Compute a structural fingerprint of a SqlStatement by emitting SQL and hashing.
/// Uses the canonical product emitter for deterministic emission, then FNV-1a 64-bit hash.
let fingerprint (stmt: SqlStatement) : string =
    let emitted = EmitStatement.emitStatement (EmitContext()) stmt
    let mutable h = 14695981039346656037UL
    for c in emitted.Sql do
        h <- (h ^^^ uint64 c) * 1099511628211UL
    sprintf "%016X" h

/// Run a single pass, producing an audit row and the output statement.
let runPass (pass: Pass) (input: SqlStatement) : PassAuditRow * SqlStatement =
    let inputFp = fingerprint input
    let output = pass.Transform input
    let outputFp = fingerprint output
    let audit = {
        PassName = pass.Name
        InputFingerprint = inputFp
        OutputFingerprint = outputFp
        Changed = inputFp <> outputFp
    }
    (audit, output)

/// Run an ordered list of passes, returning the pipeline result with full audit trail.
let runPipeline (passes: Pass list) (input: SqlStatement) : PipelineResult =
    let mutable current = input
    let mutable trail = []
    for pass in passes do
        let (audit, output) = runPass pass current
        trail <- trail @ [audit]
        current <- output
    { Input = input; Output = current; AuditTrail = trail }
