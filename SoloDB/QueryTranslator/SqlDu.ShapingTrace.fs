module SoloDatabase.ShapingTrace

open SqlDu.Engine.C1.Spec
open SoloDatabase.PassRunner
open SoloDatabase.IndexModel
open SoloDatabase.ExpressionMatcher
open SoloDatabase.SelectCoreBoundary

// ══════════════════════════════════════════════════════════════
// Index-shaping decision trace.
//
// Per-shape audit artifact with verdict, reason code,
// before/after fingerprints, and changed flag.
//
// Verdict taxonomy:
//   RESHAPED_FOR_INDEX  — index-aware canonicalized expression form(s)
//   PRESERVED_FOR_INDEX — expressions already match index form
//   NO_CHANGE           — no index-relevant adjustment possible
//
// Reason codes enumerate WHY the verdict was reached.
// ══════════════════════════════════════════════════════════════

/// Shaping verdict: what happened to the statement.
type ShapingVerdict =
    | ReshapedForIndex
    | PreservedForIndex
    | NoChange

/// Reason code: why the verdict was reached.
type ShapingReason =
    | PredicateCanonicalized
    | OrderByAligned
    | JoinProbeReordered
    | AlreadyInIndexForm
    | MustNotShapeBoundary
    | UnionAllBoundary
    | DmlPassthrough
    | NoMatchingIndex
    | ExpressionMismatch

/// A single shaping decision trace record.
type ShapingDecisionRecord = {
    ShapeId: string
    Verdict: ShapingVerdict
    ReasonCode: ShapingReason
    BeforeFingerprint: string
    AfterFingerprint: string
    Changed: bool
}

// ── Classification helpers ──────────────────────────────────

/// Check if an expression or operand matches an index.
/// Cast nodes are matched as whole nodes only; inner cast operands are excluded.
let private exprOrOperandMatchesIndex (model: IndexModel) (tableName: string) (expr: SqlExpr) : bool =
    let hasCastMatch =
        SqlExpr.exists
            (fun node ->
                match node with
                | Cast _ -> hasMatchingIndex model tableName node
                | _ -> false)
            expr
    if hasCastMatch then true
    else
        let withoutCastSubtrees =
            SqlExpr.map
                (fun node ->
                    match node with
                    | Cast _ -> Literal Null
                    | _ -> node)
                expr
        SqlExpr.exists (fun node -> hasMatchingIndex model tableName node) withoutCastSubtrees

/// Check if any expression in a list has a matching index (including sub-expressions).
let private anyExprMatchesIndex (model: IndexModel) (tableName: string) (exprs: SqlExpr list) : bool =
    exprs |> List.exists (fun e -> exprOrOperandMatchesIndex model tableName e)

/// Extract all index-relevant expressions from a SelectCore (WHERE operands, ORDER BY, JOIN ON).
let private extractRelevantExprs (core: SelectCore) : SqlExpr list =
    let whereExprs =
        match core.Where with
        | Some w -> [w]
        | None -> []
    let orderExprs = core.OrderBy |> List.map (fun ob -> ob.Expr)
    let joinOnExprs =
        core.Joins |> List.choose (fun j -> j.On)
    whereExprs @ orderExprs @ joinOnExprs

/// Classify a single shape's decision based on model, input, and output.
let classifyShaping (model: IndexModel) (shapeId: string) (input: SqlStatement) (output: SqlStatement) : ShapingDecisionRecord =
    let beforeFp = fingerprint input
    let afterFp = fingerprint output
    let changed = beforeFp <> afterFp

    let verdict, reason =
        match input with
        // DML passthrough
        | InsertStmt _ | UpdateStmt _ | DeleteStmt _ | DdlStmt _ ->
            NoChange, DmlPassthrough

        | SelectStmt sel ->
            match sel.Body with
            // UNION ALL boundary — hard fence
            | UnionAllSelect _ ->
                NoChange, UnionAllBoundary

            | SingleSelect core ->
                // Must-not-shape boundary
                if hasMustNotBoundary core then
                    NoChange, MustNotShapeBoundary
                elif hasAggregateOrWindowProjections core then
                    NoChange, MustNotShapeBoundary
                else
                    let tableName =
                        match core.Source with
                        | Some(BaseTable(name, _)) -> Some name
                        | _ -> None

                    match tableName with
                    | None ->
                        NoChange, NoMatchingIndex
                    | Some tName ->
                        if changed then
                            // Determine which adjustment fired
                            let inputCore = core
                            let outputCore =
                                match output with
                                | SelectStmt outSel ->
                                    match outSel.Body with
                                    | SingleSelect c -> Some c
                                    | _ -> None
                                | _ -> None

                            match outputCore with
                            | None ->
                                ReshapedForIndex, PredicateCanonicalized
                            | Some outCore ->
                                // Check what changed
                                let whereChanged =
                                    (fingerprint (SelectStmt { Ctes = []; Body = SingleSelect { inputCore with OrderBy = []; Joins = [] } }))
                                    <> (fingerprint (SelectStmt { Ctes = []; Body = SingleSelect { outCore with OrderBy = []; Joins = [] } }))
                                let orderChanged =
                                    inputCore.OrderBy <> outCore.OrderBy
                                let joinsChanged =
                                    inputCore.Joins <> outCore.Joins

                                if joinsChanged then ReshapedForIndex, JoinProbeReordered
                                elif orderChanged then ReshapedForIndex, OrderByAligned
                                elif whereChanged then ReshapedForIndex, PredicateCanonicalized
                                else ReshapedForIndex, PredicateCanonicalized
                        else
                            // No change — determine why
                            let relevantExprs = extractRelevantExprs core
                            let anyMatch = anyExprMatchesIndex model tName relevantExprs
                            if anyMatch then
                                PreservedForIndex, AlreadyInIndexForm
                            else
                                NoChange, NoMatchingIndex

    { ShapeId = shapeId
      Verdict = verdict
      ReasonCode = reason
      BeforeFingerprint = beforeFp
      AfterFingerprint = afterFp
      Changed = changed }

/// Classify a batch of shapes, returning one decision record per shape.
let classifyBatch (model: IndexModel) (shapes: (string * SqlStatement * SqlStatement) list) : ShapingDecisionRecord list =
    shapes |> List.map (fun (id, input, output) -> classifyShaping model id input output)

/// Format a decision record as a single-line JSON (for c8-audit.jsonl).
let formatAsJsonl (record: ShapingDecisionRecord) : string =
    let verdictStr =
        match record.Verdict with
        | ReshapedForIndex -> "RESHAPED_FOR_INDEX"
        | PreservedForIndex -> "PRESERVED_FOR_INDEX"
        | NoChange -> "NO_CHANGE"
    let reasonStr =
        match record.ReasonCode with
        | PredicateCanonicalized -> "PredicateCanonicalized"
        | OrderByAligned -> "OrderByAligned"
        | JoinProbeReordered -> "JoinProbeReordered"
        | AlreadyInIndexForm -> "AlreadyInIndexForm"
        | MustNotShapeBoundary -> "MustNotShapeBoundary"
        | UnionAllBoundary -> "UnionAllBoundary"
        | DmlPassthrough -> "DmlPassthrough"
        | NoMatchingIndex -> "NoMatchingIndex"
        | ExpressionMismatch -> "ExpressionMismatch"
    sprintf "{\"shapeId\":\"%s\",\"verdict\":\"%s\",\"reasonCode\":\"%s\",\"beforeFingerprint\":\"%s\",\"afterFingerprint\":\"%s\",\"changed\":%s}"
        record.ShapeId verdictStr reasonStr
        record.BeforeFingerprint record.AfterFingerprint
        (if record.Changed then "true" else "false")
