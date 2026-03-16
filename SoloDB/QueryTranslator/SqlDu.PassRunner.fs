module SoloDatabase.PassRunner

open SqlDu.Engine.C1.Spec
open SoloDatabase.PassTypes
open SoloDatabase.PathCanonicalizer
open SoloDatabase.ProjectionLiveness
open SoloDatabase.Provenance

let private addMetric (d1, u1, p1, w1) (d2, u2, p2, w2) =
    (d1 + d2, u1 + u2, p1 + p2, w1 + w2)

let private metricIsLower candidate current =
    compare candidate current < 0

let private verifyStatement (stmt: SqlStatement) =
    try
        EmitStatement.emitStatement (EmitContext(InlineLiterals = true)) stmt |> ignore
        true
    with _ ->
        false

let rec private exprMetric (expr: SqlExpr) =
    let localWrapperCount =
        SqlExpr.fold
            (fun count node ->
                if canonicalizeJsonbExpr node |> Option.isSome then count + 1
                else count)
            0
            expr

    let recurseList xs =
        xs |> List.fold (fun acc item -> addMetric acc (exprMetric item)) (0, 0, 0, 0)

    let recurseBranches firstBranch restBranches =
        (firstBranch :: restBranches)
        |> List.fold (fun acc (condExpr, resultExpr) ->
            let acc = addMetric acc (exprMetric condExpr)
            addMetric acc (exprMetric resultExpr)) (0, 0, 0, 0)

    let subqueryMetric =
        match expr with
        | InSubquery(valueExpr, subquery) ->
            addMetric (exprMetric valueExpr) (selectMetric subquery)
        | Exists subquery
        | ScalarSubquery subquery ->
            selectMetric subquery
        | JsonSetExpr(target, assignments) ->
            assignments
            |> List.fold (fun acc (_, valueExpr) -> addMetric acc (exprMetric valueExpr)) (exprMetric target)
        | JsonArrayExpr elements
        | FunctionCall(_, elements) ->
            recurseList elements
        | JsonObjectExpr properties ->
            properties
            |> List.fold (fun acc (_, valueExpr) -> addMetric acc (exprMetric valueExpr)) (0, 0, 0, 0)
        | AggregateCall(_, argument, _, separator) ->
            let acc =
                match argument with
                | Some arg -> exprMetric arg
                | None -> (0, 0, 0, 0)
            match separator with
            | Some sep -> addMetric acc (exprMetric sep)
            | None -> acc
        | WindowCall spec ->
            let acc = recurseList spec.Arguments
            let acc = addMetric acc (recurseList spec.PartitionBy)
            spec.OrderBy
            |> List.fold (fun total (orderExpr, _) -> addMetric total (exprMetric orderExpr)) acc
        | Unary(_, inner)
        | Cast(inner, _) ->
            exprMetric inner
        | Binary(left, _, right) ->
            addMetric (exprMetric left) (exprMetric right)
        | Between(valueExpr, lower, upper) ->
            let acc = addMetric (exprMetric valueExpr) (exprMetric lower)
            addMetric acc (exprMetric upper)
        | InList(valueExpr, head, tail) ->
            let acc = addMetric (exprMetric valueExpr) (exprMetric head)
            addMetric acc (recurseList tail)
        | Coalesce(head, tail) ->
            addMetric (exprMetric head) (recurseList tail)
        | CaseExpr(firstBranch, restBranches, elseExpr) ->
            let acc = recurseBranches firstBranch restBranches
            match elseExpr with
            | Some elseNode -> addMetric acc (exprMetric elseNode)
            | None -> acc
        | UpdateFragment(pathExpr, valueExpr) ->
            addMetric (exprMetric pathExpr) (exprMetric valueExpr)
        | Column _
        | Literal _
        | Parameter _
        | JsonExtractExpr _
        | JsonRootExtract _ ->
            (0, 0, 0, 0)

    addMetric (0, 0, 0, localWrapperCount) subqueryMetric

and private sourceMetric source =
    match source with
    | BaseTable _ ->
        (0, 0, 0, 0)
    | FromJsonEach(valueExpr, _) ->
        exprMetric valueExpr
    | DerivedTable(query, _) ->
        addMetric (1, 0, 0, 0) (selectMetric query)

and private joinMetric joinShape =
    match joinShape with
    | CrossJoin source ->
        sourceMetric source
    | ConditionedJoin(_, source, onExpr) ->
        addMetric (sourceMetric source) (exprMetric onExpr)

and private deadProjectionCount (core: SelectCore) =
    match core.Source with
    | Some(DerivedTable(innerSel, derivedAlias)) ->
        match innerSel.Body with
        | SingleSelect innerCore ->
            computeDeadProjections core innerCore derivedAlias
            |> Option.map Set.count
            |> Option.defaultValue 0
        | UnionAllSelect _ ->
            0
    | _ ->
        0

and private coreMetric (core: SelectCore) =
    let unresolvedCount =
        buildForCore core
        |> Map.toSeq
        |> Seq.sumBy (fun (_, source) ->
            match source with
            | Opaque -> 1
            | _ -> 0)

    let projectionMetric =
        core.Projections
        |> ProjectionSetOps.toList
        |> List.fold (fun acc projection -> addMetric acc (exprMetric projection.Expr)) (0, 0, 0, 0)

    let whereMetric =
        match core.Where with
        | Some whereExpr -> exprMetric whereExpr
        | None -> (0, 0, 0, 0)

    let groupByMetric =
        core.GroupBy |> List.fold (fun acc groupExpr -> addMetric acc (exprMetric groupExpr)) (0, 0, 0, 0)

    let havingMetric =
        match core.Having with
        | Some havingExpr -> exprMetric havingExpr
        | None -> (0, 0, 0, 0)

    let orderByMetric =
        core.OrderBy
        |> List.fold (fun acc orderBy -> addMetric acc (exprMetric orderBy.Expr)) (0, 0, 0, 0)

    let sourceAndJoinMetric =
        let sourceContribution =
            match core.Source with
            | Some source -> sourceMetric source
            | None -> (0, 0, 0, 0)
        core.Joins
        |> List.fold (fun acc joinShape -> addMetric acc (joinMetric joinShape)) sourceContribution

    addMetric
        (0, unresolvedCount, deadProjectionCount core, 0)
        (projectionMetric
         |> fun acc -> addMetric acc whereMetric
         |> fun acc -> addMetric acc groupByMetric
         |> fun acc -> addMetric acc havingMetric
         |> fun acc -> addMetric acc orderByMetric
         |> fun acc -> addMetric acc sourceAndJoinMetric)

and private bodyMetric body =
    match body with
    | SingleSelect core ->
        coreMetric core
    | UnionAllSelect(head, tail) ->
        tail
        |> List.fold (fun acc core -> addMetric acc (coreMetric core)) (coreMetric head)

and private selectMetric (select: SqlSelect) =
    let cteMetric =
        select.Ctes
        |> List.fold (fun acc cte -> addMetric acc (selectMetric cte.Query)) (0, 0, 0, 0)
    addMetric cteMetric (bodyMetric select.Body)

let private statementMetric (stmt: SqlStatement) =
    match stmt with
    | SelectStmt select ->
        selectMetric select
    | InsertStmt insert ->
        insert.Values
        |> List.fold (fun acc row ->
            row |> List.fold (fun rowAcc expr -> addMetric rowAcc (exprMetric expr)) acc) (0, 0, 0, 0)
    | UpdateStmt update ->
        let setMetric =
            update.SetClauses
            |> List.fold (fun acc (_, expr) -> addMetric acc (exprMetric expr)) (0, 0, 0, 0)
        match update.Where with
        | Some whereExpr -> addMetric setMetric (exprMetric whereExpr)
        | None -> setMetric
    | DeleteStmt delete ->
        match delete.Where with
        | Some whereExpr -> exprMetric whereExpr
        | None -> (0, 0, 0, 0)
    | DdlStmt _ ->
        (0, 0, 0, 0)

/// Compute a structural fingerprint of a SqlStatement by emitting SQL and hashing.
/// Uses the canonical product emitter for deterministic emission, then FNV-1a 64-bit hash.
let fingerprint (stmt: SqlStatement) : string =
    let emitted = EmitStatement.emitStatement (EmitContext(InlineLiterals = true)) stmt
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

/// Continue optimization rounds to a deterministic fixed point.
/// A round is accepted only if it lowers the canonical metric, verifies,
/// and reaches a fingerprint frontier point not seen before.
let runPipelineToFixedPoint (passes: Pass list) (seed: PipelineResult) : PipelineResult =
    let seen = System.Collections.Generic.HashSet<string>()
    let mutable current = seed.Output
    let mutable trail = seed.AuditTrail
    let mutable continueRounds = true

    while continueRounds do
        let currentFp = fingerprint current
        let currentMetric = statementMetric current
        seen.Add(currentFp) |> ignore

        let round = runPipeline passes current
        let candidate = round.Output
        let candidateFp = fingerprint candidate
        let candidateMetric = statementMetric candidate

        let acceptCandidate =
            candidateFp <> currentFp
            && not (seen.Contains candidateFp)
            && verifyStatement candidate
            && metricIsLower candidateMetric currentMetric

        if acceptCandidate then
            current <- candidate
            trail <- trail @ round.AuditTrail
        else
            continueRounds <- false

    { Input = seed.Input; Output = current; AuditTrail = trail }
