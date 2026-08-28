module internal SoloDatabase.PassRunner

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
        match insert.Source with
        | InsertValues rows ->
            rows
            |> List.fold (fun acc row ->
                row |> List.fold (fun rowAcc expr -> addMetric rowAcc (exprMetric expr)) acc) (0, 0, 0, 0)
        | InsertSelect select ->
            selectMetric select
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

/// Apply every pass once, reporting the resulting statement and whether any pass changed it.
///
/// This is the production round. It carries no collector: no audit row, no list, no observer, no
/// callback and no diagnostic branch is created or consulted per pass, and nothing here emits SQL.
/// The only state is the current statement and one boolean, so the cost of a round is the passes
/// themselves.
let private applyPassesOnce (passes: Pass list) (input: SqlStatement) : struct(SqlStatement * bool) =
    let mutable current = input
    let mutable anyChanged = false
    for pass in passes do
        let struct(output, changed) = pass.Transform current
        if changed then anyChanged <- true
        current <- output
    struct(current, anyChanged)

/// Optimize a statement: one unconditional round, then accepted rounds to a fixed point.
///
/// The first round is applied without the acceptance test, which is what the pipeline has always
/// done — the seed round was produced by a plain pipeline run and adopted as the starting point.
/// Every later round is accepted only when some pass changed something, the metric is strictly
/// lower, and the result verifies; the first round that fails any of those stops the loop, and the
/// iteration ceiling is unchanged.
let optimize (passes: Pass list) (input: SqlStatement) : SqlStatement =
    let struct(seeded, _) = applyPassesOnce passes input
    let mutable current = seeded
    let mutable continueRounds = true
    let mutable iterationCount = 0
    let maxIterations = 10

    while continueRounds && iterationCount < maxIterations do
        iterationCount <- iterationCount + 1
        let currentMetric = statementMetric current
        let struct(candidate, anyChanged) = applyPassesOnce passes current

        if anyChanged then
            let candidateMetric = statementMetric candidate
            if metricIsLower candidateMetric currentMetric && verifyStatement candidate then
                current <- candidate
            else
                continueRounds <- false
        else
            continueRounds <- false

    current

/// Run a single pass, producing an audit row and the output statement. Diagnostics only.
let runPass (pass: Pass) (input: SqlStatement) : PassAuditRow * SqlStatement =
    let struct(output, changed) = pass.Transform input
    ({ PassName = pass.Name; Changed = changed }, output)

/// Run an ordered list of passes once, recording an audit row per pass. Diagnostics only.
///
/// Rows are collected into a growable buffer and converted once, so collection is linear rather
/// than the quadratic list append this replaced. This entry is never reached from a product
/// translation.
let runPipeline (passes: Pass list) (input: SqlStatement) : PipelineResult =
    let rows = ResizeArray<PassAuditRow>(List.length passes)
    let mutable current = input
    for pass in passes do
        let struct(output, changed) = pass.Transform current
        rows.Add { PassName = pass.Name; Changed = changed }
        current <- output
    { Output = current; AuditTrail = List.ofSeq rows }
