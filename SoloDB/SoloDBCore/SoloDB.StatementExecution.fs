namespace SoloDatabase

open System.Collections.Generic
open Microsoft.Data.Sqlite
open SQLiteTools
open SqlDu.Engine.C1.Spec

/// Canonical execution of a typed SQL statement.
///
/// Statement building and statement execution used to be separated only inside one mutation
/// branch, where the executor was a local closure over the connections it happened to have. That
/// left every other corridor assembling SQL as text. This module is the single owner: it takes
/// the connections explicitly, discovers the tables a statement touches, loads the index model,
/// applies the chosen pipeline, emits canonically, reports the SQL, and executes.
module internal StatementExecution =

    /// How much of the optimizer a statement should receive.
    ///
    /// Not every statement wants the index-shaped pipeline. A statement whose assignments are
    /// scalar and whose predicate is already typed gains nothing from shape-changing passes and
    /// must keep its emitted form; a statement carrying subqueries needs them. The choice is made
    /// from the statement itself, never from which caller produced it.
    type OptimizationPolicy =
        /// Emit the statement as built. No shape-changing passes.
        | CanonicalEmissionOnly
        /// Load the index model for the discovered tables and run the standard pipeline.
        | IndexShapedPipeline

    /// Every table reference the statement reaches, in encounter order and with repeats kept.
    ///
    /// The set of tables is what the index model needs, but a set hides how it was produced: a
    /// traversal that runs twice yields an identical set, so set equality cannot detect it. The
    /// ordered references are therefore the primary result and the set is derived from them,
    /// which makes the traversal's shape and multiplicity observable to a test.
    ///
    /// A table is reachable only through a subquery nested in a predicate or in an assignment
    /// value in real statements, so an omission here loads the wrong index model and leaves no
    /// trace in the emitted SQL of simple statements. The recursion into expressions is delegated
    /// to SqlExpr.fold, which descends into json-set assignment values; any expression case added
    /// to the tree must be reflected there.
    let collectTableReferences (stmt: SqlStatement) : ResizeArray<string> =
        let tables = ResizeArray<string>()
        let rec collectStmt (s: SqlStatement) =
            match s with
            | SelectStmt sel -> collectSelect sel
            | InsertStmt ins ->
                tables.Add(ins.TableName)
                match ins.Source with
                | InsertSelect sel -> collectSelect sel
                | InsertValues _ -> ()
            | UpdateStmt upd ->
                tables.Add(upd.TableName)
                upd.Where |> Option.iter collectExpr
                for (_, e) in upd.SetClauses do collectExpr e
            | DeleteStmt del ->
                tables.Add(del.TableName)
                del.Where |> Option.iter collectExpr
        and collectSelect (sel: SqlSelect) =
            for cte in sel.Ctes do collectSelect cte.Query
            match sel.Body with
            | SingleSelect core -> collectCore core
            | UnionAllSelect(h, t) ->
                collectCore h
                for c in t do collectCore c
        and collectCore (core: SelectCore) =
            match core.Source with
            | Some (BaseTable(t, _)) -> tables.Add(t)
            | Some (DerivedTable(inner, _)) -> collectSelect inner
            | Some (FromJsonEach(e, _)) -> collectExpr e
            | None -> ()
            for j in core.Joins do
                match j with
                | CrossJoin (BaseTable(t, _)) -> tables.Add(t)
                | CrossJoin (DerivedTable(inner, _)) -> collectSelect inner
                | CrossJoin (FromJsonEach(e, _)) -> collectExpr e
                | ConditionedJoin(_, BaseTable(t, _), onExpr) ->
                    tables.Add(t)
                    collectExpr onExpr
                | ConditionedJoin(_, DerivedTable(inner, _), onExpr) ->
                    collectSelect inner
                    collectExpr onExpr
                | ConditionedJoin(_, FromJsonEach(e, _), onExpr) ->
                    collectExpr e
                    collectExpr onExpr
            core.Where |> Option.iter collectExpr
            core.Having |> Option.iter collectExpr
            for p in (ProjectionSetOps.toList core.Projections) do collectExpr p.Expr
            for ob in core.OrderBy do collectExpr ob.Expr
        and collectExpr (expr: SqlExpr) =
            SqlExpr.fold (fun () node ->
                match node with
                | InSubquery(_, sel) -> collectSelect sel
                | ScalarSubquery sel -> collectSelect sel
                | Exists sel -> collectSelect sel
                | _ -> ()) () expr
        collectStmt stmt
        tables

    /// The distinct tables the statement reaches. Derived from the ordered references so the two
    /// cannot disagree.
    let discoverTables (stmt: SqlStatement) : HashSet<string> =
        HashSet<string>(collectTableReferences stmt)

    /// True when the statement carries a subquery anywhere: in a source, a join, a predicate, a
    /// projection, or nested inside an assignment value. A statement that does is shaped by the
    /// index model; one that does not is emitted as built.
    ///
    /// This performs its own traversal and does not consult table discovery; discovery exists to
    /// tell the index model which tables to load, nothing more.
    let requiresIndexShaping (stmt: SqlStatement) : bool =
        let mutable found = false
        let inspect (expr: SqlExpr) =
            SqlExpr.fold (fun () node ->
                match node with
                | InSubquery _ | ScalarSubquery _ | Exists _ -> found <- true
                | _ -> ()) () expr
        match stmt with
        | SelectStmt _ -> true
        | InsertStmt ins ->
            match ins.Source with
            | InsertSelect _ -> true
            | InsertValues rows ->
                for row in rows do for e in row do inspect e
                found
        | UpdateStmt upd ->
            upd.Where |> Option.iter inspect
            for (_, e) in upd.SetClauses do inspect e
            found
        | DeleteStmt del ->
            del.Where |> Option.iter inspect
            found

    /// Choose the policy from the statement's own shape.
    let policyFor (stmt: SqlStatement) : OptimizationPolicy =
        if requiresIndexShaping stmt then IndexShapedPipeline else CanonicalEmissionOnly

    /// Emit a statement to SQL under the given policy, reporting it to the capture boundary.
    ///
    /// <param name="indexModelConnection">Connection the index model is read from. It is a separate
    /// parameter because the chain-executor corridor reads the model on a connection other than the
    /// one it executes on. Callers for which the two are the same — relation mutations pass the
    /// transaction's connection for both — simply supply it twice rather than inheriting an
    /// assumption about which connection is which.</param>
    let emit (indexModelConnection: SqliteConnection) (policy: OptimizationPolicy) (stmt: SqlStatement) : string =
        let output =
            match policy with
            | CanonicalEmissionOnly -> stmt
            | IndexShapedPipeline ->
                let tables = discoverTables stmt
                let indexModel = SoloDatabase.IndexModel.loadModelForTables indexModelConnection (tables :> seq<string>)
                let passes = PassPipeline.standardWithIndexModel indexModel
                let firstRound = PassRunner.runPipeline passes stmt
                (PassRunner.runPipelineToFixedPoint passes firstRound).Output
        let emitted = EmitStatement.emitStatement (EmitContext(InlineLiterals = true)) output
        SqlCapture.OnSqlEmitted |> Option.iter (fun cb -> cb emitted.Sql)
        emitted.Sql

    /// Emit and execute, returning the affected row count.
    let execute
        (executionConnection: SqliteConnection)
        (indexModelConnection: SqliteConnection)
        (policy: OptimizationPolicy)
        (stmt: SqlStatement)
        (variables: Dictionary<string, obj>) : int =
        let sql = emit indexModelConnection policy stmt
        executionConnection.Execute(sql, variables)
