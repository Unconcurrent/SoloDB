module internal SoloDatabase.CompositeGroupByCanonicalization

open SqlDu.Engine.C1.Spec
open SoloDatabase.QueryableGroupByAliases

// ══════════════════════════════════════════════════════════════
// CompositeGroupByCanonicalization — R-108.
//
// Decomposes compound-key GROUP BY over a JSON object constructor into
// canonical scalar multi-column GROUP BY so SQLite can match composite
// expression indexes by textual expression match.
//
// Two input shapes for the compound key are supported (both produced by
// the current writer pipeline, depending on the LINQ form):
//   (1) JsonObjectExpr(properties)                 — anonymous `new { A, B }` path
//   (2) FunctionCall("json_object"|"jsonb_object") — flush/aggregate path
//
// Pipeline placement (Queryable.HelperBase.fs):
//   ConstantFold -> FlattenPass -> PushdownPass -> ProjectionPass
//   -> CompositeGroupByCanonicalization   <-- HERE
//   -> IndexPlanShaping -> JsonbRewritePolicy
//
// Post-order traversal: at each SelectCore, children (Source DerivedTable,
// Joins DerivedTables, CTEs) are rewritten BEFORE the parent core's own
// rewrite runs. Outer rewrites therefore see already-rewritten inner
// children (anvil-rev constraint, 2026-04-22_23-13).
//
// Null-exclusion filter (Captain 23:04): AND-of-IsNotNull over all slot
// columns is AND-combined into the outer WHERE in lockstep with the
// GroupBy rewrite.
//
// Function-name match convention (two distinct rules by origin):
//   - Endogenous constructor names WE emit (json_object, jsonb_object):
//     exact-match against the shared constants
//     QueryableGroupByAliases.jsonObjectFn and .jsonbObjectFn. The
//     writer-side FunctionCall construction sites use the same constants,
//     so case drift between writer and optimizer is impossible by
//     construction.
//   - Exogenous SQLite function names in user expressions
//     (the non-deterministic list and the datetime/time/date/julianday/
//     unixepoch with 'now' sentinel): lowercase-normalized matching.
//     User-origin names have no cross-file enforcement seam; defensive
//     normalization is the correct posture.
// ══════════════════════════════════════════════════════════════

/// Closed list of SQLite scalar functions considered non-deterministic for P6.
let private nonDeterministicFunctionNames : Set<string> =
    Set.ofList [
        "random"; "randomblob"
        "changes"; "last_insert_rowid"; "total_changes"
        "current_time"; "current_date"; "current_timestamp"
    ]

/// datetime('now', ...), time('now', ...), date('now', ...), julianday('now', ...),
/// unixepoch('now', ...) are non-deterministic when invoked with the 'now' sentinel.
let private isNonDeterministicTimeCall (fname: string) (args: SqlExpr list) : bool =
    let lname = fname.ToLowerInvariant()
    (lname = "datetime" || lname = "time" || lname = "date"
     || lname = "julianday" || lname = "unixepoch")
    && args |> List.exists (fun e ->
        match e with
        | Literal(SqlLiteral.String s) when s.ToLowerInvariant() = "now" -> true
        | _ -> false)

/// Row-scope qualifier set for a single TableSource.
/// - BaseTable emits the unquoted name, the quoted name, and the optional alias.
/// - DerivedTable emits its alias (always present by DU contract).
/// - FromJsonEach emits its optional alias.
let private sourceQualifiers (src: TableSource) : Set<string> =
    match src with
    | BaseTable(name, aliasOpt) ->
        let quoted = "\"" + name + "\""
        match aliasOpt with
        | Some a -> Set.ofList [ name; quoted; a ]
        | None -> Set.ofList [ name; quoted ]
    | DerivedTable(_, alias) -> Set.singleton alias
    | FromJsonEach(_, aliasOpt) ->
        match aliasOpt with
        | Some a -> Set.singleton a
        | None -> Set.empty

/// All valid qualifier strings for columns that live in the local scope of
/// a SelectCore (Source + Joins). Excludes CTEs: they are addressable by name
/// at emit time and, if cross-referenced as a DerivedTable-equivalent, show up
/// via the Source/Joins path; pure-CTE-scope references without a local
/// alias binding are out of scope for v1 and would be rejected here.
let private coreQualifiers (core: SelectCore) : Set<string> =
    let fromSource =
        match core.Source with
        | Some s -> sourceQualifiers s
        | None -> Set.empty
    core.Joins
    |> List.fold (fun acc j ->
        let src =
            match j with
            | CrossJoin s -> s
            | ConditionedJoin(_, s, _) -> s
        Set.union acc (sourceQualifiers src)) fromSource

/// Row-scope context threaded through the P6 purity walk. `Scope` is the set
/// of valid qualifier strings for local Columns. `AllowUnqualified` is true
/// only when the core has exactly one visible qualifier source (Source
/// present and Joins empty) — anvil-rev tightening 23:35, accepted by Linus
/// and Data. In multi-source cores, unqualified columns are schema-ambiguous
/// from the pass's perspective and must be refused.
type private RowScope = {
    Scope: Set<string>
    AllowUnqualified: bool
}

let private rowScopeFor (core: SelectCore) : RowScope =
    { Scope = coreQualifiers core
      AllowUnqualified = core.Source.IsSome && core.Joins.IsEmpty }

/// P6 purity walk: component expression must be deterministic, aggregate-free,
/// window-free, subquery-free, parameter-free, and built from row-visible scalars
/// only. Additionally enforces row-scope safety (anvil-rev/Linus B2 ruling 23:35):
/// every `Column(Some q, _)` must have `q` in the local scope set;
/// `Column(None, _)` is accepted only when the core has exactly one visible
/// qualifier source (single-source canonical shape).
let rec private isPureScalarComponent (scope: RowScope) (expr: SqlExpr) : bool =
    match expr with
    | AggregateCall _ -> false
    | WindowCall _ -> false
    | Exists _ -> false
    | ScalarSubquery _ -> false
    | InSubquery _ -> false
    | Parameter _ -> false
    | Column(Some q, _) -> scope.Scope.Contains q
    | Column(None, _) -> scope.AllowUnqualified
    | Literal _ -> true
    | JsonExtractExpr(Some q, _, _) -> scope.Scope.Contains q
    | JsonExtractExpr(None, _, _) -> scope.AllowUnqualified
    | JsonRootExtract(Some q, _) -> scope.Scope.Contains q
    | JsonRootExtract(None, _) -> scope.AllowUnqualified
    | FunctionCall(name, args) ->
        let lname = name.ToLowerInvariant()
        if nonDeterministicFunctionNames.Contains lname then false
        elif isNonDeterministicTimeCall name args then false
        else args |> List.forall (isPureScalarComponent scope)
    | Unary(_, inner) -> isPureScalarComponent scope inner
    | Cast(inner, _) -> isPureScalarComponent scope inner
    | Binary(l, _, r) -> isPureScalarComponent scope l && isPureScalarComponent scope r
    | Between(v, lo, hi) ->
        isPureScalarComponent scope v
        && isPureScalarComponent scope lo
        && isPureScalarComponent scope hi
    | InList(v, head, tail) ->
        isPureScalarComponent scope v
        && isPureScalarComponent scope head
        && tail |> List.forall (isPureScalarComponent scope)
    | Coalesce(head, tail) ->
        isPureScalarComponent scope head
        && tail |> List.forall (isPureScalarComponent scope)
    | JsonArrayExpr elems ->
        elems |> List.forall (isPureScalarComponent scope)
    | JsonObjectExpr props ->
        props |> List.forall (fun (_, v) -> isPureScalarComponent scope v)
    | JsonSetExpr(target, assigns) ->
        isPureScalarComponent scope target
        && assigns |> List.forall (fun (_, v) -> isPureScalarComponent scope v)
    | CaseExpr((cond0, res0), rest, elseOpt) ->
        isPureScalarComponent scope cond0
        && isPureScalarComponent scope res0
        && rest |> List.forall (fun (c, r) ->
            isPureScalarComponent scope c && isPureScalarComponent scope r)
        && (match elseOpt with
            | Some e -> isPureScalarComponent scope e
            | None -> true)
    | UpdateFragment(path, value) ->
        isPureScalarComponent scope path
        && isPureScalarComponent scope value

/// Decompose a `FunctionCall("json_object"|"jsonb_object", args)` argument list
/// into (keys, values). P4: even arity >= 4; every even-indexed position is a
/// String literal.
let private tryDecomposeFunctionArgs
    (args: SqlExpr list) : (string list * SqlExpr list) option =
    if args.Length < 4 || args.Length % 2 <> 0 then None
    else
        let rec loop xs accK accV =
            match xs with
            | [] -> Some (List.rev accK, List.rev accV)
            | Literal(SqlLiteral.String k) :: v :: rest ->
                loop rest (k :: accK) (v :: accV)
            | _ -> None
        loop args [] []

/// Decompose a `JsonObjectExpr` property list into (keys, values).
/// P4 scaled to the structured node: at least 2 properties (N >= 2 components).
let private tryDecomposeJsonObjectProperties
    (properties: (string * SqlExpr) list) : (string list * SqlExpr list) option =
    if properties.Length < 2 then None
    else
        let keys = properties |> List.map fst
        let values = properties |> List.map snd
        Some (keys, values)

/// Recognize either compound-key shape and decompose into (keys, values).
let private tryDecomposeJsonObjectLike
    (expr: SqlExpr) : (string list * SqlExpr list) option =
    match expr with
    | JsonObjectExpr props -> tryDecomposeJsonObjectProperties props
    | FunctionCall(name, args) when name = jsonObjectFn || name = jsonbObjectFn ->
        tryDecomposeFunctionArgs args
    | _ -> None

let private slotAlias (i: int) : string = syntheticGroupKeySlotAlias i

let private andCombineOptional
    (existing: SqlExpr option) (filter: SqlExpr) : SqlExpr option =
    match existing with
    | None -> Some filter
    | Some e -> Some (Binary(e, BinaryOperator.And, filter))

/// Build AND-of-IsNotNull conjunction over the supplied component expressions.
/// Precondition: inputs.Length >= 2 (enforced by P4).
let private conjoinIsNotNull (inputs: SqlExpr list) : SqlExpr =
    let notNulls = inputs |> List.map (fun c -> Unary(UnaryOperator.IsNotNull, c))
    notNulls |> List.reduce (fun a b -> Binary(a, BinaryOperator.And, b))

/// Run the shared safety checks (P5 + P6) over a decomposed (keys, values) pair
/// scoped to the given row-scope context.
let private passesSafetyChecks
    (scope: RowScope) (keys: string list) (values: SqlExpr list) : bool =
    // P5: unique key literals
    let distinctKeys = keys |> List.distinct
    if distinctKeys.Length <> keys.Length then false
    // P6: every value is a pure scalar component in local scope
    elif not (values |> List.forall (isPureScalarComponent scope)) then false
    else true

/// P1a rewrite: outer core with GroupBy = [Column(Some "o", "__solodb_group_key")]
/// and source = DerivedTable carrying a matching synthetic-key projection.
/// Scope for P6 is `coreQualifiers innerCore` (the inner's local row scope —
/// value expressions live in the inner's projection domain).
let private tryRewriteP1a (outer: SelectCore) : SelectCore option =
    match outer.GroupBy with
    | [ Column(Some "o", g) ] when g = syntheticGroupKeyAlias ->
        match outer.Source with
        | Some (DerivedTable(innerSel, "o")) ->
            match innerSel.Body with
            | SingleSelect innerCore ->
                let projections = ProjectionSetOps.toList innerCore.Projections
                // P3: exactly one projection aliased "__solodb_group_key"
                let synthMatches =
                    projections
                    |> List.filter (fun p -> p.Alias = Some syntheticGroupKeyAlias)
                match synthMatches with
                | [ synthProj ] ->
                    match tryDecomposeJsonObjectLike synthProj.Expr with
                    | Some (keys, values) ->
                        let innerScope = rowScopeFor innerCore
                        if not (passesSafetyChecks innerScope keys values) then None
                        else
                            let n = values.Length
                            let slotNames = [ for i in 0 .. n - 1 -> slotAlias i ]
                            // E34: refuse if any slot alias already exists on inner
                            let existingAliases =
                                projections
                                |> List.choose (fun p -> p.Alias)
                                |> Set.ofList
                            if slotNames |> List.exists existingAliases.Contains then None
                            else
                                // Rewrite step 2: append slot projections; preserve all
                                // existing projections byte-for-byte (anvil-rev 23:13).
                                let slotProjections =
                                    List.zip slotNames values
                                    |> List.map (fun (a, e) -> { Alias = Some a; Expr = e })
                                let mergedProjections = projections @ slotProjections
                                let newInnerCore =
                                    { innerCore with
                                        Projections = ProjectionSetOps.ofList mergedProjections }
                                let newInnerSel =
                                    { innerSel with Body = SingleSelect newInnerCore }
                                // Rewrite step 4: outer GroupBy = N slot column refs.
                                let slotColumns =
                                    slotNames |> List.map (fun sa -> Column(Some "o", sa))
                                // Rewrite step 5: null-exclusion filter, AND-combined
                                // with any existing outer.Where (Captain 23:04).
                                let filter = conjoinIsNotNull slotColumns
                                let newWhere = andCombineOptional outer.Where filter
                                Some
                                    { outer with
                                        Source = Some (DerivedTable(newInnerSel, "o"))
                                        GroupBy = slotColumns
                                        Where = newWhere }
                    | None -> None
                | _ -> None
            | UnionAllSelect _ -> None
        | _ -> None
    | _ -> None

/// P1b rewrite: outer core with a single inlined JSON-object GROUP BY term
/// (defensive post-flatten shape; currently unreachable due to FlattenSafety F8
/// at `SqlDu.FlattenSafety.fs:64-65`, but locked as dormant scaffolding against
/// any future weakening — plan E41, L6).
///
/// Lockstep contract with synthetic-key projection (Linus 23:43 / Data 23:43):
///   - exactly one `__solodb_group_key` projection in outer → leave its Expr
///     untouched (it IS already the correct synthetic key by construction;
///     substituting or rebuilding adds risk for zero gain);
///   - zero matches → proceed (no reader to preserve);
///   - multiple matches → refuse the rewrite (ambiguous synthetic-key projection).
let private tryRewriteP1b (outer: SelectCore) : SelectCore option =
    match outer.GroupBy with
    | [ single ] ->
        match tryDecomposeJsonObjectLike single with
        | Some (keys, values) ->
            let outerScope = rowScopeFor outer
            if not (passesSafetyChecks outerScope keys values) then None
            else
                let outerProjections = ProjectionSetOps.toList outer.Projections
                let synthProjCount =
                    outerProjections
                    |> List.filter (fun p -> p.Alias = Some syntheticGroupKeyAlias)
                    |> List.length
                if synthProjCount > 1 then None
                else
                    let filter = conjoinIsNotNull values
                    let newWhere = andCombineOptional outer.Where filter
                    Some
                        { outer with
                            GroupBy = values
                            Where = newWhere }
        | None -> None
    | _ -> None

let private rewriteCore (changed: bool ref) (core: SelectCore) : SelectCore =
    match tryRewriteP1a core with
    | Some rewritten ->
        changed.Value <- true
        rewritten
    | None ->
        match tryRewriteP1b core with
        | Some rewritten ->
            changed.Value <- true
            rewritten
        | None -> core

/// Post-order rewrite: recurse into children (Source DerivedTable, Joins
/// DerivedTables, CTEs) first, then apply rewrite at the current core.
let rec private rewriteSelect (changed: bool ref) (sel: SqlSelect) : SqlSelect =
    let newBody =
        match sel.Body with
        | SingleSelect core ->
            let coreAfterSource =
                match core.Source with
                | Some (DerivedTable(innerSel, alias)) ->
                    { core with
                        Source = Some (DerivedTable(rewriteSelect changed innerSel, alias)) }
                | _ -> core
            let coreAfterJoins =
                { coreAfterSource with
                    Joins =
                        coreAfterSource.Joins
                        |> List.map (fun j ->
                            match j with
                            | CrossJoin(DerivedTable(jSel, jAlias)) ->
                                CrossJoin(DerivedTable(rewriteSelect changed jSel, jAlias))
                            | ConditionedJoin(kind, DerivedTable(jSel, jAlias), onExpr) ->
                                ConditionedJoin(
                                    kind,
                                    DerivedTable(rewriteSelect changed jSel, jAlias),
                                    onExpr)
                            | CrossJoin _ -> j
                            | ConditionedJoin _ -> j) }
            SingleSelect (rewriteCore changed coreAfterJoins)
        | UnionAllSelect _ as body ->
            // UnionAll cores are out of scope for v1 (plan E13). Falls through.
            body
    let newCtes =
        sel.Ctes
        |> List.map (fun cte ->
            { cte with Query = rewriteSelect changed cte.Query })
    { Ctes = newCtes; Body = newBody }

/// Transform entry point. Identity on non-SELECT statements (plan E35).
let transform (stmt: SqlStatement) : struct(SqlStatement * bool) =
    match stmt with
    | SelectStmt sel ->
        let changed = ref false
        let rewritten = rewriteSelect changed sel
        struct(SelectStmt rewritten, changed.Value)
    | InsertStmt _
    | UpdateStmt _
    | DeleteStmt _
    | DdlStmt _ ->
        struct(stmt, false)
