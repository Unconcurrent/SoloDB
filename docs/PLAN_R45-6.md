# PLAN R45-6: DBRefMany Collection Projection (CP-01 Only, SQL-Only)

## PROBLEM STATEMENT

DBRefMany collection projections like `.Select(o => o.Tags.Select(t => t.Label))` are currently rejected with NotSupportedException. R45-6 admits CP-01 (pure Select) only, implemented as a SQL correlated subquery using `jsonb_group_array`. CP-02 (Where+Select) and CP-03 (OrderBy+Select) are deterministic rejects in this slice.

Captain mechanism lock: SQL-ONLY. No client-side post-load.

## RESEARCH FINDINGS

1. **Existing infrastructure** — `jsonb_group_array` already used in GroupBy path (`Queryable.BuildQuery.PartA.fs:242`). `ScalarSubquery` DU case exists. Emitter handles both. Materialization policy marks `json_group_array` patterns as PRESERVE_REQUIRED.

2. **Existing DBRefMany correlated subquery pattern** — `buildQuantifierWithPredicate` in `QueryTranslator.VisitDbRef.fs:256` builds correlated EXISTS subqueries over link+target tables with unique aliases via `Interlocked.Increment`. Reusable for collection projection.

3. **Owner resolution** — `tryGetDBRefManyOwnerRef` resolves owner collection, alias, and property for both root and nested (through DBRef.Value) paths.

4. **Empty-set behavior** — `json_group_array` returns `'[]'` on empty rowset (verified in SQLite shell). No COALESCE needed.

5. **Current rejection path** — `Enumerable.Select(o.Tags, t => t.Label)` is a MethodCallExpression. None of the `preExpressionHandler` entries handle it. Falls to general `visitMethodCallDu` which fails on the unrecognized LINQ method over non-SQL source.

## RESEARCH QUOTATIONS

From AGORA `15-43-20` (Bellard):
> "CP-01 is the only admitted row in this slice. CP-02 and CP-03 are deterministic rejects unless fully proven in-slice."

From Captain (radu):
> "do everything inside the SQL"

## CHOSEN APPROACH

Add a new case in `handleDBRefManyExpression` for `Select` method calls on DBRefMany sources. Generate a `ScalarSubquery` containing `jsonb_group_array` over a correlated link+target subquery. The projection lambda body is translated via `visitDu` in a subquery context with the target alias.

CP-02/CP-03: **explicit dedicated reject guard** in the Select handler. When the source of `Select` is a `MethodCallExpression` (e.g. `Where(o.Tags, pred)` or `OrderBy(o.Tags, key)`) whose own source is `DBRefMany`, the handler raises `NotSupportedException` with a named message identifying the chained-operation boundary. This is a dedicated guard, not incidental type fallthrough.

Complexity: O(1) — single new match arm in existing handler + tests.

## EDGE CASES (ALL ENUMERATED)

| # | Case | Expected outcome |
|---|------|------------------|
| E-01 | Empty DBRefMany | `json_group_array` returns `'[]'` → empty list |
| E-02 | Single-item DBRefMany | `'["value"]'` → single-element list |
| E-03 | Multiple items | `'["a","b","c"]'` → list preserving aggregate order |
| E-04 | Projection to scalar (string) | `jsonb_extract(t.Value, '$.Name')` inside `jsonb_group_array` |
| E-05 | Projection to integer | Same pattern, integer extraction |
| E-06 | One-generic `DBRefMany<T>` | Positive test |
| E-07 | Two-generic `DBRefMany<T, TId>` | Positive test (same link table structure) |
| E-08 | CP-02 `.Where().Select()` on DBRefMany | Deterministic reject — explicit guard in Select handler checks for chained method source |
| E-09 | CP-03 `.OrderBy().Select()` on DBRefMany | Deterministic reject — same explicit guard |
| E-10 | Nested DBRefMany in projection lambda | Depth check rejects (`countDbRefManyDepth`) |
| E-11 | Non-translatable selector body | `visitDu` rejects |
| E-12 | Mixed scalar + collection projection `.Select(o => new { o.Name, Items = o.Tags.Select(t => t.Label) })` | NATURALLY ADMITTED — no additional code. `visitDu` for `NewExpression` visits each member independently; scalar fields use existing paths, collection fields use new CP-01 handler. No separate mixed-projection code path exists or is needed. E13/E15 prove this with parity. |

## ERROR CONDITIONS

| # | Condition | Detection | Handling |
|---|-----------|-----------|----------|
| 1 | Missing relation metadata | `dbRefManyLinkTable` raises `InvalidOperationException` | Existing path |
| 2 | Non-extractable lambda | `tryExtractLambdaExpression` returns `ValueNone` | NotSupportedException |
| 3 | Nested DBRefMany depth > 1 | `countDbRefManyDepth` | NotSupportedException (existing) |

## WHAT LINUS WILL CHECK

1. **Correlated subquery alias uniqueness** — uses `Interlocked.Increment(&subqueryAliasCounter)` pattern from existing `buildQuantifierWithPredicate`. Addressed: same mechanism.
2. **Flatten safety** — `ScalarSubquery` nodes are not recursed into by flatten passes. Addressed: existing invariant.
3. **json_group_array empty-set** — returns `'[]'` not NULL. Addressed: verified in SQLite shell.
4. **CP-02/CP-03 explicitly rejected** — dedicated guard in Select handler detects chained MethodCallExpression on DBRefMany source and raises NotSupportedException with named message. Addressed: explicit dedicated guard, not incidental fallthrough.
5. **No optimizer pass disabled** — no provisional disablements. Addressed: extending existing correlated subquery pattern within existing safety boundaries.

## DIVISION OF LABOR

Batch 1 (Sequential — single developer):
- **Hipp** (translator extension + tests)
  - File: `SoloDB/QueryTranslator/QueryTranslator.VisitDbRef.fs` — add Select handler in `handleDBRefManyExpression`
  - File: `SoloDBTests/RelationalApi.IQueryableParity.Tests/FamilyE_DBRefManyTests.cs` — new test rows:

### Concrete Required Test Rows

| Row | Name | Type | Shape |
|-----|------|------|-------|
| E13 | `E13_DBRefMany_SelectProjection_Parity` | CP-01 positive (1G) | `.Select(o => o.Tags.Select(t => t.Label))` — parity with LINQ-to-Objects |
| E14 | `E14_DBRefMany_SelectProjection_EmptyCollection_Parity` | CP-01 positive (1G) | Empty DBRefMany → empty list parity |
| E15 | `E15_DBRefMany_SelectProjection_TwoGeneric_Parity` | CP-01 positive (2G) | `DBRefMany<T, TId>` variant with `jsonb_group_array` |
| E16 | `E16_DBRefMany_WhereSelect_FailClosed` | CP-02 reject | `.Tags.Where(t => ...).Select(t => t.Label)` → NotSupportedException |
| E17 | `E17_DBRefMany_OrderBySelect_FailClosed` | CP-03 reject | `.Tags.OrderBy(t => ...).Select(t => t.Label)` → NotSupportedException |
| E18 | `E18_DBRefMany_NestedSelectProjection_FailClosed` | CX-02 reject | `.Tags.Select(t => t.SubItems.Select(...))` → NotSupportedException |
| E19 | `E19_DBRefMany_MixedScalarCollectionProjection_FailClosed` | E-12 reject | `.Select(o => new { o.Name, Items = o.Tags.Select(...) })` → NotSupportedException |

### Implementation Detail

New case in `handleDBRefManyExpression` (after the `All` case):

```fsharp
// Case 5: Select(proj) over DBRefMany — correlated subquery with jsonb_group_array
// EXPLICIT GUARD: only direct DBRefMany member access is admitted (CP-01).
// Chained methods (Where/OrderBy/etc) on DBRefMany produce IEnumerable<T> source,
// which is explicitly rejected here — not left to incidental type fallthrough.
| :? MethodCallExpression as mce when mce.Method.Name = "Select" ->
    let sourceArg, projArg = extractSourceAndProjection mce
    match sourceArg with
    | ValueSome sourceExpr ->
        let unwrapped = unwrapConvert sourceExpr
        if isDBRefManyType unwrapped.Type then
            // Direct DBRefMany source — CP-01 admitted
            match tryGetDBRefManyOwnerRef qb sourceExpr with
            | ValueSome ownerRef ->
                match projArg with
                | ValueSome projectionExpr ->
                    buildCollectionProjection qb ownerRef projectionExpr
                    true
                | ValueNone -> false
            | ValueNone -> false
        elif unwrapped :? MethodCallExpression then
            let innerMce = unwrapped :?> MethodCallExpression
            // Explicit CP-02/CP-03 reject: chained Where/OrderBy/etc on DBRefMany
            let innerSource =
                if not (isNull innerMce.Object) then innerMce.Object
                elif innerMce.Arguments.Count > 0 then innerMce.Arguments.[0]
                else null
            if not (isNull innerSource) && isDBRefManyType (unwrapConvert innerSource).Type then
                raise (NotSupportedException(
                    "Error: Chained collection operations on DBRefMany are not supported in this cycle.\n" +
                    "Reason: Only direct .Select(projection) over DBRefMany is admitted; " +
                    ".Where().Select(), .OrderBy().Select(), and other chained forms are deferred.\n" +
                    "Fix: Use .Select() directly on the DBRefMany property, or move the query after AsEnumerable()."))
            else false
        else false
    | _ -> false
```

New function `buildCollectionProjection`:
- Extract projection lambda
- Check depth (`countDbRefManyDepth`)
- Resolve link table, owner column, target column, target table (reuse existing helpers)
- Allocate unique aliases via `Interlocked.Increment`
- Translate projection body via `visitDu` in subquery context
- Build `ScalarSubquery` with `jsonb_group_array(translatedProjection)` over link+target join correlated to owner

## PROVISIONAL OPTIMIZER DISABLEMENTS

**NONE.** No optimizer passes are disabled. The implementation extends existing correlated subquery patterns within existing flatten-safety boundaries. `ScalarSubquery` is already preserved by all optimizer passes.

## CAPTAIN STANDING ORDER 1: COLLECTION METHOD DISPOSITION MAP

Every IEnumerable/IQueryable method on DBRefMany is either SUPPORTED, REJECTED in this slice, or EXISTING:

| Method | Status | Notes |
|--------|--------|-------|
| Select | **ADMITTED (CP-01)** | This slice — `jsonb_group_array` correlated subquery |
| Where | REJECTED (CP-02 deferred) | `isDBRefManyType` fails on chained result |
| OrderBy / OrderByDescending | REJECTED (CP-03 deferred) | Same mechanism |
| Count | EXISTING SUPPORTED | `handleDBRefManyExpression` Case 1 — scalar subquery |
| Any() | EXISTING SUPPORTED | Case 2 — EXISTS subquery |
| Any(pred) | EXISTING SUPPORTED | Case 2 — EXISTS with predicate |
| All(pred) | EXISTING SUPPORTED | Case 4 — NOT EXISTS with negated predicate |
| Take / Skip | REJECTED | NotSupportedException — not admitted, future cycle |
| First / FirstOrDefault | REJECTED | NotSupportedException — not admitted, future cycle |
| Last / LastOrDefault | REJECTED | NotSupportedException — not admitted, future cycle |
| Sum / Min / Max / Average | REJECTED | NotSupportedException — aggregate inside collection projection, future cycle |
| GroupBy | REJECTED | NotSupportedException — not admitted, future cycle |
| Distinct | REJECTED | NotSupportedException — not admitted, future cycle |
| Contains | REJECTED | Falls to general handler — not admitted for DBRefMany projection context |
| Aggregate | REJECTED | NotSupportedException — not admitted, future cycle |
| Zip / Concat / Union / Intersect / Except | REJECTED | NotSupportedException — not admitted, future cycle |

No silent gaps. Every method either works or deterministically rejects.

## CAPTAIN STANDING ORDER 2: INDEX COVERAGE VERIFICATION

Generated SQL for CP-01:
```sql
SELECT jsonb_group_array(jsonb_extract(_tgt.Value, '$.Name'))
FROM "SoloDBRelLink_Owner_Items" _lnk
INNER JOIN "Target" _tgt ON _tgt.Id = _lnk.TargetId
WHERE _lnk.SourceId = owner.Id
```

Index coverage:
- `_lnk.SourceId` — **INDEXED** (`Relations.Schema.LinkTableDDL.fs:128,264`)
- `_lnk.TargetId` — **INDEXED** (`Relations.Schema.LinkTableDDL.fs:129,265`)
- `_tgt.Id` — **PRIMARY KEY** (SQLite rowid)
- `owner.Id` — **PRIMARY KEY** (SQLite rowid)

All WHERE and JOIN conditions use indexed columns. No full table scans in correlated subquery.

## ACCEPTANCE CRITERIA

1. CP-01 positive tests green with LINQ-to-Objects parity (1G and 2G typed-id)
2. CP-02/CP-03 negative tests green with NotSupportedException
3. CX-01..CX-07 reject tests green
4. Full `RelationalApi.IQueryableParity.Tests` green
5. Full `Tests.Core` green (no regression)
6. `json_group_array` empty-set behavior verified
7. No provisional optimizer disablements

--- END OF PLAN ---
