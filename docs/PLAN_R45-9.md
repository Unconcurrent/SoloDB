# PLAN R45-9: Single-SQL Hydration Architecture

AGORA anchor: `2026-03-17_17-51-10-bellard-to-data,hipp,syme,anvil-rev,sherlock,linus,zhukov-r45-9-all-hands-agora-preplan-single-sql-hydration-superseding.md`

## PROBLEM STATEMENT

Current queryable path fires N+1 SQL queries (1 main + N batch-load calls per relation per depth). Captain requires ONE SQL statement per root query. All relation data must be embedded in the main SELECT as correlated-subquery payload columns.

## MECHANISM LOCK

**Ballot A: Canonical correlated-subquery projection** for both DBRef and DBRefMany. CTE pre-aggregation is deferred optimization. No mixed heuristics in v1. No client-side loading.

## SCOPE BOUNDARY

- **IN SCOPE**: Queryable path only (`AsQueryable()...ToList()`, `.First()`, etc.)
- **OUT OF SCOPE**: Non-queryable paths (GetById, Insert/Update read-back) — mandatory immediate R45-10 cycle
- **TEMPORARY**: BatchLoad.fs stays for non-queryable paths in R45-9 ONLY. BatchLoad non-queryable hydration is temporary and superseded by mandatory immediate R45-10; no permanent client-side hydration path is permitted under Captain absolute directive.

## CANONICAL SQL SHAPES

### DBRef (single relation)

```sql
SELECT o.Id, json_quote(o.Value),
  (SELECT json_object('Id', t.Id, 'Value', json_quote(t.Value))
   FROM "Target" t
   WHERE t.Id = jsonb_extract(o.Value, '$.RefProp')) AS RefProp_Hydrated
FROM "Owner" o WHERE ...
```

DU: `ScalarSubquery` returning `JsonObjectExpr` with Id + Value from target table, correlated on `jsonb_extract(owner.Value, '$.RefProp')`.

### DBRefMany (collection relation)

```sql
SELECT o.Id, json_quote(o.Value),
  (SELECT json_group_array(json_object('Id', t.Id, 'Value', json_quote(t.Value)))
   FROM "LinkTable" lnk
   INNER JOIN "Target" t ON t.Id = lnk.TargetId
   WHERE lnk.SourceId = o.Id) AS ManyProp_Hydrated
FROM "Owner" o WHERE ...
```

DU: `ScalarSubquery` with `FunctionCall("json_group_array", [...])` over link+target join, correlated on `lnk.SourceId = owner.Id`.

### Multi-hop (depth > 1)

Nested correlated subqueries. `Owner.Mid.Leaf` becomes:
```sql
SELECT o.Id, json_quote(o.Value),
  (SELECT json_object('Id', m.Id, 'Value', json_quote(m.Value),
    'Leaf', (SELECT json_object('Id', l.Id, 'Value', json_quote(l.Value))
             FROM "Leaf" l WHERE l.Id = jsonb_extract(m.Value, '$.Leaf')))
   FROM "Intermediate" m
   WHERE m.Id = jsonb_extract(o.Value, '$.Mid')) AS Mid_Hydrated
FROM "Owner" o WHERE ...
```

Depth cap: max 5 levels of nesting (matching existing `maxRecursiveDepth`).

## INDEX COVERAGE

All correlated-subquery predicates use indexed columns:
- `t.Id` — PRIMARY KEY (SQLite rowid)
- `lnk.SourceId` — **INDEXED** (`Relations.Schema.LinkTableDDL.fs:128,264`)
- `lnk.TargetId` — **INDEXED** (`Relations.Schema.LinkTableDDL.fs:129,265`)
- `jsonb_extract(o.Value, '$.RefProp')` — stored FK value (may benefit from expression index)

## IMPLEMENTATION SLICES

### Slice A — DBRef Single-Relation Hydration

**Mechanism:** jsonb_set Value-enrichment — correlated ScalarSubquery results are embedded directly into the owner's Value column via `jsonb_set(Value, '$.Prop', COALESCE(subquery, rawFK))` in the outer SELECT projection. The existing JSON deserializer recognizes `[id, entityJson]` array format as `DBRef.Loaded(id, entity)`. No new columns or row-type changes required. This supersedes the original separate-column parse seam described below; the enrichment approach was selected because it requires zero Provider materialization changes and reuses the existing `buildMaterializedValueExpr` pattern.

**Changes**:

| File | Change |
|------|--------|
| `Queryable.Types.fs` | Add `HydrationProjection` DU: `ScalarRelProjection` / `CollectionRelProjection` |
| `Queryable.Translation.fs` | Add `buildHydrationValueExpr` recursive function + `SingleRelationsHydrated` flag on `BatchLoadContext`; inject correlated subqueries into outer SELECT Value projection via `startTranslationCore` |
| `Queryable.Provider.fs` | Skip `batchLoadDBRefProperties` when `SingleRelationsHydrated = true` (both enumerable and scalar paths) |

**Test rows** (SA-01..SA-05):

| Row | Name | Proof |
|-----|------|-------|
| SA-01 | `SQ01_DBRef_SingleHop_OneStatement` | One-statement proof: query counter = 1, DBRef loaded |
| SA-02 | `SQ02_DBRef_MultiHop_OneStatement` | 2-hop DBRef chain, still one statement |
| SA-03 | `SQ03_DBRef_Excluded_NoSubquery` | Exclude(Ref) → no hydration subquery emitted |
| SA-04 | `SQ04_DBRef_TypedId_2G_Parity` | DBRef<T,TId> uses same correlated shape |
| SA-05 | `SQ05_DBRef_DepthCap_Truncates` | Depth > 5 stops nesting |

### Slice B — DBRefMany Collection Hydration

**Changes**:

| File | Change |
|------|--------|
| `Queryable.BuildQuery.Main.fs` | Emit json_group_array ScalarSubquery for DBRefMany relations |
| `Queryable.Provider.fs` | Parse json_group_array result into DBRefMany tracker |

**Test rows** (SB-01..SB-05):

| Row | Name | Proof |
|-----|------|-------|
| SB-01 | `SQ06_DBRefMany_OneStatement` | One-statement proof: query counter = 1, collection loaded |
| SB-02 | `SQ07_DBRefMany_Empty_ReturnsEmptyList` | Empty collection → `'[]'` → empty list |
| SB-03 | `SQ08_DBRefMany_Excluded_NoSubquery` | Exclude(Many) → no hydration subquery |
| SB-04 | `SQ09_DBRefMany_TypedId_2G_Parity` | DBRefMany<T,TId> parity |
| SB-05 | `SQ10_DBRefMany_WhitelistMode` | Exclude().Include(Many) → only Many gets subquery |

### Slice C — Integration + Legacy Bypass

**Changes**:

| File | Change |
|------|--------|
| `Queryable.Provider.fs` | Skip BatchLoad calls when hydration plan is active |
| Test harness | One-statement instrumentation: command counter wrapper |

**Test rows** (SC-01..SC-08):

| Row | Name | Proof |
|-----|------|-------|
| SC-01 | `SQ11_Mixed_DBRef_DBRefMany_OneStatement` | Both relation types in single query |
| SC-02 | `SQ12_Include_ThenExclude_Lattice_Parity` | Include(A).ThenExclude(B) → A loaded, B suppressed |
| SC-03 | `SQ13_Conflict_SamePath_Rejects` | Same-path conflict still rejects |
| SC-04 | `SQ14_Conflict_ParentExcluded_ChildIncluded_Rejects` | L-03 conflict still rejects |
| SC-05 | `SQ15_WhitelistMode_FullParity` | Exclude() whitelist mode |
| SC-06 | `SQ16_CycleGuard_SelfRef` | Self-referential type terminates at depth cap |
| SC-07 | `SQ17_PerformanceBaseline_QueryCount` | Assert exactly 1 SQL command per root query |
| SC-08 | `SQ18_LegacyBatchLoad_GetById_Unchanged` | Non-queryable path still uses BatchLoad |

## ONE-STATEMENT INSTRUMENTATION

Wrap `SqliteConnection` command execution with a counter. Assert exactly one `ExecuteReader`/`ExecuteScalar` call per root query. Capture SQL text for shape verification.

## DETERMINISTIC REJECT MAP

| Shape | Outcome |
|-------|---------|
| Depth > 5 nested subqueries | NotSupportedException |
| Missing relation metadata | NotSupportedException — deterministic reject per AGORA contract |
| Non-relation selector in Include/Exclude | NotSupportedException (existing) |
| Unsupported method-call selector | NotSupportedException (existing) |
| SQL complexity exceeds bounded column budget | NotSupportedException (new) |
| Unsupported ordering requirement for collection payloads | NotSupportedException — json_group_array order is unspecified; deterministic ordering requires explicit wrapper (deferred) |
| Scalar payload data-integrity fault (multiple competing rows for DBRef) | NotSupportedException — DBRef subquery returns >1 row indicates data corruption; fail-closed |
| One-statement preservation failure (any case requiring multi-query) | NotSupportedException — NO silent fallback to legacy batch-load |

## TYPED-ID 1G/2G MATRIX

Every SA/SB positive row has a paired 2G variant (SA-04, SB-04). Explicit 2G negative rows:

| Row | Name | Proof |
|-----|------|-------|
| N2G-01 | `SQ19_TypedId_2G_SamePath_Conflict_Rejects` | 2G same-path Include+Exclude → reject |
| N2G-02 | `SQ20_TypedId_2G_ParentExcluded_ChildIncluded_Rejects` | 2G L-03 conflict → reject |
| N2G-03 | `SQ21_TypedId_2G_MissingMetadata_Rejects` | 2G missing relation metadata → NotSupportedException |

## FULL LATTICE PARITY MAPPING (R45-8 → R45-9)

Every R45-8 admitted and reject row must produce identical outcome under single-SQL hydration:

| R45-8 Row | R45-9 Anchor | Outcome |
|-----------|-------------|---------|
| L-01 default load-all | SC-01 (mixed), SA-01, SB-01 | All relations hydrated in single SQL |
| L-02 Exclude(A) | SA-03, SB-03 | Excluded path omits subquery |
| L-04 Exclude() whitelist | SC-05, SB-05 | WhitelistMode suppresses all non-included |
| L-07 Include(A).ThenExclude(B) | SC-02 | Parent loaded, child suppressed |
| L-08 same-path conflict | SC-03 | Deterministic reject |
| L-09 parent-excluded child-included | SC-04 | Deterministic reject |
| L-10 duplicate directives | Implicit (idempotent path registration) | No semantic drift |
| L-11 disjoint paths | SC-01 | Both coexist |
| CASE 1 compile guard | Existing R45-7 TE12 | Type-system enforced |
| CASE 2 positive | SC-02 | Parent loaded, child suppressed |
| P-01/P-02 1G/2G default | SA-01, SA-04 | Hydrated in one statement |
| N-01..N-06 conflicts | SC-03, SC-04, N2G-01..N2G-03 | Deterministic reject |
| R-01 depth cap | SA-05 | Truncates at max depth |
| R-02 cycle guard | SC-06 | Self-ref terminates |

## PERFORMANCE + REGRESSION MATRIX

| Graph type | Metric | Legacy (batch-load) | Single-SQL |
|------------|--------|--------------------|-----------|
| Deep acyclic (5 hops) | Query count | 6 | 1 |
| Cyclic (self-ref) | Query count | depth-cap bounded | 1 |
| Wide fanout (10 DBRefMany) | Query count | 11 | 1 |
| Mixed DBRef + DBRefMany | Query count | N+1 | 1 |

Latency comparison required on each graph type before default-switch.

## FOLLOW-ON OBLIGATIONS

1. **Non-queryable path replacement (MANDATORY IMMEDIATE)** — GetById, Insert/Update read-back must also become single-SQL. Mapped as the NEXT cycle after R45-9 per Captain absolute directive. BatchLoad is NOT acceptable as permanent architecture for ANY path. This is not backlog — it is the immediate successor cycle.
2. **CTE optimization** — Candidate B (CTE pre-aggregation) as optional perf optimization after v1 correctness is proven. Mapped as explicit follow-on.

## BLOCKER-RESOLUTION TABLE

| Blocker | Source | Resolution |
|---------|--------|------------|
| Syme-1: Missing metadata reject type | AGORA contract | Changed from InvalidOperationException to NotSupportedException in reject map |
| Syme-2: 2G negative rows missing | Typed-id parity | Added N2G-01..N2G-03 explicit 2G conflict/reject rows |
| Syme-3: Lattice parity mapping | R45-8 baseline | Added full row-to-row mapping table (14 rows) with concrete test anchors |
| Anvil-1: Unsupported ordering reject | AGORA 17-51-10 | Added explicit reject row for collection payload ordering requirement |
| Anvil-2: Scalar multi-row fault reject | AGORA 17-51-10 | Added explicit reject row for DBRef subquery returning >1 row |
| Anvil-3: One-statement preservation failure | AGORA 17-51-10 | Added explicit reject row for multi-query fallback requirement |

## PROVISIONAL OPTIMIZER DISABLEMENTS

**NONE.**

## ACCEPTANCE CRITERIA

1. SA-01..SA-05 green (Slice A)
2. SB-01..SB-05 green (Slice B)
3. SC-01..SC-08 + N2G-01..N2G-03 green (Slice C)
4. One-statement proof: exactly 1 SQL command per root query on all positive rows
5. All existing tests green (no regression)
6. Full Tests.Core green
7. Zero optimizer disablements
8. Zero silent multi-query fallback

--- END OF PLAN ---

## IMPLEMENTATION REPORT - Hipp (Slice A)

### Batch: Slice A — DBRef Single-Relation Hydration — Completed

**Files Modified:**

| File | Change |
|------|--------|
| `SoloDB/Query/Queryable.Types.fs` | Added `HydrationProjection` DU (`ScalarRelProjection` case) |
| `SoloDB/Query/Queryable.Translation.fs` | Added `SingleRelationsHydrated` flag to `BatchLoadContext`; Added `buildHydrationValueExpr` recursive function; Modified `startTranslationCore` to inject correlated subqueries into outer SELECT Value projection |
| `SoloDB/Query/Queryable.Provider.fs` | Skip `batchLoadDBRefProperties` when `SingleRelationsHydrated = true` (both `ExecuteEnumerable` and `batchLoadSingle` paths) |

**New Test Project:**
- `SoloDBTests/SingleSqlHydration.SliceA.Tests/` — 5 test rows (SA-01..SA-05)

**Edge Cases Implemented:**

| Edge Case | Location | Comment |
|-----------|----------|---------|
| SA-03: Excluded path → no subquery | `buildHydrationValueExpr` | `shouldLoadRelationPath` check per property |
| SA-05: Depth cap at 5 | `buildHydrationValueExpr` | `depth >= maxHydrationDepth` guard |
| DBRef.None (null FK) | COALESCE fallback | Subquery returns NULL, COALESCE returns NULL → deserialized as None |
| Dangling FK | COALESCE fallback | Subquery returns NULL, COALESCE returns raw int → deserialized as Unloaded |
| Missing relation metadata | `buildHydrationValueExpr` | NotSupportedException per AGORA contract |
| All props excluded | `buildHydrationValueExpr` | `args.Count <= 1` → returns original expr unchanged |
| Multi-hop recursive | `buildHydrationValueExpr` | Recursive call with `depth + 1` and child alias |
| Column alias qualification | All subquery references | `Column(Some "o", "Value")` for outer, `Column(Some tAlias, ...)` for inner — prevents correlated ambiguity |

**Error Handling:**

| Condition | Detection | Handling |
|-----------|-----------|----------|
| Missing relation target metadata | `ctx.TryResolveRelationTarget` returns None | `NotSupportedException` with diagnostic message |
| Depth > 5 | `depth >= maxHydrationDepth` | Silent truncation (leaves FK ids as-is, matching batch-load behavior) |

**Build Status:** Success (0 warnings, 0 errors)

**Test Results:**
- SA-01 `SQ01_DBRef_SingleHop_OneStatement`: PASS — single-hop DBRef loaded via hydration
- SA-02 `SQ02_DBRef_MultiHop_OneStatement`: PASS — 2-hop DBRef chain fully loaded
- SA-03 `SQ03_DBRef_Excluded_NoSubquery`: PASS — excluded DBRef not loaded, FK preserved
- SA-04 `SQ04_DBRef_TypedId_2G_Parity`: PASS — two DBRef props + nested all loaded
- SA-05 `SQ05_DBRef_DepthCap_Truncates`: PASS — 6-hop chain, depth 1-5 loaded, depth 6 truncated

**Regression:**
- CSharpTests: 72/73 (1 pre-existing failure: R7_2 boundary)
- Tests.CSharpProxy: 249/249
- RelationalApi.IQueryableParity: 161/161
- RelationalApi.CommonEdge: 20/20
- DocsExampleTests: 25/25

**SQL Shape:** Correlated scalar subqueries embedded in `jsonb_set` within the outer `json_extract` projection. Deserializer recognizes `[id, entityJson]` array format as `DBRef.Loaded(id, entity)`.

**Blocked?** No
