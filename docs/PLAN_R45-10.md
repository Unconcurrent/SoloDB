# PLAN R45-10: Non-Queryable SQL Hydration Replacement

AGORA anchor: `2026-03-17_19-16-54-bellard-to-data,hipp,syme,anvil-rev,sherlock,linus,zhukov-r45-10-all-hands-agora-preplan-non-queryable-sql-hydration.md`

## PROBLEM STATEMENT

Non-queryable paths (GetById, TryGetById, Update/Replace old-state reads) still use N+1 BatchLoad queries for relation hydration. Captain requires ONE SQL read statement per hydration/state-read operation. All 5 batch-load call sites must be replaced with correlated-subquery SQL, reusing R45-9 infrastructure.

## STRUCTURE LOCK

**TWO-SLICE** execution. Shared typed DU generator. Mutation-prep MANY-ONLY.

## SCOPE BOUNDARY

- **IN SCOPE**: 5 call sites in SoloDB.fs (2) and Collection.Mutation.fs (3)
- **OUT OF SCOPE**: Queryable path (R45-9, frozen), Include/Exclude API for GetById, BatchLoad dead-code cleanup beyond unreachability, optimizer/translator rework

## SHARED GENERATOR EXTRACTION

Extract R45-9 hydration SQL builders into `Query/Queryable.HydrationSql.fs` (compiled before both `Queryable.Translation.fs` and `SoloDBCore/*`):

| Function | Source (R45-9) | Responsibility |
|----------|---------------|----------------|
| `buildHydrationValueExpr` | `Queryable.Translation.fs` | DBRef recursive jsonb_set enrichment, depth cap 5 |
| `buildManyHydrationProjection` | `Queryable.Translation.fs` | DBRefMany json_group_array over link+target join |
| `buildHydratedSelectSql` | NEW | Compose base SELECT + hydrated Value + HydrationJSON, emit to SQL string via existing emitter |
| `preloadQueryContextMetadata` | `Queryable.Translation.fs` | Load relation metadata into QueryContext (already shared-ready) |

**Queryable path rewiring:** `Queryable.Translation.fs` imports from shared module. Zero semantic change — byte-identical SQL output verified by existing R45-9 tests (21/21).

**Non-queryable path wiring:** `SoloDB.fs` and `Collection.Mutation.fs` call `buildHydratedSelectSql` which:
1. Constructs a QueryContext with relation metadata via `preloadQueryContextMetadata`
2. Builds SqlExpr DU tree using shared builders
3. Emits to SQL string via `emitSelectToSb` (existing emitter)
4. Returns the SQL string + variable dictionary for Dapper execution

## CALL-SITE MAP

### Slice A — Non-Queryable Hydration Reads

| Site | File | Line | Current SQL | Replacement |
|------|------|------|-------------|-------------|
| H-1 | `SoloDB.fs` | 365-366 | `SELECT Id, json_quote(Value) as ValueJSON FROM "T" WHERE Id = @id` + BatchLoad(DBRef+DBRefMany) | Single SELECT with jsonb_set Value + HydrationJSON |
| H-2 | `SoloDB.fs` | 403-404 | Same pattern via custom-id filter + BatchLoad(DBRef+DBRefMany) | Same single SELECT |

**Behavior:** Full load-all hydration (no Include/Exclude — hardcoded empty paths, whitelistMode=false). Same depth cap (5). Same COALESCE fallback for dangling/null FKs.

### Slice B — Mutation-Prep Old-State Reads

| Site | File | Line | Current SQL | Replacement |
|------|------|------|-------------|-------------|
| M-1 | `Collection.Mutation.fs` | 53 | `SELECT ... WHERE {filter} LIMIT 1` + BatchLoad(DBRefMany only) | SELECT with HydrationJSON (DBRefMany only) |
| M-2 | `Collection.Mutation.fs` | 168 | `SELECT ... WHERE {filterSql}` + BatchLoad(DBRefMany only, batch) | SELECT with HydrationJSON (DBRefMany only) |
| M-3 | `Collection.Mutation.fs` | 203 | `SELECT ... WHERE ({filterSql}) LIMIT 1` + BatchLoad(DBRefMany only) | SELECT with HydrationJSON (DBRefMany only) |

**Behavior:** DBRefMany-only projection. No DBRef enrichment. Preserves current write-plan semantics exactly.

## CANONICAL SQL SHAPES

### Slice A — GetById Full Hydration

```sql
SELECT Id,
  json_extract(
    jsonb_set(Value, '$.Ref',
      COALESCE(
        (SELECT jsonb_array(t.Id, t.Value) FROM "Target" t
         WHERE t.Id = jsonb_extract(Value, '$.Ref')),
        jsonb_extract(Value, '$.Ref'))),
    '$') AS ValueJSON,
  json_object(
    'Many', COALESCE(
      (SELECT json_group_array(json_object('Id', t.Id, 'Value', json_quote(t.Value)))
       FROM "Link" lnk INNER JOIN "Target" t ON t.Id = lnk.TargetId
       WHERE lnk.SourceId = Id),
      jsonb('[]'))
  ) AS HydrationJSON
FROM "Collection" WHERE Id = @id LIMIT 1
```

### Slice B — Mutation-Prep DBRefMany-Only

```sql
SELECT Id, json_quote(Value) AS ValueJSON,
  json_object(
    'Many', COALESCE(
      (SELECT json_group_array(json_object('Id', t.Id, 'Value', json_quote(t.Value)))
       FROM "Link" lnk INNER JOIN "Target" t ON t.Id = lnk.TargetId
       WHERE lnk.SourceId = Id),
      jsonb('[]'))
  ) AS HydrationJSON
FROM "Collection" WHERE Id = @id LIMIT 1
```

Note: Value column NOT enriched with DBRef subqueries (mutation-prep does not need loaded single-relations).

## IMPLEMENTATION SLICES

### Slice A — Non-Queryable Hydration Reads

**Changes:**

| File | Change |
|------|--------|
| `Query/Queryable.HydrationSql.fs` (NEW) | Extract `buildHydrationValueExpr`, `buildManyHydrationProjection`, `preloadQueryContextMetadata` from `Queryable.Translation.fs`; add `buildHydratedSelectSql` composer |
| `Query/Queryable.Translation.fs` | Import from shared module; remove moved functions; zero semantic change |
| `SoloDBCore/SoloDB.fs` | Replace BatchLoad calls at H-1/H-2 with `buildHydratedSelectSql` + `HydrationManyPopulator` |
| `SoloDB.fsproj` | Add `Queryable.HydrationSql.fs` before `Queryable.Translation.fs` |

**Test rows (A-01..A-05):**

| Row | Name | Proof |
|-----|------|-------|
| A-01 | `GetById_Int64_SingleSQL_OneStatement` | Runtime counter = 1, DBRef + DBRefMany loaded |
| A-02 | `TryGetById_CustomId_SingleSQL` | 2G typed-id, counter = 1, both relation types loaded |
| A-03 | `GetById_MultiHop_FullChain` | 2-hop DBRef chain loaded in one GetById call |
| A-04 | `GetById_DBRefMany_2G_Parity` | DBRefMany<T,TId> on GetById path |
| A-05 | `GetById_SelfRef_CycleGuard` | Self-ref type terminates at depth cap |

### Slice B — Mutation-Prep Old-State Reads

**Changes:**

| File | Change |
|------|--------|
| `SoloDBCore/SoloDB.Collection.Mutation.fs` | Replace BatchLoad calls at M-1/M-2/M-3 with `buildManyHydrationProjection` SQL + `HydrationManyPopulator` |

**Test rows (B-01..B-06):**

| Row | Name | Proof |
|-----|------|-------|
| B-01 | `Update_OldStateRead_OneStatement` | Old-state read counter = 1, DBRefMany state correct |
| B-02 | `Update_OldStateRead_2G_Parity` | DBRefMany<T,TId> on Update path |
| B-03 | `ReplaceMany_OldStateRead_Parity` | Multiple entities, counter = 1 per read |
| B-04 | `ReplaceOne_OldStateRead_Parity` | Single entity, counter = 1 |
| B-05 | `MutationPrep_MissingMetadata_Rejects` | Missing relation metadata → deterministic reject |
| B-06 | `MutationPrep_NoFallbackToBatchLoad` | Assert BatchLoad never called on mutation-prep paths |

## DETERMINISTIC REJECT MAP

| Shape | Outcome |
|-------|---------|
| Missing relation metadata | NotSupportedException (shared generator) |
| Missing link-table mapping | NotSupportedException (shared generator) |
| Missing typed-id unique index | InvalidOperationException (bootstrap, existing) |
| Depth > 5 nested subqueries | NotSupportedException on non-queryable paths (R45-10 contract); queryable path retains silent truncation (R45-9 frozen) |
| SQL payload-budget overflow | Not applicable in R45-10 — shared generator does not introduce column-budget limits; correlated subqueries are unbounded per property count (same as R45-9) |
| One-statement invariant failure | NotSupportedException — no fallback to BatchLoad |

## ONE-STATEMENT PROOF METHODS

### Slice A
- `QueryCommandInstrumentation.Reset()` before `GetById`/`TryGetById`
- Assert `Count() == 1` after call returns
- Assert loaded DBRef + DBRefMany data integrity
- Assert BatchLoad functions NOT invoked (verify via counter: if BatchLoad ran, counter > 1)

### Slice B
- Instrument the old-state read helper (not the whole public API which includes writes)
- `QueryCommandInstrumentation.Reset()` before old-state SELECT
- Assert `Count() == 1` for the read portion
- Assert relation write-plan parity vs current behavior (mutation outcome unchanged)

## R45-9 REGRESSION GUARD

All 21 existing R45-9 tests (SA-01..SA-05 + SB-01..SB-05 + SC-01..SC-08 + N2G-01..N2G-03) must remain green after shared generator extraction. This is the primary parity proof that queryable path is frozen.

## ACCEPTANCE CRITERIA

1. A-01..A-05 green (Slice A)
2. B-01..B-06 green (Slice B)
3. R45-9 tests unchanged: 21/21 green
4. Full regression suite stable: 527/528 (R7_2 pre-existing)
5. Zero BatchLoad invocations on queryable + GetById paths
6. Zero silent fallback to client-side hydration
7. Zero optimizer disablements

--- END OF PLAN ---
