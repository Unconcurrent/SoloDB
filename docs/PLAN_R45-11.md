# PLAN R45-11: Non-Queryable Read-Path Raw-SQL Purge

## PROBLEM STATEMENT

3 non-queryable read paths still use raw SQL string interpolation with `QueryTranslator.translate` output. These bypass the SqlExpr DU system. Replace with DU-built WHERE using the R45-10 pattern (`buildHydratedGetByIdSql` / `buildManyOnlyHydratedSql`).

## SCOPE LOCK

**IN SCOPE (3 sites):**

| Site | File | Line | Current Pattern | Purpose |
|------|------|------|-----------------|---------|
| S-1 | `ReadDelete.fs` | 135 | `$"SELECT ... WHERE {filter} LIMIT 1"` | TryGetByCustomId no-relations fallback |
| S-2 | `ReadDelete.fs` | 163 | `$"SELECT ... WHERE {filter} LIMIT 1"` | DeleteByCustomId old-state read |
| S-3 | `SoloDB.fs` | 283 | `$"SELECT ... WHERE {filter} LIMIT 1"` | Upsert existence check |

**OUT OF SCOPE (mandatory immediate R45-12):**
- 8 write-path raw-filter sites (Mutation.fs:83,92,134,164,206,211,252; ReadDelete.fs:171)
- 4 schema string-manipulation sites (Helper.Schema.fs:78,92,135,143)

## MECHANISM

DU-first. Build WHERE as `SqlExpr.Binary(jsonb_extract("o".Value, '$.PropName'), Eq, Parameter("_cid0"))` — identical to R45-10 Slice A custom-id pattern. No `SqlExpr.RawFragment`. No raw SQL concatenation.

For S-1/S-2: `idProp` is available from `CustomTypeId<'T>.Value`. Build WHERE directly as SqlExpr.
For S-3: same — `customId.Property` provides the property name.

## IMPLEMENTATION

**ONE SLICE.** All 3 sites are structurally identical (custom-id SELECT with raw filter → DU WHERE).

| Site | Change | Helper |
|------|--------|--------|
| S-1 | Replace raw SELECT with DU-built WHERE + bare SELECT (no relations = no hydration) | `buildManyOnlyHydratedSql` (returns bare SELECT for no-DBRefMany types) |
| S-2 | Replace raw SELECT with DU-built WHERE + bare SELECT (delete old-state read, no hydration) | `buildManyOnlyHydratedSql` |
| S-3 | Replace raw SELECT with DU-built WHERE + bare SELECT (upsert existence check, no hydration) | `buildManyOnlyHydratedSql` |

For S-1 (no-relations fallback): Since `hasRelations = false` on this branch, no hydration is needed. Build a bare DU SELECT (Id + json_quote(Value) AS ValueJSON) with WHERE and emit via `buildManyOnlyHydratedSql` which returns a bare SELECT when the type has no DBRefMany.

For S-2/S-3: Same pattern — build SqlExpr WHERE from `idProp`, construct simple SELECT DU, emit.

## TEST MATRIX (5 rows)

| Row | Name | Proof |
|-----|------|-------|
| T-01 | `CustomId_NoRelations_DU_Select` | S-1: `QueryCommandInstrumentation.Reset()` before call, `Count()==1` after; correct entity returned via DU SELECT |
| T-02 | `DeleteByCustomId_OldStateRead_DU` | S-2: `QueryCommandInstrumentation.Reset()` before delete, `Count()==1` for old-state read; delete succeeds, entity removed |
| T-03 | `Upsert_ExistenceCheck_DU` | S-3: `QueryCommandInstrumentation.Reset()` before upsert, `Count()==1` for existence check; correct insert-or-update behavior |
| T-04 | `CustomId_2G_Parity` | 1G/2G parity: custom [SoloId] string entity through S-1 path, counter=1, data integrity |
| T-05 | `Grep_Zero_RawFilter_InScope` | grep guard: zero `$"SELECT.*WHERE {filter` in ReadDelete.fs S-1/S-2 and SoloDB.fs S-3 |

## DETERMINISTIC REJECT MAP

| Shape | Outcome |
|-------|---------|
| Missing custom-id property | InvalidOperationException (existing, unchanged) |
| Missing relation metadata | NotSupportedException (shared generator, existing) |

## ACCEPTANCE CRITERIA

1. T-01..T-05 green
2. R45-9 tests: 21/21 unchanged
3. R45-10 tests: 11/11 unchanged
4. Full regression stable: 527/528 (R7_2 pre-existing)
5. Zero raw SQL filter interpolation at S-1/S-2/S-3

--- END OF PLAN ---
