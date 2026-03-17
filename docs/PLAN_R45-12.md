# PLAN R45-12: Write-Path + Schema Raw-SQL Purge

AGORA anchor: `2026-03-17_20-46-05-bellard-to-data,hipp,syme,anvil-rev,sherlock,linus,zhukov-r45-12-all-hands-agora-consolidated-two-slice-predicate-lock.md`

## PROBLEM STATEMENT

8 write-path sites use raw filter string concatenation from `QueryTranslator.translate` in UPDATE/DELETE SQL. 4 schema sites use `.Replace`/`.Substring` string surgery on translated SQL for index DDL. All 12 bypass the SqlExpr DU system.

## STRUCTURE LOCK

**TWO-SLICE.** No full UPDATE/DELETE DU expansion. No public RawFragment.

## MECHANISM LOCK

**`translateWhereExpr`** becomes the canonical predicate path. Returns `SqlExpr` instead of raw SQL string. `QueryTranslator.translate` becomes a thin wrapper: `translateWhereExpr` → emit → return string.

Write sites compose: fixed UPDATE/DELETE template + emitted WHERE from `SqlExpr`.
Schema sites emit index expressions from DU directly — no post-emission surgery.

## SCOPE

**IN SCOPE (12 sites):**

### Slice A — Write Raw-Filter (8 sites)

| # | File | Line | Pattern |
|---|------|------|---------|
| W-1 | Mutation.fs | 83 | Update hasRelations: `UPDATE ... WHERE ` + filter |
| W-2 | Mutation.fs | 92 | Update no-relations: `UPDATE ... WHERE ` + filter |
| W-3 | Mutation.fs | 134 | DeleteMany: `DELETE ... WHERE ` + filterSql |
| W-4 | Mutation.fs | 164 | DeleteOne no-rel: subquery with filterSql |
| W-5 | Mutation.fs | 206 | ReplaceMany write: `UPDATE ... WHERE ` + filterSql |
| W-6 | Mutation.fs | 211 | ReplaceMany no-rel: `UPDATE ... WHERE (filterSql)` |
| W-7 | Mutation.fs | 252 | ReplaceOne no-rel: subquery with filterSql |
| W-8 | ReadDelete.fs | 171 | DeleteByCustomId no-rel: `DELETE ... WHERE {filter}` |

### Slice B — Schema String-Surgery (4 sites)

| # | File | Line | Pattern |
|---|------|------|---------|
| S-1 | Helper.Schema.fs | 78 | `.Replace(tableName, "")` |
| S-2 | Helper.Schema.fs | 92 | `.Substring("jsonb_array".Length)` |
| S-3 | Helper.Schema.fs | 135 | duplicate `.Replace` |
| S-4 | Helper.Schema.fs | 143 | duplicate `.Substring` |

**OUT OF SCOPE:** Full UpdateStmt/DeleteStmt DU, non-target sites, queryable/hydration changes.

## IMPLEMENTATION

### Slice A — translateWhereExpr + Template Composition

**Changes:**

| File | Change |
|------|--------|
| `Query/QueryTranslator.fs` (line 52: `translateToSqlExpr`) | Add `translateWhereExpr` returning `SqlExpr` from `Expression<Func<'T, bool>>`; make `translate` a thin wrapper (translateWhereExpr → emitExprToSql → return string) |
| `Queryable.HydrationSql.fs` | Expose `emitExprToSql` for write-path WHERE emission (already exists) |
| `SoloDB.Collection.Mutation.fs` | All 7 write sites: call `translateWhereExpr`, emit WHERE via `emitExprToSql`, compose into UPDATE/DELETE template |
| `SoloDB.Collection.ReadDelete.fs` | W-8: same pattern for DeleteByCustomId no-rel |

**Test rows (A-01..A-06):**

| Row | Name | Proof |
|-----|------|-------|
| A-01 | `Grep_Zero_RawFilter_WriteFiles` | Zero raw filter concat/interpolation at 8 write anchors |
| A-02 | `Update_Parity_HasRelAndNoRel` | Update hasRelations + no-relations outcome unchanged |
| A-03 | `DeleteMany_DeleteOne_Parity` | Delete affected-count and survivor-set unchanged |
| A-04 | `ReplaceMany_ReplaceOne_Parity` | Replace write semantics unchanged |
| A-05 | `CustomId_TypedId_WriteParity` | Custom-id and 2G typed-id write paths |
| A-06 | `Translate_TranslateWhereExpr_Differential` | translate() wrapper produces identical SQL to translateWhereExpr() → emit |

### Slice B — DU Index Expression Emission

**Changes:**

| File | Change |
|------|--------|
| `SoloDB.Helper.Schema.fs` | Replace `.Replace`/`.Substring` with DU-level index expression emission; emit without table qualification for scalar, handle tuple prefix at DU level |

**Test rows (B-01..B-04):**

| Row | Name | Proof |
|-----|------|-------|
| B-01 | `Grep_Zero_Surgery_SchemaFiles` | Zero `.Replace`/`.Substring` surgery at 4 schema anchors |
| B-02 | `ScalarIndex_DDL_Parity` | Scalar index CREATE INDEX unchanged |
| B-03 | `TupleIndex_DDL_Parity` | Tuple index CREATE INDEX unchanged |
| B-04 | `IndexReject_Parity` | Existing rejects for relation/constant/variable/subquery shapes preserved |

## DETERMINISTIC REJECT MAP

| Shape | Outcome |
|-------|---------|
| Unrepresentable predicate in translateWhereExpr | NotSupportedException |
| Unsupported index expression normalization | Preserve current ArgumentException / InvalidOperationException |
| Missing metadata / bootstrap failures | Preserve current user-visible exception classes |
| No silent fallback to raw concat or surgery | Enforced by grep guard |

## ACCEPTANCE CRITERIA

1. A-01..A-06 green (Slice A)
2. B-01..B-04 green (Slice B)
3. R45-9: 21/21 unchanged
4. R45-10: 11/11 unchanged
5. R45-11: 5/5 unchanged
6. Full regression stable: 527/528 (R7_2 pre-existing)
7. grep-zero raw filter concat in Mutation.fs and ReadDelete.fs write paths
8. grep-zero Replace/Substring surgery in Helper.Schema.fs

## CARRY-FORWARD

Full UPDATE/DELETE DU statement types are a future optional architecture cycle. R45-12 does not block that work.

--- END OF PLAN ---
