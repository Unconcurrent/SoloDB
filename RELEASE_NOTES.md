# SoloDB release notes

## 1.2.6 — unreleased

### Security

The bundled SQLite engine moves from 3.49.1 to 3.53.3, which is above the 3.50.2 line that fixes
CVE-2025-6965. Only the native library advances: `SQLitePCLRaw.lib.e_sqlite3` is pinned to 2.1.12
while the managed components stay at 2.1.11, because measurement showed the incompatibility that
blocked the move was entirely in the engine and nothing required moving them.

### Behaviour change for applications that write their own SQL

**The new engine rejects double-quoted string literals.** It is built with `SQLITE_DQS=0`, which
disables the long-standing SQLite fallback that treats a double-quoted token as a string when no
column of that name exists.

This does not affect queries built through SoloDB's own API. It affects an application that issues
SQL of its own through the exposed connection: `WHERE Name = "Ion"` was previously accepted and is
now an error, because `"Ion"` names a column. Such SQL needs single quotes for string literals —
`WHERE Name = 'Ion'` — which is what standard SQL requires in any case.

This is about database files, not about applications. A database file written by an earlier release
is unaffected: stored schema written under the old behaviour continues to load, and such a database
migrates normally. An existing *application* can still be affected, by its own SQL, exactly as
described above.

### Fixes

- Schema creation used double-quoted string literals in its CHECK constraints, which the new engine
  rejects. They are proper string literals now. A database created after this change stores schema
  identical to one created before it.
- Migration steps are executed statement by statement rather than as one batch. A statement rejected
  inside a batch could return normally, leaving the work half done and the schema version unchanged
  with nothing raised where it failed. A failure is now reported with the statement that caused it,
  and a rollback is attempted; if the rollback or the restoration of connection state also fails,
  that is reported alongside the original failure rather than replacing it, and the connection is
  withdrawn from the pool instead of being handed out again.
- A migration is now idempotent under concurrent open. Two processes opening a database that needs
  migrating both read the version before either takes the lock; the version is re-checked under the
  lock so the second does not apply work the first has already done.
- The page-size directive in schema creation had never taken effect, because WAL mode is established
  before it and page size cannot change afterwards. It is removed; databases are unaffected, since
  none was ever created with the size it asked for.
