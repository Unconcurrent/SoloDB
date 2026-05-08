module internal SoloDatabase.SqlCapture

/// Test-side SQL emission hook. When set, every SQL statement that the
/// chain-executor pipeline emits is forwarded to the callback before it is
/// executed against the SQLite connection. Callers must restore the previous
/// value (typically `None`) once they no longer need to capture, otherwise
/// every subsequent UpdateMany on any collection emits to a stale callback.
///
/// Scope: currently fires from the chain executor's executeSqlDu helper.
/// Other SQL paths in SoloDB do not yet emit through this hook; expand the
/// emission sites if a future test cell needs broader coverage.
let mutable internal OnSqlEmitted : (string -> unit) option = None
