module internal SoloDatabase.SqlCapture

/// Chain-executor SQL emission hook. The chain executor's executeSqlDu helper
/// (SoloDB.Collection.Mutation.fs) forwards every SQL statement it emits to
/// this callback before sending it to the SQLite connection. The hook is
/// scoped to that one emission path — callers must not assume any other
/// product write path emits here. Callers must restore the previous value
/// (typically `None`) when done capturing; otherwise every subsequent
/// chain-executor UpdateMany on any collection emits to a stale callback.
let mutable internal OnSqlEmitted : (string -> unit) option = None
