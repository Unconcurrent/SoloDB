# Direct SQL connections

SoloDB exposes a native SQLite connection in `SoloDatabase`. Named SQL execution, typed results, transactions, callbacks and backups use the configured SQLitePCL engine.

```csharp
using SoloDatabase;
using static SoloDatabase.NativeSQLiteTools.IDbConnectionExtensions;

using var connection = new NativeSqliteConnection("Data Source=documents.solodb");
connection.Open();
connection.Execute("CREATE TABLE IF NOT EXISTS Notes(Value TEXT)");
connection.Execute("INSERT INTO Notes VALUES(@value)", new { value = "hello" });
var count = connection.QueryFirst<long>("SELECT COUNT(*) FROM Notes");

using var rows = connection.OpenReader("SELECT Value FROM Notes");
while (rows.Read()) Console.WriteLine(rows.Row.GetString(0));
```

`Query<T>` enumerates the first result lazily. `QueryFirst<T>` requires a row; `QueryFirstOrDefault<T>` returns a default value when no row exists: reference tuples contain default members and other types use their normal default. Classes and records map column names; tuples map column positions. Multi-mapping resolves each object within its own column range, using `splitOn` to identify the second object.

`OpenReader` holds exclusive SQL access to its connection until disposal. Lazy `Query` enumerations can overlap using independent execution leases.

A `SqlRow` borrows its current execution lease. Read it before advancing, disposing or closing the connection. Materialized strings and byte arrays are owned values. `SqlRowValue.Read<T>` and `ReadNullable<T>` expose the same scalar conversion rules used by model mapping.

## SQLite engine and WAL compatibility

The bundled native dependency supplies SQLite 3.53.3, including the upstream WAL-reset fix. Packaged JIT and linux-x64 NativeAOT controls report that engine; static asset inspection on other platforms establishes provenance rather than execution.

SoloDB honors a SQLitePCL provider selected by the application before its first connection. A package version is not the loaded SQLite version. Query the actual connection with:

```sql
SELECT sqlite_version();
```

Database startup checks the features required by SoloDB: JSONB and trigger RAISE() message expressions, available in SQLite 3.47.0 and later. It does not refuse an application-selected engine because its version label appears in a vulnerability advisory, including a vendor backport with an unchanged version label. Vulnerability remediation for the default engine belongs to dependency selection, not a runtime safety-version gate. See [SQLite's WAL-reset advisory](https://sqlite.org/wal.html#walresetbug) for the upstream issue.

Direct `NativeSqliteConnection` and ADO use is independent of the document-store startup feature check. Provider selection remains with the application; generated registration neither selects nor upgrades that provider.

## Typed arguments and direct mapping

A reusable parameter plan captures typed state once when execution begins, before SQL effects. Its binder writes primitives directly for each statement in a batch. The parameter writer is valid only during that binder invocation.

```csharp
static readonly SqlParameterPlan<(long Id, string Text), (long Id, string Text)> WriteNote =
    new(new[] { "id", "text" }, value => value, (writer, value) => {
        writer.Int64(0, value.Id);
        writer.Text(1, value.Text);
    });

connection.Execute("CREATE TABLE IF NOT EXISTS NotesWithIds(Id INTEGER PRIMARY KEY, Value TEXT)");
connection.Execute("INSERT INTO NotesWithIds(Id, Value) VALUES(@id, @text)",
                   WriteNote, (17L, "hello"));

var values = connection.Query("SELECT Value FROM Notes",
    new Func<SqlRow, string>(row => row.GetString(0)));
```

Reuse a plan for its stable parameter shape. Anonymous objects, named dictionaries and `SqlArguments` provide dynamic input convenience. Values are captured at execution acquisition; capture exceptions occur before the first statement takes effect. Missing values fail instead of reusing earlier bindings. Supply null or `DBNull.Value` for SQL NULL. An unprefixed name must identify one SQL parameter; retain its symbol prefix when names would be ambiguous. Strings and blobs preserve empty values and embedded NUL.

Generated registration infers argument capture separately from JSON model roots at checked C# and F# SQL calls. It follows available original anonymous and named input types, including source-local helpers and factories; an unavailable runtime-selected concrete type cannot be inferred from an object or interface declaration alone.

Object capture uses original readable public instance CLR properties, including inherited properties. JSON ignore and naming policies do not omit or rename SQL arguments. Each selected getter runs once during acquisition, even when its name is unused by the SQL. Boxed value-type getters operate on the original box, preserving mutations before success or an exception.

Raw SQL Decimal and BigInteger parameters and function results use invariant numeric text. Typed BigInteger reads preserve full-width integer text, construct from SQLite INTEGER or REAL values, and retain native integer parsing rules. Untyped reads return the original SQLite value, including a string for TEXT; they do not interpret document numerical keys.

## Transactions, batches and ownership

Dispose borrowed connections when their operation finishes. Public transaction scopes own a separate connection; handles escaped from a completed scope cannot execute further operations. An unfinished explicitly owned transaction rolls back on disposal. Finish raw SQL transactions before returning a connection to the pool.

SQL batches use SQLite parser boundaries, including trigger bodies and quoted semicolons. Later statements can use schema created by earlier statements. `NextResult` executes subsequent statements. Early lease disposal completes the current result and attempts unread statements, preserving earlier writes if an unread tail fails. Use explicit result advancement or `Execute` to observe subsequent statement errors. Use a transaction when all batch effects must be atomic.

Failed parameter capture, row mapping or required-row checks abort unread statements. Cleanup failures are still reported during disposal. Database failures caught inside event handlers still prevent the surrounding transaction from committing.

Catch `SoloDatabase.NativeSqliteException` for engine errors. Its `SqliteErrorCode` and `SqliteExtendedErrorCode` expose the primary and extended result codes. Connection state uses `SqliteConnectionState`.

## Standard ADO.NET access

The existing SoloDB package also provides standard ADO.NET connections, commands, parameters, readers and transactions in `SoloDatabase.Data.Sqlite`. `GetInternalConnection`, pooled `Borrow` and transaction connection properties return this presentation automatically and share their native connection. Internal SoloDB operations use native execution directly.

```csharp
using System.Data;

using var connection = collection.GetInternalConnection();
IDbConnection standard = connection;
using var command = standard.CreateCommand();
command.CommandText = "SELECT @value";
var parameter = command.CreateParameter();
parameter.ParameterName = "value";
parameter.Value = 19L;
command.Parameters.Add(parameter);
var value = (long)command.ExecuteScalar();
```

Existing inferred connection and standard-interface code keeps its acquisition syntax. Code naming the former provider types must use `SoloDatabase.Data.Sqlite`; the original `Microsoft.Data.Sqlite` package can coexist. Engine errors from this presentation use `SoloDatabase.Data.Sqlite.SqliteException`. Stored procedures and output parameters are unsupported.

Explicit `Close` ends the connection and its transactions. Disposing a transaction or event connection presentation leaves the surrounding scope's connection open; disposing an owned pooled acquisition returns it only after its raw SQL transaction is finished. An unsuccessful return can be retried after explicit rollback. Commands and readers cannot revive an expired acquisition or callback scope.

`GetStream` preserves native incremental BLOB lifetimes when a selected row identity and unchanged BLOB column come from one ordinary table or transparent single-source view, CTE or derived query. Updating that physical row expires the stream. Queries whose physical identity cannot be established return an owned snapshot of the exact selected bytes; that snapshot remains readable after row updates.
