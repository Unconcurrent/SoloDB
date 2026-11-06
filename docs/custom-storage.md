# Custom SQLite storage

Use `SqliteStorage` when you want to supply the database's bytes through streams or your own file implementation. You still use the usual collections, transactions, queries, full text search and ADO connections.

For an ordinary database file, keep using `new SoloDB(path)`.

## Choose what to supply

| Factory | What you keep | When to choose it |
| --- | --- | --- |
| `Simple(main, journal)` | Database and rollback journal | Readers normally finish before a writer commits. |
| `Wal(main, log)` | Database and write-ahead log | A reader needs to keep its snapshot while another connection commits. |
| `ReadOnly(main)` | One complete, unchanging database image | You only need to read an existing database. |
| `Native(filename, registeredVfs, profile)` | An existing native SQLite filesystem registration | You need native handles, extensions or a specialist native filesystem. |

`Simple` uses SQLite's PERSIST journal mode. `Wal` creates databases with 4096 byte pages. Both allow readers and one writer. A long-running reader can delay a writer in Simple mode or delay log cleanup in WAL mode.

The first three factories accept either streams or `ISqliteStorageFile` objects. Each factory returns a single-use `SqliteStorage` object, called `storage` in the examples. Give it to one `SoloDB` instance and dispose it even if you never open the database.

Keep the main file and its journal or log together across reopenings. Give each one separate storage. Do not replace a journal or log with an empty file: SQLite may need it to recover committed work.

An existing WAL image must already use 4096 byte pages and WAL headers. A nonempty log without its main image is refused. Simple needs an image in rollback journal format. To change layouts, back up into a fresh destination using the chosen layout; do not reopen incompatible files and expect them to be reformatted. See the [conversion steps below](#import-export-and-read-only-images).

The string and `SqliteStorage` constructors remain distinct. An F# helper with an untyped constructor argument needs its intended type, for example `let openDatabase (source: string) = new SoloDB(source)`. Give a null argument its intended type too.

## Open and reopen streams

Streams must support reading and seeking, plus writing for Simple or WAL. Pass `leaveOpen: true` if you will close them yourself, or `false` to let SoloDB close them after the database has finished using them.

This complete C# example uses memory. Its contents disappear when the process ends.

```csharp
using System.IO;
using SoloDatabase;

using var main = new MemoryStream();
using var journal = new MemoryStream();
using var storage = SqliteStorage.Simple(main, journal, leaveOpen: true);
using (var db = new SoloDB(storage))
{
    var rows = db.GetCollection<Article>();
    rows.Insert(new Article { Id = 1, Title = "alpha" });
    rows.EnsureSearchIndex(b => b.Field(x => x.Title));
}

// Reuse the streams only after every reader, connection and transaction has finished.
if (!storage.IsClosed)
    throw new System.InvalidOperationException("Finish the remaining database users before reopening.");
if (storage.ClosingFailure is System.Exception closeError)
    throw new IOException("Closing the database failed.", closeError);

using var next = SqliteStorage.Simple(main, journal, leaveOpen: true);
using var reopened = new SoloDB(next);
if (reopened.GetCollection<Article>().GetById(1L).Title != "alpha")
    throw new System.InvalidOperationException("Stored value changed.");

sealed class Article
{
    public long Id { get; set; }
    public string Title { get; set; } = "";
}
```

Sequential reuse is allowed after the previous `storage.IsClosed` is true, provided you left the streams open. Create a new `SqliteStorage` object for each database instance; you cannot attach the old object again.

Put F# disposal scopes inside a function or entry point:

```fsharp
open System.IO
open SoloDatabase

[<CLIMutable>]
type Article = { Id: int64; Title: string }

let run () =
    use main = new MemoryStream()
    use log = new MemoryStream()
    use storage = SqliteStorage.Wal(main, log, true)
    use db = new SoloDB(storage)
    let rows = db.GetCollection<Article>()
    rows.Insert({ Id = 1L; Title = "alpha" }) |> ignore
    rows.EnsureSearchIndex(fun builder ->
        builder.Field(fun value -> value.Title) |> ignore)

run ()
```

For durable storage, use files whose successful `Flush` really supplies your required persistence guarantee. SoloDB calls the supplied stream's `Flush`; it cannot turn a memory stream or an unchecked operating-system flush into a durable disk write.

Generated and NativeAOT applications still need their [normal metadata registration](generated-metadata.md). Borrow connections from the database instance you opened; its diagnostic connection string cannot recreate the supplied streams or files.

## Share a database between instances or processes

Without a coordination option, only connections borrowed from the same `SoloDB` instance can safely share your files. To open the same database from several instances or processes, use `SqliteStorageCoordination.File` with the same separate coordination path in every participant.

Each participant opens its own streams:

```csharp
using System.IO;
using SoloDatabase;

using var main = new FileStream("articles.main", FileMode.OpenOrCreate,
    FileAccess.ReadWrite, FileShare.ReadWrite, bufferSize: 1);
using var log = new FileStream("articles.wal", FileMode.OpenOrCreate,
    FileAccess.ReadWrite, FileShare.ReadWrite, bufferSize: 1);
var coordination = SqliteStorageCoordination.File("articles.coordination");
using var storage = SqliteStorage.Wal(main, log, true, coordination);
using var db = new SoloDB(storage);
```

This uses native operating-system locks and shared memory on one host. It does not make arbitrary remote storage safe to share, and the generic FileStream flush in this example does not prove power-loss durability.

While two database instances are open:

- Open a separate Stream or `ISqliteStorageFile` object for each instance. Do not share the same object or hide it behind different wrappers.
- Point every main stream at the same main bytes, and every log stream at the same log bytes.
- Ensure a successful read can see another participant's committed writes. Avoid independent buffering that returns stale bytes.
- Use the same coordination path. Do not open and close that file yourself, delete it or replace it while anyone is using the database.

SoloDB obtains the necessary locks before inspecting or initializing the files. You do not need your own initialization monitor or a registry of open databases.

For a specialist native filesystem, `SqliteStorageCoordination.File(filename, registeredVfs)` uses its existing registration instead of the ordinary operating-system filesystem. Keep that registration alive until all participating `storage.IsClosed` values are true.

## Close safely and handle errors

Dispose readers, streams selected from query results, transactions and borrowed connections before disposing their database. A normal `using` or `use` scope does this when these objects are declared after the database.

`db.Dispose()` stops new work but returns if a database operation or borrowed object still needs the files. Finish or dispose those objects, then check `storage.IsClosed`. Do not close or reuse the supplied files while it is false. Do not spin waiting for it on the thread that still owns an unfinished reader or transaction.

After completion, inspect `storage.ClosingFailure` for the first closing error. With `leaveOpen: false`, SoloDB closes the streams once all database use has finished. With `true`, you close them yourself. Your `ISqliteStorageFile` implementations always remain yours to close.

Use deterministic disposal. If you abandon a database or a reader, its files, callbacks and locks can remain allocated; garbage collection is not a promised fallback for these inputs.

For database errors, inspect `SqliteErrorCode` and `SqliteExtendedErrorCode` on `NativeSqliteException`, or on the ADO `SqliteException`. The original backing exception remains in the exception chain, including failures from either end of a backup. Use it to distinguish a full or read-only backend from another I/O failure. A failed write may have partially changed bytes; let SQLite roll back or recover rather than retrying that byte write yourself.

If another connection blocks a write with SQLite's BUSY error, finish the blocking reader or transaction. Keep transactions short rather than waiting on a reader that your own thread still holds.

The `Simple`, `Wal` and `ReadOnly` factories do not expose connection or reader `Handle` values and cannot load extensions. Those operations throw `NotSupportedException`. Use the Native factory if your application needs them. All `SqliteStorage` inputs require the bundled `e_sqlite3` provider; the ordinary string constructor retains its existing provider support.

## Implement your own byte files

Implement these five synchronous methods on `ISqliteStorageFile`:

```csharp
StorageReadResult ReadAt(long position, Span<byte> destination);
StorageFailure? WriteAt(long position, ReadOnlySpan<byte> source);
StorageFailure? Flush();
StorageLengthResult GetLength();
StorageFailure? SetLength(long length);
```

Return a null failure on success, or a `StorageFailure` with `IoError`, `Full` or `ReadOnly` and the original exception when available. A Stream implementation can throw `StorageIOException` to supply the same classification. Other exceptions become I/O errors; exception text does not determine the category.

Check the requested position and end, including on a read beyond EOF. A successful read can return fewer bytes than requested; zero means EOF. SoloDB repeats fragmented reads and fills the remaining destination with zeros only after successful EOF. Do not report a failed read as EOF.

A successful write fills its whole span. New gaps and extended ranges must read as zeros. Empty reads and writes at valid positions do not extend the file. Report the exact logical length; retained buffer capacity is not file content. After shrinking and growing a file, do not expose the discarded bytes again.

A failed write must preserve existing bytes outside its requested range, cannot shrink the file, and cannot extend it beyond the greater of the old length and the requested end. A failed resize must preserve the prefix shared by the old and requested lengths. SQLite relies on these guarantees for rollback.

SoloDB serializes calls for each supplied file object, but different calls can run on different threads. Keep spans inside the called method. In F#, do not capture a span in a `lock` closure. Do not call back into the database from these methods.

Make `Flush` preserve earlier byte and length writes to the level your backend promises. For durable files, make creation of the files and their parent directory durable too. Main and journal/log bytes all pass through your implementations; SQLite temporary files still use its ordinary native temporary-file path, so encrypting the supplied files does not encrypt those temporary files.

Writable factories also accept `coordination` and `sectorSize`. The latter describes the data backend's sector damage unit, meaning the amount of neighbouring data a device failure can damage together. Use a truthful power of two from 32 through 65536; the default is 4096. Do not take that value from the unrelated coordination file or assume SoloDB adds atomic-write guarantees.

Keep the supplied files available until `storage.IsClosed`. SoloDB clears an auxiliary journal or log by setting its length to zero and flushing it. It does not unlink your object or hide an old length behind an invalid header. Your objects are already bound to their database roles; do not treat them as a general create/delete filesystem.

## Import, export and read-only images

Use `source.BackupTo(destination)` with a fresh destination. Keep both databases open until it finishes. A managed WAL destination requires 4096 byte page geometry.

To convert a different page size, preserve the original and work on a disposable ordinary-file copy. Put that copy in rollback journal mode, set its page size to 4096 and run `VACUUM`, then back it up into the fresh WAL destination. Do not reformat the original in place.

Use `VacuumTo(path)` to export to an ordinary native file. Do not assume a transaction writing both your supplied database and an attached database commits atomically across the two. Some combinations need a super-journal, an extra file coordinating multiple rollback journals; these layouts do not supply that file, so those combinations are refused.

For `ReadOnly`, supply a complete supported SoloDB main image. Finish WAL checkpointing or export a complete image first, and leave its bytes unchanged while it is open. SoloDB retains its usual read functions and metadata, but does not migrate schema, add indexes or create missing collections.

## Native access and extension users

The Native factory borrows your registered SQLite filesystem. Keep that registration and its resources alive until `storage.IsClosed`.

Finish raw statements, blobs, backups and callback-dependent work before disposing their connection or database. Finish a raw backup destination before any SoloDB access to that destination, including disposal.

If outstanding raw objects make closing busy, `Dispose` records the error in `ClosingFailure` and keeps the connection and callback data alive. Finish those objects, call `Dispose` again on the database or storage input, and check `IsClosed`. A previous busy diagnostic can remain after a successful retry.

SQLite automatic extensions run whenever a connection opens. If your process installs them on this engine, use the expert Native path rather than the managed layouts; managed inputs require that no foreign automatic extension is installed.

## Measure your workload

Measure the complete operations and returned fields you need on your actual backend. A warm query can read entirely from SQLite's page cache and tell you nothing about stream or positional read costs. Check a read workload that exceeds that cache as well as writes and transaction completion.

Keep transactions short, reuse borrowed connections through the pool and retain compiled queries when translation matters. Include log growth and checkpoint delays when assessing long-lived WAL readers. Memory and RAM-backed files do not establish disk throughput or power-loss durability.
