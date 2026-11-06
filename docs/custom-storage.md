# Custom SQLite storage

Use `SqliteStorage` when you want to supply the database's bytes through streams or your own file implementation. You still use the usual collections, transactions, queries, full text search and ADO connections.

For an ordinary database file, keep using `new SoloDB(path)`.

## Choose what to supply

| Factory | What you keep | When to choose it |
| --- | --- | --- |
| `Simple(main, journal)` | Database and rollback journal | Readers normally finish before a writer commits. |
| `Wal(main, log)` | Database and write-ahead log | A reader needs to keep its snapshot while another connection commits. |
| `ReadOnly(main)` | One complete, unchanging database image | You only need to read an existing database. |

`Simple` uses SQLite's PERSIST journal mode. `Wal` creates databases with 4096 byte pages. Both allow readers and one writer. A long-running reader can delay a writer in Simple mode or delay log cleanup in WAL mode.

The factories accept either streams or `ISqliteStorageFile` objects. Each returns a `SqliteStorage` object, called `storage` in the examples. Several live `SoloDB` instances can use the same object. Dispose each database and dispose the storage even if you never open a database.

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

Sequential reuse is allowed after the previous `storage.IsClosed` is true, provided you left the streams open. You can open another database on the same storage while at least one database is still attached. Closing its last database retires that storage; create a new `SqliteStorage` object to reopen afterward.

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

For a writable `FileStream`, SoloDB drains its buffers and requests a disk flush through the normal .NET `FileStream.Flush(true)` API. The runtime supplies its platform implementation; exceptions it throws keep their original cause.

For other streams, including custom or encryption wrappers around a file, SoloDB calls the supplied stream's `Flush`. That implementation must provide the persistence guarantee you need; SoloDB does not unwrap it or replace its buffering or encryption. `MemoryStream` remains volatile. You must also make newly created file names and their parent directory durable before relying on recovery after power loss. A successful file synchronization call cannot establish that your filesystem or device honors its persistence contract.

Generated and NativeAOT applications still need their [normal metadata registration](generated-metadata.md). Borrow connections from the database instance you opened; its diagnostic connection string cannot recreate the supplied streams or files.

## Share a database between instances or processes

Several `SoloDB` instances using the same `SqliteStorage` share its database locks and can safely use its files without a coordination option. Separate storage objects using the same actual main Stream or `ISqliteStorageFile` object also share private database locks and WAL memory automatically. For independently opened views or separate processes accessing the same database bytes, use `SqliteStorageCoordination.File` with the same separate coordination path in every participant. Do not mix private and File coordination over one live shared object; finish its current users before changing coordination.

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

This uses the active SQLite engine's default filesystem for locks and shared memory on one host, and .NET's disk-flush API for the supplied FileStreams. It does not make arbitrary remote storage safe to share or establish your runtime, filesystem or device's power-loss behavior; file-name and directory durability still need your attention.

When another `SqliteStorage` object has not finished with the files (`IsClosed` is false):

- You may share the same Stream or `ISqliteStorageFile` object in one process; SoloDB serializes access to that actual object. Separate views must expose coherent bytes. If different wrappers share a cursor underneath, those wrappers must serialize their own seek/read/write operations together.
- Point every main stream at the same main bytes, and every log stream at the same log bytes.
- Ensure a successful read can see another participant's committed writes. Avoid independent buffering that returns stale bytes.
- Use the same coordination path. Do not open and close that file yourself, delete it or replace it while anyone is using the database.

SoloDB obtains the necessary locks before inspecting or initializing the files. You do not need your own initialization monitor or a registry of open databases.

For a specialist native filesystem, `SqliteStorageCoordination.File(filename, registeredVfs)` uses its existing registration instead of the ordinary operating-system filesystem. Keep that registration alive until all participating `storage.IsClosed` values are true.

## Close safely and handle errors

Dispose readers, streams selected from query results, transactions and borrowed connections before disposing their database. A normal `using` or `use` scope does this when these objects are declared after the database.

`db.Dispose()` stops new work on that database. Other databases attached to the same storage remain usable. Disposing the storage itself stops all its databases. Closing its last database retires the storage, but disposal returns if a database operation or borrowed object still needs the files. Finish or dispose those objects, then check `storage.IsClosed`. Do not close or reuse the supplied files while it is false. Do not spin waiting for it on the thread that still owns an unfinished reader or transaction.

After completion, inspect `storage.ClosingFailure` for the first closing error. With `leaveOpen: false`, SoloDB closes each stream after all storage objects using that actual stream have finished. With `true`, you close it yourself unless another participant transferred its ownership with `false`. A single storage reaching `IsClosed` does not let you close a stream still used by another storage. Your `ISqliteStorageFile` implementations always remain yours to close.

Use deterministic disposal. If you abandon a database or a reader, its files, callbacks and locks can remain allocated; garbage collection is not a promised fallback for these inputs.

For database errors, inspect `SqliteErrorCode` and `SqliteExtendedErrorCode` on `NativeSqliteException`, or on the ADO `SqliteException`. The original backing exception remains in the exception chain, including failures from either end of a backup. Use it to distinguish a full or read-only backend from another I/O failure. A failed write may have partially changed bytes; let SQLite roll back or recover rather than retrying that byte write yourself.

If another connection blocks a write with SQLite's BUSY error, finish the blocking reader or transaction. Keep transactions short rather than waiting on a reader that your own thread still holds.

Opening shared storage uses the connection's default timeout, normally 30 seconds, when waiting for initialization locks. Zero means no time limit; disposing the storage input stops its initialization retries. Do not use an unlimited wait when your own thread still holds the blocking database object.

A well-formed native filesystem name that is not registered causes `InvalidOperationException`. Register it before opening the database. Null or invalid factory arguments retain their argument exceptions.

Connection and reader `Handle` values and the usual extension APIs remain available. Finish every raw statement, blob, backup and callback-dependent operation before disposing its connection or database. Finish a raw backup destination before any other access to it. These raw objects do not acquire an additional SoloDB disposal or retry protocol.

Custom storage's direct native operations and the selected provider must use the same SQLite engine instance. The engine may be replaced in place; its name is not an encryption or capability policy. Missing native operations and errors retain their actual library/runtime behavior.

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

Writable factories also accept `coordination`. Custom storage reports a 4096-byte sector unit to SQLite; this is separate from the database page size. It does not inherit media or atomic-write guarantees from the coordination file.

Keep the supplied files available until `storage.IsClosed`. SoloDB clears an auxiliary journal or log by setting its length to zero and flushing it. It does not unlink your object or hide an old length behind an invalid header. Your objects are already bound to their database roles; do not treat them as a general create/delete filesystem.

## Import, export and read-only images

Use `source.BackupTo(destination)` with a fresh destination. Keep both databases open until it finishes. A managed WAL destination requires 4096 byte page geometry.

To convert a different page size, preserve the original and work on a disposable ordinary-file copy. Put that copy in rollback journal mode, set its page size to 4096 and run `VACUUM`, then back it up into the fresh WAL destination. Do not reformat the original in place.

Use `VacuumTo(path)` to export to an ordinary native file. Do not assume a transaction writing both your supplied database and an attached database commits atomically across the two. Some combinations need a super-journal, an extra file coordinating multiple rollback journals; these layouts do not supply that file, so those combinations are refused.

For `ReadOnly`, supply a complete supported SoloDB main image. Finish WAL checkpointing or export a complete image first, and leave its bytes unchanged while it is open. SoloDB retains its usual read functions and metadata, but does not migrate schema, add indexes or create missing collections.

Write-only administration, such as `Optimize`, `Vacuum`, index changes and search-index maintenance, throws `InvalidOperationException` on `ReadOnly` storage. An `EnsureIndex` or `EnsureSearchIndex` call that finds its existing index unchanged can succeed without writing. `DropIndexIfExists` and `DropSearchIndexIfExists` also succeed when their target is absent; they still refuse to remove an existing index. Raw write SQL instead keeps SQLite's READONLY error; it is not converted into an administration error.

## Measure your workload

Measure the complete operations and returned fields you need on your actual backend. A warm query can read entirely from SQLite's page cache and tell you nothing about stream or positional read costs. Check a read workload that exceeds that cache as well as writes and transaction completion.

Keep transactions short, reuse borrowed connections through the pool and retain compiled queries when translation matters. Include log growth and checkpoint delays when assessing long-lived WAL readers. Memory and RAM-backed files do not establish disk throughput or power-loss durability.
