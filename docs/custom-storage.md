# Custom SQLite storage

Use `SqliteStorage` when you want to supply the database's bytes through streams or your own file implementation. You still use the usual collections, transactions, queries, full text search and ADO connections.

For an ordinary database file, keep using `new SoloDB(path)`.

## Choose what to supply

| Factory | What you keep | When to choose it |
| --- | --- | --- |
| `Simple(main, journal, coordination)` | Database and rollback journal | Readers normally finish before a writer commits. |
| `Wal(main, log, coordination)` | Database and write-ahead log | A reader needs to keep its snapshot while another connection commits. |
| `ReadOnly(main)` | One complete, unchanging database image | You only need to read an existing database. |

`Simple` selects SQLite's PERSIST mode when a connection opens. `Wal` selects WAL; SQLite creates the image with its native default page size (4096 bytes in the bundled engine). Existing images keep their page size. Both allow readers and one writer. A long-running reader can delay a rollback-mode writer or WAL cleanup.

The factories accept either streams or `ISqliteStorageFile` objects. Stream overloads also require `leaveOpen` before `coordination`. Writable factories require a nonnull `ISqliteStorageCoordination`: choose `SqliteStorageCoordination.SingleProcessInMemory`, `SqliteStorageCoordination.File(...)`, or your own implementation. Each factory returns a `SqliteStorage` object, called `storage` in the examples. Several live `SoloDB` instances can use the same object. Dispose each database and dispose the storage even if you never open a database.

Keep the main file and its journal or log together across reopenings. Give each one separate storage. Do not replace a journal or log with an empty file: SQLite may need it to recover committed work.

Keep the matching auxiliary bytes when changing between Simple and WAL. SQLite recovers the supplied journal or log before changing the database header; opening an image does not reformat its page size. A nonempty WAL without its main image is refused. You can change journal mode or synchronization through normal SQLite PRAGMAs on a borrowed connection. The factory selects a connection default, rather than vetoing your choices; SQLite's transaction, shared-memory and recovery rules still apply. See [SQLite's journal-mode documentation](https://sqlite.org/pragma.html#pragma_journal_mode) and the [import options below](#import-export-and-read-only-images).

The string and `SqliteStorage` constructors remain distinct. An F# helper with an untyped constructor argument needs its intended type, for example `let openDatabase (source: string) = new SoloDB(source)`. Give a null argument its intended type too.

## Open and reopen streams

Streams must support reading and seeking, plus writing for Simple or WAL. Pass `leaveOpen: true` if you will close them yourself, or `false` to let SoloDB close them after the database has finished using them.

This complete C# example uses memory. Its contents disappear when the process ends.

```csharp
using System.IO;
using SoloDatabase;

using var main = new MemoryStream();
using var journal = new MemoryStream();
using var storage = SqliteStorage.Simple(main, journal, leaveOpen: true,
    coordination: SqliteStorageCoordination.SingleProcessInMemory);
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

using var next = SqliteStorage.Simple(main, journal, leaveOpen: true,
    coordination: SqliteStorageCoordination.SingleProcessInMemory);
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
    use storage = SqliteStorage.Wal(main, log, true, SqliteStorageCoordination.SingleProcessInMemory)
    use db = new SoloDB(storage)
    let rows = db.GetCollection<Article>()
    rows.Insert({ Id = 1L; Title = "alpha" }) |> ignore
    rows.EnsureSearchIndex(fun builder ->
        builder.Field(fun value -> value.Title) |> ignore)

run ()
```

For a writable `FileStream`, SoloDB drains its buffers and requests a disk flush through the normal .NET `FileStream.Flush(true)` API. The netstandard2.1 and net6.0 library assets also do this through any number of `BufferedStream` layers: the supplied stream's `Flush` drains the chain before the underlying file receives the disk-flush request. The runtime supplies its platform implementation; exceptions it throws keep their original cause.

For other streams, including custom or encryption wrappers around a file, SoloDB calls the supplied stream's `Flush`. That implementation must provide the persistence guarantee you need. The netstandard2.0 asset also treats `BufferedStream` this way because that target does not expose its underlying stream; direct `FileStream` inputs still receive `Flush(true)`. A positional-file implementation can use its retained backing resource to supply a stronger barrier. SoloDB does not discover arbitrary wrapper internals or replace buffering or encryption. `MemoryStream` remains volatile. You must also make newly created file names and their parent directory durable before relying on recovery after power loss. A successful file synchronization call cannot establish that your filesystem or device honors its persistence contract. Flushing buffered writes does not make independently buffered readers coherent.

Generated and NativeAOT applications still need their [normal metadata registration](generated-metadata.md). Borrow connections from the database instance you opened; its diagnostic connection string cannot recreate the supplied streams or files.

## Share a database between instances or processes

`SingleProcessInMemory` shares private database locks and WAL memory by the actual main Stream or `ISqliteStorageFile` object. It is reusable across unrelated databases without joining their locks or WAL regions. Several roots using one storage, or separate storage objects using the same actual main object, share that domain. Independently opened wrappers of the same virtual path do not acquire a shared domain automatically. This implementation supplies no multiprocess correctness guarantee.

For independently opened views or separate processes accessing the same database bytes, use suitable coordination and coherent byte views. `SqliteStorageCoordination.File` supplies native same-host coordination with the same separate path in every participant; a caller implementation can supply another policy. Every live participant on the same actual backing must report a compatible coordination-domain identity. Finish its current users before changing that choice.

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
- With File coordination, use the same coordination path. Do not open and close that file yourself, delete it or replace it while anyone is using the database.

SoloDB obtains the necessary locks before inspecting or initializing the files. You do not need your own initialization monitor or a registry of open databases.

For a specialist native filesystem, `SqliteStorageCoordination.File(filename, registeredVfs)` uses its existing registration instead of the ordinary operating-system filesystem. Keep that registration alive until all participating `storage.IsClosed` values are true.

## Keep a database in another SoloDB filesystem

Open the parent files once and keep the child open across updates. The parent's ordinary file streams write affected chunks through their normal transactions and callbacks; a child update does not require reopening the child or placing its work inside a parent user transaction.

```csharp
using var main = tenant.FileSystem.OpenOrCreateAt("/articles.main");
using var log = tenant.FileSystem.OpenOrCreateAt("/articles.wal");
using var storage = SqliteStorage.Wal(main, log, true,
    SqliteStorageCoordination.SingleProcessInMemory);
using var child = new SoloDB(storage);
var articles = child.GetCollection<Article>();
articles.Insert(new Article { Id = 1, Title = "alpha" });
articles.Insert(new Article { Id = 2, Title = "beta" });
var title = articles.GetById(2L).Title;
```

Here `tenant` is an open parent `SoloDB` and `Article` is the model from the first example. Keep the parent alive until the child storage has physically closed. This choice needs no coordination file and supplies no multiprocess guarantee. Separate child views still need coherent bytes and matching suitable coordination if you share them.

## Implement coordination

`ISqliteStorageCoordination.Open(main, profile)` returns a distinct `ISqliteStorageCoordinationSession` for each physical main-file open. The provider is borrowed, and its Open calls may run concurrently. A session owns only that open's locks and mappings, and SoloDB calls its `Close` after native completion or failed opening. Closing one root or the factory description must not release another live session's grants.

Implement these session members:

```csharp
object DomainIdentity { get; }
bool HasSharedMemory { get; }
int Lock(int level);
int Unlock(int level);
StorageReservedLockResult CheckReservedLock();
StorageSharedMemoryResult MapSharedMemory(int region, int size, int extend);
int LockSharedMemory(int offset, int count, int flags);
void SharedMemoryBarrier();
int UnmapSharedMemory(int delete);
int FileControl(int operation, IntPtr argument);
int Close();
```

Return SQLite result codes, including BUSY for contention and extended codes for actual errors. Reserved-lock and map results carry `ResultCode` plus `IsReserved` or `Address`. `DomainIdentity` is a stable nonnull equality value identifying compatible sessions for one database; it is separate from byte-file identity. Do not join unrelated databases merely because they use one provider. `HasSharedMemory` must describe the session's actual capability; SQLite declines WAL when it is false.

Pager levels are NONE=0, SHARED=1, RESERVED=2, PENDING=3 and EXCLUSIVE=4. Unlock targets are NONE or SHARED. Preserve partial upgrades: a failed EXCLUSIVE upgrade can retain PENDING while admitted readers finish. WAL slot flags are UNLOCK=1, LOCK=2, SHARED=4 and EXCLUSIVE=8. Every nonzero map `extend` permits extension, including recovery value 2. Keep mapped addresses stable until their matching unmap or close; release shared regions only after the final mapping ends. Follow [SQLite's I/O method contract](https://sqlite.org/c3ref/io_methods.html).

`FileControl` receives only coordination controls: LOCKSTATE=1 and optional operations 15, 16, 24, 34, 40 and 44. Return NOTFOUND=12 for unsupported controls. Its pointer is borrowed and uses the operation's native SQLite argument layout. SoloDB owns byte, media, size and identity controls. Do not retain pointers supplied only for a call or reenter the database. Methods can run on different threads. A thrown exception keeps its original cause; a barrier exception is recorded on the physical connection's fault channel because a void native callback cannot return a code.

For example, this complete decorator counts physical opens while supplying its own provider implementation. It keeps the delegated session's ownership and capability intact:

```csharp
sealed class CountingCoordination : ISqliteStorageCoordination
{
    private int opens;
    public int Opens => System.Threading.Volatile.Read(ref opens);
    public ISqliteStorageCoordinationSession Open(ISqliteStorageFile main, SqliteStorageProfile profile)
    {
        System.Threading.Interlocked.Increment(ref opens);
        return SqliteStorageCoordination.SingleProcessInMemory.Open(main, profile);
    }
}
```

Supply it through the interface just like a provider with its own locks and WAL mappings:

```csharp
ISqliteStorageCoordination coordination = new CountingCoordination();
using var storage = SqliteStorage.Simple(mainFile, journalFile, coordination);
using var db = new SoloDB(storage);
```

Here `mainFile` and `journalFile` implement `ISqliteStorageFile`. The decorator retains SingleProcessInMemory's scope. Implementing a provider alone does not make incoherent backing bytes safe to share.

## Test your coordination implementation

Use `SqliteStorageDiagnostics.TestCoordination` explicitly while developing a provider. Supply a provider configured for an isolated test database and two `ISqliteStorageFile` views of the same logical main backing. The views can be independent objects when your provider supports that arrangement. For `SingleProcessInMemory`, pass the same actual main file object twice.

```csharp
using System;
using SoloDatabase;

static void TestMyLocks(
    ISqliteStorageCoordination coordination,
    ISqliteStorageFile firstMain,
    ISqliteStorageFile secondMain)
{
    var report = SqliteStorageDiagnostics.TestCoordination(
        coordination, firstMain, secondMain, SqliteStorageProfile.Wal);

    foreach (var failure in report.Failures)
    {
        Console.WriteLine(failure.Message);
        if (failure.Exception is Exception cause)
            Console.WriteLine(cause);
    }

    report.ThrowIfFailed();
}
```

Call this helper with your provider and test views. `SqliteStorageCoordination.File(testCoordinationPath)` uses a dedicated test coordination path. To check the built-in memory implementation, call:

```csharp
var report = SqliteStorageDiagnostics.TestCoordination(
    SqliteStorageCoordination.SingleProcessInMemory,
    testFile, testFile, SqliteStorageProfile.Wal);
report.ThrowIfFailed();
```

Here `testFile` is your `ISqliteStorageFile` for the isolated test backing. No stream adapter is needed. In F# the same call is:

```fsharp
let report = SqliteStorageDiagnostics.TestCoordination(
    coordination, firstMain, secondMain, SqliteStorageProfile.Wal)
report.ThrowIfFailed()
```

`Simple` checks pager readers/writers, reservations, pending upgrades, downgrade/release and session-close isolation. `Wal` adds shared-memory capability, region growth and shared bytes, slot conflicts and peer mapping lifetime. Use `Simple` for a rollback-only provider. `ReadOnly` has no applicable diagnostic profile. Every unexpected result stops dependent operations; cleanup still attempts every distinct session it acquired. The checker releases its own WAL grants before unmapping or closing, following the native SQLite calling order.

The immutable report exposes `Passed` and `Failures`. Each failure has a `Message`, nullable `ExpectedResultCode` and `ActualResultCode`, and its original `Exception` when available. An absent code means the failure concerns a value, capability or exception rather than a returned result code. `ThrowIfFailed` does nothing on success and throws `AggregateException` on failure, retaining original backend exceptions; result-only mismatches become descriptive `InvalidOperationException` entries. Argument errors are rejected before any provider session opens.

The checker borrows your provider and byte files, opens its own sessions and closes them once. You own file disposal and test-backend cleanup. It does not read or write the main byte files directly, but providers can use them during Open. WAL checks overwrite mapped test bytes, so no database may be using that test domain. Provider methods must return normally; the synchronous diagnostic cannot interrupt blocked caller code.

Passing describes the exercised sequential session operations in one process. It does not establish cross-process exclusion, independent byte-view coherence, crash recovery, durability or correctness for every thread interleaving. Memory barriers are invoked, but a successful observed read is not proof of a barrier implementation on every architecture. Normal database opening does not run these diagnostics.

## Close safely and handle errors

Dispose readers, streams selected from query results, transactions and borrowed connections before disposing their database. A normal `using` or `use` scope does this when these objects are declared after the database.

`db.Dispose()` stops new work on that database. Other databases attached to the same storage remain usable. Disposing the storage itself stops all its databases. Closing its last database retires the storage, but disposal returns if a database operation or borrowed object still needs the files. Finish or dispose those objects, then check `storage.IsClosed`. Do not close or reuse the supplied files while it is false. Do not spin waiting for it on the thread that still owns an unfinished reader or transaction.

After completion, inspect `storage.ClosingFailure` for the first closing error. If opening fails, the thrown exception keeps the opening error. An additional Unlock or Close failure appears in `ClosingFailure`; further cleanup errors are attached to its `Data["SoloDB.OpenCleanupException"]`. With `leaveOpen: false`, SoloDB closes each stream after all storage objects using that actual stream have finished. With `true`, you close it yourself unless another participant transferred its ownership with `false`. A single storage reaching `IsClosed` does not let you close a stream still used by another storage. Your `ISqliteStorageFile` implementations always remain yours to close.

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

Writable factories require `coordination`. Custom storage reports a 4096-byte sector unit to SQLite; this is separate from the database page size. It does not inherit media or atomic-write guarantees from coordination.

Keep the supplied files available until `storage.IsClosed`. The auxiliary file holds the current rollback journal or WAL, as selected by SQLite; mode changes reuse that file after the previous role has finished. A native request to delete a supplied file clears its length and flushes it, including delete-on-close. It does not unlink your object or hide an old length behind an invalid header. Exclusive creation still fails because these objects are already supplied; they are not a general create/delete filesystem.

## Import, export and read-only images

Use `source.BackupTo(destination)` with a fresh destination. Keep both databases open until it finishes. SQLite cannot change a WAL destination's page size during backup: its page size must match the source. New WAL storage uses the engine default (4096 bytes in the bundled engine); an existing coherent image can use another native page size.

For a source with another page size, you can choose a rollback-mode destination instead. If you need a fresh 4096-byte WAL destination, preserve the original and work on an ordinary-file copy: put the copy in rollback journal mode, set its page size to 4096 and run `VACUUM`, then back it up. This is an import option, not a restriction on directly opening existing images.

Use `VacuumTo(path)` to export to an ordinary native file. Do not assume a transaction writing both your supplied database and an attached database commits atomically across the two. Some combinations need a super-journal, an extra file coordinating multiple rollback journals; these layouts do not supply that file, so those combinations are refused.

For `ReadOnly`, supply a complete supported SoloDB main image. Finish WAL checkpointing or export a complete image first, and leave its bytes unchanged while it is open. SoloDB retains its usual read functions and metadata, but does not migrate schema, add indexes or create missing collections.

Write-only administration, such as `Optimize`, `Vacuum`, index changes and search-index maintenance, throws `InvalidOperationException` on `ReadOnly` storage. An `EnsureIndex` or `EnsureSearchIndex` call that finds its existing index unchanged can succeed without writing. `DropCollectionIfExists`, `DropIndexIfExists` and `DropSearchIndexIfExists` also succeed when their target is absent; they still refuse to remove an existing target. Raw write SQL instead keeps SQLite's READONLY error; it is not converted into an administration error.

## Measure your workload

Measure the complete operations and returned fields you need on your actual backend. A warm query can read entirely from SQLite's page cache and tell you nothing about stream or positional read costs. Check a read workload that exceeds that cache as well as writes and transaction completion.

Keep transactions short, reuse borrowed connections through the pool and retain compiled queries when translation matters. Include log growth and checkpoint delays when assessing long-lived WAL readers. Memory and RAM-backed files do not establish disk throughput or power-loss durability.
