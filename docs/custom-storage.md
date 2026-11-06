# Custom SQLite storage

`SoloDB` can use caller supplied byte files or streams through `SqliteStorage`. Collections, transactions, full text search, generated queries and the ADO connection use the same database engine and execution paths as ordinary file databases.

Choose the storage layout, its persistence guarantee and its sharing configuration before opening the database.

## Choose a layout

| Factory | Supplied files | Use |
| --- | --- | --- |
| `Simple(main, journal)` | Main image and rollback journal | SQLite PERSIST journaling with one writer and conventional reader locks. |
| `Wal(main, log)` | Main image and WAL | Readers can retain snapshots while another connection commits. New images use 4096 byte pages. |
| `ReadOnly(main)` | One complete immutable main image | Read an existing SoloDB database without schema initialization or migration. |
| `Native(filename, registeredVfs, profile)` | A filename in an existing native VFS | Expert access to that VFS, including native handles and extensions. |

The first three factories accept either `ISqliteStorageFile` objects or `Stream` objects. Stream overloads require `leaveOpen`. Positional files and native VFS registrations are borrowed. A factory result belongs to one `SoloDB` instance and can be attached only once. Dispose an unused result too.

The new constructor accepts `SqliteStorage`; the existing constructor still accepts a string source. An F# helper with an unconstrained constructor argument now needs its intended type, for example `let openDatabase (source: string) = new SoloDB(source)`.

Keep each writable main, journal and log as a distinct logical file. Their contents must survive reopening together. Replacing a journal or WAL with an empty file can discard recovery data.

An existing managed WAL image must already have 4096 byte pages and WAL format headers. Simple storage requires a rollback journal image. Use an explicit backup conversion for a different layout; opening an incompatible image does not reformat it.

## Open streams

This complete C# example uses memory only. Its `Flush` has no persistence guarantee.

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

The Stream adapter serializes seeking and I/O for each supplied view. It handles fragmented reads, initializes gaps and extended ranges with zeros, and calls the stream's `Flush`. A stream must support reading and seeking, plus writing for a writable layout. The adapter cannot manufacture a durable storage barrier that the stream does not provide.

Generated and NativeAOT callers keep their [normal metadata registration](generated-metadata.md). Choosing storage does not replace that setup. A custom input also cannot be reconstructed from its diagnostic connection string; borrow connections from its existing database instance.

## Share a store between processes

Without a coordination option, a description is private to its own pooled connections. Independent descriptions and processes sharing bytes must use `SqliteStorageCoordination.File` with the same dedicated coordination identity.

For ordinary filesystem coordination, each process can open this layout independently:

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

This example supplies coherent byte views and native locks on one host. Its generic FileStream flush does not establish a physical power loss guarantee. Use a backend with a checked durable barrier when that guarantee is needed.

Every description needs independent coherent views. Do not reuse the same actual Stream or positional backend object between descriptions, even if a wrapper appears to make it safe. Do not use independent buffering that can hide a peer's committed bytes. Main and auxiliary views must refer to the same store in every process.

The coordination file is separate from the data files. Keep its identity stable: do not independently open and close it, unlink it or replace it while any participant remains. Each physical SQLite connection gets its own native lock and shared memory session. This coordinates readers, writers and WAL shared memory across processes; a managed monitor is not its authority.

`SqliteStorageCoordination.File(filename, registeredVfs)` borrows a named native coordinator instead. Its filename belongs to that VFS's namespace. Keep the registration alive until every participating storage input is closed.

Initialization takes a shared admission lock before inspecting an image. An empty WAL pair requires exclusive admission and a second inspection before seeding. Existing initialized stores do not take an exclusive initialization lock on every open.

## Implement positional files

`ISqliteStorageFile` has five synchronous operations:

```csharp
StorageReadResult ReadAt(long position, Span<byte> destination);
StorageFailure? WriteAt(long position, ReadOnlySpan<byte> source);
StorageFailure? Flush();
StorageLengthResult GetLength();
StorageFailure? SetLength(long length);
```

Return a null failure on success. Reads may return fewer bytes than requested; zero means EOF. SoloDB repeats successful fragmented reads and zero fills the unread tail only after successful EOF. A failed read is not EOF.

Validate positions and requested ends before returning EOF. Empty operations at a valid position succeed without extending the file. Writes fill the whole supplied span, and newly visible gaps contain zeros. `SetLength` reports the exact logical length, discards the truncated tail and initializes later extension with zeros. Retained buffer capacity must not change the reported length.

A failed write may change part of its requested range, but must preserve bytes outside that range, never shrink the file and never extend beyond the requested end or its previous length. A failed resize preserves the prefix that both lengths retain. These conditions let SQLite perform its own rollback and recovery.

Calls for one supplied view are serialized but may move between threads. Keep spans inside their method; an F# implementation must not capture them in a `lock` closure. Backend methods must not reenter the database. The caller owns positional resources and closes them only after the storage input is physically closed.

`Flush` is the backend's ordered barrier for bytes and length. For durable files, establish durable creation of the data roles and their parent namespace as well. A successful managed flush alone does not prove that an operating system reported every native sync error. An encrypted backend receives the supplied main and auxiliary bytes; SQLite temporary files still use the ordinary native temporary-file path.

Writable factories also accept `coordination` and `sectorSize`. The sector size describes the data backend's failure geometry, not the coordination file's disk. Its default is 4096; configure a truthful power of two from 32 through 65536. The adapter does not claim atomic writes or powersafe overwrite merely because the native coordinator provides them.

## Handle failures and disposal

Positional operations return `StorageFailure` with `IoError`, `Full` or `ReadOnly` and an optional original exception. A Stream can throw `StorageIOException` to supply the same classification. Other backend exceptions become I/O failures; their text is not used to guess a category.

SQLite failures retain primary and extended result codes. The original backing cause stays in the exception chain, including failures from either backup endpoint. Cleanup preserves the primary operation failure. Failed writes are not replayed by the adapter.

Dispose readers, selected streams, transactions and borrowed connections deterministically. Closing the database rejects new work and returns without waiting for an outstanding tracked user. The last completing user performs the actual native close. A root's `Dispose` returning therefore does not mean its files are quiet.

`storage.IsClosed` reports physical completion. `storage.ClosingFailure` retains the first close diagnostic. Check these after completing all users and before closing borrowed files. With `leaveOpen: false`, owned streams close after physical completion. Forgotten disposal can retain the whole callback and backing graph; these layouts do not promise automatic reclamation of abandoned cycles.

Managed storage does not expose connection or reader `Handle` values. Those getters throw `NotSupportedException`, as do extension enabling and loading. SQL extension loading is disabled too. Processes using managed storage must not install foreign auto-extensions on the SQLite engine. Internal SQL, typed functions, aggregates, collations, blobs and generated queries still use their normal owners.

Choose borrowed Native storage when raw access is needed. Finish every raw statement, blob, backup and callback-dependent operation before disposing its connection or root. A raw backup destination must finish before any library access to that destination, including a close probe.

If foreign raw objects nevertheless leave Native close busy, `Dispose` stays quiet and retains the entire physical owner, its PCL callback state and borrowed registration. Finish those raw objects, call `Dispose` again on the root or input, then check `IsClosed`. Keep the registration and backing alive through this completion. A previous busy diagnostic can remain in `ClosingFailure` after a successful retry.

## Import and export

Use `source.BackupTo(destination)` with a fresh destination. Both native endpoints remain owned through backup completion. A managed WAL destination requires matching 4096 byte page geometry. To import a different page size, first make a disposable ordinary-file rollback copy, change its page size and run `VACUUM` on that copy, then back up the converted copy. Preserve the original source throughout conversion.

`VacuumTo(path)` can export managed storage to an ordinary native file. Ordinary parent opens, including SQLite's ATTACH output for this operation, retain their native namespace semantics. Arbitrary additional managed main images and super-journals are not provided by the fixed roles. Native per-database commit rules still apply to supported attachments; rejecting a super-journal is not a blanket prohibition or an atomicity guarantee for every multiple-database write.

Managed immutable storage requires a complete current SoloDB image. Opening it retains required read functions and metadata but does not initialize schema, migrate versions, add indexes or create a missing collection. Finish checkpointing or export a complete main image before selecting this layout, and keep that image unchanged for its lifetime.

## Understand the native interface

The adapter supplies the complete SQLite version 3 file and VFS tables. Byte reads, writes, sync and exact truncate reach the supplied files. Lock, unlock, reserved-lock checks, WAL mapping, shared-memory locks, barriers and unmap use either the private description or real native coordination sessions. Fetch returns no native mapping and unfetch accepts that fallback.

File controls describe the data backend truthfully. Mapping size is zero; stable role identity does not move. Advisory controls do not silently resize files, and unsupported controls return SQLite's `NOTFOUND` fallback. Ordinary temporary files and native utilities are forwarded to their responsible parent VFS. Fixed roles reject destructive delete-on-close and exclusive creation, including when empty; deleting an auxiliary role means exact length zero followed by its barrier.

The namespace uses permanent role bindings. Content-based `EXISTS` can report an empty role absent while a nondestructive open still succeeds. Read and write access answers describe permissions, not length. Do not interpret this as a POSIX create/unlink namespace for the supplied objects.

Measure complete operations on the intended backend. Native calls, managed transitions, stream cursor work, legacy array copies, gap initialization and synchronization have different costs. WAL readers can delay checkpoints and grow the log. Keep transactions short, reuse the pool and retain compiled queries when repeated translation matters; an in-memory result is not a durability or production throughput measurement.
