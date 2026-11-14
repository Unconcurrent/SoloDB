# Changelog

Release history in this file starts with **1.4.2**. Examples describe the public API for that version. C# fragments use the imports from the first example and reuse models only where stated. When combining blocks in a top-level program, place type declarations after executable statements. The examples use modern C# and .NET; the library itself continues to provide .NET Standard 2.0, .NET Standard 2.1 and .NET 6 assets.

## 1.4.2

This version adds collection full-text search, databases backed by caller-supplied storage, and recorded collection model descriptions. It also expands query translation and improves generated operations, storage lifetime handling and error reporting.

- [Full-text search](#full-text-search)
- [Caller-supplied database storage](#caller-supplied-database-storage)
- [Recorded collection model descriptions](#recorded-collection-model-descriptions)
- [Query improvements](#more-query-expressions-run-inside-sqlite)
- [C#, F# and generated operations](#c-f-and-generated-operations)
- [Other fixes](#other-correctness-and-runtime-improvements)
- [Upgrade notes](#upgrade-and-operating-notes)

### Full-text search

Declare the stored string fields to search. SoloDB builds the index from existing documents and maintains it as documents are inserted, updated or deleted.

The following setup is used by the subsequent C# search examples:

```csharp
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using SoloDatabase;

using var db = new SoloDB("articles.db");
var articles = db.GetCollection<Article>("articles");

articles.EnsureIndex(article => article.Tenant);
articles.EnsureSearchIndex("text", 1, builder => {
    builder.Field(article => article.Title, 4.0, "title")
           .Field(article => article.Body, 1.0, "body");
});

articles.Insert(new Article {
    Title = "SQLite pages",
    Body = "How a database stores pages and recovers transactions.",
    Tenant = 3
});

public sealed class Article
{
    public long Id { get; set; }
    public string Title { get; set; } = "";
    public string Body { get; set; } = "";
    public int Tenant { get; set; }
}
```

The title has a larger search weight than the body. Search fields must be fixed stored string-member paths. Store computed searchable text in a string field rather than placing a computation in the field declaration.

**Upgrade requirement:** normal writable initialization runs the schema migration, including installation of the search catalog, before application features use the database. For 1.4.2, the completed schema is version 7. Stop older processes and upgrade every participating application before opening the database with the new version: SoloDB 1.4.1 and earlier refuse a schema-7 database. `EnsureSearchIndex` declares an index; it does not perform a deferred global schema migration.

#### Ranked results, filters, text and snippets

Search returns the original document as `Item` and its search `Score`. It composes with ordinary LINQ filters and paging:

```csharp
var matches = articles.SearchIn("text", SearchQuery.TypeAhead("sqlite pag"))
    .Where(hit => hit.Item.Tenant == 3)
    .Take(10)
    .Select(hit => new {
        hit.Item.Id,
        hit.Score,
        Title = hit.Text(article => article.Title),
        Preview = hit.Snippet(article => article.Body, "[", "]", 24)
    })
    .ToArray();
```

A single search defaults to native rank order, then document ID. `Score` is the negation of the configured native BM25 rank. An explicit `OrderBy` replaces the default ordering; add a `ThenBy` when you need a particular tie-breaker.

`Text` and `Snippet` belong inside the database projection. Select those strings before calling `ToArray` or `ToList`; calling these methods on an already materialized hit throws `InvalidOperationException`.

#### Literal, phrase, prefix and native query modes

```csharp
var all = articles.SearchIn("text", SearchQuery.AllWords("sqlite pages"));
var chunks = articles.SearchIn("text", SearchQuery.LiteralChunks("sqlite pages"));
var phrase = articles.SearchIn("text", SearchQuery.Phrase("database pages"));
var prefix = articles.SearchIn("text", SearchQuery.Prefix("database pag"));
var completion = articles.SearchIn("text", SearchQuery.TypeAhead("sqlite pag"));
var syntax = articles.SearchIn("text",
    SearchQuery.Fts5Syntax("title : sqlite AND body : page*"));
```

`Search("sqlite pages")` is shorthand for an all-words search on the index named `default`; `SearchIn` chooses a named index. Literal modes escape their input rather than interpreting Boolean operators. `Phrase` searches one quoted phrase; `Prefix` prefixes the final token of a phrase; `TypeAhead` requires the chunks and prefixes the last one. Empty literal searches return no hits. `AllWords` and `TypeAhead` accept NUL as a chunk separator; the other modes reject it. `Fts5Syntax` deliberately accepts SQLite's search grammar and its syntax errors.

#### Shared or copied search text

`Shared`, the default, reads searchable text from the documents. `Copied` also keeps text in the search index, which can avoid document reads for text-only output but adds storage and text-maintenance work.

```csharp
articles.EnsureSearchIndex("completion", 1, builder => {
    builder.Storage = SearchTextStorage.Copied;
    builder.Tokenizer = "unicode61 remove_diacritics 0";
    builder.PrefixLengths = new[] { 2, 3 };
    builder.Field(article => article.Title);
});
```

Both modes maintain search changes in the same transaction as the documents. Neither mode promises universally faster writes.

#### Retained search queries

Compile once and supply fresh search text and limits on each invocation:

```csharp
var find = articles.Compile(
    (IQueryable<Article> source, string text, int limit) =>
        articles.SearchIn("text", text)
            .Take(limit)
            .Select(hit => new {
                hit.Item.Id,
                Preview = hit.Snippet(article => article.Body, "[", "]", 24)
            }));

var first = find("sqlite", 10).ToArray();
var second = find("recovery", 20).ToArray();
```

An ordinary search resolves the current index declaration when executed. A compiled search retains the declaration selected during compilation. Recompile after replacing or dropping that index; rebuilding the same index preserves its declaration identity. All search sources in a compiled query must belong to the same database.

Search also supports set composition while retaining the selected hit's search context:

```csharp
var combined = articles.SearchIn("text", "sqlite")
    .Union(articles.SearchIn("text", "recovery"))
    .Take(20)
    .Select(hit => new {
        hit.Item.Id,
        Preview = hit.Snippet(article => article.Body, "[", "]", 16)
    })
    .ToArray();
```

Set equality uses the visible `Item` and `Score`. `Union` and `Distinct` retain the first surviving occurrence, including the search context used by its later text/snippet projection. Every possible source must declare the field requested by that projection.

#### Versioned declarations and maintenance

```csharp
var indexes = articles.ListSearchIndexes();
var report = articles.VerifySearchIndex("text");

foreach (var finding in report.Findings)
    Console.WriteLine($"{finding.Kind}: {finding.ObjectName}: {finding.Description}");

// Repair owned search structures from the documents when needed.
if (report.Findings.Any(f => f.Kind != SearchVerificationKind.UnownedArtifact))
    articles.RebuildSearchIndex("text");

articles.OptimizeSearchIndex("text");
bool removed = articles.DropSearchIndexIfExists("completion");
```

Repeating an identical declaration at the same version is a read-only check. Changing that definition requires a larger version:

```csharp
articles.EnsureSearchIndex("text", 2, builder => {
    builder.Field(article => article.Title, 8.0, "title")
           .Field(article => article.Body, 1.0, "body");
});
```

A larger version with unchanged semantics keeps the existing index. A changed definition is built and backfilled before the old one is retired, in one transaction. Lower versions and changed definitions at the same version are refused.

`VerifySearchIndex` requires writable access and holds a write transaction while inspecting the indexed data. Its work grows with that data. `UnownedArtifact` findings describe objects that are not owned by a known declaration; rebuilding does not remove those objects. Inspect their reported names and data before choosing any manual removal. Dropping a collection removes its owned search indexes.

See the [full-text search guide](docs/full-text-search.md) for lifecycle details and paging costs.

### Caller-supplied database storage

`SqliteStorage` lets a normal `SoloDB` use streams or an implementation of `ISqliteStorageFile` for its database bytes. Collections, transactions, search and borrowed connections continue to use the normal APIs.

The storage examples reuse the `Article` model and C# imports from the search setup above. Each storage example has its own disposal scope.

| Factory | Supplied bytes | Use |
| --- | --- | --- |
| `Simple` | Main image and rollback journal | Rollback journaling; selects PERSIST mode on opening. |
| `Wal` | Main image and write-ahead log | WAL snapshots while another connection commits. |
| `ReadOnly` | One complete immutable main image | Read an existing supported image without writes or migration. |

#### Explicit in-memory coordination

Writable factories require an `ISqliteStorageCoordination`. Select the built-in memory implementation explicitly:

```csharp
using var main = new MemoryStream();
using var log = new MemoryStream();
using var storage = SqliteStorage.Wal(
    main, log, leaveOpen: true,
    coordination: SqliteStorageCoordination.SingleProcessInMemory);

using var writer = new SoloDB(storage);
using var reader = new SoloDB(storage);

writer.GetCollection<Article>().Insert(new Article { Title = "alpha" });
var saved = reader.GetCollection<Article>().ToArray();
```

For rollback journaling, replace the factory with:

```csharp
using var storage = SqliteStorage.Simple(
    main, journal, leaveOpen: true,
    coordination: SqliteStorageCoordination.SingleProcessInMemory);
```

Here `journal` is a separate readable, writable, seekable stream, just as `log` is in the WAL example. Keep both the main and auxiliary bytes across reopenings: SQLite may need the journal or log for recovery.

`SingleProcessInMemory` shares locks and WAL memory by the actual supplied main object. It supplies no multiprocess correctness guarantee. Independently opened wrappers of the same path do not automatically share its memory coordination. The library does not require a persistent coordination file or a named operating-system mutex when you select this implementation.

Streams must be readable and seekable, and writable for `Simple` or `Wal`. `MemoryStream` storage is volatile. `leaveOpen: true` leaves disposal of the streams with the application.

#### A database inside another SoloDB filesystem

Keep the parent streams and child database open while performing updates:

```csharp
using var tenant = new SoloDB("tenant.db");
using var main = tenant.FileSystem.OpenOrCreateAt("/articles.main");
using var log = tenant.FileSystem.OpenOrCreateAt("/articles.wal");
using var storage = SqliteStorage.Wal(
    main, log, leaveOpen: true,
    coordination: SqliteStorageCoordination.SingleProcessInMemory);
using var child = new SoloDB(storage);

var articles = child.GetCollection<Article>();
long id = articles.Insert(new Article { Title = "First title" });

var article = articles.GetById(id);
article.Title = "Revised title";
articles.Update(article);

Console.WriteLine(articles.GetById(id).Title);
```

The parent's file streams write the affected chunks. This usage does not copy the whole child database, reopen it for every update, or require a parent user transaction around each child operation. Keep the parent alive through the child's physical completion.

#### File coordination for separate participants

For coherent independent views or same-host processes, a built-in implementation delegates locks and shared memory to the configured native SQLite filesystem:

```csharp
using var main = new FileStream("shared.main", FileMode.OpenOrCreate,
    FileAccess.ReadWrite, FileShare.ReadWrite, bufferSize: 1);
using var log = new FileStream("shared.wal", FileMode.OpenOrCreate,
    FileAccess.ReadWrite, FileShare.ReadWrite, bufferSize: 1);

ISqliteStorageCoordination coordination =
    SqliteStorageCoordination.File("shared.coordination");

using var storage = SqliteStorage.Wal(main, log, true, coordination);
using var db = new SoloDB(storage);
```

Every participant must use the same main/log bytes and coordination path. Independent views must actually see one another's writes; coordination cannot repair stale private buffering. Do not delete, replace or open/close the coordination file yourself while it is in use. `File(path, registeredVfs)` can select an already registered native SQLite filesystem.

#### Bring your own byte storage

Implement five positional operations: `ReadAt`, `WriteAt`, `Flush`, `GetLength` and `SetLength`. They can return structured `StorageFailure` values with `IoError`, `Full` or `ReadOnly`, retaining the original exception as `Cause`. Thrown backend exceptions are also transported as failures.

This small file implementation demonstrates the public interface. Ordinary file users can pass `FileStream` directly; the interface is useful when implementing another storage backend.

```csharp
public sealed class PositionalFile : ISqliteStorageFile, IDisposable
{
    private readonly FileStream file;

    public PositionalFile(string path)
    {
        file = new FileStream(path, FileMode.OpenOrCreate,
            FileAccess.ReadWrite, FileShare.ReadWrite, bufferSize: 1);
    }

    private static void CheckRange(long position, int count)
    {
        if (position < 0 || position > long.MaxValue - count)
            throw new ArgumentOutOfRangeException(nameof(position));
    }

    public StorageReadResult ReadAt(long position, Span<byte> destination)
    {
        CheckRange(position, destination.Length);
        if (destination.IsEmpty) return new StorageReadResult(0, null);
        file.Position = position;
        return new StorageReadResult(file.Read(destination), null);
    }

    public StorageFailure? WriteAt(long position, ReadOnlySpan<byte> source)
    {
        CheckRange(position, source.Length);
        if (source.IsEmpty) return null;
        file.Position = position;
        file.Write(source);
        return null;
    }

    public StorageLengthResult GetLength() => new(file.Length, null);
    public StorageFailure? SetLength(long length) { file.SetLength(length); return null; }
    public StorageFailure? Flush() { file.Flush(true); return null; }
    public void Dispose() => file.Dispose();
}
```

Supply two such views, using the `Article` model above:

```csharp
using var main = new PositionalFile("positional.main");
using var journal = new PositionalFile("positional.journal");
using var storage = SqliteStorage.Simple(
    main, journal, SqliteStorageCoordination.SingleProcessInMemory);
using var db = new SoloDB(storage);
db.GetCollection<Article>().Insert(new Article { Title = "positional storage" });
```

Reads may be fragmented; a successful zero-byte read means EOF. Writes must fill their requested span on success, gaps must read as zeros, and truncation must not expose discarded bytes after extension. Failed operations must preserve the ranges described in the [custom storage contract](docs/custom-storage.md#implement-your-own-byte-files). SoloDB serializes calls on the same supplied file object; coordinate distinct views when they share a cursor or backing. Positional files remain yours to dispose.

#### Pluggable coordination

Coordination is an interface, so applications can supply their own policy. `Open` returns a distinct session for each physical main-file open. A session owns that open's pager locks, WAL mappings and SQLite coordination controls, then releases its resources through `Close`.

For example, this complete provider decorates the built-in memory implementation while retaining its session behavior:

```csharp
public sealed class CountingCoordination : ISqliteStorageCoordination
{
    private int opens;
    public int Opens => System.Threading.Volatile.Read(ref opens);

    public ISqliteStorageCoordinationSession Open(
        ISqliteStorageFile main, SqliteStorageProfile profile)
    {
        System.Threading.Interlocked.Increment(ref opens);
        return SqliteStorageCoordination.SingleProcessInMemory.Open(main, profile);
    }
}
```

Use it in place of a built-in provider:

```csharp
using var main = new MemoryStream();
using var journal = new MemoryStream();
var coordination = new CountingCoordination();
using var storage = SqliteStorage.Simple(main, journal, true, coordination);
using var db = new SoloDB(storage);
db.GetCollection<Article>().Insert(new Article { Title = "custom provider" });
Console.WriteLine(coordination.Opens);
```

A provider implementing its own session returns SQLite result codes and reports a stable compatible `DomainIdentity`. Closing one session must not release another session's locks or mappings. The decorator above deliberately retains `SingleProcessInMemory`'s scope. The [coordination guide](docs/custom-storage.md#implement-coordination) lists the full session contract.

#### Explicit coordination diagnostics

`SqliteStorageDiagnostics.TestCoordination` exercises an isolated provider before using it for real databases. This example uses the `PositionalFile` class above:

```csharp
string testPath = Path.GetTempFileName();
try
{
    using var file = new PositionalFile(testPath);
    var report = SqliteStorageDiagnostics.TestCoordination(
        SqliteStorageCoordination.SingleProcessInMemory,
        file, file, SqliteStorageProfile.Wal);

    foreach (var failure in report.Failures)
        Console.WriteLine(failure.Message);

    report.ThrowIfFailed();
}
finally
{
    File.Delete(testPath);
}
```

Pass the same main object twice for memory coordination. A provider supporting independent views can instead receive two views of the same isolated backing. `Simple` checks pager locks and session ownership; `Wal` additionally checks shared mappings, slot conflicts and peer mapping lifetime. `ReadOnly` is not a diagnostic profile.

The immutable report exposes `Passed`, `Failures`, expected/actual result codes and original exceptions. `ThrowIfFailed` raises an `AggregateException` on failure. This is an explicit same-process test, never an automatic open-time gate or proof of multiprocess exclusion, crash recovery or durability. Do not run it on a coordination domain with active database users: WAL checks overwrite test mappings.

#### Reopening, read-only images and existing backup APIs

Closing the last database retires its `SqliteStorage`. To reuse left-open streams, first finish all readers, borrowed connections and transactions, check `storage.IsClosed`, then create a new storage object over the same main/auxiliary pair.

Use the existing `VacuumTo` API to create a complete image for an independent read-only open:

```csharp
using (var source = new SoloDB("articles.db"))
    source.VacuumTo("articles.snapshot.db"); // Choose a new destination path.

using var image = File.OpenRead("articles.snapshot.db");
using var storage = SqliteStorage.ReadOnly(image, leaveOpen: true);
using var snapshot = new SoloDB(storage);
var articles = snapshot.GetCollection<Article>("articles").ToArray();
```

Keep the image unchanged while open. Immutable read-only storage requires a complete image at the current schema; upgrade older databases through a writable open first. It does not migrate, add missing collections, or write type information. Write administration is refused, while unchanged Ensure operations and removal of an already absent target can succeed without writing.

The existing `source.BackupTo(destination)` API also works with supplied storage. For a new WAL destination, SQLite requires matching source/destination page sizes; choose a rollback destination when they differ. Existing images keep their page geometry. See [import/export guidance](docs/custom-storage.md#import-export-and-read-only-images).

#### Buffered file flushing and original errors

On the .NET Standard 2.1 and .NET 6 library assets, a supplied `BufferedStream` is drained before the underlying writable `FileStream` receives `Flush(true)`. Known nested `BufferedStream` layers are supported:

```csharp
using var main = new BufferedStream(new FileStream(
    "buffered.main", FileMode.OpenOrCreate, FileAccess.ReadWrite));
using var journal = new BufferedStream(new FileStream(
    "buffered.journal", FileMode.OpenOrCreate, FileAccess.ReadWrite));
using var storage = SqliteStorage.Simple(
    main, journal, true, SqliteStorageCoordination.SingleProcessInMemory);
using var db = new SoloDB(storage);
db.GetCollection<Article>().Insert(new Article { Title = "buffered files" });
```

The .NET Standard 2.0 asset retains the supplied wrapper's normal `Flush` because that target does not expose `BufferedStream.UnderlyingStream`; direct writable `FileStream` inputs still receive `Flush(true)`. Custom wrappers supply their own persistence contract. Flushing is not a guarantee against a device ignoring synchronization, and independently buffered readers still need coherent bytes.

Opening preserves its first native result and original cause even if an admission unlock or failed-open session close also fails. Additional cleanup failures are available through `ClosingFailure`:

```csharp
static SoloDB OpenAndReport(SqliteStorage storage)
{
    try
    {
        return new SoloDB(storage);
    }
    catch (NativeSqliteException opening)
    {
        Console.Error.WriteLine(
            $"Open: {opening.SqliteErrorCode}/{opening.SqliteExtendedErrorCode}");
        Console.Error.WriteLine(opening.InnerException);
        if (storage.ClosingFailure is Exception cleanup)
            Console.Error.WriteLine($"Cleanup: {cleanup}");
        throw;
    }
}
```

Also inspect `ClosingFailure` after ordinary physical completion. Returned session-close errors remain observable even when SQLite completes its close successfully. Further failed-open cleanup errors are attached to the closing error's `Data["SoloDB.OpenCleanupException"]`. Native file coordination releases its owned WAL grants before unmapping/closing.

### Recorded collection model descriptions

Acquiring a collection through `GetCollection<T>()` or `GetCollection<T>(name)` records its serializer description on a writable database. Application code needs no extra registration call for this recording:

```csharp
using var db = new SoloDB("described.db");
var articles = db.GetCollection<Article>("articles");
articles.Insert(new Article { Title = "Recorded model" });

using var connection = db.Connection.Borrow();
using var command = connection.CreateCommand();
command.CommandText = """
    SELECT Name, json(TypeInformation)
    FROM SoloDBCollections
    WHERE Name = 'articles'
    """;
using var reader = command.ExecuteReader();
while (reader.Read())
    Console.WriteLine(reader.GetString(1));
```

`TypeInformation` is a nullable JSONB BLOB containing the latest ten distinct complete model descriptions, oldest first. Descriptions include the acquired CLR identity, serialized members and scalar encodings, nested/container shapes, enum values, F# union cases, references and known polymorphic alternatives. Repeated stored names retain ordered complete member descriptions rather than losing fields through duplicate JSON keys.

Reacquiring an already covered model makes no metadata write and does not move its history entry. Compatible newly discovered alternatives enrich retained descriptions. A changed contract records a new description; the eleventh distinct description evicts the oldest complete entry. Ten limits history entries, not fields, union cases or alternatives. Assembly-version labels alone do not create another entry.

Model recording does not rewrite documents or independently advance `user_version`; normal database opening still performs any required schema migration. Read-only acquisition does not change the catalog or bytes. Dropping a collection removes its descriptions. These are facts for viewers and migration tools, not automatic data migration, a row inventory or a replacement for codecs.

See [recorded collection types](docs/collection-type-information.md) for the stored graph format.

### More query expressions run inside SQLite

The following setup is shared by the C# query examples in this section:

```csharp
using var db = new SoloDB("memory:query-examples");
var rows = db.GetCollection<QueryRow>("rows");
rows.Insert(new QueryRow {
    Group = 2, Amount = 7, Title = "alpha", Position = 1, Key = "colour",
    Numbers = new[] { 10, 20 },
    Labels = new Dictionary<string, string> { ["colour"] = "blue" },
    Clock = new DateTime(2026, 1, 2, 13, 30, 0, DateTimeKind.Unspecified),
    Offset = TimeSpan.FromHours(2), Text = "123.5"
});

public sealed class QueryRow
{
    public long Id { get; set; }
    public int Group { get; set; }
    public int Amount { get; set; }
    public string Title { get; set; } = "";
    public int Position { get; set; }
    public string Key { get; set; } = "";
    public int[] Numbers { get; set; } = Array.Empty<int>();
    public Dictionary<string, string> Labels { get; set; } = new();
    public DateTime Clock { get; set; }
    public TimeSpan Offset { get; set; }
    public string Text { get; set; } = "";
}
```

#### Computed array, list and dictionary access

Indexes and keys can come from the row, a captured value or a retained query argument:

```csharp
var chosen = rows.Select(row => new {
    Number = row.Numbers[row.Position],
    Label = row.Labels[row.Key]
}).ToArray();

var at = rows.Compile<int, int>((source, index) =>
    source.Select(row => row.Numbers[index]));
var first = at(0).ToArray();
var second = at(1).ToArray();
```

The translation preserves the stored value representation, including byte sequences and supported numeric dictionary keys. Invalid consumed array/list indexes and absent dictionary keys raise the appropriate errors; they do not silently become default values. An unconsumed conditional branch does not access its invalid index.

#### Composite joins, mixed results and grouped left joins

```csharp
var other = db.GetCollection<QueryRow>("other_rows");
other.Insert(new QueryRow { Group = 2, Title = "alpha", Amount = 13 });

var totals = rows.Join(other,
    left => new { left.Group, left.Title },
    right => new { right.Group, right.Title },
    (left, right) => left.Amount + right.Amount)
    .ToArray();

var leftJoined = (
    from left in rows
    join right in other on left.Group equals right.Group into matches
    from right in matches.DefaultIfEmpty()
    select new {
        left.Id,
        Count = matches.Count(),
        Amount = right == null ? -1 : right.Amount
    }).ToArray();
```

Composite keys preserve component equality, precision and null behavior. Grouped left joins retain both the complete group and its outer values. Result selectors can combine both sides directly or select a conditional result.

#### Join or concatenate local values

An in-memory sequence can participate in a database query without inserting those values into a collection:

```csharp
var adjustments = new[] {
    new QueryRow { Group = 2, Amount = 100 }
};

var adjusted = rows.Join(adjustments,
    row => row.Group,
    adjustment => adjustment.Group,
    (row, adjustment) => row.Amount + adjustment.Amount)
    .ToArray();

var appended = rows.Select(row => row.Title)
    .Concat(new[] { "local-only", "local-only" })
    .ToArray();
```

Local values are bound as rows; `Concat` preserves duplicates. Retained queries acquire fresh local enumerable values on invocation. Captured database query structure remains fixed by compilation. This does not permit moving a query from another database into the local side to bypass database ownership.

#### Captured expressions and nested relation composition

```csharp
int minimum = 6;
int extra = 2;
Expression<Func<QueryRow, bool>> predicate = row => row.Amount > minimum;
Expression<Func<QueryRow, int>> projection = row => row.Amount + extra;

var run = rows.Compile(source =>
    source.Where(predicate).Select(projection));

var before = run().ToArray();
minimum = 10;
extra = 5;
var after = run().ToArray();
```

The supplied expression body is retained, while its scalar captured values bind on invocation. Expression factories use the expression they actually return.

Nested `DBRefMany` query chains can retain their correlation, ordering and bounds. For example, where `parents` is an `ISoloDBCollection<Parent>`:

```csharp
var selected = parents.Select(parent => parent.Children
    .Where(child => child.Active)
    .SelectMany(child => child.Leaves)
    .Where(leaf => leaf.Amount >= 20)
    .OrderByDescending(leaf => leaf.Amount)
    .Take(2)
    .Select(leaf => leaf.Amount)
    .ToArray()).ToArray();

public sealed class Parent
{
    public long Id { get; set; }
    public DBRefMany<Child> Children { get; set; } = new();
}
public sealed class Child
{
    public long Id { get; set; }
    public bool Active { get; set; }
    public DBRefMany<Leaf> Leaves { get; set; } = new();
}
public sealed class Leaf
{
    public long Id { get; set; }
    public int Amount { get; set; }
}
```

The existing reference-depth and cycle contracts remain. Arbitrary opaque CLR delegates are not silently executed as database query predicates.

#### Date/time construction, members, formatting and parsing

```csharp
var dates = rows.Select(row => new {
    row.Clock.Ticks,
    row.Clock.Kind,
    Day = row.Clock.Date,
    Time = row.Clock.TimeOfDay,
    LocalText = new DateTimeOffset(row.Clock, row.Offset)
        .ToString("yyyy-MM-dd HH:mm:ss zzz")
}).ToArray();

var format = rows.Compile<string, string>((source, pattern) =>
    source.Select(row => row.Clock.ToString(pattern)));
var months = format("yyyy-MM").ToArray();

var parse = rows.Compile<string, DateTime>((source, text) =>
    source.Select(row => DateTime.Parse(text)));
var parsed = parse("2026-01-02").ToArray();
```

`DateTimeKind` constructor arguments may also be literal, captured, row-derived or invocation arguments:

```csharp
var construct = rows.Compile<DateTimeKind, DateTime>((source, kind) =>
    source.Select(row => new DateTime(2026, 1, 2, 13, 30, 0, kind)));
var utc = construct(DateTimeKind.Utc).ToArray();
```

String formats can be supplied at execution for `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly` and `TimeSpan`. Single-string `Parse` translation covers those types and `Decimal`, retaining parser errors and execution-time culture rather than substituting a SQL numeric cast. Formatting follows SoloDB's invariant-culture policy.

Existing storage precision remains: persisted `DateTimeOffset` keeps UTC milliseconds, not its original offset; `TimeOnly` and `TimeSpan` retain millisecond storage. A directly constructed or parsed offset value can use its available offset during the query. See [temporal queries](docs/temporal-queries.md).

#### Supported comparer overloads

```csharp
var ordered = rows.OrderBy(row => row.Title, StringComparer.Ordinal)
    .Select(row => row.Title).ToArray();

var distinct = rows.Select(row => row.Title)
    .Distinct(EqualityComparer<string>.Default).ToArray();

var smallest = rows.Select(row => row.Title).Min(StringComparer.Ordinal);
```

Supported equality overloads accept default equality or `null`, with ordinal string equality also available. Explicit string ordering supports `Comparer<string>.Default` and `StringComparer.Ordinal`, including scalar `Min`/`Max`. Retained queries bind captured comparer choices on invocation. Arbitrary custom comparers and `StringComparer.OrdinalIgnoreCase` are not executed inside SQLite; normalize a supported key or deliberately switch to CLR execution with `AsEnumerable()` when needed.

#### Compiled reads inside a transaction

```csharp
db.WithTransaction(tx => {
    var transactional = tx.GetCollection<QueryRow>("rows");
    var count = transactional.Compile(source => source.LongCount());
    Console.WriteLine(count());
});
```

Compile and consume the results while that transaction scope is alive. A retained query or lazy result does not extend a completed transaction's lifetime.

### C#, F# and generated operations

The new features participate in ordinary execution and supported generated operations. C# and F# applications keep their normal metadata registration; no separate search or storage generator package is required.

In C#, register generated operations before use in a generated Release application:

```csharp
#if !DEBUG
SoloDBAOT.Register();
#endif
```

F# can use a whole-query quotation for retained search, including NativeAOT:

```fsharp
open System
open System.Linq
open SoloDatabase

[<CLIMutable>]
type Article = { Id: int64; Title: string; Body: string; Tenant: int }

#if !DEBUG
type Generated = SoloDBAOT<0>
#endif

let search () =
#if !DEBUG
    Generated.register()
#endif
    use db = new SoloDB("fsharp-articles.db")
    let articles = db.GetCollection<Article>()
    articles.EnsureSearchIndex("text", 1, Action<_>(fun builder ->
        builder.Field((fun article -> article.Title), 4.0, "title").Field((fun article -> article.Body), 1.0, "body") |> ignore))
    let find = articles.CompileQuotation<string, int64 * string>(
        <@ fun (source: IQueryable<Article>) (text: string) ->
            articles.SearchIn("text", text).Take(10).Select(fun hit ->
                hit.Item.Id, hit.Snippet((fun article -> article.Body), "[", "]", 24)) @>)
    find.Invoke("sqlite") |> Seq.toArray
```

F# storage selection uses the same explicit provider:

```fsharp
open System.IO

let openMemoryDatabase () =
#if !DEBUG
    Generated.register()
#endif
    use main = new MemoryStream()
    use log = new MemoryStream()
    use storage = SqliteStorage.Wal(
        main, log, true, SqliteStorageCoordination.SingleProcessInMemory)
    use db = new SoloDB(storage)
    db.GetCollection<Article>().Insert(
        { Id = 0L; Title = "alpha"; Body = "example"; Tenant = 3 }) |> ignore
```

Generation now handles fixed recursive model closure without unbounded expansion. A finite recursive model such as this is supported:

```csharp
using var db = new SoloDB("memory:nodes");
var nodes = db.GetCollection<Node<int>>();

public sealed class Node<T>
{
    public long Id { get; set; }
    public T Value { get; set; } = default!;
    public Node<T>? Next { get; set; }
}
```

A model requiring endlessly new closed types, such as each `Node<T>` containing a `Node<List<T>>`, instead receives a clear generation diagnostic (`SOLO001`). Generated helper inference through recursively expanding generic application calls reports its coverage boundary rather than silently claiming complete coverage.

See [generated metadata setup](docs/generated-metadata.md) for project registration, F# source ordering and NativeAOT publishing.

### Other correctness and runtime improvements

- Read-only database initialization respects actual native access. It does not try to migrate or record collection descriptions on a read-only handle. Reopen an older image writable to migrate it before immutable use.
- Removing an absent collection or index preserves its harmless absent-result behavior. Supplied read-only storage still refuses a real removal that would write.
- Search declaration replacement, repair, document changes and rollback preserve their transactional ownership. Rebuild repairs owned structures without sweeping unrelated objects.
- Existing exact numeric and stored-value rules are retained through computed access, joins, local values and query projections. Supported document indexes remain usable; use the existing `SoloDB.GetSQL(query)` and query-plan tools to inspect a particular query.
- Metadata acquisition avoids repeatedly parsing or rewriting unchanged complete histories. Equivalent JSON encodings do not create another model-history entry.
- Storage callbacks avoid unnecessary closure/length work while preserving cursor, partial-read, error and native lifetime semantics. This is not a promise of a fixed throughput increase for every backend.
- SQLite `REGEXP` uses a 100-millisecond match timeout. Invalid patterns and timeouts fail the SQL operation rather than silently returning a nonmatch. SQL NULL input remains a nonmatch:

  ```csharp
  using var connection = db.Connection.Borrow();
  using var command = connection.CreateCommand();
  command.CommandText = "SELECT regexp('^sql', 'sqlite')";
  Console.WriteLine(command.ExecuteScalar()); // 1
  ```

### Upgrade and operating notes

- Keep ordinary path-based construction when you want an ordinary SQLite file: `new SoloDB("app.db")`. Supplying storage is an additional choice.
- New writable storage factories require explicit coordination. Choose `SingleProcessInMemory` for its documented same-object, one-process scope; choose suitable coordination and coherent bytes for other sharing arrangements.
- Preserve the main image together with its matching journal/WAL. Do not substitute an empty auxiliary file when reopening a database that may require recovery.
- Normal schema migration requires all participating readers and writers to support schema 7. Collection type recording alone does not advance `user_version`.
- Schema migration does not perform the explicit Decimal/BigInteger data migration. Applications upgrading older numerical storage still follow the [numerical migration guide](docs/numeric-migration.md).
- Synchronization depends on the backing storage, runtime, filesystem and device. Making newly created file names/directories durable remains an application deployment responsibility. Supplied-byte storage does not redirect SQLite's ordinary native temporary files.
- Existing reference-depth, native engine feature requirements, numeric representations and per-database SQLite transaction guarantees remain in effect. This version does not introduce arbitrary custom comparer execution or an automatic document migration engine.
