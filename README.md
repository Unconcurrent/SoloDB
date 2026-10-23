# SoloDB

Store your C# and F# objects, query them with LINQ, and keep related documents and files together in one SQLite database. SoloDB runs inside your application, with transactions and SQL available when you need them.

## Core Features

SoloDB is packed with features that provide a seamless and powerful developer experience.

- **SQLite Core**: Leverages the world's most deployed database engine, ensuring rock-solid stability, performance, and reliability.
- **Serverless Architecture**: As a .NET library, it runs in-process with your application. No separate server or configuration is required.
- **Hybrid NoSQL & SQL**: Store and query schemaless JSON documents with a familiar object-oriented API, or drop down to raw SQL for complex queries.
- **Full LINQ Support**: Use the full power of LINQ and IQueryable<T> to build expressive, strongly-typed queries against your data.
- **ACID Transactions**: Guarantees atomicity, consistency, isolation, and durability for all operations, thanks to SQLite's transactional nature.
- **Expressive Indexing**: Create unique or non-unique indexes on document properties for lightning-fast queries, using simple attributes.
- **Integrated File Storage**: A robust, hierarchical file system API (similar to System.IO) for storing and managing large files and binary data directly within the database.
- **Polymorphic Collections**: Store objects of different derived types within a single collection and query them by their base or concrete type.
- **Events API**: Subscribe to collection lifecycle events (`OnInserting`, `OnInserted`, `OnUpdating`, `OnUpdated`, `OnDeleting`, `OnDeleted`) with full transactional guarantees via SQLite triggers.
- **Thread-Safe**: A built-in connection pool ensures safe, concurrent access from multiple threads.
- **Customizable ID Generation**: Use the default long primary key, or implement your own custom ID generation strategy (e.g., string, Guid).
- **.NET Standard 2.0 & 2.1**: Broad compatibility with .NET Framework, .NET Core, and modern .NET runtimes.
- **Open Source**: Licensed under LGPL-3.0, with the bundling permission described below.
- **Documentation**: See the [official documentation](https://solodb.org/docs.html) for detailed guides.

## Table of Contents

- [Why SoloDB?](#why-solodb)
- [Installation](#installation)
- [Generated Metadata Registration](#generated-metadata-registration)
- [Direct Typed JSON](#direct-typed-json)
- [Getting Started: A 60-Second Guide](#getting-started-a-60-second-guide)
- [Usage and Examples](#usage-and-examples)
  - [Initializing the Database](#initializing-the-database)
  - [Working with Collections](#working-with-collections)
  - [Relations (DBRef, DBRefMany, SoloRef)](#relations-dbref-dbrefmany-soloref)
  - [Compiled Queries](#compiled-queries)
  - [Indexing for Performance](#indexing-for-performance)
  - [Atomic Transactions](#atomic-transactions)
  - [Storing Polymorphic Data](#storing-polymorphic-data)
  - [Custom ID Generation](#custom-id-generation)
  - [Integrated File Storage](#integrated-file-storage)
  - [Filesystem References](#filesystem-references)
  - [Events API](#events-api)
  - [Direct SQL Access](#direct-sql-access)
  - [ADO.NET Emulation](#adonet-emulation)
  - [F# Example](#f-example)
- [Database Management](#database-management)
  - [Backups](#backups)
  - [Optimization](#optimization)
- [License](#license)

## Why SoloDB?

In a world of countless database solutions, SoloDB was created to fill a specific niche: to provide a simple, modern, and powerful alternative to document databases like MongoDB, but with the unmatched reliability and zero-configuration nature of SQLite. It's for developers who love the flexibility of NoSQL but don't want to sacrifice the transactional integrity and robustness of a traditional SQL database.

## Installation

Install SoloDB from NuGet:

```bash
dotnet add package SoloDB
```

Before upgrading a database to 1.4, stop every older SoloDB process using it. Opening the file advances its database version to 6; SoloDB 1.3 then refuses to reopen it. Do not lower `PRAGMA user_version` to bypass that protection. See [numerical storage and migration](docs/generated-metadata.md#numerical-storage-and-migration).

## Generated Metadata Registration

C# and F# projects can register build-generated operations for their original model types. See the [registration guide](docs/generated-metadata.md) for inferred roots, explicit registration, conditional activation and NativeAOT requirements.

## Direct Typed JSON

Write typed values to UTF-8 or UTF-16 storage and read them with `ParseInto<T>`:

```csharp
using System;
using SoloDatabase.JsonSerializator;

Span<byte> destination = Span<byte>.Empty;
int[] source = { 1, 2, 3 };
int written = JsonValue.WriteJson(ref source, ref destination);
ReadOnlySpan<byte> json = destination.Slice(0, written);
int[] values = JsonValue.ParseInto<int[]>(json);
```

An empty span grows automatically; `written` counts bytes for UTF-8 and characters for UTF-16. Writable-list overloads append and return the added count. See the [direct JSON guide](docs/generated-metadata.md#direct-typed-json) for custom growth/release callbacks, final-buffer ownership and failure behavior. NativeAOT uses the generated registration described above. `Serialize<T>` and `Parse` remain available when you need a `JsonValue` tree.

Direct floating-point conversion avoids the decimal-backed tree's rounding and range limits. Direct and tree output can differ for values outside Decimal precision or range. Forward object readers run setters and conversions in input order: an invalid earlier duplicate fails, and user effects can remain after later malformed or trailing input. Exceptional late runtime-type selection can replay conversion and add effects. See the [direct JSON behavior and ownership guide](docs/generated-metadata.md#direct-typed-json).

## Getting Started: A 60-Second Guide

Here is a complete example to get you up and running instantly.

```csharp
using SoloDatabase;
using SoloDatabase.Attributes;

// 1. Initialize the database (on-disk or in-memory)
using var db = new SoloDB("my_app_data.db");

// 2. Get a strongly-typed collection
var users = db.GetCollection<User>();

// 3. Insert some data
var user = new User 
{ 
    Name = "John Doe", 
    Email = "john.doe@example.com", 
    CreatedAt = DateTime.UtcNow 
};
users.Insert(user);
Console.WriteLine($"Inserted user with auto-generated ID: {user.Id}");

// 4. Query your data with LINQ
var foundUser = users.FirstOrDefault(u => u.Email == "john.doe@example.com");
if (foundUser != null)
{
    Console.WriteLine($"Found user: {foundUser.Name}");

    // 5. Update a document
    foundUser.Name = "Johnathan Doe";
    users.Update(foundUser);
    Console.WriteLine("User has been updated.");
}

// 6. Delete a document
users.Delete(user.Id);
Console.WriteLine($"User deleted. Final count: {users.Count()}");

// Define your data model
public class User
{
    // A 'long Id' property is automatically used as the primary key.
    public long Id { get; set; }

    [Indexed] // Create an index on the 'Email' property for fast lookups.
    public string Email { get; set; }
    public string Name { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

## Usage and Examples

### Initializing the Database

You can create a database on disk for persistence or in-memory for temporary data and testing.

```csharp
using SoloDatabase;

// Create or open a database file on disk
using var onDiskDB = new SoloDB("path/to/database.db");

// Create a named, shareable in-memory database
using var sharedMemoryDB = new SoloDB("memory:my-shared-db");
```

When several workers will share a new database file, initialize it before starting them; independent simultaneous first opens can contend during journal and schema setup.

### Working with Collections

A collection is a container for your documents, analogous to a table in SQL.

```csharp
// Get a strongly-typed collection. This is the recommended approach.
var products = db.GetCollection<Product>();

// Get an untyped collection for dynamic scenarios.
var untypedProducts = db.GetUntypedCollection("Product");

public class Product { /* ... */ }
```

Untyped JSON exposes the physical document representation. For Decimal and BigInteger fields, use typed collection operations or preserve the [stored numerical representation](docs/generated-metadata.md#numerical-storage-and-migration) when writing through untyped JSON or SQL.

### Relations (DBRef, DBRefMany, SoloRef)

#### Overview

SoloDB relations are explicit, typed links between collections:

- `DBRef<TTarget>`: single relation to one target row.
- `DBRefMany<TTarget>`: many relation via link rows.
- `[SoloRef(...)]`: relation policy annotation (`OnDelete`, `OnOwnerDelete`, `Unique`).

Relations are persisted through dedicated link tables (`SoloDBRelLink_*`) and relation metadata (`SoloDBRelation`), not embedded owner-document arrays of foreign keys.

#### DBRef and DBRefMany Basics

```csharp
using SoloDatabase;
using SoloDatabase.Attributes;
using System.Collections.Generic;

public class Team
{
    public long Id { get; set; }
    public string Name { get; set; } = "";

    // Single relation
    public DBRef<Member> Lead { get; set; } = DBRef<Member>.None;

    // Many relation
    public DBRefMany<Member> Members { get; set; } = new();
}

public class Member
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
}

using var db = new SoloDB("memory:relations-basics");
var teams = db.GetCollection<Team>();
var members = db.GetCollection<Member>();

// DBRef.To(existingId): link to an already persisted target.
var existingLeadId = members.Insert(new Member { Name = "Existing Alice" });
teams.Insert(new Team
{
    Name = "Ops",
    Lead = DBRef<Member>.To(existingLeadId)
});

// DBRef.From(entity): insert target first, then link owner to inserted target.
var team = new Team
{
    Name = "Core",
    Lead = DBRef<Member>.From(new Member { Name = "Alice" })
};

// DBRefMany supports Add/Clear/Remove mutations tracked on Update.
team.Members.Add(new Member { Name = "Bob" });
team.Members.Add(new Member { Name = "Carol" });

teams.Insert(team);

// Default policy behavior in this basics example:
// - No [SoloRef(...)] overrides are applied here.
// - One owner with one referenced Member: deleting the owner deletes that orphan Member (default OnOwnerDelete=Deletion).
// - Two owners sharing the same Member: deleting one owner keeps the shared Member; deleting the last owner deletes it.
```

#### SoloRef Policy Matrix

`DeletePolicy` values in SoloDB are: `Restrict`, `Cascade`, `Unlink`, `Deletion`.

| Policy Surface | Trigger Operation | Persistence Result | Reject Behavior |
|---|---|---|---|
| `OnDelete = Restrict` | Delete target while owners still reference it | Target delete blocked; links and owners unchanged | Typed reject (`InvalidOperationException`) |
| `OnDelete = Cascade` | Delete target | Referencing owners are deleted (and their links removed) | N/A when valid |
| `OnDelete = Unlink` | Delete target | Owner rows survive; relation links removed / refs become empty | N/A when valid |
| `OnOwnerDelete = Restrict` | Delete owner that still has links | Owner delete blocked | Typed reject (`InvalidOperationException`) |
| `OnOwnerDelete = Unlink` | Delete owner | Owner removed; links removed; targets survive | N/A when valid |
| `OnOwnerDelete = Deletion` | Delete owner | Owner removed; orphaned targets in that relation lane can be deleted | N/A when valid |
| `Unique = true` on single ref | Link/update relation | Enforces one-to-one style constraints at link-table level | Constraint reject if violated |

Important constraints:

- `OnOwnerDelete = Cascade` is rejected by schema/build guards.
- Reference properties may use different policies and single-reference uniqueness settings for the same target, including mixtures of typed and untyped Single and Many references. Each property retains its own policy.
- Independent linked `Restrict` policies are checked before the associated deletion effects. Inverse views of the same shared Many links retain their existing owner-unlink behavior. Owner deletion removes applicable links before deciding whether targets reached through `Deletion` references are orphaned. Surviving external references preserve a target; an `Unlink` reference alone does not request target deletion.
- Multiple Many properties that resolve to the same shared relation remain invalid because their independent links cannot be distinguished.

#### Loading Semantics

Relation loading is performed by SoloDB's relation batch-load pipeline after owner rows are read. In practice:

```csharp
using SoloDatabase;
using System;
using System.Linq;

using var db = new SoloDB("memory:relations-loading");
var members = db.GetCollection<Member>();
var teams = db.GetCollection<Team>();

var leadId = members.Insert(new Member { Name = "Loaded Alice" });
teams.Insert(new Team { Name = "Core", Lead = DBRef<Member>.To(leadId) });

// Default path: relation is loaded, so .Value is available.
var loadedTeam = teams.AsQueryable().First(t => t.Name == "Core");
Console.WriteLine(loadedTeam.Lead.Id);         // row id
Console.WriteLine(loadedTeam.Lead.Value.Name); // works: loaded by relation batch-load

// Exclude path: relation value is not loaded.
var excludedTeam = teams.AsQueryable()
    .Exclude(t => t.Lead)
    .First(t => t.Name == "Core");

Console.WriteLine(excludedTeam.Lead.Id); // id is still available
try
{
    _ = excludedTeam.Lead.Value; // throws: not loaded
}
catch (InvalidOperationException)
{
    Console.WriteLine("Lead.Value is unavailable when excluded.");
}

// Include path: hydrate only selected relations.
var includeOnlyMembers = teams.AsQueryable()
    .Include(t => t.Members)
    .Where(t => t.Lead.Value.Name == "Loaded Alice") // predicate still works
    .Single();

Console.WriteLine(includeOnlyMembers.Lead.Id); // id is still available
try
{
    _ = includeOnlyMembers.Lead.Value; // throws: not loaded (Lead not included)
}
catch (InvalidOperationException)
{
    Console.WriteLine("Lead.Value is unavailable when not included.");
}

Console.WriteLine(includeOnlyMembers.Members.Count); // hydrated because Members was included
```

Key behavior:

- Default (no Include): relation hydration remains include-all behavior.
- Include is hydration-only: included paths are hydrated; non-included paths keep persisted ids and remain unloaded.
- Non-included relation paths can still be used in LINQ predicates/projections that require SQL joins.
- Exclude is stricter than non-included: excluded relation `.Value` access is rejected.
- Same path in Include and Exclude is rejected deterministically.
- `DBRef<T>.Id` is always the persisted relation id (or `0` for empty).
- `DBRef<T>.Value` requires materialization; excluded/unloaded access throws `InvalidOperationException`. Selecting a reference alone can return an unloaded reference; check `IsLoaded` before accessing `Value`.
- A loaded document target retains its physical row ID in its writable `long Id` property, so updating or reusing that target addresses the existing row. A typed reference also retains the target’s separate `[SoloId]` key.
- `DBRefMany<T>` relation queries (`Any`, `Count`) are translated through relation metadata/link tables.

#### UpdateMany and Relation Diffs

`UpdateMany` supports relation transforms for approved shapes:

> `Set` / `Add` / `Remove` / `Clear` here are expression-tree markers for `UpdateMany` translation only.  
> Calling them directly outside `UpdateMany` is unsupported and will throw.

```csharp
var members = db.GetCollection<Member>();
var teams = db.GetCollection<Team>();

var leadId = members.Insert(new Member { Name = "Dora" });
var member = new Member { Name = "Eve" };
members.Insert(member);

// Single ref set to an existing target
teams.UpdateMany(t => t.Name == "Core",
    t => t.Lead.Set(DBRef<Member>.To(leadId)));

// Clear single ref
teams.UpdateMany(t => t.Name == "Core",
    t => t.Lead.Set(DBRef<Member>.None));

// Many relation add/remove/clear
teams.UpdateMany(t => t.Name == "Core",
    t => t.Members.Add(member));
teams.UpdateMany(t => t.Name == "Core",
    t => t.Members.Remove(member));
teams.UpdateMany(t => t.Name == "Core",
    t => t.Members.Clear());
```

#### LINQ Relation Support

```csharp
// Any(...) over DBRefMany
var teamsWithBob = teams.AsQueryable()
    .Where(t => t.Members.Any(m => m.Name == "Bob"))
    .ToList();

// Count(...) over DBRefMany
var largeTeams = teams.AsQueryable()
    .Where(t => t.Members.Count > 10)
    .ToList();
```

`Include(...)` affects post-query hydration, not SQL predicate capability. Relation predicates (for example `DBRef.Value`, `DBRefMany.Any`, `DBRefMany.Count`) remain translatable.

```fsharp
open SoloDatabase
open System.Linq

type FLead() =
    member val Id = 0L with get, set
    member val Name = "" with get, set

type FMember() =
    member val Id = 0L with get, set
    member val Name = "" with get, set

type FTeam() =
    member val Id = 0L with get, set
    member val Name = "" with get, set
    member val Lead = DBRef<FLead>.None with get, set
    member val Members = DBRefMany<FMember>() with get, set

use db = new SoloDB("memory:relations-fsharp")
let teams = db.GetCollection<FTeam>()

let team = FTeam(Name = "Core", Lead = DBRef<FLead>.From(FLead(Name = "Alice")))
team.Members.Add(FMember(Name = "Bob"))
teams.Insert(team) |> ignore

let hasBob =
    teams.AsQueryable()
        .Where(fun t -> t.Members.Any(fun m -> m.Name = "Bob"))
        .ToList()

let manyMembers =
    teams.AsQueryable()
        .Where(fun t -> t.Members.Count > 0)
        .ToList()
```

**Ordering note:** DBRefMany item order is not guaranteed unless you explicitly apply ordering in query/projection.

F# mutable-record style (common idiom) is also supported as long as relation properties are mutable:

```fsharp
type FLeadR = { mutable Id: int64; mutable Name: string }
type FMemberR = { mutable Id: int64; mutable Name: string }
type FTeamR =
    { mutable Id: int64
      mutable Name: string
      mutable Lead: DBRef<FLeadR>
      mutable Members: DBRefMany<FMemberR> }

use db2 = new SoloDB("memory:relations-fsharp-record")
let teamsR = db2.GetCollection<FTeamR>()
let teamR =
    { Id = 0L
      Name = "RecordTeam"
      Lead = DBRef<FLeadR>.From({ Id = 0L; Name = "RecordLead" })
      Members = DBRefMany<FMemberR>() }
teamR.Members.Add({ Id = 0L; Name = "RecordMember" })
teamsR.Insert(teamR) |> ignore
```

#### Failure Contracts

1. Unsupported relation query/update expression shapes are rejected with typed exceptions (`NotSupportedException`).
2. Missing/inconsistent relation metadata is rejected where knowable (build/bootstrap), with translation-time safety net rejects for invalid metadata states.
3. Reject paths are fail-closed: no owner/target/link/catalog mutation on rejected operations.
4. Reference mutations that introduce a directed cycle or a path exceeding 10 reference hops throw `InvalidOperationException`. Admission checks the final stored graph, including bulk and callback effects, before the operation succeeds.

**No silent fallback:** SoloDB does not silently downgrade unsupported relation operations to generic JSON scanning behavior.

#### Compatibility and Migration Notes

- Existing `DBRef<TTarget>` / `DBRefMany<TTarget>` persistence model remains relation-first (`SoloDBRelLink_*`, `SoloDBRelation`).
- Relation query expressions support up to 10 reference hops. Write admission separately checks the final stored paths affected by newly introduced edges: a document starts at depth zero, 10 hops are allowed, and an 11th hop or directed cycle is rejected. Existing-ID `To` references count, including paths through previously stored ancestors. Shared targets and repeated CLR types on distinct rows are permitted when those paths remain within the limit and acyclic.
- Existing deep or cyclic data remains readable with bounded loading; default `GetById` and ordinary query loading retain their existing 11-hop boundary. Saves that leave final reference adjacency unchanged or only remove edges remain permitted, allowing incremental repair. Opening a database does not scan or repair its graph. New reciprocal navigable Many links are rejected when their forward and inverse references form a cycle.
- `DBRefMany<T>` item ordering is not a stable ordering contract unless your query/project explicitly orders results.
- Nested DBRefMany relation predicates (e.g. `Items.Any(i => i.SubItems.Any(...))`) are supported up to 10 relation hops.
- `option<DBRef<_>>` / `option<DBRefMany<_>>` relation-property shapes are not supported; use `DBRef<_>.None` for empty single refs.
- `Include` + `Exclude` on the same relation path is rejected deterministically.
- For custom-id relation scenarios (`DBRef<TTarget, TId>`), target-side id/index constraints must be satisfied before relation writes.

### Compiled Queries

Retain a compiled function when you execute the same query structure with different values. `Compile` translates and optimizes the expression once using collection indexes and available SQLite statistics, then returns a regular `Func`. Sequence expressions return `Func<IEnumerable<T>>`; result expressions return their actual result type, such as `Func<long>` for `LongCount()`.

```csharp
var findUsers = users.Compile((IQueryable<User> q, string prefix, int offset, int limit) =>
    q.Where(user => user.Name.StartsWith(prefix))
     .OrderBy(user => user.Id)
     .Skip(offset).Take(limit));

var firstPage = findUsers("Jo", 0, 20).ToArray();
var nextPage = findUsers("Jo", 20, 20).ToArray();
```

The source parameter is bound to the receiving collection. Supply zero to three additional arguments; `Select` can change the result type. Query values are bound on invocation, and each enumeration reads current data through the ordinary query executor. Retain the function rather than calling `Compile` for every request. Pass stateful values as invocation arguments; do not rely on callback evaluation order inside an optimized query expression.

Read terminals and materializers can be included directly:

```csharp
var countUsers = users.Compile((IQueryable<User> q, string prefix) =>
    q.LongCount(u => u.Name.StartsWith(prefix)));
var firstUser = users.Compile(q => q.OrderBy(u => u.Id).FirstOrDefault());
var userNames = users.Compile(q => q.OrderBy(u => u.Id).Select(u => u.Name).ToArray());

long count = countUsers("Al");
```

Counts, quantifiers, aggregates and element terminals share ordinary query execution, including empty-result and multiple-result behavior. Materializers such as `ToArray`, `ToList` and `ToDictionary` execute on each invocation. An explicit `AsEnumerable()` keeps subsequent operations on the client, just as in an ordinary query. Compiling retains query machinery, never result rows or counts.

Compile from a nontransactional collection and keep that collection alive while using the function. Recompile after changing its model or indexes, or to use refreshed statistics. Unindexed filtering and deep offsets still require database work.

For supported ordered pages with an explicit `Id` tie-breaker, compiled queries select page IDs before fetching payloads. Planning considers compound-index ordering, bounded early-page scans and, when statistics are available, a capped covering count to choose a deep-page strategy within the same statement. These choices preserve your results and ordering; gains depend on the filter, indexes and data distribution.

### Indexing for Performance

Use index attributes for property-level defaults, and use expression APIs for operational index management and composite keys.

#### Attribute Indexes

```csharp
using SoloDatabase.Attributes;

public class IndexedProduct
{
    public long Id { get; set; } // Primary key is indexed.

    [Indexed(unique: true)] // Unique property index.
    public string SKU { get; set; } = "";

    [Indexed] // Non-unique property index.
    public string Category { get; set; } = "";

    public decimal Price { get; set; }
}

var products = db.GetCollection<IndexedProduct>();
products.Insert(new IndexedProduct { SKU = "BOOK-123", Category = "Books", Price = 29.99m });
var books = products.Where(p => p.Category == "Books").ToList();
```

#### Expression Index APIs

```csharp
var users = db.GetCollection<User>();

// Create a non-unique expression index.
users.EnsureIndex(u => u.Name);

// Create a unique expression index.
users.EnsureUniqueAndIndex(u => u.Email);

// Drop if present (no-op if missing).
users.DropIndexIfExists(u => u.Email);

// Backfill attribute-declared indexes for an existing collection.
users.EnsureAddedAttributeIndexes();
```

#### Index SQL Shape and Naming

- Expression indexes are built from translated expression SQL over persisted `Value`.
- Member-path expressions are translated to JSON extraction expressions (for example `jsonb_extract(...)` paths).
- Generated names are deterministic: collection name + sanitized translated expression.
- Practical effect: the same expression shape maps to the same index identity.

#### Composite Index Boundaries

Composite (tuple) indexes are supported via expression APIs:

```csharp
// Composite non-unique index.
products.EnsureIndex(p => new ValueTuple<string, string>(p.Category, p.SKU));

// Composite unique index.
products.EnsureUniqueAndIndex(p => new ValueTuple<string, string>(p.Category, p.SKU));
```

`[Indexed]` is property-level only. It does not declare multi-column composite indexes.

#### SoloId and Typed-Relation Requirements

- `[SoloId(...)]` carries unique-index semantics via attribute inheritance behavior.
- For typed relations (`DBRef<TTarget, TId>`), target-side SoloId resolution requires a usable UNIQUE index on the target SoloId path before relation writes.

#### Reject Constraints for Invalid Index Expressions

Invalid index expressions are rejected with typed exceptions (argument/operation), including:

- constant or outside expressions,
- expressions with captured variables,
- unsupported expression node shapes.

### Atomic Transactions

For operations that must either fully complete or not at all, use WithTransaction. If an exception is thrown inside the delegate, all database changes are automatically rolled back.

- Root `WithTransaction(...)` scopes run as SQLite `BEGIN IMMEDIATE` transactions.
- Calling `WithTransaction(...)` inside an existing transaction uses a nested SQLite `SAVEPOINT`.
- Event handlers execute inside active SQL statements; opening a nested transaction from handler context is intentionally rejected.

```csharp
try
{
    db.WithTransaction(tx => {
        var accounts = tx.GetCollection<Account>();
        var fromAccount = accounts.GetById(1);
        var toAccount = accounts.GetById(2);

        fromAccount.Balance -= 100;
        toAccount.Balance += 100;

        accounts.Update(fromAccount);
        accounts.Update(toAccount);
        
        // If something fails here, both updates will be reverted.
        // throw new InvalidOperationException("Simulating a failure!");
    });
}
catch (Exception ex)
{
    Console.WriteLine($"Transaction failed and was rolled back: {ex.Message}");
}
```

### Storing Polymorphic Data

Store different but related object types in the same collection. SoloDB automatically handles serialization and deserialization.

```csharp
public abstract class Shape
{
    public long Id { get; set; }
    public string Color { get; set; }
}
public class Circle : Shape { public double Radius { get; set; } }
public class Rectangle : Shape { public double Width { get; set; } public double Height { get; set; } }

// Store all shapes in one collection
var shapes = db.GetCollection<Shape>();
shapes.Insert(new Circle { Color = "Red", Radius = 5.0 });
shapes.Insert(new Rectangle { Color = "Blue", Width = 4.0, Height = 6.0 });

// You can query for specific derived types using OfType<T>()
var circles = shapes.OfType<Circle>().ToList();
Console.WriteLine($"Found {circles.Count} circle(s).");
```

### Custom ID Generation

While the default long auto-incrementing ID is sufficient for most cases, you can define your own ID types and generation logic.

```csharp
using SoloDatabase.Attributes;

// 1. Define a custom ID generator
public class GuidIdGenerator : IIdGenerator<MyObject>
{
    public object GenerateId(ISoloDBCollection<MyObject> collection, MyObject item)
    {
        return Guid.NewGuid().ToString("N");
    }

    public bool IsEmpty(object id) => string.IsNullOrEmpty(id as string);
}

// 2. Define the model with the custom ID
public class MyObject
{
    [SoloId(typeof(GuidIdGenerator))]
    public string Id { get; set; }
    public string Data { get; set; }
}

// 3. Use it
var collection = db.GetCollection<MyObject>();
var newItem = new MyObject { Data = "Custom ID Test" };
collection.Insert(newItem); // newItem.Id is now populated with a GUID string.
Console.WriteLine($"Generated ID: {newItem.Id}");
```

#### IIdGenerator contract: cascade-context behaviour

`GenerateId` receives a typed `ISoloDBCollection<MyObject>` for both direct
`Insert` and cascade insertion through `DBRef.From(unsavedTarget)`. During a
cascade, this collection reads through the same transaction as the parent
insert. A generator may consult existing target rows when choosing an ID.
The cascade and parent insert commit together, or roll back together if the
generator fails. Do not retain the transactional collection after the insert.

### Integrated File Storage

SoloDB includes a powerful file storage system for managing binary data, large documents, or any kind of file.

```csharp
using System.Text;

var fs = db.FileSystem;
var content = "This is the content of my file.";
var contentBytes = Encoding.UTF8.GetBytes(content);

// Upload data from a stream
using (var stream = new MemoryStream(contentBytes))
{
    fs.Upload("/reports/report-2024.txt", stream);
}

// Set custom metadata
fs.SetMetadata("/reports/report-2024.txt", "Author", "Ruslan");

// Download the file
using (var targetStream = new MemoryStream())
{
    fs.Download("/reports/report-2024.txt", targetStream);
    targetStream.Position = 0;
    string downloadedContent = new StreamReader(targetStream).ReadToEnd();
    Console.WriteLine($"Downloaded content: {downloadedContent}");
}

// Check if a file exists
bool exists = fs.Exists("/reports/report-2024.txt"); // true

// Delete a file
fs.DeleteFileAt("/reports/report-2024.txt");
```

Outside an explicit transaction, `Download` and `DownloadAsync` are nonblocking reads rather than whole-file snapshots. If an upload commits during a download, different reads can observe different file versions. Coordinate the operations or use an explicit transaction when one consistent view is required.

### Filesystem references

Documents can protect existing files and directories with Single and Many references. See [Filesystem references](docs/filesystem-references.md) for C# and F# examples, loading and mutation behavior.

### Events API

Filesystem changes are available through `db.FileSystem`, including typed header, metadata and logical byte-range callbacks. See [Filesystem events](docs/filesystem-events.md) for examples and callback semantics.

SoloDB provides a powerful event system for reacting to collection changes. Events are implemented via SQLite triggers, ensuring they participate in the same transaction as the triggering operation.

```csharp
var users = db.GetCollection<User>();

// Before-events: OnInserting, OnUpdating, OnDeleting
// Throwing an exception rolls back the entire operation.
users.OnInserting(ctx => {
    if (string.IsNullOrEmpty(ctx.Item.Email))
        throw new InvalidOperationException("Email required");
    return SoloDBEventsResult.EventHandled;
});

// After-events: OnInserted, OnUpdated, OnDeleted
// Use RemoveHandler to auto-unregister after first execution.
users.OnInserted(ctx => {
    Console.WriteLine($"First user created: {ctx.Item.Id}");
    return SoloDBEventsResult.RemoveHandler; // One-shot handler
});

// Update events provide both old and new state.
users.OnUpdating(ctx => {
    var audit = ctx.GetCollection<AuditLog>(); // ctx implements ISoloDB
    audit.Insert(new AuditLog {
        Action = "Update",
        OldValue = ctx.OldItem.Name,
        NewValue = ctx.Item.Name
    });
    return SoloDBEventsResult.EventHandled;
});
```

**Important**: Event handlers must use `ctx` directly for database access. Using any other `SoloDB` instance inside a handler can cause database locking issues.

#### Events API FAQ

**Q: If a handler throws an exception, is it automatically removed?**
A: No. The handler remains registered. Only returning `RemoveHandler` removes it.

**Q: If one handler throws, do the remaining handlers still execute?**
A: No. When a handler throws, execution stops immediately and the operation rolls back. Remaining handlers are skipped.

**Q: Can I modify the item in OnInserting/OnUpdating?**
A: No. `ctx.Item` is a read-only copy. To reject an operation, throw an exception. To transform data, modify it before calling `Insert`/`Update`.

### Direct SQL Access

Use raw SQL for your own tables alongside document collections. This example creates a separate SQL table; collection properties live inside JSONB and are not ordinary SQL columns.

```csharp
using SoloDatabase;
using static SoloDatabase.NativeSQLiteTools.IDbConnectionExtensions;

using var db = new SoloDB("memory:sql-example");
using var pooledConnection = db.Connection.Borrow();

pooledConnection.Execute("CREATE TABLE Inventory(Name TEXT, PriceInCents INTEGER)");
pooledConnection.Execute("INSERT INTO Inventory VALUES(@name, @price)",
    new { name = "Notebook", price = 500L });
pooledConnection.Execute("UPDATE Inventory SET PriceInCents = PriceInCents + @amount",
    new { amount = 50L });

long highestPrice = pooledConnection.QueryFirst<long>("SELECT MAX(PriceInCents) FROM Inventory");

// Enumerate while the pooled connection remains alive.
foreach (var item in pooledConnection.Query<(string Name, long PriceInCents)>(
    "SELECT Name, PriceInCents FROM Inventory WHERE PriceInCents < @maximum",
    new { maximum = 1000L }))
{
    Console.WriteLine($"{item.Name}: {item.PriceInCents}");
}
```

Typed Decimal and BigInteger document fields use the [stored numerical representation](docs/generated-metadata.md#numerical-storage-and-migration); raw SQL keeps SQLite's own value and arithmetic rules.

### ADO.NET emulation

SoloDB includes ADO.NET emulation for compatibility in the same project, assembly and NuGet package. `GetInternalConnection()` and pooled `Borrow()` return it automatically; no extra package or wrapper is required. Its types are in `SoloDatabase.Data.Sqlite`. Recompile applications and dependent libraries against 1.4; preserved source syntax does not imply binary compatibility with assemblies built against 1.3.

This is a limited compatibility layer, not a full ADO.NET provider. Stored procedures and output parameters are unsupported; async calls perform synchronous SQLite work, and `Cancel()` does not interrupt execution. See [supported operations and ownership](docs/native-sql.md#adonet-emulation).

### F# Example

SoloDB works seamlessly with F#.

```fsharp
open SoloDatabase
open System.Linq

[<CLIMutable>]
type MyFSharpType = { Id: int64; Name: string; Data: string }

use db = new SoloDB("fsharp_demo.db")
let collection = db.GetCollection<MyFSharpType>()
        
// Insert a document
let data = { Id = 0L; Name = "F# Document"; Data = "Some data" }
collection.Insert(data) |> ignore
printfn "Inserted document with ID: %d" data.Id
        
// Materialize the query
let documents = collection.ToList()
printfn "Found %d documents" documents.Count
        
// Update a document
let updatedData = { data with Data = "Updated F# data" }
collection.Update(updatedData)
        
// Delete a document
let deleteCount = collection.Delete(data.Id)
printfn "Deleted %d document(s)" deleteCount
```

## Database Management

### Backups

Create live backups of your database without interrupting application read operations.

```csharp
// Back up to another SoloDB instance
using var backupDb = new SoloDB("path/to/backup.db");
db.BackupTo(backupDb);

// Or vacuum the database into a new, clean file
db.VacuumTo("path/to/optimized_backup.db");
```

### Optimization

You can ask SQLite to analyze the database and potentially improve query plans. This runs automatically at startup but can also be triggered manually.

```csharp
db.Optimize();
```

## License

This project is licensed under the GNU Lesser General Public License v3.0 (LGPL-3.0).

In addition, special permission is granted to distribute applications that incorporate an unmodified DLL of this library in Single-file deployments, Native AOT builds, and other bundling technologies that embed the library directly into the executable file. This ensures you can use modern .NET deployment strategies without violating the license.

Full license details are available [here](https://solodb.org/legal.html).

### Direct SQL

See [Direct SQL connections](docs/native-sql.md) for named SQL execution, typed rows, named parameters and transactions.
