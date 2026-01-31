# SoloDB - A Document Database With Full LINQ Support

**Version 1.0.0** - Stable Release

[SoloDB](https://solodb.org/) is a high-performance, lightweight, and robust embedded .NET database that elegantly combines the power of a NoSQL document store with the reliability of SQL. Built directly on top of SQLite and its native [JSONB](https://sqlite.org/jsonb.html) support, SoloDB offers a serverless, feature-rich experience, combining a simple [MongoDB](https://www.mongodb.com/)-like API with full LINQ support for expressive, strongly-typed queries.

It is designed for developers who need a fast, reliable, and easy-to-use database solution without the overhead of a separate server. It's perfect for desktop applications, mobile apps (via .NET MAUI), and small to medium-sized web applications.

I wrote a detailed comparison with a popular alternative, [LiteDB](https://github.com/litedb-org/LiteDB) — including benchmarks, API differences, and developer experience. [Read the article here](https://unconcurrent.com/articles/SoloDBvsLiteDB.html).

## Table of Contents

- [Core Features](#core-features)
- [Why SoloDB?](#why-solodb)
- [Installation](#installation)
- [Getting Started: A 60-Second Guide](#getting-started-a-60-second-guide)
- [Usage and Examples](#usage-and-examples)
  - [Initializing the Database](#initializing-the-database)
  - [Working with Collections](#working-with-collections)
  - [Indexing for Performance](#indexing-for-performance)
  - [Atomic Transactions](#atomic-transactions)
  - [Storing Polymorphic Data](#storing-polymorphic-data)
  - [Custom ID Generation](#custom-id-generation)
  - [Integrated File Storage](#integrated-file-storage)
  - [Events API](#events-api)
  - [Direct SQL Access](#direct-sql-access)
  - [F# Example](#f-example)
- [Database Management](#database-management)
  - [Backups](#backups)
  - [Optimization](#optimization)
- [License](#license)
- [FAQ](#faq)

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
- **Open Source**: Licensed under the permissive LGPL-3.0.
- **Documentation**: See the [official documentation](https://solodb.org/docs.html) for detailed guides.

## Why SoloDB?

In a world of countless database solutions, SoloDB was created to fill a specific niche: to provide a simple, modern, and powerful alternative to document databases like MongoDB, but with the unmatched reliability and zero-configuration nature of SQLite. It's for developers who love the flexibility of NoSQL but don't want to sacrifice the transactional integrity and robustness of a traditional SQL database.

## Installation

Install SoloDB directly from the NuGet Package Manager.

```bash
dotnet add package SoloDB
```

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

### Working with Collections

A collection is a container for your documents, analogous to a table in SQL.

```csharp
// Get a strongly-typed collection. This is the recommended approach.
var products = db.GetCollection<Product>();

// Get an untyped collection for dynamic scenarios.
var untypedProducts = db.GetUntypedCollection("Product");

public class Product { /* ... */ }
```

### Indexing for Performance

Create indexes on properties to dramatically speed up query performance. Simply add the [Indexed] attribute to your model.

```csharp
using SoloDatabase.Attributes;

public class IndexedProduct
{
    public long Id { get; set; } // The primary key is always indexed.

    [Indexed(unique: true)] // Create a unique index to enforce SKU uniqueness.
    public string SKU { get; set; }

    [Indexed] // Create a non-unique index for fast category lookups.
    public string Category { get; set; }
    public decimal Price { get; set; }
}

var products = db.GetCollection<IndexedProduct>();

// This query will be very fast, using the index on 'Category'.
var books = products.Where(p => p.Category == "Books").ToList();
```

### Atomic Transactions

For operations that must either fully complete or not at all, use WithTransaction. If an exception is thrown inside the delegate, all database changes are automatically rolled back.

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

### Events API

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

For complex scenarios not easily covered by LINQ, you can execute raw SQL commands directly.

```csharp
using var pooledConnection = db.Connection.Borrow();

// Execute a command that doesn't return data
pooledConnection.Execute("UPDATE Product SET Price = Price * 1.1 WHERE Category = 'Electronics'");

// Execute a query and map the first result to a value
var highestPrice = pooledConnection.QueryFirst<decimal>("SELECT MAX(Price) FROM Product");

// Execute a query and map results to objects
var cheapProducts = pooledConnection.Query<Product>("SELECT * FROM Product WHERE Price < @MaxPrice", new { MaxPrice = 10.0 });
```

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
        
// Query all documents into an F# list
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

Create live backups of your database without interrupting application operations.

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

## FAQ

### Why create this project?

For fun, for profit, and to create a simpler, more integrated alternative to document databases like MongoDB, while retaining the unparalleled reliability and simplicity of SQLite.

### Why 1.0.0?

After over two years of production use managing a 1.5TB database with zero critical issues, SoloDB has earned its stable release designation. This is battle-tested software.
