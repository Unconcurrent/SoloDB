# SoloDB

SoloDB is a light, fast and robust NoSQL and SQL embedded .NET database built on top of SQLite using the [JSONB](https://sqlite.org/jsonb.html) data type.

## Features

Imagine the power of MongoDB and SQL combined.

- [SQLite](https://sqlite.org/) at the core.
- Serverless, it is a .NET library.
- Simple API, similar to MongoDB, see the [below](#usage).
- Thread safe using a connection pool.
- [ACID](https://www.sqlite.org/transactional.html) with [full transaction support](#transactions).
- File System for large files storage.
- Support for polymorphic types.
- [Reliable](https://sqlite.org/hirely.html) with a [WAL log file](https://www.sqlite.org/wal.html).
- Support for [indexes](https://www.sqlite.org/expridx.html) for fast search.
- Full LINQ and IQueryable support.
- MongoDB inspired Custom ID Generation.
- Direct SQL support.
- [Open source](./LICENSE.txt).
- [.NET Standard 2.0 and 2.1](https://learn.microsoft.com/en-us/dotnet/standard/net-standard?tabs=net-standard-2-0)
- Pretty well tested: 565+ of tests, but in the tradition of SQLite, we keep them private.

## How to install

#### From NuGet
```cmd
dotnet add package SoloDB
```

## Usage

### Initializing the Database

You can specify either a file path or an in-memory database.

```csharp
using var onDiskDB = new SoloDB("path/to/database.db");
using var inMemoryDB = new SoloDB("memory:database-name");
```
### Creating and Accessing Collections

```csharp
public class User
{
// Any int64 'Id' property is automatically synced with the SQLite's primary key.
    public long Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
}

var db = new SoloDB("memory:my-app");

// Get a strongly-typed collection
var users = db.GetCollection<User>();

// Get an untyped collection (useful for dynamic scenarios)
var untypedUsers = db.GetUntypedCollection("User");
```

### Custom ID Generation

```csharp
using SoloDatabase.Attributes;
using SoloDatabase.Types;
using System.Linq;

public class MyStringIdGenerator : IIdGenerator<MyCustomIdType>
{
    public object GenerateId(ISoloDBCollection<MyCustomIdType> col, MyCustomIdType item)
    {
        var lastItem = col.OrderByDescending(x => long.Parse(x.Id)).FirstOrDefault();
        long maxId = (lastItem == null) ? 0 : long.Parse(lastItem.Id);
        return (maxId + 1).ToString();
    }

    public bool IsEmpty(object id) => string.IsNullOrEmpty(id as string);
}

public class MyCustomIdType
{
    [SoloId(typeof(MyStringIdGenerator))]
    public string Id { get; set; }
    public string Data { get; set; }
}

// ... in your application code
var customIdCollection = db.GetCollection<MyCustomIdType>();
var newItem = new MyCustomIdType { Data = "Custom ID Test" };
customIdCollection.Insert(newItem); // newItem.Id will be populated by MyStringIdGenerator
System.Console.WriteLine($"Generated ID: {newItem.Id}");
```

### Indexing Documents

```csharp
using SoloDatabase.Attributes;

public class IndexedProduct
{
    public long Id { get; set; } // Implicitly indexed by SoloDB

    [Indexed(/* unique = */ true)] // Create a unique index on SKU
    public string SKU { get; set; }

    [Indexed(false)] // Create a non-unique index on Category
    public string Category { get; set; }
    public decimal Price { get; set; }
}

// ...
var products = db.GetCollection<IndexedProduct>();
products.Insert(new IndexedProduct { SKU = "BOOK-123", Category = "Books", Price = 29.99m });

// Verify unique index constraint. This will throw a unique constraint violation exception.
try
{
    products.Insert(new IndexedProduct { SKU = "BOOK-123", Category = "Fiction", Price = 19.99m });
}
catch (Microsoft.Data.Sqlite.SqliteException ex)
{
    System.Console.WriteLine($"Successfully caught expected exception: {ex.Message}");
}

// Test querying with indexes
var book = products.FirstOrDefault(p => p.SKU == "BOOK-123");
System.Console.WriteLine($"Found book with SKU BOOK-123: Price {book.Price}");


products.Insert(new IndexedProduct { SKU = "BOOK-456", Category = "Books", Price = 14.99m });
var booksInCategory = products.Where(p => p.Category == "Books").ToList();
System.Console.WriteLine($"Found {booksInCategory.Count} books in the 'Books' category.");

```

### Transactions

Use the `WithTransaction` method to execute a function within a transaction.

```csharp
try
{
    var _result = db.WithTransaction<object/* result type */>(tx => {
        var collection = tx.GetCollection<ulong>();
        // Perform operations within the transaction.
        collection.Insert(420);
        throw new System.OperationCanceledException("Simulating a rollback."); // Simulate a fail.
    });
} catch (System.OperationCanceledException) {}

System.Console.WriteLine($"Collection exists after rollback: {db.CollectionExists<ulong>()}"); // False
```

### Polymorphic Types
```csharp
public abstract class Shape
{
    public long Id { get; set; }
    public string Color { get; set; }
    public abstract double CalculateArea();
}

public class Circle : Shape
{
    public double Radius { get; set; }
    public override double CalculateArea() => System.Math.PI * Radius * Radius;
}

public class Rectangle : Shape
{
    public double Width { get; set; }
    public double Height { get; set; }
    public override double CalculateArea() => Width * Height;
}

// ...
var shapes = db.GetCollection<Shape>(); // Store as the base type 'Shape'

shapes.Insert(new Circle { Color = "Red", Radius = 5.0 });
shapes.Insert(new Rectangle { Color = "Blue", Width = 4.0, Height = 6.0 });

// Get all circles
var circles = shapes.OfType<Circle>().ToList();
foreach (var circle in circles)
{
    System.Console.WriteLine($"Red Circle - Radius: {circle.Radius}, Area: {circle.CalculateArea()}");
}

// Get all shapes with Color "Blue"
var blueShapes = shapes.Where(s => s.Color == "Blue").ToList();
foreach (var shape in blueShapes)
{
    if (shape is Rectangle rect)
    {
        System.Console.WriteLine($"Blue Rectangle - Width: {rect.Width}, Height: {rect.Height}, Area: {rect.CalculateArea()}");
    }
}

```


### Direct SQLite access using the build-in SoloDatabase.SQLiteTools.IDbConnectionExtensions.

```csharp
using static SoloDatabase.SQLiteTools.IDbConnectionExtensions;
...

using var pooledConnection = db.Connection.Borrow();
pooledConnection.Execute(
    "CREATE TABLE Users (Id INTEGER PRIMARY KEY, Name TEXT, Age INTEGER)");

var insertSql = "INSERT INTO Users (Name, Age) VALUES (@Name, @Age) RETURNING Id;";
var userId = pooledConnection.QueryFirst<long>(insertSql, new { Name = "John Doe", Age = 30 });

Assert.IsTrue(userId > 0, "Failed to insert new user and get a valid ID.");

var queriedAge = pooledConnection.QueryFirst<int>("SELECT Age FROM Users WHERE Name = 'John Doe'");
Assert.AreEqual(30, queriedAge);
```


### Backing Up the Database

You can create a backup of the database using the [`BackupTo`](https://www.sqlite.org/backup.html) or [`VacuumTo`](https://www.sqlite.org/lang_vacuum.html#vacuuminto) methods.

```csharp
db.BackupTo(otherDb);
db.VacuumTo("path/to/backup.db");
```


### Optimizing the Database

The [`Optimize`](https://www.sqlite.org/pragma.html#pragma_optimize) method can optimize the database using statistically information, it runs automatically on startup.

```csharp
db.Optimize();
```
### File storage

```csharp
using SoloDatabase;
using SoloDatabase.FileStorage;
using System.IO;
using System.Text;

var fs = db.FileSystem;
var randomBytes = new byte[256];
System.Random.Shared.NextBytes(randomBytes);

// Create a directory and set metadata
var directory = fs.GetOrCreateDirAt("/my_documents/reports");
fs.SetDirectoryMetadata(directory, "Sensitivity", "Confidential");
var dirInfo = fs.GetDirAt("/my_documents/reports");
System.Console.WriteLine($"Directory '/my_documents/reports' metadata 'Sensitivity': {dirInfo.Metadata["Sensitivity"]}");

// Upload a file and set metadata
string filePath = "/my_documents/reports/annual_report.txt";
using (var ms = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes("This is a test report.")))
{
    fs.Upload(filePath, ms);
}
fs.SetMetadata(filePath, "Author", "Jane Doe");
fs.SetFileCreationDate(filePath, System.DateTimeOffset.UtcNow.AddDays(-7));
var fileInfo = fs.GetAt(filePath);
System.Console.WriteLine($"File '{fileInfo.Name}' author: {fileInfo.Metadata["Author"]}");

// Write at a specific offset (sparse file)
string sparseFilePath = "/my_documents/sparse_file.dat";
fs.WriteAt(sparseFilePath, 1024 * 1024, randomBytes); // Write at 1MB offset
var readData = fs.ReadAt(sparseFilePath, 1024 * 1024, randomBytes.Length);
System.Console.WriteLine($"Sparse file write/read successful: {System.Linq.Enumerable.SequenceEqual(randomBytes, readData)}");

// Download file content
using (var targetStream = new System.IO.MemoryStream())
{
    fs.Download(filePath, targetStream);
    targetStream.Position = 0;
    string content = new System.IO.StreamReader(targetStream).ReadToEnd();
    System.Console.WriteLine($"Downloaded content: {content}");
}

// Recursively list entries
var entries = fs.RecursiveListEntriesAt("/my_documents");
System.Console.WriteLine($"Found {entries.Count} entries recursively under /my_documents.");

// Move a file to a new location (and rename it)
fs.MoveFile(filePath, "/archive/annual_report_2023.txt");
bool originalExists = fs.Exists(filePath); // false
bool newExists = fs.Exists("/archive/annual_report_2023.txt"); // true
System.Console.WriteLine($"Original file exists after move: {originalExists}. New file exists: {newExists}");

// Bulk upload multiple files
var bulkFiles = new System.Collections.Generic.List<SoloDatabase.FileStorage.BulkFileData>
{
    // The constructor allows setting path, data, and optional timestamps.
    new("/bulk_uploads/file1.log", System.Text.Encoding.UTF8.GetBytes("Log entry 1"), null, null),
    new("/bulk_uploads/images/pic.jpg", randomBytes, null, null)
};
fs.UploadBulk(bulkFiles);
System.Console.WriteLine($"Bulk upload successful. File exists: {fs.Exists("/bulk_uploads/images/pic.jpg")}");

// Use SoloFileStream for controlled writing
using (var fileStream = fs.OpenOrCreateAt("/important_data/critical.bin"))
{
    fileStream.Write(randomBytes, 0, 10);
}
var criticalFileInfo = fs.GetAt("/important_data/critical.bin");
System.Console.WriteLine($"SoloFileStream created file with size: {criticalFileInfo.Size} and a valid hash.");

```


### Example Usage

Here is an example of how to use SoloDB to manage a collection of documents in C#:

#### SoloDB
```csharp
using SoloDatabase;
using SoloDatabase.Attributes;
using System.Linq;

public class MyDataType
{
    public long Id { get; set; }
    [Indexed(/* unique = */ false)]
    public string Name { get; set; }
    public string Data { get; set; }
}

// using var db = new SoloDB(...);
// var db = new SoloDB("./mydatabase.db");
var collection = db.GetCollection<MyDataType>();

// Insert a document
var newDoc = new MyDataType { Name = "Document 1", Data = "Some data" };
collection.Insert(newDoc); // Id will be auto-generated and set on newDoc.Id
long docId = newDoc.Id;
System.Console.WriteLine($"Inserted document with ID: {docId}");


// Another way to insert and get the ID back if the object's Id property is not yet set
var dataToInsert = new MyDataType { Name = "Document 2", Data = "More data" };
collection.Insert(dataToInsert); // dataToInsert.Id is now populated
System.Console.WriteLine($"Inserted document, ID from object: {dataToInsert.Id}");

// Query all documents into a C# list
var allDocuments = collection.ToList();
System.Console.WriteLine($"Total documents: {allDocuments.Count}");

// Query the Data property, where Name starts with 'Document'
var documentsData = collection.Where(d => d.Name.StartsWith("Document"))
                              .Select(d => d.Data)
                              .ToList();

// Update a document
var docToUpdate = collection.GetById(dataToInsert.Id); // Retrieve the second document
docToUpdate.Data = "Updated data for Document 2";
collection.Update(docToUpdate);

// Verify the update
var updatedDoc = collection.GetById(dataToInsert.Id);
System.Console.WriteLine($"Updated data: {updatedDoc.Data}"); // "Updated data for Document 2"

// Delete the first document by its primary key
int deleteCount = collection.Delete(docId); 
System.Console.WriteLine($"Documents deleted: {deleteCount}"); // 1

// Verify the final count
System.Console.WriteLine($"Final document count: {collection.Count()}"); // 1

```

And a simple one in F#:

#### SoloDB
```fsharp
[<CLIMutable>]
type MyType = { Id: int64; Name: string; Data: string }

let db = new SoloDB("./mydatabase.db")
let collection = db.GetCollection<MyType>()
        
// Insert a document
let docId = collection.Insert({ Id = 0; Name = "Document 1"; Data = "Some data" })
        
// Or
        
let data = { Id = 0; Name = "Document 1"; Data = "Some data" }
collection.Insert(data) |> ignore
printfn "%A" data.Id // 2
        
// Query all documents into a F# list
let documents = collection.ToList()
        
// Query the Data property, where Name starts with 'Document'
let documentsData = collection.Where(fun d -> d.Name.StartsWith "Document").Select(fun d -> d.Data).ToList()
        
let data = {data with  Data = "Updated data"}
        
// Update a document
collection.Update(data)
        
// Delete a document
let count = collection.Delete(data.Id) // 1
```
### Licence
You can read the [LICENSE.txt](./LICENSE.txt).

## (FA)Q

### Why create this project?
- For fun and profit, and to have a more simple alternative to MongoDB with the reliability of SQLite.

##### Footnote

###### API is subject to change.
