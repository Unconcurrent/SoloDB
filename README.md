# SoloDB

SoloDB is a light, fast and robust NoSQL and SQL embedded .NET database built on top of SQLite using the [JSONB](https://sqlite.org/jsonb.html) data type.

## Features

Imagine the power of MongoDB and SQL combined.

- [SQLite](https://sqlite.org/) at the core.
- Serverless, it is a .NET library.
- Simple API, similar to [LiteDB](https://github.com/mbdavid/LiteDB), see the [below](#usage).
- Thread safe using a connection pool.
- [ACID](https://www.sqlite.org/transactional.html) with [full transaction support](#transactions).
- [File System](./Tests/FileSystemTests.fs) for large files storage.
- Support for [polymorphic](./Tests/PolymorphicTests.fs) types.
- [Reliable](https://sqlite.org/hirely.html) with a [WAL log file](https://www.sqlite.org/wal.html).
- Support for [indexes](https://www.sqlite.org/expridx.html) for fast search.
- LINQ-like queries.
- [Direct SQL support](#direct-sqlite-access-using-dapper).
- [Open source](./LICENSE.txt).
- [.NET Standard 2.0](https://learn.microsoft.com/en-us/dotnet/standard/net-standard?tabs=net-standard-2-0)
- Pretty well tested: 180+ of tests, but in the tradition of SQLite, we keep them private.

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
var myCollection = db.GetCollection<User>();
var untypedCollection = db.GetUntypedCollection("User");
```

### Checking Collection Existence

```csharp
var exists = db.CollectionExists<User>();
```

### Dropping Collections

```csharp
db.DropCollection<User>();
db.DropCollectionIfExists<User>();
db.DropCollection("User");
db.DropCollectionIfExists("User");
```

### Transactions

Use the `WithTransaction` method to execute a function within a transaction.

```csharp
db.WithTransaction(tx => {
    var collection = tx.GetCollection<ulong>();
    // Perform operations within the transaction.
    collection.Insert(420);    
    throw null; // Simulate a fail.
});
...
db.CollectionExists<long>() // False.
```


### Direct SQLite access using [Dapper](https://github.com/DapperLib/Dapper)

```csharp
using var pooledConnection = db.Connection.Borrow();
pooledConnection.Execute(
"CREATE TABLE Users (\r\n    Id INTEGER PRIMARY KEY,\r\n    Name TEXT,\r\n    Age INTEGER\r\n)");

// Create a new user
var insertSql = "INSERT INTO Users (Name, Age) VALUES (@Name, @Age) RETURNING Id;";
var userId = pooledConnection.QuerySingle<long>(insertSql, new { Name = "John Doe", Age = 30 });
Assert.IsTrue(userId > 0, "Failed to insert new user.");
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
var fs = db.FileSystem;

// Supports directories.
var directory = fs.GetOrCreateDirAt("/example");

// Supports directory metadata.
fs.SetDirectoryMetadata(directory, "key", "value");
        
fs.Upload("/example/file.txt", new MemoryStream(randomBytes));
        
// Supports sparse files.
fs.WriteAt("/example/file.txt", /* offset */ 1000000, randomBytes, /* create if inexistent */true);

// Supports file metadata.
fs.SetMetadata("/example/file.txt", "key", "value");


using var toStream = new MemoryStream();
fs.Download("/example/file.txt", toStream);
var read = fs.ReadAt("/example/file.txt", 1000000, randomBytes.Length);

Assert.IsTrue(read.SequenceEqual(randomBytes));

var file = fs.GetOrCreateAt("/example/file.txt");

// Can list files and directories.
var fileFromListing = fs.ListFilesAt("/example/").First();
        
// Automatic SHA1 hashing.
Assert.IsTrue(file.Hash.SequenceEqual(fileFromListing.Hash));

var fileByHash = fs.GetFileByHash(fileFromListing.Hash);
```


### Example Usage

Here is an example of how to use SoloDB to manage a collection of documents in C#:

#### SoloDB
```csharp
using SoloDatabase;
using SoloDatabase.Types;

public class MyType
{
    public SqlId Id { get; set; }
    public string Name { get; set; }
    public string Data { get; set; }
}


let db = new SoloDB("./mydatabase.db")
var collection = db.GetCollection<MyType>();

// Insert a document
var docId = collection.Insert(new MyType { Id = 0, Name = "Document 1", Data = "Some data" });

// Or

var data = new MyType { Id = 0, Name = "Document 1", Data = "Some data" };
collection.Insert(data);
Console.WriteLine("{0}", data.Id); // 2

// Query all documents into a C# list
var documents = collection.Select().OnAll().Enumerate().ToList();

// Query the Data property, where Name starts with 'Document'
var documentsData = collection.Select(d => d.Data).Where(d => d.Name.StartsWith("Document")).Enumerate().ToList();

data.Data = "Updated data";

// Update a document
collection.Update(data);

// Delete a document
var count = collection.DeleteById(data.Id); // 1
```
#### MongoDB
```csharp
using MongoDB.Bson;
using MongoDB.Driver;

public class MyType
{
    public ObjectId Id { get; set; }
    public string Name { get; set; }
    public string Data { get; set; }
}

var client = new MongoClient("mongodb://localhost:27017");
var database = client.GetDatabase("mydatabase");
var collection = database.GetCollection<MyType>("MyType");

// Insert a document
var newDocument = new MyType { Name = "Document 1", Data = "Some data" };
collection.InsertOne(newDocument);
Console.WriteLine(newDocument.Id);

// Query all documents into a C# list
var documents = collection.Find(FilterDefinition<MyType>.Empty).ToList();

// Query the Data property, where Name starts with 'Document'
var filter = Builders<MyType>.Filter.Regex("Name", new BsonRegularExpression("^Document"));
var documentsData = collection.Find(filter).Project(d => d.Data).ToList();

newDocument.Data = "Updated data";

// Update a document
collection.ReplaceOne(d => d.Id == newDocument.Id, newDocument);

// Delete a document
var deleteResult = collection.DeleteOne(d => d.Id == newDocument.Id);
Console.WriteLine(deleteResult.DeletedCount); // 1
```


And in F#:

#### SoloDB
```fsharp
[<CLIMutable>]
type MyType = { Id: SqlId; Name: string; Data: string }

let db = new SoloDB("./mydatabase.db")
let collection = db.GetCollection<MyType>()
        
// Insert a document
let docId = collection.Insert({ Id = SqlId(0); Name = "Document 1"; Data = "Some data" })
        
// Or
        
let data = { Id = SqlId(0); Name = "Document 1"; Data = "Some data" }
collection.Insert(data) |> ignore
printfn "%A" data.Id // 2
        
// Query all documents into a F# list
let documents = collection.Select().OnAll().ToList()
        
// Query the Data property, where Name starts with 'Document'
let documentsData = collection.Select(fun d -> d.Data).Where(fun d -> d.Name.StartsWith "Document").ToList()
        
let data = {data with  Data = "Updated data"}
        
// Update a document
collection.Update(data)
        
// Delete a document
let count = collection.DeleteById(data.Id) // 1
```
### Licence
You can read the [LICENSE.txt](./LICENSE.txt).

## (FA)Q

### Why create this project?
- For fun and profit, and to have a more simple alternative to MongoDB.

##### Footnote

###### API is subject to change.
