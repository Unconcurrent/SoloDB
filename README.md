# <img src="./icon.png" alt="icon" width="50"/> SoloDB

SoloDB is a light, fast and robust SQL and NoSQL .NET database built on top of SQLite using the JSONB data type.

## Features

- [SQLite](https://sqlite.org/) at the core.
- Serverless, it is a .NET library.
- Simple API, see the [below](#usage).
- Thread safe using a connection pool.
- [ACID](https://www.sqlite.org/transactional.html) with [full transaction support](#transactions).
- [File System](./Tests/FileSystemTests.fs) for large files storage.
- Single data file storage on SQLite.
- [Reliable](https://sqlite.org/hirely.html) with a [WAL log file](https://www.sqlite.org/wal.html).
- Support for [indexes](https://www.sqlite.org/expridx.html) for fast search.
- LINQ-like queries.
- Direct SQL support.
- [Open source](./LICENSE.txt).
- Pretty well tested: 130+ of mostly integration tests.

## Usage

### Initializing the Database

You can specify either a file path or an in-memory database.

```csharp
var onDiskDB = SoloDB.Instantiate("path/to/database.db");
var inMemoryDB = SoloDB.Instantiate("memory:database-name");
```

### Working with Collections

#### Creating and Accessing Collections

```csharp
var myCollection = db.GetCollection<User>();
var untypedCollection = db.GetUntypedCollection("User");
```

#### Checking Collection Existence

```csharp
var exists = db.CollectionExists<User>();
```

#### Dropping Collections


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
    var collection = tx.GetCollection<long>();
    // Perform operations within the transaction.
    collection.Insert(420);    
    throw null; // Simulate a fail.
});
...
db.CollectionExists<long>() // False.
```

### Backup and Optimization

#### Backing Up the Database

You can create a backup of the database using the [`BackupTo`](https://www.sqlite.org/backup.html) or [`VacuumTo`](https://www.sqlite.org/lang_vacuum.html#vacuuminto) methods.

```csharp
db.BackupTo(otherDb);
db.VacuumTo("path/to/backup.db");
```

#### Optimizing the Database

Run the [`Optimize`](https://www.sqlite.org/pragma.html#pragma_optimize) method to optimize the database.

```csharp
db.Optimize();
```

### Example Usage

Here is an example of how to use SoloDB to manage a collection of documents in C#:


And in F#:

```fsharp
[<CLIMutable>]
type MyType = { Id: SqlId; Name: string; Data: string }

let db = SoloDB.Instantiate("./mydatabase.db")
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
collection.DeleteById(data.Id)
```
