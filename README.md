# SoloDB

todo: Add all examples to tests.

## Overview

**SoloDB** is a document database built on top of SQLite using the JSONB data type. It leverages the robustness and simplicity of SQLite to provide an efficient and lightweight database solution for handling JSON documents.

## Features

- **SQLite at the core**: Utilizes SQLite's reliability and robustness, using one of its build-in types - **JSONB**.
- **Easy to use API**: It provides an easy to use API for SQL and non SQL users.
- **File Storage**: Includes a file system functionality for storing and managing big files and directories.

## Usage

### Initializing the Database

To initialize the database, use the Instantiate method with a source string. The source string can specify either a file path or an in-memory database.

```csharp
var onDiskDB = SoloDB.Instantiate("path/to/database.db");
var inMemoryDB = SoloDB.Instantiate("memory:database-name");
```
### Working with Collections

#### Creating and Accessing Collections

To create or access a collection, use the `GetCollection` or `GetUntypedCollection` methods.

```csharp
var myCollection = db.GetCollection<MyType>();
var untypedCollection = db.GetUntypedCollection("collectionName");
```

#### Checking Collection Existence

You can check if a collection exists using the `ExistCollection` method.

```csharp
var exists = db.ExistCollection<MyType>();
```

#### Dropping Collections

To drop a collection, use the `DropCollection` or `DropCollectionIfExists` methods.

```scharp
db.DropCollection<MyType>();
```

### Transactions

SoloDB supports transactional operations. Use the `Transactionally` method to execute a function within a transaction.

```csharp
db.Transactionally(txDb => {
    var collection = txDb.GetCollection<MyType>();
    // Perform operations within the transaction
    throw null; // Simulate a fail.
});
```

### Backup and Optimization

#### Backing Up the Database

You can create a backup of the database using the `BackupTo` or `BackupVacuumTo` methods.

```csharp
db.BackupTo(otherDb);
db.BackupVacuumTo("path/to/backup.db");
```

#### Optimizing the Database

Run the `Optimize` method to optimize the database.

```csharp
db.Optimize();
```

### Example Usage

Here is an example of how to use SoloDB to manage a collection of documents:

```fsharp
[<CLIMutable>]
type MyType = { Id: SqlId; Name: string; Data: string }

let db = SoloDB.Instantiate("./mydatabase.db")
let collection = db.GetCollection<MyType>()

// Insert a document
let docId = collection.Insert({ Id = SqlId(0); Name = "Document 1"; Data = "Some data" })

// Or

let data = { Id = SqlId(0); Name = "Document 1"; Data = "Some data" }
collection.Insert(data)
printfn "%A" data.Id // 2

// Query documents
let documents = collection.Select().ToList()

// Query specific properties of documents, where Name start with 'Document'
let documentsData = collection.Select(fun d -> d.Data).Where(fun d -> d.Name.StartWith "Document").ToList()

let data = {data with  Data = "Updated data"}

// Update a document
collection.Update(data)

// Delete a document
collection.DeleteById(data.Id)
```
