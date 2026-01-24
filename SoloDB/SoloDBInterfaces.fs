namespace SoloDatabase

open System.Linq.Expressions
open System.Runtime.CompilerServices
open System.Linq
open System.Data

type ISoloDBEvents =
    abstract member OnInserting<'T>: (*collectionInstance: *) ISoloDBCollection<'T> -> (*item: *) Lazy<'T> -> unit
    abstract member OnUpdating<'T>: (*collectionInstance: *) ISoloDBCollection<'T> -> (*oldItem: *) Lazy<'T> -> (*newItem: *) Lazy<'T> -> unit
    abstract member OnDeleting<'T>: (*collectionInstance: *) ISoloDBCollection<'T> -> (*item: *) Lazy<'T> -> unit

/// <summary>
/// Represents a typed collection within a SoloDB database instance that supports LINQ querying and CRUD operations.
/// </summary>
/// <typeparam name="T">The type of entities stored in this collection. Must be serializable.</typeparam>
/// <remarks>
/// This interface extends <see cref="IOrderedQueryable{T}"/> to provide full LINQ support while adding
/// database-specific operations like Insert, Update, Delete, and indexing capabilities.
/// Supports polymorphic storage when <typeparamref name="T"/> is an abstract class or interface.
/// </remarks>
/// <seealso cref="IOrderedQueryable{T}"/>
and ISoloDBCollection<'T> =
    inherit IOrderedQueryable<'T>

    /// <summary>
    /// Gets the name of the collection within the database.
    /// </summary>
    /// <value>
    /// A string representing the collection name, typically derived from the type name <typeparamref name="T"/>.
    /// </value>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    /// Console.WriteLine($"Collection name: {collection.Name}"); // Outputs: "User"
    /// </code>
    /// </example>
    abstract member Name: string with get

    /// <summary>
    /// Gets a value indicating whether this collection is currently participating in a database transaction.
    /// </summary>
    /// <value>
    /// <c>true</c> if the collection is within a transaction context; otherwise, <c>false</c>.
    /// </value>
    /// <remarks>
    /// When <c>true</c>, all operations are deferred until the transaction is committed or rolled back.
    /// </remarks>
    /// <seealso cref="SoloDB.WithTransaction"/>
    abstract member InTransaction: bool with get

    /// <summary>
    /// Gets a value indicating whether type information is included in stored documents.
    /// </summary>
    /// <value>
    /// <c>true</c> if type information is stored for polymorphic support; otherwise, <c>false</c>.
    /// </value>
    /// <remarks>
    /// Essential for abstract types and interfaces to enable proper deserialization of derived types.
    /// Can be forced using <see cref="SoloDatabase.PolymorphicAttribute"/>.
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    ///
    /// // For abstract base class
    /// ISoloDBCollection&lt;Animal&gt; animals = db.GetCollection&lt;Animal&gt;();
    /// Console.WriteLine($"Includes type info: {animals.IncludeType}"); // true
    ///
    /// // For concrete class
    /// ISoloDBCollection&lt;User&gt; users = db.GetCollection&lt;User&gt;();
    /// Console.WriteLine($"Includes type info: {users.IncludeType}"); // false
    /// </code>
    /// </example>
    abstract member IncludeType: bool with get

    /// <summary>
    /// Inserts a new entity into the collection and returns the auto-generated unique identifier.
    /// </summary>
    /// <param name="item">The entity to insert. Any Id fields will be set.</param>
    /// <returns>
    /// The auto-generated unique identifier assigned to the inserted entity.
    /// </returns>
    /// <exception cref="System.InvalidOperationException">Thrown when item has an invalid Id value or generator.</exception>
    /// <exception cref="Microsoft.Data.Sqlite.SqliteException">Thrown when the item would violate constraints.</exception>
    /// <remarks>
    /// <para>Uses custom ID generators specified by <c>SoloIdAttribute</c> or defaults to <c>int64</c> auto-increment. </para>
    /// <para>Automatically creates indexes for properties marked with indexing attributes.</para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// User user = new User { Id = 0L, Name = "Alice", Email = "alice@example.com" };
    /// long id = collection.Insert(user);
    /// Console.WriteLine($"Inserted with ID: {id}"); // Outputs auto-generated ID
    /// Console.WriteLine($"User ID updated: {user.Id}"); // Same as returned ID
    /// </code>
    /// </example>
    /// <seealso cref="InsertBatch"/>
    /// <seealso cref="InsertOrReplace"/>
    abstract member Insert: item: 'T -> int64

    /// <summary>
    /// Inserts a new entity or replaces an existing one based on unique indexes, returning the assigned identifier.
    /// </summary>
    /// <param name="item">The entity to insert or replace.</param>
    /// <returns>
    /// The unique identifier of the inserted or replaced entity.
    /// </returns>
    /// <exception cref="Microsoft.Data.Sqlite.SqliteException">Thrown when the item would violate constraints.</exception>
    /// <remarks>
    /// <para>Replacement occurs when the entity matches an existing record on any unique index.</para>
    /// <para>If no match is found, performs a standard insert operation.</para>
    /// <para>Useful for upsert scenarios where you want to avoid duplicate constraint violations.</para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // First insert
    /// User user = new User { Id = 0L, Email = "alice@example.com", Name = "Alice" };
    /// long id1 = collection.InsertOrReplace(user);
    /// Console.WriteLine($"First insert/replace ID: {id1}");
    ///
    /// // Later upsert with same email (assuming unique index on Email)
    /// User updatedUser = new User { Id = 0L, Email = "alice@example.com", Name = "Alice Smith" };
    /// long id2 = collection.InsertOrReplace(updatedUser); // id2 == id1
    /// Console.WriteLine($"Second insert/replace ID: {id2}");
    /// </code>
    /// </example>
    /// <seealso cref="Insert"/>
    /// <seealso cref="EnsureUniqueAndIndex"/>
    abstract member InsertOrReplace: item: 'T -> int64

    /// <summary>
    /// Inserts multiple entities in a single atomic transaction, returning their assigned identifiers.
    /// </summary>
    /// <param name="items">The sequence of entities to insert.</param>
    /// <returns>
    /// A list containing the unique identifiers assigned to each inserted entity, in the same order as the input sequence.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="items"/> is <c>null</c>.</exception>
    /// <exception cref="System.InvalidOperationException">Thrown when item has an invalid Id value or generator.</exception>
    /// <exception cref="Microsoft.Data.Sqlite.SqliteException">Thrown when any item would violate constraints.</exception>
    /// <remarks>
    /// <para>All insertions occur within a single transaction - if any insertion fails, all are rolled back.</para>
    /// <para>Significantly more efficient than multiple individual <see cref="Insert"/> calls.</para>
    /// <para>Empty sequences are handled gracefully and return an empty list.</para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// List&lt;User&gt; users = new List&lt;User&gt;
    /// {
    ///      new User { Id = 0L, Name = "Alice", Email = "alice@example.com" },
    ///      new User { Id = 0L, Name = "Bob", Email = "bob@example.com" },
    ///      new User { Id = 0L, Name = "Charlie", Email = "charlie@example.com" }
    /// };
    /// IList&lt;long&gt; ids = collection.InsertBatch(users);
    /// Console.WriteLine($"Inserted {users.Count} users with IDs: {string.Join(", ", ids)}");
    /// </code>
    /// </example>
    /// <seealso cref="Insert"/>
    /// <seealso cref="InsertOrReplaceBatch"/>
    abstract member InsertBatch: items: 'T seq -> System.Collections.Generic.IList<int64>

    /// <summary>
    /// Inserts or replaces multiple entities based on unique indexes in a single atomic transaction.
    /// </summary>
    /// <param name="items">The sequence of entities to insert or replace.</param>
    /// <returns>
    /// A list containing the unique identifiers assigned to each entity, in the same order as the input sequence.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="items"/> is <c>null</c>.</exception>
    /// <exception cref="System.InvalidOperationException">Thrown when item has an invalid Id value or generator.</exception>
    /// <exception cref="Microsoft.Data.Sqlite.SqliteException">Thrown when any item would violate constraints.</exception>
    /// <remarks>
    /// <para>Combines the efficiency of batch operations with upsert semantics.</para>
    /// <para>All operations occur within a single transaction for consistency.</para>
    /// <para>Throws if any entity has a pre-assigned non-zero ID to prevent confusion about intended behavior.</para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// List&lt;User&gt; users = new List&lt;User&gt;
    /// {
    ///      new User { Id = 0L, Email = "alice@example.com", Name = "Alice" },       // Insert
    ///      new User { Id = 0L, Email = "existing@example.com", Name = "Updated" }  // Replace existing
    /// };
    /// IList&lt;long&gt; ids = collection.InsertOrReplaceBatch(users);
    /// Console.WriteLine($"Inserted/Replaced {users.Count} users with IDs: {string.Join(", ", ids)}");
    /// </code>
    /// </example>
    /// <seealso cref="InsertOrReplace"/>
    /// <seealso cref="InsertBatch"/>
    abstract member InsertOrReplaceBatch: items: 'T seq -> System.Collections.Generic.IList<int64>

    /// <summary>
    /// Attempts to retrieve an entity by its unique identifier, returning <c>None</c> if not found.
    /// </summary>
    /// <param name="id">The unique identifier of the entity to retrieve.</param>
    /// <returns>
    /// <c>Some(entity)</c> if found; otherwise, <c>None</c>.
    /// </returns>
    /// <remarks>
    /// <para>Utilizes the primary key index for optimal O(log n) performance.</para>
    /// </remarks>
    /// <seealso cref="GetById"/>
    abstract member TryGetById: id: int64 -> 'T option

    /// <summary>
    /// Retrieves an entity by its unique identifier, throwing an exception if not found.
    /// </summary>
    /// <param name="id">The unique identifier of the entity to retrieve.</param>
    /// <returns>
    /// The entity with the specified identifier.
    /// </returns>
    /// <exception cref="System.Collections.Generic.KeyNotFoundException">Thrown when no entity with the specified ID exists.</exception>
    /// <remarks>
    /// <para>Utilizes the primary key index for optimal O(log n) performance. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// try
    /// {
    ///     User user = collection.GetById(42L);
    ///     Console.WriteLine($"Found user: {user.Name}");
    /// }
    /// catch (KeyNotFoundException)
    /// {
    ///     Console.WriteLine("User not found");
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="TryGetById"/>
    abstract member GetById: id: int64 -> 'T

    /// <summary>
    /// Attempts to retrieve an entity using a custom identifier type, returning <c>None</c> if not found.
    /// </summary>
    /// <param name="id">The custom identifier of the entity to retrieve.</param>
    /// <typeparam name="IdType">The type of the custom identifier, such as <c>string</c>, <c>Guid</c>, or composite key types.</typeparam>
    /// <returns>
    /// <c>Some(entity)</c> if found; otherwise, <c>None</c>.
    /// </returns>
    /// <remarks>
    /// <para>The <typeparamref name="IdType"/> must match the type specified in the entity's <c>SoloIdAttribute</c>. </para>
    /// </remarks>
    /// <seealso cref="GetById{IdType}"/>
    /// <seealso cref="SoloDatabase.SoloIdAttribute"/>
    abstract member TryGetById<'IdType when 'IdType : equality>: id: 'IdType -> 'T option

    /// <summary>
    /// Retrieves an entity using a custom identifier type, throwing an exception if not found.
    /// </summary>
    /// <param name="id">The custom identifier of the entity to retrieve.</param>
    /// <typeparam name="IdType">The type of the custom identifier.</typeparam>
    /// <returns>
    /// The entity with the specified custom identifier.
    /// </returns>
    /// <exception cref="System.Collections.Generic.KeyNotFoundException">Thrown when no entity with the specified ID exists.</exception>
    /// <remarks>
    /// <para>For entities using custom ID generators. </para>
    /// <para>The type parameter must exactly match the ID type defined in the entity. </para>
    /// <para>Utilizes a sqlite index for optimal O(log n) performance. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// try
    /// {
    ///     // Retrieve by string ID
    ///     User user = collection.GetById&lt;string&gt;("user_abc123");
    ///     Console.WriteLine($"Found user by string ID: {user.Name}");
    ///
    ///     // Or retrieve by GUID
    ///     Guid guid = Guid.Parse(...);
    ///     User userByGuid = collection.GetById&lt;Guid&gt;(guid);
    ///     Console.WriteLine($"Found user by GUID: {userByGuid.Name}");
    /// }
    /// catch (KeyNotFoundException)
    /// {
    ///     Console.WriteLine("User not found with the specified custom ID.");
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="TryGetById{IdType}"/>
    abstract member GetById<'IdType when 'IdType : equality>: id: 'IdType -> 'T

    /// <summary>
    /// Creates a non-unique index on the specified property to optimize query performance.
    /// </summary>
    /// <param name="expression">A lambda expression identifying the property to index.</param>
    /// <typeparam name="R">The type of the property being indexed.</typeparam>
    /// <returns>
    /// The result of the Execute command on SQLite.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="expression"/> is <c>null</c>.</exception>
    /// <exception cref="System.ArgumentException">Thrown when the expression doesn't reference a valid property.</exception>
    /// <remarks>
    /// <para>Indexes dramatically improve query performance for filtered and sorted operations. </para>
    /// <para>Non-unique indexes allow multiple documents to have the same indexed value. </para>
    /// <para>Index creation is idempotent - calling multiple times has no adverse effects. </para>
    /// <para>Indexes are persistent and maintained automatically during Insert/Update/Delete operations. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // Create index on Name property for faster searches
    /// int indexEntries = collection.EnsureIndex(u => u.Name);
    /// Console.WriteLine($"Index on Name created with {indexEntries} entries.");
    ///
    /// // Create composite index (if supported)
    /// collection.EnsureIndex(u => new { u.City, u.Country });
    ///
    /// // Now queries like this will be much faster:
    /// List&lt;User&gt; users = collection.Where(u => u.Name == "Alice").ToList();
    /// </code>
    /// </example>
    /// <seealso cref="EnsureUniqueAndIndex"/>
    /// <seealso cref="DropIndexIfExists"/>
    abstract member EnsureIndex<'R>: expression: Expression<System.Func<'T, 'R>> -> int

    /// <summary>
    /// Creates a unique constraint and index on the specified property, preventing duplicate values.
    /// </summary>
    /// <param name="expression">A lambda expression identifying the property that must have unique values.</param>
    /// <typeparam name="R">The type of the property being indexed.</typeparam>
    /// <returns>
    /// The number of index entries created.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="expression"/> is <c>null</c>.</exception>
    /// <exception cref="System.ArgumentException">Thrown when the expression references the already indexed Id property, the expression contains variables, or the expression is invalid in any other cases.</exception>
    /// <exception cref="System.InvalidOperationException">Thrown when expression is not contant in relation to the parameter only.</exception>
    /// <remarks>
    /// <para>Combines the performance benefits of indexing with data integrity enforcement. </para>
    /// <para>Prevents insertion or updates that would create duplicate values. </para>
    /// <para>Essential for implementing business rules like unique email addresses or usernames. </para>
    /// <para>Can be used with <see cref="InsertOrReplace"/> for upsert operations. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // Ensure email addresses are unique
    /// collection.EnsureUniqueAndIndex(u => u.Email);
    /// Console.WriteLine("Unique index on Email ensured.");
    ///
    /// // This will succeed
    /// collection.Insert(new User { Id = 0L, Email = "alice@example.com", Name = "Alice" });
    ///
    /// // This will throw InvalidOperationException due to duplicate email
    /// try
    /// {
    ///     collection.Insert(new User { Id = 0L, Email = "alice@example.com", Name = "Another Alice" });
    /// }
    /// catch (InvalidOperationException)
    /// {
    ///     Console.WriteLine("Duplicate email address detected as expected.");
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="EnsureIndex"/>
    /// <seealso cref="InsertOrReplace"/>
    abstract member EnsureUniqueAndIndex<'R>: expression: Expression<System.Func<'T, 'R>> -> int

    /// <summary>
    /// Removes an existing index on the specified property if it exists.
    /// </summary>
    /// <param name="expression">A lambda expression identifying the property whose index should be removed.</param>
    /// <typeparam name="R">The type of the property whose index is being removed.</typeparam>
    /// <returns>
    /// The number of index entries removed, or 0 if the index didn't exist.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="expression"/> is <c>null</c>.</exception>
    /// <remarks>
    /// <para>Safe operation that doesn't fail if the index doesn't exist.</para>
    /// <para>Useful for schema migrations or performance tuning scenarios.</para>
    /// <para>Removing an index will slow down queries that rely on it but frees up storage space.</para>
    /// <para>Cannot remove unique constraints that would result in data integrity violations.</para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // Remove index on Name property
    /// int removedEntries = collection.DropIndexIfExists(u => u.Name);
    /// if (removedEntries > 0)
    /// {
    ///     Console.WriteLine($"Removed index with {removedEntries} entries");
    /// }
    /// else
    /// {
    ///     Console.WriteLine("Index did not exist");
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="EnsureIndex"/>
    /// <seealso cref="EnsureUniqueAndIndex"/>
    abstract member DropIndexIfExists<'R>: expression: Expression<System.Func<'T, 'R>> -> int

    /// <summary>
    /// Ensures that all indexes defined by attributes are created, including those added after collection creation.
    /// </summary>
    /// <remarks>
    /// <para>Automatically processes all properties marked with indexing attributes like <c>SoloIndexAttribute</c>. </para>
    /// <para>Useful for handling schema evolution where new indexes are added to existing types. </para>
    /// <para>Should be called after adding new indexing attributes to ensure they take effect. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // After adding [SoloIndex] attribute to a property
    /// collection.EnsureAddedAttributeIndexes();
    /// Console.WriteLine("All attribute-defined indexes are now guaranteed to exist.");
    /// </code>
    /// </example>
    /// <seealso cref="SoloDatabase.SoloIndexAttribute"/>
    /// <seealso cref="EnsureIndex"/>
    abstract member EnsureAddedAttributeIndexes: unit -> unit

    /// <summary>
    /// Returns the internal SQLite connection object for advanced operations.
    /// </summary>
    /// <returns>
    /// The underlying <see cref="IDbConnection"/> instance.
    /// </returns>
    /// <remarks>
    /// <para><strong>WARNING:</strong> This method is not intended for public usage and should be avoided.</para>
    /// </remarks>
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    abstract member GetInternalConnection: unit -> Microsoft.Data.Sqlite.SqliteConnection

    /// <summary>
    /// Updates an existing entity in the database based on its identifier.
    /// </summary>
    /// <param name="item">The entity to update, which must have a valid non-zero identifier.</param>
    /// <exception cref="System.InvalidOperationException">Thrown when the entity has a bad identifier.</exception>
    /// <exception cref="System.Collections.Generic.KeyNotFoundException">Thrown when no entity with the specified ID exists.</exception>
    /// <exception cref="Microsoft.Data.Sqlite.SqliteException">Thrown when the item would violate constraints.</exception>
    /// <remarks>
    /// <para>Replaces the entire entity in the database with the provided instance.</para>
    /// <para>The entity's identifier must be populated and match an existing record.</para>
    /// <para>Maintains referential integrity and enforces unique constraints.</para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // Retrieve, modify, and update
    /// User userToUpdate = collection.GetById(42L); // Assume user with ID 42 exists
    /// if (userToUpdate != null)
    /// {
    ///     userToUpdate.Name = "Updated Name";
    ///     userToUpdate.Email = "newemail@example.com";
    ///     collection.Update(userToUpdate); // Entire record is replaced
    ///     Console.WriteLine($"User with ID 42 updated to Name: {userToUpdate.Name}, Email: {userToUpdate.Email}");
    /// }
    ///
    /// // For mutable records (C# classes are mutable by default)
    /// User userData = collection.GetById(42L); // Assume user with ID 42 exists
    /// if (userData != null)
    /// {
    ///     userData.Data = "New data";
    ///     collection.Update(userData);
    ///     Console.WriteLine($"User with ID 42 data updated: {userData.Data}");
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="ReplaceOne"/>
    /// <seealso cref="ReplaceMany"/>
    abstract member Update: item: 'T -> unit

    /// <summary>
    /// Deletes an entity by its unique identifier.
    /// </summary>
    /// <param name="id">The unique identifier of the entity to delete.</param>
    /// <returns>
    /// The number of entities deleted (0 if not found, 1 if successfully deleted).
    /// </returns>
    /// <remarks>
    /// <para>Safe operation that doesn't throw exceptions when the entity doesn't exist. </para>
    /// <para>Returns 0 if no entity with the specified ID exists. </para>
    /// <para>Returns 1 if the entity was successfully deleted. </para>
    /// <para>Automatically maintains index consistency. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// int deleteCount = collection.Delete(42L);
    /// switch (deleteCount)
    /// {
    ///     case 0:
    ///         Console.WriteLine("Entity with ID 42 was not found");
    ///         break;
    ///     case 1:
    ///         Console.WriteLine("Entity with ID 42 was successfully deleted");
    ///         break;
    ///     default:
    ///         Console.WriteLine("Unexpected result"); // Should never happen
    ///         break;
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="DeleteOne"/>
    /// <seealso cref="DeleteMany"/>
    abstract member Delete: id: int64 -> int

    /// <summary>
    /// Deletes an entity using a custom identifier type.
    /// </summary>
    /// <param name="id">The custom identifier of the entity to delete.</param>
    /// <typeparam name="IdType">The type of the custom identifier.</typeparam>
    /// <returns>
    /// The number of entities deleted (0 if not found, 1 if successfully deleted).
    /// </returns>
    /// <remarks>
    /// <para>For entities using custom ID types like <c>string</c>, <c>Guid</c>, or composite keys. </para>
    /// <para>The <typeparamref name="IdType"/> must match the entity's configured ID type. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // Delete by string ID
    /// int count1 = collection.Delete&lt;string&gt;("user_abc123");
    /// Console.WriteLine($"Deleted {count1} entity by string ID.");
    ///
    /// // Delete by GUID
    /// Guid guidToDelete = Guid.Parse("550e8400-e29b-41d4-a716-446655440000"); // Replace with an actual GUID
    /// int count2 = collection.Delete&lt;Guid&gt;(guidToDelete);
    /// Console.WriteLine($"Deleted {count2} entity by GUID.");
    /// </code>
    /// </example>
    /// <seealso cref="Delete"/>
    abstract member Delete<'IdType when 'IdType : equality>: id: 'IdType -> int

    /// <summary>
    /// Deletes all entities that match the specified filter condition.
    /// </summary>
    /// <param name="filter">A lambda expression defining the deletion criteria.</param>
    /// <returns>
    /// The number of entities that were deleted.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="filter"/> is <c>null</c>.</exception>
    /// <remarks>
    /// <para>Performs a bulk delete operation in a single database command. </para>
    /// <para>More efficient than multiple individual delete operations. </para>
    /// <para>Returns 0 if no entities match the filter criteria. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // Delete all inactive users
    /// int deletedCount = collection.DeleteMany(u => u.IsActive == false);
    /// Console.WriteLine($"Deleted {deletedCount} inactive users");
    ///
    /// // Delete all users from a specific city
    /// int cityDeletions = collection.DeleteMany(u => u.City == "Old City");
    /// Console.WriteLine($"Deleted {cityDeletions} users from Old City");
    /// </code>
    /// </example>
    /// <seealso cref="DeleteOne"/>
    /// <seealso cref="Delete"/>
    abstract member DeleteMany: filter: Expression<System.Func<'T, bool>> -> int

    /// <summary>
    /// Deletes the first entity that matches the specified filter condition.
    /// </summary>
    /// <param name="filter">A lambda expression defining the deletion criteria.</param>
    /// <returns>
    /// The number of entities deleted (0 if no match found, 1 if successfully deleted).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="filter"/> is <c>null</c>.</exception>
    /// <remarks>
    /// <para>Stops after deleting the first matching entity, even if multiple matches exist. </para>
    /// <para>The order of "first" depends on the underlying storage order, not insertion order. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// // Delete one user with a specific email
    /// int deleteCount = collection.DeleteOne(u => u.Email == "old@example.com");
    /// if (deleteCount == 1)
    /// {
    ///     Console.WriteLine("User deleted successfully");
    /// }
    /// else
    /// {
    ///     Console.WriteLine("No user found with that email");
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="DeleteMany"/>
    /// <seealso cref="Delete"/>
    abstract member DeleteOne: filter: Expression<System.Func<'T, bool>> -> int

    /// <summary>
    /// Replaces all entities matching the filter condition with the provided entity.
    /// </summary>
    /// <param name="filter">A lambda expression defining which entities to replace.</param>
    /// <param name="item">The replacement entity data.</param>
    /// <returns>
    /// The number of entities that were replaced.
    /// </returns>
    /// <exception cref="Microsoft.Data.Sqlite.SqliteException">Thrown when the replacement would violate constraints.</exception>
    /// <remarks>
    /// <para>Each matching entity is completely replaced with the provided item data. </para>
    /// <para>The replacement item's ID is ignored - each replaced entity retains its original ID. </para>
    /// <para>More efficient than manual update loops for bulk replacement scenarios. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// User template = new User { Id = 0L, Status = "Updated", LastModified = DateTime.UtcNow };
    ///
    /// // Replace all entities with old status
    /// int replaceCount = collection.ReplaceMany(
    ///      u => u.Status == "Pending",
    ///      template
    /// );
    /// Console.WriteLine($"Updated {replaceCount} entities to new status");
    /// </code>
    /// </example>
    /// <seealso cref="ReplaceOne"/>
    /// <seealso cref="Update"/>
    abstract member ReplaceMany: filter: Expression<System.Func<'T, bool>> * item: 'T -> int

    /// <summary>
    /// Replaces the first entity matching the filter condition with the provided entity.
    /// </summary>
    /// <param name="filter">A lambda expression defining which entity to replace.</param>
    /// <param name="item">The replacement entity data.</param>
    /// <returns>
    /// The number of entities replaced (0 if no match found, 1 if successfully replaced).
    /// </returns>
    /// <exception cref="Microsoft.Data.Sqlite.SqliteException">Thrown when the replacement would violate constraints.</exception>
    /// <remarks>
    /// <para>Replaces only the first matching entity, preserving its original ID. </para>
    /// <para>The replacement item's ID field is ignored during the operation. </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// SoloDB db = new SoloDB(...);
    /// ISoloDBCollection&lt;User&gt; collection = db.GetCollection&lt;User&gt;();
    ///
    /// User newData = new User { Id = 0L, Name = "Updated Name", Email = "new@example.com" };
    ///
    /// // Replace specific user
    /// int replaceCount = collection.ReplaceOne(
    ///      u => u.Email == "old@example.com",
    ///      newData
    /// );
    /// if (replaceCount == 1)
    /// {
    ///     Console.WriteLine("User successfully replaced");
    /// }
    /// else
    /// {
    ///     Console.WriteLine("No user found with that email to replace");
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="ReplaceMany"/>
    /// <seealso cref="Update"/>
    abstract member ReplaceOne: filter: Expression<System.Func<'T, bool>> * item: 'T -> int

    /// <summary>
    /// Performs a partial update on all entities that match the specified filter condition.
    /// </summary>
    /// <param name="transform">An array of lambda expressions defining the modifications to apply (e.g., <code>item.Set(value)</code> method calls).</param>
    /// <param name="filter">A lambda expression defining the criteria for which entities to update.</param>
    /// <returns>
    /// The number of entities that were updated.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="transform"/> or <paramref name="filter"/> is <c>null</c>.</exception>
    /// <exception cref="Microsoft.Data.Sqlite.SqliteException">Thrown when the update would violate a database constraint (e.g., a unique index).</exception>
    /// <remarks>
    /// <para>It is required to perform only one update per expression. </para>
    /// <para>This method provides a efficient way to perform bulk partial updates. </para>
    /// <para>The <paramref name="transform"/> actions should be <code>item.Set(value) or col.Append(value) or col.SetAt(index,value) or col.RemoveAt(index)</code> </para>
    /// </remarks>
    /// <seealso cref="ReplaceMany"/>
    /// <seealso cref="Update"/>
    abstract member UpdateMany: filter: Expression<System.Func<'T, bool>> * [<System.ParamArray>] transform: Expression<System.Action<'T>> array -> int



[<Extension>]
type UntypedCollectionExt =
    [<Extension>]
    static member InsertBatchObj(collection: ISoloDBCollection<JsonSerializator.JsonValue>, s: obj seq) =
        if isNull s then raise (System.ArgumentNullException(nameof s))
        s |> Seq.map JsonSerializator.JsonValue.SerializeWithType |> collection.InsertBatch

    [<Extension>]
    static member InsertObj(collection: ISoloDBCollection<JsonSerializator.JsonValue>, o: obj) =
        o |> JsonSerializator.JsonValue.SerializeWithType |> collection.Insert