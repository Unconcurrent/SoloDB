namespace SoloDatabase

open System.Linq.Expressions
open System.Runtime.CompilerServices
open System.Linq
open System.Data
open System.IO
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading.Tasks
open SoloDatabase.Types
open System
open Microsoft.Data.Sqlite

/// <summary>
/// Represents the outcome of an event handler invocation.
/// </summary>
/// <remarks>
/// Returning <see cref="RemoveHandler"/> unregisters the handler after it runs.
/// </remarks>
type SoloDBEventsResult =
| EventHandled
| RemoveHandler


/// <summary>
/// Provides access to the old and new versions of an item during an update event.
/// </summary>
type ISoloDBUpdatingEventContext<'T> =
    inherit ISoloDB

    /// <summary>
    /// Gets the name of the collection that triggered this event.
    /// </summary>
    abstract member CollectionName: string
    /// <summary>
    /// Reads the old item state before the update.
    /// </summary>
    abstract member OldItem: 'T with get
    /// <summary>
    /// Reads the new item state after the update.
    /// </summary>
    abstract member Item: 'T with get

/// <summary>
/// Provides access to the item during a single-item event (insert/delete).
/// </summary>
and ISoloDBItemEventContext<'T> =
    inherit ISoloDB

    /// <summary>
    /// Gets the name of the collection that triggered this event.
    /// </summary>
    abstract member CollectionName: string
    /// <summary>
    /// Reads the item associated with the event.
    /// </summary>
    abstract member Item: 'T with get

/// <summary>Internal delegate for insert events.</summary>
and InsertingHandlerSystem = delegate of conn: SqliteConnection * session: int64 * json: ReadOnlySpan<byte> -> SoloDBEventsResult
/// <summary>Internal delegate for delete events.</summary>
and DeletingHandlerSystem = delegate of conn: SqliteConnection * session: int64 * json: ReadOnlySpan<byte> -> SoloDBEventsResult
/// <summary>Internal delegate for update events.</summary>
and UpdatingHandlerSystem = delegate of conn: SqliteConnection * session: int64 * jsonOld: nativeptr<byte> * jsonOldSize: int * jsonNew: nativeptr<byte> * jsonNewSize: int -> SoloDBEventsResult


/// <summary>Handler delegate for insert events.</summary>
and InsertingHandler<'T> = delegate of ctx: ISoloDBItemEventContext<'T> -> SoloDBEventsResult
/// <summary>Handler delegate for delete events.</summary>
and DeletingHandler<'T> = delegate of ctx: ISoloDBItemEventContext<'T> -> SoloDBEventsResult
/// <summary>Handler delegate for update events.</summary>
and UpdatingHandler<'T> = delegate of ctx: ISoloDBUpdatingEventContext<'T> -> SoloDBEventsResult

/// <summary>Handler delegate for after-insert events.</summary>
and InsertedHandler<'T> = delegate of ctx: ISoloDBItemEventContext<'T> -> SoloDBEventsResult
/// <summary>Handler delegate for after-delete events.</summary>
and DeletedHandler<'T> = delegate of ctx: ISoloDBItemEventContext<'T> -> SoloDBEventsResult
/// <summary>Handler delegate for after-update events.</summary>
and UpdatedHandler<'T> = delegate of ctx: ISoloDBUpdatingEventContext<'T> -> SoloDBEventsResult


/// <summary>
/// Exposes collection-level event registration APIs.
/// </summary>
and ISoloDBCollectionEvents<'T> =
    /// <summary>
    /// Registers a handler invoked before an item insert.
    /// </summary>
    abstract member OnInserting: handler: InsertingHandler<'T> -> unit
    /// <summary>
    /// Registers a handler invoked before an item delete.
    /// </summary>
    abstract member OnDeleting: handler: DeletingHandler<'T> -> unit
    /// <summary>
    /// Registers a handler invoked before an item update.
    /// </summary>
    abstract member OnUpdating: handler: UpdatingHandler<'T> -> unit
    /// <summary>
    /// Registers a handler invoked after an item insert.
    /// </summary>
    abstract member OnInserted: handler: InsertedHandler<'T> -> unit
    /// <summary>
    /// Registers a handler invoked after an item delete.
    /// </summary>
    abstract member OnDeleted: handler: DeletedHandler<'T> -> unit
    /// <summary>
    /// Registers a handler invoked after an item update.
    /// </summary>
    abstract member OnUpdated: handler: UpdatedHandler<'T> -> unit
    /// <summary>
    /// Unregisters a previously registered insert handler.
    /// </summary>
    abstract member Unregister: handler: InsertingHandler<'T> -> unit
    /// <summary>
    /// Unregisters a previously registered delete handler.
    /// </summary>
    abstract member Unregister: handler: DeletingHandler<'T> -> unit
    /// <summary>
    /// Unregisters a previously registered update handler.
    /// </summary>
    abstract member Unregister: handler: UpdatingHandler<'T> -> unit
    /// <summary>
    /// Unregisters a previously registered after-insert handler.
    /// </summary>
    abstract member Unregister: handler: InsertedHandler<'T> -> unit
    /// <summary>
    /// Unregisters a previously registered after-delete handler.
    /// </summary>
    abstract member Unregister: handler: DeletedHandler<'T> -> unit
    /// <summary>
    /// Unregisters a previously registered after-update handler.
    /// </summary>
    abstract member Unregister: handler: UpdatedHandler<'T> -> unit
    

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
    inherit ISoloDBCollectionEvents<'T>

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



/// <summary>
/// Defines the public API for the SoloDB virtual file system.
/// </summary>
and IFileSystem =
    /// <summary>
    /// Uploads a stream to a file at the specified path. If the file exists, it is overwritten. If it does not exist, it is created.
    /// </summary>
    /// <param name="path">The full path of the file.</param>
    /// <param name="stream">The stream containing the file data to upload.</param>
    abstract member Upload: path: string * stream: Stream -> unit
    /// <summary>
    /// Asynchronously uploads a stream to a file at the specified path. If the file exists, it is overwritten. If it does not exist, it is created.
    /// </summary>
    /// <param name="path">The full path of the file.</param>
    /// <param name="stream">The stream containing the file data to upload.</param>
    abstract member UploadAsync: path: string * stream: Stream -> Task<unit>
    /// <summary>
    /// Uploads a sequence of files in a single transaction. This is more efficient for uploading many small files.
    /// </summary>
    /// <param name="files">A sequence of <c>BulkFileData</c> records representing the files to upload.</param>
    abstract member UploadBulk: files: BulkFileData seq -> unit
    /// <summary>
    /// Asynchronously replaces a file's content from a stream within a single transaction.
    /// </summary>
    /// <param name="path">The path of the file to replace.</param>
    /// <param name="stream">The stream with the new content.</param>
    abstract member ReplaceAsyncWithinTransaction: path: string * stream: Stream -> Task<unit>
    /// <summary>
    /// Downloads a file from the specified path and writes its content to the provided stream.
    /// </summary>
    /// <param name="path">The full path of the file to download.</param>
    /// <param name="stream">The stream to which the file content will be written.</param>
    /// <exception cref="FileNotFoundException">Thrown if the file does not exist at the specified path.</exception>
    abstract member Download: path: string * stream: Stream -> unit
    /// <summary>
    /// Asynchronously downloads a file from the specified path and writes its content to the provided stream.
    /// </summary>
    /// <param name="path">The full path of the file to download.</param>
    /// <param name="stream">The stream to which the file content will be written.</param>
    /// <exception cref="FileNotFoundException">Thrown if the file does not exist at the specified path.</exception>
    abstract member DownloadAsync: path: string * stream: Stream -> Task<unit>
    /// <summary>
    /// Gets the header information for a file at the specified path.
    /// </summary>
    /// <param name="path">The full path of the file.</param>
    /// <returns>The <c>SoloDBFileHeader</c> for the file.</returns>
    /// <exception cref="FileNotFoundException">Thrown if the file does not exist at the specified path.</exception>
    abstract member GetAt: path: string -> SoloDBFileHeader
    /// <summary>
    /// Tries to get the header information for a file at the specified path.
    /// </summary>
    /// <param name="path">The full path of the file.</param>
    /// <returns>An option containing the <c>SoloDBFileHeader</c> if the file exists, otherwise None.</returns>
    abstract member TryGetAt: path: string -> SoloDBFileHeader option
    /// <summary>
    /// Checks if a file or directory exists at the specified path.
    /// </summary>
    /// <param name="path">The full path of the file or directory.</param>
    /// <returns>True if an entry exists at the path, otherwise false.</returns>
    abstract member Exists: path: string -> bool
    /// <summary>
    /// Gets the header information for a file at the specified path, creating it if it does not exist.
    /// </summary>
    /// <param name="path">The full path of the file.</param>
    /// <returns>The <c>SoloDBFileHeader</c> for the file.</returns>
    abstract member GetOrCreateAt: path: string -> SoloDBFileHeader
    /// <summary>
    /// Gets the header information for a directory at the specified path.
    /// </summary>
    /// <param name="path">The full path of the directory.</param>
    /// <returns>The <c>SoloDBDirectoryHeader</c> for the directory.</returns>
    /// <exception cref="DirectoryNotFoundException">Thrown if the directory does not exist at the specified path.</exception>
    abstract member GetDirAt: path: string -> SoloDBDirectoryHeader
    /// <summary>
    /// Tries to get the header information for a directory at the specified path.
    /// </summary>
    /// <param name="path">The full path of the directory.</param>
    /// <returns>An option containing the <c>SoloDBDirectoryHeader</c> if the directory exists, otherwise None.</returns>
    abstract member TryGetDirAt: path: string -> SoloDBDirectoryHeader option
    /// <summary>
    /// Gets the header information for a directory at the specified path, creating it if it does not exist.
    /// </summary>
    /// <param name="path">The full path of the directory.</param>
    /// <returns>The <c>SoloDBDirectoryHeader</c> for the directory.</returns>
    abstract member GetOrCreateDirAt: path: string -> SoloDBDirectoryHeader
    /// <summary>
    /// Opens a file stream for a given file header.
    /// </summary>
    /// <param name="file">The file header of the file to open.</param>
    /// <returns>A readable and writable <c>Stream</c> for the file.</returns>
    abstract member Open: file: SoloDBFileHeader -> FileStorageCore.DbFileStream
    /// <summary>
    /// Opens a file stream for a file at the specified path.
    /// </summary>
    /// <param name="path">The full path of the file to open.</param>
    /// <returns>A readable and writable <c>Stream</c> for the file.</returns>
    /// <exception cref="FileNotFoundException">Thrown if the file does not exist at the specified path.</exception>
    abstract member OpenAt: path: string -> FileStorageCore.DbFileStream
    /// <summary>
    /// Tries to open a file stream for a file at the specified path.
    /// </summary>
    /// <param name="path">The full path of the file to open.</param>
    /// <returns>An option containing the <c>Stream</c> if the file exists, otherwise None.</returns>
    abstract member TryOpenAt: path: string -> FileStorageCore.DbFileStream option
    /// <summary>
    /// Opens a file stream for a file at the specified path, creating the file if it does not exist.
    /// </summary>
    /// <param name="path">The full path of the file to open or create.</param>
    /// <returns>A readable and writable <c>Stream</c> for the file.</returns>
    abstract member OpenOrCreateAt: path: string -> FileStorageCore.DbFileStream
    /// <summary>
    /// Writes data to a file at a specific offset.
    /// </summary>
    /// <param name="path">The path to the file.</param>
    /// <param name="offset">The zero-based byte offset in the file at which to begin writing.</param>
    /// <param name="data">The byte array to write to the file.</param>
    /// <param name="createIfInexistent">Specifies whether to create the file if it does not exist. Defaults to true.</param>
    abstract member WriteAt: path: string * offset: int64 * data: byte[] * [<Optional; DefaultParameterValue(true)>] createIfInexistent: bool -> unit
    /// <summary>
    /// Writes data to a file at a specific offset.
    /// </summary>
    /// <param name="path">The path to the file.</param>
    /// <param name="offset">The zero-based byte offset in the file at which to begin writing.</param>
    /// <param name="data">The Stream to copy to the file.</param>
    /// <param name="createIfInexistent">Specifies whether to create the file if it does not exist. Defaults to true.</param>
    abstract member WriteAt: path: string * offset: int64 * data: Stream * [<Optional; DefaultParameterValue(true)>] createIfInexistent: bool -> unit
    /// <summary>
    /// Reads a specified number of bytes from a file at a given offset.
    /// </summary>
    /// <param name="path">The full path of the file.</param>
    /// <param name="offset">The zero-based byte offset in the file at which to begin reading.</param>
    /// <param name="len">The number of bytes to read.</param>
    /// <returns>A byte array containing the data read from the file.</returns>
    abstract member ReadAt: path: string * offset: int64 * len: int -> byte[]
    /// <summary>
    /// Sets the modification date of a file.
    /// </summary>
    /// <param name="path">The path of the file.</param>
    /// <param name="date">The new modification date.</param>
    abstract member SetFileModificationDate: path: string * date: DateTimeOffset -> unit
    /// <summary>
    /// Sets the creation date of a file.
    /// </summary>
    /// <param name="path">The path of the file.</param>
    /// <param name="date">The new creation date.</param>
    abstract member SetFileCreationDate: path: string * date: DateTimeOffset -> unit
    /// <summary>
    /// Sets a metadata key-value pair for a file.
    /// </summary>
    /// <param name="file">The file header.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    abstract member SetMetadata: file: SoloDBFileHeader * key: string * value: string -> unit
    /// <summary>
    /// Sets a metadata key-value pair for a file at a given path.
    /// </summary>
    /// <param name="path">The path of the file.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    abstract member SetMetadata: path: string * key: string * value: string -> unit
    /// <summary>
    /// Deletes a metadata key from a file.
    /// </summary>
    /// <param name="file">The file header.</param>
    /// <param name="key">The metadata key to delete.</param>
    abstract member DeleteMetadata: file: SoloDBFileHeader * key: string -> unit
    /// <summary>
    /// Deletes a metadata key from a file at a given path.
    /// </summary>
    /// <param name="path">The path of the file.</param>
    /// <param name="key">The metadata key to delete.</param>
    abstract member DeleteMetadata: path: string * key: string -> unit
    /// <summary>
    /// Sets a metadata key-value pair for a directory.
    /// </summary>
    /// <param name="dir">The directory header.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    abstract member SetDirectoryMetadata: dir: SoloDBDirectoryHeader * key: string * value: string -> unit
    /// <summary>
    /// Sets a metadata key-value pair for a directory at a given path.
    /// </summary>
    /// <param name="path">The path of the directory.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    abstract member SetDirectoryMetadata: path: string * key: string * value: string -> unit
    /// <summary>
    /// Deletes a metadata key from a directory.
    /// </summary>
    /// <param name="dir">The directory header.</param>
    /// <param name="key">The metadata key to delete.</param>
    abstract member DeleteDirectoryMetadata: dir: SoloDBDirectoryHeader * key: string -> unit
    /// <summary>
    /// Deletes a metadata key from a directory at a given path.
    /// </summary>
    /// <param name="path">The path of the directory.</param>
    /// <param name="key">The metadata key to delete.</param>
    abstract member DeleteDirectoryMetadata: path: string * key: string -> unit
    /// <summary>
    /// Deletes a file.
    /// </summary>
    /// <param name="file">The header of the file to delete.</param>
    abstract member Delete: file: SoloDBFileHeader -> unit
    /// <summary>
    /// Deletes a directory recursively, including files.
    /// </summary>
    /// <param name="dir">The header of the directory to delete.</param>
    abstract member Delete: dir: SoloDBDirectoryHeader -> unit
    /// <summary>
    /// Deletes a file at the specified path.
    /// </summary>
    /// <param name="path">The path of the file to delete.</param>
    /// <returns>True if the file was deleted, false if it did not exist.</returns>
    abstract member DeleteFileAt: path: string -> bool
    /// <summary>
    /// Deletes a directory recursively at the specified path, including files.
    /// </summary>
    /// <param name="path">The path of the directory to delete.</param>
    /// <returns>True if the directory was deleted, false if it did not exist.</returns>
    abstract member DeleteDirAt: path: string -> bool
    /// <summary>
    /// Lists all files directly within the specified directory path.
    /// </summary>
    /// <param name="path">The path of the directory.</param>
    /// <returns>A sequence of file headers.</returns>
    abstract member ListFilesAt: path: string -> SoloDBFileHeader seq
    /// <summary>
    /// Lists all subdirectories directly within the specified directory path.
    /// </summary>
    /// <param name="path">The path of the directory.</param>
    /// <returns>A sequence of directory headers.</returns>
    abstract member ListDirectoriesAt: path: string -> SoloDBDirectoryHeader seq
    /// <summary>
    /// Lists files in a directory with pagination and sorting.
    /// </summary>
    /// <param name="path">The path of the directory.</param>
    /// <param name="sortBy">The field to sort by.</param>
    /// <param name="sortDir">The sort direction.</param>
    /// <param name="limit">Maximum number of files to return.</param>
    /// <param name="offset">Number of files to skip.</param>
    /// <returns>A tuple of (files list, total count).</returns>
    abstract member ListFilesAtPaginated: path: string * sortBy: SortField * sortDir: SortDirection * limit: int * offset: int -> IList<SoloDBFileHeader> * int64
    /// <summary>
    /// Lists subdirectories with pagination and sorting.
    /// </summary>
    /// <param name="path">The path of the parent directory.</param>
    /// <param name="sortBy">The field to sort by.</param>
    /// <param name="sortDir">The sort direction.</param>
    /// <param name="limit">Maximum number of directories to return.</param>
    /// <param name="offset">Number of directories to skip.</param>
    /// <returns>A tuple of (directories list, total count).</returns>
    abstract member ListDirectoriesAtPaginated: path: string * sortBy: SortField * sortDir: SortDirection * limit: int * offset: int -> IList<SoloDBDirectoryHeader> * int64
    /// <summary>
    /// Lists directory entries (directories first, then files) with pagination and sorting.
    /// </summary>
    /// <param name="path">The path of the directory.</param>
    /// <param name="sortBy">The field to sort by.</param>
    /// <param name="sortDir">The sort direction.</param>
    /// <param name="limit">Maximum number of entries to return.</param>
    /// <param name="offset">Number of entries to skip.</param>
    /// <returns>A tuple of (entries list, directory count, file count).</returns>
    abstract member ListEntriesAtPaginated: path: string * sortBy: SortField * sortDir: SortDirection * limit: int * offset: int -> IList<SoloDBEntryHeader> * int64 * int64
    /// <summary>
    /// Recursively lists all entries (files and directories) starting from the specified path. The result is buffered into a list.
    /// </summary>
    /// <param name="path">The starting path.</param>
    /// <returns>A list of all entries.</returns>
    abstract member RecursiveListEntriesAt: path: string -> IList<SoloDBEntryHeader>
    /// <summary>
    /// Lazily and recursively lists all entries (files and directories) starting from the specified path.
    /// This method is not recommended for long-running operations as it can hold the database connection open.
    /// </summary>
    /// <param name="path">The starting path.</param>
    /// <returns>A lazy sequence of all entries.</returns>
    abstract member RecursiveListEntriesAtLazy: path: string -> SoloDBEntryHeader seq
    /// <summary>
    /// Moves a file from a source path to a destination path.
    /// </summary>
    /// <param name="from">The source path of the file.</param>
    /// <param name="toPath">The destination path for the file.</param>
    /// <exception cref="IOException">Thrown if a file already exists at the destination.</exception>
    abstract member MoveFile: from: string * toPath: string -> unit
    /// <summary>
    /// Moves a file from a source path to a destination path, replacing the destination file if it exists.
    /// </summary>
    /// <param name="from">The source path of the file.</param>
    /// <param name="toPath">The destination path for the file.</param>
    abstract member MoveReplaceFile: from: string * toPath: string -> unit
    /// <summary>
    /// Moves a directory from a source path to a destination path. All contents are moved recursively.
    /// </summary>
    /// <param name="from">The source path of the directory.</param>
    /// <param name="toPath">The destination path for the directory.</param>
    /// <exception cref="IOException">Thrown if a file or directory already exists at the destination.</exception>
    abstract member MoveDirectory: from: string * toPath: string -> unit


/// <summary>
/// Represents the common, transaction-safe surface area shared by both <see cref="SoloDB"/> and <see cref="TransactionalSoloDB"/>.
/// </summary>
/// <remarks>
/// This interface intentionally excludes administrative and global operations that are not valid within an active transaction
/// (for example, backup or vacuum operations). It is designed to be safe to pass into event callbacks and other APIs that must
/// work both inside and outside of explicit transactions.
/// </remarks>
and ISoloDB =
    inherit IDisposable

    /// <summary>
    /// Gets the SQLite connection string used by this database instance.
    /// </summary>
    /// <remarks>
    /// For transactional contexts, this reflects the underlying transactional connection string. It does not imply that a new
    /// connection will be created; it is the string used by the owning connection manager.
    /// </remarks>
    abstract member ConnectionString: string

    /// <summary>
    /// Gets the file system API associated with this database instance.
    /// </summary>
    /// <remarks>
    /// In a transactional context, all operations are scoped to the current transaction and will be committed or rolled back
    /// together with other database operations.
    /// </remarks>
    abstract member FileSystem: IFileSystem

    /// <summary>
    /// Gets a typed collection using the document type name as the collection name.
    /// Creates the collection if it does not exist.
    /// </summary>
    /// <remarks>
    /// The collection name is derived from the type name and is normalized according to SoloDB naming rules.
    /// </remarks>
    abstract member GetCollection<'T> : unit -> ISoloDBCollection<'T>

    /// <summary>
    /// Gets a typed collection using a custom collection name.
    /// Creates the collection if it does not exist.
    /// </summary>
    /// <remarks>
    /// The provided name is validated and normalized. A <c>SoloDB*</c> prefix is reserved and rejected.
    /// </remarks>
    abstract member GetCollection<'T> : name: string -> ISoloDBCollection<'T>

    /// <summary>
    /// Gets a collection that stores untyped JSON values.
    /// Creates the collection if it does not exist.
    /// </summary>
    /// <remarks>
    /// This collection stores <see cref="JsonSerializator.JsonValue"/> documents and is useful for schemaless or dynamic data.
    /// </remarks>
    abstract member GetUntypedCollection : name: string -> ISoloDBCollection<JsonSerializator.JsonValue>

    /// <summary>
    /// Returns true if a collection with the specified name exists.
    /// </summary>
    /// <remarks>
    /// This call does not create the collection and does not modify the database state.
    /// </remarks>
    abstract member CollectionExists : name: string -> bool

    /// <summary>
    /// Returns true if a collection for the specified type exists.
    /// </summary>
    /// <remarks>
    /// This call does not create the collection and does not modify the database state.
    /// </remarks>
    abstract member CollectionExists<'T> : unit -> bool

    /// <summary>
    /// Drops a collection by name if it exists and returns true if it was dropped.
    /// </summary>
    /// <remarks>
    /// This removes the collection table and its associated indexes and triggers.
    /// </remarks>
    abstract member DropCollectionIfExists : name: string -> bool

    /// <summary>
    /// Drops a collection for the specified type if it exists and returns true if it was dropped.
    /// </summary>
    /// <remarks>
    /// This removes the collection table and its associated indexes and triggers.
    /// </remarks>
    abstract member DropCollectionIfExists<'T> : unit -> bool

    /// <summary>
    /// Drops a collection by name or throws if it does not exist.
    /// </summary>
    /// <remarks>
    /// This removes the collection table and its associated indexes and triggers.
    /// </remarks>
    abstract member DropCollection : name: string -> unit

    /// <summary>
    /// Drops a collection for the specified type or throws if it does not exist.
    /// </summary>
    /// <remarks>
    /// This removes the collection table and its associated indexes and triggers.
    /// </remarks>
    abstract member DropCollection<'T> : unit -> unit

    /// <summary>
    /// Lists all collection names in the database.
    /// </summary>
    /// <remarks>
    /// The returned names are the logical SoloDB collection names, not raw SQLite table names.
    /// </remarks>
    abstract member ListCollectionNames : unit -> seq<string>

    /// <summary>
    /// Asks the SQLite engine to run analysis and optimization for query planning.
    /// </summary>
    /// <remarks>
    /// This is safe to call in both transactional and non-transactional contexts. It invokes SQLite's
    /// <c>PRAGMA optimize</c> on the underlying connection.
    /// </remarks>
    abstract member Optimize : unit -> unit


[<Extension>]
type UntypedCollectionExt =
    [<Extension>]
    static member InsertBatchObj(collection: ISoloDBCollection<JsonSerializator.JsonValue>, s: obj seq) =
        if isNull s then raise (System.ArgumentNullException(nameof s))
        s |> Seq.map JsonSerializator.JsonValue.SerializeWithType |> collection.InsertBatch

    [<Extension>]
    static member InsertObj(collection: ISoloDBCollection<JsonSerializator.JsonValue>, o: obj) =
        o |> JsonSerializator.JsonValue.SerializeWithType |> collection.Insert
