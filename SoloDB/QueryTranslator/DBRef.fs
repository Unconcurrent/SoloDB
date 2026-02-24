namespace SoloDatabase

open System
open System.Collections
open System.Collections.Generic

/// <summary>
/// A typed reference to a single entity of type 'T in another collection.
/// Serialized as the target Id (integer) or null in JSON. Backed by a link table with FK RESTRICT.
/// Loaded by default via LEFT JOIN when querying.
/// </summary>
/// <remarks>
/// Use <c>DBRef.To(id)</c> to reference an existing entity by Id.
/// Use <c>DBRef.From(entity)</c> to reference an unsaved entity (cascade-insert on parent Insert/Update).
/// Use <c>DBRef.None</c> for an empty reference (serializes as JSON null).
/// </remarks>
[<Struct; CustomEquality; CustomComparison>]
type DBRef<'T> =
    val private _id: int64
    val private _value: 'T
    val private _isLoaded: bool

    private new(id: int64, value: 'T, isLoaded: bool) =
        { _id = id; _value = value; _isLoaded = isLoaded }

    /// <summary>The Id of the referenced entity. 0 means no reference (None).</summary>
    member this.Id = this._id

    /// <summary>True if a reference exists (Id != 0).</summary>
    member this.HasValue = this._id <> 0L

    /// <summary>True if Value was populated by the query pipeline (default unless Excluded).</summary>
    member this.IsLoaded = this._isLoaded

    /// <summary>
    /// The loaded entity. Throws <see cref="InvalidOperationException"/> if not loaded
    /// (Exclude was applied) or if empty (HasValue is false).
    /// </summary>
    member this.Value =
        if not this._isLoaded then
            raise (InvalidOperationException(
                sprintf "DBRef<%s>.Value is not loaded. Either Exclude was applied or the reference was created with DBRef.To(id). Use .Id to access the reference Id without loading." typeof<'T>.Name))
        if this._id = 0L && not (obj.ReferenceEquals(this._value :> obj, null)) then
            // From(entity) case: entity is pending cascade-insert, Id not yet assigned.
            this._value
        elif this._id = 0L then
            raise (InvalidOperationException(
                sprintf "DBRef<%s> is empty (no reference). Check .HasValue before accessing .Value." typeof<'T>.Name))
        else
            this._value

    // ─── Internal: used by Relations.fs and query pipeline ────────────────────

    /// Returns the pending entity for cascade-insert (From(entity) with Id=0).
    member internal this.PendingEntity: 'T voption =
        if this._id = 0L && this._isLoaded && not (obj.ReferenceEquals(this._value :> obj, null)) then
            ValueSome this._value
        else
            ValueNone

    /// Creates a loaded instance with populated entity (used by query pipeline after LEFT JOIN).
    static member internal Loaded(id: int64, value: 'T) =
        DBRef<'T>(id, value, true)

    /// Creates an unloaded instance with Id only (used during JSON deserialization).
    static member internal Unloaded(id: int64) =
        DBRef<'T>(id, Unchecked.defaultof<'T>, false)

    // ─── Public factory methods ───────────────────────────────────────────────

    /// <summary>Create a reference to an existing entity by its database Id.</summary>
    /// <param name="id">The database Id of the target entity. Must be greater than 0.</param>
    static member To(id: int64) =
        if id <= 0L then
            raise (ArgumentOutOfRangeException(nameof id, id, "DBRef.To requires a positive Id (> 0). Use DBRef.None for an empty reference."))
        DBRef<'T>(id, Unchecked.defaultof<'T>, false)

    /// <summary>
    /// Create a reference containing an unsaved entity for cascade-insert.
    /// When the parent entity is inserted or updated, the referenced entity will be
    /// inserted first, and the resulting Id will be used for the link.
    /// </summary>
    /// <param name="entity">The entity to cascade-insert. Must not be null.</param>
    static member From(entity: 'T) =
        if obj.ReferenceEquals(entity, null) then
            raise (ArgumentNullException(nameof entity, "DBRef.From requires a non-null entity. Use DBRef.None for an empty reference."))
        DBRef<'T>(0L, entity, true)

    /// <summary>Empty reference. Serializes as null in JSON. HasValue is false.</summary>
    static member None = DBRef<'T>(0L, Unchecked.defaultof<'T>, false)

    // ─── Equality by Id ───────────────────────────────────────────────────────

    override this.Equals(other: obj) =
        match other with
        | :? DBRef<'T> as o -> this._id = o._id
        | _ -> false

    override this.GetHashCode() = this._id.GetHashCode()

    interface IEquatable<DBRef<'T>> with
        member this.Equals(other: DBRef<'T>) = this._id = other._id

    interface IComparable<DBRef<'T>> with
        member this.CompareTo(other: DBRef<'T>) = compare this._id other._id

    interface IComparable with
        member this.CompareTo(other: obj) =
            match other with
            | :? DBRef<'T> as o -> compare this._id o._id
            | _ -> raise (ArgumentException("Cannot compare DBRef with a different type."))

    override this.ToString() =
        if this._id = 0L then
            sprintf "DBRef<%s>.None" typeof<'T>.Name
        elif this._isLoaded then
            sprintf "DBRef<%s>.To(%d) [Loaded]" typeof<'T>.Name this._id
        else
            sprintf "DBRef<%s>.To(%d)" typeof<'T>.Name this._id

// ─── IDBRefManyInternal: non-generic interface for Relations.fs ───────────────

/// <summary>
/// Non-generic interface for accessing DBRefMany change tracking state from Relations.fs
/// without requiring the generic type parameter. Internal to the SoloDB assembly.
/// </summary>
type internal IDBRefManyInternal =
    /// True if this collection was populated by the query pipeline or reset after Insert.
    abstract IsLoaded: bool

    /// Ids present at the last load/reset checkpoint (snapshot before user mutations).
    abstract OriginalIds: IReadOnlyCollection<int64>

    /// Current items as boxed objects. Relations.fs uses HasTypeId to extract Ids.
    abstract GetCurrentItemsBoxed: unit -> obj seq

    /// Reset change tracking after Insert or Update commits.
    /// committedIds: the set of Ids that are now linked in the database.
    abstract ResetTracker: committedIds: int64 seq -> unit

    /// Populate this collection from the query pipeline (batch loading).
    /// items: boxed typed items. ids: their corresponding database Ids.
    abstract SetLoadedBoxed: items: obj seq -> ids: int64 seq -> unit


/// <summary>
/// A typed collection of references to entities of type 'T in another collection.
/// Not stored in JSON. All data lives in a link table with FK RESTRICT.
/// Loaded by default via batched subquery when querying.
/// Implements <see cref="IList{T}"/> with change tracking for diff-on-Update.
/// </summary>
/// <remarks>
/// Mutations (Add, Remove, Clear, indexer) are tracked. When the parent entity is updated
/// via <c>collection.Update(entity)</c>, the engine computes a diff between the original
/// loaded state and the current state, and applies link row insertions/deletions accordingly.
/// Removing an item from the list and calling Update only unlinks it — the entity always survives.
/// </remarks>
type DBRefMany<'T>() =
    let _originalIds = HashSet<int64>()
    let _currentItems = List<'T>()
    let mutable _isLoaded = false

    // ─── Public properties ────────────────────────────────────────────────────

    /// <summary>Number of items currently in the collection.</summary>
    member _.Count = _currentItems.Count

    /// <summary>True if populated from query or after Insert (TRACKER-RESET).</summary>
    member _.IsLoaded = _isLoaded

    // ─── Internal: for Relations.fs ───────────────────────────────────────────

    /// The set of Ids that were present at the last load/reset checkpoint.
    member internal _.OriginalIds = _originalIds

    /// Direct typed access to current items list. Relations.fs uses this for typed operations.
    member internal _.CurrentItems = _currentItems

    /// Set loaded state from typed items (used by Relations.fs batch loading pipeline).
    member internal _.SetLoaded(items: 'T seq, ids: int64 seq) =
        _currentItems.Clear()
        _currentItems.AddRange items
        _originalIds.Clear()
        for id in ids do
            _originalIds.Add id |> ignore
        _isLoaded <- true

    /// Reset change tracker after successful commit (Insert or Update).
    member internal _.ResetTrackerTyped(committedIds: int64 seq) =
        _originalIds.Clear()
        for id in committedIds do
            _originalIds.Add id |> ignore
        _isLoaded <- true

    // ─── IDBRefManyInternal (non-generic interface for Relations.fs) ──────────

    interface IDBRefManyInternal with
        member _.IsLoaded = _isLoaded

        member _.OriginalIds = _originalIds :> IReadOnlyCollection<int64>

        member _.GetCurrentItemsBoxed() =
            _currentItems |> Seq.cast<obj>

        member _.ResetTracker(committedIds: int64 seq) =
            _originalIds.Clear()
            for id in committedIds do
                _originalIds.Add id |> ignore
            _isLoaded <- true

        member _.SetLoadedBoxed (items: obj seq) (ids: int64 seq) =
            _currentItems.Clear()
            for item in items do
                _currentItems.Add(item :?> 'T)
            _originalIds.Clear()
            for id in ids do
                _originalIds.Add id |> ignore
            _isLoaded <- true

    // ─── IList<'T> ───────────────────────────────────────────────────────────

    /// <summary>Adds an item to the collection. Tracked for diff-on-Update.</summary>
    member _.Add(item: 'T) = _currentItems.Add item

    /// <summary>Removes the first occurrence of an item. Tracked for diff-on-Update.</summary>
    member _.Remove(item: 'T) = _currentItems.Remove item

    /// <summary>Removes the item at the specified index. Tracked for diff-on-Update.</summary>
    member _.RemoveAt(index: int) = _currentItems.RemoveAt index

    /// <summary>Removes all items. All original links will be deleted on Update.</summary>
    member _.Clear() = _currentItems.Clear()

    /// <summary>Determines whether the collection contains a specific item.</summary>
    member _.Contains(item: 'T) = _currentItems.Contains item

    /// <summary>Returns the index of the first occurrence of an item, or -1 if not found.</summary>
    member _.IndexOf(item: 'T) = _currentItems.IndexOf item

    /// <summary>Inserts an item at the specified index. Tracked for diff-on-Update.</summary>
    member _.Insert(index: int, item: 'T) = _currentItems.Insert(index, item)

    /// <summary>Copies the elements to an array, starting at a particular array index.</summary>
    member _.CopyTo(array: 'T array, arrayIndex: int) = _currentItems.CopyTo(array, arrayIndex)

    /// <summary>Gets or sets the element at the specified index.</summary>
    member _.Item
        with get(index: int) = _currentItems.[index]
        and set (index: int) (value: 'T) = _currentItems.[index] <- value

    /// <summary>Returns an enumerator that iterates through the collection.</summary>
    member _.GetEnumerator() : IEnumerator<'T> =
        (_currentItems :> IEnumerable<'T>).GetEnumerator()

    interface IList<'T> with
        member this.Count = this.Count
        member _.IsReadOnly = false
        member this.Item
            with get i = this.[i]
            and set i v = this.[i] <- v
        member this.Add item = this.Add item
        member this.Remove item = this.Remove item
        member this.RemoveAt i = this.RemoveAt i
        member this.Clear() = this.Clear()
        member this.Contains item = this.Contains item
        member this.CopyTo(arr, i) = this.CopyTo(arr, i)
        member this.IndexOf item = this.IndexOf item
        member this.Insert(i, item) = this.Insert(i, item)

    interface IEnumerable<'T> with
        member this.GetEnumerator() : IEnumerator<'T> = this.GetEnumerator()

    interface IEnumerable with
        member _.GetEnumerator() =
            (_currentItems :> IEnumerable).GetEnumerator()

    interface IReadOnlyList<'T> with
        member this.Count = this.Count
        member this.Item with get i = this.[i]

    override _.ToString() =
        if _isLoaded then
            sprintf "DBRefMany<%s> [%d items]" typeof<'T>.Name _currentItems.Count
        else
            sprintf "DBRefMany<%s> [not loaded]" typeof<'T>.Name


namespace SoloDatabase.Attributes

open System

/// <summary>
/// Specifies the delete policy for relation references.
/// </summary>
type DeletePolicy =
    /// <summary>Block the operation if references exist. Throws a descriptive error.</summary>
    | Restrict = 0
    /// <summary>
    /// Cascade the delete to referencing entities (OnDelete only).
    /// Not valid for OnOwnerDelete — use Deletion instead.
    /// </summary>
    | Cascade = 1
    /// <summary>Remove references (set DBRef to None, remove link rows). Entities survive.</summary>
    | Unlink = 2
    /// <summary>
    /// For OnOwnerDelete only: unlink first, then delete target entities that have
    /// zero remaining references across all link tables (column-aware global ref count).
    /// Entities still referenced elsewhere survive.
    /// </summary>
    | Deletion = 3


/// <summary>
/// Configures relation behavior on a <see cref="DBRef{T}"/> or <see cref="DBRefMany{T}"/> property.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description><c>OnDelete</c>: what happens when the REFERENCED (target) entity is deleted.</description></item>
/// <item><description><c>OnOwnerDelete</c>: what happens to linked entities when the OWNER is deleted (DBRefMany only).</description></item>
/// <item><description><c>Unique</c>: enforce 1:1 cardinality on DBRef (UNIQUE on both SourceId and TargetId).</description></item>
/// </list>
/// </remarks>
[<AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
[<Sealed>]
type SoloRefAttribute() =
    inherit Attribute()

    /// <summary>
    /// What happens when the REFERENCED entity (target) is deleted.
    /// <para>Restrict (default): block the delete if references exist.</para>
    /// <para>Cascade: also delete the entity that holds this reference.</para>
    /// <para>Unlink: remove the reference (set DBRef to None, remove from DBRefMany).</para>
    /// </summary>
    member val OnDelete: DeletePolicy = DeletePolicy.Restrict with get, set

    /// <summary>
    /// For DBRefMany: what happens to referenced entities when the OWNER is deleted.
    /// <para>Deletion (default): unlink first, then delete each formerly-linked target entity
    /// ONLY IF its global reference count across all link tables is zero.</para>
    /// <para>Unlink: just remove link rows, referenced entities always survive.</para>
    /// <para>Restrict: block owner deletion if any links exist.</para>
    /// <para>NOTE: Cascade is NOT valid for OnOwnerDelete. Use Deletion instead.</para>
    /// </summary>
    member val OnOwnerDelete: DeletePolicy = DeletePolicy.Deletion with get, set

    /// <summary>
    /// If true, enforce OneToOne cardinality (UNIQUE on both SourceId and TargetId in the link table).
    /// Only valid on <see cref="DBRef{T}"/>. Default is false (ManyToOne).
    /// </summary>
    member val Unique: bool = false with get, set
