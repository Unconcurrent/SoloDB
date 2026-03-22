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
                sprintf "Error: DBRef<%s>.Value is not loaded.\nReason: Exclude was applied or the reference was created with DBRef.To(id).\nFix: Use .Id or Load the reference in the query." typeof<'T>.Name))
        if this._id = 0L && not (obj.ReferenceEquals(this._value :> obj, null)) then
            // From(entity) case: entity is pending cascade-insert, Id not yet assigned.
            this._value
        elif this._id = 0L then
            raise (InvalidOperationException(
                sprintf "Error: DBRef<%s> is empty (no reference).\nReason: HasValue is false.\nFix: Check .HasValue before accessing .Value." typeof<'T>.Name))
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
            raise (ArgumentOutOfRangeException(nameof id, id,
                "Error: DBRef.To requires a positive Id (> 0).\nReason: Id 0 represents an empty reference.\nFix: Use DBRef.None for empty or supply a positive Id."))
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

/// <summary>
/// A typed reference to a single entity of type <typeparamref name="TTarget"/> using a custom Id type <typeparamref name="TId"/>.
/// Serialized as the target Id in JSON. Backed by a link table with FK RESTRICT.
/// </summary>
/// <remarks>
/// Use <c>DBRef.To(id)</c> to reference an existing entity by its typed Id.
/// Use <c>DBRef.From(entity)</c> to reference an unsaved entity (cascade-insert on parent Insert/Update).
/// Use <c>DBRef.None</c> for an empty reference (serializes as JSON null).
/// </remarks>
[<Struct; CustomEquality; CustomComparison>]
type DBRef<'TTarget, 'TId> =
    val private _id: int64
    val private _typedId: 'TId
    val private _hasTypedId: bool
    val private _value: 'TTarget
    val private _isLoaded: bool

    private new(id: int64, typedId: 'TId, hasTypedId: bool, value: 'TTarget, isLoaded: bool) =
        { _id = id; _typedId = typedId; _hasTypedId = hasTypedId; _value = value; _isLoaded = isLoaded }

    /// <summary>The internal row Id of the referenced entity. 0 means no reference or pending typed-id resolution.</summary>
    member this.Id = this._id

    /// <summary>True if a reference exists (either by row Id or typed Id).</summary>
    member this.HasValue = this._id <> 0L || this._hasTypedId

    /// <summary>True if the referenced entity has been loaded from the database.</summary>
    member this.IsLoaded = this._isLoaded

    /// <summary>The loaded entity value.</summary>
    /// <exception cref="System.InvalidOperationException">Thrown if the reference is not loaded or is empty.</exception>
    member this.Value =
        if not this._isLoaded then
            raise (InvalidOperationException(
                sprintf "Error: DBRef<%s,%s>.Value is not loaded.\nReason: Exclude was applied or the reference was created with DBRef.To(id).\nFix: Use .Id or Load the reference in the query." typeof<'TTarget>.Name typeof<'TId>.Name))
        if this._id = 0L && not (obj.ReferenceEquals(this._value :> obj, null)) then
            this._value
        elif this._id = 0L then
            raise (InvalidOperationException(
                sprintf "Error: DBRef<%s,%s> is empty (no reference).\nReason: HasValue is false.\nFix: Check .HasValue before accessing .Value." typeof<'TTarget>.Name typeof<'TId>.Name))
        else
            this._value

    member internal this.PendingEntity: 'TTarget voption =
        if this._id = 0L && this._isLoaded && not (obj.ReferenceEquals(this._value :> obj, null)) then
            ValueSome this._value
        else
            ValueNone

    member internal this.PendingTypedId: 'TId voption =
        if this._id = 0L && this._hasTypedId then ValueSome this._typedId else ValueNone

    // Serializer introspection: pending typed-id state for transient wire roundtrip.
    member internal this.HasPendingTypedId = this._id = 0L && this._hasTypedId
    member internal this.TypedIdOrDefault = this._typedId

    static member internal Loaded(id: int64, value: 'TTarget) =
        DBRef<'TTarget, 'TId>(id, Unchecked.defaultof<'TId>, false, value, true)

    static member internal Unloaded(id: int64) =
        DBRef<'TTarget, 'TId>(id, Unchecked.defaultof<'TId>, false, Unchecked.defaultof<'TTarget>, false)

    static member internal Resolved(id: int64) =
        if id <= 0L then
            raise (ArgumentOutOfRangeException(nameof id, id, "Resolved DBRef id must be > 0."))
        DBRef<'TTarget, 'TId>(id, Unchecked.defaultof<'TId>, false, Unchecked.defaultof<'TTarget>, false)

    /// <summary>Creates a reference to an existing entity by its typed Id.</summary>
    /// <param name="id">The typed Id of the target entity. Must not be null.</param>
    /// <exception cref="System.ArgumentNullException">Thrown if <paramref name="id"/> is null.</exception>
    static member To(id: 'TId) =
        if obj.ReferenceEquals(id, null) then
            raise (ArgumentNullException(nameof id, "DBRef.To requires a non-null typed id."))
        DBRef<'TTarget, 'TId>(0L, id, true, Unchecked.defaultof<'TTarget>, false)

    /// <summary>Creates a reference from an unsaved entity. The entity will be cascade-inserted when the owner is saved.</summary>
    /// <param name="entity">The entity instance to reference. Must not be null.</param>
    /// <exception cref="System.ArgumentNullException">Thrown if <paramref name="entity"/> is null.</exception>
    static member From(entity: 'TTarget) =
        if obj.ReferenceEquals(entity, null) then
            raise (ArgumentNullException(nameof entity, "DBRef.From requires a non-null entity. Use DBRef.None for an empty reference."))
        DBRef<'TTarget, 'TId>(0L, Unchecked.defaultof<'TId>, false, entity, true)

    /// <summary>An empty reference with no target entity.</summary>
    static member None = DBRef<'TTarget, 'TId>(0L, Unchecked.defaultof<'TId>, false, Unchecked.defaultof<'TTarget>, false)

    override this.Equals(other: obj) =
        match other with
        | :? DBRef<'TTarget, 'TId> as o ->
            if this._id <> o._id then false
            elif this._id <> 0L then true
            elif this._hasTypedId <> o._hasTypedId then false
            elif this._hasTypedId then EqualityComparer<'TId>.Default.Equals(this._typedId, o._typedId)
            else true
        | _ -> false

    override this.GetHashCode() =
        if this._id <> 0L then this._id.GetHashCode()
        elif this._hasTypedId then
            let typedHash = EqualityComparer<'TId>.Default.GetHashCode(this._typedId)
            hash (this._id, this._hasTypedId, typedHash)
        else hash (this._id, this._hasTypedId)

    interface IEquatable<DBRef<'TTarget, 'TId>> with
        member this.Equals(other: DBRef<'TTarget, 'TId>) = (this :> obj).Equals(other)

    interface IComparable<DBRef<'TTarget, 'TId>> with
        member this.CompareTo(other: DBRef<'TTarget, 'TId>) = compare this._id other._id

    interface IComparable with
        member this.CompareTo(other: obj) =
            match other with
            | :? DBRef<'TTarget, 'TId> as o -> compare this._id o._id
            | _ -> raise (ArgumentException("Cannot compare DBRef with a different type."))

    override this.ToString() =
        if this._id = 0L then
            if this._hasTypedId then
                sprintf "DBRef<%s,%s>.To(%O)" typeof<'TTarget>.Name typeof<'TId>.Name this._typedId
            else
                sprintf "DBRef<%s,%s>.None" typeof<'TTarget>.Name typeof<'TId>.Name
        elif this._isLoaded then
            sprintf "DBRef<%s,%s>.To(%d) [Loaded]" typeof<'TTarget>.Name typeof<'TId>.Name this._id
        else
            sprintf "DBRef<%s,%s>.To(%d)" typeof<'TTarget>.Name typeof<'TId>.Name this._id


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
/// Controls the load-time ordering of DBRefMany items.
/// </summary>
type DBRefOrder =
    /// <summary>No ordering guarantee (default). Matches current behavior.</summary>
    | Undefined = 0
    /// <summary>Order loaded items by target entity Id ascending.</summary>
    | TargetId = 1

/// <summary>
/// Configures relation behavior on a <see cref="DBRef{T}"/> or <see cref="DBRefMany{T}"/> property.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description><c>OnDelete</c>: what happens when the REFERENCED (target) entity is deleted.</description></item>
/// <item><description><c>OnOwnerDelete</c>: what happens to linked entities when the OWNER is deleted.</description></item>
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
    /// What happens to referenced entities when the OWNER is deleted.
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

    /// <summary>
    /// Controls the load-time ordering of DBRefMany items.
    /// Undefined (default): no ordering guarantee. TargetId: order by target entity Id ascending.
    /// </summary>
    member val OrderBy: DBRefOrder = DBRefOrder.Undefined with get, set
