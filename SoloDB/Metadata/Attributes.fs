namespace SoloDatabase

open System

/// Requests generated SoloDB operations for a model or a CSharp context root.
[<AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Struct ||| AttributeTargets.Enum, AllowMultiple = true)>]
type SoloDBAOTAttribute private (roots: Type array) =
    inherit Attribute()

    /// Marks the attributed model as a generated-operation root.
    new() = SoloDBAOTAttribute(Array.empty)

    /// Adds a model root to the attributed partial CSharp context.
    new(modelType: Type) =
        if isNull modelType then nullArg "modelType"
        SoloDBAOTAttribute([|modelType|])

    /// The explicitly declared roots; an empty list denotes the attributed model.
    member _.ModelTypes = Array.copy roots

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
