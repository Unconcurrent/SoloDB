namespace SoloDatabase.Attributes

open System

/// <summary>
/// Marks a property for indexing within the database collection.
/// <para>
/// An index significantly improves query performance for the decorated property.
/// The index will be created automatically under two conditions:
/// <list type="number">
/// <item>
/// <description>
/// When the collection for this document type is accessed for the <strong>very first time</strong>.
/// </description>
/// </item>
/// <item>
/// <description>
/// When the <c>EnsureAddedAttributeIndexes()</c> method is explicitly called on the collection.
/// </description>
/// </item>
/// </list>
/// </para>
/// </summary>
[<AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type IndexedAttribute(unique: bool) =
    inherit Attribute()

    /// <summary>
    /// Initializes a new instance of the <see cref="IndexedAttribute"/> class, specifying whether the index should enforce uniqueness.
    /// </summary>
    /// <param name="unique">
    /// A boolean value indicating whether the indexed values must be unique across all documents in the collection.
    /// If <c>true</c>, the database will reject insertions or updates that would result in duplicate values for this property.
    /// </param>
    new() = IndexedAttribute(false)

    /// <summary>
    /// Gets a value indicating whether the index enforces a uniqueness constraint.
    /// </summary>
    member val Unique = unique

/// <summary>
/// Instructs the serializer to include type information when storing a document.
/// <para>
/// This is essential for collections that store documents from an inheritance hierarchy (polymorphic collections).
/// By adding this attribute to a base class, the serializer will embed a type discriminator field (e.g., "_type")
/// in the stored document. This ensures that when the document is retrieved, it can be correctly deserialized
/// back into its original, specific derived type, preserving the class hierarchy.
/// </para>
/// </summary>
[<AttributeUsage(AttributeTargets.Class, AllowMultiple = false); Sealed>]
type PolimorphicAttribute() =
    inherit Attribute()

/// <summary>
/// Designates a property as the document's primary identifier (ID).
/// <para>
/// This attribute inherently creates a <strong>unique index</strong> on the property. Each document in a collection must have a unique ID.
/// It inherits from <see cref="IndexedAttribute"/> with the `unique` flag set to <c>true</c>.
/// </para>
/// </summary>
/// <param name="idGenerator">
/// Specifies a <see cref="System.Type"/> that implements a custom ID generation strategy.
/// This allows for user-defined logic for creating new document IDs (e.g., using a custom algorithm, a specific format, or an external service).
/// </param>
[<Sealed>]
[<AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type SoloId(idGenerator: Type) =
    inherit IndexedAttribute(true)

    /// <summary>
    /// Gets the <see cref="System.Type"/> of the custom ID generator to be used for this primary key.
    /// </summary>
    member val IdGenerator = idGenerator

namespace System.Runtime.CompilerServices

open System

[<AttributeUsage(AttributeTargets.All, AllowMultiple = false)>]
[<Sealed>]
type IsReadOnlyAttribute() =
    inherit System.Attribute()

[<AttributeUsage(AttributeTargets.All, AllowMultiple = false)>]
[<Sealed>]
type IsByRefLikeAttribute() =
    inherit System.Attribute()