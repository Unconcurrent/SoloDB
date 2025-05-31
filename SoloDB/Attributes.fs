namespace SoloDatabase.Attributes
open System

/// <summary>
/// If included, the DB will index this property, on: 
/// a) Only on the first initialization of the collection for this type in the storage medium(disk or memory);
/// b) On calling the SoloDatabase.Collection<T>.EnsureAddedAttributeIndexes();
/// </summary>
[<System.AttributeUsage(System.AttributeTargets.Property, AllowMultiple = false)>]
type IndexedAttribute(unique: bool) =
    inherit System.Attribute()

    new() = IndexedAttribute(false)

    member val Unique = unique

/// <summary>
/// If included, the DB will store the type information.
/// </summary>
[<System.AttributeUsage(System.AttributeTargets.Class, AllowMultiple = false); Sealed>]
type PolimorphicAttribute() =
    inherit System.Attribute()

[<Sealed>]
[<System.AttributeUsage(System.AttributeTargets.Property, AllowMultiple = false)>]
type SoloId(idGenerator: Type) =
    inherit IndexedAttribute(true)

    member val IdGenerator = idGenerator