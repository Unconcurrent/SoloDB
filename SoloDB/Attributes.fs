namespace SoloDatabase.Attributes

[<Sealed>]
[<System.AttributeUsage(System.AttributeTargets.Property)>]
type IndexedAttribute(unique: bool) =
    inherit System.Attribute()

    member this.Unique = unique