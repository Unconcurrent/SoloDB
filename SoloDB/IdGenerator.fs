namespace SoloDatabase.Attributes

open System
open SoloDatabase

#nowarn "3535"

[<Interface>]
type IIdGenerator =
    /// <summary>
    /// object collection -> object document -> id
    /// </summary>
    abstract GenerateId: obj -> obj -> obj
    abstract IsEmpty: obj -> bool

[<Sealed>]
[<System.AttributeUsage(System.AttributeTargets.Property, AllowMultiple = false)>]
type SoloId(idGenerator: Type) =
    inherit IndexedAttribute(true)

    member val IdGenerator = idGenerator