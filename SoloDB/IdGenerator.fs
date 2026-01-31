namespace SoloDatabase.Attributes

open System
open SoloDatabase
open System.Reflection
open System.Linq.Expressions

// Instantiating F# interfaces directly for generic ID generator factory pattern
#nowarn "3535"

[<Interface>]
type IIdGenerator =
    /// <summary>
    /// object collection -> object document -> id
    /// </summary>
    abstract GenerateId: obj -> obj -> obj
    abstract IsEmpty: obj -> bool

type IIdGenerator<'T> =
    /// <summary>
    /// object collection -> object document -> id
    /// </summary>
    abstract GenerateId: ISoloDBCollection<'T> -> 'T -> obj
    /// old id -> bool
    abstract IsEmpty: obj -> bool