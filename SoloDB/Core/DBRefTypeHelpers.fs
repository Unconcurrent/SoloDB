namespace SoloDatabase

open System

module internal DBRefTypeHelpers =
    [<Literal>]
    let private dbRef1Name = "SoloDatabase.DBRef`1"
    [<Literal>]
    let private dbRef2Name = "SoloDatabase.DBRef`2"
    [<Literal>]
    let private dbRefManyName = "SoloDatabase.DBRefMany`1"
    [<Literal>]
    let private fsharpOptionName = "Microsoft.FSharp.Core.FSharpOption`1"

    let internal isDBRefSingleDefinition (t: Type) =
        not (isNull t) && StringComparer.Ordinal.Equals(t.FullName, dbRef1Name)

    let internal isDBRefTypedDefinition (t: Type) =
        not (isNull t) && StringComparer.Ordinal.Equals(t.FullName, dbRef2Name)

    let internal isDBRefDefinition (t: Type) =
        isDBRefSingleDefinition t || isDBRefTypedDefinition t

    let internal isDBRefManyDefinition (t: Type) =
        not (isNull t) && StringComparer.Ordinal.Equals(t.FullName, dbRefManyName)

    let internal isFSharpOptionDefinition (t: Type) =
        not (isNull t) && StringComparer.Ordinal.Equals(t.FullName, fsharpOptionName)

    let internal isDBRefType (t: Type) =
        not (isNull t) && t.IsGenericType && isDBRefDefinition (t.GetGenericTypeDefinition())

    let internal isDBRefTypedType (t: Type) =
        not (isNull t) && t.IsGenericType && isDBRefTypedDefinition (t.GetGenericTypeDefinition())

    let internal isDBRefManyType (t: Type) =
        not (isNull t) && t.IsGenericType && isDBRefManyDefinition (t.GetGenericTypeDefinition())

    let internal isAnyRelationRefType (t: Type) =
        isDBRefType t || isDBRefManyType t

    let internal tryUnwrapFSharpOption (t: Type) =
        if not (isNull t) && t.IsGenericType && isFSharpOptionDefinition (t.GetGenericTypeDefinition()) then
            ValueSome (t.GetGenericArguments().[0])
        else
            ValueNone

    let internal isOptionWrappedRelationRefType (t: Type) =
        match tryUnwrapFSharpOption t with
        | ValueSome inner -> isAnyRelationRefType inner
        | ValueNone -> false
