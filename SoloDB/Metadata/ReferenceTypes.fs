namespace SoloDatabase

open System
open System.Collections.Concurrent

/// Publishes the closed reference contract without inspecting generic arguments at runtime.
[<AbstractClass; Sealed>]
type internal ReferenceTypeMetadata private () =
    static member Register<'Reference,'Target>(many: bool, identityType: Type) =
        TypeMetadataCache.Register(typeof<'Reference>, fun metadata -> { metadata with ReferenceType = MetadataValue.generated ({ Target = typeof<'Target>; Identity = identityType; Many = many }) })
    static member internal TryGet(referenceType: Type) =
        if isNull referenceType then ValueNone else
        MetadataValue.tryValue (TypeMetadataCache.Get(referenceType).ReferenceType)

module internal DBRefTypeHelpers =
    [<Literal>]
    let private dbRef1Name = "SoloDatabase.DBRef`1"
    [<Literal>]
    let private dbRef2Name = "SoloDatabase.DBRef`2"
    [<Literal>]
    let private dbRefManyName = "SoloDatabase.DBRefMany`1"
    [<Literal>]
    let private dbRefMany2Name = "SoloDatabase.DBRefMany`2"
    [<Literal>]
    let private fsharpOptionName = "Microsoft.FSharp.Core.FSharpOption`1"

    let internal isDBRefSingleDefinition (t: Type) =
        not (isNull t) && StringComparer.Ordinal.Equals(t.FullName, dbRef1Name)

    let internal isDBRefTypedDefinition (t: Type) =
        not (isNull t) && StringComparer.Ordinal.Equals(t.FullName, dbRef2Name)

    let internal isDBRefDefinition (t: Type) =
        isDBRefSingleDefinition t || isDBRefTypedDefinition t

    let internal isDBRefManyDefinition (t: Type) =
        not (isNull t) && (StringComparer.Ordinal.Equals(t.FullName, dbRefManyName) || StringComparer.Ordinal.Equals(t.FullName, dbRefMany2Name))

    let internal isDBRefManyTypedDefinition (t: Type) =
        not (isNull t) && StringComparer.Ordinal.Equals(t.FullName, dbRefMany2Name)

    let internal isFSharpOptionDefinition (t: Type) =
        not (isNull t) && StringComparer.Ordinal.Equals(t.FullName, fsharpOptionName)

    let internal isDBRefType (t: Type) =
        match ReferenceTypeMetadata.TryGet(t) with
        | ValueSome facts -> not facts.Many
        | ValueNone -> not (isNull t) && t.IsGenericType && isDBRefDefinition (t.GetGenericTypeDefinition())

    let internal isDBRefTypedType (t: Type) =
        match ReferenceTypeMetadata.TryGet(t) with
        | ValueSome facts -> not facts.Many && not (isNull facts.Identity)
        | ValueNone -> not (isNull t) && t.IsGenericType && isDBRefTypedDefinition (t.GetGenericTypeDefinition())

    let internal isDBRefManyType (t: Type) =
        match ReferenceTypeMetadata.TryGet(t) with
        | ValueSome facts -> facts.Many
        | ValueNone -> not (isNull t) && t.IsGenericType && isDBRefManyDefinition (t.GetGenericTypeDefinition())

    let internal isFileRefType (t: Type) =
        not (isNull t) && (t.FullName = "SoloDatabase.FileRef" || t.FullName = "SoloDatabase.DirectoryRef")

    let internal isFileRefManyType (t: Type) =
        not (isNull t) && (t.FullName = "SoloDatabase.FileRefMany" || t.FullName = "SoloDatabase.DirectoryRefMany")

    let internal isSingleReferenceType t = isDBRefType t || isFileRefType t
    let internal isManyReferenceType t = isDBRefManyType t || isFileRefManyType t

    let internal referenceTargetType (t: Type) =
      match ReferenceTypeMetadata.TryGet(t) with
      | ValueSome facts -> facts.Target
      | ValueNone ->
        match t.FullName with
        | "SoloDatabase.FileRef" | "SoloDatabase.FileRefMany" -> typeof<SoloDatabase.Types.SoloDBFileHeader>
        | "SoloDatabase.DirectoryRef" | "SoloDatabase.DirectoryRefMany" -> typeof<SoloDatabase.Types.SoloDBDirectoryHeader>
        | _ -> t.GetGenericArguments().[0]

    let internal referenceIdentityType (t: Type) =
        match ReferenceTypeMetadata.TryGet(t) with
        | ValueSome facts -> if isNull facts.Identity then ValueNone else ValueSome facts.Identity
        | ValueNone ->
            if isDBRefTypedType t || (t.IsGenericType && isDBRefManyTypedDefinition(t.GetGenericTypeDefinition())) then
                ValueSome(t.GetGenericArguments().[1])
            else ValueNone

    let internal isAnyRelationRefType (t: Type) =
        isSingleReferenceType t || isManyReferenceType t

    let internal tryUnwrapFSharpOption (t: Type) =
        match ReferenceTypeMetadata.TryGet(t) with
        | ValueSome _ -> ValueNone
        | ValueNone ->
          if not (isNull t) && t.IsGenericType && isFSharpOptionDefinition (t.GetGenericTypeDefinition()) then
            ValueSome (t.GetGenericArguments().[0])
          else
            ValueNone

    let internal isOptionWrappedRelationRefType (t: Type) =
        match tryUnwrapFSharpOption t with
        | ValueSome inner -> isAnyRelationRefType inner
        | ValueNone -> false

    let rec internal containsNestedRelationRefType (t: Type) =
        if isNull t then false
        elif isAnyRelationRefType t then true
        elif t.IsArray then
            containsNestedRelationRefType (t.GetElementType())
        elif t.IsGenericType then
            t.GetGenericArguments() |> Array.exists containsNestedRelationRefType
        else
            false
    
