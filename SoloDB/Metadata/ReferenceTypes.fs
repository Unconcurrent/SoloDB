namespace SoloDatabase

open System
open SoloDatabase.Metadata.Generation

/// Publishes the closed reference contract without inspecting generic arguments at runtime.
[<AbstractClass; Sealed>]
type internal ReferenceTypeMetadata private () =
    static member Register<'Reference,'Target>(many: bool, identityType: Type) =
        TypeMetadataCache.Register(typeof<'Reference>, fun metadata -> { metadata with ReferenceType = MetadataValue.generated ({ Target = typeof<'Target>; Identity = identityType; Many = many }) })
    static member internal TryGet(referenceType: Type) =
        if isNull referenceType then ValueNone else
        MetadataValue.tryValue (TypeMetadataCache.Get(referenceType).ReferenceType)

module internal DBRefTypeHelpers =
    let private shape (typ: Type) =
        if isNull typ then TypeShape.Object else TypeSupport.ReferenceShape typ.FullName

    let internal isDBRefSingleDefinition (typ: Type) = shape typ = TypeShape.DocumentReference
    let internal isDBRefTypedDefinition (typ: Type) = shape typ = TypeShape.TypedDocumentReference
    let internal isDBRefDefinition typ = isDBRefSingleDefinition typ || isDBRefTypedDefinition typ
    let internal isDBRefManyDefinition (typ: Type) =
        match shape typ with TypeShape.ManyReference | TypeShape.TypedManyReference -> true | _ -> false
    let internal isDBRefManyTypedDefinition (typ: Type) = shape typ = TypeShape.TypedManyReference
    let internal isFSharpOptionDefinition (typ: Type) = not(isNull typ) && TypeSupport.IsOption typ

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
        match shape t with TypeShape.FileReference | TypeShape.DirectoryReference -> true | _ -> false

    let internal isFileRefManyType (t: Type) =
        match shape t with TypeShape.FileMany | TypeShape.DirectoryMany -> true | _ -> false

    let internal isSingleReferenceType t = isDBRefType t || isFileRefType t
    let internal isManyReferenceType t = isDBRefManyType t || isFileRefManyType t

    let internal referenceTargetType (t: Type) =
      match ReferenceTypeMetadata.TryGet(t) with
      | ValueSome facts -> facts.Target
      | ValueNone ->
        match shape t with
        | TypeShape.FileReference | TypeShape.FileMany -> typeof<SoloDatabase.Types.SoloDBFileHeader>
        | TypeShape.DirectoryReference | TypeShape.DirectoryMany -> typeof<SoloDatabase.Types.SoloDBDirectoryHeader>
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
    
