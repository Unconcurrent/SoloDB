namespace SoloDatabase

open System
open System.Reflection
open SoloDatabase.Attributes
open Utils

/// Publishes the ordered reference surface without discovering runtime members.
[<AbstractClass; Sealed>]
type internal RelationMetadata private () =
    static member Member<'Owner, 'Reference>(declaringType: Type, name: string,
                                            read: Func<'Owner, 'Reference>, write: Action<obj, 'Reference>,
                                            ignored: bool, policy: RelationPolicy) =
        let reader = if isNull read then null else Func<obj, obj>(fun owner -> box (read.Invoke(unbox<'Owner> owner)))
        let writer = if isNull write then null else Action<obj, obj>(fun owner value -> write.Invoke(owner, unbox<'Reference> value))
        RelationMember(name, declaringType, typeof<'Reference>, reader, writer, ignored, policy)
    static member Register<'Owner>(properties: RelationMember[]) =
        if isNull properties then nullArg "properties"
        TypeMetadataCache.Register(typeof<'Owner>, fun metadata ->
            { metadata with Relations = MetadataValue.generated (Array.copy properties)
                            UnionKind = MetadataValue.generated metadata.Union.IsSome })
    /// Records compiler-established union admission without runtime case discovery.
    static member RegisterUnion<'Owner>(hostedReferences: bool) =
        TypeMetadataCache.Register(typeof<'Owner>, fun metadata -> { metadata with Union = MetadataValue.generated (hostedReferences); UnionKind = MetadataValue.generated true })
    static member internal IsUnion(ownerType: Type) =
        TypeMetadataCache.Discover(ownerType, (fun metadata -> metadata.UnionKind),
            (fun operation metadata -> { metadata with UnionKind = ValueSome operation }), Func<bool>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "union declaration" ownerType
                Microsoft.FSharp.Reflection.FSharpType.IsUnion(ownerType, BindingFlags.Public ||| BindingFlags.NonPublic)))
    static member internal TryGetUnion(ownerType: Type) =
        MetadataValue.tryValue (TypeMetadataCache.Get(ownerType).Union)
    static member internal TryGet(ownerType: Type) =
        MetadataValue.tryValue (TypeMetadataCache.Get(ownerType).Relations)
    static member internal Get(ownerType: Type) =
        match RelationMetadata.TryGet(ownerType) with
        | ValueSome properties -> properties
        | ValueNone when not (MetadataRuntime.supportsDynamicCode()) -> MetadataRuntime.missing "relation declarations" ownerType
        | ValueNone ->
            TypeMetadataCache.Discover(ownerType, (fun metadata -> metadata.Relations), (fun operation metadata -> { metadata with Relations = ValueSome operation }), Func<RelationMember[]>(fun () ->
                ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                |> Array.filter(fun property -> DBRefTypeHelpers.isAnyRelationRefType property.PropertyType || DBRefTypeHelpers.containsNestedRelationRefType property.PropertyType)
                |> Array.map(fun property ->
                    let attribute = property.GetCustomAttribute<SoloRefAttribute>(true)
                    let policy = if isNull attribute then null else RelationPolicy(attribute.OnDelete, attribute.OnOwnerDelete, attribute.Unique, attribute.OrderBy)
                    let reader = lazy (RelationsAccessorCache.compiledPropGetter property)
                    let writer = lazy (RelationsAccessorCache.compiledPropSetter property)
                    RelationMember(property.Name, property.DeclaringType, property.PropertyType,
                        (if property.CanRead then Func<obj,obj>(fun owner -> reader.Value.Invoke owner) else null),
                        (if property.CanWrite then Action<obj,obj>(fun owner value -> writer.Value.Invoke(owner,value)) else null),
                        not (isNull (property.GetCustomAttribute<System.Runtime.Serialization.IgnoreDataMemberAttribute>(true))), policy))))
    
