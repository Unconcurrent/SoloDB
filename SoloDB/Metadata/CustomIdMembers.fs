namespace SoloDatabase

open System
open System.Reflection
open SoloDatabase.Attributes

/// Registers both an identity declaration and its explicit absence.
[<AbstractClass; Sealed>]
type internal CustomIdMetadata private () =
    static member RegisterAbsent<'T>() = TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with CustomIdentity = MetadataValue.generated (Array.empty<CustomIdMember>) })
    static member Member<'T,'Id>(declaringType: Type, name: string, read: Func<'T,'Id>, write: Action<obj,'Id>, generator: Func<obj>) =
        if isNull read then nullArg "read"
        if isNull write then nullArg "write"
        if isNull generator then nullArg "generator"
        CustomIdMember(declaringType, name, typeof<'Id>,
            Func<obj,obj>(fun owner -> box(read.Invoke(unbox<'T> owner))),
            Action<obj,obj>(fun owner value -> write.Invoke(owner, unbox<'Id> value)), generator,
            Func<obj,bool>(fun value -> System.Collections.Generic.EqualityComparer<'Id>.Default.Equals(unbox<'Id> value, Unchecked.defaultof<'Id>)))
    static member Register<'T>(members: CustomIdMember[]) =
        if isNull members then nullArg "members"
        TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with CustomIdentity = MetadataValue.generated (Array.copy members) })
    static member internal GetMembers(ownerType: Type) =
        TypeMetadataCache.Discover(ownerType, (fun metadata -> metadata.CustomIdentity),
            (fun operation metadata -> { metadata with CustomIdentity = ValueSome operation }), Func<CustomIdMember[]>(fun () ->
                if not (MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "custom identity" ownerType
                ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                |> Array.choose(fun property ->
                    let attribute = property.GetCustomAttribute<SoloId>(true)
                    if Utils.isNull attribute then None else
                    let reader = lazy (QueryValueMetadata.Reader(property,ownerType))
                    let writer = lazy (QueryMethodDiscovery.setter property)
                    let defaultValue = lazy (
                        if not property.PropertyType.IsValueType then null else
                        let constructor = Option.ofObj(property.PropertyType.GetConstructor(Type.EmptyTypes))
                        let create = QueryMethodDiscovery.defaultConstruction property.PropertyType constructor
                        try create.Invoke() with error -> raise(TargetInvocationException(error)))
                    let createGenerator = lazy (
                        if not property.CanWrite then
                            raise (InvalidOperationException(sprintf "Cannot create a generator for a non writtable parameter '%s' for type %s" property.Name ownerType.FullName))
                        let generatorType = attribute.IdGenerator
                        if isNull generatorType then
                            raise (InvalidOperationException(sprintf "Generator type for Id property (%s) is null." property.Name))
                        if typeof<bool>.Equals(property.PropertyType) || typeof<nativeint>.Equals(property.PropertyType) || typeof<unativeint>.Equals(property.PropertyType) then
                            raise (InvalidOperationException(sprintf "Error: Invalid [<SoloId>] type on '%s.%s'.\nReason: '%s' has a value space too small or too platform-dependent to serve as a stable identity key (bool: 2 values; IntPtr/UIntPtr: width depends on runtime).\nFix: Use a wider type — int64, Guid, string, or a custom struct." ownerType.FullName property.Name property.PropertyType.Name))
                        if generatorType.GetInterfaces() |> Array.exists(fun contract -> contract.FullName.StartsWith "SoloDatabase.Attributes.IIdGenerator" && not contract.IsArray) |> not then
                            raise (InvalidOperationException(sprintf "Generator type for Id property (%s) does not implement the IIdGenerator or IIdGenerator<'T> interface." property.Name))
                        let constructor = Option.ofObj(generatorType.GetConstructor(Type.EmptyTypes))
                        if constructor.IsNone && not generatorType.IsValueType then
                            raise(MissingMethodException($"Cannot dynamically create an instance of type '{generatorType.FullName}'. Reason: No parameterless constructor defined."))
                        QueryMethodDiscovery.defaultConstruction generatorType constructor)
                    let generator = Func<obj>(fun () ->
                        let create = createGenerator.Value
                        try create.Invoke() with error -> raise(TargetInvocationException(error)))
                    Some(CustomIdMember(property.DeclaringType,property.Name,property.PropertyType,
                        Func<obj,obj>(fun owner -> reader.Value.Invoke owner),
                        Action<obj,obj>(fun owner value ->
                            try writer.Value.Invoke(owner,value)
                            with error -> raise(TargetInvocationException(error))), generator,
                        Func<obj,bool>(fun value -> obj.Equals(value,defaultValue.Value)))))))
    static member internal TryGetMembers(ownerType: Type) =
        MetadataValue.tryValue (TypeMetadataCache.Get(ownerType).CustomIdentity)
    static member internal TryGet(ownerType: Type) =
        match CustomIdMetadata.TryGetMembers ownerType with
        | ValueSome [||] -> ValueSome ValueNone
        | ValueSome members -> ValueSome(ValueSome members.[0])
        | ValueNone -> ValueNone
