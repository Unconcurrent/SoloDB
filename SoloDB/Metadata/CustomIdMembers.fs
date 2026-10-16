namespace SoloDatabase

open System
open System.Reflection
open SoloDatabase.Attributes
open SoloDatabase.Metadata.Generation

/// Registers both an identity declaration and its explicit absence.
[<AbstractClass; Sealed>]
type internal CustomIdMetadata private () =
    static member RegisterAbsent<'T>() = TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with CustomIdentity = MetadataValue.generated (Array.empty<CustomIdMember>) })
    static member private Create(declaringType, name, idType, read: Lazy<Func<obj,obj>>, write: Lazy<Action<obj,obj>>,
                                 generator: Lazy<Func<obj>>, defaultFactory: Lazy<Func<obj>>) =
        let defaultValue = lazy (
            let create = defaultFactory.Value
            try create.Invoke() with error -> raise(TargetInvocationException(error)))
        CustomIdMember(declaringType,name,idType,
            Func<obj,obj>(fun owner -> read.Value.Invoke owner),
            Action<obj,obj>(fun owner value ->
                try write.Value.Invoke(owner,value) with error -> raise(TargetInvocationException(error))),
            Func<obj>(fun () ->
                let create = generator.Value
                try create.Invoke() with error -> raise(TargetInvocationException(error))),
            Func<obj,bool>(fun value -> obj.Equals(value,defaultValue.Value)))

    static member private AdmitGenerator(ownerType: Type, name, idType: Type, writable, generatorType: Type, family, constructor) =
        if not writable then
            raise (InvalidOperationException(sprintf "Cannot create a generator for a non writtable parameter '%s' for type %s" name ownerType.FullName))
        if isNull generatorType then
            raise (InvalidOperationException(sprintf "Generator type for Id property (%s) is null." name))
        if typeof<bool>.Equals(idType) || typeof<nativeint>.Equals(idType) || typeof<unativeint>.Equals(idType) then
            raise (InvalidOperationException(sprintf "Error: Invalid [<SoloId>] type on '%s.%s'.\nReason: '%s' has a value space too small or too platform-dependent to serve as a stable identity key (bool: 2 values; IntPtr/UIntPtr: width depends on runtime).\nFix: Use a wider type — int64, Guid, string, or a custom struct." ownerType.FullName name idType.Name))
        if not family then
            raise (InvalidOperationException(sprintf "Generator type for Id property (%s) does not implement the IIdGenerator or IIdGenerator<'T> interface." name))
        if not constructor && not generatorType.IsValueType then
            raise(MissingMethodException($"Cannot dynamically create an instance of type '{generatorType.FullName}'. Reason: No parameterless constructor defined."))

    static member Member<'T,'Id>(declaringType: Type, name: string, read: Func<obj,'Id>, write: Action<obj,'Id>,
                                 generatorType: Type, family: bool, constructor: bool, writable: bool, indexed: bool,
                                 generator: Func<obj>, defaultFactory: Func<obj>) =
        let reader = lazy (
            TypeSupport.RequireQueryProperty(false,not(isNull read),indexed)
            Func<obj,obj>(fun owner -> box(read.Invoke owner)))
        let writer = lazy (
            TypeSupport.RequireQueryProperty(true,not(isNull write),indexed)
            Action<obj,obj>(fun owner value -> write.Invoke(owner,unbox<'Id> value)))
        let createGenerator = lazy (
            CustomIdMetadata.AdmitGenerator(typeof<'T>,name,typeof<'Id>,writable,generatorType,family,constructor)
            generator)
        CustomIdMetadata.Create(declaringType,name,typeof<'Id>,reader,writer,createGenerator,lazy defaultFactory)
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
                    let defaultFactory = lazy (
                        if not property.PropertyType.IsValueType then Func<obj>(fun () -> null) else
                        let constructor = Option.ofObj(property.PropertyType.GetConstructor(Type.EmptyTypes))
                        QueryMethodDiscovery.defaultConstruction property.PropertyType constructor)
                    let createGenerator = lazy (
                        let generatorType = attribute.IdGenerator
                        let constructor = if isNull generatorType then None else Option.ofObj(generatorType.GetConstructor(Type.EmptyTypes))
                        let family = not(isNull generatorType) && (generatorType.GetInterfaces() |> Array.exists(fun contract -> TypeSupport.IdentityGeneratorContract(contract.FullName,contract.IsArray)))
                        CustomIdMetadata.AdmitGenerator(ownerType,property.Name,property.PropertyType,property.CanWrite,generatorType,family,constructor.IsSome)
                        QueryMethodDiscovery.defaultConstruction generatorType constructor)
                    Some(CustomIdMetadata.Create(property.DeclaringType,property.Name,property.PropertyType,
                                                 reader,writer,createGenerator,defaultFactory)))))
    static member internal TryGetMembers(ownerType: Type) =
        MetadataValue.tryValue (TypeMetadataCache.Get(ownerType).CustomIdentity)
    static member internal TryGet(ownerType: Type) =
        match CustomIdMetadata.TryGetMembers ownerType with
        | ValueSome [||] -> ValueSome ValueNone
        | ValueSome members -> ValueSome(ValueSome members.[0])
        | ValueNone -> ValueNone
