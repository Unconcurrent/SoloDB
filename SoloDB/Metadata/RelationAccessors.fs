
/// Relation operations acquired through the shared type metadata owner.
module internal SoloDatabase.RelationsAccessorCache

open System
open SoloDatabase.DocumentReferenceConstruction
open System.Reflection
open System.Reflection.Emit
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation
open System.Linq.Expressions
open SoloDatabase

// ─── Property accessor caches (getter / setter) ─────────────────────────────


let private buildGetter (t: Type) (n: string) =
    if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "relation property reader" t
    let prop = t.GetProperty(n, BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
    if isNull prop then ValueNone else ValueSome(QueryValueMetadata.Reader(prop,t))

let private buildSetter (t: Type) (n: string) =
    if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "relation property writer" t
    let prop = t.GetProperty(n, BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
    if isNull prop then raise (InvalidOperationException($"Property '{n}' not found on type '{t.FullName}'."))
    QueryMethodDiscovery.setter prop

let internal tryCompiledGetter (ownerType: Type) (propName: string) =
    TypeMetadataCache.Discover(ownerType, (fun metadata -> MetadataMap.tryFind propName metadata.NamedGetters),
        (fun operation metadata -> { metadata with NamedGetters = MetadataMap.set propName operation metadata.NamedGetters }),
        Func<_>(fun () -> buildGetter ownerType propName))

let internal compiledGetter (ownerType: Type) (propName: string) =
    match tryCompiledGetter ownerType propName with
    | ValueSome getter -> getter
    | ValueNone -> raise (InvalidOperationException($"Property '{propName}' not found on type '{ownerType.FullName}'."))

let internal compiledPendingTypedIdAccessors (t: Type) =
    TypeMetadataCache.Discover(t, (fun metadata -> metadata.PendingTypedId), (fun operation metadata -> { metadata with PendingTypedId = ValueSome operation }), Func<_>(fun () ->
        let predicate = t.GetProperty("HasPendingTypedId", BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)
        let typedId = t.GetProperty("TypedId", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull predicate || isNull typedId then ValueNone
        else
            let pending = QueryMethodDiscovery.readerResult<bool> predicate
            let value = QueryValueMetadata.Reader(typedId,t)
            ValueSome (struct(pending, value))))

let internal compiledSetter (ownerType: Type) (propName: string) =
    TypeMetadataCache.Discover(ownerType, (fun metadata -> MetadataMap.tryFind propName metadata.NamedSetters),
        (fun operation metadata -> { metadata with NamedSetters = MetadataMap.set propName operation metadata.NamedSetters }),
        Func<_>(fun () -> buildSetter ownerType propName))

// ─── PropertyInfo-keyed accessors (for descriptors that already hold PropertyInfo) ─────


let internal compiledPropGetter (prop: PropertyInfo) =
    TypeMetadataCache.Discover(prop.DeclaringType, (fun metadata -> MetadataMap.tryFind prop metadata.PropertyGetters),
        (fun operation metadata -> { metadata with PropertyGetters = MetadataMap.set prop operation metadata.PropertyGetters }), Func<_>(fun () ->
        QueryValueMetadata.Reader(prop,prop.DeclaringType)
    ))

let internal compiledPropSetter (prop: PropertyInfo) =
    TypeMetadataCache.Discover(prop.DeclaringType, (fun metadata -> MetadataMap.tryFind prop metadata.PropertySetters),
        (fun operation metadata -> { metadata with PropertySetters = MetadataMap.set prop operation metadata.PropertySetters }), Func<_>(fun () ->
        match TypeMetadataCache.Read(prop.DeclaringType,fun metadata -> MetadataMap.tryFind prop.Name metadata.NamedSetters) with
        | ValueSome operation -> operation.Value
        | ValueNone ->
            if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "relation property writer" prop.DeclaringType
            QueryMethodDiscovery.setter prop
    ))

// ─── DBRef Id reader: reads .Id from a boxed DBRef<T> struct ─────────────────


let internal compiledDbRefIdReader (dbRefType: Type) =
    TypeMetadataCache.Discover(dbRefType, (fun metadata -> metadata.ReferenceIdReader), (fun operation metadata -> { metadata with ReferenceIdReader = ValueSome operation }), Func<_>(fun () ->
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "reference identity" dbRefType
        let id = dbRefType.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull id then Func<obj,int64>(fun _ -> 0L)
        elif typeof<int64>.Equals(id.PropertyType) then QueryMethodDiscovery.readerResult<int64> id
        else
            let read = QueryValueMetadata.Reader(id,dbRefType)
            let convert = QueryValueMetadata.Unary(ExpressionType.Convert,id.PropertyType,typeof<int64>)
            Func<obj,int64>(fun value -> unbox<int64>(convert.Invoke(read.Invoke value)))
    ))

// ─── DBRef factory delegates ─────────────────────────────────────────────────

let internal compiledDbRefFactories (dbRefType: Type) =
    TypeMetadataCache.Discover(dbRefType, (fun metadata -> metadata.ReferenceFactories), (fun operation metadata -> { metadata with ReferenceFactories = ValueSome operation }), Func<_>(fun () ->
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "reference construction" dbRefType
        let arguments = dbRefType.GetGenericArguments()
        let typed = arguments.Length = 2
        let entity = DBRefTypeHelpers.referenceTargetType dbRefType
        let methodFor name types = dbRefType.GetMethod(name,BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static,null,types,null)
        let one (method: MethodInfo) =
            if isNull method then null else
            let call = QueryValueMetadata.Call method
            Func<int64,obj>(fun id -> call.Invoke(null,[|box id|]))
        let two (method: MethodInfo) =
            if isNull method then null else
            let call = QueryValueMetadata.Call method
            Func<int64,obj,obj>(fun id value -> call.Invoke(null,[|box id;value|]))
        let three (method: MethodInfo) =
            if isNull method then null else
            let call = QueryValueMetadata.Call method
            Func<int64,obj,obj,obj>(fun id identity value -> call.Invoke(null,[|box id;identity;value|]))
        { ToOrResolved = if typed then null else one(methodFor "To" [|typeof<int64>|])
          Loaded = two(methodFor "Loaded" [|typeof<int64>;entity|])
          ResolvedTyped = if typed then two(methodFor "Resolved" [|typeof<int64>;arguments.[1]|]) else null
          LoadedTyped = if typed then three(methodFor "Loaded" [|typeof<int64>;arguments.[1];entity|]) else null }
    ))

// ─── ValueOption introspection ───────────────────────────────────────────────

let internal compiledValueOptionAccessors (t: Type) =
    TypeMetadataCache.Discover(t, (fun metadata -> metadata.ValueOption), (fun operation metadata -> { metadata with ValueOption = ValueSome operation }), Func<_>(fun () ->
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "optional reference" t
        let predicate = t.GetProperty("IsSome", BindingFlags.Public ||| BindingFlags.Instance)
        let value = t.GetProperty("Value", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull predicate || isNull value then ValueNone
        else ValueSome { IsSome = QueryMethodDiscovery.readerResult<bool> predicate
                         GetValue = QueryValueMetadata.Reader(value,t) }
    ))

// ─── Activator replacement: parameterless constructor ────────────────────────


let internal compiledDefaultCtor (t: Type) =
    TypeMetadataCache.Discover(t, (fun metadata -> metadata.DefaultConstructor), (fun operation metadata -> { metadata with DefaultConstructor = ValueSome operation }), Func<_>(fun () ->
        if not (MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "default construction" t
        let constructor = t.GetConstructor(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance,null,Type.EmptyTypes,null)
        TypeSupport.RequireDefaultConstructor(not(isNull constructor) || t.IsValueType,t.FullName)
        QueryMethodDiscovery.defaultConstruction t (Option.ofObj constructor)
    ))

/// Activator-style construction retains its public exception boundary.
let internal activateDefault (t: Type) =
    let create = compiledDefaultCtor t
    try create.Invoke()
    with error -> raise(TargetInvocationException(error))

// ─── Add method delegate for DBRefMany<T> cloning ───────────────────────────


let internal compiledAddMethod (collectionType: Type) =
    TypeMetadataCache.Discover(collectionType, (fun metadata -> metadata.CollectionAdder), (fun operation metadata -> { metadata with CollectionAdder = ValueSome operation }), Func<_>(fun () ->
        let t = collectionType
        if not (MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "collection mutation" t
        let addMethod =
            t.GetMethods(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.tryFind (fun m -> m.Name = "Add" && m.GetParameters().Length = 1)
        match addMethod with
        | None ->
            raise (InvalidOperationException($"No Add(T) method found on '{t.FullName}'."))
        | Some m ->
            let call = QueryValueMetadata.Call m
            Action<obj,obj>(fun collection item -> call.Invoke(collection,[|item|]) |> ignore)
    ))

// ─── Clone helpers: per-type property copier for cloneOwnerForReplaceMany ────

let internal compiledCloneDescriptor (ownerType: Type) =
    let operations = TypeMetadataCache.Discover(ownerType, (fun metadata -> metadata.OwnerClone), (fun operation metadata -> { metadata with OwnerClone = ValueSome operation }), Func<_>(fun () ->
        let t = ownerType
        if not (MetadataRuntime.supportsDynamicCode()) then
            invalidOp ("Collection cloning operations have not been registered: " + t.FullName)
        let ctor = compiledDefaultCtor t

        let members =
            t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.filter(fun property -> TypeSupport.CloneProperty(false,false,property.CanRead,property.CanWrite,
                typeof<IDBRefManyInternal>.IsAssignableFrom(property.PropertyType)))
            |> Array.map(fun property ->
                TypeSupport.RequireNonIndexedProperty(property.GetIndexParameters().Length <> 0)
                { Name = property.Name; Type = property.PropertyType; Field = None
                  Getter = Some(property.GetGetMethod(true) :> MethodBase)
                  Setter = Some(property.GetSetMethod(true) :> MethodBase)
                  Static = false; Readable = true; Writable = true; Ignored = false; Many = false })
        let method = DynamicMethod("CopyOwnerScalars",typeof<Void>,[|typeof<obj>;typeof<obj>|],t.Module,true)
        OwnerCloneEmitter.copy (RuntimeCodecIL(method.GetILGenerator())) t t.IsValueType members
        let copyDelegate = method.CreateDelegate(typeof<Action<obj,obj>>) :?> Action<obj,obj>

        { CreateInstance = ctor; CopyScalars = copyDelegate; Constructible = true; Indexed = false }
    ))
    TypeSupport.RequireDefaultConstructor(operations.Constructible,ownerType.FullName)
    TypeSupport.RequireNonIndexedProperty operations.Indexed
    operations
