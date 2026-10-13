module internal SoloDatabase.EntityIdentity

open System
open System.Reflection
open SoloDatabase


let internal compiledInt64IdReader (entityType: Type) =
    match EntityIdMetadata.TryGet(entityType) with
    | ValueSome operations ->
        if isNull operations.Reader then
            raise (InvalidOperationException($"Type '{entityType.FullName}' does not have a public int64 Id property."))
        operations.Reader
    | ValueNone when not (MetadataRuntime.supportsDynamicCode()) -> MetadataRuntime.missing "physical identity" entityType
    | ValueNone ->
    let operation = TypeMetadataCache.Discover(entityType, (fun metadata ->
        match metadata.PhysicalIdentity with
        | ValueSome operation -> ValueSome { Generated = operation.Generated; Value = operation.Value.Reader }
        | ValueNone -> metadata.Int64Reader), (fun operation metadata -> { metadata with Int64Reader = ValueSome operation }), Func<_>(fun () ->
        let t = entityType
        let prop = t.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull prop || not (typeof<int64>.Equals(prop.PropertyType)) then
            raise (InvalidOperationException($"Type '{t.FullName}' does not have a public int64 Id property."))
        QueryMethodDiscovery.readerResult<int64> prop
    ))
    if isNull operation then
        raise (InvalidOperationException($"Type '{entityType.FullName}' does not have a public int64 Id property."))
    operation

let internal compiledInt64IdWriter (entityType: Type) =
    match EntityIdMetadata.TryGet(entityType) with
    | ValueSome operations ->
        if isNull operations.Writer then
            raise (InvalidOperationException($"Type '{entityType.FullName}' does not have a writable int64 Id property."))
        operations.Writer
    | ValueNone when not (MetadataRuntime.supportsDynamicCode()) -> MetadataRuntime.missing "physical identity" entityType
    | ValueNone ->
    let operation = TypeMetadataCache.Discover(entityType, (fun metadata ->
        match metadata.PhysicalIdentity with
        | ValueSome operation -> ValueSome { Generated = operation.Generated; Value = operation.Value.Writer }
        | ValueNone -> metadata.Int64Writer), (fun operation metadata -> { metadata with Int64Writer = ValueSome operation }), Func<_>(fun () ->
        let t = entityType
        let prop = t.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
        if isNull prop || not (typeof<int64>.Equals(prop.PropertyType)) || not prop.CanWrite then
            raise (InvalidOperationException($"Type '{t.FullName}' does not have a writable int64 Id property."))
        QueryMethodDiscovery.setterValue<int64> prop
    ))
    if isNull operation then
        raise (InvalidOperationException($"Type '{entityType.FullName}' does not have a writable int64 Id property."))
    operation

let private tryDiscoverWritableInt64IdProperty (entityType: Type) =
    let prop = entityType.GetProperty("Id", BindingFlags.Public ||| BindingFlags.Instance)
    if isNull prop || not prop.CanRead || not prop.CanWrite || not (typeof<int64>.Equals(prop.PropertyType)) then ValueNone
    else ValueSome prop

let internal hasWritableInt64Id (entityType: Type) =
    match EntityIdMetadata.TryGet(entityType) with
    | ValueSome operations -> operations.HasWritableId
    | ValueNone when MetadataRuntime.supportsDynamicCode() ->
        TypeMetadataCache.Discover(entityType, (fun metadata ->
            match metadata.PhysicalIdentity with
            | ValueSome operation -> ValueSome { Generated = operation.Generated; Value = operation.Value.HasWritableId }
            | ValueNone -> metadata.WritableInt64), (fun operation metadata -> { metadata with WritableInt64 = ValueSome operation }), Func<bool>(fun () -> (tryDiscoverWritableInt64IdProperty entityType).IsSome))
    | ValueNone -> MetadataRuntime.missing "physical identity" entityType

let internal tryInt64IdWriter (entityType: Type) =
    if hasWritableInt64Id entityType then ValueSome (compiledInt64IdWriter entityType)
    else ValueNone

let private trivialGetter (property: PropertyInfo) =
    let getter = property.GetMethod
    if isNull getter || (getter.IsVirtual && not getter.IsFinal && not property.DeclaringType.IsSealed) then false
    else
        let body = getter.GetMethodBody()
        if isNull body then false
        else
            let il = body.GetILAsByteArray()
            // Dispatch must not override this direct field read with user code.
            not (isNull il) && il.Length = 7 && il.[0] = 0x02uy && il.[1] = 0x7buy && il.[6] = 0x2auy


let internal hasTrivialInt64IdReader (entityType: Type) =
    match EntityIdMetadata.TryGet(entityType) with
    | ValueSome operations -> operations.TrivialGetter
    | ValueNone when not (MetadataRuntime.supportsDynamicCode()) -> MetadataRuntime.missing "physical identity" entityType
    | ValueNone ->
    TypeMetadataCache.Discover(entityType, (fun metadata ->
        match metadata.PhysicalIdentity with
        | ValueSome operation -> ValueSome { Generated = operation.Generated; Value = operation.Value.TrivialGetter }
        | ValueNone -> metadata.TrivialInt64), (fun operation metadata -> { metadata with TrivialInt64 = ValueSome operation }), Func<bool>(fun () ->
        let t = entityType
        match tryDiscoverWritableInt64IdProperty t with
        | ValueSome property -> trivialGetter property
        | ValueNone -> false))
