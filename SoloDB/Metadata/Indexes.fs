namespace SoloDatabase

open System
open System.Reflection

module internal IndexMetadata =
    let internal register ownerType (indexes: DeclaredIndex[]) =
        if isNull indexes then nullArg "indexes"
        let source = ReadyIndexes(Array.copy indexes)
        TypeMetadataCache.Register(ownerType, fun metadata -> { metadata with Indexes = MetadataValue.generated source })

    let private prepare = Func<SchemaIndexSource,SchemaIndexSource voption>(function
        | PendingIndexes create -> ValueSome(ReadyIndexes(create.Invoke()))
        | ReadyIndexes _ -> ValueNone)

    let internal select name valueType (attributes: obj[]) =
        attributes |> Array.tryPick(function
            | :? Attributes.IndexedAttribute as attribute ->
                Some(DeclaredIndex(name,valueType,attribute.Unique,attribute :? Attributes.SoloId))
            | _ -> None)

    let internal fields<'T>() =
        match MetadataValue.tryValue (TypeMetadataCache.Get(typeof<'T>).Indexes) with
        | ValueSome(ReadyIndexes indexes) -> indexes
        | _ ->
        let source = TypeMetadataCache.Discover(typeof<'T>, (fun metadata -> metadata.Indexes),
            (fun value metadata -> { metadata with Indexes = ValueSome value }), Func<SchemaIndexSource>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "declared indexes" typeof<'T>
                let indexes = typeof<'T>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                              |> Array.choose(fun property -> select property.Name property.PropertyType (property.GetCustomAttributes(true)))
                ReadyIndexes indexes),prepare)
        match source with
        | ReadyIndexes indexes -> indexes
        | PendingIndexes _ -> invalidOp "Schema index acquisition did not complete."

/// Publishes original schema acquisition without executing attribute code at registration.
[<AbstractClass; Sealed>]
type internal SchemaMetadata private () =
    static member RegisterIndexes<'T>(indexes: DeclaredIndex[]) = IndexMetadata.register typeof<'T> indexes
    static member RegisterIndexFactories<'T>(properties: SchemaProperty[]) =
        if isNull properties then nullArg "properties"
        let properties = Array.copy properties
        let create = Func<DeclaredIndex[]>(fun () -> properties |> Array.choose(fun property ->
            IndexMetadata.select property.Name property.ValueType (property.Attributes.Invoke())))
        TypeMetadataCache.Register(typeof<'T>,fun metadata -> { metadata with Indexes = MetadataValue.generated(PendingIndexes create) })
    static member UncompletedAttributes(_owner: RuntimeTypeHandle,_accessor: RuntimeMethodHandle) : obj[] =
        invalidOp "Generated schema attributes require completed compiler metadata."
    static member NamedAssignmentError(name: string,property: bool,error: Exception) : unit =
        let inner = if property then TargetInvocationException(error) :> Exception else error
        let kind = if property then "property" else "field"
        raise(CustomAttributeFormatException($"'{name}' {kind} specified was not found.",inner))
