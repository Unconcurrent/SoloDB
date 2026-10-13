namespace SoloDatabase

open System
open System.Reflection
open System.Collections.Concurrent

module internal IndexMetadata =
    let internal register ownerType (indexes: DeclaredIndex[]) =
        if isNull indexes then nullArg "indexes"
        TypeMetadataCache.Register(ownerType, fun metadata -> { metadata with Indexes = MetadataValue.generated (Array.copy indexes) })
    let internal fields<'T>() =
        match MetadataValue.tryValue (TypeMetadataCache.Get(typeof<'T>).Indexes) with
        | ValueSome indexes -> indexes
        | _ when not (MetadataRuntime.supportsDynamicCode()) -> MetadataRuntime.missing "declared indexes" typeof<'T>
        | _ -> TypeMetadataCache.Discover(typeof<'T>, (fun metadata -> metadata.Indexes), (fun value metadata -> { metadata with Indexes = ValueSome value }), Func<DeclaredIndex[]>(fun () ->
            let ownerType = typeof<'T>
            ownerType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.choose(fun property ->
                property.GetCustomAttributes(true)
                |> Array.tryPick(function
                    | :? SoloDatabase.Attributes.IndexedAttribute as attribute ->
                        Some(DeclaredIndex(property.Name, property.PropertyType, attribute.Unique, attribute :? SoloDatabase.Attributes.SoloId))
                    | _ -> None))))

/// Publishes declared indexes without constructing or discovering runtime attributes.
[<AbstractClass; Sealed>]
type internal SchemaMetadata private () =
    static member RegisterIndexes<'T>(indexes: DeclaredIndex[]) = IndexMetadata.register typeof<'T> indexes
