namespace SoloDatabase

open System


/// Publishes direct construction and ordered scalar copying for replacement operations.
[<AbstractClass; Sealed>]
type internal CollectionCloneMetadata private () =
    static member Register<'Owner>(create: Func<'Owner>, copy: Action<obj, obj>, constructible: bool, indexed: bool) =
        if isNull create then nullArg "create"
        if isNull copy then nullArg "copy"
        let operations = {
            CreateInstance = Func<obj>(fun () -> box(create.Invoke()))
            CopyScalars = copy
            Constructible = constructible
            Indexed = indexed
        }
        TypeMetadataCache.Register(typeof<'Owner>, fun metadata -> { metadata with OwnerClone = MetadataValue.generated operations })

