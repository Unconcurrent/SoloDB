namespace SoloDatabase

open System


/// Publishes direct construction and ordered scalar copying for replacement operations.
[<AbstractClass; Sealed>]
type internal CollectionCloneMetadata private () =
    static member Register<'Owner>(create: Func<'Owner>, copy: Action<'Owner, obj>) =
        if isNull create then nullArg "create"
        if isNull copy then nullArg "copy"
        let operations = {
            CreateInstance = Func<obj>(fun () -> box(create.Invoke()))
            CopyScalars = Action<obj,obj>(fun source destination -> copy.Invoke(unbox<'Owner> source, destination))
        }
        TypeMetadataCache.Register(typeof<'Owner>, fun metadata -> { metadata with OwnerClone = MetadataValue.generated operations })

