namespace SoloDatabase

open System


/// Roots direct tracker construction and mutation over original closed target types.
[<AbstractClass; Sealed>]
type internal ReferenceCollectionMetadata private () =
    static member RegisterMany<'T>() =
        ReferenceTypeMetadata.Register<DBRefMany<'T>,'T>(true, null)
        TypeMetadataCache.Register(typeof<DBRefMany<'T>>, fun metadata -> { metadata with DefaultConstructor = MetadataValue.generated (Func<obj>(fun () -> box(DBRefMany<'T>()))) })
        TypeMetadataCache.Register(typeof<DBRefMany<'T>>, fun metadata -> { metadata with CollectionAdder = MetadataValue.generated (Action<obj,obj>(fun tracker item -> (unbox<DBRefMany<'T>> tracker).Add(unbox<'T> item))) })
    static member RegisterTypedMany<'T,'Id>() =
        ReferenceTypeMetadata.Register<DBRefMany<'T,'Id>,'T>(true, typeof<'Id>)
        TypeMetadataCache.Register(typeof<DBRefMany<'T,'Id>>, fun metadata -> { metadata with DefaultConstructor = MetadataValue.generated (Func<obj>(fun () -> box(DBRefMany<'T,'Id>()))) })
        TypeMetadataCache.Register(typeof<DBRefMany<'T,'Id>>, fun metadata -> { metadata with CollectionAdder = MetadataValue.generated (Action<obj,obj>(fun tracker item -> (unbox<DBRefMany<'T,'Id>> tracker).Add(unbox<'T> item))) })

/// Fixed filesystem reference contracts share the same metadata as document references.
[<AbstractClass; Sealed>]
type internal FileSystemReferenceMetadata private () =
    static member Single<'Reference,'Target>
        (create: Func<int64,'Reference>, loaded: Func<int64,'Target,'Reference>, isLoaded: Func<'Reference,bool>, value: Func<'Reference,'Target>, id: Func<'Reference,int64>) =
        ReferenceTypeMetadata.Register<'Reference,'Target>(false,null)
        let factories = {
            ToOrResolved = Func<int64,obj>(fun identity -> box(create.Invoke(identity)))
            Loaded = Func<int64,obj,obj>(fun identity entity -> box(loaded.Invoke(identity,unbox<'Target> entity)))
            ResolvedTyped = null
            LoadedTyped = null
        }
        TypeMetadataCache.Register(typeof<'Reference>,fun metadata ->
            { metadata with ReferenceFactories = MetadataValue.generated factories
                            ReferenceIdReader = MetadataValue.generated (Func<obj,int64>(fun reference -> id.Invoke(unbox<'Reference> reference)))
                            NamedGetters = metadata.NamedGetters
                                |> MetadataMap.set "IsLoaded" { Generated = true; Value = ValueSome(Func<obj,obj>(fun reference -> box(isLoaded.Invoke(unbox<'Reference> reference)))) }
                                |> MetadataMap.set "Value" { Generated = true; Value = ValueSome(Func<obj,obj>(fun reference -> box(value.Invoke(unbox<'Reference> reference)))) } })
    static member Many<'Reference,'Element,'Target>(create: Func<'Reference>, add: Action<'Reference,'Element>) =
        ReferenceTypeMetadata.Register<'Reference,'Target>(true,null)
        TypeMetadataCache.Register(typeof<'Reference>,fun metadata ->
            { metadata with DefaultConstructor = MetadataValue.generated (Func<obj>(fun () -> box(create.Invoke())))
                            CollectionAdder = MetadataValue.generated (Action<obj,obj>(fun reference item -> add.Invoke(unbox<'Reference> reference,unbox<'Element> item))) })
