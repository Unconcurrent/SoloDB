namespace SoloDatabase

open SoloDatabase.Attributes
open System.Reflection
open System
open SoloDatabase
open Utils
open SoloDatabase.Types
open System.Runtime.CompilerServices


[<AbstractClass; Sealed>]
type internal EntityDocumentId<'T> =
    static member internal Value =
        match EntityIdMetadata.TryGet(typeof<'T>) with
        | ValueNone when MetadataRuntime.supportsDynamicCode() -> EntityIdentity.hasWritableInt64Id typeof<'T>
        | ValueNone -> MetadataRuntime.missing "physical identity" typeof<'T>
        | ValueSome operations -> operations.HasWritableId
    static member internal Read(value: 'T) =
        match EntityIdMetadata.TryGet(typeof<'T>) with
        | ValueNone when MetadataRuntime.supportsDynamicCode() ->
            if not (EntityIdentity.hasWritableInt64Id typeof<'T>) then failwithf "Cannot read nonexistant Id from Type %A" typeof<'T>.FullName
            let read = TypeMetadataCache.Discover(typeof<'T>, (fun metadata ->
                    match metadata.PhysicalIdentity with
                    | ValueSome operation ->
                        let reader = if operation.Value.HasWritableId then operation.Value.TypedReader else null
                        ValueSome { Generated = operation.Generated; Value = reader }
                    | ValueNone -> metadata.DocumentIdReader),
                (fun operation metadata -> { metadata with DocumentIdReader = ValueSome operation }), Func<Delegate>(fun () ->
                    let property = typeof<'T>.GetProperty("Id",BindingFlags.Public ||| BindingFlags.Instance)
                    QueryMethodDiscovery.typedReader<'T,int64> property :> Delegate))
            if isNull read then failwithf "Cannot read nonexistant Id from Type %A" typeof<'T>.FullName
            (read :?> Func<'T,int64>).Invoke value
        | ValueNone -> MetadataRuntime.missing "physical identity" typeof<'T>
        | ValueSome operations when operations.HasWritableId -> (operations.TypedReader :?> Func<'T,int64>).Invoke(value)
        | _ -> failwithf "Cannot read nonexistant Id from Type %A" typeof<'T>.FullName
    static member internal WithId(value: 'T) (id: int64) =
        match EntityIdMetadata.TryGet(typeof<'T>) with
        | ValueNone when MetadataRuntime.supportsDynamicCode() ->
            if not (EntityIdentity.hasWritableInt64Id typeof<'T>) then failwithf "Cannot write nonexistant Id from Type %A" typeof<'T>.FullName
            let boxed = box value
            (EntityIdentity.compiledInt64IdWriter typeof<'T>).Invoke(boxed,id)
            unbox<'T> boxed
        | ValueNone -> MetadataRuntime.missing "physical identity" typeof<'T>
        | ValueSome operations when operations.HasWritableId ->
            let mutable result = value
            (operations.TypedWriter :?> EntityIdSetter<'T>).Invoke(&result, id)
            result
        | _ -> failwithf "Cannot write nonexistant Id from Type %A" typeof<'T>.FullName
    static member internal Write(value: 'T) (id: int64) =
        EntityDocumentId<'T>.WithId value id |> ignore
