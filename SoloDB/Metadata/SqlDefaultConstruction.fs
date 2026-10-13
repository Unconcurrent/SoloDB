namespace SoloDatabase

open System
open System.Reflection
open Utils
open SQLiteToolsRowConversions

module internal SQLiteToolsMapperQuery =
    type private TupleDefault<'T>() =
        static member private Build() =
            if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "SQL tuple default" typeof<'T>
            let rec construct (tupleType: Type) =
                let elements = TypeMetadata.GenericTypeArgCache.Get tupleType
                let defaults = elements |> Array.mapi (fun index element ->
                    if index = 7 && elements.Length = 8 && isTuple element then construct element
                    elif element.IsValueType then
                        let constructor = Option.ofObj(element.GetConstructor(Type.EmptyTypes))
                        let create = QueryMethodDiscovery.defaultConstruction element constructor
                        Func<obj>(fun () ->
                            try create.Invoke() with error -> raise(TargetInvocationException(error)))
                    else Func<obj>(fun () -> null))
                let create = QueryValueMetadata.Constructor(tupleType,Array.toList elements)
                Func<obj>(fun () ->
                    let values = defaults |> Array.map(fun create -> create.Invoke())
                    try create.Invoke values with error -> raise(TargetInvocationException(error)))
            let create = construct typeof<'T>
            Func<'T>(fun () -> unbox<'T>(create.Invoke()))

        static member Create() =
            let factory = TypeMetadataCache.Discover(typeof<'T>, (fun metadata -> metadata.TupleDefault),
                (fun operation metadata -> { metadata with TupleDefault = ValueSome operation }), Func<Delegate>(fun () -> TupleDefault<'T>.Build() :> Delegate))
            (factory :?> Func<'T>).Invoke()

    let internal defaultOf<'T> () : 'T =
      match SqlRowDefault<'T>.CreateCurrent() with
      | ValueNone ->
        let t = typeof<'T>
        if isTuple t && not t.IsValueType then TupleDefault<'T>.Create()
        else Unchecked.defaultof<'T>
      | ValueSome value -> value
