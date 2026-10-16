namespace SoloDatabase

open System
open System.Reflection
open System.Reflection.Emit
open SoloDatabase.Metadata.CodecGeneration
open Utils

module internal SQLiteToolsMapperQuery =
    type private TupleDefault<'T>() =
        static member private Build() =
            if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "SQL tuple default" typeof<'T>
            let create = SqlTupleDefaultEmitter.prepare (RuntimeTupleFacts.model typeof<'T>)
                            (Func<Type,bool>(fun typ -> typ.IsValueType))
                            (Func<Type,MethodBase option>(fun typ -> Option.ofObj(typ.GetConstructor(Type.EmptyTypes)) |> Option.map(fun constructor -> constructor :> MethodBase)))
                            typeof<Exception> (typeof<TargetInvocationException>.GetConstructor([|typeof<Exception>|]) :> MethodBase)
                            (Func<Type,CodecIL<Type,MethodBase,FieldInfo> * MethodBase>(fun typ ->
                                let method = DynamicMethod("SqlTupleDefault",typ,Type.EmptyTypes,typ.Module,true)
                                (RuntimeCodecIL(method.GetILGenerator()) :> CodecIL<Type,MethodBase,FieldInfo>), (method :> MethodBase)))
            (create :?> DynamicMethod).CreateDelegate(typeof<Func<'T>>) :?> Func<'T>

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
