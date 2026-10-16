namespace SoloDatabase

open System
open System.Reflection
open System.Reflection.Emit
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation

module internal NativeArgumentSources =
    let empty = NativeArgumentLayout(Array.empty)
    let register<'T> names (read: Action<obj, obj[]>) =
        if isNull names then nullArg "names"
        if isNull read then nullArg "read"
        let layout = NativeArgumentLayout(Array.copy names)
        TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with SqlArguments = MetadataValue.generated (struct(layout, read)) })
    let forType (t: Type) =
        match MetadataValue.tryValue (TypeMetadataCache.Get(t).SqlArguments) with
        | ValueSome source -> source
        | _ when not (MetadataRuntime.supportsDynamicCode()) -> MetadataRuntime.missing "SQL argument capture" t
        | _ ->
        TypeMetadataCache.Discover(t, (fun metadata -> metadata.SqlArguments), (fun value metadata -> { metadata with SqlArguments = ValueSome value }), Func<struct (NativeArgumentLayout * Action<obj,obj[]>)>(fun () ->
            let properties = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                             |> Array.filter (fun p -> TypeSupport.SqlArgumentProperty(true,p.CanRead,false,p.GetIndexParameters().Length <> 0))
            let readers = properties |> Array.map(fun property -> QueryValueMetadata.Reader(property,t))
            let method = DynamicMethod("CaptureSqlArguments",typeof<Void>,[|typeof<Func<obj,obj>[]>;typeof<obj>;typeof<obj[]>|],t.Module,true)
            let il = RuntimeCodecIL(method.GetILGenerator())
            let invoke = typeof<Func<obj,obj>>.GetMethod("Invoke")
            SqlArgumentEmitter.capture il 2 readers.Length (Action<int>(fun index ->
                il.Emit OpCodes.Ldarg_0
                il.EmitInt(OpCodes.Ldc_I4,index)
                il.Emit OpCodes.Ldelem_Ref
                il.Emit OpCodes.Ldarg_1
                il.EmitMethod(OpCodes.Callvirt,invoke)))
            let capture = method.CreateDelegate(typeof<Action<obj,obj[]>>,readers) :?> Action<obj,obj[]>
            struct (NativeArgumentLayout(properties |> Array.map _.Name),capture)))


/// Publishes direct typed argument reads for the existing native acquisition boundary.
[<AbstractClass; Sealed>]
type internal NativeArgumentMetadata private () =
    static member Register<'T>(names: string[], read: Action<obj, obj[]>) =
        NativeArgumentSources.register<'T> names read
