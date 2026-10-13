namespace SoloDatabase

open System
open System.Reflection

module internal NativeArgumentSources =
    let empty = NativeArgumentLayout(Array.empty)
    let register<'T> names (read: Action<'T, obj[]>) =
        if isNull names then nullArg "names"
        if isNull read then nullArg "read"
        let layout = NativeArgumentLayout(Array.copy names)
        let capture = Action<obj, obj[]>(fun source values -> read.Invoke(unbox<'T> source, values))
        TypeMetadataCache.Register(typeof<'T>, fun metadata -> { metadata with SqlArguments = MetadataValue.generated (struct(layout, capture)) })
    let forType (t: Type) =
        match MetadataValue.tryValue (TypeMetadataCache.Get(t).SqlArguments) with
        | ValueSome source -> source
        | _ when not (MetadataRuntime.supportsDynamicCode()) -> MetadataRuntime.missing "SQL argument capture" t
        | _ ->
        TypeMetadataCache.Discover(t, (fun metadata -> metadata.SqlArguments), (fun value metadata -> { metadata with SqlArguments = ValueSome value }), Func<struct (NativeArgumentLayout * Action<obj,obj[]>)>(fun () ->
            let properties = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                             |> Array.filter (fun p -> p.CanRead && p.GetIndexParameters().Length = 0)
            let readers = properties |> Array.map(fun property -> QueryValueMetadata.Reader(property,t))
            let capture = Action<obj,obj[]>(fun source values ->
                for index = 0 to readers.Length - 1 do
                    values[index] <- readers[index].Invoke source)
            struct (NativeArgumentLayout(properties |> Array.map _.Name),capture)))


/// Publishes direct typed argument reads for the existing native acquisition boundary.
[<AbstractClass; Sealed>]
type internal NativeArgumentMetadata private () =
    static member Register<'T>(names: string[], read: Action<'T, obj[]>) =
        NativeArgumentSources.register names read
