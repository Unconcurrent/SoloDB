module Utils

open System
open System.Collections.Concurrent
open System.Reflection

let isNumber (value: obj) =
    match value with
    | :? sbyte
    | :? byte
    | :? int16
    | :? uint16
    | :? int
    | :? uint32
    | :? int64
    | :? uint64
    | :? float32
    | :? float
    | :? decimal -> true
    | _ -> false

let isIntegerBased (value: obj) =
    match value with
    | :? sbyte
    | :? byte
    | :? int16
    | :? uint16
    | :? int32
    | :? uint32
    | :? int64
    | :? uint64 -> true
    | _ -> false

let isIntegerBasedType (t: Type) =
    match t with
    | _ when t = typeof<sbyte>  -> true
    | _ when t = typeof<byte>   -> true
    | _ when t = typeof<int16>  -> true
    | _ when t = typeof<uint16> -> true
    | _ when t = typeof<int32>  -> true
    | _ when t = typeof<uint32> -> true
    | _ when t = typeof<int64>  -> true
    | _ when t = typeof<uint64> -> true
    | _ -> false

let typeToName (t: Type) = 
    let fullname = t.FullName
    if fullname.Length > 0 && Char.IsAsciiLetter fullname.[0] // To not insert auto generated classes.
    then Some fullname
    else None

let private nameToTypeCache = ConcurrentDictionary<string, Type>()

let nameToType (typeName: string) = 
    nameToTypeCache.GetOrAdd(typeName, fun typeName -> 
                                        let fastType = Type.GetType(typeName)
                                        if fastType <> null then fastType
                                        else AppDomain.CurrentDomain.GetAssemblies() 
                                                |> Seq.collect(fun a -> a.GetTypes()) 
                                                |> Seq.find(fun t -> t.FullName = typeName)
                                        )
    
type TypeCaster =
    static member CastObject (obj: obj) (t: Type) : obj =
        let objType = obj.GetType()
        if objType = t then
            obj
        else if objType = typeof<string> && t = typeof<Type> then
            obj :?> string |> nameToType :> obj
        else
            let castHelper = typeof<TypeCaster>.GetMethod("CastHelper", BindingFlags.Static ||| BindingFlags.NonPublic)
            let genericCastHelper = castHelper.MakeGenericMethod(t)
            genericCastHelper.Invoke(null, [| obj |])
    
    static member private CastHelper<'T> (obj: obj) : 'T =
        obj :?> 'T