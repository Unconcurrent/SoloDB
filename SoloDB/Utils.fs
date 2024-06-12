module Utils

open System

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