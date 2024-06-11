module Utils

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