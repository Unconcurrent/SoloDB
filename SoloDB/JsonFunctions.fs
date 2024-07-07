namespace SoloDatabase
module JsonFunctions =
    open System
    open System.Collections.Concurrent
    open Utils
    open SoloDatabase.Types
    open JsonSerializator
    open FSharp.Interop.Dynamic

    let private hasIdTypeCache = ConcurrentDictionary<Type, bool>()
    let hasIdType (t: Type) = hasIdTypeCache.GetOrAdd(t, fun t -> t.GetProperties() |> Array.exists(fun p -> p.Name = "Id" && p.PropertyType = typeof<SqlId>))

    let toJson<'T> o = 
        let element = JsonValue.Serialize o
        element.ToJsonString()

    let toTypedJson<'T> o = 
        let element = JsonValue.SerializeWithType o
        element.ToJsonString()

    let toSQLJson<'T> (item: obj) = 
        match item with
        | :? string as s -> s :> obj, false
        | :? char as c -> c.ToString() :> obj, false

        | :? Type as t -> t.FullName :> obj, false

        | :? int8 as x -> x :> obj, false
        | :? int16 as x -> x :> obj, false
        | :? int32 as x -> x :> obj, false
        | :? int64 as x -> x :> obj, false

        | :? float32 as x -> x :> obj, false
        | :? float as x -> x :> obj, false

        | other ->

        let element = JsonValue.Serialize item
        match element with
        | Boolean b -> b :> obj, false
        | Null -> null, false
        | Number _
        | String _
            -> element.ToObject(), false
        | other -> other.ToJsonString(), true    

    let private fromJson<'T> (text: string) =
        let json = JsonValue.Parse text
        json.ToObject<'T>()    

    let rec fromJsonOrSQL<'T when 'T :> obj> (data: string) : 'T =
        if data = null then null :> obj :?> 'T
        else

        if typeof<'T> <> typeof<obj> then
            if typeof<'T> = typeof<string> then
                data :> obj :?> 'T
            else if isIntegerBased data then
                data |> int64 :> obj :?> 'T
            else if isNumber data then
                data |> float :> obj :?> 'T
            else
                fromJson<'T> data
        else

        match Int64.TryParse data with
        | true, i -> i :> obj :?> 'T
        | false, _ ->

        fromJson<'T> data


    let fromIdJson<'T> (idValueJSON: string) =
        let element = JsonValue.Parse idValueJSON
        let id = element.["Id"].ToObject<int64>()
        let value = element.GetProperty("Value").ToJsonString() |> fromJsonOrSQL<'T>
        id, value

    let fromDapper<'R when 'R :> obj> (input: obj) : 'R =
        match input with
        | :? DbObjectRow as row ->
            let mutable obj = fromJsonOrSQL<'R> (row.ValueJSON)
            if hasIdType typeof<'R> then
                obj?Id <- SqlId(row.Id)
            obj
        | :? string as input ->
            fromJsonOrSQL<'R> (input :> obj :?> string)
        | other -> failwithf "Input is not DbObjectRow or json string."