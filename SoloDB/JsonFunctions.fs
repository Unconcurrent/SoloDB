namespace SoloDatabase

open SoloDatabase.Attributes
open System.Reflection
open System.Linq.Expressions
open System
open SoloDatabase
open Utils
open SoloDatabase.Types
open SoloDatabase.JsonSerializator
open System.Runtime.CompilerServices


[<AbstractClass; Sealed>]
type internal HasTypeId<'t> =
    // Instead of concurrent dictionaries, we can use a static class
    // if the parameters can be represented as generic types.
    static member val private Properties: PropertyInfo array = typeof<'t>.GetProperties()
    static member private IdPropertyFilter (p: PropertyInfo) = p.Name = "Id" && p.PropertyType = typeof<int64> && p.CanWrite && p.CanRead
    static member val internal Value = 
        HasTypeId<'t>.Properties
        |> Array.exists HasTypeId<'t>.IdPropertyFilter

    static member val internal Read =
        match HasTypeId<'t>.Properties
                |> Array.tryFind HasTypeId<'t>.IdPropertyFilter
            with
            | Some p -> 
                let x = ParameterExpression.Parameter(typeof<'t>, "x")
                let l = LambdaExpression.Lambda<Func<'t, int64>>(
                    MethodCallExpression.Call(x, p.GetMethod),
                    [x]
                )
                let fn = l.Compile(false)
                fn.Invoke
            | None -> fun (_x: 't) -> failwithf "Cannot read nonexistant Id from Type %A" typeof<'t>.FullName

    static member val internal Write =
        match HasTypeId<'t>.Properties
                |> Array.tryFind HasTypeId<'t>.IdPropertyFilter
            with
            | Some p -> 
                let x = ParameterExpression.Parameter(typeof<'t>, "x")
                let y = ParameterExpression.Parameter(typeof<int64>, "y")
                let l = LambdaExpression.Lambda<Action<'t, int64>>(
                    MethodCallExpression.Call(x, p.SetMethod, y),
                    [|x; y|]
                )
                let fn = l.Compile(false)
                fun x y -> fn.Invoke(x, y)
            | None -> fun (_x: 't) (_y: int64) -> failwithf "Cannot write nonexistant Id from Type %A" typeof<'t>.FullName


module JsonFunctions =
    let inline internal mustIncludeTypeInformationInSerializationFn (t: Type) = 
        t.IsAbstract || not (isNull (t.GetCustomAttribute<Attributes.PolimorphicAttribute>()))

    let internal mustIncludeTypeInformationInSerialization<'T> =
        mustIncludeTypeInformationInSerializationFn typeof<'T>

    /// Used internally, do not touch!
    let toJson<'T> o = 
        let element = JsonValue.Serialize<'T> o
        // Remove the int64 Id property from the json format, as it is stored as a separate column.
        if HasTypeId<'T>.Value then
            match element with
            | Object o -> ignore (o.Remove "Id")
            | _ -> ()
        element.ToJsonString()

    let internal toTypedJson<'T> o = 
        let element = JsonValue.SerializeWithType<'T> o
        // Remove the int64 Id property from the json format, as it is stored as a separate column.
        if HasTypeId<'T>.Value then
            match element with
            | Object o -> ignore (o.Remove "Id")
            | _ -> ()
        element.ToJsonString()

    /// Used internally, do not touch!
    let toSQLJson (item: obj) = 
        match box item with
        | :? string as s -> s :> obj, false
        | :? char as c -> string c :> obj, false

        | :? Type as t -> t.FullName :> obj, false

        | :? int8 as x -> x :> obj, false
        | :? int16 as x -> x :> obj, false
        | :? int32 as x -> x :> obj, false
        | :? int64 as x -> x :> obj, false

        | :? float32 as x -> x :> obj, false
        | :? float as x -> x :> obj, false

        | _other ->

        let element = JsonValue.Serialize item
        match element with
        | Boolean b -> b :> obj, false
        | Null -> null, false
        | Number _
        | String _
            -> element.ToObject(), false
        | other -> 
            // Cannot remove the Id value from here, maybe it is needed. 
            other.ToJsonString(), true

    
    let internal fromJson<'T> (json: JsonValue) =
        match json with
        | Null when typeof<float> = typeof<'T> -> 
            nan :> obj :?> 'T
        | Null when typeof<float32> = typeof<'T> -> 
            nanf :> obj :?> 'T
        | Null when typeof<'T>.IsValueType -> 
            raise (InvalidOperationException "Invalid operation on a value type.") 
        | json -> json.ToObject<'T>()    

    /// Used internally, do not touch!
    let fromIdJson<'T> (element: JsonValue) =
        let id = element.["Id"].ToObject<int64>()
        let value = element.GetProperty("Value") |> fromJson<'T>

        id, value

    let 
        #if RELEASE
        inline
        #endif

        internal fromSQLite<'R when 'R :> obj> (row: DbObjectRow) : 'R =
        let inline asR (a: 'a when 'a : unmanaged) = 
            // No allocations.
            Unsafe.As<'a, 'R>(&Unsafe.AsRef(&a))

        if row :> obj = null then
            Unchecked.defaultof<'R>
        else

        // If the Id is NULL then the ValueJSON is a error message encoded in a JSON string.
        if not row.Id.HasValue then
            let exc = toJson<string> row.ValueJSON
            raise (exn exc)
            
        // Checking if the SQLite returned a raw string.
        if typeof<'R> = typeof<string> then
            row.ValueJSON :> obj :?> 'R
        else

        match typeof<'R> with
        | OfType float when row.ValueJSON <> null -> (asR << float) row.ValueJSON
        | OfType float32 when row.ValueJSON <> null -> (asR << float32) row.ValueJSON
        | OfType decimal -> (asR << decimal) row.ValueJSON

        | OfType int8 -> (asR << int8) row.ValueJSON
        | OfType uint8 -> (asR << uint8) row.ValueJSON
        | OfType int16 -> (asR << int16) row.ValueJSON
        | OfType uint16 -> (asR << uint16) row.ValueJSON
        | OfType int32 -> (asR << int32) row.ValueJSON
        | OfType uint32 -> (asR << uint32) row.ValueJSON
        | OfType int64 -> (asR << int64) row.ValueJSON
        | OfType uint64 -> (asR << uint64) row.ValueJSON
        | OfType nativeint -> (asR << nativeint) row.ValueJSON
        | OfType unativeint -> (asR << unativeint) row.ValueJSON

        | _ ->


        match JsonValue.Parse row.ValueJSON with
        | Null when typeof<JsonValue> = typeof<'R> -> 
            Unchecked.defaultof<'R>
        | Null when typeof<'R>.IsValueType && typeof<float> <> typeof<'R> && typeof<float32> <> typeof<'R> -> 
            Unchecked.defaultof<'R>
        | json when typeof<JsonValue> = typeof<'R> ->
            let id = row.Id.Value
            if json.JsonType = JsonValueType.Object && not (json.Contains "Id") && id >= 0 then
                json.["Id"] <- id

            json :> obj :?> 'R
        | json ->

        let mutable obj = fromJson<'R> json
            
        // An Id of -1 mean that it is an inserted object inside the IQueryable.
        if row.Id.Value <> -1 && HasTypeId<'R>.Value then
            HasTypeId<'R>.Write obj row.Id.Value
        obj