namespace SoloDatabase

open SoloDatabase.Attributes
open System.Reflection
open System.Linq.Expressions

#nowarn "3536" // IIdGenerator

module JsonFunctions =
    open System
    open System.Collections.Concurrent
    open Utils
    open SoloDatabase.Types
    open JsonSerializator
    open FSharp.Interop.Dynamic

    // Instead of concurrent dictionaries, we can use a static class
    // if the parameters can be represented as generic types.

    [<AbstractClass; Sealed>]
    type internal HasTypeId<'t> =
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

    [<AbstractClass; Sealed>]
    type internal CustomTypeId<'t> =
        static member val internal Value = 
            typeof<'t>.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Seq.choose(
                fun p -> 
                    match p.GetCustomAttribute<SoloId>(true) with
                    | a when isNull a -> None
                    | a -> 
                    if not p.CanWrite then failwithf "Cannot create a generator for a non writtable parameter '%s' for type %s" p.Name typeof<'t>.FullName
                    match a.IdGenerator with
                    | null -> None
                    | generator -> Some (p, generator)
                    ) 
            |> Seq.tryHead
            |> Option.bind(
                fun (p, gt) ->
                    let instance = (Activator.CreateInstance gt) :?> IIdGenerator
                    Some {|
                        Generator = instance
                        SetId = fun id o -> p.SetValue(o, id)
                        GetId = fun o -> p.GetValue(o)
                        IdType = p.PropertyType
                    |}
            )

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

        | _other ->

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
        if data = null then 
            Unchecked.defaultof<'T>
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

        fromJson<'T> data

    let fromIdJson<'T> (idValueJSON: string) =
        let element = JsonValue.Parse idValueJSON
        let id = element.["Id"].ToObject<int64>()
        let value = element.GetProperty("Value").ToJsonString() |> fromJsonOrSQL<'T>

        id, value

    let fromSQLite<'R when 'R :> obj> (input: obj) : 'R =
        match input with
        | :? DbObjectRow as row ->
            // If the Id is NULL then the ValueJSON is a error message encoded in a JSON string.
            if not row.Id.HasValue then
                let exc = fromJson<string> row.ValueJSON
                raise (exn exc)

            let mutable obj = fromJsonOrSQL<'R> (row.ValueJSON)
            
            // An Id of -1 mean that it is an inserted object inside the IQueryable.
            if row.Id.Value <> -1 && HasTypeId<'R>.Value then
                obj?Id <- row.Id.Value
            obj
        | :? string as input ->
            fromJsonOrSQL<'R> (input :> obj :?> string)
        | null -> Unchecked.defaultof<'R>
        | other -> failwithf "Input is not DbObjectRow or json string."