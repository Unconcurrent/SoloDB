module JsonUtils

open System.Text.Json
open System
open Utils
open SoloDBTypes
open System.Text.Json.Serialization.Metadata
open System.Text.Json.Nodes
open System.Globalization

(*
let private entryTypes = 
    AppDomain.CurrentDomain.GetAssemblies() 
    |> Seq.collect (fun s -> s.GetTypes()) 
    |> Seq.filter (fun t -> typeof<SoloDBEntry>.IsAssignableFrom t && t.IsClass && not t.IsAbstract) 
    |> Seq.map (fun t -> (typeToName t, t)) 
    |> dict*)

type private DateTimeOffsetJsonConverter() =
    inherit Serialization.JsonConverter<DateTimeOffset>()

    override this.Read(reader, typeToConvert: Type, options: JsonSerializerOptions) : DateTimeOffset =
        DateTimeOffset.FromUnixTimeMilliseconds (reader.GetInt64())

    override this.Write(writer: Utf8JsonWriter, dateTimeValue: DateTimeOffset, options) : unit =
        writer.WriteNumberValue (dateTimeValue.ToUnixTimeMilliseconds())

type private BooleanJsonConverter() =
    inherit Serialization.JsonConverter<bool>()

    override this.Read(reader, typeToConvert: Type, options: JsonSerializerOptions) : bool =
        reader.GetInt64() = 1

    override this.Write(writer: Utf8JsonWriter, booleanValue: bool, options) : unit =
        if booleanValue then writer.WriteNumberValue 1UL else writer.WriteNumberValue 0UL

type private TypeJsonConverter() =
    inherit Serialization.JsonConverter<Type>()

    override this.Read(reader, typeToConvert: Type, options: JsonSerializerOptions) : Type =
        reader.GetString() |> nameToType

    override this.Write(writer: Utf8JsonWriter, value: Type, options) : unit =
        writer.WriteStringValue (value.FullName)


let private jsonOptions = 
    let o = JsonSerializerOptions()
    o.Converters.Add (DateTimeOffsetJsonConverter())
    o.Converters.Add (BooleanJsonConverter())
    o.Converters.Add (TypeJsonConverter())
    o

let private arrayToTuple<'T> l =
    let tupleBaseTypes = Microsoft.FSharp.Reflection.FSharpType.GetTupleElements typeof<'T>
    let castedObj = tupleBaseTypes |> Array.zip l |> Array.map(fun (e, t) -> TypeCaster.CastObject e t)
    Microsoft.FSharp.Reflection.FSharpValue.MakeTuple (castedObj, typeof<'T>) :?> 'T


let toJson<'T> o = 
    let element = System.Text.Json.JsonSerializer.SerializeToNode(o, jsonOptions)
    let t = o.GetType()
    if element.GetValueKind() = JsonValueKind.Object then
        match t |> typeToName with
        | Some typeStr -> element.["$type"] <- typeStr
        | None -> ()
    element.ToJsonString().Replace("\\u002B", "+") // "Security"

let toSQLJsonAndKind<'T> item =    
    let element = System.Text.Json.JsonSerializer.SerializeToNode(item, jsonOptions)
    let t = item.GetType()
    if isNumber item then (item, element.GetValueKind())
    else if item.GetType() = typeof<DateTimeOffset> then
        (item :> obj :?> DateTimeOffset |> _.ToUnixTimeMilliseconds() :> obj, element.GetValueKind())
    else

    if element.GetValueKind() = JsonValueKind.Object then
        match t |> typeToName with
        | Some typeStr -> element.["$type"] <- JsonObject.op_Implicit typeStr
        | None -> ()
    let text = element.ToJsonString().Replace("\\u002B", "+")
    match element.GetValueKind() with
    | JsonValueKind.Object
    | JsonValueKind.Array ->
        text :> obj, element.GetValueKind()
    | other -> 
        let text = text.Trim '"'        
        text, other

let toSQLJson<'T> item =
    let json, kind = toSQLJsonAndKind item
    json

let rec private tupleToList (element: JsonElement) =
    let rec tupleToArrayInner (element: JsonElement) (i: uint) =
        match element.TryGetProperty $"Item{i}" with
        | true, item ->
            fromJsonOrSQL (item.ToString()) :: tupleToArrayInner element (i + 1u)
        | false, _ -> []

    tupleToArrayInner element 1u

and private fromJson<'T> (text: string) =
    let hasProperty (element: JsonElement) (prop: string) =
        match element.TryGetProperty(prop) with
        | true, _ -> true
        | false, _ -> false

    let element = System.Text.Json.JsonSerializer.Deserialize<JsonElement> (text, jsonOptions)

    if element.ValueKind = JsonValueKind.Object && hasProperty element "$type" then
        match element.TryGetProperty("$type") with
        | true, typeProp ->
            let elementTypeStr = typeProp.GetString()
            let elementType = elementTypeStr |> nameToType
            try element.Deserialize (elementType, jsonOptions) :?> 'T
            with ex -> element.Deserialize<'T> (jsonOptions)
        | false, _ -> failwithf "Impossible."

    else if text.StartsWith "[" then
        element.EnumerateArray() |> Seq.map(fun e -> e.ToString()) |> Seq.map fromJsonOrSQL<obj> |> Seq.toList :> obj :?> 'T
    else if text.StartsWith "{" then
        match element.TryGetProperty "Item1" with
        | true, _ -> // It is a tuple, will convert to array
            let arguments = tupleToList element |> List.toArray
            if arguments |> Array.exists(fun e -> e.GetType().IsAssignableTo typeof<SoloDBEntry>) then
                arguments |> arrayToTuple<'T>
            else element.Deserialize<'T> (jsonOptions)
        | false, _ -> element.Deserialize<'T> (jsonOptions)
    else
        element.Deserialize<'T> (jsonOptions)

    

and fromJsonOrSQL<'T when 'T :> obj> (data: string) : 'T =
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

    if data.StartsWith "\"" then
        System.Text.Json.JsonSerializer.Deserialize<string> (data, jsonOptions) :> obj :?> 'T
    else if data.StartsWith "{"  then
        let o = System.Text.Json.JsonSerializer.Deserialize<JsonElement> (data, jsonOptions)
        match o.TryGetProperty "Item1" with
        | true, _ -> // It is a tuple, will convert to array
            tupleToList o :> obj :?> 'T
        | false, _ -> fromJson<'T> data

    else if data.StartsWith "[" then
        let o = System.Text.Json.JsonSerializer.Deserialize<JsonElement> (data, jsonOptions)
        o.EnumerateArray() |> Seq.map(fun e -> e.ToString()) |> Seq.map fromJsonOrSQL<obj> |> Seq.toList :> obj :?> 'T
    else
        data :> obj :?> 'T


let fromIdJson<'T> (idValueJSON: string) =
    let element = System.Text.Json.JsonSerializer.Deserialize<JsonElement>(idValueJSON, jsonOptions)
    let id = element.GetProperty("Id").GetInt64()
    let value = fromJson<'T>(element.GetProperty("Value").ToString())
    id, value

let fromDapper<'R when 'R :> obj> (input: obj) : 'R =
    match input with
    | :? DbObjectRow as row ->
        let mutable obj = fromJsonOrSQL<'R> (row.ValueJSON)
        match obj :> obj with
        | :? SoloDBEntry as entry ->
            SoloDBEntry.InitId entry row.Id
        | other -> ()

        obj
    | :? string as input ->
        fromJsonOrSQL<'R> (input :> obj :?> string)
    | other -> failwithf "Input is not DbObjectRow or json string."