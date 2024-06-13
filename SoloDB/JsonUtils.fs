module JsonUtils

open System.Text.Json
open System
open Utils
open SoloDbTypes
open System.Text.Json.Serialization.Metadata

let private entryTypes = 
    AppDomain.CurrentDomain.GetAssemblies() 
    |> Seq.collect (fun s -> s.GetTypes()) 
    |> Seq.filter (fun t -> typeof<SoloDBEntry>.IsAssignableFrom t && t.IsClass && not t.IsAbstract) 
    |> Seq.map (fun t -> (typeToName t, t)) 
    |> dict

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
        entryTypes[reader.GetString()]

    override this.Write(writer: Utf8JsonWriter, value: Type, options) : unit =
        writer.WriteStringValue (value |> typeToName)


let private jsonOptions = 
    let o = JsonSerializerOptions()
    o.Converters.Add (DateTimeOffsetJsonConverter())
    o.Converters.Add (BooleanJsonConverter())
    o.Converters.Add (TypeJsonConverter())
    o


let toJson<'T> o = 
    System.Text.Json.JsonSerializer.Serialize<'T>(o, jsonOptions)

let toSQLJsonAndKind<'T> item =
    let element = System.Text.Json.JsonSerializer.SerializeToElement<'T>(item, jsonOptions)
    let text = element.ToString()
    match element.ValueKind with
    | JsonValueKind.Object
    | JsonValueKind.Array ->
        (sprintf "%s" text), element.ValueKind
    | other -> (text.Trim '"'), element.ValueKind

let toSQLJson<'T> item =
    let json, kind = toSQLJsonAndKind item
    json

let private fromJson<'T> (text: string) =
    System.Text.Json.JsonSerializer.Deserialize<'T> (text, jsonOptions)

let fromIdJson<'T> (idValueJSON: string) =
    let element = System.Text.Json.JsonSerializer.Deserialize<JsonElement>(idValueJSON, jsonOptions)
    let id = element.GetProperty("Id").GetInt64()
    let value = element.GetProperty("Value").Deserialize<'T>(jsonOptions)
    id, value

let rec private tupleToArray (element: JsonElement) =
    let rec tupleToArrayInner (element: JsonElement) (i: uint) =
        match element.TryGetProperty $"Item{i}" with
        | true, item ->
            fromJsonOrSQL (item.ToString()) :: tupleToArrayInner element (i + 1u)
        | false, _ -> []

    tupleToArrayInner element 1u

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
        | true, _ -> // It is a tuple, will convert ot array
            tupleToArray o :> obj :?> 'T
        | false, _ -> System.Text.Json.JsonSerializer.Deserialize<'T> (data, jsonOptions)

    else if data.StartsWith "[" then
        let o = System.Text.Json.JsonSerializer.Deserialize<JsonElement> (data, jsonOptions)
        o.EnumerateArray() |> Seq.map(fun e -> e.ToString()) |> Seq.map fromJsonOrSQL<obj> |> Seq.toList :> obj :?> 'T
    else
        data :> obj :?> 'T


let fromDapper<'R when 'R :> obj> (input: obj) : 'R =
    match input with
    | :? DbObjectRow as row ->
        match entryTypes.TryGetValue(row.Type) with 
        | true, t -> System.Text.Json.JsonSerializer.Deserialize(row.ValueJSON, t, jsonOptions) :?> 'R
        | false, _ ->

        let mutable obj = fromJsonOrSQL<'R> (row.ValueJSON)
        match obj :> obj with
        | :? SoloDBEntry as entry ->
            SoloDBEntry.InitId entry row.Id row.Type
        | other -> ()

        obj
    | :? string as input ->
        fromJsonOrSQL<'R> (input :> obj :?> string)
    | other -> failwithf "Input is not DbObjectRow or json string."