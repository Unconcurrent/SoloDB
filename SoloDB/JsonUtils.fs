﻿module JsonUtils

open System.Text.Json
open System

type DateTimeOffsetJsonConverter() =
    inherit Serialization.JsonConverter<DateTimeOffset>()

    override this.Read(reader, typeToConvert: Type, options: JsonSerializerOptions) : DateTimeOffset =
        DateTimeOffset.FromUnixTimeMilliseconds (reader.GetInt64())

    override this.Write(writer: Utf8JsonWriter, dateTimeValue: DateTimeOffset, options) : unit =
        writer.WriteNumberValue (dateTimeValue.ToUnixTimeMilliseconds())

type BooleanJsonConverter() =
    inherit Serialization.JsonConverter<bool>()

    override this.Read(reader, typeToConvert: Type, options: JsonSerializerOptions) : bool =
        reader.GetInt64() = 1

    override this.Write(writer: Utf8JsonWriter, booleanValue: bool, options) : unit =
        if booleanValue then writer.WriteNumberValue 1UL else writer.WriteNumberValue 0UL


let jsonOptions = 
    let o = JsonSerializerOptions()
    o.Converters.Add (DateTimeOffsetJsonConverter())
    o.Converters.Add (BooleanJsonConverter())
    o

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

let fromJson<'T> (text: string) =
    System.Text.Json.JsonSerializer.Deserialize<'T> (text, jsonOptions)

let fromIdJson<'T> (idValueJSON: string) =
    let element = System.Text.Json.JsonSerializer.Deserialize<JsonElement>(idValueJSON, jsonOptions)
    let id = element.GetProperty("Id").GetInt64()
    let value = element.GetProperty("Value").Deserialize<'T>(jsonOptions)
    id, value