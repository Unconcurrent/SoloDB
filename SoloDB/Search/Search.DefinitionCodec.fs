namespace SoloDatabase

open System
open System.Collections.Generic
open System.Linq.Expressions
open SoloDatabase.SqlModel
open SoloDatabase.JsonSerializator

module internal SearchDefinition =
    /// SQLite identifier case folding affects ASCII letters only.
    let identifier (value: string) =
        let text = value.ToCharArray()
        for i = 0 to text.Length - 1 do
            if text.[i] >= 'A' && text.[i] <= 'Z' then text.[i] <- char(int text.[i] + 32)
        System.String(text)
    let quote (value: string) = "\"" + value.Replace("\"","\"\"") + "\""
    let literal (value: string) = "'" + value.Replace("'","''") + "'"
    let private objectValue fields = JsonValue.Object(Dictionary<string,JsonValue>(dict fields))
    let encode (definition: SearchResolvedDefinition) =
        objectValue [
            "format",JsonValue.Number 1M
            "storage",JsonValue.Number(decimal(int definition.Storage))
            "tokenizer",JsonValue.String definition.Tokenizer
            "prefix",JsonValue.List(definition.Prefix |> Array.map (decimal >> JsonValue.Number))
            "fields",JsonValue.List(definition.Fields |> Array.map (fun field -> objectValue [
                "path",JsonValue.String field.Path; "alias",JsonValue.String field.Alias
                "weight",JsonValue.String(field.Weight.ToString("R",Globalization.CultureInfo.InvariantCulture))])) ]
        |> _.ToJsonString()
    let decode (text: string) =
        let objectAt = function JsonValue.Object value -> value | _ -> invalidOp "Invalid search definition object."
        let stringAt = function JsonValue.String value -> value | _ -> invalidOp "Invalid search definition string."
        let intAt = function JsonValue.Number value -> int value | _ -> invalidOp "Invalid search definition integer."
        let arrayAt = function JsonValue.List value -> value |> Seq.toArray | _ -> invalidOp "Invalid search definition array."
        let value = JsonValue.Parse(text) |> objectAt
        if intAt value.["format"] <> 1 then raise (NotSupportedException("This search definition format is not supported."))
        { Storage = enum<SearchTextStorage>(intAt value.["storage"])
          Tokenizer = stringAt value.["tokenizer"]
          Prefix = arrayAt value.["prefix"] |> Array.map intAt
          Fields = arrayAt value.["fields"] |> Array.map (fun field ->
              let field = objectAt field
              { Path = stringAt field.["path"]; Alias = stringAt field.["alias"]
                Weight = Double.Parse(stringAt field.["weight"],Globalization.CultureInfo.InvariantCulture) }) }
    let projection owner path =
        let input = owner + "Value"
        $"CASE WHEN json_type({input},{literal path})='text' THEN jsonb_extract({input},{literal path}) ELSE '' END"
