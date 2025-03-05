namespace SoloDatabase.JsonSerializator

open System.Collections.Concurrent
open System
open System.Collections.Generic
open System.Collections
open System.Reflection
open Dynamitey
open System.Dynamic
open SoloDatabase.Utils
open System.Text
open System.Globalization
open System.Runtime.CompilerServices
open Microsoft.FSharp.Reflection
open System.Web
open System.Linq.Expressions
open System.Linq
open SoloDatabase

module private JsonHelper =
    let internal isSupportedNewtownsoftArrayType (t: Type) =
        // For now support only primitive arrays.
        t.IsArray &&
        match t.GetElementType () with
        | elementType when elementType.IsPrimitive -> true  // Covers Boolean, Byte, SByte, Int16, UInt16, Int32, UInt32, Int64, UInt64, IntPtr, UIntPtr, Char, Double, and Single.
        | elementType when elementType = typeof<decimal> -> true
        | elementType when elementType = typeof<DateTime> -> true
        | elementType when elementType = typeof<TimeSpan> -> true
        | elementType when elementType = typeof<string> -> true
        | _ -> false

    let private implementsGenericCache = System.Collections.Concurrent.ConcurrentDictionary<struct (Type * Type), bool>()
    let internal implementsGeneric (genericTypeDefinition: Type) (targetType: Type) =        
        implementsGenericCache.GetOrAdd(struct (genericTypeDefinition, targetType), fun struct (genericTypeDefinition, targetType) ->
            targetType.IsGenericType && 
            targetType.GetGenericTypeDefinition() = genericTypeDefinition ||
            targetType.GetInterfaces() 
            |> Array.exists (fun interfaceType -> 
                interfaceType.IsGenericType && 
                interfaceType.GetGenericTypeDefinition() = genericTypeDefinition)
        )

    let internal byteArrayToJSONCompatibleString(ba: byte array) =
        Convert.ToBase64String ba

    let internal JSONStringToByteArray(s: string) =
        Convert.FromBase64String s

type internal Token =
    | NullToken
    | OpenBrace
    | CloseBrace
    | OpenBracket
    | CloseBracket
    | Comma
    | Colon
    | StringToken of string
    | NumberToken of decimal
    | BooleanToken of bool
    | EndOfInput

    static member internal Parse (input: string) : Token seq =
        let isDigit c = Char.IsDigit c || c = '-'
        let isInitialIdentifierChar c = Char.IsLetter c || c = '_'
        let isIdentifierChar c = isInitialIdentifierChar c || Char.IsDigit c

        seq {
            let mutable index = 0

            while index < input.Length do
                let currentChar = input.[index]
                match currentChar with
                | '{' -> 
                    OpenBrace
                    index <- index + 1
                | '}' -> 
                    CloseBrace
                    index <- index + 1
                | '[' -> 
                    index <- index + 1
                    OpenBracket
                | ']' -> 
                    index <- index + 1
                    CloseBracket
                | ',' -> 
                    index <- index + 1
                    Comma
                | ':' -> 
                    index <- index + 1
                    Colon
                | '"' | '\'' as quote ->
                    let sb = System.Text.StringBuilder()
                    let mutable i = index + 1
                    while i < input.Length && input.[i] <> quote do
                        if input.[i] = '\\' then
                            i <- i + 1
                            match input.[i] with
                            | '\\' -> sb.Append('\\') |> ignore
                            | '\"' -> sb.Append('"') |> ignore
                            | '\'' -> sb.Append('\'') |> ignore
                            | '/'  -> sb.Append('/') |> ignore
                            | 'b'  -> sb.Append('\b') |> ignore
                            | 'f'  -> sb.Append('\f') |> ignore
                            | 'n'  -> sb.Append('\n') |> ignore
                            | 'r'  -> sb.Append('\r') |> ignore
                            | 't'  -> sb.Append('\t') |> ignore
                            | 'u'  ->
                                // Parse the first Unicode escape sequence
                                if i + 4 < input.Length then
                                    let unicodeSeq1 = input.Substring(i + 1, 4)
                                    let unicodeVal1 = Convert.ToInt32(unicodeSeq1, 16)
                                    let mutable unicodeChar = char unicodeVal1
                                    i <- i + 4
                
                                    // Handle surrogate pairs if necessary
                                    if Char.IsHighSurrogate(unicodeChar) then
                                        if i + 6 < input.Length && input.[i + 1] = '\\' && input.[i + 2] = 'u' then
                                            let unicodeSeq2 = input.Substring(i + 3, 4)
                                            let unicodeVal2 = Convert.ToInt32(unicodeSeq2, 16)
                                            let lowSurrogate = char unicodeVal2
                                            if Char.IsLowSurrogate(lowSurrogate) then
                                                let fullCodePoint = Char.ConvertToUtf32(unicodeChar, lowSurrogate)
                                                let fullUnicodeChar = Char.ConvertFromUtf32(fullCodePoint)
                                                sb.Append(fullUnicodeChar) |> ignore
                                                i <- i + 6 // Move past the low surrogate and escape sequence
                                            else
                                                failwith "Invalid low surrogate"
                                        else
                                            failwith "Expected low surrogate after high surrogate"
                                    else
                                        sb.Append(unicodeChar) |> ignore
                                else
                                    failwith "Invalid Unicode escape sequence"
                            | other -> failwithf "Invalid escape sequence: '%c'" other
                        else
                            sb.Append(input.[i]) |> ignore
                        i <- i + 1
                    if i >= input.Length then failwith "Unterminated string"
                    index <- i + 1
                    StringToken(sb.ToString())
                | c when isDigit c ->
                    let sb = System.Text.StringBuilder()
                    let mutable i = index
                    while i < input.Length && (isDigit input.[i] || input.[i] = '.' || input.[i] = 'e' || input.[i] = 'E' || input.[i] = '+') do
                        sb.Append(input.[i]) |> ignore
                        i <- i + 1
                    index <- i
                    let s = sb.ToString()
                    NumberToken (
                        try Decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)
                        with
                        | :? OverflowException ->
                            if s.[0] = '-' then
                                Decimal.MinValue
                            else
                                Decimal.MaxValue
                    )
                | c when isInitialIdentifierChar c ->
                    let sb = System.Text.StringBuilder()
                    let mutable i = index
                    while i < input.Length && isIdentifierChar input.[i] do
                        sb.Append(input.[i]) |> ignore
                        i <- i + 1
                    let txt = sb.ToString()
                    index <- i
                    match txt with
                    | "null"
                    | "NULL" -> NullToken
                    | "true" -> BooleanToken(true)
                    | "false" -> BooleanToken(false)
                    | other -> StringToken other // Non standard, but used.
                | c when Char.IsWhiteSpace(c) ->
                    index <- index + 1
                | _ -> failwith "Invalid JSON format"

            EndOfInput
        }

type internal Grouping<'key, 'item> (key: 'key, items: 'item array) =
    member this.Key = (this :> IGrouping<'key, 'item>).Key
    member this.Length = items.LongLength

    override this.ToString (): string = 
        sprintf "Key = %A" this.Key

    interface IGrouping<'key, 'item> with
        member this.Key = key
        member this.GetEnumerator() = (items :> 'item seq).GetEnumerator()
        member this.GetEnumerator() : IEnumerator = 
            (items :> IEnumerable).GetEnumerator()

type private FSharpListDeserialize =
    static member val private Cache = ConcurrentDictionary<Type, obj seq -> obj>()
    static member private OfSeq<'a>(c: obj seq) = c |> Seq.map (fun a -> a :?> 'a) |> List.ofSeq

    static member internal MakeFrom (elementType: Type) =
        FSharpListDeserialize.Cache.GetOrAdd(elementType, Func<Type, obj seq -> obj>(
            fun elementType ->
                let method =
                    typedefof<FSharpListDeserialize>
                        .GetMethod(nameof FSharpListDeserialize.OfSeq, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(elementType)

                fun (contents) ->
                    method.Invoke(null, [|contents|])
            )
        )


// For C#
[<Struct>]
type JsonValueType =
| Null = 1uy
| Boolean = 2uy
| String = 3uy
| Number = 4uy
| List = 5uy
| Object = 6uy

type JsonValue =
    | Null
    | Boolean of bool
    | String of string
    | Number of decimal
    | List of IList<JsonValue>
    | Object of IDictionary<string, JsonValue>

    static member private SerializeDictionary<'key, 'value> (dictionary: obj) =
        let entries = new Dictionary<string, JsonValue>()
        match dictionary with
        | :? IReadOnlyDictionary<'key, 'value> as dictionary ->
            for key in dictionary.Keys do                
                let value = dictionary.[key]
                let key = string key
                entries.Add(key, JsonValue.Serialize value)
        | :? IDictionary<'key, 'value> as dictionary ->
            for key in dictionary.Keys do                
                let value = dictionary.[key]
                let key = string key
                entries.Add(key, JsonValue.Serialize value)
        | other ->
            failwithf "Type %s does not implement IDictionary<,> or IReadOnlyDictionary<,>" (other.GetType().Name)
           
        Object entries

    static member internal SerializeDictionaryGeneric (obj: obj) =
        let t = obj.GetType()
        match t.GetInterface("IReadOnlyDictionary`2") |> Option.ofObj |> Option.defaultWith (fun () -> t.GetInterface("IDictionary`2")) with
        | null ->
            failwith "Object does not implement IReadOnlyDictionary<'key, 'value> or IDictionary<'key, 'value>"
        | i ->
            let genericArguments = i.GetGenericArguments()
            let method = typeof<JsonValue>.GetMethod(nameof(JsonValue.SerializeDictionary), BindingFlags.NonPublic ||| BindingFlags.Static)
            let genericMethod = method.MakeGenericMethod(genericArguments)
            genericMethod.Invoke(null, [| obj |]) :?> JsonValue

    static member Serialize (value: obj) =
        let rec trySerializePrimitive (value: obj) =
            match value with
            | :? bool as b -> Boolean b

            | :? uint8 as i -> Number (decimal i)
            | :? uint16 as i -> Number (decimal i)
            | :? uint32 as i -> Number (decimal i)
            | :? uint64 as i -> Number (decimal i)

            | :? int8 as i -> Number (decimal i)
            | :? int16 as i -> Number (decimal i)
            | :? int32 as i -> Number (decimal i)
            | :? int64 as i -> Number (decimal i)

            | :? float32 as i -> Number (decimal i)
            | :? float as f -> Number (decimal f)
            | :? decimal as i -> Number (decimal i)

            | :? DateTimeOffset as date -> date.ToUnixTimeMilliseconds() |> decimal |> Number
            | :? DateTime as date -> date.ToBinary() |> decimal |> Number
            | :? DateOnly as date -> date.DayNumber |> decimal |> Number
            | :? TimeOnly as time -> time.ToTimeSpan().TotalMilliseconds |> int64 (* Convert to int64 to allow for higher precision in SQLite. *) |> decimal |> Number
            | :? TimeSpan as span -> span.TotalMilliseconds |> int64 |> decimal |> Number

            | :? Guid as guid -> guid.ToString("D", CultureInfo.InvariantCulture) |> String

            | :? string as s -> String s
            // For efficient storage.
            | :? (byte[]) as bs -> String(JsonHelper.byteArrayToJSONCompatibleString bs)
            | :? (byte seq) as bs -> trySerializePrimitive (bs |> Seq.toArray)
            | _ -> Null
    
        let serializeEnum (e: Enum) =
            let t = e.GetType()
            let enumValue = Convert.ChangeType(e, Enum.GetUnderlyingType(t))
            match trySerializePrimitive enumValue with
            | Null -> failwithf "Could not serialize enum: \"%A\" of type %A" e t
            | ok -> ok

        let serializeCollection (collection: IEnumerable) =
            let items = System.Collections.Generic.List<JsonValue>()
            for item in collection do
                items.Add(JsonValue.Serialize item)
            List items
    
        let serializeDictionary (dictionary: IDictionary) =
            let entries = new Dictionary<string, JsonValue>()
            for key in dictionary.Keys do
                let key = key.ToString()
                let value = dictionary.[key]
                entries.Add(key, JsonValue.Serialize value)
            Object entries

            
        let valueType = value.GetType()

        if obj.ReferenceEquals(value, null) then
            Null
        else if typeof<JsonValue>.IsAssignableFrom valueType then
            value :?> JsonValue
        else if valueType.FullName = "SoloDatabase.MongoDB.BsonDocument" then
            valueType.GetProperty("Json").GetValue(value) :?> JsonValue
        else 
        match value with
        | :? Enum as e -> serializeEnum e
        | _other -> 
        match trySerializePrimitive value with
        | Null ->
            if typeof<IDictionary>.IsAssignableFrom(valueType) then
                serializeDictionary (value :?> IDictionary)
            elif JsonHelper.implementsGeneric typedefof<IReadOnlyDictionary<_,_>> valueType || JsonHelper.implementsGeneric typedefof<IDictionary<_,_>> valueType then
                JsonValue.SerializeDictionaryGeneric value
            elif typeof<IEnumerable>.IsAssignableFrom(valueType) then
                serializeCollection (value :?> IEnumerable)
            else
                let props = valueType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                let dict = new Dictionary<string, JsonValue>()
                for prop in props do
                    if prop.CanRead then
                        let propValue = prop.GetValue(value)
                        if propValue <> null then
                            let serializedValue = JsonValue.Serialize propValue
                            dict.Add(prop.Name, serializedValue)
                Object dict
        | value -> value

    static member SerializeWithType (value: obj) =
        let json = JsonValue.Serialize value
        match json, value.GetType() |> typeToName with
        | Object _, Some t -> json["$type"] <- String t
        | _other, Some t -> () // Ignore
        | _other, None -> () // Also ignore
        json

    static member DeserializeDynamic (json: JsonValue) : obj =
        let toDecimal (input: string) =
            match Decimal.TryParse (input, NumberStyles.Float, CultureInfo.InvariantCulture) with
            | true, dec -> Some dec
            | _ -> None

        let toInt64 (input: string) =
            match Int64.TryParse (input, NumberStyles.Integer, CultureInfo.InvariantCulture) with
            | true, i64 -> Some i64
            | _ -> None

        let rec deserialize json =
            match json with
            | JsonValue.Null -> null
            | JsonValue.String s -> 
                match toInt64 s with
                | Some x -> x |> box
                | None ->
                match toDecimal s with
                | Some x -> x
                | None -> s
            | JsonValue.Number n -> if Decimal.IsInteger n && abs n < decimal Int64.MaxValue then (int64 n) else n
            | JsonValue.Boolean b -> b
            | JsonValue.List lst ->
                lst |> Seq.map deserialize |> Generic.List<obj> |> box
            | JsonValue.Object obj ->
                let expando = ExpandoObject() :> IDictionary<string, obj>
                for kvp in obj do
                    expando.[kvp.Key] <- deserialize kvp.Value
                expando :> obj
        deserialize json

    static member Deserialize (targetType: Type) (json: JsonValue) : obj =
        let createInstance (targetType: Type) =
            Utils.initEmpty targetType

        let isCollectionType (typ: Type) =
            typ.IsArray
            || typ.GetInterfaces() |> Array.exists(fun i -> i.IsGenericType && i.GetGenericTypeDefinition().Namespace = typeof<IList<_>>.Namespace && i.GetGenericTypeDefinition().Name = typeof<IList<_>>.Name)

        let isFSharpList (typ: Type) =
            JsonHelper.implementsGeneric typedefof<obj list> typ

        let tryDeserializePrimitive (targetType: Type) (value: JsonValue) : obj =
            match targetType, value with
            | t, JsonValue.Boolean b when t = typeof<bool> -> b

            | t, JsonValue.String s when t = typeof<byte array> -> JsonHelper.JSONStringToByteArray s
            | t, JsonValue.String s when t = typeof<byte seq> -> JsonHelper.JSONStringToByteArray s
            | t, JsonValue.String s when t = typeof<string> -> s
            | t, JsonValue.String s when t = typeof<Type> -> s |> nameToType |> box
            | t, JsonValue.String s when t = typeof<Guid> -> Guid.Parse(s) |> box

            | t, JsonValue.String s when t = typeof<DateTimeOffset> -> DateTimeOffset.Parse(s, CultureInfo.InvariantCulture) |> box
            | t, JsonValue.String s when t = typeof<DateTime> -> DateTime.Parse(s, CultureInfo.InvariantCulture) |> box
            | t, JsonValue.String s when t = typeof<DateOnly> -> DateOnly.Parse(s, CultureInfo.InvariantCulture) |> box
            | t, JsonValue.String s when t = typeof<TimeOnly> -> TimeOnly.Parse(s, CultureInfo.InvariantCulture) |> box
            | t, JsonValue.String s when t = typeof<TimeSpan> -> TimeSpan.Parse(s, CultureInfo.InvariantCulture) |> box


            | t, JsonValue.Number n when t = typeof<float32> -> float32 n
            | t, JsonValue.Number n when t = typeof<float> -> float n
            | t, JsonValue.Number n when t = typeof<bool> -> if n = Decimal.Zero then false else true

            | t, JsonValue.Number n when t = typeof<int8> -> int8 n
            | t, JsonValue.Number n when t = typeof<int16> -> int16 n
            | t, JsonValue.Number n when t = typeof<int32> -> int32 n
            | t, JsonValue.Number n when t = typeof<int64> -> int64 n

            | t, JsonValue.Number n when t = typeof<uint8> -> uint8 n
            | t, JsonValue.Number n when t = typeof<uint16> -> uint16 n
            | t, JsonValue.Number n when t = typeof<uint32> -> uint32 n
            | t, JsonValue.Number n when t = typeof<uint64> -> uint64 n

            | t, JsonValue.Number n when t = typeof<decimal> -> decimal n

            | t, JsonValue.Number n when t = typeof<DateTimeOffset> -> n |> int64 |> DateTimeOffset.FromUnixTimeMilliseconds |> box
            | t, JsonValue.Number n when t = typeof<DateTime> -> n |> int64 |> DateTime.FromBinary |> box
            | t, JsonValue.Number n when t = typeof<DateOnly> -> n |> int |> DateOnly.FromDayNumber |> box
            | t, JsonValue.Number n when t = typeof<TimeOnly> -> n |> float |> TimeSpan.FromMilliseconds |> TimeOnly.FromTimeSpan |> box
            | t, JsonValue.Number n when t = typeof<TimeSpan> -> n |> float |> TimeSpan.FromMilliseconds |> box

            | _ -> null

        let deserializeList (targetType: Type) (value: JsonValue) =
            match value with
            | JsonValue.List items ->
                if targetType.IsArray then
                    let elementType = targetType.GetElementType()
                    let array = Array.CreateInstance(elementType, items.Count)
                    for i, v in items |> Seq.indexed do
                        array.SetValue((JsonValue.Deserialize elementType v), i)
                    array |> box
                elif targetType.GetInterface(typedefof<IList<_>>.Name) <> null then
                    let icollectionType = targetType.GetInterfaces() |> Array.find(fun i -> i.IsGenericType && i.GetGenericTypeDefinition() = typedefof<IList<_>>) 
                    let elementType = icollectionType.GenericTypeArguments.[0]
                    let listInstance = Activator.CreateInstance(targetType) :?> IList
                    for item in items do
                        listInstance.Add(JsonValue.Deserialize elementType item) |> ignore
                    listInstance
                else failwithf "Cannot deserialize a list to type %A" targetType
            | _ -> failwith "Expected JSON array for collection deserialization."

        let deserializeFSharpList (targetType: Type) (value: JsonValue) =
            match value with
            | JsonValue.List items ->
                match GenericTypeArgCache.Get targetType |> Array.tryHead with
                | None -> failwithf "Cannot deserialize a F# list from type: %A" targetType
                | Some elementType ->
                    items
                    |> Seq.map (JsonValue.Deserialize elementType)
                    |> FSharpListDeserialize.MakeFrom elementType

            | _ -> failwith "Expected JSON array for collection deserialization."

        let deserializeTuple (targetType: Type) (value: JsonValue) =
            match value with
            | JsonValue.List items ->
                let elementsTypes = targetType.GetGenericArguments()
                let values = items |> Seq.zip elementsTypes |> Seq.map (fun (typ, item) -> JsonValue.Deserialize typ item) |> Seq.toArray
                Dynamic.InvokeConstructor(targetType, values)
            | _ -> failwith "Expected JSON array for tuple deserialization."

        let deserializeEnum (targetType: Type) (value: JsonValue) =
            match tryDeserializePrimitive (Enum.GetUnderlyingType(targetType)) value with
            | null -> failwithf "Could not deserialize enum: \"%A\" of type %A" value targetType
            | ok ->
            Enum.ToObject(targetType, ok)

        let deserializeDictionary (targetType: Type) (value: JsonValue) =
            match value with
            | JsonValue.Object properties ->
                let struct (keyType, valueType) =
                    if targetType.IsGenericType then
                        struct(targetType.GenericTypeArguments.[0], targetType.GenericTypeArguments.[1])
                    else
                        struct(typeof<string>, typeof<obj>)

                let targetType = 
                    if targetType.IsInterface then
                        typedefof<Dictionary<_,_>>.MakeGenericType(keyType, valueType)
                    else
                        targetType

                let dictInstance = Activator.CreateInstance(targetType) :?> IDictionary
                for kvp in properties do
                    let key = Convert.ChangeType(kvp.Key, keyType)
                    let value = JsonValue.Deserialize valueType kvp.Value
                    dictInstance.Add(key, value)
                dictInstance
            | _ -> failwith "Expected JSON object for dictionary deserialization."

        let deserializeIGrouping (targetType: Type) (jsonObj: JsonValue) =
            let groupingTypes = GenericTypeArgCache.Get targetType
            let key: JsonValue = jsonObj.["Key"]
            let items: JsonValue = jsonObj.["Items"]

            let key: obj = key.ToObject groupingTypes.[0]
            let items: obj = items.ToObject (groupingTypes.[1].MakeArrayType ())

            let g = Activator.CreateInstance(typedefof<Grouping<_, _>>.MakeGenericType(groupingTypes.[0], groupingTypes.[1]), key, items)
            g

        let deserializeObject (targetType: Type) (jsonObj: JsonValue) =
            let ogTargetType = targetType
            let targetType =
                // Be careful, a potential attacker can put anything in this field.
                let unsafeTypeProp = match jsonObj.TryGetProperty "$type" with true, x -> x.ToObject<string>() |> nameToType | false, _ -> null
                match targetType.IsSealed, unsafeTypeProp with
                | true, _ -> targetType
                | _, null -> targetType
                | false, (jsonType) when targetType.IsAssignableFrom jsonType -> jsonType
                | _ -> targetType

                
            match jsonObj with
            | JsonValue.Object properties when FSharpType.IsUnion targetType ->
                let cases = FSharpType.GetUnionCases targetType
                let tag = properties.["Tag"].ToObject<int>()
                let machingCase = cases |> Array.find(fun c -> c.Tag = tag)
                let fields = machingCase.GetFields()
                let fieldsValue = fields |> Array.map(fun f -> 
                    match properties.TryGetValue f.Name with 
                    | true, value -> JsonValue.Deserialize f.PropertyType value
                    | false, _ -> null
                )
                let caseInstance = FSharpValue.MakeUnion(machingCase, fieldsValue)
                    
                caseInstance

            | JsonValue.Object properties when FSharpType.IsRecord targetType ->
                let recordFields = FSharpType.GetRecordFields targetType
                let recordValues = recordFields |> Seq.map(fun field ->
                    let propName = field.Name
                    match properties.TryGetValue(propName) with
                    | true, value ->
                        let propValue = JsonValue.Deserialize field.PropertyType value
                        propValue
                    | false, _ -> null) |> Seq.toArray

                FSharpValue.MakeRecord(targetType, recordValues)
            // This is for partial support for Newtonsoft's json format.
            | JsonValue.Object properties when jsonObj.Contains "$type" && jsonObj.Contains "$values" && JsonHelper.isSupportedNewtownsoftArrayType targetType ->
                match properties.TryGetValue "$values" with
                | false, _ -> failwithf "It is impossible to execute this code."
                | true, value ->
                    
                deserializeList targetType value
            | JsonValue.Object properties ->
                let instance = createInstance targetType

                let propertiesInfo = targetType.GetProperties()

                if (not ogTargetType.IsAbstract) && propertiesInfo |> Seq.sumBy(fun p -> if p.CanWrite then 1 else 0) = 0 then
                    failwithf "Could not desealize the type %s, it does not have any public writable property." targetType.Name

                for prop in propertiesInfo do
                    if prop.CanWrite then
                        let propName = prop.Name
                        match properties.TryGetValue(propName) with
                        | true, value ->
                            let propValue = JsonValue.Deserialize prop.PropertyType value
                            prop.SetValue(instance, propValue, null)
                        | false, _ -> ()

                instance
            | _ -> failwith "Expected JSON object for object deserialization."

        if targetType = typeof<obj> then 
            JsonValue.DeserializeDynamic json
        elif targetType = typeof<JsonValue> then
            json
        // We cannot reference it forwards, so we do this.
        elif targetType.FullName = "SoloDatabase.MongoDB.BsonDocument" then
            Activator.CreateInstance(targetType, json)
        elif targetType.Name = "Nullable`1" && targetType.GenericTypeArguments.Length = 1 then 
            let innerValue = JsonValue.Deserialize targetType.GenericTypeArguments.[0] json
            if isNull innerValue then
                Activator.CreateInstance(targetType)
            else 
                Activator.CreateInstance(targetType, innerValue)
        elif targetType.IsEnum then
            deserializeEnum targetType json
        else

        match json with
        | JsonValue.Null -> null
        | json ->
        match tryDeserializePrimitive targetType json with
        | null ->
            match json with
            | JsonValue.List _ when isTuple targetType ->
                deserializeTuple targetType json
            | JsonValue.List _ when isCollectionType targetType ->
                deserializeList targetType json
            | JsonValue.List _ when isFSharpList targetType ->
                deserializeFSharpList targetType json
            | JsonValue.Object _ when typeof<IDictionary>.IsAssignableFrom targetType || JsonHelper.implementsGeneric typedefof<IDictionary<_,_>> targetType || JsonHelper.implementsGeneric typedefof<IReadOnlyDictionary<_,_>> targetType ->
                deserializeDictionary targetType json
            | JsonValue.Object _ when JsonHelper.implementsGeneric typedefof<IGrouping<_, _>> targetType ->
                deserializeIGrouping targetType json  
            | JsonValue.Object _ when targetType.IsClass || targetType.IsAbstract || FSharpType.IsUnion targetType->
                deserializeObject targetType json                    
            | JsonValue.String s -> failwithf "JSON value cannot be deserialized into type %A, value: {%s}" targetType s
            | _ -> failwithf "JSON value cannot be deserialized into type %A" targetType
        | primitive -> primitive

    // For C# usage.
    member this.JsonType = 
        match this with 
        | Null -> JsonValueType.Null
        | Boolean _b -> JsonValueType.Boolean
        | String _s -> JsonValueType.String
        | Number _n -> JsonValueType.Number
        | List _l -> JsonValueType.List
        | Object _o -> JsonValueType.Object

    member this.ToObject (targetType: Type) =
        JsonValue.Deserialize targetType this

    member this.ToObject<'T>() : 'T =
        JsonValue.Deserialize typeof<'T> this :?> 'T

    member this.SetProperty(name: string, value: JsonValue) =
        match this with
        | Object o -> o.[name] <- value
        | List list -> 
            let index = Int32.Parse(name, CultureInfo.InvariantCulture)
            list.[index] <- value
        | other -> failwithf "Cannot index %s" (other.ToString())

    member this.Contains (name: string) =
        match this with
        | Object o -> o.ContainsKey name
        | List list -> 
            match Int32.TryParse (name, NumberStyles.Integer, CultureInfo.InvariantCulture) with
            | true, index -> list.Count > index
            | false, _ -> false
        | other -> failwithf "Cannot index %s" (other.ToString())


    member this.TryGetProperty(name: string, [<System.Runtime.InteropServices.Out>] v: byref<JsonValue>) : bool =
        match this with
        | Object o -> 
            match o.TryGetValue(name) with
            | true, out ->
                v <- out
                true
            | false, out ->
                v <- out
                false
        | List list -> 
            let index = Int32.Parse (name, CultureInfo.InvariantCulture)
            v <- list.[index]
            true
        | other -> failwithf "Cannot index %s" (other.ToString())

    member this.GetProperty(name: string) =
        match this.TryGetProperty(name) with
        | true, value -> value
        | false, _ -> failwith "Property not found"

    // If rename then also rename the string from JsonValueMetaObject
    member internal this.GetPropertyForBinder(name: string) =
        match this.TryGetProperty(name) with
        | true, value ->
            match value with
            | Null -> null :> obj
            | Boolean b -> b
            | String s -> s
            | Number n -> 
                if Decimal.IsInteger n then
                    match n with
                    | n when n >= decimal Int32.MinValue && n <= decimal Int32.MaxValue -> int n :> obj
                    | n when n >= decimal Int64.MinValue && n <= decimal Int64.MaxValue -> int64 n :> obj
                    | other -> other
                else
                    match n with
                    | n when n >= decimal Double.MinValue && n <= decimal Double.MaxValue -> float n :> obj
                    | other -> other
            | List _l -> value // Return the full object for complex types.
            | Object _d -> value
        | false, _ -> failwith "Property not found"

    [<System.Runtime.CompilerServices.IndexerName("Item")>]
    member this.Item
        with get(name: string) =
            match this.TryGetProperty(name) with
            | true, value -> value
            | false, _ -> failwith "Property not found"

        and set(name: string) (jValue: JsonValue) =
            this.SetProperty (name, jValue)

    static member private Jsonize value (write: string -> unit) =
        match value with
        | Null -> write "null" |> ignore
        | Boolean b -> write (if b then "true" else "false") |> ignore
        | String s -> 
            write "\"" |> ignore
            let escapedString = HttpUtility.JavaScriptStringEncode(s, false)
            write escapedString |> ignore
            write "\"" |> ignore
        | Number n -> 
            if Decimal.IsInteger n then 
                write (n.ToString("F0", CultureInfo.InvariantCulture)) |> ignore
            else 
                write (n.ToString(CultureInfo.InvariantCulture)) |> ignore
        | List arr ->
            write "[" |> ignore
            let items = arr.Count
            for i, item in arr |> Seq.indexed do
                JsonValue.Jsonize item write
                if i < items - 1 then write ", " |> ignore
            write "]" |> ignore

        | Object obj ->
            write "{" |> ignore
            let items = obj.Count
            for i, kvp in obj |> Seq.indexed do
                write "\"" |> ignore
                write kvp.Key |> ignore
                write "\"" |> ignore
                write ": " |> ignore
                JsonValue.Jsonize kvp.Value write
                if i < items - 1 then write ", " |> ignore
            write "}" |> ignore

    member this.ToJsonString() =            
        let sb = new StringBuilder(128)
        JsonValue.Jsonize this (fun text -> sb.Append text |> ignore)
        sb.ToString()

    member this.Eq (other: obj) =
        this = JsonValue.Parse $"{other}"

    static member New() = JsonValue.Object(Dictionary())

    static member New(values: KeyValuePair<string, obj> seq) = 
        let json = JsonValue.New()
        for KeyValue(key ,v) in values do
            json.[key] <- JsonValue.Serialize v
        json
        
    static member Parse (jsonString: string) =
        let parse (tokens: Token seq) : JsonValue =
            let next(tokens: IEnumerator<Token>) =
                RuntimeHelpers.EnsureSufficientExecutionStack()
                if tokens.MoveNext() then
                    tokens.Current
                else EndOfInput
            
            let rec parseTokensOr (tokens: IEnumerator<Token>) f =
                let current = next tokens
                match current with
                | OpenBrace ->
                    let obj = parseObject tokens
                    Object obj |> Some
                | OpenBracket ->
                    let list = parseArray tokens
                    List list |> Some
                | StringToken str ->
                    String str |> Some
                | NumberToken num ->
                    Number num |> Some
                | BooleanToken b ->
                    JsonValue.Boolean b |> Some
                | NullToken ->
                    Null |> Some
                | other -> f other

            and parseTokens (tokens: IEnumerator<Token>) =
                parseTokensOr tokens (fun t -> failwithf "Malformed JSON at: %A" t) |> _.Value
                    
            and parseObject tokens =
                let dict = new Dictionary<string, JsonValue>()
                let rec parseMembers tokens =
                    let current = next tokens
                    match current with
                    | CloseBrace -> dict
                    | StringToken key ->
                        let colon = next tokens
                        if colon <> Colon then failwithf "Malformed json."
            
                        let value = parseTokens tokens
                        dict.Add(key, value)
                        parseMembers tokens
                    | Comma -> parseMembers tokens
                    | _ -> failwith "Invalid object syntax"
                parseMembers tokens
                    
            and parseArray tokens =
                let items = new List<JsonValue>()
                let rec parseElements tokens =
                    let item = parseTokensOr tokens (fun t -> match t with CloseBracket -> None | other -> failwithf "Invalid list token: %A" other)
                    match item with
                    | None -> items
                    | Some item ->
                    items.Add(item)
            
                    let current = next tokens
                    match current with
                    | CloseBracket -> items
                    | Comma -> parseElements tokens
                    | _ -> failwithf "Malformed json."
                parseElements tokens
                    
            let enumerator = tokens.GetEnumerator()
            let jsonObj = parseTokens enumerator

            jsonObj

        let tokens = Token.Parse jsonString
        parse tokens

    static member Create (values: #((obj * obj) seq)) = 
        let json = JsonValue.New()
        for (key, v) in values do
            let key = 
                match key with
                | :? string as s -> s
                | :? int16 as i -> i.ToString(CultureInfo.InvariantCulture)
                | :? uint16 as i -> i.ToString(CultureInfo.InvariantCulture)
                | :? int as i -> i.ToString(CultureInfo.InvariantCulture)
                | :? uint as i -> i.ToString(CultureInfo.InvariantCulture)
                | :? int64 as i -> i.ToString(CultureInfo.InvariantCulture)
                | :? uint64 as i -> i.ToString(CultureInfo.InvariantCulture)
    
                | :? float32 as i -> i.ToString(CultureInfo.InvariantCulture)
                | :? float as i -> i.ToString(CultureInfo.InvariantCulture)
                | :? decimal as i -> i.ToString(CultureInfo.InvariantCulture)
                | other -> string other
    
            json.[key] <- JsonValue.Serialize v
        json

    override this.ToString() = this.ToJsonString()

    static member op_Implicit(o: obj) : JsonValue =
        JsonValue.Serialize o

    interface IDynamicMetaObjectProvider with
        member this.GetMetaObject(expression: Linq.Expressions.Expression): DynamicMetaObject = 
            JsonValueMetaObject(expression, BindingRestrictions.Empty, this)

    interface IEnumerable<KeyValuePair<string, JsonValue>> with
        override this.GetEnumerator (): IEnumerator<KeyValuePair<string,JsonValue>> =
            match this with
            | Object o -> o.GetEnumerator()
            | List l -> (l |> Seq.indexed |> Seq.map(fun (i, v) -> KeyValuePair<string, JsonValue>(string i, v))).GetEnumerator()
            | _other -> failwithf "This Json type does not support iteration: %A" (this.JsonType)

        override this.GetEnumerator (): IEnumerator = 
            (this :> IEnumerable<KeyValuePair<string, JsonValue>>).GetEnumerator () :> IEnumerator

and JsonSerializerImpl<'A when 'A :> obj> =
    static member private SerializeFunc(a: 'A) : JsonValue =
        JsonSerializerImpl<'A>.Serialize a

    static member private FSharpString<'T>(a: 'T) : string = string a

    static member private CreateJsonObj(d: IDictionary<string, JsonValue>) =
        JsonValue.Object d

    static member private IEnumerableSerialize<'T>(o: 'T seq) =
        let items = System.Collections.Generic.List<JsonValue>()
        for item in o do
            items.Add(JsonSerializerImpl<'T>.Serialize item)
        List items

    static member private SerializeDictionary<'key, 'value> (dictionary: obj) =
        let entries = 
            match dictionary with
            | :? IReadOnlyDictionary<'key, 'value> as dictionary ->
                let entries = new Dictionary<string, JsonValue>(dictionary.Count)
                for key in dictionary.Keys do
                    let value = dictionary.[key]
                    let key = string key
                    entries.Add(key, JsonSerializerImpl<'value>.Serialize value)
                entries
            | :? IDictionary<'key, 'value> as dictionary ->
                let entries = new Dictionary<string, JsonValue>(dictionary.Count)
                for key in dictionary.Keys do
                    let value = dictionary.[key]
                    let key = string key
                    entries.Add(key, JsonSerializerImpl<'value>.Serialize value)
                entries
            | other ->
                failwithf "Type %s does not implement IDictionary<,> or IReadOnlyDictionary<,>" (other.GetType().Name)
           
        Object entries

    static member val Serialize: 'A -> JsonValue = 
        match typeof<'A> with
        | t when t = typeof<obj> -> 
            let cache = ConcurrentDictionary<Type, obj -> JsonValue>()
            (fun (o: obj) -> 
                let runtimeType = o.GetType()
                let fn = 
                    cache.GetOrAdd(runtimeType, Func<Type, obj -> JsonValue>(fun (t: Type) ->
                        let p = Expression.Parameter typeof<obj>
                        
                        let meth = 
                            typedefof<JsonSerializerImpl<_>>
                                .MakeGenericType(t)
                                .GetMethod(nameof JsonSerializerImpl<_>.SerializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
                        
                        let fn = 
                            Expression.Lambda<Func<obj, JsonValue>>(
                                Expression.Call(meth, Expression.Convert(p, t)),
                                [|p|]
                            ).Compile(false)

                        fn.Invoke
                    ))

                fn o
            )
            :> obj :?> ('A -> JsonValue)

        | OfType bool -> 
            (fun (o: bool) -> Boolean o)
            :> obj :?> ('A -> JsonValue)

         | OfType uint8 -> 
            (fun (o: uint8) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)
            
        | OfType uint16 ->
            (fun (o: uint16) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType uint32 -> 
            (fun (o: uint32) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType uint64 ->
            (fun (o: uint64) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType int8 ->
            (fun (o: int8) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType int16 ->
            (fun (o: int16) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType int32 ->
            (fun (o: int32) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType int64 -> 
            (fun (o: int64) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType float32 ->
            (fun (o: float32) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType float ->
            (fun (o: float) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType decimal ->
            (fun (o: decimal) -> Number o)
            :> obj :?> ('A -> JsonValue)

        | OfType DateTimeOffset -> 
            (fun (date: DateTimeOffset) -> date.ToUnixTimeMilliseconds() |> decimal |> Number)
            :> obj :?> ('A -> JsonValue)

        | OfType DateTime -> 
            (fun (date: DateTime) -> date.ToBinary() |> decimal |> Number)
            :> obj :?> ('A -> JsonValue)

        | OfType (DateOnly: unit -> DateOnly) -> 
            (fun (date: DateOnly) -> date.DayNumber |> decimal |> Number)
            :> obj :?> ('A -> JsonValue)

        | OfType TimeOnly -> 
            (fun (time: TimeOnly) -> time.ToTimeSpan().TotalMilliseconds |> int64 (* Convert to int64 to allow for higher precision in SQLite. *) |> decimal |> Number)
            :> obj :?> ('A -> JsonValue)

        | OfType TimeSpan -> 
            (fun (ts: TimeSpan) -> ts.TotalMilliseconds |> int64 |> decimal |> Number)
            :> obj :?> ('A -> JsonValue)

        | OfType (Guid: unit -> Guid) -> 
            (fun (guid: Guid) -> guid.ToString("D", CultureInfo.InvariantCulture) |> String)
            :> obj :?> ('A -> JsonValue)

        | OfType string -> 
            (fun (s: string) -> String s)
            :> obj :?> ('A -> JsonValue)

        // For efficient storage.
        | OfType (id: byte array -> byte array) ->
            (fun (ba: byte array) ->  String(JsonHelper.byteArrayToJSONCompatibleString ba))
            :> obj :?> ('A -> JsonValue)

        | OfType (id: byte seq -> byte seq) ->
            (fun (bs: byte seq) ->
                let arr = Seq.toArray bs
                JsonSerializerImpl<byte array>.Serialize arr
                )
            :> obj :?> ('A -> JsonValue)

         | t when t.IsEnum ->
            let underlyingType = Enum.GetUnderlyingType(t)
            let p = Expression.Parameter t
            
            let meth = 
                typedefof<JsonSerializerImpl<_>>
                    .MakeGenericType(underlyingType)
                    .GetMethod(nameof JsonSerializerImpl<_>.SerializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
            
            let fn = 
                Expression.Lambda<Func<'A, JsonValue>>(
                    Expression.Call(meth, Expression.Convert(p, underlyingType)),
                    [|p|]
                ).Compile(false)

            fn.Invoke

        | t when typeof<JsonValue>.IsAssignableFrom(t) ->
            (fun (v: JsonValue) -> v)
            :> obj :?> ('A -> JsonValue)

        | t when t.FullName = "SoloDatabase.MongoDB.BsonDocument" ->
            let p = Expression.Parameter(t)
            let jsonProp = t.GetProperty("Json")
            let fn = 
                Expression.Lambda<Func<'A, JsonValue>>(
                    Expression.Property(p, jsonProp),
                    [|p|]
                ).Compile(false)
            fn.Invoke

        | t when JsonHelper.implementsGeneric typedefof<IReadOnlyDictionary<_,_>> t || 
                 JsonHelper.implementsGeneric typedefof<IDictionary<_,_>> t ->

            let iface = 
                t.GetInterface("IReadOnlyDictionary`2") 
                |> Option.ofObj 
                |> Option.orElseWith(fun () -> t.GetInterface("IDictionary`2") |> Option.ofObj) 
                |> Option.defaultValue t // If not then it is an interface
    
            let genericArgs = GenericTypeArgCache.Get iface

            let param = Expression.Parameter(t)

            let serializeMeth = typeof<JsonSerializerImpl<obj>>
                                 .GetMethod(nameof JsonSerializerImpl<_>.SerializeDictionary, BindingFlags.NonPublic ||| BindingFlags.Static)
                                 .MakeGenericMethod(genericArgs)


            let lambda = Expression.Lambda<Func<'A, JsonValue>>(
                Expression.Call(serializeMeth, [|param :> Expression|]), 
                [|param|]
            )
    
            let fn = lambda.Compile(false)

            fn.Invoke :> obj :?> ('A -> JsonValue)

            
        | t when typeof<IDictionary>.IsAssignableFrom(t) ->
            (fun (dict: 'A) ->
                let entries = new Dictionary<string, JsonValue>()
                let dictionary = dict :> obj :?> IDictionary
                for key in dictionary.Keys do
                    let keyStr = key.ToString()
                    let value = dictionary.[key]
                    entries.Add(keyStr, JsonSerializerImpl<obj>.Serialize value)
                Object entries)
            :> obj :?> ('A -> JsonValue)

        | t when JsonHelper.implementsGeneric typedefof<IEnumerable<_>> t && t <> typeof<string> ->
            let elemType = GenericTypeArgCache.Get (t.GetInterface("IEnumerable`1")) |> Array.head

            let meth = typeof<JsonSerializerImpl<obj>>
                        .GetMethod((nameof JsonSerializerImpl<_>.IEnumerableSerialize), BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(elemType)

            let param = Expression.Parameter(t)

            let lambda = Expression.Lambda<Func<'A, JsonValue>>(
                Expression.Call(meth, [|param :> Expression|]),
                [|param|]
            )

            let fn = lambda.Compile()

            (fun (collection: 'A) ->
                fn.Invoke collection)
            :> obj :?> ('A -> JsonValue)

        | t when typeof<IEnumerable>.IsAssignableFrom(t) && t <> typeof<string> ->
            (fun (collection: 'A) ->
                let items = System.Collections.Generic.List<JsonValue>()
                let enumerable = collection :> obj :?> IEnumerable
                for item in enumerable do
                    items.Add(JsonSerializerImpl<obj>.Serialize item)
                List items)
            :> obj :?> ('A -> JsonValue)

        | t ->
            let props = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance) |> Array.filter(_.CanRead)
            let param = Expression.Parameter(t)
            let outDict = Expression.Variable(typeof<Dictionary<string, JsonValue>>)

            let createJsonMeth = typeof<JsonSerializerImpl<obj>>
                                    .GetMethod((nameof JsonSerializerImpl<_>.CreateJsonObj), BindingFlags.NonPublic ||| BindingFlags.Static)

            let returnLabel = Expression.Label(typeof<JsonValue>, "Return")

            let lambda = Expression.Lambda<Func<'A, JsonValue>>(
                Expression.Block(
                    [|outDict|],
                    [|
                        Expression.Assign(outDict, Expression.New(
                            typeof<Dictionary<string, JsonValue>>.GetConstructor([||]),
                            [||]
                        )) :> Expression

                        for prop in props do
                            Expression.Call(outDict, typeof<Dictionary<string, JsonValue>>.GetMethod("Add"), [|
                                Expression.Constant(prop.Name, typeof<string>) :> Expression;
                                let serializeMeth = typedefof<JsonSerializerImpl<_>>
                                                        .MakeGenericType(prop.PropertyType)
                                                        .GetMethod(nameof JsonSerializerImpl<obj>.SerializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
                                Expression.Call(
                                    serializeMeth,
                                     [|Expression.Property(param, prop) :> Expression|])
                            |])

                        Expression.Label(returnLabel, Expression.Call(createJsonMeth, [|outDict :> Expression|]))
                    |]
                ),
                [|param|]
            )

            let fn = lambda.Compile(false)


            (fun (o: 'A) ->
                if obj.ReferenceEquals(o, null) 
                then Null
                else fn.Invoke o)
            :> obj :?> ('A -> JsonValue)

and internal JsonValueMetaObject(expression: Expression, restrictions: BindingRestrictions, value: JsonValue) =
    inherit DynamicMetaObject(expression, restrictions, value)
    
    static member private GetPropertyMethod = typeof<JsonValue>.GetMethod("GetPropertyForBinder", BindingFlags.NonPublic ||| BindingFlags.Instance)
    static member private SetPropertyMethod = typeof<JsonValue>.GetMethod("set_Item")
    static member private ToObjectMethod = typeof<JsonValue>.GetMethod("ToObject")
    static member private ToStringMethod = typeof<obj>.GetMethod("ToString")
    static member private SerializeMethod = typeof<JsonValue>.GetMethod("Serialize")

    override this.BindGetMember(binder: GetMemberBinder) : DynamicMetaObject =
        let resultExpression = Expression.Call(Expression.Convert(this.Expression, typeof<JsonValue>), JsonValueMetaObject.GetPropertyMethod, Expression.Constant(binder.Name))
        DynamicMetaObject(resultExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    /// This is used for the setting of the properties and fields.
    override this.BindSetMember(binder: SetMemberBinder, value: DynamicMetaObject) : DynamicMetaObject =
        let setExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<JsonValue>), 
            JsonValueMetaObject.SetPropertyMethod, 
            Expression.Constant(binder.Name), 
            Expression.Call(JsonValueMetaObject.SerializeMethod, value.Expression)
        )

        let returnExpre = Expression.Block(setExpression, value.Expression)

        DynamicMetaObject(returnExpre, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))
    
    /// This is used for the conversion of the JsonValue to the target type.
    override this.BindConvert(binder: ConvertBinder) : DynamicMetaObject =
        let convertMethod = JsonValueMetaObject.ToObjectMethod.MakeGenericMethod(binder.Type)
        let convertExpression = Expression.Call(Expression.Convert(this.Expression, typeof<JsonValue>), convertMethod)
        DynamicMetaObject(convertExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))
   
    /// This is used for the indexers.    
    override this.BindGetIndex(binder: GetIndexBinder, indexes: DynamicMetaObject[]) : DynamicMetaObject =
        // Check that exactly one index is provided
        if indexes.Length <> 1 then
            failwithf "Json does not support indexes length <> 1: %i" indexes.Length
        
        let indexExpr = indexes.[0].Expression
        let targetType = Expression.Convert(this.Expression, typeof<JsonValue>)
        let resultExpression = Expression.Call(
            targetType,
            JsonValueMetaObject.GetPropertyMethod,
            Expression.Call(indexExpr, JsonValueMetaObject.ToStringMethod))

        DynamicMetaObject(resultExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))
        
    override this.BindSetIndex (binder: SetIndexBinder, indexes: DynamicMetaObject array, value: DynamicMetaObject): DynamicMetaObject = 
        // Check that exactly one index is provided
        if indexes.Length <> 1 then
            failwithf "Json does not support indexes length <> 1: %i" indexes.Length
        
        let indexExpr = indexes.[0].Expression
            
        let setExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<JsonValue>), 
            JsonValueMetaObject.SetPropertyMethod,
            Expression.Call(indexExpr, JsonValueMetaObject.ToStringMethod),
            Expression.Call(JsonValueMetaObject.SerializeMethod, value.Expression)
        )

        let returnExpre = Expression.Block(setExpression, value.Expression)

        DynamicMetaObject(returnExpre, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))
    
    override this.GetDynamicMemberNames() : IEnumerable<string> =
        match value with
        | Object o -> seq { for kv in o do yield kv.Key }
        | _ -> Seq.empty