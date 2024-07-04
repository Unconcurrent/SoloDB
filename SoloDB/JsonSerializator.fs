namespace SoloDatabase

// FormatterServices.GetSafeUninitializedObject for 
// types without a no parameter constructor.
#nowarn "0044"

module JsonSerializator =
    open System
    open System.Collections.Generic
    open System.Collections
    open System.Reflection
    open Dynamitey
    open System.Dynamic
    open SoloDatabase.Types
    open SoloDatabase.Utils
    open System.Text
    open System.Globalization

    let inline isNullableType (typ: Type) =
        typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Nullable<_>>

    let typeOfNullable (typ: Type) =
        if isNullableType typ then
            typ.GetGenericArguments().[0]
        else
            typ

    type Token =
        | NullToken
        | OpenBrace
        | CloseBrace
        | OpenBracket
        | CloseBracket
        | Comma
        | Colon
        | StringToken of string
        | NumberToken of decimal
        | BoolenToken of bool
        | EndOfInput

    let tokenize (input: string) : Token list =
        let isDigit c = Char.IsDigit c || c = '-'
        let isInitialIdentifierChar c = Char.IsLetter c || c = '_'
        let isIdentifierChar c = isInitialIdentifierChar c || Char.IsDigit c
        let rec tokenize' index tokens =
            if index >= input.Length then
                tokens @ [EndOfInput]
            else
                let currentChar = input.[index]
                match currentChar with
                | '{' -> tokenize' (index + 1) (tokens @ [OpenBrace])
                | '}' -> tokenize' (index + 1) (tokens @ [CloseBrace])
                | '[' -> tokenize' (index + 1) (tokens @ [OpenBracket])
                | ']' -> tokenize' (index + 1) (tokens @ [CloseBracket])
                | ',' -> tokenize' (index + 1) (tokens @ [Comma])
                | ':' -> tokenize' (index + 1) (tokens @ [Colon])
                | '"' | '\'' as quote ->
                    let sb = System.Text.StringBuilder()
                    let mutable i = index + 1
                    while input.[i] <> quote do
                        sb.Append(input.[i]) |> ignore
                        i <- i + 1
                    sb.Replace("\\u002B", "+") |> ignore
                    tokenize' (i + 1) (tokens @ [StringToken(sb.ToString())])
                | c when isDigit c ->
                    let sb = System.Text.StringBuilder()
                    let mutable i = index
                    while i < input.Length && (isDigit input.[i] || input.[i] = '.' || input.[i] = 'e' || input.[i] = '+') do
                        sb.Append(input.[i]) |> ignore
                        i <- i + 1
                    tokenize' i (tokens @ [NumberToken (Decimal.Parse(sb.ToString(), CultureInfo.InvariantCulture))])
                | c when isInitialIdentifierChar c ->
                    let sb = System.Text.StringBuilder()
                    let mutable i = index
                    while i < input.Length && isIdentifierChar input.[i] do
                        sb.Append(input.[i]) |> ignore
                        i <- i + 1
                    let txt = sb.ToString()
                    match txt with
                    | "null"
                    | "NULL" -> tokenize' i (tokens @ [NullToken])
                    | "true" -> tokenize' i (tokens @ [BoolenToken(true)])
                    | "false" -> tokenize' i (tokens @ [BoolenToken(false)])
                    | other -> tokenize' i (tokens @ [StringToken(txt)])
                | c when Char.IsWhiteSpace(c) -> tokenize' (index + 1) tokens
                | _ -> failwith "Invalid JSON format"
        tokenize' 0 []


    type JsonValue =
        | Null
        | Boolean of bool
        | String of string
        | Number of decimal
        | List of IList<JsonValue>
        | Object of IDictionary<string, JsonValue>

        static member Serialize (value: obj) =
            let trySerializePrimitive (value: obj) =
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

                | :? SqlId as i -> Number (decimal i.Value)

                | :? DateTimeOffset as date -> date.ToUnixTimeMilliseconds() |> decimal |> Number
                | :? DateTime as date -> date.ToBinary() |> decimal |> Number
                | :? DateOnly as date -> date.DayNumber |> decimal |> Number
                | :? TimeOnly as time -> time.ToTimeSpan().TotalMilliseconds |> int64 (* Convert to int64 because SQLite only stores 32 bit precision floats in JSON. *) |> decimal |> Number
                | :? TimeSpan as span -> span .TotalMilliseconds |> int64 |> decimal |> Number

                | :? string as s -> String s        
            
                | _ -> Null
    
            let serializeCollection (collection: IEnumerable) =
                let items = System.Collections.Generic.List<JsonValue>()
                for item in collection do
                    items.Add(JsonValue.Serialize item)
                List items
    
            let serializeDictionary (dictionary: IDictionary) =
                let entries = new Dictionary<string, JsonValue>(StringComparer.OrdinalIgnoreCase)
                for key in dictionary.Keys do
                    let key = key.ToString()
                    let value = dictionary.[key]
                    entries.Add(key, JsonValue.Serialize value)
                Object entries

            let valueType = value.GetType()

            if obj.ReferenceEquals(value, null) then
                Null
            else match trySerializePrimitive value with
                    | Null ->
                        if typeof<IDictionary>.IsAssignableFrom(valueType) then
                            serializeDictionary (value :?> IDictionary)
                        elif typeof<IEnumerable>.IsAssignableFrom(valueType) then
                            serializeCollection (value :?> IEnumerable)
                        else
                            let props = valueType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                            let dict = new Dictionary<string, JsonValue>(StringComparer.OrdinalIgnoreCase)
                            for prop in props do
                                if prop.CanRead then
                                    let propValue = prop.GetValue(value)
                                    let serializedValue = JsonValue.Serialize propValue
                                    dict.Add(prop.Name, serializedValue)
                            Object dict
                    | value -> value

        static member SerializeWithType (value: obj) =
            let json = JsonValue.Serialize value
            match json, value.GetType() |> typeToName with
            | Object _, Some t -> json["$type"] <- t
            | other -> ()
            json

        static member DeserializeDynamic (json: JsonValue) : obj =
            let toDecimal (input: string) =
                match Decimal.TryParse (input, CultureInfo.InvariantCulture) with
                | true, dec -> Some dec
                | _ -> None

            let toInt64 (input: string) =
                match Int64.TryParse (input, CultureInfo.InvariantCulture) with
                | true, i64 -> Some i64
                | _ -> None

            let toBoolean (input: string) =
                match input.ToLower() with
                | "true" -> Some true
                | "false" -> Some false
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
                | JsonValue.Number n -> if Decimal.IsInteger n && abs n < decimal Int64.MaxValue then Int64.CreateSaturating n else n
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
                let constr = targetType.GetConstructor(BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic, [||])
                if constr <> null then
                    constr.Invoke([||])
                else
                    System.Runtime.Serialization.FormatterServices.GetSafeUninitializedObject(targetType)            

            let isCollectionType typ =
                typeof<IList<_>>.IsAssignableFrom(typ) || typ.IsArray

            let tryDeserializePrimitive (targetType: Type) (value: JsonValue) : obj =
                match targetType, value with
                | t, JsonValue.Boolean b when t = typeof<bool> -> b
                | t, JsonValue.String s when t = typeof<string> -> s
                | t, JsonValue.String s when t = typeof<Type> -> s |> nameToType |> box
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
                | t, JsonValue.Number n when t = typeof<SqlId> -> (SqlId)(int64 n) |> box

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
                    else if typeof<IList<_>>.IsAssignableFrom targetType then
                        let icollectionType = targetType.GetInterfaces() |> Array.find(fun i -> i.IsGenericType && i.IsConstructedGenericType && i.Name.StartsWith "ICollection") 
                        let elementType = icollectionType.GenericTypeArguments.[0]
                        let listInstance = Activator.CreateInstance(targetType) :?> IList
                        for item in items do
                            listInstance.Add(JsonValue.Deserialize elementType item) |> ignore
                        listInstance
                    else failwithf "Cannot deserialize a list to type %A" targetType
                | _ -> failwith "Expected JSON array for collection deserialization."

            let deserializeTuple (targetType: Type) (value: JsonValue) =
                match value with
                | JsonValue.List items ->
                    let elementsTypes = targetType.GetGenericArguments()
                    let values = items |> Seq.zip elementsTypes |> Seq.map (fun (typ, item) -> JsonValue.Deserialize typ item) |> Seq.toArray
                    Dynamic.InvokeConstructor(targetType, values)
                | _ -> failwith "Expected JSON array for tuple deserialization."

            let deserializeObject (targetType: Type) (jsonObj: JsonValue) =
                let targetType =
                    let typeProp = match jsonObj.TryGetProperty "$type" with true, x -> x.ToObject<string>() |> nameToType | false, _ -> null
                    match targetType.IsSealed, typeProp with
                    | true, _ -> targetType
                    | _, null -> targetType
                    | false, (jsonType) when (jsonType).IsAssignableTo targetType -> jsonType
                    | _ -> targetType

                let instance = createInstance targetType
                match jsonObj with
                | JsonValue.Object properties ->
                    let propertiesInfo = targetType.GetProperties()
                    for prop in propertiesInfo do
                        if prop.CanWrite then
                            let propName = prop.Name
                            match properties.TryGetValue(propName) with
                            | true, value ->
                                let propValue = JsonValue.Deserialize prop.PropertyType value
                                prop.SetValue(instance, propValue, null)
                            | false, _ -> ()
                | _ -> failwith "Expected JSON object for object deserialization."
                instance

            if targetType = typeof<obj> then 
                JsonValue.DeserializeDynamic json
            else
            match json with
            | JsonValue.Null -> null
            | json ->
                match tryDeserializePrimitive targetType json with
                | null ->
                    match json with
                    | JsonValue.List _ when typeof<System.Runtime.CompilerServices.ITuple>.IsAssignableFrom targetType ->
                        deserializeTuple targetType json
                    | JsonValue.List _ when isCollectionType targetType ->
                        deserializeList targetType json
                    | JsonValue.Object _ when targetType.IsClass ->
                        deserializeObject targetType json
                    | _ -> failwithf "JSON value cannot be deserialized into type %A" targetType
                | primitive -> primitive


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

        member this.TryGetProperty(name: string) : bool * JsonValue =
            match this with
            | Object o -> o.TryGetValue(name)
            | List list -> 
                let index = Int32.Parse (name, CultureInfo.InvariantCulture)
                true, list.[index]
            | other -> failwithf "Cannot index %s" (other.ToString())

        member this.GetProperty(name: string) =
            match this.TryGetProperty(name) with
            | true, value -> value
            | false, _ -> failwith "Property not found"

        member this.Item
            with get(name: string) =
                match this.TryGetProperty(name) with
                | true, value -> value
                | false, _ -> failwith "Property not found"

            and set(name: string) (value: obj) =
                this.SetProperty (name, JsonValue.Serialize value)

        member this.JS
            with get(name: string) =            
                let rec toObj (json: JsonValue) : obj =
                    match json with
                    | Null -> null
                    | Boolean b -> b
                    | String s -> s
                    | Number n -> n
                    | List arr -> arr
                    | Object o -> o

                match this.TryGetProperty(name) with
                | true, value -> toObj value
                | false, _ -> failwith "Property not found"

            and set(name: string) (value: obj) =
                this.SetProperty (name, JsonValue.Serialize value)


        member this.ToJsonString() =
            let rec jsonize value (sb: StringBuilder) =
                match value with
                | Null -> sb.Append "null" |> ignore
                | Boolean b -> sb.Append (if b then "true" else "false") |> ignore
                | String s -> 
                    sb.Append '"' |> ignore
                    sb.Append s |> ignore
                    sb.Append '"' |> ignore
                | Number n -> 
                    if Decimal.IsInteger n then 
                        sb.Append (Int128.CreateSaturating(n).ToString(CultureInfo.InvariantCulture)) |> ignore
                    else 
                        sb.Append (sprintf "%f" n) |> ignore
                | List arr ->
                    sb.Append '[' |> ignore
                    let items = arr.Count
                    for i, item in arr |> Seq.indexed do
                        jsonize item sb
                        if i < items - 1 then sb.Append ", " |> ignore
                    sb.Append ']' |> ignore

                | Object obj ->
                    sb.Append '{' |> ignore
                    let items = obj.Count
                    for i, kvp in obj |> Seq.indexed do
                        sb.Append '"' |> ignore
                        sb.Append kvp.Key |> ignore
                        sb.Append '"' |> ignore
                        sb.Append ": " |> ignore
                        jsonize kvp.Value sb
                        if i < items - 1 then sb.Append ", " |> ignore
                    sb.Append '}' |> ignore
            let sb = new StringBuilder()
            jsonize this sb
            sb.ToString()


        member this.Eq (other: obj) =
            this = JsonValue.Parse $"{other}"

        static member New() = JsonValue.Object(Dictionary(StringComparer.OrdinalIgnoreCase))

        static member Parse (jsonString: string) =
            let parse (tokens: Token list) : JsonValue =
                let rec parseTokens tokens =
                    match tokens with
                    | OpenBrace :: rest ->
                        let obj, remainingTokens = parseObject rest
                        Object obj, remainingTokens
                    | OpenBracket :: rest ->
                        let list, remainingTokens = parseArray rest
                        List list, remainingTokens
                    | StringToken str :: rest ->
                        String str, rest
                    | NumberToken num :: rest ->
                        Number num, rest
                    | BoolenToken b :: rest ->
                        JsonValue.Boolean b, rest
                    | NullToken :: rest ->
                        Null, rest
                    | _ -> failwith "Unexpected token during parsing"
        
                and parseObject tokens =
                    let dict = new Dictionary<string, JsonValue>(StringComparer.OrdinalIgnoreCase)
                    let rec parseMembers tokens =
                        match tokens with
                        | CloseBrace :: rest -> dict, rest
                        | StringToken key :: Colon :: valueTokens ->
                            let value, postValueTokens = parseTokens valueTokens
                            dict.Add(key.ToLower(), value)
                            parseMembers (if postValueTokens.Head = Comma then postValueTokens.Tail else postValueTokens)
                        | _ -> failwith "Invalid object syntax"
                    parseMembers tokens
        
                and parseArray tokens =
                    let items = new List<JsonValue>()
                    let rec parseElements tokens =
                        match tokens with
                        | CloseBracket :: rest -> items, rest
                        | _ ->
                            let item, remainingTokens = parseTokens tokens
                            items.Add(item)
                            parseElements (if remainingTokens.Head = Comma then remainingTokens.Tail else remainingTokens)
                    parseElements tokens
        
                fst (parseTokens tokens)

            let tokens = tokenize jsonString
            parse tokens

        override this.ToString() = this.ToJsonString()
