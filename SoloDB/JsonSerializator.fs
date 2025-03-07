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


[<Struct>]
type internal Tokenizer =
    val mutable input: string
    val mutable index: int
    val private sb: StringBuilder

    new(source: string) = { input = source; index = 0; sb = System.Text.StringBuilder() }
    

    member this.ReadNext() : Token =
        let isDigit c = Char.IsDigit c || c = '-'
        let isInitialIdentifierChar c = Char.IsLetter c || c = '_'
        let isIdentifierChar c = isInitialIdentifierChar c || Char.IsDigit c

        RuntimeHelpers.EnsureSufficientExecutionStack()

        let input = this.input

        if this.index < input.Length then
            let currentChar = input.[this.index]
            match currentChar with
            | '{' -> 
                this.index <- this.index + 1
                OpenBrace
            | '}' -> 
                this.index <- this.index + 1
                CloseBrace
            | '[' -> 
                this.index <- this.index + 1
                OpenBracket
            | ']' -> 
                this.index <- this.index + 1
                CloseBracket
            | ',' -> 
                this.index <- this.index + 1
                Comma
            | ':' -> 
                this.index <- this.index + 1
                Colon
            | '"' | '\'' as quote ->
                let sb = this.sb.Clear()
                let mutable i = this.index + 1
                while i < input.Length && input.[i] <> quote do
                    if input.[i] = '\\' then
                        i <- i + 1
                        match this.input.[i] with
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
                this.index <- i + 1
                StringToken(sb.ToString())
            | c when isDigit c ->
                let sb = this.sb.Clear()
                let mutable i = this.index
                while i < input.Length && (isDigit input.[i] || input.[i] = '.' || input.[i] = 'e' || input.[i] = 'E' || input.[i] = '+') do
                    sb.Append(input.[i]) |> ignore
                    i <- i + 1
                this.index <- i
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
                let sb = this.sb.Clear()
                let mutable i = this.index
                while i < input.Length && isIdentifierChar input.[i] do
                    sb.Append(input.[i]) |> ignore
                    i <- i + 1
                let txt = sb.ToString()
                this.index <- i
                match txt with
                | "null"
                | "NULL" -> NullToken
                | "true" -> BooleanToken(true)
                | "false" -> BooleanToken(false)
                | other -> StringToken other // Non standard, but used.
            | c when Char.IsWhiteSpace(c) ->
                this.index <- this.index + 1
                this.ReadNext()
            | _ -> failwith "Invalid JSON format"
        else
        EndOfInput

    /// Peek at the next token without consuming it
    member this.Peek() : Token =
        let savedIndex = this.index
        try this.ReadNext()
        finally this.index <- savedIndex

    member inline private this.ParseTokensOr(f) =
        let current = this.ReadNext()
        match current with
        | OpenBrace ->
            let obj = this.ParseObject()
            Object obj |> Some
        | OpenBracket ->
            let list = this.ParseArray()
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
    
    member private this.ParseTokens() =
        this.ParseTokensOr(fun t -> failwithf "Malformed JSON at: %A" t) |> _.Value
    
    member private this.ParseMembers(dict: Dictionary<string, JsonValue>) =
        let current = this.ReadNext()
        match current with
        | CloseBrace -> dict
        | StringToken key ->
            let colon = this.ReadNext()
            if colon <> Colon then failwithf "Malformed json."
    
            let value = this.Parse()
            dict.[key] <- value
            this.ParseMembers(dict)
        | Comma -> this.ParseMembers(dict)
        | _ -> failwith "Invalid object syntax"
        
    member private this.ParseObject() =
        let dict = Dictionary<string, JsonValue>()
        this.ParseMembers(dict)
        
    member private this.ParseElements(items: List<JsonValue>) =
        let item = this.ParseTokensOr(fun t -> 
            match t with 
            | CloseBracket -> None 
            | other -> failwithf "Invalid list token: %A" other)
        
        match item with
        | None -> items
        | Some item ->
            items.Add(item)
    
            let current = this.ReadNext()
            match current with
            | CloseBracket -> items
            | Comma -> this.ParseElements(items)
            | _ -> failwithf "Malformed json."
            
    member private this.ParseArray() =
        let items = new List<JsonValue>()
        this.ParseElements(items)
        
    member this.Parse() : JsonValue =
        this.ParseTokens()

and JsonValue =
    | Null
    | Boolean of bool
    | String of string
    | Number of decimal
    | List of IList<JsonValue>
    | Object of IDictionary<string, JsonValue>

    static member Serialize<'T>(t: 'T) : JsonValue =
        JsonSerializerImpl<'T>.Serialize t

    static member SerializeWithType<'T>(t: 'T) : JsonValue =
        JsonSerializerImpl<'T>.SerializeWithType t

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
        JsonImpl.DeserializeByType targetType this

    member this.ToObject<'T>() : 'T =
        JsonDeserializerImpl<'T>.Deserialize this

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
            let index = Int32.Parse (name, NumberStyles.Integer, CultureInfo.InvariantCulture)
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

    static member private Jsonize value (sb: StringBuilder) =
        let inline write (x: string) =
            sb.Append x |> ignore

        let inline writeChar (x: char) =
            sb.Append x |> ignore

        let inline writeNumber (n: Decimal) =
            if Decimal.IsInteger n then 
                sb.AppendFormat(CultureInfo.InvariantCulture, "{0:F0}", n) |> ignore
            else 
                sb.AppendFormat(CultureInfo.InvariantCulture, "{0:0.############################}", n) |> ignore

        match value with
        | Null -> write "null"
        | Boolean b -> write (if b then "true" else "false")
        | String s -> 
            writeChar '\"'
            let escapedString = HttpUtility.JavaScriptStringEncode(s, false)
            write escapedString
            writeChar '\"'
        | Number n -> 
            writeNumber n
        | List arr ->
            writeChar '['
            let items = arr.Count
            for i in 0..(items - 1) do
                let item = arr.[i]
                JsonValue.Jsonize item sb
                if i < items - 1 then writeChar ','
            writeChar ']'

        | Object obj ->
            writeChar '{'
            let items = obj.Count
            let mutable i = 0
            for kvp in obj do
                writeChar '\"'
                write kvp.Key
                writeChar '\"'
                writeChar ':'
                JsonValue.Jsonize kvp.Value sb
                if i < items - 1 then writeChar ','
                i <- i + 1
            writeChar '}'

    member this.ToJsonString() =            
        let sb = new StringBuilder(128)
        JsonValue.Jsonize this sb
        sb.ToString()

    member this.Eq (other: obj) =
        this = JsonValue.Parse $"{other}"

    static member New() = JsonValue.Object(Dictionary())

    static member New(values: KeyValuePair<string, obj> seq) = 
        let json = JsonValue.New()
        for KeyValue(key ,v) in values do
            json.[key] <- JsonSerializerImpl<obj>.Serialize v
        json
        
    static member Parse (jsonString: string) =
        let tokenizer = Tokenizer(jsonString)
        let json = tokenizer.Parse()
        json

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
    
            json.[key] <- JsonSerializerImpl<obj>.Serialize v
        json

    override this.ToString() = this.ToJsonString()

    static member op_Implicit<'T>(o: 'T) : JsonValue =
        JsonSerializerImpl<'T>.Serialize o

    static member op_Implicit(o: obj) : JsonValue =
        JsonValue.op_Implicit<obj> o

    static member op_Implicit(o: int8) : JsonValue =
        JsonValue.op_Implicit<int8> o
    static member op_Implicit(o: int16) : JsonValue =
        JsonValue.op_Implicit<int16> o
    static member op_Implicit(o: int32) : JsonValue =
        JsonValue.op_Implicit<int32> o
    static member op_Implicit(o: int64) : JsonValue =
        JsonValue.op_Implicit<int64> o
    static member op_Implicit(o: uint8) : JsonValue =
        JsonValue.op_Implicit<uint8> o
    static member op_Implicit(o: uint16) : JsonValue =
        JsonValue.op_Implicit<uint16> o
    static member op_Implicit(o: uint32) : JsonValue =
        JsonValue.op_Implicit<uint32> o
    static member op_Implicit(o: uint64) : JsonValue =
        JsonValue.op_Implicit<uint64> o
    static member op_Implicit(o: float32) : JsonValue =
        JsonValue.op_Implicit<float32> o
    static member op_Implicit(o: float) : JsonValue =
        JsonValue.op_Implicit<float> o
    static member op_Implicit(o: decimal) : JsonValue =
        JsonValue.op_Implicit<decimal> o
    static member op_Implicit(o: bool) : JsonValue =
        JsonValue.op_Implicit<bool> o
    static member op_Implicit(o: char) : JsonValue =
        JsonValue.op_Implicit<char> o
    static member op_Implicit(o: string) : JsonValue =
        JsonValue.op_Implicit<string> o
    static member op_Implicit(o: byte array) : JsonValue =
        JsonValue.op_Implicit<byte array> o
    static member op_Implicit(o: Guid) : JsonValue =
        JsonValue.op_Implicit<Guid> o
    static member op_Implicit(o: DateTime) : JsonValue =
        JsonValue.op_Implicit<DateTime> o
    static member op_Implicit(o: DateTimeOffset) : JsonValue =
        JsonValue.op_Implicit<DateTimeOffset> o
    static member op_Implicit(o: TimeSpan) : JsonValue =
        JsonValue.op_Implicit<TimeSpan> o
    static member op_Implicit(o: DateOnly) : JsonValue =
        JsonValue.op_Implicit<DateOnly> o
    static member op_Implicit(o: TimeOnly) : JsonValue =
        JsonValue.op_Implicit<TimeOnly> o
    static member op_Implicit(o: byte seq) : JsonValue =
        JsonValue.op_Implicit<byte seq> o

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

and private ConvertStringTo<'T> =
    static member val internal Convert =
        match typeof<'T> with
        | OfType string -> 
            (id: string -> string) :> obj :?> (string -> 'T)

        | OfType int8 -> (fun (x: string) -> int8 x) :> obj :?> (string -> 'T)
        | OfType int16 -> (fun (x: string) -> int16 x) :> obj :?> (string -> 'T)
        | OfType int32 -> (fun (x: string) -> int32 x) :> obj :?> (string -> 'T)
        | OfType int64 -> (fun (x: string) -> int64 x) :> obj :?> (string -> 'T)

        | OfType uint8 -> (fun (x: string) -> uint8 x) :> obj :?> (string -> 'T)
        | OfType uint16 -> (fun (x: string) -> uint16 x) :> obj :?> (string -> 'T)
        | OfType uint32 -> (fun (x: string) -> uint32 x) :> obj :?> (string -> 'T)
        | OfType uint64 -> (fun (x: string) -> uint64 x) :> obj :?> (string -> 'T)

        | OfType float32 -> (fun (x: string) -> float32 x) :> obj :?> (string -> 'T)
        | OfType float -> (fun (x: string) -> float x) :> obj :?> (string -> 'T)
        | OfType decimal -> (fun (x: string) -> decimal x) :> obj :?> (string -> 'T)

        | other -> failwithf "Cannot convert string to %s" other.FullName

and private JsonImpl =
    static member internal CreateJsonObj(d: IDictionary<string, JsonValue>) =
        JsonValue.Object d

    static member internal AsListOrFail(json: JsonValue) =
        match json with
        | List l -> l
        | other -> failwithf "Expected json object to be a list, instead it is a %A" other

    static member internal AsDictOrFail(json: JsonValue) =
        match json with
        | Object d -> d
        | other -> failwithf "Expected json object to be a object/dictionary, instead it is a %A" other

    static member inline internal TryGetTypeFromJson<'A> (jsonObj: JsonValue) =
        let targetType = typeof<'A>
        // Be careful, a potential attacker can put anything in this field.
        let unsafeTypeProp = match jsonObj.TryGetProperty "$type" with true, x -> x.ToObject<string>() |> nameToType | false, _ -> null
        match struct (targetType.IsSealed, unsafeTypeProp) with
        | struct (true, _) -> targetType
        | struct (_, null) -> targetType
        | struct (false, (jsonType)) when targetType.IsAssignableFrom jsonType -> jsonType
        | _ -> targetType

    
    static member internal IEnumerableSerialize<'T>(o: 'T seq) =
        let items = System.Collections.Generic.List<JsonValue>()
        for item in o do
            items.Add(JsonSerializerImpl<'T>.Serialize item)
        List items

    // For the direct interface of a type with a IEnumerable contructor
    static member internal IEnumerableDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            let result = new System.Collections.Generic.List<'T>(items.Count)
            for item in items do
                result.Add(JsonDeserializerImpl<'T>.Deserialize item)
            result :> IEnumerable<'T>
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    static member internal IListDeserialize<'T, 'L when 'L :> IList<'T>>(result: 'L) (jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            for item in items do
                result.Add(JsonDeserializerImpl<'T>.Deserialize item)
            result
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    static member internal FSharpListDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            [
                for item in items do
                    JsonDeserializerImpl<'T>.Deserialize item
            ]
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    static member internal ArrayDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            let arr = Array.init items.Count (fun i -> JsonDeserializerImpl<'T>.Deserialize items.[i])
            arr
        | other ->
            failwithf "Expected JsonValue.List but got %A" other
    
    static member internal SerializeDictionary<'key, 'value> (dictionary: obj) =
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

    static member internal DictionaryDeserialize<'TKey, 'TValue when 'TKey : equality>(jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            let result = new Dictionary<'TKey, 'TValue>(properties.Count)
            for KeyValue(key, value) in properties do
                let typedKey = ConvertStringTo<'TKey>.Convert key
                let typedValue = JsonDeserializerImpl<'TValue>.Deserialize value
                result.Add(typedKey, typedValue)
            result :> IDictionary<'TKey, 'TValue>
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other


    static member internal IReadOnlyDictionaryDeserialize<'TKey, 'TValue when 'TKey : equality>(jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            let dict = new Dictionary<'TKey, 'TValue>(properties.Count)
            for KeyValue(key, value) in properties do
                let typedKey = ConvertStringTo<'TKey>.Convert key

                let typedValue = JsonDeserializerImpl<'TValue>.Deserialize value
                dict.Add(typedKey, typedValue)
            dict :> IReadOnlyDictionary<'TKey, 'TValue>
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    static member internal NonGenericDictionaryDeserialize(jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            let result = new Hashtable(properties.Count)
            for KeyValue(key, value) in properties do
                let typedValue = JsonDeserializerImpl<obj>.Deserialize value
                result.Add(key, typedValue)
            result :> IDictionary
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    static member internal CustomDictionaryDeserialize<'TKey, 'TValue, 'TDict when 'TDict :> IDictionary<'TKey, 'TValue>> (result: 'TDict) (jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            for KeyValue(key, value) in properties do
                let typedKey = ConvertStringTo<'TKey>.Convert key
                let typedValue = JsonDeserializerImpl<'TValue>.Deserialize value
                result.Add(KeyValuePair<'TKey, 'TValue>(typedKey, typedValue))
            result
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    static member internal CustomNonGenericDictionaryDeserialize<'TDict when 'TDict :> IDictionary> (result: 'TDict) (jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            for KeyValue(key, value) in properties do
                let typedValue = JsonDeserializerImpl<obj>.Deserialize value
                result.Add(key, typedValue)
            result
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    static member val internal DeserializeByType =
        let cache = ConcurrentDictionary<Type, Func<JsonValue, obj>>()
        fun (t: Type) (v: JsonValue) ->
            let fn = cache.GetOrAdd(t, Func<Type, Func<JsonValue, obj>>(fun t ->
                let p = Expression.Parameter typeof<JsonValue>
                    
                let meth = 
                    typedefof<JsonDeserializerImpl<_>>
                        .MakeGenericType(t)
                        .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
                    
                let fn = 
                    Expression.Lambda<Func<JsonValue, obj>>(
                        Expression.Convert(Expression.Call(meth, p), typeof<obj>),
                        [|p|]
                    ).Compile(false)

                fn
                ))
            
            fn.Invoke v

    static member val internal SerializeByType =
        let cache = ConcurrentDictionary<Type, Func<obj, JsonValue>>()
        (fun (runtimeType: Type) (o: obj) ->
            let fn = 
                cache.GetOrAdd(runtimeType, Func<Type, Func<obj, JsonValue>>(fun (t: Type) ->
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

                    fn
                ))

            fn.Invoke o
        )

    static member private IsAllDigits(x: ReadOnlySpan<char>) =
        if x.Length = 0 then
            true
        else

        if not (Char.IsDigit x.[0]) then
            false
        else
            JsonImpl.IsAllDigits (x.Slice 1)

    static member internal IsParsableToNumber(x: ReadOnlySpan<char>) =
        if x.Length = 0 then
            false
        else

        if x.Length = 1 && not (Char.IsDigit x.[0]) then
            false
        else

        if not (Char.IsDigit x.[0] || x.[0] = '+' || x.[0] = '-') then
            false
        else

        JsonImpl.IsAllDigits (x.Slice 1)

    static member internal DeserializeDynamic (json: JsonValue) : obj =
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

and private JsonDeserializerImpl<'A> =
    static member internal DeserializeFunc(v: JsonValue) =
        JsonDeserializerImpl<'A>.Deserialize v

    static member val internal Deserialize: JsonValue -> 'A =
        let inline asNumberOrParseToNumber (x: JsonValue) (cast: decimal -> 'T) (parse: string -> 'T) : 'T =
            match x with
            | Number i -> cast i
            | String s -> parse s
            | other -> failwithf "Cannot convert %A into %s" other typeof<'T>.FullName

        match typeof<'A> with
        | t when t = typeof<obj> ->
            (fun (json: JsonValue) -> JsonImpl.DeserializeDynamic json) :> obj :?> (JsonValue -> 'A)

        | OfType char ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json char char) :> obj :?> (JsonValue -> 'A)

        | OfType int8 ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json int8 int8) :> obj :?> (JsonValue -> 'A)

        | OfType int16 ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json int16 int16) :> obj :?> (JsonValue -> 'A)

        | OfType int32 ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json int32 int32) :> obj :?> (JsonValue -> 'A)

        | OfType int64 ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json int64 int64) :> obj :?> (JsonValue -> 'A)

        | OfType uint8 ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json uint8 uint8) :> obj :?> (JsonValue -> 'A)

        | OfType uint16 ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json uint16 uint16) :> obj :?> (JsonValue -> 'A)

        | OfType uint32 ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json uint32 uint32) :> obj :?> (JsonValue -> 'A)

        | OfType uint64 ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json uint64 uint64) :> obj :?> (JsonValue -> 'A)

        | OfType float32 ->
            (fun (json: JsonValue) -> 
                match json with
                | Null -> nanf
                | json -> asNumberOrParseToNumber json float32 float32
            ) :> obj :?> (JsonValue -> 'A)

        | OfType float ->
            (fun (json: JsonValue) -> 
                match json with
                | Null -> nan
                | json -> asNumberOrParseToNumber json float float
            ) :> obj :?> (JsonValue -> 'A)

        | OfType decimal ->
            (fun (json: JsonValue) -> asNumberOrParseToNumber json id decimal) :> obj :?> (JsonValue -> 'A)

        | OfType bool ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.Boolean b -> b
                | JsonValue.Number n -> (if n = Decimal.Zero then false else true)
                | JsonValue.String s -> s = "true" || (JsonImpl.IsParsableToNumber (s.AsSpan()) && int64 s <> 0)
                | JsonValue.Null -> false
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
                
            ) :> obj :?> (JsonValue -> 'A)

        | OfType (id : byte array -> byte array) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> JsonHelper.JSONStringToByteArray s
                | JsonValue.Null -> null
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
                
            ) :> obj :?> (JsonValue -> 'A)

        | OfType (id : List<byte> -> List<byte>) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> ResizeArray<byte>(JsonHelper.JSONStringToByteArray s)
                | JsonValue.Null -> null
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
                
            ) :> obj :?> (JsonValue -> 'A)

        | OfType (id : byte list -> byte list) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> JsonHelper.JSONStringToByteArray s |> Array.toList
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
                
            ) :> obj :?> (JsonValue -> 'A)

        | t when t = typeof<byte seq> ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.Null -> null
                | JsonValue.String s -> JsonHelper.JSONStringToByteArray s :> byte seq
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
                
            ) :> obj :?> (JsonValue -> 'A)

        | OfType string ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> s
                | JsonValue.Number i -> string i
                | JsonValue.Null -> null
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
                
            ) :> obj :?> (JsonValue -> 'A)

        | OfType (id: Type -> Type) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> nameToType s
                | JsonValue.Null -> null
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
                
            ) :> obj :?> (JsonValue -> 'A)

        | OfType (id: Guid -> Guid) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> Guid.Parse s
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
                
            ) :> obj :?> (JsonValue -> 'A)

        | OfType (id: DateTimeOffset -> DateTimeOffset) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> DateTimeOffset.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> int64 |> DateTimeOffset.FromUnixTimeMilliseconds
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
            ) :> obj :?> (JsonValue -> 'A)
    
        | OfType (id: DateTime -> DateTime) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> DateTime.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> int64 |> DateTime.FromBinary
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
            ) :> obj :?> (JsonValue -> 'A)
    
        | OfType (id: DateOnly -> DateOnly) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> DateOnly.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> int |> DateOnly.FromDayNumber
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
            ) :> obj :?> (JsonValue -> 'A)
    
        | OfType (id: TimeOnly -> TimeOnly) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> TimeOnly.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> float |> TimeSpan.FromMilliseconds |> TimeOnly.FromTimeSpan
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
            ) :> obj :?> (JsonValue -> 'A)
    
        | OfType (id: TimeSpan -> TimeSpan) ->
            (fun (json: JsonValue) -> 
                match json with
                | JsonValue.String s -> TimeSpan.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> float |> TimeSpan.FromMilliseconds
                | other -> failwithf "Cannot convert %A into %s" other typeof<'A>.FullName
            ) :> obj :?> (JsonValue -> 'A)

        | OfType (id: JsonValue -> JsonValue) ->
            (fun (json: JsonValue) -> json) :> obj :?> (JsonValue -> 'A)

        | t when t.FullName = "SoloDatabase.MongoDB.BsonDocument" ->
            let p = Expression.Parameter typeof<JsonValue>
                        
            let fn = 
                Expression.Lambda<Func<JsonValue, 'A>>(
                    Expression.New(t.GetConstructor([|typeof<JsonValue>|]), [|p :> Expression|]),
                    [|p|]
                )

            let fn = fn.Compile(false)

            (fun (json: JsonValue) -> fn.Invoke json) :> obj :?> (JsonValue -> 'A)

        | t when t.Name = "FSharpOption`1" ->
            failwithf "FSharp option is not supported yet, use Nullable<>"

        | t when t.Name = "Nullable`1" ->
            let genericType = GenericTypeArgCache.Get t |> Array.head

            let nullValue = Activator.CreateInstance(t) :?> 'A

            let p = Expression.Parameter typeof<JsonValue>
            
            let meth = typedefof<JsonDeserializerImpl<_>>
                        .MakeGenericType(genericType)
                        .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)

            let notNullFn = 
                Expression.Lambda<Func<JsonValue, 'A>>(
                    Expression.New(t.GetConstructor([|genericType|]), [|Expression.Call(meth, [|p :> Expression|]) :> Expression|]),
                    [|p|]
                ).Compile(false)


            (fun (json: JsonValue) -> 
                match json with
                | Null -> nullValue
                | Object _o when json.Contains "HasValue" -> 
                    if json.["HasValue"].ToObject<bool>() then
                        notNullFn.Invoke (json.["Value"])
                    else
                        nullValue
                | other -> notNullFn.Invoke other
            ) :> obj :?> (JsonValue -> 'A)

        | t when t.IsEnum ->
            let underlyingType = Enum.GetUnderlyingType(t)
            let p = Expression.Parameter(typeof<JsonValue>)
            
            let meth = 
                typedefof<JsonDeserializerImpl<_>>
                    .MakeGenericType(underlyingType)
                    .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
            
            let convertExpr = Expression.Call(meth, p)
            let enumConvertExpr = Expression.Convert(convertExpr, t)
            
            let fn = 
                Expression.Lambda<Func<JsonValue, 'A>>(
                    enumConvertExpr,
                    [|p|]
                )

            let fn = fn.Compile(false)

            fn.Invoke

        | t when isTuple t ->
            let isValueTuple = typeof<ValueTuple>.IsAssignableFrom t || t.Name.StartsWith "ValueTuple`"
            let tupleItemTypes = GenericTypeArgCache.Get t
            let jsonParam = Expression.Parameter(typeof<JsonValue>)
            
            // Create expressions for each item in the tuple
            let itemExpressions = 
                tupleItemTypes 
                |> Array.mapi (fun i itemType ->
                    let itemJsonExpr = Expression.Call(
                        jsonParam,
                        typeof<JsonValue>.GetMethod("get_Item", [| typeof<string> |]),
                        [| Expression.Constant(string i) :> Expression |]
                    )
                    
                    let deserializeMethod = 
                        typedefof<JsonDeserializerImpl<_>>
                            .MakeGenericType(itemType)
                            .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
                    
                    Expression.Call(deserializeMethod, itemJsonExpr)
                )
                |> Array.map(fun e -> e :> Expression)
            
            // Create tuple constructor or value tuple
            let tupleCreateExpr =
                if isValueTuple then
                    // For ValueTuple, create a new struct with field initializers
                    let ctor = t.GetConstructor(tupleItemTypes)
                    Expression.New(ctor, itemExpressions) :> Expression
                else
                    // For reference Tuple, use the static Tuple.Create method
                    let tupleCreateMethod = 
                        match tupleItemTypes.Length with
                        | n when n >= 1 && n <= 8 ->
                            typeof<Tuple>.GetMethods()
                            |> Array.find (fun m -> 
                                m.Name = "Create" && 
                                (GenericMethodArgCache.Get m).Length = tupleItemTypes.Length)
                            |> fun m -> m.MakeGenericMethod(tupleItemTypes)
                        | _ -> 
                            // Handle TupleRest for tuples with more than 8 items
                            failwith "Tuples with more than 8 items not implemented"
                    
                    Expression.Call(tupleCreateMethod, itemExpressions)
            
            // Compile the lambda
            let fn = 
                Expression.Lambda<Func<JsonValue, 'A>>(
                    Expression.Convert(tupleCreateExpr, typeof<'A>),
                    [| jsonParam |]
                )

            let fn = fn.Compile(false)
            
            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | _ -> fn.Invoke json
            )
        
        // Generic IDictionary<K,V> interface or abstract type - use Dictionary<K,V>
        | t when (JsonHelper.implementsGeneric typedefof<IDictionary<_,_>> t) && (t.IsInterface || t.IsAbstract) ->
            let iface = t.GetInterface("IDictionary`2") |> Option.ofObj |> Option.defaultValue t
            let keyType, valueType = 
                let args = GenericTypeArgCache.Get iface
                args.[0], args.[1]
                
            let jsonParam = Expression.Parameter(typeof<JsonValue>)
            
            let meth = typeof<JsonImpl>
                        .GetMethod(nameof JsonImpl.DictionaryDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod([|keyType; valueType|])
                        
            let callExpr = Expression.Call(meth, [|jsonParam :> Expression|])
            
            let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                Expression.Convert(callExpr, t),
                [|jsonParam|]
            )
            
            let fn = lambda.Compile(false)

            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | _ -> fn.Invoke json
            )

        // IReadOnlyDictionary<K,V> implementation
        | t when JsonHelper.implementsGeneric typedefof<IReadOnlyDictionary<_,_>> t && not (JsonHelper.implementsGeneric typedefof<IDictionary<_,_>> t) ->
            let iface = t.GetInterface("IReadOnlyDictionary`2") |> Option.ofObj |> Option.defaultValue t
            let keyType, valueType = 
                let args = GenericTypeArgCache.Get iface
                args.[0], args.[1]
                
            let jsonParam = Expression.Parameter(typeof<JsonValue>)
            
            let meth = typeof<JsonImpl>
                        .GetMethod(nameof JsonImpl.IReadOnlyDictionaryDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod([|keyType; valueType|])
                        
            let callExpr = Expression.Call(meth, [|jsonParam :> Expression|])
            
            let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                Expression.Convert(callExpr, t),
                [|jsonParam|]
            )
            
            let fn = lambda.Compile(false)

            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | _ -> fn.Invoke json
            )
        
        // Concrete implementation of IDictionary<K,V> - use custom deserializer
        | t when JsonHelper.implementsGeneric typedefof<IDictionary<_,_>> t ->
            let iface = t.GetInterface("IDictionary`2") |> Option.ofObj |> Option.defaultValue t
            let keyType, valueType = 
                let args = GenericTypeArgCache.Get iface
                args.[0], args.[1]
                
            let jsonParam = Expression.Parameter(typeof<JsonValue>)
            
            // Concrete dictionary type with parameterless constructor
            let ctorInfo = t.GetConstructor([||])
            if ctorInfo = null then
                failwithf "Type %s must have a parameterless constructor" t.Name
                
            let meth = typeof<JsonImpl>
                        .GetMethod(nameof JsonImpl.CustomDictionaryDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod([|keyType; valueType; t|])
                        
            let instanceExpr = Expression.New(ctorInfo)
            let callExpr = Expression.Call(meth, [|instanceExpr :> Expression; jsonParam|])
            
            let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                callExpr,
                [|jsonParam|]
            )
            
            let fn = lambda.Compile(false)

            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | _ -> fn.Invoke json
            )

        // Non-generic IDictionary without generic parameters
        | t when typeof<IDictionary>.IsAssignableFrom(t) ->
            let jsonParam = Expression.Parameter(typeof<JsonValue>)
            
            if t.IsInterface || t.IsAbstract then
                // Use the standard non-generic deserializer for interfaces
                let meth = typeof<JsonImpl>.GetMethod(nameof JsonImpl.NonGenericDictionaryDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                let callExpr = Expression.Call(meth, [|jsonParam :> Expression|])
                
                let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                    Expression.Convert(callExpr, t),
                    [|jsonParam|]
                )
                
                let fn = lambda.Compile(false)
                (fun (json: JsonValue) ->
                    match json with
                    | JsonValue.Null -> Unchecked.defaultof<'A>
                    | _ -> fn.Invoke json
                )
            else
                // Concrete non-generic dictionary type with parameterless constructor
                let ctorInfo = t.GetConstructor([||])
                if ctorInfo = null then
                    failwithf "Type %s must have a parameterless constructor" t.Name
                    
                let meth = typeof<JsonImpl>.GetMethod(nameof JsonImpl.CustomNonGenericDictionaryDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                            .MakeGenericMethod([|t|])
                            
                let instanceExpr = Expression.New(ctorInfo)
                let callExpr = Expression.Call(meth, [|instanceExpr :> Expression; jsonParam|])
                
                let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                    Expression.Convert(callExpr, t),
                    [|jsonParam|]
                )
                
                let fn = lambda.Compile(false)
                (fun (json: JsonValue) ->
                    match json with
                    | JsonValue.Null -> Unchecked.defaultof<'A>
                    | _ -> fn.Invoke json
                )

        // IGrouping<K,E> implementation
        | t when JsonHelper.implementsGeneric typedefof<IGrouping<_,_>> t ->
            let iface = t.GetInterface("IGrouping`2") |> Option.ofObj |> Option.defaultValue t
            let keyType, elemType = 
                let args = GenericTypeArgCache.Get iface
                args.[0], args.[1]
            
            let jsonParam = Expression.Parameter(typeof<JsonValue>)
            
            // Extract the Key property
            let keyJsonExpr = Expression.Call(
                jsonParam,
                typeof<JsonValue>.GetMethod("get_Item", [| typeof<string> |]),
                [| Expression.Constant("Key") :> Expression |]
            )
            
            // Deserialize the key using appropriate deserializer
            let keyDeserializerMethod = 
                typedefof<JsonDeserializerImpl<_>>
                    .MakeGenericType(keyType)
                    .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
            
            let keyExpr = Expression.Call(keyDeserializerMethod, keyJsonExpr)
            
            // Extract the Items property
            let itemsJsonExpr = Expression.Call(
                jsonParam,
                typeof<JsonValue>.GetMethod("get_Item", [| typeof<string> |]),
                [| Expression.Constant("Items") :> Expression |]
            )
            
            // Create a temporary list to hold items
            let listType = typedefof<System.Collections.Generic.List<_>>.MakeGenericType(elemType)
            let listVar = Expression.Variable(listType, "itemsList")
            

            let toListMeth = typeof<JsonImpl>.GetMethod(nameof JsonImpl.AsListOrFail, BindingFlags.NonPublic ||| BindingFlags.Static)
            // Get the JsonValue.List property and items.Count
            let listItemsExpr = Expression.Call(toListMeth, [|itemsJsonExpr :> Expression|])
            let listItemsVar = Expression.Variable(listItemsExpr.Type, "listItemsVar")
            let countExpr = Expression.Property(listItemsVar, typeof<ICollection<JsonValue>>.GetProperty("Count"))
            
            // Create a new list with capacity
            let listCtor = listType.GetConstructor([| typeof<int> |])
            let newListExpr = Expression.New(listCtor, [| countExpr :> Expression |])
            let assignListExpr = Expression.Assign(listVar, newListExpr)
            
            // Create loop through items
            let indexVar = Expression.Variable(typeof<int>, "i")
            let initIndexExpr = Expression.Assign(indexVar, Expression.Constant(0))
            
            let loopLabel = Expression.Label("LoopItems")
            let breakLabel = Expression.Label("BreakLoop")
            
            // Loop condition: i < items.Count
            let loopCondition = Expression.LessThan(indexVar, countExpr)
            let breakExpr = Expression.IfThen(
                Expression.Not(loopCondition),
                Expression.Break(breakLabel)
            )
            
            // Get current item: items[i]
            let getItemExpr = Expression.Property(
                listItemsVar,
                listItemsVar.Type.GetProperty("Item"),
                [| indexVar :> Expression |]
            )
            
            // Deserialize item
            let elemDeserializerMethod = 
                typedefof<JsonDeserializerImpl<_>>
                    .MakeGenericType(elemType)
                    .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
            
            let deserializedItemExpr = Expression.Call(elemDeserializerMethod, getItemExpr)
            
            // Add item to list
            let addMethod = listType.GetMethod("Add", [| elemType |])
            let addItemExpr = Expression.Call(listVar, addMethod, [| deserializedItemExpr :> Expression |])
            
            // Increment index
            let incrementExpr = Expression.AddAssign(indexVar, Expression.Constant(1))
            
            // Loop body
            let loopBodyExpr = Expression.Block(
                Expression.Label(loopLabel),
                breakExpr,
                addItemExpr,
                incrementExpr,
                Expression.Goto(loopLabel)
            )
            
            // Convert list to array
            let toArrayMethod = listType.GetMethod("ToArray")
            let itemsArrayExpr = Expression.Call(listVar, toArrayMethod)
            
            // Create new Grouping instance
            let groupingType = typedefof<Grouping<_,_>>.MakeGenericType([| keyType; elemType |])
            let ctor = groupingType.GetConstructor([| keyType; elemType.MakeArrayType() |])
            let newGroupingExpr = Expression.New(ctor, [| keyExpr :> Expression; itemsArrayExpr |])
            
            // Create block containing all expressions
            let blockExpr = Expression.Block(
                [| listVar; indexVar; listItemsVar |],
                Expression.Assign(listItemsVar, listItemsExpr),
                assignListExpr,
                initIndexExpr,
                loopBodyExpr,
                Expression.Label(breakLabel),
                Expression.Convert(newGroupingExpr, typeof<'A>)
            )
            
            // Compile to lambda
            let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                blockExpr,
                [| jsonParam |]
            )
            
            let fn = lambda.Compile(false)
            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | _ -> fn.Invoke json
            )


        | t when JsonHelper.implementsGeneric typedefof<IEnumerable<_>> t && t <> typeof<string> ->

            let standardStyleJsonFn =
                let elemType = GenericTypeArgCache.Get (t.GetInterface("IEnumerable`1") |> Option.ofObj |> Option.defaultValue t) |> Array.head
                let jsonParam = Expression.Parameter(typeof<JsonValue>)
            
                // Choose appropriate method based on type
                let (methodName, instanceExpr) = 
                    if t.IsArray then
                        // For arrays use ArrayDeserialize
                        (nameof JsonImpl.ArrayDeserialize, null)
                    // Check for F# list type
                    elif t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<list<_>> then
                        // For F# lists
                        (nameof JsonImpl.FSharpListDeserialize, null)
                    elif typeof<IList>.IsAssignableFrom(t) && t.IsGenericType && (GenericTypeArgCache.Get t).Length = 1 then
                        // For IList types that need an instance (like List<T>)
                        if t.GetConstructor([||]) <> null then
                            // If type has parameterless constructor, create instance for IListDeserialize
                            let instance = Expression.New(t)
                            (nameof JsonImpl.IListDeserialize, instance)
                        else
                            // If no parameterless constructor, fall back to IEnumerableDeserialize
                            (nameof JsonImpl.IEnumerableDeserialize, null)
                    else
                        // For other IEnumerable types
                        (nameof JsonImpl.IEnumerableDeserialize, null)
            
                let meth = 
                    if methodName = nameof JsonImpl.IListDeserialize then
                        typeof<JsonImpl>
                            .GetMethod(methodName, BindingFlags.NonPublic ||| BindingFlags.Static)
                            .MakeGenericMethod([|elemType; t|])
                    else
                        typeof<JsonImpl>
                            .GetMethod(methodName, BindingFlags.NonPublic ||| BindingFlags.Static)
                            .MakeGenericMethod(elemType)
            
                let callExpr = 
                    if instanceExpr <> null then
                        // For IListDeserialize which takes an instance parameter
                        Expression.Call(meth, [|instanceExpr :> Expression; jsonParam|])
                    else
                        // For methods that don't need an instance
                        Expression.Call(meth, [|jsonParam :> Expression|])
            
                let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                    Expression.Convert(callExpr, t),
                    [|jsonParam|]
                )
            
                let fn = lambda.Compile(false)
                fn.Invoke

            let newtownsoftArrayFn =
                if not (JsonHelper.isSupportedNewtownsoftArrayType t) then
                    fun (_j) -> failwithf "Type not supported for Newtownsoft deserialization: %s" t.FullName
                else
                    fun (j: JsonValue) -> JsonDeserializerImpl<'A>.Deserialize j.["$values"]

            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | List _l -> standardStyleJsonFn json
                | Object _o when json.Contains "$type" && json.Contains "$values" -> newtownsoftArrayFn json
                | other -> failwithf "Expected JsonValue.List or Object{$type, $values} for array deserialization but got %A" other
            )

        | t when FSharpType.IsUnion t ->
            let unionCases = FSharpType.GetUnionCases t
            let constructorMethods = 
                t.GetMethods(BindingFlags.Public ||| BindingFlags.Static) 
                |> Array.choose(
                    fun m -> 
                        match m.GetCustomAttributes<CompilationMappingAttribute>() |> Seq.tryFind(fun a -> a.SourceConstructFlags &&& SourceConstructFlags.UnionCase <> enum 0) with
                        | Some x -> Some (m, x.SequenceNumber)
                        | None -> None)

            let jsonParam = Expression.Parameter typeof<JsonValue>
            let getItem = typeof<JsonValue>.GetMethod("get_Item", [| typeof<string> |])
            let contains = typeof<JsonValue>.GetMethod("Contains", [| typeof<string> |])
            let toObject = typeof<JsonValue>.GetMethod("ToObject", [||])

            let deserializeForCase (case: UnionCaseInfo) =
                let m, _tag = constructorMethods |> Array.find(fun (m, tag) -> tag = case.Tag)
                
                let parameters =
                    case.GetFields()
                    |> Array.map(fun p -> 
                        let pName = [|Expression.Constant(p.Name) :> Expression|]
                        Expression.Condition(
                            Expression.Call(jsonParam, contains, pName), 
                            Expression.Call(
                                Expression.Call(jsonParam, getItem, pName),
                                toObject.MakeGenericMethod(p.PropertyType)
                                ),

                            if p.PropertyType.IsValueType then
                                Expression.Default(p.PropertyType) :> Expression
                            else
                                Expression.Constant(null, p.PropertyType) :> Expression
                            )
                        )

                        
                    |> Array.map(fun x -> x :> Expression)

                Expression.Call(m, parameters)


            let switch = 
                Expression.Switch(
                    Expression.Call(
                        Expression.Call(
                            jsonParam, 
                            getItem,
                            [| Expression.Constant("Tag") :> Expression |]),
                        toObject.MakeGenericMethod(typeof<int>)
                    ),
                    Expression.Throw(Expression.Constant(sprintf "Invalid Tag for type: %s" t.FullName |> exn), t),
                    [|
                        for case in unionCases do
                            Expression.SwitchCase(
                                (deserializeForCase case),
                                [|Expression.Constant(case.Tag) :> Expression|]
                            )
                   
                    |]
                )


            let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                    switch,
                    [|jsonParam|]
                )

            let fn = lambda.Compile(false)

            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | _ -> fn.Invoke json
            )

        // F# Record Types or has a contructor with all the properties types in order.
        | t when FSharpType.IsRecord t || (let propertiesType = t.GetProperties() |> Array.map(_.PropertyType) in t.GetConstructors() |> Array.exists(fun c -> c.GetParameters() |> Array.map(_.ParameterType) = propertiesType)) ->
            let jsonParam = Expression.Parameter(typeof<JsonValue>)
            
            let isFSharpRecord = FSharpType.IsRecord t

            // Prepare variables
            let objVar = Expression.Variable(typeof<JsonValue>)
            let propsVar = Expression.Variable(typeof<IDictionary<string, JsonValue>>)
            
            // Get record constructor - F# records have a single constructor with all fields
            let recordFields = 
                if isFSharpRecord then
                    FSharpType.GetRecordFields t
                else 
                    t.GetProperties()

            let recordFieldsType = recordFields |> Array.map(_.PropertyType)
            let ctor = t.GetConstructors() |> Array.find(fun c -> c.GetParameters() |> Array.map(_.ParameterType) = recordFieldsType)
            

            // Create parameter expressions for constructor
            let parameterExprs = recordFields |> Array.map (fun param ->
                let propNameExpr = Expression.Constant(param.Name)
                
                // Check if property exists
                let hasPropertyExpr = Expression.Call(
                    propsVar,
                    typeof<IDictionary<string, JsonValue>>.GetMethod("ContainsKey"),
                    [|propNameExpr :> Expression|]
                )
                
                // Get property value and deserialize
                let deserializeMethod = typedefof<JsonDeserializerImpl<_>>
                                            .MakeGenericType(param.PropertyType)
                                            .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, 
                                                      BindingFlags.NonPublic ||| BindingFlags.Static)
                
                let getValueAndDeserialize = Expression.Call(
                    deserializeMethod,
                    [|Expression.Call(
                        propsVar,
                        typeof<IDictionary<string, JsonValue>>.GetMethod("get_Item"),
                        [|propNameExpr :> Expression|]
                    ) :> Expression|]
                )
                
                // Default value if property not found
                let defaultValue = 
                    if param.PropertyType.IsValueType then
                        Expression.Default(param.PropertyType) :> Expression
                    else
                        Expression.Constant(null, param.PropertyType) :> Expression
                
                // Return value from condition
                Expression.Condition(
                    hasPropertyExpr,
                    getValueAndDeserialize,
                    defaultValue
                ) :> Expression
            )
            
            let toObjMeth = typeof<JsonImpl>.GetMethod(nameof JsonImpl.AsDictOrFail, BindingFlags.NonPublic ||| BindingFlags.Static)

            // Build the complete expression
            let body = Expression.Block(
                [|objVar; propsVar|],
                [|                    
                    // Get properties dictionary
                    Expression.Assign(objVar, jsonParam) :> Expression;
                    Expression.Assign(
                        propsVar,
                        Expression.Call(toObjMeth, objVar)
                    ) :> Expression;
                    
                    // Create record by calling constructor with all parameters
                    Expression.New(ctor, parameterExprs) :> Expression
                |]
            )
            
            let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                Expression.Convert(body, typeof<'A>),
                [|jsonParam|]
            )
            
            let fn = lambda.Compile(false)
            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | _ -> fn.Invoke json
            )

        | t ->
            if t.IsAbstract then
                (fun (json: JsonValue) ->
                    let extractedType = JsonImpl.TryGetTypeFromJson<'A> json

                    if not extractedType.IsAbstract then
                        // Extract differently
                        JsonImpl.DeserializeByType extractedType json :?> 'A
                    else
                        failwithf "Cannot serialize an abstract Type without the $type property from json.")

            else

            let propertiesInfo = t.GetProperties()

            if (not t.IsAbstract) && propertiesInfo |> Seq.sumBy(fun p -> if p.CanWrite then 1 else 0) = 0 then
                failwithf "Could not deserialize the type %s, it does not have any public writable property." t.Name

            let jsonDictParam = Expression.Parameter(typeof<IDictionary<string, JsonValue>>)
            let outVar = Expression.Variable(t)
            
            let outVarInitilization =
                let constr = t.GetConstructors() |> Seq.tryFind(fun c -> c.GetParameters().Length = 0 && (c.IsPublic || c.IsPrivate))
                match constr with
                | Some constr ->
                    Expression.New(constr, [||]) :> Expression
                | None -> 
                    let fn = Func<'A> (fun () -> System.Runtime.Serialization.FormatterServices.GetSafeUninitializedObject t :?> 'A)
                    Expression.Call(Expression.Constant(fn), typeof<Func<'A>>.GetMethod("Invoke"), [||])

            

            let lambda = Expression.Lambda<Func<IDictionary<string, JsonValue>, 'A>>(
                Expression.Block(
                [|outVar|],
                [|
                    Expression.Assign(outVar, outVarInitilization) :> Expression;

                    for prop in propertiesInfo do
                        if prop.CanWrite then
                            let variableValue = Expression.Variable typeof<JsonValue>
                            Expression.Block([|variableValue|],
                                [|Expression.IfThen(
                                    Expression.Call(
                                        jsonDictParam,
                                        typeof<IDictionary<string, JsonValue>>.GetMethod("TryGetValue"),
                                        Expression.Constant(prop.Name),
                                        variableValue
                                    ),
                                    
                                    Expression.Assign(
                                        Expression.Property(outVar, prop), 
                                        
                                        Expression.Call(
                                            typedefof<JsonDeserializerImpl<_>>
                                                .MakeGenericType(prop.PropertyType)
                                                .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static),
                                            variableValue))
                                    
                                    ) :> Expression
                               |])
                            ()


                    outVar;
                |]),
                [|jsonDictParam|]
            )
            
            let fn = lambda.Compile(false)
            
            // No need to check the $type
            if t.IsSealed then
                (fun (json: JsonValue) ->
                    match json with
                    | JsonValue.Null -> Unchecked.defaultof<'A>
                    | _ -> 
                        let dict = JsonImpl.AsDictOrFail json
                        fn.Invoke dict)

            else
                (fun (json: JsonValue) ->
                    match json with
                    | JsonValue.Null -> Unchecked.defaultof<'A>
                    | _ -> 

                    let extractedType = JsonImpl.TryGetTypeFromJson<'A> json

                    if extractedType <> t then
                        // Extract differently
                        JsonImpl.DeserializeByType extractedType json :?> 'A
                    else
                        let dict = JsonImpl.AsDictOrFail json
                        fn.Invoke dict)

and private JsonSerializerImpl<'A> =
    static member internal SerializeFunc(a: 'A) : JsonValue =
        JsonSerializerImpl<'A>.Serialize a

    static member internal SerializeWithType (value: 'A) : JsonValue =
        let json = JsonSerializerImpl<'A>.Serialize value
        match json, value.GetType() |> typeToName with
        | Object _, Some t -> json["$type"] <- String t
        | _other, Some _t -> () // Ignore
        | _other, None -> () // Also ignore
        json

    static member val internal Serialize: 'A -> JsonValue = 
        match typeof<'A> with
        | t when t = typeof<obj> -> 
            (fun (o: obj) -> 
                match o with
                | null -> JsonValue.Null
                | _ -> JsonImpl.SerializeByType (o.GetType()) o)
                :> obj :?> 'A -> JsonValue

        | t when t.IsAbstract -> 
            (fun (o: 'A) ->
                let o = box o
                match o with
                | null -> JsonValue.Null
                | _ -> JsonImpl.SerializeByType (o.GetType()) o)
                :> obj :?> 'A -> JsonValue

        | OfType bool -> 
            (fun (o: bool) -> Boolean o)
            :> obj :?> ('A -> JsonValue)

        | OfType char -> 
            (fun (o: char) -> Number (decimal o))
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
            (fun (s: string) -> 
                if isNull s
                then Null
                else String s)
            :> obj :?> ('A -> JsonValue)

        // For efficient storage.
        | OfType (id: byte array -> byte array) ->
            (fun (ba: byte array) ->
                if isNull ba
                then Null
                else String(JsonHelper.byteArrayToJSONCompatibleString ba))
            :> obj :?> ('A -> JsonValue)

        | OfType (id: byte seq -> byte seq) ->
            let fn = 
                Func<byte seq, JsonValue>(
                    (fun (bs: byte seq) ->
                        let arr = Seq.toArray bs
                        JsonSerializerImpl<byte array>.Serialize arr
                        )
                    )

            let p = Expression.Parameter(typeof<'A>)
            let l = Expression.Lambda<Func<'A, JsonValue>>(
                        Expression.Call(Expression.Constant(fn), typeof<Func<byte seq, JsonValue>>.GetMethod(nameof fn.Invoke), [|Expression.Convert(p, typeof<byte seq>) :> Expression|]),
                        [|p|]
                        )

            let fn = l.Compile(false)

            (fun (o: 'A) -> 
                if isNull o
                then Null
                else fn.Invoke o
            )

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
            (fun (v: 'A) -> v :> obj :?> JsonValue)
            :> obj :?> ('A -> JsonValue)

        | t when t.Name = "FSharpOption`1" ->
            failwithf "FSharp option is not supported yet, use Nullable<>"

        | t when t.Name = "Nullable`1" ->
            
            let underlyingType = (GenericTypeArgCache.Get t).[0]
            let serializerType = typedefof<JsonSerializerImpl<_>>.MakeGenericType(underlyingType)
            let serializeMethod = serializerType.GetMethod(nameof JsonSerializerImpl<_>.SerializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
    
            let p = Expression.Parameter(t)
            let hasValueProp = t.GetProperty("HasValue")
            let valueProp = t.GetProperty("Value")
    
            // Create condition: if (p.HasValue) then serialize p.Value else JsonValue.Null
            let condition = Expression.Property(p, hasValueProp)
            let valueAccess = Expression.Property(p, valueProp)
            let serializeCall = Expression.Call(serializeMethod, valueAccess)
            let nullValue = Expression.Constant(JsonValue.Null)
    
            let conditional = Expression.Condition(condition, serializeCall, nullValue, typeof<JsonValue>)
    
            let lambda = Expression.Lambda<Func<'A, JsonValue>>(
                conditional,
                [|p|]
            )
    
            let fn = lambda.Compile(false)
            

            (fun (nullable: 'A) -> fn.Invoke nullable)

        | t when t.FullName = "SoloDatabase.MongoDB.BsonDocument" ->
            let p = Expression.Parameter(t)
            let jsonProp = t.GetProperty("Json")
            let fn = 
                Expression.Lambda<Func<'A, JsonValue>>(
                    Expression.Property(p, jsonProp),
                    [|p|]
                ).Compile(false)

            (fun (o: 'A) -> 
                if isNull o
                then Null
                else fn.Invoke o
            )

        | t when JsonHelper.implementsGeneric typedefof<IReadOnlyDictionary<_,_>> t || 
                 JsonHelper.implementsGeneric typedefof<IDictionary<_,_>> t ->

            let iface = 
                t.GetInterface("IReadOnlyDictionary`2") 
                |> Option.ofObj 
                |> Option.orElseWith(fun () -> t.GetInterface("IDictionary`2") |> Option.ofObj) 
                |> Option.defaultValue t // If not then it is an interface
    
            let genericArgs = GenericTypeArgCache.Get iface

            let param = Expression.Parameter(t)

            let serializeMeth = typeof<JsonImpl>
                                 .GetMethod(nameof JsonImpl.SerializeDictionary, BindingFlags.NonPublic ||| BindingFlags.Static)
                                 .MakeGenericMethod(genericArgs)


            let lambda = Expression.Lambda<Func<'A, JsonValue>>(
                Expression.Call(serializeMeth, [|param :> Expression|]), 
                [|param|]
            )
    
            let fn = lambda.Compile(false)

            (fun (o: 'A) -> 
                if isNull o
                then Null
                else fn.Invoke o
            )

            
        | t when typeof<IDictionary>.IsAssignableFrom(t) ->
            (fun (dict: 'A) ->
                if isNull dict
                then Null
                else
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

            let meth = typeof<JsonImpl>
                        .GetMethod((nameof JsonImpl.IEnumerableSerialize), BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(elemType)

            let param = Expression.Parameter(t)

            let lambda = Expression.Lambda<Func<'A, JsonValue>>(
                Expression.Call(meth, [|param :> Expression|]),
                [|param|]
            )

            let fn = lambda.Compile()

            (fun (collection: 'A) ->
                if isNull collection
                then Null
                else fn.Invoke collection)
            :> obj :?> ('A -> JsonValue)

        | t when typeof<IEnumerable>.IsAssignableFrom(t) && t <> typeof<string> ->
            (fun (collection: 'A) ->
                if isNull collection
                then Null
                else
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

            let createJsonMeth = typeof<JsonImpl>
                                    .GetMethod((nameof JsonImpl.CreateJsonObj), BindingFlags.NonPublic ||| BindingFlags.Static)

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

            if t.IsSealed then
                (fun (o: 'A) ->
                    if obj.ReferenceEquals(o, null) 
                    then Null
                    else 
                        let json = fn.Invoke o
                        json)
                :> obj :?> ('A -> JsonValue)
            else
                // 'A can be of Class1, and o.GetType() to be of Class2
                (fun (o: 'A) ->
                    if obj.ReferenceEquals(o, null) 
                    then Null
                    else 
                        let typeOfO = o.GetType()
                        if typeOfO <> typeof<'A> then
                            JsonImpl.SerializeByType typeOfO (box o)
                        else
                            let json = fn.Invoke o
                            json)
                :> obj :?> ('A -> JsonValue)

and internal JsonValueMetaObject(expression: Expression, restrictions: BindingRestrictions, value: JsonValue) =
    inherit DynamicMetaObject(expression, restrictions, value)
    
    static member val private GetPropertyMethod = typeof<JsonValue>.GetMethod("GetPropertyForBinder", BindingFlags.NonPublic ||| BindingFlags.Instance)
    static member val private SetPropertyMethod = typeof<JsonValue>.GetMethod("set_Item")
    static member val private ToObjectMethod = typeof<JsonValue>.GetMethod("ToObject`1")
    static member val private ToStringMethod = typeof<obj>.GetMethod("ToString")
    static member val private SerializeMethod = typeof<JsonValue>.GetMethod("Serialize").MakeGenericMethod(typeof<obj>)

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