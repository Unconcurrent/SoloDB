namespace SoloDatabase.JsonSerializator

open System.Collections.Concurrent
open System
open System.Collections.Generic
open System.Collections
open System.Reflection
open System.Dynamic
open SoloDatabase.Utils
open System.Text
open System.Globalization
open System.Runtime.CompilerServices
open Microsoft.FSharp.Reflection
open System.Web
open System.Linq.Expressions
open System.Linq
open System.Numerics
open Microsoft.FSharp.NativeInterop

#nowarn "9" // NativePtr stuff

/// <summary>
/// Contains helper functions for JSON serialization and deserialization.
/// </summary>
module private JsonHelper =
    /// <summary>
    /// Checks if a given type is an array type supported for Newtownsoft-style serialization (e.g., arrays of primitives, strings, DateTime).
    /// </summary>
    /// <param name="t">The type to check.</param>
    /// <returns>True if the type is a supported array type, false otherwise.</returns>
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

    /// <summary>
    /// A cache for the results of the implementsGeneric function.
    /// </summary>
    let private implementsGenericCache = System.Collections.Concurrent.ConcurrentDictionary<struct (Type * Type), bool>()
    
    /// <summary>
    /// Checks if a target type implements a specific generic type definition.
    /// </summary>
    /// <param name="genericTypeDefinition">The generic type definition to check for (e.g., typeof&lt;IDictionary&lt;_,_&gt;&gt;).</param>
    /// <param name="targetType">The type to check.</param>
    /// <returns>True if the target type implements the generic type definition, false otherwise.</returns>
    let internal implementsGeneric (genericTypeDefinition: Type) (targetType: Type) =        
        implementsGenericCache.GetOrAdd(struct (genericTypeDefinition, targetType), fun struct (genericTypeDefinition, targetType) ->
            targetType.IsGenericType && 
            targetType.GetGenericTypeDefinition() = genericTypeDefinition ||
            targetType.GetInterfaces() 
            |> Array.exists (fun interfaceType -> 
                interfaceType.IsGenericType && 
                interfaceType.GetGenericTypeDefinition() = genericTypeDefinition)
        )

    /// <summary>
    /// Converts a byte array to a Base64 encoded string, suitable for embedding in JSON.
    /// </summary>
    /// <param name="ba">The byte array to convert.</param>
    /// <returns>A Base64 encoded string.</returns>
    let internal byteArrayToJSONCompatibleString(ba: byte array) =
        Convert.ToBase64String ba

    /// <summary>
    /// Converts a Base64 encoded string from a JSON value back to a byte array.
    /// </summary>
    /// <param name="s">The Base64 encoded string.</param>
    /// <returns>The decoded byte array.</returns>
    let internal JSONStringToByteArray(s: string) =
        Convert.FromBase64String s

    /// Try-parse a ReadOnlySpan<char> as a base-16 (hex) Int32.
    /// Accepts optional '+' or '-' sign, optional "0x"/"0X" prefix.
    /// Returns true + sets `result` on success; returns false and leaves `result` unspecified on failure.
    let internal tryParseHex (span: ReadOnlySpan<char>, result: byref<int>) : bool =
        // trim leading whitespace
        let mutable i = 0
        let len = span.Length
        let mutable j = len - 1

        // optional sign
        let mutable negative = false
        if span.[i] = '-' then
            negative <- true
            i <- i + 1
        elif span.[i] = '+' then
            i <- i + 1

        // optional 0x/0X prefix
        if i + 1 <= j && span.[i] = '0' && (span.[i + 1] = 'x' || span.[i + 1] = 'X') then
            i <- i + 2

        if i > j then false else

        // parse digits
        let mutable acc = 0u
        let mutable k = i
        let mutable ok = true
        while k <= j && ok do
            let ch = span.[k]
            let digit =
                if ch >= '0' && ch <= '9' then int ch - int '0'
                elif ch >= 'a' && ch <= 'f' then 10 + int ch - int 'a'
                elif ch >= 'A' && ch <= 'F' then 10 + int ch - int 'A'
                else -1
            if digit < 0 then
                // invalid character
                result <- 0
                ok <- false
            else
                acc <- acc * 16u + uint32 digit

            k <- k + 1

        if not ok then false else

        // produce final signed result
        if negative then
            if acc <= 0x7FFFFFFFu then
                result <- - (int acc)
                true
            elif acc = 0x80000000u then
                result <- Int32.MinValue
                true
            else
                result <- 0
                false
        else
            if acc <= 0x7FFFFFFFu then
                result <- int acc
                true
            else
                // positive overflow
                result <- 0
                false

    /// Parse and throw on failure.
    let internal parseHex (span: ReadOnlySpan<char>) : int =
        let mutable v = 0
        if tryParseHex(span, &v) then v
        else
            raise (FormatException("Input is not a valid 32-bit hexadecimal integer."))

/// <summary>
/// Represents the different types of tokens that can be found in a JSON string.
/// </summary>
type internal Token =
    /// <summary>Represents a JSON null literal.</summary>
    | NullToken
    /// <summary>Represents an opening brace '{'.</summary>
    | OpenBrace
    /// <summary>Represents a closing brace '}'.</summary>
    | CloseBrace
    /// <summary>Represents an opening bracket '['.</summary>
    | OpenBracket
    /// <summary>Represents a closing bracket ']'.</summary>
    | CloseBracket
    /// <summary>Represents a comma ','.</summary>
    | Comma
    /// <summary>Represents a colon ':'.</summary>
    | Colon
    /// <summary>Represents a string literal.</summary>
    | StringToken of string
    /// <summary>Represents a numeric value.</summary>
    | NumberToken of decimal
    /// <summary>Represents a boolean literal.</summary>
    | BooleanToken of bool
    /// <summary>Represents the end of the input string.</summary>
    | EndOfInput

/// <summary>
/// Represents a grouping of items by a key, implementing IGrouping.
/// </summary>
/// <typeparam name="'key">The type of the key.</typeparam>
/// <typeparam name="'item">The type of the items in the group.</typeparam>
type internal Grouping<'key, 'item> (key: 'key, items: 'item array) =
    /// <summary>
    /// Gets the key of the grouping.
    /// </summary>
    member this.Key = (this :> IGrouping<'key, 'item>).Key
    /// <summary>
    /// Gets the number of items in the grouping.
    /// </summary>
    member this.Length = items.LongLength

    /// <summary>
    /// Returns a string representation of the grouping, showing the key.
    /// </summary>
    override this.ToString (): string = 
        sprintf "Key = %A" this.Key

    /// <summary>
    /// Implementation of the IGrouping interface.
    /// </summary>
    interface IGrouping<'key, 'item> with
        /// <summary>
        /// Gets the key of the grouping.
        /// </summary>
        member this.Key = key
        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        member this.GetEnumerator() = (items :> 'item seq).GetEnumerator()
        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        member this.GetEnumerator() : IEnumerator = 
            (items :> IEnumerable).GetEnumerator()

/// <summary>
/// Provides a helper class for deserializing sequences into F# lists.
/// </summary>
type private FSharpListDeserialize =
    /// <summary>
    /// Caches the compiled functions for creating F# lists from sequences of objects.
    /// </summary>
    static member val private Cache = ConcurrentDictionary<Type, obj seq -> obj>()
    /// <summary>
    /// Converts a sequence of objects to a strongly-typed F# list.
    /// </summary>
    /// <typeparam name="'a">The element type of the list.</typeparam>
    /// <param name="c">The input sequence of objects.</param>
    /// <returns>An F# list of type 'a.</returns>
    static member private OfSeq<'a>(c: obj seq) = c |> Seq.map (fun a -> a :?> 'a) |> List.ofSeq

    /// <summary>
    /// Creates a function that can convert a sequence of objects to an F# list of the specified element type.
    /// </summary>
    /// <param name="elementType">The element type of the F# list to create.</param>
    /// <returns>A function that takes a sequence of objects and returns an F# list.</returns>
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

/// <summary>
/// Defines the type of a JsonValue, for use in C#.
/// </summary>
[<Struct>]
type JsonValueType =
    /// <summary>A null value.</summary>
    | Null = 1uy
    /// <summary>A boolean value.</summary>
    | Boolean = 2uy
    /// <summary>A string value.</summary>
    | String = 3uy
    /// <summary>A number value.</summary>
    | Number = 4uy
    /// <summary>A list (array) of JsonValues.</summary>
    | List = 5uy
    /// <summary>An object (dictionary) of string keys and JsonValues.</summary>
    | Object = 6uy

/// <summary>
/// A struct that tokenizes a JSON string.
/// </summary>
[<Struct>]
type internal Tokenizer =
    {
        /// <summary>The input JSON string to tokenize.</summary>
        input: string
        /// <summary>The current position within the input string.</summary>
        mutable index: int
        /// <summary>A reusable StringBuilder for parsing strings.</summary>
        sb: StringBuilder
    }

    /// <summary>
    /// Initializes a new Tokenizer with the specified source string.
    /// </summary>
    /// <param name="source">The JSON string to tokenize.</param>
    static member internal From(source: string) = { input = source; index = 0; sb = System.Text.StringBuilder() }
    
    /// <summary>
    /// Reads the next token from the input string and advances the index.
    /// </summary>
    /// <returns>The next Token from the stream.</returns>
    member private this.ReadNext() : Token =
        let inline isDigitOrMinus c = Char.IsDigit c || c = '-'
        let inline isInitialIdentifierChar c = Char.IsLetter c || c = '_'
        let inline isIdentifierChar c = isInitialIdentifierChar c || Char.IsDigit c

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
                                // parse first \uXXXX (starting at i+1)
                                let code1 = JsonHelper.parseHex (input.AsSpan(i + 1, 4))
                                let ch1 = char code1
                                i <- i + 4  // consumed the 4 hex digits

                                if Char.IsHighSurrogate(ch1) then
                                    // expect: \uXXXX (high) then sequence: \ u XXXX (low)
                                    // current i points to the last hex digit of the first \u; we need to check the next characters
                                    if i + 6 < input.Length && input.[i + 1] = '\\' && input.[i + 2] = 'u' then
                                        let code2 = JsonHelper.parseHex (input.AsSpan(i + 3, 4))
                                        let ch2 = char code2
                                        if Char.IsLowSurrogate(ch2) then
                                            // append both halves directly (no allocation)
                                            sb.Append(ch1) |> ignore
                                            sb.Append(ch2) |> ignore
                                            i <- i + 6  // consumed: '\' 'u' and 4 hex digits for the low surrogate
                                        else
                                            failwith "Invalid low surrogate"
                                    else
                                        failwith "Expected low surrogate after high surrogate"
                                else
                                    // BMP character — append single char
                                    sb.Append(ch1) |> ignore
                            else
                                failwith "Invalid Unicode escape sequence"
                        | other -> failwithf "Invalid escape sequence: '%c'" other
                    else
                        sb.Append(input.[i]) |> ignore
                    i <- i + 1
                if i >= input.Length then failwith "Unterminated string"
                this.index <- i + 1
                StringToken(sb.ToString())
            | c when isDigitOrMinus c ->                
                let mutable i = this.index
                let mutable length = 0
                
                while i < input.Length && 
                        (isDigitOrMinus input.[i] || 
                         input.[i] = '.' || 
                         input.[i] = 'e' || 
                         input.[i] = 'E' || 
                         input.[i] = '+') do
                    length <- length + 1
                    i <- i + 1
                
                #if NETSTANDARD2_1
                let s = input.AsSpan(this.index, length)
                #else
                let s = input.Substring(this.index, length)
                #endif

                this.index <- i

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
                let inline fallback (input: string) (index: int) (sb: StringBuilder) =
                    let sb = sb.Clear()
                    let mutable i = index
                    while i < input.Length && isIdentifierChar input.[i] do
                        sb.Append(input.[i]) |> ignore
                        i <- i + 1

                    let txt = sb.ToString()
                    
                    // F#'s refs restrictions
                    struct (match txt with
                            | "null"
                            | "NULL" -> NullToken
                            | "true" -> BooleanToken(true)
                            | "false" -> BooleanToken(false)
                            // Non standard. 
                            | other -> StringToken other
                            , i)


                if this.index + 4 <= input.Length then
                    // We know that all identifiers are at least 4 chars long
                    match input.AsSpan(this.index, 4) with
                    | x when x.Equals ("null".AsSpan(), StringComparison.Ordinal) && (this.index + 4 = input.Length || (not << isIdentifierChar) input.[this.index + 4]) -> 
                        this.index <- this.index + 4
                        NullToken
                    | x when x.Equals ("NULL".AsSpan(), StringComparison.Ordinal) && (this.index + 4 = input.Length || (not << isIdentifierChar) input.[this.index + 4]) -> 
                        this.index <- this.index + 4
                        NullToken
                    | x when x.Equals ("true".AsSpan(), StringComparison.Ordinal) && (this.index + 4 = input.Length || (not << isIdentifierChar) input.[this.index + 4]) -> 
                        this.index <- this.index + 4
                        BooleanToken true
                    | x when x.Equals ("fals".AsSpan(), StringComparison.Ordinal) 
                             && this.index + 4 < input.Length 
                             && input.[this.index + 4] = 'e' 
                             && (this.index + 5 = input.Length || (not << isIdentifierChar) input.[this.index + 5]) 
                             -> 
                        this.index <- this.index + 5
                        BooleanToken false
                    | _ ->
                        match fallback input this.index this.sb with struct (x, i) -> this.index <- i; x
                else 
                    match fallback input this.index this.sb with struct (x, i) -> this.index <- i; x
                
            | c when Char.IsWhiteSpace(c) ->
                this.index <- this.index + 1
                this.ReadNext()
            | c -> failwithf "Unexpected JSON character: %c, index = %i" c this.index
        else
            EndOfInput

    /// <summary>
    /// Peeks at the next token without consuming it from the input stream.
    /// </summary>
    /// <returns>The next Token.</returns>
    member this.Peek() : Token =
        let savedIndex = this.index
        try this.ReadNext()
        finally this.index <- savedIndex

    /// <summary>
    /// Parses the next token into a JsonValue, or executes a fallback function.
    /// </summary>
    /// <param name="f">A function to execute if the token is not a standard JSON value type.</param>
    /// <returns>An option containing the parsed JsonValue, or None.</returns>
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
    
    /// <summary>
    /// Parses the next token into a JsonValue, failing if the token is not a valid start of a value.
    /// </summary>
    /// <returns>The parsed JsonValue.</returns>
    member private this.ParseTokens() =
        match this.ParseTokensOr(fun _t -> None) with
        | Some x -> x
        | None -> failwithf "Could not parse the token at index = %i." this.index
    
    /// <summary>
    /// Recursively parses the members (key-value pairs) of a JSON object.
    /// </summary>
    /// <param name="dict">The dictionary to populate with parsed members.</param>
    /// <returns>The populated dictionary.</returns>
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
        
    /// <summary>
    /// Parses a JSON object from the token stream.
    /// </summary>
    /// <returns>A dictionary representing the JSON object.</returns>
    member private this.ParseObject() =
        let dict = Dictionary<string, JsonValue>()
        this.ParseMembers(dict)
        
    /// <summary>
    /// Recursively parses the elements of a JSON array.
    /// </summary>
    /// <param name="items">The list to populate with parsed elements.</param>
    /// <returns>The populated list.</returns>
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
            
    /// <summary>
    /// Parses a JSON array from the token stream.
    /// </summary>
    /// <returns>A list representing the JSON array.</returns>
    member private this.ParseArray() =
        let items = new List<JsonValue>()
        this.ParseElements(items)
        
    /// <summary>
    /// Parses the entire token stream into a single JsonValue.
    /// </summary>
    /// <returns>The root JsonValue.</returns>
    member this.Parse() : JsonValue =
        this.ParseTokens()

/// <summary>
/// Represents a JSON value, which can be a null, boolean, string, number, list, or object.
/// This is a discriminated union that forms the core of the JSON representation.
/// </summary>
and JsonValue =
    /// <summary>Represents a JSON null value.</summary>
    | Null
    /// <summary>Represents a JSON boolean value.</summary>
    | Boolean of bool
    /// <summary>Represents a JSON string value.</summary>
    | String of string
    /// <summary>Represents a JSON number value, stored as a decimal for precision.</summary>
    | Number of decimal
    /// <summary>Represents a JSON array (list) of other JsonValue instances.</summary>
    | List of IList<JsonValue>
    /// <summary>Represents a JSON object, a dictionary from string keys to JsonValue instances.</summary>
    | Object of IDictionary<string, JsonValue>

    /// <summary>
    /// Serializes a given object of type 'T into a JsonValue.
    /// </summary>
    /// <typeparam name="'T">The type of the object to serialize.</typeparam>
    /// <param name="t">The object instance to serialize.</param>
    /// <returns>A JsonValue representation of the object.</returns>
    static member Serialize<'T>(t: 'T) : JsonValue =
        JsonSerializerImpl<'T>.Serialize t

    /// <summary>
    /// Serializes a given object of type 'T into a JsonValue, including a "$type" property with the object's type name.
    /// </summary>
    /// <typeparam name="'T">The type of the object to serialize.</typeparam>
    /// <param name="t">The object instance to serialize.</param>
    /// <returns>A JsonValue representation of the object with type information.</returns>
    static member SerializeWithType<'T>(t: 'T) : JsonValue =
        JsonSerializerImpl<'T>.SerializeWithType t

    /// <summary>
    /// Gets the type of the JsonValue, for C# usage.
    /// </summary>
    member this.JsonType = 
        match this with 
        | Null -> JsonValueType.Null
        | Boolean _b -> JsonValueType.Boolean
        | String _s -> JsonValueType.String
        | Number _n -> JsonValueType.Number
        | List _l -> JsonValueType.List
        | Object _o -> JsonValueType.Object

    /// <summary>
    /// Deserializes the JsonValue into an object of the specified target type.
    /// </summary>
    /// <param name="targetType">The type to deserialize into.</param>
    /// <returns>The deserialized object.</returns>
    member this.ToObject (targetType: Type) =
        JsonImpl.DeserializeByType targetType this

    /// <summary>
    /// Deserializes the JsonValue into an object of the specified generic type 'T.
    /// </summary>
    /// <typeparam name="'T">The type to deserialize into.</typeparam>
    /// <returns>The deserialized object of type 'T.</returns>
    member this.ToObject<'T>() : 'T =
        JsonDeserializerImpl<'T>.Deserialize this

    /// <summary>
    /// Sets a property on a JsonValue.Object or an element in a JsonValue.List.
    /// </summary>
    /// <param name="name">The name of the property or the index of the list element.</param>
    /// <param name="value">The JsonValue to set.</param>
    member this.SetProperty(name: string, value: JsonValue) =
        match this with
        | Object o -> o.[name] <- value
        | List list -> 
            let index = Int32.Parse(name, CultureInfo.InvariantCulture)
            list.[index] <- value
        | other -> failwithf "Cannot index %s" (other.ToString())

    /// <summary>
    /// Checks if a JsonValue.Object contains a property with the specified name, or if a JsonValue.List has an element at the specified index.
    /// </summary>
    /// <param name="name">The property name or index to check.</param>
    /// <returns>True if the property or element exists, false otherwise.</returns>
    member this.Contains (name: string) =
        match this with
        | Object o -> o.ContainsKey name
        | List list -> 
            match Int32.TryParse (name, NumberStyles.Integer, CultureInfo.InvariantCulture) with
            | true, index -> list.Count > index
            | false, _ -> false
        | other -> failwithf "Cannot index %s" (other.ToString())

    /// <summary>
    /// Tries to get a property from a JsonValue.Object or an element from a JsonValue.List.
    /// </summary>
    /// <param name="name">The name of the property or index of the element.</param>
    /// <param name="v">An out parameter that will contain the value if found.</param>
    /// <returns>True if the property or element was found, false otherwise.</returns>
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

    /// <summary>
    /// Gets a property from a JsonValue.Object or an element from a JsonValue.List. Fails if not found.
    /// </summary>
    /// <param name="name">The name of the property or index of the element.</param>
    /// <returns>The corresponding JsonValue.</returns>
    member this.GetProperty(name: string) =
        match this.TryGetProperty(name) with
        | true, value -> value
        | false, _ -> failwith "Property not found"

    /// <summary>
    /// Gets a property and converts it to a suitable .NET object for dynamic binding.
    /// </summary>
    /// <param name="name">The name of the property.</param>
    /// <remarks>If this method will be eventually renamed in the future then please rename its name stored in JsonValueMetaObject.</remarks>
    /// <returns>The property value as a .NET object.</returns>
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

    /// <summary>
    /// Provides an indexer for getting and setting properties on a JsonValue.
    /// </summary>
    /// <param name="name">The name of the property or the index of the list element.</param>
    [<System.Runtime.CompilerServices.IndexerName("Item")>]
    member this.Item
        with get(name: string) =
            match this.TryGetProperty(name) with
            | true, value -> value
            | false, _ -> failwith "Property not found"

        and set(name: string) (jValue: JsonValue) =
            this.SetProperty (name, jValue)

    /// <summary>
    /// Recursively serializes a JsonValue to a string using a StringBuilder.
    /// </summary>
    /// <param name="value">The JsonValue to serialize.</param>
    /// <param name="sb">The StringBuilder to append the result to.</param>
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

    /// <summary>
    /// Converts the JsonValue to its JSON string representation.
    /// </summary>
    /// <returns>A JSON formatted string.</returns>
    member this.ToJsonString() =
        match this with
        | Null -> "null"
        | Boolean b -> if b then "true" else "false"
        | Number n -> 
            if Decimal.IsInteger n then 
                n.ToString("F0", CultureInfo.InvariantCulture)
            else 
                n.ToString("0.############################", CultureInfo.InvariantCulture)
        | String s -> HttpUtility.JavaScriptStringEncode(s, true)
        | _ ->

        let sb = new StringBuilder(128)
        JsonValue.Jsonize this sb
        sb.ToString()

    /// <summary>
    /// Compares this JsonValue with another object for equality by parsing the other object's string representation.
    /// </summary>
    /// <param name="other">The object to compare with.</param>
    /// <returns>True if the parsed object is equal to this JsonValue.</returns>
    member this.Eq (other: obj) =
        this = JsonValue.Parse $"{other}"

    /// <summary>
    /// Creates a new, empty JsonValue.Object.
    /// </summary>
    /// <returns>A new JsonValue.Object.</returns>
    static member New() = JsonValue.Object(Dictionary())

    /// <summary>
    /// Creates a new JsonValue.Object from a sequence of key-value pairs.
    /// </summary>
    /// <param name="values">A sequence of key-value pairs to populate the object with.</param>
    /// <returns>A new JsonValue.Object.</returns>

    static member New(values: KeyValuePair<string, obj> seq) = 
        let json = JsonValue.New()
        for KeyValue(key ,v) in values do
            json.[key] <- JsonSerializerImpl<obj>.Serialize v
        json
        
    /// <summary>
    /// Parses a JSON string into a JsonValue.
    /// </summary>
    /// <param name="jsonString">The JSON string to parse.</param>
    /// <returns>The parsed JsonValue.</returns>
    static member Parse (jsonString: string) =
        if isNull jsonString then Null else

        let tokenizer = Tokenizer.From jsonString
        let json = tokenizer.Parse()
        json

    /// <summary>
    /// Creates a new JsonValue.Object from a sequence of key-value pairs with strongly typed values.
    /// </summary>
    /// <typeparam name="'key">The type of the key.</typeparam>
    /// <typeparam name="'a">The type of the value.</typeparam>
    /// <typeparam name="'enu">The type of the enumerable sequence.</typeparam>
    /// <param name="values">The sequence of key-value pairs.</param>
    /// <returns>A new JsonValue.Object.</returns>
    static member Create<'key, 'a, 'enu when 'enu :> struct ('key * 'a) seq> (values: 'enu) = 
        let json = JsonValue.New()
        for (key, v) in values do
            json.[string key] <- JsonSerializerImpl<'a>.Serialize v
        json

    /// <summary>
    /// Returns the JSON string representation of the value.
    /// </summary>
    override this.ToString() = this.ToJsonString()

    /// <summary>Implicitly converts any object of type 'T to a JsonValue.</summary>
    static member op_Implicit<'T>(o: 'T) : JsonValue =
        JsonSerializerImpl<'T>.Serialize o

    /// <summary>Implicitly converts a generic object to a JsonValue.</summary>
    static member op_Implicit(o: obj) : JsonValue =
        JsonValue.op_Implicit<obj> o

    /// <summary>Implicitly converts an int8 to a JsonValue.</summary>
    static member op_Implicit(o: int8) : JsonValue =
        JsonValue.op_Implicit<int8> o
    /// <summary>Implicitly converts an int16 to a JsonValue.</summary>
    static member op_Implicit(o: int16) : JsonValue =
        JsonValue.op_Implicit<int16> o
    /// <summary>Implicitly converts an int32 to a JsonValue.</summary>
    static member op_Implicit(o: int32) : JsonValue =
        JsonValue.op_Implicit<int32> o
    /// <summary>Implicitly converts an int64 to a JsonValue.</summary>
    static member op_Implicit(o: int64) : JsonValue =
        JsonValue.op_Implicit<int64> o
    /// <summary>Implicitly converts a uint8 to a JsonValue.</summary>
    static member op_Implicit(o: uint8) : JsonValue =
        JsonValue.op_Implicit<uint8> o
    /// <summary>Implicitly converts a uint16 to a JsonValue.</summary>
    static member op_Implicit(o: uint16) : JsonValue =
        JsonValue.op_Implicit<uint16> o
    /// <summary>Implicitly converts a uint32 to a JsonValue.</summary>
    static member op_Implicit(o: uint32) : JsonValue =
        JsonValue.op_Implicit<uint32> o
    /// <summary>Implicitly converts a uint64 to a JsonValue.</summary>
    static member op_Implicit(o: uint64) : JsonValue =
        JsonValue.op_Implicit<uint64> o
    /// <summary>Implicitly converts a float32 to a JsonValue.</summary>
    static member op_Implicit(o: float32) : JsonValue =
        JsonValue.op_Implicit<float32> o
    /// <summary>Implicitly converts a float to a JsonValue.</summary>
    static member op_Implicit(o: float) : JsonValue =
        JsonValue.op_Implicit<float> o
    /// <summary>Implicitly converts a decimal to a JsonValue.</summary>
    static member op_Implicit(o: decimal) : JsonValue =
        JsonValue.op_Implicit<decimal> o
    /// <summary>Implicitly converts a bool to a JsonValue.</summary>
    static member op_Implicit(o: bool) : JsonValue =
        JsonValue.op_Implicit<bool> o
    /// <summary>Implicitly converts a char to a JsonValue.</summary>
    static member op_Implicit(o: char) : JsonValue =
        JsonValue.op_Implicit<char> o
    /// <summary>Implicitly converts a string to a JsonValue.</summary>
    static member op_Implicit(o: string) : JsonValue =
        JsonValue.op_Implicit<string> o
    /// <summary>Implicitly converts a byte array to a JsonValue.</summary>
    static member op_Implicit(o: byte array) : JsonValue =
        JsonValue.op_Implicit<byte array> o
    /// <summary>Implicitly converts a Guid to a JsonValue.</summary>
    static member op_Implicit(o: Guid) : JsonValue =
        JsonValue.op_Implicit<Guid> o
    /// <summary>Implicitly converts a DateTime to a JsonValue.</summary>
    static member op_Implicit(o: DateTime) : JsonValue =
        JsonValue.op_Implicit<DateTime> o
    /// <summary>Implicitly converts a DateTimeOffset to a JsonValue.</summary>
    static member op_Implicit(o: DateTimeOffset) : JsonValue =
        JsonValue.op_Implicit<DateTimeOffset> o
    /// <summary>Implicitly converts a TimeSpan to a JsonValue.</summary>
    static member op_Implicit(o: TimeSpan) : JsonValue =
        JsonValue.op_Implicit<TimeSpan> o
    /// <summary>Implicitly converts a DateOnly to a JsonValue.</summary>
    static member op_Implicit(o: DateOnly) : JsonValue =
        JsonValue.op_Implicit<DateOnly> o
    /// <summary>Implicitly converts a TimeOnly to a JsonValue.</summary>
    static member op_Implicit(o: TimeOnly) : JsonValue =
        JsonValue.op_Implicit<TimeOnly> o
    /// <summary>Implicitly converts a byte sequence to a JsonValue.</summary>
    static member op_Implicit(o: byte seq) : JsonValue =
        JsonValue.op_Implicit<byte seq> o

    /// <summary>
    /// Implementation of IDynamicMetaObjectProvider for dynamic language runtime (DLR) integration.
    /// </summary>
    interface IDynamicMetaObjectProvider with
        /// <summary>
        /// Returns the <see cref="DynamicMetaObject"/> responsible for binding operations performed on this object.
        /// </summary>
        /// <param name="expression">The expression representing this <see cref="DynamicMetaObject"/> during the binding process.</param>
        /// <returns>The <see cref="DynamicMetaObject"/> to bind this object.</returns>
        member this.GetMetaObject(expression: Linq.Expressions.Expression): DynamicMetaObject = 
            JsonValueMetaObject(expression, BindingRestrictions.Empty, this)

    /// <summary>
    /// Implementation of IEnumerable for iterating over JsonValue.Object or JsonValue.List.
    /// </summary>
    interface IEnumerable<KeyValuePair<string, JsonValue>> with
        /// <summary>
        /// Returns an enumerator that iterates through the key-value pairs of the object or indexed values of the list.
        /// </summary>
        override this.GetEnumerator (): IEnumerator<KeyValuePair<string,JsonValue>> =
            match this with
            | Object o -> o.GetEnumerator()
            | List l -> (l |> Seq.indexed |> Seq.map(fun (i, v) -> KeyValuePair<string, JsonValue>(string i, v))).GetEnumerator()
            | _other -> failwithf "This Json type does not support iteration: %A" (this.JsonType)

        /// <summary>
        /// Returns a non-generic enumerator.
        /// </summary>
        override this.GetEnumerator (): IEnumerator = 
            (this :> IEnumerable<KeyValuePair<string, JsonValue>>).GetEnumerator () :> IEnumerator

/// <summary>
/// Provides a cached conversion function from a string to a specified primitive type 'T.
/// </summary>
/// <typeparam name="'T">The target type for conversion.</typeparam>
and private ConvertStringTo<'T> =
    /// <summary>
    /// A cached function that converts a string to type 'T.
    /// </summary>
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

/// <summary>
/// Internal implementation details for JSON serialization and deserialization logic.
/// </summary>
and private JsonImpl =
    /// <summary>
    /// Creates a JsonValue.Object from a dictionary.
    /// </summary>
    /// <param name="d">The dictionary to wrap.</param>
    /// <returns>A JsonValue.Object.</returns>
    static member internal CreateJsonObj(d: IDictionary<string, JsonValue>) =
        JsonValue.Object d

    /// <summary>
    /// Asserts that a JsonValue is a List and returns the underlying list, or fails.
    /// </summary>
    /// <param name="json">The JsonValue to check.</param>
    /// <returns>The underlying IList&lt;JsonValue&gt;.</returns>
    static member internal AsListOrFail(json: JsonValue) =
        match json with
        | List l -> l
        | other -> failwithf "Expected json object to be a list, instead it is a %A" other

    /// <summary>
    /// Asserts that a JsonValue is an Object and returns the underlying dictionary, or fails.
    /// </summary>
    /// <param name="json">The JsonValue to check.</param>
    /// <returns>The underlying IDictionary&lt;string, JsonValue&gt;.</returns>
    static member internal AsDictOrFail(json: JsonValue) =
        match json with
        | Object d -> d
        | other -> failwithf "Expected json object to be a object/dictionary, instead it is a %A" other

    /// <summary>
    /// Tries to determine the target type for deserialization from a "$type" property in the JSON object.
    /// </summary>
    /// <typeparam name="'A">The expected base type.</typeparam>
    /// <param name="jsonObj">The JSON object.</param>
    /// <returns>The determined Type, or the original type 'A if no "$type" is found or if it's invalid.</returns>
    static member inline internal TryGetTypeFromJson<'A> (jsonObj: JsonValue) =
        let targetType = typeof<'A>
        // Be careful, a potential attacker can put anything in this field.
        let unsafeTypeProp = match jsonObj.TryGetProperty "$type" with true, x -> x.ToObject<string>() |> nameToType | false, _ -> null
        match struct (targetType.IsSealed, unsafeTypeProp) with
        | struct (true, _) -> targetType
        | struct (_, null) -> targetType
        | struct (false, (jsonType)) when targetType.IsAssignableFrom jsonType -> jsonType
        | _ -> targetType

    
    /// <summary>
    /// Serializes a sequence of items into a JsonValue.List.
    /// </summary>
    /// <typeparam name="'T">The type of items in the sequence.</typeparam>
    /// <param name="o">The sequence to serialize.</param>
    /// <returns>A JsonValue.List.</returns>
    static member internal IEnumerableSerialize<'T>(o: 'T seq) =
        let items = System.Collections.Generic.List<JsonValue>()
        for item in o do
            items.Add(JsonSerializerImpl<'T>.Serialize item)
        List items

    /// <summary>
    /// Deserializes a JsonValue.List into an IEnumerable of a specific type. Used for types that can be constructed from an IEnumerable.
    /// </summary>
    /// <typeparam name="'T">The element type of the resulting sequence.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>An IEnumerable&lt;'T&gt;.</returns>
    static member internal IEnumerableDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            let result = new System.Collections.Generic.List<'T>(items.Count)
            for item in items do
                result.Add(JsonDeserializerImpl<'T>.Deserialize item)
            result :> IEnumerable<'T>
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.List into a collection that implements IList.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <typeparam name="'L">The list type, which must have a parameterless constructor.</typeparam>
    /// <param name="result">An empty instance of the list to populate.</param>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>The populated list.</returns>
    static member internal IListDeserialize<'T, 'L when 'L :> IList<'T>>(result: 'L) (jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            for item in items do
                result.Add(JsonDeserializerImpl<'T>.Deserialize item)
            result
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.List into an F# list.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>An F# list.</returns>
    static member internal FSharpListDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            [
                for item in items do
                    JsonDeserializerImpl<'T>.Deserialize item
            ]
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.List into an array.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>An array of type 'T.</returns>
    static member internal ArrayDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            let arr = Array.init items.Count (fun i -> JsonDeserializerImpl<'T>.Deserialize items.[i])
            arr
        | other ->
            failwithf "Expected JsonValue.List but got %A" other
    
    /// <summary>
    /// Serializes a dictionary (IDictionary or IReadOnlyDictionary) into a JsonValue.Object.
    /// </summary>
    /// <typeparam name="'key">The key type of the dictionary.</typeparam>
    /// <typeparam name="'value">The value type of the dictionary.</typeparam>
    /// <param name="dictionary">The dictionary object to serialize.</param>
    /// <returns>A JsonValue.Object.</returns>
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

    /// <summary>
    /// Deserializes a JsonValue.List into a HashSet.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>A HashSet&lt;'T&gt;.</returns>
    static member internal HashSetDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            let result = new HashSet<'T>()
            for item in items do
                result.Add(JsonDeserializerImpl<'T>.Deserialize item) |> ignore
            result
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.List into a Queue.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>A Queue&lt;'T&gt;.</returns>
    static member internal QueueDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            let result = new Queue<'T>()
            for item in items do
                result.Enqueue(JsonDeserializerImpl<'T>.Deserialize item)
            result
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.List into a Stack.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>A Stack&lt;'T&gt;.</returns>
    static member internal StackDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            // For stack, we need to process items in reverse order to maintain order
            let result = new Stack<'T>()
            for i = items.Count - 1 downto 0 do
                result.Push(JsonDeserializerImpl<'T>.Deserialize items.[i])
            result
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.List into a LinkedList.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>A LinkedList&lt;'T&gt;.</returns>
    static member internal LinkedListDeserialize<'T>(jsonArray: JsonValue) =
        match jsonArray with
        | List items ->
            let result = new LinkedList<'T>()
            for item in items do
                result.AddLast(JsonDeserializerImpl<'T>.Deserialize item) |> ignore
            result
        | other ->
            failwithf "Expected JsonValue.List but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.Object into an IDictionary.
    /// </summary>
    /// <typeparam name="'TKey">The key type.</typeparam>
    /// <typeparam name="'TValue">The value type.</typeparam>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>An IDictionary&lt;'TKey, 'TValue&gt;.</returns>
    static member internal DictionaryDeserialize<'TKey, 'TValue when 'TKey : equality>(jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            let result = new Dictionary<'TKey, 'TValue>(properties.Count)
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedKey = ConvertStringTo<'TKey>.Convert key
                    let typedValue = JsonDeserializerImpl<'TValue>.Deserialize value
                    result.Add(typedKey, typedValue)
            result :> IDictionary<'TKey, 'TValue>
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.Object into an IReadOnlyDictionary.
    /// </summary>
    /// <typeparam name="'TKey">The key type.</typeparam>
    /// <typeparam name="'TValue">The value type.</typeparam>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>An IReadOnlyDictionary&lt;'TKey, 'TValue&gt;.</returns>
    static member internal IReadOnlyDictionaryDeserialize<'TKey, 'TValue when 'TKey : equality>(jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            let dict = new Dictionary<'TKey, 'TValue>(properties.Count)
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedKey = ConvertStringTo<'TKey>.Convert key

                    let typedValue = JsonDeserializerImpl<'TValue>.Deserialize value
                    dict.Add(typedKey, typedValue)
            dict :> IReadOnlyDictionary<'TKey, 'TValue>
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.Object into a non-generic IDictionary (Hashtable).
    /// </summary>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>An IDictionary.</returns>
    static member internal NonGenericDictionaryDeserialize(jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            let result = new Hashtable(properties.Count)
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedValue = JsonDeserializerImpl<obj>.Deserialize value
                    result.Add(key, typedValue)
            result :> IDictionary
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.Object into a custom dictionary type that implements IDictionary.
    /// </summary>
    /// <typeparam name="'TKey">The key type.</typeparam>
    /// <typeparam name="'TValue">The value type.</typeparam>
    /// <typeparam name="'TDict">The concrete dictionary type, which must have a parameterless constructor.</typeparam>
    /// <param name="result">An empty instance of the dictionary to populate.</param>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>The populated dictionary.</returns>
    static member internal CustomDictionaryDeserialize<'TKey, 'TValue, 'TDict when 'TDict :> IDictionary<'TKey, 'TValue>> (result: 'TDict) (jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedKey = ConvertStringTo<'TKey>.Convert key
                    let typedValue = JsonDeserializerImpl<'TValue>.Deserialize value
                    result.Add(KeyValuePair<'TKey, 'TValue>(typedKey, typedValue))
            result
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    /// <summary>
    /// Deserializes a JsonValue.Object into a custom non-generic dictionary type.
    /// </summary>
    /// <typeparam name="'TDict">The concrete dictionary type, which must have a parameterless constructor.</typeparam>
    /// <param name="result">An empty instance of the dictionary to populate.</param>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>The populated dictionary.</returns>
    static member internal CustomNonGenericDictionaryDeserialize<'TDict when 'TDict :> IDictionary> (result: 'TDict) (jsonObj: JsonValue) =
        match jsonObj with
        | Object properties ->
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedValue = JsonDeserializerImpl<obj>.Deserialize value
                    result.Add(key, typedValue)
            result
        | other ->
            failwithf "Expected JsonValue.Object but got %A" other

    /// <summary>
    /// A cached function to deserialize a JsonValue to a specific runtime type.
    /// </summary>
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

    /// <summary>
    /// A cached function to serialize an object of a specific runtime type to a JsonValue, including type information.
    /// </summary>
    static member val internal SerializeByTypeWithType =
        let cache = ConcurrentDictionary<Type, Func<obj, JsonValue>>()
        (fun (runtimeType: Type) (o: obj) ->
            let fn = 
                cache.GetOrAdd(runtimeType, Func<Type, Func<obj, JsonValue>>(fun (t: Type) ->
                    let p = Expression.Parameter typeof<obj>
                    
                    let meth = 
                        typedefof<JsonSerializerImpl<_>>
                            .MakeGenericType(t)
                            .GetMethod(nameof JsonSerializerImpl<_>.SerializeWithType, BindingFlags.NonPublic ||| BindingFlags.Static)
                    
                    let fn = 
                        Expression.Lambda<Func<obj, JsonValue>>(
                            Expression.Call(meth, Expression.Convert(p, t)),
                            [|p|]
                        ).Compile(false)

                    fn
                ))

            fn.Invoke o
        )

    /// <summary>
    /// Recursively checks if a ReadOnlySpan of characters contains only digits.
    /// </summary>
    /// <param name="x">The span to check.</param>
    /// <returns>True if all characters are digits, false otherwise.</returns>
    static member private IsAllDigits(x: ReadOnlySpan<char>) =
        if x.Length = 0 then
            true
        else

        if not (Char.IsDigit x.[0]) then
            false
        else
            JsonImpl.IsAllDigits (x.Slice 1)

    /// <summary>
    /// Checks if a string can be parsed as a number.
    /// </summary>
    /// <param name="x">The span to check.</param>
    /// <returns>True if the string represents a valid number, false otherwise.</returns>
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

    /// <summary>
    /// Deserializes a JsonValue into a dynamic object (ExpandoObject for objects, List&lt;obj&gt; for arrays).
    /// </summary>
    /// <param name="json">The JsonValue to deserialize.</param>
    /// <returns>A dynamic object representation.</returns>
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

/// <summary>
/// Provides a cached, compiled deserialization function for a specific type 'A.
/// </summary>
/// <typeparam name="'A">The target type for deserialization.</typeparam>
and private JsonDeserializerImpl<'A> =
    /// <summary>
    /// A static function delegate for the compiled deserializer.
    /// </summary>
    /// <param name="v">The JsonValue to deserialize.</param>
    /// <returns>An object of type 'A.</returns>
    static member internal DeserializeFunc(v: JsonValue) =
        JsonDeserializerImpl<'A>.Deserialize v

    /// <summary>
    /// The main deserialization logic, compiled into a static function for performance.
    /// This member handles deserialization for all supported types by generating and compiling expression trees.
    /// </summary>
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
            (fun (json: JsonValue) -> 
                match json with
                | String s when s.Length = 1 && not (Char.IsDigit s.[0]) -> s.[0]
                | _ -> asNumberOrParseToNumber json char char
            ) :> obj :?> (JsonValue -> 'A)

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

        | OfType (id: bigint -> bigint) -> 
            (fun (json: JsonValue) -> asNumberOrParseToNumber json bigint (fun s -> BigInteger.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture))) :> obj :?> (JsonValue -> 'A)

        | OfType (id: IntPtr -> IntPtr) -> 
            (fun (json: JsonValue) -> asNumberOrParseToNumber json (fun d -> d |> int64 |> IntPtr) (fun s -> int64 s |> IntPtr)) :> obj :?> (JsonValue -> 'A)

        | OfType (id: UIntPtr -> UIntPtr) -> 
            (fun (json: JsonValue) -> asNumberOrParseToNumber json (fun d -> d |> uint64 |> UIntPtr) (fun s -> uint64 s |> UIntPtr)) :> obj :?> (JsonValue -> 'A)

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
            

            let listFunc =
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

                fn.Compile(false)

            let objFunc =
                // Create expressions for each item in the tuple
                let itemExpressions = 
                    tupleItemTypes 
                    |> Array.mapi (fun i itemType ->
                        let itemJsonExpr = Expression.Call(
                            jsonParam,
                            typeof<JsonValue>.GetMethod("get_Item", [| typeof<string> |]),
                            [| Expression.Constant("Item" + string (i + 1)) :> Expression |]
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

                fn.Compile(false)
            
            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | JsonValue.List _ -> listFunc.Invoke json
                | JsonValue.Object _ -> objFunc.Invoke json
                | _ -> failwithf "Cannot deserialize into %s the value: %A" typeof<'A>.FullName (json.JsonType)
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
                let (callExpr: Expression) = 
                    if t.IsArray then
                        // For arrays use ArrayDeserialize
                        let method = typeof<JsonImpl>
                                        .GetMethod(nameof JsonImpl.ArrayDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                        .MakeGenericMethod(elemType)
                        Expression.Call(method, [|jsonParam :> Expression|])
                    // Check for F# list type
                    elif t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<list<_>> then
                        let method = typeof<JsonImpl>
                                        .GetMethod(nameof JsonImpl.FSharpListDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                        .MakeGenericMethod(elemType)
                        Expression.Call(method, [|jsonParam :> Expression|])
                    // Support for HashSet<T>
                    elif t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<HashSet<_>> then
                        let method = typeof<JsonImpl>
                                        .GetMethod(nameof JsonImpl.HashSetDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                        .MakeGenericMethod(elemType)
                        Expression.Call(method, [|jsonParam :> Expression|])
                    // Support for Queue<T>
                    elif t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Queue<_>> then
                        let method = typeof<JsonImpl>
                                        .GetMethod(nameof JsonImpl.QueueDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                        .MakeGenericMethod(elemType)
                        Expression.Call(method, [|jsonParam :> Expression|])
                    // Support for Stack<T>
                    elif t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Stack<_>> then
                        let method = typeof<JsonImpl>
                                        .GetMethod(nameof JsonImpl.StackDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                        .MakeGenericMethod(elemType)
                        Expression.Call(method, [|jsonParam :> Expression|])
                    // Support for LinkedList<T>
                    elif t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<LinkedList<_>> then
                        let method = typeof<JsonImpl>
                                        .GetMethod(nameof JsonImpl.LinkedListDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                        .MakeGenericMethod(elemType)
                        Expression.Call(method, [|jsonParam :> Expression|])
                    elif typeof<IList>.IsAssignableFrom(t) && t.IsGenericType && (GenericTypeArgCache.Get t).Length = 1 then
                        // For IList types that need an instance (like List<T>)
                        if t.GetConstructor([||]) <> null then
                            // If type has parameterless constructor, create instance for IListDeserialize
                            let instance = Expression.New(t)
                            let method = typeof<JsonImpl>
                                            .GetMethod(nameof JsonImpl.IListDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                            .MakeGenericMethod([|elemType; t|])
                            Expression.Call(method, [|instance :> Expression; jsonParam|])
                        else
                            // If no parameterless constructor, fall back to IEnumerableDeserialize
                            let method = typeof<JsonImpl>
                                            .GetMethod(nameof JsonImpl.IEnumerableDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                            .MakeGenericMethod(elemType)
                            Expression.Call(method, [|jsonParam :> Expression|])
                    else
                        let ienumerableType = typedefof<IEnumerable<_>>.MakeGenericType(elemType)
                        let constructor = t.GetConstructor([|ienumerableType|])
                        
                        let method = typeof<JsonImpl>
                                            .GetMethod(nameof JsonImpl.IEnumerableDeserialize, BindingFlags.NonPublic ||| BindingFlags.Static)
                                            .MakeGenericMethod(elemType)
                        let expression = Expression.Call(method, [|jsonParam :> Expression|])

                        if constructor = null then
                            // No suitable constructor, use default IEnumerableDeserialize
                            expression
                            
                        else
                            // Found constructor that takes IEnumerable<elemType>
                            Expression.New(constructor, [|expression :> Expression|])
            
            
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

            let hasIsCase (json: JsonValue) (caseIsName: string) =
                match json.TryGetProperty caseIsName with
                | true, Boolean true -> true
                | _, _ -> false

            let hasIsCase = Func<JsonValue, string, bool> hasIsCase
            let hasIsCase = Expression.Constant hasIsCase :> Expression
            let invoke = typeof<Func<JsonValue, string, bool>>.GetMethod "Invoke"

            let rec getForCaseOr (cases: UnionCaseInfo list) (orElse: Expression) =
                match cases with
                | [] -> orElse
                | case :: cases ->
                    Expression.Condition(
                        Expression.Call(hasIsCase, invoke, seq {jsonParam :> Expression; Expression.Constant $"Is{case.Name}"}), 
                        (let c = deserializeForCase case :> Expression in c),
                        (let other = getForCaseOr cases orElse in other))
                

            let body = getForCaseOr (unionCases |> Array.toList) switch

            let lambda = Expression.Lambda<Func<JsonValue, 'A>>(
                    body,
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

        | t when t.IsValueType ->
            let propertiesInfo = t.GetProperties()
            let fields = t.GetFields()

            let jsonDictParam = Expression.Parameter(typeof<IDictionary<string, JsonValue>>)
            let outVar = Expression.Variable(t)
            
            let outVarInitilization =
                let constr = t.GetConstructors() |> Seq.tryFind(fun c -> c.GetParameters().Length = 0 && (c.IsPublic || c.IsPrivate))
                match constr with
                | Some constr ->
                    Expression.New(constr, [||]) :> Expression
                | None ->
                    Expression.Default t
            

            let lambda = Expression.Lambda<Func<IDictionary<string, JsonValue>, 'A>>(
                Expression.Block(
                [|outVar|],
                [|
                    Expression.Assign(outVar, outVarInitilization) :> Expression;

                    for field in fields do
                        let variableValue = Expression.Variable typeof<JsonValue>
                        Expression.Block([|variableValue|],
                            [|Expression.IfThen(
                                Expression.Call(
                                    jsonDictParam,
                                    typeof<IDictionary<string, JsonValue>>.GetMethod("TryGetValue"),
                                    Expression.Constant(field.Name),
                                    variableValue
                                ),
                                
                                Expression.Assign(
                                    Expression.Field(outVar, field), 
                                    
                                    Expression.Call(
                                        typedefof<JsonDeserializerImpl<_>>
                                            .MakeGenericType(field.FieldType)
                                            .GetMethod(nameof JsonDeserializerImpl<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static),
                                        variableValue))
                                
                                ) :> Expression
                            |])

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


                    outVar;
                |]),
                [|jsonDictParam|]
            )
            
            let fn = lambda.Compile(false)
            
            // This is a struct, there is no need to check the $type
            (fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> Unchecked.defaultof<'A>
                | _ -> 
                    let dict = JsonImpl.AsDictOrFail json
                    fn.Invoke dict)

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

/// <summary>
/// Provides a cached, compiled serialization function for a specific type 'A.
/// </summary>
/// <typeparam name="'A">The source type for serialization.</typeparam>
and private JsonSerializerImpl<'A> =
    /// <summary>
    /// A static function delegate for the compiled serializer.
    /// </summary>
    /// <param name="a">The object of type 'A to serialize.</param>
    /// <returns>A JsonValue.</returns>
    static member internal SerializeFunc(a: 'A) : JsonValue =
        JsonSerializerImpl<'A>.Serialize a

    /// <summary>
    /// Serializes an object and adds a "$type" property to the resulting JSON object.
    /// </summary>
    /// <param name="value">The object to serialize.</param>
    /// <returns>A JsonValue with type information.</returns>
    static member internal SerializeWithType (value: 'A) : JsonValue =
        let json = JsonSerializerImpl<'A>.Serialize value
        match json, value.GetType() |> typeToName with
        | Object _, Some t -> json["$type"] <- String t
        | _other, Some _t -> () // Ignore
        | _other, None -> () // Also ignore
        json

    /// <summary>
    /// The main serialization logic, compiled into a static function for performance.
    /// This member handles serialization for all supported types by generating and compiling expression trees.
    /// </summary>
    static member val internal Serialize: 'A -> JsonValue = 
        match typeof<'A> with
        | t when t = typeof<obj> -> 
            (fun (o: obj) -> 
                match o with
                | null -> JsonValue.Null
                | _ -> 
                    let t = o.GetType()
                    if t = typeof<obj> then
                        JsonValue.New()
                    else
                        JsonImpl.SerializeByTypeWithType t o)
            :> obj :?> 'A -> JsonValue

        | t when t.IsAbstract -> 
            (fun (o: 'A) ->
                let o = box o
                match o with
                | null -> JsonValue.Null
                | _ -> JsonImpl.SerializeByTypeWithType (o.GetType()) o)
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

        | OfType (id: bigint -> bigint) -> 
            (fun (o: bigint) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)


        | OfType (id: IntPtr -> IntPtr) -> 
            (fun (o: IntPtr) -> Number (decimal o))
            :> obj :?> ('A -> JsonValue)

        | OfType (id: UIntPtr -> UIntPtr) -> 
            (fun (o: UIntPtr) -> Number (decimal o))
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

        | t when isTuple t ->
            let tupleItemTypes = GenericTypeArgCache.Get t
            let param = Expression.Parameter(t)
            let outList = Expression.Variable(typeof<List<JsonValue>>)
            
            // Get the tuple item properties (Item1, Item2, etc.)
            let tupleProps = 
                [| for i in 1..tupleItemTypes.Length do
                    t.GetProperty($"Item{i}") |] |> Array.filter(fun f -> not (isNull f))

            let tupleFields = 
                [| for i in 1..tupleItemTypes.Length do
                    t.GetField($"Item{i}") |] |> Array.filter(fun f -> not (isNull f))
            
            let lambda = Expression.Lambda<Func<'A, List<JsonValue>>>(
                Expression.Block(
                    [|outList|],
                    [|
                        // Initialize list
                        Expression.Assign(outList, Expression.New(
                            typeof<List<JsonValue>>.GetConstructor([|typeof<int>|]),
                            [|Expression.Constant(tupleProps.Length + tupleFields.Length) :> Expression|]
                        )) :> Expression
                        
                        if t.IsValueType then
                            for field in tupleFields do
                                Expression.Call(outList, typeof<List<JsonValue>>.GetMethod("Add"), [|
                                    Expression.Call(
                                        typedefof<JsonSerializerImpl<_>>
                                            .MakeGenericType(field.FieldType)
                                            .GetMethod(nameof JsonSerializerImpl<obj>.SerializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static),
                                        [|Expression.Field(param, field) :> Expression|]) :> Expression
                                |])

                        // Add each tuple element to the list
                        for prop in tupleProps do
                            Expression.Call(outList, typeof<List<JsonValue>>.GetMethod("Add"), [|
                                Expression.Call(
                                    typedefof<JsonSerializerImpl<_>>
                                        .MakeGenericType(prop.PropertyType)
                                        .GetMethod(nameof JsonSerializerImpl<obj>.SerializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static),
                                    [|Expression.Property(param, prop) :> Expression|]) :> Expression
                            |])
                        
                        // Return the JsonValue list
                        outList
                    |]
                ),
                [|param|]
            )
            let fn = lambda.Compile(false)

            (fun (o: 'A) ->
                if obj.ReferenceEquals(o, null) 
                then Null
                else 
                    let jsonList = fn.Invoke o
                    JsonValue.List jsonList
            ) :> obj :?> ('A -> JsonValue)
        | t ->
            let props = t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance) |> Array.filter(_.CanRead)
            let fields = t.GetFields()

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

                        // Serialize fields only if it is a struct.
                        if t.IsValueType then
                            for field in fields do
                                Expression.Call(outDict, typeof<Dictionary<string, JsonValue>>.GetMethod("Add"), [|
                                    Expression.Constant(field.Name, typeof<string>) :> Expression;
                                    let serializeMeth = typedefof<JsonSerializerImpl<_>>
                                                            .MakeGenericType(field.FieldType)
                                                            .GetMethod(nameof JsonSerializerImpl<obj>.SerializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
                                    Expression.Call(
                                        serializeMeth,
                                         [|Expression.Field(param, field) :> Expression|])
                                |])

                        // Props have priority over fields.
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
                        RuntimeHelpers.EnsureSufficientExecutionStack()

                        let typeOfO = o.GetType()
                        if typeOfO <> typeof<'A> then
                            JsonImpl.SerializeByTypeWithType typeOfO (box o)
                        else
                            let json = fn.Invoke o
                            json)
                :> obj :?> ('A -> JsonValue)

/// <summary>
/// A custom DynamicMetaObject for JsonValue to enable dynamic operations in languages like C#.
/// </summary>
/// <param name="expression">The expression representing this DynamicMetaObject during the binding process.</param>
/// <param name="restrictions">The set of binding restrictions under which the binding is valid.</param>
/// <param name="value">The JsonValue instance this meta-object is for.</param>
and internal JsonValueMetaObject(expression: Expression, restrictions: BindingRestrictions, value: JsonValue) =
    inherit DynamicMetaObject(expression, restrictions, value)
    
    /// <summary>Cached MethodInfo for getting a property for the binder.</summary>
    static member val private GetPropertyMethod = typeof<JsonValue>.GetMethod("GetPropertyForBinder", BindingFlags.NonPublic ||| BindingFlags.Instance)
    /// <summary>Cached MethodInfo for setting an item via the indexer.</summary>
    static member val private SetPropertyMethod = typeof<JsonValue>.GetMethod("set_Item")
    /// <summary>Cached MethodInfo for the object.ToString method.</summary>
    static member val private ToStringMethod = typeof<obj>.GetMethod("ToString")
    /// <summary>Cached MethodInfo for the generic JsonValue.Serialize method.</summary>
    static member val private SerializeMethod = typeof<JsonValue>.GetMethod("Serialize").MakeGenericMethod(typeof<obj>)

    /// <summary>
    /// Binds a dynamic get-member operation (e.g., `dynamicJson.PropertyName`).
    /// </summary>
    /// <param name="binder">The binder for the get-member operation.</param>
    /// <returns>A new DynamicMetaObject representing the result of the binding.</returns>
    override this.BindGetMember(binder: GetMemberBinder) : DynamicMetaObject =
        let resultExpression = Expression.Call(Expression.Convert(this.Expression, typeof<JsonValue>), JsonValueMetaObject.GetPropertyMethod, Expression.Constant(binder.Name))
        DynamicMetaObject(resultExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))

    /// <summary>
    /// Binds a dynamic set-member operation (e.g., `dynamicJson.PropertyName = value`).
    /// </summary>
    /// <param name="binder">The binder for the set-member operation.</param>
    /// <param name="value">The DynamicMetaObject representing the value to set.</param>
    /// <returns>A new DynamicMetaObject representing the result of the binding.</returns>
    override this.BindSetMember(binder: SetMemberBinder, value: DynamicMetaObject) : DynamicMetaObject =
        let setExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<JsonValue>), 
            JsonValueMetaObject.SetPropertyMethod, 
            Expression.Constant(binder.Name),
            Expression.Call(JsonValueMetaObject.SerializeMethod, Expression.Convert(value.Expression, typeof<obj>))
        )

        let returnExpre = Expression.Block(setExpression, Expression.Convert(value.Expression, typeof<obj>))

        DynamicMetaObject(returnExpre, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))
    
    /// <summary>
    /// Binds a dynamic conversion operation (e.g., `(MyType)dynamicJson`).
    /// </summary>
    /// <param name="binder">The binder for the conversion operation.</param>
    /// <returns>A new DynamicMetaObject representing the result of the binding.</returns>
    override this.BindConvert(binder: ConvertBinder) : DynamicMetaObject =
        let convertMethod = typeof<JsonValue>.GetMethod("ToObject", [|binder.Type|])
        let convertExpression = Expression.Call(Expression.Convert(this.Expression, typeof<JsonValue>), convertMethod)
        DynamicMetaObject(convertExpression, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))
    
    /// <summary>
    /// Binds a dynamic get-index operation (e.g., `dynamicJson["index"]`).
    /// </summary>
    /// <param name="binder">The binder for the get-index operation.</param>
    /// <param name="indexes">An array of DynamicMetaObjects representing the indexes.</param>
    /// <returns>A new DynamicMetaObject representing the result of the binding.</returns>
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
        
    /// <summary>
    /// Binds a dynamic set-index operation (e.g., `dynamicJson["index"] = value`).
    /// </summary>
    /// <param name="binder">The binder for the set-index operation.</param>
    /// <param name="indexes">An array of DynamicMetaObjects representing the indexes.</param>
    /// <param name="value">The DynamicMetaObject representing the value to set.</param>
    /// <returns>A new DynamicMetaObject representing the result of the binding.</returns>
    override this.BindSetIndex (binder: SetIndexBinder, indexes: DynamicMetaObject array, value: DynamicMetaObject): DynamicMetaObject = 
        // Check that exactly one index is provided
        if indexes.Length <> 1 then
            failwithf "Json does not support indexes length <> 1: %i" indexes.Length
        
        let indexExpr = indexes.[0].Expression
            
        let setExpression = Expression.Call(
            Expression.Convert(this.Expression, typeof<JsonValue>), 
            JsonValueMetaObject.SetPropertyMethod,
            Expression.Call(indexExpr, JsonValueMetaObject.ToStringMethod),
            Expression.Call(JsonValueMetaObject.SerializeMethod, Expression.Convert(value.Expression, typeof<obj>))
        )

        let returnExpre = Expression.Block(setExpression, Expression.Convert(value.Expression, typeof<obj>))

        DynamicMetaObject(returnExpre, BindingRestrictions.GetTypeRestriction(this.Expression, this.LimitType))
    
    /// <summary>
    /// Returns the enumeration of all dynamic member names.
    /// </summary>
    /// <returns>A sequence of strings that contains dynamic member names.</returns>
    override this.GetDynamicMemberNames() : IEnumerable<string> =
        match value with
        | Object o -> seq { for kv in o do yield kv.Key }
        | _ -> Seq.empty