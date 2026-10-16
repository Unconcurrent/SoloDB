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
open System.Runtime.InteropServices
open Microsoft.FSharp.Reflection
open System.Linq.Expressions
open System.Reflection.Emit
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation
open System.Linq
open System.Numerics
open System.Runtime.Serialization
open SoloDatabase
open JsonEqualitySupport

#nowarn "9" // NativePtr stuff
#nowarn "51" // voidptr of a stack var

/// <summary>
/// Defines the type of a JsonValue, for use in C#.
/// </summary>
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

/// Direct mutation of an original model value, including value types.
type internal JsonMemberSetter<'T, 'Value> = delegate of byref<'T> * 'Value -> unit

/// Exact stored member identity and a direct value projection.
[<Sealed>]
type JsonStoredMember
    (declaringType: Type, valueType: Type, clrName: string, storedName: string, isField: bool, read: Func<obj, obj>) =
    do
        if isNull declaringType then nullArg "declaringType"
        if isNull valueType then nullArg "valueType"
        if isNull clrName then nullArg "clrName"
        if isNull storedName then nullArg "storedName"
        if isNull read then nullArg "read"
    member _.DeclaringType = declaringType
    member _.ValueType = valueType
    member _.ClrName = clrName
    member _.StoredName = storedName
    member _.IsField = isField
    member internal _.Reader = read
    member internal _.Access(parent) = BoundValueAccessExpression(parent, valueType, read) :> Expression
    static member internal Discover(memberInfo: MemberInfo) =
        let valueType, isField =
            match memberInfo with
            | :? PropertyInfo as property -> property.PropertyType, false
            | :? FieldInfo as field -> field.FieldType, true
            | _ -> invalidArg "memberInfo" "A serialized member must be a property or field."
        let read = DynamicMemberAccess.reader memberInfo
        QueryValueMetadata.RegisterReader(memberInfo.DeclaringType,memberInfo.Name,isField,valueType,read)
        JsonStoredMember(memberInfo.DeclaringType,valueType,memberInfo.Name,memberInfo.Name,isField,read)

/// Shape selected by the actual serializer dispatch, before any values are supplied.
type internal JsonSerializationShape =
    | Dynamic
    | Scalar
    | ObjectMembers of JsonStoredMember array
    | ArrayMembers of JsonStoredMember array
    | NullableValue of Type
    | SequenceValues of Type
    | DictionaryValues of Type
    | GroupingValues of Type * Type
    | DeclaredMembers of struct(string * Type) array
    | DeclaredArrayMembers of Type array
    | ReferenceIdentity of Type
    | Uninterpreted
    member this.IsScalarCarrier =
        match this with Scalar | ReferenceIdentity _ -> true | _ -> false

/// <summary>
/// Represents a JSON value, which can be a null, boolean, string, number, list, or object.
/// This is a discriminated union that forms the core of the JSON representation.
/// </summary>
[<CustomEquality; CustomComparison>]
type JsonValue =
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

    [<System.Diagnostics.CodeAnalysis.DynamicDependency(
        System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties, typeof<JsonValue>)>]
    static member internal InvalidValue<'T>(message: string): 'T = failwith message

    static member private EqualsCore (left: JsonValue) (right: JsonValue) : bool =
        let rec equals (l: JsonValue) (r: JsonValue) =
            match l, r with
            | Null, Null -> true
            | Boolean a, Boolean b -> a = b
            | String a, String b -> StringComparer.Ordinal.Equals(a, b)
            | Number a, Number b -> a = b
            | List a, List b ->
                if a.Count <> b.Count then false
                else
                    let rec loop i =
                        if i = a.Count then true
                        elif equals a.[i] b.[i] then loop (i + 1)
                        else false
                    loop 0
            | Object a, Object b ->
                if a.Count <> b.Count then false
                else
                    let find key =
                        match b with
                        | :? Dictionary<string, JsonValue> as dictionary
                            when dictionary.Comparer = EqualityComparer<string>.Default
                                 || dictionary.Comparer = StringComparer.Ordinal ->
                            match dictionary.TryGetValue key with
                            | true, value -> ValueSome value
                            | _ -> ValueNone
                        | _ ->
                            use entries = b.GetEnumerator()
                            let mutable result = ValueNone
                            while result.IsNone && entries.MoveNext() do
                                if StringComparer.Ordinal.Equals(entries.Current.Key, key) then
                                    result <- ValueSome entries.Current.Value
                            result
                    use e = a.GetEnumerator()
                    let rec loop () =
                        if e.MoveNext() then
                            let kvp = e.Current
                            match find kvp.Key with
                            | ValueSome value when equals kvp.Value value -> loop ()
                            | _ -> false
                        else
                            true
                    loop ()
            | _ -> false

        equals left right

    static member private CompareCore (left: JsonValue) (right: JsonValue) : int =
        let inline typeOrder (v: JsonValue) =
            match v with
            | Null -> 0
            | Boolean _ -> 1
            | Number _ -> 2
            | String _ -> 3
            | List _ -> 4
            | Object _ -> 5

        let compareObjectsStack
            (n: int)
            (a: IDictionary<string, JsonValue>)
            (b: IDictionary<string, JsonValue>)
            (valueCompare: JsonValue -> JsonValue -> int)
            : int =

            let mutable bufA = Unchecked.defaultof<SoloDatabase.InternalStack128<KeyValuePair<string, JsonValue>>>
            let mutable bufB = Unchecked.defaultof<SoloDatabase.InternalStack128<KeyValuePair<string, JsonValue>>>

            let mutable i = 0
            for kv in a do
                bufA.set_Item (i, kv)
                i <- i + 1

            i <- 0
            for kv in b do
                bufB.set_Item (i, kv)
                i <- i + 1

            SoloDatabase.Sort.heapSortStack128 &bufA n KvpComparison<JsonValue>.KvpKeyCmp
            SoloDatabase.Sort.heapSortStack128 &bufB n KvpComparison<JsonValue>.KvpKeyCmp

            JsonHelper.compareObjectsCoreStack128 (n, &bufA, &bufB, valueCompare)

        let compareObjectsHeap
            (n: int)
            (a: IDictionary<string, JsonValue>)
            (b: IDictionary<string, JsonValue>)
            (valueCompare: JsonValue -> JsonValue -> int)
            : int =

            let mutable arrA = Array.zeroCreate<KeyValuePair<string, JsonValue>> n
            let mutable arrB = Array.zeroCreate<KeyValuePair<string, JsonValue>> n
                                
            let mutable i = 0
            for kv in a do
                arrA[i] <- kv
                i <- i + 1

            i <- 0
            for kv in b do
                arrB[i] <- kv
                i <- i + 1

            Array.Sort(arrA, KvpComparison<JsonValue>.KvpKeyCmp)
            Array.Sort(arrB, KvpComparison<JsonValue>.KvpKeyCmp)

            JsonHelper.compareObjectsCoreArray (n, arrA, arrB, valueCompare)

        let compareObjects
            (depth: int)
            (a: IDictionary<string, JsonValue>)
            (b: IDictionary<string, JsonValue>)
            (valueCompare: JsonValue -> JsonValue -> int)
            : int =

            let n = a.Count
            let countCompare = compare n b.Count
            if countCompare <> 0 then countCompare
            else
                let useStack = n <= 128 && depth < 64

                if useStack then
                    compareObjectsStack n a b valueCompare
                else
                    compareObjectsHeap n a b valueCompare
                

        let rec compareValues (depth: int) (l: JsonValue) (r: JsonValue) =
            let tl = typeOrder l
            let tr = typeOrder r
            if tl <> tr then compare tl tr
            else
                match l, r with
                | Null, Null -> 0
                | Boolean a, Boolean b -> compare a b
                | Number a, Number b -> a.CompareTo b
                | String a, String b -> StringComparer.Ordinal.Compare(a, b)
                | List a, List b ->
                    let minCount = if a.Count < b.Count then a.Count else b.Count
                    let rec loop i =
                        if i = minCount then compare a.Count b.Count
                        else
                            let c = compareValues (depth + 1) a[i] b[i]
                            if c = 0 then loop (i + 1) else c
                    loop 0
                | Object a, Object b ->
                    compareObjects (depth + 1) a b (fun x y -> compareValues (depth + 1) x y)
                | _ -> 0

        compareValues 0 left right

    static member private HashCore (value: JsonValue) : int =
        let inline hashDecimal (n: decimal) = uint32 (n.GetHashCode())


        let inline hashString (s: string) =
            SoloDatabase.Hashing.xxHash32Chars (s.AsSpan()) 0u

        let inline hashU32WithSeed (seed: uint32) (value: uint32) =
            let mutable v = value
            let span = ReadOnlySpan<byte>((NativeInterop.NativePtr.toVoidPtr &&v), sizeof<uint32>)
            SoloDatabase.Hashing.xxHash32 (span) seed

        let rec hashValue (v: JsonValue) : uint32 =
            match v with
            | Null -> 0u
            | Boolean b -> if b then 1u else 0u
            | String s -> hashString s
            | Number n -> hashDecimal n
            | List items ->
                let mutable h = 0u
                for i = 0 to items.Count - 1 do
                    let hv = hashValue items.[i]
                    h <- hashU32WithSeed h hv
                h
            | Object obj ->
                let mutable h = 0u
                for kvp in obj do
                    let hk = hashString kvp.Key
                    let hv = hashValue kvp.Value
                    let entry = hashU32WithSeed (hashU32WithSeed 0u hk) hv
                    h <- h ^^^ entry
                h ^^^ hashU32WithSeed 0u (uint32 obj.Count)

        let h32 = hashValue value
        int h32

    override this.Equals(other: obj) =
        match other with
        | :? JsonValue as json -> JsonValue.EqualsCore this json
        | _ -> false

    override this.GetHashCode() =
        JsonValue.HashCore this

    interface IEquatable<JsonValue> with
        member this.Equals(other: JsonValue) =
            JsonValue.EqualsCore this other

    interface IComparable with
        member this.CompareTo(other: obj) =
            match other with
            | null -> JsonValue.CompareCore this Null
            | :? JsonValue as json -> JsonValue.CompareCore this json
            | _ -> invalidArg "other" "Object is not a JsonValue."

    interface IComparable<JsonValue> with
        member this.CompareTo(other: JsonValue) =
            JsonValue.CompareCore this other

    /// <summary>
    /// Serializes a given object of type 'T into a JsonValue.
    /// </summary>
    /// <typeparam name="'T">The type of the object to serialize.</typeparam>
    /// <param name="t">The object instance to serialize.</param>
    /// <returns>A JsonValue representation of the object.</returns>
    static member Serialize<'T>(t: 'T) : JsonValue =
        JsonWriteOperations<'T>.Serialize t

    /// Writes typed JSON to UTF-8 storage, growing it when required, and returns the written byte count.
    /// A release callback requires a custom grow callback. The final span remains caller-owned.
    static member WriteJson<'T>(value: 'T, destination: byref<Span<byte>>,
                               [<Optional; DefaultParameterValue(null: JsonBufferGrow<byte>)>] grow: JsonBufferGrow<byte>,
                               [<Optional; DefaultParameterValue(null: JsonBufferRelease<byte>)>] release: JsonBufferRelease<byte>): int =
        let mutable state = JsonWriteState<byte>(null,grow,release)
        JsonRawWrite<'T,byte,JsonUtf8Writer,JsonNativeContract>.Write(value,&destination,&state)
        state.Written

    /// Writes typed JSON to UTF-16 storage, growing it when required, and returns the written character count.
    /// A release callback requires a custom grow callback. The final span remains caller-owned.
    static member WriteJson<'T>(value: 'T, destination: byref<Span<char>>,
                               [<Optional; DefaultParameterValue(null: JsonBufferGrow<char>)>] grow: JsonBufferGrow<char>,
                               [<Optional; DefaultParameterValue(null: JsonBufferRelease<char>)>] release: JsonBufferRelease<char>): int =
        let mutable state = JsonWriteState<char>(null,grow,release)
        JsonRawWrite<'T,char,JsonUtf16Writer,JsonNativeContract>.Write(value,&destination,&state)
        state.Written

    /// Appends typed UTF-8 JSON and returns the number of bytes added.
    static member WriteJson<'T>(value: 'T, destination: IList<byte>): int =
        if isNull destination then nullArg "destination"
        let mutable state = JsonWriteState<byte>(destination,null,null)
        let mutable span = Span<byte>.Empty
        JsonRawWrite<'T,byte,JsonUtf8Writer,JsonNativeContract>.Write(value,&span,&state)
        state.Written

    /// Appends typed UTF-16 JSON and returns the number of characters added.
    static member WriteJson<'T>(value: 'T, destination: IList<char>): int =
        if isNull destination then nullArg "destination"
        let mutable state = JsonWriteState<char>(destination,null,null)
        let mutable span = Span<char>.Empty
        JsonRawWrite<'T,char,JsonUtf16Writer,JsonNativeContract>.Write(value,&span,&state)
        state.Written

    static member internal WriteBuffer<'T>(value: 'T, includeType: bool, omitId: bool): ArraySegment<char> =
        let mutable destination = Span<char>.Empty
        let mutable state = JsonWriteState<char>(null,null,null)
        if includeType then (box value).GetType() |> ignore
        state.IncludeType <- includeType
        state.OmitId <- omitId
        JsonRawWrite<'T,char,JsonUtf16Writer,JsonStoredContract>.Write(value,&destination,&state)
        ArraySegment<char>(state.Storage,0,state.Written)

    /// Runtime tree values retain their explicit writer rather than a union-case implementation type.
    static member internal RuntimeWriterType(value: obj, actual: Type) =
        if value :? JsonValue then typeof<JsonValue> else actual

    static member internal WriteRuntimeBuffer(targetType: Type, value: obj, includeType: bool, omitId: bool): ArraySegment<char> =
        let mutable destination = Span<char>.Empty
        let mutable state = JsonWriteState<char>(null,null,null)
        state.IncludeType <- includeType
        state.OmitId <- omitId
        JsonRawWriteMetadata<char,JsonUtf16Writer,JsonStoredContract>.ForType(targetType,value,&destination,&state)
        ArraySegment<char>(state.Storage,0,state.Written)

    static member internal WriteText<'T>(value: 'T, includeType: bool, omitId: bool): string =
        let text = JsonValue.WriteBuffer(value,includeType,omitId)
        System.String(text.Array,text.Offset,text.Count)

    static member internal WritePhysicalIds(count: int, idAt: int -> int64): string =
        let mutable destination = Span<char>.Empty
        let mutable state = JsonWriteState<char>(null,null,null)
        JsonRawOutput<char,JsonUtf16Writer,JsonNativeContract>.BeginArray(&destination,&state)
        for index = 0 to count - 1 do
            if index > 0 then JsonRawOutput<char,JsonUtf16Writer,JsonNativeContract>.Separator(&destination,&state)
            JsonRawNumericWriter.int64Utf16 (idAt index) &destination &state
        JsonRawOutput<char,JsonUtf16Writer,JsonNativeContract>.EndArray(&destination,&state)
        System.String(state.Storage,0,state.Written)

    static member internal ReadRawRuntime(context: byref<JsonReadContext<Utf16Reader>>, targetType: Type): obj =
        JsonRawReadMetadata<JsonStoredContract>.Runtime<Utf16Reader>(&context,targetType)

    static member internal ReadRawPhysicalId<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int64 =
        JsonRawRead<JsonNativeContract>.PhysicalId<'R> &context

    static member internal ParseRuntime(targetType: Type, text: ReadOnlySpan<char>): obj =
        let mutable context = JsonReadContext(JsonLexer.fromUtf16Span(text),{ Start = 0; Length = text.Length })
        let value = JsonRawReadMetadata<JsonStoredContract>.Runtime<Utf16Reader>(&context,targetType)
        JsonReadCursor.finish &context
        value

    static member internal ParseRuntime(targetType: Type, text: string): obj =
        JsonValue.ParseRuntime(targetType,text.AsSpan())

    static member private ReadStored<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): 'T =
        let value =
            if typeof<'T>.IsValueType && not (typeof<'T>.Equals(typeof<double>)) && not (typeof<'T>.Equals(typeof<single>))
                && JsonLexer.tryReadNull &context.Cursor then
                context.Consumed <- true
                Unchecked.defaultof<'T>
            else JsonRawRead<JsonStoredContract>.Read<'T,'R> &context
        JsonReadCursor.finish &context
        value

    static member internal ParseStored<'T>(text: ReadOnlySpan<char>): 'T =
        let mutable context = JsonReadContext(JsonLexer.fromUtf16Span(text),{ Start = 0; Length = text.Length })
        JsonValue.ReadStored<'T,Utf16Reader>(&context)

    static member internal ParseStored<'T>(text: ReadOnlySpan<byte>): 'T =
        let mutable context = JsonReadContext(JsonLexer.fromUtf8Span(text),{ Start = 0; Length = text.Length })
        JsonValue.ReadStored<'T,Utf8Reader>(&context)

    static member internal ParseStored<'T>(text: string): 'T =
        JsonValue.ParseStored<'T>(text.AsSpan())

    static member private PrepareStoredParameter(targetType: Type, value: obj, comparison: bool): obj * bool =
        let text = JsonValue.WriteRuntimeBuffer(targetType,value,false,false)
        let mutable context = JsonReadContext(JsonLexer.fromUtf16Span(ReadOnlySpan(text.Array,text.Offset,text.Count)),{ Start = 0; Length = text.Count })
        // The writer's actual output controls native storage, after its user effects.
        if JsonReadCursor.numericStart &context then
            let actual = if typeof<obj>.Equals(targetType) && not(isNull value) then value.GetType() else targetType
            let underlying = Utils.optionalPayloadType actual
            if comparison && (typeof<uint64>.Equals(underlying) || UIntPtr.Size = 8 && typeof<unativeint>.Equals(underlying)) then
                box(JsonRawRead<JsonNativeContract>.Intrinsic<uint64,Utf16Reader> &context),false
            elif (typeof<double>.Equals(underlying) || typeof<single>.Equals(underlying)) then box(JsonRawRead<JsonNativeContract>.Intrinsic<double,Utf16Reader> &context),false
            elif (typeof<int64>.Equals(underlying) || typeof<int32>.Equals(underlying) || typeof<int16>.Equals(underlying) || typeof<sbyte>.Equals(underlying)) then box(JsonRawRead<JsonNativeContract>.Intrinsic<int64,Utf16Reader> &context),false
            else
                let number = JsonRawRead<JsonNativeContract>.Intrinsic<JsonSqlNumber,Utf16Reader> &context
                if number.Encoded then box text,true
                else
                    match number.Value with
                    | :? decimal as value -> numberToSQLValue value,false
                    | :? double as value when Double.IsNaN value || Double.IsInfinity value || BitConverter.DoubleToInt64Bits(value) = Int64.MinValue -> box text,true
                    | value -> value,false
        else
            match JsonReadCursor.token &context with
            | NullToken -> null,false
            | BooleanToken value -> box(if value then 1L else 0L),false
            | StringToken value -> box value,false
            | _ -> box text,true

    static member internal StoredParameter(targetType: Type, value: obj): obj * bool =
        JsonValue.PrepareStoredParameter(targetType,value,false)

    static member internal StoredComparisonParameter(targetType: Type, value: obj): obj * bool =
        JsonValue.PrepareStoredParameter(targetType,value,true)

    static member internal WriteProperties(values: seq<KeyValuePair<string,obj>>): string =
        let mutable destination = Span<char>.Empty
        let mutable state = JsonWriteState<char>(null,null,null)
        JsonRawOutput<char,JsonUtf16Writer,JsonNativeContract>.BeginObject(&destination,&state)
        let mutable comma = false
        for pair in values do
            JsonRawOutput<char,JsonUtf16Writer,JsonNativeContract>.Property(pair.Key,comma,&destination,&state)
            comma <- true
            JsonRawWrite<obj,char,JsonUtf16Writer,JsonNativeContract>.Member(pair.Value,&destination,&state)
        JsonRawOutput<char,JsonUtf16Writer,JsonNativeContract>.EndObject<obj>(comma,true,&destination,&state)
        System.String(state.Storage,0,state.Written)

    static member internal SerializeRuntime(targetType: Type, value: obj, includeType: bool) =
        if includeType then JsonImpl.SerializeByTypeWithType targetType value
        else JsonImpl.SerializeByType targetType value

    /// Query compilation reads the serializer contract once and retains typed accessors.
    static member internal SerializationShape(t: Type) : JsonSerializationShape =
        match TypeMetadataCache.Read(t,fun metadata -> metadata.JsonWriter) with
        | ValueSome operation ->
            let struct(_,shape) = operation.Value
            unbox<JsonSerializationShape>(shape.Invoke())
        | ValueNone when MetadataRuntime.supportsDynamicCode() ->
            let serializer = typedefof<JsonWriteOperations<_>>.MakeGenericType(t)
            serializer.GetProperty("Shape", BindingFlags.NonPublic ||| BindingFlags.Static).GetValue(null) :?> JsonSerializationShape
        | ValueNone -> MetadataRuntime.missing "JSON shape" t

    static member internal SerializedValueFacts(t: Type) : JsonSerializationShape =
        match TypeMetadataCache.Read(t,fun metadata -> metadata.JsonWriter) with
        | ValueSome operation ->
            let struct(_,shape)=operation.Value
            unbox<JsonSerializationShape>(shape.Invoke())
        | ValueNone when MetadataRuntime.supportsDynamicCode() -> JsonCodecDiscovery.ValueShape t
        | ValueNone -> MetadataRuntime.missing "JSON shape" t

    /// Resolves optional wrappers through their actual serialized scalar carrier.
    static member internal HasScalarCarrier(t: Type) =
        let rec scalar target =
            match JsonValue.SerializationShape target with
            | shape when shape.IsScalarCarrier -> true
            | JsonSerializationShape.NullableValue payload -> scalar payload
            | _ -> false
        scalar t

    /// Uses the same declared-type selection as the original tree reader.
    static member internal ArrayItems(value: JsonValue) = JsonImpl.AsListOrFail value

    static member internal ResolveWriterType(declared: Type, headerPresent: bool, actualHeader: JsonValue) =
        JsonImpl.ResolveWriterType(declared, headerPresent, actualHeader)

    /// <summary>
    /// Serializes a given object of type 'T into a JsonValue, including a "$type" property with the object's type name.
    /// </summary>
    /// <typeparam name="'T">The type of the object to serialize.</typeparam>
    /// <param name="t">The object instance to serialize.</param>
    /// <returns>A JsonValue representation of the object with type information.</returns>
    static member SerializeWithType<'T>(t: 'T) : JsonValue =
        JsonWriteOperations<'T>.SerializeWithType t

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
        JsonReadOperations<'T>.Deserialize this

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
            | true, index -> index >= 0 && index < list.Count
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
            match Int32.TryParse (name, NumberStyles.Integer, CultureInfo.InvariantCulture) with
            | true, index when index >= 0 && index < list.Count ->
                v <- list.[index]
                true
            | _ ->
                v <- Unchecked.defaultof<JsonValue>
                false
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

    /// Append the existing JSON escape format directly to the destination.
    static member private AppendEscaped (text: string) (sb: StringBuilder) =
        JsonStringLiteral.appendStringBuilder text sb

    /// <summary>
    /// Recursively serializes a JsonValue to a string using a StringBuilder.
    /// </summary>
    /// <param name="value">The JsonValue to serialize.</param>
    /// <param name="sb">The StringBuilder to append the result to.</param>
    static member private Jsonize orderedObjects value (sb: StringBuilder) =
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
            JsonValue.AppendEscaped s sb
            writeChar '\"'
        | Number n -> 
            writeNumber n
        | List arr ->
            writeChar '['
            let items = arr.Count
            for i in 0..(items - 1) do
                let item = arr.[i]
                JsonValue.Jsonize orderedObjects item sb
                if i < items - 1 then writeChar ','
            writeChar ']'

        | Object obj ->
            writeChar '{'
            let items = obj.Count
            let mutable i = 0
            let members =
                if orderedObjects then
                    let members = Array.ofSeq obj
                    Array.sortInPlaceWith (fun (left: KeyValuePair<string, JsonValue>) (right: KeyValuePair<string, JsonValue>) -> StringComparer.Ordinal.Compare(left.Key, right.Key)) members
                    members :> seq<_>
                else obj :> seq<_>
            for kvp in members do
                writeChar '\"'
                JsonValue.AppendEscaped kvp.Key sb
                writeChar '\"'
                writeChar ':'
                JsonValue.Jsonize orderedObjects kvp.Value sb
                if i < items - 1 then writeChar ','
                i <- i + 1
            writeChar '}'

    /// Complete native tree equality uses private ordinal object ordering.
    /// The ordinary writer retains the original member enumeration.
    member internal this.EqualityKey() =
        let destination = StringBuilder(128)
        JsonValue.Jsonize true this destination
        destination.ToString()

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
        | String s ->
            let sb = StringBuilder(if isNull s then 2 else s.Length + 2)
            sb.Append('"') |> ignore
            JsonValue.AppendEscaped s sb
            sb.Append('"').ToString()
        | _ ->

        let sb = new StringBuilder(128)
        JsonValue.Jsonize false this sb
        sb.ToString()

    /// <summary>
    /// Compares this value structurally with the plain serialized value of the argument.
    /// Unlike EqLoose, equality requires the same object keys and array elements on both sides.
    /// </summary>
    /// <param name="other">A value to serialize by its runtime type, or a JsonValue used directly.</param>
    /// <returns>True when the complete JSON structures are equal.</returns>
    member this.Eq (other: obj) =
        let json =
            match other with
            | null -> JsonValue.Null
            | :? JsonValue as value -> value
            | value -> JsonImpl.SerializeByType (value.GetType()) value
        JsonValue.EqualsCore this json

    /// <summary>
    /// Tests whether this stored value matches the argument as an object query predicate would.
    /// Extra stored object fields are allowed; missing fields compare as null, and missing
    /// type discriminators are accepted. The argument uses query serialization, with JsonValue passthrough.
    /// Use Eq for symmetric equality of complete JSON structures.
    /// </summary>
    /// <remarks>
    /// Property names follow SQLite JSON lookup: ordinal comparison, terminating at the first NUL.
    /// Names sharing that prefix can alias; colliding names can prevent Eq from implying EqLoose.
    /// Fractional numbers and integers outside Int64 retain SQLite REAL comparison precision.
    /// </remarks>
    member this.EqLoose(other: obj) = JsonEquality.Matches(this, other)

    member internal this.ToSQLValue() =
        match this with
        | Boolean b -> (if b then 1L :> obj else 0L :> obj), false
        | Null -> null, false
        | Number n -> numberToSQLValue n, false
        | String s -> s :> obj, false
        | other -> other.ToJsonString() :> obj, true

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
            json.[key] <- JsonWriteOperations<obj>.Serialize v
        json
        

    static member val internal ParserApi = {
        Null = fun() -> Null
        Bool = Boolean
        Str = String
        Num = Number
        Arr = List
        Obj = Object
    }

    /// <summary>
    /// Parses a JSON string into a JsonValue.
    /// </summary>
    /// <param name="jsonString">The JSON string to parse.</param>
    /// <returns>The parsed JsonValue.</returns>
    static member Parse (jsonString: string) =
        if isNull jsonString then Null else

        let mutable tokenizerCtx = JsonLexer.fromString jsonString
        let json = JsonParser.parse JsonValue.ParserApi &tokenizerCtx
        json

    /// <summary>
    /// Parses a JSON UTF-16 character span into a JsonValue.
    /// </summary>
    /// <param name="jsonSpan">The JSON input span.</param>
    /// <returns>The parsed JsonValue.</returns>
    static member Parse (jsonSpan: ReadOnlySpan<char>) =
        if jsonSpan.IsEmpty then invalidArg (nameof jsonSpan) "Argument is empty" else

        let mutable tokenizerCtx = JsonLexer.fromUtf16Span jsonSpan
        JsonParser.parse JsonValue.ParserApi &tokenizerCtx

    /// <summary>
    /// Parses a JSON UTF-8 byte span into a JsonValue.
    /// </summary>
    /// <param name="jsonSpan">The UTF-8 JSON input span.</param>
    /// <returns>The parsed JsonValue.</returns>
    static member Parse (jsonSpan: ReadOnlySpan<byte>) =
        if jsonSpan.IsEmpty then invalidArg (nameof jsonSpan) "Argument is empty" else

        let mutable tokenizerCtx = JsonLexer.fromUtf8Span jsonSpan
        JsonParser.parse JsonValue.ParserApi &tokenizerCtx

    /// <summary>
    /// Parses a JSON string into a 'T.
    /// </summary>
    /// <param name="jsonString">The JSON string to parse.</param>
    /// <returns>The parsed 'T.</returns>
    /// <remarks>Forward object readers convert recognized properties in input order. Invalid earlier duplicates fail,
    /// and user effects are retained after later errors. Late runtime-type selection may replay conversion.
    /// Optional wrappers, unions and directional collection readers retain their own policies.</remarks>
    static member ParseInto<'T> (jsonString: string): 'T =
        if isNull jsonString then Unchecked.defaultof<'T> else

        JsonValue.ParseInto<'T>(jsonString.AsSpan())

    /// <summary>
    /// Parses a JSON UTF-16 character span into a 'T.
    /// </summary>
    /// <param name="jsonSpan">The JSON input span.</param>
    /// <returns>The parsed 'T.</returns>
    /// <remarks>Forward object readers convert recognized properties in input order. Invalid earlier duplicates fail,
    /// and user effects are retained after later errors. Late runtime-type selection may replay conversion.
    /// Optional wrappers, unions and directional collection readers retain their own policies.</remarks>
    static member ParseInto<'T> (jsonSpan: ReadOnlySpan<char>): 'T =
        if jsonSpan.IsEmpty then invalidArg (nameof jsonSpan) "Argument is empty" else

        let cursor = JsonLexer.fromUtf16Span jsonSpan
        let mutable context = JsonReadContext(cursor,{ Start = 0; Length = jsonSpan.Length })
        let value = JsonRawRead<JsonNativeContract>.Read<'T,Utf16Reader> &context
        JsonReadCursor.finish &context
        value

    /// <summary>
    /// Parses a JSON UTF-8 byte span into a 'T.
    /// </summary>
    /// <param name="jsonSpan">The UTF-8 JSON input span.</param>
    /// <returns>The parsed 'T.</returns>
    /// <remarks>Forward object readers convert recognized properties in input order. Invalid earlier duplicates fail,
    /// and user effects are retained after later errors. Late runtime-type selection may replay conversion.
    /// Optional wrappers, unions and directional collection readers retain their own policies.</remarks>
    static member ParseInto<'T> (jsonSpan: ReadOnlySpan<byte>): 'T =
        if jsonSpan.IsEmpty then invalidArg (nameof jsonSpan) "Argument is empty" else

        let cursor = JsonLexer.fromUtf8Span jsonSpan
        let mutable context = JsonReadContext(cursor,{ Start = 0; Length = jsonSpan.Length })
        let value = JsonRawRead<JsonNativeContract>.Read<'T,Utf8Reader> &context
        JsonReadCursor.finish &context
        value

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
            json.[string key] <- JsonWriteOperations<'a>.Serialize v
        json

    /// <summary>
    /// Returns the JSON string representation of the value.
    /// </summary>
    override this.ToString() = this.ToJsonString()

    /// <summary>Implicitly converts any object of type 'T to a JsonValue.</summary>
    static member op_Implicit<'T>(o: 'T) : JsonValue =
        JsonWriteOperations<'T>.Serialize o

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

and [<Struct>] internal JsonComparison =
    | Scalar of value: JsonValue
    | Length of count: int

// Recursive with JsonValue so its members and SQL consumers share one walker.
and internal JsonEquality =
    static member Walk(ignoreDiscriminators, path, json, emit) =
        match json with
        | JsonValue.Object fields ->
            for KeyValue(key, value) in fields do
                if not (ignoreDiscriminators && key = "$type") then
                    JsonEquality.Walk(ignoreDiscriminators, Member key :: path, value, emit)
        | JsonValue.List items ->
            emit path (Length items.Count)
            for i = 0 to items.Count - 1 do
                JsonEquality.Walk(ignoreDiscriminators, Index i :: path, items.[i], emit)
        | scalar -> emit path (Scalar scalar)

    // SQLite's JSON label comparison terminates at NUL, even in escaped labels.
    static member private SameMember(left: string, right: string) =
        let leftEnd = left.IndexOf '\000'
        let rightEnd = right.IndexOf '\000'
        let leftLength = if leftEnd < 0 then left.Length else leftEnd
        let rightLength = if rightEnd < 0 then right.Length else rightEnd
        leftLength = rightLength && String.CompareOrdinal(left, 0, right, 0, leftLength) = 0

    static member private ReadPath(stored, path) =
        match path with
        | [] -> stored
        | segment :: parent ->
            match segment, JsonEquality.ReadPath(stored, parent) with
            | Member key, JsonValue.Object fields ->
                let name = key
                use entries = fields.GetEnumerator()
                let mutable found = false
                let mutable value = JsonValue.Null
                while not found && entries.MoveNext() do
                    if JsonEquality.SameMember(entries.Current.Key, name) then
                        found <- true
                        value <- entries.Current.Value
                value
            | Index index, JsonValue.List items when index < items.Count -> items.[index]
            | _ -> JsonValue.Null

    // SQLite compares INTEGER and REAL without rounding the integer to a double.
    static member private IntegerEqualsReal(integer, real) =
        real >= -9223372036854775808.0 && real < 9223372036854775808.0
        && Math.Truncate(real) = real && int64 real = integer

    static member private SqlIs(left: obj, right: obj) =
        match left, right with
        | null, null -> true
        | (:? int64 as a), (:? double as b) -> JsonEquality.IntegerEqualsReal(a, b)
        | (:? double as a), (:? int64 as b) -> JsonEquality.IntegerEqualsReal(b, a)
        | _ -> Object.Equals(left, right)

    static member Matches(stored, argument: obj) =
        let expected = match argument with :? JsonValue as json -> json | _ -> JsonValue.Serialize argument
        let mutable matches = true
        let compare path comparison =
            if matches then
                let actual = JsonEquality.ReadPath(stored, path)
                matches <-
                    match comparison with
                    | Length count ->
                        match actual with
                        | JsonValue.Null -> false
                        | JsonValue.List items -> items.Count = count
                        | _ -> count = 0
                    | Scalar value ->
                        let discriminator = match path with Member "$type" :: _ -> true | _ -> false
                        let storedValue, isJson = actual.ToSQLValue()
                        let expectedValue, _ = value.ToSQLValue()
                        (discriminator && isNull storedValue)
                        || (not isJson && JsonEquality.SqlIs(storedValue, expectedValue))
        JsonEquality.Walk(false, [], expected, compare)
        matches

/// Immutable serialization facts shared by generated codecs and query encoding.
and [<Sealed>] JsonCodecShape private (shape: JsonSerializationShape) =
    member internal _.Shape = shape
    /// A directly encoded JSON scalar.
    static member Scalar = JsonCodecShape(JsonSerializationShape.Scalar)
    /// A runtime-dependent serialization shape.
    static member Dynamic = JsonCodecShape(JsonSerializationShape.Dynamic)
    /// Ordered serialized object members and their direct typed projections.
    static member ObjectMembers(members: JsonStoredMember[]) =
        if isNull members then nullArg "members"
        JsonCodecShape(JsonSerializationShape.ObjectMembers(Array.copy members))
    /// Ordered tuple members and their direct typed projections.
    static member ArrayMembers(members: JsonStoredMember[]) =
        if isNull members then nullArg "members"
        JsonCodecShape(JsonSerializationShape.ArrayMembers(Array.copy members))
    /// The scalar shape of the underlying nullable value.
    static member NullableValue(underlyingType: Type) =
        if isNull underlyingType then nullArg "underlyingType"
        JsonCodecShape(JsonSerializationShape.NullableValue underlyingType)
    static member internal SequenceValues(elementType: Type) =
        JsonCodecShape(JsonSerializationShape.SequenceValues elementType)
    static member internal DictionaryValues(valueType: Type) =
        JsonCodecShape(JsonSerializationShape.DictionaryValues valueType)
    static member internal GroupingValues(keyType: Type, elementType: Type) =
        JsonCodecShape(JsonSerializationShape.GroupingValues(keyType,elementType))
    static member internal Uninterpreted = JsonCodecShape(JsonSerializationShape.Uninterpreted)
    static member internal ReferenceIdentity(identityType: Type) = JsonCodecShape(JsonSerializationShape.ReferenceIdentity identityType)
    /// Publishes the original value role when installing a built-in tree writer.
    static member internal ForValueType(target: Type) = JsonCodecShape(JsonCodecDiscovery.ValueShape target)

/// Immutable typed JSON conversion delegates and their stored shape.
and [<AllowNullLiteral; Sealed>] JsonCodec<'T>
    (serialize: Func<'T, JsonValue>, deserialize: Func<JsonValue, 'T>, shape: JsonCodecShape) =
    do
        if isNull serialize then nullArg "serialize"
        if obj.ReferenceEquals(shape, null) then nullArg "shape"
        if isNull deserialize then nullArg "deserialize"
    new(serialize, deserialize) = JsonCodec<'T>(serialize, deserialize, JsonCodecShape.Dynamic)
    member internal _.Shape = shape.Shape
    member internal _.Reader = deserialize
    member internal _.Writer = serialize
    member _.Serialize(value: 'T) = serialize.Invoke(value)
    member _.Deserialize(value: JsonValue) = deserialize.Invoke(value)
/// Constructs immutable serialized member descriptions.
and [<AbstractClass; Sealed>] JsonMetadata =
    static member internal RegisterUnionStorage<'T>(hasTag: bool, tags: int[], names: string[][], types: Type[][]) =
        TypeMetadataCache.Register(typeof<'T>,fun metadata ->
            match metadata.UnionStorage with
            | ValueSome _ -> metadata
            | ValueNone ->
                { metadata with UnionStorage = MetadataValue.generated(struct(hasTag,Array.copy tags,Array.map Array.copy names,Array.map Array.copy types)) })
    static member internal RegisterGenerated<'T>(codec: JsonCodec<'T>): unit =
        let read = codec.Reader :> Delegate
        let write = struct(codec.Writer :> Delegate,Func<obj>(fun () -> box codec.Shape))
        let readRuntime = Func<obj,obj>(fun json -> box(JsonReadOperations<'T>.Deserialize(unbox<JsonValue> json)))
        let writeRuntime = Func<obj,obj>(fun value -> box(JsonWriteOperations<'T>.Serialize(unbox<'T> value)))
        TypeMetadataCache.Register(typeof<'T>, fun metadata ->
            { metadata with
                JsonReader = match metadata.JsonReader with ValueSome _ -> metadata.JsonReader | ValueNone -> MetadataValue.generated read
                JsonWriter = match metadata.JsonWriter with ValueSome _ -> metadata.JsonWriter | ValueNone -> MetadataValue.generated write
                RuntimeJsonRead = match metadata.RuntimeJsonRead with ValueSome _ -> metadata.RuntimeJsonRead | ValueNone -> MetadataValue.generated readRuntime
                RuntimeJsonWrite = match metadata.RuntimeJsonWrite with ValueSome _ -> metadata.RuntimeJsonWrite | ValueNone -> MetadataValue.generated writeRuntime })
        match codec.Shape with
        | ObjectMembers members | ArrayMembers members ->
            for memberValue in members do
                let key = struct(memberValue.ClrName,memberValue.IsField,memberValue.ValueType)
                TypeMetadataCache.Register(typeof<'T>,fun metadata ->
                    match MetadataMap.tryFind key metadata.QueryMembers with
                    | ValueSome operation when operation.Generated -> metadata
                    | _ -> { metadata with QueryMembers = MetadataMap.set key { Generated = true; Value = memberValue.Reader } metadata.QueryMembers })
        | _ -> ()
    static member internal ReadRaw<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): 'T =
        JsonRawRead<JsonNativeContract>.Read<'T,'R> &context

    static member private RegisterRawCore<'T,'Contract>
        (utf8Read: JsonDirectReader<'T,Utf8Reader>, utf16Read: JsonDirectReader<'T,Utf16Reader>,
         utf8Write: JsonDirectWriter<'T,byte>, utf16Write: JsonDirectWriter<'T,char>): unit =
        let numeric = (TypeSupport.NumericCore typeof<'T>.FullName).IsSome
        let read8 = { Reader = utf8Read; ReadRuntime = JsonDirectRuntimeReader<Utf8Reader>(fun context -> box(JsonRawRead<'Contract>.Invoke<'T,Utf8Reader>(utf8Read,&context))); Intrinsic = if numeric then utf8Read else null }
        let read16 = { Reader = utf16Read; ReadRuntime = JsonDirectRuntimeReader<Utf16Reader>(fun context -> box(JsonRawRead<'Contract>.Invoke<'T,Utf16Reader>(utf16Read,&context))); Intrinsic = if numeric then utf16Read else null }
        let write8 = { Writer = utf8Write; WriteRuntime = JsonDirectRuntimeWriter<byte>(fun value destination state -> utf8Write.Invoke(unbox<'T> value,&destination,&state)) }
        let write16 = { Writer = utf16Write; WriteRuntime = JsonDirectRuntimeWriter<char>(fun value destination state -> utf16Write.Invoke(unbox<'T> value,&destination,&state)) }
        TypeMetadataCache.Register(typeof<'T>, fun metadata ->
            let metadata = JsonRawReadMetadata<'Contract>.Store<Utf8Reader>({ Generated = true; Value = read8 },metadata)
            let metadata = JsonRawReadMetadata<'Contract>.Store<Utf16Reader>({ Generated = true; Value = read16 },metadata)
            let metadata = JsonRawWriteMetadata<byte,JsonUtf8Writer,'Contract>.Store({ Generated = true; Value = write8 },metadata)
            JsonRawWriteMetadata<char,JsonUtf16Writer,'Contract>.Store({ Generated = true; Value = write16 },metadata))

    static member internal RegisterRaw<'T>
        (utf8Read: JsonDirectReader<'T,Utf8Reader>, utf16Read: JsonDirectReader<'T,Utf16Reader>,
         utf8Write: JsonDirectWriter<'T,byte>, utf16Write: JsonDirectWriter<'T,char>): unit =
        JsonMetadata.RegisterRawCore<'T,JsonNativeContract>(utf8Read,utf16Read,utf8Write,utf16Write)

    static member internal RegisterStoredRaw<'T>
        (utf8Read: JsonDirectReader<'T,Utf8Reader>, utf16Read: JsonDirectReader<'T,Utf16Reader>,
         utf8Write: JsonDirectWriter<'T,byte>, utf16Write: JsonDirectWriter<'T,char>): unit =
        JsonMetadata.RegisterRawCore<'T,JsonStoredContract>(utf8Read,utf16Read,utf8Write,utf16Write)

    static member internal RegisterStoredScalar<'T>() =
        let native = TypeMetadataCache.Get typeof<'T>
        let preparation = CodecPreparation.stored typeof<'T>.FullName (TypeSupport.Runtime typeof<'T>) typeof<'T>.IsAbstract
        let equivalentWriter = preparation.ReuseWriter
        let equivalentReader = preparation.ReuseReader
        TypeMetadataCache.Register(typeof<'T>,fun metadata ->
            let metadata =
                match (if equivalentReader then native.JsonUtf8Read else ValueNone) with
                | ValueSome operation -> JsonRawReadMetadata<JsonStoredContract>.Store<Utf8Reader>(operation,metadata)
                | ValueNone -> metadata
            let metadata =
                match (if equivalentReader then native.JsonUtf16Read else ValueNone) with
                | ValueSome operation -> JsonRawReadMetadata<JsonStoredContract>.Store<Utf16Reader>(operation,metadata)
                | ValueNone -> metadata
            let metadata =
                match (if equivalentWriter then native.JsonUtf8Write else ValueNone) with
                | ValueSome operation -> JsonRawWriteMetadata<byte,JsonUtf8Writer,JsonStoredContract>.Store(operation,metadata)
                | ValueNone -> metadata
            match (if equivalentWriter then native.JsonUtf16Write else ValueNone) with
            | ValueSome operation -> JsonRawWriteMetadata<char,JsonUtf16Writer,JsonStoredContract>.Store(operation,metadata)
            | ValueNone -> metadata)

    static member internal RegisterStoredDynamic() =
        JsonRawRead<JsonStoredContract>.Ensure<obj,Utf8Reader>()
        JsonRawRead<JsonStoredContract>.Ensure<obj,Utf16Reader>()
        JsonRawWrite<obj,byte,JsonUtf8Writer,JsonStoredContract>.Ensure()
        JsonRawWrite<obj,char,JsonUtf16Writer,JsonStoredContract>.Ensure()

    static member internal RegisterStoredDocument<'T>() =
        JsonMetadata.RegisterStoredRaw<DBRef<'T>>(JsonRawCollections<JsonStoredContract>.DocumentReader<'T,Utf8Reader>(),JsonRawCollections<JsonStoredContract>.DocumentReader<'T,Utf16Reader>(),
            JsonRawCollections<JsonStoredContract>.DocumentWriter<'T,byte,JsonUtf8Writer>(),JsonRawCollections<JsonStoredContract>.DocumentWriter<'T,char,JsonUtf16Writer>())
    static member internal RegisterStoredTypedDocument<'T,'Id>() =
        JsonMetadata.RegisterStoredRaw<DBRef<'T,'Id>>(JsonRawCollections<JsonStoredContract>.TypedDocumentReader<'T,'Id,Utf8Reader>(),JsonRawCollections<JsonStoredContract>.TypedDocumentReader<'T,'Id,Utf16Reader>(),
            JsonRawCollections<JsonStoredContract>.TypedDocumentWriter<'T,'Id,byte,JsonUtf8Writer>(),JsonRawCollections<JsonStoredContract>.TypedDocumentWriter<'T,'Id,char,JsonUtf16Writer>())
    static member internal RegisterStoredFileReference() =
        JsonMetadata.RegisterStoredRaw<FileRef>(JsonRawCollections<JsonStoredContract>.FileReferenceReader<Utf8Reader>(),JsonRawCollections<JsonStoredContract>.FileReferenceReader<Utf16Reader>(),
            JsonRawCollections<JsonStoredContract>.FileReferenceWriter<byte,JsonUtf8Writer>(),JsonRawCollections<JsonStoredContract>.FileReferenceWriter<char,JsonUtf16Writer>())
    static member internal RegisterStoredDirectoryReference() =
        JsonMetadata.RegisterStoredRaw<DirectoryRef>(JsonRawCollections<JsonStoredContract>.DirectoryReferenceReader<Utf8Reader>(),JsonRawCollections<JsonStoredContract>.DirectoryReferenceReader<Utf16Reader>(),
            JsonRawCollections<JsonStoredContract>.DirectoryReferenceWriter<byte,JsonUtf8Writer>(),JsonRawCollections<JsonStoredContract>.DirectoryReferenceWriter<char,JsonUtf16Writer>())

    static member private RegisterRawReadersCore<'T,'Contract>(utf8: JsonDirectReader<'T,Utf8Reader>, utf16: JsonDirectReader<'T,Utf16Reader>): unit =
        let read8 = { Reader = utf8; ReadRuntime = JsonDirectRuntimeReader<Utf8Reader>(fun context -> box(JsonRawRead<'Contract>.Invoke<'T,Utf8Reader>(utf8,&context))); Intrinsic = null }
        let read16 = { Reader = utf16; ReadRuntime = JsonDirectRuntimeReader<Utf16Reader>(fun context -> box(JsonRawRead<'Contract>.Invoke<'T,Utf16Reader>(utf16,&context))); Intrinsic = null }
        TypeMetadataCache.Register(typeof<'T>,fun metadata ->
            let metadata = JsonRawReadMetadata<'Contract>.Store<Utf8Reader>({ Generated = true; Value = read8 },metadata)
            JsonRawReadMetadata<'Contract>.Store<Utf16Reader>({ Generated = true; Value = read16 },metadata))

    static member internal RegisterRawReaders<'T>(utf8: JsonDirectReader<'T,Utf8Reader>, utf16: JsonDirectReader<'T,Utf16Reader>): unit =
        JsonMetadata.RegisterRawReadersCore<'T,JsonNativeContract>(utf8,utf16)

    static member internal RegisterStoredRawReaders<'T>(utf8: JsonDirectReader<'T,Utf8Reader>, utf16: JsonDirectReader<'T,Utf16Reader>): unit =
        JsonMetadata.RegisterRawReadersCore<'T,JsonStoredContract>(utf8,utf16)

    static member private RegisterRawWritersCore<'T,'Contract>(utf8: JsonDirectWriter<'T,byte>, utf16: JsonDirectWriter<'T,char>): unit =
        let write8 = { Writer = utf8; WriteRuntime = JsonDirectRuntimeWriter<byte>(fun value destination state -> utf8.Invoke(unbox<'T> value,&destination,&state)) }
        let write16 = { Writer = utf16; WriteRuntime = JsonDirectRuntimeWriter<char>(fun value destination state -> utf16.Invoke(unbox<'T> value,&destination,&state)) }
        TypeMetadataCache.Register(typeof<'T>,fun metadata ->
            let metadata = JsonRawWriteMetadata<byte,JsonUtf8Writer,'Contract>.Store({ Generated = true; Value = write8 },metadata)
            JsonRawWriteMetadata<char,JsonUtf16Writer,'Contract>.Store({ Generated = true; Value = write16 },metadata))

    static member internal RegisterRawWriters<'T>(utf8: JsonDirectWriter<'T,byte>, utf16: JsonDirectWriter<'T,char>): unit =
        JsonMetadata.RegisterRawWritersCore<'T,JsonNativeContract>(utf8,utf16)

    static member internal RegisterStoredRawWriters<'T>(utf8: JsonDirectWriter<'T,byte>, utf16: JsonDirectWriter<'T,char>): unit =
        JsonMetadata.RegisterRawWritersCore<'T,JsonStoredContract>(utf8,utf16)

    static member private RegisterWriterBodiesCore<'T,'Contract>(utf8: JsonDirectWriter<'T,byte>, utf16: JsonDirectWriter<'T,char>): unit =
        JsonDefaultMethods.publish typeof<'T> typeof<byte> typeof<JsonUtf8Writer> (JsonOperationPolicy<'Contract>.Name "raw-model-body") utf8
        JsonDefaultMethods.publish typeof<'T> typeof<char> typeof<JsonUtf16Writer> (JsonOperationPolicy<'Contract>.Name "raw-model-body") utf16

    static member internal RegisterWriterBodies<'T>(utf8: JsonDirectWriter<'T,byte>, utf16: JsonDirectWriter<'T,char>): unit =
        JsonMetadata.RegisterWriterBodiesCore<'T,JsonNativeContract>(utf8,utf16)

    static member internal RegisterStoredWriterBodies<'T>(utf8: JsonDirectWriter<'T,byte>, utf16: JsonDirectWriter<'T,char>): unit =
        JsonMetadata.RegisterWriterBodiesCore<'T,JsonStoredContract>(utf8,utf16)

    static member internal RegisterSequenceByteWriters(utf8: JsonDirectWriter<byte[],byte>, utf16: JsonDirectWriter<byte[],char>): unit =
        JsonDefaultMethods.publish typeof<byte[]> typeof<byte> typeof<byte> "sequence-bytes-write" utf8
        JsonDefaultMethods.publish typeof<byte[]> typeof<char> typeof<char> "sequence-bytes-write" utf16

    static member private RegisterSequenceWriterBodiesCore<'Element,'Contract>
        (utf8: JsonDirectWriter<IEnumerable<'Element>,byte>, utf16: JsonDirectWriter<IEnumerable<'Element>,char>): unit =
        let role = JsonOperationPolicy<'Contract>.Name(if typeof<'Element>.Equals(typeof<byte>) then "sequence-binary-write" else "sequence-write")
        JsonDefaultMethods.publish typeof<IEnumerable<'Element>> typeof<byte> typeof<JsonUtf8Writer> role utf8
        JsonDefaultMethods.publish typeof<IEnumerable<'Element>> typeof<char> typeof<JsonUtf16Writer> role utf16

    static member internal RegisterSequenceWriterBodies<'Element>
        (utf8: JsonDirectWriter<IEnumerable<'Element>,byte>, utf16: JsonDirectWriter<IEnumerable<'Element>,char>): unit =
        JsonMetadata.RegisterSequenceWriterBodiesCore<'Element,JsonNativeContract>(utf8,utf16)

    static member internal RegisterStoredSequenceWriterBodies<'Element>
        (utf8: JsonDirectWriter<IEnumerable<'Element>,byte>, utf16: JsonDirectWriter<IEnumerable<'Element>,char>): unit =
        JsonMetadata.RegisterSequenceWriterBodiesCore<'Element,JsonStoredContract>(utf8,utf16)

    static member private RegisterSequenceReadersCore<'Sequence,'Contract>(policy: string,
        utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterRawReadersCore<'Sequence,'Contract>(utf8,utf16)
        let role = JsonOperationPolicy<'Contract>.Name("sequence-read:" + policy)
        JsonDefaultMethods.publish typeof<'Sequence> typeof<Utf8Reader> typeof<Utf8Reader> role utf8
        JsonDefaultMethods.publish typeof<'Sequence> typeof<Utf16Reader> typeof<Utf16Reader> role utf16

    static member internal RegisterSequenceReaders<'Sequence>(policy: string,
        utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterSequenceReadersCore<'Sequence,JsonNativeContract>(policy,utf8,utf16)

    static member internal RegisterStoredSequenceReaders<'Sequence>(policy: string,
        utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterSequenceReadersCore<'Sequence,JsonStoredContract>(policy,utf8,utf16)

    static member private RegisterSequenceBodiesCore<'Sequence,'Contract>(policy: string, binary: bool,
        utf8Read: JsonDirectReader<'Sequence,Utf8Reader>, utf16Read: JsonDirectReader<'Sequence,Utf16Reader>,
        utf8Write: JsonDirectWriter<'Sequence,byte>, utf16Write: JsonDirectWriter<'Sequence,char>): unit =
        JsonMetadata.RegisterRawCore<'Sequence,'Contract>(utf8Read,utf16Read,utf8Write,utf16Write)
        let readRole = JsonOperationPolicy<'Contract>.Name("sequence-read:" + policy)
        JsonDefaultMethods.publish typeof<'Sequence> typeof<Utf8Reader> typeof<Utf8Reader> readRole utf8Read
        JsonDefaultMethods.publish typeof<'Sequence> typeof<Utf16Reader> typeof<Utf16Reader> readRole utf16Read
        let writeRole = JsonOperationPolicy<'Contract>.Name(if binary then "sequence-binary-write" else "sequence-write")
        JsonDefaultMethods.publish typeof<'Sequence> typeof<byte> typeof<JsonUtf8Writer> writeRole utf8Write
        JsonDefaultMethods.publish typeof<'Sequence> typeof<char> typeof<JsonUtf16Writer> writeRole utf16Write

    static member internal RegisterSequenceBodies<'Sequence>(policy: string, binary: bool,
        utf8Read: JsonDirectReader<'Sequence,Utf8Reader>, utf16Read: JsonDirectReader<'Sequence,Utf16Reader>,
        utf8Write: JsonDirectWriter<'Sequence,byte>, utf16Write: JsonDirectWriter<'Sequence,char>): unit =
        JsonMetadata.RegisterSequenceBodiesCore<'Sequence,JsonNativeContract>(policy,binary,utf8Read,utf16Read,utf8Write,utf16Write)

    static member internal RegisterStoredSequenceBodies<'Sequence>(policy: string, binary: bool,
        utf8Read: JsonDirectReader<'Sequence,Utf8Reader>, utf16Read: JsonDirectReader<'Sequence,Utf16Reader>,
        utf8Write: JsonDirectWriter<'Sequence,byte>, utf16Write: JsonDirectWriter<'Sequence,char>): unit =
        JsonMetadata.RegisterSequenceBodiesCore<'Sequence,JsonStoredContract>(policy,binary,utf8Read,utf16Read,utf8Write,utf16Write)

    static member private RegisterRangeArrayBodiesCore<'Element,'Contract>(wrapper: bool,
        utf8: JsonDirectReader<'Element[],Utf8Reader>, utf16: JsonDirectReader<'Element[],Utf16Reader>): unit =
        let role = JsonOperationPolicy<'Contract>.Name("sequence-read:Array" + (if wrapper then ":wrapper" else ""))
        JsonDefaultMethods.publish typeof<'Element[]> typeof<Utf8Reader> typeof<Utf8Reader> role utf8
        JsonDefaultMethods.publish typeof<'Element[]> typeof<Utf16Reader> typeof<Utf16Reader> role utf16

    static member internal RegisterRangeArrayBodies<'Element>(wrapper: bool,
        utf8: JsonDirectReader<'Element[],Utf8Reader>, utf16: JsonDirectReader<'Element[],Utf16Reader>): unit =
        JsonMetadata.RegisterRangeArrayBodiesCore<'Element,JsonNativeContract>(wrapper,utf8,utf16)

    static member internal RegisterStoredRangeArrayBodies<'Element>(wrapper: bool,
        utf8: JsonDirectReader<'Element[],Utf8Reader>, utf16: JsonDirectReader<'Element[],Utf16Reader>): unit =
        JsonMetadata.RegisterRangeArrayBodiesCore<'Element,JsonStoredContract>(wrapper,utf8,utf16)

    static member private RegisterGroupingBodiesCore<'Sequence,'Contract>(utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterRawReadersCore<'Sequence,'Contract>(utf8,utf16)
        JsonDefaultMethods.publish typeof<'Sequence> typeof<Utf8Reader> typeof<Utf8Reader> (JsonOperationPolicy<'Contract>.Name "grouping-read") utf8
        JsonDefaultMethods.publish typeof<'Sequence> typeof<Utf16Reader> typeof<Utf16Reader> (JsonOperationPolicy<'Contract>.Name "grouping-read") utf16

    static member internal RegisterGroupingBodies<'Sequence>(utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterGroupingBodiesCore<'Sequence,JsonNativeContract>(utf8,utf16)

    static member internal RegisterStoredGroupingBodies<'Sequence>(utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterGroupingBodiesCore<'Sequence,JsonStoredContract>(utf8,utf16)

    static member private RegisterReferenceSequenceBodiesCore<'Sequence,'Contract>(utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterRawReadersCore<'Sequence,'Contract>(utf8,utf16)
        JsonDefaultMethods.publish typeof<'Sequence> typeof<Utf8Reader> typeof<Utf8Reader> (JsonOperationPolicy<'Contract>.Name "reference-sequence-read") utf8
        JsonDefaultMethods.publish typeof<'Sequence> typeof<Utf16Reader> typeof<Utf16Reader> (JsonOperationPolicy<'Contract>.Name "reference-sequence-read") utf16

    static member internal RegisterReferenceSequenceBodies<'Sequence>(utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterReferenceSequenceBodiesCore<'Sequence,JsonNativeContract>(utf8,utf16)

    static member internal RegisterStoredReferenceSequenceBodies<'Sequence>(utf8: JsonDirectReader<'Sequence,Utf8Reader>, utf16: JsonDirectReader<'Sequence,Utf16Reader>): unit =
        JsonMetadata.RegisterReferenceSequenceBodiesCore<'Sequence,JsonStoredContract>(utf8,utf16)

    static member private RegisterRawSequence<'Sequence,'Element>(name: string, wrapper: bool): unit =
        JsonMetadata.RegisterRaw<'Sequence>(
            JsonRawCollections<JsonNativeContract>.SequenceReader<'Sequence,'Element,Utf8Reader>(name,wrapper),
            JsonRawCollections<JsonNativeContract>.SequenceReader<'Sequence,'Element,Utf16Reader>(name,wrapper),
            JsonRawCollections<JsonNativeContract>.SequenceWriter<'Sequence,'Element,byte,JsonUtf8Writer>(typeof<'Element>.Equals(typeof<byte>) && name <> "Array"),
            JsonRawCollections<JsonNativeContract>.SequenceWriter<'Sequence,'Element,char,JsonUtf16Writer>(typeof<'Element>.Equals(typeof<byte>) && name <> "Array"))

    static member private RegisterRawDictionary<'Dictionary,'Key,'Value when 'Key : equality>(name: string): unit =
        JsonMetadata.RegisterRaw<'Dictionary>(
            JsonRawCollections<JsonNativeContract>.DictionaryReader<'Dictionary,'Key,'Value,Utf8Reader>(name),
            JsonRawCollections<JsonNativeContract>.DictionaryReader<'Dictionary,'Key,'Value,Utf16Reader>(name),
            JsonRawCollections<JsonNativeContract>.DictionaryWriter<'Dictionary,'Key,'Value,byte,JsonUtf8Writer>(),
            JsonRawCollections<JsonNativeContract>.DictionaryWriter<'Dictionary,'Key,'Value,char,JsonUtf16Writer>())

    static member internal NumericTreeText<'T>(json: JsonValue) =
        match json with
        | Number value -> value.ToString(CultureInfo.InvariantCulture)
        | String value -> value
        | Null -> "NaN"
        | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'T>.FullName)

    static member internal CreateNumeric<'T>() =
        let flags = BindingFlags.NonPublic ||| BindingFlags.Static
        let text = typeof<JsonMetadata>.GetMethod("NumericTreeText",flags).MakeGenericMethod(typeof<'T>) :> MethodBase
        let number = typeof<JsonValue>.GetMethod("NewNumber",BindingFlags.Public ||| BindingFlags.Static) :> MethodBase
        let struct(write,read) = JsonNumericScalarRuntime.trees typeof<'T> typeof<JsonValue> text number
        JsonCodec<'T>(write.CreateDelegate(typeof<Func<'T,JsonValue>>) :?> Func<'T,JsonValue>,
                      read.CreateDelegate(typeof<Func<JsonValue,'T>>) :?> Func<JsonValue,'T>,JsonCodecShape.Scalar)

    /// Roots the tree representation independently of direct generated readers.
    static member internal RegisterScalarTree<'T>() =
        if typeof<byte[]>.Equals(typeof<'T>) then QueryValueMetadata.RegisterArrayType<byte>()
        if (TypeSupport.NumericTextAdapter typeof<'T>.FullName).IsSome then JsonMetadata.RegisterGenerated(JsonMetadata.CreateNumeric<'T>())
        else
        match JsonScalarOperations<'T>.Reader, JsonScalarOperations<'T>.Writer with
        | ValueSome read, ValueSome write ->
            JsonMetadata.RegisterGenerated(JsonCodec<'T>(write,read,JsonCodecShape.ForValueType typeof<'T>))
        | _ -> invalidArg "T" "The type does not have a scalar JSON representation."

    // Supplied inference facts are independent of the full object-reading operation.
    static member private WithNumericIntrinsic<'T,'R when 'R : struct and 'R :> IJsonReader>
        (reader: JsonDirectReader<'T,'R>, metadata: TypeMetadataRecord) =
        let operations =
            match JsonRawReadMetadata<JsonNativeContract>.Select<'R> metadata with
            | ValueSome current -> { current with Value = { current.Value with Intrinsic = reader } }
            | ValueNone ->
                { Generated = true; Value = JsonRawRead<JsonNativeContract>.NumericOperations<'T,'R>(reader) }
        JsonRawReadMetadata<JsonNativeContract>.Store<'R>(operations,metadata)

    static member internal RegisterNumericIntrinsics<'T>(utf8: JsonDirectReader<'T,Utf8Reader>, utf16: JsonDirectReader<'T,Utf16Reader>) =
        TypeMetadataCache.Register(typeof<'T>,fun metadata ->
            JsonMetadata.WithNumericIntrinsic(utf16,JsonMetadata.WithNumericIntrinsic(utf8,metadata)))

    /// Roots shared scalar conversions without structured reflection.
    static member internal RegisterScalar<'T>() =
        JsonMetadata.RegisterScalarTree<'T>()
        if typeof<'T>.Equals(typeof<JsonValue>) then
            JsonMetadata.RegisterRaw<'T>(
                JsonDirectReader<'T,Utf8Reader>(fun context -> unbox<'T>(box(JsonRawInput<JsonNativeContract>.Tree<Utf8Reader> &context))),
                JsonDirectReader<'T,Utf16Reader>(fun context -> unbox<'T>(box(JsonRawInput<JsonNativeContract>.Tree<Utf16Reader> &context))),
                JsonRawCollections<JsonNativeContract>.TreeWriter<'T,byte,JsonUtf8Writer>(Func<'T,JsonValue>(fun value -> unbox<JsonValue>(box value))),
                JsonRawCollections<JsonNativeContract>.TreeWriter<'T,char,JsonUtf16Writer>(Func<'T,JsonValue>(fun value -> unbox<JsonValue>(box value))))
        else
            JsonMetadata.RegisterRaw<'T>(JsonRawScalarReader.Create<'T,Utf8Reader>().Value,JsonRawScalarReader.Create<'T,Utf16Reader>().Value,
                JsonRawScalarWriter.Create<'T,byte,JsonUtf8Writer>().Value,JsonRawScalarWriter.Create<'T,char,JsonUtf16Writer>().Value)
    static member private EnsureScalarTree<'T>() =
        match TypeMetadataCache.Read(typeof<'T>,fun metadata -> metadata.JsonReader), TypeMetadataCache.Read(typeof<'T>,fun metadata -> metadata.JsonWriter) with
        | ValueSome _, ValueSome _ -> ()
        | _ -> JsonMetadata.RegisterScalarTree<'T>()

    static member private EnsureDynamicScalar<'T>() =
        JsonMetadata.EnsureScalarTree<'T>()
        JsonRawRead<JsonNativeContract>.Ensure<'T,Utf8Reader>()
        JsonRawRead<JsonNativeContract>.Ensure<'T,Utf16Reader>()
        JsonRawWrite<'T,byte,JsonUtf8Writer,JsonNativeContract>.Ensure()
        JsonRawWrite<'T,char,JsonUtf16Writer,JsonNativeContract>.Ensure()
    /// Preserves object JSON dispatch and its dynamic read representation.
    static member internal RegisterDynamicObject() =
        JsonMetadata.EnsureDynamicScalar<bool>()
        JsonMetadata.EnsureDynamicScalar<char>()
        JsonMetadata.EnsureDynamicScalar<sbyte>()
        JsonMetadata.EnsureDynamicScalar<byte>()
        JsonMetadata.EnsureDynamicScalar<int16>()
        JsonMetadata.EnsureDynamicScalar<uint16>()
        JsonMetadata.EnsureDynamicScalar<int>()
        JsonMetadata.EnsureDynamicScalar<uint32>()
        JsonMetadata.EnsureDynamicScalar<int64>()
        JsonMetadata.EnsureDynamicScalar<uint64>()
        JsonMetadata.EnsureDynamicScalar<single>()
        JsonMetadata.EnsureDynamicScalar<double>()
        JsonMetadata.EnsureDynamicScalar<decimal>()
        JsonMetadata.EnsureDynamicScalar<string>()
        JsonMetadata.EnsureDynamicScalar<Guid>()
        JsonMetadata.EnsureDynamicScalar<DateTime>()
        JsonMetadata.EnsureDynamicScalar<DateTimeOffset>()
        JsonMetadata.EnsureDynamicScalar<TimeSpan>()
        JsonMetadata.EnsureDynamicScalar<DateOnly>()
        JsonMetadata.EnsureDynamicScalar<TimeOnly>()
        JsonMetadata.EnsureDynamicScalar<BigInteger>()
        JsonMetadata.EnsureDynamicScalar<IntPtr>()
        JsonMetadata.EnsureDynamicScalar<UIntPtr>()
        JsonMetadata.EnsureDynamicScalar<byte[]>()
        JsonMetadata.EnsureDynamicScalar<JsonValue>()
        let write (value: obj) =
            if isNull value then JsonValue.Null
            elif typeof<obj>.Equals(value.GetType()) then JsonValue.New()
            else JsonImpl.SerializeByTypeWithType (value.GetType()) value
        JsonMetadata.RegisterGenerated(JsonCodec<obj>(Func<obj,JsonValue>(write), Func<JsonValue,obj>(JsonImpl.DeserializeDynamic), JsonCodecShape.Dynamic))
        JsonRawRead<JsonNativeContract>.Ensure<obj,Utf8Reader>()
        JsonRawRead<JsonNativeContract>.Ensure<obj,Utf16Reader>()
        JsonRawWrite<obj,byte,JsonUtf8Writer,JsonNativeContract>.Ensure()
        JsonRawWrite<obj,char,JsonUtf16Writer,JsonNativeContract>.Ensure()
    /// Uses a compiler-established enum width with the canonical underlying scalar codec.
    static member internal RegisterEnumTree<'Enum,'Underlying>() =
        JsonMetadata.EnsureScalarTree<'Underlying>()
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateEnum<'Enum,'Underlying>())
    static member internal RegisterEnum<'Enum,'Underlying>() =
        JsonMetadata.RegisterEnumTree<'Enum,'Underlying>()
        JsonMetadata.RegisterRaw<'Enum>(JsonRawCollections<JsonNativeContract>.EnumReader<'Enum,'Underlying,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.EnumReader<'Enum,'Underlying,Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.EnumWriter<'Enum,'Underlying,byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.EnumWriter<'Enum,'Underlying,char,JsonUtf16Writer>())
    static member internal CreateEnum<'Enum,'Underlying>(): JsonCodec<'Enum> =
        TypeMetadata.registerEnumUnderlying typeof<'Enum> typeof<'Underlying>
        let write (value: 'Enum) =
            let mutable value = value
            JsonValue.Serialize(Unsafe.As<'Enum,'Underlying>(&value))
        let read (json: JsonValue) =
            let mutable value = json.ToObject<'Underlying>()
            Unsafe.As<'Underlying,'Enum>(&value)
        JsonCodec<'Enum>(Func<'Enum,JsonValue>(write),Func<JsonValue,'Enum>(read),JsonCodecShape.Scalar)
    /// Selects an existing tuple wire element from either accepted stored form.

    static member internal TupleElement(json: JsonValue, ordinal: int) =
        match json with
        | JsonValue.List _ -> json[ordinal.ToString(CultureInfo.InvariantCulture)]
        | JsonValue.Object _ -> json["Item" + (ordinal + 1).ToString(CultureInfo.InvariantCulture)]
        | _ -> invalidOp "A tuple requires a JSON list or object."
    /// Publishes direct tuple construction and projection with the shared wire contract.
    static member internal RegisterTuple<'Tuple>(write: Func<'Tuple,JsonValue[]>, read: Func<JsonValue,'Tuple>, members: JsonStoredMember[]) =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateTuple<'Tuple>(write,read,members))
    static member internal CreateTuple<'Tuple>(write: Func<'Tuple,JsonValue[]>, read: Func<JsonValue,'Tuple>, members: JsonStoredMember[]): JsonCodec<'Tuple> =
        let serialize value =
            if isNull(box value) then JsonValue.Null
            else JsonValue.List(write.Invoke(value))
        let deserialize (json: JsonValue) =
            match json with
            | JsonValue.Null -> Unchecked.defaultof<'Tuple>
            | JsonValue.List _ | JsonValue.Object _ -> read.Invoke(json)
            | _ -> failwithf "Cannot deserialize into %s the value: %A" typeof<'Tuple>.FullName json.JsonType
        JsonCodec<'Tuple>(Func<'Tuple,JsonValue>(serialize),Func<JsonValue,'Tuple>(deserialize),JsonCodecShape.ArrayMembers(members))
    static member internal CreatePhysicalReferenceCodec<'Reference,'Target>
        (unloaded: Func<int64,'Reference>, loaded: Func<int64,'Target,'Reference>, hasValue: Func<'Reference,bool>, id: Func<'Reference,int64>): JsonCodec<'Reference> =
        let physicalIdWriter = lazy(EntityIdentity.tryInt64IdWriter typeof<'Target>)
        let write value = if hasValue.Invoke(value) then Number(decimal(id.Invoke(value))) else Null
        let read json =
            match json with
            | Null -> Unchecked.defaultof<'Reference>
            | Number number -> unloaded.Invoke(int64 number)
            | List items when items.Count = 2 ->
                let identity = int64(items[0].ToObject<decimal>())
                let value = box(items[1].ToObject<'Target>())
                if not(isNull value) then
                    match physicalIdWriter.Value with
                    | ValueSome writer -> writer.Invoke(value,identity)
                    | ValueNone -> ()
                loaded.Invoke(identity,unbox<'Target> value)
            | Object values when values.ContainsKey "$dbrefTypedId" ->
                failwithf "Cannot deserialize DBRef<%s> from $dbrefTypedId marker: only DBRef<TTarget,TId> supports typed-id wire format." typeof<'Target>.Name
            | other -> JsonValue.InvalidValue(sprintf "Cannot deserialize DBRef<%s> from %A" typeof<'Target>.Name other)
        JsonCodec<'Reference>(Func<'Reference,JsonValue>(write),Func<JsonValue,'Reference>(read),JsonCodecShape.Scalar)

    static member internal RegisterFileReference(): unit =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateFileReference())
        JsonMetadata.RegisterRaw<FileRef>(JsonRawCollections<JsonNativeContract>.FileReferenceReader<Utf8Reader>(),JsonRawCollections<JsonNativeContract>.FileReferenceReader<Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.FileReferenceWriter<byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.FileReferenceWriter<char,JsonUtf16Writer>())

    static member internal RegisterDirectoryReference(): unit =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateDirectoryReference())
        JsonMetadata.RegisterRaw<DirectoryRef>(JsonRawCollections<JsonNativeContract>.DirectoryReferenceReader<Utf8Reader>(),JsonRawCollections<JsonNativeContract>.DirectoryReferenceReader<Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.DirectoryReferenceWriter<byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.DirectoryReferenceWriter<char,JsonUtf16Writer>())

    static member internal RegisterFileMany(): unit =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateFileMany())
        JsonMetadata.RegisterRaw<FileRefMany>(JsonRawCollections<JsonNativeContract>.FileManyReader<Utf8Reader>(),JsonRawCollections<JsonNativeContract>.FileManyReader<Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.FileManyWriter<byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.FileManyWriter<char,JsonUtf16Writer>())

    static member internal RegisterDirectoryMany(): unit =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateDirectoryMany())
        JsonMetadata.RegisterRaw<DirectoryRefMany>(JsonRawCollections<JsonNativeContract>.DirectoryManyReader<Utf8Reader>(),JsonRawCollections<JsonNativeContract>.DirectoryManyReader<Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.DirectoryManyWriter<byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.DirectoryManyWriter<char,JsonUtf16Writer>())

    static member internal CreateFileReference(): JsonCodec<FileRef> =
        FileSystemReferenceMetadata.Single<FileRef,Types.SoloDBFileHeader>(
            Func<int64,FileRef>(FileRef.To),Func<int64,Types.SoloDBFileHeader,FileRef>(fun id header -> FileRef.Loaded(id,header)),
            Func<FileRef,bool>(fun value -> value.IsLoaded),Func<FileRef,Types.SoloDBFileHeader>(fun value -> value.Value),Func<FileRef,int64>(fun value -> value.Id))
        JsonMetadata.CreatePhysicalReferenceCodec<FileRef,Types.SoloDBFileHeader>(
            Func<int64,FileRef>(FileRef.Unloaded),Func<int64,Types.SoloDBFileHeader,FileRef>(fun id header -> FileRef.Loaded(id,header)),
            Func<FileRef,bool>(fun value -> value.HasValue),Func<FileRef,int64>(fun value -> value.Id))

    static member internal CreateDirectoryReference(): JsonCodec<DirectoryRef> =
        FileSystemReferenceMetadata.Single<DirectoryRef,Types.SoloDBDirectoryHeader>(
            Func<int64,DirectoryRef>(DirectoryRef.To),Func<int64,Types.SoloDBDirectoryHeader,DirectoryRef>(fun id header -> DirectoryRef.Loaded(id,header)),
            Func<DirectoryRef,bool>(fun value -> value.IsLoaded),Func<DirectoryRef,Types.SoloDBDirectoryHeader>(fun value -> value.Value),Func<DirectoryRef,int64>(fun value -> value.Id))
        JsonMetadata.CreatePhysicalReferenceCodec<DirectoryRef,Types.SoloDBDirectoryHeader>(
            Func<int64,DirectoryRef>(DirectoryRef.Unloaded),Func<int64,Types.SoloDBDirectoryHeader,DirectoryRef>(fun id header -> DirectoryRef.Loaded(id,header)),
            Func<DirectoryRef,bool>(fun value -> value.HasValue),Func<DirectoryRef,int64>(fun value -> value.Id))

    static member internal CreateFileMany(): JsonCodec<FileRefMany> =
        FileSystemReferenceMetadata.Many<FileRefMany,FileRef,Types.SoloDBFileHeader>(Func<FileRefMany>(fun () -> FileRefMany()),Action<FileRefMany,FileRef>(fun references item -> references.Add(item)))
        let read (json: JsonValue) = if json.IsNull then FileRefMany() else FileRefMany(JsonImpl.IEnumerableDeserialize<FileRef>(json))
        JsonCodec<FileRefMany>(Func<FileRefMany,JsonValue>(fun references -> if isNull references then Null else JsonImpl.IEnumerableSerialize references),Func<JsonValue,FileRefMany>(read))

    static member internal CreateDirectoryMany(): JsonCodec<DirectoryRefMany> =
        FileSystemReferenceMetadata.Many<DirectoryRefMany,DirectoryRef,Types.SoloDBDirectoryHeader>(Func<DirectoryRefMany>(fun () -> DirectoryRefMany()),Action<DirectoryRefMany,DirectoryRef>(fun references item -> references.Add(item)))
        let read (json: JsonValue) = if json.IsNull then DirectoryRefMany() else DirectoryRefMany(JsonImpl.IEnumerableDeserialize<DirectoryRef>(json))
        JsonCodec<DirectoryRefMany>(Func<DirectoryRefMany,JsonValue>(fun references -> if isNull references then Null else JsonImpl.IEnumerableSerialize references),Func<JsonValue,DirectoryRefMany>(read))

    static member internal CreateDocumentCodec<'T>() =
        ReferenceTypeMetadata.Register<DBRef<'T>,'T>(false, null)
        JsonMetadata.EnsureScalarTree<decimal>()
        DocumentReferenceConstruction.RegisterSingle<'T>()
        JsonMetadata.CreatePhysicalReferenceCodec<DBRef<'T>,'T>(
            Func<int64,DBRef<'T>>(DBRef<'T>.Unloaded), Func<int64,'T,DBRef<'T>>(fun id value -> DBRef<'T>.Loaded(id,value)),
            Func<DBRef<'T>,bool>(fun value -> value.HasValue), Func<DBRef<'T>,int64>(fun value -> value.Id))
    /// Registers the shared reference codec over an original generated target.
    static member internal RegisterDocument<'T>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateDocumentCodec<'T>())
        JsonMetadata.RegisterRaw<DBRef<'T>>(JsonRawCollections<JsonNativeContract>.DocumentReader<'T,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.DocumentReader<'T,Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.DocumentWriter<'T,byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.DocumentWriter<'T,char,JsonUtf16Writer>())
    static member internal CreateTypedDocumentCodec<'T,'Id>() =
        ReferenceTypeMetadata.Register<DBRef<'T,'Id>,'T>(false, typeof<'Id>)
        JsonMetadata.EnsureScalarTree<decimal>()
        DocumentReferenceConstruction.RegisterTyped<'T,'Id>()
        let physicalIdWriter = lazy(EntityIdentity.tryInt64IdWriter typeof<'T>)
        let write (value: DBRef<'T,'Id>) =
            if value.HasPendingTypedId then
                let values = Dictionary<string,JsonValue>()
                values.Add("$dbrefTypedId", JsonValue.Serialize(value.TypedId))
                JsonValue.Object values
            elif value.HasValue then JsonValue.Number(decimal value.Id)
            else JsonValue.Null
        let read json =
            match json with
            | JsonValue.Null -> DBRef<'T,'Id>.None
            | JsonValue.Number value -> DBRef<'T,'Id>.Unloaded(int64 value)
            | JsonValue.List values when values.Count = 2 ->
                let id = int64(values.[0].ToObject<decimal>())
                let entity = box(values.[1].ToObject<'T>())
                if not(isNull entity) then
                    match physicalIdWriter.Value with
                    | ValueSome writer -> writer.Invoke(entity,id)
                    | ValueNone -> ()
                match SoloIdAccessor.TryGetBoxedValue(typeof<'T>,entity) with
                | ValueSome typedId -> DBRef<'T,'Id>.Loaded(id,unbox<'Id> typedId,unbox<'T> entity)
                | ValueNone -> DBRef<'T,'Id>.Loaded(id,unbox<'T> entity)
            | JsonValue.Object values when values.ContainsKey "$dbrefTypedId" -> DBRef<'T,'Id>.To(values.["$dbrefTypedId"].ToObject<'Id>())
            | other -> JsonValue.InvalidValue(sprintf "Cannot deserialize DBRef<%s> from %A" typeof<'T>.Name other)
        JsonCodec<DBRef<'T,'Id>>(Func<DBRef<'T,'Id>,JsonValue>(write),Func<JsonValue,DBRef<'T,'Id>>(read),JsonCodecShape.ReferenceIdentity typeof<'Id>)
    /// Registers the shared typed-ID reference policy over original generated types.
    static member internal RegisterTypedDocument<'T,'Id>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateTypedDocumentCodec<'T,'Id>())
        JsonMetadata.RegisterRaw<DBRef<'T,'Id>>(JsonRawCollections<JsonNativeContract>.TypedDocumentReader<'T,'Id,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.TypedDocumentReader<'T,'Id,Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.TypedDocumentWriter<'T,'Id,byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.TypedDocumentWriter<'T,'Id,char,JsonUtf16Writer>())
    /// Supplies exact member facts without constructing a reflection expression witness.
    static member Member<'T, 'Value>(declaringType: Type, clrName: string, storedName: string, isField: bool, read: Func<'T, 'Value>) =
        if isNull read then nullArg "read"
        JsonStoredMember(declaringType, typeof<'Value>, clrName, storedName, isField,
                         Func<obj, obj>(fun value -> box (read.Invoke(unbox<'T> value))))
    /// Registers a statically closed original type for stored discriminators.
    static member internal RegisterType<'T>(sealedType: bool, requiresTypeInformation: bool) =
        QueryValueMetadata.RegisterType<'T>()
        ModelMetadata.RegisterComparer<'T>()
        TypeMetadata.registerFacts(ModelTypeFacts(typeof<'T>, sealedType, requiresTypeInformation))
        TypeMetadata.registerType typeof<'T>
    /// Supplies compiler-established closed generic arguments without runtime discovery.
    static member internal RegisterTypeArguments<'T>(arguments: Type[]) =
        if isNull arguments then nullArg "arguments"
        TypeMetadata.registerArguments typeof<'T> arguments
    /// Supplies ordered record constructor fields, or null for a non-record type.
    static member internal RegisterRecordFields<'T>(fields: string[]) =
        TypeMetadata.registerRecordFields typeof<'T> fields
    /// Uses the same object admission and diagnostics as ordinary deserialization.
    static member internal ObjectProperties(json: JsonValue) = JsonImpl.AsDictOrFail json
    /// Uses the existing union predicate precedence without member discovery.
    static member internal UnionCaseSelected(json: JsonValue, name: string) =
        match json.TryGetProperty name with
        | true, Boolean true -> true
        | _ -> false
    /// Retains constructor bypass for models without a parameterless constructor.
    static member internal Uninitialized<'T>() = FormatterServices.GetSafeUninitializedObject(typeof<'T>) :?> 'T
    /// Selects the original assignable discriminator through the common type owner.
    static member internal ActualType<'T>(json: JsonValue) = JsonImpl.TryGetTypeFromJson<'T> json
    /// Serializes an actual polymorphic instance through its registered operations.
    static member internal SerializeActual(value: obj) =
        JsonImpl.SerializeByTypeWithType (value.GetType()) value
    /// Registers statically closed array operations using the existing sequence policy.
    static member internal RegisterArray<'T>(supportsValuesWrapper: bool) =
        QueryValueMetadata.RegisterArrayType<'T>()
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateArray<'T>(supportsValuesWrapper))
    static member internal CreateArray<'T>(supportsValuesWrapper: bool): JsonCodec<'T[]> =
        let write (items: 'T[]) =
            if isNull items then Null else JsonImpl.IEnumerableSerialize items
        let read json =
            JsonSequenceOperations.Read<'T[]>(json, supportsValuesWrapper, Func<JsonValue, 'T[]>(JsonImpl.ArrayDeserialize<'T>))
        JsonCodec<'T[]>(Func<'T[], JsonValue>(write), Func<JsonValue, 'T[]>(read),
                       if typeof<'T>.Equals(typeof<byte>) then JsonCodecShape.Scalar else JsonCodecShape.SequenceValues typeof<'T>)

    static member internal MissingCollectionConstructor<'T>(_: JsonValue): 'T =
        failwithf "Type %s must have a parameterless constructor" typeof<'T>.Name

    static member internal CreateSequence<'Sequence,'T when 'Sequence :> IEnumerable<'T>>(write: Func<'Sequence,JsonValue>, standard: Func<JsonValue,'Sequence>): JsonCodec<'Sequence> =
        let scalarReader = lazy(
            try JsonScalarOperations<'Sequence>.Reader
            with error -> raise(TypeInitializationException("SoloDatabase.JsonSerializator.JsonDeserializerImpl`1",error)))
        let read (json: JsonValue) =
            match scalarReader.Value with
            | ValueSome scalar -> scalar.Invoke(json)
            | ValueNone -> JsonSequenceOperations.Read<'Sequence>(json,false,standard)
        JsonCodec<'Sequence>(write,Func<JsonValue,'Sequence>(read),if typeof<'T>.Equals(typeof<byte>) then JsonCodecShape.Scalar else JsonCodecShape.SequenceValues typeof<'T>)

    static member internal CreateSequence<'Sequence,'T when 'Sequence :> IEnumerable<'T>>(standard: Func<JsonValue,'Sequence>): JsonCodec<'Sequence> =
        let scalarWriter = JsonScalarOperations<'Sequence>.Writer
        let write (items: 'Sequence) =
            match scalarWriter with
            | ValueSome scalar -> scalar.Invoke(items)
            | ValueNone -> if isNull(box items) then Null else JsonImpl.IEnumerableSerialize items
        JsonMetadata.CreateSequence<'Sequence,'T>(Func<'Sequence,JsonValue>(write),standard)

    /// Registers the existing list reconstruction and element policy.
    static member internal RegisterList<'T>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateList<'T>())
        JsonMetadata.RegisterRawSequence<ResizeArray<'T>,'T>("List",false)
    static member internal CreateList<'T>(): JsonCodec<ResizeArray<'T>> =
        JsonMetadata.CreateSequence<System.Collections.Generic.List<'T>,'T>(Func<JsonValue,System.Collections.Generic.List<'T>>(fun json -> JsonImpl.IListDeserialize<'T,System.Collections.Generic.List<'T>> (System.Collections.Generic.List<'T>()) json))
    /// Registers the existing FSharp list policy without runtime union discovery.

    static member internal RegisterFSharpList<'T>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateFSharpList<'T>())
        JsonMetadata.RegisterRawSequence<'T list,'T>("FSharpList",false)
    static member internal CreateFSharpList<'T>(): JsonCodec<'T list> =
        JsonMetadata.CreateSequence<'T list,'T>(Func<JsonValue,'T list>(JsonImpl.FSharpListDeserialize<'T>))
    /// Registers the existing HashSet reconstruction and element policy.

    static member internal RegisterHashSet<'T>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateHashSet<'T>())
        JsonMetadata.RegisterRawSequence<HashSet<'T>,'T>("HashSet",false)
    static member internal CreateHashSet<'T>(): JsonCodec<HashSet<'T>> =
        JsonMetadata.CreateSequence<HashSet<'T>,'T>(Func<JsonValue,HashSet<'T>>(JsonImpl.HashSetDeserialize<'T>))
    /// Registers the existing queue reconstruction and element policy.

    static member internal RegisterQueue<'T>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateQueue<'T>())
        JsonMetadata.RegisterRawSequence<Queue<'T>,'T>("Queue",false)
    static member internal CreateQueue<'T>(): JsonCodec<Queue<'T>> =
        JsonMetadata.CreateSequence<Queue<'T>,'T>(Func<JsonValue,Queue<'T>>(JsonImpl.QueueDeserialize<'T>))
    /// Registers the existing stack reconstruction order and element policy.

    static member internal RegisterStack<'T>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateStack<'T>())
        JsonMetadata.RegisterRawSequence<Stack<'T>,'T>("Stack",false)
    static member internal CreateStack<'T>(): JsonCodec<Stack<'T>> =
        JsonMetadata.CreateSequence<Stack<'T>,'T>(Func<JsonValue,Stack<'T>>(JsonImpl.StackDeserialize<'T>))
    /// Registers the existing linked-list reconstruction and element policy.

    static member internal RegisterLinkedList<'T>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateLinkedList<'T>())
        JsonMetadata.RegisterRawSequence<LinkedList<'T>,'T>("LinkedList",false)
    static member internal CreateLinkedList<'T>(): JsonCodec<LinkedList<'T>> =
        JsonMetadata.CreateSequence<LinkedList<'T>,'T>(Func<JsonValue,LinkedList<'T>>(JsonImpl.LinkedListDeserialize<'T>))
    static member internal RegisterEnumerableReaders<'Element>(): unit =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateEnumerable<'Element>())
        JsonMetadata.RegisterRawReaders<IEnumerable<'Element>>(
            JsonRawCollections<JsonNativeContract>.SequenceReader<IEnumerable<'Element>,'Element,Utf8Reader>("Enumerable",false),
            JsonRawCollections<JsonNativeContract>.SequenceReader<IEnumerable<'Element>,'Element,Utf16Reader>("Enumerable",false))
    static member internal RegisterEnumerable<'Element>(): unit =
        JsonMetadata.RegisterEnumerableReaders<'Element>()
        JsonMetadata.RegisterRawWriters<IEnumerable<'Element>>(
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IEnumerable<'Element>,byte,JsonUtf8Writer>(),
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IEnumerable<'Element>,char,JsonUtf16Writer>())
    static member internal RegisterOrderedEnumerableReaders<'Element>(): unit =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateOrderedEnumerable<'Element>())
        JsonMetadata.RegisterRawReaders<IOrderedEnumerable<'Element>>(
            JsonRawCollections<JsonNativeContract>.SequenceReader<IOrderedEnumerable<'Element>,'Element,Utf8Reader>("OrderedEnumerable",false),
            JsonRawCollections<JsonNativeContract>.SequenceReader<IOrderedEnumerable<'Element>,'Element,Utf16Reader>("OrderedEnumerable",false))
    static member internal RegisterOrderedEnumerable<'Element>(): unit =
        JsonMetadata.RegisterOrderedEnumerableReaders<'Element>()
        JsonMetadata.RegisterRawWriters<IOrderedEnumerable<'Element>>(
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IOrderedEnumerable<'Element>,byte,JsonUtf8Writer>(),
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IOrderedEnumerable<'Element>,char,JsonUtf16Writer>())
    static member internal RegisterGroupingReaders<'Key,'Element>(): unit =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateGrouping<'Key,'Element>())
        JsonMetadata.RegisterRawReaders<IGrouping<'Key,'Element>>(
            JsonRawCollections<JsonNativeContract>.GroupingReader<'Key,'Element,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.GroupingReader<'Key,'Element,Utf16Reader>())
    static member internal RegisterGrouping<'Key,'Element>(): unit =
        JsonMetadata.RegisterGroupingReaders<'Key,'Element>()
        JsonMetadata.RegisterRawWriters<IGrouping<'Key,'Element>>(
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IGrouping<'Key,'Element>,byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.AbstractWriter<IGrouping<'Key,'Element>,char,JsonUtf16Writer>())

    static member internal CreateEnumerable<'Element>(): JsonCodec<IEnumerable<'Element>> =
        let codec = JsonMetadata.CreateSequence<IEnumerable<'Element>,'Element>(Func<IEnumerable<'Element>,JsonValue>(fun value -> JsonImpl.SerializeAbstract(box value)),Func<JsonValue,IEnumerable<'Element>>(JsonImpl.IEnumerableDeserialize<'Element>))
        JsonCodec<IEnumerable<'Element>>(codec.Writer,codec.Reader,if typeof<'Element>.Equals(typeof<byte>) then JsonCodecShape.Dynamic else JsonCodecShape.SequenceValues typeof<'Element>)

    static member internal CreateOrderedEnumerable<'Element>(): JsonCodec<IOrderedEnumerable<'Element>> =
        let codec = JsonMetadata.CreateSequence<IOrderedEnumerable<'Element>,'Element>(Func<IOrderedEnumerable<'Element>,JsonValue>(fun value -> JsonImpl.SerializeAbstract(box value)),Func<JsonValue,IOrderedEnumerable<'Element>>(JsonImpl.OrderedEnumerableDeserialize<'Element>))
        JsonCodec<IOrderedEnumerable<'Element>>(codec.Writer,codec.Reader,if typeof<'Element>.Equals(typeof<byte>) then JsonCodecShape.Dynamic else JsonCodecShape.SequenceValues typeof<'Element>)

    static member internal CreateGrouping<'Key,'Element>(): JsonCodec<IGrouping<'Key,'Element>> =
        let write (value: IGrouping<'Key,'Element>) = JsonImpl.SerializeAbstract(box value)
        let read (json: JsonValue) = if json.IsNull then null else JsonImpl.GroupingDeserialize<'Key,'Element>(json) :> IGrouping<'Key,'Element>
        JsonCodec<IGrouping<'Key,'Element>>(Func<IGrouping<'Key,'Element>,JsonValue>(write),Func<JsonValue,IGrouping<'Key,'Element>>(read),JsonCodecShape.GroupingValues(typeof<'Key>,typeof<'Element>))

    /// Registers concrete dictionary operations with the original key conversion owner.

    static member internal RegisterDictionary<'Key,'Value when 'Key : equality>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateDictionary<'Key,'Value>())
        JsonMetadata.RegisterRawDictionary<Dictionary<'Key,'Value>,'Key,'Value>("Dictionary")
    static member internal CreateDictionary<'Key,'Value when 'Key : equality>(): JsonCodec<Dictionary<'Key,'Value>> =
        let write (value: Dictionary<'Key,'Value>) = if isNull value then Null else JsonImpl.SerializeDictionary<'Key,'Value>(value)
        let read (json: JsonValue) = if json.IsNull then null else JsonImpl.CustomDictionaryDeserialize<'Key,'Value,Dictionary<'Key,'Value>> (Dictionary<'Key,'Value>()) json
        JsonCodec<Dictionary<'Key,'Value>>(Func<Dictionary<'Key,'Value>,JsonValue>(write),Func<JsonValue,Dictionary<'Key,'Value>>(read),JsonCodecShape.DictionaryValues typeof<'Value>)
    /// Registers sorted dictionary operations with the original constructor and key conversion.

    static member internal RegisterSortedDictionary<'Key,'Value when 'Key : comparison>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateSortedDictionary<'Key,'Value>())
        JsonMetadata.RegisterRawDictionary<SortedDictionary<'Key,'Value>,'Key,'Value>("SortedDictionary")
    static member internal CreateSortedDictionary<'Key,'Value when 'Key : comparison>(): JsonCodec<SortedDictionary<'Key,'Value>> =
        let write (value: SortedDictionary<'Key,'Value>) = if isNull value then Null else JsonImpl.SerializeDictionary<'Key,'Value>(value)
        let read (json: JsonValue) = if json.IsNull then null else JsonImpl.CustomDictionaryDeserialize<'Key,'Value,SortedDictionary<'Key,'Value>> (SortedDictionary<'Key,'Value>()) json
        JsonCodec<SortedDictionary<'Key,'Value>>(Func<SortedDictionary<'Key,'Value>,JsonValue>(write),Func<JsonValue,SortedDictionary<'Key,'Value>>(read),JsonCodecShape.DictionaryValues typeof<'Value>)
    /// Registers dictionary-interface reads through the existing concrete implementation.

    static member internal RegisterDictionaryInterfaceReaders<'Key,'Value when 'Key : equality>() =
        JsonMetadata.RegisterDictionary<'Key,'Value>()
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateDictionaryInterface<'Key,'Value>())
        JsonMetadata.RegisterRawReaders<IDictionary<'Key,'Value>>(
            JsonRawCollections<JsonNativeContract>.DictionaryReader<IDictionary<'Key,'Value>,'Key,'Value,Utf8Reader>("DictionaryInterface"),
            JsonRawCollections<JsonNativeContract>.DictionaryReader<IDictionary<'Key,'Value>,'Key,'Value,Utf16Reader>("DictionaryInterface"))
    static member internal RegisterDictionaryInterface<'Key,'Value when 'Key : equality>() =
        JsonMetadata.RegisterDictionaryInterfaceReaders<'Key,'Value>()
        JsonMetadata.RegisterRawWriters<IDictionary<'Key,'Value>>(
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IDictionary<'Key,'Value>,byte,JsonUtf8Writer>(),
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IDictionary<'Key,'Value>,char,JsonUtf16Writer>())
    static member internal CreateDictionaryInterface<'Key,'Value when 'Key : equality>(): JsonCodec<IDictionary<'Key,'Value>> =
        let write (value: IDictionary<'Key,'Value>) = JsonImpl.SerializeAbstract(box value)
        let read (json: JsonValue) = if json.IsNull then null else JsonImpl.DictionaryDeserialize<'Key,'Value>(json)
        JsonCodec<IDictionary<'Key,'Value>>(Func<IDictionary<'Key,'Value>,JsonValue>(write),Func<JsonValue,IDictionary<'Key,'Value>>(read),JsonCodecShape.DictionaryValues typeof<'Value>)
    /// Registers read-only dictionary-interface reads through the existing concrete implementation.

    static member internal RegisterReadOnlyDictionaryInterfaceReaders<'Key,'Value when 'Key : equality>() =
        JsonMetadata.RegisterDictionary<'Key,'Value>()
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateReadOnlyDictionaryInterface<'Key,'Value>())
        JsonMetadata.RegisterRawReaders<IReadOnlyDictionary<'Key,'Value>>(
            JsonRawCollections<JsonNativeContract>.DictionaryReader<IReadOnlyDictionary<'Key,'Value>,'Key,'Value,Utf8Reader>("ReadOnlyDictionaryInterface"),
            JsonRawCollections<JsonNativeContract>.DictionaryReader<IReadOnlyDictionary<'Key,'Value>,'Key,'Value,Utf16Reader>("ReadOnlyDictionaryInterface"))
    static member internal RegisterReadOnlyDictionaryInterface<'Key,'Value when 'Key : equality>() =
        JsonMetadata.RegisterReadOnlyDictionaryInterfaceReaders<'Key,'Value>()
        JsonMetadata.RegisterRawWriters<IReadOnlyDictionary<'Key,'Value>>(
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IReadOnlyDictionary<'Key,'Value>,byte,JsonUtf8Writer>(),
            JsonRawCollections<JsonNativeContract>.AbstractWriter<IReadOnlyDictionary<'Key,'Value>,char,JsonUtf16Writer>())
    static member internal CreateReadOnlyDictionaryInterface<'Key,'Value when 'Key : equality>(): JsonCodec<IReadOnlyDictionary<'Key,'Value>> =
        let write (value: IReadOnlyDictionary<'Key,'Value>) = JsonImpl.SerializeAbstract(box value)
        let read (json: JsonValue) = if json.IsNull then null else JsonImpl.IReadOnlyDictionaryDeserialize<'Key,'Value>(json)
        JsonCodec<IReadOnlyDictionary<'Key,'Value>>(Func<IReadOnlyDictionary<'Key,'Value>,JsonValue>(write),Func<JsonValue,IReadOnlyDictionary<'Key,'Value>>(read),JsonCodecShape.DictionaryValues typeof<'Value>)
    /// Registers the existing list policy and typed tracker operations.

    static member internal RegisterGroupingImplementation<'Sequence,'Key,'Element>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateGroupingImplementation<'Sequence,'Key,'Element>())
        JsonMetadata.RegisterRaw<'Sequence>(JsonRawCollections<JsonNativeContract>.GroupingImplementationReader<'Sequence,'Key,'Element,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.GroupingImplementationReader<'Sequence,'Key,'Element,Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.SequenceWriter<'Sequence,'Element,byte,JsonUtf8Writer>(typeof<'Element>.Equals(typeof<byte>)),JsonRawCollections<JsonNativeContract>.SequenceWriter<'Sequence,'Element,char,JsonUtf16Writer>(typeof<'Element>.Equals(typeof<byte>)))

    static member internal CreateGroupingImplementation<'Sequence,'Key,'Element>(): JsonCodec<'Sequence> =
        let write (value: 'Sequence) = if isNull(box value) then Null else JsonImpl.IEnumerableSerialize(box value :?> IEnumerable<'Element>)
        let read (json: JsonValue) = if json.IsNull then Unchecked.defaultof<'Sequence> else unbox<'Sequence>(box(JsonImpl.GroupingDeserialize<'Key,'Element> json))
        JsonCodec<'Sequence>(Func<'Sequence,JsonValue>(write),Func<JsonValue,'Sequence>(read),JsonCodecShape.SequenceValues typeof<'Element>)

    static member internal RegisterMany<'T>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateMany<'T>())
        JsonMetadata.RegisterRaw<DBRefMany<'T>>(JsonRawCollections<JsonNativeContract>.ManyReader<'T,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.ManyReader<'T,Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.SequenceWriter<DBRefMany<'T>,'T,byte,JsonUtf8Writer>(false),JsonRawCollections<JsonNativeContract>.SequenceWriter<DBRefMany<'T>,'T,char,JsonUtf16Writer>(false))
    static member internal CreateMany<'T>(): JsonCodec<DBRefMany<'T>> =
        ReferenceCollectionMetadata.RegisterMany<'T>()
        let write (items: DBRefMany<'T>) = if isNull items then Null else JsonImpl.IEnumerableSerialize items
        let standard json = JsonImpl.IListDeserialize<'T,DBRefMany<'T>>(DBRefMany<'T>()) json
        let read json = JsonSequenceOperations.Read<DBRefMany<'T>>(json,false,Func<JsonValue,DBRefMany<'T>>(standard))
        JsonCodec<DBRefMany<'T>>(Func<DBRefMany<'T>,JsonValue>(write),Func<JsonValue,DBRefMany<'T>>(read),JsonCodecShape.SequenceValues typeof<'T>)

    static member internal RegisterTypedMany<'T,'Id>() =
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateTypedMany<'T,'Id>())
        JsonMetadata.RegisterRaw<DBRefMany<'T,'Id>>(JsonRawCollections<JsonNativeContract>.TypedManyReader<'T,'Id,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.TypedManyReader<'T,'Id,Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.SequenceWriter<DBRefMany<'T,'Id>,'T,byte,JsonUtf8Writer>(false),JsonRawCollections<JsonNativeContract>.SequenceWriter<DBRefMany<'T,'Id>,'T,char,JsonUtf16Writer>(false))
    static member internal CreateTypedMany<'T,'Id>(): JsonCodec<DBRefMany<'T,'Id>> =
        ReferenceCollectionMetadata.RegisterTypedMany<'T,'Id>()
        let write (items: DBRefMany<'T,'Id>) = if isNull items then Null else JsonImpl.IEnumerableSerialize items
        let standard json = JsonImpl.IListDeserialize<'T,DBRefMany<'T,'Id>>(DBRefMany<'T,'Id>()) json
        let read json = JsonSequenceOperations.Read<DBRefMany<'T,'Id>>(json,false,Func<JsonValue,DBRefMany<'T,'Id>>(standard))
        JsonCodec<DBRefMany<'T,'Id>>(Func<DBRefMany<'T,'Id>,JsonValue>(write),Func<JsonValue,DBRefMany<'T,'Id>>(read),JsonCodecShape.SequenceValues typeof<'T>)
    /// Registers FSharp optional values using the same null/value format as Nullable.
    static member internal RegisterOptionTree<'T>() =
        JsonMetadata.EnsureScalarTree<bool>()
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateOption<'T>())
        QueryValueMetadata.RegisterGeneratedCase(typeof<option<'T>>,0,Func<obj[],obj>(fun _ -> null),Func<obj,bool>(fun value -> Option.isNone (unbox<option<'T>> value)))
        QueryValueMetadata.RegisterGeneratedCase(typeof<option<'T>>,1,Func<obj[],obj>(fun values -> box(Some(unbox<'T> values[0]))),Func<obj,bool>(fun value -> Option.isSome (unbox<option<'T>> value)))
    static member internal RegisterOption<'T>() =
        JsonMetadata.RegisterOptionTree<'T>()
        JsonMetadata.RegisterRaw<option<'T>>(JsonRawCollections<JsonNativeContract>.OptionReader<'T,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.OptionReader<'T,Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.OptionWriter<'T,byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.OptionWriter<'T,char,JsonUtf16Writer>())
    static member internal CreateOption<'T>(): JsonCodec<option<'T>> =
        let write value =
            match value with
            | None -> Null
            | Some value -> JsonWriteOperations<'T>.Serialize value
        let read json =
            match json with
            | Null -> None
            | Object _ when json.Contains "HasValue" ->
                if JsonReadOperations<bool>.Deserialize json["HasValue"] then Some(JsonReadOperations<'T>.Deserialize json["Value"])
                else None
            | other -> Some(JsonReadOperations<'T>.Deserialize other)
        JsonCodec<option<'T>>(Func<option<'T>,JsonValue>(write),Func<JsonValue,option<'T>>(read),JsonCodecShape.NullableValue typeof<'T>)

    /// Registers nullable value operations without constructing generic methods at runtime.
    static member internal RegisterNullableTree<'T when 'T: struct and 'T :> ValueType and 'T: (new: unit -> 'T)>() =
        JsonMetadata.EnsureScalarTree<bool>()
        JsonMetadata.RegisterGenerated(JsonMetadata.CreateNullable<'T>())
    static member internal RegisterNullable<'T when 'T: struct and 'T :> ValueType and 'T: (new: unit -> 'T)>() =
        JsonMetadata.RegisterNullableTree<'T>()
        JsonMetadata.RegisterRaw<Nullable<'T>>(JsonRawCollections<JsonNativeContract>.NullableReader<'T,Utf8Reader>(),JsonRawCollections<JsonNativeContract>.NullableReader<'T,Utf16Reader>(),
            JsonRawCollections<JsonNativeContract>.NullableWriter<'T,byte,JsonUtf8Writer>(),JsonRawCollections<JsonNativeContract>.NullableWriter<'T,char,JsonUtf16Writer>())
    static member internal CreateNullable<'T when 'T: struct and 'T :> ValueType and 'T: (new: unit -> 'T)>(): JsonCodec<Nullable<'T>> =
        TypeMetadata.registerNullableUnderlying typeof<Nullable<'T>> typeof<'T>
        let write (value: Nullable<'T>) =
            if value.HasValue then JsonWriteOperations<'T>.Serialize value.Value else Null
        let read json =
            match json with
            | Null -> Nullable<'T>()
            | Object _ when json.Contains "HasValue" ->
                if JsonReadOperations<bool>.Deserialize json["HasValue"] then Nullable<'T>(JsonReadOperations<'T>.Deserialize json["Value"])
                else Nullable<'T>()
            | other -> Nullable<'T>(JsonReadOperations<'T>.Deserialize other)
        JsonCodec<Nullable<'T>>(Func<Nullable<'T>, JsonValue>(write), Func<JsonValue, Nullable<'T>>(read), JsonCodecShape.NullableValue typeof<'T>)

and private JsonSequenceOperations =
    static member Read<'T>(json: JsonValue, supportsValuesWrapper: bool, standard: Func<JsonValue, 'T>): 'T =
        match json with
        | Null -> Unchecked.defaultof<'T>
        | List _ -> standard.Invoke(json)
        | Object _ when json.Contains "$type" && json.Contains "$values" ->
            if supportsValuesWrapper then JsonReadOperations<'T>.Deserialize json["$values"]
            else failwithf "Type not supported for Newtownsoft deserialization: %s" typeof<'T>.FullName
        | other -> JsonValue.InvalidValue(sprintf "Expected JsonValue.List or Object{$type, $values} for array deserialization but got %A" other)

and private JsonReadOperations<'A> =
    static let read =
        let select (metadata: TypeMetadataRecord) = metadata.JsonReader
        TypeMetadataCache.Discover(typeof<'A>,select,
            (fun operation metadata -> { metadata with JsonReader = ValueSome operation }), Func<Delegate>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then
                    Func<JsonValue,'A>(fun _ -> MetadataRuntime.missing "JSON read" typeof<'A>) :> Delegate
                else
                    try JsonDeserializerImpl<'A>.Build() :> Delegate
                    with error ->
                        let failure = TypeInitializationException("SoloDatabase.JsonSerializator.JsonDeserializerImpl`1",error)
                        Func<JsonValue,'A>(fun _ -> raise failure) :> Delegate)) :?> Func<JsonValue,'A>
    static member internal DeserializeFunc(value: JsonValue) = read.Invoke(value)
    static member internal Deserialize(value: JsonValue): 'A = read.Invoke(value)

and private JsonWriteOperations<'A> =
    static let operation =
        TypeMetadataCache.Discover(typeof<'A>,(fun metadata -> metadata.JsonWriter),
            (fun operation metadata -> { metadata with JsonWriter = ValueSome operation }),
            Func<struct(Delegate * Func<obj>)>(fun () ->
                if not(MetadataRuntime.supportsDynamicCode()) then
                    struct(Func<'A,JsonValue>(fun _ -> MetadataRuntime.missing "JSON write" typeof<'A>) :> Delegate,
                           Func<obj>(fun () -> MetadataRuntime.missing "JSON shape" typeof<'A>))
                else
                    try
                        let struct(write,shape) = JsonSerializerImpl<'A>.Build()
                        struct(write :> Delegate,Func<obj>(fun () -> box shape))
                    with error ->
                        let failure = TypeInitializationException("SoloDatabase.JsonSerializator.JsonSerializerImpl`1",error)
                        struct(Func<'A,JsonValue>(fun _ -> raise failure) :> Delegate,Func<obj>(fun () -> raise failure))))
    static let write = let struct(write,_) = operation in write :?> Func<'A,JsonValue>
    static member internal Shape =
        let struct(_,shape) = operation
        unbox<JsonSerializationShape>(shape.Invoke())
    static member internal SerializeFunc(value: 'A) = write.Invoke(value)
    static member internal Serialize(value: 'A): JsonValue = write.Invoke(value)
    static member internal SerializeWithType(value: 'A): JsonValue =
        let json = write.Invoke(value)
        match json, value.GetType() |> typeToName with
        | Object _, Some name -> json["$type"] <- String name
        | _ -> ()
        json

and private ConvertStringTo<'T> =
    /// <summary>
    /// A cached function that converts a string to type 'T.
    /// </summary>
    static member val internal Convert: (string -> 'T) =
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
        | other -> JsonValue.InvalidValue(sprintf "Expected json object to be a list, instead it is a %A" other)

    /// <summary>
    /// Asserts that a JsonValue is an Object and returns the underlying dictionary, or fails.
    /// </summary>
    /// <param name="json">The JsonValue to check.</param>
    /// <returns>The underlying IDictionary&lt;string, JsonValue&gt;.</returns>
    static member internal AsDictOrFail(json: JsonValue) =
        match json with
        | Object d -> d
        | other -> JsonValue.InvalidValue(sprintf "Expected json object to be a object/dictionary, instead it is a %A" other)

    /// <summary>
    /// Tries to determine the target type for deserialization from a "$type" property in the JSON object.
    /// </summary>
    /// <typeparam name="'A">The expected base type.</typeparam>
    /// <param name="jsonObj">The JSON object.</param>
    /// <returns>The determined Type, or the original type 'A if no "$type" is found or if it's invalid.</returns>
    static member internal TryGetTypeFromJson<'A> (jsonObj: JsonValue): Type =
        // Be careful, a potential attacker can put anything in this field.
        let present, header = jsonObj.TryGetProperty "$type"
        JsonImpl.ResolveWriterType(typeof<'A>, present, header)

    static member internal ResolveWriterType(targetType: Type, headerPresent: bool, header: JsonValue): Type =
        let actual = if headerPresent then JsonImpl.TypeFromHeader header else null
        JsonImpl.SelectActualType(targetType, actual)

    /// Actual headers retain the original public tree-to-string conversion.
    static member internal TypeFromHeader(header: JsonValue): Type =
        header.ToObject<string>() |> TypeMetadata.nameToType

    static member internal SelectActualType(targetType: Type, unsafeTypeProp: Type): Type =
        match struct (TypeMetadata.isSealed targetType, unsafeTypeProp) with
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
    static member internal IEnumerableSerialize<'T>(o: 'T seq): JsonValue =
        let items = System.Collections.Generic.List<JsonValue>()
        for item in o do
            items.Add(JsonWriteOperations<'T>.Serialize item)
        List items

    /// <summary>
    /// Deserializes a JsonValue.List into an IEnumerable of a specific type. Used for types that can be constructed from an IEnumerable.
    /// </summary>
    /// <typeparam name="'T">The element type of the resulting sequence.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>An IEnumerable&lt;'T&gt;.</returns>
    static member internal IEnumerableDeserialize<'T>(jsonArray: JsonValue): IEnumerable<'T> =
        match jsonArray with
        | List items ->
            let result = new System.Collections.Generic.List<'T>(items.Count)
            for item in items do
                result.Add(JsonReadOperations<'T>.Deserialize item)
            JsonImpl.SequenceResult<'T,_>(result) :> IEnumerable<'T>
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" other)

    static member internal KeyValuePairDeserialize<'Key,'Value>(json: JsonValue): KeyValuePair<'Key,'Value> =
        KeyValuePair<'Key,'Value>(JsonReadOperations<'Key>.Deserialize json["Key"], JsonReadOperations<'Value>.Deserialize json["Value"])

    static member internal GroupingDeserialize<'Key,'Element>(json: JsonValue): Grouping<'Key,'Element> =
        let source = JsonImpl.AsListOrFail json["Items"]
        let items = ResizeArray<'Element>(source.Count)
        for index in 0 .. source.Count - 1 do items.Add(JsonReadOperations<'Element>.Deserialize source[index])
        JsonImpl.SequenceResult<'Element,_>(Grouping<'Key,'Element>(JsonReadOperations<'Key>.Deserialize json["Key"],items.ToArray()))

    static member internal OrderedEnumerableDeserialize<'Element>(json: JsonValue): IOrderedEnumerable<'Element> =
        JsonImpl.SequenceResult<'Element,_>(Enumerable.OrderBy(JsonImpl.IEnumerableDeserialize<'Element>(json),Func<'Element,int>(fun _ -> 0)))

    // These concrete results are created here, so their writer policy is known without
    // discovering the private framework iterator type. Never infer it for caller objects.
    static member internal SequenceResult<'Element,'Sequence when 'Sequence :> IEnumerable<'Element>>(value: 'Sequence): 'Sequence =
        let target = value.GetType()
        match TypeMetadataCache.Read(target,fun metadata -> metadata.RuntimeJsonWrite) with
        | ValueSome _ -> ()
        | ValueNone ->
            TypeMetadataCache.Discover(target,(fun metadata -> metadata.RuntimeJsonWrite),
                (fun operation metadata -> { metadata with RuntimeJsonWrite = ValueSome operation }),
                Func<Func<obj,obj>>(fun () -> Func<obj,obj>(fun boxed ->
                    let sequence = boxed :?> IEnumerable<'Element>
                    box (if typeof<'Element>.Equals(typeof<byte>) then JsonImpl.SerializeByteSequence(boxed :?> byte seq)
                         else JsonImpl.IEnumerableSerialize sequence)))) |> ignore
        JsonRawCollections<JsonNativeContract>.SequenceResult<'Element,'Sequence>(value)

    static member internal SerializeAbstract(value: obj) =
        if isNull value then Null else JsonImpl.SerializeByTypeWithType (value.GetType()) value

    static member internal SerializeByteSequence(value: byte seq) = String(JsonHelper.byteArrayToJSONCompatibleString(Seq.toArray value))

    static member internal SerializeNonGenericDictionary(value: IDictionary) =
        let entries = JsonHelper.newDefaultDict<JsonValue>()
        for key in value.Keys do entries.Add(key.ToString(),JsonWriteOperations<obj>.Serialize value[key])
        Object entries

    static member internal SerializeNonGenericEnumerable(value: IEnumerable) =
        let items = ResizeArray<JsonValue>()
        for item in value do items.Add(JsonWriteOperations<obj>.Serialize item)
        List items

    /// <summary>
    /// Deserializes a JsonValue.List into a collection that implements IList.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <typeparam name="'L">The list type, which must have a parameterless constructor.</typeparam>
    /// <param name="result">An empty instance of the list to populate.</param>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>The populated list.</returns>
    static member internal IListDeserialize<'T, 'L when 'L :> IList<'T>>(result: 'L) (jsonArray: JsonValue): 'L =
        match jsonArray with
        | List items ->
            for item in items do
                result.Add(JsonReadOperations<'T>.Deserialize item)
            result
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.List into an F# list.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>An F# list.</returns>
    static member internal FSharpListDeserialize<'T>(jsonArray: JsonValue): 'T list =
        match jsonArray with
        | List items ->
            [
                for item in items do
                    JsonReadOperations<'T>.Deserialize item
            ]
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.List into an array.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>An array of type 'T.</returns>
    static member internal ArrayDeserialize<'T>(jsonArray: JsonValue): 'T[] =
        match jsonArray with
        | List items ->
            let arr = Array.init items.Count (fun i -> JsonReadOperations<'T>.Deserialize items.[i])
            arr
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" other)
    
    /// <summary>
    /// Serializes a dictionary (IDictionary or IReadOnlyDictionary) into a JsonValue.Object.
    /// </summary>
    /// <typeparam name="'key">The key type of the dictionary.</typeparam>
    /// <typeparam name="'value">The value type of the dictionary.</typeparam>
    /// <param name="dictionary">The dictionary object to serialize.</param>
    /// <returns>A JsonValue.Object.</returns>
    static member internal SerializeDictionary<'key, 'value> (dictionary: obj): JsonValue =
        let entries = 
            match dictionary with
            | :? IReadOnlyDictionary<'key, 'value> as dictionary ->
                let entries = JsonHelper.newDefaultDict<JsonValue>()
                for key in dictionary.Keys do
                    let value = dictionary.[key]
                    let key = string key
                    entries.Add(key, JsonWriteOperations<'value>.Serialize value)
                entries
            | :? IDictionary<'key, 'value> as dictionary ->
                let entries = JsonHelper.newDefaultDict<JsonValue>()
                for key in dictionary.Keys do
                    let value = dictionary.[key]
                    let key = string key
                    entries.Add(key, JsonWriteOperations<'value>.Serialize value)
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
    static member internal HashSetDeserialize<'T>(jsonArray: JsonValue): HashSet<'T> =
        match jsonArray with
        | List items ->
            let result = new HashSet<'T>()
            for item in items do
                result.Add(JsonReadOperations<'T>.Deserialize item) |> ignore
            result
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.List into a Queue.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>A Queue&lt;'T&gt;.</returns>
    static member internal QueueDeserialize<'T>(jsonArray: JsonValue): Queue<'T> =
        match jsonArray with
        | List items ->
            let result = new Queue<'T>()
            for item in items do
                result.Enqueue(JsonReadOperations<'T>.Deserialize item)
            result
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.List into a Stack.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>A Stack&lt;'T&gt;.</returns>
    static member internal StackDeserialize<'T>(jsonArray: JsonValue): Stack<'T> =
        match jsonArray with
        | List items ->
            // For stack, we need to process items in reverse order to maintain order
            let result = new Stack<'T>()
            for i = items.Count - 1 downto 0 do
                result.Push(JsonReadOperations<'T>.Deserialize items.[i])
            result
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.List into a LinkedList.
    /// </summary>
    /// <typeparam name="'T">The element type.</typeparam>
    /// <param name="jsonArray">The JsonValue.List to deserialize.</param>
    /// <returns>A LinkedList&lt;'T&gt;.</returns>
    static member internal LinkedListDeserialize<'T>(jsonArray: JsonValue): LinkedList<'T> =
        match jsonArray with
        | List items ->
            let result = new LinkedList<'T>()
            for item in items do
                result.AddLast(JsonReadOperations<'T>.Deserialize item) |> ignore
            result
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.Object into an IDictionary.
    /// </summary>
    /// <typeparam name="'TKey">The key type.</typeparam>
    /// <typeparam name="'TValue">The value type.</typeparam>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>An IDictionary&lt;'TKey, 'TValue&gt;.</returns>
    static member internal DictionaryDeserialize<'TKey, 'TValue when 'TKey : equality>(jsonObj: JsonValue): IDictionary<'TKey,'TValue> =
        match jsonObj with
        | Object properties ->
            let result = new Dictionary<'TKey, 'TValue>(properties.Count)
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedKey = ConvertStringTo<'TKey>.Convert key
                    let typedValue = JsonReadOperations<'TValue>.Deserialize value
                    result.Add(typedKey, typedValue)
            result :> IDictionary<'TKey, 'TValue>
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.Object but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.Object into an IReadOnlyDictionary.
    /// </summary>
    /// <typeparam name="'TKey">The key type.</typeparam>
    /// <typeparam name="'TValue">The value type.</typeparam>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>An IReadOnlyDictionary&lt;'TKey, 'TValue&gt;.</returns>
    static member internal IReadOnlyDictionaryDeserialize<'TKey, 'TValue when 'TKey : equality>(jsonObj: JsonValue): IReadOnlyDictionary<'TKey,'TValue> =
        match jsonObj with
        | Object properties ->
            let dict = new Dictionary<'TKey, 'TValue>(properties.Count)
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedKey = ConvertStringTo<'TKey>.Convert key

                    let typedValue = JsonReadOperations<'TValue>.Deserialize value
                    dict.Add(typedKey, typedValue)
            dict :> IReadOnlyDictionary<'TKey, 'TValue>
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.Object but got %A" other)

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
                    let typedValue = JsonReadOperations<obj>.Deserialize value
                    result.Add(key, typedValue)
            result :> IDictionary
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.Object but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.Object into a custom dictionary type that implements IDictionary.
    /// </summary>
    /// <typeparam name="'TKey">The key type.</typeparam>
    /// <typeparam name="'TValue">The value type.</typeparam>
    /// <typeparam name="'TDict">The concrete dictionary type, which must have a parameterless constructor.</typeparam>
    /// <param name="result">An empty instance of the dictionary to populate.</param>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>The populated dictionary.</returns>
    static member internal CustomDictionaryDeserialize<'TKey, 'TValue, 'TDict when 'TDict :> IDictionary<'TKey, 'TValue>> (result: 'TDict) (jsonObj: JsonValue): 'TDict =
        match jsonObj with
        | Object properties ->
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedKey = ConvertStringTo<'TKey>.Convert key
                    let typedValue = JsonReadOperations<'TValue>.Deserialize value
                    result.Add(KeyValuePair<'TKey, 'TValue>(typedKey, typedValue))
            result
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.Object but got %A" other)

    /// <summary>
    /// Deserializes a JsonValue.Object into a custom non-generic dictionary type.
    /// </summary>
    /// <typeparam name="'TDict">The concrete dictionary type, which must have a parameterless constructor.</typeparam>
    /// <param name="result">An empty instance of the dictionary to populate.</param>
    /// <param name="jsonObj">The JsonValue.Object to deserialize.</param>
    /// <returns>The populated dictionary.</returns>
    static member internal CustomNonGenericDictionaryDeserialize<'TDict when 'TDict :> IDictionary> (result: 'TDict) (jsonObj: JsonValue): 'TDict =
        match jsonObj with
        | Object properties ->
            for KeyValue(key, value) in properties do
                if key <> "$type" then
                    let typedValue = JsonReadOperations<obj>.Deserialize value
                    result.Add(key, typedValue)
            result
        | other ->
            JsonValue.InvalidValue(sprintf "Expected JsonValue.Object but got %A" other)

    /// <summary>
    /// A cached function to deserialize a JsonValue to a specific runtime type.
    /// </summary>
    static member internal DeserializeByType (t: Type) (v: JsonValue) =
        let read =
            match TypeMetadataCache.Read(t,(fun metadata -> metadata.RuntimeJsonRead)) with
            | ValueSome operation -> operation.Value
            | ValueNone ->
                TypeMetadataCache.Discover(t, (fun metadata -> metadata.RuntimeJsonRead),
                    (fun operation metadata -> { metadata with RuntimeJsonRead = ValueSome operation }), Func<Func<obj,obj>>(fun () ->
                        if not (MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "JSON read" t
                        let method = typedefof<JsonReadOperations<_>>.MakeGenericType(t)
                                         .GetMethod(nameof JsonReadOperations<_>.DeserializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
                        let generated = DynamicMethod("ReadRuntimeJson",typeof<obj>,[|typeof<obj>|],typeof<JsonValue>.Module,true)
                        CodecEmitter.boxedRead (RuntimeCodecIL(generated.GetILGenerator())) typeof<JsonValue> t t.IsValueType (method :> MethodBase)
                        generated.CreateDelegate(typeof<Func<obj,obj>>) :?> Func<obj,obj>))
        read.Invoke(box v)

    static member internal SerializeByType (runtimeType: Type) (value: obj) =
        let write =
            match TypeMetadataCache.Read(runtimeType,(fun metadata -> metadata.RuntimeJsonWrite)) with
            | ValueSome operation -> operation.Value
            | ValueNone ->
                TypeMetadataCache.Discover(runtimeType, (fun metadata -> metadata.RuntimeJsonWrite),
                    (fun operation metadata -> { metadata with RuntimeJsonWrite = ValueSome operation }), Func<Func<obj,obj>>(fun () ->
                        if not (MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "JSON write" runtimeType
                        let method = typedefof<JsonWriteOperations<_>>.MakeGenericType(runtimeType)
                                         .GetMethod(nameof JsonWriteOperations<_>.SerializeFunc, BindingFlags.NonPublic ||| BindingFlags.Static)
                        let generated = DynamicMethod("WriteRuntimeJson",typeof<obj>,[|typeof<obj>|],typeof<JsonValue>.Module,true)
                        CodecEmitter.boxedWrite (RuntimeCodecIL(generated.GetILGenerator())) runtimeType (method :> MethodBase)
                        generated.CreateDelegate(typeof<Func<obj,obj>>) :?> Func<obj,obj>))
        unbox<JsonValue>(write.Invoke(value))

    static member internal SerializeByTypeWithType (runtimeType: Type) (value: obj) =
        let json = JsonImpl.SerializeByType runtimeType value
        match json, typeToName runtimeType with
        | Object _, Some name -> json["$type"] <- String name
        | _ -> ()
        json

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

/// Shared scalar conversion policy used by generated and discovered codecs.
and private JsonScalarOperations<'A> =
    static member Reader: Func<JsonValue,'A> voption =
        let inline asNumberOrParseToNumber (x: JsonValue) (cast: decimal -> 'T) (parse: string -> 'T) : 'T =
            match x with
            | Number i -> cast i
            | String s -> parse s
            | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'T>.FullName)

        match typeof<'A> with
        | t when typeof<obj>.Equals(t) ->
            Func<_,_>(fun (json: JsonValue) -> JsonImpl.DeserializeDynamic json) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType char ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | String s when s.Length = 1 && not (Char.IsDigit s.[0]) -> s.[0]
                | _ -> asNumberOrParseToNumber json char char
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType int8 ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json int8 int8) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType int16 ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json int16 int16) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType int32 ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json int32 int32) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType int64 ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json int64 int64) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType uint8 ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json uint8 uint8) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType uint16 ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json uint16 uint16) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType uint32 ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json uint32 uint32) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType uint64 ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json uint64 uint64) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType float32 ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | Null -> nanf
                | json -> asNumberOrParseToNumber json float32 float32
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType float ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | Null -> nan
                | json -> asNumberOrParseToNumber json float float
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType decimal ->
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json id decimal) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType bool ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.Boolean b -> b
                | JsonValue.Number n -> (if n = Decimal.Zero then false else true)
                | JsonValue.String s -> s = "true" || (JsonImpl.IsParsableToNumber (s.AsSpan()) && int64 s <> 0)
                | JsonValue.Null -> false
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
                
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id: bigint -> bigint) -> 
            Func<_,_>(fun (json: JsonValue) ->
                asNumberOrParseToNumber json bigint (fun s -> BigInteger.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture))) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id: IntPtr -> IntPtr) -> 
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json (fun d -> d |> int64 |> IntPtr) (fun s -> int64 s |> IntPtr)) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id: UIntPtr -> UIntPtr) -> 
            Func<_,_>(fun (json: JsonValue) -> asNumberOrParseToNumber json (fun d -> d |> uint64 |> UIntPtr) (fun s -> uint64 s |> UIntPtr)) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id : byte array -> byte array) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> JsonHelper.JSONStringToByteArray s
                | JsonValue.Null -> null
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
                
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id : List<byte> -> List<byte>) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> ResizeArray<byte>(JsonHelper.JSONStringToByteArray s)
                | JsonValue.Null -> null
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
                
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id : byte list -> byte list) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> JsonHelper.JSONStringToByteArray s |> Array.toList
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
                
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | t when typeof<byte seq>.Equals(t) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.Null -> null
                | JsonValue.String s -> JsonHelper.JSONStringToByteArray s :> byte seq
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
                
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType string ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> s
                | JsonValue.Number i -> string i
                | JsonValue.Null -> null
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
                
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id: Type -> Type) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> TypeMetadata.nameToType s
                | JsonValue.Null -> null
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
                
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id: Guid -> Guid) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> Guid.Parse s
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
                
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id: DateTimeOffset -> DateTimeOffset) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> DateTimeOffset.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> int64 |> DateTimeOffset.FromUnixTimeMilliseconds
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome
    
        | OfType (id: DateTime -> DateTime) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> DateTime.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> int64 |> DateTime.FromBinary
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome
    
        | OfType (id: DateOnly -> DateOnly) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> DateOnly.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> int |> DateOnly.FromDayNumber
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome
    
        | OfType (id: TimeOnly -> TimeOnly) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> TimeOnly.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> float |> TimeSpan.FromMilliseconds |> TimeOnly.FromTimeSpan
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome
    
        | OfType (id: TimeSpan -> TimeSpan) ->
            Func<_,_>(fun (json: JsonValue) ->
                match json with
                | JsonValue.String s -> TimeSpan.Parse(s, CultureInfo.InvariantCulture)
                | JsonValue.Number n -> n |> float |> TimeSpan.FromMilliseconds
                | other -> JsonValue.InvalidValue(sprintf "Cannot convert %A into %s" other typeof<'A>.FullName)
            ) :> obj :?> Func<JsonValue,'A> |> ValueSome

        | OfType (id: JsonValue -> JsonValue) ->
            Func<_,_>(fun (json: JsonValue) -> unbox<'A>(box json)) |> ValueSome

        | _ -> ValueNone

    static member Writer: Func<'A,JsonValue> voption =
        match typeof<'A> with
        | OfType bool -> 
            Func<_,_>(fun (o: bool) -> Boolean o)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType char -> 
            Func<_,_>(fun (o: char) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType uint8 -> 
            Func<_,_>(fun (o: uint8) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome
            
        | OfType uint16 ->
            Func<_,_>(fun (o: uint16) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType uint32 -> 
            Func<_,_>(fun (o: uint32) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType uint64 ->
            Func<_,_>(fun (o: uint64) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType int8 ->
            Func<_,_>(fun (o: int8) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType int16 ->
            Func<_,_>(fun (o: int16) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType int32 ->
            Func<_,_>(fun (o: int32) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType int64 -> 
            Func<_,_>(fun (o: int64) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType (id: bigint -> bigint) -> 
            Func<_,_>(fun (o: bigint) ->
                if o >= BigInteger(Decimal.MinValue) && o <= BigInteger(Decimal.MaxValue) then Number(decimal o)
                else String(o.ToString(CultureInfo.InvariantCulture)))
            :> obj :?> Func<'A,JsonValue> |> ValueSome


        | OfType (id: IntPtr -> IntPtr) -> 
            Func<_,_>(fun (o: IntPtr) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType (id: UIntPtr -> UIntPtr) -> 
            Func<_,_>(fun (o: UIntPtr) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType float32 ->
            Func<_,_>(fun (o: float32) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType float ->
            Func<_,_>(fun (o: float) -> Number (decimal o))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType decimal ->
            Func<_,_>(fun (o: decimal) -> Number o)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType DateTimeOffset -> 
            Func<_,_>(fun (date: DateTimeOffset) -> date.ToUnixTimeMilliseconds() |> decimal |> Number)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType DateTime -> 
            Func<_,_>(fun (date: DateTime) -> date.ToBinary() |> decimal |> Number)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType (DateOnly: unit -> DateOnly) -> 
            Func<_,_>(fun (date: DateOnly) -> date.DayNumber |> decimal |> Number)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType TimeOnly -> 
            Func<_,_>(fun (time: TimeOnly) -> time.ToTimeSpan().TotalMilliseconds |> int64 (* Convert to int64 to allow for higher precision in SQLite. *) |> decimal |> Number)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType TimeSpan -> 
            Func<_,_>(fun (ts: TimeSpan) -> ts.TotalMilliseconds |> int64 |> decimal |> Number)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType (Guid: unit -> Guid) -> 
            Func<_,_>(fun (guid: Guid) -> guid.ToString("D", CultureInfo.InvariantCulture) |> String)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType string -> 
            Func<_,_>(fun (s: string) ->
                if isNull s
                then Null
                else String s)
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        // For efficient storage.
        | OfType (id: byte array -> byte array) ->
            Func<_,_>(fun (ba: byte array) ->
                if isNull ba
                then Null
                else String(JsonHelper.byteArrayToJSONCompatibleString ba))
            :> obj :?> Func<'A,JsonValue> |> ValueSome

        | OfType (id: JsonValue -> JsonValue) -> Func<_,_>(fun value -> box value :?> JsonValue) |> ValueSome

        | OfType (id: byte seq -> byte seq) ->
            Func<_,_>(fun (value: 'A) ->
                if isNull(box value) then Null
                else JsonImpl.SerializeByteSequence(box value :?> byte seq)) |> ValueSome

        | _ -> ValueNone

and private JsonCodecDiscovery =
    static member Decision(target: Type) = TypeSupport.Runtime target
    static member ContractArguments(target: Type, decision: TypeSupportDecision): Type[] =
        if decision.Contract<>"" then
            let original=if target.IsGenericType then target.GetGenericTypeDefinition() else target
            let contract=if original.FullName=decision.Contract then target else target.GetInterface(decision.Contract.Split('.') |> Array.last)
            TypeMetadata.GenericTypeArgCache.Get contract
        elif decision.Arguments=CodecArguments.Element then [|target.GetElementType()|]
        else TypeMetadata.GenericTypeArgCache.Get target
    static member ValueShape(target: Type) =
        if typeof<JsonValue>.IsAssignableFrom target then JsonSerializationShape.Uninterpreted
        else
            let decision=JsonCodecDiscovery.Decision target
            let modelShape() =
                let model=JsonModelDiscovery.Model target
                if model.Union then JsonModelDiscovery.UnionStorage model |> ignore
                JsonSerializationShape.DeclaredMembers(CodecEmitter.writtenMembers model |> Array.map(fun index ->
                    let memberInfo=model.Members[index]
                    struct(memberInfo.Name,memberInfo.Type)))
            let arguments()=JsonCodecDiscovery.ContractArguments(target,decision)
            if decision.Shape=TypeShape.Object && typeof<IEnumerable>.IsAssignableFrom target then JsonSerializationShape.Uninterpreted
            else
                match decision.Shape with
                | TypeShape.Scalar | TypeShape.Enum | TypeShape.DocumentReference
                | TypeShape.FileReference | TypeShape.DirectoryReference -> JsonSerializationShape.Scalar
                | TypeShape.TypedDocumentReference -> JsonSerializationShape.ReferenceIdentity((arguments())[decision.IdentityArgument])
                | TypeShape.Nullable | TypeShape.Option -> JsonSerializationShape.NullableValue((arguments())[0])
                | TypeShape.Tuple ->
                    JsonSerializationShape.DeclaredArrayMembers(CodecTupleEmitter.leaves(RuntimeTupleFacts.model target)
                        |> Array.map(fun path -> (snd(Array.last path)).Type))
                | TypeShape.Dictionary | TypeShape.SortedDictionary | TypeShape.DictionaryInterface
                | TypeShape.ReadOnlyDictionaryInterface | TypeShape.CustomDictionary | TypeShape.CustomReadOnlyDictionary ->
                    JsonSerializationShape.DictionaryValues((arguments())[decision.ElementArgument])
                | TypeShape.Grouping ->
                    let arguments=arguments()
                    JsonSerializationShape.GroupingValues(arguments[0],arguments[1])
                | TypeShape.NonGenericDictionary | TypeShape.BsonDocument -> JsonSerializationShape.Uninterpreted
                | TypeShape.DynamicObject -> JsonSerializationShape.Dynamic
                | TypeShape.ManyReference | TypeShape.TypedManyReference -> JsonSerializationShape.SequenceValues((arguments())[0])
                | TypeShape.FileMany -> JsonSerializationShape.SequenceValues typeof<FileRef>
                | TypeShape.DirectoryMany -> JsonSerializationShape.SequenceValues typeof<DirectoryRef>
                | TypeShape.Array | TypeShape.List | TypeShape.FSharpList | TypeShape.HashSet | TypeShape.Queue
                | TypeShape.Stack | TypeShape.LinkedList | TypeShape.Enumerable | TypeShape.OrderedEnumerable
                | TypeShape.GroupingImplementation | TypeShape.CustomSequence ->
                    let element=(arguments())[decision.ElementArgument]
                    if typeof<byte>.Equals element then JsonSerializationShape.Scalar
                    else JsonSerializationShape.SequenceValues element
                | _ -> modelShape()
    static member Select(target: Type): (string * Type[] * obj[]) option =
        let decision = JsonCodecDiscovery.Decision target
        let definition = if target.IsGenericType then target.GetGenericTypeDefinition().FullName else target.FullName
        let preparation = CodecPreparation.factory definition decision target.IsAbstract
        if preparation.Factory = "" then None
        else
            let arguments =
                match preparation.Arguments with
                | CodecArguments.None -> [||]
                | CodecArguments.Target -> [|target|]
                | CodecArguments.Element -> [|target.GetElementType()|]
                | CodecArguments.Generic -> TypeMetadata.GenericTypeArgCache.Get target
                | CodecArguments.TargetAndUnderlying -> [|target;TypeMetadata.enumUnderlying target|]
                | CodecArguments.TargetAndContract ->
                    Array.append [|target|] (JsonCodecDiscovery.ContractArguments(target,decision))
                | value -> invalidOp (string value)
            let values = if preparation.ArrayWrapper then [|box (TypeSupport.SupportsValuesWrapper(target.GetElementType().FullName))|] else [||]
            Some(preparation.Factory,arguments,values)
    static member TryBuild<'T>(): JsonCodec<'T> voption =
        match JsonCodecDiscovery.Select(typeof<'T>) with
        | None -> ValueNone
        | Some("Tuple",_,_) -> ValueSome(JsonTupleGeneration.Codec<'T>())
        | Some(name,arguments,values) ->
            let definition = typeof<JsonMetadata>.GetMethod("Create" + name,BindingFlags.NonPublic ||| BindingFlags.Static)
            let factory = if arguments.Length = 0 then definition else definition.MakeGenericMethod(arguments)
            try ValueSome(factory.Invoke(null,values) :?> JsonCodec<'T>)
            with :? TargetInvocationException as error when not (isNull error.InnerException) ->
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error.InnerException).Throw()
                Unchecked.defaultof<_>

and private JsonTupleGeneration =
    static member Codec<'T>(): JsonCodec<'T> =
        let model = RuntimeTupleFacts.model typeof<'T>
        let api = JsonObjectGeneration.Api()
        let writer = DynamicMethod("WriteTuple",typeof<JsonValue[]>,[|typeof<'T>|],typeof<JsonValue>.Module,true)
        CodecTupleEmitter.writer (RuntimeCodecIL(writer.GetILGenerator())) typeof<JsonValue> api.Serialize model
        let reader = DynamicMethod("ReadTuple",typeof<'T>,[|typeof<JsonValue>|],typeof<JsonValue>.Module,true)
        let tupleElement = typeof<JsonMetadata>.GetMethod("TupleElement",BindingFlags.NonPublic ||| BindingFlags.Static) :> MethodBase
        CodecTupleEmitter.reader (RuntimeCodecIL(reader.GetILGenerator())) tupleElement api.Deserialize model
        let members = CodecTupleEmitter.leaves model |> Array.mapi (fun index path ->
            let memberValue = snd(Array.last path)
            let getter = DynamicMethod("TupleMember",typeof<obj>,[|typeof<obj>|],typeof<JsonValue>.Module,true)
            CodecTupleEmitter.getter (RuntimeCodecIL(getter.GetILGenerator())) true model path memberValue.Type.IsValueType
            let read = getter.CreateDelegate(typeof<Func<obj,obj>>) :?> Func<obj,obj>
            let name = "Item" + string(index + 1)
            JsonStoredMember(typeof<'T>,memberValue.Type,name,name,typeof<'T>.IsValueType,read))
        JsonMetadata.CreateTuple<'T>(writer.CreateDelegate(typeof<Func<'T,JsonValue[]>>) :?> Func<'T,JsonValue[]>,
                                    reader.CreateDelegate(typeof<Func<JsonValue,'T>>) :?> Func<JsonValue,'T>,members)

and private JsonObjectGeneration =
    static member internal Api(): CodecApi<Type,MethodBase> =
        let flags = BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static ||| BindingFlags.Instance
        let method (owner: Type) name arguments count =
            let selected = owner.GetMethods(flags) |> Array.filter (fun method ->
                method.Name = name && method.GetGenericArguments().Length = Array.length arguments &&
                (match count with None -> true | Some count -> method.GetParameters().Length = count))
            if selected.Length <> 1 then invalidOp ("Codec operation is not unique: " + owner.Name + "." + name)
            (if arguments.Length = 0 then selected[0] else selected[0].MakeGenericMethod(arguments)) :> MethodBase
        let json name arguments = method typeof<JsonValue> name arguments None
        let metadata name arguments = method typeof<JsonMetadata> name arguments None
        { Json = typeof<JsonValue>; Int32 = typeof<int>; Dictionary = typeof<Dictionary<string,JsonValue>>
          ObjectDictionary = typeof<IDictionary<string,JsonValue>>
          Null = json "get_Null" [||]; IsNull = json "get_IsNull" [||]
          GetType = typeof<obj>.GetMethod("GetType") :> MethodBase
          TypeFromHandle = typeof<Type>.GetMethod("GetTypeFromHandle") :> MethodBase
          SerializeActual = metadata "SerializeActual" [||]
          ActualType = fun target -> metadata "ActualType" [|target|]
          ToRuntimeObject = method typeof<JsonValue> "ToObject" [||] (Some 1)
          DictionaryConstructor = typeof<Dictionary<string,JsonValue>>.GetConstructor([|typeof<int>|]) :> MethodBase
          DictionaryAdd = typeof<Dictionary<string,JsonValue>>.GetMethod("Add") :> MethodBase
          NewObject = json "NewObject" [||]; ObjectProperties = metadata "ObjectProperties" [||]
          TryGetValue = typeof<IDictionary<string,JsonValue>>.GetMethod("TryGetValue") :> MethodBase
          Uninitialized = fun target -> metadata "Uninitialized" [|target|]
          Serialize = fun target -> json "Serialize" [|target|]
          Deserialize = fun target -> json "ToObject" [|target|]
          ExceptionConstructor = typeof<Exception>.GetConstructor([|typeof<string>|]) :> MethodBase
          ArgumentExceptionConstructor = typeof<ArgumentException>.GetConstructor([|typeof<string>;typeof<string>|]) :> MethodBase
          InitializationExceptionConstructor = typeof<TypeInitializationException>.GetConstructor([|typeof<string>;typeof<Exception>|]) :> MethodBase
          MissingFactoryConstructor = typeof<KeyNotFoundException>.GetConstructor([|typeof<string>|]) :> MethodBase
          UnionCaseSelected = metadata "UnionCaseSelected" [||]
          Contains = json "Contains" [||]; GetProperty = json "GetProperty" [||]
          EnsureStack = typeof<RuntimeHelpers>.GetMethod("EnsureSufficientExecutionStack") :> MethodBase
          RawReader = false }

    static member Reader<'T>() =
        let method = DynamicMethod("ReadJson",typeof<'T>,[|typeof<JsonValue>|],typeof<JsonValue>.Module,true)
        let model = JsonModelDiscovery.Model typeof<'T>
        JsonModelDiscovery.PublishConstruction model
        CodecEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) (JsonObjectGeneration.Api()) model
        method.CreateDelegate(typeof<Func<JsonValue,'T>>) :?> Func<JsonValue,'T>

    static member Writer<'T>() =
        let model = JsonModelDiscovery.Model typeof<'T>
        if model.Union then JsonModelDiscovery.UnionStorage model |> ignore
        JsonModelDiscovery.PublishConstruction model
        let method = DynamicMethod("WriteJson",typeof<JsonValue>,[|typeof<'T>|],typeof<JsonValue>.Module,true)
        CodecEmitter.writer (RuntimeCodecIL(method.GetILGenerator())) (JsonObjectGeneration.Api()) model
        let members = CodecEmitter.writtenMembers model |> Array.map (fun index ->
            let memberValue = model.Members[index]
            let declaring =
                match memberValue.Field,memberValue.Getter with
                | Some field,_ -> field.DeclaringType
                | _,Some method -> method.DeclaringType
                | _ -> typeof<'T>
            let getter = DynamicMethod("JsonMember",typeof<obj>,[|typeof<obj>|],typeof<JsonValue>.Module,true)
            CodecEmitter.boxedMemberGetter (RuntimeCodecIL(getter.GetILGenerator())) declaring declaring.IsValueType memberValue memberValue.Type.IsValueType
            let read = getter.CreateDelegate(typeof<Func<obj,obj>>) :?> Func<obj,obj>
            QueryValueMetadata.RegisterReader(declaring,memberValue.Name,memberValue.Field.IsSome,memberValue.Type,read)
            JsonStoredMember(declaring,memberValue.Type,memberValue.Name,memberValue.Name,memberValue.Field.IsSome,read))
        struct(method.CreateDelegate(typeof<Func<'T,JsonValue>>) :?> Func<'T,JsonValue>,JsonSerializationShape.ObjectMembers members)

/// <summary>
/// Provides a cached, compiled deserialization function for a specific type 'A.
/// </summary>
/// <typeparam name="'A">The target type for deserialization.</typeparam>
and private JsonRawWriteMetadata<'E,'O,'Contract when 'O : struct and 'O :> IJsonValueOutput<'E>> =
    static member Select(metadata: TypeMetadataRecord): MetadataValue<JsonRawWriteOperations> voption =
        if JsonOperationPolicy<'Contract>.IsStored then
            if typeof<'E>.Equals(typeof<byte>) then metadata.StoredJsonUtf8Write else metadata.StoredJsonUtf16Write
        elif typeof<'E>.Equals(typeof<byte>) then metadata.JsonUtf8Write else metadata.JsonUtf16Write
    static member Store(operation: MetadataValue<JsonRawWriteOperations>, metadata: TypeMetadataRecord): TypeMetadataRecord =
        match JsonRawWriteMetadata<'E,'O,'Contract>.Select metadata with
        | ValueSome _ -> metadata
        | ValueNone ->
            if JsonOperationPolicy<'Contract>.IsStored then
                if typeof<'E>.Equals(typeof<byte>) then { metadata with StoredJsonUtf8Write = ValueSome operation }
                else { metadata with StoredJsonUtf16Write = ValueSome operation }
            elif typeof<'E>.Equals(typeof<byte>) then { metadata with JsonUtf8Write = ValueSome operation }
            else { metadata with JsonUtf16Write = ValueSome operation }
    static member ForType(target: Type, value: obj, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let operations =
            match TypeMetadataCache.Read(target,JsonRawWriteMetadata<'E,'O,'Contract>.Select) with
            | ValueSome operation -> operation.Value
            | ValueNone ->
                TypeMetadataCache.Discover(target,JsonRawWriteMetadata<'E,'O,'Contract>.Select,
                    (fun operation metadata -> JsonRawWriteMetadata<'E,'O,'Contract>.Store(operation,metadata)),Func<JsonRawWriteOperations>(fun () ->
                        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" target
                        let helper = typeof<JsonRawWrite<obj,byte,JsonUtf8Writer,'Contract>>.GetGenericTypeDefinition().MakeGenericType(target,typeof<'E>,typeof<'O>,typeof<'Contract>)
                                            .GetMethod("Fixed",BindingFlags.Static ||| BindingFlags.NonPublic)
                        try unbox<JsonRawWriteOperations>(helper.Invoke(null,[||]))
                        with :? TargetInvocationException as error ->
                            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error.InnerException).Throw()
                            Unchecked.defaultof<JsonRawWriteOperations>))
        (operations.WriteRuntime :?> JsonDirectRuntimeWriter<'E>).Invoke(value,&destination,&state)

    static member Runtime(value: obj, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        JsonRawOutput<'E,'O,'Contract>.Actual(value,value.GetType(),&destination,&state)

and private JsonRawWrite<'T,'E,'O,'Contract when 'O : struct and 'O :> IJsonValueOutput<'E>> =
    static let operations =
        try JsonRawWrite<'T,'E,'O,'Contract>.Prepare()
        with error ->
            // Keep the original preparation error outside a new CLR initializer wrapper.
            let failure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error)
            let writer = JsonDirectWriter<'T,'E>(fun _ _ _ -> failure.Throw())
            { Writer = writer; WriteRuntime = JsonDirectRuntimeWriter<'E>(fun _ _ _ -> failure.Throw()) }
    static let writer =
        if isNull operations.Writer then
            let runtime = operations.WriteRuntime :?> JsonDirectRuntimeWriter<'E>
            JsonDirectWriter<'T,'E>(fun value destination state -> runtime.Invoke(box value,&destination,&state))
        else operations.Writer :?> JsonDirectWriter<'T,'E>

    static member private Build(): JsonRawWriteOperations =
        let target = typeof<'T>
        let decision = JsonCodecDiscovery.Decision target
        let operation = CodecPreparation.writer(target.FullName,decision,JsonOperationPolicy<'Contract>.IsStored,
                            target.IsAbstract,typeof<IEnumerable<byte>>.IsAssignableFrom target,typeof<IEnumerable>.IsAssignableFrom target && not(typeof<string>.Equals target))
        let writer =
            match operation with
            | RawRootOperation.StoredNumeric ->
                (JsonStoredNumericRuntime.writerMethod target typeof<'E>).CreateDelegate(typeof<JsonDirectWriter<'T,'E>>) :?> JsonDirectWriter<'T,'E>
            | RawRootOperation.Scalar -> JsonRawScalarWriter.Create<'T,'E,'O>().Value
            | RawRootOperation.Tree ->
                JsonRawCollections<'Contract>.TreeWriter<'T,'E,'O>(Func<'T,JsonValue>(fun value -> unbox<JsonValue>(box value)))
            | RawRootOperation.Abstract -> JsonRawCollections<'Contract>.AbstractWriter<'T,'E,'O>()
            | RawRootOperation.Dynamic ->
                JsonDirectWriter<'T,'E>(fun value destination state ->
                    let boxed = box value
                    if isNull boxed then JsonRawOutput<'E,'O,'Contract>.Null(&destination,&state)
                    elif typeof<obj>.Equals(boxed.GetType()) then
                        JsonRawOutput<'E,'O,'Contract>.BeginObject(&destination,&state)
                        Unchecked.defaultof<'O>.Ascii('}'B,&destination,&state)
                    else JsonRawOutput<'E,'O,'Contract>.SerializeActual(boxed,&destination,&state))
            | _ ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" target
                if operation = RawRootOperation.Tuple then JsonRawOutput<'E,'O,'Contract>.Tuple<'T>()
                else
                    match JsonRawCollections<'Contract>.Writer<'T,'E,'O>() with
                    | ValueSome writer -> writer
                    | ValueNone -> JsonRawOutput<'E,'O,'Contract>.Model<'T>()
        { Writer = writer; WriteRuntime = JsonDirectRuntimeWriter<'E>(fun value destination state -> writer.Invoke(unbox<'T> value,&destination,&state)) }
    static member private Prepare(): JsonRawWriteOperations =
        TypeMetadataCache.Discover(typeof<'T>,JsonRawWriteMetadata<'E,'O,'Contract>.Select,
            (fun operation metadata -> JsonRawWriteMetadata<'E,'O,'Contract>.Store(operation,metadata)),
            Func<JsonRawWriteOperations>(JsonRawWrite<'T,'E,'O,'Contract>.Build))
    static member internal Fixed(): JsonRawWriteOperations = operations
    static member internal Ensure(): unit = operations |> ignore
    static member internal Write(value: 'T, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        writer.Invoke(value,&destination,&state)
    static member internal Member(value: 'T, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let previousType, previousId = state.IncludeType, state.OmitId
        state.IncludeType <- false
        state.OmitId <- false
        try JsonRawWrite<'T,'E,'O,'Contract>.Write(value,&destination,&state)
        finally
            state.IncludeType <- previousType
            state.OmitId <- previousId
    static member internal Discard(value: 'T): unit =
        let mutable destination = Span<'E>.Empty
        let mutable state = JsonWriteState<'E>(null,null,null)
        state.Discard <- true
        JsonRawWrite<'T,'E,'O,'Contract>.Write(value,&destination,&state)

and private JsonRawOutput<'E,'O,'Contract when 'O : struct and 'O :> IJsonValueOutput<'E>> =
    static member internal Null(destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        JsonOutputOperations.literal Unchecked.defaultof<'O> "null" &destination &state
    static member internal BeginObject(destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        Unchecked.defaultof<'O>.Ascii('{'B,&destination,&state)
    static member internal Property(name: string, comma: bool, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        if comma then output.Ascii(','B,&destination,&state)
        JsonStringEscaping.write output name &destination &state
        output.Ascii(':'B,&destination,&state)
    static member internal EndObject<'T>(comma: bool, ownsType: bool, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        if state.IncludeType && not ownsType then
            match typeToName typeof<'T> with
            | Some name ->
                JsonRawOutput<'E,'O,'Contract>.Property("$type",comma,&destination,&state)
                JsonStringEscaping.write output name &destination &state
            | None -> ()
        output.Ascii('}'B,&destination,&state)
    static member internal OmitId(state: byref<JsonWriteState<'E>>): bool = state.OmitId

    static member internal TypeMember<'Model,'Value>(value: 'Value, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        match (if state.IncludeType then typeToName typeof<'Model> else None) with
        | Some name ->
            JsonRawWrite<'Value,'E,'O,'Contract>.Discard(value)
            JsonStringEscaping.write Unchecked.defaultof<'O> name &destination &state
        | None -> JsonRawWrite<'Value,'E,'O,'Contract>.Member(value,&destination,&state)

    static member internal ExplicitTree(runtimeType: Type, json: JsonValue, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        match json with
        | Object values ->
            if state.OmitId then values.Remove "Id" |> ignore
            if state.IncludeType then
                match typeToName runtimeType with Some name -> values["$type"] <- String name | None -> ()
        | _ -> ()
        if not state.Discard then JsonRawOutput<'E,'O,'Contract>.Tree(json,&destination,&state)

    static member internal SerializeActual(value: obj, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let previous = state.IncludeType
        state.IncludeType <- true
        try JsonRawWriteMetadata<'E,'O,'Contract>.Runtime(value,&destination,&state)
        finally state.IncludeType <- previous
    static member internal Tree(value: JsonValue, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let output = Unchecked.defaultof<'O>
        match value with
        | Null -> JsonRawOutput<'E,'O,'Contract>.Null(&destination,&state)
        | Boolean value -> JsonOutputOperations.literal output (if value then "true" else "false") &destination &state
        | Number value -> output.Decimal(value,&destination,&state)
        | String value -> JsonStringEscaping.write output value &destination &state
        | List values ->
            output.Ascii('['B,&destination,&state)
            for index = 0 to values.Count - 1 do
                if index > 0 then output.Ascii(','B,&destination,&state)
                JsonRawOutput<'E,'O,'Contract>.Tree(values[index],&destination,&state)
            output.Ascii(']'B,&destination,&state)
        | Object values ->
            output.Ascii('{'B,&destination,&state)
            let mutable comma = false
            for KeyValue(name,value) in values do
                JsonRawOutput<'E,'O,'Contract>.Property(name,comma,&destination,&state)
                comma <- true
                JsonRawOutput<'E,'O,'Contract>.Tree(value,&destination,&state)
            output.Ascii('}'B,&destination,&state)
    static member internal BeginArray(destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        Unchecked.defaultof<'O>.Ascii('['B,&destination,&state)
    static member internal Separator(destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        Unchecked.defaultof<'O>.Ascii(','B,&destination,&state)
    static member internal EndArray(destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        Unchecked.defaultof<'O>.Ascii(']'B,&destination,&state)
    static member private CreateTupleMethod(target: Type): DynamicMethod =
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let parameters = typeof<JsonDirectWriter<obj,'E>>.GetMethod("Invoke",BindingFlags.Instance ||| flags).GetParameters() |> Array.map _.ParameterType
        parameters[0] <- target
        let state = typedefof<JsonWriteState<_>>.MakeGenericType(typeof<'E>)
        let scope: CodecTupleWriterScope<Type,FieldInfo> =
            { Boolean = typeof<bool>
              IncludeType = state.GetField("IncludeType",BindingFlags.Instance ||| flags)
              OmitId = state.GetField("OmitId",BindingFlags.Instance ||| flags)
              Required = fun child -> TypeSupport.TupleMemberScope(TypeSupport.RawPrimitive child.FullName,JsonOptionalRuntime.kind child) }
        let method = DynamicMethod("WriteRawTuple",typeof<Void>,parameters,typeof<JsonValue>.Module,true)
        CodecTupleEmitter.rawWriter (RuntimeCodecIL(method.GetILGenerator())) (JsonRawOutput<'E,'O,'Contract>.ForwardApi target) scope (RuntimeTupleFacts.model target)
        method

    static member internal Tuple<'T>(): JsonDirectWriter<'T,'E> =
        JsonDefaultMethods.callable<JsonDirectWriter<'T,'E>> typeof<'T> typeof<'E> typeof<'O> (JsonOperationPolicy<'Contract>.Name "tuple-write")
            (fun () -> JsonRawOutput<'E,'O,'Contract>.CreateTupleMethod(typeof<'T>) :> MethodBase)

    static member internal IncludeType(state: byref<JsonWriteState<'E>>) = state.IncludeType
    static member internal SetIncludeType(state: byref<JsonWriteState<'E>>, value: bool) = state.IncludeType <- value
    static member internal Actual(value: obj, actual: Type, destination: byref<Span<'E>>, state: byref<JsonWriteState<'E>>): unit =
        let target = JsonValue.RuntimeWriterType(value, actual)
        if TypeSupport.PolymorphicNative(JsonOperationPolicy<'Contract>.IsStored,target.FullName) then
            JsonRawWriteMetadata<'E,'O,JsonNativeContract>.ForType(target,value,&destination,&state)
        else JsonRawWriteMetadata<'E,'O,'Contract>.ForType(target,value,&destination,&state)
    static member internal Polymorphic(known: Type -> struct(Type * MethodBase)[]): CodecPolymorphicApi<Type,MethodBase> =
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        { RuntimeType = typeof<Type>
          Equals = typeof<Type>.GetMethod("Equals",[|typeof<Type>|]) :> MethodBase
          IncludeType = typeof<JsonRawOutput<'E,'O,'Contract>>.GetMethod("IncludeType",flags) :> MethodBase
          SetIncludeType = typeof<JsonRawOutput<'E,'O,'Contract>>.GetMethod("SetIncludeType",flags) :> MethodBase
          Actual = typeof<JsonRawOutput<'E,'O,'Contract>>.GetMethod("Actual",flags) :> MethodBase
          Known = fun declared -> known declared |> Array.map(fun struct(target,body) ->

              { Type = target; ValueType = target.IsValueType
                Operation = (body) }) }

    static member internal ChildOperation(target: Type): MethodBase option =
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let definition = if target.IsGenericType then target.GetGenericTypeDefinition().FullName else target.FullName
        let rank = if target.IsArray then target.GetArrayRank() else 0
        match TypeSupport.RawChildWriter(definition,target.IsEnum,rank,JsonOperationPolicy<'Contract>.IsStored) with
        | RawChildOperation.Member -> None
        | RawChildOperation.StoredNumeric -> Some(JsonStoredNumericRuntime.writerMethod target typeof<'E> :> MethodBase)
        | RawChildOperation.Adapted native ->
            Some(if native then JsonRawCollections<JsonNativeContract>.WriteMethod<'E,'O>(target) :> MethodBase
                 else JsonRawCollections<'Contract>.WriteMethod<'E,'O>(target) :> MethodBase)
        | RawChildOperation.Sequence _ ->
            JsonRawCollections<'Contract>.Invoke(typeof<JsonRawCollections<'Contract>>,"SequenceWriteMethod",[|target;target.GetGenericArguments().[0];typeof<'E>;typeof<'O>|],
                [|box(target.GetGenericArguments().[0].Equals(typeof<byte>))|]) :?> MethodBase |> Some
        | RawChildOperation.Numeric -> Some(JsonWriterRuntime.number target typeof<'E> :> MethodBase)
        | RawChildOperation.Text -> Some(JsonWriterRuntime.text typeof<'E> :> MethodBase)
        | RawChildOperation.Boolean -> Some(JsonWriterRuntime.boolean typeof<'E> :> MethodBase)
        | RawChildOperation.Guid -> Some(JsonWriterRuntime.guid typeof<'E> :> MethodBase)
        | RawChildOperation.Bytes -> Some(JsonWriterRuntime.bytes typeof<'E> :> MethodBase)
        | RawChildOperation.Primitive name -> Some(typeof<JsonRawPrimitive>.GetMethod("Write" + name,flags).MakeGenericMethod(typeof<'E>,typeof<'O>) :> MethodBase)
        | RawChildOperation.Array -> Some(JsonRawOutput<'E,'O,'Contract>.ArrayMethod(target.GetElementType()) :> MethodBase)
        | RawChildOperation.Formatted -> invalidOp "A formatted reader cannot be selected for output."

    static member internal ForwardApi(modelType: Type): CodecRawWriterApi<Type,MethodBase> =
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let helper name arguments =
            let method = typeof<JsonRawOutput<'E,'O,'Contract>>.GetMethod(name,flags)
            (if Array.isEmpty arguments then method else method.MakeGenericMethod arguments) :> MethodBase
        { Boolean = typeof<bool>
          Polymorphic = JsonRawOutput<'E,'O,'Contract>.Polymorphic(fun declared -> TypeMetadataCache.KnownJsonWriterBodies(declared,typeof<'E>,typeof<'O>,JsonOperationPolicy<'Contract>.Name "raw-model-body"))
          Operation = JsonRawOutput<'E,'O,'Contract>.ChildOperation
          Null = JsonWriterRuntime.constant typeof<'E> "null" :> MethodBase
          SerializeActual = helper "SerializeActual" [||]
          BeginArray = JsonWriterRuntime.constant typeof<'E> "[" :> MethodBase; Separator = JsonWriterRuntime.constant typeof<'E> "," :> MethodBase; EndArray = JsonWriterRuntime.constant typeof<'E> "]" :> MethodBase
          BeginObject = JsonWriterRuntime.constant typeof<'E> "{" :> MethodBase; Property = fun text -> JsonWriterRuntime.property typeof<'E> text :> MethodBase
          TypeLiteral = fun target -> JsonWriterRuntime.typeName target typeof<'E> :> MethodBase
          EndObject = JsonWriterRuntime.constant typeof<'E> "}" :> MethodBase; OmitId = helper "OmitId" [||]
          Discard = fun target -> typeof<JsonRawWrite<obj,byte,JsonUtf8Writer,'Contract>>.GetGenericTypeDefinition().MakeGenericType(target,typeof<'E>,typeof<'O>,typeof<'Contract>).GetMethod("Discard",flags) :> MethodBase
          Serialize = fun target -> typeof<JsonRawWrite<obj,byte,JsonUtf8Writer,'Contract>>.GetGenericTypeDefinition().MakeGenericType(target,typeof<'E>,typeof<'O>,typeof<'Contract>).GetMethod("Member",flags) :> MethodBase }

    static member private CreateArrayMethod(element: Type): DynamicMethod =
        let parameters = typeof<JsonDirectWriter<obj,'E>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters() |> Array.map _.ParameterType
        parameters[0] <- element.MakeArrayType()
        let method = DynamicMethod("WriteRawArray",typeof<Void>,parameters,typeof<JsonValue>.Module,true)
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let output: CodecArrayWriterApi<Type,MethodBase> =
            { Operation = JsonRawOutput<'E,'O,'Contract>.ChildOperation
              Serialize = fun target -> typeof<JsonRawWrite<obj,byte,JsonUtf8Writer,'Contract>>.GetGenericTypeDefinition().MakeGenericType(target,typeof<'E>,typeof<'O>,typeof<'Contract>).GetMethod("Member",flags) :> MethodBase
              Null = JsonWriterRuntime.constant typeof<'E> "null" :> MethodBase
              BeginArray = JsonWriterRuntime.constant typeof<'E> "[" :> MethodBase
              Separator = JsonWriterRuntime.constant typeof<'E> "," :> MethodBase
              EndArray = JsonWriterRuntime.constant typeof<'E> "]" :> MethodBase }
        CodecArrayEmitter.rawArrayWriter (RuntimeCodecIL(method.GetILGenerator())) typeof<int> output element
        method
    static member internal ArrayMethod(element: Type): DynamicMethod =
        JsonDefaultMethods.withContract (element.MakeArrayType()) typeof<'E> typeof<'O> (JsonOperationPolicy<'Contract>.Name "array-write") (fun () -> JsonRawOutput<'E,'O,'Contract>.CreateArrayMethod(element))
    static member internal Array<'Element>(): JsonDirectWriter<'Element[],'E> =
        JsonRawOutput<'E,'O,'Contract>.ArrayMethod(typeof<'Element>).CreateDelegate(typeof<JsonDirectWriter<'Element[],'E>>) :?> JsonDirectWriter<'Element[],'E>
    static member private CreateModelBody(target: Type): MethodBase =
        let parameters = typeof<JsonDirectWriter<obj,'E>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters() |> Array.map _.ParameterType
        parameters[0] <- target
        let method = DynamicMethod("WriteRawMembers",typeof<Void>,parameters,typeof<JsonValue>.Module,true)
        CodecRawEmitter.rawBody (RuntimeCodecIL(method.GetILGenerator())) (JsonObjectGeneration.Api()) (JsonRawOutput<'E,'O,'Contract>.ForwardApi target) (JsonModelDiscovery.Model target)
        method :> MethodBase
    static member internal ModelBody(target: Type): MethodBase =
        JsonDefaultMethods.methodWithContract target typeof<'E> typeof<'O> (JsonOperationPolicy<'Contract>.Name "raw-model-body") (fun () -> JsonRawOutput<'E,'O,'Contract>.CreateModelBody target)
    static member internal Model<'T>(): JsonDirectWriter<'T,'E> =
        let body = JsonRawOutput<'E,'O,'Contract>.ModelBody typeof<'T>
        let method = DynamicMethod("WriteRawJson",typeof<Void>,(typeof<JsonDirectWriter<'T,'E>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters() |> Array.map _.ParameterType),typeof<JsonValue>.Module,true)
        let model = JsonModelDiscovery.Model typeof<'T>
        JsonModelDiscovery.PublishConstruction model
        CodecRawEmitter.rawWriter (RuntimeCodecIL(method.GetILGenerator())) (JsonObjectGeneration.Api()) (JsonRawOutput<'E,'O,'Contract>.ForwardApi typeof<'T>) model body
        method.CreateDelegate(typeof<JsonDirectWriter<'T,'E>>) :?> JsonDirectWriter<'T,'E>

    static member internal AbstractMethod(target: Type): DynamicMethod =
        JsonDefaultMethods.withContract target typeof<'E> typeof<'O> (JsonOperationPolicy<'Contract>.Name "raw-abstract-write") (fun () ->
            let parameters = typeof<JsonDirectWriter<obj,'E>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters() |> Array.map _.ParameterType
            parameters[0] <- target
            let method = DynamicMethod("WriteRawActual",typeof<Void>,parameters,typeof<JsonValue>.Module,true)
            let il = RuntimeCodecIL(method.GetILGenerator())
            let policy = JsonRawOutput<'E,'O,'Contract>.Polymorphic(fun declared -> TypeMetadataCache.KnownJsonWriterBodies(declared,typeof<'E>,typeof<'O>,JsonOperationPolicy<'Contract>.Name "raw-model-body"))
            CodecPolymorphicEmitter.abstractWriter il (JsonObjectGeneration.Api()) policy target
                (typeof<JsonRawOutput<'E,'O,'Contract>>.GetMethod("Null",BindingFlags.Static ||| BindingFlags.NonPublic))
            method)

and private JsonRawReadMetadata<'Contract> =
    static member Select<'R when 'R : struct and 'R :> IJsonReader>(metadata: TypeMetadataRecord): MetadataValue<JsonRawReadOperations> voption =
        if JsonOperationPolicy<'Contract>.IsStored then
            if typeof<'R>.Equals(typeof<Utf8Reader>) then metadata.StoredJsonUtf8Read else metadata.StoredJsonUtf16Read
        elif typeof<'R>.Equals(typeof<Utf8Reader>) then metadata.JsonUtf8Read else metadata.JsonUtf16Read
    static member private Put<'R when 'R : struct and 'R :> IJsonReader>(operation: MetadataValue<JsonRawReadOperations>, metadata: TypeMetadataRecord) =
        if JsonOperationPolicy<'Contract>.IsStored then
            if typeof<'R>.Equals(typeof<Utf8Reader>) then { metadata with StoredJsonUtf8Read = ValueSome operation }
            else { metadata with StoredJsonUtf16Read = ValueSome operation }
        elif typeof<'R>.Equals(typeof<Utf8Reader>) then { metadata with JsonUtf8Read = ValueSome operation }
        else { metadata with JsonUtf16Read = ValueSome operation }
    static member Store<'R when 'R : struct and 'R :> IJsonReader>(operation: MetadataValue<JsonRawReadOperations>, metadata: TypeMetadataRecord): TypeMetadataRecord =
        match JsonRawReadMetadata<'Contract>.Select<'R> metadata with
        | ValueSome previous when isNull previous.Value.Intrinsic && not(isNull operation.Value.Intrinsic) ->
            let completed = { previous with Value = { previous.Value with Intrinsic = operation.Value.Intrinsic } }
            JsonRawReadMetadata<'Contract>.Put<'R>(completed,metadata)
        | ValueSome _ -> metadata
        | ValueNone ->
            JsonRawReadMetadata<'Contract>.Put<'R>(operation,metadata)
    static member Runtime<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, target: Type): obj =
        let operations =
            match JsonRawReadMetadata<'Contract>.Select<'R>(TypeMetadataCache.Get target) with
            | ValueSome operations -> operations.Value
            | ValueNone ->
                TypeMetadataCache.Discover(target,JsonRawReadMetadata<'Contract>.Select<'R>,(fun operation metadata -> JsonRawReadMetadata<'Contract>.Store<'R>(operation,metadata)),
                    Func<JsonRawReadOperations>(fun () ->
                        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON read" target
                        let helper = typeof<JsonRawReadBinding<obj,Utf8Reader,'Contract>>.GetGenericTypeDefinition().MakeGenericType(target,typeof<'R>,typeof<'Contract>).GetProperty("Value",BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public).GetMethod
                        try unbox<JsonRawReadOperations>(helper.Invoke(null,[||]))
                        with :? TargetInvocationException as error ->
                            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error.InnerException).Throw()
                            Unchecked.defaultof<JsonRawReadOperations>))
        (operations.ReadRuntime :?> JsonDirectRuntimeReader<'R>).Invoke &context

and private JsonRawReadBinding<'T,'R,'Contract when 'R : struct and 'R :> IJsonReader> =
    static let operations =
        try JsonRawRead<'Contract>.Prepare<'T,'R>()
        with error ->
            // Preserve the prior first-use failure object and its named owner.
            let failure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error)
            let reader = JsonDirectReader<'T,'R>(fun _ -> failure.Throw(); Unchecked.defaultof<'T>)
            { Reader = reader; Intrinsic = null
              ReadRuntime = JsonDirectRuntimeReader<'R>(fun _ -> failure.Throw(); null) }
    static let reader = operations.Reader :?> JsonDirectReader<'T,'R>
    static member Value = operations
    static member Reader = reader

and private JsonRawNumericBinding<'T,'R,'Contract when 'R : struct and 'R :> IJsonReader> =
    static let reader =
        try JsonRawRead<'Contract>.DiscoverIntrinsic<'T,'R>()
        with error ->
            let failure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error)
            JsonDirectReader<'T,'R>(fun _ -> failure.Throw(); Unchecked.defaultof<'T>)
    static member Value = reader

and private JsonRawRead<'Contract> =
    static member internal NumericOperations<'T,'R when 'R : struct and 'R :> IJsonReader>(number: JsonDirectReader<'T,'R>): JsonRawReadOperations =
        let root =
            if typeof<'T>.Equals(typeof<obj>) then
                let numeric = number :> Delegate :?> JsonDirectReader<obj,'R>
                JsonDirectReader<obj,'R>(fun context -> JsonRawInput<'Contract>.Dynamic<'R>(numeric,&context)) :> Delegate :?> JsonDirectReader<'T,'R>
            else number
        { Reader = root; Intrinsic = number
          ReadRuntime = JsonDirectRuntimeReader<'R>(fun context -> box(JsonRawRead<'Contract>.Invoke<'T,'R>(root,&context))) }

    static member private Build<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonRawReadOperations =
        let target = typeof<'T>
        let decision = JsonCodecDiscovery.Decision target
        let operation = CodecPreparation.reader(target.FullName,decision,JsonOperationPolicy<'Contract>.IsStored)
        let mutable numeric: Delegate = null
        let reader: JsonDirectReader<'T,'R> =
            match operation with
            | RawRootOperation.StoredNumeric ->
                let struct(_,name) = (TypeSupport.StoredNumeric target.FullName).Value
                let method = typeof<JsonStoredNumeric>.GetMethod(name,BindingFlags.Static ||| BindingFlags.NonPublic).MakeGenericMethod(typeof<'R>)
                method.CreateDelegate(typeof<JsonDirectReader<'T,'R>>) :?> JsonDirectReader<'T,'R>
            | RawRootOperation.ConvertedNumeric -> JsonNumericRuntime.storedReader<'T,'R>()
            | RawRootOperation.Scalar -> JsonRawScalarReader.Create<'T,'R>().Value
            | RawRootOperation.Tree ->
                JsonDirectReader<JsonValue,'R>(fun (context: byref<JsonReadContext<'R>>) -> JsonRawInput<'Contract>.Tree<'R> &context) :> Delegate :?> JsonDirectReader<'T,'R>
            | RawRootOperation.Dynamic ->
                let number = JsonRawRead<'Contract>.AcquireIntrinsic<obj,'R>()
                numeric <- number
                JsonDirectReader<obj,'R>(fun context -> JsonRawInput<'Contract>.Dynamic<'R>(number,&context)) :> Delegate :?> JsonDirectReader<'T,'R>
            | _ ->
                if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON read" target
                if operation = RawRootOperation.Tuple then JsonRawInput<'Contract>.Tuple<'T,'R>()
                else
                    match JsonRawCollections<'Contract>.Reader<'T,'R>() with
                    | ValueSome reader -> reader
                    | ValueNone -> JsonRawInput<'Contract>.Model<'T,'R>()
        let intrinsic =
            if operation = RawRootOperation.Dynamic then numeric
            elif (TypeSupport.NumericCore target.FullName).IsSome then reader :> Delegate
            else null
        { Reader = reader; ReadRuntime = JsonDirectRuntimeReader<'R>(fun (context: byref<JsonReadContext<'R>>) -> box(JsonRawRead<'Contract>.Invoke<'T,'R>(reader,&context))); Intrinsic = intrinsic }

    static member internal Invoke<'T,'R when 'R : struct and 'R :> IJsonReader>(reader: JsonDirectReader<'T,'R>, context: byref<JsonReadContext<'R>>): 'T =
        try
            let value = reader.Invoke &context
            if not context.Consumed then
                if not(isNull context.Properties) || not(isNull context.Elements) then context.Cursor.codeUnitIndex <- context.Range.Start + context.Range.Length
                else JsonReadCursor.skip &context
            value
        with
        | JsonPrimitiveShapeMismatch target -> JsonImpl.DeserializeByType target (JsonRawInput<'Contract>.Tree<'R> &context) |> unbox<'T>
        | JsonScalarShapeMismatch ->
            // Invalid scalar shapes retain the established diagnostic, including its formatted value.
            // Successful typed conversions never construct a tree.
            JsonReadOperations<'T>.Deserialize(JsonRawInput<'Contract>.Tree<'R> &context)

    static member internal Prepare<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonRawReadOperations =
        TypeMetadataCache.Discover(typeof<'T>,JsonRawReadMetadata<'Contract>.Select<'R>,(fun operation metadata -> JsonRawReadMetadata<'Contract>.Store<'R>(operation,metadata)),
            Func<JsonRawReadOperations>(fun () -> JsonRawRead<'Contract>.Build<'T,'R>()))
    static member internal PrimitiveRetry<'T,'R when 'R : struct and 'R :> IJsonReader>(error: exn, context: byref<JsonReadContext<'R>>): 'T =
        match error with
        | JsonPrimitiveShapeMismatch target -> JsonImpl.DeserializeByType target (JsonRawInput<'Contract>.Tree<'R> &context) |> unbox<'T>
        | _ -> raise error
    static member internal ScalarRetry<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): 'T =
        JsonReadOperations<'T>.Deserialize(JsonRawInput<'Contract>.Tree<'R> &context)
    static member internal Ensure<'T,'R when 'R : struct and 'R :> IJsonReader>(): unit =
        JsonRawReadBinding<'T,'R,'Contract>.Value |> ignore
    static member internal Read<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): 'T =
        JsonRawRead<'Contract>.Invoke<'T,'R>(JsonRawReadBinding<'T,'R,'Contract>.Reader,&context)

    static member private AcquireIntrinsic<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'T,'R> =
        JsonRawNumericBinding<'T,'R,JsonNativeContract>.Value

    static member internal DiscoverIntrinsic<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'T,'R> =
        let select metadata =
            match JsonRawReadMetadata<'Contract>.Select<'R> metadata with
            | ValueSome operations when not(isNull operations.Value.Intrinsic) ->
                ValueSome { Generated = operations.Generated; Value = operations.Value.Intrinsic }
            | _ -> ValueNone
        let store (intrinsic: MetadataValue<Delegate>) metadata =
            let operations =
                match JsonRawReadMetadata<'Contract>.Select<'R> metadata with
                | ValueSome current -> { current with Value = { current.Value with Intrinsic = intrinsic.Value } }
                | ValueNone ->
                    let reader = intrinsic.Value :?> JsonDirectReader<'T,'R>
                    { Generated = intrinsic.Generated; Value = JsonRawRead<'Contract>.NumericOperations<'T,'R>(reader) }
            JsonRawReadMetadata<'Contract>.Store<'R>(operations,metadata)
        let reader = TypeMetadataCache.Discover(typeof<'T>,select,store,
                         Func<Delegate>(fun () ->
                             if TypeSupport.NumericInfrastructure typeof<'T>.FullName then JsonNumericRuntime.reader<'T,'R>() :> Delegate
                             elif JsonOperationPolicy<'Contract>.IsStored && (TypeSupport.StoredNumericReader typeof<'T>.FullName).IsSome then
                                 JsonNumericRuntime.storedReader<'T,'R>() :> Delegate
                             else JsonRawScalarReader.Create<'T,'R>().Value :> Delegate))
        reader :?> JsonDirectReader<'T,'R>

    static member internal Intrinsic<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): 'T =
        JsonRawRead<'Contract>.Invoke<'T,'R>(JsonRawRead<'Contract>.AcquireIntrinsic<'T,'R>(),&context)
    static member internal PhysicalId<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int64 =
        // Invalid ID shapes retain reference decoding ownership.
        try JsonRawRead<'Contract>.AcquireIntrinsic<int64,'R>().Invoke &context
        with JsonPrimitiveShapeMismatch _ -> raise JsonScalarShapeMismatch
    static member internal ReadMember<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): 'T =
        let selected = context.TypeSelected
        context.TypeSelected <- false
        try JsonRawRead<'Contract>.Read<'T,'R> &context
        finally context.TypeSelected <- selected
    static member internal Child<'T,'R when 'R : struct and 'R :> IJsonReader>(context: JsonReadContext<'R>): 'T =
        let mutable child = context
        JsonRawRead<'Contract>.Read<'T,'R> &child

and private JsonRawInput<'Contract> =
    static member private ContextType<'R when 'R : struct and 'R :> IJsonReader>(): Type =
        let parameters = typeof<JsonDirectRuntimeReader<'R>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters()
        parameters[0].ParameterType.GetElementType()

    static member internal Start<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int = context.Range.Start
    static member internal BeginObject<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): bool =
        match JsonReadCursor.token &context with
        | NullToken -> false
        | OpenBrace -> true
        | _ -> raise JsonScalarShapeMismatch
    static member internal BeginArray<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): bool =
        match JsonReadCursor.token &context with
        | NullToken -> false
        | OpenBracket -> true
        | _ -> raise JsonScalarShapeMismatch
    static member internal NextElement<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, first: bool): bool =
        if JsonGrammar.nextElement &context.Cursor first then
            context.Range <- { Start = context.Cursor.codeUnitIndex; Length = context.Cursor.codeUnitLength - context.Cursor.codeUnitIndex }
            context.Properties <- null; context.Elements <- null; context.Consumed <- false
            true
        else false
    static member internal ArrayWrapper<'Element,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, value: byref<'Element[]>): bool =
        match JsonReadCursor.token &context with
        | OpenBrace ->
            value <- JsonRawCollections<'Contract>.SequenceReader<'Element[],'Element,'R>("Array",TypeSupport.SupportsValuesWrapper typeof<'Element>.FullName).Invoke &context
            true
        | _ -> false
    static member internal GrowArray<'Element>(values: 'Element[]): 'Element[] =
        if isNull values then Array.zeroCreate<'Element> 8
        else
            let mutable result = values
            Array.Resize(&result,Checked.(*) values.Length 2)
            result
    static member internal TrimArray<'Element>(values: 'Element[], count: int): 'Element[] =
        if count = 0 then Array.empty<'Element>
        elif count = values.Length then values
        else let result = Array.zeroCreate<'Element> count in Array.Copy(values,result,count); result
    static member internal Skip<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit = JsonReadCursor.skip &context
    static member internal Complete<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, origin: int): unit =
        context.Range <- { Start = origin; Length = context.Cursor.codeUnitIndex - origin }
        context.Consumed <- true
    static member internal SelectType<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): unit =
        if not context.TypeSelected then
            let value = JsonRawRead<'Contract>.Read<string,'R> &context
            let actual = if isNull value then null else TypeMetadata.nameToType value
            if not(isNull actual) && not (typeof<'T>.Equals(actual)) && typeof<'T>.IsAssignableFrom actual then raise JsonDifferentRuntimeType
    static member internal Wrap<'T,'R when 'R : struct and 'R :> IJsonReader>(read: JsonDirectReader<'T,'R>): JsonDirectReader<'T,'R> =
        JsonDirectReader<'T,'R>(fun (context: byref<JsonReadContext<'R>>) ->
            if TypeMetadata.isSealed typeof<'T> || typeof<'T>.IsValueType || context.TypeSelected then read.Invoke &context
            else
                let original = context
                try read.Invoke &context
                with error ->
                    let mutable selected = original
                    let actual =
                        try JsonRawInput<'Contract>.ActualType<'T,'R> &selected
                        with _ -> typeof<'T>
                    if not (typeof<'T>.Equals(actual)) then
                        selected.TypeSelected <- true
                        let result = JsonRawInput<'Contract>.ToRuntimeObject<'R>(&selected,actual) |> unbox<'T>
                        context <- selected
                        result
                    else
                        match error with
                        | JsonDifferentRuntimeType ->
                            selected <- original
                            selected.TypeSelected <- true
                            let result = read.Invoke &selected
                            context <- selected
                            result
                        | _ -> System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw(); Unchecked.defaultof<'T>)

    static member internal Tree<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): JsonValue =
        context.Cursor.codeUnitIndex <- context.Range.Start
        context.Cursor.decodeStrings <- true
        let value = JsonParser.parse JsonValue.ParserApi &context.Cursor
        context.Consumed <- true
        value
    static member internal IsNull<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): bool =
        match JsonReadCursor.token &context with NullToken -> true | _ -> false
    static member internal SequenceNull<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): bool =
        context.Cursor.codeUnitIndex <- context.Range.Start
        if JsonLexer.tryReadNull &context.Cursor then
            context.Consumed <- true
            true
        else false
    static member internal SequenceCount<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): int = context.Elements.Count
    static member internal SequenceElement<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, index: int): JsonReadContext<'R> =
        JsonReadCursor.child &context context.Elements[index]
    static member internal ObjectProperties<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): JsonReadContext<'R> =
        if isNull context.Properties then JsonReadCursor.scan &context
        if isNull context.Properties then JsonImpl.AsDictOrFail(JsonRawInput<'Contract>.Tree<'R> &context) |> ignore
        context
    static member internal TryGetValue<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, name: string, child: byref<JsonReadContext<'R>>): bool =
        JsonReadCursor.tryProperty &context name &child
    static member internal Contains<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, name: string): bool =
        let mutable child = JsonReadContext(context.Cursor,context.Range)
        if isNull context.Properties then JsonReadCursor.scan &context
        not(isNull context.Properties) && JsonReadCursor.tryProperty &context name &child
    static member internal GetProperty<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, name: string): JsonReadContext<'R> =
        let mutable child = JsonReadContext(context.Cursor,context.Range)
        if isNull context.Properties then JsonReadCursor.scan &context
        if isNull context.Properties || not(JsonReadCursor.tryProperty &context name &child) then failwith "Property not found"
        child
    static member internal ActualType<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>): Type =
        let target = typeof<'T>
        let mutable child = JsonReadContext(context.Cursor,context.Range)
        let actual =
            if JsonRawInput<'Contract>.Contains<'R>(&context,"$type") then
                child <- JsonRawInput<'Contract>.GetProperty<'R>(&context,"$type")
                JsonRawRead<'Contract>.Read<string,'R> &child |> TypeMetadata.nameToType
            else null
        if TypeMetadata.isSealed target || isNull actual || not(target.IsAssignableFrom actual) then target else actual
    static member internal ToRuntimeObject<'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, target: Type): obj =
        JsonRawReadMetadata<'Contract>.Runtime<'R>(&context,target)

    static member internal Dynamic<'R when 'R : struct and 'R :> IJsonReader>(number: JsonDirectReader<obj,'R>, context: byref<JsonReadContext<'R>>): obj =
        if JsonReadCursor.numericStart &context then JsonRawInput<'Contract>.DynamicNumber<'R>(number,&context)
        else
        match JsonReadCursor.token &context with
        | NullToken -> null
        | BooleanToken value -> box value
        | StringToken value ->
            match Int64.TryParse(value,NumberStyles.Integer,CultureInfo.InvariantCulture) with
            | true, number -> box number
            | _ ->
                match Decimal.TryParse(value,NumberStyles.Float,CultureInfo.InvariantCulture) with
                | true, number -> box number
                | _ -> box value
        | NumberToken _ -> JsonRawInput<'Contract>.DynamicNumber<'R>(number,&context)
        | OpenBracket ->
            if isNull context.Elements then JsonReadCursor.scan &context
            let result = ResizeArray<obj>(context.Elements.Count)
            for range in context.Elements do
                let mutable child = JsonReadCursor.child &context range
                result.Add(JsonRawRead<'Contract>.Read<obj,'R> &child)
            box result
        | OpenBrace ->
            if isNull context.Properties then JsonReadCursor.scan &context
            let result = ExpandoObject() :> IDictionary<string,obj>
            for KeyValue(name,range) in context.Properties do
                let mutable child = JsonReadCursor.child &context range
                result.Add(name,JsonRawRead<'Contract>.Read<obj,'R> &child)
            box result
        | _ -> invalidOp "The validated JSON root has no value."

    static member private DynamicNumber<'R when 'R : struct and 'R :> IJsonReader>(reader: JsonDirectReader<obj,'R>, context: byref<JsonReadContext<'R>>): obj =
        JsonRawRead<'Contract>.Invoke<obj,'R>(reader,&context)

    static member private CreateTupleMethod<'R when 'R : struct and 'R :> IJsonReader>(target: Type): DynamicMethod =
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let input = JsonRawInput<'Contract>.ForwardApi<'R>(false)
        let context = JsonRawInput<'Contract>.ContextType<'R>()
        let helper name = typeof<JsonTupleInput>.GetMethod(name,flags).MakeGenericMethod(typeof<'R>) :> MethodBase
        let retry name child = typeof<JsonRawRead<'Contract>>.GetMethod(name,flags).MakeGenericMethod(child,typeof<'R>) :> MethodBase
        let cursor = typeof<JsonValue>.Assembly.GetType("SoloDatabase.JsonSerializator.JsonReadCursor")
        let api: CodecTupleReaderApi<Type,MethodBase> =
            { Context = context; Range = typeof<JsonSourceRange>; Boolean = typeof<bool>; Int32 = typeof<int>; Names = JsonNameRuntime.api typeof<'R>
              Begin = helper "Begin"; NextArray = helper "NextArray"
              Capture = helper "Capture"; Complete = helper "Complete"
              Child = cursor.GetMethod("child",flags).MakeGenericMethod(typeof<'R>) :> MethodBase
              Missing = typeof<JsonTupleInput>.GetMethod("Missing",flags) :> MethodBase
              PrimitiveError = typeof<JsonPrimitiveShapeMismatch>; ScalarError = typeof<JsonScalarShapeMismatch>
              PrimitiveRetry = retry "PrimitiveRetry"; ScalarRetry = retry "ScalarRetry" }
        let method = DynamicMethod("ReadRawTuple",target,[|context.MakeByRefType()|],typeof<JsonValue>.Module,true)
        CodecTupleEmitter.rawReader (RuntimeCodecIL(method.GetILGenerator())) api input.Operation input.Read (RuntimeTupleFacts.model target)
        method

    static member internal Tuple<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'T,'R> =
        JsonDefaultMethods.callable<JsonDirectReader<'T,'R>> typeof<'T> typeof<'R> typeof<'R> (JsonOperationPolicy<'Contract>.Name "tuple-read")
            (fun () -> JsonRawInput<'Contract>.CreateTupleMethod<'R>(typeof<'T>) :> MethodBase)

    static member internal Api<'R when 'R : struct and 'R :> IJsonReader>(): CodecApi<Type,MethodBase> =
        let baseApi = JsonObjectGeneration.Api()
        let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
        let helper (name: string) (arguments: Type[]): MethodBase =
            let method = typeof<JsonRawInput<'Contract>>.GetMethod(name,flags)
            method.MakeGenericMethod(Array.append arguments [|typeof<'R>|]) :> MethodBase
        { baseApi with
            Json = JsonRawInput<'Contract>.ContextType<'R>(); ObjectDictionary = JsonRawInput<'Contract>.ContextType<'R>()
            IsNull = helper "IsNull" [||]
            ObjectProperties = helper "ObjectProperties" [||]
            TryGetValue = helper "TryGetValue" [||]
            ActualType = fun target -> helper "ActualType" [|target|]
            ToRuntimeObject = helper "ToRuntimeObject" [||]
            Deserialize = fun target -> typeof<JsonRawRead<'Contract>>.GetMethod("Child",flags).MakeGenericMethod(target,typeof<'R>) :> MethodBase
            Contains = helper "Contains" [||]; GetProperty = helper "GetProperty" [||]
            RawReader = true }

    static member internal ChildOperation<'R when 'R : struct and 'R :> IJsonReader>(target: Type): MethodBase option =
        let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
        let definition = if target.IsGenericType then target.GetGenericTypeDefinition().FullName else target.FullName
        let rank = if target.IsArray then target.GetArrayRank() else 0
        match TypeSupport.RawChildReader(definition,target.IsEnum,rank,JsonOperationPolicy<'Contract>.IsStored) with
        | RawChildOperation.Member -> None
        | RawChildOperation.StoredNumeric ->
            let struct(_,name) = (TypeSupport.StoredNumeric target.FullName).Value
            Some(typeof<JsonStoredNumeric>.GetMethod(name,flags).MakeGenericMethod(typeof<'R>) :> MethodBase)
        | RawChildOperation.Adapted _ -> Some(JsonRawCollections<'Contract>.ReadMethod<'R>(target) :> MethodBase)
        | RawChildOperation.Sequence policy ->
            JsonRawCollections<'Contract>.Invoke(typeof<JsonRawCollections<'Contract>>,"SequenceReadMethod",[|target;target.GetGenericArguments().[0];typeof<'R>|],
                [|box policy;box false|]) :?> MethodBase |> Some
        | RawChildOperation.Numeric ->
            Some((if JsonOperationPolicy<'Contract>.IsStored && (TypeSupport.StoredNumericReader target.FullName).IsSome then
                      JsonNumericRuntime.storedMethod target typeof<'R>
                  else JsonNumericRuntime.method target typeof<'R>) :> MethodBase)
        | RawChildOperation.Formatted -> Some(JsonFormattedRuntime.method target typeof<'R> :> MethodBase)
        | RawChildOperation.Primitive name -> Some(typeof<JsonRawPrimitive>.GetMethod("Read" + name,flags).MakeGenericMethod(typeof<'R>) :> MethodBase)
        | RawChildOperation.Array -> Some(JsonRawInput<'Contract>.ArrayMethod<'R>(target.GetElementType()) :> MethodBase)
        | RawChildOperation.Text | RawChildOperation.Boolean | RawChildOperation.Guid | RawChildOperation.Bytes ->
            invalidOp "An output operation cannot be selected for input."

    static member internal ForwardApi<'R when 'R : struct and 'R :> IJsonReader>(properties: bool): CodecRawReaderApi<Type,MethodBase> =
        let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
        let helper name = typeof<JsonRawInput<'Contract>>.GetMethod(name,flags).MakeGenericMethod(typeof<'R>) :> MethodBase
        {
            Boolean = typeof<bool>
            Operation = JsonRawInput<'Contract>.ChildOperation<'R>
            BeginArray = helper "BeginArray"; NextElement = helper "NextElement"
            GrowArray = fun target -> typeof<JsonRawInput<'Contract>>.GetMethod("GrowArray",flags).MakeGenericMethod(target) :> MethodBase
            ArrayWrapper = fun target -> typeof<JsonRawInput<'Contract>>.GetMethod("ArrayWrapper",flags).MakeGenericMethod(target,typeof<'R>) :> MethodBase
            TrimArray = fun target -> typeof<JsonRawInput<'Contract>>.GetMethod("TrimArray",flags).MakeGenericMethod(target) :> MethodBase
            Start = helper "Start"; BeginObject = helper "BeginObject"
            Properties = if properties then Some(JsonNameRuntime.api typeof<'R>) else None
            Skip = helper "Skip"; Complete = helper "Complete"
            CheckType = fun target -> typeof<JsonRawInput<'Contract>>.GetMethod("SelectType",flags).MakeGenericMethod(target,typeof<'R>) :> MethodBase
            Read = fun target -> typeof<JsonRawRead<'Contract>>.GetMethod("ReadMember",flags).MakeGenericMethod(target,typeof<'R>) :> MethodBase }

    static member private CreateArrayMethod<'R when 'R : struct and 'R :> IJsonReader>(element: Type): DynamicMethod =
        let method = DynamicMethod("ReadRawArray",element.MakeArrayType(),[|JsonRawInput<'Contract>.ContextType<'R>().MakeByRefType()|],typeof<JsonValue>.Module,true)
        CodecArrayEmitter.rawArrayReader (RuntimeCodecIL(method.GetILGenerator())) (JsonRawInput<'Contract>.Api<'R>()) (JsonRawInput<'Contract>.ForwardApi<'R>(false)) (element.MakeArrayType()) element
        method
    static member internal ArrayMethod<'R when 'R : struct and 'R :> IJsonReader>(element: Type): DynamicMethod =
        JsonDefaultMethods.get (element.MakeArrayType()) typeof<'R> (JsonOperationPolicy<'Contract>.Name "array-read") (fun () -> JsonRawInput<'Contract>.CreateArrayMethod<'R>(element))

    static member internal Array<'Element,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'Element[],'R> =
        let read = JsonRawInput<'Contract>.ArrayMethod<'R>(typeof<'Element>).CreateDelegate(typeof<JsonDirectReader<'Element[],'R>>) :?> JsonDirectReader<'Element[],'R>
        read
    static member internal Model<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'T,'R> =
        let method = DynamicMethod("ReadRawJson",typeof<'T>,[|JsonRawInput<'Contract>.ContextType<'R>().MakeByRefType()|],typeof<JsonValue>.Module,true)
        let model = JsonModelDiscovery.Model typeof<'T>
        JsonModelDiscovery.PublishConstruction model
        let il = RuntimeCodecIL(method.GetILGenerator())
        let api = JsonRawInput<'Contract>.Api<'R>()
        let forward = JsonRawInput<'Contract>.ForwardApi<'R>(CodecRawEmitter.forwardReader model)
        let union () =
            let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
            let helper (owner: Type) name = owner.GetMethod(name,flags).MakeGenericMethod(typeof<'R>) :> MethodBase
            let input: CodecUnionReaderApi<Type,MethodBase> = {
                Context = JsonRawInput<'Contract>.ContextType<'R>(); Range = typeof<JsonSourceRange>
                Boolean = typeof<bool>; Names = JsonNameRuntime.api typeof<'R>
                Begin = helper typeof<JsonUnionInput> "Begin"
                Capture = helper typeof<JsonUnionInput> "Capture"
                Complete = helper typeof<JsonTupleInput> "Complete"
                Child = helper (typeof<JsonValue>.Assembly.GetType("SoloDatabase.JsonSerializator.JsonReadCursor")) "child"
                Missing = typeof<JsonTupleInput>.GetMethod("Missing",flags) :> MethodBase
                PrimitiveError = typeof<JsonPrimitiveShapeMismatch>; ScalarError = typeof<JsonScalarShapeMismatch>
                PrimitiveRetry = fun target -> typeof<JsonRawRead<'Contract>>.GetMethod("PrimitiveRetry",flags).MakeGenericMethod(target,typeof<'R>) :> MethodBase
                ScalarRetry = fun target -> typeof<JsonRawRead<'Contract>>.GetMethod("ScalarRetry",flags).MakeGenericMethod(target,typeof<'R>) :> MethodBase }
            CodecUnionEmitter.reader il api input forward.Operation forward.Read model
        CodecRawEmitter.rawReader il api forward model union
        JsonRawInput<'Contract>.Wrap<'T,'R>(method.CreateDelegate(typeof<JsonDirectReader<'T,'R>>) :?> JsonDirectReader<'T,'R>)

and private JsonRawCollections<'Contract> =
    static member internal Invoke(helper: Type, name: string, arguments: Type[], values: obj[]): obj =
        let factory = helper.GetMethod(name,BindingFlags.Static ||| BindingFlags.NonPublic).MakeGenericMethod arguments
        try factory.Invoke(null,values)
        with :? TargetInvocationException as error when not(isNull error.InnerException) ->
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error.InnerException).Throw()
            null

    static member internal ReadMethod<'R when 'R : struct and 'R :> IJsonReader>(target: Type): DynamicMethod =
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let forward = JsonRawInput<'Contract>.ForwardApi<'R>(false)
        let operation target =
            match forward.Operation target with
            | Some call -> call
            | None ->
                forward.Read target
        let retry target primitive = typeof<JsonRawRead<'Contract>>.GetMethod((if primitive then "PrimitiveRetry" else "ScalarRetry"),flags).MakeGenericMethod(target,typeof<'R>) :> MethodBase
        JsonOptionalRuntime.reader target typeof<'R> (JsonOperationPolicy<'Contract>.Name "optional-read") operation retry forward.Skip

    static member internal WriteMethod<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(target: Type): DynamicMethod =
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let forward = JsonRawOutput<'E,'O,'Contract>.ForwardApi target
        let operation target =
            match forward.Operation target with
            | Some call -> call
            | None ->
                forward.Serialize target
        let parameters = typeof<JsonDirectWriter<obj,'E>>.GetMethod("Invoke",BindingFlags.Instance ||| flags).GetParameters() |> Array.map _.ParameterType
        JsonOptionalRuntime.writer target typeof<'E> typeof<'O> (JsonOperationPolicy<'Contract>.Name "optional-write") parameters operation forward.Null

    static member private ConstructorMethod<'T>(constructor: ConstructorInfo): DynamicMethod =
        JsonDefaultMethods.get typeof<'T> typeof<'T> "collection-construct" (fun () ->
            let method = DynamicMethod("CreateJsonCollection",typeof<'T>,Type.EmptyTypes,typeof<JsonValue>.Module,true)
            let il = method.GetILGenerator()
            if isNull constructor then
                CodecSequenceEmitter.missingConstructor (RuntimeCodecIL(il)) (typeof<Exception>.GetConstructor([|typeof<string>|]) :> MethodBase) typeof<'T>.Name
            else
                il.Emit(OpCodes.Newobj,constructor)
                il.Emit(OpCodes.Ret)
            method)

    static member internal StructuredReader<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'T,'R> voption =
        let target = typeof<'T>
        let decision = JsonCodecDiscovery.Decision target
        let arguments name =
            let definition = target.GetInterface name |> Option.ofObj |> Option.defaultValue target
            TypeMetadata.GenericTypeArgCache.Get definition
        let invoke name types values = ValueSome(JsonRawCollections<'Contract>.Invoke(typeof<JsonRawCollections<'Contract>>,name,types,values) :?> JsonDirectReader<'T,'R>)
        if decision.Shape = TypeShape.BsonDocument then
            let method = DynamicMethod("ReadBsonJson",target,[|typeof<JsonValue>|],typeof<JsonValue>.Module,true)
            CodecEmitter.typedConstruction (RuntimeCodecIL(method.GetILGenerator())) (target.GetConstructor([|typeof<JsonValue>|]) :> MethodBase) 1
            ValueSome(JsonRawCollections<'Contract>.TreeReader<'T,'R>(method.CreateDelegate(typeof<Func<JsonValue,'T>>) :?> Func<JsonValue,'T>))
        else
            let struct(name,policy) = TypeSupport.RawCollectionReader(decision.Shape,target.IsAbstract)
            if name = "" then ValueNone
            else
                let types = if decision.Contract = "" then [||] else arguments (decision.Contract.Split('.') |> Array.last)
                let signature = Array.concat [ [|target|]; types; [|typeof<'R>|] ]
                if policy <> "" then invoke name signature [|box policy|]
                elif name = "CustomSequenceReader" then
                    let populate = TypeSupport.PopulateCollection(decision.Shape,(if target.IsGenericType then (TypeMetadata.GenericTypeArgCache.Get target).Length else 0),typeof<IList>.IsAssignableFrom target,not(isNull(target.GetConstructor(Type.EmptyTypes))))
                    invoke name signature [|box populate;box(TypeSupport.ArrayValuesWrapper(target.IsArray,(if target.IsArray then target.GetElementType().FullName else "")))|]
                else invoke name signature [||]

    static member internal GroupingImplementationReader<'Sequence,'Key,'Element,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'Sequence,'R> =
        JsonDefaultMethods.callable<JsonDirectReader<'Sequence,'R>> typeof<'Sequence> typeof<'R> typeof<'R> (JsonOperationPolicy<'Contract>.Name "grouping-read")
            (fun () -> JsonRawCollections<'Contract>.CreateGroupingReader<'Sequence,'Key,'Element,'R>() :> MethodBase)

    static member Reader<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'T,'R> voption =
        let target = typeof<'T>
        let decision = JsonCodecDiscovery.Decision target
        match JsonCodecDiscovery.Select target with
        | Some(name,arguments,values) when decision.RawKind <> RawCodecKind.None && decision.RawKind <> RawCodecKind.Structured ->
            let types,parameters =
                match decision.RawKind with
                | RawCodecKind.Sequence -> [|target;arguments[0];typeof<'R>|],[|box name;box(values.Length > 0 && unbox<bool> values[0])|]
                | RawCodecKind.Dictionary -> [|target;arguments[0];arguments[1];typeof<'R>|],[|box name|]
                | RawCodecKind.Intrinsic -> Array.append arguments [|typeof<'R>|],[||]
                | value -> invalidOp (string value)
            if decision.Shape = TypeShape.Array then ValueSome(JsonRawCollections<'Contract>.Invoke(typeof<JsonRawInput<'Contract>>,"Array",[|arguments[0];typeof<'R>|],[||]) :?> JsonDirectReader<'T,'R>)
            else ValueSome(JsonRawCollections<'Contract>.Invoke(typeof<JsonRawCollections<'Contract>>,decision.RawReader,types,parameters) :?> JsonDirectReader<'T,'R>)
        | _ -> JsonRawCollections<'Contract>.StructuredReader<'T,'R>()

    static member Writer<'T,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<'T,'E> voption =
        let target = typeof<'T>
        let decision = JsonCodecDiscovery.Decision target
        match JsonCodecDiscovery.Select target with
        | Some(_,arguments,_) when decision.RawKind = RawCodecKind.Intrinsic && decision.RawWriter <> "SequenceWriter" ->
            ValueSome(JsonRawCollections<'Contract>.Invoke(typeof<JsonRawCollections<'Contract>>,decision.RawWriter,Array.append arguments [|typeof<'E>;typeof<'O>|],[||]) :?> JsonDirectWriter<'T,'E>)
        | _ when decision.Shape = TypeShape.Array ->
            let helper = typeof<JsonRawOutput<byte,JsonUtf8Writer,'Contract>>.GetGenericTypeDefinition().MakeGenericType(typeof<'E>,typeof<'O>,typeof<'Contract>)
            ValueSome(JsonRawCollections<'Contract>.Invoke(helper,"Array",[|target.GetElementType()|],[||]) :?> JsonDirectWriter<'T,'E>)
        | _ when typeof<JsonValue>.IsAssignableFrom target -> ValueSome(JsonRawCollections<'Contract>.TreeWriter<'T,'E,'O>(Func<'T,JsonValue>(fun value -> box value :?> JsonValue)))
        | _ when decision.Shape = TypeShape.BsonDocument ->
            let method = DynamicMethod("WriteBsonJson",typeof<JsonValue>,[|target|],typeof<JsonValue>.Module,true)
            CodecEmitter.directCall (RuntimeCodecIL(method.GetILGenerator())) (target.GetProperty("Json").GetGetMethod() :> MethodBase) 1
            ValueSome(JsonRawCollections<'Contract>.TreeWriter<'T,'E,'O>(method.CreateDelegate(typeof<Func<'T,JsonValue>>) :?> Func<'T,JsonValue>))
        | _ ->
            let name =
                if decision.RawKind = RawCodecKind.Dictionary then "DictionaryWriter"
                elif decision.RawWriter = "SequenceWriter" then "SequenceWriter"
                else TypeSupport.RawCollectionWriter(decision.Shape,target.IsAbstract,typeof<IEnumerable>.IsAssignableFrom target && not (typeof<string>.Equals(target)))
            if name = "" then ValueNone
            else
                let contract =
                    if decision.Contract <> "" then decision.Contract.Split('.') |> Array.last
                    elif name = "DictionaryWriter" then "IDictionary`2"
                    elif name = "SequenceWriter" then "IEnumerable`1"
                    else ""
                let arguments =
                    if contract = "" then [||]
                    else
                        let definition = target.GetInterface contract |> Option.ofObj |> Option.defaultValue target
                        TypeMetadata.GenericTypeArgCache.Get definition
                let types = Array.concat [ [|target|]; arguments; [|typeof<'E>;typeof<'O>|] ]
                let values = if name = "SequenceWriter" then [|box(typeof<IEnumerable<byte>>.IsAssignableFrom target)|] else [||]
                ValueSome(JsonRawCollections<'Contract>.Invoke(typeof<JsonRawCollections<'Contract>>,name,types,values) :?> JsonDirectWriter<'T,'E>)

    static member private PhysicalObject<'Reference,'Target,'R when 'R : struct and 'R :> IJsonReader>
        (context: byref<JsonReadContext<'R>>, marker: bool): 'Reference =
        if marker then
            failwithf "Cannot deserialize DBRef<%s> from $dbrefTypedId marker: only DBRef<TTarget,TId> supports typed-id wire format." typeof<'Target>.Name
        else JsonValue.InvalidValue(sprintf "Cannot deserialize DBRef<%s> from %A" typeof<'Target>.Name (JsonRawInput<'Contract>.Tree<'R> &context))

    static member internal PhysicalReader<'Reference,'Target,'R when 'R : struct and 'R :> IJsonReader>
        (unloaded: Func<int64,'Reference>, loaded: Func<int64,'Target,'Reference>): JsonDirectReader<'Reference,'R> =
        let physicalIdWriter = lazy(EntityIdentity.tryInt64IdWriter typeof<'Target>)
        JsonDirectReader<'Reference,'R>(fun context ->
            if JsonReadCursor.numericStart &context then unloaded.Invoke(JsonRawRead<'Contract>.PhysicalId<'R> &context)
            else
            match JsonReadCursor.token &context with
            | NullToken -> Unchecked.defaultof<'Reference>
            | NumberToken _ -> unloaded.Invoke(JsonRawRead<'Contract>.PhysicalId<'R> &context)
            | OpenBracket ->
                if isNull context.Elements then JsonReadCursor.scan &context
                if context.Elements.Count <> 2 then JsonValue.InvalidValue(sprintf "Cannot deserialize DBRef<%s> from %A" typeof<'Target>.Name (JsonRawInput<'Contract>.Tree<'R> &context))
                let mutable first = JsonReadCursor.element &context 0
                let identity = JsonValue.ReadRawPhysicalId<'R> &first
                let mutable second = JsonReadCursor.element &context 1
                let value = box(JsonRawRead<'Contract>.Read<'Target,'R> &second)
                if not(isNull value) then
                    match physicalIdWriter.Value with
                    | ValueSome writer -> writer.Invoke(value,identity)
                    | ValueNone -> ()
                loaded.Invoke(identity,unbox<'Target> value)
            | OpenBrace ->
                let marker = JsonMarkerCapture<'R>.Read &context
                JsonRawCollections<'Contract>.PhysicalObject<'Reference,'Target,'R>(&context,marker.Present)
            | _ -> JsonValue.InvalidValue(sprintf "Cannot deserialize DBRef<%s> from %A" typeof<'Target>.Name (JsonRawInput<'Contract>.Tree<'R> &context)))

    static member internal AbstractWriter<'T,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<'T,'E> =
        if MetadataRuntime.supportsDynamicCode() && not(typeof<'T>.Equals(typeof<obj>)) then
            JsonRawOutput<'E,'O,'Contract>.AbstractMethod(typeof<'T>).CreateDelegate(typeof<JsonDirectWriter<'T,'E>>) :?> JsonDirectWriter<'T,'E>
        else JsonDirectWriter<'T,'E>(fun value destination state ->
            if isNull(box value) then JsonRawOutput<'E,'O,'Contract>.Null(&destination,&state)
            else JsonRawOutput<'E,'O,'Contract>.SerializeActual(box value,&destination,&state))

    static member internal FinishGrouping<'Key,'Element>(value: Grouping<'Key,'Element>): Grouping<'Key,'Element> =
        JsonRawCollections<'Contract>.SequenceResult<'Element,_>(value)

    static member private CreateGroupingReader<'Sequence,'Key,'Element,'R when 'R : struct and 'R :> IJsonReader>(): DynamicMethod =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct grouping read" typeof<'Sequence>
        let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
        let context = JsonRawInput<'Contract>.Api<'R>().Json
        let method = DynamicMethod("ReadRawGrouping",typeof<'Sequence>,[|context.MakeByRefType()|],typeof<JsonValue>.Module,true)
        let forward = JsonRawInput<'Contract>.ForwardApi<'R>(false)
        let key =
            match forward.Operation typeof<'Key> with
            | Some call -> call
            | None -> forward.Read typeof<'Key>
        let operands =
            { Context = context; Items = typeof<'Element[]>; Key = typeof<'Key>
              IsNull = typeof<JsonRawInput<'Contract>>.GetMethod("SequenceNull",flags).MakeGenericMethod(typeof<'R>) :> MethodBase
              Begin = typeof<JsonFiniteInput>.GetMethod("Begin",flags).MakeGenericMethod(typeof<'R>) :> MethodBase
              Capture = JsonFiniteInput.Api typeof<'R>
              Child = (typeof<JsonValue>.Assembly.GetType("SoloDatabase.JsonSerializator.JsonReadCursor")).GetMethod("child",flags).MakeGenericMethod(typeof<'R>) :> MethodBase
              Missing = typeof<JsonTupleInput>.GetMethod("Missing",flags) :> MethodBase
              ReadItems = JsonRawCollections<'Contract>.SequenceReadMethod<'Element[],'Element,'R>("Array",false)
              ReadKey = key; Construct = typeof<Grouping<'Key,'Element>>.GetConstructor([|typeof<'Key>;typeof<'Element[]>|]) :> MethodBase
              Finish = typeof<JsonRawCollections<'Contract>>.GetMethod("FinishGrouping",flags).MakeGenericMethod(typeof<'Key>,typeof<'Element>) :> MethodBase
              PrimitiveError = typeof<JsonPrimitiveShapeMismatch>; ScalarError = typeof<JsonScalarShapeMismatch>
              PrimitiveRetry = typeof<JsonRawRead<'Contract>>.GetMethod("PrimitiveRetry",flags).MakeGenericMethod(typeof<'Key>,typeof<'R>) :> MethodBase
              ScalarRetry = typeof<JsonRawRead<'Contract>>.GetMethod("ScalarRetry",flags).MakeGenericMethod(typeof<'Key>,typeof<'R>) :> MethodBase }
        CodecSequenceEmitter.grouping (RuntimeCodecIL(method.GetILGenerator())) operands typeof<'Sequence>
        method

    static member internal GroupingReader<'Key,'Element,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<IGrouping<'Key,'Element>,'R> =
        JsonDefaultMethods.callable<JsonDirectReader<IGrouping<'Key,'Element>,'R>> typeof<IGrouping<'Key,'Element>> typeof<'R> typeof<'R> (JsonOperationPolicy<'Contract>.Name "grouping-read")
            (fun () -> JsonRawCollections<'Contract>.CreateGroupingReader<IGrouping<'Key,'Element>,'Key,'Element,'R>() :> MethodBase)
    static member internal FileReferenceReader<'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<FileRef,'R> =
        JsonRawCollections<'Contract>.PhysicalReader<FileRef,Types.SoloDBFileHeader,'R>(Func<int64,FileRef>(FileRef.Unloaded),Func<int64,Types.SoloDBFileHeader,FileRef>(fun id value -> FileRef.Loaded(id,value)))
    static member internal FileReferenceWriter<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<FileRef,'E> =
        JsonDirectWriter<FileRef,'E>(fun value destination state ->
            if value.HasValue then Unchecked.defaultof<'O>.Signed(value.Id,&destination,&state)
            else JsonRawOutput<'E,'O,'Contract>.Null(&destination,&state))

    static member internal DirectoryReferenceReader<'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<DirectoryRef,'R> =
        JsonRawCollections<'Contract>.PhysicalReader<DirectoryRef,Types.SoloDBDirectoryHeader,'R>(Func<int64,DirectoryRef>(DirectoryRef.Unloaded),Func<int64,Types.SoloDBDirectoryHeader,DirectoryRef>(fun id value -> DirectoryRef.Loaded(id,value)))
    static member internal DirectoryReferenceWriter<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<DirectoryRef,'E> =
        JsonDirectWriter<DirectoryRef,'E>(fun value destination state ->
            if value.HasValue then Unchecked.defaultof<'O>.Signed(value.Id,&destination,&state)
            else JsonRawOutput<'E,'O,'Contract>.Null(&destination,&state))

    static member internal FileManyReader<'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<FileRefMany,'R> =
        JsonRawCollections<'Contract>.ReferenceSequenceReader<FileRefMany,FileRef,'R>(false,true)
    static member internal FileManyWriter<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<FileRefMany,'E> =
        JsonRawCollections<'Contract>.SequenceWriter<FileRefMany,FileRef,'E,'O>(false)

    static member internal DirectoryManyReader<'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<DirectoryRefMany,'R> =
        JsonRawCollections<'Contract>.ReferenceSequenceReader<DirectoryRefMany,DirectoryRef,'R>(false,true)
    static member internal DirectoryManyWriter<'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<DirectoryRefMany,'E> =
        JsonRawCollections<'Contract>.SequenceWriter<DirectoryRefMany,DirectoryRef,'E,'O>(false)

    static member internal DocumentReader<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<DBRef<'T>,'R> =
        JsonRawCollections<'Contract>.PhysicalReader<DBRef<'T>,'T,'R>(Func<int64,DBRef<'T>>(DBRef<'T>.Unloaded),Func<int64,'T,DBRef<'T>>(fun id value -> DBRef<'T>.Loaded(id,value)))

    static member internal DocumentWriter<'T,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<DBRef<'T>,'E> =
        JsonDirectWriter<DBRef<'T>,'E>(fun value destination state ->
            if value.HasValue then Unchecked.defaultof<'O>.Signed(value.Id,&destination,&state)
            else JsonRawOutput<'E,'O,'Contract>.Null(&destination,&state))

    static member internal TypedDocumentReader<'T,'Id,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<DBRef<'T,'Id>,'R> =
        let read = JsonRawCollections<'Contract>.PhysicalReader<DBRef<'T,'Id>,'T,'R>(Func<int64,DBRef<'T,'Id>>(DBRef<'T,'Id>.Unloaded),
            Func<int64,'T,DBRef<'T,'Id>>(fun id value ->
                match SoloIdAccessor.TryGetBoxedValue(typeof<'T>,box value) with
                | ValueSome typedId -> DBRef<'T,'Id>.Loaded(id,unbox<'Id> typedId,value)
                | ValueNone -> DBRef<'T,'Id>.Loaded(id,value)))
        JsonDirectReader<DBRef<'T,'Id>,'R>(fun context ->
            context.Cursor.codeUnitIndex <- context.Range.Start
            if JsonLexer.nextSignificantCodePoint &context.Cursor = int '{' then
                context.Cursor.codeUnitIndex <- context.Cursor.codeUnitIndex + 1
                let marker = JsonMarkerCapture<'R>.Read &context
                if marker.Present then
                    let mutable child = JsonReadCursor.child &context marker.Range
                    DBRef<'T,'Id>.To(JsonRawRead<'Contract>.Read<'Id,'R> &child)
                else JsonRawCollections<'Contract>.PhysicalObject<DBRef<'T,'Id>,'T,'R>(&context,false)
            else read.Invoke &context)

    static member internal TypedDocumentWriter<'T,'Id,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<DBRef<'T,'Id>,'E> =
        JsonDirectWriter<DBRef<'T,'Id>,'E>(fun value destination state ->
            if value.HasPendingTypedId then
                JsonRawOutput<'E,'O,'Contract>.BeginObject(&destination,&state)
                JsonRawOutput<'E,'O,'Contract>.Property("$dbrefTypedId",false,&destination,&state)
                JsonRawWrite<'Id,'E,'O,'Contract>.Member(value.TypedId,&destination,&state)
                Unchecked.defaultof<'O>.Ascii('}'B,&destination,&state)
            elif value.HasValue then Unchecked.defaultof<'O>.Signed(value.Id,&destination,&state)
            else JsonRawOutput<'E,'O,'Contract>.Null(&destination,&state))

    static member internal OptionReader<'T,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<option<'T>,'R> =
        JsonRawCollections<'Contract>.ReadMethod<'R>(typeof<option<'T>>).CreateDelegate(typeof<JsonDirectReader<option<'T>,'R>>) :?> JsonDirectReader<option<'T>,'R>
    static member internal OptionWriter<'T,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<option<'T>,'E> =
        JsonRawCollections<'Contract>.WriteMethod<'E,'O>(typeof<option<'T>>).CreateDelegate(typeof<JsonDirectWriter<option<'T>,'E>>) :?> JsonDirectWriter<option<'T>,'E>
    static member internal NullableReader<'T,'R when 'T: struct and 'T :> ValueType and 'T: (new: unit -> 'T) and 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<Nullable<'T>,'R> =
        JsonRawCollections<'Contract>.ReadMethod<'R>(typeof<Nullable<'T>>).CreateDelegate(typeof<JsonDirectReader<Nullable<'T>,'R>>) :?> JsonDirectReader<Nullable<'T>,'R>
    static member internal NullableWriter<'T,'E,'O when 'T: struct and 'T :> ValueType and 'T: (new: unit -> 'T) and 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<Nullable<'T>,'E> =
        JsonRawCollections<'Contract>.WriteMethod<'E,'O>(typeof<Nullable<'T>>).CreateDelegate(typeof<JsonDirectWriter<Nullable<'T>,'E>>) :?> JsonDirectWriter<Nullable<'T>,'E>
    static member internal EnumReader<'T,'Underlying,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'T,'R> =
        JsonRawCollections<'Contract>.ReadMethod<'R>(typeof<'T>).CreateDelegate(typeof<JsonDirectReader<'T,'R>>) :?> JsonDirectReader<'T,'R>
    static member internal EnumWriter<'T,'Underlying,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<'T,'E> =
        JsonRawCollections<'Contract>.WriteMethod<'E,'O>(typeof<'T>).CreateDelegate(typeof<JsonDirectWriter<'T,'E>>) :?> JsonDirectWriter<'T,'E>

    static member internal SequenceContext<'T,'R when 'R : struct and 'R :> IJsonReader>(context: byref<JsonReadContext<'R>>, wrapper: bool): JsonReadContext<'R> =
        let mutable source = context
        match JsonReadCursor.token &source with
        | OpenBracket ->
            if isNull source.Elements then JsonReadCursor.scan &source
            context <- source
        | OpenBrace ->
            let envelope = JsonEnvelopeCapture<'R>.Read &source
            if not(envelope.TypePresent && envelope.ValuesPresent) then
                JsonValue.InvalidValue(sprintf "Expected JsonValue.List or Object{$type, $values} for array deserialization but got %A" (JsonRawInput<'Contract>.Tree<'R> &source))
            if not wrapper then failwithf "Type not supported for Newtownsoft deserialization: %s" typeof<'T>.FullName
            context <- source
            source <- JsonReadCursor.child &source envelope.Values
            JsonReadCursor.scan &source
            if isNull source.Elements then
                JsonValue.InvalidValue(sprintf "Expected JsonValue.List but got %A" (JsonRawInput<'Contract>.Tree<'R> &source))
        | _ -> JsonValue.InvalidValue(sprintf "Expected JsonValue.List or Object{$type, $values} for array deserialization but got %A" (JsonRawInput<'Contract>.Tree<'R> &source))
        source

    static member internal FinishFSharpList<'Element>(value: ResizeArray<'Element>): 'Element list = List.ofSeq value
    static member internal FinishEnumerable<'Element>(value: ResizeArray<'Element>): IEnumerable<'Element> =
        JsonRawCollections<'Contract>.SequenceResult<'Element,_>(value) :> IEnumerable<'Element>
    static member internal FinishOrderedEnumerable<'Element>(value: ResizeArray<'Element>): IOrderedEnumerable<'Element> =
        JsonRawCollections<'Contract>.SequenceResult<'Element,_>(Enumerable.OrderBy(value,Func<'Element,int>(fun _ -> 0)))
    static member internal FinishBinaryFSharpList(value: byte[]): byte list = List.ofArray value
    static member internal BinarySequenceMismatch<'Sequence>(): unit = raise(JsonPrimitiveShapeMismatch typeof<'Sequence>)

    static member private SequenceCollectionType(definition: string): Type =
        match definition with
        | "System.Collections.Generic.Stack`1" -> typedefof<Stack<_>>
        | "System.Collections.Generic.Queue`1" -> typedefof<Queue<_>>
        | "System.Collections.Generic.HashSet`1" -> typedefof<HashSet<_>>
        | "System.Collections.Generic.LinkedList`1" -> typedefof<LinkedList<_>>
        | "System.Collections.Generic.List`1" -> typedefof<ResizeArray<_>>
        | _ -> invalidArg "definition" "Unknown original collection operand."

    static member private CreateSequenceReader<'Sequence,'Element,'R when 'R : struct and 'R :> IJsonReader>(name: string, wrapper: bool): DynamicMethod =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct sequence read" typeof<'Sequence>
        let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
        let context = JsonRawInput<'Contract>.Api<'R>().Json
        let method = DynamicMethod("ReadRawSequence",typeof<'Sequence>,[|context.MakeByRefType()|],typeof<JsonValue>.Module,true)
        let il = RuntimeCodecIL(method.GetILGenerator())
        let input name = typeof<JsonRawInput<'Contract>>.GetMethod(name,flags).MakeGenericMethod(typeof<'R>) :> MethodBase
        let helper name = typeof<JsonRawCollections<'Contract>>.GetMethod(name,flags).MakeGenericMethod(typeof<'Element>) :> MethodBase
        if TypeSupport.SequenceBinaryReader name typeof<'Element>.FullName then
            let constructor = if name = "List" then Some(typeof<ResizeArray<byte>>.GetConstructor([|typeof<IEnumerable<byte>> |]) :> MethodBase) else None
            let finish = if name = "FSharpList" then Some(typeof<JsonRawCollections<'Contract>>.GetMethod("FinishBinaryFSharpList",flags) :> MethodBase) else None
            let mismatch = typeof<JsonRawCollections<'Contract>>.GetMethod("BinarySequenceMismatch",flags).MakeGenericMethod(typeof<'Sequence>) :> MethodBase
            let nullError = if TypeSupport.SequenceNullMismatch name typeof<'Element>.FullName then Some mismatch else None
            CodecSequenceEmitter.binaryReader il typeof<'Sequence> (input "SequenceNull") (JsonFormattedRuntime.method typeof<byte[]> typeof<'R> :> MethodBase) constructor finish nullError typeof<JsonPrimitiveShapeMismatch> mismatch
        else
            let struct(definition,addName,capacity,reverse,finish) = TypeSupport.SequenceConstruction name
            let array = definition = ""
            let working = if array then typeof<'Element[]> else (JsonRawCollections<'Contract>.SequenceCollectionType definition).MakeGenericType(typeof<'Element>)
            let add = if array then null else working.GetMethod(addName,[|typeof<'Element>|])
            let api =
                { Context = context; Int32 = typeof<int>; Working = working
                  IsNull = input "SequenceNull"
                  NullConstruct = None
                  Begin = typeof<JsonRawCollections<'Contract>>.GetMethod("SequenceContext",flags).MakeGenericMethod(typeof<'Sequence>,typeof<'R>) :> MethodBase
                  Count = input "SequenceCount"; Element = input "SequenceElement"
                  Construct = if array then null else working.GetConstructor(if capacity then [|typeof<int>|] else Type.EmptyTypes) :> MethodBase
                  ConstructCall = false; Admit = None; Receiver = None; BoxReceiver = false
                  Add = add :> MethodBase
                  Finish = if finish = "" then None else Some(helper ("Finish" + finish))
                  BeforeFinish = None
                  FinishConstructor = false; CastResult = false
                  PrimitiveError = typeof<JsonPrimitiveShapeMismatch>; ScalarError = typeof<JsonScalarShapeMismatch>
                  PrimitiveRetry = typeof<JsonRawRead<'Contract>>.GetMethod("PrimitiveRetry",flags).MakeGenericMethod(typeof<'Element>,typeof<'R>) :> MethodBase
                  ScalarRetry = typeof<JsonRawRead<'Contract>>.GetMethod("ScalarRetry",flags).MakeGenericMethod(typeof<'Element>,typeof<'R>) :> MethodBase }
            let forward = JsonRawInput<'Contract>.ForwardApi<'R>(false)
            CodecSequenceEmitter.reader il api forward.Operation forward.Read typeof<'Sequence> typeof<'Element> wrapper array capacity reverse (not array && not(add.ReturnType.Equals(typeof<Void>)))
        method

    static member internal SequenceReadMethod<'Sequence,'Element,'R when 'R : struct and 'R :> IJsonReader>(name: string, wrapper: bool): MethodBase =
        JsonDefaultMethods.methodWithContract typeof<'Sequence> typeof<'R> typeof<'R> (JsonOperationPolicy<'Contract>.Name("sequence-read:" + name + (if wrapper then ":wrapper" else "")))
            (fun () -> JsonRawCollections<'Contract>.CreateSequenceReader<'Sequence,'Element,'R>(name,wrapper) :> MethodBase)

    static member internal SequenceReader<'Sequence,'Element,'R when 'R : struct and 'R :> IJsonReader>(name: string, wrapper: bool): JsonDirectReader<'Sequence,'R> =
        JsonDefaultMethods.callable<JsonDirectReader<'Sequence,'R>> typeof<'Sequence> typeof<'R> typeof<'R>
            (JsonOperationPolicy<'Contract>.Name("sequence-read:" + name + (if wrapper then ":wrapper" else "")))
            (fun () -> JsonRawCollections<'Contract>.CreateSequenceReader<'Sequence,'Element,'R>(name,wrapper) :> MethodBase)

    static member internal ReferenceSequenceReader<'Sequence,'Element,'R when 'R : struct and 'R :> IJsonReader>(populate: bool, nullEmpty: bool): JsonDirectReader<'Sequence,'R> =
        JsonDefaultMethods.callable<JsonDirectReader<'Sequence,'R>> typeof<'Sequence> typeof<'R> typeof<'R> (JsonOperationPolicy<'Contract>.Name "reference-sequence-read")
            (fun () -> JsonRawCollections<'Contract>.CreateCustomSequenceReader<'Sequence,'Element,'R>(populate,false,false,nullEmpty,not populate) :> MethodBase)

    static member internal ManyReader<'Element,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<DBRefMany<'Element>,'R> =
        JsonRawCollections<'Contract>.ReferenceSequenceReader<DBRefMany<'Element>,'Element,'R>(true,false)

    static member internal TypedManyReader<'Element,'Id,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<DBRefMany<'Element,'Id>,'R> =
        JsonRawCollections<'Contract>.ReferenceSequenceReader<DBRefMany<'Element,'Id>,'Element,'R>(true,false)

    static member internal CaptureSequenceBytes(value: IEnumerable<byte>): byte[] = Seq.toArray value

    static member private CreateSequenceWriter<'Sequence,'Element,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(binary: bool, nonGeneric: bool): DynamicMethod =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct sequence write" typeof<'Sequence>
        let flags = BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static
        let parameters = typeof<JsonDirectWriter<'Sequence,'E>>.GetMethod("Invoke",BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance).GetParameters() |> Array.map _.ParameterType
        let method = DynamicMethod("WriteRawSequence",typeof<Void>,parameters,typeof<JsonValue>.Module,true)
        let enumerator = if nonGeneric then typeof<IEnumerator> else typeof<IEnumerator<'Element>>
        let enumerable = if nonGeneric then typeof<IEnumerable> else typeof<IEnumerable<'Element>>
        let sequence =
            { Enumerator = enumerator
              GetEnumerator = enumerable.GetMethod("GetEnumerator") :> MethodBase
              MoveNext = typeof<IEnumerator>.GetMethod("MoveNext") :> MethodBase
              Current = enumerator.GetProperty("Current").GetMethod :> MethodBase
              Dispose = typeof<IDisposable>.GetMethod("Dispose") :> MethodBase
              DisposeType = if nonGeneric then Some typeof<IDisposable> else None
              Snapshot = typeof<JsonRawCollections<'Contract>>.GetMethod("CaptureSequenceBytes",flags) :> MethodBase
              Bytes = if binary then JsonWriterRuntime.bytes typeof<'E> :> MethodBase else null }
        CodecSequenceEmitter.writer (RuntimeCodecIL(method.GetILGenerator())) (JsonRawOutput<'E,'O,'Contract>.ForwardApi typeof<'Sequence>)
            sequence typeof<'Sequence> typeof<'Element> typeof<'Sequence>.IsValueType binary
        method

    static member internal SequenceWriteMethod<'Sequence,'Element,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(binary: bool): MethodBase =
        JsonDefaultMethods.methodWithContract typeof<'Sequence> typeof<'E> typeof<'O> (if binary then (JsonOperationPolicy<'Contract>.Name "sequence-binary-write") else (JsonOperationPolicy<'Contract>.Name "sequence-write"))
            (fun () -> JsonRawCollections<'Contract>.CreateSequenceWriter<'Sequence,'Element,'E,'O>(binary,false) :> MethodBase)

    static member internal SequenceWriter<'Sequence,'Element,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(binary: bool): JsonDirectWriter<'Sequence,'E> =
        JsonDefaultMethods.callable<JsonDirectWriter<'Sequence,'E>> typeof<'Sequence> typeof<'E> typeof<'O>
            (if binary then (JsonOperationPolicy<'Contract>.Name "sequence-binary-write") else (JsonOperationPolicy<'Contract>.Name "sequence-write"))
            (fun () -> JsonRawCollections<'Contract>.CreateSequenceWriter<'Sequence,'Element,'E,'O>(binary,false) :> MethodBase)

    static member private PublishSequenceResult<'Element,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(target: Type): unit =
        let select = JsonRawWriteMetadata<'E,'O,'Contract>.Select
        match TypeMetadataCache.Read(target,select) with
        | ValueSome _ -> ()
        | ValueNone ->
            TypeMetadataCache.Discover(target,select,
                (fun operation metadata -> JsonRawWriteMetadata<'E,'O,'Contract>.Store(operation,metadata)),
                Func<JsonRawWriteOperations>(fun () ->
                    let binary = typeof<'Element>.Equals(typeof<byte>)
                    let role = if binary then (JsonOperationPolicy<'Contract>.Name "sequence-binary-write") else (JsonOperationPolicy<'Contract>.Name "sequence-write")
                    let writer = JsonDefaultMethods.callable<JsonDirectWriter<IEnumerable<'Element>,'E>> typeof<IEnumerable<'Element>> typeof<'E> typeof<'O> role
                                     (fun () ->
                                         if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct sequence write" typeof<IEnumerable<'Element>>
                                         JsonRawCollections<'Contract>.CreateSequenceWriter<IEnumerable<'Element>,'Element,'E,'O>(binary,false) :> MethodBase)
                    { Writer = null; WriteRuntime = JsonDirectRuntimeWriter<'E>(fun value destination state -> writer.Invoke(value :?> IEnumerable<'Element>,&destination,&state)) })) |> ignore

    static member internal SequenceResult<'Element,'Sequence when 'Sequence :> IEnumerable<'Element>>(value: 'Sequence): 'Sequence =
        let target = value.GetType()
        JsonRawCollections<'Contract>.PublishSequenceResult<'Element,byte,JsonUtf8Writer>(target)
        JsonRawCollections<'Contract>.PublishSequenceResult<'Element,char,JsonUtf16Writer>(target)
        value

    static member internal CustomDictionaryReader<'Dictionary,'Key,'Value,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'Dictionary,'R> =
        JsonRawCollections<'Contract>.DictionaryReaderBody<'Dictionary,'Key,'Value,'R>()

    static member internal AdmitCustomSequence<'Sequence>(): unit =
        try JsonScalarOperations<'Sequence>.Reader |> ignore
        with error -> raise(TypeInitializationException("SoloDatabase.JsonSerializator.JsonDeserializerImpl`1",error))

    static member private CreateCustomSequenceReader<'Sequence,'Element,'R when 'R : struct and 'R :> IJsonReader>(populate: bool, wrapper: bool, admit: bool, nullEmpty: bool, publish: bool): DynamicMethod =
        let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
        let context = JsonRawInput<'Contract>.Api<'R>().Json
        let method = DynamicMethod("ReadRawSequence",typeof<'Sequence>,[|context.MakeByRefType()|],typeof<JsonValue>.Module,true)
        let input name = typeof<JsonRawInput<'Contract>>.GetMethod(name,flags).MakeGenericMethod(typeof<'R>) :> MethodBase
        let working = if populate then typeof<'Sequence> else typeof<ResizeArray<'Element>>
        let constructor = working.GetConstructor(if populate then Type.EmptyTypes else [|typeof<int>|])
        let contract = typeof<ICollection<'Element>>
        let finish = if populate then null else typeof<'Sequence>.GetConstructor([|typeof<IEnumerable<'Element>> |])
        let api =
            { Context = context; Int32 = typeof<int>; Working = working
              IsNull = input "SequenceNull"
              NullConstruct = if nullEmpty then Some(typeof<'Sequence>.GetConstructor(Type.EmptyTypes) :> MethodBase) else None
              Begin = typeof<JsonRawCollections<'Contract>>.GetMethod("SequenceContext",flags).MakeGenericMethod(typeof<'Sequence>,typeof<'R>) :> MethodBase
              Count = input "SequenceCount"; Element = input "SequenceElement"
              Construct = if isNull constructor then JsonRawCollections<'Contract>.ConstructorMethod<'Sequence>(null) :> MethodBase else constructor :> MethodBase
              ConstructCall = isNull constructor
              Admit = if admit then Some(typeof<JsonRawCollections<'Contract>>.GetMethod("AdmitCustomSequence",flags).MakeGenericMethod(typeof<'Sequence>) :> MethodBase) else None
              Receiver = (if populate then Some typeof<IList<'Element>> else None)
              BoxReceiver = populate && typeof<'Sequence>.IsValueType
              Add = (if populate then contract else working).GetMethod("Add",[|typeof<'Element>|]) :> MethodBase
              Finish = if isNull finish then None else Some(finish :> MethodBase)
              BeforeFinish = if publish then Some(typeof<JsonRawCollections<'Contract>>.GetMethod("FinishEnumerable",flags).MakeGenericMethod(typeof<'Element>) :> MethodBase) else None
              FinishConstructor = not(isNull finish); CastResult = not populate && isNull finish
              PrimitiveError = typeof<JsonPrimitiveShapeMismatch>; ScalarError = typeof<JsonScalarShapeMismatch>
              PrimitiveRetry = typeof<JsonRawRead<'Contract>>.GetMethod("PrimitiveRetry",flags).MakeGenericMethod(typeof<'Element>,typeof<'R>) :> MethodBase
              ScalarRetry = typeof<JsonRawRead<'Contract>>.GetMethod("ScalarRetry",flags).MakeGenericMethod(typeof<'Element>,typeof<'R>) :> MethodBase }
        let forward = JsonRawInput<'Contract>.ForwardApi<'R>(false)
        CodecSequenceEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) api forward.Operation forward.Read typeof<'Sequence> typeof<'Element>
            wrapper false (not populate) false false
        method

    static member internal CustomSequenceReader<'Sequence,'Element,'R when 'R : struct and 'R :> IJsonReader>
        (populate: bool, wrapper: bool): JsonDirectReader<'Sequence,'R> =
        let method = JsonDefaultMethods.get typeof<'Sequence> typeof<'R> (JsonOperationPolicy<'Contract>.Name "custom-sequence-read")
                         (fun () -> JsonRawCollections<'Contract>.CreateCustomSequenceReader<'Sequence,'Element,'R>(populate,wrapper,true,false,false))
        method.CreateDelegate(typeof<JsonDirectReader<'Sequence,'R>>) :?> JsonDirectReader<'Sequence,'R>

    static member internal NonGenericDictionaryReader<'Dictionary,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'Dictionary,'R> =
        JsonRawCollections<'Contract>.DictionaryReaderBody<'Dictionary,obj,obj,'R>()

    static member internal NonGenericDictionaryWriter<'Dictionary,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<'Dictionary,'E> =
        JsonRawCollections<'Contract>.DictionaryWriterBody<'Dictionary,obj,obj,'E,'O>()

    static member internal NonGenericSequenceWriter<'Sequence,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<'Sequence,'E> =
        let method = JsonDefaultMethods.withContract typeof<'Sequence> typeof<'E> typeof<'O> (JsonOperationPolicy<'Contract>.Name "non-generic-sequence-write")
                         (fun () -> JsonRawCollections<'Contract>.CreateSequenceWriter<'Sequence,obj,'E,'O>(false,true))
        method.CreateDelegate(typeof<JsonDirectWriter<'Sequence,'E>>) :?> JsonDirectWriter<'Sequence,'E>

    static member internal TreeReader<'T,'R when 'R : struct and 'R :> IJsonReader>(create: Func<JsonValue,'T>): JsonDirectReader<'T,'R> =
        JsonDirectReader<'T,'R>(fun context -> create.Invoke(JsonRawInput<'Contract>.Tree<'R> &context))
    static member internal TreeWriter<'T,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(read: Func<'T,JsonValue>): JsonDirectWriter<'T,'E> =
        JsonDirectWriter<'T,'E>(fun value destination state ->
            if isNull(box value) then JsonRawOutput<'E,'O,'Contract>.Null(&destination,&state)
            else JsonRawOutput<'E,'O,'Contract>.ExplicitTree((if state.IncludeType then (box value).GetType() else null),read.Invoke value,&destination,&state))

    static member private CreateDictionaryReader<'Dictionary,'Key,'Value,'R when 'R : struct and 'R :> IJsonReader>(): MethodBase =
        let target = typeof<'Dictionary>
        let decision = JsonCodecDiscovery.Decision target
        let struct(implementation,beforeScan,pairAdd,nonGeneric) = TypeSupport.DictionaryConstruction(decision.Shape,target.IsAbstract)
        let flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic
        let context = JsonRawInput<'Contract>.Api<'R>().Json
        let working =
            match implementation with
            | "Dictionary" -> typedefof<Dictionary<_,_>>.MakeGenericType(typeof<'Key>,typeof<'Value>)
            | "SortedDictionary" -> typedefof<SortedDictionary<_,_>>.MakeGenericType(typeof<'Key>,typeof<'Value>)
            | "Hashtable" -> target
            | _ -> target
        let constructorType = if implementation = "Hashtable" then typeof<Hashtable> else working
        let constructor = constructorType.GetConstructor(Type.EmptyTypes)
        let construct = if isNull constructor then JsonRawCollections<'Contract>.ConstructorMethod<'Dictionary>(null) :> MethodBase else constructor :> MethodBase
        let receiver =
            if nonGeneric then typeof<IDictionary>
            elif pairAdd then typeof<ICollection<KeyValuePair<'Key,'Value>>>
            else typeof<IDictionary<'Key,'Value>>
        let properties = typeof<Dictionary<string,JsonSourceRange>>
        let iterator = properties.GetMethod("GetEnumerator").ReturnType
        let entry = typeof<KeyValuePair<string,JsonSourceRange>>
        let keyAcquire =
            if nonGeneric || typeof<'Key>.Equals(typeof<string>) then None
            else Some(typeof<ConvertStringTo<'Key>>.GetProperty("Convert",flags).GetMethod :> MethodBase)
        let input name = typeof<JsonRawInput<'Contract>>.GetMethod(name,flags).MakeGenericMethod(typeof<'R>) :> MethodBase
        let cursor = typeof<JsonValue>.Assembly.GetType("SoloDatabase.JsonSerializator.JsonReadCursor")
        let add =
            if nonGeneric then receiver.GetMethod("Add")
            elif pairAdd then receiver.GetMethod("Add",[| typeof<KeyValuePair<'Key,'Value>> |])
            else receiver.GetMethod("Add",[|typeof<'Key>;typeof<'Value>|])
        let api =
            { Context = context; Working = working; Receiver = receiver; Enumerator = iterator; Entry = entry
              String = typeof<string>; Key = typeof<'Key>; Value = typeof<'Value>
              IsNull = input "IsNull"; Begin = input "ObjectProperties"
              Properties = context.GetField("Properties",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
              GetEnumerator = properties.GetMethod("GetEnumerator") :> MethodBase
              MoveNext = iterator.GetMethod("MoveNext") :> MethodBase; Current = iterator.GetProperty("Current").GetMethod :> MethodBase
              Name = entry.GetProperty("Key").GetMethod :> MethodBase; Range = entry.GetProperty("Value").GetMethod :> MethodBase
              Child = cursor.GetMethod("child",flags).MakeGenericMethod(typeof<'R>) :> MethodBase
              Dispose = iterator.GetMethod("Dispose") :> MethodBase
              Construct = construct; ConstructCall = isNull constructor; ConstructCast = implementation = "Hashtable"
              BoxReceiver = working.IsValueType; CastResult = not(working.Equals target); TargetValueType = target.IsValueType
              ConvertKey = keyAcquire
              KeyInvoke = keyAcquire |> Option.map(fun _ -> typeof<string -> 'Key>.GetMethod("Invoke") :> MethodBase)
              Add = add :> MethodBase
              Pair = if pairAdd then Some(typeof<KeyValuePair<'Key,'Value>>.GetConstructor([|typeof<'Key>;typeof<'Value>|]) :> MethodBase) else None
              Equal = typeof<string>.GetMethod("op_Equality") :> MethodBase
              PrimitiveError = typeof<JsonPrimitiveShapeMismatch>; ScalarError = typeof<JsonScalarShapeMismatch>
              PrimitiveRetry = typeof<JsonRawRead<'Contract>>.GetMethod("PrimitiveRetry",flags).MakeGenericMethod(typeof<'Value>,typeof<'R>) :> MethodBase
              ScalarRetry = typeof<JsonRawRead<'Contract>>.GetMethod("ScalarRetry",flags).MakeGenericMethod(typeof<'Value>,typeof<'R>) :> MethodBase }
        let method = DynamicMethod("ReadRawDictionary",target,[|context.MakeByRefType()|],typeof<JsonValue>.Module,true)
        let input = JsonRawInput<'Contract>.ForwardApi<'R>(false)
        CodecDictionaryEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) api input.Operation input.Read target beforeScan
        method :> MethodBase

    static member private DictionaryReaderBody<'Dictionary,'Key,'Value,'R when 'R : struct and 'R :> IJsonReader>(): JsonDirectReader<'Dictionary,'R> =
        JsonDefaultMethods.callable<JsonDirectReader<'Dictionary,'R>> typeof<'Dictionary> typeof<'R> typeof<'R> (JsonOperationPolicy<'Contract>.Name "dictionary-read")
            (fun () -> JsonRawCollections<'Contract>.CreateDictionaryReader<'Dictionary,'Key,'Value,'R>())

    static member internal DictionaryReader<'Dictionary,'Key,'Value,'R when 'Key : equality and 'R : struct and 'R :> IJsonReader>(_: string): JsonDirectReader<'Dictionary,'R> =
        JsonRawCollections<'Contract>.DictionaryReaderBody<'Dictionary,'Key,'Value,'R>()

    static member private DictionaryKeyFormat<'Key>(): MethodBase =
        JsonDefaultMethods.methodWithContract typeof<'Key> typeof<string> typeof<string> "dictionary-key-format" (fun () ->
            let method = DynamicMethod("FormatDictionaryKey",typeof<string>,[|typeof<'Key>|],typeof<JsonValue>.Module,true)
            let nullable = not(isNull(Nullable.GetUnderlyingType typeof<'Key>))
            let formattable = typeof<IFormattable>.IsAssignableFrom typeof<'Key>
            let api =
                { Object = typeof<obj>; String = typeof<string>; Formattable = typeof<IFormattable>
                  Invariant = typeof<CultureInfo>.GetProperty("InvariantCulture").GetMethod :> MethodBase
                  Format = typeof<IFormattable>.GetMethod("ToString") :> MethodBase
                  ToString = typeof<obj>.GetMethod("ToString") :> MethodBase }
            CodecDictionaryEmitter.formatKey (RuntimeCodecIL(method.GetILGenerator())) api typeof<'Key> typeof<'Key>.IsValueType formattable
                (TypeSupport.DictionaryFormatProbe(formattable,typeof<'Key>.IsSealed,nullable)) (typeof<'Key>.Equals(typeof<string>))
            method :> MethodBase)

    static member private CreateDictionaryWriter<'Dictionary,'Key,'Value,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): MethodBase =
        let target = typeof<'Dictionary>
        let nonGeneric = (JsonCodecDiscovery.Decision target).Shape = TypeShape.NonGenericDictionary
        let readOnly = typeof<IReadOnlyDictionary<'Key,'Value>>.IsAssignableFrom target
        let probe = TypeSupport.DictionaryReadOnlyProbe(readOnly,target.IsSealed,nonGeneric)
        let contract (owner: Type) generic =
            let iterator = if generic then typeof<IEnumerator<'Key>> else typeof<IEnumerator>
            let enumerable = if generic then typeof<IEnumerable<'Key>> else typeof<IEnumerable>
            { Type = owner; Keys = owner.GetProperty("Keys").GetMethod :> MethodBase
              Indexer = owner.GetProperty("Item").GetMethod :> MethodBase
              GetEnumerator = enumerable.GetMethod("GetEnumerator") :> MethodBase
              Enumerator = iterator; MoveNext = typeof<IEnumerator>.GetMethod("MoveNext") :> MethodBase
              Current = iterator.GetProperty("Current").GetMethod :> MethodBase
              Dispose = typeof<IDisposable>.GetMethod("Dispose") :> MethodBase
              DisposeType = if generic then None else Some typeof<IDisposable> }
        let outputType = typeof<JsonRawOutput<'E,'O,'Contract>>
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
        let output name = outputType.GetMethod(name,flags) :> MethodBase
        let api =
            { Object = typeof<obj>; String = typeof<string>; Boolean = typeof<bool>; Key = typeof<'Key>; Value = typeof<'Value>; Names = typeof<HashSet<string>>
              ReadOnly = if readOnly || probe then Some(contract typeof<IReadOnlyDictionary<'Key,'Value>> true) else None
              Mutable = if nonGeneric then Some(contract typeof<IDictionary> false) elif not readOnly then Some(contract typeof<IDictionary<'Key,'Value>> true) else None
              ProbeReadOnly = probe
              Name = if nonGeneric then typeof<obj>.GetMethod("ToString") :> MethodBase else JsonRawCollections<'Contract>.DictionaryKeyFormat<'Key>()
              NameVirtual = nonGeneric
              NewNames = typeof<HashSet<string>>.GetConstructor(Type.EmptyTypes) :> MethodBase
              AddName = typeof<HashSet<string>>.GetMethod("Add") :> MethodBase
              Equal = typeof<string>.GetMethod("op_Equality") :> MethodBase
              Concat = typeof<string>.GetMethod("Concat",[|typeof<string>;typeof<string>|]) :> MethodBase
              ArgumentException = typeof<ArgumentException>.GetConstructor([|typeof<string>|]) :> MethodBase
              Property = output "Property"
              TypeMember = outputType.GetMethod("TypeMember",flags).MakeGenericMethod(target,typeof<'Value>) :> MethodBase
              EndObject = outputType.GetMethod("EndObject",flags).MakeGenericMethod(target) :> MethodBase }
        let parameters = typeof<JsonDirectWriter<'Dictionary,'E>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters() |> Array.map _.ParameterType
        let method = DynamicMethod("WriteRawDictionary",typeof<Void>,parameters,typeof<JsonValue>.Module,true)
        CodecDictionaryEmitter.writer (RuntimeCodecIL(method.GetILGenerator())) (JsonRawOutput<'E,'O,'Contract>.ForwardApi target) api target target.IsValueType nonGeneric
        method :> MethodBase

    static member private DictionaryWriterBody<'Dictionary,'Key,'Value,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<'Dictionary,'E> =
        JsonDefaultMethods.callable<JsonDirectWriter<'Dictionary,'E>> typeof<'Dictionary> typeof<'E> typeof<'O> (JsonOperationPolicy<'Contract>.Name "dictionary-write")
            (fun () -> JsonRawCollections<'Contract>.CreateDictionaryWriter<'Dictionary,'Key,'Value,'E,'O>())

    static member internal DictionaryWriter<'Dictionary,'Key,'Value,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>(): JsonDirectWriter<'Dictionary,'E> =
        JsonRawCollections<'Contract>.DictionaryWriterBody<'Dictionary,'Key,'Value,'E,'O>()

and private JsonRuntimeCalls =
    static member Helper(name: string, arguments: Type[]) =
        let method = typeof<JsonImpl>.GetMethod(name,BindingFlags.Static ||| BindingFlags.NonPublic)
        (if arguments.Length = 0 then method else method.MakeGenericMethod(arguments)) :> MethodBase

    static member Reader<'T>(before: ConstructorInfo option, call: MethodBase option, after: ConstructorInfo option, cast: bool, nullDefault: bool) =
        let method = DynamicMethod("ReadJson",typeof<'T>,[|typeof<JsonValue>|],typeof<JsonValue>.Module,true)
        let operation = { Result = typeof<'T>; Before = before |> Option.map (fun item -> item :> MethodBase)
                          Call = call; After = after |> Option.map (fun item -> item :> MethodBase); Cast = cast; NullDefault = nullDefault }
        CodecEmitter.readerCall (RuntimeCodecIL(method.GetILGenerator())) (typeof<JsonValue>.GetMethod("get_IsNull") :> MethodBase) operation
        method.CreateDelegate(typeof<Func<JsonValue,'T>>) :?> Func<JsonValue,'T>

    static member ReaderOperation<'T>(operation: CodecReaderCall<Type,MethodBase>) =
        let method = DynamicMethod("ReadJson",typeof<'T>,[|typeof<JsonValue>|],typeof<JsonValue>.Module,true)
        CodecEmitter.readerCall (RuntimeCodecIL(method.GetILGenerator())) (typeof<JsonValue>.GetMethod("get_IsNull") :> MethodBase) operation
        method.CreateDelegate(typeof<Func<JsonValue,'T>>) :?> Func<JsonValue,'T>

    static member WriterOperation<'T>(operation: CodecWriterCall<Type,MethodBase>) =
        let method = DynamicMethod("WriteJson",typeof<JsonValue>,[|typeof<'T>|],typeof<JsonValue>.Module,true)
        CodecEmitter.writerCall (RuntimeCodecIL(method.GetILGenerator())) (typeof<JsonValue>.GetMethod("get_Null") :> MethodBase) operation
        method.CreateDelegate(typeof<Func<'T,JsonValue>>) :?> Func<'T,JsonValue>

    static member Writer<'T>(call: MethodBase, boxValue: bool, virtualCall: bool) =
        let method = DynamicMethod("WriteJson",typeof<JsonValue>,[|typeof<'T>|],typeof<JsonValue>.Module,true)
        let operation = { Input = typeof<'T>; ValueType = typeof<'T>.IsValueType; Box = boxValue; Call = call; Virtual = virtualCall }
        CodecEmitter.writerCall (RuntimeCodecIL(method.GetILGenerator())) (typeof<JsonValue>.GetMethod("get_Null") :> MethodBase) operation
        method.CreateDelegate(typeof<Func<'T,JsonValue>>) :?> Func<'T,JsonValue>

    static member Empty<'T>(constructor: ConstructorInfo) =
        let method = DynamicMethod("JsonEmptyCollection",typeof<'T>,Type.EmptyTypes,typeof<JsonValue>.Module,true)
        CodecEmitter.typedConstruction (RuntimeCodecIL(method.GetILGenerator())) (constructor :> MethodBase) 0
        method.CreateDelegate(typeof<Func<'T>>) :?> Func<'T>

and private JsonDeserializerImpl<'A> =
    static member internal Build(): Func<JsonValue,'A> =
        match JsonScalarOperations<'A>.Reader with
        | ValueSome read -> read
        | ValueNone ->
            match JsonCodecDiscovery.TryBuild<'A>() with
            | ValueSome codec -> codec.Reader
            | ValueNone -> JsonDeserializerImpl<'A>.BuildStructuredDeserializer()

    static member private BuildStructuredDeserializer() : Func<JsonValue,'A> =
        let target = typeof<'A>
        let decision = JsonCodecDiscovery.Decision target
        let preparation = CodecPreparation.treeReader decision.Shape
        let read before call after cast nullDefault = JsonRuntimeCalls.Reader<'A>(before,call,after,cast,nullDefault)
        let genericArguments name =
            let definition = target.GetInterface(name) |> Option.ofObj |> Option.defaultValue target
            TypeMetadata.GenericTypeArgCache.Get definition
        let definition = if target.IsGenericType then target.GetGenericTypeDefinition() else target
        let selected = TypeSupport.TreeValueReader(definition.FullName,target,TypeMetadata.GenericTypeArgCache.Get target,
                            Func<string,Type[],MethodBase>(fun name types -> JsonRuntimeCalls.Helper(name,types)))
        match selected with
        | Some operation -> JsonRuntimeCalls.ReaderOperation<'A> operation
        | None ->
        if preparation = CodecTreeOperation.Collection then
            let arguments = if decision.Contract = "" then [||] else genericArguments (decision.Contract.Split('.') |> Array.last)
            let populate = TypeSupport.PopulateCollection(decision.Shape,(if target.IsGenericType then (TypeMetadata.GenericTypeArgCache.Get target).Length else 0),typeof<IList>.IsAssignableFrom target,not(isNull(target.GetConstructor(Type.EmptyTypes))))
            let constructor =
                if decision.Shape = TypeShape.CustomSequence && not populate then target.GetConstructor([|typedefof<IEnumerable<_>>.MakeGenericType(arguments[0])|])
                else target.GetConstructor(Type.EmptyTypes)
            let helper = Func<string,Type[],MethodBase>(fun name types ->
                if name = "MissingCollectionConstructor" then failwithf "Type %s must have a parameterless constructor" target.Name
                JsonRuntimeCalls.Helper(name,types))
            let operation = TypeSupport.CollectionTreeReader(decision.Shape,target,constructor |> Option.ofObj |> Option.map(fun value -> value :> MethodBase),target.IsAbstract,populate,arguments,helper)
            let standard = JsonRuntimeCalls.ReaderOperation<'A> operation
            if decision.Shape = TypeShape.CustomSequence then
                Func<JsonValue,'A>(fun json -> JsonSequenceOperations.Read<'A>(json,TypeSupport.ArrayValuesWrapper(target.IsArray,(if target.IsArray then target.GetElementType().FullName else "")),standard))
            else standard
        else
          match target with
          | t when preparation = CodecTreeOperation.Bson -> read None None (Some(t.GetConstructor([|typeof<JsonValue>|]))) false false
          | _ -> JsonObjectGeneration.Reader<'A>()

/// <summary>
/// Provides a cached, compiled serialization function for a specific type 'A.
/// </summary>
/// <typeparam name="'A">The source type for serialization.</typeparam>
and private JsonSerializerImpl<'A> =
    static member internal Build() =
        let mutable shape = JsonSerializationShape.Dynamic
        let write =
            if CodecPreparation.treeWriter(TypeShape.Object,false,typeof<'A>.IsAbstract) = CodecTreeOperation.Abstract then
                let target=typeof<'A>
                let decision=JsonCodecDiscovery.Decision target
                if typeof<JsonValue>.IsAssignableFrom target then shape <- JsonCodecDiscovery.ValueShape target
                elif TypeSupport.Collection decision.Shape then
                    let definition =
                        let original = if target.IsGenericType then target.GetGenericTypeDefinition() else target
                        if decision.Contract = "" || original.FullName = decision.Contract then target
                        else target.GetInterface(decision.Contract.Split('.') |> Array.last)
                    let arguments=TypeMetadata.GenericTypeArgCache.Get definition
                    shape <-
                        match decision.Shape with
                        | TypeShape.DictionaryInterface | TypeShape.ReadOnlyDictionaryInterface
                        | TypeShape.CustomDictionary | TypeShape.CustomReadOnlyDictionary -> JsonSerializationShape.DictionaryValues arguments[1]
                        | TypeShape.Grouping | TypeShape.GroupingImplementation -> JsonSerializationShape.GroupingValues(arguments[0],arguments[1])
                        | TypeShape.NonGenericDictionary -> JsonSerializationShape.Uninterpreted
                        | _ -> JsonSerializationShape.SequenceValues arguments[0]
                else
                    let model=JsonModelDiscovery.Model target
                    if model.Union then JsonModelDiscovery.UnionStorage model |> ignore
                    shape <- JsonSerializationShape.DeclaredMembers(CodecEmitter.writtenMembers model |> Array.map(fun index ->
                        let memberInfo=model.Members[index]
                        struct(memberInfo.Name,memberInfo.Type)))
                Func<'A,JsonValue>(fun value -> JsonImpl.SerializeAbstract(box value))
            else
                match JsonScalarOperations<'A>.Writer with
                | ValueSome write ->
                    shape <- JsonCodecDiscovery.ValueShape typeof<'A>
                    write
                | ValueNone ->
                    match JsonCodecDiscovery.TryBuild<'A>() with
                    | ValueSome codec ->
                        shape <- codec.Shape
                        codec.Writer
                    | ValueNone -> JsonSerializerImpl<'A>.BuildStructuredSerializer(fun value -> shape <- value)
        struct(write,shape)

    static member private BuildStructuredSerializer(setShape: JsonSerializationShape -> unit): Func<'A,JsonValue> =
        let target = typeof<'A>
        let decision = JsonCodecDiscovery.Decision target
        let preparation = CodecPreparation.treeWriter(decision.Shape,typeof<IEnumerable>.IsAssignableFrom target && not(typeof<string>.Equals target),target.IsAbstract)
        let write name arguments boxValue = JsonRuntimeCalls.Writer<'A>(JsonRuntimeCalls.Helper(name,arguments),boxValue,false)
        if preparation = CodecTreeOperation.Collection then
            let definition = if decision.Contract = "" then target else target.GetInterface(decision.Contract.Split('.') |> Array.last) |> Option.ofObj |> Option.defaultValue target
            let arguments = if decision.Contract = "" then [||] else TypeMetadata.GenericTypeArgCache.Get definition
            match decision.Shape with
            | TypeShape.CustomDictionary | TypeShape.CustomReadOnlyDictionary -> setShape(JsonSerializationShape.DictionaryValues arguments[1])
            | TypeShape.NonGenericDictionary -> setShape JsonSerializationShape.Uninterpreted
            | TypeShape.CustomSequence ->
                setShape(if typeof<byte>.Equals(arguments[0]) then JsonSerializationShape.Scalar else JsonSerializationShape.SequenceValues arguments[0])
            | _ -> ()
            let operation = TypeSupport.CollectionTreeWriter(decision.Shape,target,target.IsValueType,target.IsAbstract,
                                arguments.Length = 1 && typeof<byte>.Equals(arguments[0]),arguments,Func<string,Type[],MethodBase>(fun name types -> JsonRuntimeCalls.Helper(name,types)))
            JsonRuntimeCalls.WriterOperation<'A> operation
        else
          match target with
          | t when typeof<obj>.Equals(t) ->
              Func<'A,JsonValue>(fun value ->
                  let boxed = box value
                  if isNull boxed then Null
                  elif typeof<obj>.Equals(boxed.GetType()) then JsonValue.New()
                  else JsonImpl.SerializeByTypeWithType (boxed.GetType()) boxed)
          | t when preparation = CodecTreeOperation.Abstract ->
              Func<'A,JsonValue>(fun value -> let boxed = box value in if isNull boxed then Null else JsonImpl.SerializeByTypeWithType (boxed.GetType()) boxed)
          | OfType (id: byte seq -> byte seq) -> setShape JsonSerializationShape.Scalar; write "SerializeByteSequence" [||] false
          | t when typeof<JsonValue>.IsAssignableFrom t ->
              setShape(JsonCodecDiscovery.ValueShape t)
              Func<'A,JsonValue>(fun value -> box value :?> JsonValue)
          | t when preparation = CodecTreeOperation.Bson ->
              setShape JsonSerializationShape.Uninterpreted
              JsonRuntimeCalls.Writer<'A>(t.GetProperty("Json").GetGetMethod() :> MethodBase,false,true)
          | t when preparation = CodecTreeOperation.NonGenericSequence ->
              setShape JsonSerializationShape.Uninterpreted
              write "SerializeNonGenericEnumerable" [||] t.IsValueType
          | _ ->
              let struct(write,shape) = JsonObjectGeneration.Writer<'A>()
              setShape shape
              write

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
