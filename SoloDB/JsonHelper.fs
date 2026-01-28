namespace SoloDatabase.JsonSerializator

open System.Collections.Concurrent
open System
open System.Collections
open System.Reflection
open System.Linq
open System.Collections.Generic
open System.Runtime.CompilerServices

#nowarn "9"

/// <summary>
/// Contains helper functions for JSON serialization and deserialization.
/// </summary>
module internal JsonHelper =
    let mutable private useFastDefaultDict = false

    let internal setUseFastDefaultDict (value: bool) =
        useFastDefaultDict <- value

    let internal getUseFastDefaultDict () = useFastDefaultDict

    let internal newDefaultDict<'Value>() : IDictionary<string, 'Value> =
        if useFastDefaultDict then
            SoloDatabase.FastStringDictionary.FastDict<'Value>(0) :> IDictionary<string, 'Value>
        else
            Dictionary<string, 'Value>() :> IDictionary<string, 'Value>

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


    let compareObjectsCoreArray<'J>
        (n: int,
         a: KeyValuePair<string,'J> array,
         b: KeyValuePair<string,'J> array,
         valueCompare: 'J -> 'J -> int)
        : int =

        let mutable idx = 0
        let mutable res = 0
        while res = 0 && idx < n do
            let aa = a[idx]
            let bb = b[idx]
            let kc = StringComparer.Ordinal.Compare(aa.Key, bb.Key)
            if kc <> 0 then res <- kc
            else
                let vc = valueCompare aa.Value bb.Value
                if vc <> 0 then res <- vc
            idx <- idx + 1
        res

    let compareObjectsCoreStack128<'J>
        (n: int,
         a: byref<SoloDatabase.InternalStack128<KeyValuePair<string,'J>>>,
         b: byref<SoloDatabase.InternalStack128<KeyValuePair<string,'J>>>,
         valueCompare: 'J -> 'J -> int)
        : int =

        let mutable idx = 0
        let mutable res = 0
        while res = 0 && idx < n do
            let ra = SoloDatabase.InternalStack128.GetRef(&a, idx)
            let rb = SoloDatabase.InternalStack128.GetRef(&b, idx)
            let kc = StringComparer.Ordinal.Compare(ra.Key, rb.Key)
            if kc <> 0 then res <- kc
            else
                let vc = valueCompare ra.Value rb.Value
                if vc <> 0 then res <- vc
            idx <- idx + 1
        res

type internal KvpComparison<'J> =
    static member val KvpKeyCmp = Comparison<KeyValuePair<string, 'J>>(fun x y -> StringComparer.Ordinal.Compare(x.Key, y.Key))

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
