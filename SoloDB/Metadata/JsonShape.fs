namespace SoloDatabase.JsonSerializator

open SoloDatabase
open System
open System.Collections
open System.Reflection
open System.Linq
open System.Collections.Generic
open System.Runtime.CompilerServices

// NativePtr operations for low-level JSON buffer manipulation
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
        Dictionary<string, 'Value>(16) :> IDictionary<string, 'Value>

    /// <summary>
    /// Checks if a given type is an array type supported for Newtownsoft-style serialization (e.g., arrays of primitives, strings, DateTime).
    /// </summary>
    /// <param name="t">The type to check.</param>
    /// <returns>True if the type is a supported array type, false otherwise.</returns>
    let internal isSupportedNewtownsoftArrayType (t: Type) =
        t.IsArray && SoloDatabase.Metadata.Generation.TypeSupport.SupportsValuesWrapper(t.GetElementType().FullName)

    /// <summary>
    /// A cache for the results of the implementsGeneric function.
    /// </summary>
    
    /// <summary>
    /// Checks if a target type implements a specific generic type definition.
    /// </summary>
    /// <param name="genericTypeDefinition">The generic type definition to check for (e.g., typeof&lt;IDictionary&lt;_,_&gt;&gt;).</param>
    /// <param name="targetType">The type to check.</param>
    /// <returns>True if the target type implements the generic type definition, false otherwise.</returns>
    let internal implementsGeneric (genericTypeDefinition: Type) (targetType: Type) =        
        TypeMetadataCache.Discover(targetType, (fun metadata -> MetadataMap.tryFind genericTypeDefinition metadata.GenericInterfaces),
            (fun operation metadata -> { metadata with GenericInterfaces = MetadataMap.set genericTypeDefinition operation metadata.GenericInterfaces }), Func<bool>(fun () ->
            targetType.IsGenericType && 
            targetType.GetGenericTypeDefinition().Equals(genericTypeDefinition) ||
            targetType.GetInterfaces() 
            |> Array.exists (fun interfaceType -> 
                interfaceType.IsGenericType && 
                interfaceType.GetGenericTypeDefinition().Equals(genericTypeDefinition))
        ))

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
        TypeMetadataCache.Discover(elementType, (fun metadata -> metadata.ListConstruction),
            (fun operation metadata -> { metadata with ListConstruction = ValueSome operation }), Func<obj seq -> obj>(fun () ->
                let method =
                    typedefof<FSharpListDeserialize>
                        .GetMethod(nameof FSharpListDeserialize.OfSeq, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(elementType)

                fun (contents) ->
                    method.Invoke(null, [|contents|])
            )
        )
