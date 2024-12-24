namespace SoloDatabase

open System
open System.Collections.Generic

// FormatterServices.GetSafeUninitializedObject for 
// types without a parameterless constructor.
#nowarn "0044"

module Utils =
    open System.Collections.Concurrent
    open System.Security.Cryptography
    open System.IO
    open System.Text
    open System.Runtime.ExceptionServices
    
    let isNull x = Object.ReferenceEquals(x, null)

    // For the use in F# Builders, like task { .. }.
    // https://stackoverflow.com/a/72132958/9550932
    let inline reraiseAnywhere<'a> (e: exn) : 'a =
        ExceptionDispatchInfo.Capture(e).Throw()
        Unchecked.defaultof<'a>

    let private emptyObjContructor = ConcurrentDictionary<Type, unit -> obj>()

    let initEmpty t =
        emptyObjContructor.GetOrAdd(t, Func<Type, unit -> obj>(fun t -> 
            let constr = t.GetConstructors() |> Seq.tryFind(fun c -> c.GetParameters().Length = 0 && (c.IsPublic || c.IsPrivate))
            match constr with
            | Some constr -> fun () -> constr.Invoke([||])
            | None -> fun () -> System.Runtime.Serialization.FormatterServices.GetSafeUninitializedObject(t)
        ))()

    let isTuple (t: Type) =
        typeof<Tuple>.IsAssignableFrom t || typeof<ValueTuple>.IsAssignableFrom t || t.Name.StartsWith "Tuple`"

    type Random with
        static member Shared = Random()

    type System.Char with
        static member IsAsciiLetterOrDigit this =
            (this >= '0' && this <= '9') ||
            (this >= 'A' && this <= 'Z') ||
            (this >= 'a' && this <= 'z')

        static member IsAsciiLetter this =
            (this >= '0' && this <= '9') ||
            (this >= 'A' && this <= 'Z') ||
            (this >= 'a' && this <= 'z')

    type System.Decimal with
        static member IsInteger (this: Decimal) =
            this = (Decimal.Floor this)

    let isNumber (value: obj) =
        match value with
        | :? sbyte
        | :? byte
        | :? int16
        | :? uint16
        | :? int
        | :? uint32
        | :? int64
        | :? uint64
        | :? int64
        | :? float32
        | :? float
        | :? decimal -> true
        | _ -> false

    let isIntegerBasedType (t: Type) =
        match t with
        | _ when t = typeof<sbyte>  -> true
        | _ when t = typeof<byte>   -> true
        | _ when t = typeof<int16>  -> true
        | _ when t = typeof<uint16> -> true
        | _ when t = typeof<int32>  -> true
        | _ when t = typeof<uint32> -> true
        | _ when t = typeof<int64>  -> true
        | _ when t = typeof<uint64> -> true
        | _ when t = typeof<int64>  -> true
        | _ -> false

    let isIntegerBased (value: obj) =
        value.GetType() |> isIntegerBasedType

    let typeToName (t: Type) = 
        let fullname = t.FullName
        if fullname.Length > 0 && Char.IsAsciiLetter fullname.[0] // To not insert auto generated classes.
        then Some fullname
        else None

    let private nameToTypeCache = ConcurrentDictionary<string, Type>()

    let internal nameToType (typeName: string) = 
        nameToTypeCache.GetOrAdd(typeName, fun typeName -> 
                                            match typeName with
                                            | "Double" | "double" -> typeof<double>
                                            | "Single" | "float" -> typeof<float>
                                            | "Byte" | "byte" -> typeof<byte>
                                            | "SByte" | "sbyte" -> typeof<sbyte>
                                            | "Int16" | "short" -> typeof<Int16>
                                            | "UInt16" | "ushort" -> typeof<UInt16>
                                            | "Int32" | "int" -> typeof<int>
                                            | "UInt32" | "uint" -> typeof<uint>
                                            | "Int64" | "long" -> typeof<Int64>
                                            | "UInt64" | "ulong" -> typeof<UInt64>
                                            | "Char" | "char" -> typeof<char>
                                            | "Boolean" | "bool" -> typeof<bool>
                                            | "Object" | "object" -> typeof<obj>
                                            | "String" | "string" -> typeof<string>
                                            | "Decimal" | "decimal" -> typeof<decimal>
                                            | "DateTime" -> typeof<DateTime>
                                            | "Guid" -> typeof<Guid>
                                            | "TimeSpan" -> typeof<TimeSpan>
                                            | "IntPtr" -> typeof<IntPtr>
                                            | "UIntPtr" -> typeof<UIntPtr>
                                            | "Array" -> typeof<Array>
                                            | "Delegate" -> typeof<Delegate>
                                            | "MulticastDelegate" -> typeof<MulticastDelegate>
                                            | "IDisposable" -> typeof<System.IDisposable>
                                            | "Stream" -> typeof<System.IO.Stream>
                                            | "Exception" -> typeof<System.Exception>
                                            | "Thread" -> typeof<System.Threading.Thread>
                                            | typeName ->

                                            match Type.GetType(typeName) with
                                            | null -> 
                                                match Type.GetType("System." + typeName) with
                                                | null ->
                                                    AppDomain.CurrentDomain.GetAssemblies() 
                                                        |> Seq.collect(fun a -> a.GetTypes()) 
                                                        |> Seq.find(fun t -> t.FullName = typeName)
                                                | fastType -> fastType
                                            | fastType -> fastType
                                            )

    let private shaHashBytes (bytes: byte array) =
        use sha = SHA1.Create()
        sha.ComputeHash(bytes)

    let shaHash (o: obj) = 
        match o with
        | :? (byte array) as bytes -> 
            shaHashBytes(bytes)
        | :? string as str -> 
            shaHashBytes(str |> Encoding.UTF8.GetBytes)
        | other -> raise (InvalidDataException(sprintf "Cannot hash object of type: %A" (other.GetType())))

    let bytesToHex (hash: byte array) =
        let sb = new StringBuilder()
        for b in hash do
            sb.Append (b.ToString("x2")) |> ignore

        sb.ToString()

    [<return: Struct>]
    let internal (|OfType|_|) (_typ: 'x -> 'a) (objType: Type) =
        let name = typeof<'a>.Name
        let typ = nameToType name
        if typ.IsAssignableFrom(objType) then ValueSome () else ValueNone

    let mutable debug =
        #if DEBUG
        true
        #else
        false
        #endif

    type CompatilibilityList<'T>(elements: 'T seq) =
        inherit System.Collections.Generic.List<'T>(elements)
    
        ///<summary>For compatilibility, it just calls this.Count. Gets the number of elements contained in the <see cref="T:System.Collections.Generic.List`1" />.</summary>
        ///<returns>The number of elements contained in the <see cref="T:System.Collections.Generic.List`1" />.</returns>
        member this.Length = this.Count

    module SeqExt =
        let sequentialGroupBy keySelector (sequence: seq<'T>) =
            seq {
                use enumerator = sequence.GetEnumerator()
                if enumerator.MoveNext() then
                    let mutable currentKey = keySelector enumerator.Current
                    let mutable currentList = System.Collections.Generic.List<'T>()
                    let mutable looping = true
                    
                    while looping do
                        let current = enumerator.Current
                        let key = keySelector current
        
                        if key = currentKey then
                            currentList.Add current
                        else
                            yield currentList :> IList<'T>
                            currentList.Clear()
                            currentList.Add current
                            currentKey <- key
        
                        if not (enumerator.MoveNext()) then
                            yield currentList :> IList<'T>
                            looping <- false
            }