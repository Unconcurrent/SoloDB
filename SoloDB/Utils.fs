namespace SoloDatabase

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Security.Cryptography
open System.IO
open System.Text
open System.Runtime.ExceptionServices
open System.Security.Cryptography
open System.Runtime.InteropServices


// FormatterServices.GetSafeUninitializedObject for 
// types without a parameterless constructor.
#nowarn "0044"

module Utils =
    let isNull x = Object.ReferenceEquals(x, null)

    let private cryptoRandom = RandomNumberGenerator.Create()
    let mutable private variableNameCounter: uint32 = 0u
    let internal getRandomVarName () =
        variableNameCounter <- if variableNameCounter = UInt32.MaxValue then 0u else variableNameCounter + 1u
        let bytes = Array.zeroCreate<byte> (4 * sizeof<uint>)
        cryptoRandom.GetBytes bytes
        
        let randomUInts = MemoryMarshal.Cast<byte, uint>(Span<byte>(bytes))
        $"VAR{(randomUInts.[0] + variableNameCounter):X}{randomUInts.[1]:X}{randomUInts.[2]:X}{randomUInts.[3]:X}"
        

    // For the use in F# Builders, like task { .. }.
    // https://stackoverflow.com/a/72132958/9550932
    let inline reraiseAnywhere<'a> (e: exn) : 'a =
        ExceptionDispatchInfo.Capture(e).Throw()
        Unchecked.defaultof<'a>

    let private emptyObjContructor = ConcurrentDictionary<Type, unit -> obj>()

    let internal initEmpty t =
        emptyObjContructor.GetOrAdd(t, Func<Type, unit -> obj>(fun t -> 
            let constr = t.GetConstructors() |> Seq.tryFind(fun c -> c.GetParameters().Length = 0 && (c.IsPublic || c.IsPrivate))
            match constr with
            | Some constr -> fun () -> constr.Invoke Array.empty
            | None -> fun () -> System.Runtime.Serialization.FormatterServices.GetSafeUninitializedObject t
        ))()

    let internal isTuple (t: Type) =
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

    [<return: Struct>]
    let internal (|OfType|_|) (_typ: 'x -> 'a) (objType: Type) =
        if typeof<'a>.IsAssignableFrom(objType) then ValueSome () else ValueNone

    let internal isNumber (value: obj) =
        match value with
        | :? int8
        | :? uint8
        | :? int16
        | :? uint16
        | :? int32
        | :? uint32
        | :? int64
        | :? uint64
        | :? nativeint

        | :? float32
        | :? float
        | :? decimal -> true
        | _ -> false

    let internal isIntegerBasedType (t: Type) =
        match t with
        | OfType int8
        | OfType uint8
        | OfType int16
        | OfType uint16
        | OfType int32 
        | OfType uint32
        | OfType int64
        | OfType uint64
        | OfType nativeint  
                        -> true
        | _ -> false

    let internal isIntegerBased (value: obj) =
        match value with
        | :? int8
        | :? uint8
        | :? int16
        | :? uint16
        | :? int32
        | :? uint32
        | :? int64
        | :? uint64
        | :? nativeint
            -> true
        | _other -> false

    let internal typeToName (t: Type) = 
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

    let internal shaHash (o: obj) = 
        match o with
        | :? (byte array) as bytes -> 
            shaHashBytes(bytes)
        | :? string as str -> 
            shaHashBytes(str |> Encoding.UTF8.GetBytes)
        | other -> raise (InvalidDataException(sprintf "Cannot hash object of type: %A" (other.GetType())))

    let internal bytesToHex (hash: byte array) =
        let sb = new StringBuilder()
        for b in hash do
            sb.Append (b.ToString("x2")) |> ignore

        sb.ToString()

    let mutable internal debug =
        #if DEBUG
        true
        #else
        false
        #endif

    type CompatilibilityList<'T> internal (elements: 'T seq) =
        inherit System.Collections.Generic.List<'T>(elements)
    
        ///<summary>For compatilibility, it just calls this.Count. Gets the number of elements contained in the <see cref="T:System.Collections.Generic.List`1" />.</summary>
        ///<returns>The number of elements contained in the <see cref="T:System.Collections.Generic.List`1" />.</returns>
        member this.Length = this.Count

    module internal SeqExt =
        let internal sequentialGroupBy keySelector (sequence: seq<'T>) =
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