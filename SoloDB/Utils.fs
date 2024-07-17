namespace SoloDatabase

open System.Runtime.CompilerServices
open System

#nowarn "9" // Unsafe functions.

module Utils =
    open System
    open System.Collections.Concurrent
    open System.Reflection
    open SoloDatabase.Types
    open System.Security.Cryptography
    open System.IO
    open System.Text
    open System.Runtime.InteropServices
    open Microsoft.FSharp.NativeInterop
    open System.Runtime.ExceptionServices
    
    // For the use in F# Builders, like task { .. }.
    // https://stackoverflow.com/a/72132958/9550932
    let inline reraiseAnywhere<'a> (e: exn) : 'a =
        ExceptionDispatchInfo.Capture(e).Throw()
        Unchecked.defaultof<'a>

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
        | :? SqlId
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
        | _ when t = typeof<SqlId> -> true
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
                                            let fastType = Type.GetType(typeName)
                                            if fastType <> null then fastType
                                            else AppDomain.CurrentDomain.GetAssemblies() 
                                                    |> Seq.collect(fun a -> a.GetTypes()) 
                                                    |> Seq.find(fun t -> t.FullName = typeName)
                                            )    
    let shaHash (o: obj) = 
        match o with
        | :? (byte array) as bytes -> 
            SHA1.HashData(bytes)
        | :? string as str -> 
            SHA1.HashData(str |> Encoding.UTF8.GetBytes)
        | other -> raise (InvalidDataException(sprintf "Cannot hash object of type: %A" (other.GetType())))


    // Function to create the lookup table.
    let private createLookup32Unsafe () =
        let memPtr = NativeMemory.AllocZeroed(256 * sizeof<uint> |> unativeint)
        let mem = memPtr |> NativePtr.ofVoidPtr<uint>
        for i in 0..255 do
            let s = i.ToString("x2")
            let ch =
                if BitConverter.IsLittleEndian then
                    uint32 s.[0] + (uint32 s.[1] <<< 16)
                else
                    uint32 s.[1] + (uint32 s.[0] <<< 16)

            NativePtr.set mem i ch

        memPtr

    // Declare the lookup array.
    let private lookup32Unsafe = createLookup32Unsafe()
    let private getLookup() =
        Span<uint>(lookup32Unsafe, 256)

    // Function to convert byte array to hex string using the lookup table.
    let private byteArrayToHexViaLookupSafer (bytes: byte array) =
        let resultI: Span<uint> =
            if bytes.Length > 256 then
                Span<uint>(Array.zeroCreate<uint> (bytes.Length))
            else
                Span<uint>(NativePtr.toVoidPtr (NativePtr.stackalloc<uint> (bytes.Length)) , bytes.Length)

        let lookup = getLookup()

        for i in 0..(bytes.Length - 1) do
            let b = bytes.[i]
            let v = lookup.[(int b)]

            resultI[i] <- v

        let result = MemoryMarshal.Cast<uint, char>(resultI)

        new string(result)

    let bytesToHexFast (hash: byte array) =
        byteArrayToHexViaLookupSafer hash


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