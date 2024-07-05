namespace SoloDatabase

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


    // Function to create the lookup table
    let private createLookup32Unsafe () =
        let mem = NativeMemory.AllocZeroed(256 * sizeof<uint> |> unativeint) |> NativePtr.ofVoidPtr<uint>
        for i in 0..255 do
            let s = i.ToString("x2")
            let ch =
                if BitConverter.IsLittleEndian then
                    uint32 s.[0] + (uint32 s.[1] <<< 16)
                else
                    uint32 s.[1] + (uint32 s.[0] <<< 16)

            NativePtr.set mem i ch

        mem

    // Declare the lookup array and the pinned pointer
    let private lookup32Unsafe = createLookup32Unsafe()

    // Function to convert byte array to hex string using the lookup table
    let private byteArrayToHexViaLookup32Unsafe (bytes: byte[]) =
        let lookup = lookup32Unsafe
        let result = new string((char)0, bytes.Length * 2)
        use resultP = fixed result
        let resultPI = resultP |> NativePtr.toVoidPtr |> NativePtr.ofVoidPtr<uint>
        for i in 0..(bytes.Length - 1) do
            let b = bytes.[i]
            let v = NativePtr.get lookup (int b)

            NativePtr.set resultPI i v

        result

    let hashBytesToStr (hash: byte array) =
        byteArrayToHexViaLookup32Unsafe hash


    let mutable debug =
        #if DEBUG
        true
        #else
        false
        #endif