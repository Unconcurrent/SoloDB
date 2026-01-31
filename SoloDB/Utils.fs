namespace SoloDatabase

open System.Runtime.CompilerServices

[<assembly: InternalsVisibleTo("Tests")>]
[<assembly: InternalsVisibleTo("CSharpTests")>]
[<assembly: InternalsVisibleTo("BenchMaster")>]
do ()

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Security.Cryptography
open System.IO
open System.Text
open System.Runtime.ExceptionServices
open System.Runtime.InteropServices
open System.Buffers
open System.Reflection
open System.Threading
open System.Linq.Expressions


// FormatterServices.GetSafeUninitializedObject for 
// types without a parameterless constructor.
#nowarn "0044"

module Utils =
    let isNull x = Object.ReferenceEquals(x, null)

    /// Returns a deterministic variable name based on a int.
    let internal getVarName (l: int) =
        $"V{l}"

    // For the use in F# Builders, like task { .. }.
    // https://stackoverflow.com/a/72132958/9550932
    let inline reraiseAnywhere<'a> (e: exn) : 'a =
        ExceptionDispatchInfo.Capture(e).Throw()
        Unchecked.defaultof<'a>

    type internal QueryPlan =
        static member val ExplainQueryPlanReference = sprintf "This is a string reference that will be used to determine in which special mode Aggregate() method will be used...%i" 435
        static member val GetGeneratedSQLReference = sprintf "This is a string reference that will be used to determine in which special mode Aggregate() method will be used...%i" 436

    let internal isTuple (t: Type) =
        not t.IsArray &&
        (typeof<Tuple>.IsAssignableFrom t || typeof<ValueTuple>.IsAssignableFrom t || t.Name.StartsWith "Tuple`" || t.Name.StartsWith "ValueTuple`")

    type System.Char with
        static member IsAsciiLetterOrDigit this =
            (this >= '0' && this <= '9') ||
            (this >= 'A' && this <= 'Z') ||
            (this >= 'a' && this <= 'z')

        static member IsAsciiLetter this =
            (this >= 'A' && this <= 'Z') ||
            (this >= 'a' && this <= 'z')

    type System.Decimal with
        static member IsInteger (this: Decimal) =
            this = (Decimal.Floor this)

    [<return: Struct; MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let internal (|OfType|_|) (_typ: 'x -> 'a) (objType: Type) =
        if typeof<'a>.Equals objType || typeof<'a>.IsAssignableFrom(objType) then ValueSome () else ValueNone

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

    let inline internal isFloatBasedType (t: Type) =
        match t with
        | OfType float32
        | OfType float
        | OfType decimal
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

    let internal formatName (name: string) =
        let inline charOk (c: char) = Char.IsLetterOrDigit c || c = '_'
        let mutable allOk = true
        for i = 0 to name.Length - 1 do
            allOk <- allOk && charOk (name.[i])

        if allOk then 
            name
        else
            String(name.ToCharArray() |> Array.filter(fun c -> charOk c)) // Anti SQL injection

    let internal collectionNameOf<'T> =
        let formattedName = typeof<'T>.Name |> formatName
        fun () -> formattedName

    let internal collectionNameOfBytes<'T> =
        let formattedName = collectionNameOf<'T>() |> Encoding.UTF8.GetBytes
        fun () -> formattedName

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

    // LEGACY: File hashing was removed in schema version 2.
    // These functions remain for backward compatibility with v1 databases during migration.
    // The SHA_HASH SQLite function in SoloDB.fs uses shaHash.
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


    // State machine states
    type internal State =
        | Valid              // Currently valid base64 sequence
        | Invalid            // Invalid sequence
        | PaddingOne         // Seen one padding character
        | PaddingTwo         // Seen two padding characters (max allowed)

    /// <summary>
    /// Finds the last index of valid base64 content in a string using a state machine approach
    /// </summary>
    /// <param name="input">String to check for base64 content</param>
    /// <returns>Index of the last valid base64 character, or -1 if no valid base64 found</returns>
    let internal findLastValidBase64Index (input: string) =
        if System.String.IsNullOrEmpty(input) then
            -1
        else
            // Base64 alphabet check - optimized with lookup array
            let isBase64Char c =
                let code = c
                (code >= 'A' && code <= 'Z') ||
                (code >= 'a' && code <= 'z') ||
                (code >= '0' && code <= '9') ||
                c = '+' || c = '/' || c = '='
            
            
            // Track the last valid position
            let mutable lastValidPos = -1
            // Current position in the quadruplet (base64 works in groups of 4)
            let mutable quadPos = 0
            // Current state
            let mutable state = State.Valid
                
            for i = 0 to input.Length - 1 do
                let c = input.[i]
                    
                match state with
                | State.Valid ->
                    if not (isBase64Char c) then
                        // Non-base64 character found
                        state <- State.Invalid
                    elif c = '=' then
                        // Padding can only appear at positions 2 or 3 in a quadruplet
                        if quadPos = 2 then
                            state <- State.PaddingOne
                            lastValidPos <- i
                        elif quadPos = 3 then
                            state <- State.PaddingTwo
                            lastValidPos <- i
                        else
                            state <- State.Invalid
                    else
                        lastValidPos <- i
                        quadPos <- (quadPos + 1) % 4
                    
                | State.PaddingOne ->
                    if c = '=' && quadPos = 3 then
                        // Second padding character is only valid at position 3
                        state <- State.PaddingTwo
                        lastValidPos <- i
                        quadPos <- 0  // Reset for next quadruplet
                    else
                        state <- State.Invalid
                    
                | State.PaddingTwo ->
                    // After two padding characters, we should start a new quadruplet
                    if isBase64Char c && c <> '=' then
                        state <- State.Valid
                        quadPos <- 1  // Position 0 is this character
                        lastValidPos <- i
                    else
                        state <- State.Invalid
                    
                | State.Invalid ->
                    // Once invalid, check if we can start a new valid sequence
                    if isBase64Char c && c <> '=' then
                        state <- State.Valid
                        quadPos <- 1  // Position 0 is this character
                        lastValidPos <- i
                
            // Final validation: for a complete valid base64 string, we need quadPos = 0
            // or a valid padding situation at the end
            match state with
            | State.Valid when quadPos = 0 -> lastValidPos
            | State.PaddingOne | State.PaddingTwo -> lastValidPos
            | _ -> 
                // If we don't end with complete quadruplet, find the last complete one
                if lastValidPos >= 0 then
                    let remainingChars = (lastValidPos + 1) % 4
                    if remainingChars = 0 then
                        lastValidPos
                    else
                        lastValidPos - remainingChars + 1
                else
                    -1
            
    let internal trimToValidBase64 (input: string) =
        if System.String.IsNullOrEmpty(input) then
            input
        else
            let lastValidIndex = findLastValidBase64Index input
            
            if lastValidIndex >= 0 then
                if lastValidIndex = input.Length - 1 then
                    // Already valid, no need to create a new string
                    input
                else
                    // Extract only the valid part
                    input.Substring(0, lastValidIndex + 1)
            else
                // No valid base64 found
                ""

    let internal sqlBase64 (data: obj) =
        match data with
        | null -> null
        | :? (byte array) as bytes -> 
            // Requirement #2: If BLOB, encode to base64 TEXT
            System.Convert.ToBase64String(bytes) :> obj
        | :? string as str -> 
            // Requirement #6: Ignore leading and trailing whitespace
            let trimmedStr = str.Trim()
            let trimmedStr = trimToValidBase64 trimmedStr

            match trimmedStr.Length with
            | 0 ->  Array.empty<byte> :> obj
            | _ ->
            
            System.Convert.FromBase64String trimmedStr :> obj
        | _ -> 
            // Requirement #5: Raise an error for types other than TEXT, BLOB, or NULL
            failwith "The base64() function requires a TEXT, BLOB, or NULL argument"

    [<AbstractClass; Sealed>]
    type internal GenericMethodArgCache =
        static member val private cache = ConcurrentDictionary<MethodInfo, Type array>({
                new IEqualityComparer<MethodInfo> with
                    override this.Equals (x: MethodInfo, y: MethodInfo): bool =
                        x.MethodHandle.Value = y.MethodHandle.Value
                    override this.GetHashCode (obj: MethodInfo): int =
                        obj.MethodHandle.Value |> int
        })

        static member Get(method: MethodInfo) =
            let args = GenericMethodArgCache.cache.GetOrAdd(method, (fun m -> m.GetGenericArguments()))
            args

    [<AbstractClass; Sealed>]
    type internal GenericTypeArgCache =
        static member val private cache = ConcurrentDictionary<Type, Type array>({
                new IEqualityComparer<Type> with
                    override this.Equals (x: Type, y: Type): bool =
                        x.TypeHandle.Value = y.TypeHandle.Value
                    override this.GetHashCode (obj: Type): int =
                        obj.TypeHandle.Value |> int
        })

        static member Get(t: Type) =
            let args = GenericTypeArgCache.cache.GetOrAdd(t, (fun m -> m.GetGenericArguments()))
            args

    /// For the F# compiler to allow the implicit use of
    /// the .NET Expression we need to use it in a C# style class.
    [<Sealed>][<AbstractClass>] // make it static
    type internal ExpressionHelper =
        static member inline internal get<'a, 'b>(expression: Expression<System.Func<'a, 'b>>) = expression

        static member inline internal id (x: Type) =
            let parameter = Expression.Parameter x
            Expression.Lambda(parameter, [|parameter|])

        static member inline internal eq (x: Type) (b: obj) =
            let parameter = Expression.Parameter x
            Expression.Lambda(Expression.Equal(parameter, Expression.Constant(b, x)), [|parameter|])

        static member inline internal min (e1: Expression) (e2: Expression) =
            // Produces an expression tree representing min(e1, e2) using LessThan
            Expression.Condition(
                Expression.LessThan(e1, e2),
                e1,
                e2
            )

        static member inline internal constant<'T> (x: 'T) =
            // Returns a strongly-typed constant expression for value x
            Expression.Constant(x, typeof<'T>)

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


    type ReentrantSpinLock() =
        let mutable ownerTid = 0
        let mutable recursion = 0

        member _.Enter() =
            let tid = Thread.CurrentThread.ManagedThreadId
            if Volatile.Read(&ownerTid) = tid then
                recursion <- recursion + 1
            else
                // acquire
                while Interlocked.CompareExchange(&ownerTid, tid, 0) <> 0 do
                    if not (Thread.Yield()) then Thread.SpinWait(20)
                recursion <- 1

        member _.Exit() =
            let tid = Thread.CurrentThread.ManagedThreadId
            if Volatile.Read(&ownerTid) <> tid then
                invalidOp "Exit from non-owner thread"
            recursion <- recursion - 1
            if recursion = 0 then
                Volatile.Write(&ownerTid, 0)
