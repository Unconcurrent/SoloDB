namespace SoloDatabase.JsonSerializator

open System
open System.Text
open System.Reflection
open System.Reflection.Emit
open System.Runtime.CompilerServices
open SoloDatabase
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Numerics
open SoloDatabase.Metadata.Generation

/// Cold operand mapping for the shared admitted-output instruction macros.
module internal JsonWriterRuntime =
    let private flags = BindingFlags.Static ||| BindingFlags.NonPublic

    let private signature (element: Type) =
        let signature = typeof<JsonDirectWriter<obj,byte>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters()
        signature[1..2] |> Array.map(fun parameter ->
            parameter.ParameterType.GetElementType().GetGenericTypeDefinition().MakeGenericType(element).MakeByRefType())

    let internal api (element: Type) =
        let helper name = typeof<JsonWriterRegion>.GetMethod(name,flags).MakeGenericMethod(element) :> MethodBase
        let state = typedefof<JsonWriteState<_>>.MakeGenericType(element)
        let list = typedefof<System.Collections.Generic.IList<_>>.MakeGenericType(element)
        let concrete = typedefof<System.Collections.Generic.List<_>>.MakeGenericType(element)
        let collection = typedefof<System.Collections.Generic.ICollection<_>>.MakeGenericType(element)
        let field name = state.GetField(name,BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
        let offset = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                     |> Array.find(fun method -> method.Name = "Add" && method.IsGenericMethodDefinition &&
                                                 method.GetParameters().Length = 2 && method.GetParameters().[0].ParameterType.IsByRef &&
                                                 method.GetParameters().[1].ParameterType.Equals(typeof<unativeint>))
        { Int32 = typeof<int>; UInt32 = typeof<uint32>; UInt64 = typeof<uint64>; Reference = typeof<byte>.MakeByRefType()
          List = list; ConcreteList = concrete; ListField = field "List@"; Written = field "Written"; Discard = field "Discard"
          Add = collection.GetMethod("Add") :> MethodBase; ConcreteAdd = concrete.GetMethod("Add") :> MethodBase
          ReserveSpan = helper "ReserveSpan"; ReserveList = helper "ReserveList"; FitsSpan = helper "FitsSpan"
          Start = helper "Start"; Position = helper "Position"; Advance = helper "Advance"
          Offset = offset.MakeGenericMethod(typeof<byte>) :> MethodBase }

    let private digits (element: Type) output =
        { Output = output
          DigitPairs = typeof<JsonWriterRegion>.GetMethod("DigitPairReference",flags).MakeGenericMethod(element) :> MethodBase
          ReadPair = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                     |> Array.find(fun method -> method.Name="ReadUnaligned" && method.IsGenericMethodDefinition &&
                                                 method.GetParameters().Length=1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
                     |> fun method -> method.MakeGenericMethod(if element.Equals(typeof<char>) then typeof<uint32> else typeof<uint16>) :> MethodBase
          WritePair = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                      |> Array.find(fun method -> method.Name="WriteUnaligned" && method.IsGenericMethodDefinition &&
                                                  method.GetParameters().Length=2 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
                      |> fun method -> method.MakeGenericMethod(if element.Equals(typeof<char>) then typeof<uint32> else typeof<uint16>) :> MethodBase }

    let private createProperty (element: Type) (text: string) =
        let utf16 = typeof<char>.Equals(element)
        let units = if utf16 then text |> Seq.map int |> Seq.toArray else Encoding.UTF8.GetBytes(text) |> Array.map int
        let method = DynamicMethod("WriteProperty",typeof<Void>,Array.append [|typeof<bool>|] (signature element),typeof<JsonWriterRegion>.Module,true)
        CodecOutputEmitter.property (RuntimeCodecIL(method.GetILGenerator())) (api element) utf16 units
        method

    let private createConstant (element: Type) (text: string) =
        let utf16 = typeof<char>.Equals(element)
        let units = if utf16 then text |> Seq.map int |> Seq.toArray else Encoding.UTF8.GetBytes(text) |> Array.map int
        let method = DynamicMethod("WriteLiteral",typeof<Void>,signature element,typeof<JsonWriterRegion>.Module,true)
        CodecOutputEmitter.constant (RuntimeCodecIL(method.GetILGenerator())) (api element) utf16 units
        method

    let private createBoolean (element: Type) =
        let method = DynamicMethod("WriteBoolean",typeof<Void>,Array.append [|typeof<bool>|] (signature element),typeof<JsonWriterRegion>.Module,true)
        CodecOutputEmitter.boolean (RuntimeCodecIL(method.GetILGenerator())) (api element) (typeof<char>.Equals(element))
        method

    let private createTypeName target (element: Type) =
        let method = DynamicMethod("WriteTypeLiteral",typeof<Void>,signature element,typeof<JsonWriterRegion>.Module,true)
        let value = typeof<JsonWriterRegion>.GetMethod("TypeNameLiteral",flags).MakeGenericMethod(target,element)
        let reference = typeof<JsonWriterRegion>.GetMethod("LiteralReference",flags).MakeGenericMethod(element)
        CodecOutputEmitter.encodedLiteral (RuntimeCodecIL(method.GetILGenerator())) (api element)
            (element.MakeArrayType()) (value :> MethodBase) (reference :> MethodBase) (element.Equals(typeof<char>))
        method

    let private createNumber (target: Type) element =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" target
        let coreName = (TypeSupport.NumericWriterCore target.FullName).Value
        let core = if coreName = target.FullName then target else typeof<int>.Assembly.GetType(coreName,true)
        let shape = (TypeSupport.Numeric core.FullName).Value
        let method = DynamicMethod("WriteNumber" + target.Name,typeof<Void>,Array.append [|target|] (signature element),typeof<JsonWriterRegion>.Module,true)
        let il = RuntimeCodecIL(method.GetILGenerator())
        let output = api element
        let steps = TypeSupport.NumericWriterSteps target.FullName |> Array.map(fun (owner,name,address,result) ->
            let owner = if owner = target.FullName then target else typeof<int>.Assembly.GetType(owner,true)
            let result = typeof<int>.Assembly.GetType(result,true)
            let call = owner.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.Instance)
                       |> Array.find(fun method -> method.Name = name && method.ReturnType.Equals(result) &&
                                                   method.IsStatic <> address && method.GetParameters().Length = (if address then 0 else 1))
            { Result = result; Call = call :> MethodBase; Address = address })
        let value,address =
            if shape.Kind = NumericKind.ArbitraryInteger then (fun () -> ()),(fun () -> ())
            else CodecScalarWriterEmitter.prepare il target core steps (TypeSupport.NumericWriterCast target.FullName)
        match shape.Kind with
        | NumericKind.ArbitraryInteger ->
            let prepared = typeof<SoloDatabase.Numerics.BigIntegerRadixDigits>
            let memberFlags = BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic
            let facts = { Output = output; Prepared = prepared; UInt32Array = typeof<uint32[]>
                          Prepare = typeof<SoloDatabase.Numerics.BigIntegerRadix>.GetMethod("Raw",flags) :> MethodBase
                          Sign = prepared.GetProperty("Sign",memberFlags).GetMethod :> MethodBase
                          Count = prepared.GetProperty("Count",memberFlags).GetMethod :> MethodBase
                          Chunks = prepared.GetProperty("Chunks",memberFlags).GetMethod :> MethodBase }
            CodecBigIntegerWriterEmitter.writer il facts (typeof<char>.Equals(element))
        | NumericKind.Integer ->
            let width = if shape.Native then IntPtr.Size * 8 else shape.Width
            CodecIntegerWriterEmitter.writerFrom il (digits element output) width shape.Signed (typeof<char>.Equals(element)) value
        | NumericKind.Decimal ->
            let unaligned = typeof<Unsafe>.GetMethods(BindingFlags.Static ||| BindingFlags.Public)
                            |> Array.find(fun method -> method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                                                        method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
            let facts = { Digits = digits element output; Source = typeof<JsonWriterRegion>.GetMethod("DecimalSource",flags) :> MethodBase
                          Read32 = unaligned.MakeGenericMethod(typeof<uint32>) :> MethodBase; Read64 = unaligned.MakeGenericMethod(typeof<uint64>) :> MethodBase }
            CodecDecimalWriterEmitter.writerFrom il facts (typeof<char>.Equals(element)) address
        | NumericKind.Binary ->
            let methods = typeof<Unsafe>.GetMethods(BindingFlags.Static ||| BindingFlags.Public)
            let unaligned =
                methods |> Array.find(fun method ->
                    method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                    method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
            let wide = shape.Width = 64
            let prepared = typeof<BinaryDecimalCoefficient>
            let facts = { Digits = digits element output; Prepared = prepared
                          Prepare = typeof<BinaryDecimal>.GetMethod("Coefficient",flags) :> MethodBase
                          Coefficient = prepared.GetProperty("Value",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetMethod :> MethodBase
                          Exponent = prepared.GetProperty("Exponent",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetMethod :> MethodBase
                          Source = typeof<JsonWriterRegion>.GetMethod("Source",flags).MakeGenericMethod(target) :> MethodBase
                          Bits = unaligned.MakeGenericMethod(if wide then typeof<uint64> elif shape.Width = 16 then typeof<uint16> else typeof<uint32>) :> MethodBase }
            CodecFloatingWriterEmitter.writer il facts shape.Width (typeof<char>.Equals(element))
        | _ -> invalidArg "target" "Unsupported numeric writer role."

        method

    let private createGuid element =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" typeof<Guid>
        let method = DynamicMethod("WriteGuid",typeof<Void>,Array.append [|typeof<Guid>|] (signature element),typeof<JsonWriterRegion>.Module,true)
        let unaligned = typeof<Unsafe>.GetMethods(BindingFlags.Static ||| BindingFlags.Public)
                        |> Array.find(fun method -> method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                                                    method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
        let facts = { Output = api element
                      ReserveKnownSpan = typeof<JsonWriterRegion>.GetMethod("TryReserveKnownSpan",flags).MakeGenericMethod(element) :> MethodBase
                      Source = typeof<JsonWriterRegion>.GetMethod("Source",flags).MakeGenericMethod(typeof<Guid>) :> MethodBase
                      Read32 = unaligned.MakeGenericMethod(typeof<uint32>) :> MethodBase
                      Read16 = unaligned.MakeGenericMethod(typeof<uint16>) :> MethodBase }
        CodecGuidWriterEmitter.writer (RuntimeCodecIL(method.GetILGenerator())) facts (typeof<char>.Equals(element))
        method

    let private createString element =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" typeof<string>
        let method = DynamicMethod("WriteString",typeof<Void>,Array.append [|typeof<string>|] (signature element),typeof<JsonWriterRegion>.Module,true)
        let read = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                   |> Array.find(fun method -> method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                                               method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
        let facts = { Output = api element
                      Reference = typeof<JsonWriterRegion>.GetMethod("StringReference",flags) :> MethodBase
                      Length = typeof<string>.GetProperty("Length").GetMethod :> MethodBase
                      ReadWord = read.MakeGenericMethod(typeof<uint64>) :> MethodBase
                      WriteWord = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                                  |> Array.find(fun method -> method.Name = "WriteUnaligned" && method.IsGenericMethodDefinition &&
                                                              method.GetParameters().Length = 2 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
                                  |> fun method -> method.MakeGenericMethod(typeof<uint32>) :> MethodBase }
        CodecStringWriterEmitter.writer (RuntimeCodecIL(method.GetILGenerator())) facts (element.Equals(typeof<char>))
        method

    let private base64Vector (write: MethodInfo) utf16: CodecBase64VectorApi<Type,MethodBase> option =
#if NET6_0_OR_GREATER
        let read = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                   |> Array.find(fun method -> method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                                               method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
        let vector = typeof<System.Runtime.Intrinsics.Vector128>
        let bytes = typeof<System.Runtime.Intrinsics.Vector128<byte>>
        let unsigned = typeof<System.Runtime.Intrinsics.Vector128<uint32>>
        let signedBytes = typeof<System.Runtime.Intrinsics.Vector128<sbyte>>
        let sse = typeof<System.Runtime.Intrinsics.X86.Sse2>
        let ssse = typeof<System.Runtime.Intrinsics.X86.Ssse3>
        let operation (owner: Type) (name: string) (types: Type[]) = owner.GetMethod(name,types) :> MethodBase
        let reinterpret = vector.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                          |> Array.find(fun method -> method.Name = "As" && method.GetGenericArguments().Length = 2)
        let cast input result = reinterpret.MakeGenericMethod(input,result) :> MethodBase
        let binary name typ = operation sse name [|typ;typ|]
        Some { Bytes = bytes; Unsigned = unsigned
               Supported = ssse.GetProperty("IsSupported").GetMethod :> MethodBase
               CreateWord = operation vector "Create" [|typeof<uint32>|]
               CreateWords = operation vector "Create" [|typeof<uint32>;typeof<uint32>;typeof<uint32>;typeof<uint32>|]
               CreateByte = operation vector "Create" [|typeof<byte>|]
               WordToBytes = cast typeof<uint32> typeof<byte>
               BytesToUnsigned = cast typeof<byte> typeof<uint32>; UnsignedToBytes = cast typeof<uint32> typeof<byte>
               BytesToSignedBytes = cast typeof<byte> typeof<sbyte>; SignedBytesToBytes = cast typeof<sbyte> typeof<byte>
               Read = read.MakeGenericMethod(bytes) :> MethodBase; Write = write.MakeGenericMethod(bytes) :> MethodBase
               Shuffle = operation ssse "Shuffle" [|bytes;bytes|]
               And = binary "And" bytes; Or = binary "Or" bytes
               Right = operation sse "ShiftRightLogical" [|unsigned;typeof<byte>|]
               Left = operation sse "ShiftLeftLogical" [|unsigned;typeof<byte>|]
               GreaterThan = binary "CompareGreaterThan" signedBytes; Add = binary "Add" bytes
               Widening = if utf16 then Some { UnpackLow = binary "UnpackLow" bytes; UnpackHigh = binary "UnpackHigh" bytes } else None }
#else
        None
#endif

    let private createBytes element ordinarySequence =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" typeof<byte[]>
        let method = DynamicMethod("WriteBytes",typeof<Void>,Array.append [|typeof<byte[]>|] (signature element),typeof<JsonWriterRegion>.Module,true)
        let write = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                    |> Array.find(fun method -> method.Name = "WriteUnaligned" && method.IsGenericMethodDefinition &&
                                                method.GetParameters().Length = 2 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
        let facts = { Output = api element; Bytes = typeof<byte[]>
                      ReserveKnownSpan = if ordinarySequence then None else Some(typeof<JsonWriterRegion>.GetMethod("TryReserveKnownSpan",flags).MakeGenericMethod(element) :> MethodBase)
                      LengthOverflow = if ordinarySequence then Some(typeof<JsonWriterRegion>.GetMethod("Base64LengthOverflow",flags) :> MethodBase) else None
                      Capture = typeof<JsonWriterRegion>.GetMethod("CaptureBytes" + (if element.Equals(typeof<char>) then "Utf16" else "Utf8"),flags) :> MethodBase
                      Reference = typeof<JsonWriterRegion>.GetMethod("BytesReference",flags) :> MethodBase
                      Alphabet = typeof<JsonWriterRegion>.GetMethod("Base64Reference",flags) :> MethodBase
                      Vector = base64Vector write (element.Equals(typeof<char>))
                      LittleEndian = typeof<BitConverter>.GetField("IsLittleEndian")
                      WriteWord = write.MakeGenericMethod(if element.Equals(typeof<char>) then typeof<uint64> else typeof<uint32>) :> MethodBase }
        CodecBase64WriterEmitter.writer (RuntimeCodecIL(method.GetILGenerator())) facts (typeof<char>.Equals(element)) ordinarySequence
        method

    let property (element: Type) (text: string) =
        JsonDefaultMethods.get typeof<JsonWriterRegion> element ("property-write:" + text) (fun () -> createProperty element text)

    let constant (element: Type) (text: string) =
        JsonDefaultMethods.get typeof<JsonWriterRegion> element ("literal-write:" + text) (fun () -> createConstant element text)

    let boolean (element: Type) =
        JsonDefaultMethods.get typeof<bool> element "boolean-write" (fun () -> createBoolean element)

    let number (target: Type) element =
        JsonDefaultMethods.get target element "numeric-write" (fun () -> createNumber target element)

    let guid element =
        JsonDefaultMethods.get typeof<Guid> element "guid-write" (fun () -> createGuid element)

    let text element =
        JsonDefaultMethods.get typeof<string> element "string-write" (fun () -> createString element)

    let typeName target element =
        JsonDefaultMethods.get target element "type-literal" (fun () -> createTypeName target element)

    let bytes element =
        JsonDefaultMethods.get typeof<byte[]> element "bytes-write" (fun () -> createBytes element false)

    let sequenceBytes<'E>() =
        JsonDefaultMethods.callable<JsonDirectWriter<byte[],'E>> typeof<byte[]> typeof<'E> typeof<'E> "sequence-bytes-write"
            (fun () -> createBytes typeof<'E> true :> MethodBase)

    let writer<'T,'E>() =
        (number typeof<'T> typeof<'E>).CreateDelegate(typeof<JsonDirectWriter<'T,'E>>) :?> JsonDirectWriter<'T,'E>
