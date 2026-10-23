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

    let runtimeWriter<'T,'E> (writer: JsonDirectWriter<'T,'E>): JsonDirectRuntimeWriter<'E> =
        if not(MetadataRuntime.supportsDynamicCode()) then
            JsonDirectRuntimeWriter<'E>(fun value destination state ->
                let mutable value = unbox<'T> value
                writer.Invoke(&value,&destination,&state))
        else
            let callable = typeof<JsonDirectWriter<'T,'E>>
            let invoke = callable.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
            let parameters = typeof<JsonDirectRuntimeWriter<'E>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters() |> Array.map _.ParameterType
            let method = DynamicMethod("WriteBoxedJson",typeof<Void>,Array.append [|callable|] parameters,typeof<JsonWriterRegion>.Module,true)
            CodecEmitter.boxedRawWriter (RuntimeCodecIL(method.GetILGenerator())) typeof<'T> typeof<'T>.IsValueType invoke
            method.CreateDelegate(typeof<JsonDirectRuntimeWriter<'E>>,writer) :?> JsonDirectRuntimeWriter<'E>

    let private signature (element: Type) =
        let signature = typeof<JsonDirectWriter<obj,byte>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters()
        Array.init 2 (fun index ->
            signature[index + 1].ParameterType.GetElementType().GetGenericTypeDefinition().MakeGenericType(element).MakeByRefType())

    let internal api (element: Type) (output: Type) =
        let helper name = typeof<JsonWriterRegion>.GetMethod(name,flags).MakeGenericMethod(element) :> MethodBase
        let state = typedefof<JsonWriteState<_>>.MakeGenericType(element)
        let field name = state.GetField(name,BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
        let offset = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                     |> Array.find(fun method -> method.Name = "Add" && method.IsGenericMethodDefinition &&
                                                 method.GetParameters().Length = 2 && method.GetParameters().[0].ParameterType.IsByRef &&
                                                 method.GetParameters().[1].ParameterType.Equals(typeof<unativeint>))
        { Kind = CodecOutputEmitter.kind (if output.IsGenericType then output.GetGenericTypeDefinition().FullName else output.FullName)
          LittleEndian = typeof<BitConverter>.GetField("IsLittleEndian")
          Int32 = typeof<int>; UInt32 = typeof<uint32>; UInt64 = typeof<uint64>; Reference = typeof<byte>.MakeByRefType()
          Written = field "Written"; Add = helper "AddList"
          ReserveSpan = helper "ReserveSpan"; FitsSpan = helper "FitsSpan"
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

    let inlineLiteral element output (il: CodecIL<Type,MethodBase,FieldInfo>) (text: string) =
        let utf16 = typeof<char>.Equals(element)
        let units = if utf16 then text.ToCharArray() |> Array.map int else Encoding.UTF8.GetBytes(text) |> Array.map int
        CodecOutputEmitter.literal il (api element output) utf16 units
            (fun () -> il.Emit OpCodes.Ldarg_1) (fun () -> il.Emit OpCodes.Ldarg_2)

    let inlineProperty element output (il: CodecIL<Type,MethodBase,FieldInfo>) (text: string, comma: int) =
        let utf16 = typeof<char>.Equals(element)
        let units = if utf16 then text.ToCharArray() |> Array.map int else Encoding.UTF8.GetBytes(text) |> Array.map int
        CodecOutputEmitter.inlineProperty il (api element output) utf16 units comma

    let private createBoolean (element: Type) (output: Type) =
        let method = DynamicMethod("WriteBoolean",typeof<Void>,Array.append [|typeof<bool>.MakeByRefType()|] (signature element),typeof<JsonWriterRegion>.Module,true)
        CodecOutputEmitter.boolean (RuntimeCodecIL(method.GetILGenerator(),typeof<bool>.MakeByRefType())) (api element output) (typeof<char>.Equals(element))
        method

    let private createTypeName target (element: Type) (output: Type) =
        let method = DynamicMethod("WriteTypeLiteral",typeof<Void>,signature element,typeof<JsonWriterRegion>.Module,true)
        let value = typeof<JsonWriterRegion>.GetMethod("TypeNameLiteral",flags).MakeGenericMethod(target,element)
        let reference = typeof<JsonWriterRegion>.GetMethod("LiteralReference",flags).MakeGenericMethod(element)
        CodecOutputEmitter.encodedLiteral (RuntimeCodecIL(method.GetILGenerator())) (api element output)
            (element.MakeArrayType()) (value :> MethodBase) (reference :> MethodBase) (element.Equals(typeof<char>))
        method

    let private createNumber (target: Type) element output =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" target
        let plan = CodecNumericPreparation.writer target.FullName false (Some IntPtr.Size)
        let coreName = plan.Core
        let core = if coreName = target.FullName then target else typeof<int>.Assembly.GetType(coreName,true)
        let method = DynamicMethod("WriteNumber" + target.Name,typeof<Void>,Array.append [|target.MakeByRefType()|] (signature element),typeof<JsonWriterRegion>.Module,true)
        let il = RuntimeCodecIL(method.GetILGenerator(),target.MakeByRefType())
        let output = api element output
        let steps = plan.Steps |> Array.map(fun (owner,name,address,result) ->
            let owner = if owner = target.FullName then target else typeof<int>.Assembly.GetType(owner,true)
            let result = typeof<int>.Assembly.GetType(result,true)
            let call = owner.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.Instance)
                       |> Array.find(fun method -> method.Name = name && method.ReturnType.Equals(result) &&
                                                   method.IsStatic <> address && method.GetParameters().Length = (if address then 0 else 1))
            { Result = result; Call = call :> MethodBase; Address = address })
        let input =
            if plan.Family = NumericKind.ArbitraryInteger then (fun () -> ())
            else CodecScalarWriterEmitter.prepare il target core steps plan.Cast plan.AddressInput
        match plan.Family with
        | NumericKind.ArbitraryInteger ->
            let prepared = typeof<SoloDatabase.Numerics.BigIntegerRadixDigits>
            let memberFlags = BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic
            let facts = { Output = output; Prepared = prepared; UInt32Array = typeof<uint32[]>
                          Prepare = typeof<SoloDatabase.Numerics.BigIntegerRadix>.GetMethod("Raw",flags) :> MethodBase
                          Validate = typeof<SoloDatabase.Numerics.BigIntegerRadix>.GetMethod("ValidateRaw",flags) :> MethodBase
                          Sign = prepared.GetProperty("Sign",memberFlags).GetMethod :> MethodBase
                          Count = prepared.GetProperty("Count",memberFlags).GetMethod :> MethodBase
                          Chunks = prepared.GetProperty("Chunks",memberFlags).GetMethod :> MethodBase }
            CodecBigIntegerWriterEmitter.writer il facts (typeof<char>.Equals(element))
        | NumericKind.Integer ->
            let width = plan.Width
            CodecIntegerWriterEmitter.writerFrom il (digits element output) width plan.Signed (typeof<char>.Equals(element)) input
        | NumericKind.Decimal ->
            let unaligned = typeof<Unsafe>.GetMethods(BindingFlags.Static ||| BindingFlags.Public)
                            |> Array.find(fun method -> method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                                                        method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
            let facts = { Digits = digits element output; Source = typeof<JsonWriterRegion>.GetMethod("DecimalSource",flags) :> MethodBase
                          Read32 = unaligned.MakeGenericMethod(typeof<uint32>) :> MethodBase; Read64 = unaligned.MakeGenericMethod(typeof<uint64>) :> MethodBase }
            CodecDecimalWriterEmitter.writerFrom il facts (typeof<char>.Equals(element)) input
        | NumericKind.Binary ->
            let methods = typeof<Unsafe>.GetMethods(BindingFlags.Static ||| BindingFlags.Public)
            let unaligned =
                methods |> Array.find(fun method ->
                    method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                    method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
            let prepared = typeof<BinaryDecimalCoefficient>
            let facts = { Digits = digits element output; Prepared = prepared
                          Prepare = typeof<BinaryDecimal>.GetMethod("Coefficient",flags) :> MethodBase
                          Coefficient = prepared.GetProperty("Value",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetMethod :> MethodBase
                          Exponent = prepared.GetProperty("Exponent",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetMethod :> MethodBase
                          Source = typeof<JsonWriterRegion>.GetMethod("Source",flags).MakeGenericMethod(core) :> MethodBase
                          Bits = unaligned.MakeGenericMethod(typeof<int>.Assembly.GetType(plan.Bits,true)) :> MethodBase }
            CodecFloatingWriterEmitter.writerFrom il facts plan.Width (typeof<char>.Equals(element)) input
        | _ -> invalidArg "target" "Unsupported numeric writer role."

        method

    let private createGuid element output =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" typeof<Guid>
        let method = DynamicMethod("WriteGuid",typeof<Void>,Array.append [|typeof<Guid>.MakeByRefType()|] (signature element),typeof<JsonWriterRegion>.Module,true)
        let unaligned = typeof<Unsafe>.GetMethods(BindingFlags.Static ||| BindingFlags.Public)
                        |> Array.find(fun method -> method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                                                    method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
        let facts = { Output = api element output
                      ReserveKnownSpan = typeof<JsonWriterRegion>.GetMethod("TryReserveKnownSpan",flags).MakeGenericMethod(element) :> MethodBase
                      Source = typeof<JsonWriterRegion>.GetMethod("Source",flags).MakeGenericMethod(typeof<Guid>) :> MethodBase
                      Read32 = unaligned.MakeGenericMethod(typeof<uint32>) :> MethodBase
                      Read16 = unaligned.MakeGenericMethod(typeof<uint16>) :> MethodBase }
        CodecGuidWriterEmitter.writer (RuntimeCodecIL(method.GetILGenerator(),typeof<Guid>.MakeByRefType())) facts (typeof<char>.Equals(element))
        method

    let private createString element output =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" typeof<string>
        let method = DynamicMethod("WriteString",typeof<Void>,Array.append [|typeof<string>.MakeByRefType()|] (signature element),typeof<JsonWriterRegion>.Module,true)
        let read = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                   |> Array.find(fun method -> method.Name = "ReadUnaligned" && method.IsGenericMethodDefinition &&
                                               method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
        let facts = { Output = api element output
                      Reference = typeof<JsonWriterRegion>.GetMethod("StringReference",flags) :> MethodBase
                      Length = typeof<string>.GetProperty("Length").GetMethod :> MethodBase
                      ReadWord = read.MakeGenericMethod(typeof<uint64>) :> MethodBase
                      WriteWord = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                                  |> Array.find(fun method -> method.Name = "WriteUnaligned" && method.IsGenericMethodDefinition &&
                                                              method.GetParameters().Length = 2 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
                                  |> fun method -> method.MakeGenericMethod(typeof<uint32>) :> MethodBase }
        CodecStringWriterEmitter.writer (RuntimeCodecIL(method.GetILGenerator(),typeof<string>.MakeByRefType())) facts (element.Equals(typeof<char>))
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

    let private createBytes element output ordinarySequence =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON write" typeof<byte[]>
        let method = DynamicMethod("WriteBytes",typeof<Void>,Array.append [|typeof<byte[]>.MakeByRefType()|] (signature element),typeof<JsonWriterRegion>.Module,true)
        let write = typeof<Unsafe>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                    |> Array.find(fun method -> method.Name = "WriteUnaligned" && method.IsGenericMethodDefinition &&
                                                method.GetParameters().Length = 2 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
        let facts = { Output = api element output; Bytes = typeof<byte[]>
                      ReserveKnownSpan = if ordinarySequence then None else Some(typeof<JsonWriterRegion>.GetMethod("TryReserveKnownSpan",flags).MakeGenericMethod(element) :> MethodBase)
                      LengthOverflow = if ordinarySequence then Some(typeof<JsonWriterRegion>.GetMethod("Base64LengthOverflow",flags) :> MethodBase) else None
                      Capture = typeof<JsonWriterRegion>.GetMethod("CaptureBytes" + (if element.Equals(typeof<char>) then "Utf16" else "Utf8"),flags) :> MethodBase
                      Reference = typeof<JsonWriterRegion>.GetMethod("BytesReference",flags) :> MethodBase
                      Alphabet = typeof<JsonWriterRegion>.GetMethod("Base64Reference",flags) :> MethodBase
                      Vector = base64Vector write (element.Equals(typeof<char>))
                      LittleEndian = typeof<BitConverter>.GetField("IsLittleEndian")
                      WriteWord = write.MakeGenericMethod(if element.Equals(typeof<char>) then typeof<uint64> else typeof<uint32>) :> MethodBase }
        CodecBase64WriterEmitter.writer (RuntimeCodecIL(method.GetILGenerator(),typeof<byte[]>.MakeByRefType())) facts (typeof<char>.Equals(element)) ordinarySequence
        method

    let boolean (element: Type) output =
        JsonDefaultMethods.withContract typeof<bool> element output "boolean-write" (fun () -> createBoolean element output)

    let number (target: Type) element output =
        JsonDefaultMethods.withContract target element output "numeric-write" (fun () -> createNumber target element output)

    let guid element output =
        JsonDefaultMethods.withContract typeof<Guid> element output "guid-write" (fun () -> createGuid element output)

    let text element output =
        JsonDefaultMethods.withContract typeof<string> element output "string-write" (fun () -> createString element output)

    let typeName target element output =
        JsonDefaultMethods.withContract target element output "type-literal" (fun () -> createTypeName target element output)

    let bytes element output =
        JsonDefaultMethods.withContract typeof<byte[]> element output "bytes-write" (fun () -> createBytes element output false)

    let sequenceBytes<'E,'O>() =
        JsonDefaultMethods.callable<JsonDirectWriter<byte[],'E>> typeof<byte[]> typeof<'E> typeof<'O> "sequence-bytes-write"
            (fun () -> createBytes typeof<'E> typeof<'O> true :> MethodBase)

    let writer<'T,'E,'O>() =
        (number typeof<'T> typeof<'E> typeof<'O>).CreateDelegate(typeof<JsonDirectWriter<'T,'E>>) :?> JsonDirectWriter<'T,'E>
