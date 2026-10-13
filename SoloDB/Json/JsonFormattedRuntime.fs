namespace SoloDatabase.JsonSerializator

open System
open System.Reflection
open System.Reflection.Emit
open System.Runtime.CompilerServices
open SoloDatabase
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation

/// Cold original operands for shared formatted conversion and quoted admission.
module internal JsonFormattedRuntime =
    let private create (target: Type) (reader: Type) =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON read" target
        let kind = TypeSupport.FormattedReader target.FullName
        let utf16 = reader.Equals(typeof<Utf16Reader>)
        let encoding = if utf16 then "Utf16" else "Utf8"
        let flags = BindingFlags.Static ||| BindingFlags.NonPublic
        let input name = typeof<JsonNumericInput>.GetMethod(name,flags).MakeGenericMethod(reader) :> MethodBase
        let fallback name = typeof<JsonFormattedInput>.GetMethod(name+encoding,flags) :> MethodBase
        let offset = typeof<Unsafe>.GetMethods(BindingFlags.Static ||| BindingFlags.Public)
                     |> Array.find(fun method -> method.Name="Add" && method.IsGenericMethodDefinition &&
                                                 method.GetParameters().Length=2 && method.GetParameters().[0].ParameterType.IsByRef &&
                                                 method.GetParameters().[1].ParameterType.Equals(typeof<unativeint>))
        let offset = offset.MakeGenericMethod(typeof<byte>) :> MethodBase
        let context = typeof<JsonDirectReader<obj,Utf8Reader>>.GetMethod("Invoke",BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                          .GetParameters().[0].ParameterType.GetElementType().GetGenericTypeDefinition().MakeGenericType(reader).MakeByRefType()
        let decode =
            if kind="Guid" then fallback kind
            else
                let method = DynamicMethod("ReadBase64Payload",target,[|context;typeof<int>;typeof<int>|],typeof<JsonRawPrimitive>.Module,true)
                let api = { Byte=typeof<byte>; Bytes=typeof<byte[]>; Int32=typeof<int>; Reference=typeof<byte>.MakeByRefType()
                            Input=input "Reference"; Output=typeof<JsonWriterRegion>.GetMethod("BytesReference",flags) :> MethodBase
                            Empty=typeof<Array>.GetMethod("Empty",BindingFlags.Static ||| BindingFlags.Public).MakeGenericMethod(typeof<byte>) :> MethodBase
                            Offset=offset; Compatibility=fallback kind }
                CodecFormattedReaderEmitter.base64 (RuntimeCodecIL(method.GetILGenerator())) api utf16
                method :> MethodBase
        let word = typeof<Unsafe>.GetMethods(BindingFlags.Static ||| BindingFlags.Public)
                   |> Array.find(fun method -> method.Name="ReadUnaligned" && method.IsGenericMethodDefinition &&
                                               method.GetParameters().Length=1 && method.GetParameters().[0].ParameterType.Equals(typeof<byte>.MakeByRefType()))
        let api = { Int32=typeof<int>; UInt64=typeof<uint64>; SourceReference=typeof<byte>.MakeByRefType(); PositionReference=typeof<int>.MakeByRefType()
                    Begin=input "Begin"; Reference=input "Reference"; Length=input "Length"; Position=input "Position"; Complete=Some(input "Complete")
                    OffsetByte=offset; ReadWord=word.MakeGenericMethod(typeof<uint64>) :> MethodBase; Decode=decode
                    Escaped=fallback(kind+"Escaped"); Compatibility=typeof<JsonRawPrimitive>.GetMethod("Read"+kind,flags).MakeGenericMethod(reader) :> MethodBase
                    }
        let method = DynamicMethod("Read"+kind,target,[|context|],typeof<JsonRawPrimitive>.Module,true)
        let il = RuntimeCodecIL(method.GetILGenerator())
        if kind="Guid" then
            let parameters = Array.concat [ [|typeof<int>;typeof<int16>;typeof<int16>|];Array.create 8 typeof<byte> ]
            CodecFormattedReaderEmitter.guid il api (typeof<Guid>.GetConstructor(parameters) :> MethodBase) utf16
        else CodecStringEmitter.reader il api utf16
        method

    let method target reader = JsonDefaultMethods.get target reader "formatted-read" (fun () -> create target reader)

    let reader<'T,'R when 'R : struct and 'R :> IJsonReader>() =
        (method typeof<'T> typeof<'R>).CreateDelegate(typeof<JsonDirectReader<'T,'R>>) :?> JsonDirectReader<'T,'R>
