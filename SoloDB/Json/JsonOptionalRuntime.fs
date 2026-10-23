namespace SoloDatabase.JsonSerializator

open System
open System.Reflection
open System.Reflection.Emit
open System.Runtime.CompilerServices
open SoloDatabase
open SoloDatabase.Metadata.CodecGeneration
open SoloDatabase.Metadata.Generation

/// Resolves original CLR operands; the shared emitter owns optional and enum policy.
module internal JsonOptionalRuntime =
    let private flags = BindingFlags.Static ||| BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic
    let kind (target: Type) = TypeSupport.RawAdaptation (if target.IsGenericType then target.GetGenericTypeDefinition().FullName else target.FullName) target.IsEnum
    let payload (target: Type) = if target.IsEnum then Enum.GetUnderlyingType target else target.GetGenericArguments().[0]
    let private transfer source target =
        (typeof<Unsafe>.GetMethods(flags) |> Array.find(fun method -> method.Name = "As" && method.IsGenericMethodDefinition && method.GetGenericArguments().Length = 2 && method.GetParameters().Length = 1))
            .MakeGenericMethod(source,target) :> MethodBase
    let private context (reader: Type) =
        typeof<JsonDirectReader<obj,Utf8Reader>>.GetMethod("Invoke",flags).GetParameters().[0].ParameterType.GetElementType().GetGenericTypeDefinition().MakeGenericType(reader)

    let private createReader (target: Type) (reader: Type) (read: Type -> MethodBase) (retry: Type -> bool -> MethodBase) (skip: MethodBase) =
        let payload = payload target
        let context = context reader
        let operation name = typeof<JsonOptionalInput>.GetMethod(name,flags).MakeGenericMethod(reader) :> MethodBase
        let api =
            { Result = target; Payload = payload; Context = context; Boolean = typeof<bool>; Int32 = typeof<int>
              Begin = operation "Begin"; Open = operation "Open"; Capture = operation "Capture"; End = operation "End"
              Flat = operation "Flat"; Restore = operation "Restore"; Missing = typeof<JsonOptionalInput>.GetMethod("Missing",flags) :> MethodBase
              Names = (if kind target = 3 then Unchecked.defaultof<_> else JsonNameRuntime.api reader); Skip = skip
              PayloadCall = read payload; FlagCall = read typeof<bool>
              PrimitiveMismatch = typeof<JsonPrimitiveShapeMismatch>; ScalarMismatch = typeof<JsonScalarShapeMismatch>
              PrimitiveRetry = retry payload true; ScalarRetry = retry payload false
              FlagPrimitiveRetry = retry typeof<bool> true; FlagScalarRetry = retry typeof<bool> false
              Construct = if kind target = 3 then None else Some(target.GetConstructor([|payload|]) :> MethodBase)
              Transfer = if kind target = 3 then Some(transfer payload target) else None }
        let method = DynamicMethod("ReadOptional" + target.Name,target,[|context.MakeByRefType()|],typeof<JsonOptionalInput>.Module,true)
        CodecOptionalEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) api
        method

    let private createWriter (target: Type) (parameters: Type[]) (write: Type -> MethodBase) (nullBody: CodecIL<Type,MethodBase,FieldInfo> -> unit) =
        let arguments = Array.copy parameters
        arguments[0] <- target.MakeByRefType()
        let method = DynamicMethod("WriteOptional" + target.Name,typeof<Void>,arguments,typeof<JsonOptionalInput>.Module,true)
        let il = RuntimeCodecIL(method.GetILGenerator(),arguments[0])
        let payload = payload target
        let api =
            { Result = target; Payload = payload; ValueType = target.IsValueType
              Presence = if kind target = 2 then Some(target.GetProperty("HasValue").GetMethod :> MethodBase) else None
              Value = if kind target = 3 then None else Some(target.GetProperty("Value").GetMethod :> MethodBase)
              Transfer = if kind target = 3 then Some(transfer target payload) else None
              Null = (fun () -> nullBody il); PayloadCall = write payload }
        CodecOptionalEmitter.writer il api
        method

    let reader (target: Type) (reader: Type) contract read retry skip =
        JsonDefaultMethods.get target reader contract (fun () -> createReader target reader read retry skip)

    let writer (target: Type) (encoding: Type) (output: Type) contract parameters write nullBody =
        JsonDefaultMethods.withContract target encoding output contract (fun () -> createWriter target parameters write nullBody)
