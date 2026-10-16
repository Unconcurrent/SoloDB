namespace SoloDatabase.JsonSerializator

open System
open System.Numerics
open System.Reflection
open System.Reflection.Emit
open SoloDatabase
open SoloDatabase.Numerics
open SoloDatabase.Metadata.CodecGeneration

module internal JsonStoredNumericRuntime =
    let private flags = BindingFlags.Static ||| BindingFlags.NonPublic
    let private create (target: Type) (element: Type) =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "stored numerical write" target
        let signature = typeof<JsonDirectWriter<obj,byte>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters()
        let close index = signature[index].ParameterType.GetElementType().GetGenericTypeDefinition().MakeGenericType(element).MakeByRefType()
        let method = DynamicMethod("WriteNumericalKey",typeof<Void>,[|target;close 1;close 2|],typeof<JsonStoredNumeric>.Module,true)
        let helper name = typeof<JsonStoredNumeric>.GetMethod(name,flags) :> MethodBase
        let plan = CodecNumericPreparation.writer target.FullName true (Some IntPtr.Size)
        let facts =
            { Output = JsonWriterRuntime.api element; Prepared = typeof<NumericKeyDigits>
              Prepare = helper plan.StoredOperation.Value
              Length = helper "Length"; Utf8Length = helper "Utf8Length"; Unit = helper "Unit" }
        CodecStoredNumericEmitter.writer (RuntimeCodecIL(method.GetILGenerator())) facts (element.Equals(typeof<char>))
        method
    let writerMethod target element =
        JsonDefaultMethods.withContract target element typeof<JsonStoredNumeric> "stored-number-write" (fun () -> create target element)
