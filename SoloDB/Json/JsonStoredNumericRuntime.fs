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
    let private create (target: Type) (element: Type) output =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "stored numerical write" target
        let signature = typeof<JsonDirectWriter<obj,byte>>.GetMethod("Invoke",BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic).GetParameters()
        let close index = signature[index].ParameterType.GetElementType().GetGenericTypeDefinition().MakeGenericType(element).MakeByRefType()
        let method = DynamicMethod("WriteNumericalKey",typeof<Void>,[|target.MakeByRefType();close 1;close 2|],typeof<JsonStoredNumeric>.Module,true)
        let helper name = typeof<JsonStoredNumeric>.GetMethod(name,flags) :> MethodBase
        let plan = CodecNumericPreparation.writer target.FullName true (Some IntPtr.Size)
        let facts =
            { Output = JsonWriterRuntime.api element output; Prepared = typeof<NumericKeyDigits>
              Prepare = helper plan.StoredOperation.Value
              Validate = helper ("Validate" + plan.StoredOperation.Value.Substring("Prepare".Length))
              Length = helper "Length"; Utf8Length = helper "Utf8Length"; Unit = helper "Unit" }
        CodecStoredNumericEmitter.writer (RuntimeCodecIL(method.GetILGenerator(),target.MakeByRefType())) facts (element.Equals(typeof<char>))
        method
    let writerMethod target element output =
        JsonDefaultMethods.withContract target element output "stored-number-write" (fun () -> create target element output)
