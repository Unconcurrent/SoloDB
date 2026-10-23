namespace SoloDatabase.JsonSerializator

open System
open System.Globalization
open System.Reflection
open System.Reflection.Emit
open SoloDatabase
open SoloDatabase.Metadata.CodecGeneration

/// Cold binding supplies original scalar signatures to the common instruction macros.
module internal JsonNumericScalarRuntime =
    let private flags = BindingFlags.NonPublic ||| BindingFlags.Static
    let private admit target =
        if not(MetadataRuntime.supportsDynamicCode()) then MetadataRuntime.missing "direct JSON scalar" target
    let private parse (target: Type) =
        target.GetMethod("Parse",[|typeof<string>;typeof<NumberStyles>;typeof<IFormatProvider>|]) :> MethodBase
    let private widen (target: Type) =
        target.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
        |> Array.find(fun method -> method.Name = "op_Explicit" && typeof<single>.Equals(method.ReturnType) &&
                                    method.GetParameters().Length = 1 && method.GetParameters().[0].ParameterType.Equals(target))
        :> MethodBase
    let private culture = typeof<JsonNumericInput>.GetMethod("Culture",flags) :> MethodBase

    let private createCompatibility target reader context =
        admit target
        let method = DynamicMethod("ReadScalarText",target,[|context|],typeof<JsonRawPrimitive>.Module,true)
        let text = typeof<JsonRawPrimitive>.GetMethod("ReadNumericText",flags).MakeGenericMethod(target,reader) :> MethodBase
        CodecScalarAdapterEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) text culture (parse target)
        method

    let compatibility target reader context =
        JsonDefaultMethods.get target reader "numeric-compatibility" (fun () -> createCompatibility target reader context) :> MethodBase

    let trees (target: Type) source text number =
        admit target
        let read = JsonDefaultMethods.get target source "numeric-tree-read" (fun () ->
            let method = DynamicMethod("ReadScalarTree",target,[|source|],typeof<JsonRawPrimitive>.Module,true)
            CodecScalarAdapterEmitter.reader (RuntimeCodecIL(method.GetILGenerator())) text culture (parse target)
            method)
        let write = JsonDefaultMethods.get target source "numeric-tree-write" (fun () ->
            let method = DynamicMethod("WriteScalarTree",source,[|target|],typeof<JsonRawPrimitive>.Module,true)
            let decimal = typeof<decimal>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                          |> Array.find(fun method -> method.Name = "op_Explicit" && typeof<decimal>.Equals(method.ReturnType) &&
                                                      method.GetParameters().Length = 1 && typeof<single>.Equals(method.GetParameters().[0].ParameterType))
            CodecScalarAdapterEmitter.treeWriter (RuntimeCodecIL(method.GetILGenerator())) (widen target) (decimal :> MethodBase) number
            method)
        struct(write,read)

    let private createWriter (target: Type) (element: Type) (output: Type) =
        admit target
        let signature = typeof<JsonDirectWriter<obj,byte>>.GetMethod("Invoke",BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance).GetParameters()
        let close index = signature[index].ParameterType.GetElementType().GetGenericTypeDefinition().MakeGenericType(element).MakeByRefType()
        let method = DynamicMethod("WriteScalar",typeof<Void>,[|target.MakeByRefType();close 1;close 2|],typeof<JsonRawPrimitive>.Module,true)
        let write = typeof<JsonRawPrimitive>.GetMethod("WriteSingle",flags).MakeGenericMethod(element,output) :> MethodBase
        CodecScalarAdapterEmitter.rawWriter (RuntimeCodecIL(method.GetILGenerator(),target.MakeByRefType())) (widen target) write
        method

    let writerMethod target element output =
        JsonDefaultMethods.withContract target element output "numeric-adapter-write" (fun () -> createWriter target element output)

    let writer<'T,'E,'O when 'O : struct and 'O :> IJsonValueOutput<'E>>() =
        (writerMethod typeof<'T> typeof<'E> typeof<'O>).CreateDelegate(typeof<JsonDirectWriter<'T,'E>>) :?> JsonDirectWriter<'T,'E>
