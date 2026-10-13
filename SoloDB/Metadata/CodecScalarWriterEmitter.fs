namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecWriterConversion<'Type,'Method> = {
    Result: 'Type
    Call: 'Method
    Address: bool
}

module internal CodecScalarWriterEmitter =
    /// Capture required conversions once, before any output callback or discard admission.
    let prepare (il: CodecIL<'T,'M,'F>) target core (steps: CodecWriterConversion<'T,'M>[]) castInteger =
        if steps.Length = 0 && not castInteger then
            (fun () -> il.Emit OpCodes.Ldarg_0), (fun () -> il.EmitInt(OpCodes.Ldarga,0))
        else
            let mutable value = il.Local target
            il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Stloc,value)
            for step in steps do
                il.EmitLocal((if step.Address then OpCodes.Ldloca else OpCodes.Ldloc),value)
                il.EmitMethod(OpCodes.Call,step.Call)
                value <- il.Local step.Result
                il.EmitLocal(OpCodes.Stloc,value)
            if castInteger then
                il.EmitLocal(OpCodes.Ldloc,value); il.Emit OpCodes.Conv_I8
                value <- il.Local core
                il.EmitLocal(OpCodes.Stloc,value)
            let captured = value
            (fun () -> il.EmitLocal(OpCodes.Ldloc,captured)), (fun () -> il.EmitLocal(OpCodes.Ldloca,captured))
