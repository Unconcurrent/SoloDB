namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecFloatingApi<'Method> = { Kernel: 'Method }

module internal CodecFloatingEmitter =
    /// The shared numerical kernel rounds directly to the requested target bits.
    let bits (il: CodecIL<'T,'M,'F>) (api: CodecFloatingApi<'M>) width =
        match width with
        | 16 | 32 | 64 -> ()
        | _ -> invalidArg "width" "Unsupported binary format."
        il.Emit OpCodes.Ldarg_0
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitInt(OpCodes.Ldc_I4,width)
        il.EmitMethod(OpCodes.Call,api.Kernel)
        if width = 16 then il.Emit OpCodes.Conv_U2
        elif width = 32 then il.Emit OpCodes.Conv_U4
        il.Emit OpCodes.Ret
