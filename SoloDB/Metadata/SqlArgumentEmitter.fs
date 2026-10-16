namespace SoloDatabase.Metadata.CodecGeneration

open System
open System.Reflection.Emit

/// Captures each original getter once, in the supplied property order.
module internal SqlArgumentEmitter =
    let capture (il: CodecIL<'T,'M,'F>) valuesArgument count (read: Action<int>) =
        for index = 0 to count - 1 do
            il.EmitInt(OpCodes.Ldarg,valuesArgument)
            il.EmitInt(OpCodes.Ldc_I4,index)
            read.Invoke index
            il.Emit OpCodes.Stelem_Ref
        il.Emit OpCodes.Ret
