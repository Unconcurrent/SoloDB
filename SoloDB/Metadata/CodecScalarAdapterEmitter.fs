namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

module internal CodecScalarAdapterEmitter =
    /// Expanded compatibility and tree conversion for a real target-provided scalar parser.
    let reader (il: CodecIL<'T,'M,'F>) text culture parse =
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,text)
        il.EmitInt(OpCodes.Ldc_I4,int System.Globalization.NumberStyles.Float)
        il.EmitMethod(OpCodes.Call,culture)
        il.EmitMethod(OpCodes.Call,parse)
        il.Emit OpCodes.Ret

    let treeWriter (il: CodecIL<'T,'M,'F>) widen decimal number =
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,widen)
        il.EmitMethod(OpCodes.Call,decimal)
        il.EmitMethod(OpCodes.Call,number)
        il.Emit OpCodes.Ret

    let rawWriter (il: CodecIL<'T,'M,'F>) widen write =
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,widen)
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,write)
        il.Emit OpCodes.Ret
