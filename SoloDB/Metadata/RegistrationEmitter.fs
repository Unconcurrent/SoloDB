namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Generated entries complete once; a failed registration remains retryable.
module internal RegistrationEmitter =
    let initialize (il: CodecIL<'T,'M,'F>) constructor gate =
        il.EmitMethod(OpCodes.Newobj,constructor)
        il.EmitField(OpCodes.Stsfld,gate)
        il.Emit OpCodes.Ret

    let once (il: CodecIL<'T,'M,'F>) gate complete enter exit register =
        il.EmitField(OpCodes.Ldsfld,gate)
        il.EmitMethod(OpCodes.Call,enter)
        il.BeginException()
        let doneLabel = il.Label()
        il.EmitField(OpCodes.Ldsfld,complete)
        il.EmitBranch(OpCodes.Brtrue,doneLabel)
        il.EmitMethod(OpCodes.Call,register)
        il.EmitInt(OpCodes.Ldc_I4,1)
        il.EmitField(OpCodes.Stsfld,complete)
        il.Mark doneLabel
        il.Finally()
        il.EmitField(OpCodes.Ldsfld,gate)
        il.EmitMethod(OpCodes.Call,exit)
        il.EndException()
        il.Emit OpCodes.Ret
