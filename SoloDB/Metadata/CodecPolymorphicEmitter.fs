namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

module internal CodecPolymorphicEmitter =
    let dispatch (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) (policy: CodecPolymorphicApi<'T,'M>) declared exact same =
        il.EmitMethod(OpCodes.Call,api.EnsureStack)
        let actual = il.Local policy.RuntimeType
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Callvirt,api.GetType)
        il.EmitLocal(OpCodes.Stloc,actual)
        let different = il.Label()
        let finished = il.Label()
        if exact then
            il.EmitLocal(OpCodes.Ldloc,actual)
            CodecEmitter.typeToken il api declared
            il.EmitMethod(OpCodes.Callvirt,policy.Equals)
            il.EmitBranch(OpCodes.Brfalse,different)
            same()
            il.EmitBranch(OpCodes.Br,finished)
        il.Mark different
        let previous = il.Local api.Int32
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,policy.IncludeType)
        il.EmitLocal(OpCodes.Stloc,previous)
        il.Emit OpCodes.Ldarg_2
        il.Emit OpCodes.Ldc_I4_1
        il.EmitMethod(OpCodes.Call,policy.SetIncludeType)
        il.BeginException()
        let completed = il.Label()
        for candidate in policy.Known declared do
            let next = il.Label()
            il.EmitLocal(OpCodes.Ldloc,actual)
            CodecEmitter.typeToken il api candidate.Type
            il.EmitMethod(OpCodes.Callvirt,policy.Equals)
            il.EmitBranch(OpCodes.Brfalse,next)
            let arguments() =
                il.Emit OpCodes.Ldarg_0
                il.EmitType((if candidate.ValueType then OpCodes.Unbox_Any else OpCodes.Castclass),candidate.Type)
                il.Emit OpCodes.Ldarg_1
                il.Emit OpCodes.Ldarg_2
            arguments()
            il.EmitMethod(OpCodes.Call,candidate.Operation)
            il.EmitBranch(OpCodes.Br,completed)
            il.Mark next
        il.Emit OpCodes.Ldarg_0
        il.EmitLocal(OpCodes.Ldloc,actual)
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,policy.Actual)
        il.Mark completed
        il.Finally()
        il.Emit OpCodes.Ldarg_2
        il.EmitLocal(OpCodes.Ldloc,previous)
        il.EmitMethod(OpCodes.Call,policy.SetIncludeType)
        il.EndException()
        il.Mark finished

    let abstractWriter (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>) policy declared nullWriter =
        let present = il.Label()
        il.Emit OpCodes.Ldarg_0
        il.EmitBranch(OpCodes.Brtrue,present)
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,nullWriter)
        il.Emit OpCodes.Ret
        il.Mark present
        dispatch il api policy declared false (fun () -> ())
        il.Emit OpCodes.Ret
