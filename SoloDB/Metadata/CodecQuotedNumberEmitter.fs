namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecQuotedNumberApi<'Type,'Method> = {
    Result: 'Type
    Int32: 'Type
    OffsetByte: 'Method
    Decimal: 'Method
    Consumed: 'Method
    Compatibility: 'Method
}

module internal CodecQuotedNumberEmitter =
    /// Ordinary unescaped number strings share the expanded target conversion.
    let reader (il: CodecIL<'T,'M,'F>) (api: CodecQuotedNumberApi<'T,'M>) utf16 =
        let position = il.Local api.Int32
        let quote = il.Local api.Int32
        let character = il.Local api.Int32
        let result = il.Local api.Result
        let compatibility = il.Label()
        let complete = il.Label()
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let intConst value = il.EmitInt(OpCodes.Ldc_I4,value)
        let increment () = load position; intConst 1; il.Emit OpCodes.Add; save position
        let read () =
            load position; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,compatibility)
            il.Emit OpCodes.Ldarg_0; load position
            CodecAddressEmitter.offset il api.OffsetByte (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save character
        let spaces () =
            let loop = il.Label()
            let ready = il.Label()
            il.Mark loop
            read(); load character; intConst 32; il.EmitBranch(OpCodes.Bne_Un,ready)
            increment(); il.EmitBranch(OpCodes.Br,loop)
            il.Mark ready
        il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldind_I4; save position
        read(); load character; save quote
        increment(); spaces()
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        il.Emit OpCodes.Ldarg_0; il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldarg_3
        il.EmitMethod(OpCodes.Call,api.Decimal); save result
        // An exact ambiguity or uncommon string adapter already owns the complete quoted token.
        il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,api.Consumed); il.EmitBranch(OpCodes.Brtrue,complete)
        il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldind_I4; save position
        spaces()
        load character; load quote; il.EmitBranch(OpCodes.Bne_Un,compatibility)
        increment()
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        il.Mark complete
        load result; il.Emit OpCodes.Ret
        il.Mark compatibility
        il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,api.Compatibility); il.Emit OpCodes.Ret
