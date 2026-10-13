namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

module internal CodecNumericBooleanEmitter =
    let decimalReader (il: CodecIL<'T,'M,'F>) (api: CodecIntegerApi<'T,'M>) utf16 =
        let state = CodecNumberEmitter.locals il api.Int32 api.Int64
        let invalid = il.Label()
        // Exact zero is a significand fact; scale cannot turn a finite nonzero into false.
        CodecNumberEmitter.scan il api.Int32 api.Int64 api.OffsetByte utf16 0L state invalid 0 (fun _ -> ())
        CodecNumberEmitter.publish il state
        il.EmitLocal(OpCodes.Ldloc,state.SignificantDigits)
        il.EmitInt(OpCodes.Ldc_I4,0); il.Emit OpCodes.Cgt; il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid number literal"; il.EmitMethod(OpCodes.Newobj,api.InvalidNumber); il.Emit OpCodes.Throw

    let hexReader (il: CodecIL<'T,'M,'F>) (api: CodecIntegerApi<'T,'M>) utf16 =
        let position = il.Local api.Int32
        let character = il.Local api.Int32
        let digit = il.Local api.Int32
        let count = il.Local api.Int32
        let nonzero = il.Local api.Int32
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let read () =
            il.Emit OpCodes.Ldarg_0; load position
            CodecAddressEmitter.offset il api.OffsetByte (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save character
        il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldind_I4; save position
        read()
        let sign = il.Label()
        let prefix = il.Label()
        load character; constant 43; il.EmitBranch(OpCodes.Beq,sign)
        load character; constant 45; il.EmitBranch(OpCodes.Bne_Un,prefix)
        il.Mark sign
        load position; constant 1; il.Emit OpCodes.Add; save position
        il.Mark prefix
        load position; constant 2; il.Emit OpCodes.Add; save position
        let loop = il.Label()
        let collect = il.Label()
        let complete = il.Label()
        let invalid = il.Label()
        il.Mark loop
        load position; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,complete)
        read()
        load character; constant 48; il.Emit OpCodes.Sub; save digit
        load digit; constant 9; il.EmitBranch(OpCodes.Ble_Un,collect)
        load character; constant 32; il.Emit OpCodes.Or; constant 97; il.Emit OpCodes.Sub; save digit
        load digit; constant 5; il.EmitBranch(OpCodes.Bgt_Un,complete)
        load digit; constant 10; il.Emit OpCodes.Add; save digit
        il.Mark collect
        load nonzero; load digit; il.Emit OpCodes.Or; save nonzero
        load count; constant 1; il.Emit OpCodes.Add; save count
        load position; constant 1; il.Emit OpCodes.Add; save position
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark complete
        load count; il.EmitBranch(OpCodes.Brfalse,invalid)
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        load nonzero; constant 0; il.Emit OpCodes.Cgt_Un; il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid hex literal"; il.EmitMethod(OpCodes.Newobj,api.InvalidHex); il.Emit OpCodes.Throw
