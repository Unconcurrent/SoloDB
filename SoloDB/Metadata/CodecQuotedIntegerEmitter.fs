namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecQuotedIntegerApi<'Type,'Method> = {
    Int32: 'Type
    UInt32: 'Type
    UInt64: 'Type
    OffsetByte: 'Method
    Compatibility: 'Method
}

module internal CodecQuotedIntegerEmitter =
    /// Common ASCII integer strings use the destination arithmetic; other strings retain their coercion adapter.
    let reader (il: CodecIL<'T,'M,'F>) (api: CodecQuotedIntegerApi<'T,'M>) width signed utf16 native =
        let wide = width = 64
        let word = if wide then api.UInt64 else api.UInt32
        let position = il.Local api.Int32
        let quote = il.Local api.Int32
        let character = il.Local api.Int32
        let digit = il.Local api.Int32
        let negative = il.Local api.Int32
        let digits = il.Local api.Int32
        let value = il.Local word
        let cutoff = il.Local word
        let last = il.Local api.Int32
        let overflowed = il.Local api.Int32
        let compatibility = il.Label()
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let intConst value = il.EmitInt(OpCodes.Ldc_I4,value)
        let number value = if wide then il.EmitInt64(int64 value) else intConst(int value)
        let increment local = load local; intConst 1; il.Emit OpCodes.Add; save local
        let read () =
            load position; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,compatibility)
            il.Emit OpCodes.Ldarg_0; load position
            CodecAddressEmitter.offset il api.OffsetByte (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save character
        let skipSpaces () =
            let loop = il.Label()
            let skip = il.Label()
            let complete = il.Label()
            il.Mark loop
            read()
            for space in [|32|] do
                load character; intConst space; il.EmitBranch(OpCodes.Beq,skip)
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark skip
            increment position; il.EmitBranch(OpCodes.Br,loop)
            il.Mark complete
        il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldind_I4; save position
        read(); load character; save quote
        increment position
        skipSpaces()
        let sign = il.Label()
        let limits = il.Label()
        load character; intConst 43; il.EmitBranch(OpCodes.Beq,sign)
        load character; intConst 45; il.EmitBranch(OpCodes.Bne_Un,limits)
        il.Mark sign
        load character; intConst 45; il.Emit OpCodes.Ceq; save negative
        increment position
        il.Mark limits
        let positiveLimit = if signed then (1UL <<< (width - 1)) - 1UL elif wide then System.UInt64.MaxValue else (1UL <<< width) - 1UL
        let negativeLimit = if signed then 1UL <<< (width - 1) else positiveLimit
        number(positiveLimit / 10UL); save cutoff
        intConst(int(positiveLimit % 10UL)); save last
        if signed then
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            number(negativeLimit / 10UL); save cutoff
            intConst(int(negativeLimit % 10UL)); save last
            il.Mark positive
        let loop = il.Label()
        let endDigits = il.Label()
        il.Mark loop
        read()
        load character; intConst 48; il.Emit OpCodes.Sub; save digit
        load digit; intConst 9; il.EmitBranch(OpCodes.Bgt_Un,endDigits)
        increment digits
        let next = il.Label()
        load overflowed; il.EmitBranch(OpCodes.Brtrue,next)
        let tooLarge = il.Label()
        let within = il.Label()
        load value; load cutoff; il.EmitBranch(OpCodes.Bgt_Un,tooLarge)
        load value; load cutoff; il.EmitBranch(OpCodes.Bne_Un,within)
        load digit; load last; il.EmitBranch(OpCodes.Bgt,tooLarge)
        il.Mark within
        load value; number 10UL; il.Emit OpCodes.Mul
        load digit
        if wide then il.Emit OpCodes.Conv_U8
        il.Emit OpCodes.Add; save value
        il.EmitBranch(OpCodes.Br,next)
        il.Mark tooLarge
        intConst 1; save overflowed
        il.Mark next
        increment position; il.EmitBranch(OpCodes.Br,loop)
        il.Mark endDigits
        load digits; il.EmitBranch(OpCodes.Brfalse,compatibility)
        skipSpaces()
        load character; load quote; il.EmitBranch(OpCodes.Bne_Un,compatibility)
        load overflowed; il.EmitBranch(OpCodes.Brtrue,compatibility)
        if not signed then
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            load value; number 0UL; il.EmitBranch(OpCodes.Bne_Un,compatibility)
            il.Mark positive
        increment position
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        if signed then
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            number 0UL; load value; il.Emit OpCodes.Sub; save value
            il.Mark positive
        load value
        if native then il.Emit(if signed then OpCodes.Conv_I else OpCodes.Conv_U)
        elif width = 8 then il.Emit(if signed then OpCodes.Conv_I1 else OpCodes.Conv_U1)
        elif width = 16 then il.Emit(if signed then OpCodes.Conv_I2 else OpCodes.Conv_U2)
        il.Emit OpCodes.Ret
        il.Mark compatibility
        il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,api.Compatibility); il.Emit OpCodes.Ret
