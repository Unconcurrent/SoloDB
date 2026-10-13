namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecDecimalApi<'Type,'Method> = {
    Result: 'Type
    Int32: 'Type
    Int64: 'Type
    UInt64: 'Type
    OffsetByte: 'Method
    Constructor: 'Method
    Overflow: 'Method
    InvalidNumber: 'Method
}

module internal CodecDecimalEmitter =
    let hex (il: CodecIL<'T,'M,'F>) (integer: CodecIntegerApi<'T,'M>) (api: CodecDecimalApi<'T,'M>) utf16 =
        let coefficient = { Low = il.Local api.UInt64; High = il.Local api.UInt64 }
        let position = il.Local api.Int32
        let character = il.Local api.Int32
        let digit = il.Local api.Int32
        let count = il.Local api.Int32
        let significant = il.Local api.Int32
        let negative = il.Local api.Int32
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
        load character; constant 45; il.Emit OpCodes.Ceq; save negative
        load position; constant 1; il.Emit OpCodes.Add; save position
        il.Mark prefix
        load position; constant 2; il.Emit OpCodes.Add; save position
        let loop = il.Label()
        let collect = il.Label()
        let nonzero = il.Label()
        let next = il.Label()
        let complete = il.Label()
        let invalid = il.Label()
        let overflow = il.Label()
        il.Mark loop
        load position; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,complete)
        read()
        load character; constant 48; il.Emit OpCodes.Sub; save digit
        load digit; constant 9; il.EmitBranch(OpCodes.Ble_Un,collect)
        load character; constant 32; il.Emit OpCodes.Or; constant 97; il.Emit OpCodes.Sub; save digit
        load digit; constant 5; il.EmitBranch(OpCodes.Bgt_Un,complete)
        load digit; constant 10; il.Emit OpCodes.Add; save digit
        il.Mark collect
        load count; constant 1; il.Emit OpCodes.Add; save count
        load significant; il.EmitBranch(OpCodes.Brtrue,nonzero)
        load digit; il.EmitBranch(OpCodes.Brfalse,next)
        il.Mark nonzero
        load significant; constant 1; il.Emit OpCodes.Add; save significant
        load significant; constant 24; il.EmitBranch(OpCodes.Bgt,next)
        load coefficient.High; constant 4; il.Emit OpCodes.Shl
        load coefficient.Low; constant 60; il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Or; save coefficient.High
        load coefficient.Low; constant 4; il.Emit OpCodes.Shl
        load digit; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or; save coefficient.Low
        il.Mark next
        load position; constant 1; il.Emit OpCodes.Add; save position
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark complete
        load count; il.EmitBranch(OpCodes.Brfalse,invalid)
        load significant; constant 24; il.EmitBranch(OpCodes.Bgt,overflow)
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        load coefficient.Low; il.Emit OpCodes.Conv_I4
        load coefficient.Low; constant 32; il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Conv_I4
        load coefficient.High; il.Emit OpCodes.Conv_I4
        load negative; constant 0; il.EmitMethod(OpCodes.Newobj,api.Constructor); il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid hex literal"; il.EmitMethod(OpCodes.Newobj,integer.InvalidHex); il.Emit OpCodes.Throw
        il.Mark overflow
        il.EmitMethod(OpCodes.Newobj,api.Overflow); il.Emit OpCodes.Throw

    /// Keeps a bounded exact coefficient, then rounds once after the final scale is known.
    let reader (il: CodecIL<'T,'M,'F>) (api: CodecDecimalApi<'T,'M>) utf16 (compatibility: 'M option) =
        let state = CodecNumberEmitter.locals il api.Int32 api.Int64
        let coefficient = { Low = il.Local api.UInt64; High = il.Local api.UInt64 }
        let kept = il.Local api.Int32
        let guard = il.Local api.Int32
        let sticky = il.Local api.Int32
        let scale = il.Local api.Int64
        let remove = il.Local api.Int64
        let savedLow = il.Local api.UInt64
        let savedHigh = il.Local api.UInt64
        let invalid = il.Label()
        let overflow = il.Label()
        let construct = il.Label()
        let zero = il.Label()
        let normalize = il.Label()
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let intConst value = il.EmitInt(OpCodes.Ldc_I4,value)
        let increment local = load local; intConst 1; il.Emit OpCodes.Add; save local
        let collect knownPrefix =
            let retainLow () =
                load coefficient.Low; il.EmitInt64 10L; il.Emit OpCodes.Mul
                load state.Digit; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Add; save coefficient.Low
            if knownPrefix then
                retainLow(); increment kept
            else
                let discarded = il.Label()
                let collected = il.Label()
                let large = il.Label()
                let retained = il.Label()
                load kept; intConst 29; il.EmitBranch(OpCodes.Bge,discarded)
                load kept; intConst 19; il.EmitBranch(OpCodes.Bge,large)
                retainLow()
                il.EmitBranch(OpCodes.Br,retained)
                il.Mark large
                CodecDecimalArithmeticEmitter.append il api.UInt64 coefficient (Some state.Digit)
                il.Mark retained
                increment kept
                il.EmitBranch(OpCodes.Br,collected)
                il.Mark discarded
                let afterGuard = il.Label()
                load state.SignificantDigits; intConst 30; il.EmitBranch(OpCodes.Bne_Un,afterGuard)
                load state.Digit; save guard
                il.EmitBranch(OpCodes.Br,collected)
                il.Mark afterGuard
                load sticky; load state.Digit; il.Emit OpCodes.Or; save sticky
                il.Mark collected
        CodecNumberEmitter.scan il api.Int32 api.Int64 api.OffsetByte utf16 29L state invalid 19 collect
        load kept; il.Emit OpCodes.Conv_I8
        load state.IntegerDigits; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub
        load state.LeadingZeros; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Add
        load state.Exponent; il.Emit OpCodes.Sub; save scale
        load state.SignificantDigits; il.EmitBranch(OpCodes.Brfalse,zero)
        let scaleUp = il.Label()
        let reduce = il.Label()
        il.Mark scaleUp
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Bge,reduce)
        CodecDecimalArithmeticEmitter.append il api.UInt64 coefficient None
        load coefficient.High; il.EmitInt64 0xffffffffL; il.EmitBranch(OpCodes.Bgt_Un,overflow)
        load scale; il.EmitInt64 1L; il.Emit OpCodes.Add; save scale
        il.EmitBranch(OpCodes.Br,scaleUp)
        il.Mark reduce
        load scale; il.EmitInt64 28L; il.Emit OpCodes.Sub; save remove
        let haveReduction = il.Label()
        load remove; il.EmitInt64 0L; il.EmitBranch(OpCodes.Bge,haveReduction)
        il.EmitInt64 0L; save remove
        il.Mark haveReduction
        let widthReady = il.Label()
        load coefficient.High; il.EmitInt64 0xffffffffL; il.EmitBranch(OpCodes.Ble_Un,widthReady)
        // A 29-digit coefficient exceeds 96 bits only by one decimal digit.
        load remove; il.EmitBranch(OpCodes.Brtrue,widthReady)
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,overflow)
        il.EmitInt64 1L; save remove
        il.Mark widthReady
        let rounded = il.Label()
        let divide = il.Label()
        load remove; load kept; il.Emit OpCodes.Conv_I8; il.EmitBranch(OpCodes.Bgt,zero)
        il.Mark divide
        load remove; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,rounded)
        load sticky; load guard; il.Emit OpCodes.Or; save sticky
        let remainder = CodecDecimalArithmeticEmitter.divide10 il api.UInt64 coefficient
        load remainder; il.Emit OpCodes.Conv_I4; save guard
        load scale; il.EmitInt64 1L; il.Emit OpCodes.Sub; save scale
        load remove; il.EmitInt64 1L; il.Emit OpCodes.Sub; save remove
        il.EmitBranch(OpCodes.Br,divide)
        il.Mark rounded
        let roundUp = il.Label()
        load guard; intConst 5; il.EmitBranch(OpCodes.Bgt,roundUp)
        load guard; intConst 5; il.EmitBranch(OpCodes.Blt,normalize)
        load sticky; il.EmitBranch(OpCodes.Brtrue,roundUp)
        load coefficient.Low; il.EmitInt64 1L; il.Emit OpCodes.And; il.EmitBranch(OpCodes.Brfalse,normalize)
        il.Mark roundUp
        load coefficient.Low; save savedLow
        load coefficient.High; save savedHigh
        load coefficient.Low; il.EmitInt64 1L; il.Emit OpCodes.Add; save coefficient.Low
        load coefficient.Low
        let noCarry = il.Label()
        il.EmitBranch(OpCodes.Brtrue,noCarry)
        load coefficient.High; il.EmitInt64 1L; il.Emit OpCodes.Add; save coefficient.High
        il.Mark noCarry
        load coefficient.High; il.EmitInt64 0xffffffffL; il.EmitBranch(OpCodes.Ble_Un,normalize)
        load savedLow; save coefficient.Low
        load savedHigh; save coefficient.High
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,overflow)
        il.EmitInt64 1L; save remove
        il.EmitBranch(OpCodes.Br,divide)
        il.Mark zero
        il.EmitInt64 0L; save coefficient.Low
        il.EmitInt64 0L; save coefficient.High
        load scale; il.EmitInt64 0L
        let nonnegative = il.Label()
        il.EmitBranch(OpCodes.Bge,nonnegative)
        il.EmitInt64 0L; save scale
        il.Mark nonnegative
        load scale; il.EmitInt64 28L; il.EmitBranch(OpCodes.Ble,normalize)
        il.EmitInt64 28L; save scale
        il.Mark normalize
        if utf16 && compatibility.IsNone then
            let trim = il.Label()
            load coefficient.Low; load coefficient.High; il.Emit OpCodes.Or; il.EmitBranch(OpCodes.Brtrue,trim)
            il.EmitInt64 0L; save scale
            il.EmitBranch(OpCodes.Br,construct)
            il.Mark trim
            load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,construct)
            load coefficient.Low; save savedLow
            load coefficient.High; save savedHigh
            let remainder = CodecDecimalArithmeticEmitter.divide10 il api.UInt64 coefficient
            let exact = il.Label()
            load remainder; il.EmitBranch(OpCodes.Brfalse,exact)
            load savedLow; save coefficient.Low
            load savedHigh; save coefficient.High
            il.EmitBranch(OpCodes.Br,construct)
            il.Mark exact
            load scale; il.EmitInt64 1L; il.Emit OpCodes.Sub; save scale
            il.EmitBranch(OpCodes.Br,trim)
        else il.EmitBranch(OpCodes.Br,construct)
        il.Mark overflow
        match compatibility with
        | Some method -> il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,method); il.Emit OpCodes.Ret
        | None ->
            il.EmitMethod(OpCodes.Newobj,api.Overflow); il.Emit OpCodes.Throw
        il.Mark construct
        CodecNumberEmitter.publish il state
        load coefficient.Low; il.Emit OpCodes.Conv_I4
        load coefficient.Low; intConst 32; il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Conv_I4
        load coefficient.High; il.Emit OpCodes.Conv_I4
        load state.Negative
        load scale; il.Emit OpCodes.Conv_U1
        il.EmitMethod(OpCodes.Newobj,api.Constructor)
        il.Emit OpCodes.Ret
        il.Mark invalid
        match compatibility with
        | Some method -> il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,method); il.Emit OpCodes.Ret
        | None -> il.EmitString "Invalid number"; il.EmitMethod(OpCodes.Newobj,api.InvalidNumber); il.Emit OpCodes.Throw
