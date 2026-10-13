namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecFloatingReadApi<'Type,'Method> = {
    Result: 'Type
    Bits: 'Type
    Int32: 'Type
    Int64: 'Type
    UInt64: 'Type
    OffsetByte: 'Method
    RoundedBits: 'Method
    ResultReference: 'Method
    Ambiguous: 'Method
    InvalidNumber: 'Method
}

module internal CodecFloatingReadEmitter =
    let hex (il: CodecIL<'T,'M,'F>) (integer: CodecIntegerApi<'T,'M>) (api: CodecFloatingReadApi<'T,'M>) width utf16 =
        let precision = if width = 64 then 53 elif width = 16 then 11 else 24
        let fractionBits = precision - 1
        let bias = if width = 64 then 1023 elif width = 16 then 15 else 127
        let position = il.Local api.Int32
        let character = il.Local api.Int32
        let digit = il.Local api.Int32
        let count = il.Local api.Int32
        let significant = il.Local api.Int32
        let leadingBits = il.Local api.Int32
        let negative = il.Local api.Int32
        let tail = il.Local api.Int32
        let prefixValue = il.Local api.UInt64
        let exponent = il.Local api.Int64
        let shift = il.Local api.Int32
        let boundary = il.Local api.UInt64
        let remainder = il.Local api.UInt64
        let rounded = il.Local api.UInt64
        let packed = il.Local api.UInt64
        let result = il.Local api.Bits
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
        let next = il.Label()
        let first = il.Label()
        let retained = il.Label()
        let finished = il.Label()
        let invalid = il.Label()
        il.Mark loop
        load position; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,finished)
        read()
        load character; constant 48; il.Emit OpCodes.Sub; save digit
        load digit; constant 9; il.EmitBranch(OpCodes.Ble_Un,collect)
        load character; constant 32; il.Emit OpCodes.Or; constant 97; il.Emit OpCodes.Sub; save digit
        load digit; constant 5; il.EmitBranch(OpCodes.Bgt_Un,finished)
        load digit; constant 10; il.Emit OpCodes.Add; save digit
        il.Mark collect
        load count; constant 1; il.Emit OpCodes.Add; save count
        load significant; il.EmitBranch(OpCodes.Brtrue,first)
        load digit; il.EmitBranch(OpCodes.Brfalse,next)
        constant 1; save leadingBits
        for threshold in [2;4;8] do
            let below = il.Label()
            load digit; constant threshold; il.EmitBranch(OpCodes.Blt,below)
            load leadingBits; constant 1; il.Emit OpCodes.Add; save leadingBits
            il.Mark below
        il.Mark first
        load significant; constant 1; il.Emit OpCodes.Add; save significant
        load significant; constant 16; il.EmitBranch(OpCodes.Ble,retained)
        load tail; load digit; il.Emit OpCodes.Or; save tail; il.EmitBranch(OpCodes.Br,next)
        il.Mark retained
        load prefixValue; constant 4; il.Emit OpCodes.Shl
        load digit; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or; save prefixValue
        il.Mark next
        load position; constant 1; il.Emit OpCodes.Add; save position
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark finished
        load count; il.EmitBranch(OpCodes.Brfalse,invalid)
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        let signResult = il.Label()
        let infinity = il.Label()
        let aligned = il.Label()
        let round = il.Label()
        load significant; il.EmitBranch(OpCodes.Brfalse,signResult)
        load significant; constant 1; il.Emit OpCodes.Sub; il.Emit OpCodes.Conv_I8
        il.EmitInt64 4L; il.Emit OpCodes.Mul
        load leadingBits; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Add
        il.EmitInt64 1L; il.Emit OpCodes.Sub; save exponent
        load exponent; il.EmitInt64(int64 bias); il.EmitBranch(OpCodes.Bgt,infinity)
        load significant; save shift
        let short = il.Label()
        load shift; constant 16; il.EmitBranch(OpCodes.Ble,short)
        constant 16; save shift
        il.Mark short
        load shift; constant 1; il.Emit OpCodes.Sub; constant 4; il.Emit OpCodes.Mul
        load leadingBits; il.Emit OpCodes.Add; constant precision; il.Emit OpCodes.Sub; save shift
        load shift; constant 0; il.EmitBranch(OpCodes.Bgt,round)
        load prefixValue; constant 0; load shift; il.Emit OpCodes.Sub; il.Emit OpCodes.Shl; save rounded
        il.EmitBranch(OpCodes.Br,aligned)
        il.Mark round
        il.EmitInt64 1L; load shift; constant 1; il.Emit OpCodes.Sub; il.Emit OpCodes.Shl; save boundary
        load prefixValue; load boundary; il.EmitInt64 1L; il.Emit OpCodes.Sub; il.Emit OpCodes.And; save remainder
        load prefixValue; load shift; il.Emit OpCodes.Shr_Un; save rounded
        let roundedDone = il.Label()
        load prefixValue; load boundary; il.Emit OpCodes.And; il.EmitBranch(OpCodes.Brfalse,roundedDone)
        load remainder; load tail; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or
        load rounded; il.EmitInt64 1L; il.Emit OpCodes.And; il.Emit OpCodes.Or
        il.EmitBranch(OpCodes.Brfalse,roundedDone)
        load rounded; il.EmitInt64 1L; il.Emit OpCodes.Add; save rounded
        il.Mark roundedDone
        load rounded; il.EmitInt64(1L <<< precision); il.EmitBranch(OpCodes.Bne_Un,aligned)
        load rounded; constant 1; il.Emit OpCodes.Shr_Un; save rounded
        load exponent; il.EmitInt64 1L; il.Emit OpCodes.Add; save exponent
        load exponent; il.EmitInt64(int64 bias); il.EmitBranch(OpCodes.Bgt,infinity)
        il.Mark aligned
        load exponent; il.EmitInt64(int64 bias); il.Emit OpCodes.Add
        constant fractionBits; il.Emit OpCodes.Shl
        load rounded; il.EmitInt64((1L <<< fractionBits)-1L); il.Emit OpCodes.And; il.Emit OpCodes.Or; save packed
        il.EmitBranch(OpCodes.Br,signResult)
        il.Mark infinity
        il.EmitInt64((int64 (2*bias+1)) <<< fractionBits); save packed
        il.Mark signResult
        load packed; load negative; il.Emit OpCodes.Conv_U8; constant(width-1); il.Emit OpCodes.Shl; il.Emit OpCodes.Or
        if width <> 64 then il.Emit OpCodes.Conv_U4
        save result
        il.EmitLocal(OpCodes.Ldloca,result); il.EmitMethod(OpCodes.Call,api.ResultReference)
        il.EmitType(OpCodes.Ldobj,api.Result); il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid hex literal"; il.EmitMethod(OpCodes.Newobj,integer.InvalidHex); il.Emit OpCodes.Throw

    /// A raw input leaf with a fourth context argument reserved for genuine midpoint ambiguity.
    let reader (il: CodecIL<'T,'M,'F>) (api: CodecFloatingReadApi<'T,'M>) width utf16 quoted =
        let state = CodecNumberEmitter.locals il api.Int32 api.Int64
        let prefix = il.Local api.UInt64
        let kept = il.Local api.Int32
        let tail = il.Local api.Int32
        let exponent = il.Local api.Int64
        let result = il.Local api.Bits
        let invalid = il.Label()
        let complete = il.Label()
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let intConst value = il.EmitInt(OpCodes.Ldc_I4,value)
        let collect knownPrefix =
            let retain () =
                load prefix; il.EmitInt64 10L; il.Emit OpCodes.Mul
                load state.Digit; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Add; save prefix
                load kept; intConst 1; il.Emit OpCodes.Add; save kept
            if knownPrefix then retain()
            else
                let discarded = il.Label()
                let collected = il.Label()
                load kept; intConst 19; il.EmitBranch(OpCodes.Bge,discarded)
                retain()
                il.EmitBranch(OpCodes.Br,collected)
                il.Mark discarded
                load tail; load state.Digit; il.Emit OpCodes.Or; save tail
                il.Mark collected
        CodecNumberEmitter.scan il api.Int32 api.Int64 api.OffsetByte utf16 400L state invalid 19 collect
        load state.IntegerDigits; il.Emit OpCodes.Conv_I8
        load state.LeadingZeros; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub
        load kept; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub
        load state.Exponent; il.Emit OpCodes.Add; save exponent
        // Clinger: both operands are exact in the requested format, so one native operation rounds once.
        if width = 32 || width = 64 then
            let slow = il.Label()
            let ready = il.Label()
            let positive = il.Label()
            let native = il.Local api.Result
            let precision,maximumPower = if width = 32 then 24,10 else 53,22
            let factor power =
                let exact = pown 10.0 power
                if width = 32 then il.EmitFloat32(single exact) else il.EmitFloat64 exact
            load tail; il.EmitBranch(OpCodes.Brtrue,slow)
            load prefix; il.EmitInt64(1L <<< precision); il.EmitBranch(OpCodes.Bgt_Un,slow)
            load exponent; il.EmitInt64(int64 -maximumPower); il.EmitBranch(OpCodes.Blt,slow)
            load exponent; il.EmitInt64(int64 maximumPower); il.EmitBranch(OpCodes.Bgt,slow)
            load prefix; il.Emit(if width = 32 then OpCodes.Conv_R4 else OpCodes.Conv_R8); save native
            load exponent; il.EmitBranch(OpCodes.Brfalse,ready)
            let cases = Array.init (2 * maximumPower + 1) (fun _ -> il.Label())
            load exponent; il.Emit OpCodes.Conv_I4; intConst maximumPower; il.Emit OpCodes.Add
            il.EmitSwitch cases
            il.EmitBranch(OpCodes.Br,slow)
            for power in -maximumPower..maximumPower do
                il.Mark cases[power + maximumPower]
                if power <> 0 then
                    load native; factor (abs power)
                    il.Emit(if power < 0 then OpCodes.Div else OpCodes.Mul); save native
                il.EmitBranch(OpCodes.Br,ready)
            il.Mark ready
            load state.Negative; il.EmitBranch(OpCodes.Brfalse,positive)
            load native; il.Emit OpCodes.Neg; save native
            il.Mark positive
            CodecNumberEmitter.publish il state
            load native; il.Emit OpCodes.Ret
            il.Mark slow
        load prefix; load exponent; load state.Negative
        il.EmitMethod(OpCodes.Call,api.RoundedBits); save result
        load tail; il.EmitBranch(OpCodes.Brfalse,complete)
        // A nonzero discarded suffix lies strictly between these two exact endpoints.
        load prefix; il.EmitInt64 1L; il.Emit OpCodes.Add
        load exponent; load state.Negative
        il.EmitMethod(OpCodes.Call,api.RoundedBits)
        load result; il.EmitBranch(OpCodes.Beq,complete)
        il.Emit OpCodes.Ldarg_3
        il.EmitMethod(OpCodes.Call,api.Ambiguous)
        il.Emit OpCodes.Ret
        il.Mark complete
        CodecNumberEmitter.publish il state
        il.EmitLocal(OpCodes.Ldloca,result)
        il.EmitMethod(OpCodes.Call,api.ResultReference)
        il.EmitType(OpCodes.Ldobj,api.Result)
        il.Emit OpCodes.Ret
        il.Mark invalid
        if quoted then
            il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,api.Ambiguous); il.Emit OpCodes.Ret
        else
            il.EmitString "Invalid number"
            il.EmitMethod(OpCodes.Newobj,api.InvalidNumber)
            il.Emit OpCodes.Throw
