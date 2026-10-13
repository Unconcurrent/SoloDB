namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecBigIntegerApi<'Type,'Method> = {
    Magnitude: CodecMagnitudeApi<'Type,'Method>
    Int64: 'Type
    OffsetByte: 'Method
    Construct: 'Method
    InvalidNumber: 'Method
    InvalidHex: 'Method
}

module internal CodecBigIntegerEmitter =
    let private finish (il: CodecIL<'T,'M,'F>) api magnitude negative =
        il.EmitLocal(OpCodes.Ldloc,magnitude.Small)
        il.EmitLocal(OpCodes.Ldloc,magnitude.Limbs)
        il.EmitLocal(OpCodes.Ldloc,magnitude.Used)
        il.EmitLocal(OpCodes.Ldloc,negative)
        il.EmitMethod(OpCodes.Call,api.Construct)
        il.Emit OpCodes.Ret

    let decimalReader (il: CodecIL<'T,'M,'F>) (api: CodecBigIntegerApi<'T,'M>) utf16 =
        let arithmetic = api.Magnitude
        let state = CodecNumberEmitter.locals il arithmetic.Int32 api.Int64
        let magnitude = CodecMagnitudeEmitter.locals il arithmetic
        let chunk = il.Local arithmetic.Int32
        let digits = il.Local arithmetic.Int32
        let factor = il.Local arithmetic.Int32
        let scale = il.Local api.Int64
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let invalid = il.Label()
        let multiply () =
            CodecMagnitudeEmitter.multiplyAdd il arithmetic magnitude (fun () -> load factor) (fun () -> load chunk)
            constant 0; save chunk; constant 0; save digits; constant 1; save factor
        constant 1; save factor
        let collect _ =
            load chunk; constant 10; il.Emit OpCodes.Mul; load state.Digit; il.Emit OpCodes.Add; save chunk
            load factor; constant 10; il.Emit OpCodes.Mul; save factor
            load digits; constant 1; il.Emit OpCodes.Add; save digits
            let pending = il.Label()
            load digits; constant 9; il.EmitBranch(OpCodes.Bne_Un,pending)
            multiply()
            il.Mark pending
        CodecNumberEmitter.scan il arithmetic.Int32 api.Int64 api.OffsetByte utf16 (int64 System.Int32.MaxValue) state invalid 0 collect
        let flushed = il.Label()
        load digits; il.EmitBranch(OpCodes.Brfalse,flushed)
        multiply()
        il.Mark flushed
        load state.Exponent; load state.IntegerDigits; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Add
        load state.AllDigits; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub; save scale
        let scaled = il.Label()
        let negativeScale = il.Label()
        let extend = il.Label()
        load magnitude.Used; il.EmitBranch(OpCodes.Brfalse,scaled)
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Blt,negativeScale)
        il.Mark extend
        load scale; il.EmitInt64 9L; il.EmitBranch(OpCodes.Blt,scaled)
        CodecMagnitudeEmitter.multiplyAdd il arithmetic magnitude (fun () -> constant 1000000000) (fun () -> constant 0)
        load scale; il.EmitInt64 9L; il.Emit OpCodes.Sub; save scale
        il.EmitBranch(OpCodes.Br,extend)
        il.Mark negativeScale
        let reduce = il.Label()
        il.Mark reduce
        load magnitude.Used; il.EmitBranch(OpCodes.Brfalse,scaled)
        load scale; il.EmitInt64 -9L; il.EmitBranch(OpCodes.Bgt,scaled)
        CodecMagnitudeEmitter.divide il arithmetic magnitude (fun () -> constant 1000000000)
        load scale; il.EmitInt64 9L; il.Emit OpCodes.Add; save scale
        il.EmitBranch(OpCodes.Br,reduce)
        il.Mark scaled
        let result = il.Label()
        load magnitude.Used; il.EmitBranch(OpCodes.Brfalse,result)
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Beq,result)
        constant 1; save factor
        let positiveScale = il.Label()
        let remainder = il.Label()
        let ready = il.Label()
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Bge,positiveScale)
        il.EmitInt64 0L; load scale; il.Emit OpCodes.Sub; save scale
        constant 1; save digits; il.EmitBranch(OpCodes.Br,remainder)
        il.Mark positiveScale
        constant 0; save digits
        il.Mark remainder
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Beq,ready)
        load factor; constant 10; il.Emit OpCodes.Mul; save factor
        load scale; il.EmitInt64 1L; il.Emit OpCodes.Sub; save scale
        il.EmitBranch(OpCodes.Br,remainder)
        il.Mark ready
        let extendTail = il.Label()
        load digits; il.EmitBranch(OpCodes.Brfalse,extendTail)
        CodecMagnitudeEmitter.divide il arithmetic magnitude (fun () -> load factor)
        il.EmitBranch(OpCodes.Br,result)
        il.Mark extendTail
        CodecMagnitudeEmitter.multiplyAdd il arithmetic magnitude (fun () -> load factor) (fun () -> constant 0)
        il.Mark result
        CodecNumberEmitter.publish il state
        finish il api magnitude state.Negative
        il.Mark invalid
        il.EmitString "Invalid number literal"; il.EmitMethod(OpCodes.Newobj,api.InvalidNumber); il.Emit OpCodes.Throw

    let hexMagnitude (il: CodecIL<'T,'M,'F>) (api: CodecBigIntegerApi<'T,'M>) utf16 =
        let arithmetic = api.Magnitude
        let magnitude = CodecMagnitudeEmitter.locals il arithmetic
        let position = il.Local arithmetic.Int32
        let character = il.Local arithmetic.Int32
        let negative = il.Local arithmetic.Int32
        let digit = il.Local arithmetic.Int32
        let count = il.Local arithmetic.Int32
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
        CodecMagnitudeEmitter.multiplyAdd il arithmetic magnitude (fun () -> constant 16) (fun () -> load digit)
        load count; constant 1; il.Emit OpCodes.Add; save count
        load position; constant 1; il.Emit OpCodes.Add; save position
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark complete
        load count; il.EmitBranch(OpCodes.Brfalse,invalid)
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        let valid = il.Label()
        il.EmitBranch(OpCodes.Br,valid)
        il.Mark invalid
        il.EmitString "Invalid hex literal"; il.EmitMethod(OpCodes.Newobj,api.InvalidHex); il.Emit OpCodes.Throw
        il.Mark valid
        magnitude,negative

    let hexReader (il: CodecIL<'T,'M,'F>) (api: CodecBigIntegerApi<'T,'M>) utf16 =
        let magnitude,negative = hexMagnitude il api utf16
        finish il api magnitude negative
