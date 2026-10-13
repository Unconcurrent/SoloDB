namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecNumericTextApi<'Type,'Method> = {
    Number: CodecIntegerApi<'Type,'Method>
    String: 'Type
    Reference: 'Type
    Construct: 'Method
    StringReference: 'Method
}

module internal CodecNumericTextEmitter =
    let decimalReader (il: CodecIL<'T,'M,'F>) (api: CodecNumericTextApi<'T,'M>) utf16 =
        let number = api.Number
        let state = CodecNumberEmitter.locals il number.Int32 number.Int64
        let start = il.Local number.Int32
        let digits = il.Local number.Int32
        let point = il.Local number.Int64
        let length = il.Local number.Int64
        let scientific = il.Local number.Int32
        let exponent = il.Local number.UInt64
        let exponentNegative = il.Local number.Int32
        let exponentDigits = il.Local number.Int32
        let divisor = il.Local number.UInt64
        let result = il.Local api.String
        let reference = il.Local api.Reference
        let output = il.Local number.Int32
        let input = il.Local number.Int32
        let visited = il.Local number.Int32
        let written = il.Local number.Int32
        let character = il.Local number.Int32
        let remaining = il.Local number.Int64
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let increment n = load n; constant 1; il.Emit OpCodes.Add; save n
        let invalid = il.Label()
        let zero = il.Label()
        let allocate = il.Label()
        let returnValue = il.Label()
        let rawExponent = il.Label()
        let store value =
            load reference; load output; CodecAddressEmitter.offset il number.OffsetByte 2 0
            value(); il.Emit OpCodes.Stind_I2; increment output
        il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldind_I4; save start
        CodecNumberEmitter.scan il number.Int32 number.Int64 number.OffsetByte utf16 (int64 System.Int32.MaxValue) state invalid 0 (fun _ -> ())
        CodecNumberEmitter.publish il state
        load state.SignificantDigits; il.EmitBranch(OpCodes.Brfalse,zero)
        load state.Exponent; save remaining
        let positiveScale = il.Label()
        load remaining; il.EmitInt64 0L; il.EmitBranch(OpCodes.Bge,positiveScale)
        il.EmitInt64 0L; load remaining; il.Emit OpCodes.Sub; save remaining
        il.Mark positiveScale
        load remaining; load state.AllDigits; il.Emit OpCodes.Conv_I8
        il.EmitInt64(int64 System.Int32.MaxValue); il.Emit OpCodes.Add
        il.EmitBranch(OpCodes.Bge,rawExponent)
        load state.SignificantDigits; save digits
        load state.IntegerDigits; il.Emit OpCodes.Conv_I8
        load state.LeadingZeros; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub
        load state.Exponent; il.Emit OpCodes.Add; save point
        let positivePoint = il.Label()
        let trailing = il.Label()
        let sized = il.Label()
        load point; il.EmitInt64 0L; il.EmitBranch(OpCodes.Bgt,positivePoint)
        il.EmitInt64 2L; load point; il.Emit OpCodes.Sub
        load digits; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Add; save length
        il.EmitBranch(OpCodes.Br,sized)
        il.Mark positivePoint
        load point; load digits; il.Emit OpCodes.Conv_I8; il.EmitBranch(OpCodes.Bge,trailing)
        load digits; il.Emit OpCodes.Conv_I8; il.EmitInt64 1L; il.Emit OpCodes.Add; save length
        il.EmitBranch(OpCodes.Br,sized)
        il.Mark trailing
        load point; save length
        il.Mark sized
        load length; load state.AllDigits; il.Emit OpCodes.Conv_I8; il.EmitInt64 28L; il.Emit OpCodes.Add
        let fixedPoint = il.Label()
        il.EmitBranch(OpCodes.Ble,fixedPoint)
        constant 1; save scientific
        load point; il.EmitInt64 1L; il.Emit OpCodes.Sub; save remaining
        load remaining; il.EmitInt64 0L; il.Emit OpCodes.Clt; save exponentNegative
        let positiveExponent = il.Label()
        load exponentNegative; il.EmitBranch(OpCodes.Brfalse,positiveExponent)
        il.EmitInt64 0L; load remaining; il.Emit OpCodes.Sub; save remaining
        il.Mark positiveExponent
        load remaining; save exponent
        constant 1; save exponentDigits
        il.EmitInt64 1L; save divisor
        let exponentSize = il.Label()
        let exponentSized = il.Label()
        il.Mark exponentSize
        load remaining; il.EmitInt64 10L; il.EmitBranch(OpCodes.Blt_Un,exponentSized)
        load remaining; il.EmitInt64 10L; il.Emit OpCodes.Div_Un; save remaining
        increment exponentDigits
        load divisor; il.EmitInt64 10L; il.Emit OpCodes.Mul; save divisor
        il.EmitBranch(OpCodes.Br,exponentSize)
        il.Mark exponentSized
        load digits; il.Emit OpCodes.Conv_I8; il.EmitInt64 2L; il.Emit OpCodes.Add
        load exponentDigits; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Add; save length
        let oneDigit = il.Label()
        load digits; constant 1; il.EmitBranch(OpCodes.Beq,oneDigit)
        load length; il.EmitInt64 1L; il.Emit OpCodes.Add; save length
        il.Mark oneDigit
        il.EmitInt64 1L; save point
        il.Mark fixedPoint
        load length; load state.Negative; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Add; save length
        il.EmitBranch(OpCodes.Br,allocate)
        il.Mark zero
        // Retain ordinary zero scale without expanding an astronomical zero exponent.
        load state.AllDigits; load state.IntegerDigits; il.Emit OpCodes.Sub; il.Emit OpCodes.Conv_I8
        load state.Exponent; il.Emit OpCodes.Sub; save remaining
        let plainZero = il.Label()
        load remaining; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,plainZero)
        load remaining; il.EmitInt64 28L; il.EmitBranch(OpCodes.Bgt,plainZero)
        load remaining; il.EmitInt64 2L; il.Emit OpCodes.Add; save length
        il.EmitBranch(OpCodes.Br,allocate)
        il.Mark plainZero
        il.EmitInt64 1L; save length
        il.Mark allocate
        constant 0; load length; il.Emit OpCodes.Conv_Ovf_I4; il.EmitMethod(OpCodes.Newobj,api.Construct); save result
        load result; il.EmitMethod(OpCodes.Call,api.StringReference); save reference
        let nonzero = il.Label()
        load state.SignificantDigits; il.EmitBranch(OpCodes.Brtrue,nonzero)
        store(fun () -> constant 48)
        load length; il.EmitInt64 1L; il.EmitBranch(OpCodes.Beq,returnValue)
        store(fun () -> constant 46)
        let zeroLoop = il.Label()
        il.Mark zeroLoop
        load output; il.Emit OpCodes.Conv_I8; load length; il.EmitBranch(OpCodes.Bge,returnValue)
        store(fun () -> constant 48); il.EmitBranch(OpCodes.Br,zeroLoop)
        il.Mark nonzero
        let unsigned = il.Label()
        load state.Negative; il.EmitBranch(OpCodes.Brfalse,unsigned)
        store(fun () -> constant 45)
        il.Mark unsigned
        let coefficient = il.Label()
        load point; il.EmitInt64 0L; il.EmitBranch(OpCodes.Bgt,coefficient)
        store(fun () -> constant 48); store(fun () -> constant 46)
        il.EmitInt64 0L; load point; il.Emit OpCodes.Sub; save remaining
        let leading = il.Label()
        il.Mark leading
        load remaining; il.EmitBranch(OpCodes.Brfalse,coefficient)
        store(fun () -> constant 48)
        load remaining; il.EmitInt64 1L; il.Emit OpCodes.Sub; save remaining
        il.EmitBranch(OpCodes.Br,leading)
        il.Mark coefficient
        load start; save input
        let read = il.Label()
        let skip = il.Label()
        let next = il.Label()
        let coefficientDone = il.Label()
        il.Mark read
        load written; load digits; il.EmitBranch(OpCodes.Bge,coefficientDone)
        il.Emit OpCodes.Ldarg_0; load input
        CodecAddressEmitter.offset il number.OffsetByte (if utf16 then 2 else 1) 0
        il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save character
        increment input
        load character; constant 48; il.Emit OpCodes.Sub; constant 9; il.EmitBranch(OpCodes.Bgt_Un,read)
        load visited; load state.LeadingZeros; il.EmitBranch(OpCodes.Blt,skip)
        load point; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,next)
        load written; il.Emit OpCodes.Conv_I8; load point; il.EmitBranch(OpCodes.Bne_Un,next)
        store(fun () -> constant 46)
        il.Mark next
        store(fun () -> load character); increment written
        il.EmitBranch(OpCodes.Br,read)
        il.Mark skip
        increment visited; il.EmitBranch(OpCodes.Br,read)
        il.Mark coefficientDone
        let suffix = il.Label()
        load scientific; il.EmitBranch(OpCodes.Brtrue,suffix)
        load point; load digits; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub; save remaining
        let trailingLoop = il.Label()
        il.Mark trailingLoop
        load remaining; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,returnValue)
        store(fun () -> constant 48)
        load remaining; il.EmitInt64 1L; il.Emit OpCodes.Sub; save remaining
        il.EmitBranch(OpCodes.Br,trailingLoop)
        il.Mark suffix
        store(fun () -> constant 69)
        let plus = il.Label()
        let exponentBody = il.Label()
        load exponentNegative; il.EmitBranch(OpCodes.Brfalse,plus)
        store(fun () -> constant 45); il.EmitBranch(OpCodes.Br,exponentBody)
        il.Mark plus
        store(fun () -> constant 43)
        il.Mark exponentBody
        store(fun () -> load exponent; load divisor; il.Emit OpCodes.Div_Un; il.Emit OpCodes.Conv_I4; constant 48; il.Emit OpCodes.Add)
        load exponent; load divisor; il.Emit OpCodes.Rem_Un; save exponent
        load divisor; il.EmitInt64 10L; il.Emit OpCodes.Div_Un; save divisor
        load divisor; il.EmitBranch(OpCodes.Brtrue,exponentBody)
        il.Mark returnValue
        load result; il.Emit OpCodes.Ret
        il.Mark rawExponent
        // Retain an unbounded exponent spelling instead of converting a saturated exponent.
        state.Position |> load; load start; il.Emit OpCodes.Sub; save input
        constant 0; load input; il.EmitMethod(OpCodes.Newobj,api.Construct); save result
        load result; il.EmitMethod(OpCodes.Call,api.StringReference); save reference
        constant 0; save output
        let rawLoop = il.Label()
        let rawDone = il.Label()
        il.Mark rawLoop
        load output; load input; il.EmitBranch(OpCodes.Bge,rawDone)
        store(fun () ->
            il.Emit OpCodes.Ldarg_0; load start; load output; il.Emit OpCodes.Add
            CodecAddressEmitter.offset il number.OffsetByte (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1))
        il.EmitBranch(OpCodes.Br,rawLoop)
        il.Mark rawDone
        load result; il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid number literal"; il.EmitMethod(OpCodes.Newobj,number.InvalidNumber); il.Emit OpCodes.Throw

    let hexReader (il: CodecIL<'T,'M,'F>) (api: CodecNumericTextApi<'T,'M>) (arithmetic: CodecMagnitudeApi<'T,'M>) uint32Array utf16 =
        let number = api.Number
        let magnitude = CodecMagnitudeEmitter.locals il arithmetic
        let input = il.Local number.Int32
        let character = il.Local number.Int32
        let digit = il.Local number.Int32
        let count = il.Local number.Int32
        let negative = il.Local number.Int32
        let value = il.Local number.UInt64
        let quotient = il.Local number.UInt64
        let length = il.Local number.Int32
        let result = il.Local api.String
        let reference = il.Local api.Reference
        let position = il.Local number.Int32
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let increment n = load n; constant 1; il.Emit OpCodes.Add; save n
        let read () =
            il.Emit OpCodes.Ldarg_0; load input
            CodecAddressEmitter.offset il number.OffsetByte (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save character
        let hexDigit stopped =
            let admitted = il.Label()
            read()
            load character; constant 48; il.Emit OpCodes.Sub; save digit
            load digit; constant 9; il.EmitBranch(OpCodes.Ble_Un,admitted)
            load character; constant 32; il.Emit OpCodes.Or; constant 97; il.Emit OpCodes.Sub; save digit
            load digit; constant 5; il.EmitBranch(OpCodes.Bgt_Un,stopped)
            load digit; constant 10; il.Emit OpCodes.Add; save digit
            il.Mark admitted
            increment count
        let publish () = il.Emit OpCodes.Ldarg_2; load input; il.Emit OpCodes.Stind_I4
        let invalid = il.Label()
        let nativeLoop = il.Label()
        let nativeDone = il.Label()
        let extend = il.Label()
        let wideLoop = il.Label()
        let wideDone = il.Label()
        il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldind_I4; save input
        read()
        let sign = il.Label()
        let prefix = il.Label()
        load character; constant 43; il.EmitBranch(OpCodes.Beq,sign)
        load character; constant 45; il.EmitBranch(OpCodes.Bne_Un,prefix)
        il.Mark sign
        load character; constant 45; il.Emit OpCodes.Ceq; save negative
        increment input
        il.Mark prefix
        load input; constant 2; il.Emit OpCodes.Add; save input
        il.Mark nativeLoop
        load input; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,nativeDone)
        hexDigit nativeDone
        load value; il.EmitInt64 0x0fffffffffffffffL; il.EmitBranch(OpCodes.Bgt_Un,extend)
        load value; constant 4; il.Emit OpCodes.Shl
        load digit; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or; save value
        increment input; il.EmitBranch(OpCodes.Br,nativeLoop)
        il.Mark extend
        // Only a genuinely overflowing native prefix needs mutable wide storage.
        constant 4; il.EmitType(OpCodes.Newarr,arithmetic.UInt32); save magnitude.Limbs
        load magnitude.Limbs; constant 0; load value; il.Emit OpCodes.Conv_U4; il.Emit OpCodes.Stelem_I4
        load magnitude.Limbs; constant 1; load value; constant 32; il.Emit OpCodes.Shr_Un
        il.Emit OpCodes.Conv_U4; il.Emit OpCodes.Stelem_I4
        load magnitude.Limbs; il.EmitMethod(OpCodes.Call,arithmetic.ReferenceOf); save magnitude.Reference
        constant 2; save magnitude.Used
        // The stopping digit is already classified. Append it before advancing.
        CodecMagnitudeEmitter.multiplyAddInRegion il arithmetic magnitude (fun () -> constant 16) (fun () -> load digit)
        increment input
        il.Mark wideLoop
        load input; il.Emit OpCodes.Ldarg_1; il.EmitBranch(OpCodes.Bge,wideDone)
        hexDigit wideDone
        CodecMagnitudeEmitter.multiplyAdd il arithmetic magnitude (fun () -> constant 16) (fun () -> load digit)
        increment input; il.EmitBranch(OpCodes.Br,wideLoop)
        il.Mark nativeDone
        load count; il.EmitBranch(OpCodes.Brfalse,invalid)
        publish()
        let nonzero = il.Label()
        load value; il.EmitBranch(OpCodes.Brtrue,nonzero)
        il.EmitString "0"; il.Emit OpCodes.Ret
        il.Mark nonzero
        load value; save quotient
        constant 1; save length
        let size = il.Label()
        let sized = il.Label()
        il.Mark size
        load quotient; il.EmitInt64 10L; il.EmitBranch(OpCodes.Blt_Un,sized)
        load quotient; il.EmitInt64 10L; il.Emit OpCodes.Div_Un; save quotient
        increment length; il.EmitBranch(OpCodes.Br,size)
        il.Mark sized
        load length; load negative; il.Emit OpCodes.Add; save length
        constant 0; load length; il.EmitMethod(OpCodes.Newobj,api.Construct); save result
        load result; il.EmitMethod(OpCodes.Call,api.StringReference); save reference
        load length; save position
        let digits = il.Label()
        il.Mark digits
        load position; constant 1; il.Emit OpCodes.Sub; save position
        load reference; load position; CodecAddressEmitter.offset il number.OffsetByte 2 0
        load value; il.EmitInt64 10L; il.Emit OpCodes.Rem_Un; il.Emit OpCodes.Conv_I4
        constant 48; il.Emit OpCodes.Add; il.Emit OpCodes.Stind_I2
        load value; il.EmitInt64 10L; il.Emit OpCodes.Div_Un; save value
        load value; il.EmitBranch(OpCodes.Brtrue,digits)
        let positive = il.Label()
        load negative; il.EmitBranch(OpCodes.Brfalse,positive)
        load reference; constant 45; il.Emit OpCodes.Stind_I2
        il.Mark positive
        load result; il.Emit OpCodes.Ret
        il.Mark wideDone
        publish()
        let chunks = CodecMagnitudeEmitter.decimalChunks il arithmetic number.UInt32 uint32Array magnitude
        load chunks.Index; constant 9; il.Emit OpCodes.Mul_Ovf
        load chunks.Digits; il.Emit OpCodes.Add_Ovf; load negative; il.Emit OpCodes.Add_Ovf; save length
        constant 0; load length; il.EmitMethod(OpCodes.Newobj,api.Construct); save result
        load result; il.EmitMethod(OpCodes.Call,api.StringReference); save reference
        constant 0; save position
        let store value =
            load reference; load position; CodecAddressEmitter.offset il number.OffsetByte 2 0
            value(); il.Emit OpCodes.Stind_I2; increment position
        let widePositive = il.Label()
        load negative; il.EmitBranch(OpCodes.Brfalse,widePositive)
        store(fun () -> constant 45)
        il.Mark widePositive
        CodecMagnitudeEmitter.decimalDigits il chunks store
        load result; il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid hex literal"; il.EmitMethod(OpCodes.Newobj,number.InvalidHex); il.Emit OpCodes.Throw
