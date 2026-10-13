namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecDynamicNumberApi<'Type,'Method> = {
    Number: CodecIntegerApi<'Type,'Method>
    Decimal: CodecDecimalApi<'Type,'Method>
    Floating: CodecFloatingReadApi<'Type,'Method>
    BigInteger: 'Type
    BigReader: 'Method
    Parameter: 'Method option
}

/// Type inference uses numeric facts, not a discarded successful Decimal conversion.
module internal CodecDynamicNumberEmitter =
    let private returnNative (il: CodecIL<'T,'M,'F>) parameter =
        match parameter with
        | Some constructor -> il.EmitInt(OpCodes.Ldc_I4,0); il.EmitMethod(OpCodes.Newobj,constructor)
        | None -> ()
        il.Emit OpCodes.Ret

    let private returnEncoded (il: CodecIL<'T,'M,'F>) constructor =
        il.Emit OpCodes.Ldnull; il.EmitInt(OpCodes.Ldc_I4,1); il.EmitMethod(OpCodes.Newobj,constructor); il.Emit OpCodes.Ret

    let private decimalValue (il: CodecIL<'T,'M,'F>) (api: CodecDecimalApi<'T,'M>) parameter coefficient negative scale =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        load coefficient.Low; il.Emit OpCodes.Conv_I4
        load coefficient.Low; il.EmitInt(OpCodes.Ldc_I4,32); il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Conv_I4
        load coefficient.High; il.Emit OpCodes.Conv_I4
        load negative; scale(); il.Emit OpCodes.Conv_U1
        il.EmitMethod(OpCodes.Newobj,api.Constructor)
        il.EmitType(OpCodes.Box,api.Result); returnNative il parameter

    let private integerOrDecimal (il: CodecIL<'T,'M,'F>) (api: CodecDecimalApi<'T,'M>) parameter coefficient negative =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let decimal = il.Label()
        let positive = il.Label()
        load coefficient.High; il.EmitBranch(OpCodes.Brtrue,decimal)
        load coefficient.Low; il.EmitInt64 System.Int64.MaxValue; il.EmitBranch(OpCodes.Bge_Un,decimal)
        load negative; il.EmitBranch(OpCodes.Brfalse,positive)
        il.EmitInt64 0L; load coefficient.Low; il.Emit OpCodes.Sub
        il.EmitType(OpCodes.Box,api.Int64); returnNative il parameter
        il.Mark positive
        load coefficient.Low; il.EmitType(OpCodes.Box,api.Int64); returnNative il parameter
        il.Mark decimal
        decimalValue il api parameter coefficient negative (fun () -> il.EmitInt(OpCodes.Ldc_I4,0))

    let decimalReader (il: CodecIL<'T,'M,'F>) (api: CodecDynamicNumberApi<'T,'M>) utf16 =
        let number = api.Decimal
        let floating = api.Floating
        let state = CodecNumberEmitter.locals il number.Int32 number.Int64
        let coefficient = { Low = il.Local number.UInt64; High = il.Local number.UInt64 }
        let kept = il.Local number.Int32
        let trailingZeros = il.Local number.Int32
        let prefix = il.Local number.UInt64
        let tail = il.Local number.Int32
        let effective = il.Local number.Int32
        let scale = il.Local number.Int64
        let point = il.Local number.Int64
        let exponent = il.Local number.Int64
        let bits = il.Local floating.Bits
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let increment n = load n; constant 1; il.Emit OpCodes.Add; save n
        let invalid = il.Label()
        let zero = il.Label()
        let binary = il.Label()
        let big = il.Label()
        let collect knownPrefix =
            let nonzero = il.Label()
            let collectPrefix = il.Label()
            load state.Digit; il.EmitBranch(OpCodes.Brtrue,nonzero)
            increment trailingZeros; il.EmitBranch(OpCodes.Br,collectPrefix)
            il.Mark nonzero
            constant 0; save trailingZeros
            il.Mark collectPrefix
            let retainLow () =
                load coefficient.Low; il.EmitInt64 10L; il.Emit OpCodes.Mul
                load state.Digit; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Add; save coefficient.Low
                increment kept
            if knownPrefix then retainLow()
            else
                let large = il.Label()
                let complete = il.Label()
                load kept; constant 19; il.EmitBranch(OpCodes.Bge,large)
                retainLow(); il.EmitBranch(OpCodes.Br,complete)
                il.Mark large
                let captured = il.Label()
                load state.SignificantDigits; constant 20; il.EmitBranch(OpCodes.Bne_Un,captured)
                load coefficient.Low; save prefix
                il.Mark captured
                load tail; load state.Digit; il.Emit OpCodes.Or; save tail
                load kept; constant 29; il.EmitBranch(OpCodes.Bge,complete)
                CodecDecimalArithmeticEmitter.append il number.UInt64 coefficient (Some state.Digit)
                increment kept
                il.Mark complete
        CodecNumberEmitter.scan il number.Int32 number.Int64 number.OffsetByte utf16 32768L state invalid 19 collect
        let widePrefix = il.Label()
        load state.SignificantDigits; constant 19; il.EmitBranch(OpCodes.Bgt,widePrefix)
        load coefficient.Low; save prefix
        il.Mark widePrefix
        load state.SignificantDigits; il.EmitBranch(OpCodes.Brfalse,zero)
        load state.SignificantDigits; load trailingZeros; il.Emit OpCodes.Sub; save effective
        load state.IntegerDigits; il.Emit OpCodes.Conv_I8
        load state.LeadingZeros; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub
        load state.Exponent; il.Emit OpCodes.Add; save point
        load effective; il.Emit OpCodes.Conv_I8; load point; il.Emit OpCodes.Sub; save scale
        let integral = il.Label()
        let decimal = il.Label()
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Ble,integral)
        load scale; il.EmitInt64 28L; il.EmitBranch(OpCodes.Bgt,binary)
        load effective; constant 29; il.EmitBranch(OpCodes.Bgt,binary)
        il.EmitBranch(OpCodes.Br,decimal)
        il.Mark integral
        load point; il.EmitInt64 29L; il.EmitBranch(OpCodes.Bgt,big)
        il.Mark decimal
        let trim = il.Label()
        let trimmed = il.Label()
        il.Mark trim
        load kept; load effective; il.EmitBranch(OpCodes.Ble,trimmed)
        CodecDecimalArithmeticEmitter.divide10 il number.UInt64 coefficient |> ignore
        load kept; constant 1; il.Emit OpCodes.Sub; save kept
        il.EmitBranch(OpCodes.Br,trim)
        il.Mark trimmed
        let scaleUp = il.Label()
        let sized = il.Label()
        il.Mark scaleUp
        load scale; il.EmitInt64 0L; il.EmitBranch(OpCodes.Bge,sized)
        CodecDecimalArithmeticEmitter.append il number.UInt64 coefficient None
        load scale; il.EmitInt64 1L; il.Emit OpCodes.Add; save scale
        il.EmitBranch(OpCodes.Br,scaleUp)
        il.Mark sized
        let fractional = il.Label()
        let constructDecimal = il.Label()
        load coefficient.High; il.EmitInt64 0xffffffffL; il.EmitBranch(OpCodes.Ble_Un,fractional)
        load scale; il.EmitBranch(OpCodes.Brtrue,binary)
        il.EmitBranch(OpCodes.Br,big)
        il.Mark fractional
        CodecNumberEmitter.publish il state
        load scale; il.EmitBranch(OpCodes.Brtrue,constructDecimal)
        integerOrDecimal il number api.Parameter coefficient state.Negative
        il.Mark constructDecimal
        decimalValue il number api.Parameter coefficient state.Negative (fun () -> load scale)
        il.Mark zero
        load state.Negative; il.EmitBranch(OpCodes.Brtrue,binary)
        CodecNumberEmitter.publish il state
        il.EmitInt64 0L; il.EmitType(OpCodes.Box,number.Int64); returnNative il api.Parameter
        il.Mark binary
        load state.IntegerDigits; il.Emit OpCodes.Conv_I8
        load state.LeadingZeros; il.Emit OpCodes.Conv_I8; il.Emit OpCodes.Sub
        load state.Exponent; il.Emit OpCodes.Add
        load state.SignificantDigits; constant 19
        let fewer = il.Label()
        let exponentReady = il.Label()
        il.EmitBranch(OpCodes.Ble,fewer)
        il.EmitInt64 19L; il.EmitBranch(OpCodes.Br,exponentReady)
        il.Mark fewer
        load state.SignificantDigits; il.Emit OpCodes.Conv_I8
        il.Mark exponentReady
        il.Emit OpCodes.Sub; save exponent
        load prefix; load exponent; load state.Negative
        il.EmitMethod(OpCodes.Call,floating.RoundedBits); save bits
        let exact = il.Label()
        load tail; il.EmitBranch(OpCodes.Brfalse,exact)
        load prefix; il.EmitInt64 1L; il.Emit OpCodes.Add; load exponent; load state.Negative
        il.EmitMethod(OpCodes.Call,floating.RoundedBits); load bits; il.EmitBranch(OpCodes.Beq,exact)
        il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,floating.Ambiguous)
        il.EmitType(OpCodes.Box,floating.Result); returnNative il api.Parameter
        il.Mark exact
        CodecNumberEmitter.publish il state
        il.EmitLocal(OpCodes.Ldloca,bits); il.EmitMethod(OpCodes.Call,floating.ResultReference)
        il.EmitType(OpCodes.Ldobj,floating.Result); il.EmitType(OpCodes.Box,floating.Result); returnNative il api.Parameter
        il.Mark big
        match api.Parameter with
        | Some constructor -> CodecNumberEmitter.publish il state; returnEncoded il constructor
        | None ->
            // Rare arbitrary-width inference resumes the concrete native target, never a tree.
            il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,api.BigReader)
            il.EmitType(OpCodes.Box,api.BigInteger); il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid number literal"; il.EmitMethod(OpCodes.Newobj,number.InvalidNumber); il.Emit OpCodes.Throw

    let hexReader (il: CodecIL<'T,'M,'F>) (api: CodecDynamicNumberApi<'T,'M>) utf16 =
        let number = api.Number
        let coefficient = { Low = il.Local number.UInt64; High = il.Local number.UInt64 }
        let position = il.Local number.Int32
        let character = il.Local number.Int32
        let digit = il.Local number.Int32
        let count = il.Local number.Int32
        let significant = il.Local number.Int32
        let negative = il.Local number.Int32
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let read () =
            il.Emit OpCodes.Ldarg_0; load position
            CodecAddressEmitter.offset il number.OffsetByte (if utf16 then 2 else 1) 0
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
        let big = il.Label()
        load significant; constant 24; il.EmitBranch(OpCodes.Bgt,big)
        il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
        let value = il.Label()
        load significant; il.EmitBranch(OpCodes.Brtrue,value)
        load negative; il.EmitBranch(OpCodes.Brfalse,value)
        let bits = il.Local api.Floating.Bits
        il.EmitInt64 System.Int64.MinValue; save bits
        il.EmitLocal(OpCodes.Ldloca,bits); il.EmitMethod(OpCodes.Call,api.Floating.ResultReference)
        il.EmitType(OpCodes.Ldobj,api.Floating.Result); il.EmitType(OpCodes.Box,api.Floating.Result); returnNative il api.Parameter
        il.Mark value
        integerOrDecimal il api.Decimal api.Parameter coefficient negative
        il.Mark big
        match api.Parameter with
        | Some constructor ->
            il.Emit OpCodes.Ldarg_2; load position; il.Emit OpCodes.Stind_I4
            returnEncoded il constructor
        | None ->
            il.Emit OpCodes.Ldarg_3; il.EmitMethod(OpCodes.Call,api.BigReader)
            il.EmitType(OpCodes.Box,api.BigInteger); il.Emit OpCodes.Ret
        il.Mark invalid
        il.EmitString "Invalid hex literal"; il.EmitMethod(OpCodes.Newobj,number.InvalidHex); il.Emit OpCodes.Throw
