namespace SoloDatabase.Metadata.CodecGeneration

open System
open System.Reflection.Emit

type internal CodecFloatingWriterApi<'Type,'Method,'Field> = {
    Digits: CodecDigitWriterApi<'Type,'Method,'Field>
    Prepared: 'Type
    Prepare: 'Method
    Coefficient: 'Method
    Exponent: 'Method
    Source: 'Method
    Bits: 'Method
}

module internal CodecFloatingWriterEmitter =
    let writerFrom (il: CodecIL<'T,'M,'F>) (api: CodecFloatingWriterApi<'T,'M,'F>) width utf16 address =
        let output = api.Digits.Output
        let wide = width = 64
        let wordType = if wide then output.UInt64 else output.UInt32
        let bits = il.Local wordType
        let mantissa = il.Local wordType
        let exponent = il.Local output.Int32
        let negative = il.Local output.Int32
        let fixedFormat = il.Local output.Int32
        let prepared = il.Local api.Prepared
        let coefficient = il.Local wordType
        let decimalExponent = il.Local output.Int32
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let number n = if wide then il.EmitInt64(int64 n) else constant(int n)
        let destination() = il.Emit OpCodes.Ldarg_1
        let context() = il.Emit OpCodes.Ldarg_2
        let zero = il.Label()
        let valid = il.Label()
        let complete = il.Label()
        let mantissaBits = if width = 64 then 52 elif width = 16 then 10 else 23
        let exponentMask = if width = 64 then 2047 else if width = 16 then 31 else 255
        address(); il.EmitMethod(OpCodes.Call,api.Source); il.EmitMethod(OpCodes.Call,api.Bits); save bits
        load bits; constant(width-1); il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Conv_I4; save negative
        load bits; number((1UL <<< mantissaBits)-1UL); il.Emit OpCodes.And; save mantissa
        load bits; constant mantissaBits; il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Conv_I4; constant exponentMask; il.Emit OpCodes.And; save exponent
        load exponent; constant exponentMask; il.EmitBranch(OpCodes.Bne_Un,valid)
        let infinity = il.Label()
        let positiveInfinity = il.Label()
        load mantissa; il.EmitBranch(OpCodes.Brfalse,infinity)
        CodecOutputEmitter.literal il output utf16 [|78;97;78|] destination context
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark infinity
        load negative; il.EmitBranch(OpCodes.Brfalse,positiveInfinity)
        CodecOutputEmitter.literal il output utf16 [|45;73;110;102;105;110;105;116;121|] destination context
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark positiveInfinity
        CodecOutputEmitter.literal il output utf16 [|73;110;102;105;110;105;116;121|] destination context
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark valid
        load mantissa; load exponent
        if wide then il.Emit OpCodes.Conv_U8
        il.Emit OpCodes.Or; il.EmitBranch(OpCodes.Brfalse,zero)
        if width = 16 then constant 1; save fixedFormat
        else
            let scientific = il.Label()
            let selected = il.Label()
            load bits; number(if wide then 0x7fffffffffffffffUL else 0x7fffffffUL); il.Emit OpCodes.And; save bits
            let lower = if wide then uint64(BitConverter.DoubleToInt64Bits(1e-28)) else uint64(BitConverter.ToUInt32(BitConverter.GetBytes(single 1e-28),0))
            let upper = if wide then uint64(BitConverter.DoubleToInt64Bits(79228162514264337593543950335.0)) else uint64(BitConverter.ToUInt32(BitConverter.GetBytes(single 79228162514264337593543950335.0),0))
            load bits; number lower; il.EmitBranch(OpCodes.Blt_Un,scientific)
            load bits; number upper; il.EmitBranch(OpCodes.Bgt_Un,scientific)
            constant 1; save fixedFormat; il.EmitBranch(OpCodes.Br,selected)
            il.Mark scientific
            constant 0; save fixedFormat
            il.Mark selected
        load bits; il.Emit OpCodes.Conv_U8; constant width; il.EmitMethod(OpCodes.Call,api.Prepare); save prepared
        il.EmitLocal(OpCodes.Ldloca,prepared); il.EmitMethod(OpCodes.Call,api.Coefficient)
        if not wide then il.Emit OpCodes.Conv_U4
        save coefficient
        il.EmitLocal(OpCodes.Ldloca,prepared); il.EmitMethod(OpCodes.Call,api.Exponent); save decimalExponent
        let digits = il.Local output.Int32
        let point = il.Local output.Int32
        let length = il.Local output.Int32
        let absoluteExponent = il.Local output.Int32
        let negativeExponent = il.Local output.Int32
        let exponentDigits = il.Local output.Int32
        let dot = il.Local output.Int32
        let coefficientEnd = il.Local output.Int32
        let position = il.Local output.Int32
        let index = il.Local output.Int32
        let remaining = il.Local output.Int32
        let reference = il.Local output.Reference
        let sized = il.Label()
        let digitMaximum = if wide then 17 elif width = 16 then 5 else 9
        let powers = Array.init (digitMaximum-1) (fun i -> pown 10UL (i+1))
        let rec choose low high =
            if low = high then constant low; save digits; il.EmitBranch(OpCodes.Br,sized)
            else
                let split = (low+high)/2
                let upper = il.Label()
                load coefficient; number powers[split-1]; il.EmitBranch(OpCodes.Bge_Un,upper)
                choose low split; il.Mark upper; choose (split+1) high
        choose 1 (powers.Length+1)
        il.Mark sized
        load digits; load decimalExponent; il.Emit OpCodes.Add; save point
        let scientificSize = il.Label()
        let admitted = il.Label()
        load fixedFormat; il.EmitBranch(OpCodes.Brfalse,scientificSize)
        let positivePoint = il.Label()
        load point; constant 0; il.EmitBranch(OpCodes.Bgt,positivePoint)
        load digits; constant 2; il.Emit OpCodes.Add; load point; il.Emit OpCodes.Sub; load negative; il.Emit OpCodes.Add; save length
        il.EmitBranch(OpCodes.Br,admitted)
        il.Mark positivePoint
        let fractional = il.Label()
        load point; load digits; il.EmitBranch(OpCodes.Blt,fractional)
        load point; load negative; il.Emit OpCodes.Add; save length; il.EmitBranch(OpCodes.Br,admitted)
        il.Mark fractional
        load digits; load negative; il.Emit OpCodes.Add; constant 1; il.Emit OpCodes.Add; save length; il.EmitBranch(OpCodes.Br,admitted)
        il.Mark scientificSize
        load point; constant 1; il.Emit OpCodes.Sub; save absoluteExponent
        let positiveExponent = il.Label()
        load absoluteExponent; constant 0; il.EmitBranch(OpCodes.Bge,positiveExponent)
        constant 1; save negativeExponent
        load absoluteExponent; il.Emit OpCodes.Neg; save absoluteExponent
        il.Mark positiveExponent
        load absoluteExponent; constant 100; il.Emit OpCodes.Clt_Un; constant 0; il.Emit OpCodes.Ceq; constant 2; il.Emit OpCodes.Add; save exponentDigits
        load negative; load digits; il.Emit OpCodes.Add
        load digits; constant 1; il.Emit OpCodes.Cgt; il.Emit OpCodes.Add
        constant 2; il.Emit OpCodes.Add; load exponentDigits; il.Emit OpCodes.Add; save length
        il.Mark admitted
        let list = il.Label()
        let discarded = il.Label()
        let selected = CodecOutputEmitter.beginOperation il output context (fun () -> load length)
        il.Emit OpCodes.Dup; il.EmitBranch(OpCodes.Brfalse,discarded)
        constant 1; il.EmitBranch(OpCodes.Bne_Un,list)
        CodecOutputEmitter.admitSpan il output selected destination context
        destination(); il.EmitMethod(OpCodes.Call,output.Start); save reference
        context(); il.EmitMethod(OpCodes.Call,output.Position); save position
        let image = CodecDigitWriterEmitter.capture il api.Digits (fun () ->
            load digits; constant 1; il.Emit OpCodes.Cgt
            load fixedFormat; constant 0; il.Emit OpCodes.Ceq
            il.Emit OpCodes.Or)
        let store offset value = CodecOutputEmitter.store il output utf16 reference offset value
        let write value = store (fun () -> load index) value; load index; constant 1; il.Emit OpCodes.Add; save index
        load position; save index
        let unsigned = il.Label()
        load negative; il.EmitBranch(OpCodes.Brfalse,unsigned)
        write(fun () -> constant(int '-'))
        il.Mark unsigned
        constant -1; save dot
        let scientificOutput = il.Label()
        let coefficientReady = il.Label()
        load fixedFormat; il.EmitBranch(OpCodes.Brfalse,scientificOutput)
        let positive = il.Label()
        load point; constant 0; il.EmitBranch(OpCodes.Bgt,positive)
        write(fun () -> constant(int '0')); write(fun () -> constant(int '.'))
        constant 0; load point; il.Emit OpCodes.Sub; save remaining
        let leading = il.Label()
        let leadingDone = il.Label()
        il.Mark leading
        load remaining; il.EmitBranch(OpCodes.Brfalse,leadingDone)
        write(fun () -> constant(int '0'))
        load remaining; constant 1; il.Emit OpCodes.Sub; save remaining; il.EmitBranch(OpCodes.Br,leading)
        il.Mark leadingDone
        load index; load digits; il.Emit OpCodes.Add; constant 1; il.Emit OpCodes.Sub; save coefficientEnd
        il.EmitBranch(OpCodes.Br,coefficientReady)
        il.Mark positive
        let fraction = il.Label()
        load point; load digits; il.EmitBranch(OpCodes.Blt,fraction)
        load index; load digits; il.Emit OpCodes.Add; constant 1; il.Emit OpCodes.Sub; save coefficientEnd
        load index; load digits; il.Emit OpCodes.Add; save index
        load decimalExponent; save remaining
        let trailing = il.Label()
        il.Mark trailing
        load remaining; il.EmitBranch(OpCodes.Brfalse,coefficientReady)
        write(fun () -> constant(int '0'))
        load remaining; constant 1; il.Emit OpCodes.Sub; save remaining; il.EmitBranch(OpCodes.Br,trailing)
        il.Mark fraction
        load index; load point; il.Emit OpCodes.Add; save dot
        store (fun () -> load dot) (fun () -> constant(int '.'))
        load index; load digits; il.Emit OpCodes.Add; save coefficientEnd
        il.EmitBranch(OpCodes.Br,coefficientReady)
        il.Mark scientificOutput
        load index; load digits; il.Emit OpCodes.Add
        load digits; constant 1; il.Emit OpCodes.Cgt; il.Emit OpCodes.Add; constant 1; il.Emit OpCodes.Sub; save coefficientEnd
        let noDot = il.Label()
        load digits; constant 1; il.EmitBranch(OpCodes.Ble,noDot)
        load index; constant 1; il.Emit OpCodes.Add; save dot
        store (fun () -> load dot) (fun () -> constant(int '.'))
        il.Mark noDot
        load coefficientEnd; constant 1; il.Emit OpCodes.Add; save index
        write(fun () -> constant(int 'E'))
        let plus = il.Label()
        let signDone = il.Label()
        load negativeExponent; il.EmitBranch(OpCodes.Brfalse,plus)
        write(fun () -> constant(int '-')); il.EmitBranch(OpCodes.Br,signDone)
        il.Mark plus
        write(fun () -> constant(int '+'))
        il.Mark signDone
        load position; load length; il.Emit OpCodes.Add; constant 1; il.Emit OpCodes.Sub; save index
        CodecDigitWriterEmitter.reverseChunk il api.Digits utf16 image absoluteExponent exponentDigits reference index None
        il.Mark coefficientReady
        load coefficientEnd; save index
        CodecDigitWriterEmitter.reverse il api.Digits utf16 image wide coefficient digits digitMaximum reference index (Some dot)
        context(); load length; il.EmitMethod(OpCodes.Call,output.Advance); il.EmitBranch(OpCodes.Br,complete)
        il.Mark list
        // Arbitrary IList retains ordered successful-Add accounting, using captured numeric facts.
        CodecOutputEmitter.listBody il output utf16 selected context false None (Some (fun () -> load length)) (fun append ->
            let image = CodecDigitWriterEmitter.capture il api.Digits (fun () ->
                load digits; constant 1; il.Emit OpCodes.Cgt
                load fixedFormat; constant 0; il.Emit OpCodes.Ceq
                il.Emit OpCodes.Or)
            let listUnsigned = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,listUnsigned)
            append(fun () -> constant(int '-'))
            il.Mark listUnsigned
            let listCoefficient = il.Label()
            load fixedFormat; il.EmitBranch(OpCodes.Brfalse,listCoefficient)
            load point; constant 0; il.EmitBranch(OpCodes.Bgt,listCoefficient)
            append(fun () -> constant(int '0')); append(fun () -> constant(int '.'))
            constant 0; load point; il.Emit OpCodes.Sub; save remaining
            let listZeros = il.Label()
            il.Mark listZeros
            load remaining; il.EmitBranch(OpCodes.Brfalse,listCoefficient)
            append(fun () -> constant(int '0'))
            load remaining; constant 1; il.Emit OpCodes.Sub; save remaining; il.EmitBranch(OpCodes.Br,listZeros)
            il.Mark listCoefficient
            constant -1; save dot
            let ordinaryPoint = il.Label()
            let pointReady = il.Label()
            load fixedFormat; il.EmitBranch(OpCodes.Brtrue,ordinaryPoint)
            load digits; constant 1; il.EmitBranch(OpCodes.Ble,pointReady)
            constant 1; save dot; il.EmitBranch(OpCodes.Br,pointReady)
            il.Mark ordinaryPoint
            load point; constant 0; il.EmitBranch(OpCodes.Ble,pointReady)
            load point; load digits; il.EmitBranch(OpCodes.Bge,pointReady)
            load point; save dot
            il.Mark pointReady
            constant 0; save index
            CodecDigitWriterEmitter.forward il api.Digits utf16 image wide coefficient digits digitMaximum index (Some dot) append
            let listExponent = il.Label()
            load fixedFormat; il.EmitBranch(OpCodes.Brfalse,listExponent)
            load decimalExponent; save remaining
            load remaining; constant 0; il.EmitBranch(OpCodes.Ble,complete)
            let listTrailing = il.Label()
            il.Mark listTrailing
            append(fun () -> constant(int '0'))
            load remaining; constant 1; il.Emit OpCodes.Sub; save remaining
            load remaining; il.EmitBranch(OpCodes.Brtrue,listTrailing); il.EmitBranch(OpCodes.Br,complete)
            il.Mark listExponent
            append(fun () -> constant(int 'E'))
            let listPlus = il.Label()
            let listSignDone = il.Label()
            load negativeExponent; il.EmitBranch(OpCodes.Brfalse,listPlus)
            append(fun () -> constant(int '-')); il.EmitBranch(OpCodes.Br,listSignDone)
            il.Mark listPlus
            append(fun () -> constant(int '+'))
            il.Mark listSignDone
            constant 0; save index
            CodecDigitWriterEmitter.forwardChunk il api.Digits utf16 image absoluteExponent exponentDigits 3 index None append
            il.EmitBranch(OpCodes.Br,complete)
        )
        il.Mark zero
        let positiveZero = il.Label()
        load negative; il.EmitBranch(OpCodes.Brfalse,positiveZero)
        CodecOutputEmitter.literal il output utf16 [|45;48|] destination context
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark positiveZero
        CodecOutputEmitter.literal il output utf16 [|48|] destination context
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark discarded
        il.Emit OpCodes.Pop
        il.Mark complete
        il.Emit OpCodes.Ret
