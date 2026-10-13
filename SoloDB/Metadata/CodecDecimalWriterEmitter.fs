namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecDecimalWriterApi<'Type,'Method,'Field> = {
    Digits: CodecDigitWriterApi<'Type,'Method,'Field>
    Source: 'Method
    Read32: 'Method
    Read64: 'Method
}

module internal CodecDecimalWriterEmitter =
    /// Emits the Win32 DECIMAL coefficient/sign/scale operations, not a formatted-text detour.
    let writerFrom (il: CodecIL<'T,'M,'F>) (api: CodecDecimalWriterApi<'T,'M,'F>) utf16 address =
        let output = api.Digits.Output
        let low = il.Local output.UInt32
        let middle = il.Local output.UInt32
        let high = il.Local output.UInt32
        let qLow = il.Local output.UInt32
        let qMiddle = il.Local output.UInt32
        let qHigh = il.Local output.UInt32
        let carry = il.Local output.UInt64
        let dividend = il.Local output.UInt64
        let word = il.Local output.UInt64
        let flags = il.Local output.Int32
        let negative = il.Local output.Int32
        let scale = il.Local output.Int32
        let digits = il.Local output.Int32
        let point = il.Local output.Int32
        let length = il.Local output.Int32
        let position = il.Local output.Int32
        let dot = il.Local output.Int32
        let index = il.Local output.Int32
        let remaining = il.Local output.Int32
        let reference = il.Local output.Reference
        let source = il.Local output.Reference
        let mode = il.Local output.Int32
        let chunks = Array.init 4 (fun _ -> il.Local output.UInt32)
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let wide n = il.EmitInt64(int64 n)
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        let read offset method =
            load source
            if offset <> 0 then constant offset; CodecAddressEmitter.offset il output.Offset 1 0
            il.EmitMethod(OpCodes.Call,method)
        let divide divisor =
            load high; il.Emit OpCodes.Conv_U8; save dividend
            load dividend; wide divisor; il.Emit OpCodes.Div_Un; il.Emit OpCodes.Conv_U4; save qHigh
            load dividend; wide divisor; il.Emit OpCodes.Rem_Un; save carry
            load carry; constant 32; il.Emit OpCodes.Shl
            load middle; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or; save dividend
            load dividend; wide divisor; il.Emit OpCodes.Div_Un; il.Emit OpCodes.Conv_U4; save qMiddle
            load dividend; wide divisor; il.Emit OpCodes.Rem_Un; save carry
            load carry; constant 32; il.Emit OpCodes.Shl
            load low; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or; save dividend
            load dividend; wide divisor; il.Emit OpCodes.Div_Un; il.Emit OpCodes.Conv_U4; save qLow
            load dividend; wide divisor; il.Emit OpCodes.Rem_Un; save carry
        let acceptQuotient() =
            load qLow; save low; load qMiddle; save middle; load qHigh; save high
        let captureChunks() =
            let full = il.Label()
            let paired = il.Label()
            let captured = il.Label()
            load high; il.EmitBranch(OpCodes.Brtrue,full)
            load middle; il.EmitBranch(OpCodes.Brtrue,paired)
            load low; constant 1000000000; il.Emit OpCodes.Rem_Un; save chunks[0]
            load low; constant 1000000000; il.Emit OpCodes.Div_Un; save chunks[1]
            il.EmitBranch(OpCodes.Br,captured)
            il.Mark paired
            load middle; il.Emit OpCodes.Conv_U8; constant 32; il.Emit OpCodes.Shl
            load low; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or; save word
            for chunk = 0 to 1 do
                load word; wide 1000000000UL; il.Emit OpCodes.Rem_Un; il.Emit OpCodes.Conv_U4; save chunks[chunk]
                load word; wide 1000000000UL; il.Emit OpCodes.Div_Un; save word
            load word; il.Emit OpCodes.Conv_U4; save chunks[2]
            il.EmitBranch(OpCodes.Br,captured)
            il.Mark full
            for chunk = 0 to 1 do
                divide 1000000000UL
                load carry; il.Emit OpCodes.Conv_U4; save chunks[chunk]
                acceptQuotient()
            // The remaining coefficient is below 2 to power 37 after two divisions.
            load middle; il.Emit OpCodes.Conv_U8; constant 32; il.Emit OpCodes.Shl
            load low; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or; save word
            load word; wide 1000000000UL; il.Emit OpCodes.Rem_Un; il.Emit OpCodes.Conv_U4; save chunks[2]
            load word; wide 1000000000UL; il.Emit OpCodes.Div_Un; il.Emit OpCodes.Conv_U4; save chunks[3]
            il.Mark captured
        let zero = il.Label()
        let coefficient = il.Label()
        let trimmed = il.Label()
        let trim = il.Label()
        let sized = il.Label()
        let fitting = il.Label()
        let list = il.Label()
        let discarded = il.Label()
        let complete = il.Label()
        address(); il.EmitMethod(OpCodes.Call,api.Source); save source
        read 0 api.Read32; save flags
        read 4 api.Read32; save high
        read 8 api.Read64; save word
        load word; il.Emit OpCodes.Conv_U4; save low
        load word; constant 32; il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Conv_U4; save middle
        load low; load middle; il.Emit OpCodes.Or; load high; il.Emit OpCodes.Or
        il.EmitBranch(OpCodes.Brfalse,zero)
        load flags; constant 16; il.Emit OpCodes.Shr_Un; constant 255; il.Emit OpCodes.And; save scale
        load flags; constant 31; il.Emit OpCodes.Shr_Un; save negative
        il.Mark trim
        load scale; il.EmitBranch(OpCodes.Brfalse,trimmed)
        divide 10UL
        load carry; il.EmitBranch(OpCodes.Brtrue,trimmed)
        acceptQuotient()
        load scale; constant 1; il.Emit OpCodes.Sub; save scale
        il.EmitBranch(OpCodes.Br,trim)
        il.Mark trimmed
        let powers = Array.init 28 (fun index -> System.Numerics.BigInteger.Pow(System.Numerics.BigInteger(10),index+1))
        let rec choose lowDigits highDigits =
            if lowDigits = highDigits then
                constant lowDigits; save digits; il.EmitBranch(OpCodes.Br,sized)
            else
                let split = (lowDigits+highDigits)/2
                let power = powers[split-1]
                let lo = uint32(power &&& System.Numerics.BigInteger(System.UInt32.MaxValue))
                let mid = uint32((power >>> 32) &&& System.Numerics.BigInteger(System.UInt32.MaxValue))
                let hi = uint32(power >>> 64)
                let upper = il.Label()
                let lower = il.Label()
                load high; constant(int hi); il.EmitBranch(OpCodes.Bgt_Un,upper)
                load high; constant(int hi); il.EmitBranch(OpCodes.Blt_Un,lower)
                load middle; constant(int mid); il.EmitBranch(OpCodes.Bgt_Un,upper)
                load middle; constant(int mid); il.EmitBranch(OpCodes.Blt_Un,lower)
                load low; constant(int lo); il.EmitBranch(OpCodes.Bge_Un,upper)
                il.Mark lower
                choose lowDigits split
                il.Mark upper
                choose (split+1) highDigits
        choose 1 29
        il.Mark sized
        load digits; load scale; il.Emit OpCodes.Sub; save point
        let positivePoint = il.Label()
        load point; constant 0; il.EmitBranch(OpCodes.Bgt,positivePoint)
        load scale; constant 2; il.Emit OpCodes.Add; load negative; il.Emit OpCodes.Add; save length
        il.EmitBranch(OpCodes.Br,coefficient)
        il.Mark positivePoint
        load digits; load negative; il.Emit OpCodes.Add
        load scale; constant 0; il.Emit OpCodes.Cgt_Un; il.Emit OpCodes.Add; save length
        il.Mark coefficient
        let selected = CodecOutputEmitter.beginOperation il output state (fun () -> load length)
        il.Emit OpCodes.Dup; il.EmitBranch(OpCodes.Brfalse,discarded)
        save mode
        captureChunks()
        load mode; constant 1; il.EmitBranch(OpCodes.Bne_Un,list)
        il.Mark fitting
        CodecOutputEmitter.admitSpan il output selected destination state
        destination(); il.EmitMethod(OpCodes.Call,output.Start); save reference
        state(); il.EmitMethod(OpCodes.Call,output.Position); save position
        let store offset value = CodecOutputEmitter.store il output utf16 reference offset value
        let unsigned = il.Label()
        load negative; il.EmitBranch(OpCodes.Brfalse,unsigned)
        store (fun () -> load position) (fun () -> constant(int '-'))
        il.Mark unsigned
        let noFraction = il.Label()
        let leading = il.Label()
        load scale; il.EmitBranch(OpCodes.Brfalse,noFraction)
        // Place the decimal point and leading zeros before the reverse coefficient loop.
        let dotReady = il.Label()
        load point; constant 0; il.EmitBranch(OpCodes.Ble,leading)
        load position; load negative; il.Emit OpCodes.Add; load point; il.Emit OpCodes.Add; save dot
        store (fun () -> load dot) (fun () -> constant(int '.'))
        il.EmitBranch(OpCodes.Br,dotReady)
        il.Mark leading
        load position; load negative; il.Emit OpCodes.Add; constant 1; il.Emit OpCodes.Add; save dot
        store (fun () -> load dot; constant 1; il.Emit OpCodes.Sub) (fun () -> constant(int '0'))
        store (fun () -> load dot) (fun () -> constant(int '.'))
        load dot; constant 1; il.Emit OpCodes.Add; save index
        let zeros = il.Label()
        let zerosDone = il.Label()
        constant 0; load point; il.Emit OpCodes.Sub; save remaining
        il.Mark zeros
        load remaining; il.EmitBranch(OpCodes.Brfalse,zerosDone)
        store (fun () -> load index) (fun () -> constant(int '0'))
        load index; constant 1; il.Emit OpCodes.Add; save index
        load remaining; constant 1; il.Emit OpCodes.Sub; save remaining
        il.EmitBranch(OpCodes.Br,zeros)
        il.Mark zerosDone
        il.EmitBranch(OpCodes.Br,dotReady)
        il.Mark noFraction
        constant -1; save dot
        il.Mark dotReady
        load position; load length; il.Emit OpCodes.Add; constant 1; il.Emit OpCodes.Sub; save index
        let image = CodecDigitWriterEmitter.capture il api.Digits (fun () -> load digits; constant 1; il.Emit OpCodes.Cgt)
        for chunk = 0 to 3 do
            let absent = il.Label()
            load digits; constant(chunk*9); il.Emit OpCodes.Sub; save remaining
            load remaining; constant 0; il.EmitBranch(OpCodes.Ble,absent)
            let bounded = il.Label()
            load remaining; constant 9; il.EmitBranch(OpCodes.Ble,bounded)
            constant 9; save remaining
            il.Mark bounded
            CodecDigitWriterEmitter.reverseChunk il api.Digits utf16 image chunks[chunk] remaining reference index (Some dot)
            il.Mark absent
        state(); load length; il.EmitMethod(OpCodes.Call,output.Advance)
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark list
        CodecOutputEmitter.listBody il output utf16 selected state false None (Some (fun () -> load length)) (fun append ->
            let listUnsigned = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,listUnsigned)
            append(fun () -> constant(int '-'))
            il.Mark listUnsigned
            let listPositive = il.Label()
            load point; constant 0; il.EmitBranch(OpCodes.Bgt,listPositive)
            append(fun () -> constant(int '0')); append(fun () -> constant(int '.'))
            constant 0; load point; il.Emit OpCodes.Sub; save remaining
            let zeroLoop = il.Label()
            let zeroDone = il.Label()
            il.Mark zeroLoop
            load remaining; il.EmitBranch(OpCodes.Brfalse,zeroDone)
            append(fun () -> constant(int '0'))
            load remaining; constant 1; il.Emit OpCodes.Sub; save remaining
            il.EmitBranch(OpCodes.Br,zeroLoop)
            il.Mark zeroDone
            il.Mark listPositive
            constant 0; save index
            constant -1; save dot
            let noPoint = il.Label()
            load scale; il.EmitBranch(OpCodes.Brfalse,noPoint)
            load point; constant 0; il.EmitBranch(OpCodes.Ble,noPoint)
            load point; save dot
            il.Mark noPoint
            let image = CodecDigitWriterEmitter.capture il api.Digits (fun () -> load digits; constant 1; il.Emit OpCodes.Cgt)
            for chunk = 3 downto 0 do
                let absent = il.Label()
                let bounded = il.Label()
                load digits; constant(chunk*9); il.Emit OpCodes.Sub; save remaining
                load remaining; constant 0; il.EmitBranch(OpCodes.Ble,absent)
                load remaining; constant 9; il.EmitBranch(OpCodes.Ble,bounded)
                constant 9; save remaining
                il.Mark bounded
                CodecDigitWriterEmitter.forwardChunk il api.Digits utf16 image chunks[chunk] remaining (min 9 (29-chunk*9)) index (Some dot) append
                il.Mark absent
            il.EmitBranch(OpCodes.Br,complete)
        )
        il.Mark zero
        CodecOutputEmitter.literal il output utf16 [|int '0'|] destination state
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark discarded
        il.Emit OpCodes.Pop
        il.Mark complete
        il.Emit OpCodes.Ret

    let writer il api utf16 =
        writerFrom il api utf16 (fun () -> il.EmitInt(OpCodes.Ldarga,0))
