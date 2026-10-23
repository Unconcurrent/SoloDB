namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecBase64WriterApi<'Type,'Method,'Field> = {
    Output: CodecOutputApi<'Type,'Method,'Field>
    ReserveKnownSpan: 'Method option
    LengthOverflow: 'Method option
    Bytes: 'Type
    Capture: 'Method
    Reference: 'Method
    Alphabet: 'Method
    Vector: CodecBase64VectorApi<'Type,'Method> option
    LittleEndian: 'Field
    WriteWord: 'Method
}

module internal CodecBase64WriterEmitter =
    let writer (il: CodecIL<'T,'M,'F>) (api: CodecBase64WriterApi<'T,'M,'F>) utf16 ordinarySequence =
        let output = api.Output
        let bytes = il.Local api.Bytes
        let source = il.Local output.Reference
        let count = il.Local output.Int32
        let length = il.Local output.Int32
        let index = il.Local output.Int32
        let position = il.Local output.Int32
        let reference = il.Local output.Reference
        let packed = il.Local output.UInt32
        let alphabetReference = il.Local output.Reference
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let constant value = il.EmitInt(OpCodes.Ldc_I4,value)
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        let complete = il.Label()
        if not ordinarySequence then
            let present = il.Label()
            il.InputValue(); il.EmitBranch(OpCodes.Brtrue,present)
            CodecOutputEmitter.literal il output utf16 [|110;117;108;108|] destination state
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark present
        il.InputValue(); il.Emit OpCodes.Ldlen; il.Emit OpCodes.Conv_I4; save count
        load count; il.Emit OpCodes.Conv_I8; il.EmitInt64 2L; il.Emit OpCodes.Add
        il.EmitInt64 3L; il.Emit OpCodes.Div; il.EmitInt64 4L; il.Emit OpCodes.Mul
        if ordinarySequence then
            let encoded = il.Local output.UInt64
            save encoded
            let fitting = il.Label()
            load encoded; il.EmitInt64 (int64 System.Int32.MaxValue); il.EmitBranch(OpCodes.Ble,fitting)
            il.EmitMethod(OpCodes.Call,api.LengthOverflow.Value)
            il.Mark fitting
            load encoded; il.Emit OpCodes.Conv_I4; save length
        else
            il.EmitInt64 2L; il.Emit OpCodes.Add; il.Emit OpCodes.Conv_Ovf_I4; save length
        let bodyCount() =
            load length
            if not ordinarySequence then constant 2; il.Emit OpCodes.Sub
        let captureCount() =
            load length
            if ordinarySequence then constant 2; il.Emit OpCodes.Add
        let noSource = il.Label()
        load count; il.EmitBranch(OpCodes.Brfalse,noSource)
        il.InputValue(); destination(); state(); captureCount()
        il.EmitMethod(OpCodes.Call,api.Capture); save bytes
        load bytes; il.EmitMethod(OpCodes.Call,api.Reference); save source
        il.Mark noSource
        let alphabet value =
            load alphabetReference; value(); constant 63; il.Emit OpCodes.And
            CodecAddressEmitter.offset il output.Offset 1 0; il.Emit OpCodes.Ldind_U1
        let input offset =
            load source; load index
            if offset <> 0 then constant offset; il.Emit OpCodes.Add
            CodecAddressEmitter.offset il output.Offset 1 0; il.Emit OpCodes.Ldind_U1
        let body store quartet initialize repeat =
            if initialize then constant 0; save index
            let loop = il.Label()
            let tail = il.Label()
            let bodyDone = il.Label()
            il.Mark loop
            load index; load count; constant 3; il.Emit OpCodes.Sub; il.EmitBranch(OpCodes.Bgt,tail)
            input 0; constant 16; il.Emit OpCodes.Shl
            input 1; constant 8; il.Emit OpCodes.Shl; il.Emit OpCodes.Or
            input 2; il.Emit OpCodes.Or; save packed
            let digits = [| for shift in [18;12;6;0] ->
                                fun () -> alphabet(fun () -> load packed; if shift <> 0 then constant shift; il.Emit OpCodes.Shr_Un) |]
            match quartet with
            | Some write -> write digits
            | None -> for digit in digits do store digit
            load index; constant 3; il.Emit OpCodes.Add; save index
            if repeat then il.EmitBranch(OpCodes.Br,loop)
            il.Mark tail
            load index; load count; il.EmitBranch(OpCodes.Beq,bodyDone)
            load source; load index; CodecAddressEmitter.offset il output.Offset 1 0; il.Emit OpCodes.Ldind_U1; constant 16; il.Emit OpCodes.Shl; save packed
            let one = il.Label()
            load index; constant 1; il.Emit OpCodes.Add; load count; il.EmitBranch(OpCodes.Beq,one)
            load packed; load source; load index; constant 1; il.Emit OpCodes.Add; CodecAddressEmitter.offset il output.Offset 1 0; il.Emit OpCodes.Ldind_U1
            constant 8; il.Emit OpCodes.Shl; il.Emit OpCodes.Or; save packed
            il.Mark one
            for shift in [18;12] do store(fun () -> alphabet(fun () -> load packed; constant shift; il.Emit OpCodes.Shr_Un))
            let padding = il.Label()
            let last = il.Label()
            load index; constant 1; il.Emit OpCodes.Add; load count; il.EmitBranch(OpCodes.Beq,padding)
            store(fun () -> alphabet(fun () -> load packed; constant 6; il.Emit OpCodes.Shr_Un))
            il.EmitBranch(OpCodes.Br,last)
            il.Mark padding
            store(fun () -> constant(int '='))
            il.Mark last
            store(fun () -> constant(int '='))
            il.Mark bodyDone
        let acquire() =
            destination(); il.EmitMethod(OpCodes.Call,output.Start); save reference
            state(); il.EmitMethod(OpCodes.Call,output.Position); save position
        let raw value =
            CodecOutputEmitter.store il output utf16 reference (fun () -> load position) value
            load position; constant 1; il.Emit OpCodes.Add; save position
        let spanBody() =
            let doneBody = il.Label()
            load count; il.EmitBranch(OpCodes.Brfalse,doneBody)
            constant 0; save index
            match api.Vector with
            | Some vector -> CodecBase64VectorEmitter.write il output vector utf16 source count index reference position
            | None -> ()
            let bigEndian = il.Label()
            let writeQuartet littleEndian (digits: (unit -> unit)[]) =
                load reference; load position; CodecAddressEmitter.offset il output.Offset (if utf16 then 2 else 1) 0
                for lane = 0 to 3 do
                    digits[lane]()
                    if utf16 then il.Emit OpCodes.Conv_U8
                    let shift = (if littleEndian then lane else 3-lane) * (if utf16 then 16 else 8)
                    if shift <> 0 then constant shift; il.Emit OpCodes.Shl
                    if lane <> 0 then il.Emit OpCodes.Or
                il.EmitMethod(OpCodes.Call,api.WriteWord)
                load position; constant 4; il.Emit OpCodes.Add; save position
            il.EmitField(OpCodes.Ldsfld,api.LittleEndian); il.EmitBranch(OpCodes.Brfalse,bigEndian)
            body raw (Some(writeQuartet true)) false true; il.EmitBranch(OpCodes.Br,doneBody)
            il.Mark bigEndian
            body raw (Some(writeQuartet false)) false true
            il.Mark doneBody
        CodecOutputEmitter.withDestination il output state (fun selected mode ->
            let complete = il.Label()
            if mode <> 0 then
                let noAlphabet = il.Label()
                load count; il.EmitBranch(OpCodes.Brfalse,noAlphabet)
                il.EmitMethod(OpCodes.Call,api.Alphabet); save alphabetReference
                il.Mark noAlphabet
            if mode = 1 && not ordinarySequence then
                let partial = il.Label()
                destination(); state(); load length; il.EmitMethod(OpCodes.Call,api.ReserveKnownSpan.Value)
                il.EmitBranch(OpCodes.Brfalse,partial)
                acquire()
                raw(fun () -> constant(int '"'))
                spanBody()
                raw(fun () -> constant(int '"'))
                state(); load length; il.EmitMethod(OpCodes.Call,output.Advance)
                il.EmitBranch(OpCodes.Br,complete)
                il.Mark partial
            CodecOutputEmitter.literalMode il output utf16 selected mode [|int '"'|] destination state
            let closing = il.Label()
            load count; il.EmitBranch(OpCodes.Brfalse,closing)
            if ordinarySequence && not utf16 && mode = 2 then
                let append value =
                    CodecOutputEmitter.checkCount il output state (fun () -> constant 1); il.Emit OpCodes.Pop
                    CodecOutputEmitter.listAppend il output utf16 state value
                body append None true true
            else
                CodecOutputEmitter.checkCount il output state bodyCount
                if mode = 1 then
                    save selected.Minimum
                    CodecOutputEmitter.admitSpan il output selected destination state
                    acquire()
                    spanBody()
                    state(); bodyCount(); il.EmitMethod(OpCodes.Call,output.Advance)
                else
                    il.Emit OpCodes.Pop
                    if mode = 2 then
                        body (fun value -> CodecOutputEmitter.listAppend il output utf16 state value) None true true
            il.Mark closing
            if mode <> 0 then
                CodecOutputEmitter.literalMode il output utf16 selected mode [|int '"'|] destination state
            il.Mark complete)
        il.Mark complete
        il.Emit OpCodes.Ret
