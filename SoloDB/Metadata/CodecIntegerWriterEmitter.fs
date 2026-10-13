namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

module internal CodecIntegerWriterEmitter =
    /// Width and sign belong to the captured value, rather than a common Int64 formatter.
    let writerFrom (il: CodecIL<'T,'M,'F>) (api: CodecDigitWriterApi<'T,'M,'F>) width signed utf16 input =
        let output = api.Output
        let wide = width = 64
        let numberType = if wide then output.UInt64 else output.UInt32
        let magnitude = il.Local numberType
        let negative = il.Local output.Int32
        let digits = il.Local output.Int32
        let length = il.Local output.Int32
        let position = il.Local output.Int32
        let index = il.Local output.Int32
        let reference = il.Local output.Reference
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let constant value = il.EmitInt(OpCodes.Ldc_I4,value)
        let number value = if wide then il.EmitInt64(int64 value) else constant(int value)
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        let captured = il.Label()
        if signed then
            let positive = il.Label()
            input(); il.Emit(if wide then OpCodes.Conv_I8 else OpCodes.Conv_I4); number 0UL; il.EmitBranch(OpCodes.Bge,positive)
            constant 1; save negative
            input(); il.Emit(if wide then OpCodes.Conv_I8 else OpCodes.Conv_I4); il.Emit OpCodes.Neg
            il.Emit(if wide then OpCodes.Conv_U8 else OpCodes.Conv_U4)
            save magnitude; il.EmitBranch(OpCodes.Br,captured)
            il.Mark positive
        input()
        il.Emit(if wide then OpCodes.Conv_U8 else OpCodes.Conv_U4)
        save magnitude
        il.Mark captured
        let limit = if signed then 1UL <<< (width - 1) elif wide then System.UInt64.MaxValue else (1UL <<< width)-1UL
        let powers = ResizeArray<uint64>()
        let mutable power = 10UL
        for _ = 1 to 19 do
            if power <= limit then powers.Add power
            if power <= System.UInt64.MaxValue / 10UL then power <- power * 10UL
            else power <- System.UInt64.MaxValue
        let size = il.Label()
        let rec selectLength low high =
            if low = high then
                constant low; save digits; il.EmitBranch(OpCodes.Br,size)
            else
                let middle = (low + high) / 2
                let upper = il.Label()
                load magnitude; number powers[middle-1]; il.EmitBranch(OpCodes.Bge_Un,upper)
                selectLength low middle
                il.Mark upper
                selectLength (middle+1) high
        selectLength 1 (powers.Count+1)
        il.Mark size
        load digits; load negative; il.Emit OpCodes.Add; save length
        let fallback = il.Label()
        let complete = il.Label()
        let discarded = il.Label()
        let selected = CodecOutputEmitter.beginOperation il output state (fun () -> load length)
        il.Emit OpCodes.Dup; il.EmitBranch(OpCodes.Brfalse,discarded)
        constant 1; il.EmitBranch(OpCodes.Bne_Un,fallback)
        CodecOutputEmitter.admitSpan il output selected destination state
        destination(); il.EmitMethod(OpCodes.Call,output.Start); save reference
        state(); il.EmitMethod(OpCodes.Call,output.Position); save position
        let unsigned = il.Label()
        load negative; il.EmitBranch(OpCodes.Brfalse,unsigned)
        CodecOutputEmitter.store il output utf16 reference (fun () -> load position) (fun () -> constant(int '-'))
        il.Mark unsigned
        load position; load length; il.Emit OpCodes.Add; constant 1; il.Emit OpCodes.Sub; save index
        let image = CodecDigitWriterEmitter.capture il api (fun () -> load digits; constant 1; il.Emit OpCodes.Cgt)
        CodecDigitWriterEmitter.reverse il api utf16 image wide magnitude digits (powers.Count+1) reference index None
        state(); load length; il.EmitMethod(OpCodes.Call,output.Advance)
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark fallback
        CodecOutputEmitter.listBody il output utf16 selected state false None (Some (fun () -> load length)) (fun append ->
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            append(fun () -> constant(int '-'))
            il.Mark positive
            constant 0; save index
            let image = CodecDigitWriterEmitter.capture il api (fun () -> load digits; constant 1; il.Emit OpCodes.Cgt)
            CodecDigitWriterEmitter.forward il api utf16 image wide magnitude digits (powers.Count+1) index None append
        )
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark discarded
        il.Emit OpCodes.Pop
        il.Mark complete
        il.Emit OpCodes.Ret

    let writer il api width signed utf16 =
        writerFrom il api width signed utf16 (fun () -> il.Emit OpCodes.Ldarg_0)
