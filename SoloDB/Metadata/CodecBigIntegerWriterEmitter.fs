namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecBigIntegerWriterApi<'Type,'Method,'Field> = {
    Output: CodecOutputApi<'Type,'Method,'Field>
    Prepared: 'Type
    UInt32Array: 'Type
    Prepare: 'Method
    Sign: 'Method
    Count: 'Method
    Chunks: 'Method
}

module internal CodecBigIntegerWriterEmitter =
    let writer (il: CodecIL<'T,'M,'F>) (api: CodecBigIntegerWriterApi<'T,'M,'F>) utf16 =
        let output = api.Output
        let prepared = il.Local api.Prepared
        let negative = il.Local output.Int32
        let length = il.Local output.Int32
        let reference = il.Local output.Reference
        let position = il.Local output.Int32
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let destination () = il.Emit OpCodes.Ldarg_1
        let context () = il.Emit OpCodes.Ldarg_2
        let zero = il.Label()
        let complete = il.Label()
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Prepare); save prepared
        il.EmitLocal(OpCodes.Ldloca,prepared); il.EmitMethod(OpCodes.Call,api.Sign)
        il.Emit OpCodes.Dup; il.EmitBranch(OpCodes.Brfalse,zero)
        constant 0; il.Emit OpCodes.Clt; save negative
        let chunks: CodecMagnitudeDecimalState =
            { Chunks = il.Local api.UInt32Array; Count = il.Local output.Int32; Index = il.Local output.Int32
              Digits = il.Local output.Int32; Coefficient = il.Local output.UInt32; Divisor = il.Local output.UInt32 }
        il.EmitLocal(OpCodes.Ldloca,prepared); il.EmitMethod(OpCodes.Call,api.Chunks); save chunks.Chunks
        il.EmitLocal(OpCodes.Ldloca,prepared); il.EmitMethod(OpCodes.Call,api.Count); save chunks.Count
        load chunks.Count; constant 1; il.Emit OpCodes.Sub; save chunks.Index
        load chunks.Chunks; load chunks.Index; il.Emit OpCodes.Ldelem_U4; save chunks.Coefficient
        constant 1; save chunks.Digits
        let size = il.Label()
        let doneSize = il.Label()
        il.Mark size
        load chunks.Coefficient; constant 10; il.EmitBranch(OpCodes.Blt_Un,doneSize)
        load chunks.Coefficient; constant 10; il.Emit OpCodes.Div_Un; save chunks.Coefficient
        load chunks.Digits; constant 1; il.Emit OpCodes.Add; save chunks.Digits
        il.EmitBranch(OpCodes.Br,size)
        il.Mark doneSize
        load chunks.Index; constant 9; il.Emit OpCodes.Mul_Ovf
        load chunks.Digits; il.Emit OpCodes.Add_Ovf; load negative; il.Emit OpCodes.Add_Ovf; save length
        let body store =
            let positive = il.Label()
            load negative; il.EmitBranch(OpCodes.Brfalse,positive)
            store(fun () -> constant(int '-'))
            il.Mark positive
            CodecMagnitudeEmitter.decimalDigits il chunks store
        let list = il.Label()
        let discarded = il.Label()
        let selected = CodecOutputEmitter.beginOperation il output context (fun () -> load length)
        il.Emit OpCodes.Dup; il.EmitBranch(OpCodes.Brfalse,discarded)
        constant 1; il.EmitBranch(OpCodes.Bne_Un,list)
        CodecOutputEmitter.admitSpan il output selected destination context
        destination(); il.EmitMethod(OpCodes.Call,output.Start); save reference
        context(); il.EmitMethod(OpCodes.Call,output.Position); save position
        body(fun value ->
            CodecOutputEmitter.store il output utf16 reference (fun () -> load position) value
            load position; constant 1; il.Emit OpCodes.Add; save position)
        context(); load length; il.EmitMethod(OpCodes.Call,output.Advance)
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark list
        CodecOutputEmitter.listBody il output utf16 selected context false None (Some (fun () -> load length)) body
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark zero
        il.Emit OpCodes.Pop
        CodecOutputEmitter.literal il output utf16 [|48|] destination context
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark discarded
        il.Emit OpCodes.Pop
        il.Mark complete
        il.Emit OpCodes.Ret
