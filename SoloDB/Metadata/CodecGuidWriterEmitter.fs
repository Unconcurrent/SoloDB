namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecGuidWriterApi<'Type,'Method,'Field> = {
    Output: CodecOutputApi<'Type,'Method,'Field>
    ReserveKnownSpan: 'Method
    Source: 'Method
    Read32: 'Method
    Read16: 'Method
}

module internal CodecGuidWriterEmitter =
    let writer (il: CodecIL<'T,'M,'F>) (api: CodecGuidWriterApi<'T,'M,'F>) utf16 =
        let output = api.Output
        let source = il.Local output.Reference
        let fields = Array.init 11 (fun _ -> il.Local output.UInt32)
        let reference = il.Local output.Reference
        let position = il.Local output.Int32
        let digit = il.Local output.Int32
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        il.EmitInt(OpCodes.Ldarga,0); il.EmitMethod(OpCodes.Call,api.Source); save source
        for index = 0 to 10 do
            load source
            let offset = if index = 0 then 0 elif index = 1 then 4 elif index = 2 then 6 else index+5
            if offset <> 0 then constant offset; CodecAddressEmitter.offset il output.Offset 1 0
            if index = 0 then il.EmitMethod(OpCodes.Call,api.Read32)
            elif index < 3 then il.EmitMethod(OpCodes.Call,api.Read16)
            else il.Emit OpCodes.Ldind_U1
            save fields[index]
        let body storeCharacter =
            let mutable offset = 0
            for field = 0 to 10 do
                for shift = (if field = 0 then 28 elif field < 3 then 12 else 4) downto 0 do
                    if shift % 4 = 0 then
                        load fields[field]
                        if shift <> 0 then constant shift; il.Emit OpCodes.Shr_Un
                        constant 15; il.Emit OpCodes.And; save digit
                        storeCharacter offset (fun () -> CodecOutputEmitter.lowerHex il digit)
                        offset <- offset+1
                if field = 0 || field = 1 || field = 2 || field = 4 then
                    storeCharacter offset (fun () -> constant(int '-'))
                    offset <- offset+1
        let acquire() =
            destination(); il.EmitMethod(OpCodes.Call,output.Start); save reference
            state(); il.EmitMethod(OpCodes.Call,output.Position); save position
        let store offset value = CodecOutputEmitter.store il output utf16 reference
                                    (fun () -> load position; constant offset; il.Emit OpCodes.Add) value
        CodecOutputEmitter.withDestination il output state (fun selected mode concrete ->
            let complete = il.Label()
            if mode = 1 then
                let partial = il.Label()
                destination(); state(); constant 38; il.EmitMethod(OpCodes.Call,api.ReserveKnownSpan)
                il.EmitBranch(OpCodes.Brfalse,partial)
                acquire()
                store 0 (fun () -> constant(int '"'))
                body (fun offset value -> store (offset+1) value)
                store 37 (fun () -> constant(int '"'))
                state(); constant 38; il.EmitMethod(OpCodes.Call,output.Advance)
                il.EmitBranch(OpCodes.Br,complete)
                il.Mark partial
            CodecOutputEmitter.literalMode il output utf16 selected mode concrete [|int '"'|] destination state
            CodecOutputEmitter.checkCount il output state (fun () -> constant 36)
            if mode = 1 then
                save selected.Minimum
                CodecOutputEmitter.admitSpan il output selected destination state
                acquire()
                body store
                state(); constant 36; il.EmitMethod(OpCodes.Call,output.Advance)
            else
                il.Emit OpCodes.Pop
                if mode = 2 then
                    if concrete then
                        load selected.ConcreteList; constant 36; il.EmitMethod(OpCodes.Call,output.ReserveList)
                    body (fun _ value -> CodecOutputEmitter.listAppend il output utf16 selected state concrete true value)
            if mode <> 0 then
                CodecOutputEmitter.literalMode il output utf16 selected mode concrete [|int '"'|] destination state
            il.Mark complete)
        il.Emit OpCodes.Ret
