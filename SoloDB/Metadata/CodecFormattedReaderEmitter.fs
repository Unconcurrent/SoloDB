namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecBase64ReaderApi<'Type,'Method> = {
    Byte: 'Type
    Bytes: 'Type
    Int32: 'Type
    Reference: 'Type
    Input: 'Method
    Output: 'Method
    Empty: 'Method
    Offset: 'Method
    Compatibility: 'Method
}

module internal CodecFormattedReaderEmitter =
    /// Exact D-format ASCII establishes lexical admission while consuming each unit once.
    let guid (il: CodecIL<'T,'M,'F>) (api: CodecStringApi<'T,'M>) constructor utf16 =
        CodecStringEmitter.readerWith il api utf16 (fun frame ->
            let load n = il.EmitLocal(OpCodes.Ldloc,n)
            let save n = il.EmitLocal(OpCodes.Stloc,n)
            let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
            let fallback = il.Label()
            let mismatch = il.Label()
            let digit = il.Local api.Int32
            let fields = [|for _ = 0 to 10 do yield il.Local api.Int32|]
            load frame.Length; load frame.Start; il.Emit OpCodes.Sub; constant 37
            il.EmitBranch(OpCodes.Blt,fallback)
            let read offset =
                load frame.Start; constant offset; il.Emit OpCodes.Add; save frame.Index
                load frame.Source; load frame.Index
                CodecAddressEmitter.offset il api.OffsetByte (if utf16 then 2 else 1) 0
                il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save frame.Point
            let hex() =
                let letter = il.Label()
                let complete = il.Label()
                load frame.Point; constant 48; il.Emit OpCodes.Sub; save digit
                load digit; constant 9; il.EmitBranch(OpCodes.Bgt_Un,letter)
                il.EmitBranch(OpCodes.Br,complete)
                il.Mark letter
                load frame.Point; constant 32; il.Emit OpCodes.Or; constant 97; il.Emit OpCodes.Sub; save digit
                load digit; constant 5; il.EmitBranch(OpCodes.Bgt_Un,mismatch)
                load digit; constant 10; il.Emit OpCodes.Add; save digit
                il.Mark complete
            let number field start count =
                constant 0; save field
                for offset = start to start+count-1 do
                    read offset; hex()
                    load field; constant 4; il.Emit OpCodes.Shl; load digit; il.Emit OpCodes.Or; save field
            number fields[0] 0 8
            for offset in [8;13;18;23] do
                read offset; load frame.Point; constant 45; il.EmitBranch(OpCodes.Bne_Un,mismatch)
                match offset with
                | 8 -> number fields[1] 9 4
                | 13 -> number fields[2] 14 4
                | 18 -> number fields[3] 19 2; number fields[4] 21 2
                | _ -> for n = 0 to 5 do number fields[n+5] (24+n*2) 2
            read 36; load frame.Point; load frame.Quote; il.EmitBranch(OpCodes.Bne_Un,mismatch)
            for field in fields do load field
            il.EmitMethod(OpCodes.Newobj,constructor)
            load frame.Position; load frame.Index; constant 1; il.Emit OpCodes.Add; il.Emit OpCodes.Stind_I4
            match api.Complete with
            | Some complete -> il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,complete)
            | None -> ()
            il.Emit OpCodes.Ret
            // The validated ASCII prefix is retained; the stopping unit enters shared framing directly.
            il.Mark mismatch
            load frame.Index; constant 1; il.Emit OpCodes.Add; save frame.ScalarEnd
            il.EmitBranch(OpCodes.Br,frame.ScalarPoint)
            il.Mark fallback)

    /// Framed ASCII decodes directly into its final array; alternate forms retain lexical ownership.
    let base64 (il: CodecIL<'T,'M,'F>) (api: CodecBase64ReaderApi<'T,'M>) utf16 =
        let source = il.Local api.Reference
        let target = il.Local api.Reference
        let result = il.Local api.Bytes
        let count = il.Local api.Int32
        let padding = il.Local api.Int32
        let length = il.Local api.Int32
        let index = il.Local api.Int32
        let written = il.Local api.Int32
        let point = il.Local api.Int32
        let digits = [|for _ = 0 to 3 do yield il.Local api.Int32|]
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let compatibility = il.Label()
        let nonempty = il.Label()
        let loop = il.Label()
        let terminal = il.Label()
        let one = il.Label()
        let two = il.Label()
        let complete = il.Label()
        il.Emit OpCodes.Ldarg_2; save count
        load count; il.EmitBranch(OpCodes.Brtrue,nonempty)
        il.EmitMethod(OpCodes.Call,api.Empty); il.Emit OpCodes.Ret
        il.Mark nonempty
        load count; constant 3; il.Emit OpCodes.And; il.EmitBranch(OpCodes.Brtrue,compatibility)
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Input)
        il.Emit OpCodes.Ldarg_1; CodecAddressEmitter.offset il api.Offset (if utf16 then 2 else 1) 0; save source
        let read at =
            load source; at(); CodecAddressEmitter.offset il api.Offset (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1); save point
        let value n =
            let lower = il.Label()
            let numeric = il.Label()
            let plus = il.Label()
            let slash = il.Label()
            let doneDigit = il.Label()
            load point; constant 65; il.Emit OpCodes.Sub; save n
            load n; constant 25; il.EmitBranch(OpCodes.Bgt_Un,lower)
            il.EmitBranch(OpCodes.Br,doneDigit)
            il.Mark lower
            load point; constant 97; il.Emit OpCodes.Sub; save n
            load n; constant 25; il.EmitBranch(OpCodes.Bgt_Un,numeric)
            load n; constant 26; il.Emit OpCodes.Add; save n; il.EmitBranch(OpCodes.Br,doneDigit)
            il.Mark numeric
            load point; constant 48; il.Emit OpCodes.Sub; save n
            load n; constant 9; il.EmitBranch(OpCodes.Bgt_Un,plus)
            load n; constant 52; il.Emit OpCodes.Add; save n; il.EmitBranch(OpCodes.Br,doneDigit)
            il.Mark plus
            load point; constant 43; il.EmitBranch(OpCodes.Bne_Un,slash)
            constant 62; save n; il.EmitBranch(OpCodes.Br,doneDigit)
            il.Mark slash
            load point; constant 47; il.EmitBranch(OpCodes.Bne_Un,compatibility)
            constant 63; save n
            il.Mark doneDigit
        let noPadding = il.Label()
        let onePadding = il.Label()
        read(fun () -> load count; constant 1; il.Emit OpCodes.Sub)
        load point; constant 61; il.EmitBranch(OpCodes.Bne_Un,noPadding)
        constant 1; save padding
        read(fun () -> load count; constant 2; il.Emit OpCodes.Sub)
        load point; constant 61; il.EmitBranch(OpCodes.Bne_Un,onePadding)
        constant 2; save padding
        il.Mark onePadding
        il.Mark noPadding
        load count; constant 4; il.Emit OpCodes.Div; constant 3; il.Emit OpCodes.Mul; load padding; il.Emit OpCodes.Sub; save length
        load length; il.EmitType(OpCodes.Newarr,api.Byte); save result
        load result; il.EmitMethod(OpCodes.Call,api.Output); save target
        constant 0; save index; constant 0; save written
        let store value =
            load target; load written; CodecAddressEmitter.offset il api.Offset 1 0
            value(); il.Emit OpCodes.Stind_I1
            load written; constant 1; il.Emit OpCodes.Add; save written
        il.Mark loop
        for n = 0 to 1 do read(fun () -> load index; constant n; il.Emit OpCodes.Add); value digits[n]
        store(fun () -> load digits[0]; constant 2; il.Emit OpCodes.Shl; load digits[1]; constant 4; il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Or)
        load index; load count; constant 4; il.Emit OpCodes.Sub; il.EmitBranch(OpCodes.Beq,terminal)
        for n = 2 to 3 do read(fun () -> load index; constant n; il.Emit OpCodes.Add); value digits[n]
        store(fun () -> load digits[1]; constant 4; il.Emit OpCodes.Shl; load digits[2]; constant 2; il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Or)
        store(fun () -> load digits[2]; constant 6; il.Emit OpCodes.Shl; load digits[3]; il.Emit OpCodes.Or)
        load index; constant 4; il.Emit OpCodes.Add; save index
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark terminal
        load padding; constant 2; il.EmitBranch(OpCodes.Beq,one)
        read(fun () -> load index; constant 2; il.Emit OpCodes.Add); value digits[2]
        store(fun () -> load digits[1]; constant 4; il.Emit OpCodes.Shl; load digits[2]; constant 2; il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Or)
        load padding; constant 1; il.EmitBranch(OpCodes.Beq,two)
        read(fun () -> load index; constant 3; il.Emit OpCodes.Add); value digits[3]
        store(fun () -> load digits[2]; constant 6; il.Emit OpCodes.Shl; load digits[3]; il.Emit OpCodes.Or)
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark one
        read(fun () -> load index; constant 2; il.Emit OpCodes.Add)
        load point; constant 61; il.EmitBranch(OpCodes.Bne_Un,compatibility)
        il.Mark two
        read(fun () -> load index; constant 3; il.Emit OpCodes.Add)
        load point; constant 61; il.EmitBranch(OpCodes.Bne_Un,compatibility)
        il.Mark complete
        load result; il.Emit OpCodes.Ret
        il.Mark compatibility
        il.Emit OpCodes.Ldarg_0; il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,api.Compatibility); il.Emit OpCodes.Ret
