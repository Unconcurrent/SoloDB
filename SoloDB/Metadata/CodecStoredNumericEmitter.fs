namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecStoredNumericApi<'T,'M,'F> =
    { Output: CodecOutputApi<'T,'M,'F>
      Prepared: 'T
      Prepare: 'M
      Validate: 'M
      Length: 'M
      Utf8Length: 'M
      Unit: 'M }

/// Both destinations expand the same selected sink and canonical-key stores.
module internal CodecStoredNumericEmitter =
    let private outputWriter (il: CodecIL<'T,'M,'F>) (facts: CodecStoredNumericApi<'T,'M,'F>) utf16 =
        let api = facts.Output
        let prepared = il.Local facts.Prepared
        let symbols = il.Local api.Int32
        let units = il.Local api.Int32
        let index = il.Local api.Int32
        let current = il.Local api.Int32
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        il.InputValue(); il.EmitMethod(OpCodes.Call,facts.Prepare); save prepared
        load prepared; il.EmitMethod(OpCodes.Call,facts.Length); save symbols
        load prepared; il.EmitMethod(OpCodes.Call,if utf16 then facts.Length else facts.Utf8Length)
        il.Emit OpCodes.Ldc_I4_2; il.Emit OpCodes.Add_Ovf; save units
        let body append =
            append(fun () -> il.EmitInt(OpCodes.Ldc_I4,34))
            il.Emit OpCodes.Ldc_I4_0; save index
            let loop = il.Label()
            let finish = il.Label()
            il.Mark loop
            load index; load symbols; il.EmitBranch(OpCodes.Bge,finish)
            load prepared; load index; il.EmitMethod(OpCodes.Call,facts.Unit); save current
            if utf16 then append(fun () -> load current)
            else
                let ascii = il.Label()
                let next = il.Label()
                load current; il.EmitInt(OpCodes.Ldc_I4,128); il.EmitBranch(OpCodes.Blt,ascii)
                append(fun () -> load current; il.Emit OpCodes.Ldc_I4_6; il.Emit OpCodes.Shr_Un
                                 il.EmitInt(OpCodes.Ldc_I4,192); il.Emit OpCodes.Or)
                append(fun () -> load current; il.EmitInt(OpCodes.Ldc_I4,63); il.Emit OpCodes.And
                                 il.EmitInt(OpCodes.Ldc_I4,128); il.Emit OpCodes.Or)
                il.EmitBranch(OpCodes.Br,next)
                il.Mark ascii
                append(fun () -> load current)
                il.Mark next
            load index; il.Emit OpCodes.Ldc_I4_1; il.Emit OpCodes.Add; save index
            il.EmitBranch(OpCodes.Br,loop)
            il.Mark finish
            append(fun () -> il.EmitInt(OpCodes.Ldc_I4,34))
        CodecOutputEmitter.withDestination il api state (fun selected mode ->
            CodecOutputEmitter.checkCount il api state (fun () -> load units)
            if mode = 1 then
                save selected.Minimum
                CodecOutputEmitter.admitSpan il api selected destination state
                let reference = il.Local api.Reference
                let position = il.Local api.Int32
                destination(); il.EmitMethod(OpCodes.Call,api.Start); save reference
                state(); il.EmitMethod(OpCodes.Call,api.Position); save position
                body (fun value ->
                    CodecOutputEmitter.store il api utf16 reference (fun () -> load position) value
                    load position; il.Emit OpCodes.Ldc_I4_1; il.Emit OpCodes.Add; save position)
                state(); load units; il.EmitMethod(OpCodes.Call,api.Advance)
            else
                il.Emit OpCodes.Pop
                if mode = 2 then
                    body (CodecOutputEmitter.listAppend il api utf16 state))
        il.Emit OpCodes.Ret

    let writer (il: CodecIL<'T,'M,'F>) (facts: CodecStoredNumericApi<'T,'M,'F>) utf16 =
        if facts.Output.Kind = CodecOutputKind.Validation then
            il.InputValue()
            il.EmitInt(OpCodes.Ldc_I4,if utf16 then 1 else 0)
            il.EmitMethod(OpCodes.Call,facts.Validate)
            il.Emit OpCodes.Ret
        else outputWriter il facts utf16
