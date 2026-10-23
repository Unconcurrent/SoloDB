namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit
open SoloDatabase.JsonSerializator

type internal CodecStringWriterApi<'Type,'Method,'Field> = {
    Output: CodecOutputApi<'Type,'Method,'Field>
    Reference: 'Method
    Length: 'Method
    ReadWord: 'Method
    WriteWord: 'Method
}

module internal CodecStringWriterEmitter =
    let private escaped =
        [| for point = 0 to 65535 do
               let code = JsonStringLiteral.escape (char point)
               if code <> 0 then yield point,code |]

    let writer (il: CodecIL<'T,'M,'F>) (api: CodecStringWriterApi<'T,'M,'F>) utf16 =
        let output = api.Output
        let source = il.Local output.Reference
        let length = il.Local output.Int32
        let index = il.Local output.Int32
        let run = il.Local output.Int32
        let byteCount = il.Local output.Int32
        let point = il.Local output.Int32
        let following = il.Local output.Int32
        let escapedUnit = il.Local output.Int32
        let word = il.Local output.UInt64
        let candidate = il.Local output.UInt64
        let digit = il.Local output.Int32
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        let read at =
            load source; at(); CodecAddressEmitter.offset il output.Offset 2 0; il.Emit OpCodes.Ldind_U2
        // The scan supplies exact UTF8 length before callbacks; emission never recounts it.
        let codePoint at finish code low =
            let ordinary = il.Label()
            let replacement = il.Label()
            let complete = il.Label()
            load code; constant 0xD800; il.Emit OpCodes.Sub; constant 0x7FF; il.EmitBranch(OpCodes.Bgt_Un,ordinary)
            load code; constant 0xDBFF; il.EmitBranch(OpCodes.Bgt_Un,replacement)
            load at; constant 1; il.Emit OpCodes.Add; load finish; il.EmitBranch(OpCodes.Bge,replacement)
            read(fun () -> load at; constant 1; il.Emit OpCodes.Add); save low
            load low; constant 0xDC00; il.Emit OpCodes.Sub; constant 0x3FF; il.EmitBranch(OpCodes.Bgt_Un,replacement)
            load code; constant 0xD800; il.Emit OpCodes.Sub; constant 10; il.Emit OpCodes.Shl
            load low; constant 0xDC00; il.Emit OpCodes.Sub; il.Emit OpCodes.Add
            constant 0x10000; il.Emit OpCodes.Add; save code
            load at; constant 1; il.Emit OpCodes.Add; save at
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark replacement
            constant 0xFFFD; save code
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark ordinary
            il.Mark complete
        let width code body =
            let two = il.Label()
            let three = il.Label()
            let four = il.Label()
            let complete = il.Label()
            load code; constant 0x80; il.EmitBranch(OpCodes.Bge_Un,two)
            body 1; il.EmitBranch(OpCodes.Br,complete)
            il.Mark two
            load code; constant 0x800; il.EmitBranch(OpCodes.Bge_Un,three)
            body 2; il.EmitBranch(OpCodes.Br,complete)
            il.Mark three
            load code; constant 0x10000; il.EmitBranch(OpCodes.Bge_Un,four)
            body 3; il.EmitBranch(OpCodes.Br,complete)
            il.Mark four
            body 4
            il.Mark complete
        CodecOutputEmitter.withDestination il output state (fun selected mode ->
            let complete = il.Label()
            let present = il.Label()
            il.InputValue(); il.EmitBranch(OpCodes.Brtrue,present)
            CodecOutputEmitter.literalMode il output utf16 selected mode [|110;117;108;108|] destination state
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark present
            if mode <> 0 then
                CodecOutputEmitter.literalMode il output utf16 selected mode [|34|] destination state
                il.InputValue(); il.EmitMethod(OpCodes.Call,api.Reference); save source
                il.InputValue(); il.EmitMethod(OpCodes.Callvirt,api.Length); save length
                constant 0; save index; constant 0; save run; constant 0; save byteCount
                let writeRun() =
                    let count() = if utf16 then load index; load run; il.Emit OpCodes.Sub else load byteCount
                    let target = il.Local output.Reference
                    let position = il.Local output.Int32
                    if mode = 1 then
                        CodecOutputEmitter.checkCount il output state count; save selected.Minimum
                        CodecOutputEmitter.admitSpan il output selected destination state
                        destination(); il.EmitMethod(OpCodes.Call,output.Start); save target
                        state(); il.EmitMethod(OpCodes.Call,output.Position); save position
                    if utf16 then
                        if mode = 1 then
                            // Both regions are admitted in elements; byte scaling is native-width.
                            load target; load position; CodecAddressEmitter.offset il output.Offset 2 0
                            load source; load run; CodecAddressEmitter.offset il output.Offset 2 0
                            count(); il.Emit OpCodes.Conv_U; constant 2; il.Emit OpCodes.Conv_U; il.Emit OpCodes.Mul
                            il.EmitInt(OpCodes.Unaligned,1); il.Emit OpCodes.Cpblk
                            state(); count(); il.EmitMethod(OpCodes.Call,output.Advance)
                        else
                            CodecOutputEmitter.checkCount il output state count; il.Emit OpCodes.Pop
                            let current = il.Local output.Int32
                            let loop = il.Label()
                            let doneRun = il.Label()
                            load run; save current
                            il.Mark loop
                            load current; load index; il.EmitBranch(OpCodes.Bge,doneRun)
                            CodecOutputEmitter.listAppend il output utf16 state (fun () -> read(fun () -> load current))
                            load current; constant 1; il.Emit OpCodes.Add; save current
                            il.EmitBranch(OpCodes.Br,loop)
                            il.Mark doneRun
                    else
                        let current = il.Local output.Int32
                        let code = il.Local output.Int32
                        let low = il.Local output.Int32
                        let loop = il.Label()
                        let doneRun = il.Label()
                        load run; save current
                        let store value =
                            if mode = 1 then
                                CodecOutputEmitter.store il output false target (fun () -> load position) value
                                load position; constant 1; il.Emit OpCodes.Add; save position
                            else CodecOutputEmitter.listAppend il output false state value
                        let unicodeRun = il.Label()
                        load byteCount; load index; load run; il.Emit OpCodes.Sub; il.EmitBranch(OpCodes.Bne_Un,unicodeRun)
                        if mode = 1 then
                            let ascii = il.Label()
                            let tail = il.Label()
                            il.Mark ascii
                            load index; load current; il.Emit OpCodes.Sub; constant 4; il.EmitBranch(OpCodes.Blt,tail)
                            load source; load current; CodecAddressEmitter.offset il output.Offset 2 0; il.EmitMethod(OpCodes.Call,api.ReadWord); save word
                            load target; load position; CodecAddressEmitter.offset il output.Offset 1 0
                            for shift,mask in [0,0xFFL;8,0xFF00L;16,0xFF0000L;24,0xFF000000L] do
                                load word
                                if shift <> 0 then constant shift; il.Emit OpCodes.Shr_Un
                                il.EmitInt64 mask; il.Emit OpCodes.And
                                if shift <> 0 then il.Emit OpCodes.Or
                            il.Emit OpCodes.Conv_U4; il.EmitMethod(OpCodes.Call,api.WriteWord)
                            load current; constant 4; il.Emit OpCodes.Add; save current
                            load position; constant 4; il.Emit OpCodes.Add; save position
                            il.EmitBranch(OpCodes.Br,ascii)
                            il.Mark tail
                            let asciiTail = il.Label()
                            il.Mark asciiTail
                            load current; load index; il.EmitBranch(OpCodes.Bge,doneRun)
                            store(fun () -> read(fun () -> load current))
                            load current; constant 1; il.Emit OpCodes.Add; save current
                            il.EmitBranch(OpCodes.Br,asciiTail)
                        else
                            let ascii = il.Label()
                            il.Mark ascii
                            load current; load index; il.EmitBranch(OpCodes.Bge,doneRun)
                            CodecOutputEmitter.checkCount il output state (fun () -> constant 1); il.Emit OpCodes.Pop
                            store(fun () -> read(fun () -> load current))
                            load current; constant 1; il.Emit OpCodes.Add; save current
                            il.EmitBranch(OpCodes.Br,ascii)
                        il.Mark unicodeRun
                        il.Mark loop
                        load current; load index; il.EmitBranch(OpCodes.Bge,doneRun)
                        read(fun () -> load current); save code
                        codePoint current index code low
                        width code (fun units ->
                            if mode = 2 then
                                CodecOutputEmitter.checkCount il output state (fun () -> constant units); il.Emit OpCodes.Pop
                            if units = 1 then store(fun () -> load code)
                            else
                                store(fun () -> load code; constant(6 * (units - 1)); il.Emit OpCodes.Shr_Un; constant(if units = 2 then 0xC0 elif units = 3 then 0xE0 else 0xF0); il.Emit OpCodes.Or)
                                for shift in [for n = units-2 downto 0 do yield n*6] do
                                    store(fun () ->
                                        load code
                                        if shift <> 0 then constant shift; il.Emit OpCodes.Shr_Un
                                        constant 63; il.Emit OpCodes.And; constant 0x80; il.Emit OpCodes.Or))
                        load current; constant 1; il.Emit OpCodes.Add; save current
                        il.EmitBranch(OpCodes.Br,loop)
                        il.Mark doneRun
                        if mode = 1 then state(); count(); il.EmitMethod(OpCodes.Call,output.Advance)
                let scan = il.Label()
                let scalar = il.Label()
                let plain = il.Label()
                let special = il.Label()
                let tail = il.Label()
                let finish = il.Label()
                il.Mark scan
                load length; load index; il.Emit OpCodes.Sub; constant 4; il.EmitBranch(OpCodes.Blt,scalar)
                load source; load index; CodecAddressEmitter.offset il output.Offset 2 0; il.EmitMethod(OpCodes.Call,api.ReadWord); save word
                let high = 0x8000800080008000UL
                if not utf16 then
                    load word; il.EmitInt64(int64 0xFF80FF80FF80FF80UL); il.Emit OpCodes.And; il.EmitBranch(OpCodes.Brtrue,scalar)
                load word; il.EmitInt64 0x0020002000200020L; il.Emit OpCodes.Sub
                load word; il.Emit OpCodes.Not; il.Emit OpCodes.And
                il.EmitInt64(int64 high); il.Emit OpCodes.And; il.EmitBranch(OpCodes.Brtrue,scalar)
                for value,_ in escaped do
                    if value >= 32 && (utf16 || value < 128) then
                        load word; il.EmitInt64(int64(0x0001000100010001UL * uint64 value)); il.Emit OpCodes.Xor; save candidate
                        load candidate; il.EmitInt64 0x0001000100010001L; il.Emit OpCodes.Sub
                        load candidate; il.Emit OpCodes.Not; il.Emit OpCodes.And
                        il.EmitInt64(int64 high); il.Emit OpCodes.And; il.EmitBranch(OpCodes.Brtrue,scalar)
                if not utf16 then load byteCount; constant 4; il.Emit OpCodes.Add_Ovf; save byteCount
                load index; constant 4; il.Emit OpCodes.Add; save index
                il.EmitBranch(OpCodes.Br,scan)
                il.Mark scalar
                load index; load length; il.EmitBranch(OpCodes.Bge,tail)
                read(fun () -> load index); save point
                load point; constant 32; il.EmitBranch(OpCodes.Blt_Un,special)
                for value,_ in escaped do
                    if value >= 32 then load point; constant value; il.EmitBranch(OpCodes.Beq,special)
                il.Mark plain
                if not utf16 then
                    codePoint index length point following
                    width point (fun units -> load byteCount; constant units; il.Emit OpCodes.Add_Ovf; save byteCount)
                load index; constant 1; il.Emit OpCodes.Add; save index
                il.EmitBranch(OpCodes.Br,scan)
                il.Mark special
                load point; save escapedUnit
                let noRun = il.Label()
                load index; load run; il.EmitBranch(OpCodes.Beq,noRun)
                writeRun()
                il.Mark noRun
                let unicode = il.Label()
                let shortEscape = il.Label()
                let nextRun = il.Label()
                for value,code in escaped do
                    if code < 0 then
                        let next = il.Label()
                        load escapedUnit; constant value; il.EmitBranch(OpCodes.Bne_Un,next)
                        constant -code; save digit; il.EmitBranch(OpCodes.Br,shortEscape)
                        il.Mark next
                il.EmitBranch(OpCodes.Br,unicode)
                il.Mark shortEscape
                CodecOutputEmitter.fragmentMode il output utf16 selected mode
                    [|(fun () -> constant 92);(fun () -> load digit)|] destination state
                il.EmitBranch(OpCodes.Br,nextRun)
                il.Mark unicode
                let hex shift () =
                    load escapedUnit
                    if shift <> 0 then constant shift; il.Emit OpCodes.Shr_Un
                    constant 15; il.Emit OpCodes.And; save digit
                    CodecOutputEmitter.lowerHex il digit
                CodecOutputEmitter.fragmentMode il output utf16 selected mode
                    [|(fun () -> constant 92);(fun () -> constant 117);hex 12;hex 8;hex 4;hex 0|] destination state
                il.Mark nextRun
                load index; constant 1; il.Emit OpCodes.Add; save index
                load index; save run
                constant 0; save byteCount
                il.EmitBranch(OpCodes.Br,scan)
                il.Mark tail
                load index; load run; il.EmitBranch(OpCodes.Beq,finish)
                writeRun()
                il.Mark finish
                CodecOutputEmitter.literalMode il output utf16 selected mode [|34|] destination state
            il.Mark complete)
        il.Emit OpCodes.Ret
