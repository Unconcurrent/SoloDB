namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecDigitWriterApi<'Type,'Method,'Field> = {
    Output: CodecOutputApi<'Type,'Method,'Field>
    DigitPairs: 'Method
    ReadPair: 'Method
    WritePair: 'Method
}

/// Expands decimal groups into the caller's admitted span or original ordered Add operations.
module internal CodecDigitWriterEmitter =
    let capture (il: CodecIL<'T,'M,'F>) (api: CodecDigitWriterApi<'T,'M,'F>) needed =
        let image = il.Local api.Output.Reference
        let complete = il.Label()
        needed(); il.EmitBranch(OpCodes.Brfalse,complete)
        il.EmitMethod(OpCodes.Call,api.DigitPairs)
        il.EmitLocal(OpCodes.Stloc,image)
        il.Mark complete
        image

    let private selectCount (il: CodecIL<'T,'M,'F>) count maximum body =
        let complete = il.Label()
        let rec choose low high =
            if low = high then body low; il.EmitBranch(OpCodes.Br,complete)
            else
                let upper = il.Label()
                let middle = (low+high)/2
                il.EmitLocal(OpCodes.Ldloc,count); il.EmitInt(OpCodes.Ldc_I4,middle)
                il.EmitBranch(OpCodes.Bgt,upper)
                choose low middle
                il.Mark upper
                choose (middle+1) high
        choose 1 maximum
        il.Mark complete

    let private unit (il: CodecIL<'T,'M,'F>) (api: CodecDigitWriterApi<'T,'M,'F>) utf16 image pair offset =
        il.EmitLocal(OpCodes.Ldloc,image)
        il.EmitLocal(OpCodes.Ldloc,pair); il.EmitInt(OpCodes.Ldc_I4,2); il.Emit OpCodes.Mul
        if offset <> 0 then il.EmitInt(OpCodes.Ldc_I4,offset); il.Emit OpCodes.Add
        CodecAddressEmitter.offset il api.Output.Offset (if utf16 then 2 else 1) 0
        il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1)

    let private reverseRun (il: CodecIL<'T,'M,'F>) (api: CodecDigitWriterApi<'T,'M,'F>) utf16 image value count target index consume =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let remaining = il.Local api.Output.Int32
        let quotient = il.Local api.Output.UInt32
        let block = il.Local api.Output.UInt32
        let pair = il.Local api.Output.UInt32
        let writePair() =
            load target; load index; constant 1; il.Emit OpCodes.Sub
            CodecAddressEmitter.offset il api.Output.Offset (if utf16 then 2 else 1) 0
            load image; load pair; constant 2; il.Emit OpCodes.Mul
            CodecAddressEmitter.offset il api.Output.Offset (if utf16 then 2 else 1) 0
            il.EmitMethod(OpCodes.Call,api.ReadPair); il.EmitMethod(OpCodes.Call,api.WritePair)
            load index; constant 2; il.Emit OpCodes.Sub; save index
        let extractPair() =
            load value; constant 100; il.Emit OpCodes.Div_Un; save quotient
            load value; load quotient; constant 100; il.Emit OpCodes.Mul; il.Emit OpCodes.Sub; save pair
            load quotient; save value
        let loop = il.Label()
        let tail = il.Label()
        let lastPair = il.Label()
        let single = il.Label()
        let complete = il.Label()
        load count; save remaining
        il.Mark loop
        load remaining; constant 4; il.EmitBranch(OpCodes.Blt,tail)
        load value; constant 10000; il.Emit OpCodes.Div_Un; save quotient
        load value; load quotient; constant 10000; il.Emit OpCodes.Mul; il.Emit OpCodes.Sub; save block
        load quotient; save value
        load block; constant 100; il.Emit OpCodes.Rem_Un; save pair; writePair()
        load block; constant 100; il.Emit OpCodes.Div_Un; save pair; writePair()
        load remaining; constant 4; il.Emit OpCodes.Sub; save remaining
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark tail
        load remaining; constant 3; il.EmitBranch(OpCodes.Bne_Un,lastPair)
        extractPair(); writePair()
        constant 1; save remaining
        il.Mark lastPair
        load remaining; constant 2; il.EmitBranch(OpCodes.Bne_Un,single)
        if consume then extractPair() else load value; save pair
        writePair(); il.EmitBranch(OpCodes.Br,complete)
        il.Mark single
        load remaining; il.EmitBranch(OpCodes.Brfalse,complete)
        if consume then
            load value; constant 10; il.Emit OpCodes.Div_Un; save quotient
            load value; load quotient; constant 10; il.Emit OpCodes.Mul; il.Emit OpCodes.Sub; save pair
            load quotient; save value
        else load value; save pair
        CodecOutputEmitter.store il api.Output utf16 target (fun () -> load index) (fun () -> load pair; constant(int '0'); il.Emit OpCodes.Add)
        load index; constant 1; il.Emit OpCodes.Sub; save index
        il.Mark complete

    let private reverse32 (il: CodecIL<'T,'M,'F>) api utf16 image value count target index separator =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        match separator with
        | None -> reverseRun il api utf16 image value count target index false
        | Some dot ->
            let regular = il.Label()
            let contiguous = il.Label()
            let complete = il.Label()
            let right = il.Local api.Output.Int32
            let left = il.Local api.Output.Int32
            load index; load dot; il.EmitBranch(OpCodes.Bne_Un,regular)
            load index; constant 1; il.Emit OpCodes.Sub; save index
            il.Mark regular
            load dot; constant 0; il.EmitBranch(OpCodes.Blt,contiguous)
            load index; load dot; il.Emit OpCodes.Sub; save right
            load right; constant 0; il.EmitBranch(OpCodes.Ble,contiguous)
            load right; load count; il.EmitBranch(OpCodes.Bge,contiguous)
            reverseRun il api utf16 image value right target index true
            load index; constant 1; il.Emit OpCodes.Sub; save index
            load count; load right; il.Emit OpCodes.Sub; save left
            reverseRun il api utf16 image value left target index false
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark contiguous
            reverseRun il api utf16 image value count target index false
            il.Mark complete

    let private forwardRun (il: CodecIL<'T,'M,'F>) (api: CodecDigitWriterApi<'T,'M,'F>) utf16 image value count maximum append =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        if maximum = 1 then append(fun () -> load value; constant(int '0'); il.Emit OpCodes.Add)
        else
            let groupDigits = il.Local api.Output.Int32
            let block = il.Local api.Output.UInt32
            let pair = il.Local api.Output.UInt32
            let power = il.Local api.Output.UInt32
            let loop = il.Label()
            let ready = il.Label()
            let complete = il.Label()
            selectCount il count maximum (fun digits ->
                let first = (digits - 1) % 4 + 1
                constant first; save groupDigits
                constant(int(pown 10u (digits - first))); save power)
            il.Mark loop
            load value; load power; il.Emit OpCodes.Div_Un; save block
            load value; load block; load power; il.Emit OpCodes.Mul; il.Emit OpCodes.Sub; save value
            let cases = Array.init 4 (fun _ -> il.Label())
            load groupDigits; constant 1; il.Emit OpCodes.Sub; il.EmitSwitch cases
            il.EmitBranch(OpCodes.Br,complete)
            for digits in 1..4 do
                il.Mark cases[digits - 1]
                if digits >= 3 then
                    load block; constant 100; il.Emit OpCodes.Div_Un; save pair
                    if digits = 3 then append(fun () -> load pair; constant(int '0'); il.Emit OpCodes.Add)
                    else
                        append(fun () -> unit il api utf16 image pair 0)
                        append(fun () -> unit il api utf16 image pair 1)
                    load block; constant 100; il.Emit OpCodes.Rem_Un; save block
                if digits = 1 then append(fun () -> load block; constant(int '0'); il.Emit OpCodes.Add)
                else
                    load block; save pair
                    append(fun () -> unit il api utf16 image pair 0)
                    append(fun () -> unit il api utf16 image pair 1)
                il.EmitBranch(OpCodes.Br,ready)
            il.Mark ready
            load power; constant 10000; il.Emit OpCodes.Div_Un; save power
            load power; il.EmitBranch(OpCodes.Brfalse,complete)
            constant 4; save groupDigits
            il.EmitBranch(OpCodes.Br,loop)
            il.Mark complete

    let private forward32 (il: CodecIL<'T,'M,'F>) api utf16 image value count maximum logical separator append =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        match separator with
        | None -> forwardRun il api utf16 image value count maximum append
        | Some dot when maximum = 1 ->
            let ready = il.Label()
            load dot; load logical; il.EmitBranch(OpCodes.Bne_Un,ready)
            append(fun () -> constant(int '.'))
            il.Mark ready
            forwardRun il api utf16 image value count 1 append
        | Some dot ->
            let unsplit = il.Label()
            let ready = il.Label()
            let complete = il.Label()
            let left = il.Local api.Output.Int32
            let right = il.Local api.Output.Int32
            let power = il.Local api.Output.UInt32
            let head = il.Local api.Output.UInt32
            let tail = il.Local api.Output.UInt32
            load dot; load logical; il.EmitBranch(OpCodes.Bne_Un,ready)
            append(fun () -> constant(int '.'))
            il.Mark ready
            load dot; load logical; il.Emit OpCodes.Sub; save left
            load left; constant 0; il.EmitBranch(OpCodes.Ble,unsplit)
            load left; load count; il.EmitBranch(OpCodes.Bge,unsplit)
            load count; load left; il.Emit OpCodes.Sub; save right
            selectCount il right (max 1 (maximum-1)) (fun digits -> constant(int(pown 10u digits)); save power)
            load value; load power; il.Emit OpCodes.Div_Un; save head
            load value; load head; load power; il.Emit OpCodes.Mul; il.Emit OpCodes.Sub; save tail
            forwardRun il api utf16 image head left (maximum-1) append
            append(fun () -> constant(int '.'))
            forwardRun il api utf16 image tail right (maximum-1) append
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark unsplit
            forwardRun il api utf16 image value count maximum append
            il.Mark complete
        load logical; load count; il.Emit OpCodes.Add; save logical

    /// Decimal runs are already native UInt32 chunks with independent padding counts.
    let reverseChunk il api utf16 image value count target index separator =
        reverse32 il api utf16 image value count target index separator

    let forwardChunk il api utf16 image value count maximum logical separator append =
        forward32 il api utf16 image value count maximum logical separator append

    let private wideChunks (il: CodecIL<'T,'M,'F>) (api: CodecDigitWriterApi<'T,'M,'F>) value count maximum body =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let chunks = Array.init ((maximum+8)/9) (fun _ -> il.Local api.Output.UInt32)
        let current = il.Local api.Output.UInt64
        let quotient = il.Local api.Output.UInt64
        let captured = il.Label()
        let wide = il.Label()
        let full = il.Label()
        load count; constant 9; il.EmitBranch(OpCodes.Bgt,wide)
        load value; il.Emit OpCodes.Conv_U4; save chunks[0]
        il.EmitBranch(OpCodes.Br,captured)
        let divide chunk =
            load current; il.EmitInt64 1000000000L; il.Emit OpCodes.Div_Un; save quotient
            load current; load quotient; il.EmitInt64 1000000000L; il.Emit OpCodes.Mul; il.Emit OpCodes.Sub; il.Emit OpCodes.Conv_U4; save chunk
            load quotient; save current
        il.Mark wide
        load value; save current
        divide chunks[0]
        if maximum > 18 then
            load count; constant 18; il.EmitBranch(OpCodes.Bgt,full)
        load current; il.Emit OpCodes.Conv_U4; save chunks[1]
        il.EmitBranch(OpCodes.Br,captured)
        il.Mark full
        if maximum > 18 then
            divide chunks[1]
            load current; il.Emit OpCodes.Conv_U4; save chunks[2]
        il.Mark captured
        body chunks

    let reverse (il: CodecIL<'T,'M,'F>) api utf16 image wide value count maximum target index separator =
        if not wide then reverse32 il api utf16 image value count target index separator
        else
            wideChunks il api value count maximum (fun chunks ->
                let remaining = il.Local api.Output.Int32
                for chunk = 0 to chunks.Length-1 do
                    let absent = il.Label()
                    let bounded = il.Label()
                    il.EmitLocal(OpCodes.Ldloc,count); il.EmitInt(OpCodes.Ldc_I4,chunk*9); il.Emit OpCodes.Sub
                    il.EmitLocal(OpCodes.Stloc,remaining)
                    il.EmitLocal(OpCodes.Ldloc,remaining); il.EmitInt(OpCodes.Ldc_I4,0); il.EmitBranch(OpCodes.Ble,absent)
                    il.EmitLocal(OpCodes.Ldloc,remaining); il.EmitInt(OpCodes.Ldc_I4,9); il.EmitBranch(OpCodes.Ble,bounded)
                    il.EmitInt(OpCodes.Ldc_I4,9); il.EmitLocal(OpCodes.Stloc,remaining)
                    il.Mark bounded
                    reverse32 il api utf16 image chunks[chunk] remaining target index separator
                    il.Mark absent)

    let forward (il: CodecIL<'T,'M,'F>) api utf16 image wide value count maximum logical separator append =
        if not wide then forward32 il api utf16 image value count maximum logical separator append
        else
            wideChunks il api value count maximum (fun chunks ->
                let remaining = il.Local api.Output.Int32
                for chunk = chunks.Length-1 downto 0 do
                    let absent = il.Label()
                    let bounded = il.Label()
                    il.EmitLocal(OpCodes.Ldloc,count); il.EmitInt(OpCodes.Ldc_I4,chunk*9); il.Emit OpCodes.Sub
                    il.EmitLocal(OpCodes.Stloc,remaining)
                    il.EmitLocal(OpCodes.Ldloc,remaining); il.EmitInt(OpCodes.Ldc_I4,0); il.EmitBranch(OpCodes.Ble,absent)
                    il.EmitLocal(OpCodes.Ldloc,remaining); il.EmitInt(OpCodes.Ldc_I4,9); il.EmitBranch(OpCodes.Ble,bounded)
                    il.EmitInt(OpCodes.Ldc_I4,9); il.EmitLocal(OpCodes.Stloc,remaining)
                    il.Mark bounded
                    forward32 il api utf16 image chunks[chunk] remaining (min 9 (maximum-chunk*9)) logical separator append
                    il.Mark absent)
