namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecBase64WideningApi<'Method> = {
    UnpackLow: 'Method
    UnpackHigh: 'Method
}

type internal CodecBase64VectorApi<'Type,'Method> = {
    Bytes: 'Type
    Unsigned: 'Type
    Supported: 'Method
    CreateWord: 'Method
    CreateWords: 'Method
    CreateByte: 'Method
    WordToBytes: 'Method
    BytesToUnsigned: 'Method
    UnsignedToBytes: 'Method
    BytesToSignedBytes: 'Method
    SignedBytesToBytes: 'Method
    Read: 'Method
    Write: 'Method
    Shuffle: 'Method
    And: 'Method
    Or: 'Method
    Right: 'Method
    Left: 'Method
    GreaterThan: 'Method
    Add: 'Method
    Widening: CodecBase64WideningApi<'Method> option
}

/// Four independent 24-bit lanes become four six-bit symbols each.
module internal CodecBase64VectorEmitter =
    let write (il: CodecIL<'T,'M,'F>) (output: CodecOutputApi<'T,'M,'F>)
              (api: CodecBase64VectorApi<'T,'M>) utf16 source count index destination position =
        let permutation = il.Local api.Bytes
        let masks = [ for shift,mask in [18,0x3fu;4,0x3f00u;-10,0x3f0000u;-24,0x3f000000u] -> shift,mask,il.Local api.Bytes ]
        let triples = il.Local api.Unsigned
        let symbols = il.Local api.Bytes
        let encoded = il.Local api.Bytes
        let numeric = il.Local api.Bytes
        let exceptions = il.Local api.Bytes
        let zero = if utf16 then Some(il.Local api.Bytes) else None
        let load value = il.EmitLocal(OpCodes.Ldloc,value)
        let save value = il.EmitLocal(OpCodes.Stloc,value)
        let constant value = il.EmitInt(OpCodes.Ldc_I4,value)
        let call method = il.EmitMethod(OpCodes.Call,method)
        let complete = il.Label()
        let loop = il.Label()
        let extent() = load index; load count; constant 16; il.Emit OpCodes.Sub; il.EmitBranch(OpCodes.Bgt,complete)
        extent()
        call api.Supported; il.EmitBranch(OpCodes.Brfalse,complete)
        // Each word contains c,b,a,0, hence the original triplet's integer value.
        for lane in 0..3 do
            let first = 3 * lane
            constant (int (0x80000000u ||| (uint32 first <<< 16) ||| (uint32(first + 1) <<< 8) ||| uint32(first + 2)))
        call api.CreateWords; call api.WordToBytes; save permutation
        for _,mask,target in masks do
            constant (int mask); call api.CreateWord; call api.WordToBytes; save target
        for word in [0;0;0;int 0xf4f10000u] do constant word
        call api.CreateWords; call api.WordToBytes; save exceptions
        match zero with Some target -> constant 0; call api.CreateByte; save target | None -> ()
        il.Mark loop
        load source; load index; CodecAddressEmitter.offset il output.Offset 1 0
        call api.Read; load permutation; call api.Shuffle; call api.BytesToUnsigned; save triples
        constant 0; call api.CreateByte; save symbols
        for shift,_,mask in masks do
            load triples
            if shift > 0 then constant shift; call api.Right
            else constant (-shift); call api.Left
            call api.UnsignedToBytes; load mask; call api.And
            load symbols; call api.Or; save symbols
        load symbols; constant 65; call api.CreateByte; call api.Add; save encoded
        // The two contiguous ranges need offsets; the final two symbols
        // have independent corrections selected only within the numeric range.
        load symbols; call api.BytesToSignedBytes
        constant 25; call api.CreateByte; call api.BytesToSignedBytes; call api.GreaterThan
        call api.SignedBytesToBytes; constant 6; call api.CreateByte; call api.And
        load encoded; call api.Add; save encoded
        load symbols; call api.BytesToSignedBytes
        constant 51; call api.CreateByte; call api.BytesToSignedBytes; call api.GreaterThan
        call api.SignedBytesToBytes; save numeric
        load numeric; constant 181; call api.CreateByte; call api.And
        load encoded; call api.Add; save encoded
        load exceptions; load symbols; call api.Shuffle; load numeric; call api.And
        load encoded; call api.Add; save encoded
        let store offset unpack =
            load destination; load position
            CodecAddressEmitter.offset il output.Offset (if utf16 then 2 else 1) offset
            load encoded
            match unpack,zero with
            | Some method,Some target -> load target; call method
            | _ -> ()
            call api.Write
        if utf16 then
            let widening = api.Widening.Value
            store 0 (Some widening.UnpackLow); store 16 (Some widening.UnpackHigh)
        else store 0 None
        load index; constant 12; il.Emit OpCodes.Add; save index
        load position; constant 16; il.Emit OpCodes.Add; save position
        load index; load count; constant 16; il.Emit OpCodes.Sub; il.EmitBranch(OpCodes.Ble,loop)
        il.Mark complete
