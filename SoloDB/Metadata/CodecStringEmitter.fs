namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit
open SoloDatabase.JsonSerializator

type internal CodecStringApi<'Type,'Method> = {
    Int32: 'Type
    UInt64: 'Type
    SourceReference: 'Type
    PositionReference: 'Type
    Begin: 'Method
    Reference: 'Method
    Length: 'Method
    Position: 'Method
    Complete: 'Method option
    OffsetByte: 'Method
    ReadWord: 'Method
    Decode: 'Method
    Escaped: 'Method
    Compatibility: 'Method
}

type internal CodecStringFrame = {
    Source: int
    Position: int
    Length: int
    Quote: int
    Start: int
    Index: int
    Point: int
    ScalarEnd: int
    ScalarPoint: int
}

/// Expands quoted framing for the concrete encoding; ownership stays at its conversion seam.
module internal CodecStringEmitter =
    let private frame (il: CodecIL<'T,'M,'F>) (api: CodecStringApi<'T,'M>) utf16 admitted =
        let source = il.Local api.SourceReference
        let position = il.Local api.PositionReference
        let length = il.Local api.Int32
        let quote = il.Local api.Int32
        let start = il.Local api.Int32
        let index = il.Local api.Int32
        let point = il.Local api.Int32
        let word = il.Local api.UInt64
        let candidate = il.Local api.UInt64
        let packedQuote = il.Local api.UInt64
        let scalarEnd = il.Local api.Int32
        let fallback = il.Label()
        let escaped = il.Label()
        let quoted = il.Label()
        let doubleQuoted = il.Label()
        let loop = il.Label()
        let next = il.Label()
        let finish = il.Label()
        let scalar = il.Label()
        let scalarLoop = il.Label()
        let scalarPoint = il.Label()
        let blockScalar = il.Label()
        let packed = il.Label()
        let intConst value = il.EmitInt(OpCodes.Ldc_I4,value)
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let read offset =
            load source; load index
            if offset <> 0 then intConst offset; il.Emit OpCodes.Add
            CodecAddressEmitter.offset il api.OffsetByte (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1)
        let remaining count missing =
            load length; load index; il.Emit OpCodes.Sub
            intConst count; il.EmitBranch(OpCodes.Blt,missing)
        let lanes = if utf16 then 0x0001000100010001UL else 0x0101010101010101UL
        let high = if utf16 then 0x8000800080008000UL else 0x8080808080808080UL
        let matchLane delimiter =
            load word
            match delimiter with
            | None -> load packedQuote
            | Some value -> il.EmitInt64(int64(lanes * uint64 value))
            il.Emit OpCodes.Xor; save candidate
            load candidate; il.EmitInt64(int64 lanes); il.Emit OpCodes.Sub
            load candidate; il.Emit OpCodes.Not; il.Emit OpCodes.And
            il.EmitInt64(int64 high); il.Emit OpCodes.And
            il.EmitBranch(OpCodes.Brtrue,blockScalar)
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.Begin); save quote
        load quote; intConst 34; il.EmitBranch(OpCodes.Beq,doubleQuoted)
        load quote; intConst 39; il.EmitBranch(OpCodes.Bne_Un,fallback)
        il.EmitInt64(int64(lanes * 39UL)); save packedQuote
        il.EmitBranch(OpCodes.Br,quoted)
        il.Mark doubleQuoted
        il.EmitInt64(int64(lanes * 34UL)); save packedQuote
        il.Mark quoted
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Reference); save source
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Length); save length
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Position); save position
        load position; il.Emit OpCodes.Ldind_I4; intConst 1; il.Emit OpCodes.Add; save start
        load start; save index
        match admitted with
        | Some emit -> emit { Source = source; Position = position; Length = length; Quote = quote; Start = start
                              Index = index; Point = point; ScalarEnd = scalarEnd; ScalarPoint = scalarPoint }
        | None -> ()
        il.Mark loop
        let stride = if utf16 then 4 else 8
        remaining stride scalar
        load source; load index
        CodecAddressEmitter.offset il api.OffsetByte (if utf16 then 2 else 1) 0
        il.EmitMethod(OpCodes.Call,api.ReadWord); save word
        load word; il.EmitInt64(int64(if utf16 then 0xFF80FF80FF80FF80UL else high)); il.Emit OpCodes.And
        if utf16 then il.EmitBranch(OpCodes.Brtrue,blockScalar)
        else
            il.EmitBranch(OpCodes.Brfalse,packed)
            matchLane (Some 0xE2)
        il.Mark packed
        for delimiter in [None;Some 92;Some 13;Some 10] do matchLane delimiter
        load index; intConst stride; il.Emit OpCodes.Add; save index
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark blockScalar
        load index; intConst stride; il.Emit OpCodes.Add; save scalarEnd
        il.EmitBranch(OpCodes.Br,scalarLoop)
        il.Mark scalar
        load length; save scalarEnd
        il.Mark scalarLoop
        load index; load length; il.EmitBranch(OpCodes.Bge,escaped)
        read 0; save point
        il.Mark scalarPoint
        load point; load quote; il.EmitBranch(OpCodes.Beq,finish)
        for value in [92;13;10] do
            load point; intConst value; il.EmitBranch(OpCodes.Beq,escaped)
        if utf16 then
            for value in [0x2028;0x2029] do
                load point; intConst value; il.EmitBranch(OpCodes.Beq,escaped)
            load point; intConst 0xD800; il.Emit OpCodes.Sub; intConst 0x7FF
            il.EmitBranch(OpCodes.Bgt_Un,next)
            load point; intConst 0xDC00; il.EmitBranch(OpCodes.Bge,escaped)
            remaining 2 escaped
            read 1; intConst 0xDC00; il.Emit OpCodes.Sub; intConst 0x3FF
            il.EmitBranch(OpCodes.Bgt_Un,escaped)
            load index; intConst 1; il.Emit OpCodes.Add; save index
        else
            load point; intConst 0xE2; il.EmitBranch(OpCodes.Bne_Un,next)
            remaining 3 next
            read 1; intConst 0x80; il.EmitBranch(OpCodes.Bne_Un,next)
            read 2; intConst 0xA8; il.Emit OpCodes.Sub; intConst 1
            il.EmitBranch(OpCodes.Ble_Un,escaped)
        il.Mark next
        load index; intConst 1; il.Emit OpCodes.Add; save index
        load index; load scalarEnd; il.EmitBranch(OpCodes.Blt,scalarLoop)
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark finish
        il.Emit OpCodes.Ldarg_0; load start; load index; load start; il.Emit OpCodes.Sub
        il.EmitMethod(OpCodes.Call,api.Decode)
        load position; load index; intConst 1; il.Emit OpCodes.Add; il.Emit OpCodes.Stind_I4
        match api.Complete with
        | Some complete -> il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,complete)
        | None -> ()
        il.Emit OpCodes.Ret
        il.Mark escaped
        il.Emit OpCodes.Ldarg_0; load start; load index; load start; il.Emit OpCodes.Sub; load quote
        il.EmitMethod(OpCodes.Call,api.Escaped)
        match api.Complete with
        | Some complete -> il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,complete)
        | None -> ()
        il.Emit OpCodes.Ret
        il.Mark fallback
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.Compatibility)
        il.Emit OpCodes.Ret

    let reader il api utf16 = frame il api utf16 None

    let readerWith il api utf16 admitted = frame il api utf16 (Some admitted)
