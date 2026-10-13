namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecMagnitudeApi<'Type,'Method> = {
    UInt32: 'Type
    Limbs: 'Type
    Int32: 'Type
    UInt64: 'Type
    Reference: 'Type
    Offset: 'Method
    ReferenceOf: 'Method
    Resize: 'Method
}

type internal CodecMagnitudeState = {
    Small: int
    Limbs: int
    Reference: int
    Used: int
    Index: int
    Carry: int
    Word: int
}

type internal CodecMagnitudeDecimalState = {
    Chunks: int
    Count: int
    Index: int
    Digits: int
    Coefficient: int
    Divisor: int
}

/// Expands mutable unsigned base-2^32 arithmetic over native limb storage.
module internal CodecMagnitudeEmitter =
    let locals (il: CodecIL<'T,'M,'F>) (api: CodecMagnitudeApi<'T,'M>) =
        { Small = il.Local api.UInt32; Limbs = il.Local api.Limbs; Reference = il.Local api.Reference
          Used = il.Local api.Int32; Index = il.Local api.Int32
          Carry = il.Local api.UInt64; Word = il.Local api.UInt64 }

    let private address (il: CodecIL<'T,'M,'F>) api state =
        il.EmitLocal(OpCodes.Ldloc,state.Reference)
        il.EmitLocal(OpCodes.Ldloc,state.Index)
        CodecAddressEmitter.offset il api.Offset 1 0

    /// The caller has proved carry-limb capacity and holds the current array reference.
    let multiplyAddInRegion (il: CodecIL<'T,'M,'F>) (api: CodecMagnitudeApi<'T,'M>) state factor addend =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        addend(); il.Emit OpCodes.Conv_U8; save state.Carry
        constant 0; save state.Index
        let loop = il.Label()
        let carry = il.Label()
        let complete = il.Label()
        il.Mark loop
        load state.Index; load state.Used; il.EmitBranch(OpCodes.Bge,carry)
        address il api state; il.Emit OpCodes.Ldind_U4; il.Emit OpCodes.Conv_U8
        factor(); il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Mul
        load state.Carry; il.Emit OpCodes.Add; save state.Word
        address il api state; load state.Word; il.Emit OpCodes.Conv_U4; il.Emit OpCodes.Stind_I4
        load state.Word; constant 32; il.Emit OpCodes.Shr_Un; save state.Carry
        load state.Index; constant 1; il.Emit OpCodes.Add; save state.Index
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark carry
        load state.Carry; il.EmitBranch(OpCodes.Brfalse,complete)
        address il api state; load state.Carry; il.Emit OpCodes.Conv_U4; il.Emit OpCodes.Stind_I4
        load state.Used; constant 1; il.Emit OpCodes.Add; save state.Used
        il.Mark complete

    let multiplyAdd (il: CodecIL<'T,'M,'F>) (api: CodecMagnitudeApi<'T,'M>) state factor addend =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let wide = il.Label()
        let complete = il.Label()
        let small = il.Label()
        load state.Limbs; il.EmitBranch(OpCodes.Brtrue,wide)
        load state.Small; il.Emit OpCodes.Conv_U8; factor(); il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Mul
        addend(); il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Add; save state.Word
        load state.Word; il.Emit OpCodes.Conv_U4; save state.Small
        load state.Word; constant 32; il.Emit OpCodes.Shr_Un; save state.Carry
        load state.Carry; il.EmitBranch(OpCodes.Brfalse,small)
        constant 4; il.EmitType(OpCodes.Newarr,api.UInt32); save state.Limbs
        load state.Limbs; constant 0; load state.Small; il.Emit OpCodes.Stelem_I4
        load state.Limbs; constant 1; load state.Carry; il.Emit OpCodes.Conv_U4; il.Emit OpCodes.Stelem_I4
        constant 2; save state.Used
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark small
        load state.Small; constant 0; il.Emit OpCodes.Cgt_Un; save state.Used
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark wide
        let capacity = il.Label()
        load state.Used; load state.Limbs; il.Emit OpCodes.Ldlen; il.Emit OpCodes.Conv_I4
        il.EmitBranch(OpCodes.Blt,capacity)
        il.EmitLocal(OpCodes.Ldloca,state.Limbs)
        load state.Limbs; il.Emit OpCodes.Ldlen; il.Emit OpCodes.Conv_I4
        constant 2; il.Emit OpCodes.Mul_Ovf
        il.EmitMethod(OpCodes.Call,api.Resize)
        il.Mark capacity
        load state.Limbs; il.EmitMethod(OpCodes.Call,api.ReferenceOf); save state.Reference
        multiplyAddInRegion il api state factor addend
        il.Mark complete

    /// Returns a native remainder and retains only significant quotient limbs.
    let divide (il: CodecIL<'T,'M,'F>) (api: CodecMagnitudeApi<'T,'M>) state divisor =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let wide = il.Label()
        let complete = il.Label()
        load state.Limbs; il.EmitBranch(OpCodes.Brtrue,wide)
        load state.Small; il.Emit OpCodes.Conv_U8; save state.Word
        load state.Word; divisor(); il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Rem_Un; save state.Carry
        load state.Word; divisor(); il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Div_Un; il.Emit OpCodes.Conv_U4; save state.Small
        load state.Small; constant 0; il.Emit OpCodes.Cgt_Un; save state.Used
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark wide
        load state.Limbs; il.EmitMethod(OpCodes.Call,api.ReferenceOf); save state.Reference
        il.EmitInt64 0L; save state.Carry
        load state.Used; save state.Index
        let loop = il.Label()
        let trim = il.Label()
        il.Mark loop
        load state.Index; il.EmitBranch(OpCodes.Brfalse,trim)
        load state.Index; constant 1; il.Emit OpCodes.Sub; save state.Index
        load state.Carry; constant 32; il.Emit OpCodes.Shl
        address il api state; il.Emit OpCodes.Ldind_U4; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Or; save state.Word
        address il api state; load state.Word; divisor(); il.Emit OpCodes.Conv_U8
        il.Emit OpCodes.Div_Un; il.Emit OpCodes.Conv_U4; il.Emit OpCodes.Stind_I4
        load state.Word; divisor(); il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Rem_Un; save state.Carry
        il.EmitBranch(OpCodes.Br,loop)
        il.Mark trim
        load state.Used; il.EmitBranch(OpCodes.Brfalse,complete)
        load state.Used; constant 1; il.Emit OpCodes.Sub; save state.Index
        address il api state; il.Emit OpCodes.Ldind_U4; il.EmitBranch(OpCodes.Brtrue,complete)
        load state.Index; save state.Used; il.EmitBranch(OpCodes.Br,trim)
        il.Mark complete
    let decimalChunks (il: CodecIL<'T,'M,'F>) (api: CodecMagnitudeApi<'T,'M>) uint32 uint32Array magnitude =
        let chunks = { Chunks = il.Local uint32Array; Count = il.Local api.Int32; Index = il.Local api.Int32
                       Digits = il.Local api.Int32; Coefficient = il.Local uint32; Divisor = il.Local uint32 }
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        load magnitude.Used; constant 4; il.Emit OpCodes.Mul_Ovf
        constant 2; il.Emit OpCodes.Add_Ovf; constant 3; il.Emit OpCodes.Div
        il.EmitType(OpCodes.Newarr,uint32); save chunks.Chunks
        let loop = il.Label()
        let doneSize = il.Label()
        il.Mark loop
        divide il api magnitude (fun () -> constant 1000000000)
        load chunks.Chunks; load chunks.Count; load magnitude.Carry; il.Emit OpCodes.Conv_U4; il.Emit OpCodes.Stelem_I4
        load chunks.Count; constant 1; il.Emit OpCodes.Add; save chunks.Count
        load magnitude.Used; il.EmitBranch(OpCodes.Brtrue,loop)
        load chunks.Count; constant 1; il.Emit OpCodes.Sub; save chunks.Index
        load chunks.Chunks; load chunks.Index; il.Emit OpCodes.Ldelem_U4; save chunks.Coefficient
        constant 1; save chunks.Digits
        let size = il.Label()
        il.Mark size
        load chunks.Coefficient; constant 10; il.EmitBranch(OpCodes.Blt_Un,doneSize)
        load chunks.Coefficient; constant 10; il.Emit OpCodes.Div_Un; save chunks.Coefficient
        load chunks.Digits; constant 1; il.Emit OpCodes.Add; save chunks.Digits
        il.EmitBranch(OpCodes.Br,size)
        il.Mark doneSize
        chunks

    let decimalDigits (il: CodecIL<'T,'M,'F>) chunks store =
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let constant n = il.EmitInt(OpCodes.Ldc_I4,n)
        let divisorLoop = il.Label()
        let chunkLoop = il.Label()
        let digitLoop = il.Label()
        let complete = il.Label()
        constant 1; save chunks.Divisor
        il.Mark divisorLoop
        load chunks.Digits; constant 1; il.EmitBranch(OpCodes.Ble,chunkLoop)
        load chunks.Divisor; constant 10; il.Emit OpCodes.Mul; save chunks.Divisor
        load chunks.Digits; constant 1; il.Emit OpCodes.Sub; save chunks.Digits
        il.EmitBranch(OpCodes.Br,divisorLoop)
        il.Mark chunkLoop
        load chunks.Chunks; load chunks.Index; il.Emit OpCodes.Ldelem_U4; save chunks.Coefficient
        il.Mark digitLoop
        store(fun () -> load chunks.Coefficient; load chunks.Divisor; il.Emit OpCodes.Div_Un; constant 48; il.Emit OpCodes.Add)
        load chunks.Coefficient; load chunks.Divisor; il.Emit OpCodes.Rem_Un; save chunks.Coefficient
        load chunks.Divisor; constant 10; il.Emit OpCodes.Div_Un; save chunks.Divisor
        load chunks.Divisor; il.EmitBranch(OpCodes.Brtrue,digitLoop)
        load chunks.Index; il.EmitBranch(OpCodes.Brfalse,complete)
        load chunks.Index; constant 1; il.Emit OpCodes.Sub; save chunks.Index
        constant 100000000; save chunks.Divisor
        il.EmitBranch(OpCodes.Br,chunkLoop)
        il.Mark complete
