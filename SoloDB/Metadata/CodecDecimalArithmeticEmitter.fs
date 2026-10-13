namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecDecimalWords = { Low: int; High: int }

module internal CodecDecimalArithmeticEmitter =
    /// Multiply the bounded coefficient by eight plus two, retaining full-word carries.
    let append (il: CodecIL<'T,'M,'F>) word coefficient digit =
        let twice = il.Local word
        let product = il.Local word
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let shift opcode count = il.EmitInt(OpCodes.Ldc_I4,count); il.Emit opcode
        load coefficient.Low; shift OpCodes.Shl 1; save twice
        load coefficient.Low; shift OpCodes.Shl 3
        load twice; il.Emit OpCodes.Add; save product
        load coefficient.High; shift OpCodes.Shl 1
        load coefficient.Low; shift OpCodes.Shr_Un 63; il.Emit OpCodes.Or
        load coefficient.High; shift OpCodes.Shl 3
        load coefficient.Low; shift OpCodes.Shr_Un 61; il.Emit OpCodes.Or
        il.Emit OpCodes.Add
        load product; load twice; il.Emit OpCodes.Clt_Un; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Add
        save coefficient.High
        match digit with
        | Some local ->
            load product; load local; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Add; save coefficient.Low
            load coefficient.High; load coefficient.Low; load product
            il.Emit OpCodes.Clt_Un; il.Emit OpCodes.Conv_U8; il.Emit OpCodes.Add; save coefficient.High
        | None -> load product; save coefficient.Low

    /// Divides the bounded coefficient once and returns the final removed digit.
    let divide10 (il: CodecIL<'T,'M,'F>) word coefficient =
        let remainder = il.Local word
        let upper = il.Local word
        let lower = il.Local word
        let high = il.Local word
        let current = il.Local word
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let small = il.Label()
        let complete = il.Label()
        load coefficient.High; il.EmitBranch(OpCodes.Brfalse,small)
        load coefficient.High; il.EmitInt64 10L; il.Emit OpCodes.Div_Un; save high
        load coefficient.High; il.EmitInt64 10L; il.Emit OpCodes.Rem_Un; save remainder
        load remainder; il.EmitInt(OpCodes.Ldc_I4,32); il.Emit OpCodes.Shl
        load coefficient.Low; il.EmitInt(OpCodes.Ldc_I4,32); il.Emit OpCodes.Shr_Un; il.Emit OpCodes.Or; save current
        load current; il.EmitInt64 10L; il.Emit OpCodes.Div_Un; save upper
        load current; il.EmitInt64 10L; il.Emit OpCodes.Rem_Un; save remainder
        load remainder; il.EmitInt(OpCodes.Ldc_I4,32); il.Emit OpCodes.Shl
        load coefficient.Low; il.EmitInt64 0xffffffffL; il.Emit OpCodes.And; il.Emit OpCodes.Or; save current
        load current; il.EmitInt64 10L; il.Emit OpCodes.Div_Un; save lower
        load current; il.EmitInt64 10L; il.Emit OpCodes.Rem_Un; save remainder
        load upper; il.EmitInt(OpCodes.Ldc_I4,32); il.Emit OpCodes.Shl; load lower; il.Emit OpCodes.Or; save coefficient.Low
        load high; save coefficient.High
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark small
        load coefficient.Low; il.EmitInt64 10L; il.Emit OpCodes.Rem_Un; save remainder
        load coefficient.Low; il.EmitInt64 10L; il.Emit OpCodes.Div_Un; save coefficient.Low
        il.Mark complete
        remainder
