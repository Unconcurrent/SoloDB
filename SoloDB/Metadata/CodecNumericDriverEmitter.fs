namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecNumericFinish<'Method> = { Method: 'Method; Constructor: bool }

type internal CodecNumericDriverApi<'Type,'Method> = {
    Result: 'Type
    Core: 'Type
    After: CodecNumericFinish<'Method> array
    Predicate: ('Method * 'Method) option
    Text: ('Method * 'Method) option
    Int32: 'Type
    SourceReference: 'Type
    PositionReference: 'Type
    Begin: 'Method
    Reference: 'Method
    Length: 'Method
    Position: 'Method
    Complete: 'Method
    OffsetByte: 'Method
    Decimal: 'Method
    Hex: 'Method
    Quoted: 'Method option
    StoredQuoted: 'Method option
    DecimalContext: bool
    HexContext: bool
    Compatibility: 'Method
}

module internal CodecNumericDriverEmitter =
    let reader (il: CodecIL<'T,'M,'F>) (api: CodecNumericDriverApi<'T,'M>) utf16 =
        let point = il.Local api.Int32
        let source = il.Local api.SourceReference
        let length = il.Local api.Int32
        let position = il.Local api.PositionReference
        let index = il.Local api.Int32
        let result = il.Local api.Core
        let compatibility = il.Label()
        let decimal = il.Label()
        let quoted = il.Label()
        let hex = il.Label()
        let complete = il.Label()
        let inspect = il.Label()
        let signed = il.Label()
        let intConst value = il.EmitInt(OpCodes.Ldc_I4,value)
        let load local = il.EmitLocal(OpCodes.Ldloc,local)
        let save local = il.EmitLocal(OpCodes.Stloc,local)
        let readIndex () =
            load source; load index
            CodecAddressEmitter.offset il api.OffsetByte (if utf16 then 2 else 1) 0
            il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1)
            save point
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Begin); save point
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Reference); save source
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Length); save length
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Position); save position
        load position; il.Emit OpCodes.Ldind_I4; save index
        if api.Quoted.IsSome || api.StoredQuoted.IsSome then
            load point; intConst 34; il.EmitBranch(OpCodes.Beq,quoted)
            load point; intConst 39; il.EmitBranch(OpCodes.Beq,quoted)
        load point; intConst 43; il.EmitBranch(OpCodes.Beq,signed)
        load point; intConst 45; il.EmitBranch(OpCodes.Beq,signed)
        il.EmitBranch(OpCodes.Br,inspect)
        il.Mark signed
        load index; intConst 1; il.Emit OpCodes.Add; save index
        load index; load length; il.EmitBranch(OpCodes.Bge,decimal)
        readIndex()
        il.Mark inspect
        load point; intConst 46; il.EmitBranch(OpCodes.Beq,decimal)
        load point; intConst 48; il.Emit OpCodes.Sub
        intConst 9; il.EmitBranch(OpCodes.Bgt_Un,compatibility)
        load point; intConst 48; il.EmitBranch(OpCodes.Bne_Un,decimal)
        load index; intConst 1; il.Emit OpCodes.Add; save index
        load index; load length; il.EmitBranch(OpCodes.Bge,decimal)
        readIndex()
        load point; intConst 120; il.EmitBranch(OpCodes.Beq,hex)
        load point; intConst 88; il.EmitBranch(OpCodes.Beq,hex)
        il.Mark decimal
        load source; load length; load position
        if api.DecimalContext then il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.Decimal); save result
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark hex
        load source; load length; load position
        if api.HexContext then il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.Hex); save result
        match api.StoredQuoted,api.Quoted with
        | Some method,_ ->
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark quoted
            il.Emit OpCodes.Ldarg_0
            il.EmitMethod(OpCodes.Call,method); save result
        | None,Some method ->
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark quoted
            load source; load length; load position; il.Emit OpCodes.Ldarg_0
            il.EmitMethod(OpCodes.Call,method); save result
        | None,None -> ()
        il.Mark complete
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Complete)
        match api.Text with
        | Some(culture,format) ->
            il.EmitLocal(OpCodes.Ldloca,result)
            il.EmitMethod(OpCodes.Call,culture); il.EmitMethod(OpCodes.Call,format)
        | None ->
            load result
            for finish in api.After do il.EmitMethod((if finish.Constructor then OpCodes.Newobj else OpCodes.Call),finish.Method)
            match api.Predicate with
            | Some(zero,compare) ->
                intConst 0; il.EmitMethod(OpCodes.Call,zero); il.EmitMethod(OpCodes.Call,compare)
            | None -> ()
        il.Emit OpCodes.Ret
        il.Mark compatibility
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Compatibility)
        il.Emit OpCodes.Ret
