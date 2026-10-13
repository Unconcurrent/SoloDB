namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Representation adapters supply operands; admitted stores and Add calls expand here.
type internal CodecOutputApi<'Type,'Method,'Field> = {
    Int32: 'Type
    UInt32: 'Type
    UInt64: 'Type
    Reference: 'Type
    List: 'Type
    ConcreteList: 'Type
    ListField: 'Field
    Written: 'Field
    Discard: 'Field
    Add: 'Method
    ConcreteAdd: 'Method
    ReserveSpan: 'Method
    ReserveList: 'Method
    FitsSpan: 'Method
    Start: 'Method
    Position: 'Method
    Advance: 'Method
    Offset: 'Method
}

type internal CodecOutputSelection = { Kind: int; List: int; ConcreteList: int; Minimum: int }

module internal CodecOutputEmitter =
    /// The caller has already masked the nibble to 0..15.
    let lowerHex (il: CodecIL<'T,'M,'F>) digit =
        let load() = il.EmitLocal(OpCodes.Ldloc,digit)
        load(); il.EmitInt(OpCodes.Ldc_I4,48); il.Emit OpCodes.Add
        load(); il.EmitInt(OpCodes.Ldc_I4,9); il.Emit OpCodes.Cgt_Un
        il.EmitInt(OpCodes.Ldc_I4,39); il.Emit OpCodes.Mul; il.Emit OpCodes.Add

    let store (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 reference offset value =
        il.EmitLocal(OpCodes.Ldloc,reference)
        offset()
        CodecAddressEmitter.offset il api.Offset (if utf16 then 2 else 1) 0
        value()
        il.Emit(if utf16 then OpCodes.Stind_I2 else OpCodes.Stind_I1)

    let select (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) state =
        let selection = { Kind = il.Local api.Int32; List = il.Local api.List; ConcreteList = il.Local api.ConcreteList; Minimum = il.Local api.Int32 }
        let active = il.Label()
        let list = il.Label()
        let complete = il.Label()
        state(); il.EmitField(OpCodes.Ldfld,api.Discard)
        il.EmitBranch(OpCodes.Brfalse,active)
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark active
        state(); il.EmitField(OpCodes.Ldfld,api.ListField); il.EmitLocal(OpCodes.Stloc,selection.List)
        il.EmitLocal(OpCodes.Ldloc,selection.List); il.EmitBranch(OpCodes.Brtrue,list)
        il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,selection.Kind)
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark list
        il.EmitLocal(OpCodes.Ldloc,selection.List); il.EmitType(OpCodes.Isinst,api.ConcreteList)
        il.EmitLocal(OpCodes.Stloc,selection.ConcreteList)
        il.Emit OpCodes.Ldc_I4_2; il.EmitLocal(OpCodes.Stloc,selection.Kind)
        il.Mark complete
        selection

    let checkCount (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) state count =
        state(); il.EmitField(OpCodes.Ldfld,api.Written); count(); il.Emit OpCodes.Add_Ovf

    let admitSpan (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) selected destination state =
        destination(); state(); il.EmitLocal(OpCodes.Ldloc,selected.Minimum); il.EmitMethod(OpCodes.Call,api.ReserveSpan)

    let beginOperation (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) state count =
        let selected = select il api state
        checkCount il api state count; il.EmitLocal(OpCodes.Stloc,selected.Minimum)
        il.EmitLocal(OpCodes.Ldloc,selected.Kind)
        selected

    let withDestination (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) state body =
        let selected = select il api state
        let discard = il.Label()
        let span = il.Label()
        let arbitrary = il.Label()
        let complete = il.Label()
        il.EmitLocal(OpCodes.Ldloc,selected.Kind); il.EmitBranch(OpCodes.Brfalse,discard)
        il.EmitLocal(OpCodes.Ldloc,selected.Kind); il.Emit OpCodes.Ldc_I4_1; il.EmitBranch(OpCodes.Beq,span)
        il.EmitLocal(OpCodes.Ldloc,selected.ConcreteList); il.EmitBranch(OpCodes.Brfalse,arbitrary)
        body selected 2 true; il.EmitBranch(OpCodes.Br,complete)
        il.Mark arbitrary
        body selected 2 false; il.EmitBranch(OpCodes.Br,complete)
        il.Mark span
        body selected 1 false; il.EmitBranch(OpCodes.Br,complete)
        il.Mark discard
        body selected 0 false
        il.Mark complete

    let listAppend (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 selected state concrete account value =
        il.EmitLocal(OpCodes.Ldloc,if concrete then selected.ConcreteList else selected.List)
        value(); il.Emit(if utf16 then OpCodes.Conv_U2 else OpCodes.Conv_U1)
        il.EmitMethod(OpCodes.Callvirt,if concrete then api.ConcreteAdd else api.Add)
        if account then
            state(); state(); il.EmitField(OpCodes.Ldfld,api.Written)
            il.Emit OpCodes.Ldc_I4_1; il.Emit OpCodes.Add; il.EmitField(OpCodes.Stfld,api.Written)

    /// Sink call identity is selected outside the expanded unit loop.
    let listBody (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 selected state reserveUnits bulkCount regionCount body =
        let arbitrary = il.Label()
        let complete = il.Label()
        let append concrete value =
            if reserveUnits then
                checkCount il api state (fun () -> il.Emit OpCodes.Ldc_I4_1); il.Emit OpCodes.Pop
                if concrete then
                    il.EmitLocal(OpCodes.Ldloc,selected.ConcreteList); il.Emit OpCodes.Ldc_I4_1; il.EmitMethod(OpCodes.Call,api.ReserveList)
            listAppend il api utf16 selected state concrete (not concrete || Option.isNone bulkCount) value
        il.EmitLocal(OpCodes.Ldloc,selected.ConcreteList); il.EmitBranch(OpCodes.Brfalse,arbitrary)
        match regionCount with
        | Some count -> il.EmitLocal(OpCodes.Ldloc,selected.ConcreteList); count(); il.EmitMethod(OpCodes.Call,api.ReserveList)
        | None -> ()
        body (append true)
        match bulkCount with
        | Some count -> state(); count(); il.EmitMethod(OpCodes.Call,api.Advance)
        | None -> ()
        il.EmitBranch(OpCodes.Br,complete)
        il.Mark arbitrary
        body (append false)
        il.Mark complete

    let literalMode (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 selected mode concrete (units: int[]) destination state =
        let count() = il.EmitInt(OpCodes.Ldc_I4,units.Length)
        checkCount il api state count
        if mode = 1 then
            il.EmitLocal(OpCodes.Stloc,selected.Minimum)
            admitSpan il api selected destination state
            let reference = il.Local api.Reference
            let position = il.Local api.Int32
            destination(); il.EmitMethod(OpCodes.Call,api.Start); il.EmitLocal(OpCodes.Stloc,reference)
            state(); il.EmitMethod(OpCodes.Call,api.Position); il.EmitLocal(OpCodes.Stloc,position)
            for index = 0 to units.Length - 1 do
                store il api utf16 reference
                    (fun () -> il.EmitLocal(OpCodes.Ldloc,position); il.EmitInt(OpCodes.Ldc_I4,index); il.Emit OpCodes.Add)
                    (fun () -> il.EmitInt(OpCodes.Ldc_I4,units[index]))
            state(); count(); il.EmitMethod(OpCodes.Call,api.Advance)
        else
            il.Emit OpCodes.Pop
            if mode = 2 then
                if concrete then
                    il.EmitLocal(OpCodes.Ldloc,selected.ConcreteList); count(); il.EmitMethod(OpCodes.Call,api.ReserveList)
                for unit in units do
                    listAppend il api utf16 selected state concrete (not concrete) (fun () -> il.EmitInt(OpCodes.Ldc_I4,unit))
                if concrete then state(); count(); il.EmitMethod(OpCodes.Call,api.Advance)

    let literal (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 units destination state =
        withDestination il api state (fun selected mode concrete -> literalMode il api utf16 selected mode concrete units destination state)

    /// Runtime-dependent CLR argument identities are encoded once, then copied as literals.
    let encodedLiteral (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) bufferType getValue getReference utf16 =
        let value = il.Local bufferType
        let count = il.Local api.Int32
        let source = il.Local api.Reference
        let load n = il.EmitLocal(OpCodes.Ldloc,n)
        let save n = il.EmitLocal(OpCodes.Stloc,n)
        let destination() = il.Emit OpCodes.Ldarg_0
        let state() = il.Emit OpCodes.Ldarg_1
        il.EmitMethod(OpCodes.Call,getValue); save value
        load value; il.Emit OpCodes.Ldlen; il.Emit OpCodes.Conv_I4; save count
        withDestination il api state (fun selected mode concrete ->
            checkCount il api state (fun () -> load count)
            if mode = 1 then
                save selected.Minimum
                admitSpan il api selected destination state
                let empty = il.Label()
                load count; il.EmitBranch(OpCodes.Brfalse,empty)
                destination(); il.EmitMethod(OpCodes.Call,api.Start)
                state(); il.EmitMethod(OpCodes.Call,api.Position)
                CodecAddressEmitter.offset il api.Offset (if utf16 then 2 else 1) 0
                load value; il.EmitMethod(OpCodes.Call,getReference)
                load count; il.Emit OpCodes.Conv_U
                if utf16 then il.Emit OpCodes.Ldc_I4_2; il.Emit OpCodes.Conv_U; il.Emit OpCodes.Mul
                il.EmitInt(OpCodes.Unaligned,1); il.Emit OpCodes.Cpblk
                state(); load count; il.EmitMethod(OpCodes.Call,api.Advance)
                il.Mark empty
            else
                il.Emit OpCodes.Pop
                if mode = 2 then
                    if concrete then load selected.ConcreteList; load count; il.EmitMethod(OpCodes.Call,api.ReserveList)
                    load value; il.EmitMethod(OpCodes.Call,getReference); save source
                    let index = il.Local api.Int32
                    il.Emit OpCodes.Ldc_I4_0; save index
                    let loop = il.Label()
                    let complete = il.Label()
                    il.Mark loop
                    load index; load count; il.EmitBranch(OpCodes.Bge,complete)
                    listAppend il api utf16 selected state concrete (not concrete) (fun () ->
                        load source; load index
                        CodecAddressEmitter.offset il api.Offset (if utf16 then 2 else 1) 0
                        il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1))
                    load index; il.Emit OpCodes.Ldc_I4_1; il.Emit OpCodes.Add; save index
                    il.EmitBranch(OpCodes.Br,loop)
                    il.Mark complete
                    if concrete then state(); load count; il.EmitMethod(OpCodes.Call,api.Advance))
        il.Emit OpCodes.Ret

    /// Fitting fragments admit one region; partial fragments keep their unit prefixes.
    let fragmentMode (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 selected mode concrete
                     (values: (unit -> unit)[]) destination state =
        if mode = 1 then
            let reference = il.Local api.Reference
            let position = il.Local api.Int32
            let partial = il.Label()
            let complete = il.Label()
            destination(); state(); il.EmitInt(OpCodes.Ldc_I4,values.Length); il.EmitMethod(OpCodes.Call,api.FitsSpan)
            il.EmitBranch(OpCodes.Brfalse,partial)
            destination(); il.EmitMethod(OpCodes.Call,api.Start); il.EmitLocal(OpCodes.Stloc,reference)
            state(); il.EmitMethod(OpCodes.Call,api.Position); il.EmitLocal(OpCodes.Stloc,position)
            for index = 0 to values.Length - 1 do
                store il api utf16 reference
                    (fun () -> il.EmitLocal(OpCodes.Ldloc,position); il.EmitInt(OpCodes.Ldc_I4,index); il.Emit OpCodes.Add) values[index]
            state(); il.EmitInt(OpCodes.Ldc_I4,values.Length); il.EmitMethod(OpCodes.Call,api.Advance)
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark partial
            for value in values do
                state(); il.EmitField(OpCodes.Ldfld,api.Written); il.Emit OpCodes.Ldc_I4_1; il.Emit OpCodes.Add_Ovf
                il.EmitLocal(OpCodes.Stloc,position)
                destination(); state(); il.EmitLocal(OpCodes.Ldloc,position); il.EmitMethod(OpCodes.Call,api.ReserveSpan)
                destination(); il.EmitMethod(OpCodes.Call,api.Start); il.EmitLocal(OpCodes.Stloc,reference)
                store il api utf16 reference
                    (fun () -> state(); il.EmitField(OpCodes.Ldfld,api.Written)) value
                state(); il.EmitLocal(OpCodes.Ldloc,position); il.EmitField(OpCodes.Stfld,api.Written)
            il.Mark complete
        elif mode = 2 then
            for value in values do
                checkCount il api state (fun () -> il.Emit OpCodes.Ldc_I4_1); il.Emit OpCodes.Pop
                if concrete then
                    il.EmitLocal(OpCodes.Ldloc,selected.ConcreteList); il.Emit OpCodes.Ldc_I4_1; il.EmitMethod(OpCodes.Call,api.ReserveList)
                listAppend il api utf16 selected state concrete true value

    let fragment (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 values destination state =
        withDestination il api state (fun selected mode concrete ->
            fragmentMode il api utf16 selected mode concrete values destination state)


    let property (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 units =
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        withDestination il api state (fun selected mode concrete ->
            let body = il.Label()
            il.Emit OpCodes.Ldarg_0; il.EmitBranch(OpCodes.Brfalse,body)
            literalMode il api utf16 selected mode concrete [|int ','|] destination state
            il.Mark body
            literalMode il api utf16 selected mode concrete units destination state)
        il.Emit OpCodes.Ret

    let constant (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 units =
        literal il api utf16 units (fun () -> il.Emit OpCodes.Ldarg_0) (fun () -> il.Emit OpCodes.Ldarg_1)
        il.Emit OpCodes.Ret

    let boolean (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 =
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        withDestination il api state (fun selected mode concrete ->
            let no = il.Label()
            let complete = il.Label()
            il.Emit OpCodes.Ldarg_0; il.EmitBranch(OpCodes.Brfalse,no)
            literalMode il api utf16 selected mode concrete [|116;114;117;101|] destination state
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark no
            literalMode il api utf16 selected mode concrete [|102;97;108;115;101|] destination state
            il.Mark complete)
        il.Emit OpCodes.Ret
