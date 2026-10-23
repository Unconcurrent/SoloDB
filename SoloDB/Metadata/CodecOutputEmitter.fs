namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

type internal CodecOutputKind =
    | Validation = 0
    | Span = 1
    | List = 2

/// Representation adapters supply operands; admitted stores and Add calls expand here.
type internal CodecOutputApi<'Type,'Method,'Field> = {
    Kind: CodecOutputKind
    LittleEndian: 'Field
    Int32: 'Type
    UInt32: 'Type
    UInt64: 'Type
    Reference: 'Type
    Written: 'Field
    Add: 'Method
    ReserveSpan: 'Method
    FitsSpan: 'Method
    Start: 'Method
    Position: 'Method
    Advance: 'Method
    Offset: 'Method
}

type internal CodecOutputSelection = { Minimum: int }

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

    let kind name =
        match name with
        | "SoloDatabase.JsonSerializator.JsonUtf8Writer" | "SoloDatabase.JsonSerializator.JsonUtf16Writer" -> CodecOutputKind.Span
        | "SoloDatabase.JsonSerializator.JsonUtf8IListWriter" | "SoloDatabase.JsonSerializator.JsonUtf16IListWriter" -> CodecOutputKind.List
        | "SoloDatabase.JsonSerializator.JsonValidationOutput`1" -> CodecOutputKind.Validation
        | _ -> invalidArg "name" "Unknown JSON output strategy."

    let select (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) =
        { Minimum = il.Local api.Int32 }

    let checkCount (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) state count =
        state(); il.EmitField(OpCodes.Ldfld,api.Written); count(); il.Emit OpCodes.Add_Ovf

    let admitSpan (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) selected destination state =
        destination(); state(); il.EmitLocal(OpCodes.Ldloc,selected.Minimum); il.EmitMethod(OpCodes.Call,api.ReserveSpan)

    let beginOperation (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) state count =
        let selected = select il api
        checkCount il api state count; il.EmitLocal(OpCodes.Stloc,selected.Minimum)
        selected

    let withDestination (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) state body =
        let selected = select il api
        match api.Kind with
        | CodecOutputKind.Span -> body selected 1
        | CodecOutputKind.List -> body selected 2
        | CodecOutputKind.Validation -> body selected 0
        | _ -> invalidArg "api" "Unknown JSON output strategy."

    let listAppend (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 state value =
        state(); value(); il.Emit(if utf16 then OpCodes.Conv_U2 else OpCodes.Conv_U1)
        il.EmitMethod(OpCodes.Call,api.Add)
        state(); state(); il.EmitField(OpCodes.Ldfld,api.Written)
        il.Emit OpCodes.Ldc_I4_1; il.Emit OpCodes.Add; il.EmitField(OpCodes.Stfld,api.Written)

    let listBody (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 state body =
        body (listAppend il api utf16 state)

    let private literalCore (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 selected mode (units: int[]) destination state fits =
        let count() = il.EmitInt(OpCodes.Ldc_I4,units.Length)
        if not fits then checkCount il api state count
        if mode = 1 then
            if not fits then
                il.EmitLocal(OpCodes.Stloc,selected.Minimum)
                admitSpan il api selected destination state
            let reference = il.Local api.Reference
            let position = il.Local api.Int32
            destination(); il.EmitMethod(OpCodes.Call,api.Start); il.EmitLocal(OpCodes.Stloc,reference)
            state(); il.EmitMethod(OpCodes.Call,api.Position); il.EmitLocal(OpCodes.Stloc,position)
            let packed little =
                let bits = if utf16 then 16 else 8
                let mutable index = 0
                while index < units.Length do
                    // Four-byte stores also lower directly on 32-bit targets.
                    let count = min (32 / bits) (units.Length - index)
                    let count = if count = 3 then 2 else count
                    let mutable value = 0u
                    for offset = 0 to count - 1 do
                        let shift = bits * (if little then offset else count - offset - 1)
                        value <- value ||| (uint32 units[index + offset] <<< shift)
                    il.EmitLocal(OpCodes.Ldloc,reference)
                    il.EmitLocal(OpCodes.Ldloc,position); il.EmitInt(OpCodes.Ldc_I4,index); il.Emit OpCodes.Add
                    CodecAddressEmitter.offset il api.Offset (if utf16 then 2 else 1) 0
                    il.EmitInt(OpCodes.Ldc_I4,int value)
                    let bytes = count * bits / 8
                    if bytes > 1 then il.EmitInt(OpCodes.Unaligned,1)
                    il.Emit(if bytes = 4 then OpCodes.Stind_I4 elif bytes = 2 then OpCodes.Stind_I2 else OpCodes.Stind_I1)
                    index <- index + count
            if units.Length > 1 then
                let big = il.Label()
                let complete = il.Label()
                il.EmitField(OpCodes.Ldsfld,api.LittleEndian); il.EmitBranch(OpCodes.Brfalse,big)
                packed true
                il.EmitBranch(OpCodes.Br,complete)
                il.Mark big
                packed false
                il.Mark complete
            else packed true
            state(); count(); il.EmitMethod(OpCodes.Call,api.Advance)
        else
            il.Emit OpCodes.Pop
            if mode = 2 then
                for unit in units do
                    listAppend il api utf16 state (fun () -> il.EmitInt(OpCodes.Ldc_I4,unit))

    let literalMode il api utf16 selected mode units destination state =
        literalCore il api utf16 selected mode units destination state false

    let literal (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 units destination state =
        withDestination il api state (fun selected mode -> literalMode il api utf16 selected mode units destination state)

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
        withDestination il api state (fun selected mode ->
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
                    load value; il.EmitMethod(OpCodes.Call,getReference); save source
                    let index = il.Local api.Int32
                    il.Emit OpCodes.Ldc_I4_0; save index
                    let loop = il.Label()
                    let complete = il.Label()
                    il.Mark loop
                    load index; load count; il.EmitBranch(OpCodes.Bge,complete)
                    listAppend il api utf16 state (fun () ->
                        load source; load index
                        CodecAddressEmitter.offset il api.Offset (if utf16 then 2 else 1) 0
                        il.Emit(if utf16 then OpCodes.Ldind_U2 else OpCodes.Ldind_U1))
                    load index; il.Emit OpCodes.Ldc_I4_1; il.Emit OpCodes.Add; save index
                    il.EmitBranch(OpCodes.Br,loop)
                    il.Mark complete)
        il.Emit OpCodes.Ret

    /// Fitting fragments admit one region; partial fragments keep their unit prefixes.
    let fragmentMode (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 selected mode
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
                listAppend il api utf16 state value

    let fragment (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 values destination state =
        withDestination il api state (fun selected mode ->
            fragmentMode il api utf16 selected mode values destination state)


    /// Negative prefix inputs are absent (-1), comma (-2), or opening brace (-3).
    let inlineProperty (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 (units: int[]) comma =
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        withDestination il api state (fun selected mode ->
            let plain() = literalMode il api utf16 selected mode units destination state
            let prefixed prefix =
                if mode = 1 then
                    let partial = il.Label()
                    let complete = il.Label()
                    destination(); state(); il.EmitInt(OpCodes.Ldc_I4,units.Length + 1); il.EmitMethod(OpCodes.Call,api.FitsSpan)
                    il.EmitBranch(OpCodes.Brfalse,partial)
                    literalCore il api utf16 selected mode (Array.append [|int prefix|] units) destination state true
                    il.EmitBranch(OpCodes.Br,complete)
                    il.Mark partial
                    literalMode il api utf16 selected mode [|int prefix|] destination state
                    plain()
                    il.Mark complete
                else
                    literalMode il api utf16 selected mode [|int prefix|] destination state
                    plain()
            if comma = -1 then plain()
            elif comma = -2 then prefixed ','
            elif comma = -3 then prefixed '{'
            else
                let body = il.Label()
                let complete = il.Label()
                il.EmitLocal(OpCodes.Ldloc,comma); il.EmitBranch(OpCodes.Brfalse,body)
                prefixed ','
                il.EmitBranch(OpCodes.Br,complete)
                il.Mark body
                plain()
                il.Mark complete)

    let boolean (il: CodecIL<'T,'M,'F>) (api: CodecOutputApi<'T,'M,'F>) utf16 =
        let destination() = il.Emit OpCodes.Ldarg_1
        let state() = il.Emit OpCodes.Ldarg_2
        withDestination il api state (fun selected mode ->
            let no = il.Label()
            let complete = il.Label()
            il.InputValue(); il.EmitBranch(OpCodes.Brfalse,no)
            literalMode il api utf16 selected mode [|116;114;117;101|] destination state
            il.EmitBranch(OpCodes.Br,complete)
            il.Mark no
            literalMode il api utf16 selected mode [|102;97;108;115;101|] destination state
            il.Mark complete)
        il.Emit OpCodes.Ret
