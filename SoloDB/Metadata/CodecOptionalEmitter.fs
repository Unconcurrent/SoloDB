namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Closed optional/enum operations expand in both instruction destinations.
type internal CodecOptionalReaderApi<'T,'M> = {
    Result: 'T; Payload: 'T; Context: 'T; Boolean: 'T; Int32: 'T
    Begin: 'M; Open: 'M; Capture: 'M; End: 'M; Flat: 'M; Restore: 'M; Missing: 'M
    Names: CodecNameApi<'T,'M>; Skip: 'M
    PayloadCall: 'M; FlagCall: 'M
    PrimitiveMismatch: 'T; ScalarMismatch: 'T
    PrimitiveRetry: 'M; ScalarRetry: 'M; FlagPrimitiveRetry: 'M; FlagScalarRetry: 'M
    Construct: 'M option; Transfer: 'M option
}

type internal CodecOptionalWriterApi<'T,'M> = {
    Result: 'T; Payload: 'T; ValueType: bool
    Presence: 'M option; Value: 'M option; Transfer: 'M option
    Null: unit -> unit; PayloadCall: 'M
}

module internal CodecOptionalEmitter =
    let reader (il: CodecIL<'T,'M,'F>) (api: CodecOptionalReaderApi<'T,'M>) =
        let payload = il.Local api.Payload
        let defaultRead target call primitiveRetry scalarRetry arguments =
            let value = il.Local target
            il.BeginException()
            CodecEmitter.readLocal il call arguments value
            il.Catch api.PrimitiveMismatch
            arguments(); il.EmitMethod(OpCodes.Call,primitiveRetry)
            il.EmitLocal(OpCodes.Stloc,value)
            il.Catch api.ScalarMismatch
            il.Emit OpCodes.Pop
            arguments(); il.EmitMethod(OpCodes.Call,scalarRetry)
            il.EmitLocal(OpCodes.Stloc,value)
            il.EndException()
            il.EmitLocal(OpCodes.Ldloc,value)
        let readPayload arguments =
            defaultRead api.Payload api.PayloadCall api.PrimitiveRetry api.ScalarRetry arguments
            il.EmitLocal(OpCodes.Stloc,payload)
        let present () =
            match api.Transfer with
            | Some transfer ->
                il.EmitLocal(OpCodes.Ldloca,payload)
                il.EmitMethod(OpCodes.Call,transfer)
                il.EmitType(OpCodes.Ldobj,api.Result)
            | None ->
                il.EmitLocal(OpCodes.Ldloc,payload)
                il.EmitMethod(OpCodes.Newobj,api.Construct.Value)
        match api.Transfer with
        | Some _ ->
            readPayload(fun () -> il.Emit OpCodes.Ldarg_0)
            present(); il.Emit OpCodes.Ret
        | None ->
            let point = il.Local api.Int32
            let absent = il.Label()
            let flat = il.Label()
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Begin)
            il.EmitLocal(OpCodes.Stloc,point)
            il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,-1)
            il.EmitBranch(OpCodes.Beq,absent)
            il.EmitLocal(OpCodes.Ldloc,point); il.EmitInt(OpCodes.Ldc_I4,int '{')
            il.EmitBranch(OpCodes.Bne_Un,flat)
            let original = il.Local api.Context
            let flag = il.Local api.Context
            let value = il.Local api.Context
            let hasFlag = il.Local api.Boolean
            let hasValue = il.Local api.Boolean
            let first = il.Local api.Boolean
            let loop = il.Label()
            let flagName = il.Label()
            let valueName = il.Label()
            let unknown = il.Label()
            let endObject = il.Label()
            let next = il.Label()
            il.Emit OpCodes.Ldarg_0; il.EmitType(OpCodes.Ldobj,api.Context); il.EmitLocal(OpCodes.Stloc,original)
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Open)
            il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,first)
            il.Mark loop
            CodecNameEmitter.recognize il api.Names first [|"HasValue",flagName;"Value",valueName|] unknown endObject
            let capture target found =
                il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Names.Colon)
                il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Names.Value)
                il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Skip)
                il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloca,target); il.EmitMethod(OpCodes.Call,api.Capture)
                il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,found)
                il.EmitBranch(OpCodes.Br,next)
            il.Mark flagName; capture flag hasFlag
            il.Mark valueName; capture value hasValue
            il.Mark unknown
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Names.Colon)
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Names.Value)
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Skip)
            il.Mark next; il.EmitBranch(OpCodes.Br,loop)
            il.Mark endObject
            il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloca,original); il.EmitMethod(OpCodes.Call,api.End)
            il.EmitLocal(OpCodes.Ldloc,hasFlag); il.EmitBranch(OpCodes.Brfalse,flat)
            defaultRead api.Boolean api.FlagCall api.FlagPrimitiveRetry api.FlagScalarRetry (fun () -> il.EmitLocal(OpCodes.Ldloca,flag))
            il.EmitBranch(OpCodes.Brfalse,absent)
            let valuePresent = il.Label()
            il.EmitLocal(OpCodes.Ldloc,hasValue); il.EmitBranch(OpCodes.Brtrue,valuePresent)
            il.EmitMethod(OpCodes.Call,api.Missing)
            il.Mark valuePresent
            readPayload(fun () -> il.EmitLocal(OpCodes.Ldloca,value))
            il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloca,original); il.EmitMethod(OpCodes.Call,api.Restore)
            present(); il.Emit OpCodes.Ret
            il.Mark flat
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Flat)
            readPayload(fun () -> il.Emit OpCodes.Ldarg_0)
            present(); il.Emit OpCodes.Ret
            il.Mark absent
            let result = il.Local api.Result
            il.EmitLocal(OpCodes.Ldloc,result); il.Emit OpCodes.Ret

    let writer (il: CodecIL<'T,'M,'F>) (api: CodecOptionalWriterApi<'T,'M>) =
        let loadValue () = if api.ValueType then il.InputAddress() else il.InputValue()
        match api.Transfer with
        | Some _ -> ()
        | None ->
            let present = il.Label()
            match api.Presence with
            | Some presence -> loadValue(); il.EmitMethod(OpCodes.Call,presence)
            | None -> il.InputValue()
            il.EmitBranch(OpCodes.Brtrue,present)
            api.Null(); il.Emit OpCodes.Ret
            il.Mark present
        let payload = il.Local api.Payload
        match api.Transfer with
        | Some transfer ->
            il.InputAddress(); il.EmitMethod(OpCodes.Call,transfer); il.EmitType(OpCodes.Ldobj,api.Payload)
        | None -> loadValue(); il.EmitMethod(OpCodes.Call,api.Value.Value)
        il.EmitLocal(OpCodes.Stloc,payload)
        let arguments () = CodecEmitter.callLocal il api.PayloadCall 0 payload; il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
        arguments(); il.EmitMethod(OpCodes.Call,api.PayloadCall)
        il.Emit OpCodes.Ret
