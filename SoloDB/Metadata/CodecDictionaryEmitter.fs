namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Original normalized-range, conversion and insertion operands.
type internal CodecDictionaryReaderApi<'T,'M,'F> = {
    Context: 'T; Working: 'T; Receiver: 'T; Enumerator: 'T; Entry: 'T
    String: 'T; Key: 'T; Value: 'T
    IsNull: 'M; Begin: 'M; Properties: 'F; GetEnumerator: 'M
    MoveNext: 'M; Current: 'M; Name: 'M; Range: 'M; Child: 'M; Dispose: 'M
    Construct: 'M; ConstructCall: bool; ConstructCast: bool; BoxReceiver: bool
    CastResult: bool; TargetValueType: bool
    ConvertKey: 'M option; KeyInvoke: 'M option
    Add: 'M; Pair: 'M option; Equal: 'M
    PrimitiveError: 'T; ScalarError: 'T; PrimitiveRetry: 'M; ScalarRetry: 'M
}

/// One original Keys/indexer interface contract, selected outside enumeration.
type internal CodecDictionaryContract<'T,'M> = {
    Type: 'T; Keys: 'M; Indexer: 'M; GetEnumerator: 'M
    Enumerator: 'T; MoveNext: 'M; Current: 'M; Dispose: 'M
    DisposeType: 'T option
}

type internal CodecDictionaryWriterApi<'T,'M> = {
    Object: 'T; String: 'T; Boolean: 'T; Key: 'T; Value: 'T; Names: 'T
    ReadOnly: CodecDictionaryContract<'T,'M> option
    Mutable: CodecDictionaryContract<'T,'M> option
    ProbeReadOnly: bool
    Name: 'M; NameVirtual: bool
    NewNames: 'M; AddName: 'M; Equal: 'M; Concat: 'M; ArgumentException: 'M
    Property: 'M; TypeMember: 'M; EndObject: 'M
}

type internal CodecDictionaryKeyApi<'T,'M> = {
    Object: 'T; String: 'T; Formattable: 'T
    Invariant: 'M; Format: 'M; ToString: 'M
}

module internal CodecDictionaryEmitter =
    /// Expands the original FSharp string conversion with established closed-type facts.
    let formatKey (il: CodecIL<'T,'M,'F>) (api: CodecDictionaryKeyApi<'T,'M>)
                  target valueType knownFormattable probe stringKey =
        let result = il.Local api.String
        let finish = il.Label()
        let empty = il.Label()
        let format () =
            if valueType then il.InputAddress() else il.InputValue()
            il.Emit OpCodes.Ldnull
            il.EmitMethod(OpCodes.Call,api.Invariant)
            if valueType then il.EmitType(OpCodes.Constrained,target)
            il.EmitMethod(OpCodes.Callvirt,api.Format)
        if stringKey then
            il.InputValue()
        elif probe then
            let boxed = il.Local api.Object
            let receiver = il.Local api.Formattable
            il.InputValue()
            if valueType then il.EmitType(OpCodes.Box,target)
            il.EmitLocal(OpCodes.Stloc,boxed)
            il.EmitLocal(OpCodes.Ldloc,boxed); il.EmitBranch(OpCodes.Brfalse,empty)
            il.EmitLocal(OpCodes.Ldloc,boxed); il.EmitType(OpCodes.Isinst,api.Formattable)
            il.EmitLocal(OpCodes.Stloc,receiver)
            let ordinary = il.Label()
            il.EmitLocal(OpCodes.Ldloc,receiver); il.EmitBranch(OpCodes.Brfalse,ordinary)
            il.EmitLocal(OpCodes.Ldloc,receiver); il.Emit OpCodes.Ldnull
            il.EmitMethod(OpCodes.Call,api.Invariant)
            il.EmitMethod(OpCodes.Callvirt,api.Format)
            il.EmitLocal(OpCodes.Stloc,result); il.EmitBranch(OpCodes.Br,finish)
            il.Mark ordinary
            il.InputAddress(); il.EmitType(OpCodes.Constrained,target)
            il.EmitMethod(OpCodes.Callvirt,api.ToString)
        else
            if not valueType then
                il.InputValue(); il.EmitBranch(OpCodes.Brfalse,empty)
            if knownFormattable then format()
            else
                il.InputAddress(); il.EmitType(OpCodes.Constrained,target)
                il.EmitMethod(OpCodes.Callvirt,api.ToString)
        il.EmitLocal(OpCodes.Stloc,result)
        il.Mark finish
        il.EmitLocal(OpCodes.Ldloc,result); il.EmitBranch(OpCodes.Brfalse,empty)
        il.EmitLocal(OpCodes.Ldloc,result); il.Emit OpCodes.Ret
        il.Mark empty
        il.EmitString ""; il.Emit OpCodes.Ret

    let reader (il: CodecIL<'T,'M,'F>) (api: CodecDictionaryReaderApi<'T,'M,'F>)
               operation fallback target beforeScan =
        let present = il.Label()
        let empty = il.Local target
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.IsNull)
        il.EmitBranch(OpCodes.Brfalse,present)
        il.EmitLocal(OpCodes.Ldloc,empty)
        il.Emit OpCodes.Ret
        il.Mark present
        let source = il.Local api.Context
        let result = il.Local api.Working
        let receiver = il.Local api.Receiver
        let construct () =
            il.EmitMethod((if api.ConstructCall then OpCodes.Call else OpCodes.Newobj),api.Construct)
            if api.ConstructCast then il.EmitType(OpCodes.Castclass,api.Working)
            il.EmitLocal(OpCodes.Stloc,result)
            il.EmitLocal(OpCodes.Ldloc,result)
            if api.BoxReceiver then il.EmitType(OpCodes.Box,api.Working)
            il.EmitType(OpCodes.Castclass,api.Receiver)
            il.EmitLocal(OpCodes.Stloc,receiver)
        if beforeScan then construct()
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.Begin)
        il.EmitLocal(OpCodes.Stloc,source)
        if not beforeScan then construct()
        let iterator = il.Local api.Enumerator
        let entry = il.Local api.Entry
        let name = il.Local api.String
        let key = il.Local api.Key
        let value = il.Local api.Value
        let child = il.Local api.Context
        il.EmitLocal(OpCodes.Ldloca,source)
        il.EmitField(OpCodes.Ldfld,api.Properties)
        il.EmitMethod(OpCodes.Callvirt,api.GetEnumerator)
        il.EmitLocal(OpCodes.Stloc,iterator)
        il.BeginException()
        let next = il.Label()
        let finish = il.Label()
        il.Mark next
        il.EmitLocal(OpCodes.Ldloca,iterator)
        il.EmitMethod(OpCodes.Call,api.MoveNext)
        il.EmitBranch(OpCodes.Brfalse,finish)
        il.EmitLocal(OpCodes.Ldloca,iterator)
        il.EmitMethod(OpCodes.Call,api.Current)
        il.EmitLocal(OpCodes.Stloc,entry)
        il.EmitLocal(OpCodes.Ldloca,entry)
        il.EmitMethod(OpCodes.Call,api.Name)
        il.EmitLocal(OpCodes.Stloc,name)
        il.EmitLocal(OpCodes.Ldloc,name)
        il.EmitString "$type"
        il.EmitMethod(OpCodes.Call,api.Equal)
        il.EmitBranch(OpCodes.Brtrue,next)
        match api.ConvertKey with
        | Some acquire ->
            il.EmitMethod(OpCodes.Call,acquire)
            il.EmitLocal(OpCodes.Ldloc,name)
            il.EmitMethod(OpCodes.Callvirt,api.KeyInvoke.Value)
        | None -> il.EmitLocal(OpCodes.Ldloc,name)
        il.EmitLocal(OpCodes.Stloc,key)
        il.EmitLocal(OpCodes.Ldloca,source)
        il.EmitLocal(OpCodes.Ldloca,entry)
        il.EmitMethod(OpCodes.Call,api.Range)
        il.EmitMethod(OpCodes.Call,api.Child)
        il.EmitLocal(OpCodes.Stloc,child)
        let read = match operation api.Value with Some read -> read | None -> fallback api.Value
        il.BeginException()
        CodecEmitter.readLocal il read (fun () -> il.EmitLocal(OpCodes.Ldloca,child)) value
        il.Catch api.PrimitiveError
        il.EmitLocal(OpCodes.Ldloca,child)
        il.EmitMethod(OpCodes.Call,api.PrimitiveRetry)
        il.EmitLocal(OpCodes.Stloc,value)
        il.Catch api.ScalarError
        il.Emit OpCodes.Pop
        il.EmitLocal(OpCodes.Ldloca,child)
        il.EmitMethod(OpCodes.Call,api.ScalarRetry)
        il.EmitLocal(OpCodes.Stloc,value)
        il.EndException()
        il.EmitLocal(OpCodes.Ldloc,receiver)
        il.EmitLocal(OpCodes.Ldloc,key)
        il.EmitLocal(OpCodes.Ldloc,value)
        match api.Pair with Some pair -> il.EmitMethod(OpCodes.Newobj,pair) | None -> ()
        il.EmitMethod(OpCodes.Callvirt,api.Add)
        il.EmitBranch(OpCodes.Br,next)
        il.Mark finish
        il.Finally()
        il.EmitLocal(OpCodes.Ldloca,iterator)
        il.EmitMethod(OpCodes.Call,api.Dispose)
        il.EndException()
        il.EmitLocal(OpCodes.Ldloc,result)
        if api.CastResult then
            if api.BoxReceiver then il.EmitType(OpCodes.Box,api.Working)
            il.EmitType((if api.TargetValueType then OpCodes.Unbox_Any else OpCodes.Castclass),target)
        il.Emit OpCodes.Ret

    let writer (il: CodecIL<'T,'M,'F>) (output: CodecRawWriterApi<'T,'M>)
               (api: CodecDictionaryWriterApi<'T,'M>) target valueType nonGeneric =
        let boxed = il.Local api.Object
        if not valueType then
            let present = il.Label()
            il.InputValue()
            il.EmitLocal(OpCodes.Stloc,boxed)
            il.EmitLocal(OpCodes.Ldloc,boxed)
            il.EmitBranch(OpCodes.Brtrue,present)
            output.Literal "null"
            il.Emit OpCodes.Ret
            il.Mark present
        output.Literal "{"
        let comma = il.Local api.Boolean
        let ownsType = il.Local api.Boolean
        let names = il.Local api.Names
        il.EmitMethod(OpCodes.Newobj,api.NewNames)
        il.EmitLocal(OpCodes.Stloc,names)
        let write = match output.Operation api.Value with Some write -> write | None -> output.Serialize api.Value
        let key = il.Local api.Key
        let value = il.Local api.Value
        let name = il.Local api.String
        let emitContract (contract: CodecDictionaryContract<'T,'M>) receiver =
            let iterator = il.Local contract.Enumerator
            receiver()
            if valueType then il.EmitType(OpCodes.Constrained,target)
            il.EmitMethod(OpCodes.Callvirt,contract.Keys)
            il.EmitMethod(OpCodes.Callvirt,contract.GetEnumerator)
            il.EmitLocal(OpCodes.Stloc,iterator)
            il.BeginException()
            let next = il.Label()
            let finish = il.Label()
            il.Mark next
            il.EmitLocal(OpCodes.Ldloc,iterator)
            il.EmitMethod(OpCodes.Callvirt,contract.MoveNext)
            il.EmitBranch(OpCodes.Brfalse,finish)
            il.EmitLocal(OpCodes.Ldloc,iterator)
            il.EmitMethod(OpCodes.Callvirt,contract.Current)
            il.EmitLocal(OpCodes.Stloc,key)
            let format () =
                if api.NameVirtual then il.EmitLocal(OpCodes.Ldloc,key)
                else CodecEmitter.callLocal il api.Name 0 key
                il.EmitMethod((if api.NameVirtual then OpCodes.Callvirt else OpCodes.Call),api.Name)
                il.EmitLocal(OpCodes.Stloc,name)
            let fetch () =
                receiver()
                il.EmitLocal(OpCodes.Ldloc,key)
                if valueType then il.EmitType(OpCodes.Constrained,target)
                il.EmitMethod(OpCodes.Callvirt,contract.Indexer)
                il.EmitLocal(OpCodes.Stloc,value)
            if nonGeneric then
                format()
                fetch()
            else
                fetch()
                format()
            let unique = il.Label()
            il.EmitLocal(OpCodes.Ldloc,names); il.EmitLocal(OpCodes.Ldloc,name)
            il.EmitMethod(OpCodes.Callvirt,api.AddName)
            il.EmitBranch(OpCodes.Brtrue,unique)
            CodecEmitter.callLocal il (output.Discard api.Value) 0 value
            il.EmitMethod(OpCodes.Call,output.Discard api.Value)
            il.EmitString "An item with the same key has already been added. Key: "
            il.EmitLocal(OpCodes.Ldloc,name)
            il.EmitMethod(OpCodes.Call,api.Concat)
            il.EmitMethod(OpCodes.Newobj,api.ArgumentException)
            il.Emit OpCodes.Throw
            il.Mark unique
            let emit = il.Label()
            il.EmitLocal(OpCodes.Ldloc,name); il.EmitString "Id"
            il.EmitMethod(OpCodes.Call,api.Equal)
            il.EmitBranch(OpCodes.Brfalse,emit)
            il.Emit OpCodes.Ldarg_2; il.EmitMethod(OpCodes.Call,output.OmitId)
            il.EmitBranch(OpCodes.Brfalse,emit)
            CodecEmitter.callLocal il (output.Discard api.Value) 0 value
            il.EmitMethod(OpCodes.Call,output.Discard api.Value)
            il.EmitBranch(OpCodes.Br,next)
            il.Mark emit
            il.EmitLocal(OpCodes.Ldloc,name); il.EmitLocal(OpCodes.Ldloc,comma)
            il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
            il.EmitMethod(OpCodes.Call,api.Property)
            il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,comma)
            let ordinary = il.Label()
            il.EmitLocal(OpCodes.Ldloc,name); il.EmitString "$type"
            il.EmitMethod(OpCodes.Call,api.Equal)
            il.EmitBranch(OpCodes.Brfalse,ordinary)
            il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,ownsType)
            CodecEmitter.callLocal il api.TypeMember 0 value; il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
            il.EmitMethod(OpCodes.Call,api.TypeMember)
            il.EmitBranch(OpCodes.Br,next)
            il.Mark ordinary
            CodecEmitter.callLocal il write 0 value; il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
            il.EmitMethod(OpCodes.Call,write)
            il.EmitBranch(OpCodes.Br,next)
            il.Mark finish
            il.Finally()
            let disposed = il.Label()
            match contract.DisposeType with
            | Some disposable ->
                let owner = il.Local disposable
                il.EmitLocal(OpCodes.Ldloc,iterator); il.EmitType(OpCodes.Isinst,disposable)
                il.EmitLocal(OpCodes.Stloc,owner); il.EmitLocal(OpCodes.Ldloc,owner)
                il.EmitBranch(OpCodes.Brfalse,disposed)
                il.EmitLocal(OpCodes.Ldloc,owner)
            | None ->
                il.EmitLocal(OpCodes.Ldloc,iterator); il.EmitBranch(OpCodes.Brfalse,disposed)
                il.EmitLocal(OpCodes.Ldloc,iterator)
            il.EmitMethod(OpCodes.Callvirt,contract.Dispose)
            il.Mark disposed
            il.EndException()
        let finish = il.Label()
        let bind (contract: CodecDictionaryContract<'T,'M>) test =
            let receiver = il.Local contract.Type
            il.EmitLocal(OpCodes.Ldloc,boxed)
            il.EmitType((if test then OpCodes.Isinst else OpCodes.Castclass),contract.Type)
            il.EmitLocal(OpCodes.Stloc,receiver)
            receiver
        let emitBound contract =
            if valueType then emitContract contract (fun () -> il.InputAddress())
            else
                let receiver = bind contract false
                emitContract contract (fun () -> il.EmitLocal(OpCodes.Ldloc,receiver))
        match api.ReadOnly,api.Mutable with
        | Some readOnly,Some mutableContract when api.ProbeReadOnly && not valueType ->
            let receiver = bind readOnly true
            let mutablePath = il.Label()
            il.EmitLocal(OpCodes.Ldloc,receiver); il.EmitBranch(OpCodes.Brfalse,mutablePath)
            emitContract readOnly (fun () -> il.EmitLocal(OpCodes.Ldloc,receiver))
            il.EmitBranch(OpCodes.Br,finish)
            il.Mark mutablePath
            emitBound mutableContract
        | Some readOnly,_ -> emitBound readOnly
        | _,Some mutableContract -> emitBound mutableContract
        | _ -> invalidOp "A dictionary writer requires its original collection contract."
        il.Mark finish
        il.EmitLocal(OpCodes.Ldloc,comma); il.EmitLocal(OpCodes.Ldloc,ownsType)
        il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,api.EndObject)
        il.Emit OpCodes.Ret
