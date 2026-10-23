namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Original enumeration operands, supplied after cold shape admission.
type internal CodecSequenceWriterApi<'T,'M> = {
    BytesType: 'T
    Enumerator: 'T
    GetEnumerator: 'M
    MoveNext: 'M
    Current: 'M
    Dispose: 'M
    DisposeType: 'T option
    Snapshot: 'M
    Bytes: 'M
}

/// Original range and construction operands after pure sequence policy selection.
type internal CodecSequenceReaderApi<'T,'M> = {
    Context: 'T
    Int32: 'T
    Working: 'T
    IsNull: 'M
    NullConstruct: 'M option
    Begin: 'M
    Count: 'M
    Element: 'M
    Construct: 'M
    ConstructCall: bool
    Admit: 'M option
    Receiver: 'T option
    BoxReceiver: bool
    Add: 'M
    Finish: 'M option
    BeforeFinish: 'M option
    FinishConstructor: bool
    CastResult: bool
    PrimitiveError: 'T
    ScalarError: 'T
    PrimitiveRetry: 'M
    ScalarRetry: 'M
}

type internal CodecGroupingReaderApi<'T,'M> = {
    Context: 'T
    Items: 'T
    Key: 'T
    IsNull: 'M
    Begin: 'M
    Capture: CodecFiniteCaptureApi<'T,'M>
    Child: 'M
    Missing: 'M
    ReadItems: 'M
    ReadKey: 'M
    Construct: 'M
    Finish: 'M
    PrimitiveError: 'T
    ScalarError: 'T
    PrimitiveRetry: 'M
    ScalarRetry: 'M
}

module internal CodecSequenceEmitter =
    let missingConstructor (il: CodecIL<'T,'M,'F>) constructor name =
        il.EmitString("Type " + name + " must have a parameterless constructor")
        il.EmitMethod(OpCodes.Newobj,constructor)
        il.Emit OpCodes.Throw

    let private readChild (il: CodecIL<'T,'M,'F>) primitive scalar retry scalarRetry context value read =
        il.BeginException()
        CodecEmitter.readLocal il read (fun () -> il.EmitLocal(OpCodes.Ldloca,context)) value
        il.Catch primitive
        il.EmitLocal(OpCodes.Ldloca,context)
        il.EmitMethod(OpCodes.Call,retry)
        il.EmitLocal(OpCodes.Stloc,value)
        il.Catch scalar
        il.Emit OpCodes.Pop
        il.EmitLocal(OpCodes.Ldloca,context)
        il.EmitMethod(OpCodes.Call,scalarRetry)
        il.EmitLocal(OpCodes.Stloc,value)
        il.EndException()

    let grouping (il: CodecIL<'T,'M,'F>) (api: CodecGroupingReaderApi<'T,'M>) target =
        let present = il.Label()
        let empty = il.Local target
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.IsNull)
        il.EmitBranch(OpCodes.Brfalse,present)
        il.EmitLocal(OpCodes.Ldloc,empty)
        il.Emit OpCodes.Ret
        il.Mark present
        let objectInput = il.Label()
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Begin)
        il.EmitBranch(OpCodes.Brtrue,objectInput)
        il.EmitMethod(OpCodes.Call,api.Missing)
        il.Mark objectInput
        let slots = CodecFiniteCaptureEmitter.capture il api.Capture [|"Items",true;"Key",true|]
        let range = il.Local api.Context
        let items = il.Local api.Items
        let key = il.Local api.Key
        let child index =
            let selected = il.Label()
            il.EmitLocal(OpCodes.Ldloc,snd slots[index]); il.EmitBranch(OpCodes.Brtrue,selected)
            il.EmitMethod(OpCodes.Call,api.Missing)
            il.Mark selected
            il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloc,(fst slots[index]).Value)
            il.EmitMethod(OpCodes.Call,api.Child); il.EmitLocal(OpCodes.Stloc,range)
        child 0
        CodecEmitter.readLocal il api.ReadItems (fun () -> il.EmitLocal(OpCodes.Ldloca,range)) items
        child 1
        readChild il api.PrimitiveError api.ScalarError api.PrimitiveRetry api.ScalarRetry range key
            api.ReadKey
        il.EmitLocal(OpCodes.Ldloc,key)
        il.EmitLocal(OpCodes.Ldloc,items)
        il.EmitMethod(OpCodes.Newobj,api.Construct)
        il.EmitMethod(OpCodes.Call,api.Finish)
        il.EmitType(OpCodes.Castclass,target)
        il.Emit OpCodes.Ret

    let binaryReader (il: CodecIL<'T,'M,'F>) target isNull bytes construct finish nullError primitiveError mismatch =
        let present = il.Label()
        let empty = il.Local target
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,isNull)
        il.EmitBranch(OpCodes.Brfalse,present)
        match nullError with Some error -> il.EmitMethod(OpCodes.Call,error) | None -> ()
        il.EmitLocal(OpCodes.Ldloc,empty)
        il.Emit OpCodes.Ret
        il.Mark present
        let value = il.Local target
        il.BeginException()
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,bytes)
        match construct with Some constructor -> il.EmitMethod(OpCodes.Newobj,constructor) | None -> ()
        match finish with Some convert -> il.EmitMethod(OpCodes.Call,convert) | None -> ()
        il.EmitLocal(OpCodes.Stloc,value)
        il.Catch primitiveError
        il.Emit OpCodes.Pop
        il.EmitMethod(OpCodes.Call,mismatch)
        il.EndException()
        il.EmitLocal(OpCodes.Ldloc,value)
        il.Emit OpCodes.Ret

    let reader (il: CodecIL<'T,'M,'F>) (api: CodecSequenceReaderApi<'T,'M>)
               operation fallback target element wrapper array capacity reverse discardAdd =
        let present = il.Label()
        let empty = il.Local target
        match api.Admit with Some admit -> il.EmitMethod(OpCodes.Call,admit) | None -> ()
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.IsNull)
        il.EmitBranch(OpCodes.Brfalse,present)
        match api.NullConstruct with
        | Some constructor -> il.EmitMethod(OpCodes.Newobj,constructor)
        | None -> il.EmitLocal(OpCodes.Ldloc,empty)
        il.Emit OpCodes.Ret
        il.Mark present
        let source = il.Local api.Context
        let count = il.Local api.Int32
        let result = il.Local api.Working
        let child = il.Local api.Context
        let value = il.Local element
        let index = il.Local api.Int32
        il.Emit OpCodes.Ldarg_0
        il.EmitInt(OpCodes.Ldc_I4,if wrapper then 1 else 0)
        il.EmitMethod(OpCodes.Call,api.Begin)
        il.EmitLocal(OpCodes.Stloc,source)
        il.EmitLocal(OpCodes.Ldloca,source)
        il.EmitMethod(OpCodes.Call,api.Count)
        il.EmitLocal(OpCodes.Stloc,count)
        if array then
            il.EmitLocal(OpCodes.Ldloc,count)
            il.EmitType(OpCodes.Newarr,element)
        else
            if capacity then il.EmitLocal(OpCodes.Ldloc,count)
            il.EmitMethod((if api.ConstructCall then OpCodes.Call else OpCodes.Newobj),api.Construct)
        il.EmitLocal(OpCodes.Stloc,result)
        let receiver = api.Receiver |> Option.map(fun receiverType ->
            let local = il.Local receiverType
            il.EmitLocal(OpCodes.Ldloc,result)
            if api.BoxReceiver then il.EmitType(OpCodes.Box,api.Working)
            il.EmitType(OpCodes.Castclass,receiverType)
            il.EmitLocal(OpCodes.Stloc,local)
            local)
        let completed = il.Label()
        il.EmitLocal(OpCodes.Ldloc,count)
        il.EmitBranch(OpCodes.Brfalse,completed)
        if reverse then
            il.EmitLocal(OpCodes.Ldloc,count)
            il.Emit OpCodes.Ldc_I4_1
            il.Emit OpCodes.Sub
            il.EmitLocal(OpCodes.Stloc,index)
        let defaultRead = match operation element with Some call -> call | None -> fallback element
        do
            let loop = il.Label()
            let endLoop = il.Label()
            il.Mark loop
            il.EmitLocal(OpCodes.Ldloca,source)
            il.EmitLocal(OpCodes.Ldloc,index)
            il.EmitMethod(OpCodes.Call,api.Element)
            il.EmitLocal(OpCodes.Stloc,child)
            // Keep the actual failed child type and its selected input range.
            readChild il api.PrimitiveError api.ScalarError api.PrimitiveRetry api.ScalarRetry child value defaultRead
            il.EmitLocal(OpCodes.Ldloc,Option.defaultValue result receiver)
            if array then il.EmitLocal(OpCodes.Ldloc,index)
            il.EmitLocal(OpCodes.Ldloc,value)
            if array then il.EmitType(OpCodes.Stelem,element)
            else
                il.EmitMethod(OpCodes.Callvirt,api.Add)
                if discardAdd then il.Emit OpCodes.Pop
            il.EmitLocal(OpCodes.Ldloc,index)
            il.Emit OpCodes.Ldc_I4_1
            il.Emit(if reverse then OpCodes.Sub else OpCodes.Add)
            il.EmitLocal(OpCodes.Stloc,index)
            il.EmitLocal(OpCodes.Ldloc,index)
            if reverse then
                il.Emit OpCodes.Ldc_I4_0
                il.EmitBranch(OpCodes.Blt,endLoop)
            else
                il.EmitLocal(OpCodes.Ldloc,count)
                il.EmitBranch(OpCodes.Bge,endLoop)
            il.EmitBranch(OpCodes.Br,loop)
            il.Mark endLoop
        il.Mark completed
        il.EmitLocal(OpCodes.Ldloc,result)
        match api.BeforeFinish with Some finish -> il.EmitMethod(OpCodes.Call,finish) | None -> ()
        match api.Finish with
        | Some finish -> il.EmitMethod((if api.FinishConstructor then OpCodes.Newobj else OpCodes.Call),finish)
        | None -> ()
        if api.CastResult then il.EmitType(OpCodes.Castclass,target)
        il.Emit OpCodes.Ret

    let writer (il: CodecIL<'T,'M,'F>) (output: CodecRawWriterApi<'T,'M>)
               (sequence: CodecSequenceWriterApi<'T,'M>) target element valueType binary =
        if not valueType then
            let present = il.Label()
            il.InputValue()
            il.EmitBranch(OpCodes.Brtrue,present)
            output.Literal "null"
            il.Emit OpCodes.Ret
            il.Mark present
        if binary then
            let bytes = il.Local sequence.BytesType
            il.InputValue()
            if valueType then il.EmitType(OpCodes.Box,target)
            il.EmitMethod(OpCodes.Call,sequence.Snapshot)
            il.EmitLocal(OpCodes.Stloc,bytes)
            CodecEmitter.callLocal il sequence.Bytes 0 bytes
            il.Emit OpCodes.Ldarg_1
            il.Emit OpCodes.Ldarg_2
            il.EmitMethod(OpCodes.Call,sequence.Bytes)
        else
            let iterator = il.Local sequence.Enumerator
            let current = il.Local element
            output.Literal "["
            il.InputValue()
            if valueType then il.EmitType(OpCodes.Box,target)
            il.EmitMethod(OpCodes.Callvirt,sequence.GetEnumerator)
            il.EmitLocal(OpCodes.Stloc,iterator)
            il.BeginException()
            let finish = il.Label()
            il.EmitLocal(OpCodes.Ldloc,iterator)
            il.EmitMethod(OpCodes.Callvirt,sequence.MoveNext)
            il.EmitBranch(OpCodes.Brfalse,finish)
            il.EmitLocal(OpCodes.Ldloc,iterator)
            il.EmitMethod(OpCodes.Callvirt,sequence.Current)
            il.EmitLocal(OpCodes.Stloc,current)
            // Current precedes child preparation; the captured first value is not replayed.
            let defaultWrite = match output.Operation element with Some call -> call | None -> output.Serialize element
            let write arguments = arguments(); il.EmitMethod(OpCodes.Call,defaultWrite)
            do
                let next = il.Label()
                let doneLoop = il.Label()
                let arguments () =
                    CodecEmitter.callLocal il defaultWrite 0 current
                    il.Emit OpCodes.Ldarg_1
                    il.Emit OpCodes.Ldarg_2
                write arguments
                il.Mark next
                il.EmitLocal(OpCodes.Ldloc,iterator)
                il.EmitMethod(OpCodes.Callvirt,sequence.MoveNext)
                il.EmitBranch(OpCodes.Brfalse,doneLoop)
                output.Literal ","
                il.EmitLocal(OpCodes.Ldloc,iterator)
                il.EmitMethod(OpCodes.Callvirt,sequence.Current)
                il.EmitLocal(OpCodes.Stloc,current)
                write arguments
                il.EmitBranch(OpCodes.Br,next)
                il.Mark doneLoop
            il.Mark finish
            output.Literal "]"
            il.Finally()
            let disposed = il.Label()
            match sequence.DisposeType with
            | None ->
                il.EmitLocal(OpCodes.Ldloc,iterator)
                il.EmitBranch(OpCodes.Brfalse,disposed)
                il.EmitLocal(OpCodes.Ldloc,iterator)
                il.EmitMethod(OpCodes.Callvirt,sequence.Dispose)
            | Some disposable ->
                let owner = il.Local disposable
                il.EmitLocal(OpCodes.Ldloc,iterator)
                il.EmitType(OpCodes.Isinst,disposable)
                il.EmitLocal(OpCodes.Stloc,owner)
                il.EmitLocal(OpCodes.Ldloc,owner)
                il.EmitBranch(OpCodes.Brfalse,disposed)
                il.EmitLocal(OpCodes.Ldloc,owner)
                il.EmitMethod(OpCodes.Callvirt,sequence.Dispose)
            il.Mark disposed
            il.EndException()
        il.Emit OpCodes.Ret
