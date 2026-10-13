namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Original tuple grammar and child-error operands for concrete capture.
type internal CodecTupleReaderApi<'T,'M> = {
    Context: 'T; Range: 'T; Boolean: 'T; Int32: 'T; Names: CodecNameApi<'T,'M>
    Begin: 'M; NextArray: 'M; Capture: 'M; Complete: 'M
    Child: 'M; Missing: 'M
    PrimitiveError: 'T; ScalarError: 'T
    PrimitiveRetry: 'T -> 'M; ScalarRetry: 'T -> 'M
}

/// Composite concrete writers retain the original member flag scope.
type internal CodecTupleWriterScope<'T,'F> = {
    Boolean: 'T; IncludeType: 'F; OmitId: 'F; Required: 'T -> bool
}

module internal CodecTupleEmitter =
    let leaves (model: CodecTuple<'T,'M,'F>) =
        let rec collect prefix (tuple: CodecTuple<'T,'M,'F>) = [|
            for item in tuple.Members do
                let path = Array.append prefix [|tuple.ValueType,item.Member|]
                match item.Rest with
                | Some rest -> yield! collect path rest
                | None -> yield path
        |]
        collect [||] model

    let private load (il: CodecIL<'T,'M,'F>) boxed (model: CodecTuple<'T,'M,'F>) (path: (bool * CodecMember<'T,'M,'F>)[]) =
        if boxed then
            il.Emit OpCodes.Ldarg_0
            il.EmitType((if model.ValueType then OpCodes.Unbox else OpCodes.Castclass),model.Type)
        else il.EmitInt((if model.ValueType then OpCodes.Ldarga else OpCodes.Ldarg),0)
        for index = 0 to path.Length - 1 do
            let parentValueType,memberValue = path[index]
            let address = index + 1 < path.Length && fst path[index + 1]
            match memberValue.Field,memberValue.Getter with
            | Some field,_ -> il.EmitField((if address then OpCodes.Ldflda else OpCodes.Ldfld),field)
            | _,Some getter ->
                il.EmitMethod((if parentValueType then OpCodes.Call else OpCodes.Callvirt),getter)
                if address then
                    let value = il.Local memberValue.Type
                    il.EmitLocal(OpCodes.Stloc,value)
                    il.EmitLocal(OpCodes.Ldloca,value)
            | _ -> invalidOp "A tuple element has no readable member."

    let getter (il: CodecIL<'T,'M,'F>) boxed (model: CodecTuple<'T,'M,'F>) path valueType =
        load il boxed model path
        if boxed && valueType then il.EmitType(OpCodes.Box,(snd (Array.last path)).Type)
        il.Emit OpCodes.Ret

    let writer (il: CodecIL<'T,'M,'F>) json serialize (model: CodecTuple<'T,'M,'F>) =
        let elements = leaves model
        il.EmitInt(OpCodes.Ldc_I4,elements.Length)
        il.EmitType(OpCodes.Newarr,json)
        for index = 0 to elements.Length - 1 do
            let path = elements[index]
            il.Emit OpCodes.Dup
            il.EmitInt(OpCodes.Ldc_I4,index)
            load il false model path
            il.EmitMethod(OpCodes.Call,serialize (snd (Array.last path)).Type)
            il.Emit OpCodes.Stelem_Ref
        il.Emit OpCodes.Ret

    let reader (il: CodecIL<'T,'M,'F>) tupleElement deserialize (model: CodecTuple<'T,'M,'F>) =
        let mutable ordinal = 0
        let rec construct (tuple: CodecTuple<'T,'M,'F>) =
            for item in tuple.Members do
                match item.Rest with
                | Some rest -> construct rest
                | None ->
                    il.Emit OpCodes.Ldarg_0
                    il.EmitInt(OpCodes.Ldc_I4,ordinal)
                    ordinal <- ordinal + 1
                    il.EmitMethod(OpCodes.Call,tupleElement)
                    il.EmitMethod(OpCodes.Callvirt,deserialize item.Member.Type)
            il.EmitMethod(OpCodes.Newobj,tuple.Constructor)
        construct model
        il.Emit OpCodes.Ret

    let rawReader (il: CodecIL<'T,'M,'F>) (api: CodecTupleReaderApi<'T,'M>)
                  operation fallback (model: CodecTuple<'T,'M,'F>) =
        let elements = leaves model
        let slots = elements |> Array.map(fun _ -> il.Local api.Range,il.Local api.Boolean)
        let mode = il.Local api.Int32
        il.Emit OpCodes.Ldarg_0
        il.EmitMethod(OpCodes.Call,api.Begin)
        il.EmitLocal(OpCodes.Stloc,mode)
        let present = il.Label()
        il.EmitLocal(OpCodes.Ldloc,mode); il.Emit OpCodes.Ldc_I4_M1
        il.EmitBranch(OpCodes.Bne_Un,present)
        CodecEmitter.returnDefault il model.Type
        il.Mark present
        let first = il.Local api.Boolean
        il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,first)
        let range = il.Local api.Range
        let completed = il.Label()
        let objectPath = il.Label()
        il.EmitLocal(OpCodes.Ldloc,mode); il.EmitBranch(OpCodes.Brtrue,objectPath)
        let ordinal = il.Local api.Int32
        let nextArray = il.Label()
        let storeLabels = slots |> Array.map(fun _ -> il.Label())
        let advance = il.Label()
        il.Mark nextArray
        il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloc,first)
        il.EmitMethod(OpCodes.Call,api.NextArray)
        il.EmitBranch(OpCodes.Brfalse,completed)
        il.Emit OpCodes.Ldarg_0; il.Emit OpCodes.Ldc_I4_1
        il.EmitMethod(OpCodes.Call,api.Capture); il.EmitLocal(OpCodes.Stloc,range)
        il.EmitLocal(OpCodes.Ldloc,ordinal); il.EmitSwitch storeLabels
        il.EmitBranch(OpCodes.Br,advance)
        for index = 0 to slots.Length - 1 do
            let value,presence = slots[index]
            il.Mark storeLabels[index]
            il.EmitLocal(OpCodes.Ldloc,range); il.EmitLocal(OpCodes.Stloc,value)
            il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,presence)
            il.EmitBranch(OpCodes.Br,advance)
        il.Mark advance
        il.EmitLocal(OpCodes.Ldloc,ordinal); il.Emit OpCodes.Ldc_I4_1; il.Emit OpCodes.Add
        il.EmitLocal(OpCodes.Stloc,ordinal)
        il.Emit OpCodes.Ldc_I4_0; il.EmitLocal(OpCodes.Stloc,first)
        il.EmitBranch(OpCodes.Br,nextArray)
        il.Mark objectPath
        let nextObject = il.Label()
        let afterName = il.Label()
        let objectLabels = slots |> Array.map(fun _ -> il.Label())
        il.Mark nextObject
        CodecNameEmitter.recognize il api.Names first (objectLabels |> Array.mapi(fun index label -> "Item" + string(index + 1),label)) afterName completed
        let capture() =
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Names.Colon)
            il.Emit OpCodes.Ldarg_0; il.Emit OpCodes.Ldc_I4_0
            il.EmitMethod(OpCodes.Call,api.Capture); il.EmitLocal(OpCodes.Stloc,range)
        let captured = il.Label()
        for index = 0 to slots.Length - 1 do
            let value,presence = slots[index]
            il.Mark objectLabels[index]
            capture()
            il.EmitLocal(OpCodes.Ldloc,range); il.EmitLocal(OpCodes.Stloc,value)
            il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,presence)
            il.EmitBranch(OpCodes.Br,captured)
        il.Mark afterName; capture()
        il.Mark captured
        il.EmitBranch(OpCodes.Br,nextObject)
        il.Mark completed
        il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,api.Complete)
        let child = il.Local api.Context
        let values = elements |> Array.map(fun path -> il.Local (snd(Array.last path)).Type)
        for index = 0 to elements.Length - 1 do
            let target = (snd(Array.last elements[index])).Type
            let range,presence = slots[index]
            let available = il.Label()
            il.EmitLocal(OpCodes.Ldloc,presence); il.EmitBranch(OpCodes.Brtrue,available)
            il.EmitMethod(OpCodes.Call,api.Missing)
            il.Mark available
            il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloc,range)
            il.EmitMethod(OpCodes.Call,api.Child); il.EmitLocal(OpCodes.Stloc,child)
            let read = match operation target with Some call -> call | None -> fallback target
            il.BeginException()
            il.EmitLocal(OpCodes.Ldloca,child); il.EmitMethod(OpCodes.Call,read)
            il.EmitLocal(OpCodes.Stloc,values[index])
            il.Catch api.PrimitiveError
            il.EmitLocal(OpCodes.Ldloca,child); il.EmitMethod(OpCodes.Call,api.PrimitiveRetry target)
            il.EmitLocal(OpCodes.Stloc,values[index])
            il.Catch api.ScalarError
            il.Emit OpCodes.Pop
            il.EmitLocal(OpCodes.Ldloca,child); il.EmitMethod(OpCodes.Call,api.ScalarRetry target)
            il.EmitLocal(OpCodes.Stloc,values[index])
            il.EndException()
        let mutable index = 0
        let rec construct (tuple: CodecTuple<'T,'M,'F>) =
            for memberValue in tuple.Members do
                match memberValue.Rest with
                | Some rest -> construct rest
                | None -> il.EmitLocal(OpCodes.Ldloc,values[index]); index <- index + 1
            il.EmitMethod(OpCodes.Newobj,tuple.Constructor)
        construct model
        il.Emit OpCodes.Ret

    let rawWriter (il: CodecIL<'T,'M,'F>) (output: CodecRawWriterApi<'T,'M>)
                  (scope: CodecTupleWriterScope<'T,'F>) (model: CodecTuple<'T,'M,'F>) =
        if not model.ValueType then
            let present = il.Label()
            il.Emit OpCodes.Ldarg_0
            il.EmitBranch(OpCodes.Brtrue,present)
            il.Emit OpCodes.Ldarg_1
            il.Emit OpCodes.Ldarg_2
            il.EmitMethod(OpCodes.Call,output.Null)
            il.Emit OpCodes.Ret
            il.Mark present
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,output.BeginArray)
        let elements = leaves model
        for index = 0 to elements.Length - 1 do
            if index > 0 then
                il.Emit OpCodes.Ldarg_1
                il.Emit OpCodes.Ldarg_2
                il.EmitMethod(OpCodes.Call,output.Separator)
            let path = elements[index]
            let target = (snd(Array.last path)).Type
            let direct = output.Operation target
            let scoped = direct.IsSome && scope.Required target
            let value = il.Local target
            load il false model path
            il.EmitLocal(OpCodes.Stloc,value)
            let arguments () =
                il.EmitLocal(OpCodes.Ldloc,value); il.Emit OpCodes.Ldarg_1; il.Emit OpCodes.Ldarg_2
            let call = match direct with Some call -> call | None -> output.Serialize target
            if scoped then
                let previousType = il.Local scope.Boolean
                let previousId = il.Local scope.Boolean
                il.Emit OpCodes.Ldarg_2; il.EmitField(OpCodes.Ldfld,scope.IncludeType)
                il.EmitLocal(OpCodes.Stloc,previousType)
                il.Emit OpCodes.Ldarg_2; il.EmitField(OpCodes.Ldfld,scope.OmitId)
                il.EmitLocal(OpCodes.Stloc,previousId)
                il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldc_I4_0; il.EmitField(OpCodes.Stfld,scope.IncludeType)
                il.Emit OpCodes.Ldarg_2; il.Emit OpCodes.Ldc_I4_0; il.EmitField(OpCodes.Stfld,scope.OmitId)
                il.BeginException()
                arguments(); il.EmitMethod(OpCodes.Call,call)
                il.Finally()
                il.Emit OpCodes.Ldarg_2; il.EmitLocal(OpCodes.Ldloc,previousType); il.EmitField(OpCodes.Stfld,scope.IncludeType)
                il.Emit OpCodes.Ldarg_2; il.EmitLocal(OpCodes.Ldloc,previousId); il.EmitField(OpCodes.Stfld,scope.OmitId)
                il.EndException()
            else
                arguments()
                il.EmitMethod(OpCodes.Call,call)
        il.Emit OpCodes.Ldarg_1
        il.Emit OpCodes.Ldarg_2
        il.EmitMethod(OpCodes.Call,output.EndArray)
        il.Emit OpCodes.Ret
