namespace SoloDatabase.Metadata.CodecGeneration

open System.Reflection.Emit

/// Original admitted grammar and child conversion operands for selected unions.
type internal CodecUnionReaderApi<'T,'M> = {
    Context: 'T; Range: 'T; Boolean: 'T; Names: CodecNameApi<'T,'M>
    Begin: 'M; Capture: 'M; Complete: 'M
    Child: 'M; Missing: 'M
    PrimitiveError: 'T; ScalarError: 'T
    PrimitiveRetry: 'T -> 'M; ScalarRetry: 'T -> 'M
}

module internal CodecUnionEmitter =
    let reader (il: CodecIL<'T,'M,'F>) (api: CodecApi<'T,'M>)
               (input: CodecUnionReaderApi<'T,'M>) operation fallback (model: CodecModel<'T,'M,'F>) =
        if model.Cases |> Array.exists(fun case -> case.Factory.IsNone) then
            il.EmitString "An index satisfying the predicate was not found in the collection."
            il.EmitMethod(OpCodes.Newobj,api.MissingFactoryConstructor)
            il.Emit OpCodes.Throw
        else
            il.Emit OpCodes.Ldarg_0
            il.EmitMethod(OpCodes.Call,input.Begin)
            let present = il.Label()
            il.EmitBranch(OpCodes.Brtrue,present)
            CodecEmitter.returnDefault il model.Type
            il.Mark present

            let names = [| yield "Tag"; for case in model.Cases do for name,_ in case.Fields do yield name |] |> Array.distinct
            let slots = names |> Array.map(fun name -> name,il.Local input.Range,il.Local input.Boolean)
            let predicates = model.Cases |> Array.map(fun case -> "Is" + case.Name,il.Local input.Boolean)
            let identities = Array.append names (predicates |> Array.map fst) |> Array.distinct
            let first = il.Local input.Boolean
            let range = il.Local input.Range
            let truth = il.Local input.Boolean
            il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,first)
            let loop = il.Label()
            let completed = il.Label()
            let next = il.Label()
            let targets = identities |> Array.map(fun identity -> identity,il.Label())
            let unmatched = il.Label()
            il.Mark loop
            CodecNameEmitter.recognize il input.Names first targets unmatched completed
            let capture() =
                il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,input.Names.Colon)
                il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloca,truth)
                il.EmitMethod(OpCodes.Call,input.Capture); il.EmitLocal(OpCodes.Stloc,range)
            for identity,target in targets do
                il.Mark target; capture()
                for slotName,slot,presence in slots do
                    if slotName = identity then
                        il.EmitLocal(OpCodes.Ldloc,range); il.EmitLocal(OpCodes.Stloc,slot)
                        il.Emit OpCodes.Ldc_I4_1; il.EmitLocal(OpCodes.Stloc,presence)
                for predicate,value in predicates do
                    if predicate = identity then
                        il.EmitLocal(OpCodes.Ldloc,truth); il.EmitLocal(OpCodes.Stloc,value)
                il.EmitBranch(OpCodes.Br,next)
            il.Mark unmatched; capture()
            il.Mark next
            il.EmitBranch(OpCodes.Br,loop)
            il.Mark completed
            il.Emit OpCodes.Ldarg_0; il.EmitMethod(OpCodes.Call,input.Complete)

            let child = il.Local input.Context
            let convert target range result =
                il.Emit OpCodes.Ldarg_0; il.EmitLocal(OpCodes.Ldloc,range)
                il.EmitMethod(OpCodes.Call,input.Child); il.EmitLocal(OpCodes.Stloc,child)
                let call = match operation target with Some call -> call | None -> fallback target
                il.BeginException()
                CodecEmitter.readLocal il call (fun () -> il.EmitLocal(OpCodes.Ldloca,child)) result
                il.Catch input.PrimitiveError
                il.EmitLocal(OpCodes.Ldloca,child); il.EmitMethod(OpCodes.Call,input.PrimitiveRetry target)
                il.EmitLocal(OpCodes.Stloc,result)
                il.Catch input.ScalarError
                il.Emit OpCodes.Pop
                il.EmitLocal(OpCodes.Ldloca,child); il.EmitMethod(OpCodes.Call,input.ScalarRetry target)
                il.EmitLocal(OpCodes.Stloc,result)
                il.EndException()

            let labels = model.Cases |> Array.map(fun _ -> il.Label())
            for index = 0 to predicates.Length - 1 do
                il.EmitLocal(OpCodes.Ldloc,snd predicates[index]); il.EmitBranch(OpCodes.Brtrue,labels[index])
            let _,tagRange,tagPresent = slots[0]
            let hasTag = il.Label()
            il.EmitLocal(OpCodes.Ldloc,tagPresent); il.EmitBranch(OpCodes.Brtrue,hasTag)
            il.EmitMethod(OpCodes.Call,input.Missing)
            il.Mark hasTag
            let tag = il.Local api.Int32
            convert api.Int32 tagRange tag
            for index = 0 to model.Cases.Length - 1 do
                il.EmitLocal(OpCodes.Ldloc,tag); il.EmitInt(OpCodes.Ldc_I4,model.Cases[index].Tag)
                il.EmitBranch(OpCodes.Beq,labels[index])
            il.EmitString("Invalid Tag for type: " + model.RuntimeName)
            il.EmitMethod(OpCodes.Newobj,api.ExceptionConstructor); il.Emit OpCodes.Throw
            for index = 0 to model.Cases.Length - 1 do
                let case = model.Cases[index]
                il.Mark labels[index]
                let values = case.Fields |> Array.map(fun (name,target) ->
                    let _,range,presence = slots |> Array.find(fun (slot,_,_) -> slot = name)
                    let value = il.Local target
                    let missing = il.Label()
                    il.EmitLocal(OpCodes.Ldloc,presence); il.EmitBranch(OpCodes.Brfalse,missing)
                    convert target range value
                    il.Mark missing
                    value)
                for value in values do il.EmitLocal(OpCodes.Ldloc,value)
                il.EmitMethod(OpCodes.Call,case.Factory.Value)
                if model.CastUnionResult then il.EmitType(OpCodes.Castclass,model.Type)
                il.Emit OpCodes.Ret
